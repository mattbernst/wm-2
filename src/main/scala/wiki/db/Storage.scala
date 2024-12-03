package wiki.db

import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import scalikejdbc.*
import wiki.extractor.types.*
import wiki.extractor.util.{FileHelpers, Logging, Progress}

import java.util

/**
  * A SQLite database storage writer and reader for representing and mining
  * extracted Wikipedia data.
  *
  * @param fileName The name of the on-disk file containing the SQLite db
  */
class Storage(fileName: String) extends Logging {
  ConnectionPool.singleton(url = s"jdbc:sqlite:$fileName", user = null, password = null)

  /**
    * Try to get one or more Page records from storage. This is implemented
    * here instead of in PageStorage because it needs elements from PageStorage
    * and from NamespaceStorage.
    *
    * @param pageIds Numeric IDs for pages to retrieve
    * @return       The full page records for the IDs, where retrievable
    */
  def getPages(pageIds: Seq[Int]): Seq[Page] = {
    val batches = pageIds.grouped(Storage.batchSqlSize).toSeq
    DB.autoCommit { implicit session =>
      batches.flatMap { batch =>
        sql"""SELECT * FROM page WHERE id IN ($batch)""".map { r =>
          Page(
            id = r.int("id"),
            namespace = namespaceCache.get(r.int("namespace_id")),
            pageType = PageTypes.byNumber(r.int("page_type")),
            title = r.string("title"),
            redirectTarget = r.stringOpt("redirect_target"),
            lastEdited = r.long("last_edited"),
            markupSize = r.intOpt("markup_size")
          )
        }.list()
      }
    }
  }

  /**
    * Try to get a single Page from storage by title. This is implemented here
    * instead of in PageStorage because it needs elements from PageStorage and
    * from NamespaceStorage.
    *
    * @param title A page title a page to retrieve
    * @return      The full page record for the title, if retrievable
    */
  def getPage(title: String): Option[Page] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT * FROM page WHERE title=$title""".map { r =>
        Page(
          id = r.int("id"),
          namespace = namespaceCache.get(r.int("namespace_id")),
          pageType = PageTypes.byNumber(r.int("page_type")),
          title = r.string("title"),
          redirectTarget = r.stringOpt("redirect_target"),
          lastEdited = r.long("last_edited"),
          markupSize = r.intOpt("markup_size")
        )
      }.single()
    }
  }

  /**
    * Get links from the link table.
    * Only includes links to articles and disambiguation pages. This is used to
    * set up the LabelCounter data before processing raw text of each page.
    *
    * This needs to be an iterator because memory requirements are excessive
    * to fetch all results in one query.
    *
    * @return An iterator of Anchors
    */
  def getLinkAnchors(): Iterator[Anchor] = {
    val batchSize = Storage.batchSqlSize
    val targets   = page.getAnchorPages()

    new Iterator[Anchor] {
      private var total                       = 0
      private var index                       = 0
      private var offset                      = 0
      private var currentBatch: Array[Anchor] = Array()

      def fetchNextBatch(): Unit = {
        val pageIds = targets.slice(offset, offset + batchSize)
        val lower   = pageIds.headOption.getOrElse(Int.MaxValue)
        val upper   = pageIds.lastOption.map(_ + 1).getOrElse(Int.MaxValue)
        val rows = DB.autoCommit { implicit session =>
          sql"""SELECT source, destination, anchor_text
             FROM link
             WHERE source >= $lower AND source < $upper
             ORDER BY anchor_text
             """
            .map(rs => Anchor(rs.int("source"), rs.int("destination"), rs.string("anchor_text")))
            .list()
            .filter(a => util.Arrays.binarySearch(targets, a.destination) >= 0)
        }
        currentBatch = rows.toArray
        offset += batchSize
      }

      override def hasNext: Boolean = {
        if (index >= currentBatch.length) {
          if (currentBatch.isEmpty || currentBatch.length < batchSize) {
            false
          } else {
            fetchNextBatch()
            index = 0
            currentBatch.nonEmpty
          }
        } else {
          true
        }
      }

      override def next(): Anchor = {
        if (!hasNext) {
          throw new NoSuchElementException("Iterator is empty")
        }
        val result = currentBatch(index)
        total += 1
        Progress.tick(total, "+", 100_000)
        index += 1
        result
      }

      // Initialize first batch
      fetchNextBatch()

      override def nextOption(): Option[Anchor] = {
        if (hasNext) {
          Some(currentBatch(index))
        } else {
          None
        }
      }
    }
  }

  // Create tables for multiple phases at once
  def createTableDefinitions(phases: Seq[Int]): Unit =
    phases.foreach(phase => createTableDefinitions(phase))

  /**
    *  Create all tables from the .sql files in sql/tables/phaseXX
    */
  def createTableDefinitions(phase: Int): Unit = {
    val paddedPhase      = String.format("%02d", phase)
    val pattern          = s"sql/tables/phase$paddedPhase/*.sql"
    val tableDefinitions = FileHelpers.glob(pattern).sorted
    if (tableDefinitions.isEmpty) {
      logger.warn(s"No SQL files for tables found in $pattern")
    }
    tableDefinitions.foreach { fileName =>
      val sql = FileHelpers.readTextFile(fileName)
      logger.info(s"Creating table from $fileName")
      executeUnsafely(sql)
    }
  }

  // Create indexes for multiple phases at once
  def createIndexes(phases: Seq[Int]): Unit =
    phases.foreach(phase => createIndexes(phase))

  /**
    * Create all indexes from the .sql files in sql/indexes/phaseXX
    *  Separating index-creation from table-creation can improve performance
    *  for bulk data inserts, because adding an index after rows are created
    *  is faster than having the index in place while rows are being created.
    */
  def createIndexes(phase: Int): Unit = {
    val paddedPhase      = String.format("%02d", phase)
    val pattern          = s"sql/indexes/phase$paddedPhase/*.sql"
    val indexDefinitions = FileHelpers.glob(pattern).sorted
    if (indexDefinitions.isEmpty) {
      logger.warn(s"No SQL files for indexes found in $pattern")
    }
    indexDefinitions.foreach { fileName =>
      val sql = FileHelpers.readTextFile(fileName)
      logger.info(s"Creating index from $fileName")
      executeUnsafely(sql)
    }
  }

  /**
    * Execute arbitrary commands on the database without any safety checks.
    * Returns nothing. Useful for creating tables and indexes plus setting
    * SQLite pragmas.
    *
    * @param command Anything to tell the database.
    */
  def executeUnsafely(command: String): Unit = {
    DB.autoCommit { implicit session =>
      Storage.execute(command)
    }
  }

  def closeAll(): Unit = {
    ConnectionPool.closeAll()
  }

  val configuration: ConfigurationStorage.type = ConfigurationStorage
  val depth: DepthStorage.type                 = DepthStorage
  val label: LabelStorage.type                 = LabelStorage
  val link: LinkStorage.type                   = LinkStorage
  val log: LogStorage.type                     = LogStorage
  val namespace: NamespaceStorage.type         = NamespaceStorage
  val page: PageStorage.type                   = PageStorage
  val phase: PhaseStorage.type                 = PhaseStorage
  val transclusion: TransclusionStorage.type   = TransclusionStorage

  private lazy val namespaceCache: LoadingCache[Int, Namespace] =
    Scaffeine()
      .build(loader = (id: Int) => {
        namespace.read(id).getOrElse {
          throw new NoSuchElementException(s"Could not retrieve namespace $id")
        }
      })
}

object Storage extends Logging {

  /**
    * Generate table name reference for use in SQL.
    *
    * @param name Name of the table
    * @return SQLSyntax for the table that can be used in queries
    */
  def table(name: String): SQLSyntax = SQLSyntax.createUnsafely(name)

  def enableSqlitePragmas(db: Storage): Unit = {
    val pragmas = Seq(
      "pragma cache_size=1048576;",
      "pragma journal_mode=wal;",
      "pragma synchronous=normal;"
    )

    pragmas.foreach { pragma =>
      db.executeUnsafely(pragma)
      logger.info(s"Applied SQLite pragma: $pragma")
    }
  }

  def execute(sqls: String*)(implicit session: DBSession): Unit = {
    @annotation.tailrec
    def loop(xs: List[String], errors: List[Throwable]): Unit = {
      xs match {
        case sql :: t =>
          try {
            SQL(sql).execute.apply(): Unit
          } catch {
            case e: Exception =>
              loop(t, e :: errors)
          }
        case Nil =>
          throw new RuntimeException(s"Failed to execute sqls : $sqls ($errors)")
      }
    }
    loop(sqls.toList, Nil)
  }

  val batchSqlSize: Int = 5_000
}
