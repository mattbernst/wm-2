package wiki.db

import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import scalikejdbc.*
import wiki.extractor.types.{Namespace, Page, PageTypes}
import wiki.extractor.util.{FileHelpers, Logging}

/**
  * A SQLite database storage writer and reader for representing and mining
  * extracted Wikipedia data.
  *
  * @param fileName The name of the on-disk file containing the SQLite db
  */
class Storage(fileName: String) extends Logging {
  ConnectionPool.singleton(url = s"jdbc:sqlite:$fileName", user = null, password = null)

  /**
    * Try to get a single Page from storage. This is implemented here instead
    * of in PageStorage because it needs elements from PageStorage and from
    * NamespaceStorage.
    *
    * @param pageId A numeric ID for a page to retrieve
    * @return       The full page record for the ID, if retrievable
    */
  def getPage(pageId: Int): Option[Page] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT * from page where id=$pageId""".map { r =>
        Page(
          id = r.int("id"),
          namespace = namespaceCache.get(r.int("namespace_id")),
          pageType = PageTypes.byNumber(r.int("page_type")),
          depth = r.intOpt("depth"),
          title = r.string("title"),
          redirectTarget = r.stringOpt("redirect_target"),
          lastEdited = r.long("last_edited")
        )
      }.single()
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
      sql"""SELECT * from page where title=$title""".map { r =>
        Page(
          id = r.int("id"),
          namespace = namespaceCache.get(r.int("namespace_id")),
          pageType = PageTypes.byNumber(r.int("page_type")),
          depth = r.intOpt("depth"),
          title = r.string("title"),
          redirectTarget = r.stringOpt("redirect_target"),
          lastEdited = r.long("last_edited")
        )
      }.single()
    }
  }

  def closeAll(): Unit = {
    ConnectionPool.closeAll()
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

  val link: LinkStorage.type                 = LinkStorage
  val log: LogStorage.type                   = LogStorage
  val namespace: NamespaceStorage.type       = NamespaceStorage
  val page: PageStorage.type                 = PageStorage
  val phase: PhaseStorage.type               = PhaseStorage
  val transclusion: TransclusionStorage.type = TransclusionStorage

  private lazy val namespaceCache: LoadingCache[Int, Namespace] =
    Scaffeine()
      .build(loader = (id: Int) => {
        namespace.read(id).getOrElse {
          throw new NoSuchElementException(s"Could not retrieve namespace $id")
        }
      })
}

object Storage extends Logging {

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
}
