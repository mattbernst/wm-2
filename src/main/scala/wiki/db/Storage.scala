package wiki.db

import scalikejdbc.*
import wiki.extractor.types.*
import wiki.extractor.util.{FileHelpers, Logging, ZString}

import scala.collection.mutable

case class Redirect(pageId: Int, title: String, redirectTarget: String)

/**
  * A SQLite database storage writer and reader for representing and mining
  * extracted Wikipedia data.
  *
  * @param fileName The name of the on-disk file containing the SQLite db
  */
class Storage(fileName: String) extends Logging {
  ConnectionPool.singleton(url = s"jdbc:sqlite:$fileName", user = null, password = null)

  /**
    * These counts give the number of times each named transclusion appears as
    * the last transclusion in a page. The accumulated statistics can help to
    * configure the disambiguationPrefixes for a new language in languages.json
    *
    * @param input A map of transclusion names to counts
    */
  def writeLastTransclusionCounts(input: Map[String, Int]): Unit = {
    val batches = input.toSeq.sorted.grouped(batchInsertSize)
    DB.autoCommit { implicit session =>
      batches.foreach { batch =>
        val params: Seq[Seq[SQLSyntax]] = batch.map(t => Seq(sqls"${t._1}", sqls"${t._2}"))
        val cols: SQLSyntax             = sqls"""name, n"""
        val values: SQLSyntax           = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT INTO last_transclusion_count ($cols) VALUES $values""".update()
      }
    }
  }

  /**
    * Write a namespace to the namespace table. This table provides a permanent
    * record of the namespaces encountered in the input Wikipedia dump at
    * extraction time.
    *
    * @param input A Namespace to persist
    */
  def writeNamespace(input: Namespace): Unit = {
    DB.autoCommit { implicit session =>
      sql"""INSERT INTO namespace
           (id, casing, name) VALUES (${input.id}, ${input.casing}, ${input.name})""".update(): Unit
    }
  }

  /**
    * Read a namespace from the namespace table. A namespace may exist in the
    * Wikipedia dump file but be absent from the namespace table if it is not
    * one of the valid namespaces extracted by fragmentToPage in
    * FragmentProcessor.scala
    *
    * @param id The numeric ID of the namespace to read
    * @return   A namespace, if found in the table
    */
  def readNamespace(id: Int): Option[Namespace] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT * FROM namespace WHERE id=$id""".map { rs =>
        val casing: Casing = rs.string("casing") match {
          case "FIRST_LETTER"   => FIRST_LETTER
          case "CASE_SENSITIVE" => CASE_SENSITIVE
        }
        Namespace(id = rs.int("id"), casing = casing, name = rs.string("name"))
      }.single()
    }
  }

  /**
    * Write dump pages to the page table. The page table contains all the DumpPage
    * data except the raw markup.
    *
    * @param input One or more DumpPages to write
    */
  def writeDumpPages(input: Seq[DumpPage]): Unit = {
    val batches = input.grouped(batchInsertSize)
    DB.autoCommit { implicit session =>
      batches.foreach { batch =>
        val cols: SQLSyntax = sqls"""id, namespace_id, page_type, last_edited, title, redirect_target"""
        val params: Seq[Seq[SQLSyntax]] = batch.map(
          t =>
            Seq(
              sqls"${t.id}",
              sqls"${t.namespace.id}",
              sqls"${PageTypes.bySymbol(t.pageType)}",
              sqls"${t.lastEdited}",
              sqls"${t.title}",
              sqls"${t.redirectTarget}"
            )
        )
        val values: SQLSyntax = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT INTO page ($cols) VALUES $values""".update()
      }
    }
  }

  /**
    * Update a page's type in the page table to a new page type. This is used
    * to mark DANGLING_REDIRECT pages identified by the TitleFinder after all
    * page data has been initially written.
    *
    * @param id       The numeric page identifier
    * @param pageType The new page type to assign
    */
  def updatePageType(id: Int, pageType: PageType): Unit = {
    val pageTypeId = PageTypes.bySymbol(pageType)
    DB.autoCommit { implicit session =>
      sql"""UPDATE page SET page_type=$pageTypeId WHERE id=$id""".update(): Unit
    }
  }

  /**
    * Read redirects from the page table, giving title and redirect target. For
    * example, page 367 "Algeria/Transnational Issues" has redirectTarget
    * "Foreign relations of Algeria".
    *
    * @return All redirects known in the page title
    */
  def readRedirects(): Seq[Redirect] = {
    val redirectPageTypeId = PageTypes.bySymbol(REDIRECT)
    DB.autoCommit { implicit session =>
      sql"""SELECT id, title, redirect_target FROM page WHERE page_type=$redirectPageTypeId"""
        .map(r => Redirect(r.int("id"), r.string("title"), r.string("redirect_target")))
        .list()
    }
  }

  /**
    * Read non-redirect data from the page table, mapping title to page ID. For
    * example, "Foreign relations of Algeria" gets mapped to its page ID 67579.
    * By combining this table's map with the map from readRedirectMap(), titles
    * of redirected pages can be mapped to the IDs of their redirect targets.
    *
    * Page 367 "Algeria/Transnational Issues" (a redirect) would get mapped to
    * page 67579.
    *
    * @return A map of titles to page IDs for non-redirecting pages
    */
  def readTitlePageMap(): mutable.Map[String, Int] = {
    val result             = mutable.Map[String, Int]()
    val redirectPageTypeId = PageTypes.bySymbol(REDIRECT)
    val rows = DB.autoCommit { implicit session =>
      sql"""SELECT id, title FROM page WHERE page_type != $redirectPageTypeId"""
        .map(r => (r.string("title"), r.int("id")))
        .list()
    }

    rows.foreach(r => result.put(r._1, r._2))
    result
  }

  /**
    * Write the flattened TitleFinder data into title_to_page. This table gives
    * a direct mapping from every title to its destination page ID.
    *
    * Occasionally a page has a redirect target that does not actually exist
    * (deleted page?). In those cases the page_id is null.
    *
    * @param source A sequence of data provided by TitleFinder getFlattened()
    */
  def writeTitleToPage(source: Seq[(String, Int)]): Unit = {
    val batches = source.grouped(batchInsertSize)
    DB.autoCommit { implicit session =>
      batches.foreach { batch =>
        val cols: SQLSyntax = sqls"""title, page_id"""
        val params: Seq[Seq[SQLSyntax]] = batch.map(
          t =>
            Seq(
              sqls"${t._1}",
              sqls"${t._2}"
            )
        )
        val values: SQLSyntax = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT INTO title_to_page ($cols) VALUES $values""".update()
      }
    }
  }

  /**
    * Write page markup to the page_markup table. The page_markup table only
    * contains the raw markup for each page. The markup is stored in a separate
    * table because it is so much larger than the other page data.
    *
    * @param input One or more MarkupU tuples to write
    */
  def writeMarkups(input: Seq[Storage.MarkupU]): Unit = {
    val batches = input.grouped(batchInsertSize)
    DB.autoCommit { implicit session =>
      batches.foreach { batch =>
        val cols: SQLSyntax = sqls"""page_id, markup, json"""
        val params: Seq[Seq[SQLSyntax]] = batch.map(
          t =>
            Seq(
              sqls"${t._1}",
              sqls"${t._2}",
              sqls"${t._3}"
            )
        )
        val values: SQLSyntax = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT INTO page_markup ($cols) VALUES $values""".update()
      }
    }
  }

  /**
    * Get the markup for a single page (if it exists) from the page_markup
    * table.
    *
    * @param pageId The numeric ID for the corresponding page
    * @return       The stored markup, if it exists
    */
  def readMarkup(pageId: Int): Option[String] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT markup FROM page_markup WHERE page_id=$pageId""".map(rs => rs.stringOpt("markup")).single().flatten
    }
  }

  /**
    * Get the raw markup for a single page (if it exists) from the
    * page_markup_z table.
    *
    * @param pageId The numeric ID for the corresponding page
    * @return       The stored markup, if it exists
    */
  def readMarkup_Z(pageId: Int): Option[String] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT markup FROM page_markup_z WHERE page_id=$pageId"""
        .map(rs => rs.bytesOpt("markup").map(bin => ZString.decompress(bin)))
        .single()
        .flatten
    }
  }

  /**
    * Write compressed page markup and JSON to the page_markup_z table. Writing
    * in compressed form significantly reduces the disk space required for
    * the database and may be faster than standard uncompressed storage
    * on systems with relatively slow disks. The downside is that the
    * page_markup_z data is not human-readable.
    *
    * @param input One or more MarkupZ tuples to write
    */
  def writeMarkups_Z(input: Seq[Storage.MarkupZ]): Unit = {
    val batches = input.grouped(batchInsertSize)
    DB.autoCommit { implicit session =>
      batches.foreach { batch =>
        val cols: SQLSyntax = sqls"""page_id, markup, json"""
        val params: Seq[Seq[SQLSyntax]] = batch.map(
          t =>
            Seq(
              sqls"${t._1}",
              sqls"${t._2}",
              sqls"${t._3}"
            )
        )
        val values: SQLSyntax = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT INTO page_markup_z ($cols) VALUES $values""".update()
      }
    }
  }

  /**
    *  Create all tables from the .sql files in sql/tables/
    */
  def createTableDefinitions(): Unit = {
    val pattern          = "sql/tables/*.sql"
    val tableDefinitions = FileHelpers.glob(pattern).sorted
    require(tableDefinitions.nonEmpty, s"No SQL files for tables found in $pattern")
    tableDefinitions.foreach { fileName =>
      val sql = FileHelpers.readTextFile(fileName)
      logger.info(s"Creating table from $fileName")
      executeUnsafely(sql)
    }
  }

  /**
    * Create all indexes from the .sql files in sql/indexes/
    * This should be run after initial bulk-loading of rows into tables,
    * because adding an index after rows are created is faster than
    * having the index in place while rows are being created.
    */
  def createIndexes(): Unit = {
    val pattern          = "sql/indexes/*.sql"
    val tableDefinitions = FileHelpers.glob(pattern).sorted
    require(tableDefinitions.nonEmpty, s"No SQL files for indexes found in $pattern")
    tableDefinitions.foreach { fileName =>
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

  val batchInsertSize: Int = 10000
}

object Storage {
  type MarkupZ = (Int, Option[Array[Byte]], Option[Array[Byte]])
  type MarkupU = (Int, Option[String], Option[String])

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
          throw new RuntimeException(
            "Failed to execute sqls :" + sqls + " " + errors
          )
      }
    }
    loop(sqls.toList, Nil)
  }
}
