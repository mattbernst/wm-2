package wiki.db

import scalikejdbc.*
import wiki.extractor.types.*

import java.util.Locale
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object PageStorage {

  /**
    * Write pages to the page table. The page table contains all the Page
    * data except the raw markup.
    *
    * @param input One or more Pages to write
    */
  def writePages(input: Seq[Page]): Unit = {
    val batches = input.grouped(Storage.batchSqlSize)
    DB.autoCommit { implicit session =>
      val cols: SQLSyntax = sqls"""id, namespace_id, page_type, last_edited, markup_size, title, redirect_target"""
      batches.foreach { batch =>
        val params: Seq[Seq[SQLSyntax]] = batch.map(
          t =>
            Seq(
              sqls"${t.id}",
              sqls"${t.namespace.id}",
              sqls"${PageTypes.bySymbol(t.pageType)}",
              sqls"${t.lastEdited}",
              sqls"${t.markupSize}",
              sqls"${t.title}",
              sqls"${t.redirectTarget}"
            )
        )
        val values: SQLSyntax = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT OR IGNORE INTO $table ($cols) VALUES $values""".update()
      }
    }
  }

  /**
    * Get already-processed page IDs to accelerate partially completed stage01
    * page extraction. Any known ID can be skipped as soon as the page ID has
    * been extracted from its page fragment.
    *
    * @return The set of all completed page IDs
    */
  def getCompletedPageIds(): mutable.Set[Int] = {
    val result = mutable.Set[Int]()
    DB.autoCommit { implicit session =>
      sql"""SELECT page_id FROM markup""".foreach(rs => result.add(rs.int("page_id")): Unit)
      sql"""SELECT page_id FROM markup_z""".foreach(rs => result.add(rs.int("page_id")): Unit)
    }
    result
  }

  /**
    * Update a page's type in the page table to a new page type. This is used
    * to mark DANGLING_REDIRECT and UNPARSEABLE pages after they are detected.
    *
    * @param id       The numeric page identifier
    * @param pageType The new page type to assign
    */
  def updatePageType(id: Int, pageType: PageType): Unit = {
    val pageTypeId = PageTypes.bySymbol(pageType)
    DB.autoCommit { implicit session =>
      sql"""UPDATE $table SET page_type=$pageTypeId WHERE id=$id""".update(): Unit
    }
  }

  /**
    * Read redirects from the page table, giving title and redirect target. For
    * example, page 367 "Algeria/Transnational Issues" has redirectTarget
    * "Foreign relations of Algeria".
    *
    * @return All redirects known in the page title
    */
  def readRedirects(): Seq[Redirection] = {
    val redirectPageTypeId = PageTypes.bySymbol(PageType.REDIRECT)
    DB.autoCommit { implicit session =>
      sql"""SELECT id, title, redirect_target FROM $table WHERE page_type=$redirectPageTypeId"""
        .map(r => Redirection(r.int("id"), r.string("title"), r.string("redirect_target")))
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
    val result   = mutable.Map[String, Int]()
    val excluded = Seq(PageTypes.bySymbol(PageType.REDIRECT), PageTypes.bySymbol(PageType.UNPARSEABLE))
    val rows = DB.autoCommit { implicit session =>
      sql"""SELECT id, title FROM $table WHERE page_type NOT IN ($excluded)"""
        .map(r => (r.string("title"), r.int("id")))
        .list()
    }

    rows.foreach(r => result.put(r._1, r._2))
    result
  }

  /**
    * Get ARTICLE and CATEGORY pages for use in depth mapping.
    *
    * @return Page IDs keyed by page type
    */
  def getPagesForDepth(): mutable.Map[PageType, mutable.Set[Int]] = {
    val included = Seq(PageTypes.bySymbol(PageType.ARTICLE), PageTypes.bySymbol(PageType.CATEGORY))
    val result   = mutable.Map[PageType, mutable.Set[Int]]()

    DB.autoCommit { implicit session =>
      sql"""SELECT id, page_type FROM $table WHERE page_type IN ($included)""".foreach { r =>
        val key = PageTypes.byNumber(r.int("page_type"))
        if (result.contains(key)) {
          result(key).add(r.int("id")): Unit
        } else {
          val s = mutable.Set[Int]()
          s.add(r.int("id"))
          result(key) = s
        }
      }
    }

    result
  }

  /**
    * Get the page IDs for ARTICLE and DISAMBIGUATION pages. This is used to
    * select all the relevant pages from the link table during anchor statistic
    * generation.
    *
    * @return A sorted array of page IDs
    */
  def getAnchorPages(): Array[Int] =
    getPagesByTypes(Seq(PageType.ARTICLE, PageType.DISAMBIGUATION))

  /**
    * Get all page IDs for the given types of pages.
    *
    * @param types Symbolic page types to get IDs for
    * @return A sorted array of page IDs matching the given types
    */
  def getPagesByTypes(types: Seq[PageType]): Array[Int] = {
    val result       = ListBuffer[Int]()
    val numericTypes = types.map(t => PageTypes.bySymbol(t))
    DB.autoCommit { implicit session =>
      numericTypes.foreach { pt =>
        sql"""SELECT id FROM $table WHERE page_type=$pt"""
          .foreach(rs => result.append(rs.int("id")))
      }
    }

    result.toArray.sorted
  }

  /**
    * Count all page IDs for the given types of pages.
    *
    * @param types Symbolic page types to get IDs for
    * @return A count of how many pages exist with the given types
    */
  def countPagesByTypes(types: Seq[PageType]): Int = {
    var total        = 0
    val numericTypes = types.map(t => PageTypes.bySymbol(t))
    DB.autoCommit { implicit session =>
      numericTypes.foreach { pt =>
        sql"""SELECT count(*) AS ct FROM $table WHERE page_type=$pt"""
          .foreach(rs => total += rs.int("ct"))
      }
    }

    total
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
    val batches = source.grouped(Storage.batchSqlSize)
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
        sql"""INSERT OR IGNORE INTO title_to_page ($cols) VALUES $values""".update()
      }
    }
  }

  /**
    * Read the flattened TitleFinder data back from the db with all page
    * titles transformed to lower case.
    *
    * @return A map of page titles to page IDs
    */
  def readTitleToPage(locale: Locale): mutable.Map[String, Int] = {
    val result = mutable.Map[String, Int]()
    DB.autoCommit { implicit session =>
      sql"""SELECT * FROM title_to_page"""
        .foreach(rs => result.put(rs.string("title").toLowerCase(locale), rs.int("page_id")): Unit)
    }
    result
  }

  /**
    * Drop the title_to_page table before trying to build it again.
    */
  def clearTitleToPage(): Unit = {
    DB.autoCommit { implicit session =>
      sql"""DROP TABLE IF EXISTS title_to_page;""".update(): Unit
    }
  }

  /**
    * Write page markup to the markup table. The markup table contains
    * the raw markup for each page along with a parsed derivative. This is
    * stored in a separate table because it is so much larger than the other
    * page data.
    *
    * @param input One or more PageMarkup_U entries to write
    */
  def writeMarkups(input: Seq[PageMarkup_U]): Unit = {
    val batches = input.grouped(Storage.batchSqlSize)
    DB.autoCommit { implicit session =>
      batches.foreach { batch =>
        val cols: SQLSyntax = sqls"""page_id, markup, parsed"""
        val params: Seq[Seq[SQLSyntax]] = batch.map(
          t =>
            Seq(
              sqls"${t.pageId}",
              sqls"${t.wikitext}",
              sqls"${t.parseResult}"
            )
        )
        val values: SQLSyntax = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT OR IGNORE INTO markup ($cols) VALUES $values""".update()
      }
    }
  }

  /**
    * Get the page markup data for a single page (if it exists) from whichever
    * table this database is using.
    *
    * @param pageId The numeric ID for the corresponding page
    * @return       The stored markup, if it exists
    */
  def readMarkupAuto(pageId: Int): Option[PageMarkup] = {
    if (usingCompression) {
      readMarkup_Z(pageId)
    } else {
      readMarkup(pageId)
    }
  }

  /**
    * Get the page markup data for a single page (if it exists) from the
    * markup table.
    *
    * @param pageId The numeric ID for the corresponding page
    * @return The stored markup, if it exists
    */
  def readMarkup(pageId: Int): Option[PageMarkup] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT * FROM markup WHERE page_id=$pageId""".map { rs =>
        val pm = PageMarkup_U(rs.int("page_id"), rs.stringOpt("markup"), rs.stringOpt("parsed"))
        PageMarkup.deserializeUncompressed(pm)
      }.single()
    }
  }

  /**
    * Read multiple rows from markup, all IDs between start and end.
    * Also get page type by joining to page table.
    *
    * @param start Starting page ID
    * @param end Ending page ID
    * @return    All TypedPageMarkup with IDs between those ranges
    */
  def readMarkupSlice(start: Int, end: Int): Seq[TypedPageMarkup] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT page_type, markup.* FROM
           $table, markup
           WHERE page.id=page_id AND page_id >= $start AND page_id < $end""".map { rs =>
        val pmu = PageMarkup_U(rs.int("page_id"), rs.stringOpt("markup"), rs.stringOpt("parsed"))
        TypedPageMarkup(Some(pmu), None, PageTypes.byNumber(rs.int("page_type")))
      }.list()
    }
  }

  /**
    * Read multiple rows from markup_z, all IDs between start and end.
    *  Also get page type by joining to page table.
    *
    * @param start Starting page ID
    * @param end Ending page ID
    * @return    All TypedPageMarkup with IDs between those ranges
    */
  def readMarkupSlice_Z(start: Int, end: Int): Seq[TypedPageMarkup] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT page_type, markup_z.* FROM
           $table, markup_z
           WHERE page.id=page_id AND page_id >= $start AND page_id < $end""".map { rs =>
        val pmz = PageMarkup_Z(rs.int("page_id"), rs.bytes("data"))
        TypedPageMarkup(None, Some(pmz), PageTypes.byNumber(rs.int("page_type")))
      }.list()
    }
  }

  /**
    * Get the page markup data for a single page (if it exists) from the
    * markup_z table.
    *
    * @param pageId The numeric ID for the corresponding page
    * @return       The stored markup, if it exists
    */
  def readMarkup_Z(pageId: Int): Option[PageMarkup] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT * FROM markup_z WHERE page_id=$pageId""".map { rs =>
        val pmz = PageMarkup_Z(rs.int("page_id"), rs.bytes("data"))
        PageMarkup.deserializeCompressed(pmz)
      }.single()
    }
  }

  /**
    * Write compressed page markup and parsed data the markup_z table.
    * Writing in compressed form significantly reduces the disk space required
    * for the database and may be faster than standard uncompressed storage
    * on systems with relatively slow disks. The downside is that the
    * markup_z data is not human-readable.
    *
    * @param input One or more PageMarkup_Z entries to write
    */
  def writeMarkups_Z(input: Seq[PageMarkup_Z]): Unit = {
    val batches = input.grouped(Storage.batchSqlSize)
    DB.autoCommit { implicit session =>
      batches.foreach { batch =>
        val cols: SQLSyntax = sqls"""page_id, data"""
        val params: Seq[Seq[SQLSyntax]] = batch.map(
          t =>
            Seq(
              sqls"${t.pageId}",
              sqls"${t.data}"
            )
        )
        val values: SQLSyntax = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT OR IGNORE INTO markup_z ($cols) VALUES $values""".update()
      }
    }
  }

  lazy val compressedMax: Int = {
    DB.autoCommit { implicit session =>
      sql"""SELECT COALESCE(MAX(page_id), 0) AS m FROM markup_z"""
        .map(rs => rs.int("m"))
        .list()
        .head
    }
  }

  lazy val uncompressedMax: Int = {
    DB.autoCommit { implicit session =>
      sql"""SELECT COALESCE(MAX(page_id), 0) AS m FROM markup"""
        .map(rs => rs.int("m"))
        .list()
        .head
    }
  }

  lazy val usingCompression: Boolean = {
    val err1 = "Same page counts for markup and markup_z. Did extraction run?"
    require(compressedMax != uncompressedMax, err1)
    val err2 = "Both markup and markup_z have entries. This should not happen."
    require(compressedMax == 0 || uncompressedMax == 0, err2)
    compressedMax > uncompressedMax
  }

  private val table = Storage.table("page")
}
