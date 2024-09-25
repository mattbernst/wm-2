package wiki.db

import scalikejdbc.*
import wiki.extractor.types.{
  DumpPage,
  PageMarkup,
  PageMarkup_U,
  PageMarkup_Z,
  PageType,
  PageTypes,
  REDIRECT,
  Redirection
}

import scala.collection.mutable

trait PageStorage {

  /**
    * Clear out all stored data from phase 1. This is used if the
    * extraction stage needs to run again (e.g. it was interrupted before
    * completion.)
    */
  def clearPhase01(): Unit = {
    DB.autoCommit { implicit session =>
      sql"""DELETE FROM last_transclusion_count"""
        .update()
      sql"""DELETE FROM page"""
        .update()
      sql"""DELETE FROM namespace"""
        .update()
      sql"""DELETE FROM page_markup"""
        .update()
      sql"""DELETE FROM page_markup_z"""
        .update()
      sql"""DELETE FROM title_to_page"""
        .update()
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
  def readRedirects(): Seq[Redirection] = {
    val redirectPageTypeId = PageTypes.bySymbol(REDIRECT)
    DB.autoCommit { implicit session =>
      sql"""SELECT id, title, redirect_target FROM page WHERE page_type=$redirectPageTypeId"""
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
    * Write page markup to the page_markup table. The page_markup table contains
    * the raw markup for each page along with a parsed derivative. This is
    * stored in a separate table because it is so much larger than the other
    * page data.
    *
    * @param input One or more PageMarkup_U entries to write
    */
  def writeMarkups(input: Seq[PageMarkup_U]): Unit = {
    val batches = input.grouped(batchInsertSize)
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
        sql"""INSERT INTO page_markup ($cols) VALUES $values""".update()
      }
    }
  }

  /**
    * Get the page markup data for a single page (if it exists) from the
    * page_markup table.
    *
    * @param pageId The numeric ID for the corresponding page
    * @return       The stored markup, if it exists
    */
  def readMarkup(pageId: Int): Option[PageMarkup] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT * FROM page_markup WHERE page_id=$pageId""".map { rs =>
        val pm = PageMarkup_U(rs.int("page_id"), rs.stringOpt("markup"), rs.stringOpt("parsed"))
        PageMarkup.deserializeUncompressed(pm)
      }.single()
    }
  }

  /**
    * Get the page markup data for a single page (if it exists) from the
    * page_markup_z table.
    *
    * @param pageId The numeric ID for the corresponding page
    * @return       The stored markup, if it exists
    */
  def readMarkup_Z(pageId: Int): Option[PageMarkup] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT * FROM page_markup_z WHERE page_id=$pageId""".map { rs =>
        val pmz = PageMarkup_Z(rs.int("page_id"), rs.bytes("data"))
        PageMarkup.deserializeCompressed(pmz)
      }.single()
    }
  }

  /**
    * Write compressed page markup and parsed data the page_markup_z table.
    * Writing in compressed form significantly reduces the disk space required
    * for the database and may be faster than standard uncompressed storage
    * on systems with relatively slow disks. The downside is that the
    * page_markup_z data is not human-readable.
    *
    * @param input One or more PageMarkup_Z entries to write
    */
  def writeMarkups_Z(input: Seq[PageMarkup_Z]): Unit = {
    val batches = input.grouped(batchInsertSize)
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
        sql"""INSERT INTO page_markup_z ($cols) VALUES $values""".update()
      }
    }
  }

  val batchInsertSize: Int = 2000
}
