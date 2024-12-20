package wiki.extractor.phases

import wiki.db.Storage
import wiki.extractor.TitleFinder
import wiki.extractor.types.{PageType, Redirection}
import wiki.extractor.util.DBLogging

class Phase02(db: Storage) extends Phase(db: Storage) {

  /**
    * Resolve title_to_page mapping and index the new table.
    */
  override def run(): Unit = {
    db.phase.deletePhase(number)
    db.phase.createPhase(number, s"Building title_to_page map")
    db.page.clearTitleToPage()
    db.createTableDefinitions(number)
    val danglingRedirects = storeMappedTitles()
    markDanglingRedirects(danglingRedirects)
    db.createIndexes(number)
    db.phase.completePhase(number)
  }

  /**
    * Resolve all title-to-ID mappings (e.g. resolve redirects) and store the
    * flattened data in the title_to_page table.
    *
    * @return Dangling redirects that need their page type updated
    */
  private def storeMappedTitles(): Seq[Redirection] = {
    DBLogging.info(s"Getting TitleFinder data")
    val redirects = db.page.readRedirects()
    DBLogging.info(s"Loaded ${redirects.length} redirects")
    val titlePageMap = db.page.readTitlePageMap()
    DBLogging.info(s"Loaded ${titlePageMap.size} title-to-ID map entries")
    val tf = new TitleFinder(titlePageMap, redirects)
    DBLogging.info(s"Started writing title to page ID mappings to db")
    val fpm = tf.getFlattenedPageMapping()
    db.page.writeTitleToPage(fpm)
    DBLogging.info(s"Finished writing ${fpm.length} title to page ID mappings to db")
    tf.danglingRedirects
  }

  /**
    * Mark all dangling redirect pages that were discovered during
    * redirect resolution. This has to run after createIndexes() or
    * it takes way too long to find the page by ID.
    *
    * @param input Dangling redirect pages that need to be marked
    */
  private def markDanglingRedirects(input: Seq[Redirection]): Unit = {
    input.foreach(r => db.page.updatePageType(r.pageId, PageType.DANGLING_REDIRECT))
    DBLogging.info(s"Marked ${input.length} pages as ${PageType.DANGLING_REDIRECT}")
  }

  override def number: Int               = 2
  override val incompleteMessage: String = s"Phase $number incomplete -- redoing"
}
