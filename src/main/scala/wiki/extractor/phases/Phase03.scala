package wiki.extractor.phases

import wiki.db.{LinkSink, PageMarkupSource, Storage}
import wiki.extractor.PageMarkupLinkProcessor
import wiki.extractor.types.{PageType, SiteInfo, TypedPageMarkup, Worker}
import wiki.extractor.util.{Config, ConfiguredProperties}

class Phase03(db: Storage, props: ConfiguredProperties) extends Phase(db: Storage, props: ConfiguredProperties) {

  /**
    * Resolve links to destinations in link table (or add entry to dead_link,
    * if resolution was not possible.)
    */
  override def run(): Unit = {
    db.phase.deletePhase(number)
    db.phase.createPhase(number, s"Resolving links to destinations")
    db.executeUnsafely("DROP TABLE IF EXISTS link;")
    db.executeUnsafely("DROP TABLE IF EXISTS dead_link;")
    db.createTableDefinitions(number)
    val source   = new PageMarkupSource(db)
    val titleMap = db.page.readTitleToPage(props.language.locale)
    val categoryName = db.namespace
      .read(SiteInfo.CATEGORY_KEY)
      .map(_.name)
      .getOrElse(throw new NoSuchElementException("Could not find CATEGORY_KEY in namespace table"))
    val processor                    = new PageMarkupLinkProcessor(titleMap, props.language, categoryName)
    val sink                         = new LinkSink(db)
    val workers                      = assignLinkWorkers(props.nWorkers, processor, source.getFromQueue _, sink)
    val relevantPages: Set[PageType] = Set(PageType.ARTICLE, PageType.CATEGORY, PageType.DISAMBIGUATION)
    source.enqueueMarkup(relevantPages)
    workers.foreach(_.thread.join())
    sink.stopWriting()
    sink.writerThread.join()
    db.createIndexes(number)
    db.phase.completePhase(number)
  }

  private def assignLinkWorkers(
    n: Int,
    processor: PageMarkupLinkProcessor,
    source: () => Option[TypedPageMarkup],
    sink: LinkSink
  ): Seq[Worker] = {
    0.until(n).map { id =>
      processor.worker(id = id, source = source, sink = sink)
    }
  }

  override val incompleteMessage: String = s"Phase $number incomplete -- redoing"
  override def number: Int               = 3
}
