package wiki.extractor

import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import wiki.db.*
import wiki.extractor.phases.{Phase01, Phase02}
import wiki.extractor.types.*
import wiki.extractor.util.{Config, DBLogging, Logging}

object WikipediaExtractor extends Logging {

  def main(args: Array[String]): Unit = {
    DBLogging.initDb(db)
    init()
    lazy val phase01 = new Phase01(1, db, Config.props)
    lazy val phase02 = new Phase02(2, db, Config.props)
    db.phase.getPhaseState(1) match {
      case Some(COMPLETED) =>
        logger.info(phase01.finishedMessage)
      case Some(CREATED) =>
        logger.warn(phase01.incompleteMessage)
        phase01.run(args)
      case None =>
        phase01.run(args)
    }
    db.phase.getPhaseState(2) match {
      case Some(COMPLETED) =>
        logger.info(phase02.finishedMessage)
      case Some(CREATED) =>
        logger.warn(phase02.incompleteMessage)
        phase02.run()
      case None =>
        phase02.run()
    }
    db.phase.getPhaseState(3) match {
      case Some(COMPLETED) =>
        logger.info("Already completed phase 3")
      case Some(CREATED) =>
        logger.warn("Phase 3 incomplete -- redoing")
        phase03()
      case None =>
        phase03()
    }

    db.phase.getPhaseState(4) match {
      case Some(COMPLETED) =>
        logger.info("Already completed phase 4")
      case Some(CREATED) =>
        logger.warn("Phase 4 incomplete -- restarting")
        phase04()
      case None =>
        phase04()
    }

    db.closeAll()
  }

  // Initialize system tables before running any extraction
  private def init(): Unit = {
    db.createTableDefinitions(0)
  }

  /**
    * Resolve links to destinations in link table (or add entry to dead_link,
    * if resolution was not possible.)
    */
  private def phase03(): Unit = {
    val phase = 3
    db.phase.deletePhase(phase)
    db.phase.createPhase(phase, s"Resolving links to destinations")
    db.executeUnsafely("DROP TABLE IF EXISTS link;")
    db.executeUnsafely("DROP TABLE IF EXISTS dead_link;")
    db.createTableDefinitions(phase)
    val source   = new PageMarkupSource(db)
    val titleMap = db.page.readTitleToPage()
    val categoryName = db.namespace
      .read(SiteInfo.CATEGORY_KEY)
      .map(_.name)
      .getOrElse(throw new NoSuchElementException("Could not find CATEGORY_KEY in namespace table"))
    val processor = new PageMarkupLinkProcessor(titleMap, Config.props.language, categoryName)
    val sink      = new LinkSink(db)
    val workers   = assignLinkWorkers(Config.props.nWorkers, processor, source.getFromQueue _, sink)
    source.enqueueMarkup()
    workers.foreach(_.thread.join())
    sink.stopWriting()
    sink.writerThread.join()
    db.createIndexes(phase)
    db.phase.completePhase(phase)
  }

  // Assign a page depth to categories and articles
  private def phase04(): Unit = {
    val phase = 4
    db.phase.deletePhase(phase)
    db.createTableDefinitions(phase)
    val rootPage = Config.props.language.rootPage
    db.phase.createPhase(phase, s"Mapping depth starting from $rootPage")
    DBLogging.info(s"Getting candidates for depth mapping")
    val pageGroups: Map[PageType, Set[Int]] = db.page.getPagesForDepth()
    DBLogging.info(s"Got ${pageGroups.values.map(_.size).sum} candidates for depth mapping")
    DBLogging.info(s"Getting source-to-destination mapping")

    val destinationCache: LoadingCache[Int, Seq[Int]] =
      Scaffeine()
        .maximumSize(10000000)
        .build(loader = (id: Int) => {
          db.link.getBySource(id).map(_.destination)
        })

    val sink           = new DepthSink(db)
    var completedCount = 0

    val maxDepth = 31
    1.until(maxDepth).foreach { depthLimit =>
      val processor = new DepthProcessor(db, sink, pageGroups, destinationCache, depthLimit)
      processor.markDepths(rootPage)
      completedCount += db.depth.count(depthLimit)
      DBLogging.info(s"Completed marking $completedCount pages to max depth $depthLimit")
    }

    sink.stopWriting()
    sink.writerThread.join()
    db.phase.completePhase(phase)
  }

  private val db: Storage =
    new Storage(fileName = Config.props.language.code + "_wiki.db")

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
}
