package wiki.extractor.phases

import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import wiki.db.{DepthSink, Storage}
import wiki.extractor.DepthProcessor
import wiki.extractor.types.PageType
import wiki.extractor.util.{Config, ConfiguredProperties, DBLogging}

class Phase04(db: Storage, props: ConfiguredProperties) extends Phase(db: Storage, props: ConfiguredProperties) {

  // Assign a page depth to categories and articles
  override def run(): Unit = {
    db.phase.deletePhase(number)
    db.createTableDefinitions(number)
    val rootPage = Config.props.language.rootPage
    db.phase.createPhase(number, s"Mapping depth starting from $rootPage")
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
    db.phase.completePhase(number)
  }

  override val incompleteMessage: String = s"Phase $number incomplete -- restarting"
  override def number: Int               = 4
}
