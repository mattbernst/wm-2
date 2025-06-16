package wiki.extractor.phases

import wiki.db.{SenseSink, Storage}
import wiki.extractor.AnchorLogic
import wiki.extractor.types.WordSense
import wiki.extractor.util.{ConfiguredProperties, DBLogging}

import java.util
import scala.collection.mutable

class Phase06(db: Storage) extends Phase(db: Storage) {

  override def run(): Unit = {
    db.phase.deletePhase(number)
    db.phase.createPhase(number, s"Gathering label senses")
    db.createTableDefinitions(number)
    DBLogging.info("Loading known labels from db")
    val targets = db.label.readKnownLabels()
    DBLogging.info("Counting label senses")
    countSenses(targets)
    db.createIndexes(number)
    DBLogging.info("Getting source counts by destination")
    val sourceCounts = db.link.getSourceCountsByDestination()
    DBLogging.info("Writing source counts by destination")
    db.link.writeSourceCountsByDestination(sourceCounts)
    DBLogging.info("Getting destination counts by source")
    val destinationCounts = db.link.getDestinationCountsBySource()
    DBLogging.info("Writing destination counts by source")
    db.link.writeDestinationCountsBySource(destinationCounts)
    db.phase.completePhase(number)
  }

  /**
    * Count the number of valid senses for labels.
    *
    * @param targets Valid labels mapped to their label IDs
    */
  private def countSenses(targets: mutable.Map[String, Int]): Unit = {
    val anchorLogic = new AnchorLogic(props.language)
    DBLogging.info("Loading relevant pages from db")
    val anchorPages = db.page.getAnchorPages()
    DBLogging.info("Loading grouped links from db")
    val groupedLinks = db.link.getGroupedLinks()
    DBLogging.info("Loaded grouped links from db")
    val sink = new SenseSink(db)

    // Clean anchors in-place. Bad anchors become empty strings,
    // and will not match anything in targets.
    groupedLinks.labels.mapInPlace(l => anchorLogic.cleanAnchor(l))
    val transitions = changes(groupedLinks.labels)

    DBLogging.info("Storing sense counts for labels")
    if (transitions.isEmpty) {
      DBLogging.error(s"Did not find any link groups to process")
    } else {
      var left  = 0
      var right = 0
      var j     = 0
      while (j < transitions.length) {
        right = transitions(j)
        val cleanSlice = groupedLinks
          .slice(left, right)
          .filter(e => targets.contains(e.label))
          .filter(e => util.Arrays.binarySearch(anchorPages, e.destination) >= 0)

        if (cleanSlice.nonEmpty) {
          val labelId           = targets(cleanSlice.head.label)
          val destinationCounts = mutable.Map[Int, Int]()
          cleanSlice.foreach(e => destinationCounts.put(e.destination, e.count))
          sink.addSense(WordSense(labelId = labelId, senseCounts = destinationCounts))
        }

        j += 1
        left = right
      }
    }

    sink.stopWriting()
    sink.writerThread.join()
  }

  private def changes(arr: Array[String]): Array[Int] = {
    if (arr.isEmpty) {
      Array.empty
    } else {
      arr.zipWithIndex
        .sliding(2)
        .collect { case Array((a, _), (b, idx)) if a != b => idx }
        .toArray
    }
  }

  private lazy val props: ConfiguredProperties =
    db.configuration.readConfiguredPropertiesOptimistic()

  override def number: Int               = 6
  override val incompleteMessage: String = s"Phase $number incomplete -- redoing"
}
