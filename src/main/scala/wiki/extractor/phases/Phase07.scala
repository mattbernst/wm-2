package wiki.extractor.phases

import wiki.db.{SenseSink, Storage}
import wiki.extractor.AnchorLogic
import wiki.extractor.types.{Anchor, Sense}
import wiki.extractor.util.{ConfiguredProperties, DBLogging}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Phase07(db: Storage) extends Phase(db: Storage) {

  override def run(): Unit = {
    db.phase.deletePhase(number)
    db.phase.createPhase(number, s"Gathering label senses")
    db.createTableDefinitions(number)
    DBLogging.info("Loading known labels from db")
    val targets = db.label.readKnownLabels()
    DBLogging.info("Counting label senses")
    countSenses(targets)
    db.createIndexes(number)
    db.phase.completePhase(number)
  }

  /**
    * @param targets Valid labels mapped to their label IDs
    */
  private def countSenses(targets: mutable.Map[String, Int]): Unit = {
    val anchorLogic     = new AnchorLogic(props.language)
    val sink: SenseSink = new SenseSink(db)

    val anchorIterator = db
      .getLinkAnchors()
      .filter(a => anchorLogic.cleanAnchor(a.text).nonEmpty)

    var label = anchorIterator
      .nextOption()
      .map(_.text)
      .getOrElse(throw new IndexOutOfBoundsException("No starting label found!"))

    val buffer = ListBuffer[Anchor]()
    anchorIterator.foreach { anchor =>
      // Keep appending destinations if still processing same label
      if (anchor.text == label) {
        buffer.append(anchor)
      } else {
        if (targets.contains(label)) {
          // Get a sense-to-count map for the just-finished label
          val destinationCounts: Map[Int, Int] = buffer.toArray
            .groupBy(_.destination)
            .view
            .mapValues(anchors => anchors.length)
            .toMap

          if (destinationCounts.nonEmpty) {
            val sense = Sense(labelId = targets(label), destinationCounts = destinationCounts)
            sink.addSense(sense)
          }
        }

        // Clear buffer, update label, append latest
        buffer.clear()
        label = anchor.text
        buffer.append(anchor)
      }
    }

    sink.stopWriting()
    sink.writerThread.join()
  }

  private lazy val props: ConfiguredProperties =
    db.configuration.readConfiguredPropertiesOptimistic()

  override def number: Int               = 7
  override val incompleteMessage: String = s"Phase $number incomplete -- redoing"
}
