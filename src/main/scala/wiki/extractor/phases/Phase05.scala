package wiki.extractor.phases

import wiki.db.Storage
import wiki.extractor.AnchorLogic
import wiki.extractor.types.{Anchor, LabelCounter}
import wiki.extractor.util.{ConfiguredProperties, DBLogging}

import scala.collection.mutable.ListBuffer

class Phase05(db: Storage) extends Phase(db: Storage) {

  override def run(): Unit = {
    db.phase.deletePhase(number)
    db.phase.createPhase(number, s"Gathering link label statistics")
    db.createTableDefinitions(number)
    db.label.delete()
    DBLogging.info("Collecting link label statistics from db")
    val counter = prepareLabelCounter()
    DBLogging.info("Storing link label statistics to db")
    db.label.write(counter)
    db.createIndexes(number)
    db.phase.completePhase(number)
  }

  private def prepareLabelCounter(): LabelCounter = {
    val anchorLogic = new AnchorLogic(props.language)
    val counter     = new LabelCounter
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
        // Set link occurrence count, link document count for completed label
        val linkOccurrenceCount = buffer.length
        val linkDocCount        = buffer.map(_.source).toSet.size
        counter.updateLinkCount(label, linkOccurrenceCount, linkDocCount)

        // Clear buffer, update label, append latest
        buffer.clear()
        label = anchor.text
        buffer.append(anchor)
      }
    }

    counter
  }

  private lazy val props: ConfiguredProperties =
    db.configuration.readConfiguredPropertiesOptimistic()

  override def number: Int               = 5
  override val incompleteMessage: String = s"Phase $number incomplete -- redoing"
}
