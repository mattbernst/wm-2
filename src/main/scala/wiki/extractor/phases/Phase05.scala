package wiki.extractor.phases

import wiki.db.Storage
import wiki.extractor.types.{Anchor, LabelCounter}
import wiki.extractor.util.{ConfiguredProperties, DBLogging}

import scala.collection.mutable.ListBuffer

class Phase05(db: Storage, props: ConfiguredProperties) extends Phase(db: Storage, props: ConfiguredProperties) {

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
    val counter        = new LabelCounter
    val anchorIterator = db.getLinkAnchors()
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

  override def number: Int = 5

  override val incompleteMessage: String = s"Phase $number incomplete -- redoing"
}
