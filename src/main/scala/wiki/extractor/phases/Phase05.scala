package wiki.extractor.phases

import wiki.db.Storage
import wiki.extractor.types.{Anchor, AnchorCounter}
import wiki.extractor.util.{ConfiguredProperties, DBLogging}

import scala.collection.mutable.ListBuffer

class Phase05(db: Storage, props: ConfiguredProperties) extends Phase(db: Storage, props: ConfiguredProperties) {

  override def run(): Unit = {
    db.phase.deletePhase(number)
    db.phase.createPhase(number, s"Gathering link anchor statistics")
    db.createTableDefinitions(number)
    db.anchor.delete()
    DBLogging.info("Loading links from DB")
    val ac = prepareAnchorCounter()

    DBLogging.info("Storing link anchor statistics")
    db.anchor.write(ac)
    db.phase.completePhase(number)
  }

  private def prepareAnchorCounter(): AnchorCounter = {
    val ac             = new AnchorCounter
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
        ac.updateLinkCount(label, linkOccurrenceCount, linkDocCount)

        // Clear buffer, update label, append latest
        buffer.clear()
        label = anchor.text
        buffer.append(anchor)
      }
    }

    ac
  }

  override def number: Int = 5

  override val incompleteMessage: String = s"Phase $number incomplete -- redoing"
}
