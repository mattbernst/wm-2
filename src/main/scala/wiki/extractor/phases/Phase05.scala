package wiki.extractor.phases

import wiki.db.Storage
import wiki.extractor.types.AnchorCounter
import wiki.extractor.util.ConfiguredProperties

import scala.collection.mutable.ListBuffer

class Phase05(db: Storage, props: ConfiguredProperties) extends Phase(db: Storage, props: ConfiguredProperties) {

  override def run(): Unit = {
    db.phase.deletePhase(number)
    db.phase.createPhase(number, s"Gathering anchor statistics")
    db.createTableDefinitions(number)
    db.anchor.delete()
    val ac = prepareAnchorCounter()

    db.anchor.write(ac)
    db.phase.completePhase(number)
  }

  private def prepareAnchorCounter(): AnchorCounter = {
    val ac             = new AnchorCounter
    val anchorIterator = db.getLinkAnchors()
    var label = anchorIterator
      .nextOption()
      .map(_._1)
      .getOrElse(throw new IndexOutOfBoundsException("No starting label found!"))

    val buffer = ListBuffer[Int]()
    anchorIterator.foreach { t =>
      // Keep appending destinations if still processing same label
      if (t._1 == label) {
        buffer.append(t._2)
      } else {
        // Set link count, link document count for completed label
        val groups = buffer.groupBy(identity)
        groups.keys.foreach { k =>
          ac.updateLinkCount(label, groups(k).length, groups(k).distinct.length)
        }

        // Clear buffer, update label, append latest
        buffer.clear()
        label = t._1
        buffer.append(t._2)
      }
    }

    ac
  }

  override def number: Int = 5

  override val incompleteMessage: String = s"Phase $number incomplete -- redoing"
}
