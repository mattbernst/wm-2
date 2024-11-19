package wiki.extractor.types

import scala.collection.mutable

class AnchorCounter {

  /**
    * Set or update link occurrence count and link document count for a label.
    * This should be called using data from the link table to initialize the
    * AnchorCounter before processing the raw text of pages.
    *
    * @param label               A label attached to a link (e.g. its anchor_text)
    * @param linkOccurrenceCount The number of times this label is used as a link
    * @param linkDocCount        The number of distinct pages where this label
    *                            is used as a link
    */
  def updateLinkCount(label: String, linkOccurrenceCount: Int, linkDocCount: Int): Unit = {
    if (!labelToCount.contains(label)) {
      val counts = Array(0, 0, linkOccurrenceCount, linkDocCount)
      labelToCount.put(label, counts): Unit
    } else {
      labelToCount(label)(AnchorCounter.linkOccurrenceCountIndex) += linkOccurrenceCount
      labelToCount(label)(AnchorCounter.linkOccurrenceDocCountIndex) += linkDocCount
    }
  }

  /**
    * This gets called once for each page. The input map gives a count of label
    * occurrences in the source page. The label's occurrence count gets
    * incremented by N while the label's occurrence document count increments
    * by 1.
    *
    * This requires that the label has already been created by setLinkCount.
    * There is no validation.
    *
    * @param input A map of labels to occurrence counts for one page
    */
  def updateOccurrences(input: Map[String, Int]): Unit = {
    input.toSeq.foreach { t =>
      labelToCount(t._1)(AnchorCounter.occurrenceCountIndex) += t._2
      labelToCount(t._1)(AnchorCounter.occurrenceDocCountIndex) += 1
    }
  }

  /**
    * Add counts for the label to the counter. Used when reading back data from
    * the anchor table.
    *
    * @param label  The label being counted
    * @param counts The different count values
    */
  def insert(label: String, counts: Array[Int]): Unit = {
    require(counts.length == 4, s"Expected a length-4 array but got ${counts.length}")
    labelToCount.put(label, counts): Unit
  }

  def getOccurrenceCount(label: String): Option[Int] =
    labelToCount.get(label).map(a => a(AnchorCounter.occurrenceCountIndex))

  def getOccurrenceDocCount(label: String): Option[Int] =
    labelToCount.get(label).map(a => a(AnchorCounter.occurrenceDocCountIndex))

  def getlinkOccurrenceCount(label: String): Option[Int] =
    labelToCount.get(label).map(a => a(AnchorCounter.linkOccurrenceCountIndex))

  def getLinkOccurrenceDocCount(label: String): Option[Int] =
    labelToCount.get(label).map(a => a(AnchorCounter.linkOccurrenceDocCountIndex))

  def getEntries(): Iterator[(String, Array[Int])] =
    labelToCount.iterator

  def getLabels(): collection.Set[String] =
    labelToCount.keySet

  def canEqual(that: Any): Boolean = that.isInstanceOf[AnchorCounter]

  override def equals(that: Any): Boolean = that match {
    case that: AnchorCounter =>
      that.canEqual(this) && {
        if (this.labelToCount.size != that.labelToCount.size) false
        else {
          this.labelToCount.forall {
            case (label, counts) =>
              that.labelToCount.get(label) match {
                case Some(otherCounts) => counts.sameElements(otherCounts)
                case None              => false
              }
          }
        }
      }
    case _ => false
  }

  override def hashCode(): Int = {
    // Create a stable hash code based on the map contents
    labelToCount.toSeq.sortBy(_._1).foldLeft(0) {
      case (acc, (label, counts)) =>
        41 * (41 * acc + label.hashCode) + java.util.Arrays.hashCode(counts)
    }
  }

  private val labelToCount = mutable.Map[String, Array[Int]]()
}

object AnchorCounter {
  val occurrenceCountIndex: Int        = 0
  val occurrenceDocCountIndex: Int     = 1
  val linkOccurrenceCountIndex: Int    = 2
  val linkOccurrenceDocCountIndex: Int = 3
}
