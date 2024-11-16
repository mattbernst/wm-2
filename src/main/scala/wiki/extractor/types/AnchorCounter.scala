package wiki.extractor.types

import scala.collection.mutable

case class AnchorCounter() {

  /**
    * Set link occurrence count and link document count for a label. This should
    * be called using data from the link table to initialize the AnchorCounter
    * before processing the raw text of pages.
    *
    * @param label               A label attached to a link (e.g. its anchor_text)
    * @param linkOccurrenceCount The number of times this label is used as a link
    * @param linkDocCount        The number of distinct pages where this label
    *                            is used as a link
    */
  def setLinkCount(label: String, linkOccurrenceCount: Int, linkDocCount: Int): Unit = {
    if (!labelToCount.contains(label)) {
      val counts = Array(0, 0, linkOccurrenceCount, linkDocCount)
      labelToCount.put(label, counts): Unit
    } else {
      labelToCount(label)(linkOccurrenceCountIndex) = linkOccurrenceCount
      labelToCount(label)(linkOccurrenceDocCountIndex) = linkDocCount
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
      labelToCount(t._1)(occurrenceCountIndex) += t._2
      labelToCount(t._1)(occurrenceDocCountIndex) += 1
    }
  }

  def getOccurrenceCount(label: String): Option[Int] =
    labelToCount.get(label).map(a => a(occurrenceCountIndex))

  def getOccurrenceDocCount(label: String): Option[Int] =
    labelToCount.get(label).map(a => a(occurrenceDocCountIndex))

  def getlinkOccurrenceCount(label: String): Option[Int] =
    labelToCount.get(label).map(a => a(linkOccurrenceCountIndex))

  def getLinkOccurrenceDocCount(label: String): Option[Int] =
    labelToCount.get(label).map(a => a(linkOccurrenceDocCountIndex))

  def getLabels(): collection.Set[String] =
    labelToCount.keySet

  val occurrenceCountIndex: Int        = 0
  val occurrenceDocCountIndex: Int     = 1
  val linkOccurrenceCountIndex: Int    = 2
  val linkOccurrenceDocCountIndex: Int = 3
  private val labelToCount             = mutable.Map[String, Array[Int]]()
}
