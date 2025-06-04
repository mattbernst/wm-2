package wiki.extractor.types

// A label_id using the id from the label table, mapped to a per-destination
// count of page links for that sense
case class Sense(labelId: Int, senseCounts: Map[Int, Int]) {

  def commonness(destination: Int): Double = {
    if (senseCounts.contains(destination)) {
      senseCounts(destination) / total
    } else {
      0.0
    }
  }

  private lazy val total: Double =
    senseCounts.values.sum.toDouble

  lazy val commonestSense: Int =
    senseCounts.maxBy(_._2)._1
}
