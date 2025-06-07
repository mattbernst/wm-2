package wiki.extractor.types

import scala.collection.mutable

// A label_id using the id from the label table, mapped to a per-destination
// count of page links for that sense.
case class Sense(labelId: Int, senseCounts: mutable.Map[Int, Int]) {

  /**
    * The fraction of senses pointing to this destination
    * (for example, the fraction of "Mercury" links pointing
    * to Mercury (element) vs. Mercury (automobile)).
    *
    * @param destination A Wikipedia page ID that has been used as link
    *                    destination in association with this label
    *                    used as an anchor text
    * @return            The fraction of senses pointing at this destination
    */
  def commonness(destination: Int): Double = {
    if (senseCounts.contains(destination)) {
      senseCounts(destination) / total
    } else {
      0.0
    }
  }

  /**
    * Prune low-probability senses from current sense to generate a new sense
    * with only commoner senses retained.
    *
    * @param minSenseProbability The minimum sense-fraction of senses to retain
    * @return                    A pruned Sense
    */
  def pruned(minSenseProbability: Double): Sense = {
    val filtered = senseCounts.filter(t => t._2 / total > minSenseProbability)
    Sense(labelId, filtered)
  }

  private lazy val total: Double =
    senseCounts.values.sum.toDouble

  lazy val commonestSense: Int =
    senseCounts.maxBy(_._2)._1
}
