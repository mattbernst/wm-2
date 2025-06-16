package wiki.extractor.types

import scala.collection.mutable

/**
  * A label_id using the id from the label table, mapped to a per-destination
  * count of page links for that word sense. A "word sense" may be the
  * sense of a multi-word identifier like "Public Health Service" or of a
  * single word like "Mercury."
  *
  * @param labelId     The label ID for a single label like "Mercury"
  * @param senseCounts A map of sense IDs to sense counts, giving the
  *                    distribution of word sense counts in Wikipedia for a
  *                    label
  */
case class WordSense(labelId: Int, senseCounts: mutable.Map[Int, Int]) {

  /**
    * The fraction of word senses pointing to this destination
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
    * Prune low-probability word senses from current sense to generate a new word
    * sense with only commoner senses retained.
    *
    * @param minSenseProbability The minimum sense-fraction of senses to retain
    * @return                    A pruned Sense
    */
  def pruned(minSenseProbability: Double): WordSense = {
    val filtered = senseCounts.filter(t => t._2 / total > minSenseProbability)
    WordSense(labelId, filtered)
  }

  private lazy val total: Double =
    senseCounts.values.sum.toDouble

  lazy val commonestSense: Int =
    senseCounts
      .maxByOption(_._2)
      .map(_._2)
      .getOrElse(-1)
}
