package wiki.extractor.types

import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import wiki.db.Storage

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
  private def pruned(minSenseProbability: Double): Option[WordSense] = {
    val filtered = senseCounts.filter(t => t._2 / total > minSenseProbability)
    // Some noise-labels will have a very large number of senses, none of them
    // significant. All of them may be filtered out by a reasonable
    // minSenseProbability. In that case we can ignore the sense entirely.
    if (filtered.nonEmpty) {
      Some(WordSense(labelId, filtered))
    } else {
      None
    }
  }

  private lazy val total: Double =
    senseCounts.values.sum.toDouble

  lazy val commonestSense: Int =
    senseCounts
      .maxByOption(_._2)
      .map(_._1)
      .getOrElse(-1)
}

object WordSense {

  def getSenseCache(db: Storage, maximumSize: Int, minSenseProbability: Double): LoadingCache[Int, Option[WordSense]] =
    Scaffeine()
      .maximumSize(maximumSize)
      .build(
        loader = (labelId: Int) => {
          db.sense.getSenseByLabelId(labelId).flatMap(_.pruned(minSenseProbability))
        },
        allLoader = Some((labelIds: Iterable[Int]) => {
          val bulkResults = db.sense.getSensesByLabelIds(labelIds.toSeq)
          labelIds.map { labelId =>
            labelId -> bulkResults.get(labelId).flatMap(_.pruned(minSenseProbability))
          }.toMap
        })
      )
}
