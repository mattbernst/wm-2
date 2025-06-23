package wiki.ml

import ai.catboost.CatBoostModel
import ai.catboost.CatBoostPredictions

case class WordSenseGroup(label: String, contextQuality: Double, candidates: Array[WordSenseCandidate])

case class WordSenseCandidate(
  commonness: Double,
  inLinkVectorMeasure: Double,
  outLinkVectorMeasure: Double,
  inLinkGoogleMeasure: Double,
  outLinkGoogleMeasure: Double,
  pageId: Int)

class WordSenseDisambiguator(catBoostRankerModel: Array[Byte]) {

  /**
    * Apply a previously trained CatBoostRanker model to predict which of the
    * different word senses is the most appropriate one. For example, suppose
    * that we have 3 word senses under consideration:
    *
    * A: Mercury (the chemical element)
    * B: Mercury (the planet closest to the Sun)
    * C: Mercury (the Roman god)
    *
    * The group candidates will have page IDs A, B, and C. If the top ranked
    * sense is B, that means this instance of Mercury is likely mentioned in
    * a context where it refers to the planet Mercury. The returned page ID
    * will then correspond to the Wikipedia page titled "Mercury (planet)".
    *
    * @param group A collection of candidates with features, plus context
    *              quality for the whole group, used for sense prediction
    * @return      The top-ranked sense found from the given candidates
    */
  def getBestSense(group: WordSenseGroup): Int = {
    val possibleSenses = group.candidates.map(_.pageId)
    require(possibleSenses.nonEmpty, s"Must have at least one candidate in $group")
    require(possibleSenses.length == possibleSenses.distinct.length, s"Duplicate senses found in $group")
    val numSenses = possibleSenses.length

    if (numSenses == 1) {
      group.candidates.head.pageId
    } else {
      // Prepare numerical features.
      // The feature order must exactly match the order used during training.
      // See pysrc/train_word_sense_disambiguation.py
      // Python feature order: ['commonness', 'inLinkVectorMeasure', 'outLinkVectorMeasure',
      //                        'inLinkGoogleMeasure', 'outLinkGoogleMeasure', 'contextQuality']
      val numericalFeatures: Array[Array[Float]] = group.candidates.map { candidate =>
        Array(
          candidate.commonness.toFloat,
          candidate.inLinkVectorMeasure.toFloat,
          candidate.outLinkVectorMeasure.toFloat,
          candidate.inLinkGoogleMeasure.toFloat,
          candidate.outLinkGoogleMeasure.toFloat,
          group.contextQuality.toFloat
        )
      }

      // Create a placeholder for categorical features to satisfy the method signature.
      // For each row of numerical features, we provide an empty array of string features.
      val categoricalFeatures: Array[Array[String]] = Array.fill(numSenses)(Array.empty[String])
      val predictions: CatBoostPredictions          = model.predict(numericalFeatures, categoricalFeatures)

      // Extract scores from the predictions object.
      // For ranking, each item gets one score, located at column index 0.
      val scores = (0 until numSenses).map { i =>
        predictions.get(i, 0) // Get score for i-th sense
      }

      // Find the index of the highest score and return its sense
      val bestSenseIndex = scores.zipWithIndex.maxBy(_._1)._2
      group.candidates(bestSenseIndex).pageId
    }
  }

  private val model = CatBoostModel.loadModel(catBoostRankerModel)
}
