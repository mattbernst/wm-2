package wiki.ml

import ai.catboost.CatBoostModel
import ai.catboost.CatBoostPredictions

case class WordSenseCandidate(commonness: Double,
                              inLinkVectorMeasure: Double,
                              outLinkVectorMeasure: Double,
                              inLinkGoogleMeasure: Double,
                              outLinkGoogleMeasure: Double,
                              contextQuality: Double)

class WordSenseDisambiguator(catBoostRankerModel: Array[Byte]) {
  /**
   * Apply a previously trained CatBoostRanker model to predict which of the
   * different word senses is the most appropriate one. For example, suppose
   * that we have 3 word senses under consideration:
   *
   * 1: Mercury (the element)
   * 2: Mercury (the planet)
   * 3: Mercury (the god)
   *
   * The possibleSenses input will have keys 1, 2, and 3. If the top ranked
   * sense is number 2, that means this instance of Mercury is likely
   * mentioned in a context where it means the planet Mercury.
   *
   * @param possibleSenses A map of possible senses to features that can be
   *                       used for prediction
   * @return               The top-ranked sense found from the alternatives in
   *                       possibleSenses
   */
  def getTopRankedSense(possibleSenses: Map[Int, WordSenseCandidate]): Int = {
    require(possibleSenses.nonEmpty, "The map of possible senses cannot be empty.")

    if (possibleSenses.size == 1) {
      return possibleSenses.keys.head
    }

    val orderedSenses = possibleSenses.toSeq
    val numSenses = orderedSenses.size

    // 1. Prepare numerical features.
    // The feature order must exactly match the order used during training.
    // Python feature order: ['commonness', 'inLinkVectorMeasure', 'outLinkVectorMeasure',
    //                        'inLinkGoogleMeasure', 'outLinkGoogleMeasure', 'contextQuality']
    val numericalFeatures: Array[Array[Float]] = orderedSenses.map { case (_, candidate) =>
      Array(
        candidate.commonness.toFloat,
        candidate.inLinkVectorMeasure.toFloat,
        candidate.outLinkVectorMeasure.toFloat,
        candidate.inLinkGoogleMeasure.toFloat,
        candidate.outLinkGoogleMeasure.toFloat,
        candidate.contextQuality.toFloat
      )
    }.toArray

    // 2. Create a placeholder for categorical features to satisfy the method signature.
    // For each row of numerical features, we provide an empty array of string features.
    val categoricalFeatures: Array[Array[String]] = Array.fill(numSenses)(Array.empty[String])

    // 3. Call the correct overloaded `predict` method.
    // This returns a CatBoostPredictions object, not a raw array.
    val predictions: CatBoostPredictions = model.predict(numericalFeatures, categoricalFeatures)

    // 4. Extract scores from the predictions object.
    // For ranking, each item gets one score, located at column index 0.
    val scores = (0 until numSenses).map { i =>
      predictions.get(i, 0) // Get score for i-th sense
    }

    // 5. Find the index of the highest score.
    val bestSenseIndex = scores.zipWithIndex.maxBy(_._1)._2

    // 6. Return the ID of the sense corresponding to the highest score.
    val topSenseId = orderedSenses(bestSenseIndex)._1

    topSenseId
  }

  private val model = CatBoostModel.loadModel(catBoostRankerModel)
}
