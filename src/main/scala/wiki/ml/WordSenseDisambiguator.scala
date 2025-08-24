package wiki.ml

import ai.catboost.{CatBoostModel, CatBoostPredictions}
import upickle.default.*

import scala.collection.mutable

case class WordSenseGroup(label: String, contextQuality: Double, candidates: Array[WordSenseCandidate])

case class WordSenseCandidate(
  commonness: Double,
  inLinkVectorMeasure: Double,
  outLinkVectorMeasure: Double,
  inLinkGoogleMeasure: Double,
  outLinkGoogleMeasure: Double,
  pageId: Int)

case class ScoredSenses(scores: mutable.Map[Int, Double]) {

  val bestPageId: Int =
    scores.maxBy(_._2)._1

  val bestScore: Double =
    scores(bestPageId)
}

object ScoredSenses {
  implicit val rw: ReadWriter[ScoredSenses] = macroRW
}

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
    * a context where it refers to the planet Mercury. The best page ID
    * will then correspond to the Wikipedia page titled "Mercury (planet)".
    *
    * @param group A collection of candidates with features, plus context
    *              quality for the whole group, used for sense prediction
    * @return      The scored senses found from the given candidates with
    *              probabilities that sum to 1.0
    */
  def getScoredSenses(group: WordSenseGroup): ScoredSenses = {
    val possibleSenses = group.candidates.map(_.pageId)
    require(possibleSenses.nonEmpty, s"Must have at least one candidate in $group")
    require(possibleSenses.length == possibleSenses.distinct.length, s"Duplicate senses found in $group")
    val numSenses = possibleSenses.length

    if (numSenses == 1) {
      ScoredSenses(mutable.Map(group.candidates.head.pageId -> 1.0))
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

      // Extract raw scores from the predictions object, then apply softmax
      // to convert scores into probabilities
      val rawScores = Array.ofDim[Double](numSenses)
      var j         = 0
      while (j < numSenses) {
        rawScores(j) = predictions.get(j, 0)
        j += 1
      }

      val softmaxProbs = softmax(rawScores)
      val senseScores  = mutable.Map[Int, Double]()

      j = 0
      while (j < numSenses) {
        senseScores(possibleSenses(j)) = softmaxProbs(j)
        j += 1
      }

      ScoredSenses(senseScores)
    }
  }

  /**
    * Apply softmax transformation to convert raw scores to probabilities.
    * The resulting values will be between 0 and 1 and sum to 1.0.
    *
    * @param scores Array of raw scores
    * @return Array of probabilities
    */
  private def softmax(scores: Array[Double]): Array[Double] = {
    // For numerical stability, subtract the maximum value
    val maxScore     = scores.max
    val expScores    = scores.map(score => math.exp(score - maxScore))
    val sumExpScores = expScores.sum
    expScores.map(_ / sumExpScores)
  }

  private val model = CatBoostModel.loadModel(catBoostRankerModel)
}
