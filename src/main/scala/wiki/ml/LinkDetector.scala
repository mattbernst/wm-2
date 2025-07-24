package wiki.ml

import upickle.default.*
import ai.catboost.{CatBoostModel, CatBoostPredictions}

case class LabelLinkFeatures(
  linkedPageId: Int,
  // Linked page ID is for bookkeeping. Fields below are model features.
  normalizedOccurrences: Double,
  maxDisambigConfidence: Double,
  avgDisambigConfidence: Double,
  relatednessToContext: Double,
  relatednessToOtherTopics: Double,
  avgLinkProbability: Double,
  maxLinkProbability: Double,
  firstOccurrence: Double,
  lastOccurrence: Double,
  spread: Double)

case class PageLinkPrediction(linkedPageId: Int, prediction: Double)

object PageLinkPrediction {
  implicit val rw: ReadWriter[PageLinkPrediction] = macroRW
}

class LinkDetector(catBoostModel: Array[Byte]) {

  /**
    * Apply a previously trained CatBoost model to predict whether a label
    * extracted from a document would be a linked word or phrase in Wikipedia.
    * This is dependent on features of the label and the larger context of
    * the document it appears in. For example, the Wikipedia article at
    * https://en.wikipedia.org/wiki/Mercury_sulfide
    * contains the sentence
    * "Mercury is produced from the cinnabar ore by roasting in air."
    * The original Wikitext markup for this sentence is
    * "Mercury is produced from the [[cinnabar]] ore by roasting in air."
    *
    * Only the term "cinnabar" is linked in this sentence in the original
    * document. The trained model tries to predict which labels from plain text
    * documents should be linked to a Wikipedia page.
    *
    * Before a label enters this link-detection stage, its word sense has
    * already been chosen by the WordSenseDisambiguator. In the above sentence,
    * "Mercury" would resolve to mercury the element rather than Mercury the
    * planet or Mercury the god. If a label's sense has been mispredicted
    * by the disambiguator, this can also impair predictions of its
    * link-worthiness.
    *
    * A label's link-worthiness can be considered as a signal that it is an
    * important topic related to the document; in Milne's original Wikipedia
    * Miner, this logic was implemented in the TopicDetector.
    *
    * @param candidates An array of candidates with features, used for link
    *                   prediction
    * @return           An array of per-candidate predictions. Higher
    *                   prediction score means more likely to be linked.
    */
  def predict(candidates: Array[LabelLinkFeatures]): Array[PageLinkPrediction] = {
    // Prepare numerical features.
    // The feature order must exactly match the order used during training.
    // See pysrc/train_linking.py and ExtractLinkTrainingData.scala
    val numericalFeatures: Array[Array[Float]] = candidates.map { candidate =>
      Array(
        candidate.normalizedOccurrences.toFloat,
        candidate.maxDisambigConfidence.toFloat,
        candidate.avgDisambigConfidence.toFloat,
        candidate.relatednessToContext.toFloat,
        candidate.relatednessToOtherTopics.toFloat,
        candidate.avgLinkProbability.toFloat,
        candidate.maxLinkProbability.toFloat,
        candidate.firstOccurrence.toFloat,
        candidate.lastOccurrence.toFloat,
        candidate.spread.toFloat
      )
    }

    // Create a placeholder for categorical features to satisfy the method signature.
    // For each row of numerical features, we provide an empty array of string features.
    val categoricalFeatures: Array[Array[String]] = Array.fill(candidates.length)(Array())
    val predictions: CatBoostPredictions          = model.predict(numericalFeatures, categoricalFeatures)
    candidates.indices.toArray.map { j =>
      val candidate = candidates(j)
      // The JVM CatBoost library doesn't have an implementation of
      // "predict_proba" like that used in the Python training code
      // (via sklearn), so manually convert the raw prediction to probability
      // using the sigmoid function.
      val rawPred     = predictions.get(j, 0)
      val probability = 1.0 / (1.0 + math.exp(-rawPred))
      PageLinkPrediction(
        linkedPageId = candidate.linkedPageId,
        prediction = probability
      )
    }
  }

  private val model = CatBoostModel.loadModel(catBoostModel)
}
