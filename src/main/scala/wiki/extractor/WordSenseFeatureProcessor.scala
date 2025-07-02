package wiki.extractor

import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import wiki.db.Storage
import wiki.extractor.types.*
import wiki.extractor.util.Progress
import wiki.service.{ServiceOps, ServiceParams}
import wiki.util.ConfiguredProperties

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class WordSenseFeatureProcessor(db: Storage, props: ConfiguredProperties) {

  /**
    * Get features to train on from a Wikipedia article. We're trying
    * to predict the correct word sense of an ambiguous term from commonness,
    * context quality, and sense-level features.
    *
    * This encapsulates logic similar to "train" in Disambiguator.java
    * The original Milne model that lumped all measures together into
    * "relatedness" did not perform as well as a CatBoost model given
    * the underlying individual features like inLinkVectorMeasure.
    *
    * @param pageId    The numeric ID of a Wikipedia page used for feature
    *                  extraction
    * @param groupName The name of the feature extraction group
    */
  def articleToFeatures(pageId: Int, groupName: String): SenseFeatures = {
    tick()
    val context = contextualizer.getContext(pageId, minSenseProbability)
    val buffer  = ListBuffer[SenseModelEntry]()

    val links = db.link
      .getBySource(pageId)
      .filter(l => labelToId(l.anchorText) > 0)
      .distinctBy(rl => (rl.anchorText, rl.destination))

    // Prime label cache
    labelIdToSense.getAll(links.map(link => labelToId(link.anchorText)))

    // Identify ambiguous-sense links from article.
    // Ambiguous links go into the training data.
    links.foreach { link =>
      val batch: Seq[SenseModelEntry] = labelIdToSense.get(labelToId(link.anchorText)) match {
        case Some(sense) =>
          // An ambiguous label must have multiple senses and must not be totally
          // dominated by the commonest sense.
          val dominated = sense.commonness(sense.commonestSense) > 1.0 - minSenseProbability
          if (sense.senseCounts.size > 1 && !dominated) {
            // "Each existing link provides one positive example, namely its chosen
            // destination, and several negative examples, namely the destinations that
            // have been chosen for this link text in other articles but not this one."
            sense.senseCounts.keys.toSeq.map { senseId =>
              val sensePageTitle       = pageCache.get(senseId).title
              val relatednessByFeature = comparer.getRelatednessByFeature(senseId, context)
              SenseModelEntry(
                sourcePageId = pageId,
                linkDestination = link.destination,
                label = link.anchorText,
                sensePageTitle = sensePageTitle,
                senseId = senseId,
                commonness = sense.commonness(senseId),
                inLinkVectorMeasure = relatednessByFeature("inLinkVectorMeasure"),
                outLinkVectorMeasure = relatednessByFeature("outLinkVectorMeasure"),
                inLinkGoogleMeasure = relatednessByFeature("inLinkGoogleMeasure"),
                outLinkGoogleMeasure = relatednessByFeature("outLinkGoogleMeasure"),
                contextQuality = context.quality,
                isCorrectSense = senseId == link.destination
              )
            }
          } else {
            Seq()
          }
        case None =>
          Seq()
      }

      // Only add the batch to the buffer if one of them has the correct
      // sense. There are some cases where isCorrectSense is false for all
      // of them, e.g. for rare senses that have been excluded from
      // labelIdToSense.
      if (batch.exists(_.isCorrectSense)) {
        buffer.addAll(batch)
      }
    }

    SenseFeatures(
      group = groupName,
      page = pageCache.get(pageId),
      context = ops.enrichContext(context),
      examples = buffer.toArray
    )
  }

  private def tick(): Unit = this.synchronized {
    Progress.tick(count = nProcessed, marker = "+", n = 10)
    nProcessed += 1
  }

  private var nProcessed = 0

  private val minSenseProbability = 0.01

  private val pageCache: LoadingCache[Int, Page] =
    Scaffeine()
      .maximumSize(250_000)
      .build((pageId: Int) => db.getPage(pageId).get)

  private val labelIdToSense: LoadingCache[Int, Option[WordSense]] =
    WordSense.getSenseCache(
      db = db,
      maximumSize = 500_000,
      minSenseProbability = minSenseProbability
    )

  private lazy val contextualizer =
    new Contextualizer(
      maxContextSize = 32,
      labelIdToSense = labelIdToSense,
      labelToId = labelToId,
      labelCounter = db.label.read(),
      comparer = comparer,
      db = db,
      language = props.language
    )

  private lazy val labelToId: mutable.Map[String, Int] = db.label
    .readKnownLabels()

  private lazy val ops = new ServiceOps(
    db = db,
    params = ServiceParams(minSenseProbability = minSenseProbability, cacheSize = 250_000)
  )

  private lazy val comparer = new ArticleComparer(db)
}
