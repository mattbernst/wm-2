package wiki.extractor

import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import wiki.db.Storage
import wiki.extractor.types.*
import wiki.extractor.util.{ConfiguredProperties, Progress}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ArticleFeatureProcessor(db: Storage, props: ConfiguredProperties) {

  /**
    * Get features to train on from a Wikipedia article. We're trying
    * to predict the correct sense of an ambiguous term from commonness,
    * relatedness, and context quality.
    *
    * This encapsulates logic similar to "train" in Disambiguator.java
    *
    * @param pageId   The numeric ID of a Wikipedia page used for feature
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
              val sensePageTitle = pageCache.get(senseId).title
              SenseModelEntry(
                sourcePageId = pageId,
                linkDestination = link.destination,
                label = link.anchorText,
                sensePageTitle = sensePageTitle,
                senseId = senseId,
                commonness = sense.commonness(senseId),
                relatedness = comparer.getRelatednessTo(senseId, context),
                contextQuality = context.quality,
                isCorrectSense = senseId == link.destination,
                weight = None
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
      context = enrichContext(context),
      examples = buffer.toArray
    )
  }

  private def tick(): Unit = this.synchronized {
    Progress.tick(count = nProcessed, marker = "+", n = 10)
    nProcessed += 1
  }

  /**
    * Enrich context by adding full Page objects to each representative page.
    * This is useful for reading/debugging/understanding the Context output.
    *
    * @param context A Context object that may not yet have complete
    *                page descriptions for representative pages
    * @return        A Context object with complete page descriptions for
    *                representative pages
    */
  private def enrichContext(context: Context): Context = {
    val enriched = context.pages.map { rep =>
      if (rep.page.nonEmpty) {
        rep
      } else {
        rep.copy(page = Some(pageCache.get(rep.pageId)))
      }
    }

    context.copy(pages = enriched)
  }

  private var nProcessed = 0

  private val minSenseProbability = 0.01

  private val pageCache: LoadingCache[Int, Page] =
    Scaffeine()
      .maximumSize(500_000)
      .build((pageId: Int) => db.getPage(pageId).get)

  private val labelIdToSense: LoadingCache[Int, Option[Sense]] =
    Scaffeine()
      .maximumSize(1_000_000)
      .build(
        loader = (labelId: Int) => {
          db.sense.getSenseByLabelId(labelId).map(_.pruned(minSenseProbability))
        },
        allLoader = Some((labelIds: Iterable[Int]) => {
          val bulkResults = db.sense.getSensesByLabelIds(labelIds.toSeq)
          labelIds.map { labelId =>
            labelId -> bulkResults.get(labelId).map(_.pruned(minSenseProbability))
          }.toMap
        })
      )

  private lazy val contextualizer =
    new Contextualizer(
      maxContextSize = 32,
      labelIdToSense = labelIdToSense,
      labelToId = labelToId,
      comparer = comparer,
      db = db,
      language = props.language
    )

  private lazy val labelToId: mutable.Map[String, Int] = db.label
    .readKnownLabels()
    // Empirical hack: very short labels like "$" or "2d" are poor labels
    .filter(_._1.length > 2)

  private lazy val comparer = new ArticleComparer(db)
}
