package wiki.extractor.phases

import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import wiki.db.Storage
import wiki.extractor.language.LanguageLogic
import wiki.extractor.types.{Context, ModelEntry, Sense, SenseFeatures}
import wiki.extractor.util.ConfiguredProperties
import wiki.extractor.{ArticleComparer, ArticleSelector, Contextualizer}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class TextWithContext(text: String, labels: Seq[String], context: Context)

class Phase08(db: Storage) extends Phase(db: Storage) {

  /**
    * Generate 3 randomized exclusive sets of articles:
    *  - Training set
    *  - Disambiguation test set
    *  - Topic detector test set
    *
    *  The article set only contains PageType.ARTICLE
    *
    *  We need to implement data extraction for train and test. The Context is
    *  also part of this.
    *
    *  Implement equivalent of Context with:
    *  getRelatednessTo
    *  getQuality
    *  constructor (including isDate)
    *  The Milne papers say things like
    *  "The context is obtained by gathering all concepts that relate to
    *  unambiguous labels within the document." However, looking at the
    *  Context.java this appears to be incorrect. The Context is actually
    *  initialized with *all* labels, retaining the top N candidates after sorting/weighting.
    *
    *  Also need to implement ArticleComparer getRelatedness (without-ml path only)
    *  e.g. def getRelatedness(a, b) = getRelatednessNoML(a, b)
    *  see also setPageLinkFeatures where googleMeasure, vectorMeasure, union, intersectionProportion get set
    *  We're going to always implement these features for page links in AND page links out
    */
  override def run(): Unit = {
    db.phase.deletePhase(number)
    db.executeUnsafely("DROP TABLE IF EXISTS sense_training_context;")
    db.executeUnsafely("DROP TABLE IF EXISTS sense_training_context_page;")
    db.executeUnsafely("DROP TABLE IF EXISTS sense_training_example;")
    db.phase.createPhase(number, s"Building training/test data")
    db.createTableDefinitions(number)

    val ll       = LanguageLogic.getLanguageLogic(props.language.code)
    val selector = new ArticleSelector(db, ll)

    // Training articles, disambiguation-test articles, topic-test articles
    //val sizes = Seq(1000, 500, 500)
    val groups = Seq(("training", 5), ("disambiguation-test", 2), ("topic-test", 2))

    val res = selector
      .extractSets(
        sizes = groups.map(_._2),
        minOutLinks = 15,
        minInLinks = 20,
        maxListProportion = 0.1,
        minWordCount = 400,
        maxWordCount = 4000
      )

    // Generate features from the subsets of articles
    res.zip(groups).foreach { set =>
      val subset    = set._1
      val groupName = set._2._1
      subset.foreach { pageId =>
        val senseFeatures = articleToFeatures(pageId, groupName)
        db.senseTraining.write(senseFeatures)
      }
    }

    //db.phase.completePhase(number)
  }

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
  private def articleToFeatures(pageId: Int, groupName: String): SenseFeatures = {
    val context = contextualizer.getContext(pageId, minSenseProbability)
    val buffer  = ListBuffer[ModelEntry]()

    val links = db.link
      .getBySource(pageId)
      .filter(l => labelToId(l.anchorText) > 0)
      .distinctBy(rl => (rl.anchorText, rl.destination))

    // Prime label cache
    labelIdToSense.getAll(links.map(link => labelToId(link.anchorText)))

    // Identify ambiguous-sense links from article.
    // Ambiguous links go into the training data.
    links.foreach { link =>
      val batch: Seq[ModelEntry] = labelIdToSense.get(labelToId(link.anchorText)) match {
        case Some(sense) =>
          // An ambiguous label must have multiple senses and must not be totally
          // dominated by the commonest sense.
          val dominated = sense.commonness(sense.commonestSense) > 1.0 - minSenseProbability
          if (sense.senseCounts.size > 1 && !dominated) {
            // "Each existing link provides one positive example, namely its chosen
            // destination, and several negative examples, namely the destinations that
            // have been chosen for this link text in other articles but not this one."
            sense.senseCounts.keys.toSeq.map { senseId =>
              val sensePageTitle = db.getPage(senseId).map(_.title).getOrElse("UNKNOWN")
              ModelEntry(
                sourcePageId = pageId,
                linkDestination = link.destination,
                label = link.anchorText,
                sensePageTitle = sensePageTitle,
                senseId = senseId,
                commonness = sense.commonness(senseId),
                relatedness = comparer.getRelatednessTo(senseId, context),
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
      page = db.getPage(pageId).get,
      context = contextualizer.enrichContext(context),
      examples = buffer.toArray
    )
  }

  // Get a Context for the page from its plain text rendition, with no
  // knowledge of its original links.
  private def getBlindedPageContext(pageId: Int): TextWithContext = {
    val pageText   = db.page.readMarkupAuto(pageId).flatMap(_.parseResult).map(_.text).get
    val linkLabels = contextualizer.getLinkLabels(pageText)
    val context    = contextualizer.getContext(linkLabels, minSenseProbability)
    TextWithContext(pageText, linkLabels.toSeq, contextualizer.enrichContext(context))
  }

  private val minSenseProbability = 0.01

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
      maxContextSize = 64,
      labelIdToSense = labelIdToSense,
      labelToId = labelToId,
      comparer = comparer,
      db = db,
      language = props.language
    )

  private lazy val labelToId: mutable.Map[String, Int] = db.label.readKnownLabels()
  private lazy val comparer                            = new ArticleComparer(db)

  private lazy val props: ConfiguredProperties =
    db.configuration.readConfiguredPropertiesOptimistic()

  override def number: Int               = 8
  override val incompleteMessage: String = s"Phase $number incomplete -- redoing"
}
