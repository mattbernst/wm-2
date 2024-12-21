package wiki.extractor.phases

import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import wiki.db.Storage
import wiki.extractor.{ArticleComparer, ArticleSelector, Contextualizer}
import wiki.extractor.language.LanguageLogic
import wiki.extractor.types.Sense
import wiki.extractor.util.ConfiguredProperties

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
    db.phase.createPhase(number, s"Building training/test data")
    val ll       = LanguageLogic.getLanguageLogic(props.language.code)
    val selector = new ArticleSelector(db, ll)

    // Training articles, disambiguation-test articles, topic-test articles
    val sizes = Seq(1000, 500, 500)

    val res = selector
      .extractSets(
        sizes = sizes,
        minOutLinks = 50,
        minInLinks = 25,
        maxListProportion = 0.1,
        minWordCount = 200,
        maxWordCount = 4000
      )

    // Generate features from the subsets of articles

    //db.phase.completePhase(number)
  }

  private def articleToFeatures(pageId: Int) = {
    val minSenseProbability = 0.01

    // Identify ambiguous-sense links from article
    val links = db.link
      .getBySource(pageId)
      .distinctBy(rl => (rl.anchorText, rl.destination))

    // For each link, we need to determine if it is ambiguous or not.
    val ambiguousLinks = links.filter { link =>
      val sense = senseCache.get(link.destination)
      // An ambiguous label must have multiple senses and must not be totally
      // dominated by the commonest sense
      sense.senseCounts.size > 1 &&
      sense.commonness(sense.commonestSense) < 1.0 - minSenseProbability
    }

    // Generate context from article

    // Use context to resolve ambiguous links. Each ambiguous link where
    // the sense.priorProbability >= minSenseProbability becomes a row in
    // the training data set.
  }

  private lazy val senseCache: LoadingCache[Int, Sense] =
    Scaffeine()
      .maximumSize(1_000_000)
      .build(loader = (destinationId: Int) => {
        db.sense.getSenseByDestinationId(destinationId).get
      })

  private lazy val contextualizer =
    new Contextualizer(new ArticleComparer(db), db, props.language)

  private lazy val props: ConfiguredProperties =
    db.configuration.readConfiguredPropertiesOptimistic()

  override def number: Int               = 8
  override val incompleteMessage: String = s"Phase $number incomplete -- redoing"
}
