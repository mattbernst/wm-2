package wiki.extractor.phases

import wiki.db.Storage
import wiki.extractor.ArticleSelection
import wiki.extractor.language.LanguageLogic
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
    db.phase.createPhase(number, s"Building title_to_page map")
    val ll       = LanguageLogic.getLanguageLogic(props.language.code)
    val selector = new ArticleSelection(db, ll)
    val sizes    = Seq(1000, 500, 500)
    val res = selector
      .extractSets(
        sizes = sizes,
        minOutLinks = 50,
        minInLinks = 25,
        maxListProportion = 0.1,
        minWordCount = 200,
        maxWordCount = 4000
      )
    //db.phase.completePhase(number)
  }

  private lazy val props: ConfiguredProperties =
    db.configuration.readConfiguredPropertiesOptimistic()

  override def number: Int               = 8
  override val incompleteMessage: String = s"Phase $number incomplete -- redoing"
}
