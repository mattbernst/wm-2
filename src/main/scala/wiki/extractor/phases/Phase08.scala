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
        minOutLinks = 15,
        minInLinks = 20,
        maxListProportion = 0.1,
        minWordCount = 200,
        maxWordCount = 2000
      )
    //db.phase.completePhase(number)
  }

  private lazy val props: ConfiguredProperties =
    db.configuration.readConfiguredPropertiesOptimistic()

  override def number: Int               = 8
  override val incompleteMessage: String = s"Phase $number incomplete -- redoing"
}
