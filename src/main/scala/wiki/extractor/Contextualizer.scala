package wiki.extractor

import com.github.blemale.scaffeine.LoadingCache
import wiki.db.{ResolvedLink, Storage}
import wiki.extractor.types.{LabelCounter, Language, Sense}

class Contextualizer(
  senseCache: LoadingCache[Int, Sense],
  comparer: ArticleComparer,
  db: Storage,
  language: Language) {

  def getContext(pageId: Int, minSenseProbability: Double) = {
    val minLinkProbability = 0.005
    val minLinksIn         = 4
    val topN               = 50
    // Get links from page. Get distinct labels from links, only for labels
    // with label.link_doc_count >= minLinksIn and
    // (label.link_count / label.occurrence_count) >= minLinkProbability

    // These are the NGrams that have been used as links anywhere in Wikipedia

    // These are the actual links from the page
    val links = db.link
      .getBySource(pageId)
      .distinctBy(_.anchorText)
      .filter(l => labelCounter.getLinkOccurrenceDocCount(l.anchorText).exists(_ >= minLinksIn))
      .filter(l => labelCounter.getLinkProbability(l.anchorText).exists(_ >= minLinkProbability))

    val topArticles = collectTopArticles(links, topN)

    // We want top articles by weight
  }

  private def collectTopArticles(links: Seq[ResolvedLink], topN: Int): Unit = {}

  private val labelCounter: LabelCounter = db.label.read()
}
