package wiki.extractor

import com.github.blemale.scaffeine.LoadingCache
import wiki.db.{ResolvedLink, Storage}
import wiki.extractor.language.LanguageLogic
import wiki.extractor.types.{LabelCounter, Language, Sense}

import scala.collection.mutable

class Contextualizer(
  senseCache: LoadingCache[Int, Sense],
  comparer: ArticleComparer,
  db: Storage,
  language: Language) {

  def getContext(pageId: Int, minSenseProbability: Double) = {
    // These are NGrams that have been used as links anywhere in Wikipedia.
    val pageText = db.page.readMarkupAuto(pageId).flatMap(_.parseResult).map(_.text).getOrElse("")
    val labels = linkLabels(pageText)
    // This filtering out of dates is not mentioned in any of the papers,
    // but it is a step in the original Context.java
      .filterNot(l => language.isDate(l))
      .filter(l => labelCounter.getLinkOccurrenceDocCount(l).exists(_ >= minLinksIn))
      .filter(l => labelCounter.getLinkProbability(l).exists(_ >= minLinkProbability))

    collectTopArticles(labels)

    // These are the actual links from the page
//    val links = db.link
//      .getBySource(pageId)
//      .distinctBy(_.anchorText)
//      .filterNot(l => language.isDate(l.anchorText))
//      .filter(l => labelCounter.getLinkOccurrenceDocCount(l.anchorText).exists(_ >= minLinksIn))
//      .filter(l => labelCounter.getLinkProbability(l.anchorText).exists(_ >= minLinkProbability))

    //val topArticles = collectTopArticles(links, topN)

    // We want top articles by weight
  }

  /**
    *
    * @param text
    * @param minSenseProbability
    */
  def getContext(text: String, minSenseProbability: Double) = {
    // Get links from document. Get distinct labels from links, only for labels
    // with label.link_doc_count >= minLinksIn and
    // (label.link_count / label.occurrence_count) >= minLinkProbability
  }

  private def collectTopArticles(labels: Seq[String]) = {
    val xs = labels.map { label =>
      // Get linkProbability for label, then for all senses of label get sense prior probability
      // (e.g. Mercury-the-planet v.s. Mercury-the-god prior prob)
      val linkProbability = labelCounter.getLinkProbability(label)
      val xx              = senseCache.get(labelToId(label)).commonestSense
      // senses.foreach { sense => if (!isDate(sense))... }
    }
  }

  private def linkLabels(text: String): Array[String] = {
    languageLogic
      .wikiWordNGrams(text, goodLabels)
      .distinct
  }

  private val minLinkProbability = 0.005
  private val minLinksIn         = 4
  private val topN               = 50

  private val languageLogic: LanguageLogic        = LanguageLogic.getLanguageLogic(language.code)
  private val labelCounter: LabelCounter          = db.label.read()
  private val labelToId: mutable.Map[String, Int] = db.label.readKnownLabels()
  private val goodLabels: collection.Set[String]  = labelToId.keySet
}
