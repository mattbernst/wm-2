package wiki.extractor

import com.github.blemale.scaffeine.LoadingCache
import wiki.db.Storage
import wiki.extractor.language.LanguageLogic
import wiki.extractor.types.*

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Contextualizer(
                      maxContextSize: Int,
                      labelIdToSense: LoadingCache[Int, Option[Sense]],
                      labelToId: mutable.Map[String, Int],
                      comparer: ArticleComparer,
                      db: Storage,
                      language: Language) {

  /**
    * Construct a context containing top candidate Wikipedia pages from an
    * existing Wikipedia page. Since we have the ground truth about how
    * links were labeled, use only link anchor text to generate context.
    * This is used for generating model training data.
    *
    * @param pageId              The numeric ID of the Wikipedia page
    * @param minSenseProbability The minimum prior probability for any sense
    * @return                    A Context containing top candidate Wikipedia
    *                            pages
    */
  def getContext(pageId: Int, minSenseProbability: Double): Context = {
    val parseResult = db.page.readMarkupAuto(pageId).flatMap(_.parseResult)
    // Use only anchor texts
    val links = parseResult.map(_.links).getOrElse(Seq())

    val allLinks = links.map(_.anchorText).mkString(" ")
    getContext(allLinks, minSenseProbability)
  }

  /**
    * Construct a context containing top candidate Wikipedia pages from an
    * arbitrary text document.
    *
    * @param text                The document text
    * @param minSenseProbability The minimum prior probability for any sense
    * @return                     A Context containing top candidate Wikipedia
    *                             pages
    */
  def getContext(text: String, minSenseProbability: Double): Context = {
    // Get distinct valid labels from document, only for labels with
    // label.link_doc_count >= minLinksIn and
    // (label.link_count / label.occurrence_count) >= minLinkProbability.
    // A label is an NGram that has been used as anchor text anywhere in Wikipedia.

    val labels = linkLabels(text)
      .filter(l => labelCounter.getLinkOccurrenceDocCount(l).exists(_ >= minLinksIn))
      .filter(l => labelCounter.getLinkProbability(l).exists(_ >= minLinkProbability))

    val candidates    = collectCandidates(labels, minSenseProbability)
    val topCandidates = collectTopCandidates(candidates)

    Context(pages = topCandidates, quality = topCandidates.map(_.weight).sum)
  }

  /**
    * Collect initial representative-page candidates to represent a document.
    * These initial candidates are more numerous than the final candidate set.
    * They include pages that meet the threshold minSenseProbability for a
    * certain sense, but discard especially rare senses. For example, "Mercury"
    * might be interpreted as Mercury the planet, Mercury the god, or mercury
    * the element, but at a typical minSenseProbability it won't include Mercury
    * the Marvel Comics character. The last sense of usage is very rare in
    * Wikipedia.
    *
    * @param labels               Distinct labels from a single document
    * @param minSenseProbability  Minimum sense prior probability allowed in
    *                             generated candidates
    * @return                     A set of candidate representative pages
    */
  private def collectCandidates(labels: Array[String], minSenseProbability: Double): Array[RepresentativePage] = {
    val pages = ListBuffer[RepresentativePage]()
    // Filtering out of dates is not mentioned in any of the publications
    // related to Wikipedia Miner, but it is a step in the original
    // Context.java. Filter out labels that are dates as well as any senses
    // that resolve to pages titled as dates.
    val goodLabels = labels.filterNot(l => dateStrings.contains(l))
    goodLabels.foreach { label =>
      // Get linkProbability for label, then for all senses of label get sense prior probability
      // (e.g. Mercury-the-planet v.s. Mercury-the-god prior probability)
      labelCounter
        .getLinkProbability(label)
        .foreach { linkProbability =>
          labelIdToSense.get(labelToId(label)).foreach { sense =>
            sense.senseCounts.keys.foreach { senseId =>
              val sensePriorProbability = sense.commonness(senseId)
              val isDatePage            = datePageIds.contains(senseId)
              if (!isDatePage && sensePriorProbability > minSenseProbability) {
                val weight = (linkProbability + sensePriorProbability) / 2
                pages.append(RepresentativePage(senseId, weight))
              }
            }
          }
        }
    }

    pages.toArray.distinct
      .sortBy(-_.weight)
      .take(maxContextSize * 5)
  }

  /**
    * Collect top representative-page candidates to represent a document. The
    * top candidates are those with the greatest average relatedness compared
    * to all other representative-page candidates in the input. The idea is
    * that the average relatedness should be higher for topics that are
    * thematically adjacent.
    *
    * A document that mentions "Mercury", "chondrite", and "orbit" is likelier
    * to have Mercury-the-planet as one of its top candidates senses than a
    * document mentioning "Mercury", "vermilion", and "alchemy"; the latter
    * will have Mercury-the-element as a top representative page.
    *
    * This is less effective for very short documents, like photo captions,
    * since there are unlikely to be enough context clues to disambiguate terms
    * with multiple meanings.
    *
    * @param candidates An initial candidate set for ranking
    * @return           Candidates reweighted, ranked, and limited to a maximum
    *                   of maxContextSize results
    */
  private def collectTopCandidates(candidates: Array[RepresentativePage]): Array[RepresentativePage] = {
    val pages = ListBuffer[RepresentativePage]()
    candidates.foreach { a =>
      var averageRelatedness = 0.0
      candidates.foreach { b =>
        if (a.pageId != b.pageId) {
          comparer
            .compare(a.pageId, b.pageId)
            .foreach(comparison => averageRelatedness += comparison.mean)
        }
      }

      if (candidates.length > 1) {
        averageRelatedness /= (candidates.length - 1)
      }

      val weight = a.weight + (4 * averageRelatedness) / 5
      pages.append(RepresentativePage(pageId = a.pageId, weight = weight))
    }

    pages.toArray
      .sortBy(-_.weight)
      .take(maxContextSize)
  }

  private def linkLabels(text: String): Array[String] = {
    languageLogic
      .wordNGrams(language = language, documentText = text)
      .filter(n => goodLabels.contains(n))
      .distinct
  }

  private val minLinkProbability = 0.0025
  private val minLinksIn         = 4

  private val languageLogic: LanguageLogic       = LanguageLogic.getLanguageLogic(language.code)
  private val labelCounter: LabelCounter         = db.label.read()
  private val goodLabels: collection.Set[String] = labelToId.keySet
  private val dateStrings                        = language.generateValidDateStrings()
  private val datePageIds                        = dateStrings.flatMap(d => db.getPage(d)).map(_.id)
}
