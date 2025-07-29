package wiki.service

import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import org.apache.commons.lang3.StringUtils
import upickle.default.*
import wiki.db.PhaseState.COMPLETED
import wiki.db.Storage
import wiki.extractor.language.types.NGram
import wiki.extractor.types.*
import wiki.extractor.{ArticleComparer, Contextualizer}
import wiki.ml.*
import wiki.util.ConfiguredProperties

import scala.collection.mutable

case class TopicPage(linkedPageId: Int, linkPrediction: Double, surfaceForms: Seq[String])

object TopicPage {
  implicit val rw: ReadWriter[TopicPage] = macroRW
}

case class ResolvedLabel(label: String, page: Page, scoredSenses: ScoredSenses)

object ResolvedLabel {
  implicit val rw: ReadWriter[ResolvedLabel] = macroRW
}

case class NGResolvedLabel(nGram: NGram, resolvedLabel: ResolvedLabel)

case class LabelsAndLinks(
  context: Context,
  labels: Seq[String],
  resolvedLabels: Seq[ResolvedLabel],
  links: Seq[(Page, TopicPage)])

object LabelsAndLinks {
  implicit val rw: ReadWriter[LabelsAndLinks] = macroRW
}

case class ServiceParams(minSenseProbability: Double, cacheSize: Int)

class ServiceOps(db: Storage, params: ServiceParams) extends ModelProperties {
  def getPageById(pageId: Int): Option[Page] = db.getPage(pageId)

  def getPageByTitle(title: String): Option[Page] = db.getPage(title)

  /**
    * Get all valid labels derivable from a document, their resolved senses,
    * and an enriched Context for the document. Also get link predictions for
    * pages that the document might be linked to. The pages included in the
    * Context indicate the topical tendency of the document contents. Very
    * short documents and very long documents will produce low-quality
    * Contexts.
    *
    * @param req DocumentProcessingRequest containing a plain text document
    * @return    All derivable labels, links, and a representative Context
    */
  def getLabelsAndLinks(req: DocumentProcessingRequest): LabelsAndLinks = {
    val labels          = contextualizer.getLabels(req.doc)
    val context         = contextualizer.getContext(labels.map(_.stringContent), params.minSenseProbability)
    val cleanedLabels   = removeNoisyLabels(labels, params.minSenseProbability)
    val enrichedContext = enrichContext(context)
    val resolvedLabels  = resolveSenses(cleanedLabels, context)
    val linkedTopics = getLinkedTopics(req.doc, resolvedLabels, context)
      .sortBy(-_.linkPrediction)
    val linkedPages = linkedTopics.map(e => (pageCache.get(e.linkedPageId), e))

    LabelsAndLinks(
      context = enrichedContext,
      labels = cleanedLabels.map(_.stringContent).toSeq.distinct,
      resolvedLabels = resolvedLabels.map(_.resolvedLabel).toSeq,
      links = linkedPages.toSeq
    )
  }

  /**
    * Remove labels that have no associated sense resolution or a
    * below-threshold sense probability. It does not make sense to resolve
    * these labels to senses or to return them as part of the API response.
    *
    * @param labels              NGrams derived from original document
    * @param minSenseProbability The minimum prior probability for any label
    * @return                    Labels minus low-information labels
    */
  def removeNoisyLabels(labels: Array[NGram], minSenseProbability: Double): Array[NGram] = {
    labels.filter { label =>
      labelIdToSense.get(labelToId(label.stringContent)) match {
        case Some(sense) =>
          sense.commonness(sense.commonestSense) > minSenseProbability
        case None =>
          false
      }
    }
  }

  /**
    * Resolve senses of cleaned labels according to the source document's
    * Context. Each label will be resolved to the single word sense that
    * best matches the document Context via a previously trained word sense
    * disambiguation model.
    *
    * For example, in a document about pollution from coal, the sentence
    * "Mercury emissions know no national or continental boundaries." will
    * resolve "Mercury" to the Wikipedia page referring to the chemical
    * element mercury.
    *
    * In a document about the Mariner 10 spacecraft, the sentence
    * "Mariner 10 flew by Mercury again on 21 September 1974." will resolve
    * "Mercury" to the Wikipedia page referring to the planet Mercury.
    *
    * @param labels  Valid NGram labels from a document, to be resolved into
    *                definite word senses
    * @param context The source document's Context
    */
  def resolveSenses(labels: Array[NGram], context: Context): Array[NGResolvedLabel] = {
    case class DGroup(nGram: NGram, wordSenseGroup: WordSenseGroup)

    val groups: Array[DGroup] = labels.flatMap { ng =>
      val label = ng.stringContent
      labelIdToSense.get(labelToId(label)).map { sense =>
        val candidates = sense.senseCounts.keys.map { senseId =>
          val features = comparer.getRelatednessByFeature(senseId, context)
          WordSenseCandidate(
            commonness = sense.commonness(senseId),
            inLinkVectorMeasure = features("inLinkVectorMeasure"),
            outLinkVectorMeasure = features("outLinkVectorMeasure"),
            inLinkGoogleMeasure = features("inLinkGoogleMeasure"),
            outLinkGoogleMeasure = features("outLinkGoogleMeasure"),
            pageId = senseId
          )
        }

        DGroup(
          nGram = ng,
          wordSenseGroup = WordSenseGroup(
            label = label,
            contextQuality = context.quality,
            candidates = candidates.toArray
          )
        )
      }
    }

    // If subgroups have identical positions, keep the one with the highest
    // scoring best sense. Identical positions can show up from casing
    // variants generated for beginning-of-sentence NGrams.
    // Drop irrelevant groups (those having negative-scored best sense)
    groups
      .groupBy(e => (e.nGram.start, e.nGram.end))
      .toArray
      .flatMap { posGroup =>
        val candidates            = posGroup._2
        val candidateScoredSenses = candidates.map(e => wsd.getScoredSenses(e.wordSenseGroup))
        val bestScores            = candidateScoredSenses.map(_.bestScore)
        val bestScore             = bestScores.max
        if (bestScore > 0.0) {
          val chosen = candidates(bestScores.indexOf(bestScore))
          val senses = candidateScoredSenses(bestScores.indexOf(bestScore))
          val rl = ResolvedLabel(
            label = chosen.wordSenseGroup.label,
            page = pageCache.get(senses.bestPageId),
            scoredSenses = senses
          )
          Some(NGResolvedLabel(nGram = chosen.nGram, resolvedLabel = rl))
        } else {
          None
        }
      }
  }

  private def getLinkedTopics(
    text: String,
    resolvedNgrams: Array[NGResolvedLabel],
    context: Context
  ): Array[TopicPage] = {
    val docLength   = text.length.toDouble
    val topicGroups = resolvedNgrams.groupBy(_.resolvedLabel.page.id)
    val topics = topicGroups.flatMap {
      case (pageId, allLabels) =>
        val linkProbabilities: Map[String, Double] = allLabels
          .map(e => (e.nGram.stringContent, labelCounter.getLinkProbability(e.nGram.stringContent)))
          .filter(_._2.exists(_ > params.minSenseProbability))
          .map(e => (e._1, e._2.get))
          .toMap

        if (linkProbabilities.nonEmpty) {
          val labels                = allLabels.filter(e => linkProbabilities.contains(e.nGram.stringContent))
          val occurrences           = labels.map(e => StringUtils.countMatches(text, e.nGram.stringContent)).sum
          val disambigConfidences   = labels.map(_.resolvedLabel.scoredSenses.bestScore)
          val maxDisambigConfidence = disambigConfidences.max
          val avgDisambigConfidence = disambigConfidences.sum / labels.length
          val firstOccurrence       = labels.map(e => e.nGram.start).min / docLength
          val lastOccurrence        = labels.map(e => e.nGram.start).max / docLength
          val avgLinkProbability    = linkProbabilities.values.sum / linkProbabilities.size
          val maxLinkProbability    = linkProbabilities.values.max

          val e = LabelLinkFeatures(
            linkedPageId = pageId,
            normalizedOccurrences = math.log(occurrences + 1),
            maxDisambigConfidence = maxDisambigConfidence,
            avgDisambigConfidence = avgDisambigConfidence,
            relatednessToContext = contextualizer.getRelatedness(pageId, context),
            relatednessToOtherTopics = -1.0,
            avgLinkProbability = avgLinkProbability,
            maxLinkProbability = maxLinkProbability,
            firstOccurrence = firstOccurrence,
            lastOccurrence = lastOccurrence,
            spread = lastOccurrence - firstOccurrence
          )
          Some(e)
        } else {
          None
        }
    }.toArray

    val topicLinks  = topics.map(e => TopicLink(e.linkedPageId, e.avgLinkProbability, e.normalizedOccurrences))
    val relatedness = getRelatednessToOtherTopics(input = topicLinks, topN = 25)
    val candidates  = topics.map(e => e.copy(relatednessToOtherTopics = relatedness(e.linkedPageId)))
    linkDetector
      .predict(candidates)
      .map { e =>
        TopicPage(
          linkedPageId = e.linkedPageId,
          linkPrediction = e.prediction,
          surfaceForms = topicGroups(e.linkedPageId).map(_.nGram.stringContent).toSeq
        )
      }
  }

  /**
    * A topic's relatedness to other topics can't be calculated until the
    * full collection of topics is ready. Calculate relatedness to other
    * topics using a context generated from the top N topics.
    *
    * @param input A sequence of TopicLink objects from a single document
    * @param topN  Maximum number of "other topics" to use for relatedness
    * @return      A map of sense IDs to other-topic relatedness values
    */
  def getRelatednessToOtherTopics(input: Array[TopicLink], topN: Int): Map[Int, Double] = {
    val senses = input.map(_.senseId)
    require(senses.length == senses.distinct.length, "Sense IDs for topics must not repeat")
    // Generate a new context for comparison, based on link relevance
    val topCandidates = input
      .map(
        e =>
          RepresentativePage(
            pageId = e.senseId,
            weight = e.avgLinkProbability * e.normalizedOccurrences,
            page = None
          )
      )
      .sortBy(-_.weight)
      .take(topN)

    val context = Context(pages = topCandidates, quality = topCandidates.map(_.weight).sum)

    input.map { entry =>
      val relatedness = contextualizer.getRelatedness(entry.senseId, context)
      (entry.senseId, relatedness)
    }.toMap
  }

  /**
    * Enrich the Context with full page data. Normally the representative pages
    * only have bare page IDs.
    *
    * @param context A Context containing representative pages
    * @return        An enriched Context with full page data for each
    *                representative page
    */
  def enrichContext(context: Context): Context = {
    pageCache.getAll(context.pages.map(_.pageId)): Unit
    val enriched = context.pages
      .map(rep => rep.copy(page = Some(pageCache.get(rep.pageId))))

    context.copy(pages = enriched)
  }

  def validateWordSenseModel(): Unit = {
    require(
      db.phase.getPhaseState(db.phase.lastPhase).contains(COMPLETED),
      "Extraction has not completed. Finish extraction and training first."
    )
    require(
      db.mlModel.read(wsdModelName).nonEmpty,
      s"Could not find model $wsdModelName in db. Run make load_disambiguation."
    )
  }

  def validateLinkingModel(): Unit = {
    require(
      db.phase.getPhaseState(db.phase.lastPhase).contains(COMPLETED),
      "Extraction has not completed. Finish extraction and training first."
    )
    require(
      db.mlModel.read(linkingModelName).nonEmpty,
      s"Could not find model $linkingModelName in db. Run make load_linking."
    )
  }

  val pageCache: LoadingCache[Int, Page] =
    Scaffeine()
      .maximumSize(params.cacheSize)
      .build(
        loader = (pageId: Int) => db.getPage(pageId).get,
        allLoader = Some((pageIds: Iterable[Int]) => {
          db.getPages(pageIds.toSeq)
            .map(r => (r.id, r))
            .toMap
        })
      )

  val labelIdToSense: LoadingCache[Int, Option[WordSense]] =
    WordSense.getSenseCache(
      db = db,
      maximumSize = params.cacheSize,
      minSenseProbability = params.minSenseProbability
    )

  lazy val comparer: ArticleComparer = new ArticleComparer(db)

  lazy val labelToId: mutable.Map[String, Int] = db.label.readKnownLabels()

  lazy val labelCounter: LabelCounter = db.label.read()

  lazy val contextualizer =
    new Contextualizer(
      maxContextSize = 25,
      labelIdToSense = labelIdToSense,
      labelToId = labelToId,
      labelCounter = labelCounter,
      comparer = comparer,
      db = db,
      language = props.language
    )

  private lazy val wsd: WordSenseDisambiguator = {
    val modelData = db.mlModel.read(wsdModelName).get
    new WordSenseDisambiguator(modelData)
  }

  private lazy val linkDetector: LinkDetector = {
    val modelData = db.mlModel.read(linkingModelName).get
    new LinkDetector(modelData)
  }

  private lazy val props: ConfiguredProperties =
    db.configuration.readConfiguredPropertiesOptimistic()
}
