package wiki.service

import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import upickle.default.*
import wiki.db.Storage
import wiki.extractor.language.types.NGram
import wiki.extractor.types.{Context, Page, WordSense}
import wiki.extractor.{ArticleComparer, Contextualizer}
import wiki.ml.{WordSenseCandidate, WordSenseDisambiguator, WordSenseGroup}
import wiki.util.ConfiguredProperties

import scala.collection.mutable

case class ResolvedLabel(label: String, page: Page, allSenses: mutable.Map[Int, Double])
case class NGResolvedLabel(nGram: NGram, resolvedLabel: ResolvedLabel)

object ResolvedLabel {
  implicit val rw: ReadWriter[ResolvedLabel] = macroRW
}

case class ContextWithLabels(context: Context, labels: Seq[String], resolvedLabels: Seq[ResolvedLabel])

object ContextWithLabels {
  implicit val rw: ReadWriter[ContextWithLabels] = macroRW
}

case class ServiceParams(minSenseProbability: Double, cacheSize: Int, wordSenseModelName: String)

class ServiceOps(db: Storage, params: ServiceParams) {
  def getPageById(pageId: Int): Option[Page] = db.getPage(pageId)

  def getPageByTitle(title: String): Option[Page] = db.getPage(title)

  /**
    * Get all valid labels derivable from a document, their resolved senses,
    * and an enriched Context for the document. The pages included in the
    * Context indicate the topical tendency of the document contents. Very
    * short documents and very long documents will produce low-quality
    * Contexts.
    *
    * @param req DocumentProcessingRequest containing a plain text document
    * @return    All derivable labels and a representative Context
    */
  def getContextWithLabels(req: DocumentProcessingRequest): ContextWithLabels = {
    val labels          = contextualizer.getLabels(req.doc)
    val context         = contextualizer.getContext(labels.map(_.stringContent), params.minSenseProbability)
    val cleanedLabels   = removeNoisyLabels(labels, params.minSenseProbability)
    val enrichedContext = enrichContext(context)
    ContextWithLabels(
      context = enrichedContext,
      labels = cleanedLabels.map(_.stringContent).toSeq.distinct,
      resolvedLabels = resolveSenses(cleanedLabels, context)
        .map(_.resolvedLabel)
        .toSeq
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
  private def removeNoisyLabels(labels: Array[NGram], minSenseProbability: Double): Array[NGram] = {
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
            allSenses = senses.scores
          )
          Some(NGResolvedLabel(nGram = chosen.nGram, resolvedLabel = rl))
        } else {
          None
        }
      }
  }

  /**
    * Enrich the Context with full page data. Normally the representative pages
    * only have bare page IDs.
    *
    * @param context A Context containing representative pages
    * @return        An enriched Context with full page data for each
    *                representative page
    */
  private def enrichContext(context: Context): Context = {
    pageCache.getAll(context.pages.map(_.pageId)): Unit
    val enriched = context.pages
      .map(rep => rep.copy(page = Some(pageCache.get(rep.pageId))))

    context.copy(pages = enriched)
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

  val comparer: ArticleComparer = new ArticleComparer(db)

  val labelToId: mutable.Map[String, Int] = db.label.readKnownLabels()

  val contextualizer =
    new Contextualizer(
      maxContextSize = 32,
      labelIdToSense = labelIdToSense,
      labelToId = labelToId,
      comparer = comparer,
      db = db,
      language = props.language
    )

  private val wsd: WordSenseDisambiguator = {
    val modelData = db.mlModel.read(params.wordSenseModelName).get
    new WordSenseDisambiguator(modelData)
  }

  private lazy val props: ConfiguredProperties =
    db.configuration.readConfiguredPropertiesOptimistic()
}
