package wiki.service

import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import wiki.db.Storage
import wiki.extractor.types.{Context, Page, WordSense}
import upickle.default.*
import wiki.extractor.{ArticleComparer, Contextualizer}
import wiki.util.ConfiguredProperties

import scala.collection.mutable

case class ContextWithLabels(labels: Seq[String], context: Context)

object ContextWithLabels {
  implicit val rw: ReadWriter[ContextWithLabels] = macroRW
}

case class ServiceParams(minSenseProbability: Double, cacheSize: Int)

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
    val labels        = contextualizer.getLabels(req.doc)
    val context       = contextualizer.getContext(labels, params.minSenseProbability)
    val cleanedLabels = removeNoisyLabels(labels, params.minSenseProbability)
    ContextWithLabels(cleanedLabels.toSeq, enrichContext(context))
  }

  /**
    * Remove labels that have no associated sense resolution or a
    * below-threshold sense probability. It does not make sense to resolve
    * these labels to senses or to return them as part of the API response.
    *
    * @param labels              NGram strings derived from original document
    * @param minSenseProbability The minimum prior probability for any label
    * @return                    Labels minus low-information labels
    */
  private def removeNoisyLabels(labels: Array[String], minSenseProbability: Double): Array[String] = {
    labels.filter { label =>
      labelIdToSense.get(labelToId(label)) match {
        case Some(sense) =>
          sense.commonness(sense.commonestSense) > minSenseProbability
        case None =>
          false
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

  private val pageCache: LoadingCache[Int, Page] =
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

  private val labelIdToSense: LoadingCache[Int, Option[WordSense]] =
    WordSense.getSenseCache(
      db = db,
      maximumSize = params.cacheSize,
      minSenseProbability = params.minSenseProbability
    )

  private val comparer: ArticleComparer = new ArticleComparer(db)

  private val labelToId: mutable.Map[String, Int] = db.label.readKnownLabels()

  private val contextualizer =
    new Contextualizer(
      maxContextSize = 32,
      labelIdToSense = labelIdToSense,
      labelToId = labelToId,
      comparer = comparer,
      db = db,
      language = props.language
    )

  private lazy val props: ConfiguredProperties =
    db.configuration.readConfiguredPropertiesOptimistic()
}
