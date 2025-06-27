package wiki.extractor

import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import wiki.db.Storage
import wiki.extractor.types.{LinkFeatures, Page, WordSense}
import wiki.extractor.util.Progress
import wiki.util.ConfiguredProperties

import scala.collection.mutable

class LinkFeatureProcessor(db: Storage, props: ConfiguredProperties) {

  def articleToFeatures(pageId: Int, groupName: String): LinkFeatures = {
    tick()

    val context = contextualizer.getContext(pageId, minSenseProbability)
    val x       = contextualizer.getLabels("foo")
    /*
    case class LinkModelEntry(
  sourcePageId: Int,
  linkDestination: Int,
  label: String,
  sensePageTitle: String,
  senseId: Int,
  normalizedOccurrences: Double,
  maxDisambigConfidence: Double,
  avgDisambigConfidence: Double,
  relatednessToContext: Double,
  relatednessToOtherTopics: Double,
  maxLinkProbability: Double,
  avgLinkProbability: Double,
  firstOccurrence: Double,
  lastOccurrence: Double,
  spread: Double,
  isValidLink: Boolean)
     */

    LinkFeatures(
      group = groupName,
      page = pageCache.get(pageId),
      context = context,
      examples = Array()
    )
  }

  private def tick(): Unit = this.synchronized {
    Progress.tick(count = nProcessed, marker = "+", n = 10)
    nProcessed += 1
  }

  private var nProcessed = 0

  private val minSenseProbability = 0.01

  private val pageCache: LoadingCache[Int, Page] =
    Scaffeine()
      .maximumSize(250_000)
      .build((pageId: Int) => db.getPage(pageId).get)

  private val labelIdToSense: LoadingCache[Int, Option[WordSense]] =
    WordSense.getSenseCache(
      db = db,
      maximumSize = 500_000,
      minSenseProbability = minSenseProbability
    )

  private lazy val contextualizer =
    new Contextualizer(
      maxContextSize = 32,
      labelIdToSense = labelIdToSense,
      labelToId = labelToId,
      comparer = comparer,
      db = db,
      language = props.language
    )

  private lazy val labelToId: mutable.Map[String, Int] = db.label
    .readKnownLabels()

  private lazy val comparer = new ArticleComparer(db)
}
