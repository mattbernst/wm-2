package wiki.extractor

import pprint.PPrinter.BlackWhite
import wiki.db.Storage
import wiki.extractor.types.LinkFeatures
import wiki.extractor.util.Progress
import wiki.service.{ModelProperties, ServiceOps, ServiceParams}
import wiki.util.{ConfiguredProperties, Logging}

class LinkFeatureProcessor(db: Storage, props: ConfiguredProperties) extends ModelProperties with Logging {

  def articleToFeatures(pageId: Int, groupName: String): LinkFeatures = {
    tick()

    val context = ops.contextualizer.getContext(pageId, minSenseProbability)
    // Get two label sets: those from links only (ground truth links)
    // and those from full plain page text.
    val links = db.link
      .getBySource(pageId)
      .filter(l => ops.labelToId(l.anchorText) > 0)
      .distinctBy(rl => (rl.anchorText, rl.destination))

    val linkLabels = links.map(_.anchorText).toSet
    val page       = db.getPage(pageId)
    val pagePlainText = db.page
      .readMarkupAuto(pageId)
      .flatMap(_.parseResult)
      .map(_.text)
      .getOrElse("")

    val pageLabels                     = ops.contextualizer.getLabels(pagePlainText)
    val (linkedLabels, unlinkedLabels) = pageLabels.partition(l => linkLabels.contains(l.stringContent))
    BlackWhite.pprintln(page)
    println(pagePlainText)
    println("LINK DATA")
    println("--- LINKED ---")
    BlackWhite.pprintln(linkedLabels, height = 10000)
    println("--- NOT LINKED ---")
    BlackWhite.pprintln(unlinkedLabels, height = 10000)
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
      page = ops.pageCache.get(pageId),
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

  private val ops = {
    val params = ServiceParams(
      minSenseProbability = minSenseProbability,
      cacheSize = 500_000,
      wordSenseModelName = wsdModelName
    )
    new ServiceOps(db, params = params)
  }
}
