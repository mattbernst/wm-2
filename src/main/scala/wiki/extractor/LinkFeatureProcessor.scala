package wiki.extractor

import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import org.apache.commons.lang3.StringUtils
import wiki.db.Storage
import wiki.extractor.language.types.NGram
import wiki.extractor.types.*
import wiki.extractor.types.PageType.ARTICLE
import wiki.extractor.util.Progress
import wiki.service.{NGResolvedLabel, ServiceOps, ServiceParams}
import wiki.util.{ConfiguredProperties, Logging}

class LinkFeatureProcessor(db: Storage, props: ConfiguredProperties) extends Logging {

  /**
   * Generate per-topic linking model features from the contents of a
   * Wikipedia page. Topics that are not ordinary articles will be
   * ignored.
   *
   * @param pageId    The numeric ID of the Wikipedia page
   * @param groupName The name of the data group this page is in, like
   *                  "linking-train"
   * @return          A LinkFeatures object with context and examples from
   *                  the original page
   */
  def articleToFeatures(pageId: Int, groupName: String): LinkFeatures = {
    tick()
    val context = ops.contextualizer.getContext(pageId, minSenseProbability)
    ops.pageCache.getAll(context.pages.map(_.pageId))
    val page   = db.getPage(pageId).get
    val markup = db.page.readMarkupAuto(pageId).get

    val linkedPages = markup.parseResult
      .map(_.links)
      .getOrElse(Seq())
      .flatMap(e => titlePageCache.get(e.target))
      .toSet

    val pageLabels = markup.wikitext
      .map(ops.contextualizer.getLabels)
      .map(labels => ops.removeNoisyLabels(labels, minSenseProbability))
      .getOrElse(Array())

    val resolvedNgrams = ops.resolveSenses(pageLabels, context)

    val rawEntries = assignRelatednessToOtherTopics(
      input = makeLinkModelEntries(
        markup = markup.wikitext.get,
        context = context,
        page = page,
        examples = resolvedNgrams
      ),
      topN = 25
    )

    val examples = Array.ofDim[LinkModelEntry](rawEntries.length)
    var j = 0
    // A topic that is not an ordinary article gets ignored
    // completely. If a topic appears among the link targets, it serves as a
    // positive example for training a linking model. Otherwise, it is a
    // negative example.
    rawEntries
      .foreach { entry =>
        if (pageTypeCache.get(entry.senseId) == ARTICLE) {
          if (linkedPages.contains(entry.senseId)) {
            examples(j) = entry.copy(isValidLink = true)
          }
          else {
            examples(j) = entry.copy(isValidLink = false)
          }
          j += 1
        }
    }

    LinkFeatures(
      group = groupName,
      page = ops.pageCache.get(pageId),
      context = ops.enrichContext(context),
      examples = examples.take(j)
    )
  }

  /**
    * Combine disambiguated references to the same Wikipedia pages to form
    * "topics." Each group of resolved labels that goes into a topic gets
    * combined to form a LinkModelEntry. At this stage we don't know a
    * LinkModelEntry's relatedness to other topics or whether it should be
    * treated as a positive example or negative example. These features get
    * filled in later.
    *
    * @param markup   The Wikitext markup for a Wikipedia page
    * @param context  The Context object representing a central core of
    *                 labels for the page
    * @param page     The Wikipedia page corresponding to the Wikitext markup
    * @param examples NGrams from the page that have already had their
    *                 word senses resolved by the word sense disambiguation
    *                 model
    * @return         One LinkModelEntry per topic (linked page)
    */
  private def makeLinkModelEntries(
    markup: String,
    context: Context,
    page: Page,
    examples: Array[NGResolvedLabel]
  ): Array[LinkModelEntry] = {
    val topicGroups = examples.groupBy(_.resolvedLabel.page)

    topicGroups.flatMap {
      case (topicPage, allLabels) =>
        val linkProbabilities: Map[String, Double] = allLabels
          .map(e => (e.nGram.stringContent, ops.labelCounter.getLinkProbability(e.nGram.stringContent)))
          .filter(_._2.exists(_ > minSenseProbability))
          .map(e => (e._1, e._2.get))
          .toMap

        if (linkProbabilities.nonEmpty) {
          val labels                = allLabels.filter(e => linkProbabilities.contains(e.nGram.stringContent))
          val occurrences           = labels.map(e => StringUtils.countMatches(markup, e.nGram.stringContent)).sum
          val disambigConfidences   = labels.map(_.resolvedLabel.scoredSenses.bestScore)
          val maxDisambigConfidence = disambigConfidences.max
          val avgDisambigConfidence = disambigConfidences.sum / labels.length
          val firstOccurrence       = labels.map(e => e.nGram.start).min
          val lastOccurrence        = labels.map(e => e.nGram.start).max
          val avgLinkProbability    = linkProbabilities.values.sum / linkProbabilities.size
          val maxLinkProbability    = linkProbabilities.values.max

          val e = LinkModelEntry(
            sourcePageId = page.id,
            sensePageTitle = topicPage.title,
            senseId = topicPage.id,
            normalizedOccurrences = math.log(occurrences + 1),
            maxDisambigConfidence = maxDisambigConfidence,
            avgDisambigConfidence = avgDisambigConfidence,
            relatednessToContext = ops.contextualizer.getRelatedness(topicPage.id, context),
            relatednessToOtherTopics = -1.0, // Assigned later in assignRelatednessToOtherTopics
            avgLinkProbability = avgLinkProbability,
            maxLinkProbability = maxLinkProbability,
            firstOccurrence = firstOccurrence,
            lastOccurrence = lastOccurrence,
            spread = lastOccurrence - firstOccurrence,
            isValidLink = false // Assigned later in caller
          )
          Some(e)
        } else {
          None
        }
    }.toArray
  }

  /**
    * A topic's relatedness to other topics can't be calculated until the
    * full collection of topics is ready. Update each of the entries with
    * relatedness to other topics.
    *
    * @param input A group of LinkModelEntry objects from a single document,
    *              without relatednessToOtherTopics
    * @param topN  Maximum number of "other topics" to use for relatedness
    * @return      A group of LinkModelEntry objects from a single document,
    *              with relatednessToOtherTopics assigned
    */
  private def assignRelatednessToOtherTopics(input: Array[LinkModelEntry], topN: Int): Array[LinkModelEntry] = {
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
      val relatedness = ops.contextualizer.getRelatedness(entry.senseId, context)
      entry.copy(relatednessToOtherTopics = relatedness)
    }
  }

  private def tick(): Unit = this.synchronized {
    Progress.tick(count = nProcessed, marker = "+", n = 10)
    nProcessed += 1
  }

  private var nProcessed = 0

  private val minSenseProbability = 0.01

  private val cacheSize = 500_000

  private val ops = {
    val params = ServiceParams(
      minSenseProbability = minSenseProbability,
      cacheSize = cacheSize
    )
    val serviceOps = new ServiceOps(db, params = params)
    serviceOps.validateWordSenseModel()
    serviceOps
  }

  private val titlePageCache: LoadingCache[String, Option[Int]] =
    Scaffeine()
      .maximumSize(cacheSize)
      .build(loader = (title: String) => db.getPage(title).map(_.id))

  private val pageTypeCache: LoadingCache[Int, PageType] =
    Scaffeine()
      .maximumSize(cacheSize)
      .build(loader = (pageId: Int) => db.getPage(pageId).map(_.pageType).get)
}
