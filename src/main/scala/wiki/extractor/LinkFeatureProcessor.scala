package wiki.extractor

import org.apache.commons.lang3.StringUtils
import wiki.db.Storage
import wiki.extractor.language.types.NGram
import wiki.extractor.types.*
import wiki.extractor.util.Progress
import wiki.service.{ServiceOps, ServiceParams}
import wiki.util.{ConfiguredProperties, Logging}

class LinkFeatureProcessor(db: Storage, props: ConfiguredProperties) extends Logging {

  def articleToFeatures(pageId: Int, groupName: String): LinkFeatures = {
    tick()
    val context = ops.contextualizer.getContext(pageId, minSenseProbability)
    ops.pageCache.getAll(context.pages.map(_.pageId))
    val page   = db.getPage(pageId).get
    val markup = db.page.readMarkupAuto(pageId).get

    val locatedLinks = markup.parseResult.map(_.links).getOrElse(Seq())
    val startToLink: Map[Int, LocatedLink] = locatedLinks
      .map(e => (e.left, e))
      .toMap

    val pageLabels = markup.wikitext
      .map(ops.contextualizer.getLabels)
      .map(labels => ops.removeNoisyLabels(labels, minSenseProbability))
      .getOrElse(Array())

    // If a label starts at the same position as a LocatedLink and has the
    // same string content, it's a linked label. These linked labels are the
    // positive examples for training a linking model.
    val positiveExamples = pageLabels.filter { pl =>
      startToLink.contains(pl.start) &&
      startToLink(pl.start).anchorText.toLowerCase(language.locale) == pl.stringContent.toLowerCase(language.locale)
    }

    val linkedLabelSet = positiveExamples
      .map(_.stringContent.toLowerCase(language.locale))
      .toSet

    // If a label is never used as a link in the document, it provides a
    // negative example.
    val negativeExamples = pageLabels
      .filterNot(pl => linkedLabelSet.contains(pl.stringContent.toLowerCase(language.locale)))

    // Generate positive examples from labels that are linked anywhere
    val positiveExampleEntries = makeLinkModelEntries(
      markup = markup.wikitext.get,
      context = context,
      page = page,
      examples = positiveExamples,
      linksAreValid = true
    )

    // Generate negative example from labels that are linked nowhere
    val negativeExampleEntries = makeLinkModelEntries(
      markup = markup.wikitext.get,
      context = context,
      page = page,
      examples = negativeExamples,
      linksAreValid = false
    )

    val combined = (positiveExampleEntries ++ negativeExampleEntries)
      .filter(_.linkProbability < 1.0) // 1.0 indicates badly counted links
      .distinct

    val combinedWithRelated = assignRelatednessToOtherTopics(combined, 25)

    LinkFeatures(
      group = groupName,
      page = ops.pageCache.get(pageId),
      context = ops.enrichContext(context),
      examples = combinedWithRelated
    )
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
        e => RepresentativePage(pageId = e.senseId, weight = e.linkProbability * e.normalizedOccurrences, page = None)
      )
      .sortBy(-_.weight)
      .take(topN)

    val context = Context(pages = topCandidates, quality = topCandidates.map(_.weight).sum)

    input.map { entry =>
      val relatedness = ops.contextualizer.getRelatedness(entry.senseId, context)
      entry.copy(relatednessToOtherTopics = relatedness)
    }
  }

  private def makeLinkModelEntries(
    markup: String,
    context: Context,
    page: Page,
    examples: Array[NGram],
    linksAreValid: Boolean
  ): Array[LinkModelEntry] = {
    val docLength = markup.length.toDouble
    val resolvedSenses = ops
      .resolveSenses(examples, context)
      .map(r => (r.nGram, r.resolvedLabel))
      .toMap

    examples.flatMap { example =>
      val firstOccurrence = markup.indexOf(example.stringContent) / docLength
      val lastOccurrence  = markup.lastIndexOf(example.stringContent) / docLength
      assert(firstOccurrence > -1)
      assert(lastOccurrence > -1)
      val occurrences     = StringUtils.countMatches(markup, example.stringContent)
      val linkProbability = ops.labelCounter.getLinkProbability(example.stringContent)

      resolvedSenses.get(example).filter(_ => linkProbability.nonEmpty).map { resolvedLabel =>
        val senseId = resolvedLabel.page.id

        LinkModelEntry(
          sourcePageId = page.id,
          label = example.stringContent,
          sensePageTitle = resolvedLabel.page.title,
          senseId = senseId,
          normalizedOccurrences = math.log(occurrences + 1),
          maxDisambigConfidence = resolvedLabel.allSenses.values.max,
          avgDisambigConfidence = resolvedLabel.allSenses.values.sum / resolvedLabel.allSenses.size,
          relatednessToContext = ops.contextualizer.getRelatedness(senseId, context),
          relatednessToOtherTopics = -1.0, // Assigned later in assignRelatednessToOtherTopics
          linkProbability = linkProbability.get,
          firstOccurrence = firstOccurrence,
          lastOccurrence = lastOccurrence,
          spread = lastOccurrence - firstOccurrence,
          isValidLink = linksAreValid
        )
      }
    }
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
      cacheSize = 500_000
    )
    val serviceOps = new ServiceOps(db, params = params)
    serviceOps.validateWordSenseModel()
    serviceOps
  }

  private val language = props.language
}
