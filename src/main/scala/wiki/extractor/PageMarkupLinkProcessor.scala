package wiki.extractor

import wiki.db.{DeadLink, LinkSink, ResolvedLink}
import wiki.extractor.types.{Language, PageMarkup, TypedPageMarkup, Worker}
import wiki.extractor.util.DBLogging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class LinkResults(resolved: Seq[ResolvedLink], dead: Seq[DeadLink])

class PageMarkupLinkProcessor(titleMap: mutable.Map[String, Int], language: Language, categoryName: String) {

  /**
    * Get all internal links from a single page out to other links within the
    * Wikipedia instance. If a destination link title can be resolved to a
    * numeric page ID, the link is transformed to a ResolvedLink. If the
    * destination could not be resolved, it is returned as a DeadLink.
    *
    * @param tpm PageMarkup data, including links
    * @return    Resolved and dead links linking out from the input page
    */
  def extract(tpm: TypedPageMarkup): LinkResults = {
    val pm = if (tpm.pmu.nonEmpty) {
      PageMarkup.deserializeUncompressed(tpm.pmu.get)
    } else {
      PageMarkup.deserializeCompressed(tpm.pmz.get)
    }
    val resolved = ListBuffer[ResolvedLink]()
    val dead     = ListBuffer[DeadLink]()
    val src      = pm.pageId
    pm.parseResult
      .map(_.links)
      .getOrElse(Seq())
      .foreach { link =>
        val dst        = link.target.replace("&nbsp;", " ")
        val key        = keyFromTarget(link.target)
        val anchorText = cleanAnchor(link.anchorText)
        titleMap.get(key) match {
          // Valid links have destination text resolved to numeric page IDs,
          // and the source ID is different from the destination.
          // Dead links either have destination text that could not be matched
          // to a page or they have a source that is the same as the destination.
          case Some(id) if src != id => resolved.append(ResolvedLink(src, id, anchorText))
          case _                     => dead.append(DeadLink(src, dst, anchorText))
        }
      }

    LinkResults(resolved.toSeq, dead.toSeq)
  }

  def worker(id: Int, source: () => Option[TypedPageMarkup], sink: LinkSink): Worker = {
    val thread = new Thread(() => {
      var completed = false
      while (!completed) {
        source() match {
          case Some(tpm) =>
            val results = extract(tpm)
            results.resolved.foreach(l => sink.addLink(resolvedLink = Some(l), deadLink = None))
            results.dead.foreach(l => sink.addLink(resolvedLink = None, deadLink = Some(l)))
          case None =>
            completed = true
            DBLogging.info(s"PageMarkupLinkProcessor Worker $id finished")
        }
      }
    })
    DBLogging.info(s"Starting PageMarkupLinkProcessor Worker $id")
    thread.setDaemon(true)
    thread.start()
    Worker(thread)
  }

  /**
    * Modify link destination text in certain cases to make it directly
    * matchable to page titles, and therefore resolvable as a link. Links
    * that cannot be resolved will be added to the dead_link table instead
    * of the link table.
    *
    * N.B. this does not resolve within-page links that point to a different
    * section of the same article (e.g. "#History of Earth") because the
    * link table does not contain self-links.
    *
    * @param target A link destination that may contain special formatting
    * @return       A link massaged into a (likely) page title
    */
  private[extractor] def keyFromTarget(target: String): String = {
    val k = target
      .split('#')
      .headOption
      .map(_.replace('_', ' '))
      .map(_.trim)
      .getOrElse("")
      .toLowerCase(language.locale)
    // Links like ":fr:Les Cahiers de l'Orient" point to the named page
    // in the language-specific Wikipedia instance. We can only resolve
    // links that point within the current Wikipedia.
    val cleaned = if (k.startsWith(currentWikiPrefix)) {
      k.slice(currentWikiPrefix.length, k.length).trim
    }
    // Category links may start this way, e.g. ":Category:Songwriters"
    else if (k.startsWith(extraColonCatPrefix)) {
      k.tail
    } else {
      k
    }
    // Categories can have stray spaces following the colon -- fix them here
    cleaned.replace(spacedCat, unspacedCat)
  }

  /**
    * Perform some less aggressive modifications to the anchor text. We need
    * to simplify/normalize link text but preserve casing.
    *
    * @param input An anchor text
    * @return      Anchor text with cleanup
    */
  private[extractor] def cleanAnchor(input: String): String = {
    val k = input
      .split('#')
      .headOption
      .map(_.replace("&nbsp;", " "))
      .map(_.replace('_', ' '))
      .map(_.trim)
      .getOrElse("")

    // Links like ":fr:Les Cahiers de l'Orient" point to the named page
    // in the language-specific Wikipedia instance. Remove these
    // language-indicator prefixes.
    val cleaned = if (k.startsWith(currentWikiPrefix)) {
      k.slice(currentWikiPrefix.length, k.length).trim
    } else {
      k
    }

    cleaned.trim
  }

  // Normally this is based on the language code, but in the case of the Simple
  // English Wikipedia it's "simple".
  private val currentWikiPrefix: String = {
    val code = if (language.code == "en_simple") {
      "simple"
    } else {
      language.code
    }
    s":$code:"
  }

  private val extraColonCatPrefix: String =
    s":$categoryName:".toLowerCase(language.locale)

  private val spacedCat: String =
    s"$categoryName: ".toLowerCase(language.locale)

  private val unspacedCat: String =
    s"$categoryName:".toLowerCase(language.locale)
}
