package wiki.extractor

import wiki.db.{DeadLink, LinkSink, ResolvedLink}
import wiki.extractor.types.{Language, PageMarkup, Worker}
import wiki.extractor.util.Logging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class LinkResults(resolved: Seq[ResolvedLink], dead: Seq[DeadLink])

class PageMarkupLinkProcessor(titleMap: mutable.Map[String, Int], language: Language, categoryName: String)
    extends Logging {

  /**
    * Get all internal links from a single page out to other links within the
    * Wikipedia instance. If a destination link title can be resolved to a
    * numeric page ID, the link is transformed to a ResolvedLink. If the
    * destination could not be resolved, it is returned as a DeadLink.
    *
    * @param pm PageMarkup data, including links
    * @return   Resolved and dead links linking out from the input page
    */
  def extract(pm: PageMarkup): LinkResults = {
    val resolved = new ListBuffer[ResolvedLink]
    val dead     = new ListBuffer[DeadLink]
    val src      = pm.pageId
    pm.parseResult
      .map(_.links)
      .getOrElse(Seq())
      .foreach { link =>
        val dst = link.target.replace("&nbsp;", " ")
        val key = keyFromTarget(dst)
        // Filter out anchor text if it's just whitespace.
        val anchorText = link.anchorText.map(_.trim).filter(_.nonEmpty)
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

  def worker(id: Int, source: () => Option[PageMarkup], sink: LinkSink): Worker = {
    val thread = new Thread(() => {
      var completed = false
      while (!completed) {
        source() match {
          case Some(pm) =>
            val results = extract(pm)
            results.resolved.foreach(l => sink.addLink(resolvedLink = Some(l), deadLink = None))
            results.dead.foreach(l => sink.addLink(resolvedLink = None, deadLink = Some(l)))
          case None =>
            completed = true
            logger.info(s"Worker $id finished")
        }
      }
    })
    logger.info(s"Starting Worker $id")
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
      .toLowerCase
    // Links like ":fr:Les Cahiers de l'Orient" point to the named page
    // in the language-specific Wikipedia instance. We can only resolve
    // links that point within the current Wikipedia.
    val cleaned = if (k.startsWith(currentWikiPrefix)) {
      k.slice(currentWikiPrefix.length, k.length).trim
    }
    // Category links may start this way, e.g. ":Category:Songwriters"
    else if (k.startsWith(catPrefix)) {
      k.tail
    } else {
      k
    }
    // Categories can have stray spaces following the colon -- fix them here
    cleaned.replace(spacedCat, unspacedCat)
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

  private val catPrefix: String =
    s":$categoryName:".toLowerCase

  private val spacedCat: String =
    s"$categoryName: ".toLowerCase

  private val unspacedCat: String =
    s"$categoryName:".toLowerCase
}
