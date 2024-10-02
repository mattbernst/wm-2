package wiki.extractor

import wiki.db.{DeadLink, LinkSink, ResolvedLink}
import wiki.extractor.types.{PageMarkup, Worker}
import wiki.extractor.util.Logging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class LinkResults(resolved: Seq[ResolvedLink], dead: Seq[DeadLink])

class PageMarkupLinkProcessor(titleMap: mutable.Map[String, Int]) extends Logging {

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
    val source   = pm.pageId
    pm.parseResult
      .map(_.links)
      .getOrElse(Seq())
      .foreach { link =>
        val key = link.target.split('#').headOption.getOrElse("").toLowerCase
        // Filter out anchor text if it's just whitespace
        val anchorText = link.anchorText.map(_.trim).filter(_.nonEmpty)
        titleMap.get(key) match {
          case Some(id) => resolved.append(ResolvedLink(source, id, anchorText))
          case None     => dead.append(DeadLink(source, link.target, anchorText))
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
}
