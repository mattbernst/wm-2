package wiki.extractor

import wiki.db.IDLink
import wiki.extractor.types.{PageMarkup, Worker}
import wiki.extractor.util.Logging

import scala.collection.mutable

class PageMarkupLinkProcessor(titleMap: mutable.Map[String, Int]) extends Logging {

  /**
    * Get all resolvable internal links from a single page out to other links
    * within the Wikipedia instance.
    *
    * @param pm PageMarkup data, including links
    * @return   Links based on numeric source and destination page IDs
    */
  def extract(pm: PageMarkup): Seq[IDLink] = {
    val source = pm.pageId
    pm.parseResult
      .map(_.links)
      .getOrElse(Seq())
      .flatMap { link =>
        val key = link.target.split('#').headOption.getOrElse("").toLowerCase
        titleMap.get(key) match {
          case Some(destination) =>
            Some(IDLink(source, destination, link.anchorText))
          case None =>
            // println(s"Nothing found for $source, $link, $key")
            None
        }
      }
  }

  // TODO add LinkSink
  def worker(id: Int, source: () => Option[PageMarkup]): Worker = {
    val thread = new Thread(() => {
      var completed = false
      while (!completed) {
        source() match {
          case Some(pm) =>
            val links     = extract(pm)
            val textLinks = pm.parseResult.map(_.links).getOrElse(Seq())
            if (textLinks.nonEmpty) {
              println(s"For ${pm.pageId} resolved ${links.length}/${textLinks.length} links")
            }
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
