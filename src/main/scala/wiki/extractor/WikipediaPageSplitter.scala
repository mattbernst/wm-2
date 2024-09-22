package wiki.extractor

import wiki.extractor.util.Text

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}

class WikipediaPageSplitter(source: Iterator[String], queueSize: Int = 8000) {
  var pageCount = 0

  /**
    * Split input strings into page chunks by matching "<page>" and "</page>"
    * pairs. Since the Wikipedia XML dump does not have nested pages and has
    * page tags on their own lines, page-splitting can be performed with simple
    * string operations instead of full XML parsing.
    *
    * The page chunks are added to the internal queue for later use by
    * consumers calling getFromQueue. This keeps the memory footprint
    * smaller than generating all page chunks before consuming them.
    *
    * See https://en.wikipedia.org/wiki/Wikipedia:Database_download for
    * information about the XML dumps.
    */
  def extractPages(): Unit = {
    while (source.hasNext) {
      val slice = Text.tagSlice("page", source)
      if (slice.nonEmpty) {
        queue.put(slice)
        pageCount += 1
      }
    }
  }

  def getFromQueue(): Option[String] =
    Option(queue.poll(3, TimeUnit.SECONDS))

  private val queue = new ArrayBlockingQueue[String](queueSize)
}
