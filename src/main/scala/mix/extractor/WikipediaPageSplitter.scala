package mix.extractor

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}

class WikipediaPageSplitter(source: Iterator[String], queueSize: Int = Short.MaxValue) {
  var pageCount = 0

  /**
   * Split input strings into page chunks by matching "<page>" and "</page>"
   * pairs. Since the Wikipedia XML dump does not have nested pages and has
   * page tags on their own lines, page-splitting can be performed with simple
   * string operations instead of full XML parsing.
   *
   * The page chunks are added to the internal queue for later use by
   * consumers calling getFromQueue. This keeps less data in memory than
   * generating all page chunks before consuming them.
   *
   * See https://en.wikipedia.org/wiki/Wikipedia:Database_download for
   * information about the XML dumps.
   */
  def extractPages(): Unit = {
    source.foreach { line =>
      val trimmed = line.trim
      if (trimmed == "<page>") {
        inPage = true
      }
      if (trimmed == "</page>") {
        accumulator.append(line + "\n")
        inPage = false
        val page = accumulator.toString
        queue.put(page)
        pageCount += 1
        accumulator.clear()
      }
      if (inPage) {
        accumulator.append(line + "\n")
      }
      if (!inPage && trimmed != "</page>") {
      }
    }
  }

  def getFromQueue(): Option[String] =
    Option(queue.poll(3, TimeUnit.SECONDS))

  private val queue = new ArrayBlockingQueue[String](queueSize)
  private var inPage = false
  private val accumulator = new StringBuilder
}
