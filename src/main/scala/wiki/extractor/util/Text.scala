package wiki.extractor.util

import scala.io.Source

object Text extends Logging {
  def readTextFile(fileName: String): String = {
    val source = Source.fromFile(fileName)
    val lines = source.getLines().toList
    source.close()
    lines.mkString("\n")
  }

  /**
   * Extract one XML slice from an input source by looking for matched opening
   * and closing tags. This works assuming that opening tags appear as single
   * strings (ignoring whitespace) in the source and that the input does not
   * have a tag-close before a tag-open.
   *
   * @param tag     The name of the tag to search for, e.g. "page" or "siteinfo"
   * @param source  A line oriented iterator over XML string data
   * @return        The extracted XML fragment, including opening/closing tags
   */
  def tagSlice(tag: String, source: Iterator[String]): String = {
    val accumulator = new StringBuilder
    val open = s"<$tag>"
    val close = s"</$tag>"
    var completed = false
    var accumulating = false
    while (source.hasNext && !completed) {
      val line = source.next()
      val trimmed = line.trim
      if (trimmed == open) {
        accumulating = true
      }
      if (trimmed == close) {
        completed = true
      }
      if (accumulating) {
        accumulator.append(line + "\n")
      }
    }

    accumulator.toString
  }
}
