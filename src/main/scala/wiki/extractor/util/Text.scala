package wiki.extractor.util

import scala.util.hashing.MurmurHash3

object Text {

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
    val initialCapacity = 4096

    val accumulator  = new java.lang.StringBuilder(initialCapacity)
    val open         = s"<$tag>"
    val close        = s"</$tag>"
    var completed    = false
    var accumulating = false
    while (source.hasNext && !completed) {
      val line    = source.next()
      val trimmed = line.trim
      if (trimmed == open) {
        accumulating = true
      } else if (trimmed == close) {
        completed = true
      }
      if (accumulating) {
        accumulator.append(line)
        accumulator.append("\n")
      }
    }

    accumulator.toString
  }

  /**
    * Generate a 64-bit hash of the given string for fast comparison.
    *
    * @param text Input text to hash
    * @return     A 64-bit hash
    */
  def longHash(text: String): Long = {
    val hash1 = MurmurHash3.stringHash(text)
    val hash2 = MurmurHash3.stringHash(text, seed = hash1)
    (hash1.toLong << 32) | (hash2.toLong & 0xFFFFFFFFL)
  }
}
