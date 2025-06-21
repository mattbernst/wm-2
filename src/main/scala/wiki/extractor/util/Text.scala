package wiki.extractor.util

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
    * Replace every codepoint that is not a letter or digit with a single
    * space. For example, "'Doctor Who', starring Jodie Whittaker" becomes
    * " Doctor Who   starring Jodie Whittaker".
    *
    * This is useful for supplementary NGram generation because it can
    * match phrases that would otherwise mismatch due to punctuation
    * variations.
    *
    * Properly handles Unicode supplementary characters.
    *
    * @param input A string to strip of extraneous characters
    * @return
    */
  def filterToLettersAndDigits(input: String): String = {
    val result = new StringBuilder
    var i      = 0

    while (i < input.length) {
      val codePoint = input.codePointAt(i)

      if (Character.isLetterOrDigit(codePoint)) {
        val chars = Character.toChars(codePoint)
        result.appendAll(chars)
      } else {
        result.append(' ')
      }

      // Move to next character (handles supplementary characters)
      i += Character.charCount(codePoint)
    }

    result.toString
  }
}
