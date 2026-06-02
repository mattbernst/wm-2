package wiki.extractor.util

object Text {

  /**
    * A sentinel emitted in place of stripped template markup (WtTemplate) during
    * wikitext-to-plain-text conversion. It marks where a template used to be so
    * that an enclosing parenthetical can later be deleted wholesale. This is a
    * Unicode private-use character: it never appears in real wikitext and is left
    * untouched by the whitespace-normalizing regexes applied downstream.
    */
  val templatePlaceholder: String = ""

  /**
    * Remove parentheticals that contained template markup. Many templates (e.g.
    * `{{langx|bn|...}}`, `{{IPAc-en|...}}`) render to nothing useful in plain
    * text, and when one is the substance of a parenthetical it would otherwise
    * leave behind an empty or nonsensical `(...)`. Templates are replaced with
    * [[templatePlaceholder]] during conversion; here we delete any parenthetical
    * that contains the placeholder, then strip any remaining placeholders left by
    * templates that were not inside parentheses.
    *
    * Note: only the innermost level of parentheses is considered, so deeply
    * nested parens containing a template are not fully removed. This is
    * vanishingly rare in practice; any leftover double spaces are collapsed by
    * later cleanup.
    *
    * @param input Text that may contain [[templatePlaceholder]] sentinels
    * @return      Text with template-bearing parentheticals and sentinels removed
    */
  def removeMarkedParentheticals(input: String): String = {
    input
      .replaceAll("""\([^()]*""" + templatePlaceholder + """[^()]*\)""", "")
      .replace(templatePlaceholder, "")
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
