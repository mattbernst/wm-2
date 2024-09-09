package wiki.extractor.types

import upickle.default.*
import wiki.extractor.util.Text

case class NamespaceAlias(from: String, to: String)
object NamespaceAlias {
  implicit val rw: ReadWriter[NamespaceAlias] = macroRW
}
case class Language(
                     code: String, // e.g. "en"
                     name: String, // e.g. "English"
                     // See https://en.wikipedia.org/wiki/Template:Disambiguation
                     // and also transclusion_counts.json after running with
                     // COUNT_LAST_TRANSCLUSIONS=true
                     disambiguationPrefixes: Seq[String],
                     // See https://en.wikipedia.org/wiki/Wikipedia:Namespace#Aliases_and_pseudo-namespaces
                     aliases: Seq[NamespaceAlias]
                   ) {
  /**
   * Determine if the last transclusion from a page indicates that the page is
   * a disambiguation page. The page is considered a disambiguation page if
   * the lower-cased, suffix-stripped version of the transclusion matches
   * one of the lower cased strings in disambiguationPrefixes.
   *
   * @param transclusion The last transclusion found on a page
   * @return             Whether the transclusion matches a disambiguation prefix
   */
  def isDisambiguation(transclusion: String): Boolean = {
    val tHead = transclusion
      .split('|')
      .headOption
      .map(_.toLowerCase)
      .getOrElse("")
    normalizedDisambiguationPrefixes.contains(tHead)
  }

  private val normalizedDisambiguationPrefixes: Set[String] =
    disambiguationPrefixes.map(_.toLowerCase).toSet
}

object Language {
  implicit val rw: ReadWriter[Language] = macroRW

  def toJSON(input: Seq[Language]): String =
    write(input)

  def fromJSON(input: String): Seq[Language] =
    read[Seq[Language]](input)

  def fromJSONFile(fileName: String): Seq[Language] =
    fromJSON(Text.readTextFile(fileName))
}
