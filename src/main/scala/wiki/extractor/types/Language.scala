package wiki.extractor.types

import upickle.default.*
import wiki.extractor.util.FileHelpers

case class Language(
  code: String, // e.g. "en"
  name: String, // e.g. "English"
  /*
                      See https://en.wikipedia.org/wiki/Template:Disambiguation
                      and also transclusion_counts.json after running with
                      COUNT_LAST_TRANSCLUSIONS=true
   */
  disambiguationPrefixes: Seq[String],
  /*
                      The root page is a somewhat arbitrary starting point
                      for determining the "depth" of a Wikipedia page. Milne used
                      "Category:Fundamental categories" as his root page for
                      English Wikipedia, but it was deleted in December 2016:
                      https://en.wikipedia.org/wiki/Wikipedia:Categories_for_discussion/Log/2016_December_18#Category:Fundamental_categories
   */
  rootPage: String) {

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
    val tHead = transclusion.split('|').headOption.map(_.toLowerCase).getOrElse("")
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
    fromJSON(FileHelpers.readTextFile(fileName))
}
