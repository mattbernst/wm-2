package wiki.extractor.types

import upickle.default.*
import wiki.util.FileHelpers

import java.text.BreakIterator
import java.time.format.DateTimeFormatter
import java.time.{MonthDay, YearMonth}
import java.util.Locale
import scala.collection.mutable

case class DataGroup(name: String, size: Int)

object DataGroup {
  implicit val rw: ReadWriter[DataGroup] = macroRW
}

case class TrainingProfile(
  groups: Seq[DataGroup],
  minOutLinks: Int,
  minInLinks: Int,
  maxListProportion: Double,
  minWordCount: Int,
  maxWordCount: Int)

object TrainingProfile {
  implicit val rw: ReadWriter[TrainingProfile] = macroRW

  def empty: TrainingProfile =
    TrainingProfile(
      groups = Seq(),
      minOutLinks = 0,
      minInLinks = 0,
      maxListProportion = 0.0,
      minWordCount = 0,
      maxWordCount = 0
    )
}

case class Language(
  code: String, // an ISO 639-1 language code e.g. "en"
  name: String, // e.g. "English"
  /*
   * See https://en.wikipedia.org/wiki/Template:Disambiguation
   * and also transclusion_counts.json after running with
   * COUNT_LAST_TRANSCLUSIONS=true
   */
  disambiguationPrefixes: Seq[String],
  trainingProfile: TrainingProfile) {

  /**
    * Generate all possible "MMMM d" combinations (full month name + day) that are valid
    * in the current locale.
    *
    * @return A Set of all valid date strings in "MMMM d" format
    */
  def generateValidDateStrings(): mutable.Set[String] = {
    val formatter = DateTimeFormatter.ofPattern("MMMM d", locale)
    val validStrings = for {
      month <- 1 to 12
      // Get the maximum days for this month (using a leap year)
      maxDay = YearMonth.of(2024, month).lengthOfMonth()
      day <- 1 to maxDay
      monthDay   = MonthDay.of(month, day)
      dateString = monthDay.format(formatter)
    } yield dateString

    mutable.Set.from(validStrings)
  }

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
    val tHead = transclusion.split('|').headOption.map(_.toLowerCase(locale)).getOrElse("")
    normalizedDisambiguationPrefixes.contains(tHead)
  }

  val locale: Locale = {
    val cc = if (code == "en_simple") {
      "en"
    } else {
      code
    }

    // Should be Locale.of(cc) for Java 19+, but that doesn't work before 19
    new Locale(cc)
  }

  /**
    * Capitalize the first visual character of a string according to the
    * current locale.
    *
    * @param input A string that may or may not already start with a capital
    * @return      A capital-character-first string
    */
  def capitalizeFirst(input: String): String = {
    if (input.nonEmpty) {
      // Use BreakIterator to find the first grapheme cluster (visual character)
      val boundary = BreakIterator.getCharacterInstance(locale)
      boundary.setText(input)

      val firstBoundary = boundary.next()
      if (firstBoundary == BreakIterator.DONE || firstBoundary <= 0) {
        input
      } else {
        val firstGrapheme = input.substring(0, firstBoundary)
        val upperFirst    = firstGrapheme.toUpperCase(locale)
        upperFirst + input.substring(firstBoundary)
      }
    } else {
      input
    }
  }

  /**
    * Un-capitalize the first visual character of a string according to the
    * current locale.
    *
    * @param input A string that may or may not already start with a capital
    * @return      A non-capital-character-first string
    */
  def unCapitalizeFirst(input: String): String = {
    if (input.nonEmpty) {
      // Use BreakIterator to find the first grapheme cluster (visual character)
      val boundary = BreakIterator.getCharacterInstance(locale)
      boundary.setText(input)

      val firstBoundary = boundary.next()
      if (firstBoundary == BreakIterator.DONE || firstBoundary <= 0) {
        input
      } else {
        val firstGrapheme = input.substring(0, firstBoundary)
        val upperFirst    = firstGrapheme.toLowerCase(locale)
        upperFirst + input.substring(firstBoundary)
      }
    } else {
      input
    }
  }

  // Normally this is based on the language code, but in the case of the Simple
  // English Wikipedia it's "simple".
  val currentWikiPrefix: String = {
    val prefix = if (code == "en_simple") {
      "simple"
    } else {
      code
    }
    s":$prefix:"
  }

  private val normalizedDisambiguationPrefixes: mutable.Set[String] =
    mutable.Set.from(disambiguationPrefixes.map(_.toLowerCase(locale)))
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
