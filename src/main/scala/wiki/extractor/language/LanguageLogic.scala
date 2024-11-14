package wiki.extractor.language
import opennlp.tools.sentdetect.SentenceDetectorME
import upickle.default.*

case class Snippet(firstParagraph: Option[String], firstSentence: Option[String])

object Snippet {
  implicit val rw: ReadWriter[Snippet] = macroRW
}

trait LanguageLogic {

  /**
    * Use heuristics to get the first paragraph and first sentence of content
    * from a potentially large input string representing a plain-text version
    * of a Wikipedia page.
    *
    * Heuristic: Get groups of sentences separated by newlines. The first
    * multi-sentence group is the first paragraph. The first sentence is taken
    * from the first paragraph. If there are no multi-sentence groups, but there
    * are single-sentence groups, then the first paragraph is None and the first
    * sentence is the first sentence from the input. If there are no sentences
    * at all, then both firstParagraph and firstSentence are None.
    *
    * @param input Text to process for extraction
    * @return A snippet of extracted text content
    */
  def getSnippet(input: String): Snippet = {
    val lines = input.split('\n')

    val firstParaGroup = lines.iterator
      .map(chunk => sentenceDetector.get().sentDetect(chunk))
      .find(_.length > 1)

    val firstParagraph = firstParaGroup.map(_.mkString(" "))
    val firstSentence = firstParaGroup
      .flatMap(_.headOption)
      .orElse {
        lines.iterator
          .map(chunk => sentenceDetector.get().sentDetect(chunk))
          .find(_.nonEmpty)
          .map(_.head)
      }

    Snippet(firstParagraph = firstParagraph, firstSentence = firstSentence)
  }

  protected def sentenceDetector: ThreadLocal[SentenceDetectorME]
}

object LanguageLogic {

  val logicForLanguage: Map[String, LanguageLogic] = Map(
    "en"        -> EnglishLanguageLogic,
    "en_simple" -> EnglishLanguageLogic,
    "fr"        -> FrenchLanguageLogic
  )
}
