package wiki.extractor.language
import upickle.default.*

case class Snippet(firstParagraph: Option[String], firstSentence: Option[String])

object Snippet {
  implicit val rw: ReadWriter[Snippet] = macroRW
}

trait LanguageLogic {
  def getSnippet(input: String): Snippet
}

object LanguageLogic {

  val logicForLanguage: Map[String, LanguageLogic] = Map(
    "en"        -> EnglishLanguageLogic,
    "en_simple" -> EnglishLanguageLogic
  )
}
