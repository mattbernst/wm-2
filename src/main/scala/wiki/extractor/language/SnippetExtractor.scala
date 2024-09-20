package wiki.extractor.language
import upickle.default.*

case class Snippet(firstParagraph: Option[String], firstSentence: Option[String])

object Snippet {
  implicit val rw: ReadWriter[Snippet] = macroRW
}

trait SnippetExtractor {
  def getSnippet(input: String): Snippet
}
