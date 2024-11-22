package wiki.extractor.language.types

import upickle.default.*

case class Snippet(firstParagraph: Option[String], firstSentence: Option[String])

object Snippet {
  implicit val rw: ReadWriter[Snippet] = macroRW
}
