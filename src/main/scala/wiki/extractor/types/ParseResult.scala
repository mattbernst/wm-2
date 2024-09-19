package wiki.extractor.types

import upickle.default.*

// Links as extracted here match the page text exactly.
// Properly casing the target and stripping sub-page sections happens later.
case class Link(target: String, anchorText: Option[String])

object Link {
  implicit val rw: ReadWriter[Link] = macroRW
}

case class ParseResult(
  firstSentence: String,
  firstParagraph: String,
  text: String,
  links: Seq[Link])

object ParseResult {
  implicit val rw: ReadWriter[ParseResult] = macroRW
}
