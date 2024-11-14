package wiki.extractor.types

import upickle.default.*
import wiki.extractor.language.Snippet

// Links as extracted here match the page text exactly.
// Properly casing the target and stripping sub-page sections happens later.
case class Link(target: String, anchorText: String)

object Link {
  implicit val rw: ReadWriter[Link] = macroRW
}

case class ParseResult(snippet: Snippet, text: String, links: Seq[Link])

object ParseResult {
  implicit val rw: ReadWriter[ParseResult] = macroRW
}
