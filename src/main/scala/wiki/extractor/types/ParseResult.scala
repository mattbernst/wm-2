package wiki.extractor.types

import upickle.default.*
import wiki.extractor.language.types.Snippet

case class Link(target: String, anchorText: String)

object Link {
  implicit val rw: ReadWriter[Link] = macroRW
}

case class LocatedLink(
  target: String,
  anchorText: String,
  left: Int,
  right: Int)

object LocatedLink {
  implicit val rw: ReadWriter[LocatedLink] = macroRW
}

case class ParseResult(snippet: Snippet, text: String, links: Seq[LocatedLink])

object ParseResult {
  implicit val rw: ReadWriter[ParseResult] = macroRW
}
