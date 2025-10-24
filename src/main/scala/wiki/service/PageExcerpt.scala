package wiki.service

import upickle.default.*

case class PageExcerpt(pageId: Int, firstParagraph: String)

object PageExcerpt {
  implicit val rw: ReadWriter[PageExcerpt] = macroRW
}
