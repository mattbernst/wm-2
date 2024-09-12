package wiki.extractor.types

import wiki.extractor.util.ZString

case class PageMarkup(pageId: Int, text: Option[String])

case class PageMarkup_Z(pageId: Int, text: Option[Array[Byte]])

object PageMarkup {
  def compress(input: PageMarkup): PageMarkup_Z =
    PageMarkup_Z(input.pageId, input.text.map(s => ZString.compress(s)))
}
