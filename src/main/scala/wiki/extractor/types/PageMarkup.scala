package wiki.extractor.types

import wiki.extractor.util.ZString

case class PageMarkup(pageId: Int, wikitext: Option[String], json: Option[String])

case class PageMarkup_Z(pageId: Int, wikitext: Option[Array[Byte]], json: Option[Array[Byte]])

object PageMarkup {

  def compress(input: PageMarkup): PageMarkup_Z =
    PageMarkup_Z(
      pageId = input.pageId,
      wikitext = input.wikitext.map(s => ZString.compress(s)),
      json = input.json.map(s => ZString.compress(s))
    )
}
