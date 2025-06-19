package wiki.extractor.types

import upickle.default.*

case class Page(
  id: Int,
  namespace: Namespace,
  pageType: PageType,
  title: String,
  redirectTarget: Option[String],
  lastEdited: Long,
  markupSize: Option[Int])

object Page {
  implicit val rw: ReadWriter[Page] = macroRW
}
