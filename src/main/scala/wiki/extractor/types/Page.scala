package wiki.extractor.types

case class Page(
  id: Int,
  namespace: Namespace,
  pageType: PageType,
  title: String,
  redirectTarget: Option[String],
  lastEdited: Long,
  markupSize: Int)
