package wiki.extractor.types

case class Page(
  id: Int,
  namespace: Namespace,
  pageType: PageType,
  depth: Option[Int],
  title: String,
  redirectTarget: Option[String],
  lastEdited: Long)
