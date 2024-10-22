package wiki.extractor.types

case class DumpPage(
  id: Int,
  namespace: Namespace,
  pageType: PageType,
  depth: Option[Int],
  title: String,
  redirectTarget: Option[String],
  lastEdited: Long)
