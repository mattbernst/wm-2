package wiki.extractor.types

case class DumpPage(
                     id: Int,
                     namespace: Namespace,
                     pageType: PageType,
                     title: String,
                     text: Option[String],
                     redirectTarget: Option[String],
                     lastEdited: Option[Long]
                   )
