package mix.extractor.types

case class DumpPage(
                     id: Int,
                     namespace: Namespace,
                     pageType: PageType,
                     title: String,
                     text: String,
                     redirectTarget: Option[String],
                     lastEdited: Long
                   )
