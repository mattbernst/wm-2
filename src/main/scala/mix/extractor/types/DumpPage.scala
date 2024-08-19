package mix.extractor.types

case class DumpPage(
                     id: Int,
                     // namespace: Namespace,
                     pageType: PageType,
                     title: String,
                     text: String,
                     target: String,
                     lastEdited: Long
                   )
