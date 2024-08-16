package mix.extractor.types

case class DumpPage(
                     id: Int,
                     // namespace: Namespace,
                     pageType: PageType,
                     title: String,
                     markup: String,
                     target: String,
                     lastEdited: Long
                   )