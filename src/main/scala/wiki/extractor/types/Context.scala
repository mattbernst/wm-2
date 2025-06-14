package wiki.extractor.types

case class RepresentativePage(pageId: Int, weight: Double, page: Option[Page])
case class Context(pages: Array[RepresentativePage], quality: Double)
