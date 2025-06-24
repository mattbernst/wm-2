package wiki.extractor.types

import upickle.default.*

case class RepresentativePage(pageId: Int, weight: Double, page: Option[Page])

object RepresentativePage {
  implicit val rw: ReadWriter[RepresentativePage] = macroRW
}
case class Context(pages: Array[RepresentativePage], quality: Double)

object Context {
  implicit val rw: ReadWriter[Context] = macroRW
}
