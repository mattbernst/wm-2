package wiki.service

import upickle.default.*

// Keep all of these case class definitions together so it's easy to refer
// to them by consumers of the /doc/labels/simple endpoint.

case class TopicPage(linkedPageId: Int, linkPrediction: Double, surfaceForms: Seq[String])

object TopicPage {
  implicit val rw: ReadWriter[TopicPage] = macroRW
}

case class SimplePage(id: Int, title: String)

object SimplePage {
  implicit val rw: ReadWriter[SimplePage] = macroRW
}
case class SimpleRepresentativePage(page: SimplePage, weight: Double)

object SimpleRepresentativePage {
  implicit val rw: ReadWriter[SimpleRepresentativePage] = macroRW
}
case class SimpleResolvedLabel(label: String, page: SimplePage, scoredSenses: Map[Int, Double])

object SimpleResolvedLabel {
  implicit val rw: ReadWriter[SimpleResolvedLabel] = macroRW
}

case class StreamlinedLinks(
  contextPages: Array[SimpleRepresentativePage],
  contextQuality: Double,
  labels: Seq[String],
  resolvedLabels: Seq[SimpleResolvedLabel],
  links: Seq[(SimplePage, TopicPage)])

object StreamlinedLinks {
  implicit val rw: ReadWriter[StreamlinedLinks] = macroRW
}
