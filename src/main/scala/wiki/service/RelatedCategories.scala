package wiki.service

import upickle.default.*

case class ScoredCategory(
  categoryId: Int,
  title: String,
  score: Double,
  frequency: Int)

object ScoredCategory {
  implicit val rw: ReadWriter[ScoredCategory] = macroRW
}

case class RelatedCategoriesResponse(categories: Seq[ScoredCategory], inputCount: Int)

object RelatedCategoriesResponse {
  implicit val rw: ReadWriter[RelatedCategoriesResponse] = macroRW
}
