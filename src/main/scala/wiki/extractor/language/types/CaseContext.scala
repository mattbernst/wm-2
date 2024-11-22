package wiki.extractor.language.types

sealed trait CaseContext

object CaseContext {
  case object Lower      extends CaseContext
  case object Upper      extends CaseContext
  case object UpperFirst extends CaseContext
  case object Mixed      extends CaseContext
}
