package wiki.extractor.language.types

sealed trait CaseContext

object CaseContext {
  case object LOWER       extends CaseContext
  case object UPPER       extends CaseContext
  case object UPPER_FIRST extends CaseContext
  case object MIXED       extends CaseContext
}
