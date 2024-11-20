package wiki.extractor.phases

import wiki.db.Storage
import wiki.extractor.language.LanguageLogic
import wiki.extractor.util.{ConfiguredProperties, DBLogging}

import scala.util.{Success, Try}

class Phase06(db: Storage, props: ConfiguredProperties) extends Phase(db: Storage, props: ConfiguredProperties) {

  override def run(): Unit = {
    db.phase.deletePhase(number)
    db.phase.createPhase(number, s"Gathering page-level label statistics")
    db.anchor.clearOccurrenceCounts()
    DBLogging.info("Loading link anchor statistics from db")
    val counter = db.anchor.read()
    val ll      = LanguageLogic.getLanguageLogic(props.language.code)
  }

  override def number: Int = 6

  override val incompleteMessage: String = s"Phase $number incomplete -- redoing"
}
