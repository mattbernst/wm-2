package wiki.extractor.phases

import wiki.db.Storage
import wiki.extractor.util.ConfiguredProperties

class Phase05(db: Storage, props: ConfiguredProperties) extends Phase(db: Storage, props: ConfiguredProperties) {

  override def run(): Unit = {
    db.phase.deletePhase(number)
    db.createTableDefinitions(number)
    db.anchor.delete()
  }

  override def number: Int = 5

  override val incompleteMessage: String = s"Phase $number incomplete -- redoing"
}
