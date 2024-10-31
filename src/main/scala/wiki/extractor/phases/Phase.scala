package wiki.extractor.phases

import wiki.db.Storage
import wiki.extractor.util.ConfiguredProperties

abstract class Phase(number: Int, db: Storage, props: ConfiguredProperties) {
  val finishedMessage: String = s"Already completed phase $number"
  val incompleteMessage: String

  def run(): Unit = {}

  def run(args: Array[String]): Unit = {}

}
