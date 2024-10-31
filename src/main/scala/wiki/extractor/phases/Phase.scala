package wiki.extractor.phases

import wiki.db.Storage
import wiki.extractor.util.ConfiguredProperties

abstract class Phase(db: Storage, props: ConfiguredProperties) {
  def number: Int
  val incompleteMessage: String
  val finishedMessage: String = s"Already completed phase $number"

  def run(): Unit = {}

  def run(args: Array[String]): Unit = {}

}
