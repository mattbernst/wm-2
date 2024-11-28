package wiki.extractor.phases

import wiki.db.Storage
import wiki.extractor.util.ConfiguredProperties

abstract class Phase(protected val db: Storage, protected val props: ConfiguredProperties) {
  def number: Int
  val incompleteMessage: String
  val finishedMessage: String = s"Already completed phase $number"

  def run(): Unit = {}

  def run(args: Array[String]): Unit = {}

}
