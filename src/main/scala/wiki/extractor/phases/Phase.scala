package wiki.extractor.phases

import wiki.db.Storage

abstract class Phase(protected val db: Storage) {
  def number: Int
  val incompleteMessage: String
  val finishedMessage: String = s"Already completed phase $number"

  def run(): Unit = {}

  def run(args: Array[String]): Unit = {}

}
