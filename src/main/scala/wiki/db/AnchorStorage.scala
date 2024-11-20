package wiki.db

import scalikejdbc.*
import wiki.extractor.types.AnchorCounter
import wiki.extractor.util.{DBLogging, Progress}

object AnchorStorage {

  /**
    * Write all the anchors and counts from the counter into the anchor table.
    *
    * @param counter AnchorCounter containing accumulated counts
    */
  def write(counter: AnchorCounter): Unit = {
    var id = 0
    DB.autoCommit { implicit session =>
      val batches         = counter.getEntries().grouped(batchSize)
      val cols: SQLSyntax = sqls"""label, id, occurrence_count, occurrence_doc_count, link_count, link_doc_count"""
      batches.foreach { batch =>
        val params: Seq[Seq[SQLSyntax]] = batch.map { t =>
          id += 1
          Progress.tick(id, "+")
          Seq(
            sqls"${t._1}",
            sqls"$id",
            sqls"${t._2(AnchorCounter.occurrenceCountIndex)}",
            sqls"${t._2(AnchorCounter.occurrenceDocCountIndex)}",
            sqls"${t._2(AnchorCounter.linkOccurrenceCountIndex)}",
            sqls"${t._2(AnchorCounter.linkOccurrenceDocCountIndex)}"
          )
        }
        val values: SQLSyntax = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT INTO $table ($cols) VALUES $values""".update()
      }
    }
  }

  /**
    * Read the entire anchor table back as an AnchorCounter.
    *
    * @return An AnchorCounter containing all data from the anchor table
    */
  def read(): AnchorCounter = {
    val counter = new AnchorCounter
    var current = 0
    val end: Int = DB.autoCommit { implicit session =>
      sql"""SELECT MAX(id) AS max_id FROM $table"""
        .map(rs => rs.intOpt("max_id"))
        .single()
        .flatten
    }.getOrElse {
      DBLogging.warn("Did not find any IDs in anchor table")
      0
    }

    while (current < end) {
      DB.autoCommit { implicit session =>
        sql"""SELECT * FROM $table
             WHERE id >= $current AND id < ${current + batchSize}""".foreach { rs =>
          val label = rs.string("label")
          val counts = Array(
            rs.int("occurrence_count"),
            rs.int("occurrence_doc_count"),
            rs.int("link_count"),
            rs.int("link_doc_count")
          )
          counter.insert(label, counts)
        }
      }
      current += batchSize
    }

    counter
  }

  /**
    * Delete contents of anchor table. This will get called if the
    * anchor-counting phase needs to run again (in case data had been partially
    * written).
    */
  def delete(): Unit = {
    DB.autoCommit { implicit session =>
      sql"""DELETE FROM $table"""
        .update(): Unit
    }
  }

  private val batchSize = Storage.batchSqlSize * 3

  private val table = Storage.table("anchor")
}
