package wiki.db

import scalikejdbc.*
import wiki.extractor.types.LabelCounter
import wiki.extractor.util.{DBLogging, Progress}

object LabelStorage {

  /**
    * Write all the labels and counts from the counter into the label table.
    *
    * @param counter LabelCounter containing accumulated counts
    */
  def write(counter: LabelCounter): Unit = {
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
            sqls"${t._2(LabelCounter.occurrenceCountIndex)}",
            sqls"${t._2(LabelCounter.occurrenceDocCountIndex)}",
            sqls"${t._2(LabelCounter.linkOccurrenceCountIndex)}",
            sqls"${t._2(LabelCounter.linkOccurrenceDocCountIndex)}"
          )
        }
        val values: SQLSyntax = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT OR REPLACE INTO $table ($cols) VALUES $values""".update()
      }
    }
  }

  /**
    * Read the entire label table back as a LabelCounter.
    *
    * @return A LabelCounter containing all data from the label table
    */
  def read(): LabelCounter = {
    val counter = new LabelCounter
    var current = 0
    val end: Int = DB.autoCommit { implicit session =>
      sql"""SELECT MAX(id) AS max_id FROM $table"""
        .map(rs => rs.intOpt("max_id"))
        .single()
        .flatten
    }.getOrElse {
      DBLogging.warn("Did not find any IDs in label table")
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
    * Delete contents of label table. This will get called if the link
    * label-counting phase needs to run again (in case data had been partially
    * written).
    */
  def delete(): Unit = {
    DB.autoCommit { implicit session =>
      sql"""DELETE FROM $table"""
        .update(): Unit
    }
  }

  /**
    * Zero out the occurrence_count and occurrence_doc_count statistics.
    * This will get called if the page label-counting phase needs to run
    * again (in case data had been partially written).
    */
  def clearOccurrenceCounts(): Unit = {
    DB.autoCommit { implicit session =>
      sql"""UPDATE $table SET occurrence_count=0, occurrence_doc_count=0"""
        .update(): Unit
    }
  }

  private val batchSize = Storage.batchSqlSize * 3

  private val table = Storage.table("label")
}
