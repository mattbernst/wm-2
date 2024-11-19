package wiki.db

import scalikejdbc.*
import wiki.extractor.types.AnchorCounter

object AnchorStorage {

  /**
    * Write all the anchors and counts from the counter into the anchor table.
    *
    * @param counter AnchorCounter containing accumulated counts
    */
  def write(counter: AnchorCounter): Unit = {
    DB.autoCommit { implicit session =>
      val batches         = counter.getEntries().grouped(Storage.batchSqlSize)
      val cols: SQLSyntax = sqls"""label, occurrence_count, occurrence_doc_count, link_count, link_doc_count"""
      batches.foreach { batch =>
        val params: Seq[Seq[SQLSyntax]] = batch.map(
          t =>
            Seq(
              sqls"${t._1}",
              sqls"${t._2(AnchorCounter.occurrenceCountIndex)}",
              sqls"${t._2(AnchorCounter.occurrenceDocCountIndex)}",
              sqls"${t._2(AnchorCounter.linkOccurrenceCountIndex)}",
              sqls"${t._2(AnchorCounter.linkOccurrenceDocCountIndex)}"
            )
        )
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
    val ac = new AnchorCounter
    DB.autoCommit { implicit session =>
      sql"""SELECT * FROM $table""".foreach { rs =>
        val label = rs.string("label")
        val counts = Array(
          rs.int("occurrence_count"),
          rs.int("occurrence_doc_count"),
          rs.int("link_count"),
          rs.int("link_doc_count")
        )
        ac.insert(label, counts)
      }
    }

    ac
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

  private val table = Storage.table("anchor")
}
