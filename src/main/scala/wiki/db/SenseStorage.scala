package wiki.db

import scalikejdbc.*
import wiki.extractor.types.Sense

object SenseStorage {

  /**
    * Write senses to the sense table.
    *
    * @param input Label senses to write
    */
  def write(input: Seq[Sense]): Unit = {
    if (input.nonEmpty) {
      //val batches = input.grouped(Storage.batchSqlSize)
      val batches = input.grouped(1)
      DB.autoCommit { implicit session =>
        val cols: SQLSyntax = sqls"""label_id, destination, n"""
        batches.foreach { batch =>
          val params: Seq[Seq[SQLSyntax]] = batch.flatMap { sense =>
            sense.destinationCounts.map {
              case (destination, count) =>
                Seq(
                  sqls"${sense.labelId}",
                  sqls"$destination",
                  sqls"$count"
                )
            }
          }
          val values: SQLSyntax = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
          sql"""INSERT OR IGNORE INTO $table ($cols) VALUES $values""".update()
        }
      }
    }
  }

  /**
    * Read label senses from the sense table, if the label referenced by ID
    * has senses stored. The senses are the particular destination pages
    * associated with the label. For example, the senses for "Mercury"
    * (label ID 488460) include:
    *
    * 19694: Mercury (planet)
    * 19726: Mercury (programming language)
    * 37417: Mercury (mythology)
    *
    * Some senses are more common than others. The sense table counts how many
    * times the label is used in association with each of its senses.
    *
    * @param labelId The numeric ID of the label
    * @return Senses for the label, if found in the table
    */
  def read(labelId: Int): Option[Sense] = {
    val destinationCounts = DB.autoCommit { implicit session =>
      sql"""SELECT destination, n FROM $table WHERE label_id=$labelId""".map { r =>
        (r.int("destination"), r.int("n"))
      }.list()
        .toMap
    }
    if (destinationCounts.isEmpty) {
      None
    } else {
      Some(Sense(labelId = labelId, destinationCounts = destinationCounts))
    }
  }

  private val table = Storage.table("sense")
}
