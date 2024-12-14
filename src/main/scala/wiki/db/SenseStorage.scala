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
      val batches = input.grouped(Storage.batchSqlSize)
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
    * associated with the label.
    *
    * Some senses are more common than others. The sense table counts how many
    * times the label is used in association with each of its senses.
    *
    * For example, the senses for "mercury" and "Mercury" include:
    *
    * sqlite> SELECT label.label, page.title, page.id, n FROM label, page, sense
    * WHERE label_id=label.id AND page.id=destination AND label.label LIKE 'mercury'
    * ORDER BY n;
    *
    * label    title                                            id        n
    * -------  -----------------------------------------------  --------  ----
    * Mercury  BSA Mercury Air Rifle                            33325935  1
    * mercury  Mercury hydride                                  37012116  1
    * Mercury  USS Mercury (ID-3012)                            12812537  2
    * Mercury  Mercury Club                                     23616501  2
    * Mercury  Norton Mercury                                   68128798  3
    * Mercury  Mercury (Duquesnoy)                              65344995  3
    * Mercury  Mercury (RemObjects BASIC programming language)  70564140  3
    * Mercury  Russian corvette Merkury                         66990534  4
    * Mercury  Mercury (crystallography)                        65059134  5
    * Mercury  Russian brig Merkurii                            21511891  7
    * Mercury  Pittsburgh Mercury                               43534037  9
    * mercury  Mercury regulation in the United States          31080818  15
    * Mercury  Mercury Park Lane                                1450020   32
    * Mercury  Geology of Mercury                               2090039   47
    * Mercury  Mercury, Nevada                                  1455516   56
    * Mercury  The Mercury (Hobart)                             3596130   229
    * Mercury  Mercury (mythology)                              37417     953
    * Mercury  Mercury (planet)                                 19694     2078
    * mercury  Mercury (element)                                18617142  3082
    *
    * Note that the most common sense for lower-case mercury is the element while
    * that for capitalized Mercury is the planet Mercury. When disambiguating
    * senses, the caller needs to be sure to check for both forms on
    * beginning-of-sentence ngrams.
    *
    * @param labelId The numeric ID of the label
    * @return Senses for the label, if found in the table
    */
  def read(labelId: Int): Option[Sense] = {
    val destinationCounts = DB.autoCommit { implicit session =>
      sql"""SELECT destination, n FROM $table WHERE label_id=$labelId""".map { r =>
        (r.int("destination"), r.int("n"))
      }.list().toMap
    }
    if (destinationCounts.isEmpty) {
      None
    } else {
      Some(Sense(labelId = labelId, destinationCounts = destinationCounts))
    }
  }

  private val table = Storage.table("sense")
}
