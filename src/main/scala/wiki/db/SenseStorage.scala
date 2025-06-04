package wiki.db

import scalikejdbc.*
import wiki.extractor.types.Sense

import scala.collection.mutable

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
            sense.senseCounts.map {
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
    * WHERE n > 15 AND label_id=label.id AND page.id=destination AND label.label LIKE 'mercury'
    * ORDER BY n;
    *
    * label    title                                            id        n
    * -------  -----------------------------------------------  --------  ----
    * Mercury  Mercury (Marvel Comics)                          1780343   31
    * Mercury  Mercury Park Lane                                1450020   32
    * Mercury  Mercury (programming language)                   19726     33
    * Mercury  Mercury (TV series)                              19456320  38
    * mercury  Mercury poisoning                                344287    40
    * Mercury  Mercury (film)                                   54375444  41
    * Mercury  Geology of Mercury                               2090039   47
    * Mercury  Mercury, Nevada                                  1455516   56
    * Mercury  Planets in astrology                             30872816  69
    * Mercury  The Mercury (Hobart)                             3596130   229
    * Mercury  Project Mercury                                  19812     250
    * Mercury  Mercury (element)                                18617142  300
    * Mercury  Mercury Marine                                   2248401   332
    * Mercury  Mercury (automobile)                             256339    889
    * Mercury  Mercury (mythology)                              37417     953
    * Mercury  Mercury Records                                  325909    1672
    * Mercury  Mercury (planet)                                 19694     2078
    * mercury  Mercury (element)                                18617142  3082
    *
    *
    * Note that the most common sense for lower-case mercury is the element
    * while that for capitalized Mercury is the planet Mercury. But there are
    * also 300 references to the element mercury with the capitalized form,
    * due to beginning-of-sentence instances of usage. When disambiguating
    * senses, the caller needs to be sure to check for both upper and lower
    * case forms on beginning-of-sentence NGrams.
    *
    * @param labelId The numeric ID of the label
    * @return        Senses for the label, if found in the table
    */
  def getSenseByLabelId(labelId: Int): Option[Sense] = {
    val senseCounts = mutable.Map[Int, Int]()
    DB.autoCommit { implicit session =>
      sql"""SELECT destination, n FROM $table WHERE label_id=$labelId""".foreach { r =>
        senseCounts.put(r.int("destination"), r.int("n")): Unit
      }
    }
    if (senseCounts.isEmpty) {
      None
    } else {
      Some(Sense(labelId = labelId, senseCounts = senseCounts))
    }
  }

  private val table = Storage.table("sense")
}
