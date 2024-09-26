package wiki.db

import scalikejdbc.*

object TransclusionStorage {

  /**
    * These counts give the number of times each named transclusion appears as
    * the last transclusion in a page. The accumulated statistics can help to
    * configure the disambiguationPrefixes for a new language in languages.json
    *
    * @param input A map of transclusion names to counts
    */
  def writeLastTransclusionCounts(input: Map[String, Int]): Unit = {
    val batches = input.toSeq.sorted.grouped(1000)
    DB.autoCommit { implicit session =>
      batches.foreach { batch =>
        val params: Seq[Seq[SQLSyntax]] = batch.map(t => Seq(sqls"${t._1}", sqls"${t._2}"))
        val cols: SQLSyntax             = sqls"""name, n"""
        val values: SQLSyntax           = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT OR IGNORE INTO last_transclusion_count ($cols) VALUES $values""".update()
      }
    }
  }
}
