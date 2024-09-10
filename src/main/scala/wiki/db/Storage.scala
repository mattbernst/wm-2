package wiki.db

import scalikejdbc.*
import wiki.db.types.NamedSql
import wiki.extractor.util.Logging

class Storage(fileName: String) extends Logging {
  ConnectionPool.singleton(url = s"jdbc:sqlite:$fileName", user = null, password = null)

  def writeLastTransclusionCounts(input: Map[String, Int]): Unit = {
    val batches = input.toSeq.grouped(batchInsertSize)
    DB.autoCommit { implicit session =>
      batches.foreach { batch =>
        val params: Seq[Seq[SQLSyntax]] = batch.map(t => Seq(sqls"${t._1}", sqls"${t._2}"))
        val cols: SQLSyntax = sqls"""name, n"""
        val values: SQLSyntax = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT INTO last_transclusion_count ($cols) VALUES $values"""
          .update()
      }
    }
  }

  def createTableDefinitions(): Unit = {
    DB.localTx { implicit session =>
      Storage.tableDefinitions.foreach { ns =>
        logger.info(s"Creating ${ns.name} in $fileName")
        Storage.execute(ns.statement)
      }
    }
  }

  private val batchInsertSize = 1000
}

object Storage {
  val tableDefinitions: Seq[NamedSql] = Seq(
    NamedSql(
      name = "last_transclusion_count",
      statement = """CREATE TABLE IF NOT EXISTS last_transclusion_count
                    |(
                    |    name      TEXT NOT NULL,
                    |    n         INTEGER NOT NULL,
                    |    PRIMARY KEY (name)
                    |);""".stripMargin
    )
  )

  def execute(sqls: String*)(implicit session: DBSession): Unit = {
    @annotation.tailrec
    def loop(xs: List[String], errors: List[Throwable]): Unit = {
      xs match {
        case sql :: t =>
          try {
            SQL(sql).execute.apply(): Unit
          } catch {
            case e: Exception =>
              loop(t, e :: errors)
          }
        case Nil =>
          throw new RuntimeException(
            "Failed to execute sqls :" + sqls + " " + errors
          )
      }
    }
    loop(sqls.toList, Nil)
  }
}
