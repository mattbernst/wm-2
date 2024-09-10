package wiki.db

import scalikejdbc.*
import wiki.extractor.types.*
import wiki.extractor.util.{FileHelpers, Logging}

/**
 * A SQLite database storage writer and reader for representing and mining
 * extracted Wikipedia data.
 *
 * @param fileName The name of the on-disk file containing the SQLite db
 */
class Storage(fileName: String) extends Logging {
  ConnectionPool.singleton(url = s"jdbc:sqlite:$fileName", user = null, password = null)

  /**
   * These counts give the number of times each named transclusion appears as
   * the last transclusion in a page. The accumulated statistics can help to
   * configure the disambiguationPrefixes for a new language in languages.json
   *
   * @param input A map of transclusion names to counts
   */
  def writeLastTransclusionCounts(input: Map[String, Int]): Unit = {
    val batches = input.toSeq.sorted.grouped(batchInsertSize)
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

  /**
   * Write a namespace to the namespace table. This table provides a permanent
   * record of the namespaces encountered in the input Wikipedia dump at
   * extraction time.
   *
   * @param input A Namespace to persist
   */
  def writeNamespace(input: Namespace): Unit = {
    DB.autoCommit { implicit session =>
      sql"""INSERT INTO namespace
           (id, casing, name) VALUES (${input.id}, ${input.casing}, ${input.name})"""
        .update() : Unit
    }
  }

  /**
   * Read a namespace from the namespace table. A namespace may exist in the
   * Wikipedia dump file but be absent from the namespace table if it is not
   * one of the valid namespaces extracted by fragmentToPage in
   * FragmentProcessor.scala
   *
   * @param id The numeric ID of the namespace to read
   * @return   A namespace, if found in the table
   */
  def readNamespace(id: Int): Option[Namespace] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT * FROM namespace WHERE id=$id"""
        .map { rs =>
          val casing: Casing = rs.string("casing") match {
            case "FIRST_LETTER" => FIRST_LETTER
            case "CASE_SENSITIVE" => CASE_SENSITIVE
          }
          Namespace(id = rs.int("id"), casing = casing, name = rs.string("name"))
        }
        .single()
    }
  }

  /**
   * Write dump pages to the page table. The page table contains all the DumpPage
   * data except the raw markup.
   *
   * @param input One or more DumpPages to write
   */
  def writeDumpPages(input: Seq[DumpPage]): Unit = {
    val batches = input.grouped(batchInsertSize)
    DB.autoCommit { implicit session =>
      batches.foreach { batch =>
        val cols: SQLSyntax = sqls"""id, namespace_id, page_type, last_edited, title, redirect_target"""
        val params: Seq[Seq[SQLSyntax]] = batch.map(
            t => Seq(
              sqls"${t.id}",
              sqls"${t.namespace.id}",
              sqls"${PageTypes.bySymbol(t.pageType)}",
              sqls"${t.lastEdited}",
              sqls"${t.title}",
              sqls"${t.redirectTarget}"
            )
          )
        val values: SQLSyntax = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT INTO page ($cols) VALUES $values"""
          .update()
      }
    }
  }

  /**
   * Write page markup to the page_markup table. The page_markup table only
   * contains the raw markup for each page. The markup is stored in a separate
   * table because it is so much larger than the other page data.
   *
   * @param input One or more DumpPages to write
   */
  def writeMarkups(input: Seq[DumpPage]): Unit = {
    val batches = input.grouped(batchInsertSize)
    DB.autoCommit { implicit session =>
      batches.foreach { batch =>
        val cols: SQLSyntax = sqls"""page_id, markup"""
        val params: Seq[Seq[SQLSyntax]] = batch.map(
          t => Seq(
            sqls"${t.id}",
            sqls"${t.text}"
          )
        )
        val values: SQLSyntax = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT INTO page_markup ($cols) VALUES $values"""
          .update()
      }
    }
  }

  /**
   *  Create all tables from the .sql files in sql/tables/
   */
  def createTableDefinitions(): Unit = {
    val pattern = "sql/tables/*.sql"
    val tableDefinitions = FileHelpers.glob("sql/tables/*.sql").sorted
    require(tableDefinitions.nonEmpty, s"No SQL files for tables found in $pattern")
    DB.localTx { implicit session =>
      tableDefinitions.foreach { fileName =>
        val sql = FileHelpers.readTextFile(fileName)
        logger.info(s"Creating table from $fileName")
        Storage.execute(sql)
      }
    }
  }

  private val batchInsertSize = 1000
}

object Storage {
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
