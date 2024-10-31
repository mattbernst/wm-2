package wiki.db

import scalikejdbc.*

case class PageDepth(pageId: Int, n: Int, route: Seq[Int])

object DepthStorage {

  /**
    * Write page depths to the depth table. The depth n is the distance to
    * the root page. The route is the sequence of pages connecting the page
    * to the root page.
    *
    * @param input Page depths to write
    */
  def write(input: Seq[PageDepth]): Unit = {
    val batches = input.grouped(Storage.batchSqlSize)
    DB.autoCommit { implicit session =>
      val cols: SQLSyntax = sqls"""page_id, n, route"""
      batches.foreach { batch =>
        val params: Seq[Seq[SQLSyntax]] = batch.map { t =>
          val serialized = upickle.default.write(t.route)
          Seq(
            sqls"${t.pageId}",
            sqls"${t.n}",
            sqls"$serialized"
          )
        }
        val values: SQLSyntax = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT OR IGNORE INTO $table ($cols) VALUES $values""".update()
      }
    }
  }

  /**
    * Read a page depth from the depth table, if one is stored. Depending on
    * the wiki dump and the configuration, not all pages may be depth-mapped.
    *
    * @param pageId The numeric ID of the page
    * @return A depth record, if found in the table
    */
  def read(pageId: Int): Option[PageDepth] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT * FROM $table WHERE page_id=$pageId""".map { r =>
        val route = upickle.default.read[Seq[Int]](r.string("route"))
        PageDepth(pageId = r.int("page_id"), n = r.int("n"), route = route)
      }.single()
    }
  }

  /**
    * Count number of pages at depth n
    *
    * @param n The depth at which to count marked pages
    * @return The number of marked pages at depth n from the root
    */
  def count(n: Int): Int = {
    DB.autoCommit { implicit session =>
      sql"""SELECT count(*) AS ct FROM $table WHERE n=$n"""
        .map(r => r.int("ct"))
        .single()
        .getOrElse(0)
    }
  }

  private val table = Storage.table("depth")
}
