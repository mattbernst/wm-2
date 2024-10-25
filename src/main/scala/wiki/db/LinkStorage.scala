package wiki.db

import scalikejdbc.*

case class ResolvedLink(source: Int, destination: Int, anchorText: Option[String])
case class DeadLink(source: Int, destination: String, anchorText: Option[String])

object LinkStorage {

  /**
    * Write resolved links to the link table with batching.
    *
    * @param links Cross-page links
    */
  def writeResolved(links: Seq[ResolvedLink]): Unit = {
    val batchSize = 10000
    val batched   = links.grouped(batchSize)
    DB.autoCommit { implicit session =>
      batched.foreach { batch =>
        val cols: SQLSyntax = sqls"""source, destination, anchor_text"""
        val params: Seq[Seq[SQLSyntax]] = batch.map(
          t =>
            Seq(
              sqls"${t.source}",
              sqls"${t.destination}",
              sqls"${t.anchorText}"
            )
        )
        val values: SQLSyntax = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT INTO link ($cols) VALUES $values""".update()
      }
    }
  }

  /**
    * Write dead links to the dead_link table with batching.
    *
    * @param links Cross-page links
    */
  def writeDead(links: Seq[DeadLink]): Unit = {
    val batchSize = 1000
    val batched   = links.grouped(batchSize)
    DB.autoCommit { implicit session =>
      batched.foreach { batch =>
        val cols: SQLSyntax = sqls"""source, destination, anchor_text"""
        val params: Seq[Seq[SQLSyntax]] = batch.map(
          t =>
            Seq(
              sqls"${t.source}",
              sqls"${t.destination}",
              sqls"${t.anchorText}"
            )
        )
        val values: SQLSyntax = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT INTO dead_link ($cols) VALUES $values""".update()
      }
    }
  }

  /**
    * Get links by source page ID.
    *
    * @param id ID of the source page involved in a link
    * @return   All matching links
    */
  def getBySource(id: Int): Seq[ResolvedLink] =
    getBySource(Seq(id)).getOrElse(id, Seq())

  /**
    * Get links for multiple source page IDs, keyed by source page ID.
    *
    * @param ids IDs of the source pages involved in a link
    * @return    Source IDs mapped to matching links
    */
  def getBySource(ids: Seq[Int]): Map[Int, Seq[ResolvedLink]] = {
    val batches = ids.grouped(1000).toSeq
    val rows = DB.autoCommit { implicit session =>
      batches.flatMap { batch =>
        sql"""SELECT * FROM link WHERE source IN ($batch)"""
          .map(toResolvedLink)
          .list()
      }
    }
    rows.groupBy(_.source)
  }

  /**
    * Get links by destination page ID.
    *
    * @param id ID of the destination page involved in a link
    * @return   All matching links
    */
  def getByDestination(id: Int): Seq[ResolvedLink] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT * FROM link WHERE destination=$id"""
        .map(toResolvedLink)
        .list()
    }
  }

  private def toResolvedLink(rs: WrappedResultSet): ResolvedLink =
    ResolvedLink(
      source = rs.int("source"),
      destination = rs.int("destination"),
      anchorText = rs.stringOpt("anchor_text")
    )
}
