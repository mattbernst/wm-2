package wiki.db

import scalikejdbc.*

case class IDLink(source: Int, destination: Int, anchorText: Option[String])

object LinkStorage {
  /**
    * Write links to the link table with batching.
    *
    * @param links Cross-page links
    */
  def write(links: Seq[IDLink]): Unit = {
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
    * Get links by source page ID.
    *
    * @param id ID of the source page involved in a link
    * @return   All matching links
    */
  def getBySource(id: Int): Seq[IDLink] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT * FROM link WHERE source=$id"""
        .map(toLink)
        .list()
    }
  }

  /**
    * Get links by destination page ID.
    *
    * @param id ID of the destination page involved in a link
    * @return   All matching links
    */
  def getByDestination(id: Int): Seq[IDLink] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT * FROM link WHERE destination=$id"""
        .map(toLink)
        .list()
    }
  }

  private def toLink(rs: WrappedResultSet): IDLink =
    IDLink(
      source = rs.int("source"),
      destination = rs.int("destination"),
      anchorText = rs.stringOpt("anchor_text")
    )
}
