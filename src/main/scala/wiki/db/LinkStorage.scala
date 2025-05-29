package wiki.db

import scalikejdbc.*
import wiki.extractor.types.GroupedLinks

case class ResolvedLink(source: Int, destination: Int, anchorText: String)
case class DeadLink(source: Int, destination: String, anchorText: String)

object LinkStorage {

  /**
    * Write resolved links to the link table with batching.
    *
    * @param links Cross-page links
    */
  def writeResolved(links: Seq[ResolvedLink]): Unit = {
    val batched = links.grouped(Storage.batchSqlSize)
    DB.autoCommit { implicit session =>
      val cols: SQLSyntax = sqls"""source, destination, anchor_text"""
      batched.foreach { batch =>
        val params: Seq[Seq[SQLSyntax]] = batch.map(
          t =>
            Seq(
              sqls"${t.source}",
              sqls"${t.destination}",
              sqls"${t.anchorText}"
            )
        )
        val values: SQLSyntax = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT INTO $table ($cols) VALUES $values""".update()
      }
    }
  }

  /**
    * Write dead links to the dead_link table with batching.
    *
    * @param links Cross-page links
    */
  def writeDead(links: Seq[DeadLink]): Unit = {
    val batched = links.grouped(Storage.batchSqlSize)
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
    val batches = ids.grouped(Storage.batchSqlSize).toSeq
    val rows = DB.autoCommit { implicit session =>
      batches.flatMap { batch =>
        sql"""SELECT * FROM $table WHERE source IN ($batch)"""
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
      sql"""SELECT * FROM $table WHERE destination=$id"""
        .map(toResolvedLink)
        .list()
    }
  }

  /**
    * Get labels along with their per-destination counts. These per-destination
    * counts show the relative likelihood of labels used in different semantic
    * senses.
    *
    * The data is returned as a structure of arrays to minimize the memory
    * overhead.
    *
    * @return Grouped links containing parallel arrays of labels, destinations,
    *         and counts
    */
  def getGroupedLinks(): GroupedLinks = {
    val n            = countGroupedLinks()
    val labels       = Array.ofDim[String](n)
    val destinations = Array.ofDim[Int](n)
    val counts       = Array.ofDim[Int](n)
    var j            = 0
    DB.autoCommit { implicit session =>
      sql"""SELECT anchor_text, destination, count(*)
            FROM $table
            GROUP BY anchor_text, destination
            ORDER BY anchor_text""".foreach { rs =>
        labels(j) = rs.string("anchor_text")
        destinations(j) = rs.int("destination")
        counts(j) = rs.int("count(*)")
        j += 1
      }
    }

    GroupedLinks(size = n, labels = labels, destinations = destinations, counts = counts)
  }

  private def countGroupedLinks(): Int = {
    DB.autoCommit { implicit session =>
      sql"""WITH grouped AS
            (SELECT anchor_text, destination, count(*) FROM $table GROUP BY anchor_text, destination)
            SELECT count(*) FROM grouped"""
        .map(rs => rs.int("count(*)"))
        .single()
        .getOrElse(0)
    }
  }

  private def toResolvedLink(rs: WrappedResultSet): ResolvedLink =
    ResolvedLink(
      source = rs.int("source"),
      destination = rs.int("destination"),
      anchorText = rs.string("anchor_text")
    )

  private val table = Storage.table("link")
}
