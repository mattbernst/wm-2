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
  def getByDestination(id: Int): Seq[ResolvedLink] =
    getByDestination(Seq(id)).getOrElse(id, Seq())

  /**
    * Get links for multiple destination page IDs, keyed by destination page ID.
    *
    * @param ids IDs of the destination pages involved in a link
    * @return    Destination IDs mapped to matching links
    */
  def getByDestination(ids: Seq[Int]): Map[Int, Seq[ResolvedLink]] = {
    val batches = ids.grouped(Storage.batchSqlSize).toSeq
    val rows = DB.autoCommit { implicit session =>
      batches.flatMap { batch =>
        sql"""SELECT * FROM $table WHERE destination IN ($batch)"""
          .map(toResolvedLink)
          .list()
      }
    }
    rows.groupBy(_.destination)
  }

  /**
    * Retrieves source IDs grouped by destination ID.
    *
    * This function takes a sequence of destination IDs and returns a mapping
    * where each destination ID is associated with an array of all source IDs
    * that connect to it. The query is batched for performance when dealing
    * with large ID sequences.
    *
    * @param ids Sequence of destination IDs to look up
    * @return Map where keys are destination IDs and values are arrays of
    *         source IDs that have connections to those destinations
    */
  def getSourcesByDestination(ids: Seq[Int]): Map[Int, Array[Int]] = {
    val batches = ids.grouped(Storage.batchSqlSize).toSeq
    val rows: Seq[(Int, Int)] = DB.autoCommit { implicit session =>
      batches.flatMap { batch =>
        sql"""SELECT source, destination FROM $table WHERE destination IN ($batch)"""
          .map(rs => (rs.int("destination"), rs.int("source")))
          .list()
      }
    }

    // Group by destination ID and collect source IDs into arrays
    rows
      .groupBy(_._1)
      .view
      .mapValues(_.map(_._2).toArray)
      .toMap
  }

  /**
    * Retrieves destination IDs grouped by source ID.
    *
    * This function takes a sequence of source IDs and returns a mapping where
    * each source ID is associated with an array of all destination IDs that it
    * connects to. The query is batched for performance when dealing with large
    * ID sequences.
    *
    * @param ids Sequence of source IDs to look up
    * @return Map where keys are source IDs and values are arrays of
    *         destination IDs that those sources connect to
    */
  def getDestinationsBySource(ids: Seq[Int]): Map[Int, Array[Int]] = {
    val batches = ids.grouped(Storage.batchSqlSize).toSeq
    val rows: Seq[(Int, Int)] = DB.autoCommit { implicit session =>
      batches.flatMap { batch =>
        sql"""SELECT source, destination FROM $table WHERE source IN ($batch)"""
          .map(rs => (rs.int("source"), rs.int("destination")))
          .list()
      }
    }

    // Group by source ID and collect destination IDs into arrays
    rows
      .groupBy(_._1)
      .view
      .mapValues(_.map(_._2).toArray)
      .toMap
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
