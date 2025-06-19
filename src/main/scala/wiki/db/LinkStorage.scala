package wiki.db

import scalikejdbc.*
import wiki.extractor.types.GroupedLinks

import java.util

case class ResolvedLink(source: Int, destination: Int, anchorText: String)
case class DeadLink(source: Int, destination: String, anchorText: String)

case class PageCount(pageIds: Array[Int], counts: Array[Int]) {
  require(pageIds.length == counts.length)

  def count(pageId: Int): Int = {
    val index = util.Arrays.binarySearch(pageIds, pageId)
    if (index > -1) {
      counts(index)
    } else {
      0
    }
  }

  def grouped(n: Int): Iterator[PageCount] = {
    val groupedPages: Iterator[Array[Int]]  = pageIds.grouped(n)
    val groupedCounts: Iterator[Array[Int]] = counts.grouped(n)

    groupedPages.zip(groupedCounts).map {
      case (pageGroup, countGroup) =>
        PageCount(pageGroup, countGroup)
    }
  }
}

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
      .mapValues(_.map(_._2).toArray.sorted)
      .toMap
  }

  /**
    * Retrieves the count of distinct page sources for each destination page
    * from the database. For example, if only pages 3 and 7 link to page 123,
    * the source_count for page 123 would be 2.
    *
    * @return GroupedCount with destination IDs sorted in ascending order and
    *         their corresponding source counts. The sorted IDs enable efficient
    *         binary search operations on the result.
    */
  def getSourceCountsByDestination(): PageCount = {
    val rows: List[(Int, Int)] = DB.autoCommit { implicit session =>
      sql"""SELECT destination, COUNT(DISTINCT source) AS source_count FROM $table GROUP BY destination"""
        .map(rs => (rs.int("destination"), rs.int("source_count")))
        .list()
    }

    val sortedRows    = rows.sortBy(_._1)
    val (ids, counts) = sortedRows.unzip
    PageCount(ids.toArray, counts.toArray)
  }

  /**
    * Retrieves the count of distinct page destinations for each source page
    * from the database. For example, if page 7 links to pages 123, 456, and 789,
    * the destination_count for page 7 would be 3.
    *
    * @return GroupedCount with source IDs sorted in ascending order and their
    *         corresponding destination counts. The sorted IDs enable efficient
    *         binary search operations on the result.
    */
  def getDestinationCountsBySource(): PageCount = {
    val rows: List[(Int, Int)] = DB.autoCommit { implicit session =>
      sql"""SELECT source, COUNT(DISTINCT destination) AS dest_count FROM $table GROUP BY source"""
        .map(rs => (rs.int("source"), rs.int("dest_count")))
        .list()
    }

    val sortedRows    = rows.sortBy(_._1)
    val (ids, counts) = sortedRows.unzip
    PageCount(ids.toArray, counts.toArray)
  }

  /**
    * Writes page source counts to the link_source_count table.
    *
    * Each page ID in the input represents a destination page, and the
    * corresponding count represents how many source pages link to that
    * destination. These counts are important during calculation of
    * TF-IDF-like terms in the ArticleComparer.
    *
    * @param input PageCount containing destination page IDs and their source
    *              counts
    */
  def writeSourceCountsByDestination(input: PageCount): Unit = {
    val batched = input.grouped(Storage.batchSqlSize)
    DB.autoCommit { implicit session =>
      val cols: SQLSyntax = sqls"""destination, n"""
      batched.foreach { batch =>
        val params: Seq[Seq[SQLSyntax]] = batch.pageIds
          .zip(batch.counts)
          .map {
            case (pageId, count) =>
              Seq(sqls"$pageId", sqls"$count")
          }
          .toSeq
        val values: SQLSyntax = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT INTO link_source_count ($cols) VALUES $values""".update()
      }
    }
  }

  /**
    * Reads back the aggregated data previously stored by
    * writeSourceCountsByDestination and instantiates it back
    * into a PageCount object.
    *
    * @return PageCount containing destination page IDs and their source
    *         counts
    */
  def readSourceCountsByDestination(): PageCount = {
    DB.autoCommit { implicit session =>
      val results = sql"""SELECT destination, n FROM link_source_count ORDER BY destination"""
        .map(rs => (rs.int("destination"), rs.int("n")))
        .list()

      val (pageIds, counts) = results.unzip
      PageCount(pageIds.toArray, counts.toArray)
    }
  }

  /**
    * Writes page destination counts to the link_destination_count table.
    *
    * Each page ID in the input represents a source page, and the
    * corresponding count represents how many destination pages that source
    * links to. These counts are important during calculation of
    * TF-IDF-like terms in the ArticleComparer.
    *
    * @param input PageCount containing source page IDs and their destination
    *              counts
    */
  def writeDestinationCountsBySource(input: PageCount): Unit = {
    val batched = input.grouped(Storage.batchSqlSize)
    DB.autoCommit { implicit session =>
      val cols: SQLSyntax = sqls"""source, n"""
      batched.foreach { batch =>
        val params: Seq[Seq[SQLSyntax]] = batch.pageIds
          .zip(batch.counts)
          .map {
            case (pageId, count) =>
              Seq(sqls"$pageId", sqls"$count")
          }
          .toSeq
        val values: SQLSyntax = sqls.csv(params.map(param => sqls"(${sqls.csv(param *)})") *)
        sql"""INSERT INTO link_destination_count ($cols) VALUES $values""".update()
      }
    }
  }

  /**
    * Reads back the aggregated data previously stored by
    * writeDestinationCountsBySource and instantiates it back
    * into a PageCount object.
    *
    * @return PageCount containing source page IDs and their destination
    *         counts
    */
  def readDestinationCountsBySource(): PageCount = {
    DB.autoCommit { implicit session =>
      val results = sql"""SELECT source, n FROM link_destination_count ORDER BY source"""
        .map(rs => (rs.int("source"), rs.int("n")))
        .list()

      val (pageIds, counts) = results.unzip
      PageCount(pageIds.toArray, counts.toArray)
    }
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
      .mapValues(_.map(_._2).toArray.sorted)
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
