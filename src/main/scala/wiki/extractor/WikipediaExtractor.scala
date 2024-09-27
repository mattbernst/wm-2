package wiki.extractor

import io.airlift.compress.bzip2.BZip2HadoopStreams
import io.airlift.compress.zstd.ZstdInputStream
import wiki.db.*
import wiki.extractor.types.{DANGLING_REDIRECT, PageMarkup, Redirection, SiteInfo, Worker}
import wiki.extractor.util.{Config, DBLogging, Logging}

import java.io.{BufferedInputStream, FileInputStream}
import java.nio.charset.StandardCharsets
import scala.io.{BufferedSource, Source}

object WikipediaExtractor extends Logging {

  def main(args: Array[String]): Unit = {
    DBLogging.initDb(db)
    phase00()
    db.phase.getPhaseState(1) match {
      case Some(COMPLETED) =>
        logger.info("Already completed phase 1")
      case Some(CREATED) =>
        logger.warn("Phase 1 incomplete -- resuming")
        phase01(args)
      case None =>
        phase01(args)
    }
    db.phase.getPhaseState(2) match {
      case Some(COMPLETED) =>
        logger.info("Already completed phase 2")
      case Some(CREATED) =>
        logger.warn("Phase 2 incomplete -- redoing")
        phase02()
      case None =>
        phase02()
    }
    db.phase.getPhaseState(3) match {
      case Some(COMPLETED) =>
        logger.info("Already completed phase 3")
      case Some(CREATED) =>
        logger.warn("Phase 3 incomplete -- redoing")
        phase03()
      case None =>
        phase03()
    }
  }

  // Initialize system tables before running any extraction
  private def phase00(): Unit = {
    db.createTableDefinitions(0)
  }

  /**
    * Extract a Wikipedia dump into structured data (first pass). This phase
    * gets page descriptors, page wikitext, links, excerpts, and pages rendered
    * as plain text.
    *
    * @param args Command line arguments: the path to a Wikipedia dump file
    */
  private def phase01(args: Array[String]): Unit = {
    val phase = 1
    val usage = "Usage: WikipediaExtractor <path-to-xml-dump>"
    if (args.length == 0) {
      System.err.println(usage)
    }
    if (args.length > 1) {
      val pArgs = args.mkString(", ")
      println(s"$usage (Expected one file, got multiple arguments: '$pArgs')")
      sys.exit(1)
    }

    val dumpFilePath = args(0)
    logger.info(s"Starting WikipediaExtractor with language ${Config.props.language.name}, input $dumpFilePath")
    val dumpSource  = getInputSource(dumpFilePath)
    val dumpStrings = dumpSource.getLines()
    val head        = dumpStrings.take(128).toSeq
    val siteInfo    = SiteInfo(head.mkString("\n"))

    val source = new XMLSource(head.iterator ++ dumpStrings)
    db.phase.createPhase(phase, s"Extracting $dumpFilePath pages with language ${Config.props.language.name}")
    db.createTableDefinitions(phase)
    db.createIndexes(phase)
    val completedPages = db.page.getCompletedPageIds()
    if (completedPages.nonEmpty) {
      logger.info(s"Resuming page extraction with ${completedPages.size} pages already completed")
    }
    val processor = new XMLStructuredPageProcessor(
      siteInfo = siteInfo,
      language = Config.props.language,
      completedPages = completedPages
    )

    val workers = assignXMLWorkers(Config.props.nWorkers, processor, source.getFromQueue _)

    source.extractPages()
    dumpSource.close()
    workers.foreach(_.thread.join())
    logger.info(s"Split out ${source.pageCount} pages")
    pageSink.stopWriting()
    pageSink.writerThread.join()
    logger.info(s"Wrote ${pageSink.pageCount} pages to database")
    writeTransclusions(processor.getLastTransclusionCounts())
    db.phase.completePhase(phase)
  }

  /**
    * Resolve title_to_page mapping and index the new table.
    *
    */
  private def phase02(): Unit = {
    val phase = 2
    db.phase.deletePhase(phase)
    db.phase.createPhase(phase, s"Building title_to_page map")
    db.page.clearTitleToPage()
    db.createTableDefinitions(phase)
    val danglingRedirects = storeMappedTitles()
    markDanglingRedirects(danglingRedirects)
    db.createIndexes(phase)
    db.phase.completePhase(phase)
  }

  private def phase03(): Unit = {
    val phase = 3
    db.phase.deletePhase(phase)
    //db.phase.createPhase(phase, s"Building title_to_page map")
    // TODO set up for interrupt-and-redo
    db.createTableDefinitions(phase)
    // Get all pages
    val source    = new PageMarkupSource(db)
    val titleMap  = db.page.readTitleToPage()
    val processor = new PageMarkupLinkProcessor(titleMap)
    val workers   = assignLinkWorkers(Config.props.nWorkers, processor, source.getFromQueue _)
    source.enqueueMarkup()
    workers.foreach(_.thread.join())
    // db.createIndexes(phase)
    //db.phase.completePhase(phase)
  }

  /**
    * Get input to process. This can read a .xml.bz2 dump file as downloaded
    * from Wikipedia, a ZStandard compressed .xml.zst file, or an uncompressed
    * .xml file.
    *
    * When reading directly from a .bz2 file, decompression is the bottleneck.
    * Downstream workers will be mostly idle, most cores on a multicore system
    * will be idle, and the wall clock time to complete will be much higher.
    *
    * Converting the original .bz2 dump to a .zst dump offers reasonable
    * performance when read here, and it consumes much less disk space than
    * fully decompressed XML. The .zst dump file is about 20% larger than
    * a .bz2 file for the same data, but much faster to read. The extraction
    * process takes about 10% longer when using .zst input than uncompressed
    * XML.
    *
    * Run this with a bz2-compressed dump only if disk space is at a dear
    * premium.
    *
    * @param  fileName Name of the Wikipedia dump file on disk
    * @return
    */
  private def getInputSource(fileName: String): BufferedSource = {
    if (fileName.endsWith(".xml")) {
      Source.fromFile(fileName)(StandardCharsets.UTF_8)
    } else if (fileName.endsWith(".bz2")) {
      val bz2   = new BZip2HadoopStreams
      val input = bz2.createInputStream(new BufferedInputStream(new FileInputStream(fileName)))
      logger.warn(s"Reading from compressed bz2 input. This is much slower than uncompressed XML.")
      Source.fromInputStream(input)(StandardCharsets.UTF_8)
    } else if (fileName.endsWith(".zst")) {
      // N.B. the Apache Commons ZstdCompressorInputStream relies on native
      // libraries that are missing by default on macOS, so use the pure-Java
      // Airlift library for decompression here.
      val input = new ZstdInputStream(new BufferedInputStream(new FileInputStream(fileName)))
      logger.info(s"Reading from compressed zst input.")
      Source.fromInputStream(input)(StandardCharsets.UTF_8)
    } else {
      throw new RuntimeException(s"Unknown file extension: $fileName")
    }
  }

  /**
    * Write last-transclusions of above-average size to the database. The
    * accumulated statistics can help to configure the disambiguationPrefixes
    * for a new language in languages.json. Only common names (those with
    * above average counts) are included because less common ones are unlikely
    * to be useful and because writing all the minor names to the db can be
    * time-consuming.
    *
    * @param input A map of transclusion names to counts
    */
  private def writeTransclusions(input: Map[String, Int]): Unit = {
    if (input.nonEmpty) {
      val totalCounts = input.values.map(_.toDouble).sum
      val average     = totalCounts / input.size
      val aboveAverage = input.filter {
        case (_, n) => n > average
      }
      logger.info(s"Started writing ${aboveAverage.size} common last-transclusions to db (out of ${input.size} total)")
      db.transclusion.writeLastTransclusionCounts(aboveAverage)
      logger.info(s"Finished writing ${aboveAverage.size} common last-transclusions to db")
    }
  }

  /**
    * Resolve all title-to-ID mappings (e.g. resolve redirects) and store the
    * flattened data in the title_to_page table.
    *
    * @return Dangling redirects that need their page type updated
    */
  private def storeMappedTitles(): Seq[Redirection] = {
    logger.info(s"Getting TitleFinder data")
    val redirects = db.page.readRedirects()
    logger.info(s"Loaded ${redirects.length} redirects")
    val titlePageMap = db.page.readTitlePageMap()
    logger.info(s"Loaded ${titlePageMap.size} title-to-ID map entries")
    val tf = new TitleFinder(titlePageMap, redirects)
    logger.info(s"Started writing title to page ID mappings to db")
    val fpm = tf.getFlattenedPageMapping()
    db.page.writeTitleToPage(fpm)
    logger.info(s"Finished writing ${fpm.length} title to page ID mappings to db")
    tf.danglingRedirects
  }

  /**
    * Mark all dangling redirect pages that were discovered during
    * redirect resolution. This has to run after createIndexes() or
    * it takes way too long to find the page by ID.
    *
    * @param input Dangling redirect pages that need to be marked
    */
  private def markDanglingRedirects(input: Seq[Redirection]): Unit = {
    input.foreach(r => db.page.updatePageType(r.pageId, DANGLING_REDIRECT))
    logger.info(s"Marked ${input.length} pages as $DANGLING_REDIRECT")
  }

  private val db: Storage =
    new Storage(fileName = Config.props.language.code + "_wiki.db")

  private val pageSink: PageSink = new PageSink(db)

  private def assignXMLWorkers(
    n: Int,
    processor: XMLStructuredPageProcessor,
    source: () => Option[String]
  ): Seq[Worker] = {
    0.until(n).map { id =>
      processor.worker(
        id = id,
        source = source,
        sink = pageSink,
        compressMarkup = Config.props.compressMarkup
      )
    }
  }

  private def assignLinkWorkers(
    n: Int,
    processor: PageMarkupLinkProcessor,
    source: () => Option[PageMarkup]
  ): Seq[Worker] = {
    0.until(n).map { id =>
      processor.worker(id = id, source = source)
    }
  }
}
