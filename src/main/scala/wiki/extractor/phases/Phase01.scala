package wiki.extractor.phases

import io.airlift.compress.bzip2.BZip2HadoopStreams
import io.airlift.compress.zstd.ZstdInputStream
import wiki.db.{PageSink, Storage}
import wiki.extractor.types.{SiteInfo, Worker}
import wiki.extractor.util.DBLogging
import wiki.extractor.{XMLSource, XMLStructuredPageProcessor}
import wiki.util.{Config, Logging}

import java.io.{BufferedInputStream, FileInputStream}
import java.nio.charset.StandardCharsets
import scala.io.{BufferedSource, Source}

class Phase01(db: Storage) extends Phase(db: Storage) with Logging {

  /**
    * Extract a Wikipedia dump into structured data (first pass). This phase
    * gets page descriptors, page wikitext, links, excerpts, and pages rendered
    * as plain text.
    *
    * @param args Command line arguments: the path to a Wikipedia dump file
    */
  override def run(args: Array[String]): Unit = {
    val usage = "Usage: WikipediaExtractor <path-to-xml-dump>"
    if (args.length == 0) {
      System.err.println(usage)
    }
    if (args.length > 1) {
      val pArgs = args.mkString(", ")
      logger.error(s"$usage (Expected one file, got multiple arguments: '$pArgs')")
      sys.exit(1)
    }

    val dumpFilePath = args(0)
    logger.info(s"Starting WikipediaExtractor with language ${props.language.name}, input $dumpFilePath")
    val dumpSource  = getInputSource(dumpFilePath)
    val dumpStrings = dumpSource.getLines()
    val head        = dumpStrings.take(128).toSeq
    val siteInfo    = SiteInfo(head.mkString("\n"))

    val source = new XMLSource(head.iterator ++ dumpStrings)
    db.phase.createPhase(number, s"Extracting $dumpFilePath pages with language ${props.language.name}")
    db.createTableDefinitions(number)
    db.createIndexes(number)
    val completedPages = db.page.getCompletedPageIds()
    if (completedPages.nonEmpty) {
      logger.info(s"Resuming page extraction with ${completedPages.size} pages already completed")
    }
    val processor = new XMLStructuredPageProcessor(
      siteInfo = siteInfo,
      language = props.language,
      db = db,
      completedPages = completedPages
    )

    val sink: PageSink = new PageSink(db)
    val workers        = assignXMLWorkers(props.nWorkers, processor, source.getFromQueue _, sink)

    source.extractPages()
    dumpSource.close()
    workers.foreach(_.thread.join())
    DBLogging.info(s"Split out ${source.pageCount} pages")
    sink.stopWriting()
    sink.writerThread.join()
    DBLogging.info(s"Wrote ${sink.pageCount} pages to database")
    writeTransclusions(processor.getLastTransclusionCounts())
    db.phase.completePhase(number)
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
      DBLogging.warn(s"Reading from compressed bz2 input. This is slow.")
      Source.fromInputStream(input)(StandardCharsets.UTF_8)
    } else if (fileName.endsWith(".zst")) {
      val input = new ZstdInputStream(new BufferedInputStream(new FileInputStream(fileName)))
      DBLogging.info(s"Reading from compressed zst input.")
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
      DBLogging.info(
        s"Started writing ${aboveAverage.size} common last-transclusions to db (out of ${input.size} total)"
      )
      db.transclusion.writeLastTransclusionCounts(aboveAverage)
      DBLogging.info(s"Finished writing ${aboveAverage.size} common last-transclusions to db")
    }
  }

  private def assignXMLWorkers(
    n: Int,
    processor: XMLStructuredPageProcessor,
    source: () => Option[String],
    sink: PageSink
  ): Seq[Worker] = {
    0.until(n).map { id =>
      processor.worker(
        id = id,
        source = source,
        sink = sink,
        compressMarkup = props.compressMarkup
      )
    }
  }

  private lazy val props                 = Config.props
  override def number: Int               = 1
  override val incompleteMessage: String = s"Phase $number incomplete -- resuming"
}
