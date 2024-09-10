package wiki.extractor

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import wiki.db.{PageWriter, Storage}
import wiki.extractor.types.SiteInfo
import wiki.extractor.util.{Config, Logging}

import java.io.{BufferedInputStream, FileInputStream}
import java.nio.charset.StandardCharsets
import scala.io.{BufferedSource, Source}

object WikipediaExtractor extends Logging {
  def main(args: Array[String]): Unit = {
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
    val dumpSource = getInputSource(dumpFilePath)
    val dumpStrings = dumpSource.getLines()
    val head = dumpStrings.take(128).toSeq
    val siteInfo = SiteInfo(head.mkString("\n"))

    val fragmentProcessor = new FragmentProcessor(
      siteInfo = siteInfo,
      language = Config.props.language
    )
    val splitter = new WikipediaPageSplitter(head.iterator ++ dumpStrings)
    dbStorage.createTableDefinitions()
    val workers = assignWorkers(Config.props.fragmentWorkers, fragmentProcessor, splitter.getFromQueue _)

    splitter.extractPages()
    dumpSource.close()
    workers.foreach(_.thread.join())
    logger.info(s"Split out ${splitter.pageCount} pages")
    writer.stopWriting()
    writer.writerThread.join()
    logger.info(s"Wrote ${writer.pageCount} pages to database")
    writeTransclusions(fragmentProcessor.getLastTransclusionCounts())

    // Following wikipedia-miner, we need to:
    // - Process XML fragments into DumpPage via the pageSummary InitialMapper
    // - Iteratively process DumpPage results via SubsequentMapper until all Unforwarded counts reach 0.
  }


  /**
   * Get input to process, either directly from a .xml.bz2 dump file as downloaded
   * from Wikipedia or from an uncompressed .xml file.
   *
   * When reading directly from a .bz2 file, decompression is the bottleneck.
   * Downstream workers will be mostly idle, most cores on a multicore system
   * will be idle, and the wall clock time to complete will be much higher.
   *
   * Run with a previously decompressed input dump unless disk space is at a
   * dear premium.
   *
   * @param  fileName Name of the Wikipedia dump file on disk
   * @return
   */
  private def getInputSource(fileName: String): BufferedSource = {
    if (fileName.endsWith(".xml")) {
      Source.fromFile(fileName)(StandardCharsets.UTF_8)
    }
    else if (fileName.endsWith(".bz2")) {
      val input = new BZip2CompressorInputStream(new BufferedInputStream(new FileInputStream(fileName)))
      logger.warn(s"Reading from compressed bz2 input. This is much slower than uncompressed XML.")
      Source.fromInputStream(input)
    }
    else {
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
    assert(input.nonEmpty)
    val totalCounts = input.values.map(_.toDouble).sum
    val average = totalCounts / input.size
    val aboveAverage = input.filter {
      case (_, n) => n > average
    }
    logger.info(s"Started writing ${aboveAverage.size} common last-transclusions to db (out of ${input.size} total)")
    dbStorage.writeLastTransclusionCounts(aboveAverage)
    logger.info(s"Started writing ${aboveAverage.size} common last-transclusions to db")
  }

  private val dbStorage: Storage =
    new Storage(fileName = Config.props.language.code + "_wiki.db")

  private val writer: PageWriter =
    new PageWriter(dbStorage)

  private def assignWorkers(n: Int,
                            fragmentProcessor: FragmentProcessor,
                            source: () => Option[String]): Seq[FragmentWorker] = {
    0.until(n).map { id =>
      fragmentProcessor.fragmentWorker(id = id, source = source, writer = writer)
    }
  }
}
