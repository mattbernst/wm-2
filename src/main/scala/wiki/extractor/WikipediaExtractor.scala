package wiki.extractor

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import upickle.default.*
import wiki.extractor.types.SiteInfo
import wiki.extractor.util.{Config, Logging}

import java.io.{BufferedInputStream, FileInputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
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
      language = Config.props.language,
      countTransclusions = Config.props.countLastTransclusions
    )
    val splitter = new WikipediaPageSplitter(head.iterator ++ dumpStrings)
    val workers = assignWorkers(Config.props.fragmentWorkers, fragmentProcessor, splitter.getFromQueue _)

    splitter.extractPages()
    dumpSource.close()
    logger.info(s"Split out ${splitter.pageCount} pages")
    workers.foreach(_.thread.join())

    if (Config.props.countLastTransclusions) {
      dumpTransclusions(fragmentProcessor.getTransclusionCounts())
    }

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
   * Write counts of every transclusion to a JSON file for later analysis.
   *
   * @param data Names and counts of every transclusion, ordered most-common-first
   */
  private def dumpTransclusions(data: Seq[(String, Int)]): Unit = {
    val outputName = "transclusion_counts.json"
    val serialized = write(data).getBytes(StandardCharsets.UTF_8)
    Files.write(Paths.get(outputName), serialized)
    logger.info(s"Wrote ${data.size} counts to $outputName")
  }

  private def assignWorkers(n: Int,
                            fragmentProcessor: FragmentProcessor,
                            source: () => Option[String]): Seq[FragmentWorker] = {
    0.until(n).map { id =>
      fragmentProcessor.fragmentWorker(id = id, source = source)
    }
  }
}
