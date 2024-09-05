package wiki.extractor

import upickle.default.*
import wiki.extractor.types.SiteInfo
import wiki.extractor.util.{Config, Logging}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.io.Source

object WikipediaExtractor extends Logging {

  // e.g. sbt "runMain wiki.extractor.WikipediaExtractor /Users/mernst/git/mix/wm-data/wm-extract-20160713/input/dump.xml"
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println(s"Usage: WikipediaExtractor <path-to-xml-dump> (${args.toSeq})")
      sys.exit(1)
    }

    val xmlFilePath = args(0)
    logger.info(s"Starting WikipediaExtractor with language ${Config.props.language.name}, input $xmlFilePath")
    val dumpSource = Source.fromFile(xmlFilePath)(StandardCharsets.UTF_8)
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
