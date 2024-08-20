package mix.extractor

import mix.extractor.types.SiteInfo
import mix.extractor.util.{Config, Logging}

import java.nio.charset.StandardCharsets
import scala.io.Source

object WikipediaExtractor extends Logging {

  // e.g. sbt "runMain mix.extractor.WikipediaExtractor /Users/mernst/git/mix/wm-data/wm-extract-20160713/input/dump.xml"
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
    val fragmentProcessor = new FragmentProcessor(siteInfo)
    val splitter = new WikipediaPageSplitter(head.iterator ++ dumpStrings)
    val workers = assignWorkers(Config.props.fragmentWorkers, fragmentProcessor, splitter.getFromQueue _)

    splitter.extractPages()
    dumpSource.close()
    logger.info(s"Split out ${splitter.pageCount} pages")
    workers.foreach(_.thread.join())


    // Following wikipedia-miner, we need to:
    // - Process XML fragments into DumpPage via the pageSummary InitialMapper
    // - Iteratively process DumpPage results via SubsequentMapper until all Unforwarded counts reach 0.
  }

  private def assignWorkers(n: Int,
                            fragmentProcessor: FragmentProcessor,
                            source: () => Option[String]): Seq[FragmentWorker] = {
    0.until(n).map { id =>
      fragmentProcessor.fragmentWorker(id = id, source = source)
    }
  }
}
