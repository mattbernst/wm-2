package mix.extractor

import mix.extractor.util.{Configuration, Logging}

import java.io.FileInputStream
import javax.xml.parsers.SAXParserFactory

object WikipediaExtractor extends Logging {

  // e.g. sbt "runMain mix.extractor.WikipediaExtractor /Users/mernst/git/mix/wm-data/wm-extract-20160713/input/dump.xml"
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println(s"Usage: WikipediaExtractor <path-to-xml-dump> (${args.toSeq})")
      sys.exit(1)
    }

    val xmlFilePath = args(0)
    logger.info(s"Starting WikipediaExtractor with language ${Configuration.props.language.name}, input $xmlFilePath")

    val factory = SAXParserFactory.newInstance()
    val saxParser = factory.newSAXParser()
    val handler = new WikipediaXMLHandler
    val inputStream = new FileInputStream(xmlFilePath)
    saxParser.parse(inputStream, handler)
    inputStream.close()

    // Following wikipedia-miner, we need to:
    // - Process XML fragments into DumpPage via the pageSummary InitialMapper
    // - Iteratively process DumpPage results via SubsequentMapper until all Unforwarded counts reach 0.
  }
}
