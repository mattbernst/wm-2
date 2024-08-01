package mix.extractor

import java.io.FileInputStream
import javax.xml.parsers.SAXParserFactory

object WikipediaExtractor {

  // e.g. sbt "runMain mix.extractor.WikipediaExtractor /Users/mernst/git/mix/wm-data/wm-extract-20160713/input/dump.xml"
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println(s"Usage: WikipediaExtractor <path-to-xml-dump> (${args.toList})")
      sys.exit(1)
    }

    val xmlFilePath = args(0)

    val factory = SAXParserFactory.newInstance()
    val saxParser = factory.newSAXParser()
    val handler = new WikipediaXMLHandler
    val inputStream = new FileInputStream(xmlFilePath)
    saxParser.parse(inputStream, handler)
    inputStream.close()
  }
}
