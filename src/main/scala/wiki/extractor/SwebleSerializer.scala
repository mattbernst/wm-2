package wiki.extractor

import org.sweble.wikitext.example.{SerializationMethod, Serializer}
import wiki.extractor.util.Logging

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import scala.util.{Failure, Success, Try}

object SwebleSerializer extends Logging {

  /**
    * Parse wikitext markup for an article using Sweble and store its serialized
    * JSON representation. See https://github.com/sweble/sweble-wikitext
    *
    * TODO: replace the swc-example-serialization with a bit more direct use of
    * the Sweble parser here (could e.g. replace GSON, set only needed options).
    * This code is currently based on SerializationIntegrationTest.java from
    * swc-example-serialization.
    *
    * @param title  The title of the article for the markup being processed
    * @param markup Wikitext markup
    * @return       Parsed JSON if Sweble could handle it, otherwise None
    */
  def serializeAsJson(title: String, markup: String): Option[String] = {
    val markupStream = new ByteArrayInputStream(markup.getBytes(StandardCharsets.UTF_8))
    val ss           = new Serializer(markupStream, title, StandardCharsets.UTF_8.toString)

    ss.setParserAutoCorrectEnabled(false)
    ss.setParserWarningsEnabled(true)
    ss.setParserRtdEnabled(true)

    // Postprocessing options
    ss.setPpSimplifyAst(true)
    ss.setPpStripLocations(false)
    ss.setPpStripAllAttributes(false)
    ss.setPpStripRtdAttributes(false)
    ss.setQuiet(true)

    Try(ss.serializeTo(SerializationMethod.JSON)) match {
      case Success(jsonBytes) =>
        Some(new String(jsonBytes, StandardCharsets.UTF_8))
      case Failure(_) =>
        logger.warn(s"""Could not process "$title" wikitext markup to JSON""")
        None
    }
  }
}
