package wiki.extractor

import org.sweble.wikitext.example.{SerializationMethod, Serializer}
import org.sweble.wikitext.parser.nodes.*
import org.sweble.wikitext.parser.utils.NonExpandingParser
import wiki.extractor.util.Logging

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

// Links as extracted here match the page text exactly.
// Properly casing the target and stripping sub-page sections happens later.
case class Link(target: String, anchorText: Option[String])
case class ParseResult(firstSentence: String,
                       firstParagraph: String,
                       text: String,
                       links: Seq[Link])

object WikitextParser extends Logging {

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
    Try {
      val ss = new Serializer(markupStream, title, StandardCharsets.UTF_8.toString)

      ss.setParserAutoCorrectEnabled(false)
      ss.setParserWarningsEnabled(true)
      ss.setParserRtdEnabled(true)

      // Postprocessing options
      ss.setPpSimplifyAst(true)
      ss.setPpStripLocations(false)
      ss.setPpStripAllAttributes(false)
      ss.setPpStripRtdAttributes(false)
      ss.setQuiet(true)
      ss.serializeTo(SerializationMethod.JSON)
    } match {
      case Success(jsonBytes) =>
        Some(new String(jsonBytes, StandardCharsets.UTF_8))
      case Failure(ex) =>
        logger.warn(s"""Could not process "$title" wikitext markup to JSON: ${ex.getClass.getSimpleName}""")
        None
    }
  }

  def parse(title: String, markup: String): List[WtNode] = {
    val parser = new NonExpandingParser()
    val parsed = parser.parseArticle(markup, title)
    parsed.iterator().asScala.toList
  }


  def processNodes(input: List[WtNode]): ParseResult = {
    val links = extractNodes[WtInternalLink](input).map { link =>
      if (link.hasTitle) {
        Link(target = textualize(link.getTarget), Some(textualize(link.getTitle)))
      }
      else {
        Link(target = textualize(link.getTarget), anchorText = None)
      }
    }
    val text = nodesToText(input)
    // TODO: use OpenNLP sentence detection.
    // First paragraph is first WtParagraph that renders to multiple sentences.
    // First sentence is the first sentence of the first paragraph as defined above.
    // If no multi-sentence paragraphs found, first sentence and first paragraph alike are
    // the first detected sentence (may be empty).
    val firstParagraph = "???"
    val firstSentence = "???"
    ParseResult(
      firstSentence = firstSentence,
      firstParagraph = firstParagraph,
      text = text,
      links = links
    )
  }

  def nodesToText(input: List[WtNode]): String =
    input.map(textualize).mkString

  def textualize(wtNode: WtNode): String = wtNode match {
    case node: WtText => node.getContent
    case node: WtInternalLink if !node.hasTitle => textualize(node.getTarget)
    case node: WtInternalLink if node.hasTitle => textualize(node.getTitle)

    // All of these add noise to the text version of the page. Eliminate
    // textualized image links, template noise, and WtTagExtensions
    // (like citations)
    case _: WtImageLink => ""
    case _: WtTemplate => ""
    case _: WtTagExtension => ""
    case _: WtTable => ??? // Do something with header etc to avoid "wikitext" injection

    case other: WtNode => other.iterator().asScala.map(textualize).mkString
  }

  def extractNodes[T <: WtNode : ClassTag](nodes: List[WtNode]): List[T] = {
    def collectNodes(node: WtNode): List[T] = node match {
      case n: T => List(n)
      case other: WtNode => other.iterator().asScala.toList.flatMap(collectNodes)
      case _ => List.empty
    }

    nodes.flatMap(collectNodes)
  }

  private[extractor] def cleanSimpleText(input: String): String = {
    removeDuplicateSpaces(input)
  }

  @tailrec
  private def removeDuplicateSpaces(input: String): String = {
    val replaced = input.replace("  ", " ")
    if (!replaced.contains("  ")) {
      replaced
    }
    else {
      removeDuplicateSpaces(replaced)
    }
  }
}
