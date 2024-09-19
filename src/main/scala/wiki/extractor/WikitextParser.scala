package wiki.extractor

import org.sweble.wikitext.parser.nodes.*
import org.sweble.wikitext.parser.utils.NonExpandingParser
import wiki.extractor.types.{Link, ParseResult}
import wiki.extractor.util.Logging

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object WikitextParser extends Logging {

  /**
    * Parse wikitext markup for an article using Sweble and retain selected
    * features. See https://github.com/sweble/sweble-wikitext
    *
    * The retained features currently include:
    * - A minimalist plain-text rendition of the page content
    * - The first paragraph of that plain text content
    * - The first sentence of that plain text content
    * - Internal links to other wiki pages
    *
    * @param title  The title of the article for the markup being processed
    * @param markup Wikitext markup
    * @return       Parsed result if Sweble could handle it, otherwise None
    */
  def parseMarkup(title: String, markup: String): Option[ParseResult] = {
    Try {
      parse(title, markup)
    } match {
      case Success(nodes) =>
        Some(processNodes(nodes))
      case Failure(ex) =>
        logger.warn(s"""Could not parse "$title" wikitext markup: ${ex.getClass.getSimpleName}""")
        None
    }
  }

  private[extractor] def parse(title: String, markup: String): Array[WtNode] = {
    val parser = new NonExpandingParser(
      true,  // warningsEnabled
      false, // gather round trip data
      false  // autoCorrect
    )

    parser.parseArticle(markup, title).iterator().asScala.toArray
  }

  private[extractor] def processNodes(input: Array[WtNode]): ParseResult = {
    val links = extractNodes[WtInternalLink](input).map { link =>
      if (link.hasTitle) {
        Link(target = textualize(link.getTarget), Some(textualize(link.getTitle)))
      } else {
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
    val firstSentence  = "???"
    ParseResult(
      firstSentence = firstSentence,
      firstParagraph = firstParagraph,
      text = cleanSimpleText(text),
      links = links.toSeq
    )
  }

  private[extractor] def nodesToText(input: Array[WtNode]): String =
    input.map(textualize).mkString

  private[extractor] def textualize(wtNode: WtNode): String = wtNode match {
    case node: WtText                           => node.getContent
    case node: WtInternalLink if !node.hasTitle => textualize(node.getTarget)
    case node: WtInternalLink if node.hasTitle  => textualize(node.getTitle)

    // All of these add noise to the text version of the page. Eliminate
    // textualized image links, template noise, XML attributes, and
    // WtTagExtensions (like citations).
    case _: WtImageLink     => ""
    case _: WtTemplate      => ""
    case _: WtXmlAttributes => ""
    case _: WtTagExtension  => ""

    case other: WtNode => other.iterator().asScala.map(textualize).mkString
  }

  private[extractor] def extractNodes[T <: WtNode: ClassTag](nodes: Array[WtNode]): Array[T] = {
    def collectNodes(node: WtNode): Array[T] = node match {
      case n: T          => Array(n)
      case other: WtNode => other.iterator().asScala.toArray.flatMap(collectNodes)
      case _             => Array()
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
    } else {
      removeDuplicateSpaces(replaced)
    }
  }
}
