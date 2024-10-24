package wiki.extractor

import org.sweble.wikitext.parser.nodes.*
import org.sweble.wikitext.parser.utils.NonExpandingParser
import wiki.extractor.language.LanguageLogic
import wiki.extractor.types.{Link, ParseResult}
import wiki.extractor.util.DBLogging

import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

class WikitextParser(languageLogic: LanguageLogic) {

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
        DBLogging.warn(s"""Could not parse "$title" wikitext markup: ${ex.getClass.getSimpleName}""", both = false)
        None
    }
  }

  private[extractor] def parse(title: String, markup: String): Array[WtNode] = {
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

    val text    = cleanString(nodesToText(input))
    val snippet = languageLogic.getSnippet(text)
    ParseResult(
      snippet = snippet,
      text = text,
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

  private[extractor] def cleanString(input: String): String = {
    input
      .replaceAll("[ \t]+", " ")              // Replace multiple spaces or tabs with a single space
      .replaceAll("(?m)^ +| +$", "")          // Remove leading/trailing spaces from each line
      .replaceAll("\n{3,}", "\n\n")           // Replace 3+ newlines with 2 newlines
      .replaceAll("(?m)(\n\\s*){3,}", "\n\n") // Replace 3+ lines containing only whitespace with 2 newlines
      .trim                                   // Remove leading and trailing whitespace from the entire string
  }

  private val parser = new NonExpandingParser(
    true,  // warningsEnabled
    false, // gather round trip data
    false  // autoCorrect
  )
}
