package wiki.extractor

import org.sweble.wikitext.parser.nodes.*
import org.sweble.wikitext.parser.utils.NonExpandingParser
import wiki.extractor.language.LanguageLogic
import wiki.extractor.types.{Link, LocatedLink, ParseResult}
import wiki.extractor.util.{DBLogging, Text}
import wiki.util.FileHelpers

import scala.collection.mutable.ListBuffer
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
        Some(processNodes(nodes, markup))
      case Failure(ex) =>
        DBLogging.warn(s"""Could not parse "$title" wikitext markup: ${ex.getClass.getSimpleName}""", both = false)
        None
    }
  }

  /**
    * Recursively extract all nodes of type T into a flattened array.
    *
    * @param nodes Nodes that have been parsed from wikitext
    * @tparam T The node type to extract
    * @return All nodes of type T
    */
  def extractNodes[T <: WtNode: ClassTag](nodes: Array[WtNode]): Array[T] = {
    def collectNodes(node: WtNode): Array[T] = node match {
      case n: T          => Array(n)
      case other: WtNode => other.iterator().asScala.toArray.flatMap(collectNodes)
      case _             => Array()
    }

    nodes.flatMap(collectNodes)
  }

  private[extractor] def parse(title: String, markup: String): Array[WtNode] = {
    parser.parseArticle(markup, title).iterator().asScala.toArray
  }

  private[extractor] def processNodes(input: Array[WtNode], markup: String): ParseResult = {
    // Links as extracted here match the page text exactly.
    // Properly casing the target and stripping sub-page sections happens later.
    val links = extractNodes[WtInternalLink](input).map { link =>
      if (link.hasTitle) {
        Link(target = textualize(link.getTarget).trim, textualize(link.getTitle).trim)
      } else {
        val target = textualize(link.getTarget).trim
        Link(target = target, anchorText = target)
      }
    }
      .filter(_.anchorText.trim.nonEmpty)

    val text    = cleanString(nodesToText(input))
    val snippet = languageLogic.getSnippet(text)
    ParseResult(
      snippet = snippet,
      text = text,
      links = WikitextParser.getLinkLocations(links.toSeq, markup)
    )
  }

  private[extractor] def nodesToText(input: Array[WtNode]): String =
    input.map(textualize).mkString

  private[extractor] def textualize(wtNode: WtNode): String = wtNode match {
    case node: WtText                           => node.getContent
    case node: WtImageLink if node.hasTitle     => textualize(node.getTitle)
    case node: WtImageLink if !node.hasTitle    => textualize(node.getTarget)
    case node: WtInternalLink if !node.hasTitle => textualize(node.getTarget)
    case node: WtInternalLink if node.hasTitle  => textualize(node.getTitle)
    case node: WtListItem                       => "\n" + node.iterator().asScala.map(textualize).mkString
    case node: WtTableHeader                    => " : " + node.iterator().asScala.map(textualize).mkString
    case node: WtTableCell                      => " | " + node.iterator().asScala.map(textualize).mkString

    // All of these add noise to the text version of the page. Eliminate
    // template noise, XML attributes, and WtTagExtensions (like citations).
    case _: WtTemplate      => ""
    case _: WtXmlAttributes => ""
    case _: WtTagExtension  => ""

    case other: WtNode => other.iterator().asScala.map(textualize).mkString
  }

  private[extractor] def excludeNodes[T <: WtNode: ClassTag](nodes: Array[WtNode]): Array[WtNode] = {
    def collectNodes(node: WtNode): Array[WtNode] = node match {
      case _: T          => Array() // Skip nodes of type T
      case other: WtNode => Array(other)
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

object WikitextParser {

  /**
    * Take links previously extracted with the Sweble wikitext parser and
    * assign positions relative to the original page markup, using the anchor
    * text.
    *
    * @param links  Links to match to page text
    * @param markup Raw wikitext page markup
    * @return       Links with left/right locations populated
    */
  def getLinkLocations(links: Seq[Link], markup: String): Seq[LocatedLink] = {
    val result = ListBuffer[LocatedLink]()
    var searchStartIndex = 0

    for (link <- links.filter(_.anchorText.trim.nonEmpty)) {
      // Find the next occurrence of this anchor text that's actually within link brackets
      var found = false
      var currentSearchPos = searchStartIndex

      while (!found && currentSearchPos < markup.length) {
        val index = markup.indexOf(link.target, currentSearchPos)

        if (index >= 0) {
          // Check if this anchor text is within a wikilink by looking backwards for [[
          val linkStart = markup.lastIndexOf("[[", index)
          val linkEnd = markup.indexOf("]]", index)

          // Verify this is actually within a complete link
          if (linkStart >= 0 && linkEnd >= 0 && linkEnd > index) {
            // Make sure there's no intervening [[ between linkStart and our anchor
            val interferingStart = markup.indexOf("[[", linkStart + 2)
            if (interferingStart == -1 || interferingStart > index) {
              val left = index
              val right = index + link.target.length

              result += LocatedLink(
                target = link.target,
                anchorText = link.anchorText,
                left = left,
                right = right
              )

              searchStartIndex = right
              found = true
            } else {
              // Keep searching past this false match
              currentSearchPos = index + 1
            }
          } else {
            // Keep searching past this false match
            currentSearchPos = index + 1
          }
        } else {
          // No more occurrences found - exit the loop
          found = true
        }
      }
    }

    val res = result.toSeq
    if (res.length != links.length) {
      import upickle.default.*
      val hash = markup.hashCode.abs.toString
      FileHelpers.writeTextFile(hash + ".wikitext", markup)
      FileHelpers.writeTextFile(hash + ".links", write(links))
      FileHelpers.writeTextFile(hash + ".loclinks", write(res))
      println(s"UHOH! $hash, ${links.length}, ${res.length}")
    }
//    assert(res.length == links.length, s"${links.length} links in input should have matched ${res.length} in output")
    res
  }
}
