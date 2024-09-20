package wiki.extractor

import wiki.db.PageWriter
import wiki.extractor.language.{EnglishSnippetExtractor, SnippetExtractor}
import wiki.extractor.types.*
import wiki.extractor.util.Logging

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}
import scala.xml.XML

case class FragmentWorker(thread: Thread)
case class StructuredPage(page: DumpPage, markup: PageMarkup)

class FragmentProcessor(siteInfo: SiteInfo, language: Language) extends Logging {

  /**
    * Convert a single Wikipedia page of XML to structured output for further
    * storage and processing.
    *
    * For the format of the XML to be processed, see
    * https://www.mediawiki.org/wiki/Help:Export#Export_format
    * and https://www.mediawiki.org/xml/export-0.11.xsd
    *
    * @param pageXML A string of XML as extracted by WikipediaPageSplitter
    * @param workerId The unique ID for a worker running in a multithreaded context
    * @return        A StructuredPage with data about the page and its markup
    */
  def extract(pageXML: String): StructuredPage = {
    val xml   = XML.loadString(pageXML)
    val title = (xml \ "title").text
    // e.g. <redirect title="History of Afghanistan" />
    val redirect    = (xml \ "redirect" \ "@title").headOption.map(_.text)
    val namespaceId = (xml \ "ns").text.toInt
    val namespace   = siteInfo.namespaceById(namespaceId)
    val id          = (xml \ "id").text.toInt
    assert(id > 0, s"Expected id > 0. Input was:\n $pageXML")
    assert(title.nonEmpty, s"Expected non-empty title. Input was:\n $pageXML")
    val revision = xml \ "revision"

    val text   = Some((revision \ "text").text).map(_.trim).filter(_.nonEmpty)
    val parsed = text.flatMap(markup => parser.parseMarkup(title, markup))
    val markup = PageMarkup(pageId = id, wikitext = text, parseResult = parsed)
    if (text.nonEmpty && parsed.isEmpty) {
      addUnparseable(id)
    }

    val lastEdited = (revision \ "timestamp").headOption
      .map(_.text)
      .map(string => OffsetDateTime.parse(string, DateTimeFormatter.ISO_DATE_TIME))
      .map(_.toInstant.toEpochMilli)

    val pageType = if (redirect.nonEmpty) {
      REDIRECT
    } else {
      inferPageType(wikiText = text.getOrElse(""), namespace = namespace)
    }

    val dp = DumpPage(
      id = id,
      namespace = namespace,
      pageType = pageType,
      title = title,
      redirectTarget = redirect,
      lastEdited = lastEdited
    )

    StructuredPage(page = dp, markup = markup)
  }

  def fragmentWorker(
    id: Int,
    source: () => Option[String],
    writer: PageWriter,
    compressMarkup: Boolean
  ): FragmentWorker = {
    val progressDotInterval = 10000
    val thread = new Thread(() => {
      var count     = 0
      var completed = false
      while (!completed) {
        source() match {
          case Some(article) if article.trim.nonEmpty =>
            val result = extract(article)
            if (compressMarkup) {
              val compressed = PageMarkup.serializeCompressed(result.markup)
              writer.addPage(page = result.page, markupU = None, markupZ = Some(compressed))
            } else {
              val uncompressed = PageMarkup.serializeUncompressed(result.markup)
              writer.addPage(page = result.page, markupU = Some(uncompressed), markupZ = None)
            }
            count += 1
            if (count % progressDotInterval == 0) {
              System.err.print(".")
              System.err.flush()
            }
          case _ =>
            completed = true
            logger.info(s"FragmentWorker $id finished")
        }
      }
    })
    logger.info(s"Starting FragmentWorker $id")
    thread.setDaemon(true)
    thread.start()
    FragmentWorker(thread)
  }

  /**
    * Get counts of how many times each transclusion appeared as the last
    * transclusion on a page. These counts can be used to narrow the search
    * for transclusions that indicate a disambiguation page.
    *
    * @return A map of each transclusion name to a count of appearances
    */
  def getLastTransclusionCounts(): Map[String, Int] =
    lastTransclusions.toMap

  /**
    * Get IDs of pages with wikitext markup that Sweble could not parse.
    * These pages will be excluded from title mapping and further analysis.
    *
    * @return Set of page IDs for pages that Sweble could not parse
    */
  def getUnparseable(): Set[Int] = this.synchronized {
    unparseablePages.toSet
  }

  /**
    * Infer the page type from the page text and namespace. We need
    * the page text to determine if the page is a disambiguation. Otherwise,
    * the type can be determined from the namespace or presence of a
    * redirect declaration.
    *
    * @param wikiText  Wikipedia markup for the page content
    * @param namespace The namespace that the page belongs to
    */
  private[extractor] def inferPageType(wikiText: String, namespace: Namespace): PageType = {
    namespace.id match {
      case siteInfo.CATEGORY_KEY => CATEGORY
      case siteInfo.TEMPLATE_KEY => TEMPLATE
      case siteInfo.MAIN_KEY =>
        val transclusions    = getTransclusions(wikiText)
        val lastTransclusion = transclusions.lastOption
        lastTransclusion.foreach(t => incrementTransclusion(t))
        if (lastTransclusion.exists(t => language.isDisambiguation(t))) {
          DISAMBIGUATION
        } else {
          ARTICLE
        }

      case _ =>
        // Distinguish more PageTypes based on namespaces?
        UNHANDLED
    }
  }

  /**
    * Get any transclusions from the page. Transclusions are anything inside
    * {{double braces like this}}.
    * See also https://en.wikipedia.org/wiki/Help:Transclusion
    *
    * @param wikiText Wikipedia markup for the page content
    * @return         All transclusions from inside double braces
    */
  private[extractor] def getTransclusions(wikiText: String): Seq[String] = {
    val transclusions = new ListBuffer[String]
    var startIndex    = -1

    wikiText.indices.foreach { i =>
      if (wikiText(i) == '{') {
        startIndex = i + 1
      } else if (wikiText(i) == '}' && startIndex != -1) {
        transclusions.append(wikiText.substring(startIndex, i))
        startIndex = -1
      }
    }

    transclusions.filter(_.nonEmpty).toSeq
  }

  private def incrementTransclusion(transclusion: String): Unit = this.synchronized {
    val count = lastTransclusions.getOrElse(transclusion, 0)
    lastTransclusions.put(transclusion, count + 1): Unit
  }

  private def addUnparseable(pageId: Int): Unit = this.synchronized {
    unparseablePages.add(pageId): Unit
  }

  private val parser: WikitextParser = {
    val snippetExtractors: Map[String, SnippetExtractor] = Map(
      "en" -> EnglishSnippetExtractor
    )
    Try(new WikitextParser(snippetExtractors(language.code))) match {
      case Success(v) =>
        v
      case Failure(ex: NoSuchElementException) =>
        logger.error(s"No SnippetExtractor defined for language code '${language.code}'")
        throw ex
      case Failure(ex) =>
        throw ex
    }
  }

  // Counting transclusions that end a page can be useful to find the
  // most common disambiguation transclusions for configuring the
  // disambiguationPrefixes in languages.json. These are written
  // to last_transclusion_count in the db.
  private lazy val lastTransclusions = mutable.Map[String, Int]()

  // These pages failed to parse via Sweble and should be excluded from
  // the title-to-page mapping.
  private lazy val unparseablePages = mutable.Set[Int]()
}
