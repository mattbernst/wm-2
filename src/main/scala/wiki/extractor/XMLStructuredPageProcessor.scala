package wiki.extractor

import wiki.db.PageSink
import wiki.extractor.language.LanguageLogic
import wiki.extractor.types.*
import wiki.extractor.util.{DBLogging, Progress}
import wiki.util.Logging

import java.nio.charset.StandardCharsets
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.xml.XML

case class StructuredPage(page: Page, markup: PageMarkup)

class XMLStructuredPageProcessor(
  siteInfo: SiteInfo,
  language: Language,
  completedPages: mutable.Set[Int] = mutable.Set())
    extends Logging {

  /**
    * Convert a single Wikipedia page of XML to structured output for further
    * storage and processing.
    *
    * For the format of the XML to be processed, see
    * https://www.mediawiki.org/wiki/Help:Export#Export_format
    * and https://www.mediawiki.org/xml/export-0.11.xsd
    *
    * @param pageXML A string of XML as extracted by WikipediaPageSplitter
    * @return        A StructuredPage with data about the page and its markup,
    *                or None if the page was already completed
    */
  def extract(pageXML: String): Option[StructuredPage] = {
    val xml = XML.loadString(pageXML)
    val id  = (xml \ "id").text.toInt
    if (completedPages.contains(id)) {
      None
    } else {
      val title = (xml \ "title").text
      // e.g. <redirect title="History of Afghanistan" />
      val redirect    = (xml \ "redirect" \ "@title").headOption.map(_.text)
      val namespaceId = (xml \ "ns").text.toInt
      val namespace   = siteInfo.namespaceById(namespaceId)
      assert(id > 0, s"Expected id > 0. Input was:\n $pageXML")
      assert(title.nonEmpty, s"Expected non-empty title. Input was:\n $pageXML")
      val revision = xml \ "revision"

      val text = Some((revision \ "text").text)
        .map(_.replace(" ", " "))
        .map(_.replace("&nbsp;", " "))
        .map(_.trim)
        .filter(_.nonEmpty)

      val markupSize = text.map(_.getBytes(StandardCharsets.UTF_8).length)
      val parsed     = text.flatMap(markup => parser.parseMarkup(title, markup))
      val markup     = PageMarkup(pageId = id, wikitext = text, parseResult = parsed)

      val lastEdited = (revision \ "timestamp").headOption
        .map(_.text)
        .map(string => OffsetDateTime.parse(string, DateTimeFormatter.ISO_DATE_TIME))
        .map(_.toInstant.toEpochMilli)
        .get

      val pageType = if (redirect.nonEmpty) {
        PageType.REDIRECT
      } else {
        inferPageType(wikiText = text.getOrElse(""), namespace = namespace)
      }

      val dp = Page(
        id = id,
        namespace = namespace,
        pageType = pageType,
        title = title,
        redirectTarget = redirect,
        lastEdited = lastEdited,
        markupSize = markupSize
      )

      Some(StructuredPage(page = dp, markup = markup))
    }
  }

  def worker(
    id: Int,
    source: () => Option[String],
    sink: PageSink,
    compressMarkup: Boolean
  ): Worker = {
    val thread = new Thread(() => {
      var skippedPages = 0
      var completed    = false
      while (!completed) {
        source() match {
          case Some(article) if article.trim.nonEmpty =>
            extract(article) match {
              case Some(result) =>
                if (result.markup.wikitext.nonEmpty && result.markup.parseResult.isEmpty) {
                  sink.markUnparseable(result.page.id)
                }
                if (compressMarkup) {
                  val compressed = PageMarkup.serializeCompressed(result.markup)
                  sink.addPage(page = result.page, markupU = None, markupZ = Some(compressed))
                } else {
                  val uncompressed = PageMarkup.serializeUncompressed(result.markup)
                  sink.addPage(page = result.page, markupU = Some(uncompressed), markupZ = None)
                }
              case None =>
                skippedPages += 1
                Progress.tick(skippedPages, ".")
            }
          case _ =>
            completed = true
            DBLogging.info(s"XMLStructuredPageProcessor Worker $id finished")
        }
      }
    })
    DBLogging.info(s"Starting XMLStructuredPageProcessor Worker $id")
    thread.setDaemon(true)
    thread.start()
    Worker(thread)
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
      case SiteInfo.CATEGORY_KEY => PageType.CATEGORY
      case SiteInfo.TEMPLATE_KEY => PageType.TEMPLATE
      case SiteInfo.MAIN_KEY =>
        val transclusions    = getTransclusions(wikiText)
        val lastTransclusion = transclusions.lastOption
        lastTransclusion.foreach(t => incrementTransclusion(t))
        if (lastTransclusion.exists(t => language.isDisambiguation(t))) {
          PageType.DISAMBIGUATION
        } else {
          PageType.ARTICLE
        }

      case _ =>
        // Distinguish more PageTypes based on namespaces?
        PageType.UNHANDLED
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
    val transclusions = ListBuffer[String]()
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

  private val parser: WikitextParser =
    new WikitextParser(LanguageLogic.getLanguageLogic(language.code))

  // Counting transclusions that end a page can be useful to find the
  // most common disambiguation transclusions for configuring the
  // disambiguationPrefixes in languages.json. These are written
  // to last_transclusion_count in the db.
  private lazy val lastTransclusions = mutable.Map[String, Int]()
}
