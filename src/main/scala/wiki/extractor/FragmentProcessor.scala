package wiki.extractor

import wiki.db.PageWriter
import wiki.extractor.types.*
import wiki.extractor.util.{Logging, ZString}

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.xml.XML


case class FragmentWorker(thread: Thread)
case class StructuredPage(page: DumpPage, markup: PageMarkup)

class FragmentProcessor(siteInfo: SiteInfo,
                        language: Language) extends Logging {
  /**
   * Convert a single Wikipedia page of XML to structured output for further
   * storage and processing.
   *
   * For the format of the XML to be processed, see
   * https://www.mediawiki.org/wiki/Help:Export#Export_format
   * and https://www.mediawiki.org/xml/export-0.11.xsd
   *
   * @param pageXML A string of XML as extracted by WikipediaPageSplitter
   * @return        A StructuredPage with data about the page and its markup
   */
  def extract(pageXML: String): StructuredPage = {
    val xml = XML.loadString(pageXML)
    val title = (xml \ "title").text
    // e.g. <redirect title="History of Afghanistan" />
    val redirect = (xml \ "redirect" \ "@title").headOption.map(_.text)
    val namespaceId = (xml \ "ns").text.toInt
    val namespace = siteInfo.namespaceById(namespaceId)
    val id = (xml \ "id").text.toInt
    assert(id > 0, s"Expected id > 0. Input was:\n $pageXML")
    assert(title.nonEmpty, s"Expected non-empty title. Input was:\n $pageXML")
    val revision = xml \ "revision"

    val text = Some((revision \ "text").text).map(_.trim).filter(_.nonEmpty)
    val markup = PageMarkup(pageId = id, text = text)

    val lastEdited = (revision \ "timestamp")
      .headOption
      .map(_.text)
      .map(string => OffsetDateTime.parse(string, DateTimeFormatter.ISO_DATE_TIME))
      .map(_.toInstant.toEpochMilli)

    val pageType = if (redirect.nonEmpty) {
      REDIRECT
    }
    else {
      inferPageType(pageText = text.getOrElse(""), namespace = namespace)
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

  def fragmentWorker(id: Int,
                     source: () => Option[String],
                     writer: PageWriter,
                     compressMarkup: Boolean): FragmentWorker = {
    val progressDotInterval = 10000
    val thread = new Thread(() => {
      var count = 0
      var completed = false
      while (!completed) {
        source() match {
          case Some(article) if article.trim.nonEmpty =>
            val result = extract(article)
            if (compressMarkup) {
              val compressed = PageMarkup_Z(result.markup.pageId, result.markup.text.map(s => ZString.compress(s)))
              writer.addPage(page = result.page, markup = None, markup_Z = Some(compressed))
            }
            else {
              writer.addPage(page = result.page, markup = Some(result.markup), markup_Z = None)
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
   * Infer the page type from the page text and namespace. We need
   * the page text to determine if the page is a disambiguation. Otherwise,
   * the type can be determined from the namespace or presence of a
   * redirect declaration.
   *
   * @param pageText  Wikipedia markup for the page content
   * @param namespace The namespace that the page belongs to
   */
  private[extractor] def inferPageType(pageText: String,
                                       namespace: Namespace): PageType = {
    namespace.id match {
      case siteInfo.CATEGORY_KEY => CATEGORY
      case siteInfo.TEMPLATE_KEY => TEMPLATE
      case siteInfo.MAIN_KEY =>
        val transclusions = getTransclusions(pageText)
        val lastTransclusion = transclusions.lastOption
        lastTransclusion.foreach(t => incrementTransclusion(t))
        if (lastTransclusion.exists(t => language.isDisambiguation(t))) {
          DISAMBIGUATION
        }
        else {
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
   * @param pageText Wikipedia markup for the page content
   * @return         All transclusions from inside double braces
   */
  private[extractor] def getTransclusions(pageText: String): Seq[String] = {
    val transclusions = new ListBuffer[String]
    var startIndex = -1

    pageText.indices.foreach { i =>
      if (pageText(i) == '{') {
        startIndex = i + 1
      } else if (pageText(i) == '}' && startIndex != -1) {
        transclusions.append(pageText.substring(startIndex, i))
        startIndex = -1
      }
    }

    transclusions.filter(_.nonEmpty).toSeq
  }

  private def incrementTransclusion(transclusion: String): Unit = this.synchronized {
    val count = lastTransclusions.getOrElse(transclusion, 0)
    lastTransclusions.put(transclusion, count + 1): Unit
  }

  // Counting transclusions that end a page can be useful to find the
  // most common disambiguation transclusions for configuring the
  // disambiguationPrefixes in languages.json. These are written
  // to last_transclusion_count in the db.
  private lazy val lastTransclusions = mutable.Map[String, Int]()
}
