package wiki.extractor

import wiki.db.Storage
import wiki.extractor.types.*
import wiki.extractor.util.Logging

import java.text.SimpleDateFormat
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try
import scala.xml.XML

case class FragmentWorker(thread: Thread)

class FragmentProcessor(siteInfo: SiteInfo,
                        language: Language) extends Logging {
  /**
   * Convert a single Wikipedia page of XML to a structured DumpPage.
   * Only categories, templates, and articles get converted by default;
   * other page types can be ignored.
   *
   * For the format of the XML to be processed, see
   * https://www.mediawiki.org/wiki/Help:Export#Export_format
   * and https://www.mediawiki.org/xml/export-0.11.xsd
   *
   * @param pageXML A string of XML as extracted by WikipediaPageSplitter
   * @return        An optional DumpPage with text and structured page data
   */
  def fragmentToPage(pageXML: String,
                     validNamespaces: Set[Namespace] = defaultValidNamespaces): Option[DumpPage] = {
    val xml = XML.loadString(pageXML)
    val title = (xml \ "title").text
    // e.g. <redirect title="History of Afghanistan" />
    val redirect = (xml \ "redirect" \ "@title").headOption.map(_.text)
    val namespace = getNamespace(title)
    if (validNamespaces.contains(namespace)) {
      val id = (xml \ "id").text.toInt
      assert(id > 0, s"Expected id > 0. Input was:\n $pageXML")
      assert(title.nonEmpty, s"Expected non-empty title. Input was:\n $pageXML")
      val revision = xml \ "revision"

      val text = (revision \ "text").text
      val lastEdited = (revision \ "timestamp")
        .map(_.text)
        .flatMap(editDate => Try(dateFormat.parse(editDate)).toOption)
        .headOption
        .map(_.toInstant.toEpochMilli)
        .getOrElse(0L)

      val pageType = if (redirect.nonEmpty) {
        REDIRECT
      }
      else {
        inferPageType(pageText = text, namespace = namespace)
      }

      val res = DumpPage(
        id = id,
        namespace = namespace,
        pageType = pageType,
        title = title,
        text = text,
        redirectTarget = redirect,
        lastEdited = lastEdited
      )
      Some(res)
    }
    else {
      None
    }
  }

  def fragmentWorker(id: Int,
                     source: () => Option[String],
                     writer: Storage): FragmentWorker = {
    val thread = new Thread(() => {
      var completed = false
      while (!completed) {
        source() match {
          case Some(article) if article.trim.nonEmpty =>
            fragmentToPage(article).foreach { result =>
              if (result.text.isEmpty) {
                logger.warn(s"Did not get any page text for $result")
              }
              else {
                // TODO do something with result
                // println(result.copy(text = "MARKUP"), result.text.length)
              }
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
   * Determine the namespace from the page title. Titles in a namespace start
   * with a prefix: value that can be matched in siteinfo.
   *
   * @param title A page title
   * @return      The matching namespace, or default namespace if nothing matches
   */
  private[extractor] def getNamespace(title: String): Namespace =
    siteInfo.prefixToNamespace(title.split(':').head)

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
    // There's one case for every namespace that needs to be handled.
    // Update this match if defaultValidNamespaces expands.
    namespace.key match {
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
        logger.error(s"Got INVALID page type from namespace $namespace and page text $pageText")
        INVALID
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

  val defaultValidNamespaces: Set[Namespace] = {
    val article = siteInfo.defaultNamespace
    val category = siteInfo.namespaces.find(_.name == "Category")
    val template = siteInfo.namespaces.find(_.name == "Template")
    require(category.nonEmpty, s"SiteInfo namespaces is missing category: ${siteInfo.namespaces}")
    require(template.nonEmpty, s"SiteInfo namespaces is missing template: ${siteInfo.namespaces}")
    Set(article, category.get, template.get)
  }

  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  // Counting transclusions that end a page can be useful to find the
  // most common disambiguation transclusions for configuring the
  // disambiguationPrefixes in languages.json. These are written
  // to last_transclusion_count in the db.
  private lazy val lastTransclusions = mutable.Map[String, Int]()
}
