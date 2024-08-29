package wiki.extractor

import wiki.extractor.types.*
import wiki.extractor.util.Logging

import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer
import scala.util.Try
import scala.xml.XML

case class FragmentWorker(thread: Thread)

class FragmentProcessor(siteInfo: SiteInfo, language: Language) extends Logging {
  /**
   * Convert a single Wikipedia page of XML to a structured DumpPage.
   * Only categories, templates, and articles get converted by default;
   * other page types can be ignored.
   *
   * @param pageXML A string of XML as extracted by WikipediaPageSplitter
   * @return        An optional DumpPage with text and structured page data
   */
  def fragmentToPage(pageXML: String,
                     validNamespaces: Set[Namespace] = defaultValidNamespaces): Option[DumpPage] = {
    val xml = XML.loadString(pageXML)
    val title = (xml \ "title").text
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
      val redirects = getRedirects(text)
      val pageType = getPageType(pageText = text, redirects = redirects, namespace = namespace)

      val res = DumpPage(
        id = id,
        namespace = namespace,
        pageType = pageType,
        title = title, // TODO normalize title
        text = text,
        redirectTarget = redirects.headOption,
        lastEdited = lastEdited
      )
      Some(res)
    }
    else {
      None
    }
  }

  def fragmentWorker(id: Int,
                     source: () => Option[String]): FragmentWorker = {
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
   * Determine the namespace from the page title. Titles in a namespace start
   * with a prefix: value that can be matched in siteinfo.
   *
   * @param title A page title
   * @return      The matching namespace, or default namespace if nothing matches
   */
  private[extractor] def getNamespace(title: String): Namespace =
    siteInfo.prefixToNamespace(title.split(':').head)


  /**
   * Get the page type from the page text, redirects, and namespace. We need
   * the page text to determine if the page is a disambiguation. Otherwise,
   * the type can be determined from the namespace or redirects.
   *
   * @param pageText  Wikipedia markup for the page content
   * @param redirects One or more bracketed-text pages following a #REDIRECT
   * @param namespace The namespace that the page belongs to
   */
  private[extractor] def getPageType(pageText: String,
                                     redirects: Seq[String],
                                     namespace: Namespace): PageType = {
    if (redirects.nonEmpty) {
      REDIRECT
    }
    else {
      // There's one case for every namespace that needs to be handled.
      // Update this match if defaultValidNamespaces expands.
      namespace.key match {
        case siteInfo.CATEGORY_KEY =>
          CATEGORY
        case siteInfo.TEMPLATE_KEY =>
          TEMPLATE
        case siteInfo.MAIN_KEY =>
          if (language.disambiguationPattern.matcher(pageText).find()) {
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
  }

  /**
   * Get the pages following a #redirect directive.
   *
   * @param pageText Wikipedia markup for the page content
   * @return         All pages inside double brackets, following a redirect
   */
  private[extractor] def getRedirects(pageText: String): Seq[String] = {
    val identifiers = new ListBuffer[String]
    var startIndex = -1
    val head = pageText.take(Byte.MaxValue).toUpperCase

    if (head.contains("#REDIRECT")) {
      pageText.indices.foreach { i =>
        if (pageText(i) == '[') {
          startIndex = i + 1
        } else if (pageText(i) == ']' && startIndex != -1) {
          identifiers.append(pageText.substring(startIndex, i))
          startIndex = -1
        }
      }
    }

    identifiers
      .toSeq
      // We want to get whole-page identifiers, so convert redirects like
      // "Thermal expansion#Coefficient of thermal expansion"
      // to
      // "Thermal expansion"
      .map(_.split('#').head)
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
}
