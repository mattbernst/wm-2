package mix.extractor

import mix.extractor.types.*
import mix.extractor.util.Logging

import java.io.StringReader
import java.text.SimpleDateFormat
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

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
    val tags = fragmentToMap(pageXML)
    val title = tags("title").headOption.getOrElse("")
    val namespace = getNamespace(title)
    if (validNamespaces.contains(namespace)) {
      val id = tags("id").headOption.map(_.toInt).getOrElse(0)
      assert(id > 0, s"Expected id > 0. Input was:\n $pageXML")
      assert(title.nonEmpty, s"Expected non-empty title. Input was:\n $pageXML")

      val text = tags("text").headOption.getOrElse("")
      val lastEdited = tags("timestamp")
        .flatMap(editDate => Try(dateFormat.parse(editDate)).toOption)
        .headOption
        .map(_.toInstant.toEpochMilli)
        .getOrElse(0L)
      val pageType = getPageType(text, namespace)
      val redirectTarget = if (pageType == REDIRECT) {
        val res = getRedirectTarget(text)
        assert(res.nonEmpty, s"Expected to get redirect target for REDIRECT: $id $text")
        res
      }
      else {
        None
      }

      val res = DumpPage(
        id = id,
        namespace = namespace,
        pageType = pageType,
        title = title, // TODO normalize title
        text = text,
        redirectTarget = redirectTarget,
        lastEdited = lastEdited
      )
      Some(res)
    }
    else {
      None
    }
  }

  /**
   * Convert an XML fragment to a map with one or more string values for each
   * key. This flattens everything down in traversal order.
   *
   * @param fragment Some XML with tags and values
   * @return A map preserving every tag that directly contained a value
   */
  def fragmentToMap(fragment: String): mutable.Map[String, ListBuffer[String]] = {
    val result = mutable.Map[String, ListBuffer[String]]()
    var characters = new StringBuffer
    val stringReader = new StringReader(fragment)
    val reader = inputFactory.createXMLStreamReader(stringReader)
    Try {
      while (reader.hasNext) {
        val eventCode = reader.next
        eventCode match {
          case XMLStreamConstants.END_ELEMENT =>
            val localName = reader.getLocalName
            val value = characters.toString.trim
            if (value.nonEmpty) {
              if (result.contains(localName)) {
                result(localName).append(value)
              }
              else {
                result(localName) = ListBuffer(value)
              }
            }

            characters = new StringBuffer
          case XMLStreamConstants.CHARACTERS =>
            characters.append(reader.getText)
          case _ =>
        }
      }
    } match {
      case Success(_) =>
      // If there is a failure, due to e.g. incomplete XML fragment, stuff the exception into the result
      case Failure(ex) =>
        result("exception!") = ListBuffer(ex.toString)
    }

    reader.close()
    stringReader.close()
    result.withDefaultValue(ListBuffer[String]())
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
   * Get the page type from the page text and namespace. We need the page text
   * to determine if the page is a redirect or disambiguation. Otherwise, the
   * type can be determined from the namespace.
   *
   * @param pageText  Wikipedia markup for the page content
   * @param namespace The namespace that the page belongs to
   */
  private[extractor] def getPageType(pageText: String, namespace: Namespace): PageType = {
    if (language.redirectPattern.matcher(pageText).find()) {
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
   * Get the redirect target for a redirect page.
   *
   * @param pageText Wikipedia markup for the page content
   * @return         The target, or None if no target could be found
   */
  private[extractor] def getRedirectTarget(pageText: String): Option[String] = {
    val matcher = language.redirectPattern.matcher(pageText)
    if (matcher.find()) {
      val result = if (matcher.group(2) != null) {
        matcher.group(2)
      }
      else {
        println(s"3, ${matcher.group(3)}, ${pageText.take(500)}...")
        matcher.group(3)
      }
      Some(result)
    }
    else {
      None
    }
  }

  val defaultValidNamespaces: Set[Namespace] = {
    val article = siteInfo.defaultNamespace
    val category = siteInfo.namespaces.find(_.name == "Category")
    val template = siteInfo.namespaces.find(_.name == "Template")
    require(category.nonEmpty, s"SiteInfo namespaces is missing category: ${siteInfo.namespaces}")
    require(template.nonEmpty, s"SiteInfo namespaces is missing template: ${siteInfo.namespaces}")
    Set(article, category.get, template.get)
  }

  private val inputFactory = XMLInputFactory.newInstance
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
}
