package mix.extractor

import mix.extractor.types.{ARTICLE, DumpPage}
import mix.extractor.util.Logging

import java.io.StringReader
import java.text.SimpleDateFormat
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants}
import scala.util.Try

case class FragmentWorker(thread: Thread)

object FragmentToDumpPage extends Logging {


  /**
   * Convert a single Wikipedia page of XML to a structured DumpPage
   *
   * @param pageXML A string of XML as extracted by WikipediaPageSplitter
   * @return
   */
  def processFragment(pageXML: String): DumpPage = {
    var id = 0
    var title = ""
    var text = ""
    var lastEdited = 0L
    var characters = new StringBuffer
    val stringReader = new StringReader(pageXML)
    val reader = inputFactory.createXMLStreamReader(stringReader)
    while (reader.hasNext) {
      val eventCode = reader.next
      eventCode match {
        case XMLStreamConstants.END_ELEMENT =>
          val localName = reader.getLocalName
          localName match {
            // Only take the first id (there is a 2nd one for the revision)
            case "id" if id == 0 =>
              id = Integer.parseInt(characters.toString.trim)
            case "title" =>
              title = characters.toString.trim
            case "text" =>
              text = characters.toString.trim
            case "timestamp" =>
              Try(dateFormat.parse(characters.toString.trim).toInstant.toEpochMilli)
                .foreach(ts => lastEdited = ts)
            case _ =>
          }
          characters = new StringBuffer
        case XMLStreamConstants.CHARACTERS =>
          characters.append(reader.getText)
        case _ =>
      }
    }
    reader.close()
    stringReader.close()
//    if (title == "Animalia (book)") {
//      println(pageXML)
//    }
    assert(id > 0, s"Expected id > 0. Input was:\n $pageXML")
    assert(title.nonEmpty, s"Expected non-empty title. Input was:\n $pageXML")

    DumpPage(
      id = id,
      pageType = ARTICLE, // TODO
      title = title,
      text = text,
      target = "???", // TODO
      lastEdited = lastEdited
    )
  }

  def worker(id: Int, source: () => Option[String]): FragmentWorker = {
    val thread = new Thread( new Runnable {
      override def run(): Unit = {
        var completed = false
        while (!completed) {
          source() match {
            case Some(article) =>
              val result = processFragment(article)
            if (result.text.isEmpty) {
              logger.warn(s"Did not get any page text for $result")
            }
            else {
              // TODO do something with result
              // println(result.copy(text = "MARKUP"), result.text.length)
            }
            case None =>
              completed = true
              logger.info(s"FragmentWorker $id finished")
          }
        }
      }
    })
    logger.info(s"Starting FragmentWorker $id")
    thread.setDaemon(true)
    thread.start()
    FragmentWorker(thread)
  }

  private val inputFactory = XMLInputFactory.newInstance
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
}
