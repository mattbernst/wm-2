package mix.extractor

import org.xml.sax.helpers.DefaultHandler
import org.xml.sax.{Attributes, SAXException}

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}

case class WPArticle(title: String, text: String)

class WikipediaXMLHandler extends DefaultHandler {
  private var count = 0
  private val queue = new ArrayBlockingQueue[WPArticle](5000)
  private var currentElement = ""
  private var currentTitle = ""
  private val currentText = new StringBuilder
  private var inPage = false

  def getFromQueue(): Option[WPArticle] =
    Option(queue.poll(3, TimeUnit.SECONDS))

  @throws[SAXException]
  override def startElement(uri: String, localName: String, qName: String, attributes: Attributes): Unit = {
    currentElement = qName
    if (qName == "page") {
      inPage = true
    }
  }

  @throws[SAXException]
  override def characters(ch: Array[Char], start: Int, length: Int): Unit = {
    if (inPage) {
      currentElement match {
        case "title" => currentTitle += new String(ch, start, length)
        case "text" => currentText.appendAll(ch, start, length)
        case _ =>
      }
    }
  }

  @throws[SAXException]
  override def endElement(uri: String, localName: String, qName: String): Unit = {
    if (qName == "page") {
      count += 1
      inPage = false
      val article = WPArticle(title = currentTitle.trim, text = currentText.result().trim)
      if (count % 10000 == 0) println(s"${article.title} size: ${article.text.length} count: $count")
      queue.put(article)
      currentTitle = ""
      currentText.clear()
    }
  }
}
