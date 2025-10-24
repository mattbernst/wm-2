package wiki.service

import cask.model.Response
import org.rogach.scallop.*
import upickle.default.*
import wiki.db.Storage
import wiki.extractor.types.Page
import wiki.util.{FileHelpers, Logging}

import java.nio.file.NoSuchFileException
import scala.collection.mutable

case class DocumentProcessingRequest(doc: String)

object DocumentProcessingRequest {
  implicit val rw: ReadWriter[DocumentProcessingRequest] = macroRW
}

case class MultiArticleRequest(ids: Seq[Int])

object MultiArticleRequest {
  implicit val rw: ReadWriter[MultiArticleRequest] = macroRW
}

object WebService extends cask.MainRoutes with ModelProperties with Logging {
  override def port: Int      = configuredPort
  override def host: String   = "0.0.0.0"
  private var configuredPort  = 0
  private var ops: ServiceOps = _
  private val startedAt: Long = System.currentTimeMillis()

  /**
    * Get labels and links from an arbitrary text document. This endpoint
    * includes all internal details of Pages from extraction and
    * disambiguation.
    *
    * @param req A DocumentProcessingRequest object as JSON
    * @return    A LabelsAndLinks object as JSON
    */
  @cask.post("/doc/labels")
  def getDocumentLabels(req: cask.Request): Response[String] = {
    incrementEndpoint("getDocumentLabels")
    val docReq                 = read[DocumentProcessingRequest](req.text())
    val result: LabelsAndLinks = ops.getLabelsAndLinks(docReq)
    jsonResponse(write(result))
  }

  /**
    * Get labels and links from an arbitrary text document. This endpoint
    * simplifies the Page data that gets returned to the client, since
    * for most applications the extra Page details are irrelevant.
    *
    * @param req A DocumentProcessingRequest object as JSON
    * @return    A StreamlinedLinks object as JSON
    */
  @cask.post("/doc/labels/simple")
  def getDocumentLabelsSimple(req: cask.Request): Response[String] = {
    incrementEndpoint("getDocumentLabelsSimple")
    val docReq = read[DocumentProcessingRequest](req.text())
    val res    = ops.getLabelsAndLinks(docReq)

    def simplify(p: Page): SimplePage = SimplePage(p.id, p.title)

    val simplified = StreamlinedLinks(
      contextPages = res.context.pages.map(p => SimpleRepresentativePage(simplify(p.page.get), p.weight)),
      contextQuality = res.context.quality,
      labels = res.labels,
      resolvedLabels =
        res.resolvedLabels.map(l => SimpleResolvedLabel(l.label, simplify(l.page), l.scoredSenses.scores.toMap)),
      links = res.links.map(t => (simplify(t._1), t._2))
    )

    jsonResponse(write(simplified))
  }

  /**
    * Get excerpts from Wikipedia pages for each requested page ID. If a page
    * does not have a valid first paragraph stored, it will not appear in
    * the response.
    *
    * @param req A MultiArticleRequest object as JSON
    * @return    A sequence of PageExcerpt objects as JSON
    */
  @cask.post("/wiki/excerpts")
  def getArticleExcerpts(req: cask.Request): Response[String] = {
    incrementEndpoint("getArticleExcerpts")
    val artReq = read[MultiArticleRequest](req.text())

    val excerpts = artReq.ids.flatMap { pageId =>
      val firstParagraph = ops.db.page
        .readMarkupAuto(pageId)
        .flatMap(_.parseResult)
        .flatMap(_.snippet.firstParagraph)

      firstParagraph.map(text => PageExcerpt(pageId = pageId, firstParagraph = text))
    }

    jsonResponse(write(excerpts))
  }

  /**
    * A very basic health check.
    *
    * @return The magic word PONG plus uptime in milliseconds
    */
  @cask.get("/ping")
  def ping(): Response[String] = {
    incrementEndpoint("ping")
    cask.Response(
      data = s"PONG: Uptime ${System.currentTimeMillis() - startedAt}",
      headers = Seq("Content-Type" -> "text/plain")
    )
  }

  /**
    * @return
    */
  @cask.get("/stats")
  def stats(): Response[String] = {
    incrementEndpoint("stats")
    cask.Response(
      data = write(endpointCount.toMap),
      headers = Seq("Content-Type" -> "text/plain")
    )
  }

  /**
    * Get a Wikipedia page by numeric page ID.
    *
    * @param pageId  The numeric ID of the Wikipedia page
    * @return        The Page object as JSON
    */
  @cask.get("/wiki/page_id/:pageId")
  def getArticleByPageId(pageId: Int): Response[String] = {
    incrementEndpoint("getArticleByPageId")
    ops.db.getPage(pageId) match {
      case Some(page) => jsonResponse(write(page))
      case None       => cask.Response(data = "", statusCode = 404)
    }
  }

  /**
    * Get a Wikipedia page by page title.
    *
    * @param pageTitle The full title of the page to retrieve
    * @return          The Page object as JSON
    */
  @cask.get("/wiki/page_title/:pageTitle")
  def getArticleByPageTitle(pageTitle: String): Response[String] = {
    incrementEndpoint("getArticleByPageTitle")
    ops.db.getPage(pageTitle) match {
      case Some(page) => jsonResponse(write(page))
      case None       => cask.Response(data = "", statusCode = 404)
    }
  }

  private val endpointCount = mutable.Map[String, Int]()

  private def incrementEndpoint(name: String): Unit = this.synchronized {
    if (endpointCount.contains(name)) {
      endpointCount(name) += 1
    } else {
      endpointCount(name) = 1
    }
  }

  private def jsonResponse(jsonString: String): Response[String] =
    cask.Response(data = jsonString, headers = Seq("Content-Type" -> "application/json"))

  override def main(args: Array[String]): Unit = {

    val conf = new ServiceConf(args.toIndexedSeq)
    val databaseFileName = conf.database
      .orElse(inferDbFile())
      .getOrElse(throw new RuntimeException("No database file found or given!"))

    val defaultPort = 7777
    configuredPort = conf.port.getOrElse(defaultPort)

    val db = if (FileHelpers.isFileReadable(databaseFileName)) {
      new Storage(fileName = databaseFileName)
    } else {
      throw new NoSuchFileException(s"Database file $databaseFileName is not readable")
    }

    ops = new ServiceOps(db, defaultServiceParams)
    ops.validateWordSenseModel()
    ops.validateLinkingModel()
    // Load lazy data in advance
    logger.info(s"Initializing data")
    ops.contextualizer
    logger.info(s"Starting web service on port $port with db $databaseFileName")

    initialize()
    // The Main in cask does not actually make use of command line args.
    // Call it with empty args to make that explicit.
    super.main(Array())
  }
}

class ServiceConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val database: ScallopOption[String] = opt[String]()
  val port: ScallopOption[Int]        = opt[Int]()
  verify()
}
