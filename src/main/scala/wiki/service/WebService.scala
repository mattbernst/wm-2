package wiki.service

import cask.model.Response
import org.rogach.scallop.*
import upickle.default.*
import wiki.db.PhaseState.COMPLETED
import wiki.db.Storage
import wiki.util.{FileHelpers, Logging}

import java.nio.file.NoSuchFileException

case class DocumentProcessingRequest(doc: String)

object DocumentProcessingRequest {
  implicit val rw: ReadWriter[DocumentProcessingRequest] = macroRW
}

object WebService extends cask.MainRoutes with ServiceProperties with Logging {
  override def port: Int      = configuredPort
  private var configuredPort  = 0
  private var ops: ServiceOps = null
  private val startedAt: Long = System.currentTimeMillis()

  @cask.post("/doc/labels")
  def getDocumentLabels(req: cask.Request): Response[String] = {
    val docReq                    = read[DocumentProcessingRequest](req.text())
    val result: ContextWithLabels = ops.getContextWithLabels(docReq)
    jsonResponse(write(result))
  }

  @cask.get("/ping")
  def ping(): Response[String] =
    cask.Response(
      data = s"PONG: Uptime ${System.currentTimeMillis() - startedAt}",
      headers = Seq("Content-Type" -> "text/plain")
    )

  @cask.get("/wiki/page_id/:pageId")
  def getArticleByPageId(pageId: Int): Response[String] = {
    ops.getPageById(pageId) match {
      case Some(page) => jsonResponse(write(page))
      case None       => cask.Response(data = "", statusCode = 404)
    }
  }

  @cask.get("/wiki/page_title/:pageTitle")
  def getArticleByPageTitle(pageTitle: String): Response[String] = {
    ops.getPageByTitle(pageTitle) match {
      case Some(page) => jsonResponse(write(page))
      case None       => cask.Response(data = "", statusCode = 404)
    }
  }

  private def jsonResponse(jsonString: String): Response[String] =
    cask.Response(data = jsonString, headers = Seq("Content-Type" -> "application/json"))

  private def validateData(db: Storage): Unit = {
    require(
      db.phase.getPhaseState(db.phase.lastPhase).contains(COMPLETED),
      "Extraction has not completed. Finish extraction and training first."
    )
    require(
      db.mlModel.read(wsdModelName).nonEmpty,
      s"Could not find model $wsdModelName in db. Run make prepare-disambiguation."
    )
  }

  override def main(args: Array[String]): Unit = {
    val serviceParams = ServiceParams(
      minSenseProbability = 0.01,
      cacheSize = 500_000,
      wordSenseModelName = wsdModelName
    )

    val conf = new Conf(args.toIndexedSeq)
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

    validateData(db)
    ops = new ServiceOps(db, serviceParams)
    logger.info(s"Starting web service on port $port with db $databaseFileName")

    initialize()
    // The Main in cask does not actually make use of command line args.
    // Call it with empty args to make that explicit.
    super.main(Array())
  }

  private class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val database: ScallopOption[String] = opt[String]()
    val port: ScallopOption[Int]        = opt[Int]()
    verify()
  }
}
