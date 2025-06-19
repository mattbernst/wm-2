package wiki.service

import cask.model.Response
import org.rogach.scallop.*
import wiki.db.Storage
import wiki.util.FileHelpers

import java.nio.file.NoSuchFileException

object WebService extends cask.MainRoutes with ServiceProperties {

  private class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val database: ScallopOption[String] = opt[String]()
    val port: ScallopOption[Int]        = opt[Int]()
    verify()
  }

  override def port: Int     = configuredPort
  private var configuredPort = 0

  @cask.get("/ping")
  def ping(): Response[String] = {
    cask.Response(
      data = "PONG",
      headers = Seq("Content-Type" -> "text/plain")
    )
  }

  override def main(args: Array[String]): Unit = {
    val conf = new Conf(args.toIndexedSeq)
    val databaseFileName = conf.database
      .orElse(inferDbFile())
      .getOrElse(throw new RuntimeException("No database file found or given!"))

    val defaultPort = 7777
    configuredPort = conf.port.getOrElse(defaultPort)

    println(s"Starting web service on port $port with db $databaseFileName")
    val db = if (FileHelpers.isFileReadable(databaseFileName)) {
      new Storage(fileName = databaseFileName)
    } else {
      throw new NoSuchFileException(s"Database file $databaseFileName is not readable")
    }

    initialize()
    // The Main in cask does not actually make use of command line args.
    // Call it with empty args to make that explicit.
    super.main(Array())
  }
}
