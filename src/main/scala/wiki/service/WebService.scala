package wiki.service

import cask.model.Response

object WebService extends cask.MainRoutes with ServiceProperties {
  override def port: Int = 7777

  @cask.get("/ping")
  def ping(): Response[String] = {
    cask.Response(
      data = "PONG",
      headers = Seq("Content-Type" -> "text/plain")
    )
  }

  initialize()
}
