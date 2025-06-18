package wiki.service

import cask.model.Response

object WebService extends cask.MainRoutes {
  override def port: Int = 7777

  def ping(): Response[String] = {
    cask.Response(
      data = "PONG",
      headers = Seq("Content-Type" -> "text/plain")
    )
  }

  initialize()
}
