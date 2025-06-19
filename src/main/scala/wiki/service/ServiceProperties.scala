package wiki.service

import wiki.util.FileHelpers

trait ServiceProperties {
  val wsdModelName: String = "word_sense_disambiguation_ranker"

  /**
    * Try to automatically infer the name of the DB file to use for running the
    * service. This only works if there is a single DB file, located in the
    * current working directory. Otherwise, the name must be given manually.
    *
    * @return The name of the file (if it can be inferred)
    */
  def inferDbFile(): Option[String] = {
    val candidates = FileHelpers.glob("./*.db")
    if (candidates.isEmpty) {
      println("No db file found in current directory. Give db file name via command line or generate one.")
      None
    } else if (candidates.length > 1) {
      println(s"Found multiple db files: ${candidates.mkString(", ")}. Give db file name via command line.")
      None
    } else {
      candidates.headOption
    }
  }
}
