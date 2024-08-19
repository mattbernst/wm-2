package mix.extractor.util

import scala.io.Source

object Text {
  def readTextFile(fileName: String): String = {
    val source = Source.fromFile(fileName)
    val lines = source.getLines().toList
    source.close()
    lines.mkString("\n")
  }
}
