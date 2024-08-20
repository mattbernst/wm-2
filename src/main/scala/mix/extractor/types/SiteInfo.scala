package mix.extractor.types

import mix.extractor.util.Text

import scala.xml.*


sealed trait Casing
case object FIRST_LETTER extends Casing
case object CASE_SENSITIVE extends Casing

case class Namespace(key: Int, kase: Casing, name: String)
case class SiteInfo(
                     siteName: String,
                     dbName: String,
                     base: String,
                     kase: Casing,
                     namespaces: Seq[Namespace]
                   )

object SiteInfo {
  def apply(input: String): SiteInfo = {
    val namespaces = {
      val xml = XML.loadString(sliceAndValidate("namespaces", input))
      (xml \ "namespace").map { ns =>
        val key = (ns \ "@key").text.toInt
        val name = ns.text.trim
        val kase = caseToCasing((ns \ "@case").text)
        Namespace(key, kase, name)
      }
    }

    val xml = XML.loadString(sliceAndValidate("siteinfo", input))
    new SiteInfo(
      siteName = (xml \ "sitename").text,
      dbName = (xml \ "dbname").text,
      base = (xml \ "base").text,
      kase = caseToCasing((xml \ "case").text),
      namespaces = namespaces
    )
  }

  def caseToCasing(input: String): Casing = input match {
    case "first-letter" => FIRST_LETTER
    case "case-sensitive" => CASE_SENSITIVE
  }

  private def sliceAndValidate(tag: String, xml: String): String = {
    val slice = Text.tagSlice(tag, xml.split('\n').iterator)
    val end = s"</$tag>\n"
    assert(slice.endsWith(end), s"Data for tag `$tag` does not match expected format:\n$slice")
    slice
  }
}
