package wiki.extractor.types

import wiki.extractor.util.Text

import scala.xml.*


sealed trait Casing
case object FIRST_LETTER extends Casing
case object CASE_SENSITIVE extends Casing

case class Namespace(id: Int, casing: Casing, name: String)
case class SiteInfo(
                     siteName: String,
                     dbName: String,
                     base: String,
                     casing: Casing,
                     namespaces: Seq[Namespace]
                   )
{
  val defaultNamespace: Namespace =
    namespaces.find(_.id == 0).getOrElse(throw new NoSuchElementException("No namespace 0 in siteinfo!"))

  val namespaceById: Map[Int, Namespace] =
    namespaces
      .map(ns => (ns.id, ns))
      .toMap

  // These common keys are frequently referenced during data processing.
  // The numbers each refer to a namespace. The numbering is used consistently
  // across different language Wikipedia dumps.
  val MAIN_KEY: Int = 0
  val SPECIAL_KEY: Int = -1
  val FILE_KEY: Int = 6
  val TEMPLATE_KEY: Int = 10
  val CATEGORY_KEY: Int = 14
  // To be sure, validate that all common keys are found in current namespaces
  Seq(MAIN_KEY, SPECIAL_KEY, FILE_KEY, TEMPLATE_KEY, CATEGORY_KEY).foreach { key =>
    val msg = s"Expected to find common key $key in namespaces, but it was missing: $namespaces"
    assert(namespaces.map(_.id).contains(key), msg)
  }
}

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
      casing = caseToCasing((xml \ "case").text),
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
