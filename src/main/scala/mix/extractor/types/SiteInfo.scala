package mix.extractor.types

import mix.extractor.util.Text

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
  def apply(xml: String): SiteInfo = {
    val tag = "siteinfo"
    val slice = Text.tagSlice(tag, xml.split('\n').iterator)
    val end = s"</siteinfo>\n"
    assert(slice.endsWith(end), s"Siteinfo does not match expected format:\n$slice")
    
    ???
  }
}
