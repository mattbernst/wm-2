package mix.extractor.types

import upickle.default.*

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.regex.Pattern

case class NamespaceAlias(from: String, to: String)
object NamespaceAlias {
  implicit val rw: ReadWriter[NamespaceAlias] = macroRW
}
case class Language(
                     code: String, // e.g. "en"
                     name: String, // e.g. "English"
                     disambiguationCategories: Seq[String],
                     // See https://en.wikipedia.org/wiki/Template:Disambiguation
                     disambiguationTemplates: Seq[String],
                     redirectIdentifiers: Seq[String],
                     // See https://en.wikipedia.org/wiki/Wikipedia:Namespace#Aliases_and_pseudo-namespaces
                     aliases: Seq[NamespaceAlias]
                   ) {

  val redirectPattern: Pattern = {
    val redirectRegex = new StringBuffer("\\#")
    redirectRegex.append("(")
    redirectIdentifiers.foreach { ri =>
      redirectRegex.append(ri)
      redirectRegex.append("|")
    }

    redirectRegex.deleteCharAt(redirectRegex.length - 1)
    redirectRegex.append(")[:\\s]*(?:\\[\\[(.*)\\]\\]|(.*))")

    Pattern.compile(redirectRegex.toString, Pattern.CASE_INSENSITIVE)
  }

  val disambiguationPattern: Pattern = {
    var disambigCategoryRegex: String = null
    if (disambiguationCategories.nonEmpty) {
      val tmp = new StringBuffer
      tmp.append("\\[\\[\\s*")
      val payload = if (disambiguationCategories.size == 1) {
        disambiguationCategories.head
      }
      else {
        s"(${disambiguationCategories.mkString("|")})"
      }
      tmp.append(payload)
      tmp.append("\\s*\\]\\]")
      disambigCategoryRegex = tmp.toString
    }

    var disambigTemplateRegex: String = null
    if (disambiguationTemplates.nonEmpty) {
      val tmp = new StringBuffer
      tmp.append("\\{\\{\\s*")
      val payload = if (disambiguationTemplates.size == 1) {
        disambiguationTemplates.head
      }
      else {
        s"(${disambiguationTemplates.mkString("|")})"
      }
      tmp.append(payload)
      tmp.append("\\s*\\}\\}")
      disambigTemplateRegex = tmp.toString
    }

    if (disambigCategoryRegex == null && disambigTemplateRegex == null) {
      val msg = "Language configuration does not specify any categories or templates for identifying disambiguation pages"
      throw new AssertionError(msg)
    }

    if (disambigCategoryRegex != null && disambigTemplateRegex != null) {
      val combined = s"($disambigCategoryRegex|$disambigTemplateRegex)"
      Pattern.compile(combined, Pattern.CASE_INSENSITIVE)
    }
    else if (disambigCategoryRegex != null) {
      Pattern.compile(disambigCategoryRegex, Pattern.CASE_INSENSITIVE)
    }
    else {
      Pattern.compile(disambigTemplateRegex, Pattern.CASE_INSENSITIVE)
    }
  }

}

object Language {
  implicit val rw: ReadWriter[Language] = macroRW

  def toJSON(input: Seq[Language]): String =
    write(input)

  def fromJSON(input: String): Seq[Language] =
    read[Seq[Language]](input)

  def fromJSONFile(fileName: String): Seq[Language] =
    fromJSON(Files.readString(Path.of(fileName), StandardCharsets.UTF_8))
}
