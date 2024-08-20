package mix.extractor.types

import upickle.default.*

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

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
                   )

object Language {
  implicit val rw: ReadWriter[Language] = macroRW

  def toJSON(input: Seq[Language]): String =
    write(input)

  def fromJSON(input: String): Seq[Language] =
    read[Seq[Language]](input)

  def fromJSONFile(fileName: String): Seq[Language] =
    fromJSON(Files.readString(Path.of(fileName), StandardCharsets.UTF_8))
}
