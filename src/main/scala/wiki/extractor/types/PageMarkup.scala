package wiki.extractor.types

import upickle.default.*
import wiki.extractor.util.Compressor

case class TypedPageMarkup(pm: PageMarkup, pageType: PageType)
case class PageMarkup(pageId: Int, wikitext: Option[String], parseResult: Option[ParseResult])

// These uncompressed/compressed variants exist so that storage of the
// voluminous Wikipedia page markup data (raw and parsed) can be switched
// between human-readable (uncompressed) and compact (compressed).

// Uncompressed-storage version of PageMarkup, with parseResult stored as JSON
case class PageMarkup_U(pageId: Int, wikitext: Option[String], parseResult: Option[String])
// Compressed-storage version of PageMarkup, with data stored as compressed messagepack
case class Data_Z(wikitext: Option[String], parseResult: Option[ParseResult])

object Data_Z {
  implicit val rw: ReadWriter[Data_Z] = macroRW
}
case class PageMarkup_Z(pageId: Int, data: Array[Byte])

object PageMarkup {

  def serializeUncompressed(input: PageMarkup): PageMarkup_U = {
    PageMarkup_U(
      pageId = input.pageId,
      wikitext = input.wikitext,
      parseResult = input.parseResult.map(r => write(r))
    )
  }

  def deserializeUncompressed(input: PageMarkup_U): PageMarkup = {
    PageMarkup(
      pageId = input.pageId,
      wikitext = input.wikitext,
      parseResult = input.parseResult.map(e => read[ParseResult](e))
    )
  }

  def serializeCompressed(input: PageMarkup): PageMarkup_Z = {
    // Put the raw wikitext and parseResult into one structure together before
    // compressing to achieve better compression
    val dz   = Data_Z(input.wikitext, input.parseResult)
    val data = Compressor.compress(writeBinary(dz))
    PageMarkup_Z(
      pageId = input.pageId,
      data = data
    )
  }

  def deserializeCompressed(input: PageMarkup_Z): PageMarkup = {
    val decompressed = Compressor.decompress(input.data)
    val data         = readBinary[Data_Z](decompressed)
    PageMarkup(
      pageId = input.pageId,
      wikitext = data.wikitext,
      parseResult = data.parseResult
    )
  }
}
