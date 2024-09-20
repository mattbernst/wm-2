package wiki.extractor.types

import upickle.default.*
import wiki.extractor.util.Compressor

case class PageMarkup(pageId: Int, wikitext: Option[String], parseResult: Option[ParseResult])

// These uncompressed/compressed variants exist so that storage of the
// voluminous Wikipedia page markup data (raw and parsed) can be switched
// between human-readable (uncompressed) and compact (compressed).

// Uncompressed-storage version of PageMarkup, with parseResult stored as JSON
case class PageMarkup_U(pageId: Int, wikitext: Option[String], parseResult: Option[String])
// Compressed-storage version of PageMarkup, with parseResult stored as compressed messagepack
case class PageMarkup_Z(pageId: Int, wikitext: Option[Array[Byte]], parseResult: Option[Array[Byte]])

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
    PageMarkup_Z(
      pageId = input.pageId,
      wikitext = input.wikitext.map(e => Compressor.compress(e)),
      parseResult = input.parseResult.map(e => Compressor.compress(writeBinary(e)))
    )
  }

  def deserializeCompressed(input: PageMarkup_Z): PageMarkup = {
    PageMarkup(
      pageId = input.pageId,
      wikitext = input.wikitext.map(e => Compressor.decompressToString(e)),
      parseResult = input.parseResult.map(r => readBinary[ParseResult](Compressor.decompress(r)))
    )
  }
}
