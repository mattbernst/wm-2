package wiki.extractor.types

import wiki.extractor.util.UnitSpec

class LanguageSpec extends UnitSpec {
  "serialization/deserialization" should "serialize an entry and restore it" in {
    val serialized = Language.toJSON(Seq(lang))
    Language.fromJSON(serialized) shouldBe Seq(lang)
  }

  behavior of "isDisambiguation"

  it should "return true for a direct match" in {
    lang.isDisambiguation("geodis") shouldBe true
  }

  it should "return true for a differently-cased transclusion that matches" in {
    lang.isDisambiguation("Disambiguation") shouldBe true
  }

  it should "return true for a transclusion that starts with a prefix" in {
    lang.isDisambiguation("disambiguation|geo") shouldBe true
  }

  it should "return false for an unrelated transclusion" in {
    lang.isDisambiguation("reflist") shouldBe false
  }

  private lazy val lang = Language(
    code = "en",
    name = "English",
    disambiguationPrefixes = Seq("disambiguation", "disambig", "geodis"),
    rootCategory = "Category:Main topic classifications"
  )
}
