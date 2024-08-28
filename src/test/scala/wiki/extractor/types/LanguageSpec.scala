package wiki.extractor.types

import wiki.extractor.util.UnitSpec

class LanguageSpec extends UnitSpec {
  behavior of "serialization/deserialization"

  it should "serialize an entry and restore it" in {
    val lang = Language(
      code = "en",
      name = "English",
      disambiguationCategories = Seq("Disambiguation"),
      disambiguationTemplates = Seq("disambiguation", "disambig", "geodis"),
      redirectIdentifiers = Seq("REDIRECT"),
      aliases = Seq(
        NamespaceAlias(from = "WP", to = "Wikipedia"),
        NamespaceAlias(from = "WT", to = "Wikipedia talk")
      )
    )

    val serialized = Language.toJSON(Seq(lang))
    Language.fromJSON(serialized) shouldBe Seq(lang)
  }
}
