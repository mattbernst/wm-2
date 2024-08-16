package mix.extractor.types

import mix.extractor.util.UnitSpec

class LanguageSpec extends UnitSpec {
  behavior of "serialization/deserialization"

  it should "serialize an entry and restore it" in {
    val lang = Language(
      code = "en",
      name = "English",
      disambiguationCategory = "Disambiguation",
      disambiguationTemplates = Seq("disambiguation", "disambig", "geodis"),
      redirectIdentifier = "REDIRECT",
      aliases = Seq(
        NamespaceAlias(from = "WP", to = "Wikipedia"),
        NamespaceAlias(from = "WT", to = "Wikipedia talk")
      )
    )

    val serialized = Language.toJSON(Seq(lang))
    Language.fromJSON(serialized) shouldBe Seq(lang)
  }
}
