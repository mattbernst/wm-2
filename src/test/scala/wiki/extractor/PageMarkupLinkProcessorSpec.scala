package wiki.extractor

import wiki.extractor.types.Language
import wiki.extractor.util.UnitSpec

import scala.collection.mutable

class PageMarkupLinkProcessorSpec extends UnitSpec {
  behavior of "keyFromTarget"

  it should "trim whitespace" in {
    lp.keyFromTarget("Medera ") shouldBe "medera"
  }

  it should "lowercase" in {
    lp.keyFromTarget("Manel Filali") shouldBe "manel filali"
  }

  it should "remove matching-language prefix" in {
    lp.keyFromTarget(":en:Dzongkha") shouldBe "dzongkha"
  }

  it should "remove matching-language prefix and trim" in {
    lp.keyFromTarget(":en: n-Butanol") shouldBe "n-butanol"
  }

  it should "leave non-matching-language prefix in place" in {
    lp.keyFromTarget(":fr:Jeux et Stratégie") shouldBe ":fr:jeux et stratégie"
  }

  it should "rewrite :-prefixed category designators" in {
    lp.keyFromTarget(":Category:Atlantic hurricanes") shouldBe "category:atlantic hurricanes"
  }

  it should "rewrite :-prefixed category designators, dropping whitespace" in {
    lp.keyFromTarget(":Category: People from Copenhagen") shouldBe "category:people from copenhagen"
  }

  it should "strip section heading destination" in {
    lp.keyFromTarget("Alchemy#Hellenistic Egypt") shouldBe "alchemy"
  }

  it should "replace underscores with spaces" in {
    lp.keyFromTarget("Coordination_chemistry") shouldBe "coordination chemistry"
  }

  private lazy val lp = new PageMarkupLinkProcessor(mutable.Map(), language, "Category")

  private lazy val language = Language(
    code = "en",
    name = "English",
    disambiguationPrefixes = Seq("disambiguation", "disambig", "geodis"),
    rootCategory = "Category:Main topic classifications"
  )
}
