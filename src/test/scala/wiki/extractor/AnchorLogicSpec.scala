package wiki.extractor

import wiki.extractor.types.{Language, TrainingProfile}
import wiki.extractor.util.UnitSpec

class AnchorLogicSpec extends UnitSpec {
  behavior of "cleanAnchor"

  it should "trim whitespace" in {
    al.cleanAnchor("Medera ") shouldBe "Medera"
  }

  it should "remove matching-language prefix" in {
    al.cleanAnchor(":en:Dzongkha") shouldBe "Dzongkha"
  }

  it should "remove matching-language prefix and trim" in {
    al.cleanAnchor(":en: n-Butanol") shouldBe "n-Butanol"
  }

  it should "strip section heading destination" in {
    al.cleanAnchor("Alchemy#Hellenistic Egypt") shouldBe "Alchemy"
  }

  it should "replace underscores with spaces" in {
    al.cleanAnchor("Coordination_chemistry") shouldBe "Coordination chemistry"
  }

  private lazy val al = new AnchorLogic(language)

  private lazy val language = Language(
    code = "en",
    name = "English",
    disambiguationPrefixes = Seq("disambiguation", "disambig", "geodis"),
    trainingProfile = TrainingProfile.empty
  )
}
