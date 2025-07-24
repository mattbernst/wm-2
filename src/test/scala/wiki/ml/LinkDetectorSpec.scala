package wiki.ml

import wiki.util.{FileHelpers, UnitSpec}

class LinkDetectorSpec extends UnitSpec {
  behavior of "predict"

  it should "handle empty input" in {
    detector.predict(Array()) shouldBe Array()
  }

  it should "predict link-worthiness from features (1)" in {
    val input = LabelLinkFeatures(
      linkedPageId = randomInt(),
      normalizedOccurrences = 2.1972245773362196,
      maxDisambigConfidence = 4.66313376056129,
      avgDisambigConfidence = 2.831566880280645,
      relatednessToContext = 0.17740502993759308,
      relatednessToOtherTopics = 0.1276921533737109,
      avgLinkProbability = 0.33660340009315326,
      maxLinkProbability = 0.49557522123893805,
      firstOccurrence = 0.051762114537444934,
      lastOccurrence = 0.13892385147891756,
      spread = 0.08716173694147263
    )

    val res = detector.predict(Array(input)).head
    // Detected as likely link from features
    approximatelyEqual(res.prediction, 0.53) shouldBe true
  }

  it should "predict link-worthiness from features (2)" in {
    val input = LabelLinkFeatures(
      linkedPageId = randomInt(),
      normalizedOccurrences = 0.6931471805599453,
      maxDisambigConfidence = 0.9016288116962997,
      avgDisambigConfidence = 0.9016288116962997,
      relatednessToContext = 0.06215413428961055,
      relatednessToOtherTopics = 0.048611439367950836,
      avgLinkProbability = 0.0254642499722006,
      maxLinkProbability = 0.0254642499722006,
      firstOccurrence = 0.6114725800026557,
      lastOccurrence = 0.6114725800026557,
      spread = 0.0
    )

    val res = detector.predict(Array(input)).head
    // Very unlikely link, according to features
    approximatelyEqual(res.prediction, 0.0) shouldBe true
  }

  private def approximatelyEqual(a: Double, b: Double, epsilon: Double = 0.01): Boolean =
    (a - b).abs < epsilon

  private lazy val detector = {
    val model = FileHelpers.readBinaryFile("src/test/resources/wiki_en_link_validity.cbm")
    new LinkDetector(model)
  }
}
