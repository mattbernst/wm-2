package wiki.ml

import wiki.extractor.language.types.{CaseContext, NGram}
import wiki.util.{FileHelpers, UnitSpec}

class LinkDetectorSpec extends UnitSpec {
  behavior of "predict"

  it should "handle empty input" in {
    detector.predict(Array()) shouldBe Array()
  }

  it should "predict link-worthiness from features (1)" in {
    val input = LabelLinkFeatures(
      label = NGram(
        start = 2,
        end = 18,
        tokenSpans = Array(),
        caseContext = CaseContext.MIXED,
        stringContent = "transition metal",
        isSentenceStart = false
      ),
      linkedPageId = randomInt(),
      normalizedOccurrences = 1.6094379124341003,
      maxDisambigConfidence = 6.361778236171788,
      avgDisambigConfidence = 1.6183168756885833,
      relatednessToContext = 0.3485844618885575,
      relatednessToOtherTopics = 0.36619919729558004,
      linkProbability = 0.7222605099931082,
      firstOccurrence = 0.15125918973877678,
      lastOccurrence = 0.9516658845612389,
      spread = 0.8004066948224622
    )

    val res = detector.predict(Array(input)).head
    // Detected as likely link from features
    approximatelyEqual(res.prediction, 0.76) shouldBe true
  }

  it should "predict link-worthiness from features (2)" in {
    val input = LabelLinkFeatures(
      label = NGram(
        start = 2,
        end = 18,
        tokenSpans = Array(),
        caseContext = CaseContext.MIXED,
        stringContent = "transition state",
        isSentenceStart = false
      ),
      linkedPageId = randomInt(),
      normalizedOccurrences = 1.791759469228055,
      maxDisambigConfidence = 0.055369234205536216,
      avgDisambigConfidence = -2.7327749246020914,
      relatednessToContext = 0.1965921754552392,
      relatednessToOtherTopics = 0.20174607117995083,
      linkProbability = 0.008542702710329909,
      firstOccurrence = 0.003910527139058345,
      lastOccurrence = 0.9974972626310027,
      spread = 0.9935867354919443
    )

    val res = detector.predict(Array(input)).head
    // Very unlikely link, according to features
    approximatelyEqual(res.prediction, 0.0) shouldBe true
  }

  private def approximatelyEqual(a: Double, b: Double, epsilon: Double = 0.01): Boolean =
    (a-b).abs < epsilon

  lazy val detector = {
    val model = FileHelpers.readBinaryFile("src/test/resources/wiki_en_link_validity.cbm")
    new LinkDetector(model)
  }
}
