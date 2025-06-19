package wiki.ml

import wiki.util.{FileHelpers, UnitSpec}

class WordSenseDisambiguatorSpec extends UnitSpec {
  it should "predict the correct sense from features (1)" in {
    val candidates = Array(
      WordSenseCandidate(
        commonness = 0.08666215301286391,
        inLinkVectorMeasure = 0.6085386512324241,
        outLinkVectorMeasure = 0.1734285926053662,
        inLinkGoogleMeasure = -0.17627708926535277,
        outLinkGoogleMeasure = -0.19588538270606481,
        pageId = 1
      ),
      WordSenseCandidate(
        commonness = 0.8392010832769127,
        inLinkVectorMeasure = 0.8260383567579118,
        outLinkVectorMeasure = 0.4759820742580498,
        inLinkGoogleMeasure = -0.041630234217986915,
        outLinkGoogleMeasure = -0.007006403184876254,
        pageId = 2
      ),
      WordSenseCandidate(
        commonness = 0.07413676371022343,
        inLinkVectorMeasure = 0.5650244197262115,
        outLinkVectorMeasure = 0.30393905763370915,
        inLinkGoogleMeasure = -0.16823953632113248,
        outLinkGoogleMeasure = -0.11158807159167264,
        pageId = 3
      )
    )

    val input = WordSenseGroup(contextQuality = 17.0, candidates = candidates)
    wsd.getBestSense(input) shouldBe 2
  }

  lazy val wsd = {
    val model = FileHelpers.readBinaryFile("src/test/resources/wiki_en_word_sense_ranker.cbm")
    new WordSenseDisambiguator(model)
  }
}
