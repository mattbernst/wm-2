package wiki.ml

import wiki.util.{FileHelpers, UnitSpec}

import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import scala.util.Random

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

  it should "predict the correct sense from features (2)" in {
    val candidates = Array(
      WordSenseCandidate(
        commonness = 0.16326530612244897,
        inLinkVectorMeasure = 0.0,
        outLinkVectorMeasure = 0.21871832710927375,
        inLinkGoogleMeasure = -0.36415478867148776,
        outLinkGoogleMeasure = -0.16992540277374385,
        pageId = 1
      ),
      WordSenseCandidate(
        commonness = 0.06802721088435375,
        inLinkVectorMeasure = 0.0,
        outLinkVectorMeasure = 0.09373199089254683,
        inLinkGoogleMeasure = -0.36415478867148776,
        outLinkGoogleMeasure = -0.27827771604222923,
        pageId = 2
      ),
      WordSenseCandidate(
        commonness = 0.3877551020408163,
        inLinkVectorMeasure = 0.03125,
        outLinkVectorMeasure = 0.12487538463931357,
        inLinkGoogleMeasure = -0.3498486448817889,
        outLinkGoogleMeasure = -0.25032479076402975,
        pageId = 3
      ),
      WordSenseCandidate(
        commonness = 0.12244897959183673,
        inLinkVectorMeasure = 0.0,
        outLinkVectorMeasure = 0.0,
        inLinkGoogleMeasure = -0.36415478867148776,
        outLinkGoogleMeasure = -0.36415478867148776,
        pageId = 4
      ),
      WordSenseCandidate(
        commonness = 0.23809523809523808,
        inLinkVectorMeasure = 0.9683371355146617,
        outLinkVectorMeasure = 0.12496079878060279,
        inLinkGoogleMeasure = 0.33444435738491024,
        outLinkGoogleMeasure = -0.2562786574779584,
        pageId = 5
      )
    )

    val input = WordSenseGroup(contextQuality = 31.0, candidates = candidates)
    wsd.getBestSense(input) shouldBe 5
  }

  it should "always return the first sense if there is only one sense" in {
    def randomGroup(pageId: Int): WordSenseGroup =
      WordSenseGroup(
        contextQuality = Random.nextDouble(),
        candidates = Array(
          WordSenseCandidate(
            commonness = Random.nextDouble(),
            inLinkVectorMeasure = Random.nextDouble(),
            outLinkVectorMeasure = Random.nextDouble(),
            inLinkGoogleMeasure = Random.nextDouble(),
            outLinkGoogleMeasure = Random.nextDouble(),
            pageId = pageId
          )
        )
      )

    0.until(100).par.foreach { _ =>
      val pageId = randomInt()
      wsd.getBestSense(randomGroup(pageId)) shouldBe pageId
    }
  }

  it should "throw an error on duplicate senses" in {
    val wsc = WordSenseCandidate(
      commonness = 0.08666215301286391,
      inLinkVectorMeasure = 0.6085386512324241,
      outLinkVectorMeasure = 0.1734285926053662,
      inLinkGoogleMeasure = -0.17627708926535277,
      outLinkGoogleMeasure = -0.19588538270606481,
      pageId = 1
    )

    val candidates = Array(wsc, wsc)
    val input      = WordSenseGroup(contextQuality = 1.0, candidates = candidates)
    assertThrows[IllegalArgumentException] {
      wsd.getBestSense(input)
    }
  }

  it should "throw an error on empty senses" in {
    assertThrows[IllegalArgumentException] {
      wsd.getBestSense(WordSenseGroup(contextQuality = 1.0, candidates = Array()))
    }
  }

  lazy val wsd = {
    val model = FileHelpers.readBinaryFile("src/test/resources/wiki_en_word_sense_ranker.cbm")
    new WordSenseDisambiguator(model)
  }
}
