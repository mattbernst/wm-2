package wiki.extractor

import wiki.extractor.util.UnitSpec

class ArticleComparerSpec extends UnitSpec {
  behavior of "ArticleComparer.googleMeasure"

  it should "measure total similarity for identical inputs" in {
    val a = randomInts(5).toArray
    ArticleComparer.googleMeasure(a, a, articleCount) shouldBe 1.0
  }

  it should "measure 0 similarity for empty inputs" in {
    ArticleComparer.googleMeasure(Array(), Array(), articleCount) shouldBe 0.0
  }

  it should "measure 0 similarity for non-overlapping inputs" in {
    val a = randomInts(5).toArray
    val b = randomInts(10).filterNot(e => a.contains(e)).toArray

    ArticleComparer.googleMeasure(a, b, articleCount) shouldBe 0.0
  }

  it should "measure partial similarity for partially overlapping inputs (1)" in {
    val a = randomInts(10).toArray
    val b = a.take(5)

    ArticleComparer.googleMeasure(a, b, articleCount) should be > 0.93
  }

  it should "measure partial similarity for partially overlapping inputs (2)" in {
    val a = randomInts(10).toArray
    val b = a.take(5)

    // With a smaller article count, a partial overlap of 5
    // indicates less similarity than with the default 500000
    val smallerCount = 1_000
    ArticleComparer.googleMeasure(a, b, smallerCount) should be > 0.86
    ArticleComparer.googleMeasure(a, b, smallerCount) should be < 0.87
  }

  it should "measure partial similarity for partially overlapping inputs (3)" in {
    val a = randomInts(10).toArray
    val b = a.take(1)

    // Smaller overlap, smaller similarity
    ArticleComparer.googleMeasure(a, b, articleCount) should be > 0.82
    ArticleComparer.googleMeasure(a, b, articleCount) should be < 0.83
  }

  it should "measure partial similarity for partially overlapping inputs (4)" in {
    val a = randomInts(100).toArray
    val b = a.take(10)

    // Identical percentage of overlap, but more links for both, so
    // similarity is smaller.
    ArticleComparer.googleMeasure(a, b, articleCount) should be > 0.78
    ArticleComparer.googleMeasure(a, b, articleCount) should be < 0.79
  }

  private lazy val articleCount = 500_000
}
