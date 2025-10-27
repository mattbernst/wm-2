package wiki.extractor

import wiki.extractor.language.types.CaseContext.MIXED
import wiki.extractor.language.types.NGram
import wiki.util.UnitSpec

class ContextualizerSpec extends UnitSpec {
  behavior of "ArticleComparer.filterShadowed"

  it should "retain only unshadowed NGrams (1)" in {
    val input = Array(
      NGram(
        start = 0,
        end = 5,
        tokenSpans = Array(),
        caseContext = MIXED,
        stringContent = "Namco",
        isSentenceStart = true,
        isDowncased = false
      ),
      NGram(
        start = 15,
        end = 19,
        tokenSpans = Array(),
        caseContext = MIXED,
        stringContent = "Pac-",
        isSentenceStart = false,
        isDowncased = false
      ),
      NGram(
        start = 15,
        end = 22,
        tokenSpans = Array(),
        caseContext = MIXED,
        stringContent = "Pac-Man",
        isSentenceStart = false,
        isDowncased = false
      ),
      NGram(
        start = 19,
        end = 22,
        tokenSpans = Array(),
        caseContext = MIXED,
        stringContent = "Man",
        isSentenceStart = false,
        isDowncased = false
      )
    )

    val expected = Array(
      NGram(
        start = 0,
        end = 5,
        tokenSpans = Array(),
        caseContext = MIXED,
        stringContent = "Namco",
        isSentenceStart = true,
        isDowncased = false
      ),
      NGram(
        start = 15,
        end = 22,
        tokenSpans = Array(),
        caseContext = MIXED,
        stringContent = "Pac-Man",
        isSentenceStart = false,
        isDowncased = false
      )
    )

    val result = Contextualizer.filterShadowed(input)
    result shouldBe expected
  }
}
