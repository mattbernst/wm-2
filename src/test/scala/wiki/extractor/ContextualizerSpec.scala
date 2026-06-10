package wiki.extractor

import opennlp.tools.util.Span
import wiki.extractor.language.types.CaseContext.{LOWER, MIXED, UPPER, UPPER_FIRST}
import wiki.extractor.language.types.{CaseContext, NGram}
import wiki.extractor.types.{Language, TrainingProfile}
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

  it should "retain a recased variant that shares a span with a natural NGram" in {
    val natural = ngram("iceland", LOWER, isRecased = false)
    val recased = natural.copy(stringContent = "Iceland", isRecased = true)

    // Both occupy the same span. filterShadowed should keep whichever one is
    // passed in (it does not drop a single NGram), and the caller buckets them
    // separately, so both survive when bucketed by recasing.
    Contextualizer.filterShadowed(Array(natural)) shouldBe Array(natural)
    Contextualizer.filterShadowed(Array(recased)) shouldBe Array(recased)
  }

  behavior of "Contextualizer.labelVariants"

  it should "recase an NGram whose natural form is not a usable label" in {
    // "iceland" is technically a known label but too rare to pass thresholds,
    // so it is not usable. It must still be expanded to the strong canonical
    // "Iceland" rather than being kept as-is and later filtered away.
    val out = Contextualizer.labelVariants(ngram("iceland", LOWER), language, isUsableLabel = _ == "Iceland")
    out.map(_.stringContent).toSeq shouldBe Seq("iceland", "Iceland")
  }

  it should "keep a usable natural label as-is, without recasing" in {
    // "apple" the fruit is a meaningful lowercase label, so its case is
    // preserved as a word-sense signal and it is not forced to "Apple".
    val out = Contextualizer.labelVariants(ngram("apple", LOWER), language, isUsableLabel = _ == "apple")
    out.map(_.stringContent).toSeq shouldBe Seq("apple")
  }

  it should "recase a multi-word phrase to its sentence-cased canonical label" in {
    // Wikipedia article titles are sentence-cased ("Blue-gray tanager"), not
    // title-cased ("Blue-Gray Tanager"). A lowercase multi-word phrase must
    // therefore be expanded to its sentence-cased form so it can match.
    val ng = multiTokenNgram(
      "blue-gray tanager",
      Array(new Span(0, 4), new Span(4, 5), new Span(5, 9), new Span(10, 17)),
      LOWER
    )
    val out = Contextualizer.labelVariants(ng, language, isUsableLabel = _ == "Blue-gray tanager")
    out.map(_.stringContent).toSeq should contain("Blue-gray tanager")
  }

  behavior of "Contextualizer.caseVariants"

  it should "title-case a lowercase single-token NGram" in {
    val variants = Contextualizer.caseVariants(ngram("iceland", LOWER), language)
    variants.map(_.stringContent).toSeq shouldBe Seq("Iceland")
    variants.head.isRecased shouldBe true
    variants.head.isDowncased shouldBe false
  }

  it should "title-case an uppercase single-token NGram" in {
    val variants = Contextualizer.caseVariants(ngram("ICELAND", UPPER), language)
    variants.map(_.stringContent).toSeq shouldBe Seq("Iceland")
  }

  it should "produce both title-cased and sentence-cased variants of a multi-token NGram" in {
    // Wikipedia uses title case for some multi-word titles ("New York") and
    // sentence case for others ("Blue-gray tanager"), so both candidates are
    // generated and the usable one is selected downstream.
    Contextualizer
      .caseVariants(multiTokenNgram("new york", Array(new Span(0, 3), new Span(4, 8)), LOWER), language)
      .map(_.stringContent)
      .toSeq should contain theSameElementsAs Seq("New York", "New york")

    Contextualizer
      .caseVariants(multiTokenNgram("NEW YORK", Array(new Span(0, 3), new Span(4, 8)), UPPER), language)
      .map(_.stringContent)
      .toSeq should contain theSameElementsAs Seq("New York", "New york")
  }

  it should "produce no variant for MIXED or UPPER_FIRST NGrams" in {
    Contextualizer.caseVariants(ngram("iPhone", MIXED), language) shouldBe Array.empty[NGram]
    Contextualizer.caseVariants(ngram("Iceland", UPPER_FIRST), language) shouldBe Array.empty[NGram]
  }

  it should "produce no variant when title-casing would not change the surface form" in {
    // A single lowercase letter title-cases to itself only if already capital;
    // a token with no cased characters (digits) is unchanged.
    Contextualizer.caseVariants(ngram("2024", LOWER), language) shouldBe Array.empty[NGram]
  }

  private def ngram(content: String, caseContext: CaseContext, isRecased: Boolean = false): NGram =
    NGram(
      start = 0,
      end = content.length,
      tokenSpans = Array(new Span(0, content.length)),
      caseContext = caseContext,
      stringContent = content,
      isSentenceStart = false,
      isDowncased = false,
      isRecased = isRecased
    )

  private def multiTokenNgram(content: String, tokenSpans: Array[Span], caseContext: CaseContext): NGram =
    NGram(
      start = 0,
      end = content.length,
      tokenSpans = tokenSpans,
      caseContext = caseContext,
      stringContent = content,
      isSentenceStart = false,
      isDowncased = false
    )

  private lazy val language = Language(
    code = "en",
    name = "English",
    disambiguationPrefixes = Seq("disambiguation", "disambig", "geodis"),
    trainingProfile = TrainingProfile.empty
  )
}
