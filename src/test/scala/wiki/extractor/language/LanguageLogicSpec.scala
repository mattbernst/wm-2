package wiki.extractor.language

import wiki.extractor.util.UnitSpec

class LanguageLogicSpec extends UnitSpec {
  behavior of "EnglishLanguageLogic.getSnippet"

  it should "get the first sentence and the first paragraph" in {
    val input =
      "Mafic is an adjective describing a silicate mineral or rock that is rich in magnesium and iron. Most mafic minerals are dark in color. Common mafic rocks include basalt, dolerite and gabbro."
    val snippet = EnglishLanguageLogic.getSnippet(input)
    snippet.firstParagraph shouldBe Some(input)
    snippet.firstSentence shouldBe Some(
      "Mafic is an adjective describing a silicate mineral or rock that is rich in magnesium and iron."
    )
  }

  it should "gracefully handle empty input" in {
    val snippet = EnglishLanguageLogic.getSnippet("")
    snippet shouldBe Snippet(firstParagraph = None, firstSentence = None)
  }

  it should "skip the initial sentence if a full paragraph starts later" in {
    val input =
      """Chemically, mafic rocks are on the other side of the rock spectrum from the felsic rocks.
        |Mafic is an adjective describing a silicate mineral or rock that is rich in magnesium and iron. Most mafic minerals are dark in color. Common mafic rocks include basalt, dolerite and gabbro.
        |""".stripMargin

    val snippet = EnglishLanguageLogic.getSnippet(input)
    val efp =
      "Mafic is an adjective describing a silicate mineral or rock that is rich in magnesium and iron. Most mafic minerals are dark in color. Common mafic rocks include basalt, dolerite and gabbro."
    snippet.firstParagraph shouldBe Some(efp)
    snippet.firstSentence shouldBe Some(
      "Mafic is an adjective describing a silicate mineral or rock that is rich in magnesium and iron."
    )
  }

  it should "get the first sentence alone if there is no multi-sentence paragraph" in {
    val input =
      """Chemically, mafic rocks are on the other side of the rock spectrum from the felsic rocks.
        |""".stripMargin

    val snippet = EnglishLanguageLogic.getSnippet(input)
    snippet.firstParagraph shouldBe None
    snippet.firstSentence shouldBe Some(
      "Chemically, mafic rocks are on the other side of the rock spectrum from the felsic rocks."
    )
  }
}
