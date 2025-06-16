package wiki.extractor.language

import wiki.extractor.language.types.Snippet
import wiki.extractor.types.Language
import wiki.extractor.util.UnitSpec

class LanguageLogicSpec extends UnitSpec {
  behavior of "EnglishLanguageLogic.getSnippet"

  it should "get the first sentence and the first paragraph" in {
    val input =
      "Pierre Vinken, 61 years old, will join the board as a nonexecutive director Nov. 29. " +
        "Mr. Vinken is chairman of Elsevier N.V., the Dutch publishing group. " +
        "Rudolph Agnew, 55 years old and former chairman of Consolidated Gold Fields PLC, " +
        "was named a director of this British industrial conglomerate."
    val snippet = EnglishLanguageLogic.getSnippet(input)
    snippet.firstParagraph shouldBe Some(input)
    snippet.firstSentence shouldBe Some(
      "Pierre Vinken, 61 years old, will join the board as a nonexecutive director Nov. 29."
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

  behavior of "FrenchLanguageLogic.getSnippet"

  it should "gracefully handle empty input" in {
    val snippet = FrenchLanguageLogic.getSnippet("")
    snippet shouldBe Snippet(firstParagraph = None, firstSentence = None)
  }

  it should "get the first sentence and the first paragraph" in {
    val input =
      "Le 1er novembre 2000, Sega Enterprises, Ltd. change son nom en Sega Corporation. En France, la devise de Sega est « Sega, c’est plus fort que toi ! »."
    val snippet = FrenchLanguageLogic.getSnippet(input)
    snippet.firstParagraph shouldBe Some(input)
    // Note: OpenNLP's answer is wrong here (as it frequently is)
    // Java's BreakIterator handles this correctly but is wrong
    // in other cases that OpenNLP gets right. Is there an across-the-board
    // better sentence splitter? One that doesn't require a large language model?
    snippet.firstSentence shouldBe Some(
      "Le 1er novembre 2000, Sega Enterprises, Ltd."
    )
  }

  behavior of "EnglishLanguageLogic.wordNGrams"

  it should "generate NGrams with beginning-of-sentence casing variants" in {
    val input = "Mercury has been smelted from cinnabar since antiquity. It dissolves many metals."
    val expected = Array(
      "Mercury",
      "mercury",
      "Mercury has",
      "mercury has",
      "Mercury has been",
      "mercury has been",
      "Mercury has been smelted",
      "mercury has been smelted",
      "Mercury has been smelted from",
      "mercury has been smelted from",
      "Mercury has been smelted from cinnabar",
      "mercury has been smelted from cinnabar",
      "Mercury has been smelted from cinnabar since",
      "mercury has been smelted from cinnabar since",
      "Mercury has been smelted from cinnabar since antiquity",
      "mercury has been smelted from cinnabar since antiquity",
      "has",
      "has been",
      "has been smelted",
      "has been smelted from",
      "has been smelted from cinnabar",
      "has been smelted from cinnabar since",
      "has been smelted from cinnabar since antiquity",
      "been",
      "been smelted",
      "been smelted from",
      "been smelted from cinnabar",
      "been smelted from cinnabar since",
      "been smelted from cinnabar since antiquity",
      "smelted",
      "smelted from",
      "smelted from cinnabar",
      "smelted from cinnabar since",
      "smelted from cinnabar since antiquity",
      "from",
      "from cinnabar",
      "from cinnabar since",
      "from cinnabar since antiquity",
      "cinnabar",
      "cinnabar since",
      "cinnabar since antiquity",
      "since",
      "since antiquity",
      "antiquity",
      "It",
      "it",
      "It dissolves",
      "it dissolves",
      "It dissolves many",
      "it dissolves many",
      "It dissolves many metals",
      "it dissolves many metals",
      "dissolves",
      "dissolves many",
      "dissolves many metals",
      "many",
      "many metals",
      "metals"
    )

    val nGrams = EnglishLanguageLogic.wordNGrams(language = englishLanguage, documentText = input)
    nGrams.toList shouldBe expected.toList
  }

  private lazy val englishLanguage = Language(
    code = "en",
    name = "English",
    disambiguationPrefixes = Seq("disambiguation", "disambig", "geodis")
  )
}
