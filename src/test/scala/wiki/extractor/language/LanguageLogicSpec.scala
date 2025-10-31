package wiki.extractor.language

import wiki.db.Storage
import wiki.extractor.WikitextParser
import wiki.extractor.language.types.Snippet
import wiki.extractor.types.{Language, TrainingProfile}
import wiki.util.UnitSpec

class LanguageLogicSpec extends UnitSpec {
  behavior of "EnglishLanguageLogic.getSnippet"

  it should "get the first sentence and the first paragraph" in {
    val input =
      "Pierre Vinken, 61 years old, will join the board as a nonexecutive director Nov. 29. " +
        "Mr. Vinken is chairman of Elsevier N.V., the Dutch publishing group. " +
        "Rudolph Agnew, 55 years old and former chairman of Consolidated Gold Fields PLC, " +
        "was named a director of this British industrial conglomerate."
    val snippet = englishLanguageLogic.getSnippet(input)
    snippet.firstParagraph shouldBe Some(input)
    snippet.firstSentence shouldBe Some(
      "Pierre Vinken, 61 years old, will join the board as a nonexecutive director Nov. 29."
    )
  }

  it should "gracefully handle empty input" in {
    val snippet = englishLanguageLogic.getSnippet("")
    snippet shouldBe Snippet(firstParagraph = None, firstSentence = None)
  }

  it should "skip the initial sentence if a full paragraph starts later" in {
    val input =
      """Chemically, mafic rocks are on the other side of the rock spectrum from the felsic rocks.
        |Mafic is an adjective describing a silicate mineral or rock that is rich in magnesium and iron. Most mafic minerals are dark in color. Common mafic rocks include basalt, dolerite and gabbro.
        |""".stripMargin

    val snippet = englishLanguageLogic.getSnippet(input)
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

    val snippet = englishLanguageLogic.getSnippet(input)
    snippet.firstParagraph shouldBe None
    snippet.firstSentence shouldBe Some(
      "Chemically, mafic rocks are on the other side of the rock spectrum from the felsic rocks."
    )
  }

  it should "handle wikitext input, discarding beginning-of-article images" in {
    val wt =
      """{{Short description|Harbor on the island of Oahu, Hawaii}}
        |{{About||its current operations as a military base|Joint Base Pearl Harbor–Hickam|the attack operation in 1941|Attack on Pearl Harbor|other uses}}
        |{{pp-move}}
        |{{pp-semi-indef}}
        |{{Use mdy dates|date=November 2023}}
        |{{Infobox body of water
        || pushpin_map = Hawaii#Pacific Ocean
        || coordinates = {{coord|21.3679|-157.9771|type:waterbody_region:US|display=inline,title}}
        || image_map = {{infobox mapframe | stroke-width = 1 }}
        |}}
        |[[File:Ford Island aerial photo RIMPAC 1986.JPEG|thumb|Seen in 1986 with [[Ford Island]] in center. The [[USS Arizona Memorial]] is the small white dot on the left side above Ford Island.]]
        |'''Pearl Harbor''' is a [[lagoon]] [[harbor]] on the island of [[Oahu]], Hawaii, United States, west of [[Honolulu]]. It was often visited by the naval fleet of the [[United States]], before it was acquired from the [[Hawaiian Kingdom]] by the U.S. with the signing of the [[Reciprocity Treaty of 1875]]. Much of the harbor and surrounding lands are now a [[United States Navy]] deep-water naval base. It is also the headquarters of the [[United States Pacific Fleet]]. The U.S. government first obtained exclusive use of the inlet and the right to maintain a repair and coaling station for ships here in 1887.<ref>{{cite web | url=https://www.history.navy.mil/research/library/online-reading-room/title-list-alphabetically/u/the-us-navy-and-hawaii-a-historical-summary/pearl-harbor-its-origin-and-administrative-history.html | title=Pearl Harbor: Its Origin and Administrative History Through World War II | publisher=Naval History and Heritage Command | date=April 23, 2015 | access-date=September 9, 2016 | archive-date=August 21, 2016 | archive-url=https://web.archive.org/web/20160821051252/http://www.history.navy.mil/research/library/online-reading-room/title-list-alphabetically/u/the-us-navy-and-hawaii-a-historical-summary/pearl-harbor-its-origin-and-administrative-history.html | url-status=live }}</ref> The [[Attack on Pearl Harbor|surprise attack]] on the harbor by the [[Imperial Japanese Navy]] on December 7, 1941, led the United States to [[United States declaration of war on Japan|declare war]] on the [[Empire of Japan]], marking the [[American entry into World War II|United States' entry into World War II]].<ref>{{cite video |url=https://www.youtube.com/watch?v=CrVI6ENDL8Y  |archive-url=https://web.archive.org/web/20100724155011/http://www.youtube.com//watch?v=CrVI6ENDL8Y |archive-date=2010-07-24 |url-status=dead|title=FDR Pearl Harbor Speech |date=December 8, 1941 | access-date=2011-02-05 |quote=December 7th, 1941, a day that will live in infamy.}}</ref><ref name = nrhpinv>{{Cite web | last = Apple | first = Russell A. | author2 = Benjamin Levy | title = Pearl Harbor | work = National Register of Historic Places – Nomination and Inventory | publisher = [[National Park Service]] | date = February 8, 1974 | url = https://npgallery.nps.gov/NRHP/GetAsset/NHLS/66000940_text | format = pdf | access-date = 25 May 2012 | archive-date = June 16, 2023 | archive-url = https://web.archive.org/web/20230616032031/https://npgallery.nps.gov/NRHP/GetAsset/NHLS/66000940_text | url-status = live }}</ref><ref name = nrhpphotos>{{Cite web | title = Pearl Harbor | work = Photographs | publisher = [[National Park Service]] | url = https://npgallery.nps.gov/NRHP/GetAsset/NHLS/66000940_photos | format = pdf | access-date = 25 May 2012 | archive-date = June 16, 2023 | archive-url = https://web.archive.org/web/20230616032032/https://npgallery.nps.gov/NRHP/GetAsset/NHLS/66000940_photos | url-status = live }}</ref>""".stripMargin

    val expected = Snippet(
      firstParagraph = Some(
        value =
          "Pearl Harbor is a lagoon harbor on the island of Oahu, Hawaii, United States, west of Honolulu. It was often visited by the naval fleet of the United States, before it was acquired from the Hawaiian Kingdom by the U.S. with the signing of the Reciprocity Treaty of 1875. Much of the harbor and surrounding lands are now a United States Navy deep-water naval base. It is also the headquarters of the United States Pacific Fleet. The U.S. government first obtained exclusive use of the inlet and the right to maintain a repair and coaling station for ships here in 1887. The surprise attack on the harbor by the Imperial Japanese Navy on December 7, 1941, led the United States to declare war on the Empire of Japan, marking the United States' entry into World War II."
      ),
      firstSentence = Some(
        value = "Pearl Harbor is a lagoon harbor on the island of Oahu, Hawaii, United States, west of Honolulu."
      )
    )

    val wp      = new WikitextParser(englishLanguageLogic)
    val parsed  = wp.parse("Pearl Harbor", wt)
    val snippet = englishLanguageLogic.getSnippet(parsed)

    snippet shouldBe expected
  }

  behavior of "FrenchLanguageLogic.getSnippet"

  it should "gracefully handle empty input" in {
    val snippet = frenchLanguageLogic.getSnippet("")
    snippet shouldBe Snippet(firstParagraph = None, firstSentence = None)
  }

  it should "get the first sentence and the first paragraph" in {
    val input =
      "Le 1er novembre 2000, Sega Enterprises, Ltd. change son nom en Sega Corporation. En France, la devise de Sega est « Sega, c’est plus fort que toi ! »."
    val snippet = frenchLanguageLogic.getSnippet(input)
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
      "Mercury has been smelted from cinnabar since antiquity.",
      "mercury has been smelted from cinnabar since antiquity.",
      "has",
      "has been",
      "has been smelted",
      "has been smelted from",
      "has been smelted from cinnabar",
      "has been smelted from cinnabar since",
      "has been smelted from cinnabar since antiquity",
      "has been smelted from cinnabar since antiquity.",
      "been",
      "been smelted",
      "been smelted from",
      "been smelted from cinnabar",
      "been smelted from cinnabar since",
      "been smelted from cinnabar since antiquity",
      "been smelted from cinnabar since antiquity.",
      "smelted",
      "smelted from",
      "smelted from cinnabar",
      "smelted from cinnabar since",
      "smelted from cinnabar since antiquity",
      "smelted from cinnabar since antiquity.",
      "from",
      "from cinnabar",
      "from cinnabar since",
      "from cinnabar since antiquity",
      "from cinnabar since antiquity.",
      "cinnabar",
      "cinnabar since",
      "cinnabar since antiquity",
      "cinnabar since antiquity.",
      "since",
      "since antiquity",
      "since antiquity.",
      "antiquity",
      "antiquity.",
      ".",
      "It",
      "it",
      "It dissolves",
      "it dissolves",
      "It dissolves many",
      "it dissolves many",
      "It dissolves many metals",
      "it dissolves many metals",
      "It dissolves many metals.",
      "it dissolves many metals.",
      "dissolves",
      "dissolves many",
      "dissolves many metals",
      "dissolves many metals.",
      "many",
      "many metals",
      "many metals.",
      "metals",
      "metals.",
      "."
    )

    val nGrams = englishLanguageLogic.wordNGrams(language = englishLanguage, documentText = input)
    nGrams.map(_.stringContent).toList shouldBe expected.toList
  }

  private lazy val lm                   = new LanguageModel(Storage.getTestDb())
  private lazy val englishLanguageLogic = new EnglishLanguageLogic(lm)
  private lazy val frenchLanguageLogic  = new FrenchLanguageLogic(lm)

  private lazy val englishLanguage = Language(
    code = "en",
    name = "English",
    disambiguationPrefixes = Seq("disambiguation", "disambig", "geodis"),
    trainingProfile = TrainingProfile.empty
  )
}
