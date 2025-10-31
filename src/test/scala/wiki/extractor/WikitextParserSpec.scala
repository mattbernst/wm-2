package wiki.extractor

import de.fau.cs.osr.utils.visitor.VisitingException
import org.sweble.wikitext.parser.nodes.WtListItem
import wiki.db.Storage
import wiki.extractor.language.{EnglishLanguageLogic, LanguageModel}
import wiki.extractor.types.LocatedLink
import wiki.util.{FileHelpers, UnitSpec}

class WikitextParserSpec extends UnitSpec {
  behavior of "parseMarkup"

  it should "extract links and simplified text (1)" in {
    // This is a simple article without images or tables
    val title  = "Anthophyta"
    val markup = FileHelpers.readTextFile("src/test/resources/anthophyta.wikitext")
    val parsed = parser.parseMarkup(title, markup).get

    val expectedLinks = Seq(
      LocatedLink(target = "paraphyletic", anchorText = "paraphyletic", left = 139, right = 151),
      LocatedLink(target = "clade", anchorText = "clade", left = 256, right = 261),
      LocatedLink(target = "Flowering plants", anchorText = "angiosperm", left = 281, right = 297),
      LocatedLink(target = "Rosaceae", anchorText = "roses", left = 352, right = 360),
      LocatedLink(target = "Poaceae", anchorText = "grasses", left = 375, right = 382),
      LocatedLink(target = "Gnetales", anchorText = "Gnetales", left = 412, right = 420),
      LocatedLink(target = "Bennettitales", anchorText = "Bennettitales", left = 441, right = 454),
      LocatedLink(target = "monophyletic", anchorText = "monophyletic", left = 879, right = 891),
      LocatedLink(target = "gnetophyte", anchorText = "gnetophyte", left = 1626, right = 1636),
      LocatedLink(target = "angiosperm", anchorText = "angiosperm", left = 1650, right = 1660),
      LocatedLink(target = "gymnosperm", anchorText = "gymnosperm", left = 1824, right = 1834),
      LocatedLink(target = "Glossopteridales", anchorText = "glossopterids", left = 2420, right = 2436),
      LocatedLink(target = "Corystospermaceae", anchorText = "corystosperms", left = 2456, right = 2473),
      LocatedLink(target = "Petriellales", anchorText = "Petriellales", left = 2493, right = 2505),
      LocatedLink(target = "Pentoxylales", anchorText = "Pentoxylales", left = 2510, right = 2522),
      LocatedLink(target = "Bennettitales", anchorText = "Bennettitales", left = 2528, right = 2541),
      LocatedLink(target = "Caytoniales", anchorText = "Caytoniales", left = 2550, right = 2561),
      LocatedLink(
        target = "Category:Historically recognized plant taxa",
        anchorText = "Category:Historically recognized plant taxa",
        left = 3663,
        right = 3706
      )
    )

    parsed.links shouldBe expectedLinks
    val expectedText = FileHelpers.readTextFile("src/test/resources/anthophyta.txt")
    parsed.text shouldBe expectedText
  }

  it should "extract links and simplified text (2)" in {
    // This article contains an image and table
    val title  = "Mafic"
    val markup = FileHelpers.readTextFile("src/test/resources/mafic.wikitext")
    val parsed = parser.parseMarkup(title, markup).get

    val expectedLinks = Seq(
      LocatedLink(target = "silicate mineral", anchorText = "silicate mineral", left = 85, right = 101),
      LocatedLink(target = "magnesium", anchorText = "magnesium", left = 130, right = 139),
      LocatedLink(target = "iron", anchorText = "iron", left = 148, right = 152),
      LocatedLink(target = "portmanteau", anchorText = "portmanteau", left = 173, right = 184),
      LocatedLink(target = "ferric", anchorText = "ferric", left = 209, right = 215),
      LocatedLink(target = "olivine", anchorText = "olivine", left = 412, right = 419),
      LocatedLink(target = "pyroxene", anchorText = "pyroxene", left = 425, right = 433),
      LocatedLink(target = "amphibole", anchorText = "amphibole", left = 439, right = 448),
      LocatedLink(target = "biotite", anchorText = "biotite", left = 458, right = 465),
      LocatedLink(target = "basalt", anchorText = "basalt", left = 498, right = 504),
      LocatedLink(target = "dolerite", anchorText = "dolerite", left = 510, right = 518),
      LocatedLink(target = "gabbro", anchorText = "gabbro", left = 527, right = 533),
      LocatedLink(target = "felsic", anchorText = "felsic", left = 616, right = 622),
      LocatedLink(target = "lava", anchorText = "lava", left = 707, right = 711),
      LocatedLink(target = "viscosity", anchorText = "viscosity", left = 743, right = 752),
      LocatedLink(target = "felsic", anchorText = "felsic", left = 775, right = 781),
      LocatedLink(target = "shield volcano", anchorText = "shield volcano", left = 1053, right = 1067),
      LocatedLink(target = "Hawaii", anchorText = "Hawaii", left = 1089, right = 1095),
      LocatedLink(target = "Pegmatite", anchorText = "Pegmatitic", left = 1170, right = 1179),
      LocatedLink(target = "Gabbro", anchorText = "Gabbro", left = 1198, right = 1204),
      LocatedLink(target = "pegmatite", anchorText = "pegmatite", left = 1209, right = 1218),
      LocatedLink(target = "phaneritic", anchorText = "phaneritic", left = 1245, right = 1255),
      LocatedLink(target = "Gabbro", anchorText = "Gabbro", left = 1264, right = 1270),
      LocatedLink(target = "porphyritic", anchorText = "porphyritic", left = 1300, right = 1311),
      LocatedLink(target = "gabbro", anchorText = "gabbro", left = 1331, right = 1337),
      LocatedLink(target = "Diabase", anchorText = "Diabase", left = 1366, right = 1373),
      LocatedLink(target = "Dolerite", anchorText = "Dolerite", left = 1381, right = 1389),
      LocatedLink(target = "aphanitic", anchorText = "aphanitic", left = 1427, right = 1436),
      LocatedLink(target = "Basalt", anchorText = "Basalt", left = 1445, right = 1451),
      LocatedLink(target = "Pyroclastic", anchorText = "Pyroclastic", left = 1519, right = 1530),
      LocatedLink(target = "Basalt", anchorText = "Basalt", left = 1538, right = 1544),
      LocatedLink(target = "tuff", anchorText = "tuff", left = 1549, right = 1553),
      LocatedLink(target = "breccia", anchorText = "breccia", left = 1561, right = 1568),
      LocatedLink(target = "Vesicular texture", anchorText = "Vesicular", left = 1579, right = 1596),
      LocatedLink(target = "basalt", anchorText = "basalt", left = 1624, right = 1630),
      LocatedLink(target = "Amygdule", anchorText = "Amygdaloidal", left = 1641, right = 1649),
      LocatedLink(target = "basalt", anchorText = "basalt", left = 1683, right = 1689),
      LocatedLink(target = "Scoria", anchorText = "Scoria", left = 1723, right = 1729),
      LocatedLink(target = "Tachylyte", anchorText = "Tachylyte", left = 1758, right = 1767),
      LocatedLink(target = "sideromelane", anchorText = "sideromelane", left = 1773, right = 1785),
      LocatedLink(target = "palagonite", anchorText = "palagonite", left = 1791, right = 1801),
      LocatedLink(target = "QAPF diagram", anchorText = "QAPF diagram", left = 1827, right = 1839),
      LocatedLink(
        target = "List of minerals",
        anchorText = "List of minerals",
        left = 1845,
        right = 1861
      ),
      LocatedLink(
        target = "List of rock types",
        anchorText = "List of rock types",
        left = 1867,
        right = 1885
      ),
      LocatedLink(
        target = "Category:Mineralogy",
        anchorText = "Category:Mineralogy",
        left = 1938,
        right = 1957
      ),
      LocatedLink(
        target = "Category:Igneous petrology",
        anchorText = "Category:Igneous petrology",
        left = 1962,
        right = 1988
      )
    )

    val expectedText = FileHelpers.readTextFile("src/test/resources/mafic.txt")
    parsed.text shouldBe expectedText
    parsed.links shouldBe expectedLinks

    // The original markup for this sentence was
    // "Most mafic-lava volcanoes are [[shield volcano]]es, like those in [[Hawaii]]."
    // Appears to be Sweble error (node list from raw parse loses "es" also)
    // Related to special handling of blend links?
    // https://en.wikipedia.org/wiki/Help:Wikitext#Blend_link
    val error1   = "Most mafic-lava volcanoes are shield volcano, like those in Hawaii."
    val correct1 = "Most mafic-lava volcanoes are shield volcanoes, like those in Hawaii."
    parsed.text.contains(error1) shouldBe true
    parsed.text.contains(correct1) shouldBe false
  }

  it should "extract some links from a trickier page" in {
    val title = "B.F. Skinner"
    // This page is missing most of its links from single-pass link extraction.
    // Nonetheless, I reverted the approach that sliced [[link]] sections out
    // of the page and parsed them all individually, because it was slower and
    // led to undesirable false positives.
    val markup = FileHelpers.readTextFile("src/test/resources/bfskinner.wikitext")
    val expected = Seq(
      LocatedLink(target = "B. F. Skinner", anchorText = "Burrhus Frederic", left = 77, right = 90),
      LocatedLink(target = "B. F. Skinner", anchorText = "(B. F.) Skinner", left = 1366, right = 1379),
      LocatedLink(
        target = "Category:Psychology portal",
        anchorText = "Category:Psychology portal",
        left = 1408,
        right = 1434
      )
    )

    val parsed = parser.parseMarkup(title, markup).get
    parsed.links shouldBe expected
  }

  it should "produce readable plain text from tables" in {
    val title = "List of cat breeds"
    val markup =
      """This page lists [[breed]]s of [[domestic cat]]s. The list includes breeds that are old traditional breeds, and also rare breeds or new breeds that are still being developed. Please see individual articles for more information.{{-}}
        |
        |==Breeds==
        |{|class="wikitable sortable"
        |!Breed!!Country!!Origin!!Body type!!Coat!!Pattern!!class="unsortable"|Image
        ||-
        ||[[Abyssinian cat]]||Egypt||Natural||Oriental||Short||Ticked||[[File:Gustav chocolate.jpg|100px]]
        ||-
        ||[[Aegean cat]]||Greece||Natural/Standard||||Semi-long|| Bi- or tri-colored ||[[File:Aegean cat.jpg|100px]]""".stripMargin

    val parsed = parser.parseMarkup(title, markup).get
    val expectedText =
      """This page lists breed of domestic cat. The list includes breeds that are old traditional breeds, and also rare breeds or new breeds that are still being developed. Please see individual articles for more information.
        |
        |Breeds
        |: Breed : Country : Origin : Body type : Coat : Pattern : Image
        || Abyssinian cat | Egypt | Natural | Oriental | Short | Ticked |
        || Aegean cat | Greece | Natural/Standard | ||Semi-long|| Bi- or tri-colored ||""".stripMargin

    parsed.text shouldBe expectedText
  }

  "parse" should "fail on Departments of Nicaragua (VisitingException)" in {
    val title  = "Departments of Nicaragua"
    val markup = FileHelpers.readTextFile("src/test/resources/departments_of_nicaragua.wikitext")
    assertThrows[VisitingException] {
      parser.parse(title, markup)
    }
  }

  "extractNodes" should "extract only nodes of matching type" in {
    val markup = FileHelpers.readTextFile("src/test/resources/mercury.wikitext")
    val parsed = parser.parse("Mercury", markup)

    val listItems = parser.extractNodes[WtListItem](parsed)
    listItems.length shouldBe 116
  }

  private lazy val parser = new WikitextParser(new EnglishLanguageLogic(new LanguageModel(Storage.getTestDb())))
}
