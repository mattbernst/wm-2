package wiki.extractor

import de.fau.cs.osr.utils.visitor.VisitingException
import org.sweble.wikitext.parser.nodes.WtListItem
import wiki.extractor.language.EnglishLanguageLogic
import wiki.extractor.types.Link
import wiki.util.{FileHelpers, UnitSpec}

class WikitextParserSpec extends UnitSpec {
  behavior of "parseMarkup"

  it should "extract links and simplified text (1)" in {
    // This is a simple article without images or tables
    val title  = "Anthophyta"
    val markup = FileHelpers.readTextFile("src/test/resources/anthophyta.wikitext")
    val parsed = parser.parseMarkup(title, markup).get

    val expectedLinks = Seq(
      Link(target = "paraphyletic", anchorText = "paraphyletic"),
      Link(target = "clade", anchorText = "clade"),
      Link(target = "Flowering plant", anchorText = "angiosperms"),
      Link(target = "Rosaceae", anchorText = "roses"),
      Link(target = "Poaceae", anchorText = "grasses"),
      Link(target = "Gnetales", anchorText = "Gnetales"),
      Link(target = "Bennettitales", anchorText = "Bennettitales"),
      Link(target = "monophyletic", anchorText = "monophyletic"),
      Link(target = "gnetophyte", anchorText = "gnetophyte"),
      Link(target = "angiosperm", anchorText = "angiosperm"),
      Link(target = "gymnosperm", anchorText = "gymnosperm"),
      Link(target = "Glossopteridales", anchorText = "glossopterids"),
      Link(target = "Corystospermaceae", anchorText = "corystosperms"),
      Link(target = "Petriellales", anchorText = "Petriellales"),
      Link(target = "Pentoxylales", anchorText = "Pentoxylales"),
      Link(target = "Bennettitales", anchorText = "Bennettitales"),
      Link(target = "Caytoniales", anchorText = "Caytoniales"),
      Link(
        target = "Category:Historically recognized plant taxa",
        anchorText = "Category:Historically recognized plant taxa"
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
      Link(target = "silicate mineral", anchorText = "silicate mineral"),
      Link(target = "magnesium", anchorText = "magnesium"),
      Link(target = "iron", anchorText = "iron"),
      Link(target = "portmanteau", anchorText = "portmanteau"),
      Link(target = "ferric", anchorText = "ferric"),
      Link(target = "olivine", anchorText = "olivine"),
      Link(target = "pyroxene", anchorText = "pyroxene"),
      Link(target = "amphibole", anchorText = "amphibole"),
      Link(target = "biotite", anchorText = "biotite"),
      Link(target = "basalt", anchorText = "basalt"),
      Link(target = "dolerite", anchorText = "dolerite"),
      Link(target = "gabbro", anchorText = "gabbro"),
      Link(target = "felsic", anchorText = "felsic"),
      Link(target = "lava", anchorText = "lava"),
      Link(target = "viscosity", anchorText = "viscosity"),
      Link(target = "felsic", anchorText = "felsic"),
      Link(target = "shield volcano", anchorText = "shield volcano"),
      Link(target = "Hawaii", anchorText = "Hawaii"),
      Link(target = "Pegmatite", anchorText = "Pegmatitic"),
      Link(target = "Gabbro", anchorText = "Gabbro"),
      Link(target = "pegmatite", anchorText = "pegmatite"),
      Link(target = "phaneritic", anchorText = "phaneritic"),
      Link(target = "Gabbro", anchorText = "Gabbro"),
      Link(target = "porphyritic", anchorText = "porphyritic"),
      Link(target = "gabbro", anchorText = "gabbro"),
      Link(target = "Diabase", anchorText = "Diabase"),
      Link(target = "Dolerite", anchorText = "Dolerite"),
      Link(target = "aphanitic", anchorText = "aphanitic"),
      Link(target = "Basalt", anchorText = "Basalt"),
      Link(target = "Pyroclastic", anchorText = "Pyroclastic"),
      Link(target = "Basalt", anchorText = "Basalt"),
      Link(target = "tuff", anchorText = "tuff"),
      Link(target = "breccia", anchorText = "breccia"),
      Link(target = "Vesicular texture", anchorText = "Vesicular"),
      Link(target = "basalt", anchorText = "basalt"),
      Link(target = "Amygdule", anchorText = "Amygdaloidal"),
      Link(target = "basalt", anchorText = "basalt"),
      Link(target = "Scoria", anchorText = "Scoria"),
      Link(target = "Tachylyte", anchorText = "Tachylyte"),
      Link(target = "sideromelane", anchorText = "sideromelane"),
      Link(target = "palagonite", anchorText = "palagonite"),
      Link(target = "QAPF diagram", anchorText = "QAPF diagram"),
      Link(target = "List of minerals", anchorText = "List of minerals"),
      Link(target = "List of rock types", anchorText = "List of rock types"),
      Link(target = "Category:Mineralogy", anchorText = "Category:Mineralogy"),
      Link(target = "Category:Igneous petrology", anchorText = "Category:Igneous petrology")
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
        || Abyssinian cat | Egypt | Natural | Oriental | Short | Ticked | File:Gustav chocolate.jpg
        || Aegean cat | Greece | Natural/Standard | ||Semi-long|| Bi- or tri-colored ||File:Aegean cat.jpg""".stripMargin

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

  private lazy val parser = new WikitextParser(EnglishLanguageLogic)
}
