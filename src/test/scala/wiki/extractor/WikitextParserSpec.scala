package wiki.extractor

import de.fau.cs.osr.utils.visitor.VisitingException
import wiki.extractor.language.EnglishSnippetExtractor
import wiki.extractor.types.Link
import wiki.extractor.util.{FileHelpers, UnitSpec}

class WikitextParserSpec extends UnitSpec {
  behavior of "parseMarkup"

  it should "extract links and simplified text (1)" in {
    // This is a simple article without images or tables
    val title  = "Anthophyta"
    val markup = FileHelpers.readTextFile("src/test/resources/anthophyta.wikitext")
    val parsed = parser.parseMarkup(title, markup).get

    val expectedLinks = Seq(
      Link(target = "paraphyletic", anchorText = None),
      Link(target = "clade", anchorText = None),
      Link(target = "Flowering plant", anchorText = Some("angiosperms")),
      Link(target = "Rosaceae", anchorText = Some("roses")),
      Link(target = "Poaceae", anchorText = Some("grasses")),
      Link(target = "Gnetales", anchorText = None),
      Link(target = "Bennettitales", anchorText = None),
      Link(target = "monophyletic", anchorText = None),
      Link(target = "gnetophyte", anchorText = None),
      Link(target = "angiosperm", anchorText = None),
      Link(target = "gymnosperm", anchorText = None),
      Link(target = "Glossopteridales", anchorText = Some("glossopterids")),
      Link(target = "Corystospermaceae", anchorText = Some("corystosperms")),
      Link(target = "Petriellales", anchorText = None),
      Link(target = "Pentoxylales", anchorText = None),
      Link(target = "Bennettitales", anchorText = None),
      Link(target = "Caytoniales", anchorText = None),
      Link(target = "Category:Historically recognized plant taxa", anchorText = None)
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
      Link(target = "silicate mineral", anchorText = None),
      Link(target = "magnesium", anchorText = None),
      Link(target = "iron", anchorText = None),
      Link(target = "portmanteau", anchorText = None),
      Link(target = "ferric", anchorText = None),
      Link(target = "olivine", anchorText = None),
      Link(target = "pyroxene", anchorText = None),
      Link(target = "amphibole", anchorText = None),
      Link(target = "biotite", anchorText = None),
      Link(target = "basalt", anchorText = None),
      Link(target = "dolerite", anchorText = None),
      Link(target = "gabbro", anchorText = None),
      Link(target = "felsic", anchorText = None),
      Link(target = "lava", anchorText = None),
      Link(target = "viscosity", anchorText = None),
      Link(target = "felsic", anchorText = None),
      Link(target = "shield volcano", anchorText = None),
      Link(target = "Hawaii", anchorText = None),
      Link(target = "Pegmatite", anchorText = Some("Pegmatitic")),
      Link(target = "Gabbro", anchorText = None),
      Link(target = "pegmatite", anchorText = None),
      Link(target = "phaneritic", anchorText = None),
      Link(target = "Gabbro", anchorText = None),
      Link(target = "porphyritic", anchorText = None),
      Link(target = "gabbro", anchorText = None),
      Link(target = "Diabase", anchorText = None),
      Link(target = "Dolerite", anchorText = None),
      Link(target = "aphanitic", anchorText = None),
      Link(target = "Basalt", anchorText = None),
      Link(target = "Pyroclastic", anchorText = None),
      Link(target = "Basalt", anchorText = None),
      Link(target = "tuff", anchorText = None),
      Link(target = "breccia", anchorText = None),
      Link(target = "Vesicular texture", anchorText = Some("Vesicular")),
      Link(target = "basalt", anchorText = None),
      Link(target = "Amygdule", anchorText = Some("Amygdaloidal")),
      Link(target = "basalt", anchorText = None),
      Link(target = "Scoria", anchorText = None),
      Link(target = "Tachylyte", anchorText = None),
      Link(target = "sideromelane", anchorText = None),
      Link(target = "palagonite", anchorText = None),
      Link(target = "QAPF diagram", anchorText = None),
      Link(target = "List of minerals", anchorText = None),
      Link(target = "List of rock types", anchorText = None),
      Link(target = "Category:Mineralogy", anchorText = None),
      Link(target = "Category:Igneous petrology", anchorText = None)
    )

    parsed.links shouldBe expectedLinks
    val expectedText = FileHelpers.readTextFile("src/test/resources/mafic.txt")
    parsed.text shouldBe expectedText

    // The original markup for this sentence was
    // "Most mafic-lava volcanoes are [[shield volcano]]es, like those in [[Hawaii]]."
    // Appears to be Sweble error (node list from raw parse loses "es" also)
    val error1   = "Most mafic-lava volcanoes are shield volcano, like those in Hawaii."
    val correct1 = "Most mafic-lava volcanoes are shield volcanoes, like those in Hawaii."
    parsed.text.contains(error1) shouldBe true
    parsed.text.contains(correct1) shouldBe false

    // This is my problem. It's hard to deal with in the current stateless approach.
    // Adding extra space when rendering links messes up the text in different ways.
    // Maybe use a placeholder character from outside the basic multilingual plane to
    // make post-processing work? That's a later todo.
    val error2 = "QAPF diagramList of mineralsList of rock types"
    parsed.text.contains(error2) shouldBe true
  }

  "parse" should "fail on Departments of Nicaragua (VisitingException)" in {
    val title  = "Departments of Nicaragua"
    val markup = FileHelpers.readTextFile("src/test/resources/departments_of_nicaragua.wikitext")
    assertThrows[VisitingException] {
      parser.parse(title, markup)
    }
  }

  private lazy val parser = new WikitextParser(EnglishSnippetExtractor)
}
