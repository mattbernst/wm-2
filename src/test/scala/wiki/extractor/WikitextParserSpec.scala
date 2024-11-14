package wiki.extractor

import de.fau.cs.osr.utils.visitor.VisitingException
import wiki.extractor.language.EnglishLanguageLogic
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

    parsed.links shouldBe expectedLinks
    val expectedText = FileHelpers.readTextFile("src/test/resources/mafic.txt")
    parsed.text shouldBe expectedText

    // The original markup for this sentence was
    // "Most mafic-lava volcanoes are [[shield volcano]]es, like those in [[Hawaii]]."
    // Appears to be Sweble error (node list from raw parse loses "es" also)
    // Related to special handling of blend links?
    // https://en.wikipedia.org/wiki/Help:Wikitext#Blend_link
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

  private lazy val parser = new WikitextParser(EnglishLanguageLogic)
}
