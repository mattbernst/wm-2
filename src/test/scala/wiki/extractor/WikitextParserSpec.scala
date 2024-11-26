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
  }

  it should "filter out File: links" in {
    val title  = "Star formation"
    val markup = FileHelpers.readTextFile("src/test/resources/star_formation.wikitext")
    val parsed = parser.parseMarkup(title, markup).get

    val expectedLinks = Seq(
      Link(target = "molecular cloud", anchorText = "molecular cloud"),
      Link(target = "gravitation", anchorText = "gravitation"),
      Link(target = "Plasma (physics)", anchorText = "plasma"),
      Link(target = "star", anchorText = "star"),
      Link(target = "Hubble telescope", anchorText = "Hubble telescope"),
      Link(target = "Pillars of Creation", anchorText = "Pillars of Creation"),
      Link(target = "Eagle nebula", anchorText = "Eagle nebula"),
      Link(target = "interstellar medium", anchorText = "interstellar medium"),
      Link(target = "nebula", anchorText = "nebulae"),
      Link(target = "molecular cloud", anchorText = "molecular cloud"),
      Link(target = "solar mass", anchorText = "solar mass"),
      Link(target = "Milky Way", anchorText = "Milky Way"),
      Link(target = "Sun", anchorText = "Sun"),
      Link(target = "Orion nebula", anchorText = "Orion nebula"),
      Link(target = "Rho Ophiuchi cloud complex", anchorText = "ρ Ophiuchi cloud complex"),
      Link(target = "Bok globule", anchorText = "Bok globule"),
      Link(target = "solar mass", anchorText = "solar mass"),
      Link(target = "emission nebula", anchorText = "emission nebula"),
      Link(target = "spiral galaxy", anchorText = "spiral galaxy"),
      Link(target = "Milky Way", anchorText = "Milky Way"),
      Link(target = "star", anchorText = "star"),
      Link(target = "interstellar medium", anchorText = "interstellar medium"),
      Link(target = "hydrogen", anchorText = "hydrogen"),
      Link(target = "helium", anchorText = "helium"),
      Link(target = "main sequence", anchorText = "main sequence"),
      Link(target = "elliptical galaxy", anchorText = "elliptical galaxy"),
      Link(target = "billion", anchorText = "billion"),
      Link(target = "protostar", anchorText = "protostar"),
      Link(target = "Sun", anchorText = "Sun"),
      Link(target = "radio telescope", anchorText = "radio telescope"),
      Link(target = "light-years", anchorText = "light-years"),
      Link(target = "stellar wind", anchorText = "stellar wind"),
      Link(target = "spherical", anchorText = "spherical"),
      Link(target = "ionization", anchorText = "ionized"),
      Link(target = "ellipse", anchorText = "elliptical"),
      Link(target = "Leiden University", anchorText = "Leiden University"),
      Link(target = "Astronomical object", anchorText = "object"),
      Link(target = "Category:Stars", anchorText = "")
    )

    parsed.links shouldBe expectedLinks
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
