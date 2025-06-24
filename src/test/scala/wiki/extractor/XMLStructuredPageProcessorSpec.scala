package wiki.extractor

import wiki.extractor.types.*
import wiki.util.{FileHelpers, UnitSpec}

class XMLStructuredPageProcessorSpec extends UnitSpec {
  behavior of "fragmentToPage"

  it should "extract standard page fields from a basic article" in {
    val page = fragmentProcessor.extract(FileHelpers.readTextFile("src/test/resources/animalia.xml")).get.page
    page.id shouldBe 332
    page.pageType shouldBe PageType.ARTICLE
    page.title shouldBe "Animalia (book)"
    page.redirectTarget shouldBe None
    page.lastEdited shouldBe 1463339539000L // "2016-05-15T19:12:19Z"
  }

  it should "detect a redirect page" in {
    val page =
      fragmentProcessor.extract(FileHelpers.readTextFile("src/test/resources/accessiblecomputing.xml")).get.page
    page.id shouldBe 10
    page.pageType shouldBe PageType.REDIRECT
    page.title shouldBe "AccessibleComputing"
    page.redirectTarget shouldBe Some("Computer accessibility")
    page.lastEdited shouldBe 1414299023000L
  }

  it should "return an UNHANDLED page type for a Wikipedia: namespace page" in {
    val expected = Page(
      id = 551039,
      namespace = Namespace(id = 4, casing = Casing.FIRST_LETTER, name = "Wikipedia"),
      pageType = PageType.UNHANDLED,
      title = "Wikipedia:WikiProject Missing encyclopedic articles/biographies/G",
      redirectTarget = None,
      lastEdited = 1219519229000L,
      markupSize = None
    )
    val result = fragmentProcessor.extract(FileHelpers.readTextFile("src/test/resources/missing-text.xml")).get.page
    result shouldBe expected
  }

  behavior of "getTransclusions"

  it should "get transclusions (1)" in {
    val wikiText = FileHelpers.readTextFile("src/test/resources/mercury.wikitext")
    val expected = Seq(
      "wiktionary|Mercury|mercury",
      "tocright",
      "HMS|Mercury",
      "USS|Mercury",
      "ship|Russian brig|Mercury",
      "ship||Mercury|ship",
      "Commons|mercury",
      "look from",
      "in title|Mercury",
      "disambiguation|geo"
    )

    fragmentProcessor.getTransclusions(wikiText) shouldBe expected
  }

  it should "get transclusions (2)" in {
    val markup =
      fragmentProcessor.extract(FileHelpers.readTextFile("src/test/resources/accessiblecomputing.xml")).get.markup
    val expected = Seq("Redr|move|from CamelCase|up")

    fragmentProcessor.getTransclusions(markup.wikitext.get) shouldBe expected
  }

  it should "get transclusions (3)" in {
    val markup =
      fragmentProcessor.extract(FileHelpers.readTextFile("src/test/resources/accessiblecomputing.xml")).get.markup
    val expected = Seq("Redr|move|from CamelCase|up")

    fragmentProcessor.getTransclusions(markup.wikitext.get) shouldBe expected
  }

  behavior of "inferPageType"

  it should "detect a DISAMBIGUATION page from page text" in {
    val wikiText = FileHelpers.readTextFile("src/test/resources/mercury.wikitext")
    fragmentProcessor.inferPageType(wikiText, siteInfo.defaultNamespace) shouldBe PageType.DISAMBIGUATION
  }

  it should "detect a CATEGORY page from namespace" in {
    fragmentProcessor.inferPageType("foo", siteInfo.namespaceById(14)) shouldBe PageType.CATEGORY
  }

  it should "detect a TEMPLATE page from namespace" in {
    fragmentProcessor.inferPageType("foo", siteInfo.namespaceById(10)) shouldBe PageType.TEMPLATE
  }

  it should "detect an UNHANDLED page from namespace" in {
    val namespace = Namespace(id = -3, casing = Casing.FIRST_LETTER, name = "Unknown")
    fragmentProcessor.inferPageType("foo", namespace) shouldBe PageType.UNHANDLED
  }

  private lazy val language = Language(
    code = "en",
    name = "English",
    disambiguationPrefixes = Seq("disambiguation", "disambig", "geodis"),
    trainingProfile = TrainingProfile.empty
  )

  private lazy val siteInfo = SiteInfo(
    siteName = "Wikipedia",
    dbName = "enwiki",
    base = "https://en.wikipedia.org/wiki/Main_Page",
    casing = Casing.FIRST_LETTER,
    namespaces = List(
      Namespace(id = -2, casing = Casing.FIRST_LETTER, name = "Media"),
      Namespace(id = -1, casing = Casing.FIRST_LETTER, name = "Special"),
      Namespace(id = 0, casing = Casing.FIRST_LETTER, name = ""),
      Namespace(id = 1, casing = Casing.FIRST_LETTER, name = "Talk"),
      Namespace(id = 2, casing = Casing.FIRST_LETTER, name = "User"),
      Namespace(id = 3, casing = Casing.FIRST_LETTER, name = "User talk"),
      Namespace(id = 4, casing = Casing.FIRST_LETTER, name = "Wikipedia"),
      Namespace(id = 5, casing = Casing.FIRST_LETTER, name = "Wikipedia talk"),
      Namespace(id = 6, casing = Casing.FIRST_LETTER, name = "File"),
      Namespace(id = 7, casing = Casing.FIRST_LETTER, name = "File talk"),
      Namespace(id = 8, casing = Casing.FIRST_LETTER, name = "MediaWiki"),
      Namespace(id = 9, casing = Casing.FIRST_LETTER, name = "MediaWiki talk"),
      Namespace(id = 10, casing = Casing.FIRST_LETTER, name = "Template"),
      Namespace(id = 11, casing = Casing.FIRST_LETTER, name = "Template talk"),
      Namespace(id = 12, casing = Casing.FIRST_LETTER, name = "Help"),
      Namespace(id = 13, casing = Casing.FIRST_LETTER, name = "Help talk"),
      Namespace(id = 14, casing = Casing.FIRST_LETTER, name = "Category"),
      Namespace(id = 15, casing = Casing.FIRST_LETTER, name = "Category talk"),
      Namespace(id = 100, casing = Casing.FIRST_LETTER, name = "Portal"),
      Namespace(id = 101, casing = Casing.FIRST_LETTER, name = "Portal talk"),
      Namespace(id = 118, casing = Casing.FIRST_LETTER, name = "Draft"),
      Namespace(id = 119, casing = Casing.FIRST_LETTER, name = "Draft talk"),
      Namespace(id = 710, casing = Casing.FIRST_LETTER, name = "TimedText"),
      Namespace(id = 711, casing = Casing.FIRST_LETTER, name = "TimedText talk"),
      Namespace(id = 828, casing = Casing.FIRST_LETTER, name = "Module"),
      Namespace(id = 829, casing = Casing.FIRST_LETTER, name = "Module talk"),
      Namespace(id = 2300, casing = Casing.CASE_SENSITIVE, name = "Gadget"),
      Namespace(id = 2301, casing = Casing.CASE_SENSITIVE, name = "Gadget talk"),
      Namespace(id = 2302, casing = Casing.CASE_SENSITIVE, name = "Gadget definition"),
      Namespace(id = 2303, casing = Casing.CASE_SENSITIVE, name = "Gadget definition talk")
    )
  )

  private lazy val fragmentProcessor = new XMLStructuredPageProcessor(siteInfo, language)
}
