package wiki.extractor

import wiki.extractor.types.*
import wiki.extractor.util.{Text, UnitSpec}

class FragmentProcessorSpec extends UnitSpec {
  behavior of "fragmentToPage"

  it should "extract standard fields from a basic article" in {
    val page = fragmentProcessor.fragmentToPage(Text.readTextFile("src/test/resources/animalia.xml")).get
    page.id shouldBe 332
    page.pageType shouldBe ARTICLE
    page.title shouldBe "Animalia (book)"
    page.redirectTarget shouldBe None
    page.lastEdited.get shouldBe 1463364739000L // "2016-05-15T19:12:19Z"
  }

  it should "detect a redirect page" in {
    val page = fragmentProcessor.fragmentToPage(Text.readTextFile("src/test/resources/accessiblecomputing.xml")).get
    page.id shouldBe 10
    page.pageType shouldBe REDIRECT
    page.title shouldBe "AccessibleComputing"
    page.redirectTarget shouldBe Some("Computer accessibility")
    page.lastEdited.get shouldBe 1414324223000L
  }

  it should "return nothing for a Wikipedia: namespace" in {
    val result = fragmentProcessor.fragmentToPage(Text.readTextFile("src/test/resources/missing-text.xml"))
    result shouldBe None
  }

  behavior of "getNamespace"

  it should "get the default namespace for a title without prefix" in {
    fragmentProcessor.getNamespace("Apollo 11") shouldBe siteInfo.defaultNamespace
  }

  it should "get the default namespace for a title with no matching prefix" in {
    fragmentProcessor.getNamespace("A.D. Police: Dead End City") shouldBe siteInfo.defaultNamespace
  }

  it should "get the matching namespace for a defined namespace (1)" in {
    val expected = siteInfo.prefixToNamespace("Template")
    fragmentProcessor.getNamespace("Template:Periodic table") shouldBe expected
  }

  it should "get the matching namespace for a defined namespace (2)" in {
    val expected = siteInfo.prefixToNamespace("Category")
    fragmentProcessor.getNamespace("Category:Brass instruments") shouldBe expected
  }

  behavior of "getTransclusions"

  it should "get transclusions (1)" in {
    val pageText = Text.readTextFile("src/test/resources/mercury.txt")
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

    fragmentProcessor.getTransclusions(pageText) shouldBe expected
  }

  it should "get transclusions (2)" in {
    val page = fragmentProcessor.fragmentToPage(Text.readTextFile("src/test/resources/accessiblecomputing.xml")).get
    val expected = Seq("Redr|move|from CamelCase|up")

    fragmentProcessor.getTransclusions(page.text.get) shouldBe expected
  }

  it should "get transclusions (3)" in {
    val page = fragmentProcessor.fragmentToPage(Text.readTextFile("src/test/resources/accessiblecomputing.xml")).get
    val expected = Seq("Redr|move|from CamelCase|up")

    fragmentProcessor.getTransclusions(page.text.get) shouldBe expected
  }

  behavior of "inferPageType"

  it should "detect a DISAMBIGUATION page from page text" in {
    val pageText = Text.readTextFile("src/test/resources/mercury.txt")
    fragmentProcessor.inferPageType(pageText, siteInfo.defaultNamespace) shouldBe DISAMBIGUATION
  }

  it should "detect a CATEGORY page from namespace" in {
    fragmentProcessor.inferPageType("foo", siteInfo.prefixToNamespace("Category")) shouldBe CATEGORY
  }

  it should "detect a TEMPLATE page from namespace" in {
    fragmentProcessor.inferPageType("foo", siteInfo.prefixToNamespace("Template")) shouldBe TEMPLATE
  }

  it should "detect an INVALID page from namespace" in {
    val namespace = Namespace(id = -3, casing = FIRST_LETTER, name = "Unknown")
    fragmentProcessor.inferPageType("foo", namespace) shouldBe INVALID
  }

  private lazy val language = Language(
    code = "en",
    name = "English",
    disambiguationPrefixes = Seq("disambiguation", "disambig", "geodis"),
    aliases = Seq(
      NamespaceAlias(from = "WP", to = "Wikipedia"),
      NamespaceAlias(from = "WT", to = "Wikipedia talk")
    )
  )

  private lazy val siteInfo = SiteInfo(
    siteName = "Wikipedia",
    dbName = "enwiki",
    base = "https://en.wikipedia.org/wiki/Main_Page",
    casing = FIRST_LETTER,
    namespaces = List(
      Namespace(id = -2, casing = FIRST_LETTER, name = "Media"),
      Namespace(id = -1, casing = FIRST_LETTER, name = "Special"),
      Namespace(id = 0, casing = FIRST_LETTER, name = ""),
      Namespace(id = 1, casing = FIRST_LETTER, name = "Talk"),
      Namespace(id = 2, casing = FIRST_LETTER, name = "User"),
      Namespace(id = 3, casing = FIRST_LETTER, name = "User talk"),
      Namespace(id = 4, casing = FIRST_LETTER, name = "Wikipedia"),
      Namespace(id = 5, casing = FIRST_LETTER, name = "Wikipedia talk"),
      Namespace(id = 6, casing = FIRST_LETTER, name = "File"),
      Namespace(id = 7, casing = FIRST_LETTER, name = "File talk"),
      Namespace(id = 8, casing = FIRST_LETTER, name = "MediaWiki"),
      Namespace(id = 9, casing = FIRST_LETTER, name = "MediaWiki talk"),
      Namespace(id = 10, casing = FIRST_LETTER, name = "Template"),
      Namespace(id = 11, casing = FIRST_LETTER, name = "Template talk"),
      Namespace(id = 12, casing = FIRST_LETTER, name = "Help"),
      Namespace(id = 13, casing = FIRST_LETTER, name = "Help talk"),
      Namespace(id = 14, casing = FIRST_LETTER, name = "Category"),
      Namespace(id = 15, casing = FIRST_LETTER, name = "Category talk"),
      Namespace(id = 100, casing = FIRST_LETTER, name = "Portal"),
      Namespace(id = 101, casing = FIRST_LETTER, name = "Portal talk"),
      Namespace(id = 118, casing = FIRST_LETTER, name = "Draft"),
      Namespace(id = 119, casing = FIRST_LETTER, name = "Draft talk"),
      Namespace(id = 710, casing = FIRST_LETTER, name = "TimedText"),
      Namespace(id = 711, casing = FIRST_LETTER, name = "TimedText talk"),
      Namespace(id = 828, casing = FIRST_LETTER, name = "Module"),
      Namespace(id = 829, casing = FIRST_LETTER, name = "Module talk"),
      Namespace(id = 2300, casing = CASE_SENSITIVE, name = "Gadget"),
      Namespace(id = 2301, casing = CASE_SENSITIVE, name = "Gadget talk"),
      Namespace(id = 2302, casing = CASE_SENSITIVE, name = "Gadget definition"),
      Namespace(id = 2303, casing = CASE_SENSITIVE, name = "Gadget definition talk")
    )
  )

  private lazy val fragmentProcessor = new FragmentProcessor(siteInfo, language)
}
