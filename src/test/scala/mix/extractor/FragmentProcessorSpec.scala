package mix.extractor

import mix.extractor.types.*
import mix.extractor.util.{Text, UnitSpec}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class FragmentProcessorSpec extends UnitSpec {
  behavior of "fragmentToPage"

  it should "extract standard fields from a basic article" in {
    val page = fragmentProcessor.fragmentToPage(Text.readTextFile("src/test/resources/animalia.xml")).get
    page.id shouldBe 332
    page.pageType shouldBe ARTICLE
    page.title shouldBe "Animalia (book)"
    page.redirectTarget shouldBe None
    page.lastEdited shouldBe 1463364739000L // "2016-05-15T19:12:19Z"
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

  behavior of "getRedirectTarget"

  it should "get None for a non-redirect page" in {
    val text = fragmentProcessor.fragmentToPage(Text.readTextFile("src/test/resources/animalia.xml")).get.text
    fragmentProcessor.getRedirectTarget(text) shouldBe None
  }

  it should "get redirect for a page that is redirected TO (1)" in {
    val text = "#REDIRECT [[History of Afghanistan]] {{R from CamelCase}}"
    fragmentProcessor.getRedirectTarget(text) shouldBe Some("History of Afghanistan")
  }

  it should "get redirect for a page that is redirected TO (2)" in {
    val text = "#REDIRECT [[Constructed language]]"
    fragmentProcessor.getRedirectTarget(text) shouldBe Some("Constructed language")
  }

  // TODO fix
  ignore should "get redirect for a page that is redirected TO (3)" in {
    val text = "#REDIRECT [[Molecular mass]][[Category:Amount of substance]]"
    fragmentProcessor.getRedirectTarget(text) shouldBe Some("Molecular mass")
  }

  // TODO fix
  ignore should "get redirect for a page that is redirected TO a specific article section" in {
    val text = "#REDIRECT [[Thermal expansion#Coefficient of thermal expansion]]"
    fragmentProcessor.getRedirectTarget(text) shouldBe Some("Thermal expansion")
  }

  // N.B. in the original Milne wikipediaminer code, the DumpPageParser.java
  // code treated pages as redirects whether they were redirected TO or FROM.
  // Either this was a mistake in the original code or the redirectTarget was
  // reused even in cases where it was a source rather than a direct. I believe
  // that the original code was in error.
  it should "get None for a page that is redirected FROM" in {
    val text = """{{redirect|Faithful Departed|the Cranberries album|To the Faithful Departed}}
                 """.stripMargin
    fragmentProcessor.getRedirectTarget(text) shouldBe None
  }

  behavior of "getPageType"

  it should "detect a REDIRECT page from page text" in {
    val pageText = """#REDIRECT [[Molecular mass]][[Category:Amount of substance]]"""
    fragmentProcessor.getPageType(pageText, siteInfo.defaultNamespace) shouldBe REDIRECT
  }

  // TODO the disambiguation detector is broken, since the Mercury page should
  // match. Fix this later.
  ignore should "detect a DISAMBIGUATION page from page text" in {
    val pageText = Text.readTextFile("src/test/resources/mercury.txt")
    println(language.disambiguationPattern.matcher(pageText).find())
    fragmentProcessor.getPageType(pageText, siteInfo.defaultNamespace) shouldBe DISAMBIGUATION
  }

  it should "detect a CATEGORY page from namespace" in {
    fragmentProcessor.getPageType("foo", siteInfo.prefixToNamespace("Category")) shouldBe CATEGORY
  }

  it should "detect a TEMPLATE page from namespace" in {
    fragmentProcessor.getPageType("foo", siteInfo.prefixToNamespace("Template")) shouldBe TEMPLATE
  }

  it should "detect an INVALID page from namespace" in {
    val namespace = Namespace(key = -3, kase = FIRST_LETTER, name = "Unknown")
    fragmentProcessor.getPageType("foo", namespace) shouldBe INVALID
  }

  private lazy val language = Language(
    code = "en",
    name = "English",
    disambiguationCategories = Seq("Disambiguation"),
    disambiguationTemplates = Seq("disambiguation", "disambig", "geodis"),
    redirectIdentifiers = Seq("REDIRECT"),
    aliases = Seq(
      NamespaceAlias(from = "WP", to = "Wikipedia"),
      NamespaceAlias(from = "WT", to = "Wikipedia talk")
    )
  )

  private lazy val siteInfo = SiteInfo(
    siteName = "Wikipedia",
    dbName = "enwiki",
    base = "https://en.wikipedia.org/wiki/Main_Page",
    kase = FIRST_LETTER,
    namespaces = List(
      Namespace(key = -2, kase = FIRST_LETTER, name = "Media"),
      Namespace(key = -1, kase = FIRST_LETTER, name = "Special"),
      Namespace(key = 0, kase = FIRST_LETTER, name = ""),
      Namespace(key = 1, kase = FIRST_LETTER, name = "Talk"),
      Namespace(key = 2, kase = FIRST_LETTER, name = "User"),
      Namespace(key = 3, kase = FIRST_LETTER, name = "User talk"),
      Namespace(key = 4, kase = FIRST_LETTER, name = "Wikipedia"),
      Namespace(key = 5, kase = FIRST_LETTER, name = "Wikipedia talk"),
      Namespace(key = 6, kase = FIRST_LETTER, name = "File"),
      Namespace(key = 7, kase = FIRST_LETTER, name = "File talk"),
      Namespace(key = 8, kase = FIRST_LETTER, name = "MediaWiki"),
      Namespace(key = 9, kase = FIRST_LETTER, name = "MediaWiki talk"),
      Namespace(key = 10, kase = FIRST_LETTER, name = "Template"),
      Namespace(key = 11, kase = FIRST_LETTER, name = "Template talk"),
      Namespace(key = 12, kase = FIRST_LETTER, name = "Help"),
      Namespace(key = 13, kase = FIRST_LETTER, name = "Help talk"),
      Namespace(key = 14, kase = FIRST_LETTER, name = "Category"),
      Namespace(key = 15, kase = FIRST_LETTER, name = "Category talk"),
      Namespace(key = 100, kase = FIRST_LETTER, name = "Portal"),
      Namespace(key = 101, kase = FIRST_LETTER, name = "Portal talk"),
      Namespace(key = 118, kase = FIRST_LETTER, name = "Draft"),
      Namespace(key = 119, kase = FIRST_LETTER, name = "Draft talk"),
      Namespace(key = 710, kase = FIRST_LETTER, name = "TimedText"),
      Namespace(key = 711, kase = FIRST_LETTER, name = "TimedText talk"),
      Namespace(key = 828, kase = FIRST_LETTER, name = "Module"),
      Namespace(key = 829, kase = FIRST_LETTER, name = "Module talk"),
      Namespace(key = 2300, kase = CASE_SENSITIVE, name = "Gadget"),
      Namespace(key = 2301, kase = CASE_SENSITIVE, name = "Gadget talk"),
      Namespace(key = 2302, kase = CASE_SENSITIVE, name = "Gadget definition"),
      Namespace(key = 2303, kase = CASE_SENSITIVE, name = "Gadget definition talk")
    )
  )

  private lazy val fragmentProcessor = new FragmentProcessor(siteInfo, language)
}
