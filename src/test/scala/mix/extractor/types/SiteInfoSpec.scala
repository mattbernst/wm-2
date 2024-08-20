package mix.extractor.types

import mix.extractor.util.{Text, UnitSpec}

class SiteInfoSpec extends UnitSpec {
  behavior of "apply"

  it should "construct a populated SiteInfo from the early portion of a Wikipedia dump" in {
    val text = Text.readTextFile("src/test/resources/dump-head.xml")
    val expected = SiteInfo(
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

    val siteInfo = SiteInfo(text)
    siteInfo shouldBe expected
  }
}
