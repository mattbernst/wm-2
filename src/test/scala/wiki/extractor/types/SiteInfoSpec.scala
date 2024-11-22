package wiki.extractor.types

import wiki.extractor.util.{FileHelpers, UnitSpec}

class SiteInfoSpec extends UnitSpec {
  behavior of "apply"

  it should "construct a populated SiteInfo from the early portion of a Wikipedia dump" in {
    val text = FileHelpers.readTextFile("src/test/resources/dump-head.xml")
    val expected = SiteInfo(
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

    val siteInfo = SiteInfo(text)
    siteInfo shouldBe expected
  }
}
