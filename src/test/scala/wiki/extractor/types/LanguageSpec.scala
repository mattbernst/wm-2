package wiki.extractor.types

import wiki.extractor.util.UnitSpec

class LanguageSpec extends UnitSpec {
  "serialization/deserialization" should "serialize an entry and restore it" in {
    val serialized = Language.toJSON(Seq(lang))
    Language.fromJSON(serialized) shouldBe Seq(lang)
  }

  behavior of "isDisambiguation"

  it should "return true for a direct match" in {
    lang.isDisambiguation("geodis") shouldBe true
  }

  it should "return true for a differently-cased transclusion that matches" in {
    lang.isDisambiguation("Disambiguation") shouldBe true
  }

  it should "return true for a transclusion that starts with a prefix" in {
    lang.isDisambiguation("disambiguation|geo") shouldBe true
  }

  it should "return false for an unrelated transclusion" in {
    lang.isDisambiguation("reflist") shouldBe false
  }

  behavior of "isDate"

  it should "return true for valid month and day combinations" in {
    lang.isDate("January 1") shouldBe true
    lang.isDate("February 29") shouldBe true // Valid in leap years
    lang.isDate("March 15") shouldBe true
    lang.isDate("April 30") shouldBe true
    lang.isDate("May 25") shouldBe true
    lang.isDate("June 10") shouldBe true
    lang.isDate("July 4") shouldBe true
    lang.isDate("August 31") shouldBe true
    lang.isDate("September 22") shouldBe true
    lang.isDate("October 31") shouldBe true
    lang.isDate("November 11") shouldBe true
    lang.isDate("December 25") shouldBe true
  }

  it should "return true for single-digit days" in {
    lang.isDate("January 1") shouldBe true
    lang.isDate("February 5") shouldBe true
    lang.isDate("December 9") shouldBe true
  }

  it should "return false for invalid month names" in {
    lang.isDate("Invalidmonth 15") shouldBe false
    lang.isDate("Foo 10") shouldBe false
    lang.isDate("13th 5") shouldBe false
  }

  it should "return false for invalid day numbers" in {
    lang.isDate("January 32") shouldBe false
    lang.isDate("February 30") shouldBe false
    lang.isDate("April 31") shouldBe false
    lang.isDate("June 31") shouldBe false
    lang.isDate("September 31") shouldBe false
    lang.isDate("November 31") shouldBe false
  }

  it should "return false for day 0 or negative days" in {
    lang.isDate("January 0") shouldBe false
    lang.isDate("March -1") shouldBe false
  }

  it should "return false for completely invalid formats" in {
    lang.isDate("not a date") shouldBe false
    lang.isDate("15 January") shouldBe false // Wrong order
    lang.isDate("Jan 15") shouldBe false     // Abbreviated month
    lang.isDate("January") shouldBe false    // Missing day
    lang.isDate("15") shouldBe false         // Missing month
    lang.isDate("") shouldBe false           // Empty string
  }

  it should "return false for dates with extra content" in {
    lang.isDate("January 15th") shouldBe false
    lang.isDate("January 15, 2023") shouldBe false
    lang.isDate("On January 15") shouldBe false
    lang.isDate("January 15 is a date") shouldBe false
  }

  it should "handle edge cases with whitespace" in {
    lang.isDate(" January 15") shouldBe false // Leading space
    lang.isDate("January 15 ") shouldBe false // Trailing space
    lang.isDate("January  15") shouldBe false // Extra space between
  }

  it should "work with different locales" in {
    val frenchLang = Language(
      code = "fr",
      name = "French",
      disambiguationPrefixes = Seq("homonymie"),
      rootPage = "Catégorie:Accueil"
    )

    // These should work for French locale
    frenchLang.isDate("janvier 15") shouldBe true
    frenchLang.isDate("décembre 25") shouldBe true

    // English month names should not work for French locale
    frenchLang.isDate("January 15") shouldBe false
    frenchLang.isDate("December 25") shouldBe false
  }

  private lazy val lang = Language(
    code = "en",
    name = "English",
    disambiguationPrefixes = Seq("disambiguation", "disambig", "geodis"),
    rootPage = "Category:Main topic classifications"
  )
}
