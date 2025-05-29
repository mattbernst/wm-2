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

  behavior of "capitalizeFirst"

  it should "capitalize the first character of a simple string" in {
    lang.capitalizeFirst("hello") shouldBe "Hello"
    lang.capitalizeFirst("world") shouldBe "World"
    lang.capitalizeFirst("test") shouldBe "Test"
  }

  it should "leave already capitalized strings unchanged" in {
    lang.capitalizeFirst("Hello") shouldBe "Hello"
    lang.capitalizeFirst("World") shouldBe "World"
    lang.capitalizeFirst("CAPS") shouldBe "CAPS"
  }

  it should "handle empty strings" in {
    lang.capitalizeFirst("") shouldBe ""
  }

  it should "handle single character strings" in {
    lang.capitalizeFirst("a") shouldBe "A"
    lang.capitalizeFirst("A") shouldBe "A"
    lang.capitalizeFirst("1") shouldBe "1"
    lang.capitalizeFirst("!") shouldBe "!"
  }

  it should "only capitalize the first character" in {
    lang.capitalizeFirst("hello world") shouldBe "Hello world"
    lang.capitalizeFirst("multiple words here") shouldBe "Multiple words here"
    lang.capitalizeFirst("mixed Case String") shouldBe "Mixed Case String"
  }

  it should "handle strings with numbers and special characters" in {
    lang.capitalizeFirst("123abc") shouldBe "123abc"
    lang.capitalizeFirst("!hello") shouldBe "!hello"
    lang.capitalizeFirst("@test") shouldBe "@test"
    lang.capitalizeFirst("#hashtag") shouldBe "#hashtag"
  }

  it should "handle Unicode characters correctly" in {
    lang.capitalizeFirst("ñoño") shouldBe "Ñoño"
    lang.capitalizeFirst("café") shouldBe "Café"
    lang.capitalizeFirst("naïve") shouldBe "Naïve"
  }

  it should "handle complex Unicode grapheme clusters" in {
    // Test with combining characters
    lang.capitalizeFirst("é́motion") should not be empty // Should handle grapheme clusters properly
    lang.capitalizeFirst("🙂face") shouldBe "🙂face"     // Emoji should remain unchanged
  }

  it should "preserve whitespace and formatting" in {
    lang.capitalizeFirst(" hello") shouldBe " hello"   // Leading space means first char is space, not 'h'
    lang.capitalizeFirst("\thello") shouldBe "\thello" // Tab character first
    lang.capitalizeFirst("\nhello") shouldBe "\nhello" // Newline character first
  }

  behavior of "unCapitalizeFirst"

  it should "uncapitalize the first character of a capitalized string" in {
    lang.unCapitalizeFirst("Hello") shouldBe "hello"
    lang.unCapitalizeFirst("World") shouldBe "world"
    lang.unCapitalizeFirst("Test") shouldBe "test"
  }

  it should "leave already uncapitalized strings unchanged" in {
    lang.unCapitalizeFirst("hello") shouldBe "hello"
    lang.unCapitalizeFirst("world") shouldBe "world"
    lang.unCapitalizeFirst("test") shouldBe "test"
  }

  it should "handle empty strings" in {
    lang.unCapitalizeFirst("") shouldBe ""
  }

  it should "handle single character strings" in {
    lang.unCapitalizeFirst("A") shouldBe "a"
    lang.unCapitalizeFirst("a") shouldBe "a"
    lang.unCapitalizeFirst("1") shouldBe "1"
    lang.unCapitalizeFirst("!") shouldBe "!"
  }

  it should "only uncapitalize the first character" in {
    lang.unCapitalizeFirst("Hello World") shouldBe "hello World"
    lang.unCapitalizeFirst("Multiple Words Here") shouldBe "multiple Words Here"
    lang.unCapitalizeFirst("Mixed Case String") shouldBe "mixed Case String"
  }

  it should "handle strings with numbers and special characters" in {
    lang.unCapitalizeFirst("123ABC") shouldBe "123ABC"
    lang.unCapitalizeFirst("!Hello") shouldBe "!Hello"
    lang.unCapitalizeFirst("@Test") shouldBe "@Test"
    lang.unCapitalizeFirst("#Hashtag") shouldBe "#Hashtag"
  }

  it should "handle Unicode characters correctly" in {
    lang.unCapitalizeFirst("Ñoño") shouldBe "ñoño"
    lang.unCapitalizeFirst("Café") shouldBe "café"
    lang.unCapitalizeFirst("Naïve") shouldBe "naïve"
  }

  it should "handle complex Unicode grapheme clusters" in {
    // Test with combining characters
    lang.unCapitalizeFirst("É́motion") should not be empty // Should handle grapheme clusters properly
    lang.unCapitalizeFirst("🙂Face") shouldBe "🙂Face"     // Emoji should remain unchanged
  }

  it should "preserve whitespace and formatting" in {
    lang.unCapitalizeFirst(" Hello") shouldBe " Hello"   // Leading space means first char is space, not 'H'
    lang.unCapitalizeFirst("\tHello") shouldBe "\tHello" // Tab character first
    lang.unCapitalizeFirst("\nHello") shouldBe "\nHello" // Newline character first
  }

  behavior of "capitalizeFirst and unCapitalizeFirst"

  it should "be inverse operations for basic strings" in {
    val testStrings = Seq("hello", "Hello", "WORLD", "mixed", "Test123", "café")

    testStrings.foreach { str =>
      // capitalizeFirst -> unCapitalizeFirst should give lowercase first char
      val capitalized   = lang.capitalizeFirst(str)
      val uncapitalized = lang.unCapitalizeFirst(capitalized)
      if (str.nonEmpty && str.head.isLetter) {
        uncapitalized.head.isLower shouldBe true
      }

      // unCapitalizeFirst -> capitalizeFirst should give uppercase first char
      val uncappedFirst = lang.unCapitalizeFirst(str)
      val cappedAgain   = lang.capitalizeFirst(uncappedFirst)
      if (str.nonEmpty && str.head.isLetter) {
        cappedAgain.head.isUpper shouldBe true
      }
    }
  }

  it should "handle locale-specific capitalization" in {
    // Test with Turkish locale which has special rules for i/İ
    val turkishLang = Language(
      code = "tr",
      name = "Turkish",
      disambiguationPrefixes = Seq("anlam ayrımı"),
      rootPage = "Kategori:Ana konu sınıflandırmaları"
    )

    // In Turkish, lowercase 'i' capitalizes to 'İ' (with dot)
    // This test assumes the Language class uses Turkish locale internally
    val result = turkishLang.capitalizeFirst("istanbul")
    result should startWith("İ")
  }

  private lazy val lang = Language(
    code = "en",
    name = "English",
    disambiguationPrefixes = Seq("disambiguation", "disambig", "geodis"),
    rootPage = "Category:Main topic classifications"
  )
}
