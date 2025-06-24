package wiki.extractor.types

import wiki.util.UnitSpec

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
      trainingProfile = TrainingProfile.empty
    )

    // In Turkish, lowercase 'i' capitalizes to 'İ' (with dot)
    val result = turkishLang.capitalizeFirst("istanbul")
    result should startWith("İ")
  }

  private lazy val lang = Language(
    code = "en",
    name = "English",
    disambiguationPrefixes = Seq("disambiguation", "disambig", "geodis"),
    trainingProfile = TrainingProfile.empty
  )
}
