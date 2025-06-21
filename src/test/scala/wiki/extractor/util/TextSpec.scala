package wiki.extractor.util

import wiki.util.{FileHelpers, UnitSpec}

class TextSpec extends UnitSpec {
  behavior of "filterToLettersAndDigits"

  it should "handle simple English text" in {
    val input    = "'Doctor Who', starring Jodie Whittaker"
    val expected = " Doctor Who   starring Jodie Whittaker"
    Text.filterToLettersAndDigits(input) shouldBe expected
  }

  it should "handle French accented characters" in {
    val input    = "Café, résumé & naïve"
    val expected = "Café  résumé   naïve"
    Text.filterToLettersAndDigits(input) shouldBe expected
  }

  it should "handle German umlauts and eszett" in {
    val input    = "Mädchen & Jungen: Straße"
    val expected = "Mädchen   Jungen  Straße"
    Text.filterToLettersAndDigits(input) shouldBe expected
  }

  it should "handle Spanish characters" in {
    val input    = "¿Cómo estás? ¡Niño!"
    val expected = " Cómo estás   Niño "
    Text.filterToLettersAndDigits(input) shouldBe expected
  }

  it should "handle numbers and mixed punctuation" in {
    val input    = "Item #42: $19.99 (50% off!)"
    val expected = "Item  42   19 99  50  off  "
    Text.filterToLettersAndDigits(input) shouldBe expected
  }

  it should "handle emoji (replaced with spaces)" in {
    val input    = "Hello 👋 world! 🌍 How are you? 😊"
    val expected = "Hello   world    How are you   "
    Text.filterToLettersAndDigits(input) shouldBe expected
  }

  it should "handle Chinese characters" in {
    val input    = "你好，世界! 123"
    val expected = "你好 世界  123"
    Text.filterToLettersAndDigits(input) shouldBe expected
  }

  it should "handle Japanese hiragana, katakana, and kanji" in {
    val input    = "こんにちは・カタカナ・漢字456"
    val expected = "こんにちは カタカナ 漢字456"
    Text.filterToLettersAndDigits(input) shouldBe expected
  }

  it should "handle Arabic script" in {
    val input    = "مرحبا، العالم! 789"
    val expected = "مرحبا  العالم  789"
    Text.filterToLettersAndDigits(input) shouldBe expected
  }

  it should "handle Russian Cyrillic" in {
    val input    = "Привет, мир! Это тест."
    val expected = "Привет  мир  Это тест "
    Text.filterToLettersAndDigits(input) shouldBe expected
  }

  it should "handle Greek letters" in {
    val input    = "Αλφάβητο: α, β, γ, δ"
    val expected = "Αλφάβητο  α  β  γ  δ"
    Text.filterToLettersAndDigits(input) shouldBe expected
  }

  it should "handle mathematical symbols and special Unicode" in {
    val input    = "E=mc² ∆x ≈ π/2"
    val expected = "E mc   x   π 2"
    Text.filterToLettersAndDigits(input) shouldBe expected
  }

  it should "handle supplementary Unicode characters (beyond BMP)" in {
    val input    = "𝒽𝑒𝓁𝓁𝑜 𝟙𝟚𝟛 𝕨𝕠𝕣𝕝𝕕!"
    val expected = "𝒽𝑒𝓁𝓁𝑜 𝟙𝟚𝟛 𝕨𝕠𝕣𝕝𝕕 "
    Text.filterToLettersAndDigits(input) shouldBe expected
  }

  it should "handle empty string" in {
    val input    = ""
    val expected = ""
    Text.filterToLettersAndDigits(input) shouldBe expected
  }

  it should "handle string with only punctuation" in {
    val input    = "!@#$%^&*()[]{}|\\:;\"'<>,.?/"
    val expected = "                          "
    Text.filterToLettersAndDigits(input) shouldBe expected
  }

  it should "handle string with only letters and digits" in {
    val input    = "abc123XYZ"
    val expected = "abc123XYZ"
    Text.filterToLettersAndDigits(input) shouldBe expected
  }

  it should "handle mixed scripts with complex punctuation" in {
    val input    = "Hello世界! Привет🌍 Café № 42"
    val expected = "Hello世界  Привет  Café   42"
    Text.filterToLettersAndDigits(input) shouldBe expected
  }

  behavior of "tagSlice"

  it should "extract outer siteinfo" in {
    val expected = """  <siteinfo>
    <sitename>Wikipedia</sitename>
    <dbname>enwiki</dbname>
    <base>https://en.wikipedia.org/wiki/Main_Page</base>
    <generator>MediaWiki 1.41.0-wmf.24</generator>
    <case>first-letter</case>
    <namespaces>
      <namespace key="-2" case="first-letter">Media</namespace>
      <namespace key="-1" case="first-letter">Special</namespace>
      <namespace key="0" case="first-letter" />
      <namespace key="1" case="first-letter">Talk</namespace>
      <namespace key="2" case="first-letter">User</namespace>
      <namespace key="3" case="first-letter">User talk</namespace>
      <namespace key="4" case="first-letter">Wikipedia</namespace>
      <namespace key="5" case="first-letter">Wikipedia talk</namespace>
      <namespace key="6" case="first-letter">File</namespace>
      <namespace key="7" case="first-letter">File talk</namespace>
      <namespace key="8" case="first-letter">MediaWiki</namespace>
      <namespace key="9" case="first-letter">MediaWiki talk</namespace>
      <namespace key="10" case="first-letter">Template</namespace>
      <namespace key="11" case="first-letter">Template talk</namespace>
      <namespace key="12" case="first-letter">Help</namespace>
      <namespace key="13" case="first-letter">Help talk</namespace>
      <namespace key="14" case="first-letter">Category</namespace>
      <namespace key="15" case="first-letter">Category talk</namespace>
      <namespace key="100" case="first-letter">Portal</namespace>
      <namespace key="101" case="first-letter">Portal talk</namespace>
      <namespace key="118" case="first-letter">Draft</namespace>
      <namespace key="119" case="first-letter">Draft talk</namespace>
      <namespace key="710" case="first-letter">TimedText</namespace>
      <namespace key="711" case="first-letter">TimedText talk</namespace>
      <namespace key="828" case="first-letter">Module</namespace>
      <namespace key="829" case="first-letter">Module talk</namespace>
      <namespace key="2300" case="case-sensitive">Gadget</namespace>
      <namespace key="2301" case="case-sensitive">Gadget talk</namespace>
      <namespace key="2302" case="case-sensitive">Gadget definition</namespace>
      <namespace key="2303" case="case-sensitive">Gadget definition talk</namespace>
    </namespaces>
  </siteinfo>
"""

    val text = FileHelpers.readTextFile("src/test/resources/dump-head.xml")
    val res  = Text.tagSlice("siteinfo", text.split('\n').iterator)
    res shouldBe expected
  }

  it should "extract inner namespaces" in {
    val expected = """    <namespaces>
      <namespace key="-2" case="first-letter">Media</namespace>
      <namespace key="-1" case="first-letter">Special</namespace>
      <namespace key="0" case="first-letter" />
      <namespace key="1" case="first-letter">Talk</namespace>
      <namespace key="2" case="first-letter">User</namespace>
      <namespace key="3" case="first-letter">User talk</namespace>
      <namespace key="4" case="first-letter">Wikipedia</namespace>
      <namespace key="5" case="first-letter">Wikipedia talk</namespace>
      <namespace key="6" case="first-letter">File</namespace>
      <namespace key="7" case="first-letter">File talk</namespace>
      <namespace key="8" case="first-letter">MediaWiki</namespace>
      <namespace key="9" case="first-letter">MediaWiki talk</namespace>
      <namespace key="10" case="first-letter">Template</namespace>
      <namespace key="11" case="first-letter">Template talk</namespace>
      <namespace key="12" case="first-letter">Help</namespace>
      <namespace key="13" case="first-letter">Help talk</namespace>
      <namespace key="14" case="first-letter">Category</namespace>
      <namespace key="15" case="first-letter">Category talk</namespace>
      <namespace key="100" case="first-letter">Portal</namespace>
      <namespace key="101" case="first-letter">Portal talk</namespace>
      <namespace key="118" case="first-letter">Draft</namespace>
      <namespace key="119" case="first-letter">Draft talk</namespace>
      <namespace key="710" case="first-letter">TimedText</namespace>
      <namespace key="711" case="first-letter">TimedText talk</namespace>
      <namespace key="828" case="first-letter">Module</namespace>
      <namespace key="829" case="first-letter">Module talk</namespace>
      <namespace key="2300" case="case-sensitive">Gadget</namespace>
      <namespace key="2301" case="case-sensitive">Gadget talk</namespace>
      <namespace key="2302" case="case-sensitive">Gadget definition</namespace>
      <namespace key="2303" case="case-sensitive">Gadget definition talk</namespace>
    </namespaces>
"""

    val text = FileHelpers.readTextFile("src/test/resources/dump-head.xml")
    val res  = Text.tagSlice("namespaces", text.split('\n').iterator)
    res shouldBe expected
  }
}
