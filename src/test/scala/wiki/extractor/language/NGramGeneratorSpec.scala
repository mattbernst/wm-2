package wiki.extractor.language

import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}
import opennlp.tools.tokenize.{TokenizerME, TokenizerModel}
import pprint.PPrinter.BlackWhite
import wiki.extractor.language.types.NGram
import wiki.extractor.util.UnitSpec

import java.io.FileInputStream

class NGramGeneratorSpec extends UnitSpec {
  behavior of "generate"

  it should "generate word based ngrams from a sentence" in {
    val ngg   = generator(2)
    val input = "The order of the memory bytes storing the bits varies; see endianness."
    val expected = List(
      "The",
      "The order",
      "order",
      "order of",
      "of",
      "of the",
      "the",
      "the memory",
      "memory",
      "memory bytes",
      "bytes",
      "bytes storing",
      "storing",
      "storing the",
      "the",
      "the bits",
      "bits",
      "bits varies",
      "varies",
      "see",
      "see endianness",
      "endianness"
    )

    val result  = ngg.generate(input).toList
    val strings = NGram.sliceString(input, result)

    strings shouldBe expected
  }

  it should "generate more ngrams with higher maxTokens" in {
    val ngg   = generator(3) // Each NGram can contain up to 3 tokens
    val input = "The order of the memory bytes storing the bits varies; see endianness."
    val expected = List(
      "The",
      "The order",
      "The order of",
      "order",
      "order of",
      "order of the",
      "of",
      "of the",
      "of the memory",
      "the",
      "the memory",
      "the memory bytes",
      "memory",
      "memory bytes",
      "memory bytes storing",
      "bytes",
      "bytes storing",
      "bytes storing the",
      "storing",
      "storing the",
      "storing the bits",
      "the",
      "the bits",
      "the bits varies",
      "bits",
      "bits varies",
      "varies",
      "varies; see",
      "see",
      "see endianness",
      "endianness"
    )

    val result  = ngg.generate(input).toList
    val strings = NGram.sliceString(input, result)

    strings shouldBe expected
  }

  it should "generate word based ngrams from multiple sentences" in {
    val ngg = generator(2)
    val input =
      "Palladium-103 is a radioisotope of the element palladium. It may be created from rhodium-102 using a cyclotron."
    val expected = List(
      "Palladium",
      "103",
      "103 is",
      "is",
      "is a",
      "a",
      "a radioisotope",
      "radioisotope",
      "radioisotope of",
      "of",
      "of the",
      "the",
      "the element",
      "element",
      "element palladium",
      "palladium", // First sentence ends here; NGrams do not cross sentences
      "It",
      "It may",
      "may",
      "may be",
      "be",
      "be created",
      "created",
      "created from",
      "from",
      "from rhodium",
      "rhodium",
      "102",
      "102 using",
      "using",
      "using a",
      "a",
      "a cyclotron",
      "cyclotron"
    )

    val result  = ngg.generate(input).toList
    val strings = NGram.sliceString(input, result)

    strings shouldBe expected
  }

  it should "indicate start-of-sentence in NGrams" in {
    val ngg   = generator(2)
    val input = "You knew their purpose, yet you made them. If you had scruples, you betrayed them."

    val expected = List(
      (true, "You"),
      (true, "You knew"),
      (false, "knew"),
      (false, "knew their"),
      (false, "their"),
      (false, "their purpose"),
      (false, "purpose"),
      (false, "yet"),
      (false, "yet you"),
      (false, "you"),
      (false, "you made"),
      (false, "made"),
      (false, "made them"),
      (false, "them"),
      (true, "If"),
      (true, "If you"),
      (false, "you"),
      (false, "you had"),
      (false, "had"),
      (false, "had scruples"),
      (false, "scruples"),
      (false, "you"),
      (false, "you betrayed"),
      (false, "betrayed"),
      (false, "betrayed them"),
      (false, "them")
    )

    val result   = ngg.generate(input).toList
    val strings  = NGram.sliceString(input, result)
    val combined = result.map(_.isSentenceStart).zip(strings)

    combined shouldBe expected
  }

  behavior of "generateSimple"

  // TODO fixme after cleaning up ngg.generate
  ignore should "directly generate string-ngrams" in {
    val ngg     = generator(3)
    val input   = "The order of the memory bytes storing the bits varies; see endianness."
    val result  = ngg.generateSimple(input).toList
    val resultG = ngg.generate(input).toList
    val strings = NGram.sliceString(input, resultG)

    // Results are generated in a different order but otherwise match
    result.sorted shouldBe strings.sorted
  }

  it should "handle block of text" in {
    val ngg   = generator(10)
    val input = """Ando (Japanese: 安藤) is a common Japan surname. Notable people with this name are listed below.
                  |
                  |Momofuku Ando - founder and chairman of Nissin Food Products
                  |Tadao Ando - architect
                  |Sportspeople
                  |
                  |Miki Ando - figure skater
                  |Footballers
                  |
                  |Kozue Ando
                  |Shunsuke Ando
                  |Masahiro Ando
                  |Jun Ando
                  |Tomoyasu Ando
                  |
                  |Category:Japanese-language surnames""".stripMargin
    val expected = List(
      "Ando (Japanese: 安藤) is a common Japan",
      "Ando (Japanese: 安藤) is a common",
      "Ando (Japanese: 安藤) is a",
      "Ando (Japanese: 安藤) is",
      "Ando (Japanese: 安藤)",
      "Ando (Japanese: 安藤",
      "Ando (Japanese:",
      "Ando (Japanese",
      "Ando (",
      "Ando",
      "(Japanese: 安藤) is a common Japan surname",
      "(Japanese: 安藤) is a common Japan",
      "(Japanese: 安藤) is a common",
      "(Japanese: 安藤) is a",
      "(Japanese: 安藤) is",
      "(Japanese: 安藤)",
      "(Japanese: 安藤",
      "(Japanese:",
      "(Japanese",
      "(",
      "Japanese: 安藤) is a common Japan surname.",
      "Japanese: 安藤) is a common Japan surname",
      "Japanese: 安藤) is a common Japan",
      "Japanese: 安藤) is a common",
      "Japanese: 安藤) is a",
      "Japanese: 安藤) is",
      "Japanese: 安藤)",
      "Japanese: 安藤",
      "Japanese:",
      "Japanese",
      ": 安藤) is a common Japan surname.",
      ": 安藤) is a common Japan surname",
      ": 安藤) is a common Japan",
      ": 安藤) is a common",
      ": 安藤) is a",
      ": 安藤) is",
      ": 安藤)",
      ": 安藤",
      ":",
      "安藤) is a common Japan surname.",
      "安藤) is a common Japan surname",
      "安藤) is a common Japan",
      "安藤) is a common",
      "安藤) is a",
      "安藤) is",
      "安藤)",
      "安藤",
      ") is a common Japan surname.",
      ") is a common Japan surname",
      ") is a common Japan",
      ") is a common",
      ") is a",
      ") is",
      ")",
      "is a common Japan surname.",
      "is a common Japan surname",
      "is a common Japan",
      "is a common",
      "is a",
      "is",
      "a common Japan surname.",
      "a common Japan surname",
      "a common Japan",
      "a common",
      "a",
      "common Japan surname.",
      "common Japan surname",
      "common Japan",
      "common",
      "Japan surname.",
      "Japan surname",
      "Japan",
      "surname.",
      "surname",
      ".",
      "Notable people with this name are listed below.",
      "Notable people with this name are listed below",
      "Notable people with this name are listed",
      "Notable people with this name are",
      "Notable people with this name",
      "Notable people with this",
      "Notable people with",
      "Notable people",
      "Notable",
      "people with this name are listed below.",
      "people with this name are listed below",
      "people with this name are listed",
      "people with this name are",
      "people with this name",
      "people with this",
      "people with",
      "people",
      "with this name are listed below.",
      "with this name are listed below",
      "with this name are listed",
      "with this name are",
      "with this name",
      "with this",
      "with",
      "this name are listed below.",
      "this name are listed below",
      "this name are listed",
      "this name are",
      "this name",
      "this",
      "name are listed below.",
      "name are listed below",
      "name are listed",
      "name are",
      "name",
      "are listed below.",
      "are listed below",
      "are listed",
      "are",
      "listed below.",
      "listed below",
      "listed",
      "below.",
      "below",
      ".",
      "Momofuku Ando - founder and chairman of Nissin Food Products",
      "Momofuku Ando - founder and chairman of Nissin Food",
      "Momofuku Ando - founder and chairman of Nissin",
      "Momofuku Ando - founder and chairman of",
      "Momofuku Ando - founder and chairman",
      "Momofuku Ando - founder and",
      "Momofuku Ando - founder",
      "Momofuku Ando -",
      "Momofuku Ando",
      "Momofuku",
      "Ando - founder and chairman of Nissin Food Products",
      "Ando - founder and chairman of Nissin Food",
      "Ando - founder and chairman of Nissin",
      "Ando - founder and chairman of",
      "Ando - founder and chairman",
      "Ando - founder and",
      "Ando - founder",
      "Ando -",
      "Ando",
      "- founder and chairman of Nissin Food Products",
      "- founder and chairman of Nissin Food",
      "- founder and chairman of Nissin",
      "- founder and chairman of",
      "- founder and chairman",
      "- founder and",
      "- founder",
      "-",
      "founder and chairman of Nissin Food Products",
      "founder and chairman of Nissin Food",
      "founder and chairman of Nissin",
      "founder and chairman of",
      "founder and chairman",
      "founder and",
      "founder",
      "and chairman of Nissin Food Products",
      "and chairman of Nissin Food",
      "and chairman of Nissin",
      "and chairman of",
      "and chairman",
      "and",
      "chairman of Nissin Food Products",
      "chairman of Nissin Food",
      "chairman of Nissin",
      "chairman of",
      "chairman",
      "of Nissin Food Products",
      "of Nissin Food",
      "of Nissin",
      "of",
      "Nissin Food Products",
      "Nissin Food",
      "Nissin",
      "Food Products",
      "Food",
      "Products",
      "Tadao Ando - architect",
      "Tadao Ando -",
      "Tadao Ando",
      "Tadao",
      "Ando - architect",
      "Ando -",
      "Ando",
      "- architect",
      "-",
      "architect",
      "Sportspeople",
      "Miki Ando - figure skater",
      "Miki Ando - figure",
      "Miki Ando -",
      "Miki Ando",
      "Miki",
      "Ando - figure skater",
      "Ando - figure",
      "Ando -",
      "Ando",
      "- figure skater",
      "- figure",
      "-",
      "figure skater",
      "figure",
      "skater",
      "Footballers",
      "Kozue Ando",
      "Kozue",
      "Ando",
      "Shunsuke Ando",
      "Shunsuke",
      "Ando",
      "Masahiro Ando",
      "Masahiro",
      "Ando",
      "Jun Ando",
      "Jun",
      "Ando",
      "Tomoyasu Ando",
      "Tomoyasu",
      "Ando",
      "Category:Japanese-language surnames",
      "Category:Japanese-language",
      "Category:Japanese",
      "-language surnames",
      "-language",
      "surnames"
    )

    val result = ngg.generateSimple(input).toList
    result shouldBe expected
  }

  def generator(maxTokens: Int) = {
    val sd = {
      val inStream = new FileInputStream("opennlp/en/opennlp-en-ud-ewt-sentence-1.1-2.4.0.bin")
      val model    = new SentenceModel(inStream)
      val result   = new SentenceDetectorME(model)
      inStream.close()
      result
    }
    val tokenizer = {
      val inStream = new FileInputStream("opennlp/en/opennlp-en-ud-ewt-tokens-1.1-2.4.0.bin")
      val model    = new TokenizerModel(inStream)
      val result   = new TokenizerME(model)
      inStream.close()
      result
    }

    new NGramGenerator(sd, tokenizer, maxTokens)
  }
}
