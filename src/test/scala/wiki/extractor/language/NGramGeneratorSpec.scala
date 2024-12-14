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

  it should "generate word based ngrams from a block of text" in {
    val ngg   = generator(3)
    val input = """Indigenous languages of Canada:
                  |
                  |    Abenaki, 10 speakers
                  |    Dane-zaa, 300 speakers
                  |    Cayuga, 360 speakers
                  |    Delaware (Munsee), fewer than 10 speakers""".stripMargin
    val expected = List(
      "Indigenous",
      "Indigenous languages",
      "Indigenous languages of",
      "languages",
      "languages of",
      "languages of Canada",
      "of",
      "of Canada",
      "Canada",
      "Abenaki",
      "Abenaki, 10",
      "10",
      "10 speakers",
      "speakers",
      "Dane",
      "Dane-zaa",
      "-zaa",
      "-zaa, 300",
      "300",
      "300 speakers",
      "speakers",
      "Cayuga",
      "Cayuga, 360",
      "360",
      "360 speakers",
      "speakers",
      "Delaware",
      "Delaware (Munsee",
      "Munsee",
      "fewer",
      "fewer than",
      "fewer than 10",
      "than",
      "than 10",
      "than 10 speakers",
      "10",
      "10 speakers",
      "speakers"
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

  behavior of "generateFast"

  it should "directly generate string-ngrams" in {
    val ngg   = generator(3)
    val input = "The order of the memory bytes storing the bits varies; see endianness."
    val expected = List(
      "The order of",
      "The order",
      "The",
      "order of the",
      "order of",
      "order",
      "of the memory",
      "of the",
      "of",
      "the memory bytes",
      "the memory",
      "the",
      "memory bytes storing",
      "memory bytes",
      "memory",
      "bytes storing the",
      "bytes storing",
      "bytes",
      "storing the bits",
      "storing the",
      "storing",
      "the bits varies",
      "the bits",
      "the",
      "bits varies;",
      "bits varies",
      "bits",
      "varies; see",
      "varies;",
      "varies",
      "; see endianness",
      "; see",
      ";",
      "see endianness.",
      "see endianness",
      "see",
      "endianness.",
      "endianness",
      "."
    )
    val result = ngg.generateFast(input).toList

    result shouldBe expected
  }

  it should "generate all the strings generated by generate" in {
    // generateFast also generates more strings than generate does, because
    // it does not guard against ngrams that start/end with punctuation.
    val ngg   = generator(3)
    val input = "The order of the memory bytes storing the bits varies; see endianness."

    val result     = ngg.generate(input).toList
    val strings    = NGram.sliceString(input, result)
    val resultFast = ngg.generateFast(input).toList

    strings.foreach(s => resultFast.contains(s) shouldBe true)
  }

  it should "filter results by allowed strings" in {
    val ngg         = generator(10)
    val nggFiltered = generator(10, collection.Set("figure skater", "Ando", "Japanese"))
    val input       = """Ando (Japanese: 安藤) is a common Japan surname. Notable people with this name are listed below.
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

    val resultRaw      = ngg.generateFast(input).toList
    val resultFiltered = nggFiltered.generateFast(input).toList

    val expected = List(
      "Ando",
      "Japanese",
      "Ando",
      "Ando",
      "Ando",
      "figure skater",
      "Ando",
      "Ando",
      "Ando",
      "Ando",
      "Ando"
    )

    resultRaw.length should be > resultFiltered.length
    resultFiltered shouldBe expected
  }

  def generator(maxTokens: Int, allowedStrings: collection.Set[String] = collection.Set()) = {
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

    new NGramGenerator(
      sentenceDetector = sd,
      tokenizer = tokenizer,
      maxTokens = maxTokens,
      allowedStrings = allowedStrings
    )
  }
}
