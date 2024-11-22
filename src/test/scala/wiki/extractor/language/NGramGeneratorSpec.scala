package wiki.extractor.language

import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}
import opennlp.tools.tokenize.{TokenizerME, TokenizerModel}
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

    val result  = ngg.generate(input)
    val strings = NGram.sliceString(input, result).toList

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

    val result  = ngg.generate(input)
    val strings = NGram.sliceString(input, result).toList

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

    val result  = ngg.generate(input)
    val strings = NGram.sliceString(input, result).toList

    strings shouldBe expected
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
