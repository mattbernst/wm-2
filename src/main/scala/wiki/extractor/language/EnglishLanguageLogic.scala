package wiki.extractor.language
import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}

import java.io.FileInputStream

object EnglishLanguageLogic extends LanguageLogic {

  /**
    * Use heuristics to get the first paragraph and first sentence of content
    * from a potentially large input string representing a plain-text version
    * of a Wikipedia page.
    *
    * Heuristic: Get groups of sentences separated by newlines. The first
    * multi-sentence group is the first paragraph. The first sentence is taken
    * from the first paragraph. If there are no multi-sentence groups, but there
    * are single-sentence groups, then the first paragraph is None and the first
    * sentence is the first sentence from the input. If there are no sentences
    * at all, then both firstParagraph and firstSentence are None.
    *
    * @param input Text to process for extraction
    * @return A snippet of extracted text content
    */
  override def getSnippet(input: String): Snippet = {
    val lines = input.split('\n')

    val firstParaGroup = lines.iterator
      .map(chunk => sentenceDetector.get().sentDetect(chunk))
      .find(_.length > 1)

    val firstParagraph = firstParaGroup.map(_.mkString(" "))
    val firstSentence = firstParaGroup
      .flatMap(_.headOption)
      .orElse {
        lines.iterator
          .map(chunk => sentenceDetector.get().sentDetect(chunk))
          .find(_.nonEmpty)
          .map(_.head)
      }

    Snippet(firstParagraph = firstParagraph, firstSentence = firstSentence)
  }

  // OpenNLP offers models for other languages here:
  // https://opennlp.apache.org/models.html
  // This needs to be a ThreadLocal because OpenNLP is not thread-safe
  private val sentenceDetector = new ThreadLocal[SentenceDetectorME] {

    override def initialValue(): SentenceDetectorME = {
      val inStream = new FileInputStream("opennlp/en/opennlp-en-ud-ewt-sentence-1.1-2.4.0.bin")
      val model    = new SentenceModel(inStream)
      val result   = new SentenceDetectorME(model)
      inStream.close()
      result
    }
  }
}
