package wiki.extractor.language
import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}
import opennlp.tools.tokenize.{TokenizerME, TokenizerModel}

import java.io.FileInputStream

object FrenchLanguageLogic extends LanguageLogic {

  // This needs to be a ThreadLocal because OpenNLP is not thread-safe
  protected val sentenceDetector: ThreadLocal[SentenceDetectorME] = new ThreadLocal[SentenceDetectorME] {

    override def initialValue(): SentenceDetectorME = {
      val inStream = new FileInputStream("opennlp/fr/opennlp-fr-ud-gsd-sentence-1.1-2.4.0.bin")
      val model    = new SentenceModel(inStream)
      val result   = new SentenceDetectorME(model)
      inStream.close()
      result
    }
  }

  protected val tokenizer: ThreadLocal[TokenizerME] = new ThreadLocal[TokenizerME] {

    override def initialValue(): TokenizerME = {
      val inStream = new FileInputStream("opennlp/fr/opennlp-fr-ud-gsd-tokens-1.1-2.4.0.bin")
      val model    = new TokenizerModel(inStream)
      val result   = new TokenizerME(model)
      inStream.close()
      result
    }
  }
}
