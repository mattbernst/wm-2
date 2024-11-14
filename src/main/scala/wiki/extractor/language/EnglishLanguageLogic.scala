package wiki.extractor.language
import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}

import java.io.FileInputStream

object EnglishLanguageLogic extends LanguageLogic {

  // OpenNLP offers models for other languages here:
  // https://opennlp.apache.org/models.html
  // This needs to be a ThreadLocal because OpenNLP is not thread-safe
  protected val sentenceDetector: ThreadLocal[SentenceDetectorME] = new ThreadLocal[SentenceDetectorME] {

    override def initialValue(): SentenceDetectorME = {
      val inStream = new FileInputStream("opennlp/en/opennlp-en-ud-ewt-sentence-1.1-2.4.0.bin")
      val model    = new SentenceModel(inStream)
      val result   = new SentenceDetectorME(model)
      inStream.close()
      result
    }
  }
}
