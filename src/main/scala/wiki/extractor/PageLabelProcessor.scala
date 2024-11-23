package wiki.extractor

import wiki.extractor.language.LanguageLogic
import wiki.extractor.types.{PageMarkup, TypedPageMarkup, Worker}
import wiki.extractor.util.DBLogging

import scala.collection.mutable

class PageLabelProcessor(languageLogic: LanguageLogic, goodLabels: collection.Set[String]) {

  /**
    * Get a count of all valid labels that could be extracted from the plain
    * text version of a Wikipedia page. The plain text version of the page gets
    * converted to a sequence of token based ngrams which are then filtered against
    * goodLabels before their counts get added up in the return map.
    *
    * TODO? See if whitespace handling in plain-text conversion can
    * be improved to match more labels.
    *
    * @param tpm PageMarkup data, including plain text version of page
    * @return    A map of valid labels to counts from within the page
    */
  def extract(tpm: TypedPageMarkup): mutable.Map[String, Int] = {
    val pm: PageMarkup = if (tpm.pmu.nonEmpty) {
      PageMarkup.deserializeUncompressed(tpm.pmu.get)
    } else {
      PageMarkup.deserializeCompressed(tpm.pmz.get)
    }

    val pageNgrams = mutable.Map[String, Int]().withDefaultValue(0)

    pm.parseResult
      .map(_.text)
      .foreach { plainText =>
        languageLogic
          .wordNgrams(plainText, goodLabels)
          .foreach(label => pageNgrams(label) += 1)
      }

    pageNgrams
  }

  def worker(id: Int, source: () => Option[TypedPageMarkup], accumulator: LabelAccumulator): Worker = {
    val thread = new Thread(() => {
      var completed = false
      while (!completed) {
        source() match {
          case Some(tpm) =>
            val results = extract(tpm)
            accumulator.enqueue(results)
          case None =>
            completed = true
            DBLogging.info(s"PageLabelProcessor Worker $id finished")
        }
      }
    })
    DBLogging.info(s"Starting PageLabelProcessor Worker $id")
    thread.setDaemon(true)
    thread.start()
    Worker(thread)
  }
}