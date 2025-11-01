package wiki.extractor.phases

import wiki.db.{PageMarkupSource, Storage}
import wiki.extractor.language.LanguageLogic
import wiki.extractor.types.{PageType, TypedPageMarkup, Worker}
import wiki.extractor.util.DBLogging
import wiki.extractor.{LabelAccumulator, PageLabelProcessor}
import wiki.util.ConfiguredProperties

class Phase05(db: Storage) extends Phase(db: Storage) {

  override def run(): Unit = {
    db.phase.deletePhase(number)
    db.phase.createPhase(number, s"Gathering page-level label statistics")
    db.label.clearOccurrenceCounts()
    DBLogging.info("Loading link label statistics from db")
    val counter     = db.label.read()
    val ll          = LanguageLogic.getLanguageLogic(props.language.code, db)
    val goodLabels  = counter.getLabels()
    val source      = new PageMarkupSource(db)
    val accumulator = new LabelAccumulator(counter)
    val processor   = new PageLabelProcessor(ll, goodLabels)
    val workers     = assignLabelWorkers(props.nWorkers, processor, source.getFromQueue _, accumulator)
    DBLogging.info("Gathering page label statistics")
    val relevantPages: Set[PageType] = Set(PageType.ARTICLE, PageType.DISAMBIGUATION)
    source.enqueueMarkup(relevantPages)
    workers.foreach(_.thread.join())
    accumulator.stopWriting()
    accumulator.accumulatorThread.join()

    val completed = accumulator.count
    DBLogging.info(s"Storing page label statistics to db (processed $completed pages)")
    db.label.write(counter)
    db.phase.completePhase(number)
  }

  private def assignLabelWorkers(
    n: Int,
    processor: PageLabelProcessor,
    source: () => Option[TypedPageMarkup],
    accumulator: LabelAccumulator
  ): Seq[Worker] = {
    0.until(n).map { id =>
      processor.worker(id = id, source = source, accumulator = accumulator)
    }
  }

  private lazy val props: ConfiguredProperties =
    db.configuration.readConfiguredPropertiesOptimistic()

  override def number: Int               = 5
  override val incompleteMessage: String = s"Phase $number incomplete -- redoing"
}
