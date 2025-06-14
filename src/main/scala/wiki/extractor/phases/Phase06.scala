package wiki.extractor.phases

import wiki.db.{PageMarkupSource, Storage}
import wiki.extractor.language.LanguageLogic
import wiki.extractor.types.{PageType, TypedPageMarkup, Worker}
import wiki.extractor.util.{ConfiguredProperties, DBLogging}
import wiki.extractor.{LabelAccumulator, PageLabelProcessor}

class Phase06(db: Storage) extends Phase(db: Storage) {

  override def run(): Unit = {
    db.phase.deletePhase(number)
    db.phase.createPhase(number, s"Gathering page-level label statistics")
    db.label.clearOccurrenceCounts()
    DBLogging.info("Loading link label statistics from db")
    val counter     = db.label.read()
    val ll          = LanguageLogic.getLanguageLogic(props.language.code)
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

  override def number: Int               = 6
  override val incompleteMessage: String = s"Phase $number incomplete -- redoing"
}
