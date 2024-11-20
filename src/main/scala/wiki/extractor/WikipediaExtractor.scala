package wiki.extractor

import wiki.db.*
import wiki.extractor.phases.*
import wiki.extractor.util.{Config, DBLogging, Logging}

object WikipediaExtractor extends Logging {

  def main(args: Array[String]): Unit = {
    DBLogging.initDb(db)
    init()

    // TODO: add db storage for configured properties so that only phase 1
    // needs environment variables set.
    val configuredProperties = Config.props

    val phases = Array(
      new Phase01(db, Config.props),
      new Phase02(db, configuredProperties),
      new Phase03(db, configuredProperties),
      new Phase04(db, configuredProperties),
      new Phase05(db, configuredProperties),
      new Phase06(db, configuredProperties)
    )

    // Update lastPhase whenever adding a new phase
    assert(phases.length == db.phase.lastPhase)

    phases.indices.foreach { index =>
      val phase = index + 1

      // Phase 1 runs with command line argument giving Wikipedia dump location
      if (phase == 1) {
        db.phase.getPhaseState(phase) match {
          case Some(COMPLETED) =>
            logger.info(phases(index).finishedMessage)
          case Some(CREATED) =>
            logger.warn(phases(index).incompleteMessage)
            phases(index).run(args)
          case None =>
            phases(index).run(args)
        }
      }

      // Subsequent phases do not use command line arguments
      else if (phase > 1) {
        db.phase.getPhaseState(phase) match {
          case Some(COMPLETED) =>
            logger.info(phases(index).finishedMessage)
          case Some(CREATED) =>
            logger.warn(phases(index).incompleteMessage)
            phases(index).run()
          case None =>
            phases(index).run()
        }
      } else {
        throw new IndexOutOfBoundsException(s"Invalid phase/index $phase/$index")
      }
    }

    db.closeAll()
  }

  // Initialize system tables before running any extraction
  private def init(): Unit = {
    db.createTableDefinitions(0)
  }

  private val db: Storage =
    new Storage(fileName = Config.props.language.code + "_wiki.db")
}
