package wiki.extractor

import wiki.db.*
import wiki.extractor.phases.*
import wiki.extractor.util.{Config, DBLogging, Logging}

object WikipediaExtractor extends Logging {

  def main(args: Array[String]): Unit = {
    // Initialize database with name passed as first command line argument,
    // if first command line argument ends in ".db". Otherwise, the name will
    // be automatically generated from the environmental configuration.
    val db = database(args.headOption)
    DBLogging.initDb(db)

    val phases = Array(
      new Phase01(db),
      new Phase02(db),
      new Phase03(db),
      new Phase04(db),
      new Phase05(db),
      new Phase06(db)
    )

    // Update lastPhase whenever adding a new phase
    assert(phases.length == db.phase.lastPhase, "The number of phases does not match lastPhase.")

    phases.indices.foreach { index =>
      val phase = index + 1

      // Phase 1 runs with command line argument giving Wikipedia dump location
      if (phase == 1) {
        db.phase.getPhaseState(phase) match {
          case Some(PhaseState.COMPLETED) =>
            logger.info(phases(index).finishedMessage)
          case Some(PhaseState.CREATED) =>
            logger.warn(phases(index).incompleteMessage)
            phases(index).run(args)
          case None =>
            // First run only: initialize the database and store the
            // environmental configuration to it.
            init(db)
            db.configuration.write(Config.props)
            phases(index).run(args)
        }
      }

      // Subsequent phases do not use command line arguments
      else if (phase > 1) {
        db.phase.getPhaseState(phase) match {
          case Some(PhaseState.COMPLETED) =>
            logger.info(phases(index).finishedMessage)
          case Some(PhaseState.CREATED) =>
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
  private def init(db: Storage): Unit = {
    db.createTableDefinitions(0)
  }

  private def database(diskFileName: Option[String]): Storage = {
    diskFileName match {
      case Some(fileName) if fileName.endsWith(".db") =>
        new Storage(fileName = fileName)
      case _ =>
        new Storage(fileName = Config.props.language.code + "_wiki.db")
    }
  }
}
