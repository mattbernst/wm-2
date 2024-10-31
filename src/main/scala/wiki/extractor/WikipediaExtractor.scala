package wiki.extractor

import wiki.db.*
import wiki.extractor.phases.{Phase01, Phase02, Phase03, Phase04}
import wiki.extractor.util.{Config, DBLogging, Logging}

object WikipediaExtractor extends Logging {

  def main(args: Array[String]): Unit = {
    DBLogging.initDb(db)
    init()

    lazy val phase01 = new Phase01(db, Config.props)
    lazy val phase02 = new Phase02(db, Config.props)
    lazy val phase03 = new Phase03(db, Config.props)
    lazy val phase04 = new Phase04(db, Config.props)

    db.phase.getPhaseState(1) match {
      case Some(COMPLETED) =>
        logger.info(phase01.finishedMessage)
      case Some(CREATED) =>
        logger.warn(phase01.incompleteMessage)
        phase01.run(args)
      case None =>
        phase01.run(args)
    }
    db.phase.getPhaseState(2) match {
      case Some(COMPLETED) =>
        logger.info(phase02.finishedMessage)
      case Some(CREATED) =>
        logger.warn(phase02.incompleteMessage)
        phase02.run()
      case None =>
        phase02.run()
    }
    db.phase.getPhaseState(3) match {
      case Some(COMPLETED) =>
        logger.info(phase03.finishedMessage)
      case Some(CREATED) =>
        logger.warn(phase03.incompleteMessage)
        phase03.run()
      case None =>
        phase03.run()
    }
    db.phase.getPhaseState(4) match {
      case Some(COMPLETED) =>
        logger.info(phase04.finishedMessage)
      case Some(CREATED) =>
        logger.warn(phase04.incompleteMessage)
        phase04.run()
      case None =>
        phase04.run()
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
