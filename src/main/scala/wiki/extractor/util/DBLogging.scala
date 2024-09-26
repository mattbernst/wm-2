package wiki.extractor.util

import org.slf4j.event.Level
import wiki.db.Storage

object DBLogging extends Logging {

  def info(message: String): Unit = {
    db match {
      case Some(_) => write(Level.INFO, message)
      case None    => logger.info(message)
    }
  }

  def warn(message: String): Unit = {
    db match {
      case Some(_) => write(Level.WARN, message)
      case None    => logger.warn(message)
    }
  }

  def error(message: String): Unit = {
    db match {
      case Some(_) => write(Level.ERROR, message)
      case None    => logger.error(message)
    }
  }

  def initDb(s: Storage): Unit = {
    db = Some(s)
  }

  private def write(level: Level, message: String): Unit = {
    val now = System.currentTimeMillis()
    db.foreach(_.log.write(level = level, message = message, timestamp = now))
  }

  private var db: Option[Storage] = None
}
