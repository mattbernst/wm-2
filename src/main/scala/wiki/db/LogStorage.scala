package wiki.db
import org.slf4j.event.Level
import scalikejdbc.*

case class StoredLog(level: Level, message: String, timestamp: Long)

trait LogStorage {

  /**
    * Write a message to the log table.
    *
    * @param level     Logging level of the message
    * @param message   The actual message
    * @param timestamp When the message was originally generated
    */
  def writeLog(level: Level, message: String, timestamp: Long = System.currentTimeMillis()): Unit = {
    DB.autoCommit { implicit session =>
      sql"""INSERT INTO log VALUES ($level, $timestamp, $message)"""
        .update(): Unit
    }
  }

  /**
    * Read all logs matching timestmap. Used for testing.
    *
    * @param timestamp The numeric timestamp of logs to retrieve
    * @return All matching logs
    */
  def readLogs(timestamp: Long): Seq[StoredLog] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT * FROM log WHERE ts=$timestamp""".map { rs =>
        val level = Level.valueOf(rs.string("log_level"))
        StoredLog(level = level, message = rs.string("message"), timestamp = rs.long("ts"))
      }.list()
    }
  }
}
