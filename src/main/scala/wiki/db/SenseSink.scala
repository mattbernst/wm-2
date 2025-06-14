package wiki.db

import wiki.extractor.types.Sense
import wiki.extractor.util.{DBLogging, Progress}

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import scala.collection.mutable.ListBuffer

/**
  * A writer with an internal buffer that continually buffers senses, then writes
  * them in batches to the sense table.
  *
  * @param db        A database storage writer
  * @param queueSize The maximum number of senses enqueued before writing
  */
class SenseSink(db: Storage, queueSize: Int = Storage.batchSqlSize * 2) {

  /**
    * Enqueue one Sense for writing. The data will be automatically written
    * by the continually running writerThread.
    *
    * @param sense A Sense mapping a label ID to counts of its various senses
    */
  def addSense(sense: Sense): Unit = {
    queue.put(QueueEntry(sense))
  }

  def stopWriting(): Unit = this.synchronized {
    availableForWriting = false
  }

  val writerThread: Thread = {
    Storage.enableSqlitePragmas(db)
    val thread = new Thread(() => {
      while (!finished) {
        write()
      }
    })
    thread.setDaemon(true)
    DBLogging.info("Starting writerThread for SenseSink")
    thread.start()
    thread
  }

  /**
    * Consume items from the queue and write them to the database. This batches
    * data for efficient database writes. If availableForWriting is false and
    * there is nothing left in the queue, finished will be set to false so the
    * writer thread can stop.
    */
  private def write(): Unit = {
    val unwritten: Seq[QueueEntry] = {
      var emptied = false
      val buffer  = ListBuffer[QueueEntry]()
      while (!emptied && buffer.size < queueSize) {
        Option(queue.poll(3, TimeUnit.SECONDS)) match {
          case Some(entry) =>
            buffer.append(entry)
            senseCount += 1
            Progress.tick(senseCount, "+", 100_000)
          case None =>
            emptied = true
        }
      }

      buffer.toSeq
    }

    if (unwritten.nonEmpty) {
      val senses = unwritten.map(_.sense)
      if (senses.nonEmpty) {
        db.sense.write(senses)
      }
    } else {
      this.synchronized {
        if (!availableForWriting) {
          finished = true
        }
      }
    }
  }

  private case class QueueEntry(sense: Sense)

  var senseCount: Int                      = 0
  private var availableForWriting: Boolean = true
  private var finished: Boolean            = false
  private lazy val queue                   = new ArrayBlockingQueue[QueueEntry](queueSize)
}
