package wiki.db

import wiki.extractor.util.{DBLogging, Progress}

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import scala.collection.mutable.ListBuffer

/**
  * A writer with an internal buffer that continually buffers depth records,
  * then writes them in batches to the depth table.
  *
  * @param db        A database storage writer
  * @param queueSize The maximum number of depth records enqueued before writing
  */
class DepthSink(db: Storage, queueSize: Int = Storage.batchSqlSize * 2) {

  /**
    * Enqueue one page depth record for writing. The data will be
    * automatically written by the continually running writerThread.
    *
    * @param depth A page depth record with depth and route to root page
    */
  def addDepth(depth: PageDepth): Unit = {
    queue.put(QueueEntry(depth))
    depthCount += 1
    Progress.tick(depthCount, "+")
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
    DBLogging.info("Starting writerThread for DepthSink")
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
      val buffer  = new ListBuffer[QueueEntry]
      while (!emptied && buffer.size < queueSize) {
        Option(queue.poll(3, TimeUnit.SECONDS)) match {
          case Some(entry) => buffer.append(entry)
          case None        => emptied = true
        }
      }

      buffer.toSeq
    }

    if (unwritten.nonEmpty) {
      val depths = unwritten.map(_.depth)
      db.depth.write(depths)
    } else {
      this.synchronized {
        if (!availableForWriting) {
          finished = true
        }
      }
    }
  }

  private case class QueueEntry(depth: PageDepth)

  var depthCount: Int                      = 0
  private var availableForWriting: Boolean = true
  private var finished: Boolean            = false
  private lazy val queue                   = new ArrayBlockingQueue[QueueEntry](queueSize)
}
