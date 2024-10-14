package wiki.db

import wiki.extractor.util.{DBLogging, Progress}

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import scala.collection.mutable.ListBuffer

/**
  * A writer with an internal buffer that continually buffers links, then writes
  * them in batches to the link and dead_link database tables.
  *
  * @param db        A database storage writer
  * @param queueSize The maximum number of links enqueued before writing
  */
class LinkSink(db: Storage, queueSize: Int = 100000) {

  /**
    * Enqueue one link (resolved or dead) for writing. The data will be
    * automatically written by the continually running writerThread.
    *
    * @param resolvedLink A link with link text mapped to numeric IDs
    * @param deadLink A link that could not resolve to a numeric destination
    */
  def addLink(resolvedLink: Option[ResolvedLink], deadLink: Option[DeadLink]): Unit = {
    queue.put(QueueEntry(resolvedLink = resolvedLink, deadLink = deadLink))
    linkCount += 1
    Progress.tick(linkCount, "+", 100000)
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
    DBLogging.info("Starting writerThread for PageWriter")
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
      val resolvedLinks = unwritten.flatMap(_.resolvedLink)
      if (resolvedLinks.nonEmpty) {
        db.link.writeResolved(resolvedLinks)
      }
      val deadLinks = unwritten.flatMap(_.deadLink)
      if (deadLinks.nonEmpty) {
        db.link.writeDead(deadLinks)
      }
    } else {
      this.synchronized {
        if (!availableForWriting) {
          finished = true
        }
      }
    }
  }

  private case class QueueEntry(resolvedLink: Option[ResolvedLink], deadLink: Option[DeadLink])

  var linkCount: Int                       = 0
  private var availableForWriting: Boolean = true
  private var finished: Boolean            = false
  private lazy val queue                   = new ArrayBlockingQueue[QueueEntry](queueSize)
}
