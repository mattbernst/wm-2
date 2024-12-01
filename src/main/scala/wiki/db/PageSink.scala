package wiki.db

import wiki.extractor.types.*
import wiki.extractor.util.{DBLogging, Progress}

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * A writer with an internal buffer that continually buffers Pages and
  * markup, then writes them in batches to the page and markup or markup_z
  * database tables. Since it is optimized for write speed while processing
  * a new dump file, it sets SQLite pragmas that are unsafe for general use.
  *
  * The caller can choose between markup readability (for debugging and
  * spelunking) and compact storage by supplying either string PageMarkup_U
  * data for the markup table or compressed binary PageMarkup_Z data
  * for the markup_z table.
  *
  * @param db        A database storage writer
  * @param queueSize The maximum number of pages enqueued before writing
  */
class PageSink(db: Storage, queueSize: Int = Storage.batchSqlSize * 2) {

  /**
    * Enqueue one page for writing along with its markup. The caller supplies
    * uncompressed or compressed markup. The data will be automatically written
    * by the continually running writerThread.
    *
    * @param page A structured Page
    * @param markupU Optional uncompressed-text PageMarkup
    * @param markupZ Optional compressed-text PageMarkup
    */
  def addPage(page: Page, markupU: Option[PageMarkup_U], markupZ: Option[PageMarkup_Z]): Unit = {
    queue.put(QueueEntry(page = page, markupU = markupU, markupZ = markupZ))
    pageCount += 1
    Progress.tick(pageCount, "+")
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
    DBLogging.info("Starting writerThread for PageSink")
    thread.start()
    thread
  }

  // Mark page as UNPARSEABLE when Sweble fails to parse wikitext
  def markUnparseable(pageId: Int): Unit = {
    db.page.updatePageType(pageId, PageType.UNPARSEABLE)
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
      while (!emptied && buffer.size < Storage.batchSqlSize) {
        Option(queue.poll(3, TimeUnit.SECONDS)) match {
          case Some(entry) => buffer.append(entry)
          case None        => emptied = true
        }
      }

      buffer.toSeq
    }

    val pages = unwritten.map(_.page)
    if (pages.nonEmpty) {
      // Write any unknown namespaces as they are encountered
      unwritten.map(_.page).map(_.namespace).toSet.diff(seenNamespaces).foreach { namespace =>
        db.namespace.write(namespace)
        seenNamespaces.add(namespace)
      }

      // Write page descriptors and markup
      db.page.writePages(pages)
      val markups = unwritten.flatMap(_.markupU)
      if (markups.nonEmpty) {
        db.page.writeMarkups(markups)
      }
      val markupsZ = unwritten.flatMap(_.markupZ)
      if (markupsZ.nonEmpty) {
        db.page.writeMarkups_Z(markupsZ)
      }
    } else {
      this.synchronized {
        if (!availableForWriting) {
          finished = true
        }
      }
    }
  }

  private case class QueueEntry(page: Page, markupU: Option[PageMarkup_U], markupZ: Option[PageMarkup_Z])

  var pageCount: Int                       = 0
  private var availableForWriting: Boolean = true
  private var finished: Boolean            = false
  private lazy val queue                   = new ArrayBlockingQueue[QueueEntry](queueSize)
  private lazy val seenNamespaces          = mutable.Set[Namespace]()
}
