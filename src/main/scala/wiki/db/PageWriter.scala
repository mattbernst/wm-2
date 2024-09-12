package wiki.db

import wiki.extractor.types.{DumpPage, Namespace, PageMarkup, PageMarkup_Z}
import wiki.extractor.util.Logging

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
 * A writer with an internal buffer that continually buffers DumpPages and
 * markup, then writes them in batches to the page and page_markup or
 * page_markup_z database tables. Since it is optimized for write speed while
 * processing a new dump file, it sets SQLite pragmas that are unsafe for
 * general use.
 *
 * The caller can choose between markup readability (for debugging and
 * spelunking) and compact storage by supplying either string PageMarkup
 * data for the page_markup table or compressed binary PageMarkupZ data
 * for the page_markup_z table.
 *
 * @param db        A database storage writer
 * @param queueSize The maximum number of pages enqueued before writing
 */
class PageWriter(db: Storage, queueSize: Int = 65000) extends Logging {
  def enableSqliteFastPragmas(): Unit = {
    val pragmas = Seq(
      "pragma cache_size=1048576;",
      "pragma threads=4;",
      "pragma journal_mode=off;",
      "pragma synchronous=off;"
    )

    pragmas.foreach { pragma =>
      db.executeUnsafely(pragma)
      logger.info(s"Applied SQLite pragma: $pragma")
    }
  }

  /**
   * Enqueue one page for writing along with its markup. The caller supplies
   * uncompressed or compressed markup. The data will be automatically written
   * by the continually running writerThread.
   *
   * @param page A structured DumpPage
   */
  def addPage(page: DumpPage,
              markup: Option[PageMarkup],
              markup_Z: Option[PageMarkup_Z]): Unit =
    queue.put(QueueEntry(page = page, markup = markup, markup_Z = markup_Z))

  def stopWriting(): Unit = this.synchronized {
    availableForWriting = false
  }

  val writerThread: Thread = {
    enableSqliteFastPragmas()
    val thread = new Thread(() => {
      while (!finished) {
        write()
      }
    })
    thread.setDaemon(true)
    logger.info("Starting writerThread for PageWriter")
    thread.start()
    thread
  }

  private def write(): Unit = {
    val unwritten = {
      var emptied = false
      val buffer = new ListBuffer[QueueEntry]
      while (!emptied && buffer.size < db.batchInsertSize) {
        Option(queue.poll(1, TimeUnit.SECONDS)) match {
          case Some(entry) => buffer.append(entry)
          case None => emptied = true
        }
      }

      buffer.toSeq
    }

    val pages = unwritten.map(_.page)
    if (pages.nonEmpty) {
      // Write any unknown namespaces as they are encountered
      unwritten
        .map(_.page)
        .map(_.namespace)
        .toSet
        .diff(seenNamespaces)
        .foreach { namespace =>
          db.writeNamespace(namespace)
          seenNamespaces.add(namespace)
        }

      // Write page descriptors and markup
      db.writeDumpPages(pages)
      val markups = unwritten.flatMap(_.markup).map(e => (e.pageId, e.text))
      if (markups.nonEmpty) {
        db.writeMarkups(markups)
      }
      val markupsZ = unwritten.flatMap(_.markup_Z).map(e => (e.pageId, e.text))
      if (markupsZ.nonEmpty) {
        db.writeMarkups_Z(markupsZ)
      }
      pageCount += unwritten.length
    }
    else {
      this.synchronized {
        if (!availableForWriting) {
          finished = true
        }
      }
    }
  }

  private case class QueueEntry(page: DumpPage, markup: Option[PageMarkup], markup_Z: Option[PageMarkup_Z])

  var pageCount: Int = 0
  private var availableForWriting: Boolean = true
  private var finished: Boolean = false
  private lazy val queue = new ArrayBlockingQueue[QueueEntry](queueSize)
  private lazy val seenNamespaces = mutable.Set[Namespace]()
}
