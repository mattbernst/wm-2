package wiki.db

import wiki.extractor.types.{DumpPage, Namespace}
import wiki.extractor.util.Logging

import java.util.concurrent.ArrayBlockingQueue
import scala.collection.mutable


/**
 * A writer with an internal buffer that continually buffers DumpPages and
 * writes them in batches to the page and page_markup database tables.
 *
 * @param writer     A database storage writer
 * @param queueSize  The maximum number of DumpPages enqueued before writing
 */
class PageWriter(writer: Storage, queueSize: Int = Short.MaxValue) extends Logging {
  /**
   * Enqueue one page for writing. The page will be automatically written
   * by the continually running writerThread.
   *
   * @param page A structured DumpPage
   */
  def addPage(page: DumpPage): Unit =
    queue.put(page)

  val writerThread: Thread = {
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

  def stopWriting(): Unit = this.synchronized {
    availableForWriting = false
  }

  private def write(): Unit = {
    val pages = if (!queue.isEmpty) {
      this.synchronized {
        val entries = queue.toArray.toSeq.map(_.asInstanceOf[DumpPage])
        queue.clear()
        entries
      }
    }
    else {
      Seq()
    }
    if (pages.nonEmpty) {
      // Write any unknown namespaces
      pages
        .map(_.namespace)
        .toSet
        .diff(seenNamespaces)
        .foreach { namespace =>
          writer.writeNamespace(namespace)
          seenNamespaces.add(namespace)
        }

      // Write page descriptors and markup
      writer.writeDumpPages(pages)
      writer.writeMarkups(pages)
      pageCount += pages.length
    }
    else {
      this.synchronized {
        if (!availableForWriting) {
          finished = true
        }
      }
    }
  }

  var pageCount: Int = 0
  private var availableForWriting: Boolean = true
  private var finished: Boolean = false
  private lazy val queue = new ArrayBlockingQueue[DumpPage](queueSize)
  private lazy val seenNamespaces = mutable.Set[Namespace]()
}
