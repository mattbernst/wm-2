package wiki.db

import wiki.extractor.types.{DumpPage, Namespace}
import wiki.extractor.util.Logging

import java.util.concurrent.ArrayBlockingQueue
import scala.collection.mutable


/**
 * A writer with an internal buffer that continually buffers DumpPages and
 * writes them in batches to the page and page_markup database tables. Since
 * it is optimized for write speed while processing a new dump file, it sets
 * SQLite pragmas that are unsafe for general use.
 *
 * @param writer     A database storage writer
 * @param queueSize  The maximum number of DumpPages enqueued before writing
 */
class PageWriter(writer: Storage, queueSize: Int = 65000) extends Logging {
  def enableSqliteFastPragmas(): Unit = {
    val pragmas = Seq(
      "pragma cache_size=1048576;",
      "pragma threads=4;",
      "pragma journal_mode=off;",
      "pragma synchronous=off;"
    )

    pragmas.foreach { pragma =>
      writer.executeUnsafely(pragma)
      logger.info(s"Applied SQLite pragma: $pragma")
    }
  }

  /**
   * Enqueue one page for writing. The page will be automatically written
   * by the continually running writerThread.
   *
   * @param page A structured DumpPage
   */
  def addPage(page: DumpPage): Unit =
    queue.put(page)

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
      // Write any unknown namespaces as they are encountered
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
      writer.writeMarkups(pages.map(p => (p.id, p.text)))
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
