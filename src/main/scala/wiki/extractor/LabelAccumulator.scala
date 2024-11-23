package wiki.extractor

import wiki.db.Storage
import wiki.extractor.types.LabelCounter
import wiki.extractor.util.{DBLogging, Progress}

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class LabelAccumulator(counter: LabelCounter, queueSize: Int = Storage.batchSqlSize) {

  /**
    * Add all page labels for a single page. The page labels will be used to
    * update label statistics for occurrence_count and occurrence_doc_count
    * in the label table.
    *
    * @param pageLabels A map of valid labels to counts for one Wikipedia page
    */
  def enqueue(pageLabels: mutable.Map[String, Int]): Unit = {
    queue.put(QueueEntry(pageLabels.toMap))
    linkCount += 1
    Progress.tick(linkCount, "+", 10_000)
  }

  def stopWriting(): Unit = this.synchronized {
    availableForWriting = false
  }

  val accumulatorThread: Thread = {
    val thread = new Thread(() => {
      while (!finished) {
        write()
      }
    })
    thread.setDaemon(true)
    DBLogging.info("Starting accumulatorThread for LabelAccumulator")
    thread.start()
    thread
  }

  /**
    * Consume items from the queue and accumulate them into the counter. If
    * availableForWriting is false and there is nothing left in the queue,
    * finished will be set to false so that the accumulator thread can stop.
    */
  private def write(): Unit = {
    val unwritten: Seq[QueueEntry] = {
      var emptied = false
      val buffer  = ListBuffer[QueueEntry]()
      while (!emptied && buffer.size < queueSize / 2) {
        Option(queue.poll(3, TimeUnit.SECONDS)) match {
          case Some(entry) => buffer.append(entry)
          case None        => emptied = true
        }
      }

      buffer.toSeq
    }

    if (unwritten.nonEmpty) {
      unwritten.map(_.data).foreach { pageLabels =>
        counter.updateOccurrences(pageLabels)
      }
    } else {
      this.synchronized {
        if (!availableForWriting) {
          finished = true
        }
      }
    }
  }

  private case class QueueEntry(data: Map[String, Int])

  var linkCount: Int                       = 0
  private var availableForWriting: Boolean = true
  private var finished: Boolean            = false
  private lazy val queue                   = new ArrayBlockingQueue[QueueEntry](queueSize)
}
