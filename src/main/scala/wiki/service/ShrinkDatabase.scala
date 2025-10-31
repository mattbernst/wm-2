package wiki.service

import wiki.db.Storage
import wiki.extractor.types.{PageMarkup, PageMarkup_U, PageMarkup_Z}
import wiki.extractor.util.Progress
import wiki.util.{FileHelpers, Logging}

import java.nio.file.NoSuchFileException
import java.util.concurrent.{ArrayBlockingQueue, ForkJoinPool}
import scala.collection.parallel.CollectionConverters.*
import scala.collection.parallel.ForkJoinTaskSupport
import scala.jdk.CollectionConverters.*

object ShrinkDatabase extends ModelProperties with Logging {

  def main(args: Array[String]): Unit = {

    val conf = new ServiceConf(args.toIndexedSeq)
    val databaseFileName = conf.database.toOption
      .orElse(inferDbFile())
      .getOrElse(throw new RuntimeException("No database file found or given!"))

    if (FileHelpers.isFileReadable(databaseFileName)) {
      db = new Storage(fileName = databaseFileName)
    } else {
      throw new NoSuchFileException(s"Database file $databaseFileName is not readable")
    }

    val beforeSize = FileHelpers.getFileSize(databaseFileName)

    val ops = new ServiceOps(db, defaultServiceParams)
    ops.validateWordSenseModel()
    ops.validateLinkingModel()
    logger.info("Pruning non-essential page contents data")
    pruneMarkup()
    logger.info("Vacuuming database to reclaim space")
    db.executeUnsafely("VACUUM;")
    val afterSize = FileHelpers.getFileSize(databaseFileName)
    val ratio     = (afterSize / beforeSize.toDouble).toString.take(4)
    logger.info(s"Completed optimization. Size before: $beforeSize After: $afterSize Ratio: $ratio")
  }

  private class MarkupWriter[T](serialize: PageMarkup => T, write: Seq[T] => Unit) {
    private val queue      = new ArrayBlockingQueue[T](Storage.batchSqlSize * 2)
    private var nProcessed = 0

    def add(pm: PageMarkup): Unit = {
      queue.add(serialize(pm))
      if (queue.size() >= Storage.batchSqlSize) {
        flush()
      }
    }

    def flush(): Unit = queue.synchronized {
      if (!queue.isEmpty) {
        val toWrite = new java.util.ArrayList[T]()
        queue.drainTo(toWrite)
        nProcessed += toWrite.size()
        Progress.tick(count = nProcessed, marker = ".")
        if (!toWrite.isEmpty) {
          write(toWrite.asScala.toSeq)
        }
      }
    }
  }

  /**
    * Selectively prune unneeded data from the markup table once model
    * training has completed. This reclaims a large amount of database space.
    */
  private def pruneMarkup(): Unit = {
    val pages       = db.page.getCompletedPageIds().toSeq.sorted
    val pool        = new ForkJoinPool(props.nWorkers)
    val taskSupport = new ForkJoinTaskSupport(pool)

    // Select appropriate writer based on compression setting
    val writer = if (db.page.usingCompression) {
      new MarkupWriter[PageMarkup_Z](
        PageMarkup.serializeCompressed,
        db.page.writeMarkups_Z
      )
    } else {
      new MarkupWriter[PageMarkup_U](
        PageMarkup.serializeUncompressed,
        db.page.writeMarkups
      )
    }

    val parPages = pages.par
    parPages.tasksupport = taskSupport
    parPages.foreach { pageId =>
      db.page.readMarkupAuto(pageId) match {
        case Some(pm) =>
          // The pruned entry retains the Snippet object inside the parseResult.
          // It drops raw wikitext, raw page text, and link sequences. This
          // preserves only the data used by the live service (model training
          // does not work once this data has been removed).
          val smaller = pm.copy(
            wikitext = None,
            parseResult = pm.parseResult.map(pr => pr.copy(text = "", links = Seq()))
          )
          writer.add(smaller)

        case None =>
      }
    }

    pool.shutdown()
    writer.flush()
  }

  private var db: Storage = _
  private lazy val props  = db.configuration.readConfiguredPropertiesOptimistic()
}
