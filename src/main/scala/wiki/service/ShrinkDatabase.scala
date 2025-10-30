package wiki.service

import wiki.db.Storage
import wiki.extractor.types.{PageMarkup, PageMarkup_U}
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
    pruneMarkup()
    logger.info("Vacuuming database to reclaim space")
    db.executeUnsafely("VACUUM;")
    val afterSize = FileHelpers.getFileSize(databaseFileName)
    val ratio     = (afterSize / beforeSize.toDouble).toString.take(4)
    logger.info(s"Completed optimization. Size before: $beforeSize After: $afterSize Ratio: $ratio")
  }

  /**
   *
   * Selectively prune unneeded data from the markup table once model training
   * has completed. This reclaims a large amount of database space.
   */
  /**
   *
   * Selectively prune unneeded data from the markup table once model training
   * has completed. This reclaims a large amount of database space.
   */
  private def pruneMarkup(): Unit = {
    val pages       = db.page.getCompletedPageIds().toSeq
    val pool        = new ForkJoinPool(props.nWorkers)
    val taskSupport = new ForkJoinTaskSupport(pool)

    val parPages = pages.par
    parPages.tasksupport = taskSupport
    parPages.foreach { pageId =>
      db.page.readMarkupAuto(pageId) match {
        case Some(pm) =>
          // The pruned entry retains the Snippet object inside the parseResult.
          // It drops raw wikitext, raw page text, and link sequences. This
          // preserves only the data used by the live service (model training
          // does not work once this data has been removed).
          val smaller =
            pm.copy(wikitext = None, parseResult = pm.parseResult.map(pr => pr.copy(text = "", links = Seq())))
          val prunedEntry: PageMarkup_U = PageMarkup.serializeUncompressed(smaller)
          puQueue.add(prunedEntry)

          // Write batch when queue reaches threshold
          if (puQueue.size() >= Storage.batchSqlSize) {
            flushQueue()
          }

        case None =>
      }
    }

    pool.shutdown()

    // Flush any remaining entries
    if (!puQueue.isEmpty) {
      val remaining = new java.util.ArrayList[PageMarkup_U]()
      puQueue.drainTo(remaining)
      import scala.jdk.CollectionConverters.*
      db.page.writeMarkups(remaining.asScala.toSeq)
    }
  }

  private def flushQueue(): Unit = {
    puQueue.synchronized {
      if (puQueue.size() >= Storage.batchSqlSize) {
        val toWrite = new java.util.ArrayList[PageMarkup_U]()
        puQueue.drainTo(toWrite)
        if (!toWrite.isEmpty) {
          db.page.writeMarkups(toWrite.asScala.toSeq)
        }
      }
    }
  }

  private var db: Storage  = _
  private lazy val props   = db.configuration.readConfiguredPropertiesOptimistic()
  private lazy val puQueue = new ArrayBlockingQueue[PageMarkup_U](Storage.batchSqlSize * 2)
}
