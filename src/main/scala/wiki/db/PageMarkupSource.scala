package wiki.db

import wiki.extractor.types.*

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}

class PageMarkupSource(db: Storage, queueSize: Int = 40_000) {

  /**
    * Continually enqueue PageMarkup into the internal queue until all relevant
    * page_markup or page_markup_z entries have been read from the database.
    * Automatically chooses compressed or uncompressed table depending on which
    * has data.
    *
    * @param relevantPages Page types to include in the queue
    *
    */
  def enqueueMarkup(relevantPages: Set[PageType]): Unit = {
    val max = Math.max(db.page.compressedMax, db.page.uncompressedMax)
    val fetch: (Int, Int) => Seq[TypedPageMarkup] = if (db.page.usingCompression) {
      db.page.readMarkupSlice_Z
    } else {
      db.page.readMarkupSlice
    }
    var j = 0
    // N.B. if sliceSize is too small, this could accidentally terminate early
    // (if all IDs in range were irrelevant page types)
    val sliceSize = 20_000
    while (j < max && j < 40000) {
      val entries = fetch(j, j + sliceSize)
      entries
        .filter(e => relevantPages.contains(e.pageType))
        .foreach(e => queue.put(e))
      j += sliceSize
      // Shorten poll time after first entries have been added to queue.
      // Without this, the source can time out getting the first slice.
      pollTime = 3L
    }
  }

  def getFromQueue(): Option[TypedPageMarkup] =
    Option(queue.poll(pollTime, TimeUnit.SECONDS))

  private var pollTime   = 30L
  private lazy val queue = new ArrayBlockingQueue[TypedPageMarkup](queueSize)
}
