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
    */
  def enqueueMarkup(): Unit = {
    val relevantPages: Set[PageType] = Set(ARTICLE, CATEGORY, DISAMBIGUATION)
    val max                          = Math.max(db.page.compressedMax, db.page.uncompressedMax)
    val fetch: (Int, Int) => Seq[TypedPageMarkup] = if (db.page.usingCompression) {
      db.page.readMarkupSlice_Z
    } else {
      db.page.readMarkupSlice
    }
    var j = 0
    // N.B. if sliceSize is too small, this could accidentally terminate early
    // (if all IDs in range were irrelevant page types)
    val sliceSize = 20_000
    while (j < max) {
      val entries = fetch(j, j + sliceSize)
      entries
        .filter(e => relevantPages.contains(e.pageType))
        .foreach(e => queue.put(e))
      j += sliceSize
    }
  }

  def getFromQueue(): Option[TypedPageMarkup] =
    Option(queue.poll(3, TimeUnit.SECONDS))

  private lazy val queue = new ArrayBlockingQueue[TypedPageMarkup](queueSize)
}
