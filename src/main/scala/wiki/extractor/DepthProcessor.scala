package wiki.extractor

import wiki.db.Storage
import wiki.extractor.util.Progress

import scala.collection.mutable

class DepthProcessor(db: Storage) {

  def markDepths(rootTitle: String): Unit = {
    db.getPage(rootTitle) match {
      case Some(page) =>
        val depth = 0
        markedPages.add(page.id)
        db.page.writeDepths(Map(page.id -> depth))
        markDepth(Set(page.id), depth)
      case None =>
        val msg = s"Could not find root category title '$rootTitle'"
        throw new NoSuchElementException(msg)
    }
  }

  private def markDepth(pageIds: Set[Int], depth: Int): Unit = {
    pageIds.foreach { id =>
      val links  = db.link.getBySource(id).filter(link => !markedPages.contains(link.destination))
      val depths = links.map(link => (link.destination, depth)).toMap
      if (depths.nonEmpty) {
        db.page.writeDepths(depths)
      }
      links.foreach { link =>
        markedCount += 1
        Progress.tick(markedCount, "+")
        markedPages.add(link.destination)
      }
      markDepth(depths.keySet, depth + 1)
    }
  }

  private var markedCount = 0
  private val markedPages = mutable.Set[Int]()
}
