package wiki.extractor

import com.github.blemale.scaffeine.LoadingCache
import wiki.db.Storage
import wiki.extractor.types.{ARTICLE, CATEGORY, PageType}
import wiki.extractor.util.Progress

import scala.collection.mutable

class DepthProcessor(
  db: Storage,
  pageGroups: Map[PageType, Set[Int]],
  destinationCache: LoadingCache[Int, Seq[Int]],
  maximumDepth: Int) {

  /**
    * Start with one named page to mark depths of all connected pages.
    *
    * @param rootTitle The title of the page (usually, category) to start from
    */
  def markDepths(rootTitle: String): Unit = {
    db.getPage(rootTitle) match {
      case Some(page) =>
        val depth = 0
        db.page.writeDepths(Map(page.id -> depth))
        markDepth(Seq(page.id), depth)
      case None =>
        val msg = s"Could not find root category title '$rootTitle'"
        throw new NoSuchElementException(msg)
    }
  }

  def getCompletedCount(): Int =
    completedPages.size

  /**
    * Mark depth of the given page IDs and their children up to maximum depth.
    *
    * @param pageIds Current page IDs
    * @param depth Current depth
    */
  private def markDepth(pageIds: Seq[Int], depth: Int): Unit = {
    val nextDestinations = mutable.Set[Int]()

    pageIds
      .filterNot(p => completedPages.contains(p))
      .foreach { id =>
        completedPages.add(id)
        val links = destinationCache.get(id).filter(dst => !completedPages.contains(dst))
        links.foreach { link =>
          markedCount += 1
          Progress.tick(markedCount, "+")
          nextDestinations.add(link)
        }

        val depths = links.map(link => (link, depth)).toMap
        db.page.writeDepths(depths)
      }

    if (depth < maximumDepth) {
      // Traverse deeper by way of non-redirecting children
      val childCategories = nextDestinations.intersect(pageGroups(CATEGORY)).toSeq.sorted
      val childArticles   = nextDestinations.intersect(pageGroups(ARTICLE)).toSeq.sorted

      if (childCategories.nonEmpty) {
        markDepth(childCategories, depth + 1)
      }

      if (childArticles.nonEmpty) {
        markDepth(childArticles, depth + 1)
      }
    }
  }

  private var markedCount    = 0
  private val completedPages = mutable.Set[Int]()
}
