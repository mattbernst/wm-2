package wiki.extractor

import com.github.blemale.scaffeine.LoadingCache
import wiki.db.{DepthSink, PageDepth, Storage}
import wiki.extractor.types.{ARTICLE, CATEGORY, PageType}
import wiki.extractor.util.Progress

import scala.collection.mutable

class DepthProcessor(
  db: Storage,
  sink: DepthSink,
  pageGroups: mutable.Map[PageType, mutable.Set[Int]],
  destinationCache: LoadingCache[Int, Array[Int]],
  depthLimit: Int) {

  /**
    * Start with one named page to mark depths of all connected pages.
    *
    * @param rootTitle The title of the page (usually, category) to start from
    */
  def markDepths(rootTitle: String): Unit = {
    db.getPage(rootTitle) match {
      case Some(page) =>
        val depth = 1
        sink.addDepth(PageDepth(page.id, depth, Seq(page.id)))
        markDepth(page.id, List(page.id), depth)
      case None =>
        val msg = s"Could not find root category title '$rootTitle'"
        throw new NoSuchElementException(msg)
    }
  }

  /**
    * Mark depth of the given page ID and its unseen children up to maximum depth.
    *
    * @param pageId Current page ID
    * @param route Sequence of pages connecting current page ID to the root page
    * @param depth Current depth
    */
  private def markDepth(pageId: Int, route: List[Int], depth: Int): Unit = {
    if (!completedPages.contains(pageId)) {
      val nextDestinations = mutable.Set[Int]()
      sink.addDepth(PageDepth(pageId, depth, route))
      completedPages.add(pageId)
      Progress.tick(completedPages.size, "+")
      val links = destinationCache.get(pageId).filter(dst => !completedPages.contains(dst))
      links.foreach { link =>
        nextDestinations.add(link)
      }

      if (depth < depthLimit) {
        // Traverse deeper by way of non-redirecting children
        val childCategories = nextDestinations.intersect(pageGroups(CATEGORY)).toSeq.sorted
        val childArticles   = nextDestinations.intersect(pageGroups(ARTICLE)).toSeq.sorted
        childArticles.foreach(id => markDepth(id, id :: route, depth + 1))
        childCategories.foreach(id => markDepth(id, id :: route, depth + 1))
      }
    }
  }

  private val completedPages = mutable.Set[Int]()
}
