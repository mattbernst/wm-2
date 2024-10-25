package wiki.extractor

import wiki.db.Storage
import wiki.extractor.types.{ARTICLE, CATEGORY}
import wiki.extractor.util.Progress

import scala.collection.mutable

class DepthProcessor(db: Storage) {

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

  /**
    * Mark depth of the given page IDs and their children up to maximum depth.
    *
    * @param pageIds Current page IDs
    * @param depth Current depth
    * @param maximumDepth Maximum depth to explore
    */
  private def markDepth(pageIds: Seq[Int], depth: Int, maximumDepth: Int = 64): Unit = {
    val nextDestinations = mutable.Set[Int]()

    pageIds
      .filterNot(p => completedPages.contains(p))
      .foreach { id =>
        completedPages.add(id)
        val links = db.link.getBySource(id).filter(link => !completedPages.contains(link.destination))
        links.foreach { link =>
          markedCount += 1
          Progress.tick(markedCount, "+")
          if (!completedPages.contains(link.destination)) {
            nextDestinations.add(link.destination)
          }
        }

        val depths = links.map(link => (link.destination, depth)).toMap
        db.page.writeDepths(depths)
      }

    if (depth < maximumDepth) {
      val childPages = {
        // Sort pages so that those with more incomplete destination links get
        // processed first. When pages have the same number of links, the page
        // ID is the tie-breaker.
        val pages = db.getPages(nextDestinations.toSeq).filter(_.redirectTarget.isEmpty)
        val remainingDestinationsByPage = db.link
          .getBySource(pages.map(_.id))
          .map(t => t.copy(_2 = t._2.count(link => !completedPages.contains(link.destination))))

        val decorated = pages
          .map(p => ((remainingDestinationsByPage.getOrElse(p.id, 0), -p.id), p))
          .sortBy(_._1)
          .reverse

        decorated.map(_._2)
      }

      // Traverse deeper by way of non-redirecting children
      val childArticles   = childPages.filter(_.pageType == ARTICLE)
      val childCategories = childPages.filter(_.pageType == CATEGORY)

      if (childCategories.nonEmpty) {
        println(s"Depth: $depth")
        println(s"childCategories: ${childCategories.map(_.title)}")
        markDepth(childCategories.map(_.id), depth + 1, maximumDepth = maximumDepth)
      }

      if (childArticles.nonEmpty) {
        println(s"Depth: $depth")
        println(s"childArticles: ${childArticles.map(_.title)}")
        markDepth(childArticles.map(_.id), depth + 1, maximumDepth = maximumDepth)
      }
    }
  }

  private var markedCount    = 0
  private val completedPages = mutable.Set[Int]()
}
