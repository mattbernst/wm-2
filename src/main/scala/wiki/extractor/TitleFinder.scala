package wiki.extractor

import wiki.db.Redirect
import wiki.extractor.util.Logging

import scala.collection.mutable

/**
 * When processing a Wikipedia dump for the first time, the TitleFinder needs
 * both the pageMap and the redirects to be read in from the database to
 * perform redirect resolution. After the resolved title to page mapping has
 * been stored in the database, a faster title resolver based on hashed titles
 * can be used.
 *
 * @param pageMap     A map of page titles to page IDs
 * @param redirects   All redirects from the page table
 */
class TitleFinder(pageMap: mutable.Map[String, Int],
                  redirects: Seq[Redirect]) extends Logging {
  /**
   * Resolve a page title to its page ID. If the title belongs to an ordinary
   * page, get it directly from the title map. Otherwise, the title is a
   * redirect. The redirect-title will get resolved to its redirect target
   * title and that new title will be looked up recursively.
   *
   * Example: "ASCII art" will be found directly in the title map as page 1884
   * But "AsciiArt" is not there directly. It is in the redirect map, which
   * points to "ASCII art", which then points to page 1884. Both "AsciiArt"
   * and "ASCII art" will return page 1884, but "AsciiArt" needs an additional
   * lookup first.
   *
   * @param title The title of a page, ordinary or redirected
   * @param depth The current search depth (increments for each redirect followed)
   * @return      The direct ID of an ordinary page, or the ID of the page's
   *              redirect target for a redirect page
   */
  def getId(title: String, depth: Int = 1): Option[Int] = {
    if (depth > maxDepth) {
      logger.warn(s"Exceeded max depth trying to resolve title '$title' to a non-redirect page")
      None
    }
    else if (pageMap.contains(title)) {
      pageMap.get(title)
    }
    else if (redirectMap.contains(title)) {
      getId(redirectMap(title)._1, depth + 1)
    }
    // Dangling redirect does not point to an ordinary page or to
    // another redirect
    else {
      None
    }
  }

  /**
   * Get all page titles, redirects or otherwise, mapped directly to their
   * destination page ID. This would be simple except that Wikipedia dumps
   * are not fully consistent. For example, in a dump from early September 2024,
   * there was a recently added redirect from "Roger Broughton (cricketer)" to
   * "Roger Broughton". The page titled "Roger Broughton" did not yet exist in
   * that dump.
   *
   *  sqlite> select * from page where title in ('Roger Broughton (cricketer)', 'Roger Broughton');
   *  id        namespace_id  page_type  last_edited    title                        redirect_target
   *  --------  ------------  ---------  -------------  ---------------------------  ---------------
   *  10065903  0             1          1680320772000  Roger Broughton (cricketer)
   *  77774224  0             3          1725288686000  Roger Broughton (cricketer)  Roger Broughton
   *
   * To deal with this edge case, omit "redirects to nowhere."
   *
   * @return A map of page titles to their resolved page IDs
   */
  def getFlattenedPageMapping(): Seq[(String, Int)] = {
    val fromRedirects = redirectMap
      .keysIterator
      .filterNot(title => getId(title).isEmpty)
      .toSeq
      .map(title => (title, getId(title).get))

    val fromTitles = pageMap.toSeq
    (fromRedirects ++ fromTitles).distinct
  }

  /**
   * Get "redirects to nowhere" that should be omitted from the construction of
   * the flattened page mapping. These dangling redirects get marked with a
   * special page type DANGLING_REDIRECT in the page table.
   */
  lazy val danglingRedirects: Seq[Redirect] = {
    redirectMap
      .keysIterator
      .filter(title => getId(title).isEmpty)
      .toSeq
      .map(title => Redirect(pageId = redirectMap(title)._2, title = title, redirectTarget = redirectMap(title)._1))
  }

  private val redirectMap = {
    // Map redirect titles to (redirect_target, page_id) tuples
    val mm = mutable.Map[String, (String, Int)]()
    redirects.foreach(r => mm.put(r.title, (r.redirectTarget, r.pageId)))
    mm
  }

  // The maximum number of redirects to follow. In practice only self-redirects
  // appear to have a depth greater than 4.
  private val maxDepth = 16
}
