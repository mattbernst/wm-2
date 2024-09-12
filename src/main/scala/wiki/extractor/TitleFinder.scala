package wiki.extractor

import wiki.db.Redirect
import wiki.extractor.util.Logging

import scala.collection.mutable
import scala.util.Try

/**
 * When processing a Wikipedia dump for the first time, the TitleFinder needs
 * both the pageMap and the redirects to be read in from the database to
 * perform redirect resolution. After the resolved title to page mapping has
 * been stored in the database, this can be initialized with just the contents
 * of the title_to_page table as the pageMap parameter (redirects can be
 * left empty).
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
   * title and that new title will subsequently be looked up from the title
   * map.
   *
   * Example: "ASCII art" will be found directly in the title map as page 1884
   * But "AsciiArt" is not there directly. It is in the redirect map, which
   * points to "ASCII art", which then points to page 1884. Both "AsciiArt"
   * and "ASCII art" will return page 1884, but "AsciiArt" needs an additional
   * lookup first.
   *
   * @param title The title of a page, ordinary or redirected
   * @return      The direct ID of an ordinary page, or the ID of the page's
   *              redirect target for a redirect page
   */
  def titleToId(title: String): Int = {
    pageMap.getOrElse(title, pageMap(redirectMap(title)._1))
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
    val badRedirectTitles = danglingRedirects.map(_.title).toSet

    val fromRedirects = redirectMap
      .keysIterator
      .filterNot(title => badRedirectTitles.contains(title))
      .toSeq
      .map(title => (title, titleToId(title)))

    val fromTitles = pageMap.toSeq

    fromRedirects ++ fromTitles
  }

  /**
   * Get "redirects to nowhere" that should be omitted from the construction of
   * the flattened page mapping. These dangling redirects ge marked with a
   * special page type DANGLING_REDIRECT in the page table.
   */
  lazy val danglingRedirects: Seq[Redirect] = {
    redirectMap
      .keysIterator
      .filter(title => Try(titleToId(title)).isFailure)
      .toSeq
      .map(title => Redirect(pageId = redirectMap(title)._2, title = title, redirectTarget = redirectMap(title)._1))
  }

  private val redirectMap = {
    // Map titles to (redirect, page_id) tuples
    val mm = mutable.Map[String, (String, Int)]()
    redirects.foreach(r => mm.put(r.title, (r.redirectTarget, r.pageId)))
    mm
  }
}
