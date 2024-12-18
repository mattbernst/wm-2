package wiki.extractor

import com.github.blemale.scaffeine.{Cache, LoadingCache, Scaffeine}
import wiki.db.Storage
import wiki.extractor.types.{Comparison, PageType}

class ArticleComparer(db: Storage, cacheSize: Int = 1_000_000) {

  /**
    * Compare article A with article B.
    *
    * @param a Page ID for article A
    * @param b Page ID for article B
    * @return A Comparison measure if pages were comparable, or None if they
    *         represented the same page
    */
  def compare(a: Int, b: Int): Option[Comparison] = {
    // Comparison is symmetric, so results for (A,B) also match (B,A)
    val key = List(a, b).sorted

    comparisonCache.get(key, _ => {
      if (a == b) {
        None
      } else {
        Some(makeComparison(a, b))
      }
    })
  }

  private def makeComparison(a: Int, b: Int): Comparison = {
    val linksAIn  = inLinkCache.get(a)
    val linksBIn  = inLinkCache.get(b)
    val linksAOut = outLinkCache.get(a)
    val linksBOut = outLinkCache.get(b)

    Comparison(
      inLinkVectorMeasure = ???,
      outLinkVectorMeasure = ???,
      inLinkGoogleMeasure = ArticleComparer.googleMeasure(linksAIn, linksBIn, articleCount),
      outLinkGoogleMeasure = ArticleComparer.googleMeasure(linksAOut, linksBOut, articleCount)
    )
  }

  private val inLinkCache: LoadingCache[Int, Array[Int]] =
    Scaffeine()
      .maximumSize(cacheSize)
      .build(loader = (id: Int) => {
        db.link.getByDestination(id).map(_.source).toArray
      })

  private val outLinkCache: LoadingCache[Int, Array[Int]] =
    Scaffeine()
      .maximumSize(cacheSize)
      .build(loader = (id: Int) => {
        db.link.getBySource(id).map(_.destination).toArray
      })

  private val comparisonCache: Cache[List[Int], Option[Comparison]] =
    Scaffeine()
      .maximumSize(cacheSize)
      .build()

  private lazy val articleCount: Int =
    db.page.countPagesByTypes(Seq(PageType.ARTICLE))
}

object ArticleComparer {

  /**
    * Calculate a measure of similarity between two different link collections,
    * inspired by "The Google similarity distance"
    * https://doi.org/10.1109/TKDE.2007.48
    *
    * See the discussion of "relatedness" in section 3.1 of
    * "Learning to Link with Wikipedia"
    * https://doi.org/10.1145/1458082.1458150
    *
    * See also "normalized link distance" in section 3.1.1 of
    * "An open-source toolkit for mining Wikipedia"
    * https://doi.org/10.1016/j.artint.2012.06.007
    *
    * Also see ArticleComparison.java in the original Milne implementation.
    *
    * Note that for a realistic (large) articleCount, this measure is 0 when
    * the links are completely non-overlapping but abruptly reaches > 0.5 when
    * links have even one match. That is because the original Google definition
    * was tuned for a different use case. TODO?: normalize this measure to
    * scale it to the actual range (e.g. set a lower range equal to 1 link
    * matched out of 1000). Otherwise, this dominates cosine similarity when
    * all the measures are averaged.
    *
    * @param linksA       Links into or out of article A
    * @param linksB       Links into or out of article B
    * @param articleCount A count of the total number of Wikipedia articles
    * @return             A number between 0 and 1, with a high number
    *                     representing "more similar"
    */
  def googleMeasure(linksA: Array[Int], linksB: Array[Int], articleCount: Int): Double = {
    require(articleCount > math.max(linksA.length, linksB.length), "Article count must be > link count")
    val intersections = countIntersection(linksA, linksB)

    val normalizedGoogleDistance = if (intersections == 0) {
      1.0
    } else {
      val a  = math.log(linksA.length)
      val b  = math.log(linksB.length)
      val ab = math.log(intersections)
      val m  = math.log(articleCount)

      (math.max(a, b) - ab) / (m - math.min(a, b))
    }

    // The original Normalized Google Distance definition used 0 for perfect
    // similarity and 1.0 for no similarity, but we invert it here so it
    // composes more naturally with the cosine similarity vector measure.
    if (normalizedGoogleDistance >= 1.0) {
      0.0
    } else {
      1.0 - normalizedGoogleDistance
    }
  }

  def intersectionProportion(linksA: Array[Int], linksB: Array[Int]): Double = {
    val u = countUnion(linksA, linksB)
    if (u == 0) {
      0.0
    } else {
      countIntersection(linksA, linksB) / u.toDouble
    }
  }

  private def countIntersection(linksA: Array[Int], linksB: Array[Int]): Int =
    linksA.toSet.intersect(linksB.toSet).size

  private def countUnion(linksA: Array[Int], linksB: Array[Int]): Int =
    linksA.toSet.union(linksB.toSet).size
}
