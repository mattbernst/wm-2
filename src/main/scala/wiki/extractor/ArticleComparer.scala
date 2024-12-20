package wiki.extractor

import com.github.blemale.scaffeine.{Cache, LoadingCache, Scaffeine}
import wiki.db.Storage
import wiki.extractor.types.{Comparison, PageType, VectorPair}

import scala.collection.mutable

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

    val ilvm = makeVectors(linksAIn, linksBIn, distinctLinkInCountCache)
    val olvm = makeVectors(linksAOut, linksBOut, distinctLinkOutCountCache)

    Comparison(
      inLinkVectorMeasure = ???,
      outLinkVectorMeasure = ???,
      inLinkGoogleMeasure = ArticleComparer.googleMeasure(linksAIn, linksBIn, articleCount),
      outLinkGoogleMeasure = ArticleComparer.googleMeasure(linksAOut, linksBOut, articleCount)
    )
  }

  /**
    * Generate vectors of link weights between a pair of link sequences.
    * Only links that appear in both sequences will be processed, since
    * only common links contribute to the cosine similarity calculation later.
    *
    * The links for A and B should both come from in-links or both come from
    * out-links. Calculating the relatedness with vectors constructed from
    * both in-links and out-links provides valid but different measures of
    * relatedness between articles.
    *
    * The link weights are assigned by an analog of TF-IDF referred to as
    * "LF-IAF" in section 3.1.1 of "An open-source toolkit for mining Wikipedia"
    *
    * The Milne implementation in ArticleComparer.java had a special case not
    * mentioned in the paper: it added special high-weight links between link
    * sets if the associated pages link directly to each other. (This requires
    * passing in the page IDs for each of A and B as well as their links.)
    *
    * This implementation omits the special high-weight links for directly
    * linked pages, and it uses the "augmented frequency" approach to term
    * (link) frequency.
    *
    * @param linksA A sequence of links from article A or to article A
    * @param linksB A sequence of links from article B or to article B
    * @param cache  A cache for counts of distinct article occurrences, used
    *               for calculating the inverse term
    * @return       A pair of vectors containing weights for matched terms
    */
  private def makeVectors(linksA: Array[Int], linksB: Array[Int], cache: LoadingCache[Int, Int]): VectorPair = {
    val linkACounts = linksA.groupBy(identity).view.mapValues(_.length)
    val linkBCounts = linksB.groupBy(identity).view.mapValues(_.length)
    val commonLinks = linkACounts.keySet.intersect(linkBCounts.keySet).toArray.sorted
    if (commonLinks.nonEmpty) {
      val vectorA = Array.ofDim[Double](commonLinks.length)
      val vectorB = Array.ofDim[Double](commonLinks.length)
      // Get the count of the most heavily repeated (if any) link in each of
      // the link sequences, for constructing the augmented link frequency.
      val commonestA = linkACounts.values.max.toDouble
      val commonestB = linkBCounts.values.max.toDouble

      var j = 0
      commonLinks.foreach { link =>
        val countDistinct           = cache.get(link)
        val inverseArticleFrequency = math.log(articleCount / countDistinct.toDouble)
        val linkFrequencyA          = 0.5 + (0.5 * (linkACounts(link) / commonestA))
        val linkFrequencyB          = 0.5 + (0.5 * (linkBCounts(link) / commonestB))
        vectorA(j) = linkFrequencyA * inverseArticleFrequency
        vectorB(j) = linkFrequencyB * inverseArticleFrequency
        j += 1
      }

      VectorPair(vectorA, vectorB)
    } else {
      VectorPair(Array(), Array())
    }
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

  // We need counts of how many distinct articles link to each page to
  // calculate the inverse article frequency. Opportunistically try
  // to get the count from values already present in the inLinkCache,
  // but only cache the count (not the actual IDs) so that we don't waste
  // space caching large sequences of IDs.
  private val distinctLinkInCountCache: LoadingCache[Int, Int] =
    Scaffeine()
      .maximumSize(cacheSize)
      .build(loader = (id: Int) => {
        inLinkCache.getIfPresent(id) match {
          case Some(sources) =>
            sources.distinct.length
          case None =>
            db.link.getByDestination(id).map(_.source).distinct.length
        }
      })

  // We also need counts of how many distinct articles are linked from each
  // page to calculate the inverse article frequency. Opportunistically try
  // to get the count from values already present in the outLinkCache,
  // but only cache the count (not the actual IDs) so that we don't waste
  // space caching large sequences of IDs.
  private val distinctLinkOutCountCache: LoadingCache[Int, Int] =
    Scaffeine()
      .maximumSize(cacheSize)
      .build(loader = (id: Int) => {
        outLinkCache.getIfPresent(id) match {
          case Some(destinations) =>
            destinations.distinct.length
          case None =>
            db.link.getBySource(id).map(_.destination).distinct.length
        }
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
