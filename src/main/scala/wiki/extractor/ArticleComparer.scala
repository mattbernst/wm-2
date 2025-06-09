package wiki.extractor

import com.github.blemale.scaffeine.{Cache, LoadingCache, Scaffeine}
import wiki.db.Storage
import wiki.extractor.types.{Comparison, Context, PageType, VectorPair}

import scala.collection.mutable
import scala.util.hashing.MurmurHash3
case class CountsWithMax(m: mutable.Map[Int, Int], max: Double)
case class LinkVector(links: Array[Int], key: Long, distinctLinks: Array[Int])

object LinkVector {

  def apply(links: Array[Int]): LinkVector =
    LinkVector(
      links = links,
      key = ArticleComparer.hashLinks(links),
      distinctLinks = links.distinct.sorted
    )
}

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
    val key = if (a > b) {
      (a.toLong << 32) | (b.toLong & 0xFFFFFFFFL)
    } else {
      (b.toLong << 32) | (a.toLong & 0xFFFFFFFFL)
    }

    comparisonCache.get(key, _ => {
      if (a == b) {
        None
      } else {
        Some(makeComparison(a, b))
      }
    })
  }

  /**
    * Get relatedness measure between a given Wikipedia page ID and a Context
    * of representative pages for a document. For example, a page about the
    * planet Mercury should be more related to a Context derived from an
    * document about NASA than it is related to a Context derived from an
    * article about pollutants in coal.
    *
    * @param pageId  The numeric ID of a Wikipedia page
    * @param context A Context containing top candidate Wikipedia pages for
    *                representing a document
    * @return        A measure that is higher for pages that are more similar
    *                to the given Context
    */
  def getRelatednessTo(pageId: Int, context: Context): Double = {
    if (context.quality == 0.0 || context.pages.isEmpty) {
      0.0
    } else {
      var relatedness = 0.0
      val pageIds     = (Array(pageId) ++ context.pages.map(_.pageId)).distinct
      primeCaches(pageIds)
      context.pages.foreach { page =>
        compare(page.pageId, pageId).foreach { comparison =>
          val r = comparison.mean * page.weight
          relatedness += r
        }
      }

      relatedness / context.quality
    }
  }

  /**
    * Populate in/out link caches with the data needed for comparisons. This is
    * more efficient than loading everything on a cache miss because we can
    * do bulk database lookups for the missing elements.
    *
    * @param pageIds All the numeric IDs of Wikipedia pages to look up
    */
  def primeCaches(pageIds: Array[Int]): Unit = {
    val missingIn = pageIds
      .filter(p => inLinkCache.getIfPresent(p).isEmpty)
      .distinct
      .toSeq
    if (missingIn.nonEmpty) {
      db.link
        .getSourcesByDestination(missingIn)
        .foreach(t => inLinkCache.put(t._1, LinkVector(t._2)))
    }
    val missingOut = pageIds
      .filter(p => outLinkCache.getIfPresent(p).isEmpty)
      .distinct
      .toSeq
    if (missingOut.nonEmpty) {
      db.link
        .getDestinationsBySource(missingOut)
        .foreach(t => outLinkCache.put(t._1, LinkVector(t._2)))
    }
  }

  /**
    * Calculates similarity metrics between two Wikipedia articles using their link structures.
    *
    * For a pair of article IDs, computes four different similarity measures:
    * - Vector similarity (cosine) using weighted in-links
    * - Vector similarity (cosine) using weighted out-links
    * - Normalized Google distance using in-links
    * - Normalized Google distance using out-links
    *
    * The vector similarities are calculated using LF-IAF (Link Frequency-Inverse Article Frequency)
    * weights. The Google distance measures are normalized to distribute results more evenly
    * across the [0,1] range using a pre-computed lower limit.
    *
    * @param a Page ID of the first Wikipedia article
    * @param b Page ID of the second Wikipedia article
    * @return A Comparison containing all four similarity measures
    */
  private def makeComparison(a: Int, b: Int): Comparison = {
    val linksAIn  = inLinkCache.get(a)
    val linksBIn  = inLinkCache.get(b)
    val linksAOut = outLinkCache.get(a)
    val linksBOut = outLinkCache.get(b)

    val ilvm = makeVectors(linksAIn, linksBIn, distinctLinkInCountCache)
    val olvm = makeVectors(linksAOut, linksBOut, distinctLinkOutCountCache)

    // The Google measure needs to be normalized using googleMeasureLowerLimit;
    // otherwise, all results are crammed into the upper portion of the 0-1
    // range.
    val inGoogleMeasure = {
      val original = ArticleComparer.googleMeasure(linksAIn.distinctLinks, linksBIn.distinctLinks, articleCount)
      (original - googleMeasureLowerLimit) / (1.0 - googleMeasureLowerLimit)
    }

    val outGoogleMeasure = {
      val original = ArticleComparer.googleMeasure(linksAOut.distinctLinks, linksBOut.distinctLinks, articleCount)
      (original - googleMeasureLowerLimit) / (1.0 - googleMeasureLowerLimit)
    }

    Comparison(
      inLinkVectorMeasure = ArticleComparer.cosineSimilarity(ilvm.vectorA, ilvm.vectorB),
      outLinkVectorMeasure = ArticleComparer.cosineSimilarity(olvm.vectorA, olvm.vectorB),
      inLinkGoogleMeasure = inGoogleMeasure,
      outLinkGoogleMeasure = outGoogleMeasure
    )
  }

  /**
    * Generate vectors of link weights between a pair of link sequences
    * originating from a pair of Wikipedia pages. Only links that appear in
    * both sequences will be processed, since only common links contribute
    * to the cosine similarity calculation later.
    *
    * The links for A and B should both come from in-links or both come from
    * out-links. Calculating the relatedness with vectors constructed from
    * both in-links and out-links provides valid but different measures of
    * relatedness between articles.
    *
    * The link weights are assigned by an analog of TF-IDF referred to as
    * LF-IAF in section 3.1.1 of "An open-source toolkit for mining Wikipedia"
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
    * @param a      A sequence of links from article A or to article A
    * @param b      A sequence of links from article B or to article B
    * @param cache  A cache for counts of distinct article occurrences, used
    *               for calculating the inverse term frequency
    * @return       A pair of vectors containing weights for matched terms
    */
  private def makeVectors(a: LinkVector, b: LinkVector, cache: LoadingCache[Int, Int]): VectorPair = {
    if (a.links.nonEmpty && b.links.nonEmpty) {
      val linkACounts = getCounts(a)
      val linkBCounts = getCounts(b)

      // Find common links
      val unFilteredCommonLinks = Array.ofDim[Int](math.min(linkACounts.m.size, linkBCounts.m.size))
      var k                     = 0
      linkACounts.m.keys.foreach { link =>
        if (linkBCounts.m.contains(link)) {
          unFilteredCommonLinks(k) = link
          k += 1
        }
      }

      // Prime cache, retain only in-cache common links
      val subArray = unFilteredCommonLinks.take(k)
      cache.getAll(subArray)
      val commonLinks = subArray.filter(link => cache.get(link) > 0)
      k = commonLinks.length

      if (k > 0) {
        val vectorA = Array.ofDim[Double](k)
        val vectorB = Array.ofDim[Double](k)

        var j = 0
        while (j < k) {
          val link                    = commonLinks(j)
          val countDistinct           = cache.get(link)
          val inverseArticleFrequency = math.log(articleCount / countDistinct.toDouble)
          val linkFrequencyA          = 0.5 + (0.5 * (linkACounts.m(link) / linkACounts.max))
          val linkFrequencyB          = 0.5 + (0.5 * (linkBCounts.m(link) / linkBCounts.max))
          vectorA(j) = linkFrequencyA * inverseArticleFrequency
          vectorB(j) = linkFrequencyB * inverseArticleFrequency
          j += 1
        }

        VectorPair(vectorA, vectorB)
      } else {
        VectorPair(Array(), Array())
      }
    } else {
      VectorPair(Array(), Array())
    }
  }

  private def getCounts(v: LinkVector): CountsWithMax = {
    linkCountsCache.get(
      v.key,
      (_: Long) => {
        val linkCounts = mutable.Map[Int, Int]().withDefaultValue(0)
        var maxCount   = 0
        v.links.foreach { link =>
          val newCount = linkCounts(link) + 1
          linkCounts(link) = newCount
          if (newCount > maxCount) {
            maxCount = newCount
          }
        }
        CountsWithMax(linkCounts, maxCount.toDouble)
      }
    )
  }

  private val linkCountsCache: Cache[Long, CountsWithMax] =
    Scaffeine()
      .maximumSize(cacheSize)
      .build()

  private val inLinkCache: LoadingCache[Int, LinkVector] =
    Scaffeine()
      .maximumSize(cacheSize)
      .build(loader = (id: Int) => {
        val ids = db.link.getByDestination(id).map(_.source).toArray.sorted
        LinkVector(ids)
      })

  private val outLinkCache: LoadingCache[Int, LinkVector] =
    Scaffeine()
      .maximumSize(cacheSize)
      .build(loader = (id: Int) => {
        val ids = db.link.getBySource(id).map(_.destination).toArray.sorted
        LinkVector(ids)
      })

  // We need counts of how many distinct articles link to each page to
  // calculate the inverse article frequency. Opportunistically try
  // to get the count from values already present in the inLinkCache,
  // but only cache the count (not the actual IDs) so that we don't waste
  // space caching large sequences of IDs.
  private val distinctLinkInCountCache: LoadingCache[Int, Int] =
    Scaffeine()
      .maximumSize(cacheSize * 2)
      .build(
        loader = (id: Int) => {
          inLinkCache.getIfPresent(id) match {
            case Some(sources) =>
              sources.links.distinct.length
            case None =>
              db.link.getByDestination(id).map(_.source).distinct.length
          }
        },
        allLoader = Some((ids: Iterable[Int]) => {
          val idsSet = ids.toSet

          // Get cached values first
          val cachedResults = idsSet.flatMap { id =>
            inLinkCache.getIfPresent(id).map(id -> _.links.distinct.length)
          }.toMap

          // Identify missing IDs that need to be loaded from DB
          val missingIds = idsSet -- cachedResults.keySet

          if (missingIds.nonEmpty) {
            // Bulk load missing data from database
            val dbResults = db.link.getSourcesByDestination(missingIds.toSeq).map {
              case (id, links) =>
                id -> links.distinct.length
            }

            // Combine cached and DB results
            cachedResults ++ dbResults
          } else {
            cachedResults
          }
        })
      )

  // We also need counts of how many distinct articles are linked from each
  // page to calculate the inverse article frequency. Opportunistically try
  // to get the count from values already present in the outLinkCache,
  // but only cache the count (not the actual IDs) so that we don't waste
  // space caching large sequences of IDs.
  private val distinctLinkOutCountCache: LoadingCache[Int, Int] =
    Scaffeine()
      .maximumSize(cacheSize)
      .build(
        loader = (id: Int) => {
          outLinkCache.getIfPresent(id) match {
            case Some(destinations) =>
              destinations.links.distinct.length
            case None =>
              db.link.getBySource(id).map(_.destination).distinct.length
          }
        },
        allLoader = Some((ids: Iterable[Int]) => {
          val idsSet = ids.toSet

          // Get cached values first
          val cachedResults = idsSet.flatMap { id =>
            outLinkCache.getIfPresent(id).map(id -> _.links.distinct.length)
          }.toMap

          // Identify missing IDs that need to be loaded from DB
          val missingIds = idsSet -- cachedResults.keySet

          if (missingIds.nonEmpty) {
            // Bulk load missing data from database
            val dbResults = db.link.getDestinationsBySource(missingIds.toSeq).map {
              case (id, links) =>
                id -> links.distinct.length
            }

            // Combine cached and DB results
            cachedResults ++ dbResults
          } else {
            cachedResults
          }
        })
      )

  private val comparisonCache: Cache[Long, Option[Comparison]] =
    Scaffeine()
      .maximumSize(cacheSize)
      .build()

  private lazy val articleCount: Int =
    db.page.countPagesByTypes(Seq(PageType.ARTICLE))

  // This extreme-case value is used to scale googleMeasure results, which tend
  // to go well above 0.5 when even a single link is found in common. This is
  // used to scale results to fill the full 0-1 range.
  private lazy val googleMeasureLowerLimit = {
    val nTerms = math.min(articleCount - 1, 100_000)
    val links  = 0.until(nTerms).toArray
    ArticleComparer.googleMeasure(links, links.take(1), articleCount)
  }
}

object ArticleComparer {

  /**
    * Calculate a measure of similarity between two different link collections,
    * inspired by "The Google similarity distance"
    * https://doi.org/10.1109/TKDE.2007.48 https://arxiv.org/pdf/cs/0412098
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
    * was tuned for a different use case.
    *
    * TODO: consider smoothing for the abrupt transition
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

  /**
    * Calculate cosine similarity between the input vectors.
    *
    * @param vectorA The first vector
    * @param vectorB The second vector
    * @return        A cosine similarity measure, ranging from -1 to 1
    */
  def cosineSimilarity(vectorA: Array[Double], vectorB: Array[Double]): Double = {
    require(vectorA.length == vectorB.length)

    var dotProduct        = 0.0
    var magnitudeASquared = 0.0
    var magnitudeBSquared = 0.0
    var j                 = 0

    while (j < vectorA.length) {
      val a = vectorA(j)
      val b = vectorB(j)

      dotProduct += a * b
      magnitudeASquared += a * a
      magnitudeBSquared += b * b

      j += 1
    }

    if (magnitudeASquared == 0.0 || magnitudeBSquared == 0.0) {
      0.0
    } else {
      dotProduct / math.sqrt(magnitudeASquared * magnitudeBSquared)
    }
  }

  def countIntersection(linksA: Array[Int], linksB: Array[Int]): Int = {
    var i     = 0
    var j     = 0
    var count = 0

    while (i < linksA.length && j < linksB.length) {
      if (linksA(i) == linksB(j)) {
        count += 1
        i += 1
        j += 1
      } else if (linksA(i) < linksB(j)) {
        i += 1
      } else {
        j += 1
      }
    }

    count
  }

  private[extractor] def hashLinks(input: Array[Int]): Long =
    MurmurHash3
      .arrayHash(input)
      .toLong
}
