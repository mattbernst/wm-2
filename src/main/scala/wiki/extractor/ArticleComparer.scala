package wiki.extractor

import com.github.blemale.scaffeine.{Cache, LoadingCache, Scaffeine}
import wiki.db.{PageCount, Storage}
import wiki.extractor.types.{Comparison, Context, PageType, VectorPair}
import wiki.util.Logging

import scala.collection.mutable
import scala.util.hashing.MurmurHash3
case class CountsWithMax(m: mutable.Map[Int, Int], max: Double)
case class LinkVector(links: Array[Int], key: Long, distinctLinks: Array[Int])

object LinkVector {

  def apply(links: Array[Int]): LinkVector = {
    links.sortInPlace()
    LinkVector(
      links = links,
      key = ArticleComparer.hashLinks(links),
      distinctLinks = links.distinct
    )
  }
}

class ArticleComparer(db: Storage, cacheSize: Int = 500_000) extends Logging {

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
    * Get relatedness features between a given Wikipedia page ID and a Context
    * of representative pages for a document. For example, a page about the
    * planet Mercury should be more related to a Context derived from an
    * document about NASA than it is related to a Context derived from an
    * article about pollutants in coal.
    *
    * @param pageId  The numeric ID of a Wikipedia page
    * @param context A Context containing top candidate Wikipedia pages for
    *                representing a document
    * @return        A vector of features that are higher for pages that are
    *                more similar to the given Context
    */
  def getRelatednessByFeature(pageId: Int, context: Context): mutable.Map[String, Double] = {
    val result: mutable.Map[String, Double] = mutable.Map(
      "inLinkVectorMeasure"  -> 0.0,
      "outLinkVectorMeasure" -> 0.0,
      "inLinkGoogleMeasure"  -> 0.0,
      "outLinkGoogleMeasure" -> 0.0
    )

    if (context.quality > 0.0 && context.pages.nonEmpty) {
      val pageIds = (Array(pageId) ++ context.pages.map(_.pageId)).distinct
      primeCaches(pageIds)

      context.pages.foreach { page =>
        compare(page.pageId, pageId).foreach { comparison =>
          result("inLinkVectorMeasure") += comparison.inLinkVectorMeasure
          result("outLinkVectorMeasure") += comparison.outLinkVectorMeasure
          result("inLinkGoogleMeasure") += comparison.inLinkGoogleMeasure
          result("outLinkGoogleMeasure") += comparison.outLinkGoogleMeasure
        }
      }

      result("inLinkVectorMeasure") /= context.pages.length
      result("outLinkVectorMeasure") /= context.pages.length
      result("inLinkGoogleMeasure") /= context.pages.length
      result("outLinkGoogleMeasure") /= context.pages.length
    }

    result
  }

  /**
    * Populate in/out link caches with the data needed for comparisons. This is
    * more efficient than loading everything on a cache miss because we can
    * do bulk database lookups for the missing elements.
    *
    * @param pageIds All the numeric IDs of Wikipedia pages to look up
    */
  def primeCaches(pageIds: Array[Int]): Unit = {
    inLinkCache.getAll(pageIds): Unit
    outLinkCache.getAll(pageIds): Unit
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

    val ilvm = makeVectors(linksAIn, linksBIn, distinctLinksIn)
    val olvm = makeVectors(linksAOut, linksBOut, distinctLinksOut)

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
    * @param a          A sequence of links from article A or to article A
    * @param b          A sequence of links from article B or to article B
    * @param pageCount  A counter for distinct article occurrences, used for
    *                   calculating the inverse article frequency
    * @return           A pair of vectors containing weights for matched terms
    */
  private def makeVectors(a: LinkVector, b: LinkVector, pageCount: PageCount): VectorPair = {
    if (a.links.nonEmpty && b.links.nonEmpty) {
      val commonLinks = getCommonLinks(pageCount, a.distinctLinks, b.distinctLinks)

      if (commonLinks.nonEmpty) {
        val vectorA     = Array.ofDim[Double](commonLinks.length)
        val vectorB     = Array.ofDim[Double](commonLinks.length)
        val linkACounts = getCounts(a)
        val linkBCounts = getCounts(b)

        var j = 0
        while (j < commonLinks.length) {
          val link                    = commonLinks(j)
          val countDistinct           = pageCount.count(link)
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

  private def getCommonLinks(
    pageCount: PageCount,
    distinctLinksA: Array[Int],
    distinctLinksB: Array[Int]
  ): Array[Int] = {
    val maxDistinctOverlap      = math.min(distinctLinksA.length, distinctLinksB.length)
    val commonLinks: Array[Int] = Array.ofDim[Int](maxDistinctOverlap)

    var i = 0
    var j = 0
    var k = 0

    // Two-pointer approach since both arrays are sorted
    while (i < distinctLinksA.length && j < distinctLinksB.length) {
      if (distinctLinksA(i) == distinctLinksB(j) && pageCount.count(distinctLinksA(i)) > 0) {
        // Found a common element
        commonLinks(k) = distinctLinksA(i)
        k += 1
        i += 1
        j += 1
      } else if (distinctLinksA(i) < distinctLinksB(j)) {
        // Element in A is smaller, advance A's pointer
        i += 1
      } else {
        // Element in B is smaller, advance B's pointer
        j += 1
      }
    }

    // Return only the filled portion of the array
    commonLinks.take(k)
  }

  private def getCounts(v: LinkVector): CountsWithMax = {
    linkCountsCache.get(
      v.key,
      (_: Long) => {
        val linkCounts = mutable.Map[Int, Int]().withDefaultValue(0)
        var maxCount   = 0
        var j          = 0
        while (j < v.links.length) {
          val link     = v.links(j)
          val newCount = linkCounts(link) + 1
          linkCounts(link) = newCount
          if (newCount > maxCount) {
            maxCount = newCount
          }
          j += 1
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
      .build(
        loader = (id: Int) => {
          val ids = db.link.getByDestination(id).map(_.source).toArray
          LinkVector(ids)
        },
        allLoader = Some((pageIds: Iterable[Int]) => {
          db.link
            .getSourcesByDestination(pageIds.toSeq)
            .map(e => (e._1, LinkVector(e._2)))
        })
      )

  private val outLinkCache: LoadingCache[Int, LinkVector] =
    Scaffeine()
      .maximumSize(cacheSize)
      .build(
        loader = (id: Int) => {
          val ids = db.link.getBySource(id).map(_.destination).toArray
          LinkVector(ids)
        },
        allLoader = Some((pageIds: Iterable[Int]) => {
          db.link
            .getDestinationsBySource(pageIds.toSeq)
            .map(e => (e._1, LinkVector(e._2)))
        })
      )

  // We need counts of how many distinct articles link to each page to
  // calculate the inverse article frequency.
  private val distinctLinksIn: PageCount = {
    logger.info("Reading source counts by destination")
    db.link.readSourceCountsByDestination()
  }

  // We also need counts of how many distinct articles are linked from each
  // page to calculate the inverse article frequency.
  private val distinctLinksOut: PageCount = {
    logger.info("Reading destination counts by source")
    db.link.readDestinationCountsBySource()
  }

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
    * @param linksA       Distinct links into or out of article A
    * @param linksB       Distinct links into or out of article B
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
    var j     = 0
    var k     = 0
    var count = 0

    while (j < linksA.length && k < linksB.length) {
      if (linksA(j) == linksB(k)) {
        count += 1
        j += 1
        k += 1
      } else if (linksA(j) < linksB(k)) {
        j += 1
      } else {
        k += 1
      }
    }

    count
  }

  private[extractor] def hashLinks(input: Array[Int]): Long = {
    val seed1 = 0x9747b28c
    val seed2 = 0x4b3f4e5a
    val hash1 = MurmurHash3.arrayHash(input, seed1)
    val hash2 = MurmurHash3.arrayHash(input, seed2)
    (hash1.toLong << 32) | (hash2.toLong & 0xFFFFFFFFL)
  }
}
