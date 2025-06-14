package wiki.extractor

import org.sweble.wikitext.parser.nodes.WtListItem
import wiki.db.Storage
import wiki.extractor.language.LanguageLogic
import wiki.extractor.types.PageType.ARTICLE
import wiki.extractor.util.DBLogging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

class ArticleSelector(db: Storage, languageLogic: LanguageLogic) {

  /**
    * Extract randomized subsets of articles, each of which matches the
    * selection criteria given. For each size in "sizes", a subset of valid
    * matching articles is returned. For example, if sizes were Seq(2,3) then
    * the result might be something like
    * Seq(Seq(781169, 1134), Seq(12, 98543, 65826)).
    *
    * The subsets are all completely non-overlapping.
    *
    * @param sizes The sizes of each of the subsets desired in the result
    * @param minOutLinks Minimum number of outbound links per article
    * @param minInLinks Minimum number of inbound links per article
    * @param maxListProportion Maximum proportion of article taken by lists
    * @param minWordCount Minimum word count per article
    * @param maxWordCount Maximum word count per article
    * @return A size-N sequence of articles for each size N in sizes
    */
  def extractSets(
    sizes: Seq[Int],
    minOutLinks: Int,
    minInLinks: Int,
    maxListProportion: Double,
    minWordCount: Int,
    maxWordCount: Int
  ): Seq[Seq[Int]] = {
    val rand = new scala.util.Random(1)
    val seen = mutable.Set[Int]()

    sizes.map(s => extract(rand, seen, s, minOutLinks, minInLinks, maxListProportion, minWordCount, maxWordCount))
  }

  private def extract(
    random: Random,
    seen: mutable.Set[Int],
    size: Int,
    minOutLinks: Int,
    minInLinks: Int,
    maxListProportion: Double,
    minWordCount: Int,
    maxWordCount: Int
  ): Seq[Int] = {
    val result = ListBuffer[Int]()
    while (result.length < size) {
      val candidate = articleIds(random.nextInt(articleIds.length))
      if (!seen.contains(candidate)) {
        seen.add(candidate)
        val outLinks = db.link.getBySource(candidate).length
        val inLinks  = db.link.getByDestination(candidate).length
        if (outLinks >= minOutLinks && inLinks >= minInLinks) {
          val markup = db.page.readMarkupAuto(candidate)
          markup.flatMap(_.parseResult).foreach { parseResult =>
            val wordCount = parseResult.text.split("\\s+").length
            if (wordCount >= minWordCount && wordCount <= maxWordCount) {
              val wikiText = markup.flatMap(_.wikitext).getOrElse("")
              if (lineListRatio(wikiText) <= maxListProportion) {
                result.append(candidate)
              }
            }
          }
        }
      }
    }

    result.toSeq
  }

  /**
    * This needs to operate on the full markup of the page to detect list items.
    * While extracting representative article pages, we want to exclude pages
    * that are list-heavy.
    *
    * @param wikiText Wikipedia markup for the page content
    * @return Ratio of list items to newlines
    */
  private def lineListRatio(wikiText: String): Double = {
    val lineCount = wikiText
      .split("\n")
      .map(_.replace(':', ' '))
      .map(_.replace(';', ' '))
      .map(_.trim)
      .count(_.length > 5)

    val listCount = parser
      .extractNodes[WtListItem](parser.parse("", wikiText))
      .length

    if (lineCount > 0) {
      listCount / lineCount.toDouble
    } else {
      1.0
    }
  }

  private val parser = new WikitextParser(languageLogic)

  private lazy val articleIds: Array[Int] = {
    DBLogging.info("Loading ARTICLE page identifiers")
    db.page.getPagesByTypes(Seq(ARTICLE))
  }
}
