package wiki.extractor

import org.sweble.wikitext.parser.nodes.WtListItem
import wiki.db.Storage
import wiki.extractor.language.LanguageLogic
import wiki.extractor.types.TrainingProfile
import wiki.extractor.types.PageType.ARTICLE
import wiki.extractor.util.DBLogging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

class ArticleSelector(db: Storage, languageLogic: LanguageLogic) {

  /**
    * Extract randomized subsets of articles, each of which matches the
    * selection criteria given. For each size in "groups", a subset of valid
    * matching articles is returned. For example, if groups were
    * Seq(DataGroup("test", 2), DataGroup("train", 3)) then
    * the result might be something like
    * Seq(Seq(781169, 1134), Seq(12, 98543, 65826)).
    *
    * The subsets are all completely non-overlapping.
    *
    * @param profile A language-specific article selection profile
    * @return A size-N sequence of articles for each size N in group sizes
    */
  def extractSets(profile: TrainingProfile): Seq[Seq[Int]] = {
    val rand  = new scala.util.Random(1)
    val seen  = mutable.Set[Int]()
    val sizes = profile.groups.map(_.size)

    sizes.map(size => extract(random = rand, seen = seen, size = size, profile = profile))
  }

  private def extract(
    random: Random,
    seen: mutable.Set[Int],
    size: Int,
    profile: TrainingProfile
  ): Seq[Int] = {
    val result = ListBuffer[Int]()
    while (result.length < size) {
      val candidate = articleIds(random.nextInt(articleIds.length))
      if (!seen.contains(candidate)) {
        seen.add(candidate)
        val outLinks = db.link.getBySource(candidate).length
        val inLinks  = db.link.getByDestination(candidate).length
        if (outLinks >= profile.minOutLinks && inLinks >= profile.minInLinks) {
          val markup = db.page.readMarkupAuto(candidate)
          markup.flatMap(_.parseResult).foreach { parseResult =>
            val wordCount = parseResult.text.split("\\s+").length
            if (wordCount >= profile.minWordCount && wordCount <= profile.maxWordCount) {
              val wikiText = markup.flatMap(_.wikitext).getOrElse("")
              if (lineListRatio(wikiText) <= profile.maxListProportion) {
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

  private val articleIds: Array[Int] = {
    DBLogging.info("Loading ARTICLE page identifiers")
    db.page.getPagesByTypes(Seq(ARTICLE))
  }
}
