package wiki.extractor

import org.sweble.wikitext.parser.nodes.WtListItem
import wiki.db.Storage
import wiki.extractor.language.LanguageLogic
import wiki.extractor.types.PageType.ARTICLE
import wiki.extractor.types.{DataGroup, TrainingProfile}
import wiki.extractor.util.DBLogging

import scala.collection.mutable.ListBuffer

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
    * @param profile    A language-specific article selection profile
    * @param groups     The groups to use for extraction
    * @param exclusions A set of IDs to exclude
    * @return           A size-N sequence of articles for each size N in groups
    */
  def extractSets(profile: TrainingProfile, groups: Seq[DataGroup], exclusions: Set[Int] = Set()): Seq[Seq[Int]] = {
    val sizes = groups.map(_.size)
    sizes.map(size => extract(size = size, profile = profile, exclusions = exclusions))
  }

  private def extract(size: Int, profile: TrainingProfile, exclusions: Set[Int]): Seq[Int] = {
    val result = ListBuffer[Int]()

    while (result.length < size && articleIds.nonEmpty) {
      val candidate     = articleIds.next()
      val unseen        = exclusions.isEmpty || !exclusions.contains(candidate)
      lazy val outLinks = db.link.getBySource(candidate).length
      lazy val inLinks  = db.link.getByDestination(candidate).length
      if (unseen && outLinks >= profile.minOutLinks && inLinks >= profile.minInLinks) {
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

  val rand = new scala.util.Random(1)

  private val parser = new WikitextParser(languageLogic)

  private val articleIds: Iterator[Int] = {
    DBLogging.info("Loading ARTICLE page identifiers")
    rand
      .shuffle(db.page.getPagesByTypes(Seq(ARTICLE)))
      .iterator
  }
}
