package wiki.extractor.types

// Recognized types of Wikipedia pages
sealed trait PageType

object PageType {
  // A page that provides informative text about a topic.
  case object ARTICLE extends PageType
  // A page that hierarchically organises other pages
  case object CATEGORY extends PageType
  // A page that exists only to connect an alternative title to an article
  case object REDIRECT extends PageType
  // A page that lists possible senses of an ambiguous word
  case object DISAMBIGUATION extends PageType
  // A page that can be transcluded into other pages
  case object TEMPLATE extends PageType
  // A redirect page that points to a missing page. See discussion of
  // "Roger Broughton" in TitleFinder.scala
  case object DANGLING_REDIRECT extends PageType
  // A page that could not be parsed
  case object UNPARSEABLE extends PageType
  // A type of page that we don't currently deal with
  case object UNHANDLED extends PageType
}

object PageTypes {

  val byNumber: Map[Int, PageType] =
    tuples.map(t => (t._2, t._1)).toMap

  val bySymbol: Map[PageType, Int] =
    tuples.toMap

  private lazy val tuples: Seq[(PageType, Int)] = Seq(
    (PageType.ARTICLE, 1),
    (PageType.CATEGORY, 2),
    (PageType.REDIRECT, 3),
    (PageType.DISAMBIGUATION, 4),
    (PageType.TEMPLATE, 5),
    (PageType.DANGLING_REDIRECT, 6),
    (PageType.UNPARSEABLE, 7),
    (PageType.UNHANDLED, 8)
  )
}
