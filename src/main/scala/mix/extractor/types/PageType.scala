package mix.extractor.types

// Recognized types of Wikipedia pages
sealed trait PageType
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
// A type of page that we don't currently deal with
case object INVALID extends PageType
