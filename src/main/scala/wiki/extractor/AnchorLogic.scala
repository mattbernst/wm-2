package wiki.extractor

import wiki.extractor.types.Language

class AnchorLogic(language: Language) {

  /**
    * Perform some less aggressive modifications to the anchor text. We need
    * to simplify/normalize link text but preserve casing.
    *
    * @param input An anchor text
    * @return      Anchor text with cleanup
    */
  def cleanAnchor(input: String): String = {
    val k = input
      .split('#')
      .headOption
      .map(_.replace('_', ' '))
      .map(_.trim)
      .getOrElse("")

    // Links like ":fr:Les Cahiers de l'Orient" point to the named page
    // in the language-specific Wikipedia instance. Remove these
    // language-indicator prefixes.
    val cleaned = if (k.startsWith(language.currentWikiPrefix)) {
      k.slice(language.currentWikiPrefix.length, k.length).trim
    } else {
      k
    }

    cleaned.trim
  }
}
