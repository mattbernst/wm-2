package wiki.extractor

import wiki.db.Storage
import wiki.extractor.types.Language

class Contextualizer(comparer: ArticleComparer, db: Storage, language: Language) {
  def getContext(labels: Array[Int]) = {}
}
