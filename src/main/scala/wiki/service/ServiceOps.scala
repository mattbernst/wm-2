package wiki.service

import wiki.db.Storage
import wiki.extractor.types.Page

class ServiceOps(db: Storage) {

  def getPageById(pageId: Int): Option[Page] = db.getPage(pageId)

  def getPageByTitle(title: String): Option[Page] = db.getPage(title)

}
