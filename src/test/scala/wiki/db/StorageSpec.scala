package wiki.db

import org.scalatest.BeforeAndAfterAll
import wiki.extractor.types.{FIRST_LETTER, Namespace}
import wiki.extractor.util.{FileHelpers, UnitSpec}

class StorageSpec extends UnitSpec with BeforeAndAfterAll {
  "namespace table" should "write and read back Namespace records" in {
    val ns = Namespace(14, FIRST_LETTER, "Category")
    storage.readNamespace(ns.id) shouldBe None
    storage.writeNamespace(ns)
    storage.readNamespace(ns.id) shouldBe Some(ns)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileHelpers.deleteFileIfExists(testDbName)
  }

  private lazy val storage = {
    val db = new Storage(testDbName)
    db.createTableDefinitions()
    db
  }
  private lazy val testDbName = s"test_${randomLong()}.db"
}
