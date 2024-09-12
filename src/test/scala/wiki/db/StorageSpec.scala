package wiki.db

import org.scalatest.BeforeAndAfterAll
import wiki.extractor.TitleFinder
import wiki.extractor.types.*
import wiki.extractor.util.{FileHelpers, UnitSpec, ZString}

class StorageSpec extends UnitSpec with BeforeAndAfterAll {
  "namespace table" should "write and read back Namespace records" in {
    val ns = Namespace(14, FIRST_LETTER, "Category")
    storage.readNamespace(ns.id) shouldBe None
    storage.writeNamespace(ns)
    storage.readNamespace(ns.id) shouldBe Some(ns)
  }

  "page table redirect logic" should "write page records and resolve redirects" in {
    val defaultNamespace = Namespace(0, FIRST_LETTER, "")
    val categoryNamespace = Namespace(14, FIRST_LETTER, "Category")
    val pages = Seq(
      DumpPage(
        id = randomInt(),
        namespace = defaultNamespace,
        pageType = ARTICLE,
        title = "Ann Arbor, Michigan",
        redirectTarget = None,
        lastEdited = None
      ),
      DumpPage(
        id = randomInt(),
        namespace = categoryNamespace,
        pageType = ARTICLE,
        title = "Category:Mathematics",
        redirectTarget = None,
        lastEdited = None
      ),
      DumpPage(
        id = randomInt(),
        namespace = defaultNamespace,
        pageType = REDIRECT,
        title = "AsciiArt",
        redirectTarget = Some("ASCII art"),
        lastEdited = None
      ),
      DumpPage(
        id = randomInt(),
        namespace = defaultNamespace,
        pageType = ARTICLE,
        title = "ASCII art",
        redirectTarget = None,
        lastEdited = None
      ),
      DumpPage(
        id = randomInt(),
        namespace = categoryNamespace,
        pageType = REDIRECT,
        title = "Category:Wikipedians who are not a Wikipedian",
        redirectTarget = Some("Category:Wikipedians who retain deleted categories on their userpages"),
        lastEdited = None
      ),
      DumpPage(
        id = randomInt(),
        namespace = categoryNamespace,
        pageType = ARTICLE,
        title = "Category:Wikipedians who retain deleted categories on their userpages",
        redirectTarget = None,
        lastEdited = None
      )
    )

    storage.writeDumpPages(pages)
    val tf = new TitleFinder(storage.readTitlePageMap(), storage.readRedirects())
    storage.writeTitleToPage(tf.getFlattenedPageMapping())
    tf.titleToId("AsciiArt") shouldBe tf.titleToId("ASCII art")
    tf.titleToId("Category:Wikipedians who are not a Wikipedian") shouldBe tf.titleToId("Category:Wikipedians who retain deleted categories on their userpages")
    tf.titleToId("Ann Arbor, Michigan") shouldBe pages.head.id

    assertThrows[NoSuchElementException] {
      tf.titleToId("This title does not exist")
    }
  }

  "page_markup table" should "write and read back markup" in {
    val markup = """#REDIRECT [[Demographics of Afghanistan]]
                   |
                   |{{Redirect category shell|1=
                   |{{R from CamelCase}}
                   |}}""".stripMargin
    val entry = (randomInt(), Some(markup))
    storage.writeMarkups(Seq(entry))
    storage.readMarkup(entry._1) shouldBe Some(markup)
  }

  "page_markup_z table" should "write and read back markup" in {
    val markup = """#REDIRECT [[Demographics of Afghanistan]]
                   |
                   |{{Redirect category shell|1=
                   |{{R from CamelCase}}
                   |}}""".stripMargin
    val entry = (randomInt(), Some(ZString.compress(markup)))
    storage.writeMarkups_Z(Seq(entry))
    storage.readMarkup_Z(entry._1) shouldBe Some(markup)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileHelpers.deleteFileIfExists(testDbName)
  }

  private lazy val storage = {
    val db = new Storage(testDbName)
    db.createTableDefinitions()
    db.createIndexes()
    db
  }

  private lazy val testDbName = s"test_${randomLong()}.db"
}
