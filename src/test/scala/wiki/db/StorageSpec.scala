package wiki.db

import org.scalatest.BeforeAndAfterAll
import wiki.extractor.{TitleFinder, WikitextParser}
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
    val defaultNamespace  = Namespace(0, FIRST_LETTER, "")
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
    tf.getId("This title does not exist") shouldBe None
    tf.getId("AsciiArt") shouldBe tf.getId("ASCII art")
    tf.getId("Category:Wikipedians who are not a Wikipedian") shouldBe tf.getId(
      "Category:Wikipedians who retain deleted categories on their userpages"
    )
    tf.getId("Ann Arbor, Michigan") shouldBe Some(pages.head.id)
  }

  "page_markup table" should "write and read back markup" in {
    val markup = """#REDIRECT [[Demographics of Afghanistan]]
                   |
                   |{{Redirect category shell|1=
                   |{{R from CamelCase}}
                   |}}""".stripMargin
    val title  = "Test"
    val json   = WikitextParser.serializeAsJson(title, markup)
    val entry  = (randomInt(), Some(markup), json)
    storage.writeMarkups(Seq(entry))
    storage.readMarkup(entry._1) shouldBe Some(markup)
  }

  "page_markup_z table" should "write and read back markup" in {
    val markup = """#REDIRECT [[Demographics of Afghanistan]]
                   |
                   |{{Redirect category shell|1=
                   |{{R from CamelCase}}
                   |}}""".stripMargin
    val title  = "Test"
    val json   = WikitextParser.serializeAsJson(title, markup).get
    val entry  = (randomInt(), Some(ZString.compress(markup)), Some(ZString.compress(json)))
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
