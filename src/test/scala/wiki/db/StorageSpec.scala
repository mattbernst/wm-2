package wiki.db

import org.scalatest.BeforeAndAfterAll
import org.slf4j.event.Level
import wiki.extractor.language.EnglishSnippetExtractor
import wiki.extractor.types.*
import wiki.extractor.util.{FileHelpers, UnitSpec}
import wiki.extractor.{TitleFinder, WikitextParser}

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
    storage.writeTitleToPage(tf.getFlattenedPageMapping(Set()))
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
    val parsed = parser.parseMarkup(title, markup)
    val native = PageMarkup(randomInt(), Some(markup), parsed)
    val entry  = PageMarkup.serializeUncompressed(native)
    storage.writeMarkups(Seq(entry))
    storage.readMarkup(native.pageId) shouldBe Some(native)
    storage.readMarkup(0) shouldBe None
  }

  "page_markup_z table" should "write and read back markup" in {
    val markup = """#REDIRECT [[Demographics of Afghanistan]]
                   |
                   |{{Redirect category shell|1=
                   |{{R from CamelCase}}
                   |}}""".stripMargin
    val title  = "Test"
    val parsed = parser.parseMarkup(title, markup)
    val native = PageMarkup(randomInt(), Some(markup), parsed)
    val entry  = PageMarkup.serializeCompressed(native)
    storage.writeMarkups_Z(Seq(entry))
    storage.readMarkup_Z(native.pageId) shouldBe Some(native)
    storage.readMarkup_Z(0) shouldBe None
  }

  behavior of "PhaseStorage"

  it should "get None for phase state of unknown phase" in {
    storage.getPhaseState(-1) shouldBe None
  }

  it should "get CREATED for phase state of created phase" in {
    val id = randomInt()
    storage.createPhase(id, s"test $id")
    storage.getPhaseState(id) shouldBe Some(CREATED)
  }

  it should "update CREATED phase to COMPLETED" in {
    val id = randomInt()
    storage.createPhase(id, s"test $id")
    storage.getPhaseState(id) shouldBe Some(CREATED)
    storage.completePhase(id)
    storage.getPhaseState(id) shouldBe Some(COMPLETED)
  }

  behavior of "LogStorage"

  it should "read empty seq for unknown timestamp" in {
    storage.readLogs(randomLong()) shouldBe Seq()
  }

  it should "write and read logs" in {
    val ts = randomLong()
    storage.writeLog(Level.INFO, "Info test", ts)
    storage.writeLog(Level.WARN, "Warn test", ts)

    val expected = Seq(
      StoredLog(level = Level.INFO, message = "Info test", timestamp = ts),
      StoredLog(level = Level.WARN, message = "Warn test", timestamp = ts)
    )

    val result = storage.readLogs(ts)
    result shouldBe expected
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileHelpers.deleteFileIfExists(testDbName)
  }

  private lazy val storage = {
    val db = new Storage(testDbName)
    db.createTableDefinitions(0.to(PhaseStorage.lastPhase))
    db.createIndexes(0.to(PhaseStorage.lastPhase))
    db
  }

  private lazy val parser     = new WikitextParser(EnglishSnippetExtractor)
  private lazy val testDbName = s"test_${randomLong()}.db"
}
