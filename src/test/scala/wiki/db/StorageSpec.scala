package wiki.db

import org.scalatest.BeforeAndAfterAll
import org.slf4j.event.Level
import wiki.extractor.language.EnglishLanguageLogic
import wiki.extractor.types.*
import wiki.extractor.util.{FileHelpers, UnitSpec}
import wiki.extractor.{TitleFinder, WikitextParser}

class StorageSpec extends UnitSpec with BeforeAndAfterAll {
  behavior of "NamespaceStorage"
  it should "write and read back Namespace records" in {
    val ns = Namespace(14, FIRST_LETTER, "Category")
    storage.namespace.read(ns.id) shouldBe None
    storage.namespace.write(ns)
    storage.namespace.read(ns.id) shouldBe Some(ns)
  }

  behavior of "PageStorage"
  it should "write page records and resolve redirects" in {
    storage.page.writeDumpPages(pages)
    val tf = new TitleFinder(storage.page.readTitlePageMap(), storage.page.readRedirects())
    storage.page.writeTitleToPage(tf.getFlattenedPageMapping())
    tf.getId("This title does not exist") shouldBe None
    tf.getId("AsciiArt") shouldBe tf.getId("ASCII art")
    tf.getId("Category:Wikipedians who are not a Wikipedian") shouldBe tf.getId(
      "Category:Wikipedians who retain deleted categories on their userpages"
    )
    tf.getId("Ann Arbor, Michigan") shouldBe Some(pages.head.id)
  }

  it should "write and read back markup" in {
    val title  = "Test"
    val parsed = parser.parseMarkup(title, sampleMarkup)
    val native = PageMarkup(randomInt(), Some(sampleMarkup), parsed)
    val entry  = PageMarkup.serializeUncompressed(native)
    storage.page.writeMarkups(Seq(entry))
    storage.page.readMarkup(native.pageId) shouldBe Some(native)
    storage.page.readMarkup(0) shouldBe None
  }

  it should "write and read back compressed markup" in {
    val title  = "Test"
    val parsed = parser.parseMarkup(title, sampleMarkup)
    val native = PageMarkup(randomInt(), Some(sampleMarkup), parsed)
    val entry  = PageMarkup.serializeCompressed(native)
    storage.page.writeMarkups_Z(Seq(entry))
    storage.page.readMarkup_Z(native.pageId) shouldBe Some(native)
    storage.page.readMarkup_Z(0) shouldBe None
  }

  behavior of "PhaseStorage"

  it should "get None for phase state of unknown phase" in {
    storage.phase.getPhaseState(-1) shouldBe None
  }

  it should "get CREATED for phase state of created phase" in {
    val id = randomInt()
    storage.phase.createPhase(id, s"test $id")
    storage.phase.getPhaseState(id) shouldBe Some(CREATED)
  }

  it should "update CREATED phase to COMPLETED" in {
    val id = randomInt()
    storage.phase.createPhase(id, s"test $id")
    storage.phase.getPhaseState(id) shouldBe Some(CREATED)
    storage.phase.completePhase(id)
    storage.phase.getPhaseState(id) shouldBe Some(COMPLETED)
  }

  behavior of "LogStorage"

  it should "read empty seq for unknown timestamp" in {
    storage.log.readAll(randomLong()) shouldBe Seq()
  }

  it should "write and read logs" in {
    val ts = randomLong()
    storage.log.write(Level.INFO, "Info test", ts)
    storage.log.write(Level.WARN, "Warn test", ts)

    val expected = Seq(
      StoredLog(level = Level.INFO, message = "Info test", timestamp = ts),
      StoredLog(level = Level.WARN, message = "Warn test", timestamp = ts)
    )

    val result = storage.log.readAll(ts)
    result shouldBe expected
  }

  behavior of "TransclusionStorage"

  it should "ignore duplicated writes" in {
    val m = Map("disambiguation" -> 3)
    storage.transclusion.writeLastTransclusionCounts(m)
    // Before, this would throw on dupe write
    storage.transclusion.writeLastTransclusionCounts(m)
  }

  behavior of "LinkStorage"

  it should "read and write links" in {
    val a    = randomInt()
    val b    = randomInt()
    val c    = randomInt()
    val d    = randomInt()
    val data = Seq(ResolvedLink(a, b, None), ResolvedLink(a, c, Some("chemistry")), ResolvedLink(b, d, Some("physics")))
    storage.link.writeResolved(data)

    storage.link.getBySource(a) shouldBe Seq(ResolvedLink(a, b, None), ResolvedLink(a, c, Some("chemistry")))
    storage.link.getBySource(b) shouldBe Seq(ResolvedLink(b, d, Some("physics")))
    storage.link.getBySource(c) shouldBe Seq()
    storage.link.getByDestination(a) shouldBe Seq()
    storage.link.getByDestination(b) shouldBe Seq(ResolvedLink(a, b, None))
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

  private lazy val pages = {
    val defaultNamespace  = Namespace(0, FIRST_LETTER, "")
    val categoryNamespace = Namespace(14, FIRST_LETTER, "Category")
    val now = System.currentTimeMillis()
    Seq(
      DumpPage(
        id = randomInt(),
        namespace = defaultNamespace,
        pageType = ARTICLE,
        depth = None,
        title = "Ann Arbor, Michigan",
        redirectTarget = None,
        lastEdited = now
      ),
      DumpPage(
        id = randomInt(),
        namespace = categoryNamespace,
        pageType = ARTICLE,
        depth = None,
        title = "Category:Mathematics",
        redirectTarget = None,
        lastEdited = now
      ),
      DumpPage(
        id = randomInt(),
        namespace = defaultNamespace,
        pageType = REDIRECT,
        depth = None,
        title = "AsciiArt",
        redirectTarget = Some("ASCII art"),
        lastEdited = now
      ),
      DumpPage(
        id = randomInt(),
        namespace = defaultNamespace,
        pageType = ARTICLE,
        depth = None,
        title = "ASCII art",
        redirectTarget = None,
        lastEdited = now
      ),
      DumpPage(
        id = randomInt(),
        namespace = categoryNamespace,
        pageType = REDIRECT,
        depth = None,
        title = "Category:Wikipedians who are not a Wikipedian",
        redirectTarget = Some("Category:Wikipedians who retain deleted categories on their userpages"),
        lastEdited = now
      ),
      DumpPage(
        id = randomInt(),
        namespace = categoryNamespace,
        pageType = ARTICLE,
        depth = None,
        title = "Category:Wikipedians who retain deleted categories on their userpages",
        redirectTarget = None,
        lastEdited = now
      )
    )
  }

  private lazy val sampleMarkup = """#REDIRECT [[Demographics of Afghanistan]]
                                    |
                                    |{{Redirect category shell|1=
                                    |{{R from CamelCase}}
                                    |}}""".stripMargin

  private lazy val parser     = new WikitextParser(EnglishLanguageLogic)
  private lazy val testDbName = s"test_${randomLong()}.db"
}
