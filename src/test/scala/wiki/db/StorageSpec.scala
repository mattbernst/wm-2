package wiki.db

import org.scalatest.BeforeAndAfterAll
import org.slf4j.event.Level
import wiki.extractor.language.EnglishLanguageLogic
import wiki.extractor.types.*
import wiki.extractor.util.{ConfiguredProperties, FileHelpers, UnitSpec}
import wiki.extractor.{TitleFinder, WikitextParser}

import scala.collection.mutable

class StorageSpec extends UnitSpec with BeforeAndAfterAll {
  behavior of "Storage.getPage"

  it should "get nothing for an unknown page" in {
    storage.getPages(Seq(-1)) shouldBe Seq()
  }

  it should "get a stored page with namespace (by ID or title)" in {
    pages.map(_.namespace).foreach(ns => storage.namespace.write(ns))
    storage.page.writePages(pages)
    val expected = pages.head

    storage.getPage(expected.title) shouldBe Some(expected)
    storage.getPages(Seq(expected.id)) shouldBe Seq(expected)
  }

  behavior of "NamespaceStorage"
  it should "write and read back Namespace records" in {
    val ns = Namespace(-1, Casing.FIRST_LETTER, "Category")
    storage.namespace.read(ns.id) shouldBe None
    storage.namespace.write(ns)
    storage.namespace.read(ns.id) shouldBe Some(ns)
  }

  behavior of "PageStorage"
  it should "write page records and resolve redirects" in {
    storage.page.writePages(pages)
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
    storage.phase.getPhaseState(id) shouldBe Some(PhaseState.CREATED)
  }

  it should "update CREATED phase to COMPLETED" in {
    val id = randomInt()
    storage.phase.createPhase(id, s"test $id")
    storage.phase.getPhaseState(id) shouldBe Some(PhaseState.CREATED)
    storage.phase.completePhase(id)
    storage.phase.getPhaseState(id) shouldBe Some(PhaseState.COMPLETED)
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
    val data = Seq(ResolvedLink(a, b, "math"), ResolvedLink(a, c, "chemistry"), ResolvedLink(b, d, "physics"))
    storage.link.writeResolved(data)

    storage.link.getBySource(a) shouldBe Seq(ResolvedLink(a, b, "math"), ResolvedLink(a, c, "chemistry"))
    storage.link.getBySource(b) shouldBe Seq(ResolvedLink(b, d, "physics"))
    storage.link.getBySource(c) shouldBe Seq()
    storage.link.getByDestination(a) shouldBe Seq()
    storage.link.getByDestination(b) shouldBe Seq(ResolvedLink(a, b, "math"))
  }

  behavior of "DepthStorage"

  it should "count pages traversed at depth N" in {
    val n = 10
    storage.depth.count(n) shouldBe 0
    storage.depth.write(Seq(PageDepth(randomInt(), n, Seq())))
    storage.depth.count(n) shouldBe 1
  }

  it should "write and read depth records" in {
    val n = 4
    def randRoute(): Seq[Int] = {
      val tail = 0.to(n).map(_ => randomInt()).toList
      (n :: tail).reverse
    }
    val depths = 0.until(3).map(j => PageDepth(j, n, randRoute()))
    storage.depth.write(depths)
    depths.foreach { pd =>
      depths.contains(storage.depth.read(pd.pageId).get) shouldBe true
    }
  }

  behavior of "LabelStorage"

  it should "write and read back a LabelCounter" in {
    val ac = new LabelCounter
    ac.insert("cadmium", Array(randomInt(), randomInt(), randomInt(), randomInt()))
    ac.insert("ternary", Array(randomInt(), randomInt(), randomInt(), randomInt()))

    storage.label.read() should not be ac
    storage.label.write(ac)
    storage.label.read() shouldBe ac

    // Also support changes
    ac.insert("ternary", Array(randomInt(), randomInt(), randomInt(), randomInt()))
    ac.insert("zincate", Array(randomInt(), randomInt(), randomInt(), randomInt()))
    storage.label.write(ac)

    storage.label.read() shouldBe ac
  }

  behavior of "ConfigurationStorage"

  it should "write and read back ConfiguredProperties" in {
    val props = ConfiguredProperties(
      language = Language(
        code = "en_simple",
        name = "English (simple)",
        disambiguationPrefixes = List("dab", "disambiguation", "disambig", "geodis"),
        rootPage = "Kevin Bacon"
      ),
      nWorkers = 12,
      compressMarkup = true
    )

    storage.configuration.readConfiguredProperties() shouldBe None
    storage.configuration.write(props)
    storage.configuration.readConfiguredProperties() shouldBe Some(props)
  }

  behavior of "SenseStorage"

  it should "write and read back senses for a label" in {
    val labelId = randomInt()
    val d1      = randomInt()
    val d2      = randomInt()
    val sense = Sense(
      labelId = labelId,
      senseCounts = mutable.Map.from(
        Map(
          d1 -> 3,
          d2 -> 2
        )
      )
    )

    storage.sense.getSenseByLabelId(labelId) shouldBe None

    storage.sense.write(Seq(sense))

    storage.sense.getSenseByLabelId(labelId) shouldBe Some(sense)
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
    val defaultNamespace  = Namespace(id = 0, casing = Casing.FIRST_LETTER, name = "")
    val categoryNamespace = Namespace(id = 14, casing = Casing.FIRST_LETTER, name = "Category")
    val now               = System.currentTimeMillis()
    Seq(
      Page(
        id = randomInt(),
        namespace = defaultNamespace,
        pageType = PageType.ARTICLE,
        title = "Ann Arbor, Michigan",
        redirectTarget = None,
        lastEdited = now,
        markupSize = Some(123)
      ),
      Page(
        id = randomInt(),
        namespace = categoryNamespace,
        pageType = PageType.ARTICLE,
        title = "Category:Mathematics",
        redirectTarget = None,
        lastEdited = now,
        markupSize = Some(123)
      ),
      Page(
        id = randomInt(),
        namespace = defaultNamespace,
        pageType = PageType.REDIRECT,
        title = "AsciiArt",
        redirectTarget = Some("ASCII art"),
        lastEdited = now,
        markupSize = Some(123)
      ),
      Page(
        id = randomInt(),
        namespace = defaultNamespace,
        pageType = PageType.ARTICLE,
        title = "ASCII art",
        redirectTarget = None,
        lastEdited = now,
        markupSize = Some(123)
      ),
      Page(
        id = randomInt(),
        namespace = categoryNamespace,
        pageType = PageType.REDIRECT,
        title = "Category:Wikipedians who are not a Wikipedian",
        redirectTarget = Some("Category:Wikipedians who retain deleted categories on their userpages"),
        lastEdited = now,
        markupSize = Some(123)
      ),
      Page(
        id = randomInt(),
        namespace = categoryNamespace,
        pageType = PageType.ARTICLE,
        title = "Category:Wikipedians who retain deleted categories on their userpages",
        redirectTarget = None,
        lastEdited = now,
        markupSize = Some(123)
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
