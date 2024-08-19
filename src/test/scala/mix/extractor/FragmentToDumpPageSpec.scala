package mix.extractor

import mix.extractor.types.ARTICLE
import mix.extractor.util.{Text, UnitSpec}

class FragmentToDumpPageSpec extends UnitSpec {
  behavior of "processFragment"

  it should "extract standard fields from a basic article" in {
    val pageXML = Text.readTextFile("src/test/resources/animalia.xml")
    val page = FragmentToDumpPage.processFragment(pageXML)
    page.id shouldBe 332
    page.pageType shouldBe ARTICLE
    page.title shouldBe "Animalia (book)"
    page.target shouldBe "???"
    page.lastEdited shouldBe 1463364739000L // "2016-05-15T19:12:19Z"
  }

  it should "handle an article that has been blanked" in {
    val pageXML = Text.readTextFile("src/test/resources/missing-text.xml")
    val page = FragmentToDumpPage.processFragment(pageXML)
    page.id shouldBe 551039
    page.pageType shouldBe ARTICLE
    page.title shouldBe "Wikipedia:WikiProject Missing encyclopedic articles/biographies/G"
    page.target shouldBe "???"
    page.lastEdited shouldBe 1219544429000L
    page.text shouldBe ""
  }
}
