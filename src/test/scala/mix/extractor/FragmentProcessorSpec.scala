package mix.extractor

import mix.extractor.types.ARTICLE
import mix.extractor.util.{Text, UnitSpec}
import pprint.PPrinter.BlackWhite

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class FragmentProcessorSpec extends UnitSpec {
  behavior of "fragmentToPage"

  it should "extract standard fields from a basic article" in {
    val page = FragmentProcessor.fragmentToPage(Text.readTextFile("src/test/resources/animalia.xml"))
    page.id shouldBe 332
    page.pageType shouldBe ARTICLE
    page.title shouldBe "Animalia (book)"
    page.target shouldBe "???"
    page.lastEdited shouldBe 1463364739000L // "2016-05-15T19:12:19Z"
  }

  it should "handle an article that has been blanked" in {
    val page = FragmentProcessor.fragmentToPage(Text.readTextFile("src/test/resources/missing-text.xml"))
    page.id shouldBe 551039
    page.pageType shouldBe ARTICLE
    page.title shouldBe "Wikipedia:WikiProject Missing encyclopedic articles/biographies/G"
    page.target shouldBe "???"
    page.lastEdited shouldBe 1219544429000L
    page.text shouldBe ""
  }

  behavior of "fragmentToMap"

  it should "handle a page" in {
    val expected = mutable.Map(
      "sha1" -> ListBuffer("phoiac9h4m842xq45sp7s6u21eteeq1"),
      "ns" -> ListBuffer("4"),
      "format" -> ListBuffer("text/x-wiki"),
      "model" -> ListBuffer("wikitext"),
      "comment" -> ListBuffer("blanked the page, see main page"),
      "id" -> ListBuffer("551039", "233785688", "3340110"),
      "title" -> ListBuffer("Wikipedia:WikiProject Missing encyclopedic articles/biographies/G"),
      "parentid" -> ListBuffer("229983145"),
      "username" -> ListBuffer("Voorlandt"),
      "timestamp" -> ListBuffer("2008-08-23T19:20:29Z")
    )

    val result = FragmentProcessor.fragmentToMap(Text.readTextFile("src/test/resources/missing-text.xml"))
    result shouldBe expected
  }
}
