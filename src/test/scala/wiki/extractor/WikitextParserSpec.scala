package wiki.extractor

import pprint.PPrinter.BlackWhite
import wiki.extractor.util.{FileHelpers, UnitSpec}

class WikitextParserSpec extends UnitSpec {
  behavior of "serializeAsJson"

  it should "succeed on allotropy" in {
    val title  = "Allotropy"
    val markup = FileHelpers.readTextFile("src/test/resources/allotropy.wikitext")
    val lines = markup.split('\n')
    //lines.zipWithIndex.foreach(e => println(e._2, e._1))
    //val parsed = WikitextParser.parse(title, lines.slice(22, 64).mkString(""))
    val parsed = WikitextParser.parse(title, markup)
    val result = WikitextParser.processNodes(parsed)
//    println(result)

    BlackWhite.pprintln(result.links)
    //BlackWhite.pprintln(parsed)
    println(result.text)
    //BlackWhite.pprintln(parsed, height = 1000)
  }

//  it should "fail on cucurbitane (IncompatibleAstNodeClassException)" in {
//    // N.B. this does not throw IncompatibleAstNodeClassException when using
//    // SerializationMethod.JAVA
//    val title  = "Cucurbitane"
//    val markup = FileHelpers.readTextFile("src/test/resources/cucurbitane.wikitext")
//    val result = WikitextParser.serializeAsJson(title, markup)
//    result shouldBe None
//  }
//
//  it should "fail on Departments of Nicaragua (VisitingException)" in {
//    // N.B. this still fails even with SerializationMethod.JAVA
//    val title  = "Departments of Nicaragua"
//    val markup = FileHelpers.readTextFile("src/test/resources/departments_of_nicaragua.wikitext")
//    val result = WikitextParser.serializeAsJson(title, markup)
//    result shouldBe None
//  }
}
