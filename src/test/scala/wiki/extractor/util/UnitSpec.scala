package wiki.extractor.util

import org.scalatest.*
import org.scalatest.flatspec.*
import org.scalatest.matchers.should.*

import scala.util.Random

abstract class UnitSpec extends AnyFlatSpec with OptionValues with Matchers with Inspectors {

  protected def randomLong(): Long = Random.nextLong().abs

  protected def randomInt(): Int = Random.nextInt().abs

  protected def randomString(n: Int = 10): String = {
    val prettyChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_".toCharArray
    val chars       = new Array[Char](n)
    0.until(n).foreach(j => chars(j) = prettyChars(Random.nextInt(prettyChars.length)))
    chars.mkString("")
  }
}
