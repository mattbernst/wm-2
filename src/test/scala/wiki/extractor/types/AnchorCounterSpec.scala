package wiki.extractor.types

import wiki.extractor.util.UnitSpec

class AnchorCounterSpec extends UnitSpec {
  behavior of "setLinkCount"

  it should "initialize a label with counts" in {
    val label               = randomString()
    val linkOccurrenceCount = randomInt()
    val linkDocCount        = linkOccurrenceCount / 2
    val ac                  = AnchorCounter()

    ac.setLinkCount(label, linkOccurrenceCount, linkDocCount)
    ac.getlinkOccurrenceCount(label) shouldBe Some(linkOccurrenceCount)
    ac.getLinkOccurrenceDocCount(label) shouldBe Some(linkDocCount)
  }

  it should "update an existing label with new counts" in {
    val label               = randomString()
    val linkOccurrenceCount = randomInt()
    val linkDocCount        = linkOccurrenceCount / 2
    val ac                  = AnchorCounter()

    ac.setLinkCount(label, linkOccurrenceCount, linkDocCount)
    ac.getlinkOccurrenceCount(label) shouldBe Some(linkOccurrenceCount)
    ac.getLinkOccurrenceDocCount(label) shouldBe Some(linkDocCount)

    ac.setLinkCount(label, linkOccurrenceCount + 1, linkDocCount + 1)
    ac.getlinkOccurrenceCount(label) shouldBe Some(linkOccurrenceCount + 1)
    ac.getLinkOccurrenceDocCount(label) shouldBe Some(linkDocCount + 1)
  }

  behavior of "updateOccurrences"

  it should "throw when called for uninitialized label" in {
    val ac = AnchorCounter()
    assertThrows[NoSuchElementException] {
      ac.updateOccurrences(Map("natural numbers" -> 1))
    }
  }

  it should "update doc count by 1 and occurrence count by N" in {
    val ac = AnchorCounter()
    val l1 = randomString()
    val l2 = randomString()

    ac.setLinkCount(l1, 1, 1)
    ac.setLinkCount(l2, 2, 2)

    val updates = Map(l1 -> 3, l2 -> 2)
    ac.updateOccurrences(updates)

    ac.getOccurrenceCount(l1) shouldBe Some(3)
    ac.getOccurrenceCount(l2) shouldBe Some(2)
    ac.getOccurrenceDocCount(l1) shouldBe Some(1)
    ac.getOccurrenceDocCount(l2) shouldBe Some(1)

    ac.updateOccurrences(updates)
    ac.getOccurrenceCount(l1) shouldBe Some(3 * 2)
    ac.getOccurrenceCount(l2) shouldBe Some(2 * 2)
    ac.getOccurrenceDocCount(l1) shouldBe Some(1 * 2)
    ac.getOccurrenceDocCount(l2) shouldBe Some(1 * 2)
  }
}
