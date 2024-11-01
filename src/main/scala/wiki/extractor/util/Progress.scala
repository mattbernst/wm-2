package wiki.extractor.util

object Progress {

  def tick(count: Int, marker: String, n: Int = interval): Unit = {
    if (count % n == 0) {
      System.out.print(marker)
      System.out.flush()
    }
  }

  val interval: Int = 10_000
}
