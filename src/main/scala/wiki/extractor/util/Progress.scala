package wiki.extractor.util

object Progress {

  def tick(count: Int, marker: String): Unit = {
    if (count % interval == 0) {
      System.out.print(marker)
      System.out.flush()
    }
  }

  val interval: Int = 10000
}
