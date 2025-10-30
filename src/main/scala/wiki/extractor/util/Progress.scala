package wiki.extractor.util

object Progress {

  def tick(count: Int, marker: String, n: Int = defaultInterval): Unit = {
    val lastInterval    = if (lastCount < 0) -1 else lastCount / n
    val currentInterval = count / n

    val intervalsCrossed = currentInterval - lastInterval

    if (intervalsCrossed > 0) {
      (0.until(intervalsCrossed)).foreach(_ => System.out.print(marker))
      System.out.flush()
    }

    lastCount = count
  }

  private var lastCount: Int = -1
  val defaultInterval: Int   = 10_000
}
