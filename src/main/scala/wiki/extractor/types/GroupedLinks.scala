package wiki.extractor.types

case class GL(label: String, destination: Int, count: Int)

case class GroupedLinks(
  size: Int,
  labels: Array[String],
  destinations: Array[Int],
  counts: Array[Int]) {

  /**
    * Return a slice of grouped links as GL case classes
    * @param lower Lower index of the slice
    * @param upper Upper index of the slice
    * @return GL case classes representing values between lower and upper
    */
  def slice(lower: Int, upper: Int): Array[GL] = {
    require(lower > -1 && upper >= lower && lower < size)
    val result = Array.ofDim[GL](upper - lower)
    var j      = lower
    var n      = 0

    while (j < upper) {
      result(n) = GL(labels(j), destinations(j), counts(j))
      j += 1
      n += 1
    }

    result
  }
}
