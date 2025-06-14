package wiki.extractor.types

case class Comparison(
  inLinkVectorMeasure: Double,
  outLinkVectorMeasure: Double,
  inLinkGoogleMeasure: Double,
  outLinkGoogleMeasure: Double) {

  val mean: Double = {
    (inLinkVectorMeasure + outLinkVectorMeasure + inLinkGoogleMeasure + outLinkGoogleMeasure) / 4.0
  }
}

case class VectorPair(vectorA: Array[Double], vectorB: Array[Double])
