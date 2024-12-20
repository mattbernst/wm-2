package wiki.extractor.types

case class Comparison(
  inLinkVectorMeasure: Double,
  outLinkVectorMeasure: Double,
  inLinkGoogleMeasure: Double,
  outLinkGoogleMeasure: Double)

case class VectorPair(vectorA: Array[Double], vectorB: Array[Double])
