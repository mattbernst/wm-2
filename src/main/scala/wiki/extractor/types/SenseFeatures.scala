package wiki.extractor.types

case class SenseModelEntry(
  sourcePageId: Int,
  linkDestination: Int,
  label: String,
  sensePageTitle: String,
  senseId: Int,
  commonness: Double,
  inLinkVectorMeasure: Double,
  outLinkVectorMeasure: Double,
  inLinkGoogleMeasure: Double,
  outLinkGoogleMeasure: Double,
  contextQuality: Double,
  isCorrectSense: Boolean)

case class SenseTrainingFields(
  exampleId: Int,
  linkDestination: Int,
  commonness: Double,
  inLinkVectorMeasure: Double,
  outLinkVectorMeasure: Double,
  inLinkGoogleMeasure: Double,
  outLinkGoogleMeasure: Double,
  contextQuality: Double,
  isCorrectSense: Boolean)

case class SenseFeatures(
  group: String,
  page: Page,
  context: Context,
  examples: Array[SenseModelEntry])
