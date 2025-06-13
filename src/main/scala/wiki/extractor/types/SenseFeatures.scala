package wiki.extractor.types

case class SenseModelEntry(
  sourcePageId: Int,
  linkDestination: Int,
  label: String,
  sensePageTitle: String,
  senseId: Int,
  commonness: Double,
  relatedness: Double,
  contextQuality: Double,
  isCorrectSense: Boolean,
  weight: Option[Double])

case class SenseTrainingFields(
  exampleId: Int,
  commonness: Double,
  relatedness: Double,
  contextQuality: Double,
  isCorrectSense: Boolean,
  weight: Option[Double])

case class SenseFeatures(
  group: String,
  page: Page,
  context: Context,
  examples: Array[SenseModelEntry])
