package wiki.extractor.types

case class LinkModelEntry(
  sourcePageId: Int,
  label: String,
  sensePageTitle: String,
  senseId: Int,
  normalizedOccurrences: Double,
  maxDisambigConfidence: Double,
  avgDisambigConfidence: Double,
  relatednessToContext: Double,
  relatednessToOtherTopics: Double,
  linkProbability: Double,
  firstOccurrence: Double,
  lastOccurrence: Double,
  spread: Double,
  isValidLink: Boolean)

case class LinkTrainingFields(
  exampleId: Int,
  normalizedOccurrences: Double,
  maxDisambigConfidence: Double,
  avgDisambigConfidence: Double,
  relatednessToContext: Double,
  relatednessToOtherTopics: Double,
  linkProbability: Double,
  firstOccurrence: Double,
  lastOccurrence: Double,
  spread: Double,
  isValidLink: Boolean)

case class LinkFeatures(
  group: String,
  page: Page,
  context: Context,
  examples: Array[LinkModelEntry])
