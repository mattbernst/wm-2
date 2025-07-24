package wiki.extractor.types

// This is roughly analogous to the LinkDetector Attributes from the original
// Milne LinkDetector.java. It is missing "generality" from the original,
// and the model suffers as a result. Generality was defined as link
// distance from the "Fundamental categories" page. However, this page is
// gone in recent Wikipedia dumps, so some other way of defining
// generality may be needed.
case class LinkModelEntry(
  sourcePageId: Int,
  sensePageTitle: String,
  senseId: Int,
  normalizedOccurrences: Double,
  maxDisambigConfidence: Double,
  avgDisambigConfidence: Double,
  relatednessToContext: Double,
  relatednessToOtherTopics: Double,
  avgLinkProbability: Double,
  maxLinkProbability: Double,
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
  avgLinkProbability: Double,
  maxLinkProbability: Double,
  firstOccurrence: Double,
  lastOccurrence: Double,
  spread: Double,
  isValidLink: Boolean)

case class LinkFeatures(
  group: String,
  page: Page,
  context: Context,
  examples: Array[LinkModelEntry])
