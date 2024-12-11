package wiki.extractor.types

// A label_id using the id from the label table, mapped to a per-destination
// count of links for that sense
case class Sense(labelId: Int, destinationCounts: Map[Int, Int])
