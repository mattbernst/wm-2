package wiki.extractor

import org.rogach.scallop.*
import wiki.db.Storage
import wiki.extractor.language.LanguageLogic
import wiki.service.ModelProperties
import wiki.util.Logging

import java.io.{File, PrintWriter}
import java.util.concurrent.ForkJoinPool
import scala.collection.parallel.CollectionConverters.*
import scala.collection.parallel.ForkJoinTaskSupport

object ExtractLinkTrainingData extends ModelProperties with Logging {

  private class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val database: ScallopOption[String] = opt[String]()
    verify()
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args.toIndexedSeq)

    val databaseFileName = conf.database
      .orElse(inferDbFile())
      .getOrElse(throw new RuntimeException("No database file found or given!"))

    logger.info(s"Preparing link training data with db $databaseFileName")
    db = getDb(databaseFileName)
    val processor = new LinkFeatureProcessor(db, props)
    db.linkTraining.deleteAll()
    val profile  = props.language.trainingProfile
    val ll       = LanguageLogic.getLanguageLogic(props.language.code)
    val selector = new ArticleSelector(db, ll)

    val used: Set[Int] = profile.disambiguatorGroup
      .map(g => db.senseTraining.getTrainingPages(g.name))
      .reduce(_ ++ _)

    val subsets     = selector.extractSets(profile = profile, groups = profile.linkingGroup, exclusions = used)
    val pool        = new ForkJoinPool(props.nWorkers)
    val taskSupport = new ForkJoinTaskSupport(pool)

    // Generate features from the subsets of articles
    subsets.zip(profile.linkingGroup).foreach { set =>
      val subset    = set._1
      val groupName = set._2.name
      logger.info(s"Processing ${subset.length} pages for group $groupName")
      val parallelGroup = subset.par
      parallelGroup.tasksupport = taskSupport
      parallelGroup.foreach { pageId =>
        val linkFeatures = processor.articleToFeatures(pageId, groupName)
        db.linkTraining.write(linkFeatures)
      }
    }

    profile.linkingGroup.foreach(t => writeCSV(t.name))
    pool.shutdown()
  }

  /**
    * Write each named group of data to a separate CSV file. This is used
    * for external model training and validation.
    *
    * @param groupName The name of the data group to write
    */
  private def writeCSV(groupName: String): Unit = {
    val rows     = db.linkTraining.getTrainingFields(groupName)
    val fileName = s"wiki_${props.language.code}_$groupName.csv"
    val file     = new File(fileName)
    val writer   = new PrintWriter(file)

    try {
      val headerFields = Seq(
        "normalizedOccurrences",
        "maxDisambigConfidence",
        "avgDisambigConfidence",
        "relatednessToContext",
        "relatednessToOtherTopics",
        "linkProbability",
        "firstOccurrence",
        "lastOccurrence",
        "spread",
        "isValidLink"
      )
      writer.println(headerFields.mkString(","))
      rows.foreach { row =>
        writer.println(
          s"${row.normalizedOccurrences},${row.maxDisambigConfidence},${row.avgDisambigConfidence},${row.relatednessToContext},${row.relatednessToOtherTopics},${row.linkProbability},${row.firstOccurrence},${row.lastOccurrence},${row.isValidLink}"
        )
      }
    } finally {
      writer.close()
    }
  }

  private var db: Storage = _
  private lazy val props  = db.configuration.readConfiguredPropertiesOptimistic()
}
