package wiki.extractor.phases

import wiki.db.Storage
import wiki.extractor.language.LanguageLogic
import wiki.extractor.util.DBLogging
import wiki.extractor.{ArticleSelector, WordSenseFeatureProcessor}
import wiki.util.ConfiguredProperties

import java.io.{File, PrintWriter}
import java.util.concurrent.ForkJoinPool
import scala.collection.parallel.CollectionConverters.*
import scala.collection.parallel.ForkJoinTaskSupport

class Phase07(db: Storage) extends Phase(db: Storage) {

  /**
    * Generate two randomized exclusive sets of articles:
    *  - Training set
    *  - Word sense disambiguation test set
    *
    *  The article set only contains PageType.ARTICLE.
    */
  override def run(): Unit = {
    db.phase.deletePhase(number)
    db.executeUnsafely("DROP TABLE IF EXISTS sense_training_context;")
    db.executeUnsafely("DROP TABLE IF EXISTS sense_training_context_page;")
    db.executeUnsafely("DROP TABLE IF EXISTS sense_training_example;")
    db.phase.createPhase(number, s"Building word sense disambiguation training/test data")
    db.createTableDefinitions(number)

    val ll        = LanguageLogic.getLanguageLogic(props.language.code, db)
    val selector  = new ArticleSelector(db, ll)
    val processor = new WordSenseFeatureProcessor(db, props)

    DBLogging.info(s"Selecting eligible articles")
    val profile = props.language.trainingProfile

    val subsets     = selector.extractSets(profile = profile, groups = profile.disambiguatorGroup)
    val pool        = new ForkJoinPool(props.nWorkers)
    val taskSupport = new ForkJoinTaskSupport(pool)

    // Generate features from the subsets of articles
    subsets.zip(profile.disambiguatorGroup).foreach { set =>
      val subset    = set._1
      val groupName = set._2.name
      DBLogging.info(s"Processing ${subset.length} pages for group $groupName")
      val parallelGroup = subset.par
      parallelGroup.tasksupport = taskSupport
      parallelGroup.foreach { pageId =>
        val senseFeatures = processor.articleToFeatures(pageId, groupName)
        db.senseTraining.write(senseFeatures)
      }
    }

    pool.shutdown()

    profile.disambiguatorGroup.foreach(t => writeCSV(t.name))
    db.phase.completePhase(number)
  }

  /**
    * Write each named group of data to a separate CSV file. This is used
    * for external model training and validation.
    *
    * @param groupName The name of the data group to write
    */
  private def writeCSV(groupName: String): Unit = {
    val rows     = db.senseTraining.getTrainingFields(groupName)
    val fileName = s"wiki_${props.language.code}_$groupName.csv"
    val file     = new File(fileName)
    val writer   = new PrintWriter(file)

    try {
      val headerFields = Seq(
        "linkDestination",
        "commonness",
        "inLinkVectorMeasure",
        "outLinkVectorMeasure",
        "inLinkGoogleMeasure",
        "outLinkGoogleMeasure",
        "contextQuality",
        "isCorrectSense"
      )
      writer.println(headerFields.mkString(","))
      rows.foreach { row =>
        writer.println(
          s"${row.linkDestination},${row.commonness},${row.inLinkVectorMeasure},${row.outLinkVectorMeasure},${row.inLinkGoogleMeasure},${row.outLinkGoogleMeasure},${row.contextQuality},${row.isCorrectSense}"
        )
      }
    } finally {
      writer.close()
    }
  }

  private lazy val props: ConfiguredProperties =
    db.configuration.readConfiguredPropertiesOptimistic()

  override def number: Int               = 7
  override val incompleteMessage: String = s"Phase $number incomplete -- redoing"
}
