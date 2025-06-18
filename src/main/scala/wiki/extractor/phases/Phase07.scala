package wiki.extractor.phases

import wiki.db.Storage
import wiki.extractor.language.LanguageLogic
import wiki.extractor.types.{TrainingProfile, DataGroup}
import wiki.extractor.util.{ConfiguredProperties, DBLogging}
import wiki.extractor.{ArticleFeatureProcessor, ArticleSelectionProfile, ArticleSelector, DataGroup}

import java.io.{File, PrintWriter}
import java.util.concurrent.ForkJoinPool
import scala.collection.parallel.CollectionConverters.*
import scala.collection.parallel.ForkJoinTaskSupport

class Phase07(db: Storage) extends Phase(db: Storage) {

  /**
    * Generate 3 randomized exclusive sets of articles:
    *  - Training set
    *  - Word sense disambiguation test set
    *  - Topic detector test set
    *
    *  The article set only contains PageType.ARTICLE.
    */
  override def run(): Unit = {
    db.phase.deletePhase(number)
    db.executeUnsafely("DROP TABLE IF EXISTS sense_training_context;")
    db.executeUnsafely("DROP TABLE IF EXISTS sense_training_context_page;")
    db.executeUnsafely("DROP TABLE IF EXISTS sense_training_example;")
    db.phase.createPhase(number, s"Building training/test data")
    db.createTableDefinitions(number)

    val ll        = LanguageLogic.getLanguageLogic(props.language.code)
    val selector  = new ArticleSelector(db, ll)
    val processor = new ArticleFeatureProcessor(db, props)

    val groups = Seq(
      ("training", 500),
      ("disambiguation-test", 200),
      ("topic-test", 200)
    )

    DBLogging.info(s"Selecting eligible articles")
    val englishProfile = TrainingProfile(
      groups = Seq(
        DataGroup("training", 5000),
        DataGroup("disambiguation-test", 2000),
        DataGroup("topic-test", 2000)
      ),
      minOutLinks = 15,
      minInLinks = 20,
      maxListProportion = 0.1,
      minWordCount = 400,
      maxWordCount = 4000
    )

    val simpleEnglishProfile = TrainingProfile(
      groups = Seq(
        DataGroup("training", 1000),
        DataGroup("disambiguation-test", 500),
        DataGroup("topic-test", 500)
      ),
      minOutLinks = 2,
      minInLinks = 3,
      maxListProportion = 0.1,
      minWordCount = 150,
      maxWordCount = 4000
    )

    val profile = simpleEnglishProfile

    val res = selector.extractSets(profile)

    val pool        = new ForkJoinPool(props.nWorkers)
    val taskSupport = new ForkJoinTaskSupport(pool)

    // Generate features from the subsets of articles
    res.zip(profile.groups).foreach { set =>
      val subset    = set._1
      val groupName = set._2.name
      DBLogging.info(s"Processing ${subset.length} pages for group $groupName")
      val paralllelGroup = subset.par
      paralllelGroup.tasksupport = taskSupport
      paralllelGroup.foreach { pageId =>
        val senseFeatures = processor.articleToFeatures(pageId, groupName)
        db.senseTraining.write(senseFeatures)
      }
    }

    pool.shutdown()

    groups.foreach(t => writeCSV(t._1))
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
