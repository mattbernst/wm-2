package wiki.extractor.phases

import wiki.db.Storage
import wiki.extractor.language.LanguageLogic
import wiki.extractor.util.{ConfiguredProperties, DBLogging}
import wiki.extractor.{ArticleFeatureProcessor, ArticleSelector}

import java.io.{File, PrintWriter}
import java.util.concurrent.ForkJoinPool
import scala.collection.parallel.CollectionConverters.*
import scala.collection.parallel.ForkJoinTaskSupport

class Phase07(db: Storage) extends Phase(db: Storage) {

  /**
    * Generate 3 randomized exclusive sets of articles:
    *  - Training set
    *  - Disambiguation test set
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
      ("training", 2000),
      ("disambiguation-test", 1000),
      ("topic-test", 1000)
    )

    val res = selector
      .extractSets(
        sizes = groups.map(_._2),
        minOutLinks = 15,
        minInLinks = 20,
        maxListProportion = 0.1,
        minWordCount = 400,
        maxWordCount = 4000
      )

    val pool        = new ForkJoinPool(props.nWorkers)
    val taskSupport = new ForkJoinTaskSupport(pool)

    // Generate features from the subsets of articles
    res.zip(groups).foreach { set =>
      val subset    = set._1
      val groupName = set._2._1
      DBLogging.info(s"Processing ${subset.length} pages for group $groupName")
      val paralllelGroup = subset.par
      paralllelGroup.tasksupport = taskSupport
      paralllelGroup.foreach { pageId =>
        val senseFeatures = processor.articleToFeatures(pageId, groupName)
        db.senseTraining.write(senseFeatures)
      }
    }

    pool.shutdown()

    reweightTrainingData("training")
    groups.foreach(t => writeCSV(t._1))
    db.phase.completePhase(number)
  }

  /**
    * Reweight training data to balance classes once the training group
    * is ready, as in Disambiguator.java's weightTrainingInstances method.
    *
    * @param trainGroup The named data group to reweight
    */
  private def reweightTrainingData(trainGroup: String): Unit = {
    val trainingRows      = db.senseTraining.getTrainingFields(trainGroup)
    val positiveInstances = trainingRows.count(_.isCorrectSense).toDouble
    val negativeInstances = trainingRows.count(!_.isCorrectSense).toDouble
    val p                 = positiveInstances / (positiveInstances + negativeInstances)

    val reweighted = trainingRows.map { r =>
      if (r.isCorrectSense) {
        r.copy(weight = Some(0.5 * (1.0 / p)))
      } else {
        r.copy(weight = Some(0.5 * (1.0 / (1 - p))))
      }
    }

    db.senseTraining.updateTrainingFields(reweighted)
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
      writer.println("commonness,relatedness,contextQuality,isCorrectSense,weight")
      rows.foreach { row =>
        val weightStr = row.weight.map(_.toString).getOrElse("")
        writer.println(
          s"${row.commonness},${row.relatedness},${row.contextQuality},${row.isCorrectSense},$weightStr"
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
