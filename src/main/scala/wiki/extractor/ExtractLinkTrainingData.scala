package wiki.extractor

import org.rogach.scallop.*
import pprint.PPrinter.BlackWhite
import wiki.extractor.language.LanguageLogic
import wiki.service.ModelProperties
import wiki.util.Logging

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
    val db      = getDb(databaseFileName)
    val props   = db.configuration.readConfiguredPropertiesOptimistic()
    val profile = props.language.trainingProfile

    val ll        = LanguageLogic.getLanguageLogic(props.language.code)
    val selector  = new ArticleSelector(db, ll)
    val processor = new LinkFeatureProcessor(db, props)

    val used: Set[Int] = profile.disambiguatorGroup
      .map(g => db.senseTraining.getTrainingPages(g.name))
      .reduce(_ ++ _)

    val subsets     = selector.extractSets(profile = profile, groups = profile.linkingGroup, exclusions = used)
    val pool        = new ForkJoinPool(props.nWorkers)
    val taskSupport = new ForkJoinTaskSupport(pool)

    // Generate features from the subsets of articles
    subsets.zip(profile.disambiguatorGroup).foreach { set =>
      val subset    = set._1.take(1) // TODO remove take(1)
      val groupName = set._2.name
      logger.info(s"Processing ${subset.length} pages for group $groupName")
      val parallelGroup = subset.par
      parallelGroup.tasksupport = taskSupport
      parallelGroup.foreach { pageId =>
        println(s"FEATURES")
        BlackWhite.pprintln(processor.articleToFeatures(pageId, groupName))
//        val senseFeatures = processor.articleToFeatures(pageId, groupName)
//        db.senseTraining.write(senseFeatures)
      }
    }

    pool.shutdown()
  }
}
