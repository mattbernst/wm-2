package wiki.service

import org.rogach.scallop.*
import wiki.db.Storage
import wiki.util.{FileHelpers, Logging}

import java.nio.file.NoSuchFileException

object PrepareDisambiguation extends ModelProperties with Logging {

  private class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    // These turn into kebab-case arguments, e.g.
    // sbt "runMain wiki.service.PrepareDisambiguation --database en_wiki.db --word-sense-model en_word_sense_ranker.cbm"
    val database: ScallopOption[String]       = opt[String]()
    val wordSenseModel: ScallopOption[String] = opt[String]()
    verify()
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args.toIndexedSeq)

    val databaseFileName = conf.database
      .orElse(inferDbFile())
      .getOrElse(throw new RuntimeException("No database file found or given!"))

    logger.info(s"Preparing word sense disambiguation data with db $databaseFileName")
    val db = getDb(databaseFileName)
    prepareDisambiguationModels(db, conf)
  }

  private def prepareDisambiguationModels(db: Storage, conf: Conf): Unit = {
    val props = db.configuration.readConfiguredPropertiesOptimistic()
    if (db.mlModel.read(wsdModelName).isEmpty) {
      val defaultWsdFile = s"pysrc/wiki_${props.language.code}_word_sense_ranker.cbm"
      val wsdFile        = conf.wordSenseModel.getOrElse(defaultWsdFile)
      if (!FileHelpers.isFileReadable(wsdFile)) {
        logger.error(s"Word sense disambiguation model $wsdFile could not be read")
        logger.error(s"You need to give the CatBoost model with --word-sense-model or run train-disambiguation")
        throw new NoSuchFileException(wsdFile)
      } else {
        val modelData = FileHelpers.readBinaryFile(wsdFile)
        require(modelData.nonEmpty, s"File $wsdFile exists but contains no data")
        db.mlModel.write(wsdModelName, modelData)
        val retrieved = db.mlModel.read(wsdModelName).get
        require(retrieved.sameElements(modelData), s"Model data from $wsdFile does not match db $wsdModelName")
        logger.info(s"Model from $wsdFile saved to ml_model as $wsdModelName")
      }
    } else {
      logger.info(s"Model already stored in ml_model as $wsdModelName")
    }
  }
}
