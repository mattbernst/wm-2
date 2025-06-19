package wiki.service

import org.rogach.scallop.*
import wiki.db.PhaseState.COMPLETED
import wiki.db.Storage
import wiki.util.FileHelpers

import java.nio.file.NoSuchFileException

object PrepareWebService extends ServiceProperties {

  private class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    // These turn into kebab-case arguments, e.g.
    // sbt "runMain wiki.service.PrepareWebService --database en_wiki.db --word-sense-model en_word_sense_ranker.cbm"
    val database: ScallopOption[String]       = opt[String]()
    val wordSenseModel: ScallopOption[String] = opt[String]()
    verify()
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args.toIndexedSeq)

    val databaseFileName = conf.database
      .orElse(inferDbFile())
      .getOrElse(throw new RuntimeException("No database file found or given!"))

    println(s"Preparing web service data with db $databaseFileName")
    val db = if (FileHelpers.isFileReadable(databaseFileName)) {
      new Storage(fileName = databaseFileName)
    } else {
      throw new NoSuchFileException(s"Database file $databaseFileName is not readable")
    }

    require(
      db.phase.getPhaseState(db.phase.lastPhase).contains(COMPLETED),
      "Extraction has not completed. Finish extraction and training first."
    )
    prepareModels(db, conf)
  }

  private def prepareModels(db: Storage, conf: Conf): Unit = {
    val props = db.configuration.readConfiguredPropertiesOptimistic()
    if (db.mlModel.read(wsdModelName).isEmpty) {
      val defaultWsdFile = s"pysrc/wiki_${props.language.code}_word_sense_ranker.cbm"
      val wsdFile        = conf.wordSenseModel.getOrElse(defaultWsdFile)
      if (!FileHelpers.isFileReadable(wsdFile)) {
        println(s"Word sense disambiguation model $wsdFile could not be read")
        println(s"You need to give the CatBoost model with --word-sense-model or run train-disambiguation")
        throw new NoSuchFileException(wsdFile)
      } else {
        val modelData = FileHelpers.readBinaryFile(wsdFile)
        require(modelData.nonEmpty, s"File $wsdFile exists but contains no data")
        db.mlModel.write(wsdModelName, modelData)
        val retrieved = db.mlModel.read(wsdModelName).get
        require(retrieved.sameElements(modelData), s"Model data from $wsdFile does not match db $wsdModelName")
      }
    }
  }
}
