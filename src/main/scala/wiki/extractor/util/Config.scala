package wiki.extractor.util

import wiki.extractor.types.Language

case class ConfiguredProperties(language: Language, fragmentWorkers: Int, compressMarkup: Boolean)

object Config extends Logging {

  lazy val profile: String = {
    val envVar = "PROFILE"
    sys.env.getOrElse(envVar, {
      val default = "default"
      logger.info(s"No $envVar set for configuration -- using $default")
      default
    })
  }

  private lazy val defaultConfig = {
    val lang: String = {
      val envVar = "WP_LANG"
      sys.env.getOrElse(envVar, {
        val default = "en"
        logger.info(s"No $envVar set for wikipedia language -- defaulting to $default")
        default
      })
    }

    val languagesFile: String = {
      val envVar = "LANGUAGES_FILE"
      sys.env.getOrElse(envVar, {
        val default = "languages.json"
        logger.info(s"No $envVar set for languages file -- defaulting to $default")
        default
      })
    }

    val language = Language.fromJSONFile(languagesFile).find(_.code == lang).getOrElse {
      val msg = s"Did not find Language entry for $lang in $languagesFile"
      throw new NoSuchElementException(msg)
    }

    val workerThreads: Int = {
      val envVar = "N_WORKERS"
      val n = sys.env
        .getOrElse(
          envVar, {
            // By default, use all availableProcessors - 2.
            // The -2 is to leave one reserved for reading input and writing to DB.
            // The minimum is 1.
            val available = Runtime.getRuntime.availableProcessors()
            val default   = Math.max(available - 2, 1).toString
            logger.info(s"No $envVar set for worker thread count -- defaulting to $default")
            default
          }
        )
        .toInt
      require(n > 0, s"$envVar must be at least 1 (given: $n)")
      n
    }

    val compressMarkup: Boolean = {
      val envVar = "COMPRESS_MARKUP"
      val storeCompressed = sys.env
        .getOrElse(envVar, {
          val default = "true"
          logger.info(s"No $envVar set for markup compression -- defaulting to $default")
          default
        })
        .toBoolean
      if (storeCompressed) {
        logger.info(s"Binary compressed markup will be stored in table page_markup_z.")
      } else {
        logger.warn(s"Text markup will be stored in table page_markup. This is human-readable but slow.")
      }
      storeCompressed
    }

    ConfiguredProperties(
      language = language,
      fragmentWorkers = workerThreads,
      compressMarkup = compressMarkup
    )
  }

  lazy val props: ConfiguredProperties = {
    profile match {
      case "default" => defaultConfig
      case _         => throw new Exception(s"No configuration defined for profile: $profile")
    }
  }
}
