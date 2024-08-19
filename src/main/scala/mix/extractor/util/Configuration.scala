package mix.extractor.util

import mix.extractor.types.Language

case class ConfiguredProperties(
                                 language: Language,
                                 fragmentWorkers: Int
                               )

object Configuration extends Logging {

  lazy val profile: String = {
    val envVar = "PROFILE"
    sys.env.getOrElse(envVar, {
      val default = "default"
      logger.warn(s"No $envVar set for configuration -- using $default")
      default
    })
  }

  private lazy val defaultConfig = {
    val lang: String = {
      val envVar = "WP_LANG"
      sys.env.getOrElse(envVar, {
        val default = "en"
        logger.warn(s"No $envVar set for wikipedia language -- defaulting to $default")
        default
      })
    }
    val languagesFile: String = {
      val envVar = "LANGUAGES_FILE"
      sys.env.getOrElse(envVar, {
        val default = "languages.json"
        logger.warn(s"No $envVar set for languages file -- defaulting to $default")
        default
      })
    }

    val language = Language
      .fromJSONFile(languagesFile)
      .find(_.code == lang)
      .getOrElse {
        val msg = s"Did not find Language entry for $lang in $languagesFile"
        throw new NoSuchElementException(msg)
      }

    val fragmentWorkers: Int = {
      val envVar = "N_FRAGMENT_WORKERS"
      sys.env.getOrElse(envVar, {
        val default = "4"
        logger.warn(s"No $envVar set for fragment worker count -- defaulting to $default")
        default
      })
        .toInt
    }
    ConfiguredProperties(
      language = language,
      fragmentWorkers = fragmentWorkers
    )
  }

  lazy val props: ConfiguredProperties = {
    profile match {
      case "default" => defaultConfig
      case _ => throw new Exception(s"No configuration defined for profile: $profile")
    }
  }
}
