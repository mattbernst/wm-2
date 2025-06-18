package wiki.db

import scalikejdbc.*
import wiki.util.ConfiguredProperties

object ConfigurationStorage {

  /**
    * Write previously-generated configured properties to the database.
    * Configured properties are initialized during initial extraction of the
    * Wikipedia dump file and subsequently read back from the database.
    *
    * @param input Configured properties for a single Wikipedia instance
    */
  def write(input: ConfiguredProperties): Unit = {
    val serialized = upickle.default.write(input)
    write(props, serialized)
  }

  def readConfiguredPropertiesOptimistic() =
    readConfiguredProperties()
      .getOrElse(throw new NoSuchElementException("Could not load stored configuration from db!"))

  /**
    * Read configured properties back from the database.
    *
    * @return Configured properties for a single Wikipedia instance, if found
    */
  def readConfiguredProperties(): Option[ConfiguredProperties] = {
    read(props).map { data =>
      upickle.default.read[ConfiguredProperties](data)
    }
  }

  private def write(key: String, value: String): Unit = {
    DB.autoCommit { implicit session =>
      sql"""INSERT INTO $table
           (id, data) VALUES ($key, $value)
           """.update(): Unit
    }
  }

  private def read(key: String): Option[String] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT data FROM $table WHERE id=$key"""
        .map(rs => rs.string("data"))
        .single()
    }
  }

  private val props = "properties"
  private val table = Storage.table("configuration")
}
