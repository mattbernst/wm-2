package wiki.db

import scalikejdbc.*

object MLModelStorage {

  /**
    * Write a named model to the ml_model table. The model is just a blob of
    * binary data.
    *
    * @param name The name of the model
    * @param data The model data
    */
  def write(name: String, data: Array[Byte]): Unit = {
    DB.autoCommit { implicit session =>
      sql"""INSERT INTO $table
           (name, data) VALUES ($name, $data)""".update(): Unit
    }
  }

  /**
    * Read a model back from the ml_model table.
    *
    * @param name The name of the model
    * @return     Model data, if found in the table
    */
  def read(name: String): Option[Array[Byte]] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT * FROM $table WHERE name=$name"""
        .map(rs => rs.bytes("data"))
        .single()
    }
  }

  private val table = Storage.table("ml_model")
}
