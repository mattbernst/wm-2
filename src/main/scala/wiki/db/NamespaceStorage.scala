package wiki.db

import scalikejdbc.*
import wiki.extractor.types.{CASE_SENSITIVE, Casing, FIRST_LETTER, Namespace}

trait NamespaceStorage {

  /**
    * Write a namespace to the namespace table. This table provides a permanent
    * record of the namespaces encountered in the input Wikipedia dump at
    * extraction time.
    *
    * @param input A Namespace to persist
    */
  def writeNamespace(input: Namespace): Unit = {
    DB.autoCommit { implicit session =>
      sql"""INSERT INTO namespace
           (id, casing, name) VALUES (${input.id}, ${input.casing}, ${input.name})""".update(): Unit
    }
  }

  /**
    * Read a namespace from the namespace table. A namespace may exist in the
    * Wikipedia dump file but be absent from the namespace table if it is not
    * one of the valid namespaces extracted by fragmentToPage in
    * FragmentProcessor.scala
    *
    * @param id The numeric ID of the namespace to read
    * @return   A namespace, if found in the table
    */
  def readNamespace(id: Int): Option[Namespace] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT * FROM namespace WHERE id=$id""".map { rs =>
        val casing: Casing = rs.string("casing") match {
          case "FIRST_LETTER"   => FIRST_LETTER
          case "CASE_SENSITIVE" => CASE_SENSITIVE
        }
        Namespace(id = rs.int("id"), casing = casing, name = rs.string("name"))
      }.single()
    }
  }
}
