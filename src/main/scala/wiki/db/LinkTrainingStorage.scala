package wiki.db

import scalikejdbc.*
import wiki.extractor.types.{Context, LinkFeatures, LinkModelEntry, LinkTrainingFields}

object LinkTrainingStorage {

  /**
    * Write a LinkFeatures object to the link training tables. This includes
    * the context data and all training examples.
    *
    * @param input A LinkFeatures object to persist
    */
  def write(input: LinkFeatures): Unit = {
    DB.localTx { implicit session =>
      // First, insert the context and get its ID
      val contextId = writeContext(input.page.id, input.group, input.context)

      // Then insert all training examples with the context ID
      input.examples.foreach { example =>
        writeTrainingExample(input.page.id, input.group, contextId, example)
      }
    }
  }

  /**
    * Get rows of link training examples, containing just minimal fields,
    * by group name. This is used for CSV preparation.
    *
    * @param groupName Name of the data group to retrieve
    * @return          All matched-by-name rows of data
    */
  def getTrainingFields(groupName: String): Seq[LinkTrainingFields] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT *
           FROM $exampleTable WHERE group_name=$groupName"""
        .map(
          rs =>
            LinkTrainingFields(
              exampleId = rs.int("example_id"),
              normalizedOccurrences = rs.double("normalized_occurrences"),
              maxDisambigConfidence = rs.double("max_disambig_confidence"),
              avgDisambigConfidence = rs.double("avg_disambig_confidence"),
              relatednessToContext = rs.double("relatedness_to_context"),
              relatednessToOtherTopics = rs.double("relatedness_to_other_topics"),
              linkProbability = rs.double("link_probability"),
              firstOccurrence = rs.double("first_occurrence"),
              lastOccurrence = rs.double("last_occurrence"),
              spread = rs.double("spread"),
              isValidLink = if (rs.int("is_valid_link") == 1) true else false
            )
        )
        .list()
    }
  }

  /**
    * Get distinct page IDs used during Link training.
    * This is used to ensure that link training does not train/test on the
    * same pages.
    *
    * @param groupName The name of the word Link training group
    * @return          A set of involved page IDs
    */
  def getTrainingPages(groupName: String): Set[Int] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT DISTINCT(Link_page_id)
           FROM $exampleTable
           WHERE group_name=$groupName"""
        .map(rs => rs.int("Link_page_id"))
        .list()
        .toSet
    }
  }

  def deleteAll(): Unit = {
    DB.autoCommit { implicit session =>
      sql"""DELETE FROM $contextTable;""".update(): Unit
      sql"""DELETE FROM $contextPageTable;""".update(): Unit
      sql"""DELETE FROM $exampleTable;""".update(): Unit
    }
  }

  /**
    * Write context data to the Link_training_context and
    * Link_training_context_page tables.
    *
    * @param LinkPageId The ID of the Link page this context belongs to
    * @param group       The training group this context belongs to
    * @param context     The context data to persist
    * @param session     Implicit database session
    * @return            The generated context_id
    */
  private def writeContext(LinkPageId: Int, group: String, context: Context)(implicit session: DBSession): Long = {
    // Insert the context record
    val contextId = sql"""INSERT INTO $contextTable
         (Link_page_id, group_name, quality)
         VALUES ($LinkPageId, $group, ${context.quality})
         """
      .updateAndReturnGeneratedKey()

    // Insert all representative pages for this context
    context.pages.foreach { repPage =>
      sql"""INSERT INTO $contextPageTable
           (context_id, page_id, weight)
           VALUES ($contextId, ${repPage.pageId}, ${repPage.weight})
           """
        .update()
    }

    contextId
  }

  /**
    * Write a training example to the Link_training_example table.
    *
    * @param sourcePageId The ID of the Link page this example belongs to
    * @param group      The training group this example belongs to
    * @param contextId  The ID of the context this example belongs to
    * @param example    The ModelEntry to persist
    * @param session    Implicit database session
    */
  private def writeTrainingExample(
    sourcePageId: Int,
    group: String,
    contextId: Long,
    example: LinkModelEntry
  )(implicit session: DBSession
  ): Unit = {
    sql"""INSERT INTO $exampleTable
         (context_id, group_name, source_page_id, label,
         sense_page_title, sense_id, normalized_occurrences,
         max_disambig_confidence, avg_disambig_confidence,
         relatedness_to_context, relatedness_to_other_topics,
         link_probability, first_occurrence, last_occurrence,
         spread, is_valid_link)
         VALUES ($contextId, $group, $sourcePageId, ${example.label},
                 ${example.sensePageTitle}, ${example.senseId},
                 ${example.normalizedOccurrences}, ${example.maxDisambigConfidence},
                 ${example.avgDisambigConfidence}, ${example.relatednessToContext},
                 ${example.relatednessToOtherTopics}, ${example.linkProbability},
                 ${example.firstOccurrence}, ${example.lastOccurrence},
                 ${example.spread}, ${example.isValidLink})
       """
      .update(): Unit

  }

  private val contextTable     = Storage.table("link_training_context")
  private val contextPageTable = Storage.table("link_training_context_page")
  private val exampleTable     = Storage.table("link_training_example")
}
