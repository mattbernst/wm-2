package wiki.db

import scalikejdbc.*
import wiki.extractor.types.{Context, SenseFeatures, SenseModelEntry, SenseTrainingFields}

object SenseTrainingStorage {

  /**
    * Write a SenseFeatures object to the sense training tables. This includes
    * the context data and all training examples.
    *
    * @param input A SenseFeatures object to persist
    */
  def write(input: SenseFeatures): Unit = {
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
    * Get rows of sense training examples, containing just minimal fields,
    * by group name. This is used for CSV preparation.
    *
    * @param groupName Name of the data group to retrieve
    * @return          All matched-by-name rows of data
    */
  def getTrainingFields(groupName: String): Seq[SenseTrainingFields] = {
    DB.autoCommit { implicit session =>
      sql"""SELECT *
           FROM $exampleTable WHERE group_name=$groupName"""
        .map(
          rs =>
            SenseTrainingFields(
              exampleId = rs.int("example_id"),
              linkDestination = rs.int("link_destination"),
              commonness = rs.double("commonness"),
              inLinkVectorMeasure = rs.double("in_link_vector_measure"),
              outLinkVectorMeasure = rs.double("out_link_vector_measure"),
              inLinkGoogleMeasure = rs.double("in_link_google_measure"),
              outLinkGoogleMeasure = rs.double("out_link_google_measure"),
              contextQuality = rs.double("context_quality"),
              isCorrectSense = if (rs.int("is_correct_sense") == 1) true else false
            )
        )
        .list()
    }
  }

  /**
    * Write context data to the sense_training_context and
    * sense_training_context_page tables.
    *
    * @param sensePageId The ID of the sense page this context belongs to
    * @param group       The training group this context belongs to
    * @param context     The context data to persist
    * @param session     Implicit database session
    * @return            The generated context_id
    */
  private def writeContext(sensePageId: Int, group: String, context: Context)(implicit session: DBSession): Long = {
    // Insert the context record
    val contextId = sql"""INSERT INTO $contextTable
         (sense_page_id, group_name, quality)
         VALUES ($sensePageId, $group, ${context.quality})
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
    * Write a training example to the sense_training_example table.
    *
    * @param sensePageId The ID of the sense page this example belongs to
    * @param group       The training group this example belongs to
    * @param contextId   The ID of the context this example belongs to
    * @param example     The ModelEntry to persist
    * @param session     Implicit database session
    */
  private def writeTrainingExample(
    sensePageId: Int,
    group: String,
    contextId: Long,
    example: SenseModelEntry
  )(implicit session: DBSession
  ): Unit = {
    sql"""INSERT INTO $exampleTable
         (sense_page_id, context_id, group_name, link_destination, label,
          sense_id, commonness, in_link_vector_measure, out_link_vector_measure,
          in_link_google_measure, out_link_google_measure, context_quality,
          is_correct_sense)
         VALUES ($sensePageId, $contextId, $group, ${example.linkDestination},
                 ${example.label}, ${example.senseId}, ${example.commonness},
                 ${example.inLinkVectorMeasure}, ${example.outLinkVectorMeasure},
                 ${example.inLinkGoogleMeasure}, ${example.outLinkGoogleMeasure},
                 ${example.contextQuality}, ${example.isCorrectSense})
       """
      .update(): Unit
  }

  private val contextTable     = Storage.table("sense_training_context")
  private val contextPageTable = Storage.table("sense_training_context_page")
  private val exampleTable     = Storage.table("sense_training_example")
}
