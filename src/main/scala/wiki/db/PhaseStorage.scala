package wiki.db

import scalikejdbc.*

sealed trait PHASE_STATE
case object CREATED   extends PHASE_STATE
case object COMPLETED extends PHASE_STATE

object PhaseStorage {

  /**
    * Create a new phase entry in the STARTED state.
    *
    * @param id          Numeric ID for the phase (must be unique)
    * @param description Description of what the phase is doing
    */
  def createPhase(id: Int, description: String): Unit = {
    val startTs = System.currentTimeMillis()
    DB.autoCommit { implicit session =>
      sql"""INSERT OR IGNORE INTO phase VALUES ($id, $description, $startTs, null, $CREATED)"""
        .update(): Unit
    }
  }

  /**
    * Mark the phase as COMPLETED and set its end time.
    *
    * @param id Numeric ID for the phase (must already exist)
    */
  def completePhase(id: Int): Unit = {
    val endTs = System.currentTimeMillis()
    DB.autoCommit { implicit session =>
      sql"""UPDATE phase SET state=$COMPLETED, end_ts=$endTs WHERE id=$id"""
        .update(): Unit
    }
  }

  /**
    * Delete the phase entry so it can be written again.
    *
    * @param id Numeric ID for the phase
    */
  def deletePhase(id: Int): Unit = {
    DB.autoCommit { implicit session =>
      sql"""DELETE FROM phase WHERE id=$id"""
        .update(): Unit
    }
  }

  def getPhaseState(id: Int): Option[PHASE_STATE] = {
    val name = DB.autoCommit { implicit session =>
      sql"""SELECT state FROM phase WHERE id=$id"""
        .map(rs => rs.string("state"))
        .single()
    }
    name.map {
      case "CREATED"   => CREATED
      case "COMPLETED" => COMPLETED
    }
  }

  val lastPhase: Int = 3
}
