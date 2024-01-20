WITH
SQ_clmt_nurse_assignment_stage AS (
	SELECT clmt_nurse_assignment_stage.ClmtNurseAssignmentStageId, clmt_nurse_assignment_stage.nurse_assignment_id, clmt_nurse_assignment_stage.clmt_nurse_manage_id, clmt_nurse_assignment_stage.assigned_nurse_id, clmt_nurse_assignment_stage.open_date, clmt_nurse_assignment_stage.closed_date, clmt_nurse_assignment_stage.assignment_comment, clmt_nurse_assignment_stage.work_time_saved_weeks, clmt_nurse_assignment_stage.work_time_saved_days, clmt_nurse_assignment_stage.created_ts, clmt_nurse_assignment_stage.created_user_id, clmt_nurse_assignment_stage.modified_ts, clmt_nurse_assignment_stage.modified_user_id, clmt_nurse_assignment_stage.ExtractDate, clmt_nurse_assignment_stage.SourceSystemId 
	
	FROM
	 clmt_nurse_assignment_stage
	
	--WHERE
	--clmt_nurse_assignment_stage.created_ts >= --'@{pipeline().parameters.SELECTION_START_TS}'
	--OR
	--clmt_nurse_assignment_stage.modified_ts >= --'@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_arch_clmt_nurse_assignment_stage AS (
	SELECT
	ClmtNurseAssignmentStageId,
	nurse_assignment_id,
	clmt_nurse_manage_id,
	assigned_nurse_id,
	open_date,
	closed_date,
	assignment_comment,
	work_time_saved_weeks,
	work_time_saved_days,
	created_ts,
	created_user_id,
	modified_ts,
	modified_user_id,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_clmt_nurse_assignment_stage
),
arch_clmt_nurse_assignment_stage AS (
	INSERT INTO arch_clmt_nurse_assignment_stage
	(ClmtNurseAssignmentStageId, nurse_assignment_id, clmt_nurse_manage_id, assigned_nurse_id, open_date, closed_date, assignment_comment, work_time_saved_weeks, work_time_saved_days, created_ts, created_user_id, modified_ts, modified_user_id, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	CLMTNURSEASSIGNMENTSTAGEID, 
	NURSE_ASSIGNMENT_ID, 
	CLMT_NURSE_MANAGE_ID, 
	ASSIGNED_NURSE_ID, 
	OPEN_DATE, 
	CLOSED_DATE, 
	ASSIGNMENT_COMMENT, 
	WORK_TIME_SAVED_WEEKS, 
	WORK_TIME_SAVED_DAYS, 
	CREATED_TS, 
	CREATED_USER_ID, 
	MODIFIED_TS, 
	MODIFIED_USER_ID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_arch_clmt_nurse_assignment_stage
),