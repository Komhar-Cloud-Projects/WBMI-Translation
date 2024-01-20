WITH
SQ_nurse_assignment_impact_stage AS (
	SELECT 
	N.NurseAssignmentImpactStageId, 
	N.nurse_assignment_id, 
	N.impact_type, 
	N.impact_category, 
	N.savings_amount, 
	N.impact_comment, 
	N.created_ts, 
	N.created_user_id, 
	N.modified_ts, 
	N.modified_user_id, 
	N.ExtractDate, 
	N.SourceSystemId
	 
	FROM
	 nurse_assignment_impact_stage N
	
	--Where
	--nurse_assignment_impact_stage.created_ts >= --'@{pipeline().parameters.SELECTION_START_TS}'
	--OR
	--nurse_assignment_impact_stage.modified_ts >= --'@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_arch_nurse_assignment_impact_stage AS (
	SELECT
	NurseAssignmentImpactStageId,
	nurse_assignment_id,
	impact_type,
	impact_category,
	savings_amount,
	impact_comment,
	created_ts,
	created_user_id,
	modified_ts,
	modified_user_id,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_nurse_assignment_impact_stage
),
arch_nurse_assignment_impact_stage AS (
	INSERT INTO arch_nurse_assignment_impact_stage
	(NurseAssignmentImpactStageId, nurse_assignment_id, impact_type, impact_category, savings_amount, impact_comment, created_ts, created_user_id, modified_ts, modified_user_id, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	NURSEASSIGNMENTIMPACTSTAGEID, 
	NURSE_ASSIGNMENT_ID, 
	IMPACT_TYPE, 
	IMPACT_CATEGORY, 
	SAVINGS_AMOUNT, 
	IMPACT_COMMENT, 
	CREATED_TS, 
	CREATED_USER_ID, 
	MODIFIED_TS, 
	MODIFIED_USER_ID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_arch_nurse_assignment_impact_stage
),