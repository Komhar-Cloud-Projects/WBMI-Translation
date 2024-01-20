WITH
SQ_nurse_assignment_impact_deleted_stage AS (
	SELECT 
	N.nurse_assignment_impact_deleted_stage_id, 
	N.nurse_assignment_id, 
	N.impact_type, 
	N.impact_category, 
	N.ExtractDate, 
	N.SourceSystemId 
	
	FROM
	 nurse_assignment_impact_deleted_stage N
),
EXP_arch_nurse_assignment_impact_deleted_stage AS (
	SELECT
	nurse_assignment_impact_deleted_stage_id,
	nurse_assignment_id,
	impact_type,
	impact_category,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_nurse_assignment_impact_deleted_stage
),
arch_nurse_assignment_impact_deleted_stage AS (
	INSERT INTO arch_nurse_assignment_impact_deleted_stage
	(NurseAssignmentImpactDeletedStageId, nurse_assignment_id, impact_type, impact_category, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	nurse_assignment_impact_deleted_stage_id AS NURSEASSIGNMENTIMPACTDELETEDSTAGEID, 
	NURSE_ASSIGNMENT_ID, 
	IMPACT_TYPE, 
	IMPACT_CATEGORY, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_arch_nurse_assignment_impact_deleted_stage
),