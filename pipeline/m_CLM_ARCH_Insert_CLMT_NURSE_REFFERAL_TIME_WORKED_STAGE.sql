WITH
SQ_nurse_referral_time_worked_stage AS (
	SELECT 
	N.NurseReferralTimeWorkedStageId, 
	N.nurse_referral_id, 
	N.time_worked_seq, 
	N.date_worked, 
	N.time_worked_hours, 
	N.created_ts, 
	N.created_user_id, 
	N.modified_ts, 
	N.modified_user_id, 
	N.ExtractDate, 
	N.SourceSystemId
	 
	FROM
	 nurse_referral_time_worked_stage N
	
	--Where
	--nurse_referral_time_worked_stage.created_ts >= --'@{pipeline().parameters.SELECTION_START_TS}'
	--OR
	--nurse_referral_time_worked_stage.modified_ts >= --'@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_arch_nurse_referral_time_worked_stage AS (
	SELECT
	NurseReferralTimeWorkedStageId,
	nurse_referral_id,
	time_worked_seq,
	date_worked,
	time_worked_hours,
	created_ts,
	created_user_id,
	modified_ts,
	modified_user_id,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_nurse_referral_time_worked_stage
),
arch_nurse_referral_time_worked_stage AS (
	INSERT INTO arch_nurse_referral_time_worked_stage
	(nurse_referral_time_worked_stage_id, nurse_referral_id, time_worked_seq, date_worked, time_worked_hours, created_ts, created_user_id, modified_ts, modified_user_id, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	NurseReferralTimeWorkedStageId AS NURSE_REFERRAL_TIME_WORKED_STAGE_ID, 
	NURSE_REFERRAL_ID, 
	TIME_WORKED_SEQ, 
	DATE_WORKED, 
	TIME_WORKED_HOURS, 
	CREATED_TS, 
	CREATED_USER_ID, 
	MODIFIED_TS, 
	MODIFIED_USER_ID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_arch_nurse_referral_time_worked_stage
),