WITH
SQ_clmt_nurse_referral_stage AS (
	SELECT 
	C.ClmntNurseReferralStageId, 
	C.nurse_referral_id, 
	C.clmt_nurse_manage_id, 
	C.referred_to_nurse_id, 
	C.referral_date, 
	C.created_ts, 
	C.created_user_id, 
	C.modified_ts, 
	C.modified_user_id, 
	C.ExtractDate, 
	C.SourceSystemId
	 
	FROM
	 clmt_nurse_referral_stage C
	
	--WHERE
	--clmt_nurse_referral_stage.created_ts >= --'@{pipeline().parameters.SELECTION_START_TS}'
	--OR
	--clmt_nurse_referral_stage.modified_ts >= --'@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_arch_clmt_nurse_referral_stage AS (
	SELECT
	ClmntNurseReferralStageId,
	nurse_referral_id,
	clmt_nurse_manage_id,
	referred_to_nurse_id,
	referral_date,
	created_ts,
	created_user_id,
	modified_ts,
	modified_user_id,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_clmt_nurse_referral_stage
),
arch_clmt_nurse_referral_stage AS (
	INSERT INTO arch_clmt_nurse_referral_stage
	(ClmntNurseReferralStageId, nurse_referral_id, clmt_nurse_manage_id, referred_to_nurse_id, referral_date, created_ts, created_user_id, modified_ts, modified_user_id, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	CLMNTNURSEREFERRALSTAGEID, 
	NURSE_REFERRAL_ID, 
	CLMT_NURSE_MANAGE_ID, 
	REFERRED_TO_NURSE_ID, 
	REFERRAL_DATE, 
	CREATED_TS, 
	CREATED_USER_ID, 
	MODIFIED_TS, 
	MODIFIED_USER_ID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_arch_clmt_nurse_referral_stage
),