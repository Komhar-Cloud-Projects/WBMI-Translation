WITH
SQ_Clm_Feature_Staging AS (
	SELECT
		ClmFeatureStagingId,
		ExtractDate,
		SourceSystemId,
		Tch_Claim_Nbr,
		Tch_Client_Id,
		Cov_Type_Cd,
		Cov_Seq_Nbr,
		Bur_Cause_Loss,
		Adjuster_Client_Id,
		Created_TS,
		Modified_TS,
		created_user_id,
		modified_user_id
	FROM Clm_Feature_Staging
),
EXP_Values AS (
	SELECT
	ClmFeatureStagingId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS out_AuditId,
	Tch_Claim_Nbr,
	Tch_Client_Id,
	Cov_Type_Cd,
	Cov_Seq_Nbr,
	Bur_Cause_Loss,
	Adjuster_Client_Id,
	Created_TS,
	Modified_TS,
	created_user_id,
	modified_user_id
	FROM SQ_Clm_Feature_Staging
),
Arch_Clm_Feature_Staging AS (
	INSERT INTO Arch_Clm_Feature_Staging
	(ExtractDate, SourceSystemId, AuditId, Tch_Claim_Nbr, Tch_Client_Id, Cov_Type_Cd, Cov_Seq_Nbr, Bur_Cause_Loss, Adjuster_Client_Id, Created_TS, Modified_TS, created_user_id, modified_user_id)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	out_AuditId AS AUDITID, 
	TCH_CLAIM_NBR, 
	TCH_CLIENT_ID, 
	COV_TYPE_CD, 
	COV_SEQ_NBR, 
	BUR_CAUSE_LOSS, 
	ADJUSTER_CLIENT_ID, 
	CREATED_TS, 
	MODIFIED_TS, 
	CREATED_USER_ID, 
	MODIFIED_USER_ID
	FROM EXP_Values
),