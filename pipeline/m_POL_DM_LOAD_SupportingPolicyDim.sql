WITH
SQ_SupportingPolicy AS (
	SELECT
		PolicyAKId,
		RunDate,
		SupportingPolicyAKId,
		SupportingPolicyKey,
		SupportingPolicyType
	FROM SupportingPolicy
),
EXPTRANS AS (
	SELECT
	PolicyAKId AS EDWPolicyAKId,
	RunDate,
	SupportingPolicyAKId,
	SupportingPolicyKey,
	SupportingPolicyType,
	-- *INF*: ADD_TO_DATE(RunDate,'SS',-1)
	ADD_TO_DATE(RunDate, 'SS', - 1) AS o_RunDate
	FROM SQ_SupportingPolicy
),
LKP_policy_dim AS (
	SELECT
	pol_dim_id,
	edw_pol_ak_id,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
			pol_dim_id,
			edw_pol_ak_id,
			eff_from_date,
			eff_to_date
		FROM policy_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_ak_id,eff_from_date,eff_to_date ORDER BY pol_dim_id) = 1
),
EXPTRANS2 AS (
	SELECT
	LKP_policy_dim.pol_dim_id AS PolicyDimId,
	EXPTRANS.EDWPolicyAKId,
	EXPTRANS.RunDate,
	EXPTRANS.SupportingPolicyAKId,
	EXPTRANS.SupportingPolicyKey,
	EXPTRANS.SupportingPolicyType,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CreatedDate
	FROM EXPTRANS
	LEFT JOIN LKP_policy_dim
	ON LKP_policy_dim.edw_pol_ak_id = EXPTRANS.EDWPolicyAKId AND LKP_policy_dim.eff_from_date <= EXPTRANS.o_RunDate AND LKP_policy_dim.eff_to_date >= EXPTRANS.o_RunDate
),
SupportingPolicyDim AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SupportingPolicyDim;
	INSERT INTO @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SupportingPolicyDim
	(AuditId, CreatedDate, RunDate, PolicyDimId, EDWPolicyAKId, EDWSupportingPolicyAKId, SupportingPolicyKey, SupportingPolicyType)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	RUNDATE, 
	POLICYDIMID, 
	EDWPOLICYAKID, 
	SupportingPolicyAKId AS EDWSUPPORTINGPOLICYAKID, 
	SUPPORTINGPOLICYKEY, 
	SUPPORTINGPOLICYTYPE
	FROM EXPTRANS2
),