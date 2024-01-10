WITH
SQ_pif_03_stage AS (
	SELECT
		comments_reason_suspended,
		comments_area
	FROM pif_03_stage
	WHERE pif_03_stage.comments_reason_suspended='ZP'
),
EXP_GetValues AS (
	SELECT
	comments_reason_suspended AS i_comments_reason_suspended,
	comments_area AS i_comments_area,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: LTRIM(RTRIM(i_comments_area))
	LTRIM(RTRIM(i_comments_area
		)
	) AS o_PriorCarrierName,
	'N/A' AS o_PriorPolicyKey,
	'N/A' AS o_PriorInsuranceLine
	FROM SQ_pif_03_stage
),
AGG_RemoveDuplicate AS (
	SELECT
	o_AuditID AS AuditID,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_PriorCarrierName AS PriorCarrierName,
	o_PriorPolicyKey AS PriorPolicyKey,
	o_PriorInsuranceLine AS PriorInsuranceLine
	FROM EXP_GetValues
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PriorCarrierName, PriorPolicyKey, PriorInsuranceLine ORDER BY NULL) = 1
),
LKP_PriorCoverage_PMS AS (
	SELECT
	PriorCoverageId,
	PriorPolicyKey,
	PriorCarrierName,
	PriorInsuranceLine
	FROM (
		SELECT 
			PriorCoverageId,
			PriorPolicyKey,
			PriorCarrierName,
			PriorInsuranceLine
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PriorCoverage
		WHERE SourceSystemId='PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PriorPolicyKey,PriorCarrierName,PriorInsuranceLine ORDER BY PriorCoverageId) = 1
),
FIL_EXISTING AS (
	SELECT
	LKP_PriorCoverage_PMS.PriorCoverageId AS lkp_PriorCoverageId, 
	AGG_RemoveDuplicate.AuditID, 
	AGG_RemoveDuplicate.SourceSystemID, 
	AGG_RemoveDuplicate.CreatedDate, 
	AGG_RemoveDuplicate.ModifiedDate, 
	AGG_RemoveDuplicate.PriorCarrierName, 
	AGG_RemoveDuplicate.PriorPolicyKey, 
	AGG_RemoveDuplicate.PriorInsuranceLine
	FROM AGG_RemoveDuplicate
	LEFT JOIN LKP_PriorCoverage_PMS
	ON LKP_PriorCoverage_PMS.PriorPolicyKey = AGG_RemoveDuplicate.PriorPolicyKey AND LKP_PriorCoverage_PMS.PriorCarrierName = AGG_RemoveDuplicate.PriorCarrierName AND LKP_PriorCoverage_PMS.PriorInsuranceLine = AGG_RemoveDuplicate.PriorInsuranceLine
	WHERE ISNULL(lkp_PriorCoverageId)
),
PriorCoverage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PriorCoverage
	(AuditID, SourceSystemID, CreatedDate, ModifiedDate, PriorCarrierName, PriorPolicyKey, PriorInsuranceLine)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	PRIORCARRIERNAME, 
	PRIORPOLICYKEY, 
	PRIORINSURANCELINE
	FROM FIL_EXISTING
),