WITH
SQ_DCPriorInsuranceStaging AS (
	select DC.CarrierName,
	DC.PolicyNumber,
	WB.LineOfBusiness
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPriorInsuranceStaging DC
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBPriorInsuranceStage WB
	on DC.PriorInsuranceId=WB.PriorInsuranceId
	and DC.SessionId=WB.SessionId
),
EXP_GetValues AS (
	SELECT
	CarrierName AS i_CarrierName,
	PolicyNumber AS i_PolicyNumber,
	LineOfBusiness AS i_LineOfBusiness,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifedDate,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_CarrierName)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_CarrierName) AS o_PriorCarrierName,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_PolicyNumber)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_PolicyNumber) AS o_PriorPolicyKey,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_LineOfBusiness)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_LineOfBusiness) AS o_PriorInsuranceLine
	FROM SQ_DCPriorInsuranceStaging
),
AGG_RemoveDuplicate AS (
	SELECT
	o_AuditID AS AuditID, 
	o_SourceSystemId AS SourceSystemId, 
	o_CreatedDate AS CreatedDate, 
	o_ModifedDate AS ModifedDate, 
	o_PriorCarrierName AS PriorCarrierName, 
	o_PriorPolicyKey AS PriorPolicyKey, 
	o_PriorInsuranceLine AS PriorInsuranceLine
	FROM EXP_GetValues
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PriorCarrierName, PriorPolicyKey, PriorInsuranceLine ORDER BY NULL) = 1
),
LKP_PriorCoverage_DCT AS (
	SELECT
	PriorCoverageId,
	PriorCarrierName,
	PriorPolicyKey,
	PriorInsuranceLine
	FROM (
		SELECT 
			PriorCoverageId,
			PriorCarrierName,
			PriorPolicyKey,
			PriorInsuranceLine
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PriorCoverage
		WHERE SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PriorCarrierName,PriorPolicyKey,PriorInsuranceLine ORDER BY PriorCoverageId) = 1
),
FIL_EXISTING AS (
	SELECT
	LKP_PriorCoverage_DCT.PriorCoverageId AS lkp_PriorCoverageId, 
	AGG_RemoveDuplicate.AuditID, 
	AGG_RemoveDuplicate.SourceSystemId, 
	AGG_RemoveDuplicate.CreatedDate, 
	AGG_RemoveDuplicate.ModifedDate, 
	AGG_RemoveDuplicate.PriorCarrierName, 
	AGG_RemoveDuplicate.PriorPolicyKey, 
	AGG_RemoveDuplicate.PriorInsuranceLine
	FROM AGG_RemoveDuplicate
	LEFT JOIN LKP_PriorCoverage_DCT
	ON LKP_PriorCoverage_DCT.PriorCarrierName = AGG_RemoveDuplicate.PriorCarrierName AND LKP_PriorCoverage_DCT.PriorPolicyKey = AGG_RemoveDuplicate.PriorPolicyKey AND LKP_PriorCoverage_DCT.PriorInsuranceLine = AGG_RemoveDuplicate.PriorInsuranceLine
	WHERE ISNULL(lkp_PriorCoverageId)
),
PriorCoverage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PriorCoverage
	(AuditID, SourceSystemID, CreatedDate, ModifiedDate, PriorCarrierName, PriorPolicyKey, PriorInsuranceLine)
	SELECT 
	AUDITID, 
	SourceSystemId AS SOURCESYSTEMID, 
	CREATEDDATE, 
	ModifedDate AS MODIFIEDDATE, 
	PRIORCARRIERNAME, 
	PRIORPOLICYKEY, 
	PRIORINSURANCELINE
	FROM FIL_EXISTING
),