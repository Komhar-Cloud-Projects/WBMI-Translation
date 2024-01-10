WITH
SQ_CoverageDetailUnderlyingPolicy_DCT_Negate AS (
	SELECT CDUP.PremiumTransactionID,
	                  CDUP.UnderlyingInsuranceCompanyName,
	                  CDUP.UnderlyingPolicyKey,
	                  CDUP.UnderlyingPolicyType,
	                  CDUP.UnderlyingPolicyLimit,
	                  CDUP.UnderlyingPolicyLimitType,
	                  PT.PremiumTransactionID
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailUnderlyingPolicy CDUP
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransactionDataRepairNegate WPTDRN
	ON CDUP.PremiumTransactionID = WPTDRN.OriginalPremiumTransactionID
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	ON PT.PremiumTransactionAKID = WPTDRN.NewNegatePremiumTransactionAKID
	AND PT.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
),
EXP_CoverageDetailUnderlyingPolicy_DCT_Negate AS (
	SELECT
	PremiumTransactionId AS Old_PremiumTransactionId,
	UnderlyingInsuranceCompanyName,
	UnderlyingPolicyKey,
	UnderlyingPolicyType,
	UnderlyingPolicyLimit,
	UnderlyingPolicyLimitType,
	NewNegatePremiumTransactionID
	FROM SQ_CoverageDetailUnderlyingPolicy_DCT_Negate
),
EXP_Metadata AS (
	SELECT
	NewNegatePremiumTransactionID,
	UnderlyingInsuranceCompanyName AS In_UnderlyingInsuranceCompanyName,
	UnderlyingPolicyKey AS In_UnderlyingPolicyKey,
	UnderlyingPolicyType AS In_UnderlyingPolicyType,
	UnderlyingPolicyLimit AS In_UnderlyingPolicyLimit,
	UnderlyingPolicyLimitType AS In_UnderlyingPolicyLimitType,
	NewNegatePremiumTransactionID AS o_PremiumTransactionID,
	-- *INF*: IIF(ISNULL(In_UnderlyingInsuranceCompanyName),'N/A',In_UnderlyingInsuranceCompanyName)
	IFF(In_UnderlyingInsuranceCompanyName IS NULL,
		'N/A',
		In_UnderlyingInsuranceCompanyName
	) AS o_UnderlyingInsuranceCompanyName,
	-- *INF*: IIF(ISNULL(In_UnderlyingPolicyKey),'N/A',In_UnderlyingPolicyKey)
	IFF(In_UnderlyingPolicyKey IS NULL,
		'N/A',
		In_UnderlyingPolicyKey
	) AS o_UnderlyingPolicyKey,
	-- *INF*: IIF(ISNULL(In_UnderlyingPolicyType),'N/A',In_UnderlyingPolicyType)
	IFF(In_UnderlyingPolicyType IS NULL,
		'N/A',
		In_UnderlyingPolicyType
	) AS o_UnderlyingPolicyType,
	In_UnderlyingPolicyLimit AS o_UnderlyingPolicyLimit,
	In_UnderlyingPolicyLimitType AS o_UnderlyingPolicyLimitType
	FROM EXP_CoverageDetailUnderlyingPolicy_DCT_Negate
),
LKP_CoverageDetailUnderlyingPolicy AS (
	SELECT
	CoverageDetailUnderlyingPolicyId,
	PremiumTransactionId,
	UnderlyingInsuranceCompanyName,
	UnderlyingPolicyKey,
	UnderlyingPolicyType,
	UnderlyingPolicyLimit,
	UnderlyingPolicyLimitType
	FROM (
		SELECT 
			CoverageDetailUnderlyingPolicyId,
			PremiumTransactionId,
			UnderlyingInsuranceCompanyName,
			UnderlyingPolicyKey,
			UnderlyingPolicyType,
			UnderlyingPolicyLimit,
			UnderlyingPolicyLimitType
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailUnderlyingPolicy
		WHERE SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionId,UnderlyingInsuranceCompanyName,UnderlyingPolicyKey,UnderlyingPolicyType,UnderlyingPolicyLimitType ORDER BY CoverageDetailUnderlyingPolicyId) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_CoverageDetailUnderlyingPolicy.CoverageDetailUnderlyingPolicyId AS lkp_CoverageDetailUnderlyingPolicyId,
	LKP_CoverageDetailUnderlyingPolicy.PremiumTransactionId AS lkp_PremiumTransactionId,
	LKP_CoverageDetailUnderlyingPolicy.UnderlyingInsuranceCompanyName AS lkp_UnderlyingInsuranceCompanyName,
	LKP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyKey AS lkp_UnderlyingPolicyKey,
	LKP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyType AS lkp_UnderlyingPolicyType,
	LKP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyLimit AS lkp_UnderlyingPolicyLimit,
	LKP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyLimitType AS lkp_UnderlyingPolicyLimitType,
	EXP_Metadata.o_PremiumTransactionID AS In_PremiumTransactionID,
	EXP_Metadata.o_UnderlyingInsuranceCompanyName AS In_UnderlyingInsuranceCompanyName,
	EXP_Metadata.o_UnderlyingPolicyKey AS In_UnderlyingPolicyKey,
	EXP_Metadata.o_UnderlyingPolicyType AS In_UnderlyingPolicyType,
	EXP_Metadata.o_UnderlyingPolicyLimit AS In_UnderlyingPolicyLimit,
	EXP_Metadata.o_UnderlyingPolicyLimitType AS In_UnderlyingPolicyLimitType,
	lkp_CoverageDetailUnderlyingPolicyId AS o_CoverageDetailUnderlyingPolicyId,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.US')
	TO_DATE('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.US'
	) AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.US')
	TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.US'
	) AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	In_PremiumTransactionID AS o_PremiumTransactionID,
	In_UnderlyingInsuranceCompanyName AS o_UnderlyingInsuranceCompanyName,
	In_UnderlyingPolicyKey AS o_UnderlyingPolicyKey,
	In_UnderlyingPolicyType AS o_UnderlyingPolicyType,
	In_UnderlyingPolicyLimit AS o_UnderlyingPolicyLimit,
	In_UnderlyingPolicyLimitType AS o_UnderlyingPolicyLimitType,
	-- *INF*: IIF(ISNULL(lkp_CoverageDetailUnderlyingPolicyId),'NEW','UPDATE')
	IFF(lkp_CoverageDetailUnderlyingPolicyId IS NULL,
		'NEW',
		'UPDATE'
	) AS o_DetectChanges
	FROM EXP_Metadata
	LEFT JOIN LKP_CoverageDetailUnderlyingPolicy
	ON LKP_CoverageDetailUnderlyingPolicy.PremiumTransactionId = EXP_Metadata.o_PremiumTransactionID AND LKP_CoverageDetailUnderlyingPolicy.UnderlyingInsuranceCompanyName = EXP_Metadata.o_UnderlyingInsuranceCompanyName AND LKP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyKey = EXP_Metadata.o_UnderlyingPolicyKey AND LKP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyType = EXP_Metadata.o_UnderlyingPolicyType AND LKP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyLimitType = EXP_Metadata.o_UnderlyingPolicyLimitType
),
RTR_Insert_Update AS (
	SELECT
	o_CoverageDetailUnderlyingPolicyId AS CoverageDetailUnderlyingPolicyId,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditID AS AuditID,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_SourceSystemId AS SourceSystemId,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_PremiumTransactionID AS PremiumTransactionID,
	o_UnderlyingInsuranceCompanyName AS UnderlyingInsuranceCompanyName,
	o_UnderlyingPolicyKey AS UnderlyingPolicyKey,
	o_UnderlyingPolicyType AS UnderlyingPolicyType,
	o_UnderlyingPolicyLimit AS UnderlyingPolicyLimit,
	o_UnderlyingPolicyLimitType AS UnderlyingPolicyLimitType,
	o_DetectChanges AS DetectChanges
	FROM EXP_DetectChanges
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE DetectChanges='NEW'),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE DetectChanges='UPDATE'),
EXP_Insert AS (
	SELECT
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemId,
	CreatedDate,
	ModifiedDate,
	PremiumTransactionID,
	UnderlyingInsuranceCompanyName,
	UnderlyingPolicyKey,
	UnderlyingPolicyType,
	UnderlyingPolicyLimit,
	UnderlyingPolicyLimitType
	FROM RTR_Insert_Update_INSERT
),
CoverageDetailUnderlyingPolicy_Insert AS (
	INSERT INTO CoverageDetailUnderlyingPolicy
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, PremiumTransactionId, UnderlyingInsuranceCompanyName, UnderlyingPolicyKey, UnderlyingPolicyType, UnderlyingPolicyLimit, UnderlyingPolicyLimitType)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	PremiumTransactionID AS PREMIUMTRANSACTIONID, 
	UNDERLYINGINSURANCECOMPANYNAME, 
	UNDERLYINGPOLICYKEY, 
	UNDERLYINGPOLICYTYPE, 
	UNDERLYINGPOLICYLIMIT, 
	UNDERLYINGPOLICYLIMITTYPE
	FROM EXP_Insert
),