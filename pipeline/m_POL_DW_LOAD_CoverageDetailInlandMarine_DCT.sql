WITH
SQ_CoverageDetailIM AS (
	SELECT DISTINCT
	PT.PremiumTransactionID,
	WorkDCTCoverageTransaction.CoverageGUId,
	WBLA.TerritoryProtectionClass
	FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction WPT
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT
	ON
	WPT.PremiumTransactionAKId=PT.PremiumTransactionAKId AND WPT.SourceSystemId='DCT' AND PT.CurrentSnapshotFlag=1 AND PT.SourceSystemId='DCT'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTCoverageTransaction
	ON
	WorkDCTCoverageTransaction.CoverageId=WPT.PremiumTransactionStageId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTTransactionInsuranceLineLocationBridge
	ON WorkDCTTransactionInsuranceLineLocationBridge.CoverageId=WPT.PremiumTransactionStageId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTInsuranceLine
	ON
	WorkDCTInsuranceLine.LineId=WorkDCTTransactionInsuranceLineLocationBridge.LineId
	AND WorkDCTInsuranceLine.LineType in ('InlandMarine','GamesOfChance', 'HoleInOne')
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTLocation
	ON
	WorkDCTLocation.LocationAssociationId=WorkDCTTransactionInsuranceLineLocationBridge.LocationAssociationId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationAccountStage WBLA
	ON
	WorkDCTLocation.LocationId = WBLA.LocationId
),
EXP_Default AS (
	SELECT
	PremiumTransactionID,
	CoverageGUId,
	TerritoryProtectionClass
	FROM SQ_CoverageDetailIM
),
LKP_CoverageDetailInlandMarine AS (
	SELECT
	PremiumTransactionId
	FROM (
		SELECT 
			PremiumTransactionId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailInlandMarine
		WHERE SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionId ORDER BY PremiumTransactionId) = 1
),
EXP_Metadata AS (
	SELECT
	LKP_CoverageDetailInlandMarine.PremiumTransactionId AS LKP_PremiumTransactionId,
	EXP_Default.PremiumTransactionID AS i_PremiumTransactionID,
	EXP_Default.CoverageGUId AS i_CoverageGUID,
	EXP_Default.TerritoryProtectionClass AS i_TerritoryProtectionClass,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59' , 'MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: LTRIM(RTRIM(i_CoverageGUID))
	LTRIM(RTRIM(i_CoverageGUID)) AS o_CoverageGuid,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_TerritoryProtectionClass)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_TerritoryProtectionClass) AS o_IsoFireProtectionCode,
	-- *INF*: IIF(ISNULL(LKP_PremiumTransactionId), 'NEW','UPDATE')
	IFF(LKP_PremiumTransactionId IS NULL, 'NEW', 'UPDATE') AS v_changeflag,
	v_changeflag AS changeflag
	FROM EXP_Default
	LEFT JOIN LKP_CoverageDetailInlandMarine
	ON LKP_CoverageDetailInlandMarine.PremiumTransactionId = EXP_Default.PremiumTransactionID
),
FIL_Records AS (
	SELECT
	o_PremiumTransactionID, 
	o_CurrentSnapshotFlag, 
	o_AuditID, 
	o_EffectiveDate, 
	o_ExpirationDate, 
	o_SourceSystemID, 
	o_CreatedDate, 
	o_ModifiedDate, 
	o_CoverageGuid, 
	o_IsoFireProtectionCode, 
	changeflag
	FROM EXP_Metadata
	WHERE changeflag = 'NEW'
),
TGT_CoverageDetailInlandMarine_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailInlandMarine
	(PremiumTransactionId, CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CoverageGuid, IsoFireProtectionCode)
	SELECT 
	o_PremiumTransactionID AS PREMIUMTRANSACTIONID, 
	o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	o_AuditID AS AUDITID, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_CoverageGuid AS COVERAGEGUID, 
	o_IsoFireProtectionCode AS ISOFIREPROTECTIONCODE
	FROM FIL_Records
),
SQ_CoverageDetailInlandMarine AS (
	SELECT 
	CDIMPrevious.IsoFireProtectionCode,
	WPTOL.PremiumTransactionID AS Wrk_PremiumTransactionID
	FROM
	WorkPremiumTransactionOffsetLineage WPTOL
	inner join CoverageDetailInlandMarine CDIMPrevious
	on ( CDIMPrevious.PremiumTransactionID= WPTOL.previouspremiumtransactionid)
	inner join CoverageDetailInlandMarine CDIMToUpdate
	on ( CDIMToUpdate.PremiumTransactionID= WPTOL.PremiumTransactionid)
		INNER JOIN premiumtransaction pt WITH (NOLOCK) on
		WPTOL.premiumtransactionid=pt.premiumtransactionid and PT.OffsetOnsetCode='Offset'
	WHERE
	WPTOL.UpdateAttributeFlag = 1 
	AND (CDIMPrevious.IsoFireProtectionCode <> CDIMToUpdate.IsoFireProtectionCode)
),
Exp_CoverageDetailInlandMarine AS (
	SELECT
	IsoFireProtectionCode,
	Wrk_PremiumTransactionID,
	SYSDATE AS o_ModifiedDate
	FROM SQ_CoverageDetailInlandMarine
),
UPD_CoverageDetailInlandMarine AS (
	SELECT
	IsoFireProtectionCode, 
	Wrk_PremiumTransactionID, 
	o_ModifiedDate AS ModifiedDate
	FROM Exp_CoverageDetailInlandMarine
),
TGT_CoverageDetailInlandMarine_Upd_Offset AS (
	MERGE INTO CoverageDetailInlandMarine AS T
	USING UPD_CoverageDetailInlandMarine AS S
	ON T.PremiumTransactionId = S.Wrk_PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.IsoFireProtectionCode = S.IsoFireProtectionCode
),
SQ_CoverageDetailInlandMarine_Deprecated AS (
	SELECT 
	CDIMPrevious.IsoFireProtectionCode,
	WPTOL.PremiumTransactionID AS Wrk_PremiumTransactionID
	FROM
	WorkPremiumTransactionOffsetLineage WPTOL
	inner join CoverageDetailInlandMarine CDIMPrevious
	on ( CDIMPrevious.PremiumTransactionID= WPTOL.previouspremiumtransactionid)
	inner join CoverageDetailInlandMarine CDIMToUpdate
	on ( CDIMToUpdate.PremiumTransactionID= WPTOL.PremiumTransactionid)
		INNER JOIN premiumtransaction pt WITH (NOLOCK) on
		WPTOL.premiumtransactionid=pt.premiumtransactionid and PT.OffsetOnsetCode='Deprecated'
	WHERE
	WPTOL.UpdateAttributeFlag = 1 
	AND (CDIMPrevious.IsoFireProtectionCode <> CDIMToUpdate.IsoFireProtectionCode)
),
Exp_CoverageDetailInlandMarine_Deprecated AS (
	SELECT
	IsoFireProtectionCode,
	Wrk_PremiumTransactionID,
	SYSDATE AS o_ModifiedDate
	FROM SQ_CoverageDetailInlandMarine_Deprecated
),
UPD_CoverageDetailInlandMarine_Deprecated AS (
	SELECT
	IsoFireProtectionCode, 
	Wrk_PremiumTransactionID, 
	o_ModifiedDate AS ModifiedDate
	FROM Exp_CoverageDetailInlandMarine_Deprecated
),
TGT_CoverageDetailInlandMarine_Upd_Deprecated AS (
	MERGE INTO CoverageDetailInlandMarine AS T
	USING UPD_CoverageDetailInlandMarine_Deprecated AS S
	ON T.PremiumTransactionId = S.Wrk_PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.IsoFireProtectionCode = S.IsoFireProtectionCode
),