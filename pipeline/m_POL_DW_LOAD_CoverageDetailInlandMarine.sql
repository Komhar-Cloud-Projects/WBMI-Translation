WITH
SQ_CoverageDetailIM AS (
	SELECT 
	PT.PremiumTransactionID,
	SC.CoverageGuid,
	C.PMDNXI1TerrCode
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_4514_stage A
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43NXIMStage C
	ON C.PifSymbol=A.pif_symbol
	AND A.sar_insurance_line='IM'
	AND C.PifPolicyNumber=A.pif_policy_number
	AND C.PifModule=A.pif_module
	AND C.Pmdnxi1InsuranceLine=A.sar_insurance_line
	AND C.Pmdnxi1LocationNumber=case when LEN(ltrim(rtrim(sar_location_x)))=0
	then 0 when isnumeric(sar_location_x)=1
	then convert(numeric(4,0),sar_location_x) else -1 end
	AND C.Pmdnxi1SubLocationNumber=case when LEN(ltrim(rtrim(sar_sub_location_x)))=0
	then 0 when isnumeric(sar_sub_location_x)=1
	then convert(numeric(3,0),sar_sub_location_x) else -1 end
	JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction WPT
	ON A.pif_4514_stage_id=WPT.PremiumTransactionStageId
	JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT
	ON WPT.PremiumTransactionAKId=PT.PremiumTransactionAKId
	AND PT.SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'	
	JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage SC
	ON PT.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
),
EXP_CoverageDetailCommercialAuto AS (
	SELECT
	PremiumTransactionID AS i_PremiumTransactionID,
	CoverageGuid AS i_CoverageGuid,
	PMDNXI1TerrCode AS i_PMDNXI1TerrCode,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59' , 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_CoverageGuid AS o_CoverageGuid,
	-- *INF*: IIF(ISNULL(i_PMDNXI1TerrCode),'N/A',LTRIM(RTRIM(TO_CHAR(i_PMDNXI1TerrCode))))
	IFF(i_PMDNXI1TerrCode IS NULL, 'N/A', LTRIM(RTRIM(TO_CHAR(i_PMDNXI1TerrCode)))) AS o_IsoFireProtectionCode
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
RTR_Target AS (
	SELECT
	LKP_CoverageDetailInlandMarine.PremiumTransactionId AS lkp_PremiumTransactionId,
	EXP_CoverageDetailCommercialAuto.o_PremiumTransactionID AS PremiumTransactionId,
	EXP_CoverageDetailCommercialAuto.o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	EXP_CoverageDetailCommercialAuto.o_AuditID AS AuditID,
	EXP_CoverageDetailCommercialAuto.o_EffectiveDate AS EffectiveDate,
	EXP_CoverageDetailCommercialAuto.o_ExpirationDate AS ExpirationDate,
	EXP_CoverageDetailCommercialAuto.o_SourceSystemID AS SourceSystemID,
	EXP_CoverageDetailCommercialAuto.o_CreatedDate AS CreatedDate,
	EXP_CoverageDetailCommercialAuto.o_ModifiedDate AS ModifiedDate,
	EXP_CoverageDetailCommercialAuto.o_CoverageGuid AS CoverageGuid,
	EXP_CoverageDetailCommercialAuto.o_IsoFireProtectionCode AS IsoFireProtectionCode
	FROM EXP_CoverageDetailCommercialAuto
	LEFT JOIN LKP_CoverageDetailInlandMarine
	ON LKP_CoverageDetailInlandMarine.PremiumTransactionId = EXP_CoverageDetailCommercialAuto.o_PremiumTransactionID
),
RTR_Target_INSERT AS (SELECT * FROM RTR_Target WHERE ISNULL(lkp_PremiumTransactionId)),
RTR_Target_DEFAULT1 AS (SELECT * FROM RTR_Target WHERE NOT ( (ISNULL(lkp_PremiumTransactionId)) )),
TGT_CoverageDetailInlandMarine_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailInlandMarine
	(PremiumTransactionId, CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CoverageGuid, IsoFireProtectionCode)
	SELECT 
	PREMIUMTRANSACTIONID, 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	COVERAGEGUID, 
	ISOFIREPROTECTIONCODE
	FROM RTR_Target_INSERT
),
UPD_Target AS (
	SELECT
	PremiumTransactionId, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	CoverageGuid, 
	IsoFireProtectionCode
	FROM RTR_Target_DEFAULT1
),
TGT_CoverageDetailInlandMarine_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailInlandMarine AS T
	USING UPD_Target AS S
	ON T.PremiumTransactionId = S.PremiumTransactionId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.AuditID = S.AuditID, T.EffectiveDate = S.EffectiveDate, T.ExpirationDate = S.ExpirationDate, T.SourceSystemID = S.SourceSystemID, T.CreatedDate = S.CreatedDate, T.ModifiedDate = S.ModifiedDate, T.CoverageGuid = S.CoverageGuid, T.IsoFireProtectionCode = S.IsoFireProtectionCode
),