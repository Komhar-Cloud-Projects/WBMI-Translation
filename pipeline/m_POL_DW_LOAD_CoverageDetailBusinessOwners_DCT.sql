WITH
SQ_DCTWorkTable AS (
	SELECT
		WorkDCTPolicy.BCCCode,
		WorkDCTTransactionInsuranceLineLocationBridge.CoverageId,
		WorkDCTTransactionInsuranceLineLocationBridge.OccupancyType,
		WorkDCTTransactionInsuranceLineLocationBridge.PredominantPersonalPropertyRateNumber,
		WorkDCTTransactionInsuranceLineLocationBridge.PredominantLiabilityLiabClassGroup,
		WorkDCTCoverageTransaction.CoverageGUID,
		WorkDCTCoverageTransaction.CoverageType,
		WorkDCTInsuranceLine.LineId,
		WorkDCTTransactionInsuranceLineLocationBridge.ISOOccupancyType,
		WorkDCTTransactionInsuranceLineLocationBridge.PredominantBuildingBCCCode
	FROM WorkDCTTransactionInsuranceLineLocationBridge
	INNER JOIN WorkDCTCoverageTransaction
	INNER JOIN WorkDCTInsuranceLine
	INNER JOIN WorkDCTPolicy
	ON WorkDCTPolicy.PolicyId=WorkDCTInsuranceLine.PolicyId
	and
	WorkDCTInsuranceLine.LineId=WorkDCTTransactionInsuranceLineLocationBridge.LineId
	and
	WorkDCTInsuranceLine.LineType in ('BusinessOwners')
	and
	WorkDCTCoverageTransaction.CoverageId=WorkDCTTransactionInsuranceLineLocationBridge.CoverageId
),
EXP_Stage AS (
	SELECT
	BCCCode AS i_BCCCode,
	CoverageId,
	OccupancyType AS i_OccupancyType,
	PredominantPersonalPropertyRateNumber AS i_PropertyRateNumber,
	PredominantLiabilityLiabClassGroup AS i_LiabilityClassGroup,
	CoverageGUID,
	CoverageType AS i_CoverageType,
	ISOOccupancyType AS i_ISOOccupancyType,
	PredominantBuildingBCCCode AS i_PredominantBuildingBCCCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ISOOccupancyType)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_ISOOccupancyType) AS o_ISOOccupancyType,
	-- *INF*: IIF(IN(i_LiabilityClassGroup,'51','52','53','54','55','56','57','58','59')=1 AND i_ISOOccupancyType='Lessors Liability' ,1,0)
	IFF(
	    i_LiabilityClassGroup IN ('51','52','53','54','55','56','57','58','59') = 1
	    and i_ISOOccupancyType = 'Lessors Liability',
	    1,
	    0
	) AS v_LiabilityClassGroup_51_59_Flag,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(i_PropertyRateNumber),i_PropertyRateNumber,
	-- REG_MATCH(i_CoverageType,'.*PlusPak.*')=1,'Plus Pak',
	-- 'Policy Level')
	DECODE(
	    TRUE,
	    i_PropertyRateNumber IS NOT NULL, i_PropertyRateNumber,
	    REGEXP_LIKE(i_CoverageType, '.*PlusPak.*') = 1, 'Plus Pak',
	    'Policy Level'
	) AS o_PropertyRateNumber,
	-- *INF*: DECODE(TRUE,
	-- v_LiabilityClassGroup_51_59_Flag=1 AND i_BCCCode='1328','51-59  Office',
	-- v_LiabilityClassGroup_51_59_Flag=1 AND IN(i_BCCCode,'1326','189','1098','1330','1408','692','907')=1,'51-59  Shop/Storage',
	-- NOT ISNULL(i_LiabilityClassGroup),i_LiabilityClassGroup,
	-- REG_MATCH(i_CoverageType,'.*PlusPak.*')=1,'Plus Pak',
	-- 'Policy Level')
	DECODE(
	    TRUE,
	    v_LiabilityClassGroup_51_59_Flag = 1 AND i_BCCCode = '1328', '51-59  Office',
	    v_LiabilityClassGroup_51_59_Flag = 1 AND i_BCCCode IN ('1326','189','1098','1330','1408','692','907') = 1, '51-59  Shop/Storage',
	    i_LiabilityClassGroup IS NOT NULL, i_LiabilityClassGroup,
	    REGEXP_LIKE(i_CoverageType, '.*PlusPak.*') = 1, 'Plus Pak',
	    'Policy Level'
	) AS o_LiabilityClassGroup,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_PredominantBuildingBCCCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_PredominantBuildingBCCCode) AS o_PredominantBuildingBCCCode
	FROM SQ_DCTWorkTable
),
SQ_EDW_DCT AS (
	SELECT DISTINCT PT.PremiumTransactionID, WPT.PremiumTransactionStageId 
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransaction WPT 
	       INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Premiumtransaction PT ON PT.PremiumTransactionAKID = WPT.PremiumTransactionAKID 
	       INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Ratingcoverage RC ON RC.RatingCoverageAKID = PT.RatingCoverageAKID AND RC.Effectivedate = PT.Effectivedate 
	       INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Policycoverage PC ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID AND PC.Currentsnapshotflag =1 
	WHERE  PT.Sourcesystemid = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND WPT.Sourcesystemid = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	AND PC.InsuranceLine in ('BusinessOwners')
),
JNR_IL_Stage AS (SELECT
	SQ_EDW_DCT.PremiumTransactionID, 
	SQ_EDW_DCT.PremiumTransactionStageId, 
	EXP_Stage.CoverageId, 
	EXP_Stage.CoverageGUID, 
	EXP_Stage.o_PropertyRateNumber AS PropertyRateNumber, 
	EXP_Stage.o_LiabilityClassGroup AS LiabilityClassGroup, 
	EXP_Stage.o_ISOOccupancyType AS ISOOccupancyType, 
	EXP_Stage.o_PredominantBuildingBCCCode AS PredominantBuildingBCCCode
	FROM SQ_EDW_DCT
	INNER JOIN EXP_Stage
	ON EXP_Stage.CoverageId = SQ_EDW_DCT.PremiumTransactionStageId
),
AGG_DuplicateRemove AS (
	SELECT
	PremiumTransactionID,
	CoverageGUID,
	PropertyRateNumber AS ISOBusinessOwnersPropertyRateNumber,
	LiabilityClassGroup AS ISOBusinessOwnersLiabilityClassGroup,
	ISOOccupancyType,
	PredominantBuildingBCCCode
	FROM JNR_IL_Stage
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY NULL) = 1
),
EXP_Metadata AS (
	SELECT
	PremiumTransactionID AS i_PremiumTransactionID,
	CoverageGUID AS i_CoverageGUID,
	ISOBusinessOwnersPropertyRateNumber AS i_ISOBusinessOwnersPropertyRateNumber,
	ISOBusinessOwnersLiabilityClassGroup AS i_ISOBusinessOwnersLiabilityClassGroup,
	ISOOccupancyType AS i_ISOOccupancyType,
	PredominantBuildingBCCCode AS i_PredominantBuildingBCCCode,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	-- *INF*: RTRIM(LTRIM(i_CoverageGUID))
	RTRIM(LTRIM(i_CoverageGUID)) AS o_CoverageGUID,
	-- *INF*: IIF(NOT ISNULL(i_ISOBusinessOwnersPropertyRateNumber),i_ISOBusinessOwnersPropertyRateNumber,'N/A')
	IFF(
	    i_ISOBusinessOwnersPropertyRateNumber IS NOT NULL, i_ISOBusinessOwnersPropertyRateNumber,
	    'N/A'
	) AS o_ISOBusinessOwnersPropertyRateNumber,
	-- *INF*: IIF(NOT ISNULL(i_ISOBusinessOwnersLiabilityClassGroup),i_ISOBusinessOwnersLiabilityClassGroup,'N/A')
	IFF(
	    i_ISOBusinessOwnersLiabilityClassGroup IS NOT NULL, i_ISOBusinessOwnersLiabilityClassGroup,
	    'N/A'
	) AS o_ISOBusinessOwnersLiabilityClassGroup,
	i_ISOOccupancyType AS o_ISOOccupancyType,
	-- *INF*: IIF(i_PredominantBuildingBCCCode != 'N/A',LPAD(i_PredominantBuildingBCCCode,5,'0'),i_PredominantBuildingBCCCode)
	-- 
	-- -- pad a 4 char code to 5 with a leading zero if valid, else N/A
	IFF(
	    i_PredominantBuildingBCCCode != 'N/A', LPAD(i_PredominantBuildingBCCCode, 5, '0'),
	    i_PredominantBuildingBCCCode
	) AS o_PredominantBuildingBCCCode_padded
	FROM AGG_DuplicateRemove
),
LKP_CoverageDetailBusinessOwners AS (
	SELECT
	PremiumTransactionID,
	CoverageGuid,
	ISOBusinessOwnersPropertyRateNumber,
	ISOBusinessOwnersLiabilityClassGroup,
	ISOOccupancyType,
	BuildingBCCCode,
	BuildingClassCodeDescription
	FROM (
		SELECT 
			PremiumTransactionID,
			CoverageGuid,
			ISOBusinessOwnersPropertyRateNumber,
			ISOBusinessOwnersLiabilityClassGroup,
			ISOOccupancyType,
			BuildingBCCCode,
			BuildingClassCodeDescription
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailBusinessOwners
		WHERE SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID) = 1
),
LKP_Sup_Bus_Class_Code AS (
	SELECT
	StandardBusinessClassCode,
	StandardBusinessClassCodeDescription,
	bus_class_code
	FROM (
		SELECT 
			StandardBusinessClassCode,
			StandardBusinessClassCodeDescription,
			bus_class_code
		FROM sup_business_classification_code
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY bus_class_code ORDER BY StandardBusinessClassCode) = 1
),
EXP_Metadata1 AS (
	SELECT
	LKP_CoverageDetailBusinessOwners.PremiumTransactionID AS lkp_PremiumTransactionID,
	LKP_CoverageDetailBusinessOwners.CoverageGuid AS lkp_CoverageGuid,
	LKP_CoverageDetailBusinessOwners.ISOBusinessOwnersPropertyRateNumber AS lkp_ISOBusinessOwnersPropertyRateNumber,
	LKP_CoverageDetailBusinessOwners.ISOBusinessOwnersLiabilityClassGroup AS lkp_ISOBusinessOwnersLiabilityClassGroup,
	LKP_CoverageDetailBusinessOwners.ISOOccupancyType AS lkp_ISOOccupancyType,
	LKP_CoverageDetailBusinessOwners.BuildingBCCCode AS lkp_BuildingBCCCode,
	LKP_CoverageDetailBusinessOwners.BuildingClassCodeDescription AS lkp_BuildingClassCodeDescription,
	LKP_Sup_Bus_Class_Code.StandardBusinessClassCode AS lkp_sup_StandardBusinessClassCode,
	LKP_Sup_Bus_Class_Code.StandardBusinessClassCodeDescription AS lkp_sup_StandardBusinessClassCodeDescription,
	EXP_Metadata.o_PremiumTransactionID AS i_PremiumTransactionID,
	EXP_Metadata.o_CoverageGUID AS i_CoverageGUID,
	EXP_Metadata.o_ISOBusinessOwnersPropertyRateNumber AS i_ISOBusinessOwnersPropertyRateNumber,
	EXP_Metadata.o_ISOBusinessOwnersLiabilityClassGroup AS i_ISOBusinessOwnersLiabilityClassGroup,
	EXP_Metadata.o_ISOOccupancyType AS i_ISOOccupancyType,
	-- *INF*: RTRIM(LTRIM(lkp_CoverageGuid))
	RTRIM(LTRIM(lkp_CoverageGuid)) AS v_lkp_CoverageGuid,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.US')
	TO_TIMESTAMP('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.US') AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.US')
	TO_TIMESTAMP('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.US') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_CoverageGUID AS o_CoverageGUID,
	i_ISOBusinessOwnersPropertyRateNumber AS o_ISOBusinessOwnersPropertyRateNumber,
	i_ISOBusinessOwnersLiabilityClassGroup AS o_ISOBusinessOwnersLiabilityClassGroup,
	i_ISOOccupancyType AS o_ISOOccupancyType,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(lkp_sup_StandardBusinessClassCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(lkp_sup_StandardBusinessClassCode) AS o_StandardBusinessClassCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(lkp_sup_StandardBusinessClassCodeDescription)
	UDF_DEFAULT_VALUE_FOR_STRINGS(lkp_sup_StandardBusinessClassCodeDescription) AS o_StandardBusinessClassCodeDescription,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_PremiumTransactionID),'NEW',
	-- v_lkp_CoverageGuid != i_CoverageGUID
	-- OR lkp_ISOBusinessOwnersPropertyRateNumber != i_ISOBusinessOwnersPropertyRateNumber 
	-- OR lkp_ISOBusinessOwnersLiabilityClassGroup !=i_ISOBusinessOwnersLiabilityClassGroup 
	-- OR lkp_ISOOccupancyType != i_ISOOccupancyType
	-- OR lkp_BuildingBCCCode != lkp_sup_StandardBusinessClassCode
	-- OR lkp_BuildingClassCodeDescription != lkp_sup_StandardBusinessClassCodeDescription
	-- ,'UPDATE',
	-- 'NOCHANGE'
	-- )
	DECODE(
	    TRUE,
	    lkp_PremiumTransactionID IS NULL, 'NEW',
	    v_lkp_CoverageGuid != i_CoverageGUID OR lkp_ISOBusinessOwnersPropertyRateNumber != i_ISOBusinessOwnersPropertyRateNumber OR lkp_ISOBusinessOwnersLiabilityClassGroup != i_ISOBusinessOwnersLiabilityClassGroup OR lkp_ISOOccupancyType != i_ISOOccupancyType OR lkp_BuildingBCCCode != lkp_sup_StandardBusinessClassCode OR lkp_BuildingClassCodeDescription != lkp_sup_StandardBusinessClassCodeDescription, 'UPDATE',
	    'NOCHANGE'
	) AS o_ChangeFlag
	FROM EXP_Metadata
	LEFT JOIN LKP_CoverageDetailBusinessOwners
	ON LKP_CoverageDetailBusinessOwners.PremiumTransactionID = EXP_Metadata.o_PremiumTransactionID
	LEFT JOIN LKP_Sup_Bus_Class_Code
	ON LKP_Sup_Bus_Class_Code.bus_class_code = EXP_Metadata.o_PredominantBuildingBCCCode_padded
),
RTR_INSERT_UPDATE AS (
	SELECT
	o_PremiumTransactionID AS PremiumTransactionID,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditID AS AuditID,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_CoverageGUID AS CoverageGuid,
	o_ISOBusinessOwnersPropertyRateNumber AS ISOBusinessOwnersPropertyRateNumber,
	o_ISOBusinessOwnersLiabilityClassGroup AS ISOBusinessOwnersLiabilityClassGroup,
	o_ISOOccupancyType AS ISOOccupancyType,
	o_StandardBusinessClassCode AS StandardBusinessClassCode,
	o_StandardBusinessClassCodeDescription AS StandardBusinessClassCodeDescription,
	o_ChangeFlag AS ChangeFlag
	FROM EXP_Metadata1
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE ChangeFlag='NEW'),
TGT_CoverageDetailBusinessOwners_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailBusinessOwners
	(PremiumTransactionID, CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CoverageGuid, ISOBusinessOwnersPropertyRateNumber, ISOBusinessOwnersLiabilityClassGroup, ISOOccupancyType, BuildingBCCCode, BuildingClassCodeDescription)
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
	ISOBUSINESSOWNERSPROPERTYRATENUMBER, 
	ISOBUSINESSOWNERSLIABILITYCLASSGROUP, 
	ISOOCCUPANCYTYPE, 
	StandardBusinessClassCode AS BUILDINGBCCCODE, 
	StandardBusinessClassCodeDescription AS BUILDINGCLASSCODEDESCRIPTION
	FROM RTR_INSERT_UPDATE_INSERT
),
SQ_CoverageDetailBusinessOwners AS (
	SELECT
	CDBOPrevious.ISOBusinessOwnersPropertyRateNumber,  
	CDBOPrevious.ISOBusinessOwnersLiabilityClassGroup, 
	CDBOPrevious.ISOOccupancyType, 
	WPTOL.PremiumTransactionID AS Wrk_PremiumTransactionID,
	CDBOPrevious.BuildingBCCCode AS BuildingBCCCode,
	CDBOPrevious.BuildingClassCodeDescription AS BuildingClassCodeDescription
	FROM
	WorkPremiumTransactionOffsetLineage WPTOL 
	inner join CoverageDetailBusinessOwners CDBOPrevious on ( CDBOPrevious.PremiumTransactionID= WPTOL.previouspremiumtransactionid)
	inner join CoverageDetailBusinessOwners CDBOToUpdate on ( CDBOToUpdate.PremiumTransactionID= WPTOL.PremiumTransactionid)
	INNER JOIN premiumtransaction pt WITH (NOLOCK) on
	WPTOL .premiumtransactionID=pt.premiumtransactionID and PT.OffsetOnsetCode='Offset'
	WHERE
	WPTOL.UpdateAttributeFlag = 1 
	AND (
	CDBOPrevious.ISOBusinessOwnersLiabilityClassGroup <> CDBOToUpdate.ISOBusinessOwnersLiabilityClassGroup
	  OR CDBOPrevious.ISOBusinessOwnersPropertyRateNumber <> CDBOToUpdate.ISOBusinessOwnersPropertyRateNumber
	  OR CDBOPrevious.ISOOccupancyType <> CDBOToUpdate.ISOOccupancyType
	  OR CDBOPrevious.BuildingBCCCode <>  CDBOToUpdate.BuildingBCCCode
	  OR CDBOPrevious.BuildingClassCodeDescription <> CDBOToUpdate.BuildingClassCodeDescription
	  )
),
EXP_Coveragedetailbussinessowners AS (
	SELECT
	ISOBusinessOwnersPropertyRateNumber,
	ISOBusinessOwnersLiabilityClassGroup,
	ISOOccupancyType,
	wrk_PremiumTransactionID,
	SYSDATE AS o_ModifiedDate,
	BuildingBCCCode,
	BuildingClassCodeDescription
	FROM SQ_CoverageDetailBusinessOwners
),
UPD_Coveragedetailbussinessowners AS (
	SELECT
	ISOBusinessOwnersPropertyRateNumber, 
	ISOBusinessOwnersLiabilityClassGroup, 
	ISOOccupancyType, 
	wrk_PremiumTransactionID, 
	o_ModifiedDate AS ModifiedDate, 
	BuildingBCCCode, 
	BuildingClassCodeDescription
	FROM EXP_Coveragedetailbussinessowners
),
TGT_CoverageDetailBusinessOwners_Offsets AS (
	MERGE INTO CoverageDetailBusinessOwners AS T
	USING UPD_Coveragedetailbussinessowners AS S
	ON T.PremiumTransactionID = S.wrk_PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.ISOBusinessOwnersPropertyRateNumber = S.ISOBusinessOwnersPropertyRateNumber, T.ISOBusinessOwnersLiabilityClassGroup = S.ISOBusinessOwnersLiabilityClassGroup, T.ISOOccupancyType = S.ISOOccupancyType, T.BuildingBCCCode = S.BuildingBCCCode, T.BuildingClassCodeDescription = S.BuildingClassCodeDescription
),
SQ_CoverageDetailBusinessOwners_Deprecated AS (
	SELECT
	CDBOPrevious.ISOBusinessOwnersPropertyRateNumber,  
	CDBOPrevious.ISOBusinessOwnersLiabilityClassGroup, 
	CDBOPrevious.ISOOccupancyType, 
	WPTOL.PremiumTransactionID AS Wrk_PremiumTransactionID,
	CDBOPrevious.BuildingBCCCode AS BuildingBCCCode,
	CDBOPrevious.BuildingClassCodeDescription AS BuildingClassCodeDescription
	FROM
	WorkPremiumTransactionOffsetLineage WPTOL 
	inner join CoverageDetailBusinessOwners CDBOPrevious on ( CDBOPrevious.PremiumTransactionID= WPTOL.previouspremiumtransactionid)
	inner join CoverageDetailBusinessOwners CDBOToUpdate on ( CDBOToUpdate.PremiumTransactionID= WPTOL.PremiumTransactionid)
	INNER JOIN premiumtransaction pt WITH (NOLOCK) on
	WPTOL .premiumtransactionID=pt.premiumtransactionID and PT.OffsetOnsetCode='Deprecated'
	WHERE
	WPTOL.UpdateAttributeFlag = 1 
	AND (
	CDBOPrevious.ISOBusinessOwnersLiabilityClassGroup <> CDBOToUpdate.ISOBusinessOwnersLiabilityClassGroup
	  OR CDBOPrevious.ISOBusinessOwnersPropertyRateNumber <> CDBOToUpdate.ISOBusinessOwnersPropertyRateNumber
	  OR CDBOPrevious.ISOOccupancyType <> CDBOToUpdate.ISOOccupancyType
	  OR CDBOPrevious.BuildingBCCCode <>  CDBOToUpdate.BuildingBCCCode
	  OR CDBOPrevious.BuildingClassCodeDescription <> CDBOToUpdate.BuildingClassCodeDescription
	  )
),
EXP_Coveragedetailbussinessowners_Deprecated AS (
	SELECT
	ISOBusinessOwnersPropertyRateNumber,
	ISOBusinessOwnersLiabilityClassGroup,
	ISOOccupancyType,
	wrk_PremiumTransactionID,
	SYSDATE AS o_ModifiedDate,
	BuildingBCCCode,
	BuildingClassCodeDescription
	FROM SQ_CoverageDetailBusinessOwners_Deprecated
),
UPD_Coveragedetailbussinessowners_Deprecated AS (
	SELECT
	ISOBusinessOwnersPropertyRateNumber, 
	ISOBusinessOwnersLiabilityClassGroup, 
	ISOOccupancyType, 
	wrk_PremiumTransactionID, 
	o_ModifiedDate AS ModifiedDate, 
	BuildingBCCCode, 
	BuildingClassCodeDescription
	FROM EXP_Coveragedetailbussinessowners_Deprecated
),
TGT_CoverageDetailBusinessOwners_Deprecated AS (
	MERGE INTO CoverageDetailBusinessOwners AS T
	USING UPD_Coveragedetailbussinessowners_Deprecated AS S
	ON T.PremiumTransactionID = S.wrk_PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.ISOBusinessOwnersPropertyRateNumber = S.ISOBusinessOwnersPropertyRateNumber, T.ISOBusinessOwnersLiabilityClassGroup = S.ISOBusinessOwnersLiabilityClassGroup, T.ISOOccupancyType = S.ISOOccupancyType, T.BuildingBCCCode = S.BuildingBCCCode, T.BuildingClassCodeDescription = S.BuildingClassCodeDescription
),