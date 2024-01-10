WITH
LKP_SupISOCommercialPropertyCauseOfLossGroup_DCT AS (
	SELECT
	ISOCommercialPropertyCauseOfLossGroup,
	ProductCode,
	SublineCode
	FROM (
		SELECT 
			ISOCommercialPropertyCauseOfLossGroup,
			ProductCode,
			SublineCode
		FROM SupISOCommercialPropertyCauseOfLossGroup
		WHERE CurrentSnapshotFlag=1 and SourceSystemID='DCT'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProductCode,SublineCode ORDER BY ISOCommercialPropertyCauseOfLossGroup) = 1
),
SQ_DCTWorkTables AS (
	SELECT
		WorkDCTTransactionInsuranceLineLocationBridge.CoverageId,
		WorkDCTCoverageTransaction.CoverageGUID,
		WorkDCTTransactionInsuranceLineLocationBridge.MultipleLocationCreditFactor,
		WorkDCTTransactionInsuranceLineLocationBridge.PreferredPropertyCreditFactor,
		WorkDCTTransactionInsuranceLineLocationBridge.ProtectionClass,
		WorkDCTInsuranceLine.LineId,
		WorkDCTTransactionInsuranceLineLocationBridge.SprinkerFlag AS SprinklerFlag,
		WorkDCTTransactionInsuranceLineLocationBridge.CauseOfLoss,
		WorkDCTTransactionInsuranceLineLocationBridge.PerilGroup,
		WorkDCTTransactionInsuranceLineLocationBridge.RateType,
		WorkDCTTransactionInsuranceLineLocationBridge.PropertyType,
		WorkDCTTransactionInsuranceLineLocationBridge.OccupancyCategory,
		WorkDCTInsuranceLine.LineType,
		WorkDCTTransactionInsuranceLineLocationBridge.RetroactiveDate
	FROM WorkDCTTransactionInsuranceLineLocationBridge
	INNER JOIN WorkDCTInsuranceLine
	INNER JOIN WorkDCTCoverageTransaction
	ON WorkDCTCoverageTransaction.CoverageId=WorkDCTTransactionInsuranceLineLocationBridge.CoverageId
	and
	WorkDCTTransactionInsuranceLineLocationBridge.LineId=WorkDCTInsuranceLine.LineId
	and
	WorkDCTInsuranceLine.LineType in ('Property','SBOPProperty','BusinessOwners')
),
SQ_WorkPremiumTransaction AS (
	SELECT PT.Premiumtransactionid, 
	       PT.Currentsnapshotflag, 
	       WPT.PremiumTransactionStageId, 
	       RC.Classcode, 
	       RL.Stateprovincecode, 
	       P.Productcode, 
	       RC.Sublinecode 
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransaction WPT 
	       INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Premiumtransaction PT ON PT.PremiumTransactionAKID = WPT.PremiumTransactionAKID 
	       INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Ratingcoverage RC ON RC.RatingCoverageAKID = PT.RatingCoverageAKID AND RC.Effectivedate = PT.Effectivedate 
	       INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Policycoverage PC ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID AND PC.Currentsnapshotflag =1 
		  INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Risklocation RL ON PC.RiskLocationAKID = RL.RiskLocationAKID AND RL.Currentsnapshotflag =1 
	       INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product P ON P.ProductAKID = RC.ProductAKID and P.Currentsnapshotflag =1
	WHERE  PT.Sourcesystemid = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND WPT.Sourcesystemid = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	AND PC.InsuranceLine in ('Property','SBOPProperty','BusinessOwners')
),
JNR_Property AS (SELECT
	SQ_WorkPremiumTransaction.PremiumTransactionID, 
	SQ_WorkPremiumTransaction.CurrentSnapshotFlag, 
	SQ_WorkPremiumTransaction.PremiumTransactionStageId, 
	SQ_WorkPremiumTransaction.ClassCode, 
	SQ_WorkPremiumTransaction.StateProvinceCode, 
	SQ_WorkPremiumTransaction.ProductCode, 
	SQ_WorkPremiumTransaction.SublineCode, 
	SQ_DCTWorkTables.CoverageId, 
	SQ_DCTWorkTables.CoverageGUID, 
	SQ_DCTWorkTables.MultipleLocationCreditFactor, 
	SQ_DCTWorkTables.PreferredPropertyCreditFactor, 
	SQ_DCTWorkTables.ProtectionClass, 
	SQ_DCTWorkTables.SprinklerFlag, 
	SQ_DCTWorkTables.CauseOfLoss, 
	SQ_DCTWorkTables.PerilGroup, 
	SQ_DCTWorkTables.RateType, 
	SQ_DCTWorkTables.PropertyType, 
	SQ_DCTWorkTables.OccupancyCategory AS OccupanyCategory, 
	SQ_DCTWorkTables.LineType, 
	SQ_DCTWorkTables.RetroactiveDate
	FROM SQ_WorkPremiumTransaction
	INNER JOIN SQ_DCTWorkTables
	ON SQ_DCTWorkTables.CoverageId = SQ_WorkPremiumTransaction.PremiumTransactionStageId
),
AGG_Property AS (
	SELECT
	PremiumTransactionID,
	CurrentSnapshotFlag,
	ClassCode,
	StateProvinceCode,
	ProductCode,
	SublineCode,
	CoverageGUID,
	MultipleLocationCreditFactor,
	PreferredPropertyCreditFactor,
	ProtectionClass,
	SprinklerFlag,
	CauseOfLoss,
	PerilGroup,
	RateType,
	PropertyType,
	OccupanyCategory,
	LineType,
	RetroactiveDate
	FROM JNR_Property
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY NULL) = 1
),
EXP_CoverageDetailCommercialProperty AS (
	SELECT
	PremiumTransactionID AS i_PremiumTransactionID,
	CurrentSnapshotFlag AS i_CurrentSnapshotFlag,
	ClassCode AS i_ClassCode,
	StateProvinceCode AS i_StateProvinceCode,
	ProductCode AS i_ProductCode,
	SublineCode AS i_SublineCode,
	CoverageGUID AS i_CoverageGuid,
	MultipleLocationCreditFactor AS i_MultipleLocationCreditFactor,
	PreferredPropertyCreditFactor AS i_PreferredPropertyCredit,
	ProtectionClass AS i_TerritoryProtectionClass,
	SprinklerFlag AS i_SprinklerFlag,
	CauseOfLoss AS i_CauseOfLoss,
	PerilGroup AS i_PerilGroup,
	RateType AS i_RateType,
	PropertyType AS i_PropertyType,
	OccupanyCategory AS i_OccupanyCategory,
	LineType AS i_LineType,
	RetroactiveDate AS i_RetroactiveDate,
	-- *INF*: :LKP.LKP_SupISOCommercialPropertyCauseOfLossGroup_DCT(i_ProductCode,i_SublineCode)
	LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_DCT_i_ProductCode_i_SublineCode.ISOCommercialPropertyCauseOfLossGroup AS v_ISOPropertyCauseofLossGroup,
	-- *INF*: DECODE(TRUE,
	-- v_ISOPropertyCauseofLossGroup='SCL'  AND (i_PropertyType='' OR ISNULL(i_PropertyType)), 'Buildings',
	-- v_ISOPropertyCauseofLossGroup='SCL'  , i_OccupanyCategory,
	-- 'N/A')
	DECODE(TRUE,
		v_ISOPropertyCauseofLossGroup = 'SCL' AND ( i_PropertyType = '' OR i_PropertyType IS NULL ), 'Buildings',
		v_ISOPropertyCauseofLossGroup = 'SCL', i_OccupanyCategory,
		'N/A') AS v_ISOSpecialCauseOfLossCategory,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	-- *INF*: IIF(i_CurrentSnapshotFlag='T',1,0)
	IFF(i_CurrentSnapshotFlag = 'T', 1, 0) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59' , 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_CoverageGuid AS o_CoverageGuid,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_TerritoryProtectionClass)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_TerritoryProtectionClass) AS o_IsoFireProtectionCode,
	-- *INF*: IIF(ISNULL(i_MultipleLocationCreditFactor),0,i_MultipleLocationCreditFactor)
	IFF(i_MultipleLocationCreditFactor IS NULL, 0, i_MultipleLocationCreditFactor) AS o_MultiLocationCreditFactor,
	-- *INF*: IIF(ISNULL(i_PreferredPropertyCredit),0,TO_DECIMAL(i_PreferredPropertyCredit))
	IFF(i_PreferredPropertyCredit IS NULL, 0, TO_DECIMAL(i_PreferredPropertyCredit)) AS o_PreferredPropertyFactor,
	-- *INF*: DECODE(i_SprinklerFlag,'T',1,'F',0,0)
	DECODE(i_SprinklerFlag,
		'T', 1,
		'F', 0,
		0) AS o_SprinklerFlag,
	-- *INF*: iif(isnull(v_ISOPropertyCauseofLossGroup),'N/A',v_ISOPropertyCauseofLossGroup)
	IFF(v_ISOPropertyCauseofLossGroup IS NULL, 'N/A', v_ISOPropertyCauseofLossGroup) AS o_ISOPropertyCauseofLossGroup,
	-- *INF*: IIF(ISNULL(v_ISOSpecialCauseOfLossCategory),'N/A',v_ISOSpecialCauseOfLossCategory)
	IFF(v_ISOSpecialCauseOfLossCategory IS NULL, 'N/A', v_ISOSpecialCauseOfLossCategory) AS o_ISOSpecialCauseOfLossCategory,
	-- *INF*: --LPAD(i_ClassCode,6, '0')
	-- SUBSTR(i_ClassCode,1,4)
	SUBSTR(i_ClassCode, 1, 4) AS o_ClassCode,
	i_StateProvinceCode AS o_StateProvinceCode,
	-- *INF*: DECODE(TRUE,
	-- i_RateType='S', 'Specific',
	-- i_RateType='C', 'Class',
	-- 'N/A')
	DECODE(TRUE,
		i_RateType = 'S', 'Specific',
		i_RateType = 'C', 'Class',
		'N/A') AS o_RateType,
	i_LineType AS o_LineType,
	-- *INF*: IIF(ISNULL(i_RetroactiveDate),
	-- TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),
	-- i_RetroactiveDate
	-- )
	IFF(i_RetroactiveDate IS NULL, TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), i_RetroactiveDate) AS o_RetroactiveDate
	FROM AGG_Property
	LEFT JOIN LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_DCT LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_DCT_i_ProductCode_i_SublineCode
	ON LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_DCT_i_ProductCode_i_SublineCode.ProductCode = i_ProductCode
	AND LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_DCT_i_ProductCode_i_SublineCode.SublineCode = i_SublineCode

),
LKP_ISOSpecialCauseOfLossCategoryCode AS (
	SELECT
	ISOSpecialCauseOfLossCategoryCode,
	ISOSpecialCauseOfLossCategoryDCTCode
	FROM (
		SELECT 
			ISOSpecialCauseOfLossCategoryCode,
			ISOSpecialCauseOfLossCategoryDCTCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupISOSpecialCauseOfLossCategory
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ISOSpecialCauseOfLossCategoryDCTCode ORDER BY ISOSpecialCauseOfLossCategoryCode) = 1
),
LKP_SupClassificationCommercialProperty AS (
	SELECT
	ISOCPRatingGroup,
	CommercialPropertySpecialClass,
	ClassCode,
	RatingStateCode
	FROM (
		SELECT 
			ISOCPRatingGroup,
			CommercialPropertySpecialClass,
			ClassCode,
			RatingStateCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationCommercialProperty
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode,RatingStateCode ORDER BY ISOCPRatingGroup) = 1
),
LKP_SupClassificationCommercialProperty_default AS (
	SELECT
	ISOCPRatingGroup,
	CommercialPropertySpecialClass,
	ClassCode
	FROM (
		SELECT 
			ISOCPRatingGroup,
			CommercialPropertySpecialClass,
			ClassCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationCommercialProperty
		WHERE CurrentSnapshotFlag=1 and RatingStateCode='99'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode ORDER BY ISOCPRatingGroup) = 1
),
EXP_Calc AS (
	SELECT
	EXP_CoverageDetailCommercialProperty.o_PremiumTransactionID AS PremiumTransactionID,
	EXP_CoverageDetailCommercialProperty.o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	EXP_CoverageDetailCommercialProperty.o_AuditID AS AuditID,
	EXP_CoverageDetailCommercialProperty.o_LineType AS i_LineType,
	EXP_CoverageDetailCommercialProperty.o_EffectiveDate AS EffectiveDate,
	EXP_CoverageDetailCommercialProperty.o_ExpirationDate AS ExpirationDate,
	EXP_CoverageDetailCommercialProperty.o_SourceSystemID AS SourceSystemID,
	EXP_CoverageDetailCommercialProperty.o_CreatedDate AS CreatedDate,
	EXP_CoverageDetailCommercialProperty.o_ModifiedDate AS ModifiedDate,
	EXP_CoverageDetailCommercialProperty.o_CoverageGuid AS CoverageGuid,
	EXP_CoverageDetailCommercialProperty.o_IsoFireProtectionCode AS IsoFireProtectionCode,
	EXP_CoverageDetailCommercialProperty.o_MultiLocationCreditFactor AS MultiLocationCreditFactor,
	EXP_CoverageDetailCommercialProperty.o_PreferredPropertyFactor AS PreferredPropertyFactor,
	EXP_CoverageDetailCommercialProperty.o_SprinklerFlag AS SprinklerFlag,
	EXP_CoverageDetailCommercialProperty.o_ISOPropertyCauseofLossGroup AS ISOPropertyCauseofLossGroup,
	LKP_ISOSpecialCauseOfLossCategoryCode.ISOSpecialCauseOfLossCategoryCode AS i_ISOSpecialCauseOfLossCategory,
	EXP_CoverageDetailCommercialProperty.o_RateType AS RateType,
	LKP_SupClassificationCommercialProperty.ISOCPRatingGroup AS lk_ISOCPRatingGroup,
	LKP_SupClassificationCommercialProperty.CommercialPropertySpecialClass AS lk_PropertySpecialClass,
	LKP_SupClassificationCommercialProperty_default.ISOCPRatingGroup AS lk_ISOCPRatingGroup_default,
	LKP_SupClassificationCommercialProperty_default.CommercialPropertySpecialClass AS lk_PropertySpecialClass_default,
	-- *INF*: IIF(ISOPropertyCauseofLossGroup='BGI',
	-- DECODE(TRUE,
	-- not isnull(lk_ISOCPRatingGroup),lk_ISOCPRatingGroup,
	-- not isnull(lk_ISOCPRatingGroup_default),lk_ISOCPRatingGroup_default,
	-- 'N/A'),'N/A')
	IFF(ISOPropertyCauseofLossGroup = 'BGI', DECODE(TRUE,
		NOT lk_ISOCPRatingGroup IS NULL, lk_ISOCPRatingGroup,
		NOT lk_ISOCPRatingGroup_default IS NULL, lk_ISOCPRatingGroup_default,
		'N/A'), 'N/A') AS v_ISOCPRatingGroup,
	-- *INF*: DECODE(TRUE,
	-- not isnull(lk_PropertySpecialClass),lk_PropertySpecialClass,
	-- not isnull(lk_PropertySpecialClass_default),lk_PropertySpecialClass_default,
	-- 'N/A')
	DECODE(TRUE,
		NOT lk_PropertySpecialClass IS NULL, lk_PropertySpecialClass,
		NOT lk_PropertySpecialClass_default IS NULL, lk_PropertySpecialClass_default,
		'N/A') AS v_PropertySpecialClass,
	-- *INF*: IIF(i_LineType='Property' or i_LineType='SBOPProperty',
	-- ISOPropertyCauseofLossGroup,'N/A')
	IFF(i_LineType = 'Property' OR i_LineType = 'SBOPProperty', ISOPropertyCauseofLossGroup, 'N/A') AS o_ISOPropertyCauseofLossGroup,
	-- *INF*: IIF(i_LineType='Property' or i_LineType='SBOPProperty',
	-- iif(isnull(i_ISOSpecialCauseOfLossCategory),'N/A',i_ISOSpecialCauseOfLossCategory),'N/A')
	IFF(i_LineType = 'Property' OR i_LineType = 'SBOPProperty', IFF(i_ISOSpecialCauseOfLossCategory IS NULL, 'N/A', i_ISOSpecialCauseOfLossCategory), 'N/A') AS o_ISOSpecialCauseOfLossCategory,
	-- *INF*: IIF(i_LineType='Property' or i_LineType='SBOPProperty',RateType,'N/A')
	IFF(i_LineType = 'Property' OR i_LineType = 'SBOPProperty', RateType, 'N/A') AS o_RateType,
	-- *INF*: IIF(i_LineType='Property' or i_LineType='SBOPProperty',v_ISOCPRatingGroup,'N/A')
	IFF(i_LineType = 'Property' OR i_LineType = 'SBOPProperty', v_ISOCPRatingGroup, 'N/A') AS o_ISOCPRatingGroup,
	-- *INF*: IIF(i_LineType='Property' or i_LineType='SBOPProperty',v_PropertySpecialClass,'N/A')
	IFF(i_LineType = 'Property' OR i_LineType = 'SBOPProperty', v_PropertySpecialClass, 'N/A') AS o_PropertySpecialClass,
	EXP_CoverageDetailCommercialProperty.o_RetroactiveDate
	FROM EXP_CoverageDetailCommercialProperty
	LEFT JOIN LKP_ISOSpecialCauseOfLossCategoryCode
	ON LKP_ISOSpecialCauseOfLossCategoryCode.ISOSpecialCauseOfLossCategoryDCTCode = EXP_CoverageDetailCommercialProperty.o_ISOSpecialCauseOfLossCategory
	LEFT JOIN LKP_SupClassificationCommercialProperty
	ON LKP_SupClassificationCommercialProperty.ClassCode = EXP_CoverageDetailCommercialProperty.o_ClassCode AND LKP_SupClassificationCommercialProperty.RatingStateCode = EXP_CoverageDetailCommercialProperty.o_StateProvinceCode
	LEFT JOIN LKP_SupClassificationCommercialProperty_default
	ON LKP_SupClassificationCommercialProperty_default.ClassCode = EXP_CoverageDetailCommercialProperty.o_ClassCode
),
LKP_CoverageDetailCommercialProperty AS (
	SELECT
	PremiumTransactionId,
	IsoFireProtectionCode,
	MultiLocationCreditFactor,
	PreferredPropertyFactor,
	SprinklerFlag,
	ISOCommercialPropertyCauseofLossGroup,
	ISOCommercialPropertyRatingGroupCode,
	ISOSpecialCauseOfLossCategoryCode,
	RateType,
	CommercialPropertySpecialClass
	FROM (
		SELECT 
			PremiumTransactionId,
			IsoFireProtectionCode,
			MultiLocationCreditFactor,
			PreferredPropertyFactor,
			SprinklerFlag,
			ISOCommercialPropertyCauseofLossGroup,
			ISOCommercialPropertyRatingGroupCode,
			ISOSpecialCauseOfLossCategoryCode,
			RateType,
			CommercialPropertySpecialClass
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialProperty
		WHERE PremiumTransactionID  in (select pt.PremiumTransactionID from
		PremiumTransaction pt
		inner join WorkPremiumTransaction wpt
		on pt.PremiumTransactionAKID=wpt.PremiumTransactionAKId)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionId ORDER BY PremiumTransactionId) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_CoverageDetailCommercialProperty.PremiumTransactionId AS lkp_PremiumTransactionId,
	LKP_CoverageDetailCommercialProperty.IsoFireProtectionCode AS lkp_IsoFireProtectionCode,
	LKP_CoverageDetailCommercialProperty.MultiLocationCreditFactor AS lkp_MultiLocationCreditFactor,
	LKP_CoverageDetailCommercialProperty.PreferredPropertyFactor AS lkp_PreferredPropertyFactor,
	LKP_CoverageDetailCommercialProperty.SprinklerFlag AS lkp_SprinklerFlag_origin,
	-- *INF*: DECODE(lkp_SprinklerFlag_origin,'T',1,'F',0,NULL)
	DECODE(lkp_SprinklerFlag_origin,
		'T', 1,
		'F', 0,
		NULL) AS lkp_SprinklerFlag,
	LKP_CoverageDetailCommercialProperty.ISOCommercialPropertyCauseofLossGroup AS lkp_ISOPropertyCauseofLossGroup,
	LKP_CoverageDetailCommercialProperty.ISOCommercialPropertyRatingGroupCode AS lkp_ISOCPRatingGroup,
	LKP_CoverageDetailCommercialProperty.ISOSpecialCauseOfLossCategoryCode AS lkp_ISOSpecialCauseOfLossCategory,
	LKP_CoverageDetailCommercialProperty.RateType AS lkp_RateType,
	LKP_CoverageDetailCommercialProperty.CommercialPropertySpecialClass AS lkp_PropertySpecialClass,
	EXP_Calc.PremiumTransactionID,
	EXP_Calc.CurrentSnapshotFlag,
	EXP_Calc.AuditID,
	EXP_Calc.EffectiveDate,
	EXP_Calc.ExpirationDate,
	EXP_Calc.SourceSystemID,
	EXP_Calc.CreatedDate,
	EXP_Calc.ModifiedDate,
	EXP_Calc.CoverageGuid,
	EXP_Calc.IsoFireProtectionCode,
	EXP_Calc.MultiLocationCreditFactor,
	EXP_Calc.PreferredPropertyFactor,
	EXP_Calc.SprinklerFlag,
	EXP_Calc.o_ISOPropertyCauseofLossGroup AS ISOPropertyCauseofLossGroup,
	EXP_Calc.o_ISOSpecialCauseOfLossCategory AS ISOSpecialCauseOfLossCategory,
	EXP_Calc.o_RateType AS RateType,
	EXP_Calc.o_ISOCPRatingGroup AS ISOCPRatingGroup,
	EXP_Calc.o_PropertySpecialClass AS PropertySpecialClass,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_PremiumTransactionId),'INSERT',
	--  lkp_IsoFireProtectionCode<>IsoFireProtectionCode 
	-- OR lkp_MultiLocationCreditFactor<>MultiLocationCreditFactor 
	-- OR lkp_PreferredPropertyFactor<>PreferredPropertyFactor 
	-- OR lkp_SprinklerFlag<>SprinklerFlag
	-- OR lkp_ISOPropertyCauseofLossGroup<>ISOPropertyCauseofLossGroup 
	-- OR lkp_ISOCPRatingGroup<>ISOCPRatingGroup 
	-- OR lkp_ISOSpecialCauseOfLossCategory<>ISOSpecialCauseOfLossCategory 
	-- OR lkp_RateType<>RateType
	-- OR lkp_PropertySpecialClass<>PropertySpecialClass
	-- 
	-- , 'UPDATE',
	-- 'NO CHANGE'
	-- )
	DECODE(TRUE,
		lkp_PremiumTransactionId IS NULL, 'INSERT',
		lkp_IsoFireProtectionCode <> IsoFireProtectionCode OR lkp_MultiLocationCreditFactor <> MultiLocationCreditFactor OR lkp_PreferredPropertyFactor <> PreferredPropertyFactor OR lkp_SprinklerFlag <> SprinklerFlag OR lkp_ISOPropertyCauseofLossGroup <> ISOPropertyCauseofLossGroup OR lkp_ISOCPRatingGroup <> ISOCPRatingGroup OR lkp_ISOSpecialCauseOfLossCategory <> ISOSpecialCauseOfLossCategory OR lkp_RateType <> RateType OR lkp_PropertySpecialClass <> PropertySpecialClass, 'UPDATE',
		'NO CHANGE') AS o_ChangeFlag,
	EXP_Calc.o_RetroactiveDate AS RetroactiveDate
	FROM EXP_Calc
	LEFT JOIN LKP_CoverageDetailCommercialProperty
	ON LKP_CoverageDetailCommercialProperty.PremiumTransactionId = EXP_Calc.PremiumTransactionID
),
FILT_Records AS (
	SELECT
	PremiumTransactionID, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	CoverageGuid, 
	IsoFireProtectionCode, 
	MultiLocationCreditFactor, 
	PreferredPropertyFactor, 
	SprinklerFlag, 
	ISOPropertyCauseofLossGroup, 
	ISOSpecialCauseOfLossCategory, 
	RateType, 
	ISOCPRatingGroup, 
	PropertySpecialClass, 
	RetroactiveDate, 
	o_ChangeFlag AS ChangeFlag
	FROM EXP_DetectChanges
	WHERE ChangeFlag='INSERT'
),
CoverageDetailCommercialProperty_Insert AS (
	INSERT INTO CoverageDetailCommercialProperty
	(PremiumTransactionID, CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CoverageGuid, IsoFireProtectionCode, MultiLocationCreditFactor, PreferredPropertyFactor, SprinklerFlag, RetroactiveDate, ISOCommercialPropertyCauseofLossGroup, ISOCommercialPropertyRatingGroupCode, ISOSpecialCauseOfLossCategoryCode, RateType, CommercialPropertySpecialClass)
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
	ISOFIREPROTECTIONCODE, 
	MULTILOCATIONCREDITFACTOR, 
	PREFERREDPROPERTYFACTOR, 
	SPRINKLERFLAG, 
	RETROACTIVEDATE, 
	ISOPropertyCauseofLossGroup AS ISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP, 
	ISOCPRatingGroup AS ISOCOMMERCIALPROPERTYRATINGGROUPCODE, 
	ISOSpecialCauseOfLossCategory AS ISOSPECIALCAUSEOFLOSSCATEGORYCODE, 
	RATETYPE, 
	PropertySpecialClass AS COMMERCIALPROPERTYSPECIALCLASS
	FROM FILT_Records
),
SQ_CoverageDetailCommercialProperty AS (
	SELECT 
	CDCPPrevious.IsoFireProtectionCode, 
	CDCPPrevious.MultiLocationCreditFactor, 
	CDCPPrevious.PreferredPropertyFactor, 
	CDCPPrevious.SprinklerFlag,
	CDCPPrevious.RetroactiveDate,
	CDCPPrevious.ISOCommercialPropertyCauseofLossGroup,
	CDCPPrevious.ISOCommercialPropertyRatingGroupCode,
	CDCPPrevious.ISOSpecialCauseOfLossCategoryCode,   
	CDCPPrevious.RateType, 
	CDCPPrevious.CommercialPropertySpecialClass, 
	WPTOL.PremiumTransactionID AS Wrk_PremiumTransactionID
	FROM
	WorkPremiumTransactionOffsetLineage WPTOL inner join CoverageDetailCommercialProperty CDCPPrevious
	on ( CDCPPrevious.PremiumTransactionID= WPTOL.previouspremiumtransactionid)
	inner join CoverageDetailCommercialProperty CDCPToUpdate
	on ( CDCPToUpdate.PremiumTransactionID= WPTOL.PremiumTransactionid)
	INNER JOIN premiumtransaction pt WITH (NOLOCK) on
		WPTOL .premiumtransactionid=pt.premiumtransactionid and PT.OffsetOnsetCode='Offset'
	WHERE
	WPTOL.UpdateAttributeFlag = 1 
	AND (
	  CDCPPrevious.CommercialPropertySpecialClass <> CDCPToUpdate.CommercialPropertySpecialClass
	  OR CDCPPrevious.ISOCommercialPropertyCauseofLossGroup <> CDCPToUpdate.ISOCommercialPropertyCauseofLossGroup
	  OR CDCPPrevious.ISOCommercialPropertyRatingGroupCode <> CDCPToUpdate.ISOCommercialPropertyRatingGroupCode
	  OR CDCPPrevious.IsoFireProtectionCode <> CDCPToUpdate.IsoFireProtectionCode
	  OR CDCPPrevious.ISOSpecialCauseOfLossCategoryCode <> CDCPToUpdate.ISOSpecialCauseOfLossCategoryCode
	  OR CDCPPrevious.MultiLocationCreditFactor <> CDCPToUpdate.MultiLocationCreditFactor
	  OR CDCPPrevious.PreferredPropertyFactor <> CDCPToUpdate.PreferredPropertyFactor
	  OR CDCPPrevious.RateType <> CDCPToUpdate.RateType
	  OR CDCPPrevious.SprinklerFlag <> CDCPToUpdate.SprinklerFlag
	  )
),
Exp_CoverageDetailCommercialProperty_Upd_Offset AS (
	SELECT
	IsoFireProtectionCode,
	MultiLocationCreditFactor,
	PreferredPropertyFactor,
	SprinklerFlag,
	RetroactiveDate,
	ISOCommercialPropertyCauseofLossGroup,
	ISOCommercialPropertyRatingGroupCode,
	ISOSpecialCauseOfLossCategoryCode,
	RateType,
	CommercialPropertySpecialClass,
	Wrk_PremiumTransactionID,
	SYSDATE AS o_ModifiedDate
	FROM SQ_CoverageDetailCommercialProperty
),
UPD_CoverageDetailCommercialProperty AS (
	SELECT
	IsoFireProtectionCode, 
	MultiLocationCreditFactor, 
	PreferredPropertyFactor, 
	SprinklerFlag, 
	RetroactiveDate, 
	ISOCommercialPropertyCauseofLossGroup, 
	ISOCommercialPropertyRatingGroupCode, 
	ISOSpecialCauseOfLossCategoryCode, 
	RateType, 
	CommercialPropertySpecialClass, 
	Wrk_PremiumTransactionID, 
	o_ModifiedDate AS ModifiedDate
	FROM Exp_CoverageDetailCommercialProperty_Upd_Offset
),
TGT_CoverageDetailCommercialProperty_Upd_Offset AS (
	MERGE INTO CoverageDetailCommercialProperty AS T
	USING UPD_CoverageDetailCommercialProperty AS S
	ON T.PremiumTransactionID = S.Wrk_PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.IsoFireProtectionCode = S.IsoFireProtectionCode, T.MultiLocationCreditFactor = S.MultiLocationCreditFactor, T.PreferredPropertyFactor = S.PreferredPropertyFactor, T.SprinklerFlag = S.SprinklerFlag, T.RetroactiveDate = S.RetroactiveDate, T.ISOCommercialPropertyCauseofLossGroup = S.ISOCommercialPropertyCauseofLossGroup, T.ISOCommercialPropertyRatingGroupCode = S.ISOCommercialPropertyRatingGroupCode, T.ISOSpecialCauseOfLossCategoryCode = S.ISOSpecialCauseOfLossCategoryCode, T.RateType = S.RateType, T.CommercialPropertySpecialClass = S.CommercialPropertySpecialClass
),
SQ_CoverageDetailCommercialProperty_Deprecated AS (
	SELECT 
	CDCPPrevious.IsoFireProtectionCode, 
	CDCPPrevious.MultiLocationCreditFactor, 
	CDCPPrevious.PreferredPropertyFactor, 
	CDCPPrevious.SprinklerFlag,
	CDCPPrevious.RetroactiveDate,
	CDCPPrevious.ISOCommercialPropertyCauseofLossGroup,
	CDCPPrevious.ISOCommercialPropertyRatingGroupCode,
	CDCPPrevious.ISOSpecialCauseOfLossCategoryCode,   
	CDCPPrevious.RateType, 
	CDCPPrevious.CommercialPropertySpecialClass, 
	WPTOL.PremiumTransactionID AS Wrk_PremiumTransactionID
	FROM
	WorkPremiumTransactionOffsetLineage WPTOL inner join CoverageDetailCommercialProperty CDCPPrevious
	on ( CDCPPrevious.PremiumTransactionID= WPTOL.previouspremiumtransactionid)
	inner join CoverageDetailCommercialProperty CDCPToUpdate
	on ( CDCPToUpdate.PremiumTransactionID= WPTOL.PremiumTransactionid)
	INNER JOIN premiumtransaction pt WITH (NOLOCK) on
		WPTOL .premiumtransactionid=pt.premiumtransactionid and PT.OffsetOnsetCode='Deprecated'
	WHERE
	WPTOL.UpdateAttributeFlag = 1 
	AND (
	  CDCPPrevious.CommercialPropertySpecialClass <> CDCPToUpdate.CommercialPropertySpecialClass
	  OR CDCPPrevious.ISOCommercialPropertyCauseofLossGroup <> CDCPToUpdate.ISOCommercialPropertyCauseofLossGroup
	  OR CDCPPrevious.ISOCommercialPropertyRatingGroupCode <> CDCPToUpdate.ISOCommercialPropertyRatingGroupCode
	  OR CDCPPrevious.IsoFireProtectionCode <> CDCPToUpdate.IsoFireProtectionCode
	  OR CDCPPrevious.ISOSpecialCauseOfLossCategoryCode <> CDCPToUpdate.ISOSpecialCauseOfLossCategoryCode
	  OR CDCPPrevious.MultiLocationCreditFactor <> CDCPToUpdate.MultiLocationCreditFactor
	  OR CDCPPrevious.PreferredPropertyFactor <> CDCPToUpdate.PreferredPropertyFactor
	  OR CDCPPrevious.RateType <> CDCPToUpdate.RateType
	  OR CDCPPrevious.SprinklerFlag <> CDCPToUpdate.SprinklerFlag
	  )
),
Exp_CoverageDetailCommercialProperty_Upd_Deprecated AS (
	SELECT
	IsoFireProtectionCode,
	MultiLocationCreditFactor,
	PreferredPropertyFactor,
	SprinklerFlag,
	RetroactiveDate,
	ISOCommercialPropertyCauseofLossGroup,
	ISOCommercialPropertyRatingGroupCode,
	ISOSpecialCauseOfLossCategoryCode,
	RateType,
	CommercialPropertySpecialClass,
	Wrk_PremiumTransactionID,
	SYSDATE AS o_ModifiedDate
	FROM SQ_CoverageDetailCommercialProperty_Deprecated
),
UPD_CoverageDetailCommercialProperty_Deprecated AS (
	SELECT
	IsoFireProtectionCode, 
	MultiLocationCreditFactor, 
	PreferredPropertyFactor, 
	SprinklerFlag, 
	RetroactiveDate, 
	ISOCommercialPropertyCauseofLossGroup, 
	ISOCommercialPropertyRatingGroupCode, 
	ISOSpecialCauseOfLossCategoryCode, 
	RateType, 
	CommercialPropertySpecialClass, 
	Wrk_PremiumTransactionID, 
	o_ModifiedDate AS ModifiedDate
	FROM Exp_CoverageDetailCommercialProperty_Upd_Deprecated
),
TGT_CoverageDetailCommercialProperty_Upd_Deprecated AS (
	MERGE INTO CoverageDetailCommercialProperty AS T
	USING UPD_CoverageDetailCommercialProperty_Deprecated AS S
	ON T.PremiumTransactionID = S.Wrk_PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.IsoFireProtectionCode = S.IsoFireProtectionCode, T.MultiLocationCreditFactor = S.MultiLocationCreditFactor, T.PreferredPropertyFactor = S.PreferredPropertyFactor, T.SprinklerFlag = S.SprinklerFlag, T.RetroactiveDate = S.RetroactiveDate, T.ISOCommercialPropertyCauseofLossGroup = S.ISOCommercialPropertyCauseofLossGroup, T.ISOCommercialPropertyRatingGroupCode = S.ISOCommercialPropertyRatingGroupCode, T.ISOSpecialCauseOfLossCategoryCode = S.ISOSpecialCauseOfLossCategoryCode, T.RateType = S.RateType, T.CommercialPropertySpecialClass = S.CommercialPropertySpecialClass
),