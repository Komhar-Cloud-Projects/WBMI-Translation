WITH
LKP_SupISOSpecialCauseOfLossCategoryRule_PMS AS (
	SELECT
	ISOSpecialCauseOfLossCategoryCode,
	ClassCode
	FROM (
		SELECT 
			ISOSpecialCauseOfLossCategoryCode,
			ClassCode
		FROM SupISOSpecialCauseOfLossCategoryRule
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode ORDER BY ISOSpecialCauseOfLossCategoryCode) = 1
),
LKP_SupISOCommercialPropertyCauseOfLossGroup_PMS AS (
	SELECT
	ISOCommercialPropertyCauseOfLossGroup,
	ProductCode,
	MajorPerilCode
	FROM (
		SELECT 
			ISOCommercialPropertyCauseOfLossGroup,
			ProductCode,
			MajorPerilCode
		FROM SupISOCommercialPropertyCauseOfLossGroup
		WHERE CurrentSnapshotFlag=1 and SourceSystemID='PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProductCode,MajorPerilCode ORDER BY ISOCommercialPropertyCauseOfLossGroup) = 1
),
SQ_CoverageDetailCommericalPropery AS (
	SELECT distinct  pt.PremiumTransactionId,
	pt.CurrentSnapshotFlag,
	sc.CoverageGuid,
	pif_43.Pmdnxp1InsuranceLine,
	pif_43.Pmdnxp1OtherMod,
	pif_43.Pmdnxp1ProtectionClassPart1,
	pif_43.Pmdnxp1SprinklerType,
	sc.MajorPerilCode,
	sc.ClassCode,
	sc.InsuranceReferenceLineOfBusinessAKId,
	bsc.BureauCode2,
	sc.SubLineCode,
	bsc.BureauCode1,
	rl.StateProvinceCode as StateProvinceCode,
	product.ProductCode,
	pif_4514.sar_code_2 ,
	pif_4514.sar_code_4
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_4514_stage pif_4514
	JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction work
	ON pif_4514.pif_4514_stage_id=work.PremiumTransactionStageID
	AND pif_4514.sar_insurance_line='CF'
	JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction pt
	ON work.PremiumTransactionAKId=pt.PremiumTransactionAKID
	AND pt.SourceSystemId='PMS'
	JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage sc
	ON pt.StatisticalCoverageAKID=sc.StatisticalCoverageAKID
	JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.Product product
	ON product.ProductAKId=sc.ProductAKId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43NXCPStage pif_43
	ON pif_43.PifSymbol=pif_4514.pif_symbol
	and pif_43.PifPolicyNumber=pif_4514.pif_policy_number
	and pif_43.PifModule=pif_4514.pif_module
	and pif_43.Pmdnxp1InsuranceLine=pif_4514.sar_insurance_line
	and pif_43.Pmdnxp1LocationNumber=case when LEN(ltrim(rtrim(sar_location_x)))=0
	then 0 when patindex('%[^0-9]%',sar_location_x)=0
	then convert(numeric(4,0),sar_location_x) else -1 end
	and pif_43.Pmdnxp1SubLocationNumber=case when LEN(ltrim(rtrim(sar_sub_location_x)))=0
	then 0 when patindex('%[^0-9]%',sar_sub_location_x)=0
	then convert(numeric(3,0),sar_sub_location_x) else -1 end
	LEFT JOIN ( 
	  SELECT b2.PremiumTransactionAKID,b2.BureauCode1,b2.BureauCode2,b2.BureauCode6,b2.BureauCode9
	  FROM 
	  (
	  SELECT  b.PremiumTransactionAKID,b.BureauCode1,b.BureauCode2,b.BureauCode6,b.BureauCode9
	          ,ROW_NUMBER() OVER (PARTITION BY b.PremiumTransactionAKID  ORDER BY b.CurrentSnapshotFlag desc)  AS RN
	  FROM    @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.BureauStatisticalCode   b
	  ) b2 
	  WHERE b2.RN=1
	) bsc 
	ON bsc.PremiumTransactionAKID=pt.PremiumTransactionAKID
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC
	ON SC.PolicyCoverageAKID=PC.PolicyCoverageAKID 
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL
	ON   PC.RiskLocationAKId=RL.RiskLocationAKId
),
EXP_CoverageDetailCommercialProperty AS (
	SELECT
	PremiumTransactionID AS i_PremiumTransactionID,
	CurrentSnapshotFlag AS i_CurrentSnapshotFlag,
	CoverageGuid AS i_CoverageGuid,
	Pmdnxp1InsuranceLine AS i_Pmdnxp1InsuranceLine,
	Pmdnxp1OtherMod AS i_Pmdnxp1OtherMod,
	Pmdnxp1ProtectionClassPart1 AS i_Pmdnxp1ProtectionClassPart1,
	Pmdnxp1SprinklerType AS i_Pmdnxp1SprinklerType,
	MajorPerilCode AS i_MajorPerilCode,
	ClassCode AS i_ClassCode,
	InsuranceReferenceLineOfBusinessAKId AS i_InsuranceReferenceLineOfBusinessAKId,
	BureauCode2 AS i_BureauCode2,
	SublineCode AS i_SublineCode,
	BureauCode1 AS i_BureauCode1,
	StateProvinceCode AS i_StateProvinceCode,
	ProductCode AS i_ProductCode,
	sar_code_2 AS i_sar_code_2,
	sar_code_4 AS i_sar_code_4,
	-- *INF*: :LKP.LKP_SupISOCommercialPropertyCauseOfLossGroup_PMS(i_ProductCode,i_MajorPerilCode)
	LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_PMS_i_ProductCode_i_MajorPerilCode.ISOCommercialPropertyCauseOfLossGroup AS v_ISOPropertyCauseofLossGroup,
	-- *INF*: DECODE(true,
	-- v_ISOPropertyCauseofLossGroup='SCL' and i_BureauCode1='2',:LKP.LKP_SupISOSpecialCauseOfLossCategoryRule_PMS(i_ClassCode),
	-- v_ISOPropertyCauseofLossGroup='SCL' and i_BureauCode1!='2', '01',
	--  'N/A')
	DECODE(
	    true,
	    v_ISOPropertyCauseofLossGroup = 'SCL' and i_BureauCode1 = '2', LKP_SUPISOSPECIALCAUSEOFLOSSCATEGORYRULE_PMS_i_ClassCode.ISOSpecialCauseOfLossCategoryCode,
	    v_ISOPropertyCauseofLossGroup = 'SCL' and i_BureauCode1 != '2', '01',
	    'N/A'
	) AS v_ISOSpecialCauseOfLossCategoryCode,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	-- *INF*: IIF(i_CurrentSnapshotFlag='True',1,0)
	IFF(i_CurrentSnapshotFlag = 'True', 1, 0) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59' , 'MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_CoverageGuid AS o_CoverageGuid,
	-- *INF*: IIF(
	-- ISNULL(i_Pmdnxp1ProtectionClassPart1) OR IS_SPACES(LTRIM(RTRIM(i_Pmdnxp1ProtectionClassPart1))),
	-- IIF( ISNULL(i_sar_code_4) OR IS_SPACES(LTRIM(RTRIM(i_sar_code_4))), 'N/A',i_sar_code_4),
	-- LTRIM(RTRIM(i_Pmdnxp1ProtectionClassPart1))--'N/A'
	-- )
	-- --:UDF.DEFAULT_VALUE_FOR_STRINGS(i_Pmdnxp1ProtectionClassPart1)
	IFF(
	    i_Pmdnxp1ProtectionClassPart1 IS NULL
	    or LENGTH(LTRIM(RTRIM(i_Pmdnxp1ProtectionClassPart1)))>0
	    and TRIM(LTRIM(RTRIM(i_Pmdnxp1ProtectionClassPart1)))='',
	    IFF(
	        i_sar_code_4 IS NULL
	        or LENGTH(LTRIM(RTRIM(i_sar_code_4)))>0
	        and TRIM(LTRIM(RTRIM(i_sar_code_4)))='',
	        'N/A',
	        i_sar_code_4
	    ),
	    LTRIM(RTRIM(i_Pmdnxp1ProtectionClassPart1))
	) AS o_IsoFireProtectionCode,
	-- *INF*: IIF(LTRIM(RTRIM(i_Pmdnxp1InsuranceLine))='CF',i_Pmdnxp1OtherMod,0)
	IFF(LTRIM(RTRIM(i_Pmdnxp1InsuranceLine)) = 'CF', i_Pmdnxp1OtherMod, 0) AS o_MultiLocationCreditFactor,
	-- *INF*: IIF(LTRIM(RTRIM(i_Pmdnxp1InsuranceLine))='CF',i_Pmdnxp1OtherMod,0)
	IFF(LTRIM(RTRIM(i_Pmdnxp1InsuranceLine)) = 'CF', i_Pmdnxp1OtherMod, 0) AS o_PreferredPropertyFactor,
	-- *INF*: IIF(
	-- 	IN(i_Pmdnxp1SprinklerType,'F','P')=1,'1',
	-- 		IIF( IN(i_sar_code_2,'4','8')=1,'1', '0')
	-- )
	IFF(
	    i_Pmdnxp1SprinklerType IN ('F','P') = 1, '1',
	    IFF(
	        i_sar_code_2 IN ('4','8') = 1, '1', '0'
	    )
	) AS o_SprinklerFlag,
	-- *INF*: iif(isnull(v_ISOPropertyCauseofLossGroup),'N/A',
	-- v_ISOPropertyCauseofLossGroup)
	IFF(v_ISOPropertyCauseofLossGroup IS NULL, 'N/A', v_ISOPropertyCauseofLossGroup) AS o_ISOPropertyCauseofLossGroup,
	-- *INF*: IIF(ISNULL(v_ISOSpecialCauseOfLossCategoryCode), 'Unassigned', v_ISOSpecialCauseOfLossCategoryCode)
	IFF(
	    v_ISOSpecialCauseOfLossCategoryCode IS NULL, 'Unassigned',
	    v_ISOSpecialCauseOfLossCategoryCode
	) AS o_ISOSpecialCauseOfLossCategory,
	-- *INF*: SUBSTR(i_ClassCode,1,4)
	-- 
	-- 
	-- --LPAD(i_ClassCode,6, '0')
	SUBSTR(i_ClassCode, 1, 4) AS o_ClassCode,
	i_StateProvinceCode AS o_StateProvinceCode,
	-- *INF*: DECODE(TRUE,
	-- IN(i_BureauCode2, '1', '4', '5', '8'), 'Specific',
	-- IN(i_BureauCode2, '2', '3', '6', '7'), 'Class', 
	-- 'N/A')
	DECODE(
	    TRUE,
	    i_BureauCode2 IN ('1','4','5','8'), 'Specific',
	    i_BureauCode2 IN ('2','3','6','7'), 'Class',
	    'N/A'
	) AS o_RateType
	FROM SQ_CoverageDetailCommericalPropery
	LEFT JOIN LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_PMS LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_PMS_i_ProductCode_i_MajorPerilCode
	ON LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_PMS_i_ProductCode_i_MajorPerilCode.ProductCode = i_ProductCode
	AND LKP_SUPISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP_PMS_i_ProductCode_i_MajorPerilCode.MajorPerilCode = i_MajorPerilCode

	LEFT JOIN LKP_SUPISOSPECIALCAUSEOFLOSSCATEGORYRULE_PMS LKP_SUPISOSPECIALCAUSEOFLOSSCATEGORYRULE_PMS_i_ClassCode
	ON LKP_SUPISOSPECIALCAUSEOFLOSSCATEGORYRULE_PMS_i_ClassCode.ClassCode = i_ClassCode

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
		WHERE SourceSystemId='PMS'
		and  PremiumTransactionID  in (select pt.PremiumTransactionID from
		PremiumTransaction pt
		inner join WorkPremiumTransaction wpt
		on pt.PremiumTransactionAKID=wpt.PremiumTransactionAKId)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionId ORDER BY PremiumTransactionId) = 1
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
EXP_Cal AS (
	SELECT
	LKP_CoverageDetailCommercialProperty.PremiumTransactionId AS lkp_PremiumTransactionId,
	LKP_CoverageDetailCommercialProperty.IsoFireProtectionCode AS lkp_IsoFireProtectionCode,
	LKP_CoverageDetailCommercialProperty.MultiLocationCreditFactor AS lkp_MultiLocationCreditFactor,
	LKP_CoverageDetailCommercialProperty.PreferredPropertyFactor AS lkp_PreferredPropertyFactor,
	LKP_CoverageDetailCommercialProperty.SprinklerFlag AS lkp_SprinklerFlag_Origin,
	-- *INF*: DECODE(lkp_SprinklerFlag_Origin,'T',1,'F',0,NULL)
	DECODE(
	    lkp_SprinklerFlag_Origin,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS lkp_SprinklerFlag,
	LKP_CoverageDetailCommercialProperty.ISOCommercialPropertyCauseofLossGroup AS lkp_ISOPropertyCauseofLossGroup,
	LKP_CoverageDetailCommercialProperty.ISOCommercialPropertyRatingGroupCode AS lkp_ISOCPRatingGroup,
	LKP_CoverageDetailCommercialProperty.ISOSpecialCauseOfLossCategoryCode AS lkp_ISOSpecialCauseOfLossCategory,
	LKP_CoverageDetailCommercialProperty.RateType AS lkp_RateType,
	LKP_CoverageDetailCommercialProperty.CommercialPropertySpecialClass AS lkp_PropertySpecialClass,
	EXP_CoverageDetailCommercialProperty.o_PremiumTransactionID AS PremiumTransactionID,
	EXP_CoverageDetailCommercialProperty.o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	EXP_CoverageDetailCommercialProperty.o_AuditID AS AuditID,
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
	EXP_CoverageDetailCommercialProperty.o_ISOSpecialCauseOfLossCategory AS ISOSpecialCauseOfLossCategory,
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
	IFF(
	    ISOPropertyCauseofLossGroup = 'BGI',
	    DECODE(
	        TRUE,
	        lk_ISOCPRatingGroup IS NOT NULL, lk_ISOCPRatingGroup,
	        lk_ISOCPRatingGroup_default IS NOT NULL, lk_ISOCPRatingGroup_default,
	        'N/A'
	    ),
	    'N/A'
	) AS v_ISOCPRatingGroup,
	-- *INF*: DECODE(TRUE,
	-- not isnull(lk_PropertySpecialClass),lk_PropertySpecialClass,
	-- not isnull(lk_PropertySpecialClass_default),lk_PropertySpecialClass_default,
	-- 'N/A')
	DECODE(
	    TRUE,
	    lk_PropertySpecialClass IS NOT NULL, lk_PropertySpecialClass,
	    lk_PropertySpecialClass_default IS NOT NULL, lk_PropertySpecialClass_default,
	    'N/A'
	) AS v_PropertySpecialClass,
	v_ISOCPRatingGroup AS o_ISOCPRatingGroup,
	v_PropertySpecialClass AS o_PropertySpecialClass,
	-- *INF*: Decode(true,
	-- isnull(lkp_PremiumTransactionId),'INSERT',
	--  lkp_IsoFireProtectionCode!=IsoFireProtectionCode
	-- or lkp_MultiLocationCreditFactor!=MultiLocationCreditFactor
	-- or lkp_PreferredPropertyFactor!=PreferredPropertyFactor
	-- or lkp_SprinklerFlag!=SprinklerFlag
	-- or lkp_ISOPropertyCauseofLossGroup!=ISOPropertyCauseofLossGroup
	-- or lkp_ISOCPRatingGroup!=v_ISOCPRatingGroup
	-- or lkp_ISOSpecialCauseOfLossCategory!=ISOSpecialCauseOfLossCategory
	-- or lkp_RateType!=RateType
	-- or lkp_PropertySpecialClass!=v_PropertySpecialClass,'UPDATE',
	-- 'UNCHANGE')
	Decode(
	    true,
	    lkp_PremiumTransactionId IS NULL, 'INSERT',
	    lkp_IsoFireProtectionCode != IsoFireProtectionCode or lkp_MultiLocationCreditFactor != MultiLocationCreditFactor or lkp_PreferredPropertyFactor != PreferredPropertyFactor or lkp_SprinklerFlag != SprinklerFlag or lkp_ISOPropertyCauseofLossGroup != ISOPropertyCauseofLossGroup or lkp_ISOCPRatingGroup != v_ISOCPRatingGroup or lkp_ISOSpecialCauseOfLossCategory != ISOSpecialCauseOfLossCategory or lkp_RateType != RateType or lkp_PropertySpecialClass != v_PropertySpecialClass, 'UPDATE',
	    'UNCHANGE'
	) AS ChangeFlag,
	-- *INF*: TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
	TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AS RetroactiveDate
	FROM EXP_CoverageDetailCommercialProperty
	LEFT JOIN LKP_CoverageDetailCommercialProperty
	ON LKP_CoverageDetailCommercialProperty.PremiumTransactionId = EXP_CoverageDetailCommercialProperty.o_PremiumTransactionID
	LEFT JOIN LKP_SupClassificationCommercialProperty
	ON LKP_SupClassificationCommercialProperty.ClassCode = EXP_CoverageDetailCommercialProperty.o_ClassCode AND LKP_SupClassificationCommercialProperty.RatingStateCode = EXP_CoverageDetailCommercialProperty.o_StateProvinceCode
	LEFT JOIN LKP_SupClassificationCommercialProperty_default
	ON LKP_SupClassificationCommercialProperty_default.ClassCode = EXP_CoverageDetailCommercialProperty.o_ClassCode
),
RTR_Target AS (
	SELECT
	ChangeFlag,
	PremiumTransactionID AS PremiumTransactionId,
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
	o_ISOCPRatingGroup AS ISOCPRatingGroup,
	ISOSpecialCauseOfLossCategory,
	RateType,
	o_PropertySpecialClass AS PropertySpecialClass,
	RetroactiveDate
	FROM EXP_Cal
),
RTR_Target_INSERT AS (SELECT * FROM RTR_Target WHERE ChangeFlag='INSERT'),
RTR_Target_UPDATE AS (SELECT * FROM RTR_Target WHERE ChangeFlag='UPDATE'),
UPD_Target AS (
	SELECT
	PremiumTransactionId, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	ModifiedDate, 
	CoverageGuid, 
	IsoFireProtectionCode, 
	MultiLocationCreditFactor, 
	PreferredPropertyFactor, 
	SprinklerFlag, 
	ISOPropertyCauseofLossGroup, 
	ISOCPRatingGroup, 
	ISOSpecialCauseOfLossCategory AS ISOSpecialCauseOfLossCategory2, 
	RateType, 
	PropertySpecialClass, 
	RetroactiveDate AS RetroactiveDate3
	FROM RTR_Target_UPDATE
),
CoverageDetailCommercialProperty_Update AS (
	MERGE INTO CoverageDetailCommercialProperty AS T
	USING UPD_Target AS S
	ON T.PremiumTransactionID = S.PremiumTransactionId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.EffectiveDate = S.EffectiveDate, T.ExpirationDate = S.ExpirationDate, T.SourceSystemID = S.SourceSystemID, T.ModifiedDate = S.ModifiedDate, T.CoverageGuid = S.CoverageGuid, T.IsoFireProtectionCode = S.IsoFireProtectionCode, T.MultiLocationCreditFactor = S.MultiLocationCreditFactor, T.PreferredPropertyFactor = S.PreferredPropertyFactor, T.RetroactiveDate = S.RetroactiveDate3, T.ISOCommercialPropertyCauseofLossGroup = S.ISOPropertyCauseofLossGroup, T.ISOCommercialPropertyRatingGroupCode = S.ISOCPRatingGroup, T.ISOSpecialCauseOfLossCategoryCode = S.ISOSpecialCauseOfLossCategory2, T.RateType = S.RateType, T.CommercialPropertySpecialClass = S.PropertySpecialClass
),
CoverageDetailCommercialProperty_Insert AS (
	INSERT INTO CoverageDetailCommercialProperty
	(PremiumTransactionID, CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CoverageGuid, IsoFireProtectionCode, MultiLocationCreditFactor, PreferredPropertyFactor, SprinklerFlag, RetroactiveDate, ISOCommercialPropertyCauseofLossGroup, ISOCommercialPropertyRatingGroupCode, ISOSpecialCauseOfLossCategoryCode, RateType, CommercialPropertySpecialClass)
	SELECT 
	PremiumTransactionId AS PREMIUMTRANSACTIONID, 
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
	FROM RTR_Target_INSERT
),