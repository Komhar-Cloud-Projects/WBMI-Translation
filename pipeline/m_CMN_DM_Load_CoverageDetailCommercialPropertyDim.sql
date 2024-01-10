WITH
LKP_CoverageLimit AS (
	SELECT
	CoverageLimitValue,
	PremiumTransactionID,
	CoverageLimitType
	FROM (
		select pt.PremiumTransactionID as PremiumTransactionID,
		cl.CoverageLimitValue as CoverageLimitValue,
		LTRIM(RTRIM(cl.CoverageLimitType)) as CoverageLimitType 
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit cl
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge clb
		on cl.CoverageLimitId=clb.CoverageLimitId
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction pt
		on pt.PremiumTransactionAKId=clb.PremiumTransactionAKId
		WHERE PT.SourceSystemId='DCT' and ( '@{pipeline().parameters.SELECTION_START_TS}'<='01/01/1800 01:00:00' or 
		exists ( select 1 
		               from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT2 
		               where PT2.RatingCoverageAKId=PT.RatingCoverageAKId and PT2.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'))
		and cl.CoverageLimitType in ('ResponseExpense','DefenseAndLiability')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID,CoverageLimitType ORDER BY CoverageLimitValue) = 1
),
LKP_SupISOSpecialCauseOfLossCategory AS (
	SELECT
	ISOSpecialCauseOfLossCategoryDescription,
	ISOSpecialCauseOfLossCategoryCode
	FROM (
		SELECT 
			ISOSpecialCauseOfLossCategoryDescription,
			ISOSpecialCauseOfLossCategoryCode
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupISOSpecialCauseOfLossCategory
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ISOSpecialCauseOfLossCategoryCode ORDER BY ISOSpecialCauseOfLossCategoryDescription) = 1
),
SQ_CoverageDetailCommercialProperty AS (
	select distinct cd.CoverageDetailDimId,
	cd.EffectiveDate,
	cd.ExpirationDate,
	cp.CoverageGuid,
	cp.IsoFireProtectionCode,
	cp.MultiLocationCreditFactor,
	cp.PreferredPropertyFactor,
	sc.SpecialClassGroupCode,
	sscg.SpecialClassGroupDescription,
	cp.PremiumTransactionID,
	'N/A' as CoverageType
	,cp.SprinklerFlag
	,cp.ISOCommercialPropertyCauseofLossGroup
	,cp.ISOCommercialPropertyRatingGroupCode
	,cp.ISOSpecialCauseOfLossCategoryCode
	,cp.RateType
	,cp.CommercialPropertySpecialClass
	,cp.SourceSystemID
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialProperty cp
	join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim cd
	on cp.PremiumTransactionID=cd.EDWPremiumTransactionPKId
	and cp.SourceSystemID='PMS'
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage sc
	on cp.CoverageGuid=sc.CoverageGUID
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupSpecialClassGroup sscg
	on sc.SpecialClassGroupCode=sscg.StandardSpecialClassGroupCode and sscg.SourceSystemId='PMS' and sscg.CurrentSnapshotFlag=1
	WHERE cd.ModifedDate >='@{pipeline().parameters.SELECTION_START_TS}' 
	@{pipeline().parameters.WHERE_CLAUSE_PMS}
	
	union all
	
	select distinct cd.CoverageDetailDimId,
	cd.EffectiveDate,
	cd.ExpirationDate,
	cp.CoverageGuid,
	cp.IsoFireProtectionCode,
	cp.MultiLocationCreditFactor,
	cp.PreferredPropertyFactor,
	rc.SpecialClassGroupCode,
	sscg.SpecialClassGroupDescription,
	cp.PremiumTransactionID,
	rc.CoverageType
	,cp.SprinklerFlag
	,cp.ISOCommercialPropertyCauseofLossGroup
	,cp.ISOCommercialPropertyRatingGroupCode
	,cp.ISOSpecialCauseOfLossCategoryCode
	,cp.RateType
	,cp.CommercialPropertySpecialClass
	,cp.SourceSystemID
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialProperty cp
	join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim cd
	on cp.PremiumTransactionID=cd.EDWPremiumTransactionPKId
	and cp.SourceSystemID='DCT'
	/* Added PT and Ratingcoverage Join for DAP-879 and comment CoverageGUID join 
	 on cp.CoverageGuid=rc.CoverageGUID
	and rc.CurrentSnapshotFlag=1*/
	Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Premiumtransaction PT
	on CD.EDWPremiumTransactionPKId=pt.premiumtransactionid
	and pt.sourcesystemid='DCT'
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage rc
	on pt.ratingcoverageakid=rc.ratingcoverageakid
	and pt.EffectiveDate=rc.EffectiveDate
	and rc.sourcesystemid='DCT'
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupSpecialClassGroup sscg
	on rc.SpecialClassGroupCode=sscg.StandardSpecialClassGroupCode and sscg.SourceSystemId='DCT' and sscg.CurrentSnapshotFlag=1
	WHERE cd.ModifedDate >='@{pipeline().parameters.SELECTION_START_TS}' 
	@{pipeline().parameters.WHERE_CLAUSE_DCT}
),
EXP_GetMetaData AS (
	SELECT
	CoverageDetailDimId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	EffectiveDate,
	ExpirationDate,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	CoverageGuid,
	IsoFireProtectionCode AS i_IsoFireProtectionCode,
	MultiLocationCreditFactor,
	PreferredPropertyFactor,
	SpecialClassGroupCode AS i_SpecialClassGroupCode,
	SpecialClassGroupDescription AS i_SpecialClassGroupDescription,
	-- *INF*: IIF(ISNULL(i_SpecialClassGroupCode), 'N/A', i_SpecialClassGroupCode)
	IFF(i_SpecialClassGroupCode IS NULL,
		'N/A',
		i_SpecialClassGroupCode
	) AS o_SpecialClassGroupCode,
	-- *INF*: IIF(ISNULL(i_SpecialClassGroupDescription), 'N/A', i_SpecialClassGroupDescription)
	IFF(i_SpecialClassGroupDescription IS NULL,
		'N/A',
		i_SpecialClassGroupDescription
	) AS o_SpecialClassGroupDescription,
	i_IsoFireProtectionCode AS o_IsoFireProtectionCode,
	PremiumTransactionID AS i_PremiumTransactionID,
	CoverageType AS i_CoverageType,
	-- *INF*: IIF(LTRIM(RTRIM(i_CoverageType)) = 'DataCompromise',:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionID,'ResponseExpense'),'N/A')
	IFF(LTRIM(RTRIM(i_CoverageType
			)
		) = 'DataCompromise',
		LKP_COVERAGELIMIT_i_PremiumTransactionID_ResponseExpense.CoverageLimitValue,
		'N/A'
	) AS v_ResponseExpenseLimit,
	-- *INF*: IIF(ISNULL(v_ResponseExpenseLimit),'N/A',v_ResponseExpenseLimit)
	IFF(v_ResponseExpenseLimit IS NULL,
		'N/A',
		v_ResponseExpenseLimit
	) AS o_ResponseExpenseLimit,
	-- *INF*: IIF(LTRIM(RTRIM(i_CoverageType)) = 'DataCompromise',:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionID,'DefenseAndLiability'),'N/A')
	IFF(LTRIM(RTRIM(i_CoverageType
			)
		) = 'DataCompromise',
		LKP_COVERAGELIMIT_i_PremiumTransactionID_DefenseAndLiability.CoverageLimitValue,
		'N/A'
	) AS v_DefenseAndLiabilityLimit,
	-- *INF*: IIF(ISNULL(v_DefenseAndLiabilityLimit),'N/A',v_DefenseAndLiabilityLimit)
	IFF(v_DefenseAndLiabilityLimit IS NULL,
		'N/A',
		v_DefenseAndLiabilityLimit
	) AS o_DefenseAndLiabilityLimit,
	SprinklerFlag,
	ISOCommercialPropertyCauseofLossGroup,
	ISOCommercialPropertyRatingGroupCode,
	ISOSpecialCauseOfLossCategoryCode,
	RateType,
	CommercialPropertySpecialClass,
	SourceSystemID
	FROM SQ_CoverageDetailCommercialProperty
	LEFT JOIN LKP_COVERAGELIMIT LKP_COVERAGELIMIT_i_PremiumTransactionID_ResponseExpense
	ON LKP_COVERAGELIMIT_i_PremiumTransactionID_ResponseExpense.PremiumTransactionID = i_PremiumTransactionID
	AND LKP_COVERAGELIMIT_i_PremiumTransactionID_ResponseExpense.CoverageLimitType = 'ResponseExpense'

	LEFT JOIN LKP_COVERAGELIMIT LKP_COVERAGELIMIT_i_PremiumTransactionID_DefenseAndLiability
	ON LKP_COVERAGELIMIT_i_PremiumTransactionID_DefenseAndLiability.PremiumTransactionID = i_PremiumTransactionID
	AND LKP_COVERAGELIMIT_i_PremiumTransactionID_DefenseAndLiability.CoverageLimitType = 'DefenseAndLiability'

),
LKP_CoverageDetailCommercialPropertyDim AS (
	SELECT
	CoverageDetailDimId,
	CoverageGuid,
	MultiLocationCreditFactor,
	PreferredPropertyCreditFactor,
	SpecialClassGroupCode,
	SpecialClassGroupDescription,
	IsoFireProtectionCode,
	EffectiveDate,
	ExpirationDate,
	ResponseExpenseLimit,
	DefenseAndLiabilityLimit,
	ISOCommercialPropertyCauseofLossGroup,
	ISOCommercialPropertyRatingGroupCode,
	ISOCommercialPropertyRatingGroupDescription,
	ISOSpecialCauseOfLossCategoryCode,
	ISOSpecialCauseOfLossCategoryDescription,
	RateType,
	CommercialPropertySpecialClass,
	i_CoverageDetailDimId
	FROM (
		SELECT CDCPD.CoverageGuid as CoverageGuid, CDCPD.MultiLocationCreditFactor as MultiLocationCreditFactor, CDCPD.PreferredPropertyCreditFactor as PreferredPropertyCreditFactor, CDCPD.SpecialClassGroupCode as SpecialClassGroupCode, CDCPD.SpecialClassGroupDescription as SpecialClassGroupDescription, CDCPD.IsoFireProtectionCode as IsoFireProtectionCode, CDCPD.EffectiveDate as EffectiveDate, CDCPD.ExpirationDate as ExpirationDate, CDCPD.ResponseExpenseLimit as ResponseExpenseLimit, CDCPD.DefenseAndLiabilityLimit as DefenseAndLiabilityLimit, CDCPD.ISOCommercialPropertyCauseofLossGroup as ISOCommercialPropertyCauseofLossGroup, CDCPD.ISOCommercialPropertyRatingGroupCode as ISOCommercialPropertyRatingGroupCode, CDCPD.ISOCommercialPropertyRatingGroupDescription as ISOCommercialPropertyRatingGroupDescription, CDCPD.ISOSpecialCauseOfLossCategoryCode as ISOSpecialCauseOfLossCategoryCode, CDCPD.ISOSpecialCauseOfLossCategoryDescription as ISOSpecialCauseOfLossCategoryDescription, CDCPD.RateType as RateType, CDCPD.CommercialPropertySpecialClass as CommercialPropertySpecialClass, CDCPD.CoverageDetailDimId as CoverageDetailDimId 
		FROM 
		@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialPropertyDim CDCPD
		INNER JOIN  @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD 
		ON CDCPD.CoverageDetailDimId = CDD.CoverageDetailDimId
		INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialProperty CP
		ON CP.PremiumTransactionID=CDD.EDWPremiumTransactionPKId 
		WHERE CDD.ModifedDate >='@{pipeline().parameters.SELECTION_START_TS}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageDetailDimId ORDER BY CoverageDetailDimId) = 1
),
LKP_SupISOCommercialPropertyRatingGroup AS (
	SELECT
	ISOCommercialPropertyRatingGroupDescription,
	ISOCommercialPropertyRatingGroupCode
	FROM (
		SELECT 
			ISOCommercialPropertyRatingGroupDescription,
			ISOCommercialPropertyRatingGroupCode
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupISOCommercialPropertyRatingGroup
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ISOCommercialPropertyRatingGroupCode ORDER BY ISOCommercialPropertyRatingGroupDescription) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_CoverageDetailCommercialPropertyDim.CoverageDetailDimId AS lkp_CoverageDetailDimId,
	LKP_CoverageDetailCommercialPropertyDim.CoverageGuid AS lkp_CoverageGuid,
	LKP_CoverageDetailCommercialPropertyDim.MultiLocationCreditFactor AS lkp_MultiLocationCreditFactor,
	LKP_CoverageDetailCommercialPropertyDim.PreferredPropertyCreditFactor AS lkp_PreferredPropertyCreditFactor,
	LKP_CoverageDetailCommercialPropertyDim.SpecialClassGroupCode AS lkp_SpecialClassGroupCode,
	LKP_CoverageDetailCommercialPropertyDim.SpecialClassGroupDescription AS lkp_SpecialClassGroupDescription,
	LKP_CoverageDetailCommercialPropertyDim.IsoFireProtectionCode AS lkp_IsoFireProtectionCode,
	LKP_CoverageDetailCommercialPropertyDim.EffectiveDate AS lkp_EffectiveDate,
	LKP_CoverageDetailCommercialPropertyDim.ExpirationDate AS lkp_ExpirationDate,
	LKP_CoverageDetailCommercialPropertyDim.ResponseExpenseLimit AS lkp_ResponseExpenseLimit,
	LKP_CoverageDetailCommercialPropertyDim.DefenseAndLiabilityLimit AS lkp_DefenseAndLiabilityLimit,
	LKP_CoverageDetailCommercialPropertyDim.ISOCommercialPropertyCauseofLossGroup AS lkp_ISOCommercialPropertyCauseofLossGroup,
	LKP_CoverageDetailCommercialPropertyDim.ISOCommercialPropertyRatingGroupCode AS lkp_ISOCommercialPropertyRatingGroupCode,
	LKP_CoverageDetailCommercialPropertyDim.ISOCommercialPropertyRatingGroupDescription AS lkp_ISOCommercialPropertyRatingGroupDescription,
	LKP_CoverageDetailCommercialPropertyDim.ISOSpecialCauseOfLossCategoryCode AS lkp_ISOSpecialCauseOfLossCategoryCode,
	LKP_CoverageDetailCommercialPropertyDim.ISOSpecialCauseOfLossCategoryDescription AS lkp_ISOSpecialCauseOfLossCategoryDescription,
	LKP_CoverageDetailCommercialPropertyDim.RateType AS lkp_RateType,
	LKP_CoverageDetailCommercialPropertyDim.CommercialPropertySpecialClass AS lkp_CommercialPropertySpecialClass,
	EXP_GetMetaData.CoverageDetailDimId,
	EXP_GetMetaData.o_AuditId AS AuditId,
	EXP_GetMetaData.EffectiveDate,
	EXP_GetMetaData.ExpirationDate,
	EXP_GetMetaData.o_CreatedDate AS CreatedDate,
	EXP_GetMetaData.o_ModifiedDate AS ModifiedDate,
	EXP_GetMetaData.CoverageGuid,
	EXP_GetMetaData.MultiLocationCreditFactor,
	EXP_GetMetaData.PreferredPropertyFactor,
	EXP_GetMetaData.o_SpecialClassGroupCode AS SpecialClassGroupCode,
	EXP_GetMetaData.o_SpecialClassGroupDescription AS SpecialClassGroupDescription,
	EXP_GetMetaData.o_IsoFireProtectionCode AS IsoFireProtectionCode,
	EXP_GetMetaData.o_ResponseExpenseLimit AS ResponseExpenseLimit,
	EXP_GetMetaData.o_DefenseAndLiabilityLimit AS DefenseAndLiabilityLimit,
	EXP_GetMetaData.SprinklerFlag,
	EXP_GetMetaData.ISOCommercialPropertyCauseofLossGroup AS i_ISOCommercialPropertyCauseofLossGroup,
	EXP_GetMetaData.ISOCommercialPropertyRatingGroupCode AS i_ISOCommercialPropertyRatingGroupCode,
	LKP_SupISOCommercialPropertyRatingGroup.ISOCommercialPropertyRatingGroupDescription AS i_ISOCPRatingGroupDescription,
	EXP_GetMetaData.ISOSpecialCauseOfLossCategoryCode AS i_ISOSpecialCauseOfLossCategoryCode,
	EXP_GetMetaData.RateType AS i_RateType,
	EXP_GetMetaData.CommercialPropertySpecialClass AS i_CommercialPropertySpecialClass,
	EXP_GetMetaData.SourceSystemID AS i_SourceSystemID,
	-- *INF*: IIF(isnull(i_ISOCommercialPropertyCauseofLossGroup),'N/A',i_ISOCommercialPropertyCauseofLossGroup)
	IFF(i_ISOCommercialPropertyCauseofLossGroup IS NULL,
		'N/A',
		i_ISOCommercialPropertyCauseofLossGroup
	) AS v_ISOCommercialPropertyCauseofLossGroup,
	-- *INF*: IIF(isnull(i_ISOCommercialPropertyRatingGroupCode),'N/A',i_ISOCommercialPropertyRatingGroupCode)
	IFF(i_ISOCommercialPropertyRatingGroupCode IS NULL,
		'N/A',
		i_ISOCommercialPropertyRatingGroupCode
	) AS v_ISOCommercialPropertyRatingGroupCode,
	-- *INF*: IIF(isnull(i_ISOCPRatingGroupDescription),'N/A',i_ISOCPRatingGroupDescription)
	IFF(i_ISOCPRatingGroupDescription IS NULL,
		'N/A',
		i_ISOCPRatingGroupDescription
	) AS v_ISOGeneralLiabilityClassGroupDescription,
	-- *INF*: IIF(isnull(i_ISOSpecialCauseOfLossCategoryCode),'N/A',i_ISOSpecialCauseOfLossCategoryCode)
	IFF(i_ISOSpecialCauseOfLossCategoryCode IS NULL,
		'N/A',
		i_ISOSpecialCauseOfLossCategoryCode
	) AS v_ISOSpecialCauseOfLossCategoryCode,
	-- *INF*: DECODE(TRUE,
	-- i_ISOCommercialPropertyCauseofLossGroup='SCL' and not isnull(:LKP.LKP_SupISOSpecialCauseOfLossCategory(i_ISOSpecialCauseOfLossCategoryCode)),:LKP.LKP_SupISOSpecialCauseOfLossCategory(i_ISOSpecialCauseOfLossCategoryCode),'N/A')
	DECODE(TRUE,
		i_ISOCommercialPropertyCauseofLossGroup = 'SCL' 
		AND LKP_SUPISOSPECIALCAUSEOFLOSSCATEGORY_i_ISOSpecialCauseOfLossCategoryCode.ISOSpecialCauseOfLossCategoryDescription IS NOT NULL, LKP_SUPISOSPECIALCAUSEOFLOSSCATEGORY_i_ISOSpecialCauseOfLossCategoryCode.ISOSpecialCauseOfLossCategoryDescription,
		'N/A'
	) AS v_ISOSpecialCauseOfLossCategoryDescription,
	-- *INF*: IIF(isnull(i_RateType),'N/A',i_RateType)
	IFF(i_RateType IS NULL,
		'N/A',
		i_RateType
	) AS v_RateType,
	-- *INF*: IIF(isnull(i_CommercialPropertySpecialClass),'N/A',i_CommercialPropertySpecialClass)
	IFF(i_CommercialPropertySpecialClass IS NULL,
		'N/A',
		i_CommercialPropertySpecialClass
	) AS v_CommercialPropertySpecialClass,
	v_ISOCommercialPropertyCauseofLossGroup AS o_ISOCommercialPropertyCauseofLossGroup,
	v_ISOCommercialPropertyRatingGroupCode AS o_ISOCommercialPropertyRatingGroupCode,
	v_ISOGeneralLiabilityClassGroupDescription AS o_ISOGeneralLiabilityClassGroupDescription,
	v_ISOSpecialCauseOfLossCategoryCode AS o_ISOSpecialCauseOfLossCategoryCode,
	v_ISOSpecialCauseOfLossCategoryDescription AS o_ISOSpecialCauseOfLossCategoryDescription,
	v_RateType AS o_RateType,
	v_CommercialPropertySpecialClass AS o_CommercialPropertySpecialClass,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_CoverageDetailDimId),'INSERT',
	-- lkp_CoverageGuid<>CoverageGuid or lkp_MultiLocationCreditFactor<>MultiLocationCreditFactor or lkp_PreferredPropertyCreditFactor<>PreferredPropertyFactor or lkp_SpecialClassGroupCode<>SpecialClassGroupCode or lkp_SpecialClassGroupDescription<>SpecialClassGroupDescription or lkp_IsoFireProtectionCode<>IsoFireProtectionCode OR lkp_EffectiveDate<>EffectiveDate OR lkp_ExpirationDate<>ExpirationDate OR lkp_ResponseExpenseLimit<>ResponseExpenseLimit OR lkp_DefenseAndLiabilityLimit<>DefenseAndLiabilityLimit
	--  OR lkp_ISOCommercialPropertyCauseofLossGroup<>v_ISOCommercialPropertyCauseofLossGroup
	--  OR lkp_ISOCommercialPropertyRatingGroupCode<>v_ISOCommercialPropertyRatingGroupCode
	--  OR lkp_ISOCommercialPropertyRatingGroupDescription<>lkp_ISOCommercialPropertyRatingGroupDescription
	--  OR lkp_ISOSpecialCauseOfLossCategoryCode<>v_ISOSpecialCauseOfLossCategoryCode
	-- OR lkp_ISOSpecialCauseOfLossCategoryDescription<>
	-- v_ISOSpecialCauseOfLossCategoryDescription
	--  OR lkp_RateType<>v_RateType
	--  OR lkp_CommercialPropertySpecialClass<>v_CommercialPropertySpecialClass
	-- , 'UPDATE',
	-- 'NOCHANGE'
	-- )
	DECODE(TRUE,
		lkp_CoverageDetailDimId IS NULL, 'INSERT',
		lkp_CoverageGuid <> CoverageGuid 
		OR lkp_MultiLocationCreditFactor <> MultiLocationCreditFactor 
		OR lkp_PreferredPropertyCreditFactor <> PreferredPropertyFactor 
		OR lkp_SpecialClassGroupCode <> SpecialClassGroupCode 
		OR lkp_SpecialClassGroupDescription <> SpecialClassGroupDescription 
		OR lkp_IsoFireProtectionCode <> IsoFireProtectionCode 
		OR lkp_EffectiveDate <> EffectiveDate 
		OR lkp_ExpirationDate <> ExpirationDate 
		OR lkp_ResponseExpenseLimit <> ResponseExpenseLimit 
		OR lkp_DefenseAndLiabilityLimit <> DefenseAndLiabilityLimit 
		OR lkp_ISOCommercialPropertyCauseofLossGroup <> v_ISOCommercialPropertyCauseofLossGroup 
		OR lkp_ISOCommercialPropertyRatingGroupCode <> v_ISOCommercialPropertyRatingGroupCode 
		OR lkp_ISOCommercialPropertyRatingGroupDescription <> lkp_ISOCommercialPropertyRatingGroupDescription 
		OR lkp_ISOSpecialCauseOfLossCategoryCode <> v_ISOSpecialCauseOfLossCategoryCode 
		OR lkp_ISOSpecialCauseOfLossCategoryDescription <> v_ISOSpecialCauseOfLossCategoryDescription 
		OR lkp_RateType <> v_RateType 
		OR lkp_CommercialPropertySpecialClass <> v_CommercialPropertySpecialClass, 'UPDATE',
		'NOCHANGE'
	) AS o_ChangeFlag
	FROM EXP_GetMetaData
	LEFT JOIN LKP_CoverageDetailCommercialPropertyDim
	ON LKP_CoverageDetailCommercialPropertyDim.CoverageDetailDimId = EXP_GetMetaData.CoverageDetailDimId
	LEFT JOIN LKP_SupISOCommercialPropertyRatingGroup
	ON LKP_SupISOCommercialPropertyRatingGroup.ISOCommercialPropertyRatingGroupCode = EXP_GetMetaData.ISOCommercialPropertyRatingGroupCode
	LEFT JOIN LKP_SUPISOSPECIALCAUSEOFLOSSCATEGORY LKP_SUPISOSPECIALCAUSEOFLOSSCATEGORY_i_ISOSpecialCauseOfLossCategoryCode
	ON LKP_SUPISOSPECIALCAUSEOFLOSSCATEGORY_i_ISOSpecialCauseOfLossCategoryCode.ISOSpecialCauseOfLossCategoryCode = i_ISOSpecialCauseOfLossCategoryCode

),
RTR_CoverageDetailCommercialPropertyDim AS (
	SELECT
	o_ChangeFlag AS ChangeFlag,
	CoverageDetailDimId,
	AuditId,
	EffectiveDate,
	ExpirationDate,
	CreatedDate,
	ModifiedDate,
	CoverageGuid,
	MultiLocationCreditFactor,
	PreferredPropertyFactor,
	SpecialClassGroupCode,
	SpecialClassGroupDescription,
	IsoFireProtectionCode,
	ResponseExpenseLimit,
	DefenseAndLiabilityLimit,
	o_ISOCommercialPropertyCauseofLossGroup AS ISOCommercialPropertyCauseofLossGroup,
	o_ISOCommercialPropertyRatingGroupCode AS ISOCommercialPropertyRatingGroupCode,
	o_ISOGeneralLiabilityClassGroupDescription AS ISOCommercialPropertyRatingGroupDescription,
	o_ISOSpecialCauseOfLossCategoryCode AS ISOSpecialCauseOfLossCategoryCode,
	o_ISOSpecialCauseOfLossCategoryDescription AS ISOSpecialCauseOfLossCategoryDescription,
	o_RateType AS RateType,
	o_CommercialPropertySpecialClass AS CommercialPropertySpecialClass
	FROM EXP_DetectChanges
),
RTR_CoverageDetailCommercialPropertyDim_INSERT AS (SELECT * FROM RTR_CoverageDetailCommercialPropertyDim WHERE ChangeFlag='INSERT'),
RTR_CoverageDetailCommercialPropertyDim_UPDATE AS (SELECT * FROM RTR_CoverageDetailCommercialPropertyDim WHERE ChangeFlag='UPDATE'),
TGT_CoverageDetailCommercialPropertyDim_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialPropertyDim
	(CoverageDetailDimId, AuditId, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, CoverageGuid, MultiLocationCreditFactor, PreferredPropertyCreditFactor, SpecialClassGroupCode, SpecialClassGroupDescription, IsoFireProtectionCode, ResponseExpenseLimit, DefenseAndLiabilityLimit, ISOCommercialPropertyCauseofLossGroup, ISOCommercialPropertyRatingGroupCode, ISOCommercialPropertyRatingGroupDescription, ISOSpecialCauseOfLossCategoryCode, ISOSpecialCauseOfLossCategoryDescription, RateType, CommercialPropertySpecialClass)
	SELECT 
	COVERAGEDETAILDIMID, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	COVERAGEGUID, 
	MULTILOCATIONCREDITFACTOR, 
	PreferredPropertyFactor AS PREFERREDPROPERTYCREDITFACTOR, 
	SPECIALCLASSGROUPCODE, 
	SPECIALCLASSGROUPDESCRIPTION, 
	ISOFIREPROTECTIONCODE, 
	RESPONSEEXPENSELIMIT, 
	DEFENSEANDLIABILITYLIMIT, 
	ISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP, 
	ISOCOMMERCIALPROPERTYRATINGGROUPCODE, 
	ISOCOMMERCIALPROPERTYRATINGGROUPDESCRIPTION, 
	ISOSPECIALCAUSEOFLOSSCATEGORYCODE, 
	ISOSPECIALCAUSEOFLOSSCATEGORYDESCRIPTION, 
	RATETYPE, 
	COMMERCIALPROPERTYSPECIALCLASS
	FROM RTR_CoverageDetailCommercialPropertyDim_INSERT
),
UPD_Existing AS (
	SELECT
	CoverageDetailDimId, 
	AuditId, 
	EffectiveDate, 
	ExpirationDate, 
	ModifiedDate, 
	CoverageGuid, 
	MultiLocationCreditFactor, 
	PreferredPropertyFactor, 
	SpecialClassGroupCode, 
	SpecialClassGroupDescription, 
	IsoFireProtectionCode, 
	ResponseExpenseLimit, 
	DefenseAndLiabilityLimit, 
	ISOCommercialPropertyCauseofLossGroup AS ISOCommercialPropertyCauseofLossGroup3, 
	ISOCommercialPropertyRatingGroupCode AS ISOCommercialPropertyRatingGroupCode3, 
	ISOCommercialPropertyRatingGroupDescription AS ISOCommercialPropertyRatingGroupDescription3, 
	ISOSpecialCauseOfLossCategoryCode AS ISOSpecialCauseOfLossCategoryCode3, 
	ISOSpecialCauseOfLossCategoryDescription AS ISOSpecialCauseOfLossCategoryDescription3, 
	RateType AS RateType3, 
	CommercialPropertySpecialClass AS CommercialPropertySpecialClass3
	FROM RTR_CoverageDetailCommercialPropertyDim_UPDATE
),
TGT_CoverageDetailCommercialPropertyDim_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialPropertyDim AS T
	USING UPD_Existing AS S
	ON 
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CoverageDetailDimId = S.CoverageDetailDimId, T.AuditId = S.AuditId, T.EffectiveDate = S.EffectiveDate, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate, T.CoverageGuid = S.CoverageGuid, T.MultiLocationCreditFactor = S.MultiLocationCreditFactor, T.PreferredPropertyCreditFactor = S.PreferredPropertyFactor, T.SpecialClassGroupCode = S.SpecialClassGroupCode, T.SpecialClassGroupDescription = S.SpecialClassGroupDescription, T.IsoFireProtectionCode = S.IsoFireProtectionCode, T.ResponseExpenseLimit = S.ResponseExpenseLimit, T.DefenseAndLiabilityLimit = S.DefenseAndLiabilityLimit, T.ISOCommercialPropertyCauseofLossGroup = S.ISOCommercialPropertyCauseofLossGroup3, T.ISOCommercialPropertyRatingGroupCode = S.ISOCommercialPropertyRatingGroupCode3, T.ISOCommercialPropertyRatingGroupDescription = S.ISOCommercialPropertyRatingGroupDescription3, T.ISOSpecialCauseOfLossCategoryCode = S.ISOSpecialCauseOfLossCategoryCode3, T.ISOSpecialCauseOfLossCategoryDescription = S.ISOSpecialCauseOfLossCategoryDescription3, T.RateType = S.RateType3, T.CommercialPropertySpecialClass = S.CommercialPropertySpecialClass3
),