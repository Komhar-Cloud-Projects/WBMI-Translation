WITH
LKP_CoverageDeductibleValue AS (
	SELECT
	CoverageDeductibleValue,
	PremiumTransactionAKId,
	CoverageDeductibleType
	FROM (
		SELECT CD.CoverageDeductibleValue as CoverageDeductibleValue, 
		CDB.PremiumTransactionAKId as PremiumTransactionAKId, 
		CD.CoverageDeductibleType as CoverageDeductibleType 
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductibleBridge CDB
		JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductible CD
		ON CD.CoverageDeductibleId=CDB.CoverageDeductibleId
		where '@{pipeline().parameters.SELECTION_START_TS}'<='01/01/1800 01:00:00' OR
		exists (select 1 
		from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT,
		@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
		where CDD.EDWPremiumTransactionPKId=PT.PremiumTransactionID and 
		PT.PremiumTransactionAKID=CDB.PremiumTransactionAKId
		and CDD.ModifedDate>='@{pipeline().parameters.SELECTION_START_TS}'
		)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId,CoverageDeductibleType ORDER BY CoverageDeductibleValue) = 1
),
LKP_CoverageLimitValue AS (
	SELECT
	CoverageLimitValue,
	PremiumTransactionAKId,
	CoverageLimitType
	FROM (
		SELECT CL.CoverageLimitValue as CoverageLimitValue, 
		CLB.PremiumTransactionAKId as PremiumTransactionAKId, 
		CL.CoverageLimitType as CoverageLimitType 
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge CLB
		JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit CL
		ON CL.CoverageLimitId=CLB.CoverageLimitId
		where '@{pipeline().parameters.SELECTION_START_TS}'<='01/01/1800 01:00:00' OR
		exists (select 1 
		from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT,
		@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
		where CDD.EDWPremiumTransactionPKId=PT.PremiumTransactionID and 
		PT.PremiumTransactionAKID=CLB.PremiumTransactionAKId
		and CDD.ModifedDate>='@{pipeline().parameters.SELECTION_START_TS}'
		)
		order by CLB.PremiumTransactionAKId,CL.CoverageLimitType,CLB.CreatedDate desc
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId,CoverageLimitType ORDER BY CoverageLimitValue) = 1
),
SQ_CoverageDetailCommercialAuto AS (
	select CDD.CoverageDetailDimId AS CoverageDetailDimId,
	CDD.EffectiveDate AS EffectiveDate,
	CDD.ExpirationDate AS ExpirationDate,
	CDD.CoverageGuid AS CoverageGuid,
	CDCA.SourceSystemID AS SourceSystemID,
	CDCA.VehicleGroupCode AS VehicleType,
	CDCA.RadiusOfOperation AS RadiusOfOperation,
	CDCA.SecondaryVehicleType AS SecondaryVehicleType,
	RC.CoverageType AS CoverageType,
	CDCA.UsedInDumpingIndicator AS UsedInDumpingIndicator,
	PT.PremiumTransactionAKID,
	CDCA.VehicleYear,
	CDCA.StatedAmount,
	CDCA.CostNew,
	CDCA.VehicleDeleteDate,
	CDCA.CompositeRatedFlag,
	SC.MajorPerilCode,
	CDCA.VehicleTypeSize,
	CDCA.BusinessUseClass,
	CDCA.SecondaryClass,
	CDCA.FleetType,
	CDCA.SecondaryClassGroup,
	CDCA.VIN,
	CDCA.VehicleNumber,
	CDCA.CoordinationOfBenefits, 
	CDCA.MedicalExpensesOption, 
	CDCA.CoveredByWorkersCompensationFlag,
	CDCA.HistoricVehicleIndicator
	from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
	ON CDD.EDWPremiumTransactionPKId=PT.PremiumTransactionID
	JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CDCA
	ON CDCA.PremiumTransactionID=CDD.EDWPremiumTransactionPKId
	LEFT JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	on PT.RatingCoverageAKId=RC.RatingCoverageAKId and PT.EffectiveDate=RC.EffectiveDate
	LEFT JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
	on PT.StatisticalCoverageAKId=SC.StatisticalCoverageAKId and SC.CurrentSnapshotFlag=1
	where CDD.ModifedDate>='@{pipeline().parameters.SELECTION_START_TS}' 
	AND CDD.CoverageDetailDimId % 3 = 0
	@{pipeline().parameters.WHERE_CLAUSE_PMS}
),
EXP_MetaData AS (
	SELECT
	CoverageDetailDimId AS i_CoverageDetailDimId,
	EffectiveDate AS i_EffectiveDate,
	ExpirationDate AS i_ExpirationDate,
	CoverageGuid AS i_CoverageGuid,
	SourceSystemID AS i_SourceSystemID,
	VehicleType AS i_VehicleType,
	RadiusOfOperation AS i_RadiusOfOperation,
	SecondaryVehicleType AS i_SecondaryVehicleType,
	CoverageType AS i_CoverageType,
	UsedInDumpingIndicator AS i_UsedInDumpingIndicator,
	PremiumTransactionAKID AS i_PremiumTransactionAKID,
	VehicleYear,
	StatedAmount,
	CostNew,
	VehicleDeleteDate,
	MajorPerilCode,
	VehicleTypeSize AS i_VehicleTypeSize,
	CompositeRatedFlag AS i_CompositeRatedFlag,
	BusinessUseClass AS i_BusinessUseClass,
	SecondaryClass AS i_SecondaryClass,
	FleetType AS i_FleetType,
	SecondaryClassGroup AS i_SecondaryClassGroup,
	VIN AS i_VIN,
	VehicleNumber AS i_VehicleNumber,
	CoordinationOfBenefits AS i_CoordinationOfBenefits,
	MedicalExpensesOption AS i_MedicalExpensesOption,
	CoveredByWorkersCompensationFlag AS i_CoveredByWorkersCompensationFlag,
	HistoricVehicleIndicator AS i_HistoricVehicleIndicator,
	-- *INF*: IIF(i_CompositeRatedFlag='T',1,0)
	IFF(i_CompositeRatedFlag = 'T', 1, 0) AS o_CompositeRatedFlag,
	i_CoverageDetailDimId AS o_CoverageDetailDimId,
	i_EffectiveDate AS o_EffectiveDate,
	i_ExpirationDate AS o_ExpirationDate,
	-- *INF*: RTRIM(LTRIM(i_CoverageGuid))
	RTRIM(LTRIM(i_CoverageGuid)) AS o_CoverageGuid,
	-- *INF*: RTRIM(LTRIM(i_VehicleType))
	RTRIM(LTRIM(i_VehicleType)) AS o_VehicleType,
	-- *INF*: RTRIM(LTRIM(i_RadiusOfOperation))
	RTRIM(LTRIM(i_RadiusOfOperation)) AS o_RadiusOfOperation,
	-- *INF*: RTRIM(LTRIM(i_SecondaryVehicleType))
	RTRIM(LTRIM(i_SecondaryVehicleType)) AS o_SecondaryVehicleType,
	-- *INF*: RTRIM(LTRIM(i_UsedInDumpingIndicator))
	RTRIM(LTRIM(i_UsedInDumpingIndicator)) AS o_UsedInDumpingIndicator,
	-- *INF*: RTRIM(LTRIM(i_SourceSystemID))
	RTRIM(LTRIM(i_SourceSystemID)) AS o_SourceSystemID,
	-- *INF*: RTRIM(LTRIM(i_CoverageType))
	RTRIM(LTRIM(i_CoverageType)) AS o_CoverageType,
	i_PremiumTransactionAKID AS o_PremiumTransactionAKId,
	-- *INF*: iif(isnull(i_VehicleTypeSize) or length(i_VehicleTypeSize)=0,'N/A',i_VehicleTypeSize)
	IFF(i_VehicleTypeSize IS NULL OR length(i_VehicleTypeSize) = 0, 'N/A', i_VehicleTypeSize) AS o_VehicleTypeSize,
	-- *INF*: iif(isnull(i_BusinessUseClass) or length(i_BusinessUseClass)=0,'N/A',i_BusinessUseClass)
	IFF(i_BusinessUseClass IS NULL OR length(i_BusinessUseClass) = 0, 'N/A', i_BusinessUseClass) AS o_BusinessUseClass,
	-- *INF*: iif(isnull(i_SecondaryClass) or length(i_SecondaryClass)=0,'N/A',i_SecondaryClass)
	IFF(i_SecondaryClass IS NULL OR length(i_SecondaryClass) = 0, 'N/A', i_SecondaryClass) AS o_SecondaryClass,
	-- *INF*: iif(isnull(i_FleetType) or length(i_FleetType)=0,'N/A',i_FleetType)
	IFF(i_FleetType IS NULL OR length(i_FleetType) = 0, 'N/A', i_FleetType) AS o_FleetType,
	-- *INF*: iif(isnull(i_SecondaryClassGroup) or length(i_SecondaryClassGroup)=0,'N/A',i_SecondaryClassGroup)
	IFF(i_SecondaryClassGroup IS NULL OR length(i_SecondaryClassGroup) = 0, 'N/A', i_SecondaryClassGroup) AS o_SecondaryClassGroup,
	i_VIN AS o_VIN,
	i_VehicleNumber AS o_VehicleNumber,
	-- *INF*: RTRIM(LTRIM(i_CoordinationOfBenefits))
	RTRIM(LTRIM(i_CoordinationOfBenefits)) AS o_CoordinationOfBenefits,
	-- *INF*: RTRIM(LTRIM(i_MedicalExpensesOption))
	RTRIM(LTRIM(i_MedicalExpensesOption)) AS o_MedicalExpensesOption,
	-- *INF*: IIF(i_CoveredByWorkersCompensationFlag = 'T',1,0)
	IFF(i_CoveredByWorkersCompensationFlag = 'T', 1, 0) AS o_CoveredByWorkersCompensationFlag,
	-- *INF*: IIF(ISNULL(i_HistoricVehicleIndicator),0,IIF(i_HistoricVehicleIndicator= 'T',1,0))
	IFF(i_HistoricVehicleIndicator IS NULL, 0, IFF(i_HistoricVehicleIndicator = 'T', 1, 0)) AS o_HistoricVehicleIndicator
	FROM SQ_CoverageDetailCommercialAuto
),
EXP_Business_Logic AS (
	SELECT
	o_CoverageDetailDimId AS CoverageDetailDimId,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_CoverageGuid AS CoverageGuid,
	o_VehicleType AS VehicleType,
	o_RadiusOfOperation AS RadiusOfOperation,
	o_SecondaryVehicleType AS SecondaryVehicleType,
	o_SourceSystemID AS i_SourceSystemID,
	o_CoverageType AS i_CoverageType,
	o_PremiumTransactionAKId AS i_PremiumTransactionAKId,
	-- *INF*: :LKP.LKP_COVERAGELIMITVALUE(i_PremiumTransactionAKId, 'CombinedSingleLimit')
	LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_CombinedSingleLimit.CoverageLimitValue AS v_raw_CoverageLimitValue_CombinedSingleLimit,
	-- *INF*: :LKP.LKP_COVERAGELIMITVALUE(i_PremiumTransactionAKId, 'MedicalPaymentLimit')
	LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_MedicalPaymentLimit.CoverageLimitValue AS v_raw_CoverageLimitValue_MedicalPaymentLimit,
	-- *INF*: :LKP.LKP_COVERAGELIMITVALUE(i_PremiumTransactionAKId, 'MedicalLimit')
	LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_MedicalLimit.CoverageLimitValue AS v_raw_CoverageLimitValue_MedicalLimit,
	-- *INF*: :LKP.LKP_COVERAGELIMITVALUE(i_PremiumTransactionAKId, 'UninsuredMotoristSingleLimit')
	LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_UninsuredMotoristSingleLimit.CoverageLimitValue AS v_raw_CoverageLimitValue_UninsuredMotoristSingleLimit,
	-- *INF*: :LKP.LKP_COVERAGELIMITVALUE(i_PremiumTransactionAKId, 'UnderinsuredMotoristSingleLimit')
	LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_UnderinsuredMotoristSingleLimit.CoverageLimitValue AS v_raw_CoverageLimitValue_UnderinsuredMotoristSingleLimit,
	-- *INF*: :LKP.LKP_COVERAGELIMITVALUE(i_PremiumTransactionAKId, 'ValueEstimate')
	LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_ValueEstimate.CoverageLimitValue AS v_raw_CoverageLimitValue_ValueEstimate,
	-- *INF*: :LKP.LKP_COVERAGELIMITVALUE(i_PremiumTransactionAKId, 'Value')
	LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_Value.CoverageLimitValue AS v_raw_CoverageLimitValue_Value,
	-- *INF*: :LKP.LKP_COVERAGELIMITVALUE(i_PremiumTransactionAKId, 'PersonalInjuryProtectionLimit')
	LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionLimit.CoverageLimitValue AS v_raw_CoverageLimitValue_PersonalInjuryProtectionLimit,
	-- *INF*: :LKP.LKP_COVERAGELIMITVALUE(i_PremiumTransactionAKId, 'PersonalInjuryProtectionLimitWithStacking')
	LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionLimitWithStacking.CoverageLimitValue AS v_raw_CoverageLimitValue_PersonalInjuryProtectionLimitWithStacking,
	-- *INF*: :LKP.LKP_COVERAGELIMITVALUE(i_PremiumTransactionAKId, 'PersonalInjuryProtectionLimitWithoutStacking')
	LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionLimitWithoutStacking.CoverageLimitValue AS v_raw_CoverageLimitValue_PersonalInjuryProtectionLimitWithoutStacking,
	-- *INF*: :LKP.LKP_COVERAGELIMITVALUE(i_PremiumTransactionAKId, 'PersonalInjuryProtectionBasicLimit')
	LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionBasicLimit.CoverageLimitValue AS v_raw_CoverageLimitValue_PersonaInjuryProtectionBasicLimt,
	-- *INF*: :LKP.LKP_COVERAGELIMITVALUE(i_PremiumTransactionAKId, 'PersonalInjuryProtectionExcessLimit')
	LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionExcessLimit.CoverageLimitValue AS v_raw_CoverageLimitValue_PersonaInjuryProtectionExcessLimt,
	-- *INF*: :LKP.LKP_COVERAGEDEDUCTIBLEVALUE(i_PremiumTransactionAKId, 'ComprehensiveDeductible')
	LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_ComprehensiveDeductible.CoverageDeductibleValue AS v_raw_CoverageDeductibleValue_ComprehensiveDeductible,
	-- *INF*: :LKP.LKP_COVERAGEDEDUCTIBLEVALUE(i_PremiumTransactionAKId, 'Standard')
	LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_Standard.CoverageDeductibleValue AS v_raw_CoverageDeductibleValue_Standard,
	-- *INF*: :LKP.LKP_COVERAGEDEDUCTIBLEVALUE(i_PremiumTransactionAKId, 'GlassBuyBack')
	LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_GlassBuyBack.CoverageDeductibleValue AS v_raw_CoverageDeductibleValue_GlassBuyBack,
	-- *INF*: :LKP.LKP_COVERAGEDEDUCTIBLEVALUE(i_PremiumTransactionAKId, 'CollisionDeductible')
	LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_CollisionDeductible.CoverageDeductibleValue AS v_raw_CoverageDeductibleValue_CollisionDeductible,
	-- *INF*: :LKP.LKP_COVERAGEDEDUCTIBLEVALUE(i_PremiumTransactionAKId, 'Limited')
	LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_Limited.CoverageDeductibleValue AS v_raw_CoverageDeductibleValue_Limited,
	-- *INF*: :LKP.LKP_COVERAGEDEDUCTIBLEVALUE(i_PremiumTransactionAKId, 'PropertyDamage')
	LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_PropertyDamage.CoverageDeductibleValue AS v_raw_CoverageDeductibleValue_PropertyDamage,
	-- *INF*: :LKP.LKP_COVERAGEDEDUCTIBLEVALUE(i_PremiumTransactionAKId, 'CSL')
	LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_CSL.CoverageDeductibleValue AS v_raw_CoverageDeductibleValue_CSL,
	-- *INF*: :LKP.LKP_COVERAGEDEDUCTIBLEVALUE(i_PremiumTransactionAKId, 'PersonalInjuryProtectionDeductible')
	LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionDeductible.CoverageDeductibleValue AS v_raw_CoverageDeductibleValue_PersonalInjuryProtectionDeductible,
	-- *INF*: :LKP.LKP_COVERAGEDEDUCTIBLEVALUE(i_PremiumTransactionAKId, 'ManagedCareDeductible')
	LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_ManagedCareDeductible.CoverageDeductibleValue AS v_raw_CoverageDeductibleValue_ManagedCareDeductible,
	-- *INF*: :LKP.LKP_COVERAGEDEDUCTIBLEVALUE(i_PremiumTransactionAKId, 'ComprehensiveFullGlassCoverageDeductible')
	LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_ComprehensiveFullGlassCoverageDeductible.CoverageDeductibleValue AS v_raw_CoverageDeductibleValuee_ComprehensiveFullGlassCoverageDeductible,
	-- *INF*: :LKP.LKP_COVERAGEDEDUCTIBLEVALUE(i_PremiumTransactionAKId, 'LimitedCollisionDeductible')
	LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_LimitedCollisionDeductible.CoverageDeductibleValue AS v_raw_CoverageDeductibleValuee_LimitedCollisionDeductible,
	-- *INF*: :LKP.LKP_COVERAGEDEDUCTIBLEVALUE(i_PremiumTransactionAKId, 'BroadenedCollisionDeductible')
	LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_BroadenedCollisionDeductible.CoverageDeductibleValue AS v_raw_CoverageDeductibleValuee_BroadenedCollisionDeductible,
	-- *INF*: :LKP.LKP_COVERAGEDEDUCTIBLEVALUE(i_PremiumTransactionAKId, 'SingleLimitDeductible')
	LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_SingleLimitDeductible.CoverageDeductibleValue AS v_raw_CoverageDeductibleValuee_SingleLimitDeductible,
	-- *INF*: IIF(ISNULL(v_raw_CoverageLimitValue_CombinedSingleLimit), 'N/A', v_raw_CoverageLimitValue_CombinedSingleLimit)
	IFF(v_raw_CoverageLimitValue_CombinedSingleLimit IS NULL, 'N/A', v_raw_CoverageLimitValue_CombinedSingleLimit) AS v_CoverageLimitValue_CombinedSingleLimit,
	-- *INF*: IIF(ISNULL(v_raw_CoverageLimitValue_MedicalPaymentLimit), 'N/A', v_raw_CoverageLimitValue_MedicalPaymentLimit)
	IFF(v_raw_CoverageLimitValue_MedicalPaymentLimit IS NULL, 'N/A', v_raw_CoverageLimitValue_MedicalPaymentLimit) AS v_CoverageLimitValue_MedicalPaymentLimit,
	-- *INF*: IIF(ISNULL(v_raw_CoverageLimitValue_MedicalLimit), 'N/A', v_raw_CoverageLimitValue_MedicalLimit)
	IFF(v_raw_CoverageLimitValue_MedicalLimit IS NULL, 'N/A', v_raw_CoverageLimitValue_MedicalLimit) AS v_CoverageLimitValue_MedicalLimit,
	-- *INF*: IIF(ISNULL(v_raw_CoverageLimitValue_UninsuredMotoristSingleLimit), 'N/A', v_raw_CoverageLimitValue_UninsuredMotoristSingleLimit)
	IFF(v_raw_CoverageLimitValue_UninsuredMotoristSingleLimit IS NULL, 'N/A', v_raw_CoverageLimitValue_UninsuredMotoristSingleLimit) AS v_CoverageLimitValue_UninsuredMotoristSingleLimit,
	-- *INF*: IIF(ISNULL(v_raw_CoverageLimitValue_UnderinsuredMotoristSingleLimit), 'N/A', v_raw_CoverageLimitValue_UnderinsuredMotoristSingleLimit)
	IFF(v_raw_CoverageLimitValue_UnderinsuredMotoristSingleLimit IS NULL, 'N/A', v_raw_CoverageLimitValue_UnderinsuredMotoristSingleLimit) AS v_CoverageLimitValue_UnderinsuredMotoristSingleLimit,
	-- *INF*: IIF(ISNULL(v_raw_CoverageLimitValue_ValueEstimate), 'N/A', v_raw_CoverageLimitValue_ValueEstimate)
	IFF(v_raw_CoverageLimitValue_ValueEstimate IS NULL, 'N/A', v_raw_CoverageLimitValue_ValueEstimate) AS v_CoverageLimitValue_ValueEstimate,
	-- *INF*: IIF(ISNULL(v_raw_CoverageLimitValue_Value), 'N/A', v_raw_CoverageLimitValue_Value)
	IFF(v_raw_CoverageLimitValue_Value IS NULL, 'N/A', v_raw_CoverageLimitValue_Value) AS v_CoverageLimitValue_Value,
	-- *INF*: IIF(ISNULL(v_raw_CoverageLimitValue_PersonalInjuryProtectionLimit), 'N/A', v_raw_CoverageLimitValue_PersonalInjuryProtectionLimit)
	IFF(v_raw_CoverageLimitValue_PersonalInjuryProtectionLimit IS NULL, 'N/A', v_raw_CoverageLimitValue_PersonalInjuryProtectionLimit) AS v_CoverageLimitValue_PersonalInjuryProtectionLimit,
	-- *INF*: IIF(ISNULL(v_raw_CoverageLimitValue_PersonalInjuryProtectionLimitWithStacking), 'N/A', v_raw_CoverageLimitValue_PersonalInjuryProtectionLimitWithStacking)
	IFF(v_raw_CoverageLimitValue_PersonalInjuryProtectionLimitWithStacking IS NULL, 'N/A', v_raw_CoverageLimitValue_PersonalInjuryProtectionLimitWithStacking) AS v_CoverageLimitValue_PersonalInjuryProtectionLimitWithStacking,
	-- *INF*: IIF(ISNULL(v_raw_CoverageLimitValue_PersonalInjuryProtectionLimitWithoutStacking), 'N/A', v_raw_CoverageLimitValue_PersonalInjuryProtectionLimitWithoutStacking)
	IFF(v_raw_CoverageLimitValue_PersonalInjuryProtectionLimitWithoutStacking IS NULL, 'N/A', v_raw_CoverageLimitValue_PersonalInjuryProtectionLimitWithoutStacking) AS v_CoverageLimitValue_PersonalInjuryProtectionLimitWithoutStacking,
	-- *INF*: IIF(ISNULL(v_raw_CoverageLimitValue_PersonaInjuryProtectionBasicLimt), 'N/A', v_raw_CoverageLimitValue_PersonaInjuryProtectionBasicLimt)
	IFF(v_raw_CoverageLimitValue_PersonaInjuryProtectionBasicLimt IS NULL, 'N/A', v_raw_CoverageLimitValue_PersonaInjuryProtectionBasicLimt) AS v_CoverageLimitValue_PersonaInjuryProtectionBasicLimt,
	-- *INF*: IIF(ISNULL(v_raw_CoverageLimitValue_PersonaInjuryProtectionExcessLimt), 'N/A', v_raw_CoverageLimitValue_PersonaInjuryProtectionExcessLimt)
	IFF(v_raw_CoverageLimitValue_PersonaInjuryProtectionExcessLimt IS NULL, 'N/A', v_raw_CoverageLimitValue_PersonaInjuryProtectionExcessLimt) AS v_CoverageLimitValue_PersonaInjuryProtectionExcessLimt,
	-- *INF*: IIF(ISNULL(v_raw_CoverageDeductibleValue_ComprehensiveDeductible), 'N/A', v_raw_CoverageDeductibleValue_ComprehensiveDeductible)
	IFF(v_raw_CoverageDeductibleValue_ComprehensiveDeductible IS NULL, 'N/A', v_raw_CoverageDeductibleValue_ComprehensiveDeductible) AS v_CoverageDeductibleValue_ComprehensiveDeductible,
	-- *INF*: IIF(ISNULL(v_raw_CoverageDeductibleValue_Standard), 'N/A', v_raw_CoverageDeductibleValue_Standard)
	IFF(v_raw_CoverageDeductibleValue_Standard IS NULL, 'N/A', v_raw_CoverageDeductibleValue_Standard) AS v_CoverageDeductibleValue_Standard,
	-- *INF*: IIF(ISNULL(v_raw_CoverageDeductibleValue_GlassBuyBack), 'N/A', v_raw_CoverageDeductibleValue_GlassBuyBack)
	IFF(v_raw_CoverageDeductibleValue_GlassBuyBack IS NULL, 'N/A', v_raw_CoverageDeductibleValue_GlassBuyBack) AS v_CoverageDeductibleValue_GlassBuyBack,
	-- *INF*: IIF(ISNULL(v_raw_CoverageDeductibleValue_CollisionDeductible), 'N/A', v_raw_CoverageDeductibleValue_CollisionDeductible)
	IFF(v_raw_CoverageDeductibleValue_CollisionDeductible IS NULL, 'N/A', v_raw_CoverageDeductibleValue_CollisionDeductible) AS v_CoverageDeductibleValue_CollisionDeductible,
	-- *INF*: IIF(ISNULL(v_raw_CoverageDeductibleValue_Limited), 'N/A', v_raw_CoverageDeductibleValue_Limited)
	IFF(v_raw_CoverageDeductibleValue_Limited IS NULL, 'N/A', v_raw_CoverageDeductibleValue_Limited) AS v_CoverageDeductibleValue_Limited,
	-- *INF*: IIF(ISNULL(v_raw_CoverageDeductibleValue_PropertyDamage), 'N/A', v_raw_CoverageDeductibleValue_PropertyDamage)
	IFF(v_raw_CoverageDeductibleValue_PropertyDamage IS NULL, 'N/A', v_raw_CoverageDeductibleValue_PropertyDamage) AS v_CoverageDeductibleValue_PropertyDamage,
	-- *INF*: IIF(ISNULL(v_raw_CoverageDeductibleValue_CSL), 'N/A', v_raw_CoverageDeductibleValue_CSL)
	IFF(v_raw_CoverageDeductibleValue_CSL IS NULL, 'N/A', v_raw_CoverageDeductibleValue_CSL) AS v_CoverageDeductibleValue_CSL,
	-- *INF*: IIF(ISNULL(v_raw_CoverageDeductibleValue_PersonalInjuryProtectionDeductible), 'N/A', v_raw_CoverageDeductibleValue_PersonalInjuryProtectionDeductible)
	IFF(v_raw_CoverageDeductibleValue_PersonalInjuryProtectionDeductible IS NULL, 'N/A', v_raw_CoverageDeductibleValue_PersonalInjuryProtectionDeductible) AS v_CoverageDeductibleValue_PersonalInjuryProtectionDeuctible,
	-- *INF*: IIF(ISNULL(v_raw_CoverageDeductibleValue_ManagedCareDeductible), 'N/A', v_raw_CoverageDeductibleValue_ManagedCareDeductible)
	IFF(v_raw_CoverageDeductibleValue_ManagedCareDeductible IS NULL, 'N/A', v_raw_CoverageDeductibleValue_ManagedCareDeductible) AS v_CoverageDeductibleValue_ManagedCareDeductible,
	VehicleYear,
	StatedAmount,
	CostNew,
	VehicleDeleteDate,
	MajorPerilCode,
	o_CompositeRatedFlag AS CompositeRatedFlag,
	v_CoverageLimitValue_CombinedSingleLimit AS o_CombinedSingleLimit,
	'N/A' AS o_BodilyInjurySplitLimit,
	'N/A' AS o_PhysicalDamageSplitLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',
	-- v_CoverageLimitValue_MedicalPaymentLimit,
	-- i_SourceSystemID='DCT',
	-- v_CoverageLimitValue_MedicalLimit,
	-- 'N/A'
	-- )
	DECODE(TRUE,
	i_SourceSystemID = 'PMS', v_CoverageLimitValue_MedicalPaymentLimit,
	i_SourceSystemID = 'DCT', v_CoverageLimitValue_MedicalLimit,
	'N/A') AS o_MedicalPaymentLimit,
	-- *INF*: IIF(
	-- i_SourceSystemID='PMS',
	-- v_CoverageLimitValue_UninsuredMotoristSingleLimit,
	-- 'N/A'
	-- )
	IFF(i_SourceSystemID = 'PMS', v_CoverageLimitValue_UninsuredMotoristSingleLimit, 'N/A') AS o_UninsuredMotoristSingleLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',
	-- v_CoverageLimitValue_UnderinsuredMotoristSingleLimit,
	-- i_SourceSystemID='DCT' AND i_CoverageType='UIM',
	-- IIF(v_CoverageLimitValue_ValueEstimate='N/A', v_CoverageLimitValue_Value, v_CoverageLimitValue_ValueEstimate),
	-- 'N/A'
	-- )
	DECODE(TRUE,
	i_SourceSystemID = 'PMS', v_CoverageLimitValue_UnderinsuredMotoristSingleLimit,
	i_SourceSystemID = 'DCT' AND i_CoverageType = 'UIM', IFF(v_CoverageLimitValue_ValueEstimate = 'N/A', v_CoverageLimitValue_Value, v_CoverageLimitValue_ValueEstimate),
	'N/A') AS o_UnderinsuredMotoristSingleLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS' AND MajorPerilCode='130' ,
	-- DECODE(TRUE,
	-- v_CoverageLimitValue_PersonaInjuryProtectionBasicLimt<>'N/A',v_CoverageLimitValue_PersonaInjuryProtectionBasicLimt,
	-- 'N/A'),
	-- i_SourceSystemID='DCT' AND i_CoverageType='PIP',
	-- DECODE(TRUE,
	-- v_CoverageLimitValue_PersonaInjuryProtectionBasicLimt<>'N/A',v_CoverageLimitValue_PersonaInjuryProtectionBasicLimt,
	-- 'N/A'),
	-- 'N/A'
	-- ) 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --Removed on 05-16-2014
	-- --DECODE(TRUE,
	-- --i_SourceSystemID='PMS',
	-- --DECODE(TRUE,
	-- --v_CoverageLimitValue_PersonalInjuryProtectionLimit<>'N/A',v_CoverageLimitValue_PersonalInjuryProtectionLimit,
	-- --v_CoverageLimitValue_PersonalInjuryProtectionLimitWithStacking<>'N/A',v_CoverageLimitValue_PersonalInjuryProtectionLimitWithStacking,
	-- --v_CoverageLimitValue_PersonalInjuryProtectionLimitWithoutStacking<>'N/A',v_CoverageLimitValue_PersonalInjuryProtectionLimitWithoutStacking,
	-- --'N/A'),
	-- --i_SourceSystemID='DCT' AND i_CoverageType='PIP',
	-- --IIF(v_CoverageLimitValue_ValueEstimate='N/A', v_CoverageLimitValue_Value, v_CoverageLimitValue_ValueEstimate),
	-- --'N/A'
	-- --) 
	DECODE(TRUE,
	i_SourceSystemID = 'PMS' AND MajorPerilCode = '130', DECODE(TRUE,
	v_CoverageLimitValue_PersonaInjuryProtectionBasicLimt <> 'N/A', v_CoverageLimitValue_PersonaInjuryProtectionBasicLimt,
	'N/A'),
	i_SourceSystemID = 'DCT' AND i_CoverageType = 'PIP', DECODE(TRUE,
	v_CoverageLimitValue_PersonaInjuryProtectionBasicLimt <> 'N/A', v_CoverageLimitValue_PersonaInjuryProtectionBasicLimt,
	'N/A'),
	'N/A') AS o_PersonalInjuryProtectionLimit,
	'N/A' AS o_PhysicalDamageLiabilityDeductible,
	-- *INF*: IIF(ISNULL(v_raw_CoverageDeductibleValuee_SingleLimitDeductible),'N/A',v_raw_CoverageDeductibleValuee_SingleLimitDeductible)
	IFF(v_raw_CoverageDeductibleValuee_SingleLimitDeductible IS NULL, 'N/A', v_raw_CoverageDeductibleValuee_SingleLimitDeductible) AS o_SingleLimitDeductible,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',
	-- v_CoverageDeductibleValue_ComprehensiveDeductible,
	-- i_SourceSystemID='DCT' AND i_CoverageType='OTC',
	-- v_CoverageDeductibleValue_Standard,
	-- 'N/A'
	-- )
	DECODE(TRUE,
	i_SourceSystemID = 'PMS', v_CoverageDeductibleValue_ComprehensiveDeductible,
	i_SourceSystemID = 'DCT' AND i_CoverageType = 'OTC', v_CoverageDeductibleValue_Standard,
	'N/A') AS o_ComprehensiveDeductible,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='DCT' AND i_CoverageType='Collision',
	-- v_CoverageDeductibleValue_GlassBuyBack,
	-- i_SourceSystemID='PMS' AND NOT ISNULL(v_raw_CoverageDeductibleValuee_ComprehensiveFullGlassCoverageDeductible),v_raw_CoverageDeductibleValuee_ComprehensiveFullGlassCoverageDeductible,
	-- 'N/A'
	-- )
	DECODE(TRUE,
	i_SourceSystemID = 'DCT' AND i_CoverageType = 'Collision', v_CoverageDeductibleValue_GlassBuyBack,
	i_SourceSystemID = 'PMS' AND NOT v_raw_CoverageDeductibleValuee_ComprehensiveFullGlassCoverageDeductible IS NULL, v_raw_CoverageDeductibleValuee_ComprehensiveFullGlassCoverageDeductible,
	'N/A') AS o_ComprehensiveFullGlassCoverageDeductible,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',
	-- v_CoverageDeductibleValue_CollisionDeductible,
	-- i_SourceSystemID='DCT' AND i_CoverageType='Collision',
	--  DECODE(TRUE,
	--  v_CoverageDeductibleValue_Standard<>'N/A',
	--  v_CoverageDeductibleValue_Standard,
	--  v_CoverageDeductibleValue_GlassBuyBack<>'N/A',
	--  v_CoverageDeductibleValue_GlassBuyBack,
	--  v_CoverageDeductibleValue_Limited),
	-- 'N/A'
	-- )
	DECODE(TRUE,
	i_SourceSystemID = 'PMS', v_CoverageDeductibleValue_CollisionDeductible,
	i_SourceSystemID = 'DCT' AND i_CoverageType = 'Collision', DECODE(TRUE,
	v_CoverageDeductibleValue_Standard <> 'N/A', v_CoverageDeductibleValue_Standard,
	v_CoverageDeductibleValue_GlassBuyBack <> 'N/A', v_CoverageDeductibleValue_GlassBuyBack,
	v_CoverageDeductibleValue_Limited),
	'N/A') AS o_CollisionDeductible,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='DCT' AND i_CoverageType='Collision',
	--  DECODE(TRUE,
	--  v_CoverageDeductibleValue_Standard<>'N/A',
	--  v_CoverageDeductibleValue_Standard,
	--  v_CoverageDeductibleValue_GlassBuyBack<>'N/A',
	--  v_CoverageDeductibleValue_GlassBuyBack,
	--  v_CoverageDeductibleValue_Limited),
	-- i_SourceSystemID='PMS' AND  NOT ISNULL(v_raw_CoverageDeductibleValuee_LimitedCollisionDeductible),v_raw_CoverageDeductibleValuee_LimitedCollisionDeductible,
	-- 'N/A'
	-- )
	DECODE(TRUE,
	i_SourceSystemID = 'DCT' AND i_CoverageType = 'Collision', DECODE(TRUE,
	v_CoverageDeductibleValue_Standard <> 'N/A', v_CoverageDeductibleValue_Standard,
	v_CoverageDeductibleValue_GlassBuyBack <> 'N/A', v_CoverageDeductibleValue_GlassBuyBack,
	v_CoverageDeductibleValue_Limited),
	i_SourceSystemID = 'PMS' AND NOT v_raw_CoverageDeductibleValuee_LimitedCollisionDeductible IS NULL, v_raw_CoverageDeductibleValuee_LimitedCollisionDeductible,
	'N/A') AS o_LimitedCollisionDeductible,
	-- *INF*: IIF(i_SourceSystemID='DCT' AND i_CoverageType='DriveOtherCarLiability',
	-- IIF(v_CoverageDeductibleValue_PropertyDamage='N/A', v_CoverageDeductibleValue_CSL, v_CoverageDeductibleValue_PropertyDamage),
	-- 'N/A'
	-- )
	IFF(i_SourceSystemID = 'DCT' AND i_CoverageType = 'DriveOtherCarLiability', IFF(v_CoverageDeductibleValue_PropertyDamage = 'N/A', v_CoverageDeductibleValue_CSL, v_CoverageDeductibleValue_PropertyDamage), 'N/A') AS o_PropertyDamageLiabilityDeductible,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS' AND NOT ISNULL(v_raw_CoverageDeductibleValuee_BroadenedCollisionDeductible),
	-- v_raw_CoverageDeductibleValuee_BroadenedCollisionDeductible,
	-- i_SourceSystemID='DCT' AND i_CoverageType='Collision',
	--  DECODE(TRUE,
	--  v_CoverageDeductibleValue_Standard<>'N/A',
	--  v_CoverageDeductibleValue_Standard,
	--  v_CoverageDeductibleValue_GlassBuyBack<>'N/A',
	--  v_CoverageDeductibleValue_GlassBuyBack,
	--  v_CoverageDeductibleValue_Limited),
	-- 'N/A'
	-- )
	DECODE(TRUE,
	i_SourceSystemID = 'PMS' AND NOT v_raw_CoverageDeductibleValuee_BroadenedCollisionDeductible IS NULL, v_raw_CoverageDeductibleValuee_BroadenedCollisionDeductible,
	i_SourceSystemID = 'DCT' AND i_CoverageType = 'Collision', DECODE(TRUE,
	v_CoverageDeductibleValue_Standard <> 'N/A', v_CoverageDeductibleValue_Standard,
	v_CoverageDeductibleValue_GlassBuyBack <> 'N/A', v_CoverageDeductibleValue_GlassBuyBack,
	v_CoverageDeductibleValue_Limited),
	'N/A') AS o_BroadenedCollisionDeductible,
	-- *INF*: IIF(i_SourceSystemID='DCT' AND i_CoverageType='UMBI',
	-- IIF(v_CoverageLimitValue_ValueEstimate='N/A', v_CoverageLimitValue_Value, v_CoverageLimitValue_ValueEstimate),
	-- 'N/A'
	-- )
	IFF(i_SourceSystemID = 'DCT' AND i_CoverageType = 'UMBI', IFF(v_CoverageLimitValue_ValueEstimate = 'N/A', v_CoverageLimitValue_Value, v_CoverageLimitValue_ValueEstimate), 'N/A') AS o_UnderinsuredMotoristBodilyInjuryLimit,
	-- *INF*: IIF(i_SourceSystemID='DCT' AND i_CoverageType='UIMPD',
	-- v_CoverageLimitValue_ValueEstimate,
	-- 'N/A'
	-- )
	IFF(i_SourceSystemID = 'DCT' AND i_CoverageType = 'UIMPD', v_CoverageLimitValue_ValueEstimate, 'N/A') AS o_UnderinsuredMotoristPropertyDamageLimit,
	-- *INF*: IIF(i_SourceSystemID='DCT' AND i_CoverageType='UMBI',
	-- IIF(v_CoverageLimitValue_ValueEstimate='N/A', v_CoverageLimitValue_Value, v_CoverageLimitValue_ValueEstimate),
	-- 'N/A'
	-- )
	IFF(i_SourceSystemID = 'DCT' AND i_CoverageType = 'UMBI', IFF(v_CoverageLimitValue_ValueEstimate = 'N/A', v_CoverageLimitValue_Value, v_CoverageLimitValue_ValueEstimate), 'N/A') AS o_UninsuredMotoristBodilyInjuryLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',
	-- v_CoverageLimitValue_UninsuredMotoristSingleLimit,
	-- i_SourceSystemID='DCT' AND i_CoverageType='UMPD',
	-- v_CoverageLimitValue_ValueEstimate,
	-- 'N/A'
	-- )
	DECODE(TRUE,
	i_SourceSystemID = 'PMS', v_CoverageLimitValue_UninsuredMotoristSingleLimit,
	i_SourceSystemID = 'DCT' AND i_CoverageType = 'UMPD', v_CoverageLimitValue_ValueEstimate,
	'N/A') AS o_UninsuredMotoristPropertyDamageLimit,
	-- *INF*: IIF(i_SourceSystemID='DCT' AND i_CoverageType='UMPD',
	-- v_CoverageDeductibleValue_Standard,
	-- 'N/A'
	-- )
	IFF(i_SourceSystemID = 'DCT' AND i_CoverageType = 'UMPD', v_CoverageDeductibleValue_Standard, 'N/A') AS o_UninsuredMotoristPropertyDamageDeductible,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',
	-- v_CoverageLimitValue_PersonalInjuryProtectionLimit,
	-- i_SourceSystemID='DCT' AND i_CoverageType='PropertyProtection',
	-- v_CoverageLimitValue_ValueEstimate,
	-- 'N/A'
	-- )
	DECODE(TRUE,
	i_SourceSystemID = 'PMS', v_CoverageLimitValue_PersonalInjuryProtectionLimit,
	i_SourceSystemID = 'DCT' AND i_CoverageType = 'PropertyProtection', v_CoverageLimitValue_ValueEstimate,
	'N/A') AS o_PropertyProtectionLimit,
	'N/A' AS o_PersonalInjuryProtectionWithoutStackingLimit,
	'N/A' AS o_PersonalInjuryProtectionWithStackingLimit,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',
	-- v_CoverageDeductibleValue_PersonalInjuryProtectionDeuctible,
	-- i_SourceSystemID='DCT' AND i_CoverageType='PIP',
	-- IIF(v_CoverageDeductibleValue_Standard='N/A', v_CoverageDeductibleValue_ManagedCareDeductible, v_CoverageDeductibleValue_Standard),
	-- 'N/A'
	-- )
	DECODE(TRUE,
	i_SourceSystemID = 'PMS', v_CoverageDeductibleValue_PersonalInjuryProtectionDeuctible,
	i_SourceSystemID = 'DCT' AND i_CoverageType = 'PIP', IFF(v_CoverageDeductibleValue_Standard = 'N/A', v_CoverageDeductibleValue_ManagedCareDeductible, v_CoverageDeductibleValue_Standard),
	'N/A') AS o_PersonalInjuryProtectionDeductible,
	-- *INF*: IIF(i_SourceSystemID='DCT' AND i_CoverageType='PIP',
	-- IIF(v_CoverageDeductibleValue_Standard='N/A', v_CoverageDeductibleValue_ManagedCareDeductible, v_CoverageDeductibleValue_Standard),
	-- 'N/A'
	-- )
	IFF(i_SourceSystemID = 'DCT' AND i_CoverageType = 'PIP', IFF(v_CoverageDeductibleValue_Standard = 'N/A', v_CoverageDeductibleValue_ManagedCareDeductible, v_CoverageDeductibleValue_Standard), 'N/A') AS o_PersonalInjuryProtectionWithoutStackingDeductible,
	-- *INF*: IIF(i_SourceSystemID='DCT' AND i_CoverageType='PIP',
	-- IIF(v_CoverageDeductibleValue_Standard='N/A', v_CoverageDeductibleValue_ManagedCareDeductible, v_CoverageDeductibleValue_Standard),
	-- 'N/A'
	-- )
	IFF(i_SourceSystemID = 'DCT' AND i_CoverageType = 'PIP', IFF(v_CoverageDeductibleValue_Standard = 'N/A', v_CoverageDeductibleValue_ManagedCareDeductible, v_CoverageDeductibleValue_Standard), 'N/A') AS o_PersonalInjuryProtectionWithStackingDeductible,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS' AND MajorPerilCode='130' ,
	-- DECODE(TRUE,
	-- v_CoverageLimitValue_PersonaInjuryProtectionExcessLimt<>'N/A',v_CoverageLimitValue_PersonaInjuryProtectionExcessLimt,
	-- 'N/A'),
	-- i_SourceSystemID='DCT' AND i_CoverageType='PIP',
	-- DECODE(TRUE,
	-- v_CoverageLimitValue_PersonaInjuryProtectionExcessLimt<>'N/A',v_CoverageLimitValue_PersonaInjuryProtectionExcessLimt,
	-- 'N/A'),
	-- 'N/A'
	-- ) 
	DECODE(TRUE,
	i_SourceSystemID = 'PMS' AND MajorPerilCode = '130', DECODE(TRUE,
	v_CoverageLimitValue_PersonaInjuryProtectionExcessLimt <> 'N/A', v_CoverageLimitValue_PersonaInjuryProtectionExcessLimt,
	'N/A'),
	i_SourceSystemID = 'DCT' AND i_CoverageType = 'PIP', DECODE(TRUE,
	v_CoverageLimitValue_PersonaInjuryProtectionExcessLimt <> 'N/A', v_CoverageLimitValue_PersonaInjuryProtectionExcessLimt,
	'N/A'),
	'N/A') AS o_PersonalInjuryProtectionExcessLimit,
	o_UsedInDumpingIndicator AS UsedInDumpingIndicator,
	o_VehicleTypeSize AS VehicleTypeSize,
	o_BusinessUseClass AS BusinessUseClass,
	o_SecondaryClass AS SecondaryClass,
	o_FleetType AS FleetType,
	o_SecondaryClassGroup AS SecondaryClassGroup,
	o_VIN AS VIN,
	o_VehicleNumber AS VehicleNumber,
	o_CoordinationOfBenefits AS CoordinationOfBenefits,
	o_MedicalExpensesOption AS MedicalExpensesOption,
	o_CoveredByWorkersCompensationFlag AS CoveredByWorkersCompensationFlag,
	o_HistoricVehicleIndicator AS HistoricVehicleIndicator
	FROM EXP_MetaData
	LEFT JOIN LKP_COVERAGELIMITVALUE LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_CombinedSingleLimit
	ON LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_CombinedSingleLimit.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_CombinedSingleLimit.CoverageLimitType = 'CombinedSingleLimit'

	LEFT JOIN LKP_COVERAGELIMITVALUE LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_MedicalPaymentLimit
	ON LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_MedicalPaymentLimit.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_MedicalPaymentLimit.CoverageLimitType = 'MedicalPaymentLimit'

	LEFT JOIN LKP_COVERAGELIMITVALUE LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_MedicalLimit
	ON LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_MedicalLimit.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_MedicalLimit.CoverageLimitType = 'MedicalLimit'

	LEFT JOIN LKP_COVERAGELIMITVALUE LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_UninsuredMotoristSingleLimit
	ON LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_UninsuredMotoristSingleLimit.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_UninsuredMotoristSingleLimit.CoverageLimitType = 'UninsuredMotoristSingleLimit'

	LEFT JOIN LKP_COVERAGELIMITVALUE LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_UnderinsuredMotoristSingleLimit
	ON LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_UnderinsuredMotoristSingleLimit.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_UnderinsuredMotoristSingleLimit.CoverageLimitType = 'UnderinsuredMotoristSingleLimit'

	LEFT JOIN LKP_COVERAGELIMITVALUE LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_ValueEstimate
	ON LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_ValueEstimate.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_ValueEstimate.CoverageLimitType = 'ValueEstimate'

	LEFT JOIN LKP_COVERAGELIMITVALUE LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_Value
	ON LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_Value.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_Value.CoverageLimitType = 'Value'

	LEFT JOIN LKP_COVERAGELIMITVALUE LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionLimit
	ON LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionLimit.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionLimit.CoverageLimitType = 'PersonalInjuryProtectionLimit'

	LEFT JOIN LKP_COVERAGELIMITVALUE LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionLimitWithStacking
	ON LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionLimitWithStacking.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionLimitWithStacking.CoverageLimitType = 'PersonalInjuryProtectionLimitWithStacking'

	LEFT JOIN LKP_COVERAGELIMITVALUE LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionLimitWithoutStacking
	ON LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionLimitWithoutStacking.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionLimitWithoutStacking.CoverageLimitType = 'PersonalInjuryProtectionLimitWithoutStacking'

	LEFT JOIN LKP_COVERAGELIMITVALUE LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionBasicLimit
	ON LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionBasicLimit.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionBasicLimit.CoverageLimitType = 'PersonalInjuryProtectionBasicLimit'

	LEFT JOIN LKP_COVERAGELIMITVALUE LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionExcessLimit
	ON LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionExcessLimit.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGELIMITVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionExcessLimit.CoverageLimitType = 'PersonalInjuryProtectionExcessLimit'

	LEFT JOIN LKP_COVERAGEDEDUCTIBLEVALUE LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_ComprehensiveDeductible
	ON LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_ComprehensiveDeductible.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_ComprehensiveDeductible.CoverageDeductibleType = 'ComprehensiveDeductible'

	LEFT JOIN LKP_COVERAGEDEDUCTIBLEVALUE LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_Standard
	ON LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_Standard.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_Standard.CoverageDeductibleType = 'Standard'

	LEFT JOIN LKP_COVERAGEDEDUCTIBLEVALUE LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_GlassBuyBack
	ON LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_GlassBuyBack.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_GlassBuyBack.CoverageDeductibleType = 'GlassBuyBack'

	LEFT JOIN LKP_COVERAGEDEDUCTIBLEVALUE LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_CollisionDeductible
	ON LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_CollisionDeductible.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_CollisionDeductible.CoverageDeductibleType = 'CollisionDeductible'

	LEFT JOIN LKP_COVERAGEDEDUCTIBLEVALUE LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_Limited
	ON LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_Limited.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_Limited.CoverageDeductibleType = 'Limited'

	LEFT JOIN LKP_COVERAGEDEDUCTIBLEVALUE LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_PropertyDamage
	ON LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_PropertyDamage.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_PropertyDamage.CoverageDeductibleType = 'PropertyDamage'

	LEFT JOIN LKP_COVERAGEDEDUCTIBLEVALUE LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_CSL
	ON LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_CSL.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_CSL.CoverageDeductibleType = 'CSL'

	LEFT JOIN LKP_COVERAGEDEDUCTIBLEVALUE LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionDeductible
	ON LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionDeductible.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_PersonalInjuryProtectionDeductible.CoverageDeductibleType = 'PersonalInjuryProtectionDeductible'

	LEFT JOIN LKP_COVERAGEDEDUCTIBLEVALUE LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_ManagedCareDeductible
	ON LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_ManagedCareDeductible.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_ManagedCareDeductible.CoverageDeductibleType = 'ManagedCareDeductible'

	LEFT JOIN LKP_COVERAGEDEDUCTIBLEVALUE LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_ComprehensiveFullGlassCoverageDeductible
	ON LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_ComprehensiveFullGlassCoverageDeductible.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_ComprehensiveFullGlassCoverageDeductible.CoverageDeductibleType = 'ComprehensiveFullGlassCoverageDeductible'

	LEFT JOIN LKP_COVERAGEDEDUCTIBLEVALUE LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_LimitedCollisionDeductible
	ON LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_LimitedCollisionDeductible.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_LimitedCollisionDeductible.CoverageDeductibleType = 'LimitedCollisionDeductible'

	LEFT JOIN LKP_COVERAGEDEDUCTIBLEVALUE LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_BroadenedCollisionDeductible
	ON LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_BroadenedCollisionDeductible.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_BroadenedCollisionDeductible.CoverageDeductibleType = 'BroadenedCollisionDeductible'

	LEFT JOIN LKP_COVERAGEDEDUCTIBLEVALUE LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_SingleLimitDeductible
	ON LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_SingleLimitDeductible.PremiumTransactionAKId = i_PremiumTransactionAKId
	AND LKP_COVERAGEDEDUCTIBLEVALUE_i_PremiumTransactionAKId_SingleLimitDeductible.CoverageDeductibleType = 'SingleLimitDeductible'

),
LKP_CoverageDetailCommercialAutoDim AS (
	SELECT
	CoverageDetailDimId,
	CoverageGuid,
	VehicleGroupCode,
	RadiusOfOperation,
	SecondaryVehicleType,
	CombinedSingleLimit,
	BodilyInjurySplitLimit,
	PhysicalDamageSplitLimit,
	MedicalPaymentLimit,
	UninsuredMotoristSingleLimit,
	UnderinsuredMotoristSingleLimit,
	PersonalInjuryProtectionLimit,
	PhysicalDamageLiabilityDeductible,
	SingleLimitDeductible,
	ComprehensiveDeductible,
	ComprehensiveFullGlassCoverageDeductible,
	CollisionDeductible,
	LimitedCollisionDeductible,
	PropertyDamageLiabilityDeductible,
	BroadenedCollisionDeductible,
	UnderinsuredMotoristBodilyInjuryLimit,
	UnderinsuredMotoristPropertyDamageLimit,
	UninsuredMotoristBodilyInjuryLimit,
	UninsuredMotoristPropertyDamageLimit,
	UninsuredMotoristPropertyDamageDeductible,
	PropertyProtectionLimit,
	PersonalInjuryProtectionDeductible,
	UsedInDumpingIndicator,
	VehicleYear,
	StatedAmount,
	CostNew,
	VehicleDeleteDate,
	CompositeRatedFlag,
	PersonalInjuryProtectionExcessLimit,
	VehicleTypeSize,
	BusinessUseClass,
	SecondaryClass,
	FleetType,
	SecondaryClassGroup,
	VIN,
	VehicleNumber,
	CoordinationOfBenefits,
	CoveredByWorkersCompensationFlag,
	MedicalExpensesOption,
	HistoricVehicleIndicator,
	i_CoverageDetailDimId
	FROM (
		SELECT CDCAD.CoverageGuid as CoverageGuid, CDCAD.VehicleGroupCode as VehicleGroupCode, CDCAD.RadiusOfOperation as RadiusOfOperation, CDCAD.SecondaryVehicleType as SecondaryVehicleType, CDCAD.CombinedSingleLimit as CombinedSingleLimit, CDCAD.BodilyInjurySplitLimit as BodilyInjurySplitLimit, CDCAD.PhysicalDamageSplitLimit as PhysicalDamageSplitLimit, CDCAD.MedicalPaymentLimit as MedicalPaymentLimit, CDCAD.UninsuredMotoristSingleLimit as UninsuredMotoristSingleLimit, CDCAD.UnderinsuredMotoristSingleLimit as UnderinsuredMotoristSingleLimit, CDCAD.PersonalInjuryProtectionLimit as PersonalInjuryProtectionLimit, CDCAD.PhysicalDamageLiabilityDeductible as PhysicalDamageLiabilityDeductible, CDCAD.SingleLimitDeductible as SingleLimitDeductible, CDCAD.ComprehensiveDeductible as ComprehensiveDeductible, CDCAD.ComprehensiveFullGlassCoverageDeductible as ComprehensiveFullGlassCoverageDeductible, CDCAD.CollisionDeductible as CollisionDeductible, CDCAD.LimitedCollisionDeductible as LimitedCollisionDeductible, CDCAD.PropertyDamageLiabilityDeductible as PropertyDamageLiabilityDeductible, CDCAD.BroadenedCollisionDeductible as BroadenedCollisionDeductible, CDCAD.UnderinsuredMotoristBodilyInjuryLimit as UnderinsuredMotoristBodilyInjuryLimit, CDCAD.UnderinsuredMotoristPropertyDamageLimit as UnderinsuredMotoristPropertyDamageLimit, CDCAD.UninsuredMotoristBodilyInjuryLimit as UninsuredMotoristBodilyInjuryLimit, CDCAD.UninsuredMotoristPropertyDamageLimit as UninsuredMotoristPropertyDamageLimit, CDCAD.UninsuredMotoristPropertyDamageDeductible as UninsuredMotoristPropertyDamageDeductible, CDCAD.PropertyProtectionLimit as PropertyProtectionLimit, CDCAD.PersonalInjuryProtectionDeductible as PersonalInjuryProtectionDeductible, CDCAD.UsedInDumpingIndicator as UsedInDumpingIndicator, CDCAD.VehicleYear as VehicleYear, CDCAD.StatedAmount as StatedAmount, CDCAD.CostNew as CostNew, CDCAD.VehicleDeleteDate as VehicleDeleteDate, CDCAD.CompositeRatedFlag as CompositeRatedFlag, CDCAD.PersonalInjuryProtectionExcessLimit as PersonalInjuryProtectionExcessLimit, CDCAD.VehicleTypeSize as VehicleTypeSize, CDCAD.BusinessUseClass as BusinessUseClass, CDCAD.SecondaryClass as SecondaryClass, CDCAD.FleetType as FleetType, CDCAD.SecondaryClassGroup as SecondaryClassGroup, CDCAD.VIN as VIN, CDCAD.VehicleNumber as VehicleNumber, CDCAD.CoordinationOfBenefits as CoordinationOfBenefits, CDCAD.CoveredByWorkersCompensationFlag as CoveredByWorkersCompensationFlag, CDCAD.MedicalExpensesOption as MedicalExpensesOption, CDCAD.CoverageDetailDimId as CoverageDetailDimId, CDCAD.HistoricVehicleIndicator as HistoricVehicleIndicator
		FROM 
		@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAutoDim CDCAD
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
		ON CDCAD.CoverageDetailDimId = CDD.CoverageDetailDimId
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CDCA
		ON CDCA.PremiumTransactionID=CDD.EDWPremiumTransactionPKId
		WHERE CDD.modifeddate > '@{pipeline().parameters.SELECTION_START_TS}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageDetailDimId ORDER BY CoverageDetailDimId) = 1
),
EXP_Tgt AS (
	SELECT
	EXP_Business_Logic.CoverageDetailDimId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	EXP_Business_Logic.EffectiveDate,
	EXP_Business_Logic.ExpirationDate,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	EXP_Business_Logic.CoverageGuid,
	EXP_Business_Logic.VehicleType,
	EXP_Business_Logic.RadiusOfOperation,
	EXP_Business_Logic.SecondaryVehicleType,
	EXP_Business_Logic.o_CombinedSingleLimit AS CombinedSingleLimit,
	EXP_Business_Logic.o_BodilyInjurySplitLimit AS BodilyInjurySplitLimit,
	EXP_Business_Logic.o_PhysicalDamageSplitLimit AS PhysicalDamageSplitLimit,
	EXP_Business_Logic.o_MedicalPaymentLimit AS MedicalPaymentLimit,
	EXP_Business_Logic.o_UninsuredMotoristSingleLimit AS UninsuredMotoristSingleLimit,
	EXP_Business_Logic.o_UnderinsuredMotoristSingleLimit AS UnderinsuredMotoristSingleLimit,
	EXP_Business_Logic.o_PersonalInjuryProtectionLimit AS PersonalInjuryProtectionLimit,
	EXP_Business_Logic.o_PhysicalDamageLiabilityDeductible AS PhysicalDamageLiabilityDeductible,
	EXP_Business_Logic.o_SingleLimitDeductible AS SingleLimitDeductible,
	EXP_Business_Logic.o_ComprehensiveDeductible AS ComprehensiveDeductible,
	EXP_Business_Logic.o_ComprehensiveFullGlassCoverageDeductible AS ComprehensiveFullGlassCoverageDeductible,
	EXP_Business_Logic.o_CollisionDeductible AS CollisionDeductible,
	EXP_Business_Logic.o_LimitedCollisionDeductible AS LimitedCollisionDeductible,
	EXP_Business_Logic.o_PropertyDamageLiabilityDeductible AS PropertyDamageLiabilityDeductible,
	EXP_Business_Logic.o_BroadenedCollisionDeductible AS BroadenedCollisionDeductible,
	EXP_Business_Logic.o_UnderinsuredMotoristBodilyInjuryLimit AS UnderinsuredMotoristBodilyInjuryLimit,
	EXP_Business_Logic.o_UnderinsuredMotoristPropertyDamageLimit AS UnderinsuredMotoristPropertyDamageLimit,
	EXP_Business_Logic.o_UninsuredMotoristBodilyInjuryLimit AS UninsuredMotoristBodilyInjuryLimit,
	EXP_Business_Logic.o_UninsuredMotoristPropertyDamageLimit AS UninsuredMotoristPropertyDamageLimit,
	EXP_Business_Logic.o_UninsuredMotoristPropertyDamageDeductible AS UninsuredMotoristPropertyDamageDeductible,
	EXP_Business_Logic.o_PropertyProtectionLimit AS PropertyProtectionLimit,
	EXP_Business_Logic.o_PersonalInjuryProtectionWithoutStackingLimit AS PersonalInjuryProtectionWithoutStackingLimit,
	EXP_Business_Logic.o_PersonalInjuryProtectionWithStackingLimit AS PersonalInjuryProtectionWithStackingLimit,
	EXP_Business_Logic.o_PersonalInjuryProtectionDeductible AS PersonalInjuryProtectionDeductible,
	EXP_Business_Logic.o_PersonalInjuryProtectionWithoutStackingDeductible AS PersonalInjuryProtectionWithoutStackingDeductible,
	EXP_Business_Logic.o_PersonalInjuryProtectionWithStackingDeductible AS PersonalInjuryProtectionWithStackingDeductible,
	EXP_Business_Logic.o_PersonalInjuryProtectionExcessLimit AS PersonalInjuryProtectionExcessLimit,
	EXP_Business_Logic.UsedInDumpingIndicator,
	LKP_CoverageDetailCommercialAutoDim.CoverageDetailDimId AS LKP_CoverageDetailDimId,
	EXP_Business_Logic.VehicleTypeSize,
	EXP_Business_Logic.BusinessUseClass,
	EXP_Business_Logic.SecondaryClass,
	EXP_Business_Logic.FleetType,
	EXP_Business_Logic.SecondaryClassGroup,
	EXP_Business_Logic.VehicleYear,
	EXP_Business_Logic.StatedAmount,
	EXP_Business_Logic.CostNew,
	EXP_Business_Logic.VehicleDeleteDate,
	EXP_Business_Logic.CompositeRatedFlag,
	-- *INF*: IIF(ISNULL(LKP_CoverageDetailDimId),'NEW',
	-- IIF(lkp_CoverageGuid=CoverageGuid and
	-- lkp_VehicleGroupCode=VehicleType
	-- and 
	-- lkp_RadiusOfOperation=RadiusOfOperation and 
	-- lkp_SecondaryVehicleType=SecondaryVehicleType and 
	-- lkp_CombinedSingleLimit=CombinedSingleLimit and 
	-- lkp_BodilyInjurySplitLimit=BodilyInjurySplitLimit and 
	-- lkp_PhysicalDamageSplitLimit=PhysicalDamageSplitLimit and 
	-- lkp_MedicalPaymentLimit=MedicalPaymentLimit and 
	-- lkp_UninsuredMotoristSingleLimit=UninsuredMotoristSingleLimit and 
	-- lkp_UnderinsuredMotoristSingleLimit=UnderinsuredMotoristSingleLimit and 
	-- lkp_PersonalInjuryProtectionLimit=PersonalInjuryProtectionLimit and 
	-- lkp_PhysicalDamageLiabilityDeductible=PhysicalDamageLiabilityDeductible and 
	-- lkp_SingleLimitDeductible=SingleLimitDeductible and 
	-- lkp_ComprehensiveDeductible=ComprehensiveDeductible and 
	-- lkp_ComprehensiveFullGlassCoverageDeductible=ComprehensiveFullGlassCoverageDeductible and 
	-- lkp_CollisionDeductible=CollisionDeductible and 
	-- lkp_LimitedCollisionDeductible=LimitedCollisionDeductible and 
	-- lkp_PropertyDamageLiabilityDeductible=PropertyDamageLiabilityDeductible and 
	-- lkp_BroadenedCollisionDeductible=BroadenedCollisionDeductible and 
	-- lkp_UnderinsuredMotoristBodilyInjuryLimit=UnderinsuredMotoristBodilyInjuryLimit and 
	-- lkp_UnderinsuredMotoristPropertyDamageLimit=UnderinsuredMotoristPropertyDamageLimit and 
	-- lkp_UninsuredMotoristBodilyInjuryLimit=UninsuredMotoristBodilyInjuryLimit and 
	-- lkp_UninsuredMotoristPropertyDamageLimit=UninsuredMotoristPropertyDamageLimit and 
	-- lkp_UninsuredMotoristPropertyDamageDeductible=UninsuredMotoristPropertyDamageDeductible and 
	-- lkp_PropertyProtectionLimit=PropertyProtectionLimit and 
	-- lkp_PersonalInjuryProtectionDeductible=PersonalInjuryProtectionDeductible and 
	-- lkp_UsedInDumpingIndicator=UsedInDumpingIndicator and 
	-- lkp_VehicleYear=VehicleYear and 
	-- lkp_StatedAmount=StatedAmount and 
	-- lkp_CostNew=CostNew and 
	-- lkp_VehicleDeleteDate=VehicleDeleteDate and 
	-- iif(lkp_CompositeRatedFlag='T',1,0)=CompositeRatedFlag and 
	-- lkp_PersonalInjuryProtectionExcessLimit=PersonalInjuryProtectionExcessLimit and 
	-- lkp_VehicleTypeSize=VehicleTypeSize and 
	-- lkp_BusinessUseClass=BusinessUseClass and 
	-- lkp_SecondaryClass=SecondaryClass and 
	-- lkp_FleetType=FleetType and 
	-- lkp_SecondaryClassGroup=SecondaryClassGroup and 
	-- lkp_VIN=VIN and 
	-- lkp_VehicleNumber=VehicleNumber and 
	-- lkp_CoordinationOfBenefits=CoordinationOfBenefits and 
	-- iif(
	-- lkp_CoveredByWorkersCompensationFlag='T',1,0)=CoveredByWorkersCompensationFlag and 
	-- lkp_MedicalExpensesOption=MedicalExpensesOption and
	-- lkp_HistoricVehicleIndicator=HistoricVehicleIndicator
	--  
	-- ,'UNCHANGED','UPDATE'))
	IFF(LKP_CoverageDetailDimId IS NULL, 'NEW', IFF(lkp_CoverageGuid = CoverageGuid AND lkp_VehicleGroupCode = VehicleType AND lkp_RadiusOfOperation = RadiusOfOperation AND lkp_SecondaryVehicleType = SecondaryVehicleType AND lkp_CombinedSingleLimit = CombinedSingleLimit AND lkp_BodilyInjurySplitLimit = BodilyInjurySplitLimit AND lkp_PhysicalDamageSplitLimit = PhysicalDamageSplitLimit AND lkp_MedicalPaymentLimit = MedicalPaymentLimit AND lkp_UninsuredMotoristSingleLimit = UninsuredMotoristSingleLimit AND lkp_UnderinsuredMotoristSingleLimit = UnderinsuredMotoristSingleLimit AND lkp_PersonalInjuryProtectionLimit = PersonalInjuryProtectionLimit AND lkp_PhysicalDamageLiabilityDeductible = PhysicalDamageLiabilityDeductible AND lkp_SingleLimitDeductible = SingleLimitDeductible AND lkp_ComprehensiveDeductible = ComprehensiveDeductible AND lkp_ComprehensiveFullGlassCoverageDeductible = ComprehensiveFullGlassCoverageDeductible AND lkp_CollisionDeductible = CollisionDeductible AND lkp_LimitedCollisionDeductible = LimitedCollisionDeductible AND lkp_PropertyDamageLiabilityDeductible = PropertyDamageLiabilityDeductible AND lkp_BroadenedCollisionDeductible = BroadenedCollisionDeductible AND lkp_UnderinsuredMotoristBodilyInjuryLimit = UnderinsuredMotoristBodilyInjuryLimit AND lkp_UnderinsuredMotoristPropertyDamageLimit = UnderinsuredMotoristPropertyDamageLimit AND lkp_UninsuredMotoristBodilyInjuryLimit = UninsuredMotoristBodilyInjuryLimit AND lkp_UninsuredMotoristPropertyDamageLimit = UninsuredMotoristPropertyDamageLimit AND lkp_UninsuredMotoristPropertyDamageDeductible = UninsuredMotoristPropertyDamageDeductible AND lkp_PropertyProtectionLimit = PropertyProtectionLimit AND lkp_PersonalInjuryProtectionDeductible = PersonalInjuryProtectionDeductible AND lkp_UsedInDumpingIndicator = UsedInDumpingIndicator AND lkp_VehicleYear = VehicleYear AND lkp_StatedAmount = StatedAmount AND lkp_CostNew = CostNew AND lkp_VehicleDeleteDate = VehicleDeleteDate AND IFF(lkp_CompositeRatedFlag = 'T', 1, 0) = CompositeRatedFlag AND lkp_PersonalInjuryProtectionExcessLimit = PersonalInjuryProtectionExcessLimit AND lkp_VehicleTypeSize = VehicleTypeSize AND lkp_BusinessUseClass = BusinessUseClass AND lkp_SecondaryClass = SecondaryClass AND lkp_FleetType = FleetType AND lkp_SecondaryClassGroup = SecondaryClassGroup AND lkp_VIN = VIN AND lkp_VehicleNumber = VehicleNumber AND lkp_CoordinationOfBenefits = CoordinationOfBenefits AND IFF(lkp_CoveredByWorkersCompensationFlag = 'T', 1, 0) = CoveredByWorkersCompensationFlag AND lkp_MedicalExpensesOption = MedicalExpensesOption AND lkp_HistoricVehicleIndicator = HistoricVehicleIndicator, 'UNCHANGED', 'UPDATE')) AS ChangeFlag,
	EXP_Business_Logic.VIN,
	EXP_Business_Logic.VehicleNumber,
	EXP_Business_Logic.CoordinationOfBenefits,
	EXP_Business_Logic.MedicalExpensesOption,
	EXP_Business_Logic.CoveredByWorkersCompensationFlag,
	EXP_Business_Logic.HistoricVehicleIndicator,
	LKP_CoverageDetailCommercialAutoDim.CoverageGuid AS lkp_CoverageGuid,
	LKP_CoverageDetailCommercialAutoDim.VehicleGroupCode AS lkp_VehicleGroupCode,
	LKP_CoverageDetailCommercialAutoDim.RadiusOfOperation AS lkp_RadiusOfOperation,
	LKP_CoverageDetailCommercialAutoDim.SecondaryVehicleType AS lkp_SecondaryVehicleType,
	LKP_CoverageDetailCommercialAutoDim.CombinedSingleLimit AS lkp_CombinedSingleLimit,
	LKP_CoverageDetailCommercialAutoDim.BodilyInjurySplitLimit AS lkp_BodilyInjurySplitLimit,
	LKP_CoverageDetailCommercialAutoDim.PhysicalDamageSplitLimit AS lkp_PhysicalDamageSplitLimit,
	LKP_CoverageDetailCommercialAutoDim.MedicalPaymentLimit AS lkp_MedicalPaymentLimit,
	LKP_CoverageDetailCommercialAutoDim.UninsuredMotoristSingleLimit AS lkp_UninsuredMotoristSingleLimit,
	LKP_CoverageDetailCommercialAutoDim.UnderinsuredMotoristSingleLimit AS lkp_UnderinsuredMotoristSingleLimit,
	LKP_CoverageDetailCommercialAutoDim.PersonalInjuryProtectionLimit AS lkp_PersonalInjuryProtectionLimit,
	LKP_CoverageDetailCommercialAutoDim.PhysicalDamageLiabilityDeductible AS lkp_PhysicalDamageLiabilityDeductible,
	LKP_CoverageDetailCommercialAutoDim.SingleLimitDeductible AS lkp_SingleLimitDeductible,
	LKP_CoverageDetailCommercialAutoDim.ComprehensiveDeductible AS lkp_ComprehensiveDeductible,
	LKP_CoverageDetailCommercialAutoDim.ComprehensiveFullGlassCoverageDeductible AS lkp_ComprehensiveFullGlassCoverageDeductible,
	LKP_CoverageDetailCommercialAutoDim.CollisionDeductible AS lkp_CollisionDeductible,
	LKP_CoverageDetailCommercialAutoDim.LimitedCollisionDeductible AS lkp_LimitedCollisionDeductible,
	LKP_CoverageDetailCommercialAutoDim.PropertyDamageLiabilityDeductible AS lkp_PropertyDamageLiabilityDeductible,
	LKP_CoverageDetailCommercialAutoDim.BroadenedCollisionDeductible AS lkp_BroadenedCollisionDeductible,
	LKP_CoverageDetailCommercialAutoDim.UnderinsuredMotoristBodilyInjuryLimit AS lkp_UnderinsuredMotoristBodilyInjuryLimit,
	LKP_CoverageDetailCommercialAutoDim.UnderinsuredMotoristPropertyDamageLimit AS lkp_UnderinsuredMotoristPropertyDamageLimit,
	LKP_CoverageDetailCommercialAutoDim.UninsuredMotoristBodilyInjuryLimit AS lkp_UninsuredMotoristBodilyInjuryLimit,
	LKP_CoverageDetailCommercialAutoDim.UninsuredMotoristPropertyDamageLimit AS lkp_UninsuredMotoristPropertyDamageLimit,
	LKP_CoverageDetailCommercialAutoDim.UninsuredMotoristPropertyDamageDeductible AS lkp_UninsuredMotoristPropertyDamageDeductible,
	LKP_CoverageDetailCommercialAutoDim.PropertyProtectionLimit AS lkp_PropertyProtectionLimit,
	LKP_CoverageDetailCommercialAutoDim.PersonalInjuryProtectionDeductible AS lkp_PersonalInjuryProtectionDeductible,
	LKP_CoverageDetailCommercialAutoDim.UsedInDumpingIndicator AS lkp_UsedInDumpingIndicator,
	LKP_CoverageDetailCommercialAutoDim.VehicleYear AS lkp_VehicleYear,
	LKP_CoverageDetailCommercialAutoDim.StatedAmount AS lkp_StatedAmount,
	LKP_CoverageDetailCommercialAutoDim.CostNew AS lkp_CostNew,
	LKP_CoverageDetailCommercialAutoDim.VehicleDeleteDate AS lkp_VehicleDeleteDate,
	LKP_CoverageDetailCommercialAutoDim.CompositeRatedFlag AS lkp_CompositeRatedFlag,
	LKP_CoverageDetailCommercialAutoDim.PersonalInjuryProtectionExcessLimit AS lkp_PersonalInjuryProtectionExcessLimit,
	LKP_CoverageDetailCommercialAutoDim.VehicleTypeSize AS lkp_VehicleTypeSize,
	LKP_CoverageDetailCommercialAutoDim.BusinessUseClass AS lkp_BusinessUseClass,
	LKP_CoverageDetailCommercialAutoDim.SecondaryClass AS lkp_SecondaryClass,
	LKP_CoverageDetailCommercialAutoDim.FleetType AS lkp_FleetType,
	LKP_CoverageDetailCommercialAutoDim.SecondaryClassGroup AS lkp_SecondaryClassGroup,
	LKP_CoverageDetailCommercialAutoDim.VIN AS lkp_VIN,
	LKP_CoverageDetailCommercialAutoDim.VehicleNumber AS lkp_VehicleNumber,
	LKP_CoverageDetailCommercialAutoDim.CoordinationOfBenefits AS lkp_CoordinationOfBenefits,
	LKP_CoverageDetailCommercialAutoDim.CoveredByWorkersCompensationFlag AS lkp_CoveredByWorkersCompensationFlag,
	LKP_CoverageDetailCommercialAutoDim.MedicalExpensesOption AS lkp_MedicalExpensesOption,
	LKP_CoverageDetailCommercialAutoDim.HistoricVehicleIndicator AS lkp_HistoricVehicleIndicator
	FROM EXP_Business_Logic
	LEFT JOIN LKP_CoverageDetailCommercialAutoDim
	ON LKP_CoverageDetailCommercialAutoDim.CoverageDetailDimId = EXP_Business_Logic.CoverageDetailDimId
),
RTR_INSERT_UPDATE AS (
	SELECT
	CoverageDetailDimId,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	CreatedDate,
	ModifiedDate,
	CoverageGuid,
	VehicleType,
	RadiusOfOperation,
	SecondaryVehicleType,
	CombinedSingleLimit,
	BodilyInjurySplitLimit,
	PhysicalDamageSplitLimit,
	MedicalPaymentLimit,
	UninsuredMotoristSingleLimit,
	UnderinsuredMotoristSingleLimit,
	PersonalInjuryProtectionLimit,
	PhysicalDamageLiabilityDeductible,
	SingleLimitDeductible,
	ComprehensiveDeductible,
	ComprehensiveFullGlassCoverageDeductible,
	CollisionDeductible,
	LimitedCollisionDeductible,
	PropertyDamageLiabilityDeductible,
	BroadenedCollisionDeductible,
	UnderinsuredMotoristBodilyInjuryLimit,
	UnderinsuredMotoristPropertyDamageLimit,
	UninsuredMotoristBodilyInjuryLimit,
	UninsuredMotoristPropertyDamageLimit,
	UninsuredMotoristPropertyDamageDeductible,
	PropertyProtectionLimit,
	PersonalInjuryProtectionWithoutStackingLimit,
	PersonalInjuryProtectionWithStackingLimit,
	PersonalInjuryProtectionDeductible,
	PersonalInjuryProtectionWithoutStackingDeductible,
	PersonalInjuryProtectionWithStackingDeductible,
	PersonalInjuryProtectionExcessLimit,
	UsedInDumpingIndicator,
	VehicleYear,
	StatedAmount,
	CostNew,
	VehicleDeleteDate,
	CompositeRatedFlag,
	VehicleTypeSize,
	BusinessUseClass,
	SecondaryClass,
	FleetType,
	SecondaryClassGroup,
	ChangeFlag,
	VIN,
	VehicleNumber,
	CoordinationOfBenefits,
	CoveredByWorkersCompensationFlag,
	MedicalExpensesOption,
	HistoricVehicleIndicator
	FROM EXP_Tgt
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE ChangeFlag='NEW'),
RTR_INSERT_UPDATE_UPDATE AS (SELECT * FROM RTR_INSERT_UPDATE WHERE ChangeFlag='UPDATE'),
CoverageDetailCommercialAutoDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAutoDim
	(CoverageDetailDimId, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, CoverageGuid, VehicleGroupCode, RadiusOfOperation, SecondaryVehicleType, CombinedSingleLimit, BodilyInjurySplitLimit, PhysicalDamageSplitLimit, MedicalPaymentLimit, UninsuredMotoristSingleLimit, UnderinsuredMotoristSingleLimit, PersonalInjuryProtectionLimit, PhysicalDamageLiabilityDeductible, SingleLimitDeductible, ComprehensiveDeductible, ComprehensiveFullGlassCoverageDeductible, CollisionDeductible, LimitedCollisionDeductible, PropertyDamageLiabilityDeductible, BroadenedCollisionDeductible, UnderinsuredMotoristBodilyInjuryLimit, UnderinsuredMotoristPropertyDamageLimit, UninsuredMotoristBodilyInjuryLimit, UninsuredMotoristPropertyDamageLimit, UninsuredMotoristPropertyDamageDeductible, PropertyProtectionLimit, PersonalInjuryProtectionDeductible, UsedInDumpingIndicator, VehicleYear, StatedAmount, CostNew, VehicleDeleteDate, CompositeRatedFlag, PersonalInjuryProtectionExcessLimit, VehicleTypeSize, BusinessUseClass, SecondaryClass, FleetType, SecondaryClassGroup, VIN, VehicleNumber, CoordinationOfBenefits, CoveredByWorkersCompensationFlag, MedicalExpensesOption, HistoricVehicleIndicator)
	SELECT 
	COVERAGEDETAILDIMID, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	COVERAGEGUID, 
	VehicleType AS VEHICLEGROUPCODE, 
	RADIUSOFOPERATION, 
	SECONDARYVEHICLETYPE, 
	COMBINEDSINGLELIMIT, 
	BODILYINJURYSPLITLIMIT, 
	PHYSICALDAMAGESPLITLIMIT, 
	MEDICALPAYMENTLIMIT, 
	UNINSUREDMOTORISTSINGLELIMIT, 
	UNDERINSUREDMOTORISTSINGLELIMIT, 
	PERSONALINJURYPROTECTIONLIMIT, 
	PHYSICALDAMAGELIABILITYDEDUCTIBLE, 
	SINGLELIMITDEDUCTIBLE, 
	COMPREHENSIVEDEDUCTIBLE, 
	COMPREHENSIVEFULLGLASSCOVERAGEDEDUCTIBLE, 
	COLLISIONDEDUCTIBLE, 
	LIMITEDCOLLISIONDEDUCTIBLE, 
	PROPERTYDAMAGELIABILITYDEDUCTIBLE, 
	BROADENEDCOLLISIONDEDUCTIBLE, 
	UNDERINSUREDMOTORISTBODILYINJURYLIMIT, 
	UNDERINSUREDMOTORISTPROPERTYDAMAGELIMIT, 
	UNINSUREDMOTORISTBODILYINJURYLIMIT, 
	UNINSUREDMOTORISTPROPERTYDAMAGELIMIT, 
	UNINSUREDMOTORISTPROPERTYDAMAGEDEDUCTIBLE, 
	PROPERTYPROTECTIONLIMIT, 
	PERSONALINJURYPROTECTIONDEDUCTIBLE, 
	USEDINDUMPINGINDICATOR, 
	VEHICLEYEAR, 
	STATEDAMOUNT, 
	COSTNEW, 
	VEHICLEDELETEDATE, 
	COMPOSITERATEDFLAG, 
	PERSONALINJURYPROTECTIONEXCESSLIMIT, 
	VEHICLETYPESIZE, 
	BUSINESSUSECLASS, 
	SECONDARYCLASS, 
	FLEETTYPE, 
	SECONDARYCLASSGROUP, 
	VIN, 
	VEHICLENUMBER, 
	COORDINATIONOFBENEFITS, 
	COVEREDBYWORKERSCOMPENSATIONFLAG, 
	MEDICALEXPENSESOPTION, 
	HISTORICVEHICLEINDICATOR
	FROM RTR_INSERT_UPDATE_INSERT
),
UPD_Exists AS (
	SELECT
	CoverageDetailDimId AS CoverageDetailDimId3, 
	AuditID AS AuditID3, 
	EffectiveDate AS EffectiveDate3, 
	ExpirationDate AS ExpirationDate3, 
	ModifiedDate AS ModifiedDate3, 
	CoverageGuid AS CoverageGuid3, 
	VehicleType AS VehicleType3, 
	RadiusOfOperation AS RadiusOfOperation3, 
	SecondaryVehicleType AS SecondaryVehicleType3, 
	CombinedSingleLimit AS CombinedSingleLimit3, 
	BodilyInjurySplitLimit AS BodilyInjurySplitLimit3, 
	PhysicalDamageSplitLimit AS PhysicalDamageSplitLimit3, 
	MedicalPaymentLimit AS MedicalPaymentLimit3, 
	UninsuredMotoristSingleLimit AS UninsuredMotoristSingleLimit3, 
	UnderinsuredMotoristSingleLimit AS UnderinsuredMotoristSingleLimit3, 
	PersonalInjuryProtectionLimit AS PersonalInjuryProtectionLimit3, 
	PhysicalDamageLiabilityDeductible AS PhysicalDamageLiabilityDeductible3, 
	SingleLimitDeductible AS SingleLimitDeductible3, 
	ComprehensiveDeductible AS ComprehensiveDeductible3, 
	ComprehensiveFullGlassCoverageDeductible AS ComprehensiveFullGlassCoverageDeductible3, 
	CollisionDeductible AS CollisionDeductible3, 
	LimitedCollisionDeductible AS LimitedCollisionDeductible3, 
	PropertyDamageLiabilityDeductible AS PropertyDamageLiabilityDeductible3, 
	BroadenedCollisionDeductible AS BroadenedCollisionDeductible3, 
	UnderinsuredMotoristBodilyInjuryLimit AS UnderinsuredMotoristBodilyInjuryLimit3, 
	UnderinsuredMotoristPropertyDamageLimit AS UnderinsuredMotoristPropertyDamageLimit3, 
	UninsuredMotoristBodilyInjuryLimit AS UninsuredMotoristBodilyInjuryLimit3, 
	UninsuredMotoristPropertyDamageLimit AS UninsuredMotoristPropertyDamageLimit3, 
	UninsuredMotoristPropertyDamageDeductible AS UninsuredMotoristPropertyDamageDeductible3, 
	PropertyProtectionLimit AS PropertyProtectionLimit3, 
	PersonalInjuryProtectionWithoutStackingLimit AS PersonalInjuryProtectionWithoutStackingLimit3, 
	PersonalInjuryProtectionWithStackingLimit AS PersonalInjuryProtectionWithStackingLimit3, 
	PersonalInjuryProtectionDeductible AS PersonalInjuryProtectionDeductible3, 
	PersonalInjuryProtectionWithoutStackingDeductible AS PersonalInjuryProtectionWithoutStackingDeductible3, 
	PersonalInjuryProtectionWithStackingDeductible AS PersonalInjuryProtectionWithStackingDeductible3, 
	PersonalInjuryProtectionExcessLimit AS PersonalInjuryProtectionExcessLimit3, 
	UsedInDumpingIndicator AS UsedInDumpingIndicator3, 
	VehicleYear AS VehicleYear3, 
	StatedAmount AS StatedAmount3, 
	CostNew AS CostNew3, 
	VehicleDeleteDate AS VehicleDeleteDate3, 
	CompositeRatedFlag AS CompositeRatedFlag3, 
	VehicleTypeSize AS VehicleTypeSize3, 
	BusinessUseClass AS BusinessUseClass3, 
	SecondaryClass AS SecondaryClass3, 
	FleetType AS FleetType3, 
	SecondaryClassGroup AS SecondaryClassGroup3, 
	VIN AS VIN3, 
	VehicleNumber AS VehicleNumber3, 
	CoordinationOfBenefits AS CoordinationOfBenefits3, 
	CoveredByWorkersCompensationFlag AS CoveredByWorkersCompensationFlag3, 
	MedicalExpensesOption AS MedicalExpensesOption3, 
	HistoricVehicleIndicator AS HistoricVehicleIndicator3
	FROM RTR_INSERT_UPDATE_UPDATE
),
CoverageDetailCommercialAutoDim_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAutoDim AS T
	USING UPD_Exists AS S
	ON 
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CoverageDetailDimId = S.CoverageDetailDimId3, T.AuditID = S.AuditID3, T.EffectiveDate = S.EffectiveDate3, T.ExpirationDate = S.ExpirationDate3, T.ModifiedDate = S.ModifiedDate3, T.CoverageGuid = S.CoverageGuid3, T.VehicleGroupCode = S.VehicleType3, T.RadiusOfOperation = S.RadiusOfOperation3, T.SecondaryVehicleType = S.SecondaryVehicleType3, T.CombinedSingleLimit = S.CombinedSingleLimit3, T.BodilyInjurySplitLimit = S.BodilyInjurySplitLimit3, T.PhysicalDamageSplitLimit = S.PhysicalDamageSplitLimit3, T.MedicalPaymentLimit = S.MedicalPaymentLimit3, T.UninsuredMotoristSingleLimit = S.UninsuredMotoristSingleLimit3, T.UnderinsuredMotoristSingleLimit = S.UnderinsuredMotoristSingleLimit3, T.PersonalInjuryProtectionLimit = S.PersonalInjuryProtectionLimit3, T.PhysicalDamageLiabilityDeductible = S.PhysicalDamageLiabilityDeductible3, T.SingleLimitDeductible = S.SingleLimitDeductible3, T.ComprehensiveDeductible = S.ComprehensiveDeductible3, T.ComprehensiveFullGlassCoverageDeductible = S.ComprehensiveFullGlassCoverageDeductible3, T.CollisionDeductible = S.CollisionDeductible3, T.LimitedCollisionDeductible = S.LimitedCollisionDeductible3, T.PropertyDamageLiabilityDeductible = S.PropertyDamageLiabilityDeductible3, T.BroadenedCollisionDeductible = S.BroadenedCollisionDeductible3, T.UnderinsuredMotoristBodilyInjuryLimit = S.UnderinsuredMotoristBodilyInjuryLimit3, T.UnderinsuredMotoristPropertyDamageLimit = S.UnderinsuredMotoristPropertyDamageLimit3, T.UninsuredMotoristBodilyInjuryLimit = S.UninsuredMotoristBodilyInjuryLimit3, T.UninsuredMotoristPropertyDamageLimit = S.UninsuredMotoristPropertyDamageLimit3, T.UninsuredMotoristPropertyDamageDeductible = S.UninsuredMotoristPropertyDamageDeductible3, T.PropertyProtectionLimit = S.PropertyProtectionLimit3, T.PersonalInjuryProtectionDeductible = S.PersonalInjuryProtectionDeductible3, T.UsedInDumpingIndicator = S.UsedInDumpingIndicator3, T.VehicleYear = S.VehicleYear3, T.StatedAmount = S.StatedAmount3, T.CostNew = S.CostNew3, T.VehicleDeleteDate = S.VehicleDeleteDate3, T.CompositeRatedFlag = S.CompositeRatedFlag3, T.PersonalInjuryProtectionExcessLimit = S.PersonalInjuryProtectionExcessLimit3, T.VehicleTypeSize = S.VehicleTypeSize3, T.BusinessUseClass = S.BusinessUseClass3, T.SecondaryClass = S.SecondaryClass3, T.FleetType = S.FleetType3, T.SecondaryClassGroup = S.SecondaryClassGroup3, T.VIN = S.VIN3, T.VehicleNumber = S.VehicleNumber3, T.CoordinationOfBenefits = S.CoordinationOfBenefits3, T.CoveredByWorkersCompensationFlag = S.CoveredByWorkersCompensationFlag3, T.MedicalExpensesOption = S.MedicalExpensesOption3, T.HistoricVehicleIndicator = S.HistoricVehicleIndicator3
),