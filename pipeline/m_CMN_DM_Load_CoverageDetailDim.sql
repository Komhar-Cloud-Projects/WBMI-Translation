WITH
LKP_CoverageLimit AS (
	SELECT
	CoverageLimitValue,
	PremiumTransactionAKID,
	CoverageLimitType
	FROM (
		SELECT PremiumTransactionAKID as PremiumTransactionAKID,
		CoverageLimitType as CoverageLimitType,
		CoverageLimitValue as CoverageLimitValue from (
		select clb.PremiumTransactionAKID as PremiumTransactionAKID,
		cl.CoverageLimitType as CoverageLimitType,
		cl.CoverageLimitValue as CoverageLimitValue
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit cl
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge clb
		on cl.CoverageLimitId=clb.CoverageLimitId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction pt
		on pt.PremiumTransactionAKId=clb.PremiumTransactionAKId
		WHERE PT.SourceSystemId='DCT' and ( '@{pipeline().parameters.SELECTION_START_TS}'<='01/01/1800 01:00:00' or 
		exists ( select 1 
		               from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT2 
		               where PT2.RatingCoverageAKId=PT.RatingCoverageAKId and PT2.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
		               AND PT2.SourceSystemId='DCT'))
		
		UNION ALL
		
		SELECT
		clb.PremiumTransactionAKID as PremiumTransactionAKID,
		cl.CoverageLimitType as CoverageLimitType,
		cl.CoverageLimitValue as CoverageLimitValue
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit cl
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge clb
		on cl.CoverageLimitId=clb.CoverageLimitId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction pt
		on pt.PremiumTransactionAKId=clb.PremiumTransactionAKId
		WHERE PT.SourceSystemId='PMS' and ( '@{pipeline().parameters.SELECTION_START_TS}'<='01/01/1800 01:00:00' or 
		exists ( select 1 
		               from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT2 
		               where PT2.StatisticalCoverageAKId=PT.StatisticalCoverageAKId and 
		              PT2.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}' AND PT2.SourceSystemId='PMS'))) a
		order by PremiumTransactionAKID,CoverageLimitType
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKID,CoverageLimitType ORDER BY CoverageLimitValue) = 1
),
LKP_Supclassification_Lob AS (
	SELECT
	Result,
	LineOfBusinessAbbreviation,
	ClassCode,
	RatingStateCode
	FROM (
		select ClassCode as ClassCode 
		,RatingStateCode as RatingStateCode 
		,LineOfBusinessAbbreviation as LineOfBusinessAbbreviation
		, Result as Result  from 
		(
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'CF' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationCommercialProperty
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'CA' AS LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationCommercialAuto
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'CR' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationCrime
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'WC' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationWorkersCompensation
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'BND' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationBonds
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'IM' as LineOfBusinessAbbreviation , case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end  As Result
		from dbo.SupClassificationInlandMarine
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'UMB' as LineOfBusinessAbbreviation , case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationUmbrella
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'DNO' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationDirectorsOfficers
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'ENO' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationErrorsOmissions
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'EPLI' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationEPLI
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'EL' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result 
		from dbo.SupClassificationExcessLiability
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'GA' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationGarage
		where CurrentSnapshotFlag=1
		Union
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'GL' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationGeneralLiability
		where CurrentSnapshotFlag=1
		) a
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LineOfBusinessAbbreviation,ClassCode,RatingStateCode ORDER BY Result) = 1
),
SQ_GetDataFromEDW AS (
	WITH CTE_DCT
	AS
	(
	select 
	PC.PolicyCoverageID,
	PT.PremiumTransactionID, 
	RC.RatingCoverageId, 
	PC.RiskLocationAKID
	from       @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT 
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC on PT.RatingCoverageAKId=RC.RatingCoverageAKID 
	and PT.EffectiveDate=RC.EffectiveDate
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC on RC.PolicyCoverageAKID=PC.PolicyCoverageAKID 
	and PC.CurrentSnapshotFlag=1
	where PT.RatingCoverageAKId in 
	(
	select distinct RatingCoverageAKId  from PremiumTransaction with (nolock) where SourceSystemId IN ('DCT') and
	(CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}' or ('@{pipeline().parameters.SELECTION_START_TS}'<='01/01/1800 01:00:00'))
	)
	AND PT.ReasonAmendedCode NOT IN ('CWO', 'Claw Back') -- Added filter for the JIRA PROD-19657
	)
	 ,
	CTE_PMS
	AS
	(
	select distinct PremiumTransaction.StatisticalCoverageAKID,PolicyCoverageAKID 
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction with (nolock) 
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage on PremiumTransaction.StatisticalCoverageAKID=StatisticalCoverage.StatisticalCoverageAKID
	where PremiumTransaction.SourceSystemId IN ('PMS') 
	AND PremiumTransaction.ReasonAmendedCode NOT IN ('CWO', 'Claw Back') -- Added filter for the JIRA PROD-19657
	and (PremiumTransaction.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}' or ('@{pipeline().parameters.SELECTION_START_TS}'<='01/01/1800 01:00:00'))
	)
	
	SELECT PT.IncreasedLimitGroupCode AS IncreasedLimitGroupCode
	 , ILG.StandardIncreasedLimitGroupDescription AS IncreasedLimitGroupDescription 
	,(case when PMAG.StandardPackageModifcationAdjustmentGroupCode is null  then 'N/A' 
	else PMAG.StandardPackageModifcationAdjustmentGroupCode end) AS PackageModificationAdjustmentGroupCode
	 , PMAG.StandardPackageModificationAsjustmentGroupDescription AS PackageModificationAdjustmentGroupDescription
	 , RC.ClassCode
	 , PT.IncreasedLimitFactor
	 , RC.CoverageGUID
	 , RC.SubLocationUnitNumber
	 , Deductible.DeductibleAmount
	 , PT.PackageModificationAdjustmentFactor
	 , PT.YearBuilt
	 , RC.ClassCodeOrganizationCode
	 , SIL.StandardInsuranceLineCode
	 , PC.RiskGradeCode
	 , (case when RL.LocationIndicator='Y' then RL.LocationUnitNumber else '0000' end) as LocationUnitNumber
	 , PTRR.RatingTerritoryCode as RiskTerritory
	 , RL.StateProvinceCode
	 , RL.ZipPostalCode
	 , RL.RatingCity
	 , RL.RatingCounty
	 , RL.StreetAddress
	 , (case when RC.ClassCodeOrganizationCode='ISS' then 'ALL' else SS.state_code end)  as state_code
	 , RC.RatingCoverageEffectiveDate as CoverageEffectiveDate
	 , PT.PremiumTransactionID
	 , PT.PremiumTransactionEffectiveDate
	 , '1' as OrderKey
	,PC.PriorCoverageId
	,PT.IndividualRiskPremiumModification
	,PT.PremiumTransactionAKID
	,PT.ConstructionCode
	,PT.WindCoverageFlag
	,'DCT' AS SourceSystemID
	, null as RiskUnitGroup
	, null as RiskUnit
	, PT.BaseRate
	, PT.ExposureBasis
	, IR.InsuranceReferenceLineOfBusinessAbbreviation
	, PC.PolicyAKID
	, P.ProductAbbreviation
	,PTRR.CensusBlockGroupCountyCode as CensusBlockGroupCountyCode
	,PTRR.CensusBlockGroupTractCode as CensusBlockGroupTractCode
	,PTRR.CensusBlockGroupBlockGroupCode as CensusBlockGroupBlockGroupCode
	,PTRR.Latitude as Latitude
	,PTRR.Longitude as Longitude
	FROM 
	CTE_DCT
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC on CTE_DCT.PolicyCoverageID=PC.PolicyCoverageID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT ON CTE_DCT.PremiumTransactionID = PT.PremiumTransactionID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC on CTE_DCT.RatingCoverageId=RC.RatingCoverageID
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness IR
	ON IR.InsuranceReferenceLineOfBusinessAKId=RC.InsuranceReferenceLineOfBusinessAKId
	AND IR.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	on PC.RiskLocationAKId=RL.RiskLocationAKId and RL.CurrentSnapshotFlag=1
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupIncreasedLimitGroup ILG
	ON ILG.StandardIncreasedLimitGroupCode = PT.IncreasedLimitGroupCode AND ILG.SourceSystemId='DCT' AND ILG.CurrentSnapshotFlag=1
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupPackageModificationAdjustmentGroup PMAG
	ON PMAG.PackageModificationAdjustmentGroupCode=PT.PackageModificationAdjustmentGroupCode
	AND PMAG.SourceSystemId='DCT' AND PMAG.CurrentSnapshotFlag=1
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL
	on SIL.ins_line_code=PC.InsuranceLine and SIL.crrnt_snpsht_flag=1
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_state SS
	on (case when len(SS.state_abbrev)=1 then '0'+SS.state_abbrev else SS.state_abbrev end)=(case when len(RL.StateProvinceCode)=1 then '0'+RL.StateProvinceCode else RL.StateProvinceCode end) and SS.crrnt_snpsht_flag=1
	LEFT JOIN (
	select cdb.PremiumTransactionAKId, 
	CONVERT(varchar(20),max(case when ISNUMERIC(cd.CoverageDeductibleValue)=1 then CONVERT(decimal,CoverageDeductibleValue) else 0 end)) as DeductibleAmount
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductibleBridge cdb
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductible cd
	on cdb.CoverageDeductibleId=cd.CoverageDeductibleId
	where cdb.SourceSystemID='DCT' and cd.SourceSystemID='DCT'
	group by cdb.PremiumTransactionAKId) Deductible
	on pt.PremiumTransactionAKID=Deductible.PremiumTransactionAKId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product P
	ON P.ProductAKId = RC.ProductAKId AND P.CurrentSnapshotFlag = 1
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransactionRatingRisk PTRR
	on PTRR.PremiumTransactionID = PT.PremiumTransactionID
	@{pipeline().parameters.WHERE_CLAUSE_DCT}
	
	UNION ALL
	
	SELECT PT.IncreasedLimitGroupCode AS IncreasedLimitGroupCode
	 , ILG.StandardIncreasedLimitGroupDescription AS IncreasedLimitGroupDescription
	 ,(case when PMAG.StandardPackageModifcationAdjustmentGroupCode is null  then 'N/A' 
	else PMAG.StandardPackageModifcationAdjustmentGroupCode end) AS PackageModificationAdjustmentGroupCode
	 , PMAG.StandardPackageModificationAsjustmentGroupDescription AS PackageModificationAdjustmentGroupDescription
	 , case when SC.Riskunitgroup='286' then SC.RiskUnit else SC.ClassCode end ClassCode
	 , PT.IncreasedLimitFactor
	 , SC.CoverageGUID
	 , SC.SubLocationUnitNumber
	 , Deductible.DeductibleAmount
	 , PT.PackageModificationAdjustmentFactor
	 , PT.YearBuilt
	 , SC.ClassCodeOrganizationCode
	 , SIL.StandardInsuranceLineCode
	 , PC.RiskGradeCode
	 , (case when RL.LocationIndicator='Y' then RL.LocationUnitNumber else '0000' end) as LocationUnitNumber
	 , RL.RiskTerritory
	 , RL.StateProvinceCode
	 , RL.ZipPostalCode
	 , RL.RatingCity
	 , RL.RatingCounty
	 , RL.StreetAddress
	 , (case when SC.ClassCodeOrganizationCode='ISS' then 'ALL' else SS.state_code end)  as state_code
	 , SC.StatisticalCoverageEffectiveDate as CoverageEffectiveDate
	 , PT.PremiumTransactionID
	 , PT.PremiumTransactionEffectiveDate
	 , '2' as OrderKey
	,PC.PriorCoverageId
	,PT.IndividualRiskPremiumModification
	,PT.PremiumTransactionAKID
	,PT.ConstructionCode
	,PT.WindCoverageFlag
	,'PMS' AS SourceSystemID
	, SC.RiskUnitGroup
	, SC.RiskUnit
	, PT.BaseRate
	, PT.ExposureBasis
	, IR.InsuranceReferenceLineOfBusinessAbbreviation
	, PC.PolicyAKID
	, P.ProductAbbreviation
	,null as CensusBlockGroupCountyCode
	,null as CensusBlockGroupTractCode
	,null as CensusBlockGroupBlockGroupCode
	,null as Latitude
	,null as Longitude
	FROM 
	CTE_PMS
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC on CTE_PMS.StatisticalCoverageAKID = SC.StatisticalCoverageAKID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC on CTE_PMS.PolicyCoverageAKID= PC.PolicyCoverageAKID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT on CTE_PMS.StatisticalCoverageAKID=PT.StatisticalCoverageAKID and PT.SourceSystemID='PMS'
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness IR
	ON IR.InsuranceReferenceLineOfBusinessAKId=SC.InsuranceReferenceLineOfBusinessAKId
	AND IR.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	on PC.RiskLocationAKId=RL.RiskLocationAKId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupIncreasedLimitGroup ILG
	ON ILG.StandardIncreasedLimitGroupCode = PT.IncreasedLimitGroupCode AND ILG.SourceSystemId='PMS' AND ILG.CurrentSnapshotFlag=1
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupPackageModificationAdjustmentGroup PMAG
	ON PMAG.PackageModificationAdjustmentGroupCode =PT.PackageModificationAdjustmentGroupCode
	AND PMAG.SourceSystemId='PMS' AND PMAG.CurrentSnapshotFlag=1
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL
	on SIL.ins_line_code=PC.InsuranceLine and SIL.crrnt_snpsht_flag=1
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_state SS
	on (case when len(SS.state_abbrev)=1 then '0'+SS.state_abbrev else SS.state_abbrev end)=(case when len(RL.StateProvinceCode)=1 then '0'+RL.StateProvinceCode else RL.StateProvinceCode end) and SS.crrnt_snpsht_flag=1
	LEFT JOIN (
	select cdb.PremiumTransactionAKId, 
	CONVERT(varchar(20),max(case when ISNUMERIC(cd.CoverageDeductibleValue)=1 then CONVERT(decimal,CoverageDeductibleValue) else 0 end)) as DeductibleAmount
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductibleBridge cdb
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductible cd
	on cdb.CoverageDeductibleId=cd.CoverageDeductibleId
	where cdb.SourceSystemID='PMS' and cd.SourceSystemID='PMS'
	group by cdb.PremiumTransactionAKId) Deductible
	on pt.PremiumTransactionAKID=Deductible.PremiumTransactionAKId
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product P
	ON P.ProductAKId = SC.ProductAKId AND P.CurrentSnapshotFlag = 1
	@{pipeline().parameters.WHERE_CLAUSE_PMS}
	
	UNION ALL      ----one dummy record
	
	select null as IncreasedLimitGroupCode
	 , null as IncreasedLimitGroupDescription
	 , null as PackageModificationAdjustmentGroupCode
	 , null as PackageModificationAdjustmentGroupDescription
	 , null as ClassCode
	 , null as IncreasedLimitFactor
	 , null as CoverageGUID
	 , null as SubLocationUnitNumber
	 , null as DeductibleAmount
	 , null as PackageModificationAdjustmentFactor
	 , null as YearBuilt
	 , null as ClassCodeOrganizationCode
	 , null as StandardInsuranceLineCode
	 , null as RiskGradeCode
	 , null as LocationUnitNumber
	 , null as RiskTerritory
	 , null as StateProvinceCode
	 , null as ZipPostalCode
	 , null as RatingCity
	 , null as RatingCounty
	 , null as StreetAddress
	 , null  as state_code
	 , null as CoverageEffectiveDate
	 , null as PremiumTransactionID
	 , null as PremiumTransactionEffectiveDate
	 , '3' as OrderKey
	,null as PriorCoverageId
	,null as IndividualRiskPremiumModification
	,null as PremiumTransactionAKID
	,null as ConstructionCode
	,null as WindCoverageFlag
	,null as SourceSystemID
	, null as RiskUnitGroup
	, null as RiskUnit
	, null as BaseRate
	, null as ExposureBasis
	, null as InsuranceReferenceLineOfBusinessAbbreviation
	, null as PolicyAKID
	, null as ProductAbbreviation
	,null as CensusBlockGroupCountyCode
	,null as CensusBlockGroupTractCode
	,null as CensusBlockGroupBlockGroupCode
	,null as Latitude
	,null as Longitude
),
EXP_Input AS (
	SELECT
	IncreasedLimitGroupCode,
	StandardIncreasedLimitGroupDescription,
	PackageModificationAdjustmentGroupCode,
	StandardPackageModificationAsjustmentGroupDescription,
	ClassCode,
	IncreasedLimitFactor,
	CoverageGUID,
	SubLocationUnitNumber,
	DeductibleAmount,
	PackageModificationAdjustmentFactor,
	YearBuilt,
	ClassCodeOrganizationCode,
	InsuranceLine,
	RiskGradeCode,
	LocationUnitNumber,
	RiskTerritory,
	StateProvinceCode,
	ZipPostalCode,
	RatingCity,
	RatingCounty,
	StreetAddress,
	state_code,
	CoverageEffectiveDate,
	PremiumTransactionID,
	PremiumTransactionEffectiveDate,
	OrderKey,
	PriorCoverageId,
	IndividualRiskPremiumModification,
	PremiumTransactionAKID,
	ConstructionCode,
	WindCoverageFlag,
	SourceSystemId,
	RiskUnitGroup,
	RiskUnit,
	BaseRate,
	ExposureBasis,
	InsuranceReferenceLineOfBusinessAbbreviation,
	PolicyAKID,
	ProductAbbreviation,
	CensusBlockGroupCountyCode,
	CensusBlockGroupTractCode,
	CensusBlockGroupBlockGroupCode,
	Latitude,
	Longitude
	FROM SQ_GetDataFromEDW
),
SRT_Input AS (
	SELECT
	OrderKey, 
	PolicyAKID, 
	CoverageGUID, 
	PremiumTransactionEffectiveDate, 
	PremiumTransactionID, 
	IncreasedLimitGroupCode, 
	StandardIncreasedLimitGroupDescription, 
	PackageModificationAdjustmentGroupCode, 
	StandardPackageModificationAsjustmentGroupDescription, 
	ClassCode, 
	IncreasedLimitFactor, 
	SubLocationUnitNumber, 
	DeductibleAmount, 
	PackageModificationAdjustmentFactor, 
	YearBuilt, 
	ClassCodeOrganizationCode, 
	InsuranceLine, 
	RiskGradeCode, 
	LocationUnitNumber, 
	RiskTerritory, 
	StateProvinceCode, 
	ZipPostalCode, 
	RatingCity, 
	RatingCounty, 
	StreetAddress, 
	state_code, 
	CoverageEffectiveDate, 
	PriorCoverageId, 
	IndividualRiskPremiumModification, 
	PremiumTransactionAKID, 
	ConstructionCode, 
	WindCoverageFlag, 
	SourceSystemId, 
	RiskUnitGroup, 
	RiskUnit, 
	BaseRate, 
	ExposureBasis, 
	InsuranceReferenceLineOfBusinessAbbreviation, 
	ProductAbbreviation, 
	CensusBlockGroupCountyCode, 
	CensusBlockGroupTractCode, 
	CensusBlockGroupBlockGroupCode, 
	Latitude, 
	Longitude
	FROM EXP_Input
	ORDER BY OrderKey ASC, PolicyAKID ASC, CoverageGUID ASC, PremiumTransactionEffectiveDate DESC, PremiumTransactionID DESC
),
EXP_GetValues AS (
	SELECT
	IncreasedLimitGroupCode AS i_IncreasedLimitGroupCode,
	StandardIncreasedLimitGroupDescription AS i_StandardIncreasedLimitGroupDescription,
	PackageModificationAdjustmentGroupCode AS i_PackageModificationAdjustmentGroupCode,
	StandardPackageModificationAsjustmentGroupDescription AS i_StandardPackageModificationAsjustmentGroupDescription,
	ClassCode AS i_ClassCode,
	IncreasedLimitFactor AS i_IncreasedLimitFactor,
	CoverageGUID AS i_CoverageGUID,
	PolicyAKID AS i_PolicyAKID,
	SubLocationUnitNumber AS i_SubLocationUnitNumber,
	DeductibleAmount AS i_DeductibleAmount,
	PackageModificationAdjustmentFactor AS i_PackageModificationAdjustmentFactor,
	YearBuilt AS i_YearBuilt,
	ClassCodeOrganizationCode AS i_ClassCodeOrganizationCode,
	InsuranceLine AS i_InsuranceLine,
	RiskGradeCode AS i_RiskGradeCode,
	LocationUnitNumber AS i_LocationUnitNumber,
	RiskTerritory AS i_RiskTerritory,
	StateProvinceCode AS i_StateProvinceCode,
	ZipPostalCode AS i_ZipPostalCode,
	RatingCity AS i_RatingCity,
	RatingCounty AS i_RatingCounty,
	StreetAddress AS i_StreetAddress,
	state_code AS i_state_code,
	CoverageEffectiveDate AS i_CoverageEffectiveDate,
	PremiumTransactionID AS i_PremiumTransactionID,
	PremiumTransactionEffectiveDate AS i_PremiumTransactionEffectiveDate,
	OrderKey AS i_OrderKey,
	PriorCoverageId AS i_PriorCoverageId,
	IndividualRiskPremiumModification AS i_IndividualRiskPremiumModification,
	PremiumTransactionAKID AS i_PremiumTransactionAKID,
	ConstructionCode AS i_ConstructionCode,
	WindCoverageFlag AS i_WindCoverageFlag,
	SourceSystemId AS i_SourceSystemId,
	RiskUnitGroup AS i_RiskUnitGroup,
	RiskUnit AS i_RiskUnit,
	BaseRate AS i_BaseRate,
	ExposureBasis AS i_ExposureBasis,
	InsuranceReferenceLineOfBusinessAbbreviation AS i_InsuranceReferenceLineOfBusinessAbbreviation,
	ProductAbbreviation AS i_ProductAbbreviation,
	CensusBlockGroupCountyCode AS i_CensusBlockGroupCountyCode,
	CensusBlockGroupTractCode AS i_CensusBlockGroupTractCode,
	CensusBlockGroupBlockGroupCode AS i_CensusBlockGroupBlockGroupCode,
	Latitude AS i_Latitude,
	Longitude AS i_Longitude,
	v_Count+1 AS v_Count,
	v_Prev_InsuranceLine AS v_InsuranceLine,
	v_Prev_CoverageEffectiveDate AS v_CoverageEffectiveDate,
	v_Prev_IncreasedLimitGroupCode AS v_IncreasedLimitGroupCode,
	v_Prev_IncreasedLimitGroupDescription AS v_IncreasedLimitGroupDescription,
	v_Prev_PackageModificationAdjustmentGroupCode AS v_PackageModificationAdjustmentGroupCode,
	v_Prev_PackageModificationAdjustmentGroupDescription AS v_PackageModificationAdjustmentGroupDescription,
	v_Prev_ClassCode AS v_ClassCode,
	v_Prev_BuildingNumber AS v_BuildingNumber,
	v_Prev_LocationNumber AS v_LocationNumber,
	v_Prev_DeductibleAmount AS v_DeductibleAmount,
	v_Prev_RiskGradeCode AS v_RiskGradeCode,
	v_Prev_PackageModificationAdjustmentFactor AS v_PackageModificationAdjustmentFactor,
	v_Prev_YearBuilt AS v_YearBuilt,
	v_Prev_ClassCodeOrganizationCode AS v_ClassCodeOrganizationCode,
	v_Prev_IncreasedLimitFactor AS v_IncreasedLimitFactor,
	v_Prev_CoverageGUID AS v_CoverageGUID,
	v_Prev_RatingCity AS v_RatingCity,
	v_Prev_RatingCounty AS v_RatingCounty,
	v_Prev_RatingStateProvinceCode AS v_RatingStateProvinceCode,
	v_Prev_RatingStateProvinceAbbreviation AS v_RatingStateProvinceAbbreviation,
	v_Prev_RatingPostalCode AS v_RatingPostalCode,
	v_Prev_RatingAddress AS v_RatingAddress,
	v_Prev_RatingTerritory AS v_RatingTerritory,
	v_Prev_EDWPremiumTransactionPKId AS v_EDWPremiumTransactionPKId,
	-- *INF*: IIF( NOT ISNULL(v_Prev_EffectiveDate) AND
	-- ((i_CoverageGUID != v_Prev_CoverageGUID and i_PolicyAKID=v_Prev_PolicyAKID) or
	-- ( i_CoverageGUID != v_Prev_CoverageGUID and i_PolicyAKID!= v_Prev_PolicyAKID)) ,
	-- TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), v_Prev_EffectiveDate)
	-- -- Added above logic as part of DAP-879
	-- 
	-- --IIF( NOT ISNULL(v_Prev_EffectiveDate) AND i_CoverageGUID != v_Prev_CoverageGUID, TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), v_Prev_EffectiveDate)
	IFF(v_Prev_EffectiveDate IS NULL 
		AND ( ( i_CoverageGUID != v_Prev_CoverageGUID 
				AND i_PolicyAKID = v_Prev_PolicyAKID 
			) 
			OR ( i_CoverageGUID != v_Prev_CoverageGUID 
				AND i_PolicyAKID != v_Prev_PolicyAKID 
			)NOT  
		),
		TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'
		),
		v_Prev_EffectiveDate
	) AS v_EffectiveDate,
	v_Prev_ExpirationDate AS v_ExpirationDate,
	v_Prev_PriorCoverageId AS v_PriorCoverageId,
	v_Prev_IndividualRiskPremiumModification AS v_IndividualRiskPremiumModification,
	v_Prev_PremiumTransactionAKID AS v_PremiumTransactionAKID,
	v_Prev_ConstructionCode AS v_ConstructionCode,
	v_Prev_WindCoverageFlag AS v_WindCoverageFlag,
	v_Prev_SourceSystemId AS v_SourceSystemId,
	v_Prev_RiskUnitGroup AS v_RiskUnitGroup,
	v_Prev_RiskUnit AS v_RiskUnit,
	v_Prev_BaseRate AS v_BaseRate,
	v_Prev_ExposureBasis AS v_ExposureBasis,
	v_Prev_Out_ClassCode AS v_Out_ClassCode,
	v_Prev_LineOfBusinessAbbreviation AS v_LineOfBusinessAbbreviation,
	v_Prev_ProductAbbreviation AS v_ProductAbbreviation,
	v_Prev_CensusBlockGroupCountyCode AS v_CensusBlockGroupCountyCode,
	v_Prev_CensusBlockGroupTractCode AS v_CensusBlockGroupTractCode,
	v_Prev_CensusBlockGroupBlockGroupCode AS v_CensusBlockGroupBlockGroupCode,
	v_Prev_Latitude AS v_Latitude,
	v_Prev_Longitude AS v_Longitude,
	i_BaseRate AS v_Prev_BaseRate,
	i_InsuranceLine AS v_Prev_InsuranceLine,
	i_CoverageEffectiveDate AS v_Prev_CoverageEffectiveDate,
	-- *INF*: IIF(ISNULL(i_IncreasedLimitGroupCode), 'N/A', i_IncreasedLimitGroupCode)
	IFF(i_IncreasedLimitGroupCode IS NULL,
		'N/A',
		i_IncreasedLimitGroupCode
	) AS v_Prev_IncreasedLimitGroupCode,
	-- *INF*: IIF(ISNULL(i_StandardIncreasedLimitGroupDescription), 'N/A', i_StandardIncreasedLimitGroupDescription)
	IFF(i_StandardIncreasedLimitGroupDescription IS NULL,
		'N/A',
		i_StandardIncreasedLimitGroupDescription
	) AS v_Prev_IncreasedLimitGroupDescription,
	-- *INF*: IIF(ISNULL(i_PackageModificationAdjustmentGroupCode), 'N/A', i_PackageModificationAdjustmentGroupCode)
	IFF(i_PackageModificationAdjustmentGroupCode IS NULL,
		'N/A',
		i_PackageModificationAdjustmentGroupCode
	) AS v_Prev_PackageModificationAdjustmentGroupCode,
	-- *INF*: IIF(ISNULL(i_StandardPackageModificationAsjustmentGroupDescription), 'N/A', i_StandardPackageModificationAsjustmentGroupDescription)
	IFF(i_StandardPackageModificationAsjustmentGroupDescription IS NULL,
		'N/A',
		i_StandardPackageModificationAsjustmentGroupDescription
	) AS v_Prev_PackageModificationAdjustmentGroupDescription,
	-- *INF*: IIF(ISNULL(i_ClassCode), 'N/A', i_ClassCode)
	IFF(i_ClassCode IS NULL,
		'N/A',
		i_ClassCode
	) AS v_Prev_ClassCode,
	-- *INF*: IIF(ISNULL(i_SubLocationUnitNumber), '000', i_SubLocationUnitNumber)
	IFF(i_SubLocationUnitNumber IS NULL,
		'000',
		i_SubLocationUnitNumber
	) AS v_Prev_BuildingNumber,
	-- *INF*: IIF(ISNULL(i_LocationUnitNumber), '0000', i_LocationUnitNumber) 
	IFF(i_LocationUnitNumber IS NULL,
		'0000',
		i_LocationUnitNumber
	) AS v_Prev_LocationNumber,
	-- *INF*: IIF(ISNULL(i_DeductibleAmount), '0', i_DeductibleAmount)
	IFF(i_DeductibleAmount IS NULL,
		'0',
		i_DeductibleAmount
	) AS v_Prev_DeductibleAmount,
	-- *INF*: IIF(ISNULL(i_RiskGradeCode), 'N/A', i_RiskGradeCode)
	IFF(i_RiskGradeCode IS NULL,
		'N/A',
		i_RiskGradeCode
	) AS v_Prev_RiskGradeCode,
	-- *INF*: IIF(ISNULL(i_PackageModificationAdjustmentFactor), 0, i_PackageModificationAdjustmentFactor)
	IFF(i_PackageModificationAdjustmentFactor IS NULL,
		0,
		i_PackageModificationAdjustmentFactor
	) AS v_Prev_PackageModificationAdjustmentFactor,
	-- *INF*: IIF(ISNULL(i_YearBuilt), '0000', i_YearBuilt)
	IFF(i_YearBuilt IS NULL,
		'0000',
		i_YearBuilt
	) AS v_Prev_YearBuilt,
	-- *INF*: IIF(ISNULL(i_ClassCodeOrganizationCode), 'N/A', i_ClassCodeOrganizationCode)
	IFF(i_ClassCodeOrganizationCode IS NULL,
		'N/A',
		i_ClassCodeOrganizationCode
	) AS v_Prev_ClassCodeOrganizationCode,
	-- *INF*: IIF(ISNULL(i_IncreasedLimitFactor), 0, i_IncreasedLimitFactor)
	IFF(i_IncreasedLimitFactor IS NULL,
		0,
		i_IncreasedLimitFactor
	) AS v_Prev_IncreasedLimitFactor,
	-- *INF*: IIF(ISNULL(i_RatingCity), 'N/A', i_RatingCity)
	IFF(i_RatingCity IS NULL,
		'N/A',
		i_RatingCity
	) AS v_Prev_RatingCity,
	-- *INF*: IIF(ISNULL(i_RatingCounty), 'N/A', i_RatingCounty)
	IFF(i_RatingCounty IS NULL,
		'N/A',
		i_RatingCounty
	) AS v_Prev_RatingCounty,
	-- *INF*: IIF(ISNULL(i_StateProvinceCode), 'N/A', i_StateProvinceCode)
	IFF(i_StateProvinceCode IS NULL,
		'N/A',
		i_StateProvinceCode
	) AS v_Prev_RatingStateProvinceCode,
	-- *INF*: IIF(ISNULL(i_state_code), 'N/A', i_state_code)
	IFF(i_state_code IS NULL,
		'N/A',
		i_state_code
	) AS v_Prev_RatingStateProvinceAbbreviation,
	-- *INF*: IIF(ISNULL(i_ZipPostalCode), 'N/A', i_ZipPostalCode)
	IFF(i_ZipPostalCode IS NULL,
		'N/A',
		i_ZipPostalCode
	) AS v_Prev_RatingPostalCode,
	-- *INF*: IIF(ISNULL(i_StreetAddress), 'N/A', i_StreetAddress)
	IFF(i_StreetAddress IS NULL,
		'N/A',
		i_StreetAddress
	) AS v_Prev_RatingAddress,
	-- *INF*: IIF(ISNULL(i_RiskTerritory), 'N/A', i_RiskTerritory)
	IFF(i_RiskTerritory IS NULL,
		'N/A',
		i_RiskTerritory
	) AS v_Prev_RatingTerritory,
	i_PremiumTransactionID AS v_Prev_EDWPremiumTransactionPKId,
	-- *INF*: IIF((i_CoverageGUID != v_Prev_CoverageGUID and i_PolicyAKID=v_Prev_PolicyAKID) or
	--  ( i_CoverageGUID != v_Prev_CoverageGUID and i_PolicyAKID!= v_Prev_PolicyAKID), 
	-- TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), ADD_TO_DATE(v_Prev_EffectiveDate, 'SS', +1))
	-- 
	-- 
	-- -- Added above logic as part of DAP-879
	-- --IIF(i_CoverageGUID != v_Prev_CoverageGUID, TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), ADD_TO_DATE(v_Prev_EffectiveDate, 'SS', -1))
	IFF(( i_CoverageGUID != v_Prev_CoverageGUID 
			AND i_PolicyAKID = v_Prev_PolicyAKID 
		) 
		OR ( i_CoverageGUID != v_Prev_CoverageGUID 
			AND i_PolicyAKID != v_Prev_PolicyAKID 
		),
		TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'
		),
		DATEADD(SECOND,+ 1,v_Prev_EffectiveDate)
	) AS v_Prev_ExpirationDate,
	-- *INF*: IIF(ISNULL(i_CoverageGUID), 'N/A', i_CoverageGUID)
	IFF(i_CoverageGUID IS NULL,
		'N/A',
		i_CoverageGUID
	) AS v_Prev_CoverageGUID,
	i_PremiumTransactionEffectiveDate AS v_Prev_EffectiveDate,
	-- *INF*: IIF(ISNULL(i_PriorCoverageId),-1,i_PriorCoverageId)
	IFF(i_PriorCoverageId IS NULL,
		- 1,
		i_PriorCoverageId
	) AS v_Prev_PriorCoverageId,
	-- *INF*: IIF(ISNULL(i_IndividualRiskPremiumModification),0,i_IndividualRiskPremiumModification)
	IFF(i_IndividualRiskPremiumModification IS NULL,
		0,
		i_IndividualRiskPremiumModification
	) AS v_Prev_IndividualRiskPremiumModification,
	-- *INF*: IIF(ISNULL(i_PremiumTransactionAKID),-1,i_PremiumTransactionAKID)
	IFF(i_PremiumTransactionAKID IS NULL,
		- 1,
		i_PremiumTransactionAKID
	) AS v_Prev_PremiumTransactionAKID,
	-- *INF*: IIF(ISNULL(i_ConstructionCode), 'N/A', i_ConstructionCode)
	-- 
	IFF(i_ConstructionCode IS NULL,
		'N/A',
		i_ConstructionCode
	) AS v_Prev_ConstructionCode,
	-- *INF*: IIF(ISNULL(i_WindCoverageFlag), '0', IIF( i_WindCoverageFlag='True', '1', '0')) 
	IFF(i_WindCoverageFlag IS NULL,
		'0',
		IFF(i_WindCoverageFlag = 'True',
			'1',
			'0'
		)
	) AS v_Prev_WindCoverageFlag,
	i_PolicyAKID AS v_Prev_PolicyAKID,
	i_SourceSystemId AS v_Prev_SourceSystemId,
	-- *INF*: IIF(ISNULL(i_RiskUnitGroup), 'N/A', i_RiskUnitGroup)
	IFF(i_RiskUnitGroup IS NULL,
		'N/A',
		i_RiskUnitGroup
	) AS v_Prev_RiskUnitGroup,
	-- *INF*: IIF(ISNULL(i_RiskUnit), 'N/A',i_RiskUnit)
	IFF(i_RiskUnit IS NULL,
		'N/A',
		i_RiskUnit
	) AS v_Prev_RiskUnit,
	i_ExposureBasis AS v_Prev_ExposureBasis,
	-- *INF*: DECODE(v_Prev_SourceSystemId,
	-- 'DCT',v_Prev_ClassCode,
	-- 'PMS', IIF(v_Prev_RiskUnitGroup='286',v_Prev_RiskUnit,v_Prev_ClassCode),
	-- 'N/A')
	DECODE(v_Prev_SourceSystemId,
		'DCT', v_Prev_ClassCode,
		'PMS', IFF(v_Prev_RiskUnitGroup = '286',
			v_Prev_RiskUnit,
			v_Prev_ClassCode
		),
		'N/A'
	) AS v_Prev_Out_ClassCode,
	i_InsuranceReferenceLineOfBusinessAbbreviation AS v_Prev_LineOfBusinessAbbreviation,
	i_ProductAbbreviation AS v_Prev_ProductAbbreviation,
	-- *INF*: IIF(ISNULL(i_CensusBlockGroupCountyCode),'N/A',i_CensusBlockGroupCountyCode)
	IFF(i_CensusBlockGroupCountyCode IS NULL,
		'N/A',
		i_CensusBlockGroupCountyCode
	) AS v_Prev_CensusBlockGroupCountyCode,
	-- *INF*: IIF(ISNULL(i_CensusBlockGroupTractCode),'N/A',i_CensusBlockGroupTractCode)
	IFF(i_CensusBlockGroupTractCode IS NULL,
		'N/A',
		i_CensusBlockGroupTractCode
	) AS v_Prev_CensusBlockGroupTractCode,
	-- *INF*: IIF(ISNULL(i_CensusBlockGroupBlockGroupCode),'N/A',i_CensusBlockGroupBlockGroupCode)
	IFF(i_CensusBlockGroupBlockGroupCode IS NULL,
		'N/A',
		i_CensusBlockGroupBlockGroupCode
	) AS v_Prev_CensusBlockGroupBlockGroupCode,
	-- *INF*: IIF(ISNULL(i_Latitude),0,i_Latitude)
	IFF(i_Latitude IS NULL,
		0,
		i_Latitude
	) AS v_Prev_Latitude,
	-- *INF*: IIF(ISNULL(i_Longitude),0,i_Longitude)
	IFF(i_Longitude IS NULL,
		0,
		i_Longitude
	) AS v_Prev_Longitude,
	v_Count AS o_Count,
	v_InsuranceLine AS o_InsuranceLine,
	v_CoverageEffectiveDate AS o_CoverageEffectiveDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	v_IncreasedLimitGroupCode AS o_IncreasedLimitGroupCode,
	v_IncreasedLimitGroupDescription AS o_IncreasedLimitGroupDescription,
	v_PackageModificationAdjustmentGroupCode AS o_PackageModificationAdjustmentGroupCode,
	v_PackageModificationAdjustmentGroupDescription AS o_PackageModificationAdjustmentGroupDescription,
	v_Out_ClassCode AS Out_ClassCode,
	-- *INF*: LPAD(v_ClassCode, 6, '0')
	LPAD(v_ClassCode, 6, '0'
	) AS o_ClassCode_lkp,
	v_BuildingNumber AS o_BuildingNumber,
	v_LocationNumber AS o_LocationNumber,
	v_DeductibleAmount AS o_DeductibleAmount,
	v_RiskGradeCode AS o_RiskGradeCode,
	v_PackageModificationAdjustmentFactor AS o_PackageModificationAdjustmentFactor,
	v_YearBuilt AS o_YearBuilt,
	v_ClassCodeOrganizationCode AS o_ClassCodeOrganizationCode,
	v_IncreasedLimitFactor AS o_IncreasedLimitFactor,
	v_CoverageGUID AS o_CoverageGUID,
	v_RatingCity AS o_RatingCity,
	v_RatingCounty AS o_RatingCounty,
	v_RatingStateProvinceCode AS o_RatingStateProvinceCode,
	v_RatingStateProvinceAbbreviation AS o_RatingStateProvinceAbbreviation,
	v_RatingPostalCode AS o_RatingPostalCode,
	v_RatingAddress AS o_RatingAddress,
	v_RatingTerritory AS o_RatingTerritory,
	v_EDWPremiumTransactionPKId AS o_EDWPremiumTransactionPKId,
	v_EffectiveDate AS o_EffectiveDate,
	v_ExpirationDate AS o_ExpirationDate,
	v_PriorCoverageId AS o_PriorCoverageId,
	v_IndividualRiskPremiumModification AS o_IndividualRiskPremiumModification,
	v_PremiumTransactionAKID AS o_PremiumTransactionAKID,
	v_ConstructionCode AS o_ConstructionCode,
	v_WindCoverageFlag AS o_WindCoverageFlag,
	v_SourceSystemId AS o_SourceSystemId,
	v_BaseRate AS o_BaseRate,
	v_ExposureBasis AS o_ExposureBasis,
	v_LineOfBusinessAbbreviation AS o_LineOfBusinessAbbreviation,
	v_ProductAbbreviation AS o_ProductAbbreviation,
	v_CensusBlockGroupCountyCode AS o_CensusBlockGroupCountyCode,
	v_CensusBlockGroupTractCode AS o_CensusBlockGroupTractCode,
	v_CensusBlockGroupBlockGroupCode AS o_CensusBlockGroupBlockGroupCode,
	v_Latitude AS o_Latitude,
	v_Longitude AS o_Longitude
	FROM SRT_Input
),
LKP_CoverageDetailDim AS (
	SELECT
	CoverageDetailDimId,
	EffectiveDate,
	ExpirationDate,
	EDWPremiumTransactionPKId
	FROM (
		SELECT CDD.CoverageDetailDimId AS CoverageDetailDimId
		      ,CDD.EffectiveDate AS EffectiveDate
		      ,CDD.ExpirationDate AS ExpirationDate
			,CDD.EDWPremiumTransactionPKId AS EDWPremiumTransactionPKId
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
		where '@{pipeline().parameters.SELECTION_START_TS}'<='01/01/1800 01:00:00' OR 
		exists (
		                         select 1 
		                        from (select RC.CoverageGUID
		                         from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
		                        join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		                        on PT.RatingCoverageAKID=RC.RatingCoverageAKID and PT.SourceSystemID='DCT' and RC.EffectiveDate=PT.EffectiveDate and PT.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
						union all
		                        select SC.CoverageGUID
		                        from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
		                        join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		                        on SC.StatisticalCoverageAKID=PT.StatisticalCoverageAKID and PT.SourceSystemID='PMS' and PT.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}') a
		 where  CDD.CoverageGUID=a.CoverageGUID)
		order by CDD.EDWPremiumTransactionPKId--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPremiumTransactionPKId ORDER BY CoverageDetailDimId) = 1
),
LKP_LimitOfInsurance AS (
	SELECT
	CoverageLimitValue,
	CoverageLimitType,
	PremiumTransactionAKID
	FROM (
		select t.PremiumTransactionAKID as PremiumTransactionAKID,
		t.CoverageLimitValue as CoverageLimitValue,
		t.CoverageLimitType as CoverageLimitType
		from (
		select
		cl.CoverageLimitValue as CoverageLimitValue,
		cl.CoverageLimitType as CoverageLimitType,
		pt.PremiumTransactionAKID as PremiumTransactionAKID 
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge clb
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit cl
		on clb.CoverageLimitId = cl.CoverageLimitId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction pt
		on clb.PremiumTransactionAKId = pt.PremiumTransactionAKID and pt.CurrentSnapshotFlag=1
		WHERE PT.SourceSystemId='DCT' and ( '@{pipeline().parameters.SELECTION_START_TS}'<='01/01/1800 01:00:00' or 
		exists ( select 1 
		               from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT2 
		               where PT2.RatingCoverageAKId=PT.RatingCoverageAKId and PT2.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
		               AND PT2.SourceSystemId='DCT'))
		 
		 union all
		 
		 select
		 cl.CoverageLimitValue as CoverageLimitValue,
		 cl.CoverageLimitType as CoverageLimitType,
		 pt.PremiumTransactionAKID as PremiumTransactionAKID
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge clb
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit cl
		on clb.CoverageLimitId = cl.CoverageLimitId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction pt
		on clb.PremiumTransactionAKId = pt.PremiumTransactionAKID
		WHERE PT.SourceSystemId='PMS' and ( '@{pipeline().parameters.SELECTION_START_TS}'<='01/01/1800 01:00:00' or 
		exists ( select 1 
		               from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT2 
		               where PT2.StatisticalCoverageAKId=PT.StatisticalCoverageAKId
		               and PT2.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
		               AND PT2.SourceSystemId='PMS')) 
		)t
		order by PremiumTransactionAKId,CASE WHEN ISNUMERIC(CoverageLimitValue)=1 THEN CONVERT(bigint,CoverageLimitValue) ELSE 0 END desc,CoverageLimitValue desc
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKID ORDER BY CoverageLimitValue) = 1
),
LKP_PriorCoverage AS (
	SELECT
	PriorCarrierName,
	PriorPolicyKey,
	PriorCoverageId
	FROM (
		SELECT 
			PriorCarrierName,
			PriorPolicyKey,
			PriorCoverageId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PriorCoverage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PriorCoverageId ORDER BY PriorCarrierName) = 1
),
EXP_GetAKId AS (
	SELECT
	LKP_CoverageDetailDim.CoverageDetailDimId AS LKP_CoverageDetailDimId,
	LKP_CoverageDetailDim.EffectiveDate AS LKP_EffectiveDate,
	LKP_CoverageDetailDim.ExpirationDate AS LKP_ExpirationDate,
	LKP_PriorCoverage.PriorCarrierName AS i_PriorCarrierName,
	LKP_PriorCoverage.PriorPolicyKey AS i_PriorPolicyKey,
	EXP_GetValues.o_PremiumTransactionAKID AS i_PremiumTransactionAKID,
	EXP_GetValues.o_SourceSystemId AS i_SourceSystemId,
	EXP_GetValues.o_Count AS i_Count,
	EXP_GetValues.o_AuditID AS AuditID,
	EXP_GetValues.o_CreatedDate AS CreatedDate,
	EXP_GetValues.o_ModifiedDate AS ModifiedDate,
	EXP_GetValues.o_IncreasedLimitGroupCode AS IncreasedLimitGroupCode,
	EXP_GetValues.o_IncreasedLimitGroupDescription AS IncreasedLimitGroupDescription,
	EXP_GetValues.o_PackageModificationAdjustmentGroupCode AS PackageModificationAdjustmentGroupCode,
	EXP_GetValues.o_PackageModificationAdjustmentGroupDescription AS PackageModificationAdjustmentGroupDescription,
	EXP_GetValues.Out_ClassCode AS ClassCode,
	EXP_GetValues.o_BuildingNumber AS BuildingNumber,
	EXP_GetValues.o_LocationNumber AS LocationNumber,
	EXP_GetValues.o_DeductibleAmount AS DeductibleAmount,
	EXP_GetValues.o_RiskGradeCode AS RiskGradeCode,
	EXP_GetValues.o_PackageModificationAdjustmentFactor AS PackageModificationAdjustmentFactor,
	EXP_GetValues.o_YearBuilt AS YearBuilt,
	EXP_GetValues.o_IncreasedLimitFactor AS IncreasedLimitFactor,
	EXP_GetValues.o_CoverageGUID AS CoverageGUID,
	EXP_GetValues.o_RatingCity AS RatingCity,
	EXP_GetValues.o_RatingCounty AS RatingCounty,
	EXP_GetValues.o_RatingStateProvinceCode AS RatingStateProvinceCode,
	EXP_GetValues.o_RatingStateProvinceAbbreviation AS RatingStateProvinceAbbreviation,
	EXP_GetValues.o_RatingPostalCode AS RatingPostalCode,
	EXP_GetValues.o_RatingAddress AS RatingAddress,
	EXP_GetValues.o_RatingTerritory AS RatingTerritory,
	EXP_GetValues.o_EDWPremiumTransactionPKId AS EDWPremiumTransactionPKId,
	EXP_GetValues.o_EffectiveDate AS EffectiveDate,
	EXP_GetValues.o_ExpirationDate AS ExpirationDate,
	EXP_GetValues.o_IndividualRiskPremiumModification AS IndividualRiskPremiumModification,
	EXP_GetValues.o_ConstructionCode AS ConstructionCode,
	EXP_GetValues.o_WindCoverageFlag AS WindCoverageFlag,
	EXP_GetValues.o_ExposureBasis AS ExposureBasis,
	'N/A' AS o_ISOClassGroupDescription,
	-- *INF*: IIF(ISNULL(i_PriorCarrierName), 'N/A', i_PriorCarrierName)
	IFF(i_PriorCarrierName IS NULL,
		'N/A',
		i_PriorCarrierName
	) AS o_PriorCarrierName,
	-- *INF*: IIF(ISNULL(i_PriorPolicyKey), 'N/A',i_PriorPolicyKey)
	IFF(i_PriorPolicyKey IS NULL,
		'N/A',
		i_PriorPolicyKey
	) AS o_PriorPolicyKey,
	LKP_LimitOfInsurance.CoverageLimitValue AS i_CoverageLimitValue,
	LKP_LimitOfInsurance.CoverageLimitType AS i_CoverageLimitType,
	-- *INF*: LOWER(LTRIM(RTRIM(i_CoverageLimitType)))
	LOWER(LTRIM(RTRIM(i_CoverageLimitType
			)
		)
	) AS v_CoverageLimitType,
	-- *INF*: DECODE(TRUE,
	-- INSTR(v_CoverageLimitType,'peroccurrence')>0 
	-- OR INSTR(v_CoverageLimitType,'each')>0 
	-- OR INSTR(v_CoverageLimitType,'aggregate')>0,'N/A',
	-- i_CoverageLimitValue
	-- )
	DECODE(TRUE,
		REGEXP_INSTR(v_CoverageLimitType, 'peroccurrence'
		) > 0 
		OR REGEXP_INSTR(v_CoverageLimitType, 'each'
		) > 0 
		OR REGEXP_INSTR(v_CoverageLimitType, 'aggregate'
		) > 0, 'N/A',
		i_CoverageLimitValue
	) AS v_LimitOfInsurance,
	-- *INF*: IIF(ISNULL(v_LimitOfInsurance),'N/A',v_LimitOfInsurance)
	IFF(v_LimitOfInsurance IS NULL,
		'N/A',
		v_LimitOfInsurance
	) AS o_LimitOfInsurance,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'AGGREGATE LIMIT')),:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'AGGREGATE LIMIT'),
	-- 
	-- NOT ISNULL(:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'AggregateLimit')),:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'AggregateLimit'),
	-- 
	-- NOT ISNULL(:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'TotalAggregateLimit')),:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'TotalAggregateLimit'),
	-- 
	-- NOT ISNULL(:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'GENERAL AGGREGATE LIMIT')),:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'GENERAL AGGREGATE LIMIT'),
	-- 
	-- NOT ISNULL(:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'VoluntaryPropertyDamageAggregateLimit')),:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'VoluntaryPropertyDamageAggregateLimit'),
	-- 'N/A'
	-- )
	DECODE(TRUE,
		LKP_COVERAGELIMIT_i_PremiumTransactionAKID_AGGREGATE_LIMIT.CoverageLimitValue IS NOT NULL, LKP_COVERAGELIMIT_i_PremiumTransactionAKID_AGGREGATE_LIMIT.CoverageLimitValue,
		LKP_COVERAGELIMIT_i_PremiumTransactionAKID_AggregateLimit.CoverageLimitValue IS NOT NULL, LKP_COVERAGELIMIT_i_PremiumTransactionAKID_AggregateLimit.CoverageLimitValue,
		LKP_COVERAGELIMIT_i_PremiumTransactionAKID_TotalAggregateLimit.CoverageLimitValue IS NOT NULL, LKP_COVERAGELIMIT_i_PremiumTransactionAKID_TotalAggregateLimit.CoverageLimitValue,
		LKP_COVERAGELIMIT_i_PremiumTransactionAKID_GENERAL_AGGREGATE_LIMIT.CoverageLimitValue IS NOT NULL, LKP_COVERAGELIMIT_i_PremiumTransactionAKID_GENERAL_AGGREGATE_LIMIT.CoverageLimitValue,
		LKP_COVERAGELIMIT_i_PremiumTransactionAKID_VoluntaryPropertyDamageAggregateLimit.CoverageLimitValue IS NOT NULL, LKP_COVERAGELIMIT_i_PremiumTransactionAKID_VoluntaryPropertyDamageAggregateLimit.CoverageLimitValue,
		'N/A'
	) AS v_CoverageAggregateLimit,
	-- *INF*: LTRIM(RTRIM(v_CoverageAggregateLimit))
	LTRIM(RTRIM(v_CoverageAggregateLimit
		)
	) AS o_CoverageAggregateLimit,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'OccurrenceLimit')),:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'OccurrenceLimit'),
	-- 
	-- NOT ISNULL(:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'EACH OCCURRENCE LIMIT')),:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'EACH OCCURRENCE LIMIT'),
	-- 
	-- NOT ISNULL(:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'VoluntaryPropertyDamageOccurrenceLimit')),:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'VoluntaryPropertyDamageOccurrenceLimit'),
	-- 
	-- NOT ISNULL(:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'PerOccurrence')),:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'PerOccurrence'),
	-- 
	-- NOT ISNULL(:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'EachOccurrence')),:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'EachOccurrence'),
	-- 
	-- 'N/A'
	-- )
	DECODE(TRUE,
		LKP_COVERAGELIMIT_i_PremiumTransactionAKID_OccurrenceLimit.CoverageLimitValue IS NOT NULL, LKP_COVERAGELIMIT_i_PremiumTransactionAKID_OccurrenceLimit.CoverageLimitValue,
		LKP_COVERAGELIMIT_i_PremiumTransactionAKID_EACH_OCCURRENCE_LIMIT.CoverageLimitValue IS NOT NULL, LKP_COVERAGELIMIT_i_PremiumTransactionAKID_EACH_OCCURRENCE_LIMIT.CoverageLimitValue,
		LKP_COVERAGELIMIT_i_PremiumTransactionAKID_VoluntaryPropertyDamageOccurrenceLimit.CoverageLimitValue IS NOT NULL, LKP_COVERAGELIMIT_i_PremiumTransactionAKID_VoluntaryPropertyDamageOccurrenceLimit.CoverageLimitValue,
		LKP_COVERAGELIMIT_i_PremiumTransactionAKID_PerOccurrence.CoverageLimitValue IS NOT NULL, LKP_COVERAGELIMIT_i_PremiumTransactionAKID_PerOccurrence.CoverageLimitValue,
		LKP_COVERAGELIMIT_i_PremiumTransactionAKID_EachOccurrence.CoverageLimitValue IS NOT NULL, LKP_COVERAGELIMIT_i_PremiumTransactionAKID_EachOccurrence.CoverageLimitValue,
		'N/A'
	) AS v_CoveragePerOccurenceLimit,
	-- *INF*: LTRIM(RTRIM(v_CoveragePerOccurenceLimit))
	LTRIM(RTRIM(v_CoveragePerOccurenceLimit
		)
	) AS o_CoveragePerOccurrenceLimit,
	-- *INF*: IIF(i_SourceSystemId='PMS',:LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT'),
	-- :LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID,'ProductsAggregateLimit'))
	IFF(i_SourceSystemId = 'PMS',
		LKP_COVERAGELIMIT_i_PremiumTransactionAKID_PRODUCTS_COMPLETED_OPERATIONS_AGGREGATE_LIMIT.CoverageLimitValue,
		LKP_COVERAGELIMIT_i_PremiumTransactionAKID_ProductsAggregateLimit.CoverageLimitValue
	) AS v_CoverageProductAggregateLimit,
	-- *INF*: IIF(ISNULL(v_CoverageProductAggregateLimit),'N/A',v_CoverageProductAggregateLimit)
	IFF(v_CoverageProductAggregateLimit IS NULL,
		'N/A',
		v_CoverageProductAggregateLimit
	) AS o_CoverageProductAggregateLimit,
	ConstructionCode AS o_ConstructionCode,
	WindCoverageFlag AS o_WindCoverageFlag,
	-- *INF*: DECODE(TRUE,
	-- i_Count=1, 0, 
	-- ISNULL(LKP_CoverageDetailDimId), 1,  
	-- LKP_EffectiveDate <> EffectiveDate
	-- 	OR LKP_ExpirationDate <> ExpirationDate @{pipeline().parameters.UPDATE_CLAUSE},
	--  2, 0)
	'' AS o_ChangeFlag,
	EXP_GetValues.o_BaseRate,
	-- *INF*: IIF(i_SourceSystemId = 'PMS', :LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID, 'EachRelatedWrongfulEmploymentPractice'))
	IFF(i_SourceSystemId = 'PMS',
		LKP_COVERAGELIMIT_i_PremiumTransactionAKID_EachRelatedWrongfulEmploymentPractice.CoverageLimitValue
	) AS v_CoveragePerClaimLimit,
	-- *INF*: IIF(ISNULL(v_CoveragePerClaimLimit), 'N/A', v_CoveragePerClaimLimit)
	IFF(v_CoveragePerClaimLimit IS NULL,
		'N/A',
		v_CoveragePerClaimLimit
	) AS o_CoveragePerClaimLimit,
	-- *INF*: LTRIM(RTRIM(i_CoverageLimitType))
	LTRIM(RTRIM(i_CoverageLimitType
		)
	) AS o_LimitOfInsuranceDescription,
	EXP_GetValues.o_LineOfBusinessAbbreviation AS i_LineOfBusinessAbbreviation,
	EXP_GetValues.o_ProductAbbreviation AS i_ProductAbbreviation,
	-- *INF*: DECODE(TRUE,IN (i_LineOfBusinessAbbreviation,'Bonds - Fidelity', 'Bonds - Surety') , 'BND' ,
	-- IN (i_LineOfBusinessAbbreviation,'NFP D&O', 'D&O') , 'DNO' ,
	-- IN (i_LineOfBusinessAbbreviation,'CL B&M', 'CL Mine Sub','CL Prop','Cyber Security','Data Compromise') , 'CF' ,
	-- IN (i_LineOfBusinessAbbreviation,'CL IM') , 'IM',
	-- IN (i_LineOfBusinessAbbreviation,'CL Umb') , 'UMB',
	-- IN (i_LineOfBusinessAbbreviation,'CL Auto') , 'CA',
	-- IN (i_LineOfBusinessAbbreviation,'Crime') , 'CR',
	-- IN (i_LineOfBusinessAbbreviation,'E&O') , 'ENO' ,
	-- IN (i_LineOfBusinessAbbreviation,'EPLI') , 'EPLI' ,
	-- IN (i_LineOfBusinessAbbreviation,'Garage','Auto Dlrs') , 'GA' ,
	-- IN (i_LineOfBusinessAbbreviation,'Excess Liab') , 'EL' ,
	-- IN (i_LineOfBusinessAbbreviation,'GL') , 'GL' ,
	-- IN (i_LineOfBusinessAbbreviation,'WC') , 'WC' ,
	-- 'N/A')
	DECODE(TRUE,
		i_LineOfBusinessAbbreviation IN ('Bonds - Fidelity','Bonds - Surety'), 'BND',
		i_LineOfBusinessAbbreviation IN ('NFP D&O','D&O'), 'DNO',
		i_LineOfBusinessAbbreviation IN ('CL B&M','CL Mine Sub','CL Prop','Cyber Security','Data Compromise'), 'CF',
		i_LineOfBusinessAbbreviation IN ('CL IM'), 'IM',
		i_LineOfBusinessAbbreviation IN ('CL Umb'), 'UMB',
		i_LineOfBusinessAbbreviation IN ('CL Auto'), 'CA',
		i_LineOfBusinessAbbreviation IN ('Crime'), 'CR',
		i_LineOfBusinessAbbreviation IN ('E&O'), 'ENO',
		i_LineOfBusinessAbbreviation IN ('EPLI'), 'EPLI',
		i_LineOfBusinessAbbreviation IN ('Garage','Auto Dlrs'), 'GA',
		i_LineOfBusinessAbbreviation IN ('Excess Liab'), 'EL',
		i_LineOfBusinessAbbreviation IN ('GL'), 'GL',
		i_LineOfBusinessAbbreviation IN ('WC'), 'WC',
		'N/A'
	) AS v_LineOfBusinessAbbreviation,
	-- *INF*: DECODE(TRUE, v_LineOfBusinessAbbreviation='WC',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,substr(ClassCode,1,4),RatingStateProvinceCode)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,substr(ClassCode,1,4),RatingStateProvinceCode) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,substr(ClassCode,1,4),'99')) 
	-- ,v_LineOfBusinessAbbreviation='CA',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode)), :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode) ,
	-- IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,'99')) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,'99'),
	-- IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB('GA',ClassCode,RatingStateProvinceCode)), :LKP.LKP_SupClassification_LOB('GA',ClassCode,RatingStateProvinceCode), 
	--  :LKP.LKP_SupClassification_LOB('GA',ClassCode,'99'))))
	-- ,v_LineOfBusinessAbbreviation='CF',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,'99'))
	-- ,v_LineOfBusinessAbbreviation='BND',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,'99')) 
	-- ,v_LineOfBusinessAbbreviation='UMB',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,'99')) 
	-- ,v_LineOfBusinessAbbreviation='IM',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,'99')) 
	-- ,v_LineOfBusinessAbbreviation='CR',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,'99')) 
	-- ,v_LineOfBusinessAbbreviation='DNO',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,'99')) 
	-- ,v_LineOfBusinessAbbreviation='ENO',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,'99')) 
	-- ,v_LineOfBusinessAbbreviation='EPLI',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,'99')) 
	-- ,v_LineOfBusinessAbbreviation='EL',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,'99'))
	-- ,v_LineOfBusinessAbbreviation='GA',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode)), :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode) ,
	-- IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,'99')) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,'99'),
	-- IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB('CA',ClassCode,RatingStateProvinceCode)), :LKP.LKP_SupClassification_LOB('CA',ClassCode,RatingStateProvinceCode), 
	--  :LKP.LKP_SupClassification_LOB('CA',ClassCode,'99'))))
	-- ,v_LineOfBusinessAbbreviation='GL',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,RatingStateProvinceCode) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,ClassCode,'99'))
	-- )
	DECODE(TRUE,
		v_LineOfBusinessAbbreviation = 'WC', IFF(LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_ClassCode_1_4_RatingStateProvinceCode.Result IS NOT NULL,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_ClassCode_1_4_RatingStateProvinceCode.Result,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_ClassCode_1_4_99.Result
		),
		v_LineOfBusinessAbbreviation = 'CA', IFF(LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result IS NOT NULL,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result,
			IFF(LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_99.Result IS NOT NULL,
				LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_99.Result,
				IFF(LKP_SUPCLASSIFICATION_LOB__GA_ClassCode_RatingStateProvinceCode.Result IS NOT NULL,
					LKP_SUPCLASSIFICATION_LOB__GA_ClassCode_RatingStateProvinceCode.Result,
					LKP_SUPCLASSIFICATION_LOB__GA_ClassCode_99.Result
				)
			)
		),
		v_LineOfBusinessAbbreviation = 'CF', IFF(LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result IS NOT NULL,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_99.Result
		),
		v_LineOfBusinessAbbreviation = 'BND', IFF(LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result IS NOT NULL,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_99.Result
		),
		v_LineOfBusinessAbbreviation = 'UMB', IFF(LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result IS NOT NULL,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_99.Result
		),
		v_LineOfBusinessAbbreviation = 'IM', IFF(LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result IS NOT NULL,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_99.Result
		),
		v_LineOfBusinessAbbreviation = 'CR', IFF(LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result IS NOT NULL,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_99.Result
		),
		v_LineOfBusinessAbbreviation = 'DNO', IFF(LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result IS NOT NULL,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_99.Result
		),
		v_LineOfBusinessAbbreviation = 'ENO', IFF(LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result IS NOT NULL,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_99.Result
		),
		v_LineOfBusinessAbbreviation = 'EPLI', IFF(LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result IS NOT NULL,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_99.Result
		),
		v_LineOfBusinessAbbreviation = 'EL', IFF(LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result IS NOT NULL,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_99.Result
		),
		v_LineOfBusinessAbbreviation = 'GA', IFF(LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result IS NOT NULL,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result,
			IFF(LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_99.Result IS NOT NULL,
				LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_99.Result,
				IFF(LKP_SUPCLASSIFICATION_LOB__CA_ClassCode_RatingStateProvinceCode.Result IS NOT NULL,
					LKP_SUPCLASSIFICATION_LOB__CA_ClassCode_RatingStateProvinceCode.Result,
					LKP_SUPCLASSIFICATION_LOB__CA_ClassCode_99.Result
				)
			)
		),
		v_LineOfBusinessAbbreviation = 'GL', IFF(LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result IS NOT NULL,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.Result,
			LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_99.Result
		)
	) AS v_Result,
	-- *INF*: DECODE(TRUE, IN( i_ProductAbbreviation,'SMART','BOP') OR IN(i_LineOfBusinessAbbreviation,'SMP','BOP'), 'Not Assigned',  ISNULL(v_Result), 'N/A', SUBSTR(v_Result,1,INSTR(v_Result,'#')-1))
	DECODE(TRUE,
		i_ProductAbbreviation IN ('SMART','BOP') 
		OR i_LineOfBusinessAbbreviation IN ('SMP','BOP'), 'Not Assigned',
		v_Result IS NULL, 'N/A',
		SUBSTR(v_Result, 1, REGEXP_INSTR(v_Result, '#'
			) - 1
		)
	) AS o_ClassDescription,
	-- *INF*: DECODE(TRUE, IN( i_ProductAbbreviation,'SMART','BOP') OR IN(i_LineOfBusinessAbbreviation,'SMP','BOP'), 'N/A', ISNULL(v_Result), 'N/A', SUBSTR(v_Result,INSTR(v_Result,'#')+1,length(v_Result)))
	DECODE(TRUE,
		i_ProductAbbreviation IN ('SMART','BOP') 
		OR i_LineOfBusinessAbbreviation IN ('SMP','BOP'), 'N/A',
		v_Result IS NULL, 'N/A',
		SUBSTR(v_Result, REGEXP_INSTR(v_Result, '#'
			) + 1, length(v_Result
			)
		)
	) AS o_OriginatingOrganizationCode,
	-- *INF*: TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
	TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'
	) AS o_CoverageEffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
	TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'
	) AS o_CoverageExpirationDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
	TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'
	) AS o_CoverageCancellationDate,
	EXP_GetValues.o_CensusBlockGroupCountyCode AS CensusBlockGroupCountyCode,
	EXP_GetValues.o_CensusBlockGroupTractCode AS CensusBlockGroupTractCode,
	EXP_GetValues.o_CensusBlockGroupBlockGroupCode AS CensusBlockGroupBlockGroupCode,
	EXP_GetValues.o_Latitude AS Latitude,
	EXP_GetValues.o_Longitude AS Longitude
	FROM EXP_GetValues
	LEFT JOIN LKP_CoverageDetailDim
	ON LKP_CoverageDetailDim.EDWPremiumTransactionPKId = EXP_GetValues.o_EDWPremiumTransactionPKId
	LEFT JOIN LKP_LimitOfInsurance
	ON LKP_LimitOfInsurance.PremiumTransactionAKID = EXP_GetValues.o_PremiumTransactionAKID
	LEFT JOIN LKP_PriorCoverage
	ON LKP_PriorCoverage.PriorCoverageId = EXP_GetValues.o_PriorCoverageId
	LEFT JOIN LKP_COVERAGELIMIT LKP_COVERAGELIMIT_i_PremiumTransactionAKID_AGGREGATE_LIMIT
	ON LKP_COVERAGELIMIT_i_PremiumTransactionAKID_AGGREGATE_LIMIT.PremiumTransactionAKID = i_PremiumTransactionAKID
	AND LKP_COVERAGELIMIT_i_PremiumTransactionAKID_AGGREGATE_LIMIT.CoverageLimitType = 'AGGREGATE LIMIT'

	LEFT JOIN LKP_COVERAGELIMIT LKP_COVERAGELIMIT_i_PremiumTransactionAKID_AggregateLimit
	ON LKP_COVERAGELIMIT_i_PremiumTransactionAKID_AggregateLimit.PremiumTransactionAKID = i_PremiumTransactionAKID
	AND LKP_COVERAGELIMIT_i_PremiumTransactionAKID_AggregateLimit.CoverageLimitType = 'AggregateLimit'

	LEFT JOIN LKP_COVERAGELIMIT LKP_COVERAGELIMIT_i_PremiumTransactionAKID_TotalAggregateLimit
	ON LKP_COVERAGELIMIT_i_PremiumTransactionAKID_TotalAggregateLimit.PremiumTransactionAKID = i_PremiumTransactionAKID
	AND LKP_COVERAGELIMIT_i_PremiumTransactionAKID_TotalAggregateLimit.CoverageLimitType = 'TotalAggregateLimit'

	LEFT JOIN LKP_COVERAGELIMIT LKP_COVERAGELIMIT_i_PremiumTransactionAKID_GENERAL_AGGREGATE_LIMIT
	ON LKP_COVERAGELIMIT_i_PremiumTransactionAKID_GENERAL_AGGREGATE_LIMIT.PremiumTransactionAKID = i_PremiumTransactionAKID
	AND LKP_COVERAGELIMIT_i_PremiumTransactionAKID_GENERAL_AGGREGATE_LIMIT.CoverageLimitType = 'GENERAL AGGREGATE LIMIT'

	LEFT JOIN LKP_COVERAGELIMIT LKP_COVERAGELIMIT_i_PremiumTransactionAKID_VoluntaryPropertyDamageAggregateLimit
	ON LKP_COVERAGELIMIT_i_PremiumTransactionAKID_VoluntaryPropertyDamageAggregateLimit.PremiumTransactionAKID = i_PremiumTransactionAKID
	AND LKP_COVERAGELIMIT_i_PremiumTransactionAKID_VoluntaryPropertyDamageAggregateLimit.CoverageLimitType = 'VoluntaryPropertyDamageAggregateLimit'

	LEFT JOIN LKP_COVERAGELIMIT LKP_COVERAGELIMIT_i_PremiumTransactionAKID_OccurrenceLimit
	ON LKP_COVERAGELIMIT_i_PremiumTransactionAKID_OccurrenceLimit.PremiumTransactionAKID = i_PremiumTransactionAKID
	AND LKP_COVERAGELIMIT_i_PremiumTransactionAKID_OccurrenceLimit.CoverageLimitType = 'OccurrenceLimit'

	LEFT JOIN LKP_COVERAGELIMIT LKP_COVERAGELIMIT_i_PremiumTransactionAKID_EACH_OCCURRENCE_LIMIT
	ON LKP_COVERAGELIMIT_i_PremiumTransactionAKID_EACH_OCCURRENCE_LIMIT.PremiumTransactionAKID = i_PremiumTransactionAKID
	AND LKP_COVERAGELIMIT_i_PremiumTransactionAKID_EACH_OCCURRENCE_LIMIT.CoverageLimitType = 'EACH OCCURRENCE LIMIT'

	LEFT JOIN LKP_COVERAGELIMIT LKP_COVERAGELIMIT_i_PremiumTransactionAKID_VoluntaryPropertyDamageOccurrenceLimit
	ON LKP_COVERAGELIMIT_i_PremiumTransactionAKID_VoluntaryPropertyDamageOccurrenceLimit.PremiumTransactionAKID = i_PremiumTransactionAKID
	AND LKP_COVERAGELIMIT_i_PremiumTransactionAKID_VoluntaryPropertyDamageOccurrenceLimit.CoverageLimitType = 'VoluntaryPropertyDamageOccurrenceLimit'

	LEFT JOIN LKP_COVERAGELIMIT LKP_COVERAGELIMIT_i_PremiumTransactionAKID_PerOccurrence
	ON LKP_COVERAGELIMIT_i_PremiumTransactionAKID_PerOccurrence.PremiumTransactionAKID = i_PremiumTransactionAKID
	AND LKP_COVERAGELIMIT_i_PremiumTransactionAKID_PerOccurrence.CoverageLimitType = 'PerOccurrence'

	LEFT JOIN LKP_COVERAGELIMIT LKP_COVERAGELIMIT_i_PremiumTransactionAKID_EachOccurrence
	ON LKP_COVERAGELIMIT_i_PremiumTransactionAKID_EachOccurrence.PremiumTransactionAKID = i_PremiumTransactionAKID
	AND LKP_COVERAGELIMIT_i_PremiumTransactionAKID_EachOccurrence.CoverageLimitType = 'EachOccurrence'

	LEFT JOIN LKP_COVERAGELIMIT LKP_COVERAGELIMIT_i_PremiumTransactionAKID_PRODUCTS_COMPLETED_OPERATIONS_AGGREGATE_LIMIT
	ON LKP_COVERAGELIMIT_i_PremiumTransactionAKID_PRODUCTS_COMPLETED_OPERATIONS_AGGREGATE_LIMIT.PremiumTransactionAKID = i_PremiumTransactionAKID
	AND LKP_COVERAGELIMIT_i_PremiumTransactionAKID_PRODUCTS_COMPLETED_OPERATIONS_AGGREGATE_LIMIT.CoverageLimitType = 'PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT'

	LEFT JOIN LKP_COVERAGELIMIT LKP_COVERAGELIMIT_i_PremiumTransactionAKID_ProductsAggregateLimit
	ON LKP_COVERAGELIMIT_i_PremiumTransactionAKID_ProductsAggregateLimit.PremiumTransactionAKID = i_PremiumTransactionAKID
	AND LKP_COVERAGELIMIT_i_PremiumTransactionAKID_ProductsAggregateLimit.CoverageLimitType = 'ProductsAggregateLimit'

	LEFT JOIN LKP_COVERAGELIMIT LKP_COVERAGELIMIT_i_PremiumTransactionAKID_EachRelatedWrongfulEmploymentPractice
	ON LKP_COVERAGELIMIT_i_PremiumTransactionAKID_EachRelatedWrongfulEmploymentPractice.PremiumTransactionAKID = i_PremiumTransactionAKID
	AND LKP_COVERAGELIMIT_i_PremiumTransactionAKID_EachRelatedWrongfulEmploymentPractice.CoverageLimitType = 'EachRelatedWrongfulEmploymentPractice'

	LEFT JOIN LKP_SUPCLASSIFICATION_LOB LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_ClassCode_1_4_RatingStateProvinceCode
	ON LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_ClassCode_1_4_RatingStateProvinceCode.LineOfBusinessAbbreviation = v_LineOfBusinessAbbreviation
	AND LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_ClassCode_1_4_RatingStateProvinceCode.ClassCode = substr(ClassCode, 1, 4
		)
	AND LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_ClassCode_1_4_RatingStateProvinceCode.RatingStateCode = RatingStateProvinceCode

	LEFT JOIN LKP_SUPCLASSIFICATION_LOB LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_ClassCode_1_4_99
	ON LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_ClassCode_1_4_99.LineOfBusinessAbbreviation = v_LineOfBusinessAbbreviation
	AND LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_ClassCode_1_4_99.ClassCode = substr(ClassCode, 1, 4
		)
	AND LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_ClassCode_1_4_99.RatingStateCode = '99'

	LEFT JOIN LKP_SUPCLASSIFICATION_LOB LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode
	ON LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.LineOfBusinessAbbreviation = v_LineOfBusinessAbbreviation
	AND LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.ClassCode = ClassCode
	AND LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_RatingStateProvinceCode.RatingStateCode = RatingStateProvinceCode

	LEFT JOIN LKP_SUPCLASSIFICATION_LOB LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_99
	ON LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_99.LineOfBusinessAbbreviation = v_LineOfBusinessAbbreviation
	AND LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_99.ClassCode = ClassCode
	AND LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_ClassCode_99.RatingStateCode = '99'

	LEFT JOIN LKP_SUPCLASSIFICATION_LOB LKP_SUPCLASSIFICATION_LOB__GA_ClassCode_RatingStateProvinceCode
	ON LKP_SUPCLASSIFICATION_LOB__GA_ClassCode_RatingStateProvinceCode.LineOfBusinessAbbreviation = 'GA'
	AND LKP_SUPCLASSIFICATION_LOB__GA_ClassCode_RatingStateProvinceCode.ClassCode = ClassCode
	AND LKP_SUPCLASSIFICATION_LOB__GA_ClassCode_RatingStateProvinceCode.RatingStateCode = RatingStateProvinceCode

	LEFT JOIN LKP_SUPCLASSIFICATION_LOB LKP_SUPCLASSIFICATION_LOB__GA_ClassCode_99
	ON LKP_SUPCLASSIFICATION_LOB__GA_ClassCode_99.LineOfBusinessAbbreviation = 'GA'
	AND LKP_SUPCLASSIFICATION_LOB__GA_ClassCode_99.ClassCode = ClassCode
	AND LKP_SUPCLASSIFICATION_LOB__GA_ClassCode_99.RatingStateCode = '99'

	LEFT JOIN LKP_SUPCLASSIFICATION_LOB LKP_SUPCLASSIFICATION_LOB__CA_ClassCode_RatingStateProvinceCode
	ON LKP_SUPCLASSIFICATION_LOB__CA_ClassCode_RatingStateProvinceCode.LineOfBusinessAbbreviation = 'CA'
	AND LKP_SUPCLASSIFICATION_LOB__CA_ClassCode_RatingStateProvinceCode.ClassCode = ClassCode
	AND LKP_SUPCLASSIFICATION_LOB__CA_ClassCode_RatingStateProvinceCode.RatingStateCode = RatingStateProvinceCode

	LEFT JOIN LKP_SUPCLASSIFICATION_LOB LKP_SUPCLASSIFICATION_LOB__CA_ClassCode_99
	ON LKP_SUPCLASSIFICATION_LOB__CA_ClassCode_99.LineOfBusinessAbbreviation = 'CA'
	AND LKP_SUPCLASSIFICATION_LOB__CA_ClassCode_99.ClassCode = ClassCode
	AND LKP_SUPCLASSIFICATION_LOB__CA_ClassCode_99.RatingStateCode = '99'

),
RTR_CoverageDetailDim AS (
	SELECT
	LKP_CoverageDetailDimId,
	AuditID,
	CreatedDate,
	ModifiedDate,
	IncreasedLimitGroupCode,
	IncreasedLimitGroupDescription,
	PackageModificationAdjustmentGroupCode,
	PackageModificationAdjustmentGroupDescription,
	ClassCode,
	BuildingNumber,
	LocationNumber,
	DeductibleAmount,
	RiskGradeCode,
	PackageModificationAdjustmentFactor,
	YearBuilt,
	IncreasedLimitFactor,
	CoverageGUID,
	o_ISOClassGroupDescription AS ISOClassGroupDescription,
	RatingCity,
	RatingCounty,
	RatingStateProvinceCode,
	RatingStateProvinceAbbreviation,
	RatingPostalCode,
	RatingAddress,
	RatingTerritory,
	o_ClassDescription AS ISOClassDescription,
	EDWPremiumTransactionPKId,
	EffectiveDate,
	ExpirationDate,
	IndividualRiskPremiumModification,
	o_PriorCarrierName AS PriorCarrierName,
	o_PriorPolicyKey AS PriorPolicyKey,
	o_LimitOfInsurance AS LimitOfInsurance,
	o_CoveragePerOccurrenceLimit AS CoveragePerOccurrenceLimit,
	o_CoverageAggregateLimit AS CoverageAggregateLimit,
	o_CoverageProductAggregateLimit AS CoverageProductAggregateLimit,
	o_ConstructionCode AS ConstructionCode,
	o_WindCoverageFlag AS WindCoverageFlag,
	o_ChangeFlag AS ChangeFlag,
	o_BaseRate AS BaseRate,
	o_CoveragePerClaimLimit AS CoveragePerClaimLimit,
	o_LimitOfInsuranceDescription AS LimitOfInsuranceDescription,
	ExposureBasis,
	o_ClassDescription AS ClassDescription,
	o_OriginatingOrganizationCode AS OriginatingOrganizationCode,
	o_CoverageEffectiveDate AS CoverageEffectiveDate,
	o_CoverageExpirationDate AS CoverageExpirationDate,
	o_CoverageCancellationDate AS CoverageCancellationDate,
	CensusBlockGroupCountyCode,
	CensusBlockGroupTractCode,
	CensusBlockGroupBlockGroupCode,
	Latitude,
	Longitude
	FROM EXP_GetAKId
),
RTR_CoverageDetailDim_Insert AS (SELECT * FROM RTR_CoverageDetailDim WHERE ChangeFlag=1),
RTR_CoverageDetailDim_Update AS (SELECT * FROM RTR_CoverageDetailDim WHERE ChangeFlag=2),
TGT_CoverageDetailDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim
	(AuditId, CreateDate, ModifedDate, IncreasedLimitGroupCode, IncreasedLimitGroupDescription, PackageModificationAdjustmentGroupCode, PackageModificationAdjustmentGroupDescription, ISOClassCode, BuildingNumber, LocationNumber, DeductibleAmount, RiskGradeCode, PackageModificationAdjustmentFactor, YearBuilt, IncreasedLimitFactor, CoverageGuid, ClassGroupDescription, RatingCity, RatingCounty, RatingStateProvinceCode, RatingStateProvinceAbbreviation, RatingPostalCode, RatingAddress, RatingTerritory, ISOClassDescription, EDWPremiumTransactionPKId, EffectiveDate, ExpirationDate, IndividualRiskPremiumModification, PriorCarrierName, PriorPolicyKey, LimitOfInsurance, CoveragePerOccurrenceLimit, CoverageAggregateLimit, CoverageProductAggregateLimit, ConstructionCode, WindCoverageFlag, BaseRate, CoveragePerClaimLimit, LimitOfInsuranceDescription, ExposureBasis, ClassCode, ClassDescription, ClassCodeOrganizationCode, CoverageEffectiveDate, CoverageExpirationDate, CoverageCancellationDate, CensusBlockGroupCountyCode, CensusBlockGroupTractCode, CensusBlockGroupBlockGroupCode, Latitude, Longitude)
	SELECT 
	AuditID AS AUDITID, 
	CreatedDate AS CREATEDATE, 
	ModifiedDate AS MODIFEDDATE, 
	INCREASEDLIMITGROUPCODE, 
	INCREASEDLIMITGROUPDESCRIPTION, 
	PACKAGEMODIFICATIONADJUSTMENTGROUPCODE, 
	PACKAGEMODIFICATIONADJUSTMENTGROUPDESCRIPTION, 
	ClassCode AS ISOCLASSCODE, 
	BUILDINGNUMBER, 
	LOCATIONNUMBER, 
	DEDUCTIBLEAMOUNT, 
	RISKGRADECODE, 
	PACKAGEMODIFICATIONADJUSTMENTFACTOR, 
	YEARBUILT, 
	INCREASEDLIMITFACTOR, 
	CoverageGUID AS COVERAGEGUID, 
	ISOClassGroupDescription AS CLASSGROUPDESCRIPTION, 
	RATINGCITY, 
	RATINGCOUNTY, 
	RATINGSTATEPROVINCECODE, 
	RATINGSTATEPROVINCEABBREVIATION, 
	RATINGPOSTALCODE, 
	RATINGADDRESS, 
	RATINGTERRITORY, 
	ISOCLASSDESCRIPTION, 
	EDWPREMIUMTRANSACTIONPKID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	INDIVIDUALRISKPREMIUMMODIFICATION, 
	PRIORCARRIERNAME, 
	PRIORPOLICYKEY, 
	LIMITOFINSURANCE, 
	COVERAGEPEROCCURRENCELIMIT, 
	COVERAGEAGGREGATELIMIT, 
	COVERAGEPRODUCTAGGREGATELIMIT, 
	CONSTRUCTIONCODE, 
	WINDCOVERAGEFLAG, 
	BASERATE, 
	COVERAGEPERCLAIMLIMIT, 
	LIMITOFINSURANCEDESCRIPTION, 
	EXPOSUREBASIS, 
	CLASSCODE, 
	CLASSDESCRIPTION, 
	OriginatingOrganizationCode AS CLASSCODEORGANIZATIONCODE, 
	COVERAGEEFFECTIVEDATE, 
	COVERAGEEXPIRATIONDATE, 
	COVERAGECANCELLATIONDATE, 
	CENSUSBLOCKGROUPCOUNTYCODE, 
	CENSUSBLOCKGROUPTRACTCODE, 
	CENSUSBLOCKGROUPBLOCKGROUPCODE, 
	LATITUDE, 
	LONGITUDE
	FROM RTR_CoverageDetailDim_Insert
),
UPD_CoverageDetailDim AS (
	SELECT
	LKP_CoverageDetailDimId AS CoverageDetailDimId, 
	ModifiedDate, 
	IncreasedLimitGroupCode, 
	IncreasedLimitGroupDescription, 
	PackageModificationAdjustmentGroupCode, 
	PackageModificationAdjustmentGroupDescription, 
	ClassCode, 
	BuildingNumber, 
	LocationNumber, 
	DeductibleAmount, 
	RiskGradeCode, 
	PackageModificationAdjustmentFactor, 
	YearBuilt, 
	IncreasedLimitFactor, 
	CoverageGUID, 
	ISOClassGroupDescription AS ClassGroupDescription, 
	RatingCity, 
	RatingCounty, 
	RatingStateProvinceCode, 
	RatingStateProvinceAbbreviation, 
	RatingPostalCode, 
	RatingAddress, 
	RatingTerritory, 
	ISOClassDescription, 
	EffectiveDate, 
	ExpirationDate, 
	IndividualRiskPremiumModification, 
	PriorCarrierName, 
	PriorPolicyKey, 
	LimitOfInsurance, 
	CoveragePerOccurrenceLimit, 
	CoverageAggregateLimit, 
	CoverageProductAggregateLimit, 
	ConstructionCode, 
	WindCoverageFlag, 
	BaseRate, 
	CoveragePerClaimLimit, 
	LimitOfInsuranceDescription, 
	ExposureBasis, 
	ClassDescription, 
	OriginatingOrganizationCode AS ClassCodeOrganizationCode, 
	CoverageEffectiveDate AS CoverageEffectiveDate3, 
	CoverageExpirationDate AS CoverageExpirationDate3, 
	CoverageCancellationDate AS CoverageCancellationDate3, 
	CensusBlockGroupCountyCode AS CensusBlockGroupCountyCode3, 
	CensusBlockGroupTractCode AS CensusBlockGroupTractCode3, 
	CensusBlockGroupBlockGroupCode AS CensusBlockGroupBlockGroupCode3, 
	Latitude AS Latitude3, 
	Longitude AS Longitude3
	FROM RTR_CoverageDetailDim_Update
),
TGT_CoverageDetailDim_Update AS (
	@{pipeline().parameters.UPDATE_STATEMENT}
	FROM UPD_CoverageDetailDim S
),