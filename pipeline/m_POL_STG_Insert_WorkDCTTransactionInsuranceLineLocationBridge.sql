WITH
LKP_DCStatCode AS (
	SELECT
	Value,
	ObjectId
	FROM (
		SELECT 
			Value,
			ObjectId
		FROM DCStatCodeStaging
		WHERE Type = 'Exposure'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ObjectId ORDER BY Value) = 1
),
LKP_WBGLCOVNS0453STG AS (
	SELECT
	RadonRetroactiveDate,
	CoverageId
	FROM (
		SELECT 
			RadonRetroactiveDate,
			CoverageId
		FROM WBGLCoverageNS0453Stage
		WHERE RadonRetroactiveDate IS NOT NULL
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY RadonRetroactiveDate) = 1
),
LKP_DCCFPersonalPropertyStaging AS (
	SELECT
	PropertyType,
	CF_RiskId
	FROM (
		SELECT 
			PropertyType,
			CF_RiskId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCFPersonalPropertyStaging
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CF_RiskId ORDER BY PropertyType) = 1
),
LKP_SupSpecialClassGroup AS (
	SELECT
	StandardSpecialClassGroupShortDescription,
	SpecialClassGroupCode
	FROM (
		SELECT 
			StandardSpecialClassGroupShortDescription,
			SpecialClassGroupCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupSpecialClassGroup
		WHERE SourceSystemId='DCT' and CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SpecialClassGroupCode ORDER BY StandardSpecialClassGroupShortDescription) = 1
),
LKP_DCCFCoverageSpoilageStaging AS (
	SELECT
	SpoilageCoverageType,
	CoverageId
	FROM (
		SELECT case when BreakdownContamination=1 then 'BreakdownContamination' else '' end +
		case when PowerOutage=1 then 'PowerOutage' else '' end
		as SpoilageCoverageType,
		CoverageId as CoverageId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCFCoverageSpoilageStaging
		order by CoverageId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY SpoilageCoverageType) = 1
),
LKP_Exposure AS (
	SELECT
	Value,
	ObjectId
	FROM (
		select 
		A.Value as Value,
		B.Coverageid as Objectid
		from
		(SELECT 
		Value, 
		ObjectId
		FROM 
		dbo.DCLimitStaging 
		where Type = 'UnitsOfExposure' and ObjectName = 'DC_Coverage')A
		inner join
		(select 
		case when (Type =  'USLandH') then ObjectId 
		     else CoverageId  end as MatchId, 
		CoverageId
		from dbo.DCCoverageStaging
		where Type in  (
		'ManualPremium','USLandH')
		)B
		on A.ObjectId = B.MatchId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ObjectId ORDER BY Value) = 1
),
LKP_WBCDOCoverageDirectorsAndOfficersCondosStage AS (
	SELECT
	NumberOfUnits,
	CoverageId
	FROM (
		SELECT 
			NumberOfUnits,
			CoverageId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCDOCoverageDirectorsAndOfficersCondosStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY NumberOfUnits) = 1
),
LKP_DCCFOccupancyStaging AS (
	SELECT
	OccupancyType,
	SessionId
	FROM (
		SELECT LTRIM(RTRIM(OccupancyType)) AS OccupancyType,cf.SessionId AS Sessionid
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCFRiskStaging cf 
		inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCFOccupancyStaging cfo
		on cfo.sessionid =cf.sessionid and cfo.cf_riskid=cf.cf_riskid
		order by cf.SessionId,cf.CF_RiskId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionId ORDER BY OccupancyType) = 1
),
LKP_DCBPCoverageSpoilageStaging AS (
	SELECT
	Type,
	CoverageId
	FROM (
		SELECT 
			Type,
			CoverageId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBPCoverageSpoilageStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY Type) = 1
),
LKP_WBGLCoverageWB2525 AS (
	SELECT
	TransactionFinalCommissionValue,
	CoverageId
	FROM (
		SELECT 
			TransactionFinalCommissionValue,
			CoverageId
		FROM WBGLCoverageWB2525Stage
		WHERE TransactionFinalCommissionValue IS NOT NULL
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY TransactionFinalCommissionValue) = 1
),
LKP_WBCFCoverageDataCompromise AS (
	SELECT
	TransactionFinalCommissionValue,
	CoverageId
	FROM (
		select CF.TransactionFinalCommissionValue as TransactionFinalCommissionValue,CL.CoverageID as CoverageID
		 from WBCLCoverageDataCompromiseStage  CL	
		inner join WBCFCoverageDataCompromiseStage CF	
		on CF.WB_CL_CoverageDataCompromiseId = CL.WB_CL_CoverageDataCompromiseId
		Where CF.TransactionFinalCommissionValue IS NOT NULL
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY TransactionFinalCommissionValue) = 1
),
LKP_WBGLCoverageWB516GL AS (
	SELECT
	TransactionFinalCommissionValue,
	CoverageId
	FROM (
		SELECT 
			TransactionFinalCommissionValue,
			CoverageId
		FROM WBGLCoverageWB516GLStage
		WHERE TransactionFinalCommissionValue IS NOT NULL
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY TransactionFinalCommissionValue) = 1
),
LKP_WBCFCoverageEquipmentBreakdown AS (
	SELECT
	TransactionFinalCommissionValue,
	CoverageId
	FROM (
		SELECT 
			TransactionFinalCommissionValue,
			CoverageId
		FROM WBCFCoverageEquipmentBreakdownStage
		WHERE TransactionFinalCommissionValue IS NOT NULL
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY TransactionFinalCommissionValue) = 1
),
LKP_DCBPLocation AS (
	SELECT
	RetureValue,
	in_LineID,
	LineId
	FROM (
		select DCBPBuildingStage.PredominantLiabilityLiabExpBase + '--' +
		DCBPBuildingStage.ConstructionCode  +  '~~' +
		WBBPLocationAccountStage.ProtectionClassOverride as RetureValue, 
		DCLineStaging.LineId AS LineId, 
		DCBPLocationStage.BPLocationId AS BPLocationId,
		DCLocationAssociationStaging.LocationAssociationId  as LocationAssociationId
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging 
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging on DCLocationStaging.LocationId=DCLocationAssociationStaging.LocationId and ObjectName ='DC_BP_Location' and DCLocationStaging.Description ='Primary Location' 
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBPLocationStage on DCLocationAssociationStaging.ObjectId =DCBPLocationStage.BPLocationId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBPBuildingStage on DCBPLocationStage.BPLocationId = DCBPBuildingStage.BPLocationId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationAccountStage on DCLocationStaging.LocationId = WBLocationAccountStage.LocationId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCLLocationAccountStage on WBLocationAccountStage.WBLocationAccountId  = WBCLLocationAccountStage.WBLocationAccountId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBBPLocationAccountStage on WBBPLocationAccountStage.WB_CL_LocationAccountId = WBCLLocationAccountStage.WBCLLocationAccountId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLineStaging on DCLocationStaging.SessionId =DCLineStaging.SessionId and DCLineStaging.Type = 'BusinessOwners'
		order by DCLineStaging.LineId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LineId ORDER BY RetureValue) = 1
),
LKP_WBEPLCoverageEmploymentPracticesLiabilityStage AS (
	SELECT
	TotalNumberOfEmployees,
	CoverageId
	FROM (
		SELECT 
			TotalNumberOfEmployees,
			CoverageId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBEPLCoverageEmploymentPracticesLiabilityStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY TotalNumberOfEmployees) = 1
),
LKP_DCCROccupancyStaging AS (
	SELECT
	OccupancyTypeMonoline,
	SessionId,
	CrimeClass
	FROM (
		SELECT 
			OccupancyTypeMonoline,
			SessionId,
			CrimeClass
		FROM DCCROccupancyStage
		WHERE CrimeClass IS NOT NULL
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionId ORDER BY OccupancyTypeMonoline DESC) = 1
),
LKP_DCCRRiskType AS (
	SELECT
	RiskType,
	in_SessionId,
	SessionId
	FROM (
		select  
		CASE WHEN charindex(' ',DcCrRiskCrimeStage.Type)>1 THEN substring(DcCrRiskCrimeStage.Type,1,charindex('_',DcCrRiskCrimeStage.Type)-1)
		ELSE DcCrRiskCrimeStage.Type END as RiskType,
		DCCRLineStage.SessionId as sessionid,
		DCCRLineStage.LineId as LineId,
		DcCrRiskStage.CRRiskId as RiskId
		 from 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLineStaging 
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCROccupancyStage on DCLineStaging.SessionId = DCCROccupancyStage.SessionId and DCLineStaging.Type ='Crime'
		inner  join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCRLineStage on DCLineStaging.LineId = DCCRLineStage.LineId 
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DcCrRiskStage on DCCROccupancyStage.CR_OccupancyId = DcCrRiskStage.CrOccupancyId  
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DcCrRiskCrimeStage on DcCrRiskStage.CrRiskId =DcCrRiskCrimeStage.CrRiskId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionId ORDER BY RiskType) = 1
),
LKP_DCStatCodeStaging AS (
	SELECT
	Value,
	ObjectId,
	SessionId
	FROM (
		SELECT 
			Value,
			ObjectId,
			SessionId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCStatCodeStaging
		WHERE ObjectName='DC_Coverage' and Type='Class' and Value <> '9999'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ObjectId,SessionId ORDER BY Value) = 1
),
LKP_DC_LIMIT AS (
	SELECT
	Value,
	i_CVG_Type,
	RiskId,
	Type,
	CVG_Type
	FROM (
		SELECT
		l.value as Value,
		r.BPRiskId as RiskId,
		l.Type as Type,
		C.type as CVG_Type
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLimitStaging l
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCoverageStaging c
		on l.ObjectId=c.CoverageId and l.SessionId=c.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBPRiskStage r
		on r.BPRiskId=c.ObjectId and r.SessionId=c.SessionId
		WHERE l.ObjectName='DC_Coverage' and l.Type in ('standard','payroll','GrossSales')
		AND c.Type in ('Building','PersonalProperty','RiskLiability','FunctionalBuildingValuation','PersonalPropertyOfOthers','ImprovementsAndBetterments')
		AND l.value<>'0'
		--Added additional Coverage types and exclude zero limit values
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskId,Type,CVG_Type ORDER BY Value) = 1
),
LKP_DCBPCoverageBusinessIncomeExtendedPeriodStage AS (
	SELECT
	Days,
	CoverageId
	FROM (
		SELECT 
			Days,
			CoverageId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBPCoverageBusinessIncomeExtendedPeriodStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY Days DESC) = 1
),
LKP_DCBPCoverageBusinessIncomeOrdinaryPayrollStage AS (
	SELECT
	Days,
	CoverageId
	FROM (
		SELECT 
			Days,
			CoverageId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBPCoverageBusinessIncomeOrdinaryPayrollStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY Days DESC) = 1
),
LKP_WBBPCoverageDataCompromiseStage AS (
	SELECT
	TransactionFinalCommissionValue,
	CoverageId
	FROM (
		select BP.TransactionFinalCommissionValue as TransactionFinalCommissionValue, CL.CoverageId as CoverageId
		from WBBPCoverageDataCompromiseStage BP
		join WBCLCoverageDataCompromiseStage CL  on BP.WB_CL_CoverageDataCompromiseId=CL.WB_CL_CoverageDataCompromiseId
		where BP.TransactionFinalCommissionValue is not null
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY TransactionFinalCommissionValue) = 1
),
LKP_WBBPCoverageEmploymentPracticesLiabilityStage AS (
	SELECT
	TransactionFinalCommissionValue,
	CoverageId
	FROM (
		SELECT 
			TransactionFinalCommissionValue,
			CoverageId
		FROM WBBPCoverageEmploymentPracticesLiabilityStage
		WHERE TransactionFinalCommissionValue IS NOT NULL
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY TransactionFinalCommissionValue) = 1
),
LKP_WBBPCoverageEquipBreakdownStage AS (
	SELECT
	TransactionFinalCommissionValue,
	CoverageId
	FROM (
		SELECT 
			TransactionFinalCommissionValue,
			CoverageId
		FROM WBBPCoverageEquipBreakdownStage
		WHERE TransactionFinalCommissionValue IS NOT NULL
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY TransactionFinalCommissionValue) = 1
),
LKP_RetroactiveDate AS (
	SELECT
	RetroactiveDate,
	CoverageId
	FROM (
		select RetroactiveDate as RetroactiveDate,
		CoverageId as CoverageId
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCDOCoverageDirectorsAndOfficersCondosStage
		
		union all
		
		select RetroactiveDate as RetroactiveDate,
		CoverageId as CoverageId
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBEPLCoverageEmploymentPracticesLiabilityStage
		
		union all
		
		select RetroactiveDate as RetroactiveDate,
		CoverageId as CoverageId
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBNDOCoverageDirectorsAndOfficersNFPStage
		
		union all
		
		select RetroactiveDate as RetroactiveDate,
		CoverageId as CoverageId
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBGLCoverageWB516GLStage
		
		union all
		
		select RetroactiveDate as RetroactiveDate,
		CoverageId as CoverageId
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBBPCoverageEmploymentPracticesLiabilityStage
		
		union all
		
		select a.RetroactiveDate as RetroactiveDate,
		b.CoverageId as CoverageId
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCAEndorsementWB516Stage a
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCoverageStage b
		on a.WB_CoverageId=b.WBCoverageId
		
		union all
		
		select RetroactiveDate as RetroactiveDate,
		CoverageId as CoverageId
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBGLCoverageWB2579Stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY RetroactiveDate) = 1
),
LKP_WorkDCTPolicy AS (
	SELECT
	WBProductType,
	SessionId
	FROM (
		SELECT 
			WBProductType,
			SessionId
		FROM WorkDCTPolicy
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionId ORDER BY WBProductType) = 1
),
LKP_DCGLCoverageRailroadProtectiveLiabilityStaging AS (
	SELECT
	Exposure,
	CoverageId
	FROM (
		SELECT 
			Exposure,
			CoverageId
		FROM DCGLCoverageRailroadProtectiveLiabilityStaging
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY Exposure) = 1
),
LKP_DCGLCoverageOwnersContractorsOrPrincipalsStaging AS (
	SELECT
	Exposure,
	CoverageId
	FROM (
		SELECT 
			Exposure,
			CoverageId
		FROM DCGLCoverageOwnersContractorsOrPrincipalsStaging
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY Exposure) = 1
),
SQ_WorkDCTCoverageTransaction AS (
	select 
	sessionid,
	ParentCoverageObjectId,
	ParentCoverageObjectName,
	CoverageId,
	ParentCoverageType,
	SubCoverageType
	
	from dbo.WorkDCTCoverageTransaction
	where
	NOT (WorkDCTCoverageTransaction.ParentCoverageObjectName IN ('DC_WC_State') and Not (Parentcoveragetype = 'RetrospectiveCalculation'))
),
EXP_Default AS (
	SELECT
	SessionId,
	ParentCoverageObjectId,
	ParentCoverageObjectName,
	CoverageId,
	ParentCoverageType,
	SubCoverageType
	FROM SQ_WorkDCTCoverageTransaction
),
RTR_InsuranceLine AS (
	SELECT
	SessionId,
	ParentCoverageObjectId,
	ParentCoverageObjectName,
	CoverageId,
	ParentCoverageType AS CoverageType,
	SubCoverageType
	FROM EXP_Default
),
RTR_InsuranceLine_DC_Line AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='DC_Line'),
RTR_InsuranceLine_DC_GL_Risk AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='DC_GL_Risk'),
RTR_InsuranceLine_DC_WC_Risk AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='DC_WC_Risk'),
RTR_InsuranceLine_DC_CR_RiskCrime AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='DC_CR_RiskCrime'),
RTR_InsuranceLine_DC_IM_CoverageForm AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='DC_IM_CoverageForm'),
RTR_InsuranceLine_DC_IM_Risk AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='DC_IM_Risk'),
RTR_InsuranceLine_WB_HIO_Risk AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='WB_HIO_Risk'),
RTR_InsuranceLine_WB_HIO_State AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='WB_HIO_State'),
RTR_InsuranceLine_WB_GOC_Risk AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='WB_GOC_Risk'),
RTR_InsuranceLine_DC_BP_Risk AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='DC_BP_Risk'),
RTR_InsuranceLine_DC_BP_Location AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='DC_BP_Location'),
RTR_InsuranceLine_DC_CR_Endorsement AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='DC_CR_Endorsement'),
RTR_InsuranceLine_DC_CF_Risk AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='DC_CF_Risk'),
RTR_InsuranceLine_DC_CA_State AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='DC_CA_State'),
RTR_InsuranceLine_DC_CA_Risk AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='DC_CA_Risk'),
RTR_InsuranceLine_WB_CU_PremiumDetail AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='WB_CU_PremiumDetail'),
RTR_InsuranceLine_DC_CU_UmbrellaEmployersLiability AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='DC_CU_UmbrellaEmployersLiability'),
RTR_InsuranceLine_DC_WC_StateTerm AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='DC_WC_StateTerm'),
RTR_InsuranceLine_WB_GOC_State AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='WB_GOC_State'),
RTR_InsuranceLine_WB_IM_State AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='WB_IM_State'),
RTR_InsuranceLine_DC_CR_Risk AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='DC_CR_Risk'),
RTR_InsuranceLine_DC_CA_BusinessInterruption AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='DC_CA_BusinessInterruptionOption'),
RTR_InsuranceLine_WB_EC_Risk AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='WB_EC_Risk'),
RTR_InsuranceLine_WB_EC_State AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='WB_EC_State'),
RTR_InsuranceLine_DC_Location AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='DC_Location'),
RTR_InsuranceLine_DC_WC_State AS (SELECT * FROM RTR_InsuranceLine WHERE ParentCoverageObjectName='DC_WC_State'),
LKP_DC_BP_Location AS (
	SELECT
	LocationAssociationId,
	LineId,
	PredominantLiabilityLiabExpBase,
	ConstructionCode,
	ProtectionClassOverride,
	BuildingNumber,
	OccupancyClassDescription,
	ActiveBuildingFlag,
	BPLocationId
	FROM (
		SELECT DCLocationAssociationStaging.LocationAssociationId AS LocationAssociationID,
			DCLineStaging.LineId AS LineID,
			Null AS PredominantLiabilityLiabExpBase,
			NULL AS ConstructionCode,
			WBBPLocationAccountStage.ProtectionClassOverride AS ProtectionClassOverride,
			NULL AS BuildingNumber,
			NULL AS OccupancyClassDescription,
			NULL AS ActiveBuildingFlag,
			DCBPLocationStage.BPLocationId AS BPLocationID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging
			ON DCLocationStaging.SessionId = DCLocationAssociationStaging.SessionId
				AND DCLocationStaging.LocationId = DCLocationAssociationStaging.LocationId
				AND ObjectName = 'DC_BP_Location'
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBPLocationStage
			ON DCLocationAssociationStaging.SessionId = DCBPLocationStage.SessionId
				AND DCLocationAssociationStaging.ObjectId = DCBPLocationStage.BPLocationId
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationAccountStage
			ON DCLocationStaging.SessionId = WBLocationAccountStage.SessionId
				AND DCLocationStaging.LocationId = WBLocationAccountStage.LocationId
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCLLocationAccountStage
			ON WBLocationAccountStage.SessionId = WBCLLocationAccountStage.SessionId
				AND WBLocationAccountStage.WBLocationAccountId = WBCLLocationAccountStage.WBLocationAccountId
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBBPLocationAccountStage
			ON WBBPLocationAccountStage.SessionId = WBCLLocationAccountStage.SessionId
				AND WBBPLocationAccountStage.WB_CL_LocationAccountId = WBCLLocationAccountStage.WBCLLocationAccountId
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLineStaging
			ON DCLocationStaging.SessionId = DCLineStaging.SessionId
				AND DCLineStaging.Type = 'BusinessOwners'
		ORDER BY DCBPLocationStage.BPLocationId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY BPLocationId ORDER BY LocationAssociationId) = 1
),
EXP_DC_BP_Location AS (
	SELECT
	LKP_DC_BP_Location.LocationAssociationId,
	RTR_InsuranceLine_DC_BP_Location.SessionId,
	LKP_DC_BP_Location.LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_DC_BP_Location.CoverageId,
	RTR_InsuranceLine_DC_BP_Location.CoverageType,
	-1 AS RiskId,
	'N/A' AS RiskType,
	LKP_DC_BP_Location.PredominantLiabilityLiabExpBase,
	-1 AS CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	LKP_DC_BP_Location.BuildingNumber AS i_BuildingNumber,
	-- *INF*: LPAD(TO_INTEGER(i_BuildingNumber),3,'0')
	LPAD(CAST(i_BuildingNumber AS INTEGER), 3, '0') AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	'N/A' AS OccupancyType,
	LKP_DC_BP_Location.ConstructionCode,
	LKP_DC_BP_Location.ProtectionClassOverride,
	LKP_DC_BP_Location.OccupancyClassDescription,
	LKP_DC_BP_Location.ActiveBuildingFlag
	FROM RTR_InsuranceLine_DC_BP_Location
	LEFT JOIN LKP_DC_BP_Location
	ON LKP_DC_BP_Location.BPLocationId = RTR_InsuranceLine.ParentCoverageObjectId12
),
LKP_DC_BP_Risk AS (
	SELECT
	LocationAssociationId,
	LineId,
	ConstructionCode,
	OccupancyType,
	BOPNewLiabExpBase,
	Sprinkler,
	PredominantPersonalPropertyRateNumber,
	PredominantBuildingLiabClassGroup,
	ProtectionClassOverride,
	OccupancyOccupied,
	OccupancyPercentage,
	BuildingNumber,
	OccupancyClassDescription,
	ActiveBuildingFlag,
	YearBuilt,
	RiskId,
	PredominantBuildingBCCCode,
	PredominantBuildingClassCodeDescription
	FROM (
		SELECT DCLA.LocationAssociationId AS LocationAssociationId,
			BPRisk.LineId AS LineId,
			BPBuilding.ConstructionCode AS ConstructionCode,
			DCBPO.OccupancyType AS OccupancyType,
			ISNULL(DCBPO.BOPNewLiabExpBaseOverride, DCBPO.BOPNewLiabExpBase) AS BOPNewLiabExpBase,
			BPBuilding.Sprinkler AS Sprinkler,
			BPBuilding.PredominantPersonalPropertyRateNumber AS PredominantPersonalPropertyRateNumber,
			BPBuilding.PredominantBuildingLiabClassGroup AS PredominantBuildingLiabClassGroup,
			WBBPLocationAccountStage.ProtectionClassOverride AS ProtectionClassOverride,
			BPRisk.OccupancyOccupied AS OccupancyOccupied,
			BPRisk.OccupancyPercentage AS OccupancyPercentage,
			WBBPBuildingStage.LocationBuildingNumberShadow AS BuildingNumber,
			DCBPO.DescriptionBOP AS OccupancyClassDescription,
			CASE ISNULL(BPRisk.Deleted, 0)
				WHEN 0
					THEN '1'
				ELSE '0'
				END AS ActiveBuildingFlag,
			BPBuilding.YearBuilt AS YearBuilt,
			BPRisk.BPRiskId AS RiskId,
		      WBBPBuildingStage.PredominantBuildingBCCCode AS PredominantBuildingBCCCode,
		      WBBPBuildingStage.PredominantBuildingClassCodeDescription AS PredominantBuildingClassCodeDescription
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBPRiskStage BPRisk
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBPBuildingStage BPBuilding
			ON BPRisk.BPBuildingId = BPBuilding.BPBuildingId
				AND BPRisk.Sessionid = BPBuilding.Sessionid
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBBPBuildingStage
			ON WBBPBuildingStage.BP_BuildingId = BPBuilding.BPBuildingId
				AND WBBPBuildingStage.Sessionid = BPBuilding.Sessionid
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging DCLA
			ON DCLA.ObjectId = BPBuilding.BPLocationId
				AND DCLA.Sessionid = BPBuilding.Sessionid
				AND DCLA.ObjectName = 'DC_BP_Location'
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBPOccupancyStage DCBPO
			ON DCBPO.BPRiskId = BPRisk.BPRiskId
				AND DCBPO.Sessionid = BPRisk.Sessionid
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationAccountStage WBLocationAccountStage
			ON DCLA.LocationId = WBLocationAccountStage.LocationId
				AND DCLA.Sessionid = WBLocationAccountStage.Sessionid
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCLLocationAccountStage WBCLLocationAccountStage
			ON WBLocationAccountStage.WBLocationAccountId = WBCLLocationAccountStage.WBLocationAccountId
				AND WBLocationAccountStage.Sessionid = WBCLLocationAccountStage.Sessionid
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBBPLocationAccountStage WBBPLocationAccountStage
			ON WBBPLocationAccountStage.WB_CL_LocationAccountId = WBCLLocationAccountStage.WBCLLocationAccountId
				AND WBBPLocationAccountStage.Sessionid = WBCLLocationAccountStage.Sessionid
		ORDER BY BPRisk.BPRiskId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskId ORDER BY LocationAssociationId) = 1
),
EXP_DC_BP_Risk AS (
	SELECT
	LKP_DC_BP_Risk.OccupancyOccupied AS i_OccupancyOccupied,
	LKP_DC_BP_Risk.OccupancyPercentage AS i_OccupancyPercentage,
	RTR_InsuranceLine_DC_BP_Risk.CoverageType AS i_CoverageType,
	LKP_DC_BP_Risk.Sprinkler AS i_Sprinkler,
	LKP_DC_BP_Risk.LocationAssociationId,
	RTR_InsuranceLine_DC_BP_Risk.SessionId,
	LKP_DC_BP_Risk.LineId,
	RTR_InsuranceLine_DC_BP_Risk.CoverageId,
	LKP_DC_BP_Risk.RiskId,
	LKP_DC_BP_Risk.ConstructionCode,
	LKP_DC_BP_Risk.OccupancyType,
	LKP_DC_BP_Risk.BOPNewLiabExpBase,
	LKP_DC_BP_Risk.PredominantPersonalPropertyRateNumber,
	LKP_DC_BP_Risk.PredominantBuildingLiabClassGroup AS PredominantLiabilityLiabClassGroup,
	LKP_DC_BP_Risk.ProtectionClassOverride,
	'N/A' AS IndividualRiskPremiumModification,
	-- *INF*: IIF(LTRIM(RTRIM(i_CoverageType)) != 'Spoilage',i_CoverageType,i_CoverageType || IIF(NOT ISNULL(:LKP.LKP_DCBPCOVERAGESPOILAGESTAGING(CoverageId)),:LKP.LKP_DCBPCOVERAGESPOILAGESTAGING(CoverageId),''))
	IFF(
	    LTRIM(RTRIM(i_CoverageType)) != 'Spoilage', i_CoverageType,
	    i_CoverageType || 
	    IFF(
	        LKP_DCBPCOVERAGESPOILAGESTAGING_CoverageId.Type IS NOT NULL,
	        LKP_DCBPCOVERAGESPOILAGESTAGING_CoverageId.Type,
	        ''
	    )
	) AS v_CoverageType,
	v_CoverageType AS o_CoverageType,
	'N/A' AS RiskType,
	-- *INF*: :LKP.LKP_DC_LIMIT(RiskId,'standard','Building')
	LKP_DC_LIMIT_RiskId_standard_Building.Value AS v_Limit_BLDG,
	-- *INF*: :LKP.LKP_DC_LIMIT(RiskId,'standard','FunctionalBuildingValuation')
	LKP_DC_LIMIT_RiskId_standard_FunctionalBuildingValuation.Value AS v_Limit_BLDGFUNCVAL,
	-- *INF*: :LKP.LKP_DC_LIMIT(RiskId,'standard','ImprovementsAndBetterments')
	LKP_DC_LIMIT_RiskId_standard_ImprovementsAndBetterments.Value AS v_Limit_IMPROV,
	-- *INF*: :LKP.LKP_DC_LIMIT(RiskId,'standard','PersonalProperty')
	LKP_DC_LIMIT_RiskId_standard_PersonalProperty.Value AS v_Limit_BUSPTY,
	-- *INF*: :LKP.LKP_DC_LIMIT(RiskId,'standard','PersonalPropertyOfOthers')
	LKP_DC_LIMIT_RiskId_standard_PersonalPropertyOfOthers.Value AS v_Limit_PPTYO,
	-- *INF*: IIF(isnull(v_Limit_BLDG),IIF(ISNULL(v_Limit_BLDGFUNCVAL),v_Limit_IMPROV,v_Limit_BLDGFUNCVAL),v_Limit_BLDG)
	IFF(
	    v_Limit_BLDG IS NULL,
	    IFF(
	        v_Limit_BLDGFUNCVAL IS NULL, v_Limit_IMPROV, v_Limit_BLDGFUNCVAL
	    ),
	    v_Limit_BLDG
	) AS v_Owner_LT_10,
	-- *INF*: IIF(ISNULL(v_Limit_BUSPTY),v_Limit_PPTYO,v_Limit_BUSPTY)
	IFF(v_Limit_BUSPTY IS NULL, v_Limit_PPTYO, v_Limit_BUSPTY) AS v_Owner_GT_10_Tenant_CondoUnitOwner,
	-- *INF*: IIF( isnull(:LKP.LKP_DC_LIMIT(RiskId,'payroll','RiskLiability')), '0',:LKP.LKP_DC_LIMIT(RiskId,'payroll','RiskLiability'))
	IFF(
	    LKP_DC_LIMIT_RiskId_payroll_RiskLiability.Value IS NULL, '0',
	    LKP_DC_LIMIT_RiskId_payroll_RiskLiability.Value
	) AS v_Limit_Payroll,
	-- *INF*: IIF( isnull(:LKP.LKP_DC_LIMIT(RiskId,'GrossSales','RiskLiability')), '0',:LKP.LKP_DC_LIMIT(RiskId,'GrossSales','RiskLiability'))
	IFF(
	    LKP_DC_LIMIT_RiskId_GrossSales_RiskLiability.Value IS NULL, '0',
	    LKP_DC_LIMIT_RiskId_GrossSales_RiskLiability.Value
	) AS v_Limit_Sales,
	-- *INF*: DECODE(TRUE,
	-- BOPNewLiabExpBase='LOI',
	-- DECODE(TRUE,
	-- i_OccupancyOccupied='Owner' AND (i_OccupancyPercentage<=10 OR ISNULL(i_OccupancyPercentage)) ,v_Owner_LT_10,
	-- IN(i_OccupancyOccupied,'Tenant','CondoUnitOwner') OR (i_OccupancyOccupied='Owner' AND i_OccupancyPercentage>10),v_Owner_GT_10_Tenant_CondoUnitOwner,
	-- '-1'),
	-- BOPNewLiabExpBase='PAY', v_Limit_Payroll,
	-- BOPNewLiabExpBase='SALES', v_Limit_Sales,
	-- '-1'
	-- )
	DECODE(
	    TRUE,
	    BOPNewLiabExpBase = 'LOI', DECODE(
	        TRUE,
	        i_OccupancyOccupied = 'Owner' AND (i_OccupancyPercentage <= 10 OR i_OccupancyPercentage IS NULL), v_Owner_LT_10,
	        i_OccupancyOccupied IN ('Tenant','CondoUnitOwner') OR (i_OccupancyOccupied = 'Owner' AND i_OccupancyPercentage > 10), v_Owner_GT_10_Tenant_CondoUnitOwner,
	        '-1'
	    ),
	    BOPNewLiabExpBase = 'PAY', v_Limit_Payroll,
	    BOPNewLiabExpBase = 'SALES', v_Limit_Sales,
	    '-1'
	) AS v_Exposure,
	-- *INF*: IIF(
	-- ISNULL(v_Exposure) OR NOT IS_NUMBER(v_Exposure),
	-- -1,TO_DECIMAL(v_Exposure))
	IFF(
	    v_Exposure IS NULL OR NOT REGEXP_LIKE(v_Exposure, '^[0-9]+$'), - 1,
	    CAST(v_Exposure AS FLOAT)
	) AS o_Exposure,
	-1 AS CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	LKP_DC_BP_Risk.BuildingNumber AS i_BuildingNumber,
	-- *INF*: LPAD(TO_INTEGER(i_BuildingNumber),3,'0')
	LPAD(CAST(i_BuildingNumber AS INTEGER), 3, '0') AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	-- *INF*: DECODE(i_Sprinkler, 'T', 1, 'F', 0, NULL)
	DECODE(
	    i_Sprinkler,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Sprinkler,
	-- *INF*: DECODE(TRUE,
	-- i_OccupancyOccupied='Owner' and i_OccupancyPercentage>10,'Occupant Liability',
	-- i_OccupancyOccupied='Owner' and i_OccupancyPercentage<=10,'Lessors Liability',
	-- i_OccupancyOccupied='Tenant' or i_OccupancyOccupied='CondoUnitOwner','Occupant Liability',
	-- 'N/A'
	-- )
	-- 
	-- 
	-- 
	-- --DECODE(TRUE,i_OccupancyOccupied='Owner' and i_OccupancyPercentage>10,'Occupant Liability',
	-- --i_OccupancyOccupied='Owner' and i_OccupancyPercentage<=10,'Lessors Liability','N/A')
	DECODE(
	    TRUE,
	    i_OccupancyOccupied = 'Owner' and i_OccupancyPercentage > 10, 'Occupant Liability',
	    i_OccupancyOccupied = 'Owner' and i_OccupancyPercentage <= 10, 'Lessors Liability',
	    i_OccupancyOccupied = 'Tenant' or i_OccupancyOccupied = 'CondoUnitOwner', 'Occupant Liability',
	    'N/A'
	) AS o_ISOOccupancyType,
	LKP_DC_BP_Risk.OccupancyClassDescription,
	LKP_DC_BP_Risk.ActiveBuildingFlag,
	LKP_DC_BP_Risk.YearBuilt,
	LKP_DC_BP_Risk.PredominantBuildingBCCCode,
	LKP_DC_BP_Risk.PredominantBuildingClassCodeDescription
	FROM RTR_InsuranceLine_DC_BP_Risk
	LEFT JOIN LKP_DC_BP_Risk
	ON LKP_DC_BP_Risk.RiskId = RTR_InsuranceLine.ParentCoverageObjectId11
	LEFT JOIN LKP_DCBPCOVERAGESPOILAGESTAGING LKP_DCBPCOVERAGESPOILAGESTAGING_CoverageId
	ON LKP_DCBPCOVERAGESPOILAGESTAGING_CoverageId.CoverageId = CoverageId

	LEFT JOIN LKP_DC_LIMIT LKP_DC_LIMIT_RiskId_standard_Building
	ON LKP_DC_LIMIT_RiskId_standard_Building.RiskId = RiskId
	AND LKP_DC_LIMIT_RiskId_standard_Building.Type = 'standard'
	AND LKP_DC_LIMIT_RiskId_standard_Building.CVG_Type = 'Building'

	LEFT JOIN LKP_DC_LIMIT LKP_DC_LIMIT_RiskId_standard_FunctionalBuildingValuation
	ON LKP_DC_LIMIT_RiskId_standard_FunctionalBuildingValuation.RiskId = RiskId
	AND LKP_DC_LIMIT_RiskId_standard_FunctionalBuildingValuation.Type = 'standard'
	AND LKP_DC_LIMIT_RiskId_standard_FunctionalBuildingValuation.CVG_Type = 'FunctionalBuildingValuation'

	LEFT JOIN LKP_DC_LIMIT LKP_DC_LIMIT_RiskId_standard_ImprovementsAndBetterments
	ON LKP_DC_LIMIT_RiskId_standard_ImprovementsAndBetterments.RiskId = RiskId
	AND LKP_DC_LIMIT_RiskId_standard_ImprovementsAndBetterments.Type = 'standard'
	AND LKP_DC_LIMIT_RiskId_standard_ImprovementsAndBetterments.CVG_Type = 'ImprovementsAndBetterments'

	LEFT JOIN LKP_DC_LIMIT LKP_DC_LIMIT_RiskId_standard_PersonalProperty
	ON LKP_DC_LIMIT_RiskId_standard_PersonalProperty.RiskId = RiskId
	AND LKP_DC_LIMIT_RiskId_standard_PersonalProperty.Type = 'standard'
	AND LKP_DC_LIMIT_RiskId_standard_PersonalProperty.CVG_Type = 'PersonalProperty'

	LEFT JOIN LKP_DC_LIMIT LKP_DC_LIMIT_RiskId_standard_PersonalPropertyOfOthers
	ON LKP_DC_LIMIT_RiskId_standard_PersonalPropertyOfOthers.RiskId = RiskId
	AND LKP_DC_LIMIT_RiskId_standard_PersonalPropertyOfOthers.Type = 'standard'
	AND LKP_DC_LIMIT_RiskId_standard_PersonalPropertyOfOthers.CVG_Type = 'PersonalPropertyOfOthers'

	LEFT JOIN LKP_DC_LIMIT LKP_DC_LIMIT_RiskId_payroll_RiskLiability
	ON LKP_DC_LIMIT_RiskId_payroll_RiskLiability.RiskId = RiskId
	AND LKP_DC_LIMIT_RiskId_payroll_RiskLiability.Type = 'payroll'
	AND LKP_DC_LIMIT_RiskId_payroll_RiskLiability.CVG_Type = 'RiskLiability'

	LEFT JOIN LKP_DC_LIMIT LKP_DC_LIMIT_RiskId_GrossSales_RiskLiability
	ON LKP_DC_LIMIT_RiskId_GrossSales_RiskLiability.RiskId = RiskId
	AND LKP_DC_LIMIT_RiskId_GrossSales_RiskLiability.Type = 'GrossSales'
	AND LKP_DC_LIMIT_RiskId_GrossSales_RiskLiability.CVG_Type = 'RiskLiability'

),
LKP_DC_CA_BusinessInterruption AS (
	SELECT
	LocationAssociationId,
	LineType,
	LineId,
	CoverageForm,
	CommissionPercentage,
	CompositeRating,
	OptionRating,
	TotalExposureOptionB,
	CA_BusinessInterruptionOptionId
	FROM (
		SELECT 
		-- There can be multiple locations for a given risk and there is no direct linkage between the coverage and location, so the 
		-- correlated sub-query below grabs the location with the the lowest location number (usually location 1 in most instances).
			LocationAssociationId.LocationAssociationId as LocationAssociationId,
		    DCLine.Type as LineType,
			DCLine.LineId AS LineId,
			ISNULL(WBCALine.CoverageForm, WBCALine.PolicyType) AS CoverageForm,
			WBLine.FinalCommission AS CommissionPercentage,
			DCCALine.CompositeRating AS CompositeRating,
			DCCABIO.OptionType as OptionRating, 
			DCCABIO.TotalExposureOptionB as TotalExposureOptionB,
			DCCABIO.CA_BusinessInterruptionOptionId as CA_BusinessInterruptionOptionId
		FROM DCCABusinessInterruptionOptionStage DCCABIO
		INNER JOIN DCCABusinessInterruptionEndorsementStage DCCABIE ON DCCABIE.CA_BusinessInterruptionEndorsementId = DCCABIO.CA_BusinessInterruptionEndorsementId 
		INNER JOIN dbo.DCLineStaging DCLine ON DCLine.LineId = DCCABIE.LineId 
		LEFT OUTER JOIN dbo.DCCALineStaging DCCALine ON DCLine.SessionId = DCCALine.SessionId AND DCLine.LineId = DCCALine.LineId
		LEFT OUTER JOIN dbo.WBCALineStaging WBCALine ON DCCALine.SessionId = WBCALine.SessionId AND DCCALine.CA_LineId = WBCALine.CA_LineId
		LEFT OUTER JOIN dbo.WBLineStaging WBLine ON WBCALine.SessionId = WBLine.SessionId AND DCline.LineId = WBLine.LineId
		-- Informatica doesn't like the correlated sub query so using a cross apply
		Cross Apply
		(
		SELECT TOP 1 LocationAssociation.LocationAssociationId
		    FROM dbo.DCLocationAssociationStaging AS LocationAssociation
		    INNER JOIN dbo.WBLocationStaging AS WBLoc
		         ON WBLoc.LocationId = LocationAssociation.LocationId
		            AND WBLoc.SessionId = LocationAssociation.SessionId
		    WHERE WBLoc.SessionId = DCLine.SessionId
		          AND (ltrim(rtrim(LocationAssociation.LocationAssociationType)) = CASE
		                                                                WHEN DCLine.Type = 'BusinessOwners' THEN 'BP_Location'
		                                                                WHEN DCLine.Type = 'CommercialAuto' THEN 'CA_Location'
		                                                                WHEN DCLine.Type IN('Property', 'SBOPProperty') THEN 'CF_Location'
		                                                                WHEN DCLine.Type = 'Crime' THEN 'CR_Location'
		                                                                WHEN DCLine.Type IN('SBOPGeneralLiability', 'GeneralLiability') THEN 'GL_Location'
		                                                                WHEN DCLine.Type = 'InlandMarine' THEN 'IM_Location'
		                                                                WHEN DCLine.Type = 'WorkersCompensation' THEN 'WC_Location' ELSE 'Location'
		                                                            END
		OR ltrim(rtrim(LocationAssociation.LocationAssociationType))='Location')
		    ORDER BY CAST(ISNULL(WBLoc.LocationNumber, 9999) AS INT),case when LocationAssociationType='Location' then 2 else 1 end
		) AS LocationAssociationId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CA_BusinessInterruptionOptionId ORDER BY LocationAssociationId) = 1
),
EXP_DC_CA_BusinessInterruption AS (
	SELECT
	LKP_DC_CA_BusinessInterruption.LocationAssociationId,
	LKP_DC_CA_BusinessInterruption.LineType,
	RTR_InsuranceLine_DC_CA_BusinessInterruption.SessionId,
	LKP_DC_CA_BusinessInterruption.LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_DC_CA_BusinessInterruption.CoverageId,
	RTR_InsuranceLine_DC_CA_BusinessInterruption.CoverageType AS i_CoverageType,
	-- *INF*: IIF(i_CoverageType = 'MCCA', 'MCCA Identified',i_CoverageType)
	-- 
	-- 
	-- 
	-- --DECODE(TRUE,
	-- --i_CoverageType!='MCCA',i_CoverageType,
	-- --ISNULL(i_CoverageType),i_CoverageType,
	-- --ISNULL(i_CompositeRating),'MCCA Identified',
	-- --i_CompositeRating='0','MCCA Identified',
	-- --'MCCA Non-Identified')
	IFF(i_CoverageType = 'MCCA', 'MCCA Identified', i_CoverageType) AS CoverageType,
	RTR_InsuranceLine_DC_CA_BusinessInterruption.SubCoverageType,
	LKP_DC_CA_BusinessInterruption.CoverageForm,
	-1 AS RiskID,
	CoverageForm AS RiskType,
	0 AS Exposure,
	LKP_DC_CA_BusinessInterruption.CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	'N/A' AS OccupancyType,
	LKP_DC_CA_BusinessInterruption.CompositeRating AS i_CompositeRating,
	LKP_DC_CA_BusinessInterruption.OptionRating,
	LKP_DC_CA_BusinessInterruption.TotalExposureOptionB
	FROM RTR_InsuranceLine_DC_CA_BusinessInterruption
	LEFT JOIN LKP_DC_CA_BusinessInterruption
	ON LKP_DC_CA_BusinessInterruption.CA_BusinessInterruptionOptionId = RTR_InsuranceLine.ParentCoverageObjectId23
),
LKP_DCCACoverageCollision AS (
	SELECT
	CoverageId,
	ObjectName,
	CollisionType
	FROM (
		select 
		A.CoverageId as CoverageId,
		A.ObjectName as ObjectName,
		A.CollisionType as CollisionType
		from (
		select C.coverageid, C.ObjectName, CollisionType from 
		DCCoverageStaging C -- if object name is DC_CA_Risk then will exist in coverage collision , else parent may
		inner join DCCACoverageCollisionStaging A1 on A1.CoverageId=C.CoverageId and C.ObjectName='DC_CA_Risk'
		and A1.SessionId=C.SessionId
		union
		select C.CoverageId as coverageid, C.objectname , CollisionType
		from
		 DCCoverageStaging C -- if object name is DC_CA_Risk then will exist in coverage collision , else parent may
		inner join DCCACoverageCollisionStaging A2 on A2.CoverageId=C.ObjectId and C.ObjectName='DC_Coverage'
		and A2.SessionId=C.SessionId
		) A
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY CoverageId) = 1
),
LKP_DC_CA_Risk AS (
	SELECT
	LocationAssociationId,
	LineId,
	CoverageForm,
	RiskType,
	VehicleNumber,
	CommissionPercentage,
	GarageType,
	GarageCoverageType,
	FullCoverageGlass,
	CompositeRating,
	CA_RiskId
	FROM (
		SELECT DCLA.LocationAssociationId AS LocationAssociationId,
			CAState.LineId AS LineId,
			ISNULL(WBCALineStaging.CoverageForm, WBCALineStaging.PolicyType) AS CoverageForm,
			CARisk.Type AS RiskType,
			CARisk.VehicleNumber AS VehicleNumber,
			WBLineStaging.FinalCommission AS CommissionPercentage,
			CAGarage.GarageType AS GarageType,
		     CAGarage.coverageType AS GarageCoverageType,
			CARisk.FullCoverageGlass AS FullCoverageGlass,
			DCCALineStaging.CompositeRating AS CompositeRating,
			CARisk.CA_RiskId AS CA_RiskId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCARiskStaging CARisk
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCAStateStaging CAState
			ON CARisk.SessionId = CAState.SessionId
				AND CARisk.CA_StateId = CAState.CA_StateId
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCALineStaging
			ON CAState.SessionId = DCCALineStaging.SessionId
				AND CAState.LineId = DCCALineStaging.LineId
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCALineStaging
			ON DCCALineStaging.SessionId = WBCALineStaging.SessionId
				AND DCCALineStaging.CA_LineId = WBCALineStaging.CA_LineId
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLineStaging
			ON CAState.SessionId = WBLineStaging.SessionId
				AND CAState.LineId = WBLineStaging.LineId
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging DCLA
			ON CARisk.SessionId = DCLA.SessionId
				AND CARisk.CA_LocationId = DCLA.ObjectId
				AND DCLA.ObjectName = 'DC_CA_Location'
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCAGarageStage CAGarage
			ON CARisk.SessionId = CAGarage.SessionId
				AND CARisk.CA_RiskId = CAGarage.CARiskId
		ORDER BY CARisk.CA_RiskId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CA_RiskId ORDER BY LocationAssociationId) = 1
),
EXP_DC_CA_Risk AS (
	SELECT
	LKP_DC_CA_Risk.LocationAssociationId,
	RTR_InsuranceLine_DC_CA_Risk.SessionId,
	LKP_DC_CA_Risk.LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_DC_CA_Risk.CoverageId,
	LKP_DC_CA_Risk.CoverageForm,
	LKP_DC_CA_Risk.CA_RiskId AS RiskId,
	LKP_DC_CA_Risk.RiskType AS i_RiskType,
	LKP_DCCACoverageCollision.CollisionType AS lkp_CollisionType,
	RTR_InsuranceLine_DC_CA_Risk.CoverageType AS i_CoverageType,
	-- *INF*: DECODE(TRUE,
	-- i_CoverageType = 'MCCA' AND i_RiskType='Garage', 'MCCA Non-Identified',
	-- i_CoverageType = 'MCCA' AND i_RiskType !='Garage', 'MCCA Identified',
	-- i_CoverageType =  'Collision' AND in(lkp_CollisionType,'BroadColl','LmtdColl'),i_CoverageType||lkp_CollisionType,
	-- i_CoverageType
	-- )
	-- 
	-- 
	-- --IIF(i_CoverageType = 'MCCA', 'MCCA Identified',i_CoverageType)
	-- 
	-- 
	-- 
	-- --DECODE(TRUE,
	-- --i_CoverageType!='MCCA',i_CoverageType,
	-- --ISNULL(i_CoverageType),i_CoverageType,
	-- --ISNULL(i_CompositeRating),'MCCA Identified',
	-- --i_CompositeRating='0','MCCA Identified',
	-- --'MCCA Non-Identified')
	DECODE(
	    TRUE,
	    i_CoverageType = 'MCCA' AND i_RiskType = 'Garage', 'MCCA Non-Identified',
	    i_CoverageType = 'MCCA' AND i_RiskType != 'Garage', 'MCCA Identified',
	    i_CoverageType = 'Collision' AND lkp_CollisionType IN ('BroadColl','LmtdColl'), i_CoverageType || lkp_CollisionType,
	    i_CoverageType
	) AS CoverageType,
	LKP_DC_CA_Risk.GarageType AS i_GarageType,
	-- *INF*: IIF(ISNULL(i_GarageType), 'N/A',i_GarageType)
	IFF(i_GarageType IS NULL, 'N/A', i_GarageType) AS v_GarageType,
	LKP_DC_CA_Risk.GarageCoverageType AS i_GarageCoverageType,
	-- *INF*: IIF(ISNULL(i_GarageCoverageType),'N/A',i_GarageCoverageType)
	IFF(i_GarageCoverageType IS NULL, 'N/A', i_GarageCoverageType) AS v_GarageCoverageType,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(i_GarageType), CoverageForm  || i_RiskType  || v_GarageType  || v_GarageCoverageType,
	-- CoverageForm  || i_RiskType)
	-- 
	-- 
	-- ---- As part of PROD - 11525 and PROD-14500, added the above code.
	-- 
	-- --DECODE(TRUE,
	-- --ISNULL(CoverageForm),i_RiskType,
	-- --i_RiskType=CoverageForm,i_RiskType,
	-- --ISNULL(i_GarageType),CoverageForm||i_RiskType,
	-- --CoverageForm||i_RiskType||i_GarageType)
	DECODE(
	    TRUE,
	    i_GarageType IS NOT NULL, CoverageForm || i_RiskType || v_GarageType || v_GarageCoverageType,
	    CoverageForm || i_RiskType
	) AS RiskType,
	0 AS Exposure,
	LKP_DC_CA_Risk.CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	-- *INF*: IIF(ISNULL(:LKP.LKP_DCCFOCCUPANCYSTAGING(SessionId)),'N/A',:LKP.LKP_DCCFOCCUPANCYSTAGING(SessionId))
	IFF(
	    LKP_DCCFOCCUPANCYSTAGING_SessionId.OccupancyType IS NULL, 'N/A',
	    LKP_DCCFOCCUPANCYSTAGING_SessionId.OccupancyType
	) AS OccupancyType,
	LKP_DC_CA_Risk.VehicleNumber,
	LKP_DC_CA_Risk.FullCoverageGlass,
	LKP_DC_CA_Risk.CompositeRating AS i_CompositeRating
	FROM RTR_InsuranceLine_DC_CA_Risk
	LEFT JOIN LKP_DCCACoverageCollision
	ON LKP_DCCACoverageCollision.CoverageId = RTR_InsuranceLine.CoverageId16
	LEFT JOIN LKP_DC_CA_Risk
	ON LKP_DC_CA_Risk.CA_RiskId = RTR_InsuranceLine.ParentCoverageObjectId16
	LEFT JOIN LKP_DCCFOCCUPANCYSTAGING LKP_DCCFOCCUPANCYSTAGING_SessionId
	ON LKP_DCCFOCCUPANCYSTAGING_SessionId.SessionId = SessionId

),
LKP_DC_CA_State AS (
	SELECT
	LocationAssociationId,
	LineId,
	CoverageForm,
	CommissionPercentage,
	VehicleNumber,
	CA_StateId
	FROM (
		select A.LocationAssociationId as LocationAssociationId,   CAState.LineId AS LineId,
		       ISNULL(WBCALine.CoverageForm, WBCALine.PolicyType) AS CoverageForm,
		       WBLine.FinalCommission AS CommissionPercentage,
		       NULL As VehicleNumber,
		CAState.CA_StateId AS CA_StateId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCAStateStaging CAState
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCALineStaging CALine
		       ON CAState.LineId = CALine.LineId
		              AND CAState.SessionId = CALine.SessionId
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCALineStaging WBCALine
		       ON CALine.CA_LineId = WBCALine.CA_LineId
		              AND CALine.SessionId = WBCALine.SessionId
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLineStaging WBLine
		       ON WBLine.LineId = CALine.LineId
		              AND WBLine.SessionId = CALine.SessionId
		left hash join 
		(select SessionId,StateProv,LocationAssociationId from (
		SELECT B.SessionId,Q.StateProv,B.LocationAssociationId,Rank() over(partition by B.SessionId,Q.StateProv order by isnull(cast(c.LocationNumber as int), 9999),B.LocationAssociationId) Record_Rank
		              FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging B
		              INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCALocationStaging A
		                     ON B.SessionId = A.SessionId
		                           AND B.ObjectId = A.CA_LocationId
		              INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging Q
		                     ON Q.LocationId = B.LocationId
		                           AND Q.SessionId = B.SessionId
		              INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationStaging C
		                     ON b.LocationId = C.LocationId
		                           AND b.SessionId = C.SessionId
		              --WHERE B.SessionId = CAState.SessionId
		              --     AND Q.StateProv = CAState.LineCoverageState
		                     where ISNULL(Q.Deleted,0)<>1
		) A
		where A.Record_Rank=1) A
		on A.SessionId = CAState.SessionId
		AND A.StateProv = CAState.LineCoverageState
		ORDER BY CAState.CA_StateId --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CA_StateId ORDER BY LocationAssociationId) = 1
),
EXP_DC_CA_State AS (
	SELECT
	LKP_DC_CA_State.LocationAssociationId,
	RTR_InsuranceLine_DC_CA_State.SessionId,
	LKP_DC_CA_State.LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_DC_CA_State.CoverageId,
	LKP_DC_CA_State.CoverageForm,
	RTR_InsuranceLine_DC_CA_State.CoverageType,
	-1 AS RiskId,
	CoverageForm AS RiskType,
	0 AS Exposure,
	LKP_DC_CA_State.CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	-- *INF*: IIF(ISNULL(:LKP.LKP_DCCFOCCUPANCYSTAGING(SessionId)),'N/A',:LKP.LKP_DCCFOCCUPANCYSTAGING(SessionId))
	IFF(
	    LKP_DCCFOCCUPANCYSTAGING_SessionId.OccupancyType IS NULL, 'N/A',
	    LKP_DCCFOCCUPANCYSTAGING_SessionId.OccupancyType
	) AS OccupancyType,
	LKP_DC_CA_State.VehicleNumber
	FROM RTR_InsuranceLine_DC_CA_State
	LEFT JOIN LKP_DC_CA_State
	ON LKP_DC_CA_State.CA_StateId = RTR_InsuranceLine.ParentCoverageObjectId15
	LEFT JOIN LKP_DCCFOCCUPANCYSTAGING LKP_DCCFOCCUPANCYSTAGING_SessionId
	ON LKP_DCCFOCCUPANCYSTAGING_SessionId.SessionId = SessionId

),
LKP_DC_CF_Risk AS (
	SELECT
	LocationAssociationId,
	LineId,
	CoverageForm,
	RiskType,
	CoverageType_WTE,
	SpecialClassLevel1,
	OccupancyType,
	BuildingNumber,
	PolicyCoverage,
	CauseOfLoss,
	AttachedSign,
	ConstructionCode,
	MultipleLocationCreditFactor,
	OrginalPackageModifier,
	PreferredPropertyCreditFactor,
	ProtectionClass,
	YearBuilt,
	BuildersRiskCoverageType,
	IRPM,
	WindCoverageFlag,
	OccupancyClassDescription,
	ActiveBuildingFlag,
	RateType,
	PropertyType,
	OccupancyCategory,
	CF_RiskId
	FROM (
		SELECT 
		  LA.LocationAssociationId AS LocationAssociationId,
		  CFB.LineId AS LineId,
		  WBCFLine.PolicyCoverage AS CoverageForm,
		  CFRisk.RiskType AS RiskType,
		  WBCFTE.CoverageType AS CoverageType_WTE,
		  CFBR.SpecialClassLevel1 AS SpecialClassLevel1,
		  CFO.OccupancyType AS OccupancyType,
		  WCFB.BuildingNumber AS BuildingNumber,
		  WBCFLine.PolicyCoverage AS PolicyCoverage,
		  CFP.CauseOfLoss AS CauseOfLoss,
		  WBCFP.AttachedSignSelect AS AttachedSign,
		  CFB.ConstructionCode AS ConstructionCode,
		  WBCFLocationProperty.MultipleLocationCreditFactor AS MultipleLocationCreditFactor,
		  WBCFRisk.OriginalPackageModifier AS OrginalPackageModifier,
		  WBCFLocationAccountStage.PreferredPropertyCreditFactor AS PreferredPropertyCreditFactor,
		  WBCFLocationProperty.ProtectionClassOverride as ProtectionClass,
		  CFB.YearBuilt AS YearBuilt,
		  CFRisk.RiskType + CASE 
		    WHEN BLDR.TheftOfBuildingMaterials = 1
		       THEN 'TheftOfBuildingMaterials'
			      ELSE ''
				END + CASE 
				WHEN BLDR.Renovations = 1
					THEN 'Renovations'
				ELSE ''
				END AS BuildersRiskCoverageType,
		
			DCM.Value AS IRPM,
		
			CASE CFB.WindHailExcludeSelect
				WHEN 0
					THEN '1'
				WHEN 1
					THEN '0'
				ELSE NULL
				END AS WindCoverageFlag,
		
			CFO.Description AS OccupancyClassDescription,
		
			CASE ISNULL(CFB.Deleted, 0)
				WHEN 0
					THEN '1'
				ELSE '0'
				END AS ActiveBuildingFlag,
		
			(
				CASE WBCFB.SpecificRatedStoredValue
					WHEN 1
						THEN 'S'
					WHEN 0
						THEN 'C'
					ELSE 'N/A'
					END
				) AS RateType,
		
			CFPP.PropertyType AS PropertyType,
			WBCFP.OccupancyCategory AS OccupancyCategory,
			CFRisk.CF_RiskId AS CF_RiskId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCFRiskStaging CFRisk
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCFOccupancyStaging CFO
			ON CFO.SessionId = CFRisk.SessionId
				AND CFO.CF_RiskId = CFRisk.CF_RiskId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCFBuildingStage CFB
			ON CFB.SessionId = CFRisk.SessionId
				AND CFB.CFBuildingId = CFRisk.CF_BuildingId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCFLineStaging CFLine
			ON CFLine.SessionId = CFB.SessionId
				AND CFLine.LineId = CFB.LineId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCFBuildingRiskStaging CFBR
			ON CFBR.SessionId = CFRisk.SessionId
				AND CFBR.CF_RiskId = CFRisk.CF_RiskId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCFPropertyStaging CFP
			ON CFP.SessionId = CFRisk.SessionId
				AND CFP.CF_RiskId = CFRisk.CF_RiskId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging LA
			ON LA.SessionId = CFB.SessionId
				AND LA.ObjectId = CFB.CFLocationId
				AND LA.ObjectName = 'DC_CF_Location'
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCFBuildingStage WCFB
			ON WCFB.SessionId = CFB.SessionId
				AND WCFB.CFBuildingId = CFB.CFBuildingId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WbCfLineStage WBCFLine
			ON CFLine.SessionId = WBCFLine.SessionId
				AND CFLine.CF_LineId = WBCFLine.CFLineId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCFLocationPropertyStaging CFLP
			ON CFLP.SessionId = CFB.SessionId
				AND CFLP.CF_LocationId = CFB.CFLocationId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCModifierStaging DCM
			ON DCM.SessionId = CFLP.SessionId
				AND DCM.ObjectId = CFLP.CF_LocationPropertyId
				AND DCM.ObjectName = 'DC_CF_LocationProperty'
				AND DCM.Type = 'LocationIRPMFactor'
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCFLocationPropertyStage WBCFLocationProperty
			ON WBCFLocationProperty.SessionId = CFLP.SessionId
				AND WBCFLocationProperty.CFLocationPropertyId = CFLP.CF_LocationPropertyId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationAccountStage
			ON WBLocationAccountStage.SessionId = LA.SessionId
				AND WBLocationAccountStage.LocationId = LA.LocationId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCLLocationAccountStage
			ON WBCLLocationAccountStage.SessionId = WBLocationAccountStage.SessionId
				AND WBCLLocationAccountStage.WBLocationAccountId = WBLocationAccountStage.WBLocationAccountId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WbCfLocationAccountStage
			ON WbCfLocationAccountStage.SessionId = WBCLLocationAccountStage.SessionId
				AND WbCfLocationAccountStage.WBCLLocationAccountId = WBCLLocationAccountStage.WBCLLocationAccountId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCFRiskStage WBCFRisk
			ON WBCFRisk.SessionId = CFRisk.SessionId
				AND WBCFRisk.CF_RiskId = CFRisk.CF_RiskId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCFBuilderStaging BLDR
			ON BLDR.SessionId = CFRisk.SessionId
				AND BLDR.CF_RiskId = CFRisk.CF_RiskId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCFPropertyStage WBCFP
			ON WBCFP.SessionId = CFP.SessionId
				AND WBCFP.CF_PropertyId = CFP.CF_PropertyId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCFTimeElementStaging DCCFTE
			ON DCCFTE.SessionId = CFRisk.SessionId
				AND DCCFTE.CF_RiskId = CFRisk.CF_RiskId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCFTimeElementStage WBCFTE
			ON WBCFTE.SessionId = DCCFTE.SessionId
				AND WBCFTE.CF_TimeElementId = DCCFTE.CF_TimeElementId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCFBuildingStage WBCFB
			ON WBCFB.SessionId = CFB.SessionId
				AND WBCFB.CFBuildingId = CFB.CFBuildingId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCFPersonalPropertyStaging CFPP
			ON CFPP.SessionId = CFRisk.SessionId
				AND CFPP.CF_RiskId = CFRisk.CF_RiskId
		ORDER BY CFRisk.CF_RiskId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CF_RiskId ORDER BY LocationAssociationId) = 1
),
EXP_DC_CF_Risk AS (
	SELECT
	LKP_DC_CF_Risk.LocationAssociationId,
	RTR_InsuranceLine_DC_CF_Risk.SessionId,
	LKP_DC_CF_Risk.LineId,
	LKP_DC_CF_Risk.CF_RiskId AS RiskId,
	RTR_InsuranceLine_DC_CF_Risk.CoverageId,
	RTR_InsuranceLine_DC_CF_Risk.CoverageType AS i_CoverageType,
	RTR_InsuranceLine_DC_CF_Risk.SubCoverageType AS i_SubCoverageType,
	LKP_DC_CF_Risk.CoverageForm,
	LKP_DC_CF_Risk.RiskType,
	LKP_DC_CF_Risk.CoverageType_WTE,
	0 AS Exposure,
	-1 AS CommissionPercentage,
	'N/A' AS CoverageVersion,
	'' AS ILFTableAssignmentCode,
	LKP_DC_CF_Risk.SpecialClassLevel1,
	-- *INF*: IIF(RiskType='SPECIAL',SpecialClassLevel1,'N/A')
	IFF(RiskType = 'SPECIAL', SpecialClassLevel1, 'N/A') AS o_SpecialClassLevel1,
	LKP_DC_CF_Risk.OccupancyType AS i_OccupancyType,
	-- *INF*: IIF(NOT ISNULL(OrginalPackageModifier),i_OccupancyType,'N/A')
	IFF(OrginalPackageModifier IS NOT NULL, i_OccupancyType, 'N/A') AS OccupancyType,
	LKP_DC_CF_Risk.BuildingNumber,
	LKP_DC_CF_Risk.PolicyCoverage,
	LKP_DC_CF_Risk.CauseOfLoss,
	LKP_DC_CF_Risk.AttachedSign,
	LKP_DC_CF_Risk.ConstructionCode,
	LKP_DC_CF_Risk.MultipleLocationCreditFactor,
	LKP_DC_CF_Risk.PreferredPropertyCreditFactor,
	LKP_DC_CF_Risk.OrginalPackageModifier,
	LKP_DC_CF_Risk.ProtectionClass,
	LKP_DC_CF_Risk.YearBuilt,
	LKP_DC_CF_Risk.BuildersRiskCoverageType,
	-- *INF*: DECODE(TRUE,
	-- -- reorganize by alphabetical RiskType so it's less confusing. 1 means it will be excluded, this is a blacklist
	-- -- I removed the terrorism CoverageTypes from the RiskType rules because they redundantly exist already in the long
	-- -- list of CoverageType rules
	-- RiskType='ALS' AND IN(
	-- 	i_CoverageType,
	-- 	'BusinessIncomeLandlordAsAdditionalInsured',
	-- 	'EarthquakeRisk')=1,1,
	-- RiskType='BIEE' AND i_CoverageType='BusinessIncomeLandlordAsAdditionalInsured',1,
	-- RiskType='BLDG' AND IN(
	-- 	i_CoverageType,
	-- 	'EarthquakeRisk',
	-- 	'MineSubsidence',
	-- 	'PierOrWharf')=1,1,
	-- RiskType='BLDRK' AND i_CoverageType='EarthquakeRisk',1,
	-- RiskType='Personal Property' AND i_CoverageType='EarthquakeRisk',1,
	-- RiskType='PO' AND IN (
	-- 	i_CoverageType,
	-- 	'BuildersRisk',
	-- 	'EarthquakeRisk',
	-- 	'LegalLiability',
	-- 'ReportingForm')=1,1,
	-- RiskType='PP' AND IN(
	-- 	i_CoverageType,
	-- 	'EarthquakeRisk',
	-- 	'LossAssessment',
	-- 	'MiscellaneousRealProperty',
	-- 	'PierOrWharf')=1, 1,
	-- RiskType='TENANTS' AND IN(
	-- 	i_CoverageType,
	-- 	'BuildersRisk',
	-- 	'FireDepartmentServiceChargeCoverage',
	-- 	'EarthquakeRisk')=1,1,
	-- RiskType='TIME' AND i_CoverageType='PierOrWharf',1,
	-- 
	-- -- these are NoPeril across all RiskTypes. Keep this alphabatized
	-- IN(i_CoverageType,
	-- 'AlcoholicBeveragesTaxExclusion',
	-- 'AgreedValue',
	-- 'BrandsAndLabels',
	-- 'BusinessIncomeDiscretionaryPayroll',
	-- 'BusinessIncomeEducationalInstitutions',
	-- 'BusinessIncomeElectronicMedia',
	-- 'BusinessIncomeExtendedPeriodOfIndemnity',
	-- 'BusinessIncomeFoodContamination',
	-- 'BusinessIncomeManufacturingAndMining',
	-- 'BusinessIncomeMaxPeriodOfIndemnity',
	-- 'BusinessIncomeMercantileAndNonManufacturing',
	-- 'BusinessIncomeMiningProperties',
	-- 'BusinessIncomeMonthlyLimitOfIndemnity',
	-- 'BusinessIncomeOrdinaryPayroll',
	-- 'BusinessIncomePowerHeatRefrig',
	-- 'BusinessIncomeRentalProperties',
	-- 'BusinessIncomeSeasonalLease',
	-- 'CovGolfCourse',
	-- 'CovStationDamage',
	-- 'DischargeFromSewer',
	-- 'ExtraExpenseExpandedLimit',
	-- 'FoodContamination',
	-- 'FunctionalValuation',
	-- 'IncreaseInBuildingExpenses',
	-- 'ManufacturersConsequentialLossAssumption',
	-- 'MedicalEquipment',
	-- 'MineSubsidence',
	-- 'PollutantCleanupRisk',
	-- 'RadioTelevisionAntennasRisk',
	-- 'SprinklerLeakageEarthquakeExtension',
	-- 'TerrorismFireOnly',
	-- 'TerrorismRisk',
	-- 'UtilityServicesDirectDamage',
	-- 'UtilityServicesTimeElement',
	-- 'ReportingForm')=1,1,
	-- 0)
	-- 
	-- -- original code for reference
	-- --IN(RiskType,'BIEE','ALS')=1 AND i_CoverageType='BusinessIncomeLandlordAsAdditionalInsured', 1,
	-- --IN(RiskType, 'BIEE', 'EXTRA', 'TIME')=1 AND IN(i_CoverageType,'TerrorismRisk','TerrorismFireOnly')=1, 1,
	-- --RiskType='BLDG' AND IN(i_CoverageType,
	-- --'EarthquakeRisk',
	-- --'MineSubsidence')=1, 1,
	-- --RiskType='PP' AND IN(i_CoverageType,'LossAssessment','MiscellaneousRealProperty','EarthquakeRisk')=1, 1,
	-- --RiskType='BLDRK' AND i_CoverageType='EarthquakeRisk', 1,
	-- --IN(RiskType, 'BLDG', 'PP', 'TIME')=1 AND i_CoverageType='PierOrWharf',1,
	-- 
	-- 
	-- 
	DECODE(
	    TRUE,
	    RiskType = 'ALS' AND i_CoverageType IN ('BusinessIncomeLandlordAsAdditionalInsured','EarthquakeRisk') = 1, 1,
	    RiskType = 'BIEE' AND i_CoverageType = 'BusinessIncomeLandlordAsAdditionalInsured', 1,
	    RiskType = 'BLDG' AND i_CoverageType IN ('EarthquakeRisk','MineSubsidence','PierOrWharf') = 1, 1,
	    RiskType = 'BLDRK' AND i_CoverageType = 'EarthquakeRisk', 1,
	    RiskType = 'Personal Property' AND i_CoverageType = 'EarthquakeRisk', 1,
	    RiskType = 'PO' AND i_CoverageType IN ('BuildersRisk','EarthquakeRisk','LegalLiability','ReportingForm') = 1, 1,
	    RiskType = 'PP' AND i_CoverageType IN ('EarthquakeRisk','LossAssessment','MiscellaneousRealProperty','PierOrWharf') = 1, 1,
	    RiskType = 'TENANTS' AND i_CoverageType IN ('BuildersRisk','FireDepartmentServiceChargeCoverage','EarthquakeRisk') = 1, 1,
	    RiskType = 'TIME' AND i_CoverageType = 'PierOrWharf', 1,
	    i_CoverageType IN ('AlcoholicBeveragesTaxExclusion','AgreedValue','BrandsAndLabels','BusinessIncomeDiscretionaryPayroll','BusinessIncomeEducationalInstitutions','BusinessIncomeElectronicMedia','BusinessIncomeExtendedPeriodOfIndemnity','BusinessIncomeFoodContamination','BusinessIncomeManufacturingAndMining','BusinessIncomeMaxPeriodOfIndemnity','BusinessIncomeMercantileAndNonManufacturing','BusinessIncomeMiningProperties','BusinessIncomeMonthlyLimitOfIndemnity','BusinessIncomeOrdinaryPayroll','BusinessIncomePowerHeatRefrig','BusinessIncomeRentalProperties','BusinessIncomeSeasonalLease','CovGolfCourse','CovStationDamage','DischargeFromSewer','ExtraExpenseExpandedLimit','FoodContamination','FunctionalValuation','IncreaseInBuildingExpenses','ManufacturersConsequentialLossAssumption','MedicalEquipment','MineSubsidence','PollutantCleanupRisk','RadioTelevisionAntennasRisk','SprinklerLeakageEarthquakeExtension','TerrorismFireOnly','TerrorismRisk','UtilityServicesDirectDamage','UtilityServicesTimeElement','ReportingForm') = 1, 1,
	    0
	) AS v_NoPerilGroupFlag,
	-- *INF*: IIF(IN(i_CoverageType,'DebrisRemovalRisk','OrdinanceOrLaw','TreesShrubsPlants','BusinessIncomeDependentProperties','ExtraExpenseDependentProperties','VacancyRisk','PeakSeason','PeakSeason/PeakSeasonScheduled','InflationGuard','ExtendedReplacementCost','GuaranteedReplacementCost','ReplacementCost')=1,1,0)
	IFF(
	    i_CoverageType IN ('DebrisRemovalRisk','OrdinanceOrLaw','TreesShrubsPlants','BusinessIncomeDependentProperties','ExtraExpenseDependentProperties','VacancyRisk','PeakSeason','PeakSeason/PeakSeasonScheduled','InflationGuard','ExtendedReplacementCost','GuaranteedReplacementCost','ReplacementCost') = 1,
	    1,
	    0
	) AS v_TreesPlantsShrubsFlag,
	-- *INF*: IIF(IN(RiskType,'BLDG','BIEE','BLDRK','EXTRA','PP','SPECIAL','TIME')=1 AND v_TreesPlantsShrubsFlag!=1, 1, 0)
	IFF(
	    RiskType IN ('BLDG','BIEE','BLDRK','EXTRA','PP','SPECIAL','TIME') = 1
	    and v_TreesPlantsShrubsFlag != 1,
	    1,
	    0
	) AS v_NoRuleYetFlag,
	-- *INF*: IIF(IN(i_CoverageType, 'BG1', 'BG2', 'OtherPerils')=1,1,0)
	IFF(i_CoverageType IN ('BG1','BG2','OtherPerils') = 1, 1, 0) AS v_PPSPECIALFlag,
	-- *INF*: DECODE(TRUE,
	-- INSTR(i_SubCoverageType,'BG1')>0,'BG1',
	-- INSTR(i_SubCoverageType,'BG2')>0,'BG2',
	-- INSTR(i_SubCoverageType,'OtherPerils')>0,'OtherPerils',
	-- 'N/A')
	-- 
	-- -- for 10x restrict valid values to BG1, BG2, OtherPerils
	DECODE(
	    TRUE,
	    REGEXP_INSTR(i_SubCoverageType, 'BG1') > 0, 'BG1',
	    REGEXP_INSTR(i_SubCoverageType, 'BG2') > 0, 'BG2',
	    REGEXP_INSTR(i_SubCoverageType, 'OtherPerils') > 0, 'OtherPerils',
	    'N/A'
	) AS v_SubCoverageType,
	-- *INF*: DECODE(TRUE,
	-- IN(i_CoverageType,'BusinessIncomeDependentProperties',
	-- 'DebrisRemovalRisk',
	-- 'ExtraExpenseDependentProperties',
	-- 'OrdinanceOrLaw',
	-- 'TreesShrubsPlants',
	-- 'VacancyRisk',
	-- 'LeaseholdInterest')=1,1,
	-- 0)
	DECODE(
	    TRUE,
	    i_CoverageType IN ('BusinessIncomeDependentProperties','DebrisRemovalRisk','ExtraExpenseDependentProperties','OrdinanceOrLaw','TreesShrubsPlants','VacancyRisk','LeaseholdInterest') = 1, 1,
	    0
	) AS v_IXUP_Brkn_dwn_Coverages,
	-- *INF*: DECODE(TRUE,
	-- v_IXUP_Brkn_dwn_Coverages=1, i_CoverageType,
	-- IN(RiskType,'BIEE','TIME') and v_PPSPECIALFlag=1,RiskType||CoverageType_WTE,
	-- RiskType='SPECIAL' AND i_CoverageType='InflationGuard',:LKP.LKP_SUPSPECIALCLASSGROUP(SpecialClassLevel1)||i_CoverageType,
	-- v_NoPerilGroupFlag=1, i_CoverageType,
	-- i_CoverageType='Spoilage',i_CoverageType||IIF(NOT ISNULL(:LKP.LKP_DCCFCOVERAGESPOILAGESTAGING(CoverageId)),:LKP.LKP_DCCFCOVERAGESPOILAGESTAGING(CoverageId),''),
	-- RiskType='SPECIAL' AND v_PPSPECIALFlag=1,:LKP.LKP_SUPSPECIALCLASSGROUP(SpecialClassLevel1),
	-- RiskType='PP' AND v_PPSPECIALFlag=1,:LKP.LKP_DCCFPERSONALPROPERTYSTAGING(RiskId),
	-- IN(RiskType, 'SPECIAL', 'PP')=1 AND v_PPSPECIALFlag=0, i_CoverageType,
	-- v_TreesPlantsShrubsFlag=1, i_CoverageType,
	-- RiskType='BLDRK',BuildersRiskCoverageType,
	-- v_PPSPECIALFlag=1,RiskType,
	-- v_NoRuleYetFlag=1,i_CoverageType,
	-- RiskType)
	DECODE(
	    TRUE,
	    v_IXUP_Brkn_dwn_Coverages = 1, i_CoverageType,
	    RiskType IN ('BIEE','TIME') and v_PPSPECIALFlag = 1, RiskType || CoverageType_WTE,
	    RiskType = 'SPECIAL' AND i_CoverageType = 'InflationGuard', LKP_SUPSPECIALCLASSGROUP_SpecialClassLevel1.StandardSpecialClassGroupShortDescription || i_CoverageType,
	    v_NoPerilGroupFlag = 1, i_CoverageType,
	    i_CoverageType = 'Spoilage', i_CoverageType || 
	    IFF(
	        LKP_DCCFCOVERAGESPOILAGESTAGING_CoverageId.SpoilageCoverageType IS NOT NULL,
	        LKP_DCCFCOVERAGESPOILAGESTAGING_CoverageId.SpoilageCoverageType,
	        ''
	    ),
	    RiskType = 'SPECIAL' AND v_PPSPECIALFlag = 1, LKP_SUPSPECIALCLASSGROUP_SpecialClassLevel1.StandardSpecialClassGroupShortDescription,
	    RiskType = 'PP' AND v_PPSPECIALFlag = 1, LKP_DCCFPERSONALPROPERTYSTAGING_RiskId.PropertyType,
	    RiskType IN ('SPECIAL','PP') = 1 AND v_PPSPECIALFlag = 0, i_CoverageType,
	    v_TreesPlantsShrubsFlag = 1, i_CoverageType,
	    RiskType = 'BLDRK', BuildersRiskCoverageType,
	    v_PPSPECIALFlag = 1, RiskType,
	    v_NoRuleYetFlag = 1, i_CoverageType,
	    RiskType
	) AS CoverageType,
	-- *INF*: DECODE(TRUE,
	-- v_IXUP_Brkn_dwn_Coverages=1,v_SubCoverageType,
	-- v_NoPerilGroupFlag=1,'',
	-- i_CoverageType = 'Spoilage', '',
	-- v_TreesPlantsShrubsFlag=1, 'BG1',
	-- RiskType = 'BLDRK' AND i_CoverageType='BuildersRisk', 'BG1',
	-- v_PPSPECIALFlag=1,i_CoverageType,
	-- v_NoRuleYetFlag=1,'',
	-- i_CoverageType)
	DECODE(
	    TRUE,
	    v_IXUP_Brkn_dwn_Coverages = 1, v_SubCoverageType,
	    v_NoPerilGroupFlag = 1, '',
	    i_CoverageType = 'Spoilage', '',
	    v_TreesPlantsShrubsFlag = 1, 'BG1',
	    RiskType = 'BLDRK' AND i_CoverageType = 'BuildersRisk', 'BG1',
	    v_PPSPECIALFlag = 1, i_CoverageType,
	    v_NoRuleYetFlag = 1, '',
	    i_CoverageType
	) AS PerilGroup,
	LKP_DC_CF_Risk.IRPM AS IndividualRiskPremiumModification,
	LKP_DC_CF_Risk.WindCoverageFlag AS i_WindCoverageFlag,
	-- *INF*: IIF(ISNULL(i_WindCoverageFlag), '1', i_WindCoverageFlag)
	IFF(i_WindCoverageFlag IS NULL, '1', i_WindCoverageFlag) AS WindCoverageIndicator,
	LKP_DC_CF_Risk.OccupancyClassDescription,
	LKP_DC_CF_Risk.ActiveBuildingFlag,
	LKP_DC_CF_Risk.RateType,
	LKP_DC_CF_Risk.PropertyType,
	LKP_DC_CF_Risk.OccupancyCategory
	FROM RTR_InsuranceLine_DC_CF_Risk
	LEFT JOIN LKP_DC_CF_Risk
	ON LKP_DC_CF_Risk.CF_RiskId = RTR_InsuranceLine.ParentCoverageObjectId14
	LEFT JOIN LKP_SUPSPECIALCLASSGROUP LKP_SUPSPECIALCLASSGROUP_SpecialClassLevel1
	ON LKP_SUPSPECIALCLASSGROUP_SpecialClassLevel1.SpecialClassGroupCode = SpecialClassLevel1

	LEFT JOIN LKP_DCCFCOVERAGESPOILAGESTAGING LKP_DCCFCOVERAGESPOILAGESTAGING_CoverageId
	ON LKP_DCCFCOVERAGESPOILAGESTAGING_CoverageId.CoverageId = CoverageId

	LEFT JOIN LKP_DCCFPERSONALPROPERTYSTAGING LKP_DCCFPERSONALPROPERTYSTAGING_RiskId
	ON LKP_DCCFPERSONALPROPERTYSTAGING_RiskId.CF_RiskId = RiskId

),
LKP_DC_CR_Endorsement AS (
	SELECT
	LocationAssociationId,
	LineId,
	CoverageForm,
	RiskType,
	RiskId,
	CREndorsementId
	FROM (
		SELECT 
		B.LocationAssociationId as LocationAssociationId,
			-- There can be multiple locations for a given risk and there is no direct linkage between the coverage and location, so the 
			-- correlated sub-query below grabs the location with the the lowest location number (usually location 1 in most instances).
			CRRisk.LineId AS LineId,
			CRLine.PolicyType AS CoverageForm,
			CASE WHEN charindex('_',CRRiskCrime.Type)>1 THEN substring(CRRiskCrime.Type,1,charindex('_',CRRiskCrime.Type)-1)
		ELSE CRRiskCrime.Type END as RiskType,
			CRRisk.CRRiskId AS RiskId,
			CREndorsement.CREndorsementId AS CREndorsementId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCREndorsementStage CREndorsement
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DcCrRiskStage CRRisk
			ON CREndorsement.SessionId = CRRisk.SessionId
				AND CREndorsement.CRRiskId = CRRisk.CRRiskId
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DcCrRiskCrimeStage CRRiskCrime
			ON CREndorsement.CRRiskId=CRRiskCrime.CrRiskId
				AND CREndorsement.Sessionid=CRRiskCrime.Sessionid
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCRRiskStage WBCRRisk
			ON CRRisk.SessionId = WBCRRisk.SessionId
				AND CRRisk.CRRiskId = WBCRRisk.CRRiskId
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCRLineStage CRLine
			ON CRLine.SessionId = CRRisk.SessionId
				AND CRLine.LineId = CRRisk.LineId
		LEFT HASH JOIN
		(SELECT * FROM 
		(
				SELECT la.SessionId , LocationAssociationId , 
				Rank() over(partition by wbLoc.SessionId ORDER BY IsNull(cast(wbLoc.LocationNumber as int), 9999)) Record_Rank
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging la
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging DCLoc
					ON DCLoc.SessionId = la.SessionId
						AND DCLoc.LocationId = la.LocationId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationStaging wbLoc
					ON wbLoc.SessionId = la.SessionId
						AND wbLoc.LocationId = dcloc.LocationId
				-- The inner join below ensures that only records associated to DC_CR_Location are considered
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCRLocationStage DCCRLoc
					ON DCCRLoc.SessionId = la.SessionId
						AND DCCRLoc.CR_LocationId = la.ObjectId
				--WHERE la.SessionID = CRRisk.SessionId
				and ISNULL(DCLoc.Deleted,0)<>1
						)A WHERE A.Record_Rank =1)B
		ON B.SessionID = CRRisk.SessionId
		ORDER BY CREndorsement.CREndorsementId --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CREndorsementId ORDER BY LocationAssociationId) = 1
),
EXP_DC_CR_Endorsement AS (
	SELECT
	LKP_DC_CR_Endorsement.LocationAssociationId,
	RTR_InsuranceLine_DC_CR_Endorsement.SessionId,
	LKP_DC_CR_Endorsement.LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_DC_CR_Endorsement.CoverageId,
	RTR_InsuranceLine_DC_CR_Endorsement.CoverageType,
	LKP_DC_CR_Endorsement.CoverageForm,
	LKP_DC_CR_Endorsement.RiskId,
	LKP_DC_CR_Endorsement.RiskType,
	0 AS Exposure,
	-1 AS CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	-- *INF*: IIF(ISNULL(:LKP.LKP_DCCROccupancyStaging(SessionId)),'N/A',:LKP.LKP_DCCROccupancyStaging(SessionId))
	-- --5/9/2014 Anisha
	-- --'N/A'
	IFF(
	    LKP_DCCROCCUPANCYSTAGING_SessionId.OccupancyTypeMonoline IS NULL, 'N/A',
	    LKP_DCCROCCUPANCYSTAGING_SessionId.OccupancyTypeMonoline
	) AS OccupancyType
	FROM RTR_InsuranceLine_DC_CR_Endorsement
	LEFT JOIN LKP_DC_CR_Endorsement
	ON LKP_DC_CR_Endorsement.CREndorsementId = RTR_InsuranceLine.ParentCoverageObjectId13
	LEFT JOIN LKP_DCCROCCUPANCYSTAGING LKP_DCCROCCUPANCYSTAGING_SessionId
	ON LKP_DCCROCCUPANCYSTAGING_SessionId.SessionId = SessionId

),
LKP_DC_CR_Risk AS (
	SELECT
	LocationAssociationId,
	LineId,
	CoverageForm,
	RiskType,
	RiskId,
	in_ParentCoverageObjectId
	FROM (
		SELECT
		-- There can be multiple locations for a given risk and there is no direct linkage between the coverage and location, so the 
		-- correlated sub-query below grabs the location with the the lowest location number (usually location 1 in most instances).
			 LocationAssociationId  as LocationAssociationId,
			CRRisk.LineId AS LineId,
			CRLine.PolicyType AS CoverageForm,
			CRRiskCrime.Type as RiskType,
			CRRisk.CRRiskId AS RiskId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DcCrRiskStage CRRisk
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCRRiskCrimeStage CRRiskCrime
		ON CRRiskCrime.CRRiskId=CRRisk.CRRiskId
		AND CRRiskCrime.Sessionid=CRRisk.Sessionid
		AND NOT EXISTS (
		SELECT 1 FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCRRiskCrimeStage A
		WHERE A.CRRiskCrimeId>CRRiskCrime.CRRiskCrimeId
		AND A.CRRiskId=CRRiskCrime.CRRiskId)
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCRRiskStage WBCRRisk
			ON CRRisk.SessionId = WBCRRisk.SessionId
				AND CRRisk.CRRiskId = WBCRRisk.CRRiskId
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCRLineStage CRLine
			ON CRLine.SessionId = CRRisk.SessionId
				AND CRLine.LineId = CRRisk.LineId
		left HASH  join 
			(select SessionId,LocationAssociationId from (
				SELECT la.SessionId,la.LocationAssociationId,
				Rank() over(partition by DCLoc.SessionId 	 order by IsNull(cast(wbLoc.LocationNumber as int), 9999)) AS Record_Rank
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging la
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging DCLoc
					ON DCLoc.SessionId = la.SessionId
						AND DCLoc.LocationId = la.LocationId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationStaging wbLoc
					ON wbLoc.SessionId = la.SessionId
						AND wbLoc.LocationId = dcloc.LocationId
				-- The inner join below ensures that only records associated to DC_CR_Location are considered
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCRLocationStage DCCRLoc
					ON DCCRLoc.SessionId = la.SessionId
						AND DCCRLoc.CR_LocationId = la.ObjectId
				---WHERE la.SessionID = CRRisk.SessionId
				and ISNULL(DCLoc.Deleted,0)<>1
				) a where a.Record_Rank=1)b 
				on b.SessionId=CRRisk.SessionId
				ORDER BY CRRisk.CrRiskId --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskId ORDER BY LocationAssociationId) = 1
),
EXP_DC_CR_Risk AS (
	SELECT
	LKP_DC_CR_Risk.LocationAssociationId,
	LKP_DC_CR_Risk.LineId,
	LKP_DC_CR_Risk.CoverageForm,
	LKP_DC_CR_Risk.RiskId,
	LKP_DC_CR_Risk.RiskType,
	RTR_InsuranceLine_DC_CR_Risk.SessionId,
	'N/A' AS IndividualRiskPremiumModificationFactor,
	RTR_InsuranceLine_DC_CR_Risk.CoverageId,
	RTR_InsuranceLine_DC_CR_Risk.CoverageType AS CoverageRiskType,
	0 AS Exposure,
	-1 AS CommissionPercentage,
	'N/A' AS CoverageVersion,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	'N/A' AS SpecialClassLevel1,
	-- *INF*: IIF(ISNULL(:LKP.LKP_DCCROccupancyStaging(SessionId)),'N/A',:LKP.LKP_DCCROccupancyStaging(SessionId))
	IFF(
	    LKP_DCCROCCUPANCYSTAGING_SessionId.OccupancyTypeMonoline IS NULL, 'N/A',
	    LKP_DCCROCCUPANCYSTAGING_SessionId.OccupancyTypeMonoline
	) AS OccupancyType
	FROM RTR_InsuranceLine_DC_CR_Risk
	LEFT JOIN LKP_DC_CR_Risk
	ON LKP_DC_CR_Risk.RiskId = RTR_InsuranceLine.ParentCoverageObjectId22
	LEFT JOIN LKP_DCCROCCUPANCYSTAGING LKP_DCCROCCUPANCYSTAGING_SessionId
	ON LKP_DCCROCCUPANCYSTAGING_SessionId.SessionId = SessionId

),
LKP_DC_CR_RiskCrime AS (
	SELECT
	LocationAssociationId,
	LineId,
	CoverageForm,
	RiskType,
	RiskId,
	CRRiskCrimeId
	FROM (
		SELECT B.LocationAssociationId as LocationAssociationId,
			-- There can be multiple locations for a given risk and there is no direct linkage between the coverage and location, so the 
			-- correlated sub-query below grabs the location with the the lowest location number (usually location 1 in most instances).
				CRRisk.LineId AS LineId,
			CRLine.PolicyType AS CoverageForm,
			CRRiskCrime.Type as RiskType,
			CRRisk.CRRiskId AS RiskId,
			CRRiskCrime.CRRiskCrimeId AS CRRiskCrimeId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DcCrRiskCrimeStage CRRiskCrime
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DcCrRiskStage CRRisk
			ON CRRiskCrime.SessionId = CRRisk.SessionId
				AND CRRiskCrime.CRRiskId = CRRisk.CRRiskId
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCRRiskStage WBCRRisk
			ON CRRisk.SessionId = WBCRRisk.SessionId
				AND CRRisk.CRRiskId = WBCRRisk.CRRiskId
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCRLineStage CRLine
			ON CRLine.SessionId = CRRisk.SessionId
				AND CRLine.LineId = CRRisk.LineId
		LEFT HASH JOIN
		(SELECT LocationAssociationId , SessionID ,Record_Rank FROM (
				SELECT LocationAssociationId , la.SessionID ,
				Rank() over(partition by la.SessionID ORDER BY IsNull(cast(wbLoc.LocationNumber as int), 9999)) Record_Rank
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging la
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging DCLoc
					ON DCLoc.SessionId = la.SessionId
						AND DCLoc.LocationId = la.LocationId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationStaging wbLoc
					ON wbLoc.SessionId = la.SessionId
						AND wbLoc.LocationId = dcloc.LocationId
				-- The inner join below ensures that only records associated to DC_CR_Location are considered
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCRLocationStage DCCRLoc
					ON DCCRLoc.SessionId = la.SessionId
						AND DCCRLoc.CR_LocationId = la.ObjectId
				--WHERE la.SessionID = CRRisk.SessionId
				and ISNULL(dcloc.Deleted,0)<>1
						)A WHERE A.Record_Rank = 1)B
		ON B.SessionID = CRRisk.SessionId
		ORDER BY CRRiskCrime.CrRiskCrimeId --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CRRiskCrimeId ORDER BY LocationAssociationId) = 1
),
EXP_DC_CR_RiskCrime AS (
	SELECT
	LKP_DC_CR_RiskCrime.LocationAssociationId,
	RTR_InsuranceLine_DC_CR_RiskCrime.SessionId,
	LKP_DC_CR_RiskCrime.LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_DC_CR_RiskCrime.CoverageId,
	RTR_InsuranceLine_DC_CR_RiskCrime.CoverageType,
	LKP_DC_CR_RiskCrime.CoverageForm,
	LKP_DC_CR_RiskCrime.RiskId,
	LKP_DC_CR_RiskCrime.RiskType,
	0 AS Exposure,
	-1 AS CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	-- *INF*: IIF(ISNULL(:LKP.LKP_DCCROccupancyStaging(SessionId)),'N/A',:LKP.LKP_DCCROccupancyStaging(SessionId))
	-- --5/9/2014 Anisha
	-- --'N/A'
	IFF(
	    LKP_DCCROCCUPANCYSTAGING_SessionId.OccupancyTypeMonoline IS NULL, 'N/A',
	    LKP_DCCROCCUPANCYSTAGING_SessionId.OccupancyTypeMonoline
	) AS OccupancyType
	FROM RTR_InsuranceLine_DC_CR_RiskCrime
	LEFT JOIN LKP_DC_CR_RiskCrime
	ON LKP_DC_CR_RiskCrime.CRRiskCrimeId = RTR_InsuranceLine.ParentCoverageObjectId5
	LEFT JOIN LKP_DCCROCCUPANCYSTAGING LKP_DCCROCCUPANCYSTAGING_SessionId
	ON LKP_DCCROCCUPANCYSTAGING_SessionId.SessionId = SessionId

),
LKP_DC_CU_UmbrellaEmployersLiability AS (
	SELECT
	LocationAssociationId,
	LineId,
	CoverageForm,
	CommissionPercentage,
	CU_UmbrellaEmployersLiabilityId
	FROM (
		SELECT   LocationAssociationId.LocationAssociationId as LocationAssociationId,
			DCCUUmbrellaEL.LineId AS LineId,
			LTRIM(RTRIM(CULine.Description)) AS CoverageForm,
			WBLine.FinalCommission AS CommissionPercentage,
			DCCUUmbrellaEL.CU_UmbrellaEmployersLiabilityId AS CU_UmbrellaEmployersLiabilityId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCUUmbrellaEmployersLiabilityStaging DCCUUmbrellaEL
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCULineStaging CULine
			ON DCCUUmbrellaEL.SessionId = CULine.SessionId
				AND DCCUUmbrellaEL.LineId = CULine.LineId
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLineStaging WBLine
			ON WBLine.SessionId = CULine.SessionId
				AND WBLine.LineId = CULine.LineId
		LEFT HASH JOIN
		(SELECT * FROM (
				SELECT  la.LocationAssociationId , WBLoc.SessionId ,
				Rank() over(partition by WBLoc.SessionId order by  ISNULL(cast(wbLoc.LocationNumber as int),999)) Record_Rank
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging la
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationStaging WBLoc
					ON WBLoc.LocationId = la.LocationId
						AND WBLoc.SessionId = la.SessionId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging DCLoc
				on la.Locationid=DCLoc.LocationId
				and la.Sessionid=DCLoc.Sessionid
				--WHERE WBLoc.SessionId = DCCUUmbrellaEL.SessionId
					AND la.LocationAssociationType = 'Location'
					AND ISNULL(DCLoc.Deleted,0)<>1
				)A WHERE A.Record_Rank = 1) LocationAssociationId
				on LocationAssociationId.SessionId = DCCUUmbrellaEL.SessionId
		ORDER BY DCCUUmbrellaEL.CU_UmbrellaEmployersLiabilityId--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CU_UmbrellaEmployersLiabilityId ORDER BY LocationAssociationId) = 1
),
EXP_DC_CU_UmbrellaEmployersLiability AS (
	SELECT
	LKP_DC_CU_UmbrellaEmployersLiability.LocationAssociationId,
	RTR_InsuranceLine_DC_CU_UmbrellaEmployersLiability.SessionId,
	LKP_DC_CU_UmbrellaEmployersLiability.LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_DC_CU_UmbrellaEmployersLiability.CoverageId,
	RTR_InsuranceLine_DC_CU_UmbrellaEmployersLiability.CoverageType,
	LKP_DC_CU_UmbrellaEmployersLiability.CoverageForm,
	-1 AS RiskId,
	'N/A' AS RiskType,
	0 AS Exposure,
	LKP_DC_CU_UmbrellaEmployersLiability.CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	'N/A' AS OccupancyType
	FROM RTR_InsuranceLine_DC_CU_UmbrellaEmployersLiability
	LEFT JOIN LKP_DC_CU_UmbrellaEmployersLiability
	ON LKP_DC_CU_UmbrellaEmployersLiability.CU_UmbrellaEmployersLiabilityId = RTR_InsuranceLine.ParentCoverageObjectId18
),
LKP_DC_GL_Risk AS (
	SELECT
	LocationAssociationId,
	LineId,
	CoverageForm,
	RiskType,
	Exposure,
	CommissionPercentage,
	CoverageVersion,
	ILFTableAssignmentCode,
	OccupancyType,
	RetroactiveDate,
	GLPremiumBasis,
	LineType,
	RiskId
	FROM (
		SELECT DCLA.LocationAssociationId AS LocationAssociationId,
			GLRisk.LineId AS LineId,
			GLLine.PolicyType AS CoverageForm,
			GLRisk.Type AS RiskType,
			GLRisk.Exposure AS Exposure,
			GLLine.CommissionPercentage AS CommissionPercentage,
			GLLine.CoverageForm AS CoverageVersion,
			WBGLR.ILFTableAssignmentCode AS ILFTableAssignmentCode,
			GLO.Type AS OccupancyType,
			CASE DCLine.Type
				WHEN 'SBOPGeneralLiability'
					THEN WBGL.EmploymentPracticesRetroDate
				ELSE GLLine.RetroactiveDate
				END AS RetroactiveDate,
			GLO.GLPremiumBasis AS GLPremiumBasis,
			DCLine.Type AS LineType,
			GLRisk.GL_RiskId AS RiskId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCGLRiskStaging GLRisk
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging DCLA
			ON DCLA.SessionId = GLRisk.SessionId
				AND DCLA.ObjectId = GLRisk.GL_LocationId
				AND DCLA.ObjectName = 'DC_GL_Location'
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCGLOccupancyStaging GLO
			ON GLO.SessionId = GLRisk.SessionId
				AND GLO.GL_RiskId = GLRisk.GL_RiskId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCGLLineStaging GLLine
			ON GLLine.SessionId = GLRisk.SessionId
				AND GLLine.LineId = GLRisk.LineId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WbGlRiskStage WBGLR
			ON GLRisk.SessionId = WBGLR.SessionId
				AND GLRisk.GL_RiskId = WBGLR.GLRiskId
		--- Added as part of PROD-14731 to get Retroactive date for SBOP EPLI policies
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLineStaging DCLine
			ON GLLine.LineId = DCLine.LineId
			and GLLine.SessionId = DCLine.SessionId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBGLLineStage WBGL
			ON GLLine.SessionId = WBGL.SessionId
				AND GLLine.GL_LineId = WBGL.GL_LineId
		--- Added as part of PROD-14731 to get Retroactive date for SBOP EPLI policies
		ORDER BY GLRisk.GL_RiskId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskId ORDER BY LocationAssociationId) = 1
),
EXP_DC_GL_Risk AS (
	SELECT
	LKP_DC_GL_Risk.LocationAssociationId,
	RTR_InsuranceLine_DC_GL_Risk.SessionId,
	LKP_DC_GL_Risk.LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_DC_GL_Risk.CoverageId,
	RTR_InsuranceLine_DC_GL_Risk.CoverageType,
	LKP_DC_GL_Risk.CoverageForm,
	LKP_DC_GL_Risk.RiskId,
	LKP_DC_GL_Risk.RiskType,
	LKP_DC_GL_Risk.Exposure,
	LKP_DC_GL_Risk.CommissionPercentage,
	LKP_DC_GL_Risk.CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	LKP_DC_GL_Risk.ILFTableAssignmentCode,
	LKP_DC_GL_Risk.OccupancyType,
	LKP_DC_GL_Risk.RetroactiveDate,
	LKP_DC_GL_Risk.GLPremiumBasis,
	-- *INF*: DECODE(LOWER(SUBSTR(LTRIM(RTRIM(GLPremiumBasis)),1,1)),
	-- 'a','Area',
	-- 'c','Total Cost',
	-- 'm','Admissions',
	-- 'o','Total Operating Expenses',
	-- 'p','Payroll',
	-- 's','Gross Sales',
	-- 't','Units',
	-- 'u','Units',
	-- 'g',	'Games',
	-- 'f','Flat Charge',
	-- 'b','Bed',
	-- 'd','Days',
	-- 'e','Camper Days',
	-- 'n','Contacts',
	-- 'r','Members',
	-- 'l','Licensed',
	-- 'h','Each',
	-- 'i','Receipts',
	-- 'k','Sales',
	-- 'j','Attendant',
	-- 'q','Squre Foot',
	-- 'v','Animals',
	-- 'w','Booth',
	-- 'z','Employees',
	-- 'x','Event',
	-- 'y','Location',
	-- '1','Admission',
	-- '2','Attendee',
	-- '3','Participants',
	-- 'Units')
	-- 
	-- 
	-- 
	DECODE(
	    LOWER(SUBSTR(LTRIM(RTRIM(GLPremiumBasis)), 1, 1)),
	    'a', 'Area',
	    'c', 'Total Cost',
	    'm', 'Admissions',
	    'o', 'Total Operating Expenses',
	    'p', 'Payroll',
	    's', 'Gross Sales',
	    't', 'Units',
	    'u', 'Units',
	    'g', 'Games',
	    'f', 'Flat Charge',
	    'b', 'Bed',
	    'd', 'Days',
	    'e', 'Camper Days',
	    'n', 'Contacts',
	    'r', 'Members',
	    'l', 'Licensed',
	    'h', 'Each',
	    'i', 'Receipts',
	    'k', 'Sales',
	    'j', 'Attendant',
	    'q', 'Squre Foot',
	    'v', 'Animals',
	    'w', 'Booth',
	    'z', 'Employees',
	    'x', 'Event',
	    'y', 'Location',
	    '1', 'Admission',
	    '2', 'Attendee',
	    '3', 'Participants',
	    'Units'
	) AS o_ExposureBasis,
	LKP_DC_GL_Risk.LineType
	FROM RTR_InsuranceLine_DC_GL_Risk
	LEFT JOIN LKP_DC_GL_Risk
	ON LKP_DC_GL_Risk.RiskId = RTR_InsuranceLine.ParentCoverageObjectId3
),
LKP_DC_IM_CoverageForm AS (
	SELECT
	LocationAssociationId,
	LineId,
	RiskType,
	RiskId
	FROM (
		SELECT B.LocationAssociationId as LocationAssociationId,
			-- There can be multiple locations for a given risk and there is no direct linkage between the coverage and location, so the 
			-- correlated sub-query below grabs the location with the the lowest location number (usually location 1 in most instances)
				IMCoverageForm.LineId AS LineId,
			IMCoverageForm.Type AS RiskType,
			IMCoverageForm.IM_CoverageFormId AS RiskId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCIMCoverageFormStage IMCoverageForm
		LEFT HASH JOIN
		(SELECT * FROM (
				SELECT DCL.SessionId , LocationAssociationId , 
				Rank() over(partition by DCL.SessionId ORDER BY isnull(cast(WBL.LocationNumber as int), 9999) ASC) Record_Rank
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging DCL
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging DL
					ON DCL.Locationid = DL.LocationId
						AND DCL.Sessionid = DL.Sessionid
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationStaging WBL
					ON DCL.SessionId = WBL.SessionId
						AND DCL.LocationId = WBL.LocationId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCIMLocationStage DCIL
					ON DCIL.SessionId = DCL.SessionId
						AND DCIL.IMLocationId = DCL.ObjectId
						AND DCL.objectname = 'DC_IM_Location'
				WHERE --DCL.SessionId = IMCoverageForm.SessionId AND
					 ISNULL(DL.Deleted,0) <> 1
				)A WHERE A.Record_Rank = 1)B
		ON B.SessionId = IMCoverageForm.SessionId 
		ORDER BY IMCoverageForm.IM_CoverageFormId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskId ORDER BY LocationAssociationId) = 1
),
EXP_DC_IM_CoverageForm AS (
	SELECT
	LKP_DC_IM_CoverageForm.LocationAssociationId,
	RTR_InsuranceLine_DC_IM_CoverageForm.SessionId,
	LKP_DC_IM_CoverageForm.LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_DC_IM_CoverageForm.CoverageId,
	RTR_InsuranceLine_DC_IM_CoverageForm.CoverageType,
	-1 AS RiskId,
	LKP_DC_IM_CoverageForm.RiskType AS i_RiskType,
	-- *INF*: IIF(UPPER(SUBSTR(CoverageType,1,4))='LINE',i_RiskType,'N/A')
	IFF(UPPER(SUBSTR(CoverageType, 1, 4)) = 'LINE', i_RiskType, 'N/A') AS RiskType,
	0 AS Exposure,
	-1 AS CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	'N/A' AS OccupancyType
	FROM RTR_InsuranceLine_DC_IM_CoverageForm
	LEFT JOIN LKP_DC_IM_CoverageForm
	ON LKP_DC_IM_CoverageForm.RiskId = RTR_InsuranceLine.ParentCoverageObjectId6
),
LKP_DC_IM_Risk AS (
	SELECT
	LocationAssociationId,
	LineId,
	IndividualRiskPremiumModification,
	RiskType,
	RiskId
	FROM (
		SELECT DCLA.LocationAssociationId AS LocationAssociationId,
			IMRisk.LineId AS LineId,
			Null As IndividualRiskPremiumModification,
			IMRisk.Description AS RiskType,
			IMRisk.IM_RiskId AS RiskId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCIMRiskStage IMRisk
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBIMRiskStage WBIMRisk
			ON IMRisk.SessionId = WBIMRisk.SessionId
				AND IMRisk.IM_RiskId = WBIMRisk.IMRiskId
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCIMLocationStage DCIMLocation
			ON WBIMRisk.SessionId = DCIMLocation.SessionId
				AND WBIMRisk.IMLocationXmlId = DCIMLocation.Id
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging DCLA
			ON DCLA.ObjectId = DCIMLocation.IMLocationId
			and DCLA.SessionId = DCIMLocation.SessionId
				AND DCLA.ObjectName = 'DC_IM_Location'
		ORDER BY IMRisk.IM_RiskId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskId ORDER BY LocationAssociationId) = 1
),
EXP_DC_IM_Risk AS (
	SELECT
	LKP_DC_IM_Risk.LocationAssociationId,
	RTR_InsuranceLine_DC_IM_Risk.SessionId,
	LKP_DC_IM_Risk.LineId,
	LKP_DC_IM_Risk.IndividualRiskPremiumModification,
	RTR_InsuranceLine_DC_IM_Risk.CoverageId,
	RTR_InsuranceLine_DC_IM_Risk.CoverageType,
	NULL AS CoverageForm,
	LKP_DC_IM_Risk.RiskId,
	LKP_DC_IM_Risk.RiskType,
	0 AS Exposure,
	-1 AS CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	'N/A' AS OccupancyType
	FROM RTR_InsuranceLine_DC_IM_Risk
	LEFT JOIN LKP_DC_IM_Risk
	ON LKP_DC_IM_Risk.RiskId = RTR_InsuranceLine.ParentCoverageObjectId7
),
LKP_DC_Line AS (
	SELECT
	LocationAssociationId,
	SessionId,
	CoverageForm,
	CommissionPercentage,
	RetroactiveDate,
	RatingPlan,
	RiskType,
	CoverageVersion,
	LineType,
	ConstructionCode,
	ProtectionClass,
	i_ParentCoverageObjectId,
	LineId
	FROM (
		SELECT 
		
		LocationTables.LocationAssociationId as LocationAssociationId, 
		LocationTables.SessionId AS SessionId,
					CASE DCLine.Type
				WHEN 'CommercialUmbrella'
					THEN LTRIM(RTRIM(CULine.Description))
				WHEN 'CommercialAuto'
					THEN ISNULL(WBCALine.CoverageForm, WBCALine.PolicyType)
				WHEN 'Crime'
					THEN CRLine.PolicyType
				ELSE 'N/A'
				END AS CoverageForm,
			WBLine.FinalCommission AS CommissionPercentage,
			CASE DCLine.Type
				WHEN 'SBOPGeneralLiability'
					THEN WBGL.EmploymentPracticesRetroDate
				ELSE GLLine.RetroactiveDate
				END AS RetroactiveDate,
			WCLine.RatingPlan AS RatingPlan,
			CASE DCLine.Type
				WHEN 'CommercialAuto'
					THEN ISNULL(WBCALine.CoverageForm, WBCALine.PolicyType)
				ELSE 'N/A'
				END AS RiskType,
			GLLine.CoverageForm AS CoverageVersion,
			DCLine.Type AS LineType,
		NULL AS ConstructionCode,
		NULL AS ProtectionClass,
			DCLine.LineId AS LineId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLineStaging DCLine
		
		-- There can be multiple locations for a given risk and there is no direct linkage between the coverage and location, so the 
			-- correlated sub-query below grabs the location with the the lowest location number (usually location 1 in most instances).
		-- in order to preserve rules within where clause as opposed to bringing them out into main query this has been 
		-- changed to a Cross Apply as opposed to correlated sub-query
		
		CROSS APPLY 
		(
			SELECT TOP 1 LocationAssociation.LocationAssociationId, LocationAssociation.SessionId, DCLine.LineId
		     FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging AS LocationAssociation
		    INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationStaging AS WBLoc
		         ON WBLoc.LocationId = LocationAssociation.LocationId
		            AND WBLoc.SessionId = LocationAssociation.SessionId
		    WHERE WBLoc.SessionId = DCLine.SessionId
		          AND (ltrim(rtrim(LocationAssociation.LocationAssociationType)) = CASE
		                                                                WHEN DCLine.Type = 'BusinessOwners'
		                                                                THEN 'BP_Location'
		                                                                WHEN DCLine.Type = 'CommercialAuto'
		                                                                THEN 'CA_Location'
		                                                                WHEN DCLine.Type IN('Property', 'SBOPProperty')
		                                                                THEN 'CF_Location'
		                                                                WHEN DCLine.Type = 'Crime'
		                                                                THEN 'CR_Location'
		                                                                WHEN DCLine.Type IN('SBOPGeneralLiability', 'GeneralLiability')
		                                                                THEN 'GL_Location'
		                                                                WHEN DCLine.Type = 'InlandMarine'
		                                                                THEN 'IM_Location'
		                                                                WHEN DCLine.Type = 'WorkersCompensation'
		                                                                THEN 'WC_Location'
		                                                                ELSE 'Location'
		                                                            END
					OR ltrim(rtrim(LocationAssociation.LocationAssociationType))='Location')
		    ORDER BY CAST(ISNULL(WBLoc.LocationNumber, 9999) AS INT),case when LocationAssociationType='Location' then 2 else 1 end
			) AS LocationTables
		
		
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLineStaging WBLine
			ON WBLine.LineId = DCLine.LineId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCULineStaging CULine
			ON CULine.LineId = DCLine.LineId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCALineStaging CALine
			ON DCLine.LineId = CALine.LineId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCRLineStage CRLine
			ON DCLine.LineId = CRLine.LineId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCALineStaging WBCALine
			ON CALine.CA_LineId = WBCALine.CA_LineId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCGLLineStaging GLLine
			ON GLLine.LineId = DCLine.LineId
		--- Added as part of PROD-14731 to get Retroactive date for SBOP EPLI policies
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBGLLineStage WBGL
			ON GLLine.GL_LineId = WBGL.GL_LineId
		--- Added as part of PROD-14731 to get Retroactive date for SBOP EPLI policies
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCWCLineStaging WCLine
			ON WCLine.LineId = DCLine.LineId
		ORDER BY DCLine.LineId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LineId ORDER BY LocationAssociationId) = 1
),
LKP_StateTerm AS (
	SELECT
	ExperienceModEffectiveDate,
	RateEffectiveDate,
	LocationAssociationId
	FROM (
		select WCStateTerm.ExperienceModEffectiveDate AS ExperienceModEffectiveDate,
			WCStateTerm.RateEffectiveDate AS RateEffectiveDate,
			A.LocationAssociationId as LocationAssociationId
		 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging A
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging B
		on A.SessionId=B.SessionId
		and A.LocationId=B.LocationId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCWCStateStaging WCState
			ON A.SessionId=WCState.SessionId
				AND WCState.STATE = B.StateProv
		inner JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCWCStateTermStaging WCStateTerm
			ON WCStateTerm.WC_StateId = WCState.WC_StateId
				AND WCStateTerm.TermType = 'ORG'
		order by A.LocationAssociationId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LocationAssociationId ORDER BY ExperienceModEffectiveDate) = 1
),
EXP_DC_Line AS (
	SELECT
	LKP_DC_Line.LocationAssociationId,
	RTR_InsuranceLine_DC_Line.SessionId,
	RTR_InsuranceLine_DC_Line.ParentCoverageObjectId AS LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_DC_Line.CoverageId,
	RTR_InsuranceLine_DC_Line.CoverageType,
	-- *INF*: :LKP.LKP_WORKDCTPOLICY(SessionId)
	LKP_WORKDCTPOLICY_SessionId.WBProductType AS v_ProductType,
	-- *INF*: DECODE(TRUE,
	-- LineType='DirectorsAndOffsCondos' OR IN(LTRIM(RTRIM(v_ProductType)),'Railroad Protective','Owners Contractors Protective'),
	-- :LKP.LKP_DCSTATCODESTAGING(CoverageId,SessionId)
	-- )
	DECODE(
	    TRUE,
	    LineType = 'DirectorsAndOffsCondos' OR LTRIM(RTRIM(v_ProductType)) IN ('Railroad Protective','Owners Contractors Protective'), LKP_DCSTATCODESTAGING_CoverageId_SessionId.Value
	) AS v_lkp_Statcode,
	-- *INF*: ---EDWP -4436
	-- iif( CoverageType = 'DirectorsAndOfficersCondos' and  in (v_lkp_Statcode,'80051','80052','80053','80057') ,
	-- 'DirectorsAndOfficersCondosResidential',
	--  iif (CoverageType = 'DirectorsAndOfficersCondos'  and in(v_lkp_Statcode,'80054','80055','80056','80058'),
	-- 	'DirectorsAndOfficersCondosCommercial',CoverageType))
	-- 
	-- 
	IFF(
	    CoverageType = 'DirectorsAndOfficersCondos'
	    and v_lkp_Statcode IN ('80051','80052','80053','80057'),
	    'DirectorsAndOfficersCondosResidential',
	    IFF(
	        CoverageType = 'DirectorsAndOfficersCondos'
	        and v_lkp_Statcode IN ('80054','80055','80056','80058'),
	        'DirectorsAndOfficersCondosCommercial',
	        CoverageType
	    )
	) AS out_CoverageType,
	LKP_DC_Line.CoverageForm,
	-1 AS RiskId,
	LKP_DC_Line.RiskType,
	-- *INF*: --Commented - US-387727
	-- --DECODE(TRUE,
	-- --LineType='Crime', 
	-- --IIF(ISNULL(:LKP.LKP_DCCRRiskType(SessionId)),'N/A',:LKP.LKP_DCCRRiskType(SessionId)),
	-- --RiskType)
	-- IIF(ISNULL(RiskType),'N/A', RiskType)
	IFF(RiskType IS NULL, 'N/A', RiskType) AS o_RiskType,
	LKP_DC_Line.LineType,
	-- *INF*: DECODE(TRUE,
	-- LineType='DirectorsAndOffsCondos', :LKP.LKP_WBCDOCoverageDirectorsAndOfficersCondosStage(CoverageId),
	-- LineType='EmploymentPracticesLiab' OR 
	-- CoverageType='EmploymentPracticesLiability' OR 
	-- INSTR(CoverageType,'WB516',1,1) OR 
	-- CoverageType='NS0279', :LKP.LKP_WBEPLCoverageEmploymentPracticesLiabilityStage(CoverageId),
	-- CoverageType='BusinessIncomeExtendedPeriod', :LKP.LKP_DCBPCoverageBusinessIncomeExtendedPeriodStage(CoverageId),
	-- CoverageType='BusinessIncomeOrdinaryPayroll', :LKP.LKP_DCBPCoverageBusinessIncomeOrdinaryPayrollStage(CoverageId),
	-- LTRIM(RTRIM(v_ProductType))='Liquor Liability',:LKP.LKP_DCSTATCODE(CoverageId),
	-- LTRIM(RTRIM(v_ProductType))='Railroad Protective',:LKP.LKP_DCGLCOVERAGERAILROADPROTECTIVELIABILITYSTAGING(CoverageId),
	-- LTRIM(RTRIM(v_ProductType))='Owners Contractors Protective',:LKP.LKP_DCGLCOVERAGEOWNERSCONTRACTORSORPRINCIPALSSTAGING(CoverageId),
	-- 0)
	-- 
	DECODE(
	    TRUE,
	    LineType = 'DirectorsAndOffsCondos', LKP_WBCDOCOVERAGEDIRECTORSANDOFFICERSCONDOSSTAGE_CoverageId.NumberOfUnits,
	    LineType = 'EmploymentPracticesLiab' OR CoverageType = 'EmploymentPracticesLiability' OR REGEXP_INSTR(CoverageType, 'WB516', 1, 1) OR CoverageType = 'NS0279', LKP_WBEPLCOVERAGEEMPLOYMENTPRACTICESLIABILITYSTAGE_CoverageId.TotalNumberOfEmployees,
	    CoverageType = 'BusinessIncomeExtendedPeriod', LKP_DCBPCOVERAGEBUSINESSINCOMEEXTENDEDPERIODSTAGE_CoverageId.Days,
	    CoverageType = 'BusinessIncomeOrdinaryPayroll', LKP_DCBPCOVERAGEBUSINESSINCOMEORDINARYPAYROLLSTAGE_CoverageId.Days,
	    LTRIM(RTRIM(v_ProductType)) = 'Liquor Liability', LKP_DCSTATCODE_CoverageId.Value,
	    LTRIM(RTRIM(v_ProductType)) = 'Railroad Protective', LKP_DCGLCOVERAGERAILROADPROTECTIVELIABILITYSTAGING_CoverageId.Exposure,
	    LTRIM(RTRIM(v_ProductType)) = 'Owners Contractors Protective', LKP_DCGLCOVERAGEOWNERSCONTRACTORSORPRINCIPALSSTAGING_CoverageId.Exposure,
	    0
	) AS v_Exposure,
	-- *INF*: IIF(ISNULL(v_Exposure),0,v_Exposure)
	IFF(v_Exposure IS NULL, 0, v_Exposure) AS Exposure,
	LKP_DC_Line.CommissionPercentage,
	LKP_DC_Line.CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	-- *INF*: DECODE(TRUE,
	-- LineType='Crime', IIF(ISNULL(:LKP.LKP_DCCROccupancyStaging(SessionId)),'N/A',:LKP.LKP_DCCROccupancyStaging(SessionId)),
	-- 'N/A')
	-- 
	-- 
	-- 
	DECODE(
	    TRUE,
	    LineType = 'Crime', IFF(
	        LKP_DCCROCCUPANCYSTAGING_SessionId.OccupancyTypeMonoline IS NULL, 'N/A',
	        LKP_DCCROCCUPANCYSTAGING_SessionId.OccupancyTypeMonoline
	    ),
	    'N/A'
	) AS OccupancyType,
	LKP_DC_Line.RetroactiveDate,
	LKP_StateTerm.ExperienceModEffectiveDate,
	LKP_DC_Line.ConstructionCode AS i_ConstructionCode,
	-- *INF*: DECODE(TRUE,
	-- LineType='BusinessOwners',
	--  IIF(ISNULL(:LKP.LKP_DCBPLocation(LineId)),'N/A',
	-- SUBSTR(:LKP.LKP_DCBPLocation(LineId) , INSTR(:LKP.LKP_DCBPLocation(LineId), '--')+2 , INSTR(:LKP.LKP_DCBPLocation(LineId), '~~') -2 -INSTR(:LKP.LKP_DCBPLocation(LineId), '--')   )),
	--  NOT ISNULL(i_ConstructionCode),
	-- i_ConstructionCode,
	-- 'N/A')
	DECODE(
	    TRUE,
	    LineType = 'BusinessOwners', IFF(
	        LKP_DCBPLOCATION_LineId.RetureValue IS NULL, 'N/A',
	        SUBSTR(LKP_DCBPLOCATION_LineId.RetureValue, REGEXP_INSTR(LKP_DCBPLOCATION_LineId.RetureValue, '--') + 2, REGEXP_INSTR(LKP_DCBPLOCATION_LineId.RetureValue, '~~') - 2 - REGEXP_INSTR(LKP_DCBPLOCATION_LineId.RetureValue, '--'))
	    ),
	    i_ConstructionCode IS NOT NULL, i_ConstructionCode,
	    'N/A'
	) AS o_ConstructionCode,
	LKP_StateTerm.RateEffectiveDate,
	LKP_DC_Line.RatingPlan,
	LKP_DC_Line.ProtectionClass AS i_ProtectionClass,
	-- *INF*: DECODE(TRUE,
	-- LineType='BusinessOwners', IIF(ISNULL(:LKP.LKP_DCBPLocation(LineId)),'N/A',SUBSTR(:LKP.LKP_DCBPLocation(LineId) , INSTR(:LKP.LKP_DCBPLocation(LineId), '~~')+2 )),
	--  NOT ISNULL(i_ProtectionClass),
	-- i_ProtectionClass,
	-- 'N/A')
	-- 
	-- 
	DECODE(
	    TRUE,
	    LineType = 'BusinessOwners', IFF(
	        LKP_DCBPLOCATION_LineId.RetureValue IS NULL, 'N/A',
	        SUBSTR(LKP_DCBPLOCATION_LineId.RetureValue, REGEXP_INSTR(LKP_DCBPLOCATION_LineId.RetureValue, '~~') + 2)
	    ),
	    i_ProtectionClass IS NOT NULL, i_ProtectionClass,
	    'N/A'
	) AS o_ProtectionClass,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(v_ProductType))='Liquor Liability','Sales',
	-- LTRIM(RTRIM(v_ProductType))='Railroad Protective' and IN(v_lkp_Statcode,'40011','40012','40013','40014'),'Total Cost',
	-- LTRIM(RTRIM(v_ProductType))='Owners Contractors Protective' and IN(v_lkp_Statcode,'16291','91181','16292','17982','93161','93163','93040'),'Total Cost of Work',
	-- LTRIM(RTRIM(v_ProductType))='Owners Contractors Protective' and IN(v_lkp_Statcode,'27111','27112'),'Newcarriers',
	-- LTRIM(RTRIM(v_ProductType))='Owners Contractors Protective' and IN(v_lkp_Statcode,'15191','15192'),'Independent Contractors'
	-- )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(v_ProductType)) = 'Liquor Liability', 'Sales',
	    LTRIM(RTRIM(v_ProductType)) = 'Railroad Protective' and v_lkp_Statcode IN ('40011','40012','40013','40014'), 'Total Cost',
	    LTRIM(RTRIM(v_ProductType)) = 'Owners Contractors Protective' and v_lkp_Statcode IN ('16291','91181','16292','17982','93161','93163','93040'), 'Total Cost of Work',
	    LTRIM(RTRIM(v_ProductType)) = 'Owners Contractors Protective' and v_lkp_Statcode IN ('27111','27112'), 'Newcarriers',
	    LTRIM(RTRIM(v_ProductType)) = 'Owners Contractors Protective' and v_lkp_Statcode IN ('15191','15192'), 'Independent Contractors'
	) AS o_ExposureBasis,
	LKP_DC_Line.i_ParentCoverageObjectId AS lkp_LineId_DCLine,
	LKP_DC_Line.SessionId AS lkp_SessionId_DCLine
	FROM RTR_InsuranceLine_DC_Line
	LEFT JOIN LKP_DC_Line
	ON LKP_DC_Line.LineId = RTR_InsuranceLine.ParentCoverageObjectId1
	LEFT JOIN LKP_StateTerm
	ON LKP_StateTerm.LocationAssociationId = LKP_DC_Line.LocationAssociationId
	LEFT JOIN LKP_WORKDCTPOLICY LKP_WORKDCTPOLICY_SessionId
	ON LKP_WORKDCTPOLICY_SessionId.SessionId = SessionId

	LEFT JOIN LKP_DCSTATCODESTAGING LKP_DCSTATCODESTAGING_CoverageId_SessionId
	ON LKP_DCSTATCODESTAGING_CoverageId_SessionId.ObjectId = CoverageId
	AND LKP_DCSTATCODESTAGING_CoverageId_SessionId.SessionId = SessionId

	LEFT JOIN LKP_WBCDOCOVERAGEDIRECTORSANDOFFICERSCONDOSSTAGE LKP_WBCDOCOVERAGEDIRECTORSANDOFFICERSCONDOSSTAGE_CoverageId
	ON LKP_WBCDOCOVERAGEDIRECTORSANDOFFICERSCONDOSSTAGE_CoverageId.CoverageId = CoverageId

	LEFT JOIN LKP_WBEPLCOVERAGEEMPLOYMENTPRACTICESLIABILITYSTAGE LKP_WBEPLCOVERAGEEMPLOYMENTPRACTICESLIABILITYSTAGE_CoverageId
	ON LKP_WBEPLCOVERAGEEMPLOYMENTPRACTICESLIABILITYSTAGE_CoverageId.CoverageId = CoverageId

	LEFT JOIN LKP_DCBPCOVERAGEBUSINESSINCOMEEXTENDEDPERIODSTAGE LKP_DCBPCOVERAGEBUSINESSINCOMEEXTENDEDPERIODSTAGE_CoverageId
	ON LKP_DCBPCOVERAGEBUSINESSINCOMEEXTENDEDPERIODSTAGE_CoverageId.CoverageId = CoverageId

	LEFT JOIN LKP_DCBPCOVERAGEBUSINESSINCOMEORDINARYPAYROLLSTAGE LKP_DCBPCOVERAGEBUSINESSINCOMEORDINARYPAYROLLSTAGE_CoverageId
	ON LKP_DCBPCOVERAGEBUSINESSINCOMEORDINARYPAYROLLSTAGE_CoverageId.CoverageId = CoverageId

	LEFT JOIN LKP_DCSTATCODE LKP_DCSTATCODE_CoverageId
	ON LKP_DCSTATCODE_CoverageId.ObjectId = CoverageId

	LEFT JOIN LKP_DCGLCOVERAGERAILROADPROTECTIVELIABILITYSTAGING LKP_DCGLCOVERAGERAILROADPROTECTIVELIABILITYSTAGING_CoverageId
	ON LKP_DCGLCOVERAGERAILROADPROTECTIVELIABILITYSTAGING_CoverageId.CoverageId = CoverageId

	LEFT JOIN LKP_DCGLCOVERAGEOWNERSCONTRACTORSORPRINCIPALSSTAGING LKP_DCGLCOVERAGEOWNERSCONTRACTORSORPRINCIPALSSTAGING_CoverageId
	ON LKP_DCGLCOVERAGEOWNERSCONTRACTORSORPRINCIPALSSTAGING_CoverageId.CoverageId = CoverageId

	LEFT JOIN LKP_DCCROCCUPANCYSTAGING LKP_DCCROCCUPANCYSTAGING_SessionId
	ON LKP_DCCROCCUPANCYSTAGING_SessionId.SessionId = SessionId

	LEFT JOIN LKP_DCBPLOCATION LKP_DCBPLOCATION_LineId
	ON LKP_DCBPLOCATION_LineId.LineId = LineId

),
LKP_DC_Location AS (
	SELECT
	LocationAssociationId,
	LineId,
	PredominantLiabilityLiabExpBase,
	ConstructionCode,
	ProtectionClassOverride,
	BuildingNumber,
	OccupancyClassDescription,
	ActiveBuildingFlag,
	LocationId
	FROM (
		SELECT DCLocationAssociationStaging.LocationAssociationId AS LocationAssociationID,
			DCLineStaging.LineId AS LineID,
			Null AS PredominantLiabilityLiabExpBase,
			NULL AS ConstructionCode,
			WBBPLocationAccountStage.ProtectionClassOverride AS ProtectionClassOverride,
			NULL AS BuildingNumber,
			NULL AS OccupancyClassDescription,
			NULL AS ActiveBuildingFlag,
			DCLocationStaging.LocationId AS LocationID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging
			ON DCLocationStaging.SessionId = DCLocationAssociationStaging.SessionId
				AND DCLocationStaging.LocationId = DCLocationAssociationStaging.LocationId
				AND ObjectName = 'DC_BP_Location'
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBPLocationStage
			ON DCLocationAssociationStaging.SessionId = DCBPLocationStage.SessionId
				AND DCLocationAssociationStaging.ObjectId = DCBPLocationStage.BPLocationId
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationAccountStage
			ON DCLocationStaging.SessionId = WBLocationAccountStage.SessionId
				AND DCLocationStaging.LocationId = WBLocationAccountStage.LocationId
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCLLocationAccountStage
			ON WBLocationAccountStage.SessionId = WBCLLocationAccountStage.SessionId
				AND WBLocationAccountStage.WBLocationAccountId = WBCLLocationAccountStage.WBLocationAccountId
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBBPLocationAccountStage
			ON WBBPLocationAccountStage.SessionId = WBCLLocationAccountStage.SessionId
				AND WBBPLocationAccountStage.WB_CL_LocationAccountId = WBCLLocationAccountStage.WBCLLocationAccountId
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLineStaging
			ON DCLocationStaging.SessionId = DCLineStaging.SessionId
				AND DCLineStaging.Type = 'BusinessOwners'
		ORDER BY DCBPLocationStage.BPLocationId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LocationId ORDER BY LocationAssociationId) = 1
),
EXP_DC_Location AS (
	SELECT
	LKP_DC_Location.LocationAssociationId,
	RTR_InsuranceLine_DC_Location.SessionId,
	LKP_DC_Location.LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_DC_Location.CoverageId,
	RTR_InsuranceLine_DC_Location.CoverageType,
	-1 AS RiskId,
	'N/A' AS RiskType,
	LKP_DC_Location.PredominantLiabilityLiabExpBase,
	-1 AS CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	LKP_DC_Location.BuildingNumber AS i_BuildingNumber,
	-- *INF*: LPAD(TO_INTEGER(i_BuildingNumber),3,'0')
	LPAD(CAST(i_BuildingNumber AS INTEGER), 3, '0') AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	'N/A' AS OccupancyType,
	LKP_DC_Location.ConstructionCode,
	LKP_DC_Location.ProtectionClassOverride,
	LKP_DC_Location.OccupancyClassDescription,
	LKP_DC_Location.ActiveBuildingFlag,
	RTR_InsuranceLine_DC_Location.SubCoverageType AS SubCoverageType26
	FROM RTR_InsuranceLine_DC_Location
	LEFT JOIN LKP_DC_Location
	ON LKP_DC_Location.LocationId = RTR_InsuranceLine.ParentCoverageObjectId26
),
LKP_DC_WC_Risk AS (
	SELECT
	LocationAssociationId,
	LineId,
	ExposureBasis,
	ClassCode,
	CoverageForm,
	CommissionPercentage,
	ExperienceModEffectiveDate,
	RateEffectiveDate,
	RatingPlan,
	RiskId
	FROM (
		SELECT LA.LocationAssociationId AS LocationAssociationId,
			WCRisk.LineId AS LineId,
			WCRisk.ExposureBasis AS ExposureBasis,
			DCC.Value AS ClassCode,
			WCLine.PolicyRatingType AS CoverageForm,
			WCLine.CommissionPercentage AS CommissionPercentage,
			WCT.PeriodStartDate AS ExperienceModEffectiveDate,
			WCT.TermRateEffectiveDate AS RateEffectiveDate,
			WCLine.RatingPlan AS RatingPlan,
			WCRisk.WC_RiskId AS RiskId
		FROM DCWCRiskStaging WCRisk
		LEFT JOIN DCClassCodeStaging DCC
			ON DCC.ObjectId = WCRisk.WC_RiskId
				AND DCC.SessionId = WCRisk.SessionId
				AND DCC.ObjectName = 'DC_WC_Risk'
		LEFT JOIN DCWCLocationStaging WCL
			ON WCL.WC_LocationId = WCRisk.WC_LocationId
				AND WCL.SessionId = WCRisk.SessionId
		LEFT JOIN DCLocationAssociationStaging LA
			ON LA.ObjectId = WCL.WC_LocationId
				AND LA.ObjectName = 'DC_WC_Location'
				AND LA.SessionId = WCL.SessionId
		LEFT JOIN DCLocationStaging DCL
			ON DCL.LocationId = LA.LocationId
				AND DCL.SessionId = LA.SessionId
		LEFT JOIN DCWCLineStaging WCLine
			ON WCLine.LineId = WCRisk.LineId
				AND WCLine.SessionId = WCRisk.SessionId
		LEFT JOIN DCCoverageStaging DC
			ON WCRisk.WC_RiskId = DC.ObjectId
				AND DC.ObjectName = 'DC_WC_Risk'
				AND WCRisk.SessionId = DC.SessionId
		LEFT JOIN WBCoverageStage WC
			ON DC.CoverageId = WC.CoverageId
				AND DC.SessionId = WC.SessionId
		LEFT JOIN WBWCCoverageTermStage WCT
			ON WC.WBCoverageId = WCT.WB_CoverageId
				AND WC.SessionId = WCT.SessionId
		ORDER BY WCRisk.WC_RiskId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskId ORDER BY LocationAssociationId) = 1
),
EXP_DC_WC_Risk AS (
	SELECT
	LKP_DC_WC_Risk.LocationAssociationId,
	RTR_InsuranceLine_DC_WC_Risk.SessionId,
	LKP_DC_WC_Risk.LineId,
	LKP_DC_WC_Risk.ExposureBasis AS i_ExposureBasis,
	LKP_DC_WC_Risk.ClassCode,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_DC_WC_Risk.CoverageId,
	RTR_InsuranceLine_DC_WC_Risk.CoverageType,
	LKP_DC_WC_Risk.CoverageForm,
	LKP_DC_WC_Risk.RiskId,
	'N/A' AS RiskType,
	-- *INF*: :LKP.LKP_EXPOSURE(CoverageId)
	LKP_EXPOSURE_CoverageId.Value AS v_Exposure,
	-- *INF*: IIF(
	-- ISNULL(v_Exposure) OR NOT IS_NUMBER(v_Exposure),
	-- 0,TO_DECIMAL(v_Exposure))
	IFF(
	    v_Exposure IS NULL OR NOT REGEXP_LIKE(v_Exposure, '^[0-9]+$'), 0, CAST(v_Exposure AS FLOAT)
	) AS Exposure,
	LKP_DC_WC_Risk.CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	'N/A' AS OccupancyType,
	LKP_DC_WC_Risk.ExperienceModEffectiveDate,
	LKP_DC_WC_Risk.RateEffectiveDate,
	LKP_DC_WC_Risk.RatingPlan,
	-- *INF*: DECODE(TRUE,
	-- ClassCode = '0908','Y',
	-- ClassCode = '0913','Y',
	-- ClassCode = '7709','Y',
	-- 'N')
	-- --Flag eligible class codes of Exposure Basis 'Unit' to compensate for issue where ExampleData doesn't always correctly pass along correct exposure basis. See AP-118 and PROD-9417. Once this is fixed in ExampleIDO for good, this class code based override needs to be removed 
	DECODE(
	    TRUE,
	    ClassCode = '0908', 'Y',
	    ClassCode = '0913', 'Y',
	    ClassCode = '7709', 'Y',
	    'N'
	) AS v_UnitTypeFlag,
	-- *INF*: DECODE(TRUE,
	-- v_UnitTypeFlag = 'Y','Unit',
	-- v_UnitTypeFlag = 'N','Payroll'
	-- )
	-- -- We override lookup values with with a value depending on hard coded class codes for Unit ExposureBasis per PROD-9417 until it is fixed.
	DECODE(
	    TRUE,
	    v_UnitTypeFlag = 'Y', 'Unit',
	    v_UnitTypeFlag = 'N', 'Payroll'
	) AS o_ExposureBasis
	FROM RTR_InsuranceLine_DC_WC_Risk
	LEFT JOIN LKP_DC_WC_Risk
	ON LKP_DC_WC_Risk.RiskId = RTR_InsuranceLine.ParentCoverageObjectId4
	LEFT JOIN LKP_EXPOSURE LKP_EXPOSURE_CoverageId
	ON LKP_EXPOSURE_CoverageId.ObjectId = CoverageId

),
LKP_DC_WC_State AS (
	SELECT
	LocationAssociationId,
	LineId,
	CommissionPercentage,
	ExperienceModEffectiveDate,
	RateEffectiveDate,
	RatingPlan,
	CoverageForm,
	RiskId
	FROM (
		SELECT B.LocationAssociationId as LocationAssociationId,
		
		WCState.LineId AS LineId,
		
		WCLine.CommissionPercentage AS CommissionPercentage,
		
		WCST.PeriodStartDate AS ExperienceModEffectiveDate,
		
		WCST.RateEffectiveDate AS RateEffectiveDate,
		
		WCLine.RatingPlan AS RatingPlan,
		
		WCLine.PolicyRatingType AS CoverageForm,
		
		WCS.WC_StateId AS RiskId
		
		FROM dbo.DCWCStateStaging WCState
		
		INNER JOIN dbo.DCWCStateStaging WCS
		
		ON WCState.WC_StateId = WCS.WC_StateId
		
		AND WCState.SessionId = WCS.SessionId
		INNER JOIN dbo.DCWCStatetermStaging WCST
		
		ON WCState.WC_StateId = WCST.WC_StateId
		
		AND WCState.SessionId = WCST.SessionId
		
		LEFT JOIN dbo.DCWCLineStaging WCLine
		
		ON WCLine.LineId = WCState.LineId
		
		AND WCLine.SessionId = WCState.SessionId
		
		LEFT HASH JOIN
		
		(SELECT * FROM (
		
		SELECT A.SessionId , B.LocationAssociationId , A.StateProv ,
		
		Rank() over(partition by A.SessionId,A.StateProv ORDER BY isnull(cast(c.LocationNumber as int), 9999)) Record_Rank
		
		FROM dbo.DCLocationstaging A
		
		INNER JOIN dbo.DCLocationAssociationStaging B
		
		ON A.LocationId = B.LocationId
		
		AND A.SessionId = B.SessionId
		
		INNER JOIN dbo.WBLocationStaging C
		
		ON A.LocationId = C.LocationId
		
		AND A.SessionId = C.SessionId
		
		INNER JOIN dbo.DCWCLocationStaging D
		
		ON B.ObjectId = D.WC_LocationId
		
		AND B.SessionId = D.SessionId
		
		WHERE B.LocationAssociationType = 'WC_Location'
		
		--AND A.SessionId = WCState.SessionId
		
		--AND A.StateProv = WCState.STATE
		
		AND ISNULL(A.Deleted,0)<>1
		
		)A WHERE A.Record_Rank = 1)B
		
		ON B.SessionId = WCState.SessionId
		
		AND B.StateProv = WCState.STATE
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskId ORDER BY LocationAssociationId) = 1
),
EXP_DC_WC_State AS (
	SELECT
	LKP_DC_WC_State.LocationAssociationId,
	RTR_InsuranceLine_DC_WC_State.SessionId,
	LKP_DC_WC_State.LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_DC_WC_State.CoverageId,
	RTR_InsuranceLine_DC_WC_State.CoverageType,
	LKP_DC_WC_State.CoverageForm,
	-1 AS RiskId,
	'N/A' AS RiskType,
	0 AS Exposure,
	LKP_DC_WC_State.CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	'N/A' AS OccupancyType,
	LKP_DC_WC_State.ExperienceModEffectiveDate,
	LKP_DC_WC_State.RateEffectiveDate,
	LKP_DC_WC_State.RatingPlan
	FROM RTR_InsuranceLine_DC_WC_State
	LEFT JOIN LKP_DC_WC_State
	ON LKP_DC_WC_State.RiskId = RTR_InsuranceLine.ParentCoverageObjectId27
),
LKP_DC_WC_StateTerm AS (
	SELECT
	LocationAssociationId,
	LineId,
	CommissionPercentage,
	ExperienceModEffectiveDate,
	RateEffectiveDate,
	RatingPlan,
	CoverageForm,
	RiskId
	FROM (
		SELECT B.LocationAssociationId as LocationAssociationId, 
			-- There can be multiple locations for a given risk and there is no direct linkage between the coverage and location, so the 
			-- correlated sub-query below grabs the location with the lowest location number (usually location 1 in most instances) for
			-- for a given state in question.
			WCState.LineId AS LineId,
			WCLine.CommissionPercentage AS CommissionPercentage,
			WCST.PeriodStartDate AS ExperienceModEffectiveDate,
			WCST.RateEffectiveDate AS RateEffectiveDate,
			WCLine.RatingPlan AS RatingPlan,
			WCLine.PolicyRatingType AS CoverageForm,
			WCST.WC_StateTermId AS RiskId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCWCStateStaging WCState
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCWCStateTermStaging WCST
			ON WCState.WC_StateId = WCST.WC_StateId
				AND WCState.SessionId = WCST.SessionId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCWCLineStaging WCLine
			ON WCLine.LineId = WCState.LineId
				AND WCLine.SessionId = WCState.SessionId
		LEFT HASH JOIN
		(SELECT * FROM (
				SELECT A.SessionId , B.LocationAssociationId , A.StateProv ,
				Rank() over(partition by A.SessionId,A.StateProv ORDER BY isnull(cast(c.LocationNumber as int), 9999)) Record_Rank
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging A
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging B
					ON A.LocationId = B.LocationId
						AND A.SessionId = B.SessionId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationStaging C
					ON A.LocationId = C.LocationId
						AND A.SessionId = C.SessionId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCWCLocationStaging D
					ON B.ObjectId = D.WC_LocationId
						AND B.SessionId = D.SessionId
				WHERE B.LocationAssociationType = 'WC_Location'
					--AND A.SessionId = WCState.SessionId
					--AND A.StateProv = WCState.STATE
					AND ISNULL(A.Deleted,0)<>1
						)A WHERE A.Record_Rank = 1)B
		ON B.SessionId = WCState.SessionId
		AND B.StateProv = WCState.STATE
		ORDER BY WCST.WC_StateTermId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskId ORDER BY LocationAssociationId) = 1
),
EXP_DC_WC_StateTerm AS (
	SELECT
	LKP_DC_WC_StateTerm.LocationAssociationId,
	RTR_InsuranceLine_DC_WC_StateTerm.SessionId,
	LKP_DC_WC_StateTerm.LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_DC_WC_StateTerm.CoverageId,
	RTR_InsuranceLine_DC_WC_StateTerm.CoverageType,
	LKP_DC_WC_StateTerm.CoverageForm,
	-1 AS RiskId,
	'N/A' AS RiskType,
	0 AS Exposure,
	LKP_DC_WC_StateTerm.CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	'N/A' AS OccupancyType,
	LKP_DC_WC_StateTerm.ExperienceModEffectiveDate,
	LKP_DC_WC_StateTerm.RateEffectiveDate,
	LKP_DC_WC_StateTerm.RatingPlan
	FROM RTR_InsuranceLine_DC_WC_StateTerm
	LEFT JOIN LKP_DC_WC_StateTerm
	ON LKP_DC_WC_StateTerm.RiskId = RTR_InsuranceLine.ParentCoverageObjectId19
),
LKP_WB_CU_PremiumDetail AS (
	SELECT
	LocationAssociationId,
	LineId,
	CoverageForm,
	CommissionPercentage,
	WBCUPremiumDetailId
	FROM (
		SELECT B.LocationAssociationId as LocationAssociationId,
			-- There can be multiple locations for a given risk and there is no direct linkage between the coverage and location, so the 
			-- correlated sub-query below grabs the location with the lowest location number (usually location 1 in most instances). It
			-- focuses in on risk locations by only grabbing records with a type of Location.
			WBCUPremiumDetail.LineId AS LineId,
			LTRIM(RTRIM(CULine.Description)) AS CoverageForm,
			WBLine.FinalCommission AS CommissionPercentage,
			WBCUPremiumDetail.WBCUPremiumDetailId AS WBCUPremiumDetailId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCUPremiumDetailStage WBCUPremiumDetail
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCULineStaging CULine
			ON WBCUPremiumDetail.SessionId = CULine.SessionId
				AND WBCUPremiumDetail.LineId = CULine.LineId
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLineStaging WBLine
			ON WBLine.SessionId = CULine.SessionId
				AND WBLine.LineId = CULine.LineId
		LEFT HASH JOIN 
		(SELECT * FROM (
				SELECT WBLoc.SessionId, la.LocationAssociationId ,
				Rank() over(partition by WBLoc.SessionId order by ISNULL(cast(wbLoc.LocationNumber as int),999)) Record_Rank
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging la
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationStaging WBLoc
					ON WBLoc.LocationId = la.LocationId
						AND WBLoc.SessionId = la.SessionId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging DCLoc
				on la.Locationid=DCLoc.LocationId
				and la.Sessionid=DCLoc.Sessionid
				--WHERE WBLoc.SessionId = WBCUPremiumDetail.SessionId
					AND la.LocationAssociationType = 'Location'
					AND ISNULL(DCLoc.Deleted,0)<>1
				)A WHERE A.Record_Rank = 1)B
		ON B.SessionId = WBCUPremiumDetail.SessionId
		ORDER BY WBCUPremiumDetail.WBCUPremiumDetailId--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WBCUPremiumDetailId ORDER BY LocationAssociationId) = 1
),
EXP_WB_CU_PremiumDetail AS (
	SELECT
	LKP_WB_CU_PremiumDetail.LocationAssociationId,
	RTR_InsuranceLine_WB_CU_PremiumDetail.SessionId,
	LKP_WB_CU_PremiumDetail.LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_WB_CU_PremiumDetail.CoverageId,
	RTR_InsuranceLine_WB_CU_PremiumDetail.CoverageType,
	LKP_WB_CU_PremiumDetail.CoverageForm,
	-1 AS RiskId,
	'N/A' AS RiskType,
	0 AS Exposure,
	LKP_WB_CU_PremiumDetail.CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	'N/A' AS OccupancyType
	FROM RTR_InsuranceLine_WB_CU_PremiumDetail
	LEFT JOIN LKP_WB_CU_PremiumDetail
	ON LKP_WB_CU_PremiumDetail.WBCUPremiumDetailId = RTR_InsuranceLine.ParentCoverageObjectId17
),
LKP_WB_EC_Risk AS (
	SELECT
	LocationAssociationId,
	LineId,
	RiskId
	FROM (
		SELECT DCL.LocationAssociationId AS LocationAssociationId,
			ECRisk.LineId AS LineId,
			ECRisk.WB_EC_RiskId AS RiskId
		FROM WBECRiskStage ECRisk
		LEFT OUTER JOIN DCLocationAssociationStaging DCL
			ON DCL.LocationXmlId = ECRisk.LocationId
				AND DCL.SessionId = ECRisk.SessionId
		ORDER BY ECRisk.WB_EC_RiskId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskId ORDER BY LocationAssociationId) = 1
),
EXP_WB_EC_Risk AS (
	SELECT
	LKP_WB_EC_Risk.LocationAssociationId,
	RTR_InsuranceLine_WB_EC_Risk.SessionId,
	LKP_WB_EC_Risk.LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_WB_EC_Risk.CoverageId,
	RTR_InsuranceLine_WB_EC_Risk.CoverageType,
	-1 AS RiskId,
	'N/A' AS RiskType,
	0 AS Exposure,
	-1 AS CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	'N/A' AS OccupancyType
	FROM RTR_InsuranceLine_WB_EC_Risk
	LEFT JOIN LKP_WB_EC_Risk
	ON LKP_WB_EC_Risk.RiskId = RTR_InsuranceLine.ParentCoverageObjectId24
),
LKP_WB_EC_State AS (
	SELECT
	LocationAssociationId,
	LineId,
	RiskId
	FROM (
		SELECT B.LocationAssociationId as LocationAssociationId,
			-- There can be multiple locations for a given risk and there is no direct linkage between the coverage and location, so the 
			-- correlated sub-query below grabs the location with the lowest location number (usually location 1 in most instances).
			ECState.LineId AS LineId,
			ECState.WB_EC_StateId AS RiskId
		FROM .WBECStateStage ECState
		LEFT HASH JOIN
			(SELECT * FROM (
				SELECT A.SessionId , B.LocationAssociationId ,A.StateProv
				,Rank() over(partition by A.SessionId,A.StateProv ORDER BY isnull(cast(c.LocationNumber as int), 9999)) Record_Rank
				FROM DCLocationStaging A
				INNER JOIN DCLocationAssociationStaging B
					ON A.LocationId = B.LocationId
						AND A.SessionId = B.SessionId
				INNER JOIN WBLocationStaging C
					ON A.LocationId = C.LocationId
						AND A.SessionId = C.SessionId
				WHERE B.LocationAssociationType = 'Location'
					AND ISNULL(A.Deleted,0)<>1
			)A
		WHERE A.Record_Rank = 1
		)B
		ON 
		B.SessionId = ECState.SessionId
		AND B.StateProv = ECState.StateAbbreviation		
		ORDER BY ECState.WB_EC_StateId --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskId ORDER BY LocationAssociationId) = 1
),
EXP_WB_EC_State AS (
	SELECT
	LKP_WB_EC_State.LocationAssociationId,
	RTR_InsuranceLine_WB_EC_State.SessionId,
	LKP_WB_EC_State.LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_WB_EC_State.CoverageId,
	RTR_InsuranceLine_WB_EC_State.CoverageType,
	-1 AS RiskId,
	'N/A' AS RiskType,
	0 AS Exposure,
	-1 AS CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	'N/A' AS OccupancyType
	FROM RTR_InsuranceLine_WB_EC_State
	LEFT JOIN LKP_WB_EC_State
	ON LKP_WB_EC_State.RiskId = RTR_InsuranceLine.ParentCoverageObjectId25
),
LKP_WB_GOC_Risk AS (
	SELECT
	LocationAssociationId,
	LineId,
	RiskId
	FROM (
		SELECT DCL.LocationAssociationId AS LocationAssociationId,
			GOCRisk.LineId AS LineId,
			GOCRisk.WBGOCRiskId AS RiskId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBGOCRiskStage GOCRisk
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging DCL
			ON DCL.LocationXmlId = GOCRisk.LocationId
				AND DCL.SessionId = GOCRisk.SessionId
		ORDER BY GOCRisk.WBGOCRiskId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskId ORDER BY LocationAssociationId) = 1
),
EXP_WB_GOC_Risk AS (
	SELECT
	LKP_WB_GOC_Risk.LocationAssociationId,
	RTR_InsuranceLine_WB_GOC_Risk.SessionId,
	LKP_WB_GOC_Risk.LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_WB_GOC_Risk.CoverageId,
	RTR_InsuranceLine_WB_GOC_Risk.CoverageType,
	LKP_WB_GOC_Risk.RiskId,
	'N/A' AS RiskType,
	0 AS Exposure,
	-1 AS CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	'N/A' AS OccupancyType
	FROM RTR_InsuranceLine_WB_GOC_Risk
	LEFT JOIN LKP_WB_GOC_Risk
	ON LKP_WB_GOC_Risk.RiskId = RTR_InsuranceLine.ParentCoverageObjectId10
),
LKP_WB_GOC_State AS (
	SELECT
	LocationAssociationId,
	LineId,
	RiskId
	FROM (
		SELECT
			B.LocationAssociationId as LocationAssociationId,
			-- There can be multiple locations for a given risk and there is no direct linkage between the coverage and location, so the 
			-- correlated sub-query below grabs the location with the lowest location number (usually location 1 in most instances).
			GOCState.LineId AS LineId,
			GOCState.WBGOCStateId AS RiskId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBGOCStateStage GOCState
		LEFT HASH JOIN
		(
		SELECT A.SessionId , A.StateProv , LocationAssociationId ,Record_Rank FROM (
				SELECT A.SessionId , A.StateProv , B.LocationAssociationId ,
				Rank() over(partition by A.SessionId , A.StateProv ORDER BY isnull(cast(c.LocationNumber as int), 9999)) Record_Rank
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging A
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging B
					ON A.LocationId = B.LocationId
						AND A.SessionId = B.SessionId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationStaging C
					ON A.LocationId = C.LocationId
						AND A.SessionId = C.SessionId
				WHERE B.LocationAssociationType = 'Location'
				and ISNULL(A.Deleted,0)<>1
				--	AND A.SessionId = GOCState.SessionId
				--	AND A.StateProv = GOCState.StateAbbreviation
				)A WHERE A.Record_Rank = 1)B
		ON 
		B.SessionId = GOCState.SessionId
		AND B.StateProv = GOCState.StateAbbreviation
		ORDER BY GOCState.WBGOCStateId --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskId ORDER BY LocationAssociationId) = 1
),
EXP_WB_GOC_State AS (
	SELECT
	LKP_WB_GOC_State.LocationAssociationId,
	RTR_InsuranceLine_WB_GOC_State.SessionId,
	LKP_WB_GOC_State.LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_WB_GOC_State.CoverageId,
	RTR_InsuranceLine_WB_GOC_State.CoverageType,
	-1 AS RiskId,
	'N/A' AS RiskType,
	0 AS Exposure,
	-1 AS CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	'N/A' AS OccupancyType
	FROM RTR_InsuranceLine_WB_GOC_State
	LEFT JOIN LKP_WB_GOC_State
	ON LKP_WB_GOC_State.RiskId = RTR_InsuranceLine.ParentCoverageObjectId20
),
LKP_WB_HIO_Risk AS (
	SELECT
	LocationAssociationId,
	LineId,
	RiskId
	FROM (
		SELECT DCL.LocationAssociationId AS LocationAssociationId,
			HIORisk.LineId AS LineId,
			HIORisk.WBHIORiskId AS RiskId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBHIORiskStage HIORisk
		LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging DCL
			ON DCL.SessionId = hiorisk.SessionId
				AND dcl.LocationXmlId = HIORisk.LocationId
		ORDER BY HIORisK.WBHIORiskId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskId ORDER BY LocationAssociationId) = 1
),
EXP_WB_HIO_Risk AS (
	SELECT
	LKP_WB_HIO_Risk.LocationAssociationId,
	RTR_InsuranceLine_WB_HIO_Risk.SessionId,
	LKP_WB_HIO_Risk.LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_WB_HIO_Risk.CoverageId,
	RTR_InsuranceLine_WB_HIO_Risk.CoverageType,
	-1 AS RiskId,
	'N/A' AS RiskType,
	0 AS Exposure,
	-1 AS CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	'N/A' AS OccupancyType
	FROM RTR_InsuranceLine_WB_HIO_Risk
	LEFT JOIN LKP_WB_HIO_Risk
	ON LKP_WB_HIO_Risk.RiskId = RTR_InsuranceLine.ParentCoverageObjectId8
),
LKP_WB_HIO_State AS (
	SELECT
	LocationAssociationId,
	LineId,
	RiskId
	FROM (
		SELECT B.LocationAssociationId as LocationAssociationId,
			-- There can be multiple locations for a given risk and there is no direct linkage between the coverage and location, so the 
			-- correlated sub-query below grabs the location with the lowest location number (usually location 1 in most instances).
			HIOState.LineId AS LineId,
			HIOState.WBHIOStateId AS RiskId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBHIOStateStage HIOState
		LEFT HASH JOIN
			(SELECT * FROM (
				SELECT A.SessionId , B.LocationAssociationId ,A.StateProv
				,Rank() over(partition by A.SessionId,A.StateProv ORDER BY isnull(cast(c.LocationNumber as int), 9999)) Record_Rank
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging A
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging B
					ON A.LocationId = B.LocationId
						AND A.SessionId = B.SessionId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationStaging C
					ON A.LocationId = C.LocationId
						AND A.SessionId = C.SessionId
				WHERE B.LocationAssociationType = 'Location'
					--AND A.SessionId = HIOState.SessionId
					--AND A.StateProv = HIOState.StateAbbreviation
					AND ISNULL(A.Deleted,0)<>1
			)A
		WHERE A.Record_Rank = 1
		)B
		ON 
		B.SessionId = HIOState.SessionId
		AND B.StateProv = HIOState.StateAbbreviation		
		ORDER BY HIOState.WBHIOStateId --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskId ORDER BY LocationAssociationId) = 1
),
EXP_WB_HIO_State AS (
	SELECT
	LKP_WB_HIO_State.LocationAssociationId,
	RTR_InsuranceLine_WB_HIO_State.SessionId,
	LKP_WB_HIO_State.LineId,
	'N/A' AS IndividualRiskPremiumModification,
	RTR_InsuranceLine_WB_HIO_State.CoverageId,
	RTR_InsuranceLine_WB_HIO_State.CoverageType,
	-1 AS RiskId,
	'N/A' AS RiskType,
	0 AS Exposure,
	-1 AS CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	'N/A' AS OccupancyType
	FROM RTR_InsuranceLine_WB_HIO_State
	LEFT JOIN LKP_WB_HIO_State
	ON LKP_WB_HIO_State.RiskId = RTR_InsuranceLine.ParentCoverageObjectId9
),
LKP_WB_IM_State AS (
	SELECT
	LocationAssociationId,
	LineId,
	IndividualRiskPremiumModification,
	WBIMStateId
	FROM (
		SELECT
			-- There can be multiple locations for a given risk and there is no direct linkage between the coverage and location, so the 
			-- correlated sub-query below grabs the location with the lowest location number (usually location 1 in most instances).
			 LocationAssociationId as LocationAssociationId,
			DCIML.LineId AS LineId,
			Null AS IndividualRiskPremiumModification,
			IMState.WBIMStateId AS WBIMStateId
		FROM WBIMStateStage IMState
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBIMLineStage WBIML
			ON IMState.WBIMLineId = WBIML.WB_IM_LineId
				AND IMState.SessionId = WBIML.SessionId
		LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCIMLineStage DCIML
			ON WBIML.IM_LineId = DCIML.IM_LineId
				AND WBIML.SessionId = DCIML.SessionId
		left HASH join 
		(
		select SessionId,StateProv,LocationAssociationId from (
				SELECT  a.SessionId,A.StateProv,B.LocationAssociationId,
				Rank() over(partition by a.SessionId,A.StateProv ORDER BY isnull(cast(c.LocationNumber as int), 999),B.LocationAssociationId) AS Record_Rank
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging A
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging B
					ON A.LocationId = B.LocationId
						AND A.SessionId = B.SessionId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationStaging C
					ON A.LocationId = C.LocationId
						AND A.SessionId = C.SessionId
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCIMLocationStage D
					ON B.ObjectId = D.IMLocationId
						AND B.SessionId = D.SessionId
				WHERE B.LocationAssociationType = 'IM_Location'
				--	AND A.SessionId = IMState.SessionId
				--	AND A.StateProv = IMState.StateAbbreviation
					and ISNULL(A.Deleted,0)<>1
						) a where a.Record_Rank=1)b
						on b.sessionid=IMState.SessionId
						and b.StateProv=IMState.StateAbbreviation
		ORDER BY IMState.WBIMStateId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WBIMStateId ORDER BY LocationAssociationId) = 1
),
EXP_WB_IM_State AS (
	SELECT
	LKP_WB_IM_State.LocationAssociationId,
	RTR_InsuranceLine_WB_IM_State.SessionId,
	LKP_WB_IM_State.LineId,
	LKP_WB_IM_State.IndividualRiskPremiumModification,
	RTR_InsuranceLine_WB_IM_State.CoverageId,
	RTR_InsuranceLine_WB_IM_State.CoverageType,
	NULL AS CoverageForm,
	-1 AS RiskId,
	'N/A' AS RiskType,
	0 AS Exposure,
	-1 AS CommissionPercentage,
	'N/A' AS CoverageVersion,
	'N/A' AS SpecialClassLevel1,
	'000' AS BuildingNumber,
	'' AS ILFTableAssignmentCode,
	'N/A' AS OccupancyType
	FROM RTR_InsuranceLine_WB_IM_State
	LEFT JOIN LKP_WB_IM_State
	ON LKP_WB_IM_State.WBIMStateId = RTR_InsuranceLine.ParentCoverageObjectId21
),
Union_InsuranceLines AS (
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, out_CoverageType AS CoverageType, CoverageForm, RiskId, o_RiskType AS RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, RetroactiveDate, ExperienceModEffectiveDate, o_ConstructionCode AS ConstructionCode, RateEffectiveDate, RatingPlan, o_ProtectionClass AS ProtectionClass, o_ExposureBasis AS ExposureBasis, SubCoverageType, LineType, lkp_LineId_DCLine, lkp_SessionId_DCLine
	FROM EXP_DC_Line
	-- Manually join with RTR_InsuranceLine_DC_Line
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, CoverageForm, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, RetroactiveDate, o_ExposureBasis AS ExposureBasis, SubCoverageType, LineType
	FROM EXP_DC_GL_Risk
	-- Manually join with RTR_InsuranceLine_DC_GL_Risk
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, CoverageForm, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, ExperienceModEffectiveDate, RateEffectiveDate, RatingPlan, o_ExposureBasis AS ExposureBasis, SubCoverageType
	FROM EXP_DC_WC_Risk
	-- Manually join with RTR_InsuranceLine_DC_WC_Risk
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, CoverageForm, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, SubCoverageType
	FROM EXP_DC_CR_RiskCrime
	-- Manually join with RTR_InsuranceLine_DC_CR_RiskCrime
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, SubCoverageType
	FROM EXP_DC_IM_CoverageForm
	-- Manually join with RTR_InsuranceLine_DC_IM_CoverageForm
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, CoverageForm, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, SubCoverageType
	FROM EXP_DC_IM_Risk
	-- Manually join with RTR_InsuranceLine_DC_IM_Risk
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, CoverageForm, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, ExperienceModEffectiveDate, RateEffectiveDate, RatingPlan, SubCoverageType
	FROM EXP_DC_WC_StateTerm
	-- Manually join with RTR_InsuranceLine_DC_WC_StateTerm
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, SubCoverageType
	FROM EXP_WB_HIO_Risk
	-- Manually join with RTR_InsuranceLine_WB_HIO_Risk
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, SubCoverageType
	FROM EXP_WB_HIO_State
	-- Manually join with RTR_InsuranceLine_WB_HIO_State
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, SubCoverageType
	FROM EXP_WB_GOC_Risk
	-- Manually join with RTR_InsuranceLine_WB_GOC_Risk
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, o_CoverageType AS CoverageType, RiskId, RiskType, o_Exposure AS Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, YearBuilt, ConstructionCode, ProtectionClassOverride AS ProtectionClass, BOPNewLiabExpBase AS ExposureBasis, o_Sprinkler AS Sprinkler, PredominantPersonalPropertyRateNumber, PredominantLiabilityLiabClassGroup, o_ISOOccupancyType AS ISOOccupancyType, OccupancyClassDescription, ActiveBuildingFlag, SubCoverageType, PredominantBuildingBCCCode, PredominantBuildingClassCodeDescription
	FROM EXP_DC_BP_Risk
	-- Manually join with RTR_InsuranceLine_DC_BP_Risk
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, RiskId, RiskType, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, ConstructionCode, ProtectionClassOverride AS ProtectionClass, PredominantLiabilityLiabExpBase AS ExposureBasis, OccupancyClassDescription, ActiveBuildingFlag, SubCoverageType
	FROM EXP_DC_BP_Location
	-- Manually join with RTR_InsuranceLine_DC_BP_Location
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, CoverageForm, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, SubCoverageType
	FROM EXP_DC_CR_Endorsement
	-- Manually join with RTR_InsuranceLine_DC_CR_Endorsement
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, CoverageForm, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, o_SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, PolicyCoverage, CauseOfLoss, ILFTableAssignmentCode, OccupancyType, OrginalPackageModifier, YearBuilt, ConstructionCode, ProtectionClass, MultipleLocationCreditFactor, PreferredPropertyCreditFactor, PerilGroup, WindCoverageIndicator, OccupancyClassDescription, ActiveBuildingFlag, SubCoverageType, RateType, PropertyType, OccupancyCategory AS OccupanyCategory
	FROM EXP_DC_CF_Risk
	-- Manually join with RTR_InsuranceLine_DC_CF_Risk
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, CoverageForm, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, VehicleNumber, SubCoverageType
	FROM EXP_DC_CA_State
	-- Manually join with RTR_InsuranceLine_DC_CA_State
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, CoverageForm, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, VehicleNumber, FullCoverageGlass, SubCoverageType
	FROM EXP_DC_CA_Risk
	-- Manually join with RTR_InsuranceLine_DC_CA_Risk
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, CoverageForm, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, SubCoverageType
	FROM EXP_WB_CU_PremiumDetail
	-- Manually join with RTR_InsuranceLine_WB_CU_PremiumDetail
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, CoverageForm, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, SubCoverageType
	FROM EXP_DC_CU_UmbrellaEmployersLiability
	-- Manually join with RTR_InsuranceLine_DC_CU_UmbrellaEmployersLiability
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, SubCoverageType
	FROM EXP_WB_GOC_State
	-- Manually join with RTR_InsuranceLine_WB_GOC_State
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, CoverageForm, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, SubCoverageType
	FROM EXP_WB_IM_State
	-- Manually join with RTR_InsuranceLine_WB_IM_State
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModificationFactor AS IndividualRiskPremiumModification, CoverageId, CoverageRiskType AS CoverageType, CoverageForm, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, SubCoverageType
	FROM EXP_DC_CR_Risk
	-- Manually join with RTR_InsuranceLine_DC_CR_Risk
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, CoverageForm, RiskID AS RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, SubCoverageType, LineType
	FROM EXP_DC_CA_BusinessInterruption
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType
	FROM EXP_WB_EC_State
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType
	FROM EXP_WB_EC_Risk
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, RiskId, RiskType, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, ConstructionCode, ProtectionClassOverride AS ProtectionClass, PredominantLiabilityLiabExpBase AS ExposureBasis, OccupancyType AS ISOOccupancyType, OccupancyClassDescription, ActiveBuildingFlag, SubCoverageType26 AS SubCoverageType
	FROM EXP_DC_Location
	UNION
	SELECT LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageType, CoverageForm, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1 AS SpecialClassLevel, BuildingNumber, ILFTableAssignmentCode, OccupancyType, ExperienceModEffectiveDate, RateEffectiveDate, RatingPlan
	FROM EXP_DC_WC_State
),
EXP_SetWBLocationAccountInputs AS (
	SELECT
	LocationAssociationId,
	CoverageId,
	SessionId AS i_SessionId,
	LineId AS i_LineId,
	lkp_LineId_DCLine,
	lkp_SessionId_DCLine,
	-- *INF*: IIF(NOT ISNULL(lkp_SessionId_DCLine), lkp_SessionId_DCLine,i_SessionId)
	-- 
	-- -- DC SessionId is the only one that writes to lkp_Sessionid_id
	-- 
	-- 
	IFF(lkp_SessionId_DCLine IS NOT NULL, lkp_SessionId_DCLine, i_SessionId) AS o_SessionId,
	-- *INF*: IIF(NOT ISNULL(lkp_LineId_DCLine), lkp_LineId_DCLine,i_LineId)
	-- 
	-- -- DC LIne is the only one that writes to lkp_Line_id
	IFF(lkp_LineId_DCLine IS NOT NULL, lkp_LineId_DCLine, i_LineId) AS o_LineId
	FROM Union_InsuranceLines
),
LKP_WBLocationAccount_And_Territory AS (
	SELECT
	Latitude,
	Longitude,
	CBG,
	TerritoryCode,
	TerritoryCodeEarthQuake,
	CoverageType,
	CoverageId,
	LocationAssociationId,
	SessionId,
	LineId
	FROM (
		SELECT Latitude as Latitude, Longitude as Longitude, CBG as CBG, TerritoryCode as TerritoryCode, 
		TerritoryCodeEarthQuake  as TerritoryCodeEarthQuake, CoverageType as CoverageType, 
		LocationAssociationId  as LocationAssociationId, SessionId as SessionId, 
		LineId as LineId, CoverageId as CoverageId
		FROM 
		(
		SELECT  isnull(ltrim(rtrim(A.Latitude)),'0') AS Latitude,  isnull(ltrim(rtrim(A.Longitude)),'0') AS Longitude,  
		isnull(ltrim(rtrim(A.CBG)),'N/A') AS CBG,  Case 
		when C.LineType in ('Property') then isnull(ltrim(rtrim(A.TerritoryCodeProperty)),'N/A')
		when C.LineType in ('SBOPProperty') then isnull(ltrim(rtrim(A.TerritoryCodeProperty)),'N/A')
		else 'N/A'
		end AS TerritoryCode,  isnull(ltrim(rtrim(A.TerritoryCodeEarthQuake)),'N/A') AS TerritoryCodeEarthQuake, 
		 D.CoverageType AS CoverageType,  B.LocationAssociationId AS LocationAssociationId,  A.SessionId AS SessionId,  
		 C.LineId AS LineId,  D.CoverageId AS CoverageId 
		 FROM dbo.WBLocationAccountStage A with (nolock)
		INNER JOIN dbo.DCLocationAssociationStaging B  with (nolock) on 
		B.LocationId=A.LocationId and B.SessionId=A.SessionId and B.LocationAssociationType ='Location' 
		INNER JOIN dbo.WorkDCTInsuranceLine C  with (nolock) on A.SessionId=C.SessionId  and linetype not in ('BusinessOwners','SBOPGeneralLiability',
		'GeneralLiability','CommercialAuto','Property','SBOPProperty')
		INNER JOIN dbo.WorkDCTCoverageTransaction D with (nolock) on 	D.SessionId=A.SessionId
		INNER JOIN dbo.WBEDWIncrementalDataQualitySessions EDQS with (nolock) on EDQS.sessionid = A.SessionId and  indicator = 1 
		and autoshred = 0
		
		UNION
		
		SELECT  isnull(ltrim(rtrim(A.Latitude)),'0') AS Latitude,  isnull(ltrim(rtrim(A.Longitude)),'0') AS Longitude,  
		isnull(ltrim(rtrim(A.CBG)),'N/A') AS CBG,  Case 
		when C.LineType in ('Property') then isnull(ltrim(rtrim(A.TerritoryCodeProperty)),'N/A')
		when C.LineType in ('SBOPProperty') then isnull(ltrim(rtrim(A.TerritoryCodeProperty)),'N/A')
		else 'N/A'
		end AS TerritoryCode,  isnull(ltrim(rtrim(A.TerritoryCodeEarthQuake)),'N/A') AS TerritoryCodeEarthQuake, 
		 D.CoverageType AS CoverageType,  B.LocationAssociationId AS LocationAssociationId,  A.SessionId AS SessionId,  
		 C.LineId AS LineId,  D.CoverageId AS CoverageId 
		 FROM dbo.WBLocationAccountStage A with (nolock)
		INNER JOIN dbo.DCLocationAssociationStaging B  with (nolock) on 
		B.LocationId=A.LocationId and B.SessionId=A.SessionId and B.LocationAssociationType ='CF_Location'
		INNER JOIN dbo.WorkDCTInsuranceLine C  with (nolock) on A.SessionId=C.SessionId  and linetype  in ('Property','SBOPProperty')
		INNER JOIN dbo.WorkDCTCoverageTransaction D with (nolock) on 	D.SessionId=A.SessionId
		INNER JOIN dbo.WBEDWIncrementalDataQualitySessions EDQS with (nolock) on EDQS.sessionid = A.SessionId and  indicator = 1 and autoshred = 0
		
		UNION
		
		SELECT  isnull(ltrim(rtrim(A.Latitude)),'0') AS Latitude,  isnull(ltrim(rtrim(A.Longitude)),'0') AS Longitude, 
		isnull(ltrim(rtrim(A.CBG)),'N/A') AS CBG,  Case 
		when C.linetype in ('BusinessOwners') then isnull(ltrim(rtrim(BP.Territory)),'N/A')
		else 'N/A'
		end AS TerritoryCode,  isnull(ltrim(rtrim(A.TerritoryCodeEarthQuake)),'N/A') AS TerritoryCodeEarthQuake,  
		D.CoverageType AS CoverageType,  B.LocationAssociationId AS LocationAssociationId,  A.SessionId AS SessionId,  
		C.LineId AS LineId,  D.CoverageId AS CoverageId 
		FROM dbo.WBLocationAccountStage A with (nolock)
		INNER JOIN dbo.DCLocationAssociationStaging B  with (nolock) on 
		B.LocationId=A.LocationId and B.SessionId=A.SessionId and B.LocationAssociationType = 'BP_Location' 
		INNER JOIN dbo.WorkDCTInsuranceLine C  with (nolock) on A.SessionId=C.SessionId and linetype = 'BusinessOwners'
		INNER JOIN dbo.WorkDCTCoverageTransaction D with (nolock) on D.SessionId=A.SessionId
		INNER JOIN dbo.WBEDWIncrementalDataQualitySessions EDQS with (nolock) on EDQS.sessionid = A.SessionId and  indicator = 1 and autoshred = 0
		LEFT JOIN DCBPLocationStage BP  with (nolock) on A.SessionId=BP.SessionId and B.objectid=BPLocationId and B.ObjectName='DC_BP_Location'
		
		UNION
		
		 SELECT  isnull(ltrim(rtrim(A.Latitude)),'0') AS Latitude,  isnull(ltrim(rtrim(A.Longitude)),'0') AS Longitude,  
		 isnull(ltrim(rtrim(A.CBG)),'N/A') AS CBG,  Case 
		when C.LineType in ('CommercialAuto') then isnull(convert(varchar(3),CA.Territory),'N/A')
		else 'N/A'
		end AS TerritoryCode,  isnull(ltrim(rtrim(A.TerritoryCodeEarthQuake)),'N/A') AS TerritoryCodeEarthQuake,  
		D.CoverageType AS CoverageType,  B.LocationAssociationId AS LocationAssociationId,  A.SessionId AS SessionId,  C.LineId AS LineId,  
		D.CoverageId AS CoverageId 
		FROM dbo.WBLocationAccountStage A with (nolock)
		INNER JOIN dbo.DCLocationAssociationStaging B  with (nolock) on 
		B.LocationId=A.LocationId and B.SessionId=A.SessionId and B.LocationAssociationType = 'CA_Location'
		INNER JOIN dbo.WorkDCTInsuranceLine C  with (nolock) on A.SessionId=C.SessionId and linetype = 'CommercialAuto'
		INNER JOIN dbo.WorkDCTCoverageTransaction D with (nolock) on 	D.SessionId=A.SessionId
		INNER JOIN dbo.WBEDWIncrementalDataQualitySessions EDQS with (nolock) on EDQS.sessionid = A.SessionId and  indicator = 1 and autoshred = 0
		LEFT JOIN DCCALocationStaging CA with (nolock) on A.sessionid=CA.sessionid and  B.objectid=CA_LocationId and B.ObjectName='DC_CA_Location' 
		
		UNION
		
		SELECT  isnull(ltrim(rtrim(A.Latitude)),'0') AS Latitude,  isnull(ltrim(rtrim(A.Longitude)),'0') AS Longitude,  
		isnull(ltrim(rtrim(A.CBG)),'N/A') AS CBG,  
		Case 
		when C.LineType in ('GeneralLiability') then isnull(ltrim(rtrim(GL.Territory)),'N/A')
		when C.LineType in ('SBOPGeneralLiability') then isnull(ltrim(rtrim(GL.Territory)),'N/A')
		else 'N/A'
		end AS TerritoryCode,  isnull(ltrim(rtrim(A.TerritoryCodeEarthQuake)),'N/A') AS TerritoryCodeEarthQuake,  
		D.CoverageType AS CoverageType,  B.LocationAssociationId AS LocationAssociationId,  A.SessionId AS SessionId,  
		C.LineId AS LineId,  D.CoverageId AS CoverageId 
		FROM dbo.WBLocationAccountStage A with (nolock)
		INNER JOIN dbo.DCLocationAssociationStaging B  with (nolock) on 
		B.LocationId=A.LocationId and B.SessionId=A.SessionId and B.LocationAssociationType = 'GL_Location'
		INNER JOIN dbo.WorkDCTInsuranceLine C  with (nolock) on A.SessionId=C.SessionId and linetype in ('GeneralLiability','SBOPGeneralLiability')
		INNER JOIN dbo.WorkDCTCoverageTransaction D with (nolock) on D.SessionId=A.SessionId
		INNER JOIN dbo.WBEDWIncrementalDataQualitySessions EDQS with (nolock) on EDQS.sessionid = A.SessionId and  indicator = 1 and autoshred = 0
		LEFT JOIN DCGLLocationStaging GL with (nolock) on A.SessionId=GL.SessionId and B.objectid=GL_LocationId and B.ObjectName='DC_GL_Location'
		
		) A
		@{pipeline().parameters.WHERE_CLAUSE_LKP}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LocationAssociationId,SessionId,LineId,CoverageId ORDER BY Latitude) = 1
),
EXP_Target AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	Union_InsuranceLines.LocationAssociationId,
	Union_InsuranceLines.SessionId,
	Union_InsuranceLines.LineId,
	Union_InsuranceLines.IndividualRiskPremiumModification,
	Union_InsuranceLines.CoverageId,
	Union_InsuranceLines.CoverageType AS CoverageRiskType,
	Union_InsuranceLines.CoverageForm AS i_CoverageForm,
	-- *INF*: IIF(NOT ISNULL(i_CoverageForm), i_CoverageForm, 'N/A')
	IFF(i_CoverageForm IS NOT NULL, i_CoverageForm, 'N/A') AS CoverageForm,
	Union_InsuranceLines.RiskId,
	Union_InsuranceLines.RiskType,
	Union_InsuranceLines.Exposure,
	Union_InsuranceLines.CommissionPercentage,
	Union_InsuranceLines.CoverageVersion,
	Union_InsuranceLines.SpecialClassLevel AS SpecialClassLevel1,
	Union_InsuranceLines.BuildingNumber,
	Union_InsuranceLines.PolicyCoverage,
	Union_InsuranceLines.CauseOfLoss,
	Union_InsuranceLines.ILFTableAssignmentCode,
	Union_InsuranceLines.OccupancyType,
	Union_InsuranceLines.LineType,
	Union_InsuranceLines.RetroactiveDate AS i_RetroactiveDate,
	-- *INF*: :LKP.LKP_RETROACTIVEDATE(CoverageId)
	LKP_RETROACTIVEDATE_CoverageId.RetroactiveDate AS v_RetroactiveDate,
	-- *INF*: DECODE(TRUE,
	-- LineType='SBOPGeneralLiability',IIF( NOT ISNULL(i_RetroactiveDate), i_RetroactiveDate,v_RetroactiveDate),
	-- IIF( NOT ISNULL(v_RetroactiveDate), v_RetroactiveDate, i_RetroactiveDate)
	-- )
	DECODE(
	    TRUE,
	    LineType = 'SBOPGeneralLiability', IFF(
	        i_RetroactiveDate IS NOT NULL, i_RetroactiveDate, v_RetroactiveDate
	    ),
	    IFF(
	        v_RetroactiveDate IS NOT NULL, v_RetroactiveDate, i_RetroactiveDate
	    )
	) AS o_RetroactiveDate,
	Union_InsuranceLines.OrginalPackageModifier,
	Union_InsuranceLines.YearBuilt,
	Union_InsuranceLines.ExperienceModEffectiveDate,
	Union_InsuranceLines.ConstructionCode,
	Union_InsuranceLines.RateEffectiveDate,
	Union_InsuranceLines.RatingPlan,
	Union_InsuranceLines.ProtectionClass AS i_ProtectionClass,
	-- *INF*: SUBSTR(i_ProtectionClass,1,5)
	SUBSTR(i_ProtectionClass, 1, 5) AS o_ProtectionClass,
	Union_InsuranceLines.MultipleLocationCreditFactor,
	Union_InsuranceLines.PreferredPropertyCreditFactor,
	Union_InsuranceLines.PerilGroup,
	Union_InsuranceLines.WindCoverageIndicator,
	Union_InsuranceLines.VehicleNumber,
	Union_InsuranceLines.ExposureBasis,
	Union_InsuranceLines.Sprinkler,
	Union_InsuranceLines.PredominantPersonalPropertyRateNumber,
	Union_InsuranceLines.PredominantLiabilityLiabClassGroup,
	Union_InsuranceLines.FullCoverageGlass,
	Union_InsuranceLines.ISOOccupancyType,
	Union_InsuranceLines.OccupancyClassDescription,
	Union_InsuranceLines.ActiveBuildingFlag,
	Union_InsuranceLines.SubCoverageType,
	Union_InsuranceLines.RateType,
	Union_InsuranceLines.PropertyType,
	Union_InsuranceLines.OccupanyCategory,
	LKP_WBLocationAccount_And_Territory.Latitude AS i_Latitude,
	LKP_WBLocationAccount_And_Territory.Longitude AS i_Longitude,
	LKP_WBLocationAccount_And_Territory.CBG AS i_CBG,
	LKP_WBLocationAccount_And_Territory.TerritoryCode AS i_TerritoryCode,
	LKP_WBLocationAccount_And_Territory.TerritoryCodeEarthQuake AS i_TerritoryCodeEarthQuake,
	LKP_WBLocationAccount_And_Territory.CoverageType AS i_CoverageType,
	-- *INF*: REG_MATCH(UPPER(i_CoverageType),'.*EARTHQUAKE.*')
	-- --REG_MATCH(UPPER(CoverageRiskType),'.*EARTHQUAKE.*')
	REGEXP_LIKE(UPPER(i_CoverageType), '.*EARTHQUAKE.*') AS v_EarthquakeFlag,
	-- *INF*: DECODE(TRUE,
	-- v_EarthquakeFlag = 1 and NOT ISNULL( i_TerritoryCodeEarthQuake), i_TerritoryCodeEarthQuake,
	-- v_EarthquakeFlag = 0 and NOT ISNULL(i_TerritoryCode),i_TerritoryCode,
	-- 'N/A'
	-- )
	-- 
	-- -- output rules regarding territory lookup can be combined here
	DECODE(
	    TRUE,
	    v_EarthquakeFlag = 1 and i_TerritoryCodeEarthQuake IS NOT NULL, i_TerritoryCodeEarthQuake,
	    v_EarthquakeFlag = 0 and i_TerritoryCode IS NOT NULL, i_TerritoryCode,
	    'N/A'
	) AS v_TerritoryCode,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_CBG),'N/A',
	-- length(i_CBG) < 10, 'N/A',
	-- i_CBG)
	-- 
	-- -- Only accept CBG values that contain all 10 of the characters
	DECODE(
	    TRUE,
	    i_CBG IS NULL, 'N/A',
	    length(i_CBG) < 10, 'N/A',
	    i_CBG
	) AS o_CBG,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(i_Latitude) and IS_NUMBER(i_Latitude), TO_DECIMAL(i_Latitude,6),
	-- 0
	-- )
	DECODE(
	    TRUE,
	    i_Latitude IS NULL and REGEXP_LIKE(i_Latitude, '^[0-9]NOT +$'), CAST(i_Latitude AS FLOAT),
	    0
	) AS o_Latitude,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(i_Longitude) and IS_NUMBER(i_Longitude), TO_DECIMAL(i_Longitude,6),
	-- 0
	-- )
	DECODE(
	    TRUE,
	    i_Longitude IS NULL and REGEXP_LIKE(i_Longitude, '^[0-9]NOT +$'), CAST(i_Longitude AS FLOAT),
	    0
	) AS o_Longitude,
	v_TerritoryCode AS o_TerritoryCode,
	Union_InsuranceLines.PredominantBuildingBCCCode,
	Union_InsuranceLines.PredominantBuildingClassCodeDescription
	FROM Union_InsuranceLines
	LEFT JOIN LKP_WBLocationAccount_And_Territory
	ON LKP_WBLocationAccount_And_Territory.LocationAssociationId = EXP_SetWBLocationAccountInputs.LocationAssociationId AND LKP_WBLocationAccount_And_Territory.SessionId = EXP_SetWBLocationAccountInputs.o_SessionId AND LKP_WBLocationAccount_And_Territory.LineId = EXP_SetWBLocationAccountInputs.o_LineId AND LKP_WBLocationAccount_And_Territory.CoverageId = EXP_SetWBLocationAccountInputs.CoverageId
	LEFT JOIN LKP_RETROACTIVEDATE LKP_RETROACTIVEDATE_CoverageId
	ON LKP_RETROACTIVEDATE_CoverageId.CoverageId = CoverageId

),
LKP_WorkDCTInsuranceLine AS (
	SELECT
	LineType,
	LineId,
	i_LineId
	FROM (
		SELECT 
			LineType,
			LineId,
			i_LineId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkDCTInsuranceLine
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LineId ORDER BY LineType) = 1
),
EXP_ComissionPercentage AS (
	SELECT
	LKP_WorkDCTInsuranceLine.LineType,
	EXP_Target.CoverageId,
	EXP_Target.CoverageRiskType,
	EXP_Target.CommissionPercentage,
	-- *INF*: DECODE(TRUE,
	-- Substr(CoverageRiskType,1,5)= 'WB516'  
	-- OR 
	-- (LineType = 'BusinessOwners'
	-- AND CoverageRiskType = 'EmploymentPracticesLiability' ),
	-- IIF(ISNULL(:LKP.LKP_WBGLCOVERAGEWB516GL(CoverageId)),:LKP.LKP_WBBPCOVERAGEEMPLOYMENTPRACTICESLIABILITYSTAGE(CoverageId), :LKP.LKP_WBGLCOVERAGEWB516GL(CoverageId)),
	-- CoverageRiskType = 'DataCompromise',
	-- IIF(ISNULL(:LKP.LKP_WBCFCOVERAGEDATACOMPROMISE(CoverageId)), :LKP.LKP_WBBPCOVERAGEDATACOMPROMISESTAGE(CoverageId), :LKP.LKP_WBCFCOVERAGEDATACOMPROMISE(CoverageId)),
	-- CoverageRiskType = 'EquipBreakdown' or CoverageRiskType = 'EquipmentBreakdown',
	-- IIF(ISNULL(:LKP.LKP_WBCFCOVERAGEEQUIPMENTBREAKDOWN(CoverageId)), :LKP.LKP_WBBPCOVERAGEEQUIPBREAKDOWNSTAGE(CoverageId), :LKP.LKP_WBCFCOVERAGEEQUIPMENTBREAKDOWN(CoverageId)),
	-- CoverageRiskType = 'WB2525',:LKP.LKP_WBGLCOVERAGEWB2525(CoverageId),
	-- CommissionPercentage)
	DECODE(
	    TRUE,
	    Substr(CoverageRiskType, 1, 5) = 'WB516' OR (LineType = 'BusinessOwners' AND CoverageRiskType = 'EmploymentPracticesLiability'), IFF(
	        LKP_WBGLCOVERAGEWB516GL_CoverageId.TransactionFinalCommissionValue IS NULL,
	        LKP_WBBPCOVERAGEEMPLOYMENTPRACTICESLIABILITYSTAGE_CoverageId.TransactionFinalCommissionValue,
	        LKP_WBGLCOVERAGEWB516GL_CoverageId.TransactionFinalCommissionValue
	    ),
	    CoverageRiskType = 'DataCompromise', IFF(
	        LKP_WBCFCOVERAGEDATACOMPROMISE_CoverageId.TransactionFinalCommissionValue IS NULL,
	        LKP_WBBPCOVERAGEDATACOMPROMISESTAGE_CoverageId.TransactionFinalCommissionValue,
	        LKP_WBCFCOVERAGEDATACOMPROMISE_CoverageId.TransactionFinalCommissionValue
	    ),
	    CoverageRiskType = 'EquipBreakdown' or CoverageRiskType = 'EquipmentBreakdown', IFF(
	        LKP_WBCFCOVERAGEEQUIPMENTBREAKDOWN_CoverageId.TransactionFinalCommissionValue IS NULL,
	        LKP_WBBPCOVERAGEEQUIPBREAKDOWNSTAGE_CoverageId.TransactionFinalCommissionValue,
	        LKP_WBCFCOVERAGEEQUIPMENTBREAKDOWN_CoverageId.TransactionFinalCommissionValue
	    ),
	    CoverageRiskType = 'WB2525', LKP_WBGLCOVERAGEWB2525_CoverageId.TransactionFinalCommissionValue,
	    CommissionPercentage
	) AS v_CommissionPercentage,
	v_CommissionPercentage AS o_CommissionPercentage
	FROM EXP_Target
	LEFT JOIN LKP_WorkDCTInsuranceLine
	ON LKP_WorkDCTInsuranceLine.LineId = EXP_Target.LineId
	LEFT JOIN LKP_WBGLCOVERAGEWB516GL LKP_WBGLCOVERAGEWB516GL_CoverageId
	ON LKP_WBGLCOVERAGEWB516GL_CoverageId.CoverageId = CoverageId

	LEFT JOIN LKP_WBBPCOVERAGEEMPLOYMENTPRACTICESLIABILITYSTAGE LKP_WBBPCOVERAGEEMPLOYMENTPRACTICESLIABILITYSTAGE_CoverageId
	ON LKP_WBBPCOVERAGEEMPLOYMENTPRACTICESLIABILITYSTAGE_CoverageId.CoverageId = CoverageId

	LEFT JOIN LKP_WBCFCOVERAGEDATACOMPROMISE LKP_WBCFCOVERAGEDATACOMPROMISE_CoverageId
	ON LKP_WBCFCOVERAGEDATACOMPROMISE_CoverageId.CoverageId = CoverageId

	LEFT JOIN LKP_WBBPCOVERAGEDATACOMPROMISESTAGE LKP_WBBPCOVERAGEDATACOMPROMISESTAGE_CoverageId
	ON LKP_WBBPCOVERAGEDATACOMPROMISESTAGE_CoverageId.CoverageId = CoverageId

	LEFT JOIN LKP_WBCFCOVERAGEEQUIPMENTBREAKDOWN LKP_WBCFCOVERAGEEQUIPMENTBREAKDOWN_CoverageId
	ON LKP_WBCFCOVERAGEEQUIPMENTBREAKDOWN_CoverageId.CoverageId = CoverageId

	LEFT JOIN LKP_WBBPCOVERAGEEQUIPBREAKDOWNSTAGE LKP_WBBPCOVERAGEEQUIPBREAKDOWNSTAGE_CoverageId
	ON LKP_WBBPCOVERAGEEQUIPBREAKDOWNSTAGE_CoverageId.CoverageId = CoverageId

	LEFT JOIN LKP_WBGLCOVERAGEWB2525 LKP_WBGLCOVERAGEWB2525_CoverageId
	ON LKP_WBGLCOVERAGEWB2525_CoverageId.CoverageId = CoverageId

),
LKP_DCLocationAssocation_GetAccountLocation AS (
	SELECT
	LocationAssociationId,
	SessionId
	FROM (
		SELECT 
			LocationAssociationId,
			SessionId
		FROM DCLocationAssociationStaging
		WHERE LocationAssociationType='Account'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionId ORDER BY LocationAssociationId) = 1
),
EXP_PreInsert AS (
	SELECT
	EXP_Target.o_ExtractDate AS ExtractDate,
	EXP_Target.o_SourceSystemId AS SourceSystemId,
	EXP_Target.LocationAssociationId,
	LKP_DCLocationAssocation_GetAccountLocation.LocationAssociationId AS LKP_Account_LocationAssociationId,
	-- *INF*: IIF(ISNULL(LocationAssociationId),LKP_Account_LocationAssociationId,LocationAssociationId)
	IFF(LocationAssociationId IS NULL, LKP_Account_LocationAssociationId, LocationAssociationId) AS O_LocationAssociationid,
	EXP_Target.SessionId,
	EXP_Target.LineId,
	EXP_Target.IndividualRiskPremiumModification,
	EXP_Target.CoverageId,
	EXP_Target.CoverageRiskType,
	EXP_Target.CoverageForm,
	EXP_Target.RiskId,
	EXP_Target.RiskType,
	EXP_Target.Exposure,
	EXP_ComissionPercentage.o_CommissionPercentage AS CommissionPercentage,
	EXP_Target.CoverageVersion,
	EXP_Target.SpecialClassLevel1,
	EXP_Target.BuildingNumber,
	EXP_Target.PolicyCoverage,
	EXP_Target.CauseOfLoss,
	EXP_Target.ILFTableAssignmentCode,
	EXP_Target.OccupancyType,
	-- *INF*: IIF(Substr(CoverageRiskType,1,6) = 'NS0453',(:LKP.LKP_WBGLCOVNS0453STG (CoverageId)),RetroactiveDate)
	IFF(
	    Substr(CoverageRiskType, 1, 6) = 'NS0453',
	    (LKP_WBGLCOVNS0453STG_CoverageId.RadonRetroactiveDate),
	    RetroactiveDate
	) AS v_RetroactiveDate,
	-- *INF*: IIF (isNull(v_RetroactiveDate),TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),v_RetroactiveDate)
	IFF(
	    v_RetroactiveDate IS NULL, TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
	    v_RetroactiveDate
	) AS o_RetroactiveDate,
	EXP_Target.o_RetroactiveDate AS RetroactiveDate,
	EXP_Target.OrginalPackageModifier,
	EXP_Target.YearBuilt,
	EXP_Target.ExperienceModEffectiveDate,
	EXP_Target.ConstructionCode,
	EXP_Target.RateEffectiveDate,
	EXP_Target.RatingPlan,
	EXP_Target.o_ProtectionClass AS ProtectionClass,
	EXP_Target.MultipleLocationCreditFactor,
	EXP_Target.PreferredPropertyCreditFactor,
	EXP_Target.PerilGroup,
	EXP_Target.WindCoverageIndicator,
	EXP_Target.VehicleNumber,
	EXP_Target.PredominantPersonalPropertyRateNumber,
	EXP_Target.PredominantLiabilityLiabClassGroup,
	EXP_Target.ExposureBasis,
	EXP_Target.Sprinkler AS SprinkerFlag,
	EXP_Target.FullCoverageGlass,
	-- *INF*: Decode(True, isnull(FullCoverageGlass)=1,0,FullCoverageGlass='F',0,1)
	Decode(
	    True,
	    FullCoverageGlass IS NULL = 1, 0,
	    FullCoverageGlass = 'F', 0,
	    1
	) AS O_FullCoverageGlass,
	EXP_Target.ISOOccupancyType,
	EXP_Target.OccupancyClassDescription,
	EXP_Target.ActiveBuildingFlag,
	EXP_Target.RateType,
	EXP_Target.PropertyType,
	EXP_Target.OccupanyCategory,
	EXP_Target.o_CBG AS CBG,
	EXP_Target.o_Latitude AS Latitude,
	EXP_Target.o_Longitude AS Longitude,
	EXP_Target.o_TerritoryCode AS TerritoryCode,
	EXP_Target.PredominantBuildingBCCCode,
	EXP_Target.PredominantBuildingClassCodeDescription
	FROM EXP_ComissionPercentage
	 -- Manually join with EXP_Target
	LEFT JOIN LKP_DCLocationAssocation_GetAccountLocation
	ON LKP_DCLocationAssocation_GetAccountLocation.SessionId = EXP_Target.SessionId
	LEFT JOIN LKP_WBGLCOVNS0453STG LKP_WBGLCOVNS0453STG_CoverageId
	ON LKP_WBGLCOVNS0453STG_CoverageId.CoverageId = CoverageId

),
WorkDCTTransactionInsuranceLineLocationBridge AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkDCTTransactionInsuranceLineLocationBridge;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkDCTTransactionInsuranceLineLocationBridge
	(ExtractDate, SourceSystemId, LocationAssociationId, SessionId, LineId, IndividualRiskPremiumModification, CoverageId, CoverageRiskType, CoverageForm, RiskId, RiskType, Exposure, CommissionPercentage, CoverageVersion, SpecialClassLevel1, BuildingNumber, PolicyCoverage, CauseOfLoss, ILFTableAssignmentCode, OccupancyType, RetroactiveDate, OrginalPackageModifier, YearBuilt, ExperienceModEffectiveDate, ConstructionCode, RateEffectiveDate, RatingPlan, ProtectionClass, MultipleLocationCreditFactor, PreferredPropertyCreditFactor, PerilGroup, WindCoverageIndicator, VehicleNumber, ExposureBasis, SprinkerFlag, PredominantPersonalPropertyRateNumber, PredominantLiabilityLiabClassGroup, FullCoverageGlass, ISOOccupancyType, OccupancyClassDescription, ActiveBuildingFlag, RateType, PropertyType, OccupancyCategory, CensusBlockGroup, Latitude, Longitude, RatingTerritoryCode, PredominantBuildingBCCCode, PredominantBuildingClassCodeDescription)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	O_LocationAssociationid AS LOCATIONASSOCIATIONID, 
	SESSIONID, 
	LINEID, 
	INDIVIDUALRISKPREMIUMMODIFICATION, 
	COVERAGEID, 
	COVERAGERISKTYPE, 
	COVERAGEFORM, 
	RISKID, 
	RISKTYPE, 
	EXPOSURE, 
	COMMISSIONPERCENTAGE, 
	COVERAGEVERSION, 
	SPECIALCLASSLEVEL1, 
	BUILDINGNUMBER, 
	POLICYCOVERAGE, 
	CAUSEOFLOSS, 
	ILFTABLEASSIGNMENTCODE, 
	OCCUPANCYTYPE, 
	o_RetroactiveDate AS RETROACTIVEDATE, 
	ORGINALPACKAGEMODIFIER, 
	YEARBUILT, 
	EXPERIENCEMODEFFECTIVEDATE, 
	CONSTRUCTIONCODE, 
	RATEEFFECTIVEDATE, 
	RATINGPLAN, 
	PROTECTIONCLASS, 
	MULTIPLELOCATIONCREDITFACTOR, 
	PREFERREDPROPERTYCREDITFACTOR, 
	PERILGROUP, 
	WINDCOVERAGEINDICATOR, 
	VEHICLENUMBER, 
	EXPOSUREBASIS, 
	SPRINKERFLAG, 
	PREDOMINANTPERSONALPROPERTYRATENUMBER, 
	PREDOMINANTLIABILITYLIABCLASSGROUP, 
	O_FullCoverageGlass AS FULLCOVERAGEGLASS, 
	ISOOCCUPANCYTYPE, 
	OCCUPANCYCLASSDESCRIPTION, 
	ACTIVEBUILDINGFLAG, 
	RATETYPE, 
	PROPERTYTYPE, 
	OccupanyCategory AS OCCUPANCYCATEGORY, 
	CBG AS CENSUSBLOCKGROUP, 
	LATITUDE, 
	LONGITUDE, 
	TerritoryCode AS RATINGTERRITORYCODE, 
	PREDOMINANTBUILDINGBCCCODE, 
	PREDOMINANTBUILDINGCLASSCODEDESCRIPTION
	FROM EXP_PreInsert
),
SQ_WorkDCTTransactionInsuranceLineLocationBridge_AllElse AS (
	with WorkCTE as (
	select   A.WorkDCTTransactionInsuranceLineLocationBridgeId,
	A.LineId, 
	A.LocationAssociationId, 
	A.SessionId, 
	A.CoverageRiskType,
	DCLAS.LocationId,
	A.RatingTerritoryCode,
	A.Latitude, 
	A.Longitude, 
	A.CensusBlockGroup
	from 
	DBO.WorkDCTTransactionInsuranceLineLocationBridge A
	INNER JOIN 
	dbo.WorkDCTInsuranceLine C  with (nolock) on 
		A.SessionId=C.SessionId and
		A.LineId=C.LineId and	
		C.LineType NOT IN ('BusinessOwners','SBOPGeneralLiability','GeneralLiability','CommercialAuto','Property','SBOPProperty')
	INNER JOIN dbo.DCLocationAssociationStaging DCLAS  with (nolock) on   
		DCLAS.LocationAssociationId=A.LocationAssociationId and
		DCLAS.SessionId=A.SessionId and
		DCLAS.LocationAssociationType ='Location'
	where 
	A.CensusBlockGroup='N/A'  and NOT A.LineId is null and NOT A.LocationAssociationId is null
	)
	
	select 
	A.WorkDCTTransactionInsuranceLineLocationBridgeId as WorkDCTTransactionInsuranceLineLocationBridgeId, 
	A.TerritoryCode as RatingTerritoryCode, 
	A.Latitude as Latitude, 
	A.Longitude as Longitude, 
	A.CBG  as CensusBlockGroup
	FROM 
	(
		Select 
		A.WorkDCTTransactionInsuranceLineLocationBridgeId,
		B.LocationAssociationId as LocationAssociationId, 
		A.SessionId as SessionId,
		B.LineId as LineId,
		ISNULL(WBLAS.Latitude,'0') as Latitude, 
		ISNULL(WBLAS.Longitude,'0') as Longitude, 
		ISNULL(WBLAS.CBG,'N/A') as CBG, 
		'N/A' as TerritoryCode,
		B.RatingTerritoryCode as cte_TerritoryCode,
		B.Latitude as cte_latitude,
		B.Longitude as cte_longitude,
		B.CensusBlockGroup as cte_CensusBlockGroup
		FROM
		WorkDCTTransactionInsuranceLineLocationBridge A  
		inner join WorkCTE B on 
			A.WorkDCTTransactionInsuranceLineLocationBridgeId=B.WorkDCTTransactionInsuranceLineLocationBridgeId	
			and A.LineId=B.LineId
			and A.LocationAssociationId=B.LocationAssociationId
			and A.SessionId=B.SessionId
		inner join WBLocationAccountStage WBLAS with (nolock) on 
			WBLAS.SessionId=B.SessionId and 	
			WBLAS.LocationId=B.LocationId
		inner join WorkDCTCoverageTransaction C with (nolock) on 
			C.SessionId=A.SessionId and 
			C.CoverageId=A.CoverageId
	) A
	WHERE
	(
		A.TerritoryCode != A.cte_TerritoryCode OR
		A.Latitude != A.cte_latitude OR
		A.Longitude != A.cte_longitude OR
		A.cte_CensusBlockGroup != A.CBG
	)
),
SQ_WorkDCTTransactionInsuranceLineLocationBridge_BP AS (
	with WorkCTE as (
	select   A.WorkDCTTransactionInsuranceLineLocationBridgeId,
	A.LineId, 
	A.LocationAssociationId, 
	A.SessionId, 
	A.CoverageRiskType,
	DCLAS.LocationId,
	A.RatingTerritoryCode,
	A.Latitude, 
	A.Longitude, 
	A.CensusBlockGroup,
	DCLAS.ObjectId
	from 
	DBO.WorkDCTTransactionInsuranceLineLocationBridge A
	INNER JOIN 
	dbo.WorkDCTInsuranceLine C  with (nolock) on 
		A.SessionId=C.SessionId and
		A.LineId=C.LineId and	
		C.LineType in('BusinessOwners')
	INNER JOIN dbo.DCLocationAssociationStaging DCLAS  with (nolock) on   
		DCLAS.LocationAssociationId=A.LocationAssociationId and
		DCLAS.SessionId=A.SessionId and
		DCLAS.ObjectName='DC_BP_Location'
	where 
	A.RatingTerritoryCode='N/A'  and NOT A.LineId is null and NOT A.LocationAssociationId is null
	)
	
	select 
	A.WorkDCTTransactionInsuranceLineLocationBridgeId as WorkDCTTransactionInsuranceLineLocationBridgeId, 
	A.TerritoryCode as RatingTerritoryCode, 
	A.Latitude as Latitude, 
	A.Longitude as Longitude, 
	A.CBG  as CensusBlockGroup
	FROM 
	(
		Select 
		A.WorkDCTTransactionInsuranceLineLocationBridgeId,
		B.LocationAssociationId as LocationAssociationId, 
		A.SessionId as SessionId,
		B.LineId as LineId,
		ISNULL(WBLAS.Latitude,'0') as Latitude, 
		ISNULL(WBLAS.Longitude,'0') as Longitude, 
		ISNULL(WBLAS.CBG,'N/A') as CBG, 
		CASE 
		WHEN UPPER(C.CoverageType) like '%EARTHQUAKE%' then isnull(WBLAS.TerritoryCodeEarthQuake,'N/A')
		ELSE ISNULL(BP.Territory,'N/A')
		END as TerritoryCode,
		B.RatingTerritoryCode as cte_TerritoryCode,
		B.Latitude as cte_latitude,
		B.Longitude as cte_longitude,
		B.CensusBlockGroup as cte_CensusBlockGroup
		FROM
		WorkDCTTransactionInsuranceLineLocationBridge A  
		inner join WorkCTE B on 
			A.WorkDCTTransactionInsuranceLineLocationBridgeId=B.WorkDCTTransactionInsuranceLineLocationBridgeId
			and A.LineId=B.LineId
			and A.LocationAssociationId=B.LocationAssociationId
			and A.SessionId=B.SessionId
		inner join WBLocationAccountStage WBLAS with (nolock) on 
			WBLAS.SessionId=B.SessionId and 	
			WBLAS.LocationId=B.LocationId
		inner join WorkDCTCoverageTransaction C with (nolock) on 
			C.SessionId=A.SessionId and 
			C.CoverageId=A.CoverageId
		INNER JOIN DCBPLocationStage BP  with (nolock) on 
			B.SessionId=BP.SessionId and 
			B.objectid=BP.BPLocationId 		
	) A
	WHERE
	(
		A.TerritoryCode != A.cte_TerritoryCode OR
		A.Latitude != A.cte_latitude OR
		A.Longitude != A.cte_longitude OR
		A.cte_CensusBlockGroup != A.CBG
	)
),
SQ_WorkDCTTransactionInsuranceLineLocationBridge_CA AS (
	with WorkCTE as (
	select   A.WorkDCTTransactionInsuranceLineLocationBridgeId,
	A.LineId, 
	A.LocationAssociationId, 
	A.SessionId, 
	A.CoverageRiskType,
	DCLAS.LocationId,
	A.RatingTerritoryCode,
	A.Latitude, 
	A.Longitude, 
	A.CensusBlockGroup,
	DCLAS.ObjectId
	from 
	DBO.WorkDCTTransactionInsuranceLineLocationBridge A
	INNER JOIN 
	dbo.WorkDCTInsuranceLine C  with (nolock) on 
		A.SessionId=C.SessionId and
		A.LineId=C.LineId and	
		C.LineType in('CommercialAuto')
	INNER JOIN dbo.DCLocationAssociationStaging DCLAS  with (nolock) on   
		DCLAS.LocationAssociationId=A.LocationAssociationId and
		DCLAS.SessionId=A.SessionId and
		DCLAS.ObjectName='DC_CA_Location'
	where 
	A.RatingTerritoryCode='N/A'  and NOT A.LineId is null and NOT A.LocationAssociationId is null
	)
	
	select 
	A.WorkDCTTransactionInsuranceLineLocationBridgeId as WorkDCTTransactionInsuranceLineLocationBridgeId, 
	A.TerritoryCode as RatingTerritoryCode, 
	A.Latitude as Latitude, 
	A.Longitude as Longitude, 
	A.CBG  as CensusBlockGroup
	FROM 
	(
		Select 
		A.WorkDCTTransactionInsuranceLineLocationBridgeId,
		B.LocationAssociationId as LocationAssociationId, 
		A.SessionId as SessionId,
		B.LineId as LineId,
		ISNULL(WBLAS.Latitude,'0') as Latitude, 
		ISNULL(WBLAS.Longitude,'0') as Longitude, 
		ISNULL(WBLAS.CBG,'N/A') as CBG, 
		CASE 
		WHEN UPPER(C.CoverageType) like '%EARTHQUAKE%' THEN ISNULL(WBLAS.TerritoryCodeEarthQuake,'N/A')
		ELSE	ISNULL(convert(varchar(3),CA.Territory),'N/A')
		END as TerritoryCode,
		B.RatingTerritoryCode as cte_TerritoryCode,
		B.Latitude as cte_latitude,
		B.Longitude as cte_longitude,
		B.CensusBlockGroup as cte_CensusBlockGroup
		FROM
		WorkDCTTransactionInsuranceLineLocationBridge A  
		inner join WorkCTE B on 
			A.WorkDCTTransactionInsuranceLineLocationBridgeId=B.WorkDCTTransactionInsuranceLineLocationBridgeId
			and A.LineId=B.LineId
			and A.LocationAssociationId=B.LocationAssociationId
			and A.SessionId=B.SessionId
		inner join WBLocationAccountStage WBLAS with (nolock) on 
			WBLAS.SessionId=B.SessionId and 	
			WBLAS.LocationId=B.LocationId
		inner join WorkDCTCoverageTransaction C with (nolock) on 
			C.SessionId=A.SessionId and 
			C.CoverageId=A.CoverageId
		INNER JOIN DCCALocationStaging CA  with (nolock) on 
			B.SessionId=CA.SessionId and 
			B.objectid=CA.CA_LocationId 		
	) A
	WHERE
	(
		A.TerritoryCode != A.cte_TerritoryCode OR
		A.Latitude != A.cte_latitude OR
		A.Longitude != A.cte_longitude OR
		A.cte_CensusBlockGroup != A.CBG
	)
),
SQ_WorkDCTTransactionInsuranceLineLocationBridge_GL AS (
	with WorkCTE as (
	select   A.WorkDCTTransactionInsuranceLineLocationBridgeId,
	A.LineId, 
	A.LocationAssociationId, 
	A.SessionId, 
	A.CoverageRiskType,
	DCLAS.LocationId,
	A.RatingTerritoryCode,
	A.Latitude, 
	A.Longitude, 
	A.CensusBlockGroup,
	DCLAS.ObjectId
	from 
	DBO.WorkDCTTransactionInsuranceLineLocationBridge A
	INNER JOIN 
	dbo.WorkDCTInsuranceLine C  with (nolock) on 
		A.SessionId=C.SessionId and
		A.LineId=C.LineId and	
		C.LineType in('GeneralLiability','SBOPGeneralLiability')
	INNER JOIN dbo.DCLocationAssociationStaging DCLAS  with (nolock) on   
		DCLAS.LocationAssociationId=A.LocationAssociationId and
		DCLAS.SessionId=A.SessionId and
		DCLAS.ObjectName='DC_GL_Location'
	where 
	A.RatingTerritoryCode='N/A'  and NOT A.LineId is null and NOT A.LocationAssociationId is null
	)
	
	select 
	A.WorkDCTTransactionInsuranceLineLocationBridgeId as WorkDCTTransactionInsuranceLineLocationBridgeId, 
	A.TerritoryCode as RatingTerritoryCode, 
	A.Latitude as Latitude, 
	A.Longitude as Longitude, 
	A.CBG  as CensusBlockGroup
	FROM 
	(
		Select 
		A.WorkDCTTransactionInsuranceLineLocationBridgeId,
		B.LocationAssociationId as LocationAssociationId, 
		A.SessionId as SessionId,
		B.LineId as LineId,
		ISNULL(WBLAS.Latitude,'0') as Latitude, 
		ISNULL(WBLAS.Longitude,'0') as Longitude, 
		ISNULL(WBLAS.CBG,'N/A') as CBG, 
		CASE 
		WHEN UPPER(C.CoverageType) like '%EARTHQUAKE%' THEN ISNULL(WBLAS.TerritoryCodeEarthQuake,'N/A')
		ELSE	ISNULL(GL.Territory,'N/A') 
		END as TerritoryCode,
		B.RatingTerritoryCode as cte_TerritoryCode,
		B.Latitude as cte_latitude,
		B.Longitude as cte_longitude,
		B.CensusBlockGroup as cte_CensusBlockGroup
		FROM
		WorkDCTTransactionInsuranceLineLocationBridge A  
		inner join WorkCTE B on 
			A.WorkDCTTransactionInsuranceLineLocationBridgeId=B.WorkDCTTransactionInsuranceLineLocationBridgeId
			and A.LineId=B.LineId
			and A.LocationAssociationId=B.LocationAssociationId
			and A.SessionId=B.SessionId
		inner join WBLocationAccountStage WBLAS with (nolock) on 
			WBLAS.SessionId=B.SessionId and 	
			WBLAS.LocationId=B.LocationId
		inner join WorkDCTCoverageTransaction C with (nolock) on 
			C.SessionId=A.SessionId and 
			C.CoverageId=A.CoverageId
		INNER JOIN DCGLLocationStaging GL  with (nolock) on 
			B.SessionId=GL.SessionId and 
			B.objectid=GL.GL_LocationId 		
	) A
	WHERE
	(
		A.TerritoryCode != A.cte_TerritoryCode OR
		A.Latitude != A.cte_latitude OR
		A.Longitude != A.cte_longitude OR
		A.cte_CensusBlockGroup != A.CBG
	)
),
SQ_WorkDCTTransactionInsuranceLineLocationBridge_Property AS (
	with WorkCTE as (
	select   A.WorkDCTTransactionInsuranceLineLocationBridgeId,
	A.LineId, 
	A.LocationAssociationId, 
	A.SessionId, 
	A.CoverageRiskType,
	DCLAS.LocationId,
	A.RatingTerritoryCode,
	A.Latitude, 
	A.Longitude, 
	A.CensusBlockGroup
	from 
	DBO.WorkDCTTransactionInsuranceLineLocationBridge A
	INNER JOIN 
	dbo.WorkDCTInsuranceLine C  with (nolock) on 
		A.SessionId=C.SessionId and
		A.LineId=C.LineId and	
		C.LineType in('Property','SBOPProperty')
	INNER JOIN dbo.DCLocationAssociationStaging DCLAS  with (nolock) on   
		DCLAS.LocationAssociationId=A.LocationAssociationId and
		DCLAS.SessionId=A.SessionId and
		DCLAS.LocationAssociationType ='CF_Location'
	where 
	A.RatingTerritoryCode='N/A'  and NOT A.LineId is null and NOT A.LocationAssociationId is null
	)
	
	select 
	A.WorkDCTTransactionInsuranceLineLocationBridgeId as WorkDCTTransactionInsuranceLineLocationBridgeId, 
	A.TerritoryCode as RatingTerritoryCode, 
	A.Latitude as Latitude, 
	A.Longitude as Longitude, 
	A.CBG  as CensusBlockGroup
	FROM 
	(
		Select 
		A.WorkDCTTransactionInsuranceLineLocationBridgeId,
		B.LocationAssociationId as LocationAssociationId, 
		A.SessionId as SessionId,
		B.LineId as LineId,
		ISNULL(WBLAS.Latitude,'0') as Latitude, 
		ISNULL(WBLAS.Longitude,'0') as Longitude, 
		ISNULL(WBLAS.CBG,'N/A') as CBG, 
		CASE 
		WHEN UPPER(C.CoverageType) like '%EARTHQUAKE%' then isnull(WBLAS.TerritoryCodeEarthQuake,'N/A')
		ELSE ISNULL(WBLAS.TerritoryCodeProperty,'N/A')
		END as TerritoryCode,
		B.RatingTerritoryCode as cte_TerritoryCode,
		B.Latitude as cte_latitude,
		B.Longitude as cte_longitude,
		B.CensusBlockGroup as cte_CensusBlockGroup
		FROM
		WorkDCTTransactionInsuranceLineLocationBridge A  
		inner join WorkCTE B on 
			A.WorkDCTTransactionInsuranceLineLocationBridgeId=B.WorkDCTTransactionInsuranceLineLocationBridgeId	
			and A.LineId=B.LineId
			and A.LocationAssociationId=B.LocationAssociationId
			and A.SessionId=B.SessionId
		inner join WBLocationAccountStage WBLAS with (nolock) on 
			WBLAS.SessionId=B.SessionId and 	
			WBLAS.LocationId=B.LocationId
		inner join WorkDCTCoverageTransaction C with (nolock) on 
			C.SessionId=A.SessionId and 
			C.CoverageId=A.CoverageId
	) A
	WHERE
	(
		A.TerritoryCode != A.cte_TerritoryCode OR
		A.Latitude != A.cte_latitude OR
		A.Longitude != A.cte_longitude OR
		A.cte_CensusBlockGroup != A.CBG
	)
),
Union_CBG_TerrCodes AS (
	SELECT WorkDCTTransactionInsuranceLineLocationBridgeId, RatingTerritoryCode, Latitude, Longitude, CensusBlockGroup
	FROM SQ_WorkDCTTransactionInsuranceLineLocationBridge_Property
	UNION
	SELECT WorkDCTTransactionInsuranceLineLocationBridgeId, RatingTerritoryCode, Latitude, Longitude, CensusBlockGroup
	FROM SQ_WorkDCTTransactionInsuranceLineLocationBridge_CA
	UNION
	SELECT WorkDCTTransactionInsuranceLineLocationBridgeId, RatingTerritoryCode, Latitude, Longitude, CensusBlockGroup
	FROM SQ_WorkDCTTransactionInsuranceLineLocationBridge_GL
	UNION
	SELECT WorkDCTTransactionInsuranceLineLocationBridgeId, RatingTerritoryCode, Latitude, Longitude, CensusBlockGroup
	FROM SQ_WorkDCTTransactionInsuranceLineLocationBridge_BP
	UNION
	SELECT WorkDCTTransactionInsuranceLineLocationBridgeId, RatingTerritoryCode, Latitude, Longitude, CensusBlockGroup
	FROM SQ_WorkDCTTransactionInsuranceLineLocationBridge_AllElse
),
EXP_Territory_Input AS (
	SELECT
	WorkDCTTransactionInsuranceLineLocationBridgeId,
	RatingTerritoryCode,
	Latitude,
	Longitude,
	CensusBlockGroup
	FROM Union_CBG_TerrCodes
),
UPD_Territory AS (
	SELECT
	WorkDCTTransactionInsuranceLineLocationBridgeId, 
	RatingTerritoryCode, 
	Latitude, 
	Longitude, 
	CensusBlockGroup
	FROM EXP_Territory_Input
),
WorkDCTTransactionInsuranceLineLocationBridge_UPDATE_Territory AS (
	MERGE INTO WorkDCTTransactionInsuranceLineLocationBridge AS T
	USING UPD_Territory AS S
	ON T.WorkDCTTransactionInsuranceLineLocationBridgeId = S.WorkDCTTransactionInsuranceLineLocationBridgeId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CensusBlockGroup = S.CensusBlockGroup, T.Latitude = S.Latitude, T.Longitude = S.Longitude, T.RatingTerritoryCode = S.RatingTerritoryCode
),