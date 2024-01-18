WITH
LKP_PolicyLimits AS (
	SELECT
	PolicyPerOccurenceLimit,
	PolicyAKId,
	pol_key,
	InsuranceLine,
	in_pol_key
	FROM (
		select 
		 max(PL.PolicyPerOccurenceLimit) AS PolicyPerOccurenceLimit,
		P.pol_key as pol_key
		from 
		@{pipeline().parameters.DATABASE_NAME_IL}.dbo.PolicyLimit PL
		inner join @{pipeline().parameters.DATABASE_NAME_IL}.v2.policy P on PL.PolicyAKId=P.pol_ak_id and P.crrnt_snpsht_flag=1 and PL.CurrentSnapshotFlag=1
		where PL.InsuranceLine in
		( 
		'BusinessOwners',
		'DirectorsAndOfficersNFP',
		'DirectorsAndOffsCondos',
		'EmploymentPracticesLiab',
		'ExcessLiability',
		'GamesOfChance',
		'GeneralLiability',
		'GL',
		'HoleInOne',
		'SBOPGeneralLiability'
		)
		and (YEAR(P.pol_eff_date)=@{pipeline().parameters.YEAR} or YEAR(P.pol_exp_date)=@{pipeline().parameters.YEAR})
		--and P.pol_eff_date != P.pol_cancellation_date
		and isnumeric(PolicyPerOccurenceLimit)=1
		group by P.pol_key --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY PolicyPerOccurenceLimit) = 1
),
LKP_StatCovClass11111 AS (
	SELECT
	StatisticalCoverageID,
	StatisticalCoverageAKID,
	RiskUnitGroup,
	MajorPerilCode,
	ClassCode
	FROM (
		SELECT 
			StatisticalCoverageID,
			StatisticalCoverageAKID,
			RiskUnitGroup,
			MajorPerilCode,
			ClassCode
		FROM StatisticalCoverage
		WHERE ClassCode='11111' and RiskUnitGroup='340' and  MajorPerilCode in ('540','530')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StatisticalCoverageAKID ORDER BY StatisticalCoverageID) = 1
),
LKP_PolicyLimits_Umbrella AS (
	SELECT
	PolicyPerOccurenceLimit,
	PolicyAKId,
	pol_key,
	InsuranceLine,
	in_pol_key
	FROM (
		select 
		 max(PL.PolicyPerOccurenceLimit) AS PolicyPerOccurenceLimit,
		P.pol_key as pol_key
		from 
		@{pipeline().parameters.DATABASE_NAME_IL}.dbo.PolicyLimit PL
		inner join @{pipeline().parameters.DATABASE_NAME_IL}.v2.policy P on PL.PolicyAKId=P.pol_ak_id and P.crrnt_snpsht_flag=1 and PL.CurrentSnapshotFlag=1
		where PL.InsuranceLine in
		( 
		'CommercialUmbrella'
		)
		and (YEAR(P.pol_eff_date)=@{pipeline().parameters.YEAR} or YEAR(P.pol_exp_date)=@{pipeline().parameters.YEAR})
		--and P.pol_eff_date != P.pol_cancellation_date
		and isnumeric(PolicyPerOccurenceLimit)=1
		group by P.pol_key --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY PolicyPerOccurenceLimit) = 1
),
SQ_WorkNAICTerrorismControl AS (
	Declare @YearStart as varchar(4) = @{pipeline().parameters.YEAR}
	
	SELECT DISTINCT
	WorkNAICTerrorismControl.AuditId, 
	WorkNAICTerrorismControl.CreatedDate, 
	WorkNAICTerrorismControl.SourceSystemID,
	WorkNAICTerrorismControl.StatisticalCoverageAKID, 
	WorkNAICTerrorismControl.RatingCoverageAKId, 
	WorkNAICTerrorismControl.ZipPostalCode,
	WorkNAICTerrorismControl.StateProvinceCodeAbbreviation, 
	WorkNAICTerrorismControl.LocationNumber, 
	WorkNAICTerrorismControl.PolicyEffectiveDateYear, 
	WorkNAICTerrorismControl.pol_key, 
	WorkNAICTerrorismControl.InsuranceLineCode, 
	WorkNAICTerrorismControl.InsuranceLineDescription, 
	WorkNAICTerrorismControl.InsuranceReferenceLineOfBusinessDescription, 
	WorkNAICTerrorismControl.CoverageCode, 
	WorkNAICTerrorismControl.CoverageDescription, 
	WorkNAICTerrorismControl.Lob, 
	WorkNAICTerrorismControl.Code, 
	WorkNAICTerrorismControl.DctRiskTypeCode, 
	WorkNAICTerrorismControl.PropertyCoverageCode, 
	WorkNAICTerrorismControl.DctCoverageTypeCode, 
	WorkNAICTerrorismControl.BOPCoverageCode, 
	WorkNAICTerrorismControl.BureauCode1, 
	WorkNAICTerrorismControl.PMSCoverageCode, 
	WorkNAICTerrorismControl.PolicyOfferingCode, 
	WorkNAICTerrorismControl.ProductCode, 
	WorkNAICTerrorismControl.InsuranceReferenceLineOfBusinessCode, 
	WorkNAICTerrorismControl.LiabilityCoverageCode, 
	WorkNAICTerrorismControl.PolCat, 
	WorkNAICTerrorismControl.TerrorismRiskInd, 
	WorkNAICTerrorismControl.CoCode, 
	WorkNAICTerrorismControl.CoType, 
	WorkNAICTerrorismControl.IndCodeType, 
	WorkNAICTerrorismControl.PolType, 
	WorkNAICTerrorismControl.CovType, 
	WorkNAICTerrorismControl.TableCode, 
	WorkNAICTerrorismControl.TableCodeInsuranceLine, 
	WorkNAICTerrorismControl.ReinsurancePercent, 
	WorkNAICTerrorismControl.BlackListCoverageFlag 
	FROM
	 WorkNAICTerrorismControl
	WHERE
	WorkNAICTerrorismControl.PolicyEffectiveDateYear=@YearStart
	and
	WorkNAICTerrorismControl.AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CONTROL}
),
EXP_InputFromControlTable AS (
	SELECT
	AuditId,
	CreatedDate,
	SourceSystemID,
	StatisticalCoverageAKID,
	RatingCoverageAKId,
	ZipPostalCode,
	StateProvinceCodeAbbreviation,
	LocationNumber,
	-- *INF*: IIF(IS_NUMBER(LocationNumber)=1,TO_INTEGER(LocationNumber),1)
	IFF(REGEXP_LIKE(LocationNumber, '^[0-9]+$') = 1, CAST(LocationNumber AS INTEGER), 1) AS v_LocationNumber,
	v_LocationNumber AS o_LocationNumber,
	PolicyEffectiveDateYear,
	pol_key,
	InsuranceLineCode,
	InsuranceLineDescription,
	InsuranceReferenceLineOfBusinessDescription,
	CoverageCode,
	CoverageDescription,
	Lob,
	Code,
	DctRiskTypeCode,
	PropertyCoverageCode,
	DctCoverageTypeCode,
	BOPCoverageCode,
	BureauCode1,
	PMSCoverageCode,
	PolicyOfferingCode,
	ProductCode,
	InsuranceReferenceLineOfBusinessCode,
	LiabilityCoverageCode,
	PolCat,
	TerrorismRiskInd,
	-- *INF*: IIF(DctCoverageTypeCode='TerrorismFireOnly','Y',TerrorismRiskInd)
	-- 
	-- --- force TerrorismFireOnly to always be counted as terrorism limit and premium
	IFF(DctCoverageTypeCode = 'TerrorismFireOnly', 'Y', TerrorismRiskInd) AS v_TerrorismRiskInd,
	v_TerrorismRiskInd AS o_TerrorismRiskInd,
	CoCode,
	CoType,
	IndCodeType,
	PolType,
	CovType,
	TableCode AS TableCode1,
	TableCodeInsuranceLine,
	ReinsurancePercent,
	BlackListCoverageFlag,
	-- *INF*: IIF(InsuranceReferenceLineOfBusinessDescription='Employment Practices Liability Insurance' and TableCodeInsuranceLine='Liability' and not (PolicyOfferingCode='330' and InsuranceReferenceLineOfBusinessCode='330' and ProductCode='330') ,'Y','N')
	IFF(
	    InsuranceReferenceLineOfBusinessDescription = 'Employment Practices Liability Insurance'
	    and TableCodeInsuranceLine = 'Liability'
	    and not (PolicyOfferingCode = '330'
	    and InsuranceReferenceLineOfBusinessCode = '330'
	    and ProductCode = '330'),
	    'Y',
	    'N'
	) AS v_LiabilityLimitAddOnFlag,
	v_LiabilityLimitAddOnFlag AS o_LiabilityLimitAddOnFlag,
	-- *INF*: DECODE(TRUE,
	-- TableCode1='1' and PropertyCoverageCode !='N/A',PropertyCoverageCode,
	-- TableCode1='1' and BOPCoverageCode!='N/A',BOPCoverageCode,
	-- TableCode1='1' and PMSCoverageCode!='N/A',PMSCoverageCode,
	-- TableCode1='2' and LiabilityCoverageCode !='N/A' ,LiabilityCoverageCode,
	-- TableCode1='3','',
	-- 'N/A'||TableCode1
	-- )
	DECODE(
	    TRUE,
	    TableCode1 = '1' and PropertyCoverageCode != 'N/A', PropertyCoverageCode,
	    TableCode1 = '1' and BOPCoverageCode != 'N/A', BOPCoverageCode,
	    TableCode1 = '1' and PMSCoverageCode != 'N/A', PMSCoverageCode,
	    TableCode1 = '2' and LiabilityCoverageCode != 'N/A', LiabilityCoverageCode,
	    TableCode1 = '3', '',
	    'N/A' || TableCode1
	) AS v_Coverage,
	v_Coverage AS o_Coverage,
	-- *INF*: DECODE (TRUE,
	-- SourceSystemID='PMS', StatisticalCoverageAKID,
	-- SourceSystemID='DCT',RatingCoverageAKId,
	-- -1
	-- )
	DECODE(
	    TRUE,
	    SourceSystemID = 'PMS', StatisticalCoverageAKID,
	    SourceSystemID = 'DCT', RatingCoverageAKId,
	    - 1
	) AS o_RCStatCoverageAkID,
	-- *INF*: MD5(pol_key||ZipPostalCode||StateProvinceCodeAbbreviation||Lob||Code||PolCat||TableCode1||v_Coverage||PolType)
	MD5(pol_key || ZipPostalCode || StateProvinceCodeAbbreviation || Lob || Code || PolCat || TableCode1 || v_Coverage || PolType) AS o_HashKey,
	-- *INF*: IIF(IN(DctCoverageTypeCode,'RatingGroup','Blanket'),'Y','N')
	IFF(DctCoverageTypeCode IN ('RatingGroup','Blanket'), 'Y', 'N') AS o_DCTCoverageTypeCodeRGBFlag
	FROM SQ_WorkNAICTerrorismControl
),
SRT_ControlInput AS (
	SELECT
	o_HashKey AS HashKey, 
	o_LocationNumber AS LocationNumber, 
	pol_key, 
	SourceSystemID, 
	PolCat, 
	Lob, 
	o_Coverage AS CoverageCode, 
	TableCode1, 
	ZipPostalCode AS ZIP, 
	StateProvinceCodeAbbreviation AS STABBR, 
	o_RCStatCoverageAkID AS RCStatCoverageAkID, 
	Code, 
	CoCode, 
	CoType, 
	IndCodeType, 
	PolType, 
	CovType, 
	TableCodeInsuranceLine, 
	ReinsurancePercent, 
	PolicyEffectiveDateYear, 
	CreatedDate AS ExtractDate, 
	AuditId, 
	o_TerrorismRiskInd AS TerrorismRiskInd, 
	BlackListCoverageFlag, 
	o_LiabilityLimitAddOnFlag AS LiabilityLimitAddOnFlag, 
	o_DCTCoverageTypeCodeRGBFlag AS DctCoverageTypeCode
	FROM EXP_InputFromControlTable
	ORDER BY HashKey ASC, LocationNumber ASC
),
EXP_SrtControlOutput AS (
	SELECT
	pol_key,
	RCStatCoverageAkID,
	SourceSystemID,
	PolCat,
	Lob,
	CoverageCode,
	ZIP,
	STABBR,
	LocationNumber,
	TableCode1,
	Code,
	CoCode,
	CoType,
	IndCodeType,
	PolType,
	CovType,
	TableCodeInsuranceLine,
	ReinsurancePercent,
	PolicyEffectiveDateYear,
	ExtractDate,
	AuditId,
	TerrorismRiskInd,
	BlackListCoverageFlag,
	LiabilityLimitAddOnFlag,
	HashKey,
	-- *INF*: DECODE(TRUE,
	-- isnull(v_PreviousLocationNumber), 1,
	-- HashKey != v_PreviousHashKey, 1,
	-- LocationNumber != v_PreviousLocationNumber,1,
	-- 0)
	-- 
	DECODE(
	    TRUE,
	    v_PreviousLocationNumber IS NULL, 1,
	    HashKey != v_PreviousHashKey, 1,
	    LocationNumber != v_PreviousLocationNumber, 1,
	    0
	) AS v_LocationCount,
	v_LocationCount AS o_LocationCount,
	LocationNumber AS v_PreviousLocationNumber,
	HashKey AS v_PreviousHashKey,
	DctCoverageTypeCode AS DctCoverageTypeCodeCBGFlag
	FROM SRT_ControlInput
),
AGG_MaxLocationNumber AS (
	SELECT
	HashKey,
	o_LocationCount AS LocationCounter,
	-- *INF*: sum(LocationCounter)
	sum(LocationCounter) AS o_LocationCounter
	FROM EXP_SrtControlOutput
	GROUP BY HashKey
),
JNRTRANS AS (SELECT
	EXP_SrtControlOutput.pol_key, 
	EXP_SrtControlOutput.RCStatCoverageAkID, 
	EXP_SrtControlOutput.SourceSystemID, 
	EXP_SrtControlOutput.PolCat, 
	EXP_SrtControlOutput.Lob, 
	EXP_SrtControlOutput.CoverageCode, 
	EXP_SrtControlOutput.ZIP, 
	EXP_SrtControlOutput.STABBR, 
	EXP_SrtControlOutput.LocationNumber, 
	EXP_SrtControlOutput.TableCode1, 
	EXP_SrtControlOutput.Code, 
	EXP_SrtControlOutput.CoCode, 
	EXP_SrtControlOutput.CoType, 
	EXP_SrtControlOutput.IndCodeType, 
	EXP_SrtControlOutput.PolType, 
	EXP_SrtControlOutput.CovType, 
	EXP_SrtControlOutput.TableCodeInsuranceLine, 
	EXP_SrtControlOutput.ReinsurancePercent, 
	EXP_SrtControlOutput.PolicyEffectiveDateYear, 
	EXP_SrtControlOutput.ExtractDate, 
	EXP_SrtControlOutput.AuditId, 
	EXP_SrtControlOutput.TerrorismRiskInd, 
	EXP_SrtControlOutput.BlackListCoverageFlag, 
	EXP_SrtControlOutput.LiabilityLimitAddOnFlag, 
	EXP_SrtControlOutput.HashKey, 
	AGG_MaxLocationNumber.HashKey AS HashKey1, 
	AGG_MaxLocationNumber.o_LocationCounter, 
	EXP_SrtControlOutput.DctCoverageTypeCodeCBGFlag AS DctCoverageTypeCode
	FROM EXP_SrtControlOutput
	INNER JOIN AGG_MaxLocationNumber
	ON AGG_MaxLocationNumber.HashKey = EXP_SrtControlOutput.HashKey
),
AGG_RemoveDuplicates AS (
	SELECT
	pol_key,
	RCStatCoverageAkID AS RCStatCoverageAKId,
	SourceSystemID,
	TableCode1,
	PolicyEffectiveDateYear,
	STABBR,
	CoCode AS COCODE,
	CoType AS COTYPE,
	Lob AS LOB,
	ZIP,
	PolCat AS POLCAT,
	CoverageCode AS COVERAGE,
	IndCodeType AS IND_CODE_TYPE,
	Code AS CODE,
	PolType AS POLTYPE,
	CovType AS COVTYPE,
	o_LocationCounter AS location_counter,
	TableCodeInsuranceLine,
	ReinsurancePercent,
	TerrorismRiskInd AS terrorism_risk_ind,
	ExtractDate,
	AuditId AS AuditID,
	BlackListCoverageFlag AS IsBlackListCoverage,
	LiabilityLimitAddOnFlag,
	DctCoverageTypeCode AS DctCoverageTypeCodeCBGFlag
	FROM JNRTRANS
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key, RCStatCoverageAKId, SourceSystemID, TableCode1, PolicyEffectiveDateYear, STABBR, COCODE, COTYPE, LOB, ZIP, POLCAT, COVERAGE, IND_CODE_TYPE, CODE, POLTYPE, COVTYPE, location_counter, TableCodeInsuranceLine, ReinsurancePercent, terrorism_risk_ind, IsBlackListCoverage, LiabilityLimitAddOnFlag, DctCoverageTypeCodeCBGFlag ORDER BY NULL) = 1
),
EXP_BreakOutCalculations AS (
	SELECT
	pol_key,
	RCStatCoverageAKId,
	SourceSystemID,
	TableCode1,
	PolicyEffectiveDateYear,
	STABBR,
	COCODE,
	COTYPE,
	LOB,
	ZIP,
	POLCAT,
	COVERAGE,
	IND_CODE_TYPE,
	CODE,
	POLTYPE,
	COVTYPE,
	location_counter AS Location_Counter,
	-- *INF*: IIF(Location_Counter=0,1,Location_Counter)
	IFF(Location_Counter = 0, 1, Location_Counter) AS o_Location_Counter,
	TableCodeInsuranceLine,
	ReinsurancePercent,
	terrorism_risk_ind,
	ExtractDate,
	AuditID,
	IsBlackListCoverage,
	LiabilityLimitAddOnFlag,
	DctCoverageTypeCodeCBGFlag
	FROM AGG_RemoveDuplicates
),
RTR_LimitsBySourceSystem AS (
	SELECT
	pol_key,
	RCStatCoverageAKId,
	SourceSystemID,
	TableCode1,
	LiabilityLimitAddOnFlag,
	DctCoverageTypeCodeCBGFlag
	FROM EXP_BreakOutCalculations
),
RTR_LimitsBySourceSystem_PMS AS (SELECT * FROM RTR_LimitsBySourceSystem WHERE SourceSystemID='PMS'),
RTR_LimitsBySourceSystem_DCT AS (SELECT * FROM RTR_LimitsBySourceSystem WHERE SourceSystemID='DCT'),
LKP_RCActiveCoverageLimit AS (
	SELECT
	pol_key,
	LocationUnitNumber,
	SubLocationUnitNumber,
	CoverageLimitType,
	CoverageLimitValue,
	CoverageType,
	RiskType,
	RatingCoverageAKID,
	in_pol_key,
	in_RCStatCoverageAKId,
	inSourceSystemID,
	in_TableCode1,
	in_LiabilityLimitAddOnFlag,
	in_DctCoverageTypeCodeCBGFlag
	FROM (
		Select 
		
		LocationUnitNumber as LocationUnitNumber,
		SubLocationUnitNumber as SubLocationUnitNumber,
		CoverageLimitType as CoverageLimitType,
		CoverageLimitValue as CoverageLimitValue,
		CoverageType as CoverageType,
		RiskType as RiskType,
		pol_key as pol_key ,
		RatingCoverageAKID as RatingCoverageAKID 
		From
		(
		SELECT  Distinct
		
		RL.LocationUnitNumber AS LocationUnitNumber,  
		RC.SubLocationUnitNumber AS SubLocationUnitNumber,  
		LMT.CoverageLimitType AS CoverageLimitType,  
		FIRST_VALUE(LMT.CoverageLimitValue) OVER (PARTITION BY POL.pol_key, RL.LocationUnitNumber, RC.CoverageType, RC.SubLocationUnitNumber,LMT.CoverageLimitType ORDER BY pt.PremiumTransactionEffectiveDate desc, pt.PremiumTransactionEnteredDate desc,PT.Effectivedate desc, CLB.CreatedDate desc,lmt.coveragelimitvalue desc) AS CoverageLimitValue,
		CoverageType as CoverageType,
		RiskType as RiskType,
		POL.pol_key AS pol_key,
		RC.RatingCoverageAKID AS RatingCoverageAKID 
		FROM 
		@{pipeline().parameters.DATABASE_NAME_IL}.dbo.PremiumTransaction PT  with (nolock)	
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.RatingCoverage RC with (nolock) ON PT.RatingCoverageAKID = RC.RatingCoverageAKID
			AND RC.EffectiveDate = PT.EffectiveDate
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.PolicyCoverage PC with (nolock) ON PC.PolicyCoverageAKID = RC.PolicyCoverageAKID
			AND PC.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.RiskLocation RL with (nolock) ON PC.RiskLocationAKID = RL.RiskLocationAKID
			AND RL.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.V2.policy POL with (nolock) ON POL.pol_ak_id = RL.PolicyAKID
			AND POL.crrnt_snpsht_flag = 1
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.CoverageLimitBridge CLB with (nolock) ON CLB.PremiumTransactionAKId = PT.PremiumTransactionAKID
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.CoverageLimit LMT with (nolock) ON CLB.CoverageLimitId = LMT.CoverageLimitId
		WHERE  PT.CurrentSnapshotFlag = 1
		and LMT.CoverageLimitType Not In ('Scheduled Property','Equipment Scheduled','GKLL','Non Std Open Lots','OTC Bldg Inv','Std Open Lots') 
		and (YEAR(POL.pol_eff_date)=@{pipeline().parameters.YEAR} or YEAR(POL.pol_exp_date)=@{pipeline().parameters.YEAR})
		--and POL.pol_eff_date != POL.pol_cancellation_date
		and isnumeric(LMT.CoverageLimitValue)=1
		@{pipeline().parameters.WHERE_LIMIT_DCT}
		) B
		
		UNION
		
		select distinct 
			
						LocationUnitNumber as LocationUnitNumber,
						SubLocationUnitNumber as SubLocationUnitNumber,			
						CoverageLimitType as CoverageLimitType,
						case when CoverageLimitType='GKLL' and cast(CoverageLimitValue as varchar) = '6000'  then
						FIRST_VALUE(cast(CoverageLimitValue as varchar)-6000) OVER (PARTITION BY  pol_key, LocationUnitNumber, SubLocationUnitNumber, CoverageType, CoverageLimitType ORDER BY PremiumTransactionEffectiveDate desc, PremiumTransactionEnteredDate desc,Effectivedate desc, CreatedDate desc) else
						FIRST_VALUE(cast(CoverageLimitValue as varchar)) OVER (PARTITION BY  pol_key, LocationUnitNumber, SubLocationUnitNumber, CoverageType, CoverageLimitType ORDER BY PremiumTransactionEffectiveDate desc, PremiumTransactionEnteredDate desc,Effectivedate desc, CreatedDate desc) 
						end as CoverageLimitValue,			
						CoverageType as CoverageType,
						RiskType as RiskType,
					pol_key as pol_key,
		RatingCoverageAKID as  RatingCoverageAKID
						from (
		SELECT DISTINCT
						POL.pol_key,
						RL.LocationUnitNumber,
						RC.SubLocationUnitNumber,
						LMT.CoverageLimitType,
						RC.CoverageType,
		 	  			RC.RiskType,
		  RC.RatingCoverageAKID,
		 (Case When LMT.CoverageLimitType in ('Scheduled Property','Equipment Scheduled') then
				        Sum(cast(LMT.CoverageLimitValue as bigint)*CLB.CoverageLimitIDCount) OVER (PARTITION BY  POL.pol_key, RL.LocationUnitNumber, RC.SubLocationUnitNumber,  RC.CoverageType,LMT.CoverageLimitType ,pt.PremiumTransactionEffectiveDate , pt.PremiumTransactionEnteredDate ,PT.Effectivedate , CLB.CreatedDate ) 
					     else
						Sum(cast(LMT.CoverageLimitValue as bigint)) OVER (PARTITION BY  POL.pol_key, RL.LocationUnitNumber, RC.SubLocationUnitNumber,  RC.CoverageType,LMT.CoverageLimitType ,pt.PremiumTransactionEffectiveDate , pt.PremiumTransactionEnteredDate ,PT.Effectivedate , CLB.CreatedDate ) 
					    end) AS CoverageLimitValue,			  
			  pt.PremiumTransactionEffectiveDate , pt.PremiumTransactionEnteredDate ,PT.Effectivedate , CLB.CreatedDate
			  from
		@{pipeline().parameters.DATABASE_NAME_IL}.dbo.PremiumTransaction PT with (nolock)
			
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.RatingCoverage RC with (nolock) ON PT.RatingCoverageAKID = RC.RatingCoverageAKID
			AND RC.EffectiveDate = PT.EffectiveDate
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.PolicyCoverage PC with (nolock) ON PC.PolicyCoverageAKID = RC.PolicyCoverageAKID
			AND PC.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.RiskLocation RL with (nolock) ON PC.RiskLocationAKID = RL.RiskLocationAKID
			AND RL.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.v2.policy POL with (nolock) ON POL.pol_ak_id = RL.PolicyAKID
			AND POL.crrnt_snpsht_flag = 1
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.CoverageLimitBridge CLB with (nolock) ON CLB.PremiumTransactionAKId = PT.PremiumTransactionAKID
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.CoverageLimit LMT with (nolock) ON CLB.CoverageLimitId = LMT.CoverageLimitId
		WHERE PT.CurrentSnapshotFlag = 1
		and LMT.CoverageLimitType In ('Scheduled Property','Equipment Scheduled','GKLL','Non Std Open Lots','OTC Bldg Inv','Std Open Lots') 
		and (YEAR(POL.pol_eff_date)=@{pipeline().parameters.YEAR} or YEAR(POL.pol_exp_date)=@{pipeline().parameters.YEAR}) 
		and isnumeric(LMT.CoverageLimitValue)=1 
		--and POL.pol_eff_date != POL.pol_cancellation_date
		@{pipeline().parameters.WHERE_LIMIT_DCT}
		) A
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key,RatingCoverageAKID ORDER BY pol_key) = 1
),
EXP_RC_SetHashCode AS (
	SELECT
	pol_key,
	LocationUnitNumber,
	SubLocationUnitNumber,
	CoverageLimitType,
	CoverageLimitValue,
	-- *INF*: IIF(IN(in_DctCoverageTypeCodeCBGFlag,'Y'),'0',CoverageLimitValue)
	IFF(in_DctCoverageTypeCodeCBGFlag IN ('Y'), '0', CoverageLimitValue) AS v_CoverageLimitValue,
	v_CoverageLimitValue AS o_CoverageLimitValue,
	CoverageType,
	RiskType,
	RatingCoverageAKID,
	in_pol_key,
	in_RCStatCoverageAKId,
	inSourceSystemID,
	in_TableCode1,
	in_LiabilityLimitAddOnFlag,
	in_DctCoverageTypeCodeCBGFlag,
	-- *INF*: MD5(in_pol_key||LocationUnitNumber||SubLocationUnitNumber||CoverageLimitType||v_CoverageLimitValue||CoverageType||RiskType)
	MD5(in_pol_key || LocationUnitNumber || SubLocationUnitNumber || CoverageLimitType || v_CoverageLimitValue || CoverageType || RiskType) AS LimitHashKey
	FROM LKP_RCActiveCoverageLimit
),
LKP_SCActiveCoverageLimit AS (
	SELECT
	pol_key,
	LocationUnitNumber,
	SubLocationUnitNumber,
	CoverageLimitType,
	CoverageLimitValue,
	StatisticalCoverageAKID,
	RiskUnitGroup,
	ClassCode,
	in_pol_key,
	in_RCStatCoverageAKId,
	in_SourceSystemID,
	in_TableCode1,
	in_LiabilityLimitAddOnFlag
	FROM (
		Select 
		LocationUnitNumber as LocationUnitNumber ,
		SubLocationUnitNumber as SubLocationUnitNumber,
		CoverageLimitType as CoverageLimitType,
		CoverageLimitValue as CoverageLimitValue,
		RiskUnitGroup as RiskUnitGroup,
		ClassCode as ClassCode,
		pol_key as pol_key,
		StatisticalCoverageAKID as  StatisticalCoverageAKID
		From (
		SELECT DISTINCT 
			RL.LocationUnitNumber as LocationUnitNumber,
			SC.SubLocationUnitNumber as SubLocationUnitNumber,
			LMT.CoverageLimitType as CoverageLimitType,
		      FIRST_VALUE(LMT.CoverageLimitValue) OVER (PARTITION BY 
			  POL.pol_key, RL.LocationUnitNumber, SC.SubLocationUnitNumber, LMT.CoverageLimitType ORDER BY pt.PremiumTransactionEffectiveDate desc, pt.PremiumTransactionEnteredDate desc,PT.Effectivedate desc, CLB.CreatedDate desc,lmt.coveragelimitvalue desc) AS CoverageLimitValue,
		 POL.pol_key as pol_key,
		  SC.StatisticalCoverageAKID as StatisticalCoverageAKID,
		SC.RiskUnitGroup as RiskUnitGroup, SC.ClassCode as ClassCode
		FROM 
		@{pipeline().parameters.DATABASE_NAME_IL}.dbo.PremiumTransaction PT  with (nolock)
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.StatisticalCoverage SC with (nolock) ON PT.StatisticalCoverageAKID = SC.StatisticalCoverageAKID
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.PolicyCoverage PC with (nolock) ON PC.PolicyCoverageAKID = SC.PolicyCoverageAKID
			AND PC.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.RiskLocation RL with (nolock) ON PC.RiskLocationAKID = RL.RiskLocationAKID
			AND RL.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.V2.policy POL with (nolock) ON POL.pol_ak_id = RL.PolicyAKID
			AND POL.crrnt_snpsht_flag = 1 
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.CoverageLimitBridge CLB with (nolock) ON CLB.PremiumTransactionAKId = PT.PremiumTransactionAKID
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.CoverageLimit LMT with (nolock) ON CLB.CoverageLimitId = LMT.CoverageLimitId
		WHERE  PT.CurrentSnapshotFlag = 1
		and LMT.CoverageLimitType Not In ('Scheduled Property','Equipment Scheduled','GKLL','Non Std Open Lots','OTC Bldg Inv','Std Open Lots','ProductsCompletedAggregate') 
		and (YEAR(POL.pol_eff_date)=@{pipeline().parameters.YEAR} or YEAR(POL.pol_exp_date)=@{pipeline().parameters.YEAR})
		--and POL.pol_eff_date != POL.pol_cancellation_date
		and isnumeric(LMT.CoverageLimitValue)=1
		@{pipeline().parameters.WHERE_LIMIT_PMS}
		) B
		
		UNION
		
		select distinct 				
						LocationUnitNumber as LocationUnitNumber,
						SubLocationUnitNumber as SubLocationUnitNumber,			
						CoverageLimitType as CoverageLimitType,
						case when CoverageLimitType='GKLL' and cast(CoverageLimitValue as varchar) = '6000'  then
						FIRST_VALUE(cast(CoverageLimitValue as varchar)-6000) OVER (PARTITION BY  pol_key, LocationUnitNumber, SubLocationUnitNumber, CoverageLimitType ORDER BY PremiumTransactionEffectiveDate desc, PremiumTransactionEnteredDate desc,Effectivedate desc, CreatedDate desc) else
						FIRST_VALUE(cast(CoverageLimitValue as varchar)) OVER (PARTITION BY  pol_key, LocationUnitNumber, SubLocationUnitNumber,  CoverageLimitType ORDER BY PremiumTransactionEffectiveDate desc, PremiumTransactionEnteredDate desc,Effectivedate desc, CreatedDate desc) 
						end as CoverageLimitValue,
						RiskUnitGroup as RiskUnitGroup,
		ClassCode as ClassCode,
		pol_key as pol_key,
		StatisticalCoverageAKID as  StatisticalCoverageAKID
						from (
		SELECT DISTINCT
		POL.pol_key,				
						RL.LocationUnitNumber,
						SC.SubLocationUnitNumber,
						LMT.CoverageLimitType,
		SC.StatisticalCoverageAKID,
		 (Case When LMT.CoverageLimitType in ('Scheduled Property','Equipment Scheduled') then
				        Sum(cast(LMT.CoverageLimitValue as bigint)*CLB.CoverageLimitIDCount) OVER (PARTITION BY  POL.pol_key, RL.LocationUnitNumber, SC.SubLocationUnitNumber,  LMT.CoverageLimitType ,pt.PremiumTransactionEffectiveDate , pt.PremiumTransactionEnteredDate ,PT.Effectivedate , CLB.CreatedDate ) 
					     else
						Sum(cast(LMT.CoverageLimitValue as bigint)) OVER (PARTITION BY  POL.pol_key, RL.LocationUnitNumber, SC.SubLocationUnitNumber,  LMT.CoverageLimitType ,pt.PremiumTransactionEffectiveDate , pt.PremiumTransactionEnteredDate ,PT.Effectivedate , CLB.CreatedDate) 
					    end) AS CoverageLimitValue,			  
			  pt.PremiumTransactionEffectiveDate , pt.PremiumTransactionEnteredDate ,PT.Effectivedate , CLB.CreatedDate,
		SC.RiskUnitGroup,SC.ClassCode
			  from
		@{pipeline().parameters.DATABASE_NAME_IL}.dbo.PremiumTransaction PT with (nolock)
			
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.StatisticalCoverage SC with (nolock) ON PT.StatisticalCoverageAKID = SC.StatisticalCoverageAKID
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.PolicyCoverage PC with (nolock) ON PC.PolicyCoverageAKID = SC.PolicyCoverageAKID
			AND PC.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.RiskLocation RL with (nolock) ON PC.RiskLocationAKID = RL.RiskLocationAKID
			AND RL.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.v2.policy POL with (nolock) ON POL.pol_ak_id = RL.PolicyAKID
			AND POL.crrnt_snpsht_flag = 1 
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.CoverageLimitBridge CLB with (nolock) ON CLB.PremiumTransactionAKId = PT.PremiumTransactionAKID
		INNER JOIN @{pipeline().parameters.DATABASE_NAME_IL}.dbo.CoverageLimit LMT with (nolock) ON CLB.CoverageLimitId = LMT.CoverageLimitId
		WHERE PT.CurrentSnapshotFlag = 1
		and LMT.CoverageLimitType In ('Scheduled Property','Equipment Scheduled','GKLL','Non Std Open Lots','OTC Bldg Inv','Std Open Lots') 
		and (YEAR(POL.pol_eff_date)=@{pipeline().parameters.YEAR} or YEAR(POL.pol_exp_date)=@{pipeline().parameters.YEAR})
		--and POL.pol_eff_date != POL.pol_cancellation_date
		and isnumeric(LMT.CoverageLimitValue)=1
		@{pipeline().parameters.WHERE_LIMIT_PMS}
		) A
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key,StatisticalCoverageAKID ORDER BY pol_key) = 1
),
EXP_SCSetHashKey AS (
	SELECT
	pol_key,
	LocationUnitNumber,
	SubLocationUnitNumber,
	CoverageLimitType,
	CoverageLimitValue,
	StatisticalCoverageAKID,
	RiskUnitGroup,
	ClassCode,
	in_pol_key,
	in_RCStatCoverageAKId,
	in_SourceSystemID,
	in_TableCode1,
	in_LiabilityLimitAddOnFlag,
	-- *INF*: IIF(RiskUnitGroup='966' and ClassCode='966','0',CoverageLimitValue)
	-- -- force plus pak to 0, else it double counts
	IFF(RiskUnitGroup = '966' and ClassCode = '966', '0', CoverageLimitValue) AS v_Limit,
	v_Limit AS o_Limit,
	-- *INF*: MD5(in_pol_key||LocationUnitNumber||SubLocationUnitNumber||CoverageLimitType||v_Limit||RiskUnitGroup||ClassCode)
	MD5(in_pol_key || LocationUnitNumber || SubLocationUnitNumber || CoverageLimitType || v_Limit || RiskUnitGroup || ClassCode) AS LimitHashKey
	FROM LKP_SCActiveCoverageLimit
),
Union_Limits AS (
	SELECT LocationUnitNumber, SubLocationUnitNumber, CoverageLimitType, o_Limit AS CoverageLimitValue, in_SourceSystemID, in_TableCode1, in_pol_key AS original_pol_key, in_RCStatCoverageAKId AS original_coverageakid, in_LiabilityLimitAddOnFlag AS LiabilityLimitAddOnFlag, LimitHashKey
	FROM EXP_SCSetHashKey
	UNION
	SELECT LocationUnitNumber, SubLocationUnitNumber, CoverageLimitType, o_CoverageLimitValue AS CoverageLimitValue, inSourceSystemID AS in_SourceSystemID, in_TableCode1, in_pol_key AS original_pol_key, in_RCStatCoverageAKId AS original_coverageakid, in_LiabilityLimitAddOnFlag AS LiabilityLimitAddOnFlag, LimitHashKey
	FROM EXP_RC_SetHashCode
),
EXP_ConsolodateLimitOutput AS (
	SELECT
	LocationUnitNumber,
	SubLocationUnitNumber,
	CoverageLimitType,
	-- *INF*: IIF(ISNULL(CoverageLimitType),'N/A',CoverageLimitType)
	IFF(CoverageLimitType IS NULL, 'N/A', CoverageLimitType) AS o_CoverageLimitType,
	CoverageLimitValue,
	-- *INF*: IIF(isnull(CoverageLimitValue),'0',CoverageLimitValue)
	IFF(CoverageLimitValue IS NULL, '0', CoverageLimitValue) AS o_CoverageLimitValue,
	in_SourceSystemID,
	in_TableCode1,
	original_pol_key,
	original_coverageakid,
	LiabilityLimitAddOnFlag,
	LimitHashKey
	FROM Union_Limits
),
EXP_ApplyLimitRules AS (
	SELECT
	original_pol_key AS pol_key,
	LocationUnitNumber,
	SubLocationUnitNumber,
	o_CoverageLimitType AS CoverageLimitType,
	o_CoverageLimitValue AS CoverageLimitValue,
	-- *INF*: DECODE(TRUE,
	-- isnull(CoverageLimitValue),'0',
	-- LiabilityLimitAddOnFlag='Y' and CoverageLimitType ='EachRelatedWrongfulEmploymentPractice',CoverageLimitValue,
	-- in_TableCode1='2','0',
	-- CoverageLimitValue
	-- )
	-- 
	-- 
	-- -- if liability, unless it is an add on, then 0 this out, then we can assume all liability limits at coverage level going forward can be added to policy limits.
	DECODE(
	    TRUE,
	    CoverageLimitValue IS NULL, '0',
	    LiabilityLimitAddOnFlag = 'Y' and CoverageLimitType = 'EachRelatedWrongfulEmploymentPractice', CoverageLimitValue,
	    in_TableCode1 = '2', '0',
	    CoverageLimitValue
	) AS o_CoverageLimitValue,
	original_coverageakid AS RCStatCoverageAKID,
	in_SourceSystemID,
	in_TableCode1,
	-- *INF*: IIF(in_TableCode1='3','SUM','MAX')
	-- 
	-- -- need decode logic to set MAX,SUM
	IFF(in_TableCode1 = '3', 'SUM', 'MAX') AS LimitRule,
	LiabilityLimitAddOnFlag,
	LimitHashKey
	FROM EXP_ConsolodateLimitOutput
),
SRT_Limits AS (
	SELECT
	pol_key, 
	RCStatCoverageAKID, 
	LocationUnitNumber, 
	SubLocationUnitNumber, 
	CoverageLimitType, 
	o_CoverageLimitValue AS CoverageLimitValue, 
	in_SourceSystemID, 
	in_TableCode1, 
	LimitRule, 
	LimitHashKey
	FROM EXP_ApplyLimitRules
	ORDER BY pol_key ASC, RCStatCoverageAKID ASC, LocationUnitNumber ASC, SubLocationUnitNumber ASC, in_SourceSystemID ASC, in_TableCode1 ASC, LimitRule ASC
),
AGG_LimitsSubTotalLocationLevel AS (
	SELECT
	pol_key,
	RCStatCoverageAKID,
	LocationUnitNumber,
	SubLocationUnitNumber,
	CoverageLimitType,
	CoverageLimitValue,
	-- *INF*: DECODE(TRUE,
	-- LimitRule='MAX', MAX(TO_INTEGER(CoverageLimitValue)),
	-- LimitRule='SUM', SUM(TO_INTEGER(CoverageLimitValue)),
	-- TO_INTEGER(CoverageLimitValue)
	-- )
	DECODE(
	    TRUE,
	    LimitRule = 'MAX', MAX(CAST(CoverageLimitValue AS INTEGER)),
	    LimitRule = 'SUM', SUM(CAST(CoverageLimitValue AS INTEGER)),
	    CAST(CoverageLimitValue AS INTEGER)
	) AS o_CoverageLimitValue,
	in_SourceSystemID,
	in_TableCode1,
	LimitRule,
	LimitHashKey
	FROM SRT_Limits
	GROUP BY pol_key, RCStatCoverageAKID, LocationUnitNumber, SubLocationUnitNumber, in_SourceSystemID, in_TableCode1, LimitRule
),
EXP_LimitSubTotalView AS (
	SELECT
	pol_key,
	LocationUnitNumber,
	SubLocationUnitNumber,
	CoverageLimitType,
	o_CoverageLimitValue AS CoverageLimitValue,
	RCStatCoverageAKID,
	in_SourceSystemID,
	in_TableCode1,
	LimitRule,
	LimitHashKey
	FROM AGG_LimitsSubTotalLocationLevel
),
AGG_LimitTotalPolicyCoverage AS (
	SELECT
	pol_key,
	RCStatCoverageAKID,
	LocationUnitNumber,
	SubLocationUnitNumber,
	CoverageLimitType,
	CoverageLimitValue,
	-- *INF*: sum(CoverageLimitValue)
	sum(CoverageLimitValue) AS o_CoverageLimitValue,
	in_SourceSystemID,
	in_TableCode1,
	LimitRule,
	LimitHashKey
	FROM EXP_LimitSubTotalView
	GROUP BY pol_key, RCStatCoverageAKID
),
LKP_CoverageDeductibleActive AS (
	SELECT
	pol_key,
	RatingCoverageAKId,
	LocationUnitNumber,
	SubLocationUnitNumber,
	CoverageDeductibleValue,
	in_pol_key,
	in_RCStatCoverageAKId,
	in_SourceSystemID,
	in_TableCode1
	FROM (
		select distinct 
		POL.pol_key as pol_key,
		RC.RatingCoverageAKId as RatingCoverageAKId,
		RL.LocationUnitNumber as LocationUnitNumber,
		RC.SubLocationUnitNumber as SubLocationUnitNumber,
		Max(CoverageDeductibleValue) as CoverageDeductibleValue
		FROM
		@{pipeline().parameters.DATABASE_NAME_IL}.dbo.PremiumTransaction PT
		inner join @{pipeline().parameters.DATABASE_NAME_IL}.dbo.RatingCoverage RC
		on PT.RatingCoverageAKID=RC.RatingCoverageAKID
		and RC.EffectiveDate=PT.EffectiveDate 
		inner join @{pipeline().parameters.DATABASE_NAME_IL}.dbo.PolicyCoverage PC
		on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
		and PC.CurrentSnapshotFlag=1
		inner join @{pipeline().parameters.DATABASE_NAME_IL}.dbo.RiskLocation RL
		on PC.RiskLocationAKID=RL.RiskLocationAKID
		and RL.CurrentSnapshotFlag=1
		inner join @{pipeline().parameters.DATABASE_NAME_IL}.V2.policy POL
		on POL.pol_ak_id=RL.PolicyAKID
		and POL.crrnt_snpsht_flag=1
		inner join @{pipeline().parameters.DATABASE_NAME_IL}.dbo.CoverageDeductibleBridge CDB
		on CDB.PremiumTransactionAKId = PT.PremiumTransactionAKID
		inner join @{pipeline().parameters.DATABASE_NAME_IL}.dbo.CoverageDeductible CD 
		on CDB.CoverageDeductibleId = CD.CoverageDeductibleId
		WHERE 
		isnumeric(CD.CoverageDeductibleValue)=1
		@{pipeline().parameters.WHERE_DEDUCTIBLE}
		group by RL.LocationUnitNumber,  RC.SubLocationUnitNumber, POL.pol_key,  RC.RatingCoverageAKId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key,RatingCoverageAKId ORDER BY pol_key) = 1
),
EXP_Deductible_out AS (
	SELECT
	LocationUnitNumber,
	SubLocationUnitNumber,
	CoverageDeductibleValue,
	-- *INF*: IIF(ISNULL(CoverageDeductibleValue),'0',CoverageDeductibleValue)
	IFF(CoverageDeductibleValue IS NULL, '0', CoverageDeductibleValue) AS o_CoverageDeductibleValue,
	in_pol_key AS pol_key,
	in_RCStatCoverageAKId AS RCStatCoverageAKId,
	in_SourceSystemID AS SourceSystemID,
	in_TableCode1 AS TableCode1
	FROM LKP_CoverageDeductibleActive
),
AGG_Deductible AS (
	SELECT
	pol_key,
	RCStatCoverageAKId,
	LocationUnitNumber,
	SubLocationUnitNumber,
	o_CoverageDeductibleValue AS CoverageDeductibleValue,
	-- *INF*: TO_INTEGER(CoverageDeductibleValue)
	CAST(CoverageDeductibleValue AS INTEGER) AS o_CoverageDeductibleValue,
	SourceSystemID,
	TableCode1
	FROM EXP_Deductible_out
	GROUP BY pol_key, RCStatCoverageAKId
),
SRT_Deductible AS (
	SELECT
	pol_key, 
	RCStatCoverageAKId, 
	SourceSystemID, 
	TableCode1, 
	o_CoverageDeductibleValue AS CoverageDeductibleValue
	FROM AGG_Deductible
	ORDER BY pol_key ASC, RCStatCoverageAKId ASC, SourceSystemID ASC, TableCode1 ASC
),
LKP_PremiumDCT AS (
	SELECT
	sumPremiumTransactionAmount,
	sumChangeInEarnedPremium,
	YEAR,
	PolicyKey,
	RatingCoverageAKId
	FROM (
		select 
		sum(PremiumTransactionAmount) as sumPremiumTransactionAmount, 
		sum(ChangeInEarnedPremium) as sumChangeInEarnedPremium, 
		year(RunDate) as YEAR,
		PolicyKey as PolicyKey,
		RatingCoverageAKID as RatingCoverageAKID
		From
		(
		SELECT 
		EarnedPremiumMonthlyCalculation.PremiumTransactionAmount, 
		EarnedPremiumMonthlyCalculation.ChangeInEarnedPremium, 
		EarnedPremiumMonthlyCalculation.PolicyKey as PolicyKey, 
		EarnedPremiumMonthlyCalculation.RatingCoverageAKID as RatingCoverageAKID ,
		EarnedPremiumMonthlyCalculation.RunDate
		from 
		@{pipeline().parameters.DATABASE_NAME_IL}.dbo.EarnedPremiumMonthlyCalculation  with (nolock)
		where RatingCoverageAKId !=-1 and year(RunDate)=@{pipeline().parameters.YEAR} 
		--and PolicyEffectiveDate != StatisticalCoverageCancellationDate
		and PremiumType='D'
		@{pipeline().parameters.WHERE_PREM_DCT}
		) A
		group by year(RunDate),PolicyKey,RatingCoverageAKID
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,RatingCoverageAKId ORDER BY sumPremiumTransactionAmount) = 1
),
LKP_PremiumPMS AS (
	SELECT
	sumPremiumTransactionAmount,
	sumChangeInEarnedPremium,
	YEAR,
	PolicyKey,
	StatisticalCoverageAKID
	FROM (
		select 
		sum(PremiumTransactionAmount) as sumPremiumTransactionAmount, 
		sum(ChangeInEarnedPremium) as sumChangeInEarnedPremium, 
		year(RunDate) as YEAR,
		PolicyKey as PolicyKey,
		StatisticalCoverageAKID as StatisticalCoverageAKID
		From
		(
		SELECT 
		EarnedPremiumMonthlyCalculation.PremiumTransactionAmount, 
		EarnedPremiumMonthlyCalculation.ChangeInEarnedPremium, 
		EarnedPremiumMonthlyCalculation.PolicyKey as PolicyKey, 
		EarnedPremiumMonthlyCalculation.StatisticalCoverageAKID as StatisticalCoverageAKID,
		EarnedPremiumMonthlyCalculation.RatingCoverageAKId as RatingCoverageAKId,
		EarnedPremiumMonthlyCalculation.RunDate
		from 
		@{pipeline().parameters.DATABASE_NAME_IL}.dbo.EarnedPremiumMonthlyCalculation  with (nolock)
		where
		RatingCoverageAKId=-1 and year(RunDate)=@{pipeline().parameters.YEAR} 
		--and PolicyEffectiveDate != StatisticalCoverageCancellationDate
		and PremiumType='D'
		@{pipeline().parameters.WHERE_PREM_PMS}
		) A
		group by year(RunDate),PolicyKey,StatisticalCoverageAKID
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,StatisticalCoverageAKID ORDER BY sumPremiumTransactionAmount) = 1
),
EXP_ConsolodatePremium AS (
	SELECT
	EXP_BreakOutCalculations.pol_key,
	EXP_BreakOutCalculations.RCStatCoverageAKId,
	EXP_BreakOutCalculations.SourceSystemID,
	EXP_BreakOutCalculations.TableCode1,
	EXP_BreakOutCalculations.PolicyEffectiveDateYear AS YEAR,
	EXP_BreakOutCalculations.STABBR,
	EXP_BreakOutCalculations.COCODE,
	EXP_BreakOutCalculations.COTYPE,
	EXP_BreakOutCalculations.LOB,
	EXP_BreakOutCalculations.ZIP,
	EXP_BreakOutCalculations.POLCAT,
	EXP_BreakOutCalculations.COVERAGE,
	EXP_BreakOutCalculations.IND_CODE_TYPE,
	EXP_BreakOutCalculations.CODE,
	EXP_BreakOutCalculations.POLTYPE,
	EXP_BreakOutCalculations.COVTYPE,
	1 AS POLNUM,
	EXP_BreakOutCalculations.o_Location_Counter AS ESTNUM,
	0 AS Limit,
	LKP_PremiumPMS.sumPremiumTransactionAmount AS PMSsumPremiumTransactionAmount,
	LKP_PremiumPMS.sumChangeInEarnedPremium AS PMSsumChangeInEarnedPremium,
	LKP_PremiumDCT.sumPremiumTransactionAmount AS DCTsumPremiumTransactionAmount,
	LKP_PremiumDCT.sumChangeInEarnedPremium AS DCTsumChangeInEarnedPremium,
	-- *INF*: DECODE(True,
	-- SourceSystemID='PMS' and not ISNULL( PMSsumPremiumTransactionAmount),PMSsumPremiumTransactionAmount,
	-- SourceSystemID='DCT' and not ISNULL(DCTsumPremiumTransactionAmount),DCTsumPremiumTransactionAmount,
	-- 0
	-- )
	DECODE(
	    True,
	    SourceSystemID = 'PMS' and PMSsumPremiumTransactionAmount IS NOT NULL, PMSsumPremiumTransactionAmount,
	    SourceSystemID = 'DCT' and DCTsumPremiumTransactionAmount IS NOT NULL, DCTsumPremiumTransactionAmount,
	    0
	) AS DirectWrittenPremium,
	-- *INF*: DECODE(True,
	-- SourceSystemID='PMS' and not isnull(PMSsumChangeInEarnedPremium),PMSsumChangeInEarnedPremium,
	-- SourceSystemID='DCT' and not isnull(DCTsumChangeInEarnedPremium),DCTsumChangeInEarnedPremium,
	-- 0
	-- )
	DECODE(
	    True,
	    SourceSystemID = 'PMS' and PMSsumChangeInEarnedPremium IS NOT NULL, PMSsumChangeInEarnedPremium,
	    SourceSystemID = 'DCT' and DCTsumChangeInEarnedPremium IS NOT NULL, DCTsumChangeInEarnedPremium,
	    0
	) AS EarnedPremium,
	EXP_BreakOutCalculations.TableCodeInsuranceLine,
	EXP_BreakOutCalculations.ReinsurancePercent,
	-- *INF*: DECODE(True,
	-- SourceSystemID='PMS' and not isnull(PMSsumChangeInEarnedPremium ),PMSsumChangeInEarnedPremium * ReinsurancePercent,
	-- SourceSystemID='DCT' and not isnull(DCTsumChangeInEarnedPremium),DCTsumChangeInEarnedPremium * ReinsurancePercent,
	-- 0
	-- )
	DECODE(
	    True,
	    SourceSystemID = 'PMS' and PMSsumChangeInEarnedPremium IS NOT NULL, PMSsumChangeInEarnedPremium * ReinsurancePercent,
	    SourceSystemID = 'DCT' and DCTsumChangeInEarnedPremium IS NOT NULL, DCTsumChangeInEarnedPremium * ReinsurancePercent,
	    0
	) AS ReinsuranceEarnedPremium,
	EXP_BreakOutCalculations.terrorism_risk_ind,
	EXP_BreakOutCalculations.ExtractDate,
	EXP_BreakOutCalculations.AuditID,
	EXP_BreakOutCalculations.IsBlackListCoverage
	FROM EXP_BreakOutCalculations
	LEFT JOIN LKP_PremiumDCT
	ON LKP_PremiumDCT.PolicyKey = EXP_BreakOutCalculations.pol_key AND LKP_PremiumDCT.RatingCoverageAKId = EXP_BreakOutCalculations.RCStatCoverageAKId
	LEFT JOIN LKP_PremiumPMS
	ON LKP_PremiumPMS.PolicyKey = EXP_BreakOutCalculations.pol_key AND LKP_PremiumPMS.StatisticalCoverageAKID = EXP_BreakOutCalculations.RCStatCoverageAKId
),
SRT_Premium AS (
	SELECT
	pol_key, 
	RCStatCoverageAKId, 
	SourceSystemID, 
	TableCode1, 
	YEAR, 
	STABBR, 
	COCODE, 
	COTYPE, 
	LOB, 
	ZIP, 
	POLCAT, 
	COVERAGE, 
	IND_CODE_TYPE, 
	CODE, 
	POLTYPE, 
	COVTYPE, 
	POLNUM, 
	ESTNUM, 
	DirectWrittenPremium, 
	EarnedPremium, 
	TableCodeInsuranceLine, 
	ReinsurancePercent, 
	ReinsuranceEarnedPremium, 
	terrorism_risk_ind, 
	ExtractDate, 
	AuditID, 
	IsBlackListCoverage
	FROM EXP_ConsolodatePremium
	ORDER BY pol_key ASC, RCStatCoverageAKId ASC, SourceSystemID ASC, TableCode1 ASC
),
JNR_Premium_Deductible AS (SELECT
	SRT_Deductible.pol_key, 
	SRT_Deductible.RCStatCoverageAKId, 
	SRT_Deductible.CoverageDeductibleValue AS o_CoverageDeductibleValue, 
	SRT_Premium.pol_key AS pol_key1, 
	SRT_Premium.RCStatCoverageAKId AS RCStatCoverageAKId1, 
	SRT_Premium.SourceSystemID, 
	SRT_Premium.TableCode1, 
	SRT_Premium.YEAR, 
	SRT_Premium.STABBR, 
	SRT_Premium.COCODE, 
	SRT_Premium.COTYPE, 
	SRT_Premium.LOB, 
	SRT_Premium.ZIP, 
	SRT_Premium.POLCAT, 
	SRT_Premium.COVERAGE, 
	SRT_Premium.IND_CODE_TYPE, 
	SRT_Premium.CODE, 
	SRT_Premium.POLTYPE, 
	SRT_Premium.COVTYPE, 
	SRT_Premium.POLNUM, 
	SRT_Premium.ESTNUM, 
	SRT_Premium.DirectWrittenPremium, 
	SRT_Premium.EarnedPremium, 
	SRT_Premium.TableCodeInsuranceLine, 
	SRT_Premium.ReinsurancePercent, 
	SRT_Premium.ReinsuranceEarnedPremium, 
	SRT_Premium.terrorism_risk_ind, 
	SRT_Premium.ExtractDate, 
	SRT_Premium.AuditID, 
	SRT_Premium.IsBlackListCoverage
	FROM SRT_Deductible
	RIGHT OUTER JOIN SRT_Premium
	ON SRT_Premium.pol_key = SRT_Deductible.pol_key AND SRT_Premium.RCStatCoverageAKId = SRT_Deductible.RCStatCoverageAKId
),
JNR_LimitsAndPremiumns AS (SELECT
	AGG_LimitTotalPolicyCoverage.pol_key, 
	AGG_LimitTotalPolicyCoverage.o_CoverageLimitValue AS CoverageLimitValue, 
	AGG_LimitTotalPolicyCoverage.RCStatCoverageAKID, 
	AGG_LimitTotalPolicyCoverage.in_SourceSystemID, 
	AGG_LimitTotalPolicyCoverage.in_TableCode1, 
	AGG_LimitTotalPolicyCoverage.LimitHashKey, 
	JNR_Premium_Deductible.pol_key1, 
	JNR_Premium_Deductible.RCStatCoverageAKId1, 
	JNR_Premium_Deductible.SourceSystemID, 
	JNR_Premium_Deductible.TableCode1, 
	JNR_Premium_Deductible.YEAR, 
	JNR_Premium_Deductible.STABBR, 
	JNR_Premium_Deductible.COCODE, 
	JNR_Premium_Deductible.COTYPE, 
	JNR_Premium_Deductible.LOB, 
	JNR_Premium_Deductible.ZIP, 
	JNR_Premium_Deductible.POLCAT, 
	JNR_Premium_Deductible.COVERAGE, 
	JNR_Premium_Deductible.IND_CODE_TYPE, 
	JNR_Premium_Deductible.CODE, 
	JNR_Premium_Deductible.POLTYPE, 
	JNR_Premium_Deductible.COVTYPE, 
	JNR_Premium_Deductible.POLNUM, 
	JNR_Premium_Deductible.ESTNUM, 
	JNR_Premium_Deductible.DirectWrittenPremium, 
	JNR_Premium_Deductible.EarnedPremium, 
	JNR_Premium_Deductible.TableCodeInsuranceLine, 
	JNR_Premium_Deductible.ReinsurancePercent, 
	JNR_Premium_Deductible.ReinsuranceEarnedPremium, 
	JNR_Premium_Deductible.terrorism_risk_ind, 
	JNR_Premium_Deductible.ExtractDate, 
	JNR_Premium_Deductible.AuditID, 
	JNR_Premium_Deductible.IsBlackListCoverage, 
	JNR_Premium_Deductible.o_CoverageDeductibleValue AS CoverageDeductibleValue
	FROM JNR_Premium_Deductible
	LEFT OUTER JOIN AGG_LimitTotalPolicyCoverage
	ON AGG_LimitTotalPolicyCoverage.pol_key = JNR_Premium_Deductible.pol_key1 AND AGG_LimitTotalPolicyCoverage.RCStatCoverageAKID = JNR_Premium_Deductible.RCStatCoverageAKId1 AND AGG_LimitTotalPolicyCoverage.in_SourceSystemID = JNR_Premium_Deductible.SourceSystemID AND AGG_LimitTotalPolicyCoverage.in_TableCode1 = JNR_Premium_Deductible.TableCode1
),
EXP_JoinerOutput AS (
	SELECT
	pol_key1,
	RCStatCoverageAKId1,
	SourceSystemID,
	TableCode1,
	YEAR,
	STABBR,
	COCODE,
	COTYPE,
	LOB,
	ZIP,
	POLCAT,
	COVERAGE,
	IND_CODE_TYPE,
	CODE,
	-- *INF*: IIF(CODE='N/A','',CODE)
	-- 
	-- -- replace N/A with an empty string, we can't have nulls and NAICS doesn't want N/A
	IFF(CODE = 'N/A', '', CODE) AS o_CODE,
	POLTYPE,
	COVTYPE,
	POLNUM,
	ESTNUM,
	DirectWrittenPremium,
	EarnedPremium,
	TableCodeInsuranceLine,
	ReinsurancePercent,
	ReinsuranceEarnedPremium,
	terrorism_risk_ind,
	ExtractDate,
	AuditID,
	CoverageLimitValue,
	-- *INF*: IIF(ISNULL(CoverageLimitValue),0,CoverageLimitValue)
	IFF(CoverageLimitValue IS NULL, 0, CoverageLimitValue) AS v_CoverageLimitValue,
	-- *INF*: DECODE(TRUE,
	-- TableCodeInsuranceLine='Liability' and COVERAGE='04',:LKP.LKP_POLICYLIMITS_UMBRELLA(pol_key1),
	-- TableCodeInsuranceLine='Liability',:LKP.LKP_POLICYLIMITS(pol_key1),
	-- 'N/A')
	-- 
	-- --IIF(TableCodeInsuranceLine='Liability',:LKP.LKP_POLICYLIMITS(pol_key1),'N/A')
	DECODE(
	    TRUE,
	    TableCodeInsuranceLine = 'Liability' and COVERAGE = '04', LKP_POLICYLIMITS_UMBRELLA_pol_key1.PolicyPerOccurenceLimit,
	    TableCodeInsuranceLine = 'Liability', LKP_POLICYLIMITS_pol_key1.PolicyPerOccurenceLimit,
	    'N/A'
	) AS v_Lkp_PolicyLimitValue,
	v_Lkp_PolicyLimitValue AS o_Lkp_PolicyLimitValue,
	v_CoverageLimitValue AS o_CoverageLimitValue,
	-- *INF*: DECODE(TRUE,
	-- TableCodeInsuranceLine='Liability' and IS_NUMBER(v_Lkp_PolicyLimitValue) and v_CoverageLimitValue=0,to_integer(v_Lkp_PolicyLimitValue),
	-- v_CoverageLimitValue)
	DECODE(
	    TRUE,
	    TableCodeInsuranceLine = 'Liability' and REGEXP_LIKE(v_Lkp_PolicyLimitValue, '^[0-9]+$') and v_CoverageLimitValue = 0, CAST(v_Lkp_PolicyLimitValue AS INTEGER),
	    v_CoverageLimitValue
	) AS o_CoverageLimitValue_SubTotal,
	IsBlackListCoverage,
	-- *INF*: IIF(SourceSystemID='PMS' and NOT isnull(:LKP.LKP_STATCOVCLASS11111(RCStatCoverageAKId1)),'TRUE' ,'FALSE')
	IFF(
	    SourceSystemID = 'PMS'
	    and LKP_STATCOVCLASS11111_RCStatCoverageAKId1.StatisticalCoverageID IS NOT NULL,
	    'TRUE',
	    'FALSE'
	) AS v_PMSPlusPakClassCode,
	-- *INF*: IIF(v_PMSPlusPakClassCode='TRUE','Y',IsBlackListCoverage)
	-- 
	-- --override the policy blacklist value for this record if it happens to be a pms plus pak coverage record.  Blacklist means do not count it in ther terrorism calcs
	IFF(v_PMSPlusPakClassCode = 'TRUE', 'Y', IsBlackListCoverage) AS o_BlackListCoverage,
	-- *INF*: DECODE(TRUE,
	-- TableCodeInsuranceLine='Liability' and IS_NUMBER(v_Lkp_PolicyLimitValue) and v_CoverageLimitValue!=0,to_integer(v_Lkp_PolicyLimitValue) + v_CoverageLimitValue,
	-- 
	-- TableCodeInsuranceLine='Liability' and COVERAGE='04' and is_number(:LKP.LKP_POLICYLIMITS(pol_key1)) and is_number(v_Lkp_PolicyLimitValue), to_integer(:LKP.LKP_POLICYLIMITS(pol_key1))+ to_integer(v_Lkp_PolicyLimitValue),
	-- 
	--  IS_NUMBER(v_Lkp_PolicyLimitValue),to_integer(v_Lkp_PolicyLimitValue),
	-- 0
	-- )
	-- 
	-- -- case 1 is for EPLI coverage to be added to policy limit
	-- -- case 2 is for commercial umbrella to combine with GL policy limit
	-- -- case three is for all the other GL that just return a policy limit
	DECODE(
	    TRUE,
	    TableCodeInsuranceLine = 'Liability' and REGEXP_LIKE(v_Lkp_PolicyLimitValue, '^[0-9]+$') and v_CoverageLimitValue != 0, CAST(v_Lkp_PolicyLimitValue AS INTEGER) + v_CoverageLimitValue,
	    TableCodeInsuranceLine = 'Liability' and COVERAGE = '04' and REGEXP_LIKE(LKP_POLICYLIMITS_pol_key1.PolicyPerOccurenceLimit, '^[0-9]+$') and REGEXP_LIKE(v_Lkp_PolicyLimitValue, '^[0-9]+$'), CAST(LKP_POLICYLIMITS_pol_key1.PolicyPerOccurenceLimit AS INTEGER) + CAST(v_Lkp_PolicyLimitValue AS INTEGER),
	    REGEXP_LIKE(v_Lkp_PolicyLimitValue, '^[0-9]+$'), CAST(v_Lkp_PolicyLimitValue AS INTEGER),
	    0
	) AS o_CoverageLimitValue_GL_State_Override,
	LimitHashKey,
	CoverageDeductibleValue AS in_CoverageDeductibleValue,
	-- *INF*: IIF(ISNULL(in_CoverageDeductibleValue),0,in_CoverageDeductibleValue)
	IFF(in_CoverageDeductibleValue IS NULL, 0, in_CoverageDeductibleValue) AS o_CoverageDeductibleValue
	FROM JNR_LimitsAndPremiumns
	LEFT JOIN LKP_POLICYLIMITS_UMBRELLA LKP_POLICYLIMITS_UMBRELLA_pol_key1
	ON LKP_POLICYLIMITS_UMBRELLA_pol_key1.pol_key = pol_key1

	LEFT JOIN LKP_POLICYLIMITS LKP_POLICYLIMITS_pol_key1
	ON LKP_POLICYLIMITS_pol_key1.pol_key = pol_key1

	LEFT JOIN LKP_STATCOVCLASS11111 LKP_STATCOVCLASS11111_RCStatCoverageAKId1
	ON LKP_STATCOVCLASS11111_RCStatCoverageAKId1.StatisticalCoverageAKID = RCStatCoverageAKId1

),
EXP_Apply_GLOverrideRule AS (
	SELECT
	pol_key1,
	RCStatCoverageAKId1,
	SourceSystemID,
	TableCode1,
	YEAR,
	STABBR,
	COCODE,
	COTYPE,
	LOB,
	ZIP,
	POLCAT,
	COVERAGE,
	IND_CODE_TYPE,
	o_CODE AS CODE,
	POLTYPE,
	COVTYPE,
	POLNUM,
	ESTNUM,
	DirectWrittenPremium,
	EarnedPremium,
	TableCodeInsuranceLine,
	ReinsurancePercent,
	ReinsuranceEarnedPremium,
	terrorism_risk_ind,
	ExtractDate,
	AuditID,
	o_CoverageLimitValue_SubTotal AS CoverageLimitValue,
	o_BlackListCoverage AS IsBlackListCoverage,
	o_CoverageLimitValue_GL_State_Override AS CoverageLimitValue_GL_State_Override,
	LimitHashKey
	FROM EXP_JoinerOutput
),
RTR_RouteByLines AS (
	SELECT
	pol_key1 AS pol_key,
	RCStatCoverageAKId1 AS RCStatCoverageAKId,
	SourceSystemID,
	TableCode1,
	YEAR,
	STABBR,
	COCODE,
	COTYPE,
	LOB,
	ZIP,
	POLCAT,
	COVERAGE,
	IND_CODE_TYPE,
	CODE,
	POLTYPE,
	COVTYPE,
	POLNUM,
	ESTNUM,
	CoverageLimitValue AS Limit,
	DirectWrittenPremium,
	EarnedPremium,
	TableCodeInsuranceLine,
	ReinsurancePercent,
	ReinsuranceEarnedPremium,
	terrorism_risk_ind,
	ExtractDate,
	AuditID,
	IsBlackListCoverage,
	CoverageLimitValue_GL_State_Override AS CoverageLimitValueGLPolicyOverrideState,
	LimitHashKey
	FROM EXP_Apply_GLOverrideRule
),
RTR_RouteByLines_Property AS (SELECT * FROM RTR_RouteByLines WHERE TableCode1='1' and substr(COVERAGE,1,3) !='N/A'),
RTR_RouteByLines_LIability AS (SELECT * FROM RTR_RouteByLines WHERE TableCode1='2' and substr(COVERAGE,1,3) !='N/A'),
RTR_RouteByLines_InlandMarine AS (SELECT * FROM RTR_RouteByLines WHERE TableCode1='3'),
EXP_InlandMarineBreakout AS (
	SELECT
	pol_key,
	RCStatCoverageAKId,
	SourceSystemID,
	TableCode1,
	YEAR,
	STABBR,
	COCODE,
	COTYPE,
	LOB,
	ZIP,
	POLCAT,
	COVERAGE,
	IND_CODE_TYPE,
	CODE,
	POLTYPE,
	COVTYPE,
	pol_key AS v_current_pol_key,
	-- *INF*: iif(v_current_pol_key=v_previous_pol_key,0,1)
	-- 
	-- -- counter for unique policies- assign 1 to a unique policies, 0 to the rest.
	IFF(v_current_pol_key = v_previous_pol_key, 0, 1) AS POLNUM,
	ESTNUM,
	Limit,
	DirectWrittenPremium,
	EarnedPremium,
	TableCodeInsuranceLine,
	ReinsurancePercent,
	ReinsuranceEarnedPremium,
	terrorism_risk_ind,
	ExtractDate,
	AuditID,
	v_current_pol_key AS v_previous_pol_key,
	IsBlackListCoverage AS IsBlackListCoverage4
	FROM RTR_RouteByLines_InlandMarine
),
AGG_IMPolicyTerrorismIndLevel AS (
	SELECT
	pol_key,
	TableCode1,
	YEAR,
	STABBR,
	COCODE,
	COTYPE,
	LOB,
	ZIP,
	POLCAT,
	COVERAGE,
	IND_CODE_TYPE,
	CODE,
	POLTYPE,
	COVTYPE,
	Limit,
	DirectWrittenPremium,
	EarnedPremium,
	ReinsuranceEarnedPremium,
	terrorism_risk_ind,
	ExtractDate,
	AuditID,
	IsBlackListCoverage4 AS IsBlackListCoverage,
	-- *INF*: IIF(terrorism_risk_ind='Y'  and IsBlackListCoverage='N',sum(DirectWrittenPremium))
	IFF(terrorism_risk_ind = 'Y' and IsBlackListCoverage = 'N', sum(DirectWrittenPremium)) AS PRWTERR,
	-- *INF*: sum(DirectWrittenPremium)
	sum(DirectWrittenPremium) AS PRWTOT,
	-- *INF*: IIF(terrorism_risk_ind='Y' and IsBlackListCoverage='N',sum(Limit))
	IFF(terrorism_risk_ind = 'Y' and IsBlackListCoverage = 'N', sum(Limit)) AS LIMITSTERR,
	-- *INF*: sum(Limit)
	sum(Limit) AS LIMITSTOT,
	-- *INF*: IIF(terrorism_risk_ind='Y' and IsBlackListCoverage='N',sum(EarnedPremium))
	IFF(terrorism_risk_ind = 'Y' and IsBlackListCoverage = 'N', sum(EarnedPremium)) AS PRETERR,
	-- *INF*: sum(EarnedPremium)
	sum(EarnedPremium) AS PRETOT,
	-- *INF*: sum(ReinsuranceEarnedPremium)
	sum(ReinsuranceEarnedPremium) AS GREINSPREM
	FROM EXP_InlandMarineBreakout
	GROUP BY pol_key, TableCode1, YEAR, STABBR, COCODE, COTYPE, LOB, ZIP, POLCAT, COVERAGE, IND_CODE_TYPE, CODE, POLTYPE, COVTYPE, terrorism_risk_ind, IsBlackListCoverage
),
SRT_IMBYPolicyZip AS (
	SELECT
	pol_key, 
	ZIP, 
	TableCode1, 
	YEAR, 
	STABBR, 
	COCODE, 
	COTYPE, 
	LOB, 
	POLCAT, 
	COVERAGE, 
	IND_CODE_TYPE, 
	CODE, 
	POLTYPE, 
	COVTYPE, 
	ExtractDate, 
	AuditID, 
	PRWTERR, 
	PRWTOT, 
	LIMITSTERR, 
	LIMITSTOT, 
	PRETERR, 
	PRETOT, 
	GREINSPREM
	FROM AGG_IMPolicyTerrorismIndLevel
	ORDER BY pol_key ASC, ZIP ASC
),
AGG_InlandMarineZIP AS (
	SELECT
	TableCode1 AS TableCode11,
	YEAR AS YEAR1,
	STABBR AS STABBR1,
	COCODE AS COCODE1,
	COTYPE AS COTYPE1,
	LOB AS LOB1,
	ZIP AS ZIP1,
	POLCAT AS POLCAT1,
	COVERAGE AS COVERAGE1,
	IND_CODE_TYPE AS IND_CODE_TYPE1,
	CODE AS CODE1,
	POLTYPE AS POLTYPE1,
	COVTYPE AS COVTYPE1,
	ExtractDate,
	AuditID,
	pol_key,
	-- *INF*: DECODE(TRUE,
	-- pol_key=v_previous_pol_key and ZIP1=v_previous_zip, v_POLNUM,
	-- pol_key!=v_previous_pol_key and ZIP1=v_previous_zip, v_POLNUM+1,
	-- 1
	-- )
	-- 
	-- 
	DECODE(
	    TRUE,
	    pol_key = v_previous_pol_key and ZIP1 = v_previous_zip, v_POLNUM,
	    pol_key != v_previous_pol_key and ZIP1 = v_previous_zip, v_POLNUM + 1,
	    1
	) AS v_POLNUM,
	pol_key AS v_previous_pol_key,
	ZIP1 AS v_previous_zip,
	v_POLNUM AS o_PolNum,
	PRWTERR AS in_PRWTERR,
	PRWTOT AS in_PRWTOT,
	LIMITSTERR AS in_LIMITSTERR,
	LIMITSTOT AS in_LIMITSTOT,
	PRETERR AS in_PRETERR,
	PRETOT AS in_PRETOT,
	GREINSPREM AS in_GREINSPREM,
	-- *INF*: sum(in_PRWTERR)
	sum(in_PRWTERR) AS PRWTERR,
	-- *INF*: sum(in_PRWTOT)
	sum(in_PRWTOT) AS PRWTOT,
	-- *INF*: sum(in_LIMITSTERR)
	sum(in_LIMITSTERR) AS LIMITSTERR,
	-- *INF*: sum(in_LIMITSTOT)
	sum(in_LIMITSTOT) AS LIMITSTOT,
	-- *INF*: sum(in_PRETERR)
	sum(in_PRETERR) AS PRETERR,
	-- *INF*: sum(in_PRETOT)
	sum(in_PRETOT) AS PRETOT,
	-- *INF*: sum(in_GREINSPREM)
	sum(in_GREINSPREM) AS GREINSPREM
	FROM SRT_IMBYPolicyZip
	GROUP BY TableCode11, YEAR1, STABBR1, COCODE1, COTYPE1, LOB1, ZIP1, POLCAT1, COVERAGE1, IND_CODE_TYPE1, CODE1, POLTYPE1, COVTYPE1
),
AGG_InlandMarineState AS (
	SELECT
	TableCode11,
	YEAR1,
	STABBR1,
	COCODE1,
	PRWTERR AS i_PRWTERR,
	-- *INF*: sum(i_PRWTERR)
	sum(i_PRWTERR) AS o_PRWTERR,
	PRWTOT AS i_PRWTOT,
	-- *INF*: sum(i_PRWTOT)
	sum(i_PRWTOT) AS o_PRWTOT,
	LIMITSTERR AS i_TIVTERR,
	-- *INF*: sum(i_TIVTERR)
	sum(i_TIVTERR) AS o_TIVTERR,
	LIMITSTOT AS i_TIVTOT,
	-- *INF*: sum(i_TIVTOT)
	sum(i_TIVTOT) AS o_TIVTOT,
	GREINSPREM AS i_GREINSPREM,
	-- *INF*: sum(i_GREINSPREM)
	sum(i_GREINSPREM) AS o_GREINSPREM,
	PRETERR AS i_PRETERR,
	-- *INF*: sum(i_PRETERR)
	sum(i_PRETERR) AS o_PRETERR,
	PRETOT AS i_PRETOT,
	-- *INF*: sum(i_PRETOT)
	sum(i_PRETOT) AS o_PRETOT,
	ExtractDate,
	AuditID
	FROM AGG_InlandMarineZIP
	GROUP BY TableCode11, YEAR1, STABBR1, COCODE1
),
EXP_InlandMarineStateOutput AS (
	SELECT
	'NaicTerrorismInlandMarineState' || @{pipeline().parameters.YEAR}||'.csv' AS Filename,
	TableCode11,
	TableCode11 || 'S' AS TableCode,
	YEAR1 AS YEAR,
	COCODE1 AS COCODE,
	STABBR1 AS STABBR,
	o_PRWTERR AS PRWTERR,
	o_PRWTOT AS PRWTOT,
	o_PRETERR AS PRETERR,
	o_PRETOT AS PRETOT,
	o_GREINSPREM AS GREINSPREM,
	o_TIVTERR AS TIVTERR,
	o_TIVTOT AS TIVTOT,
	ExtractDate,
	AuditID
	FROM AGG_InlandMarineState
),
NaicTerrorismInlandMarineStateExtractFile AS (
	INSERT INTO NaicTerrorismInlandMarineStateExtractFile
	(FileName, TableCode, YEAR, COCODE, STABBR, PRWTERR, PRWTOT, PRETERR, PRETOT, GREINSPREM, TIVTERR, TIVTOT)
	SELECT 
	Filename AS FILENAME, 
	TABLECODE, 
	YEAR, 
	COCODE, 
	STABBR, 
	PRWTERR, 
	PRWTOT, 
	PRETERR, 
	PRETOT, 
	GREINSPREM, 
	TIVTERR, 
	TIVTOT
	FROM EXP_InlandMarineStateOutput
),
EXP_LiabilityBreakout AS (
	SELECT
	pol_key,
	RCStatCoverageAKId,
	SourceSystemID AS SourceSystemID3,
	TableCode1,
	YEAR,
	STABBR,
	COCODE,
	COTYPE,
	LOB,
	ZIP,
	POLCAT,
	COVERAGE,
	IND_CODE_TYPE,
	CODE,
	POLTYPE,
	COVTYPE,
	ESTNUM,
	Limit,
	DirectWrittenPremium,
	EarnedPremium,
	TableCodeInsuranceLine,
	ReinsurancePercent,
	ReinsuranceEarnedPremium,
	terrorism_risk_ind,
	ExtractDate,
	AuditID,
	IsBlackListCoverage AS IsBlackListCoverage3,
	CoverageLimitValueGLPolicyOverrideState AS CoverageLimitValueGLPolicyOverrideState3
	FROM RTR_RouteByLines_LIability
),
EXP_LiabilityCheckPolicyLimits AS (
	SELECT
	pol_key,
	TableCode1,
	YEAR,
	STABBR,
	COCODE,
	COTYPE,
	LOB,
	ZIP,
	POLCAT,
	COVERAGE,
	IND_CODE_TYPE,
	CODE,
	POLTYPE,
	COVTYPE,
	Limit,
	DirectWrittenPremium,
	EarnedPremium,
	ReinsuranceEarnedPremium,
	terrorism_risk_ind,
	ExtractDate,
	AuditID,
	IsBlackListCoverage3,
	CoverageLimitValueGLPolicyOverrideState3
	FROM EXP_LiabilityBreakout
),
AGG_LiabilityPreZipPolicyLevelCalculateByBlacklistFlag AS (
	SELECT
	pol_key,
	TableCode1,
	YEAR,
	STABBR,
	COCODE,
	COTYPE,
	LOB,
	ZIP,
	POLCAT,
	COVERAGE,
	IND_CODE_TYPE,
	CODE,
	POLTYPE,
	COVTYPE,
	Limit,
	-- *INF*: IIF(terrorism_risk_ind='Y' and IsBlackListCoverage='N',MAX(Limit))
	IFF(terrorism_risk_ind = 'Y' and IsBlackListCoverage = 'N', MAX(Limit)) AS LIMITSTERR,
	-- *INF*: MAX(Limit)
	MAX(Limit) AS LIMITSTOT,
	DirectWrittenPremium,
	-- *INF*: IIF(terrorism_risk_ind='Y' and IsBlackListCoverage='N',sum(DirectWrittenPremium))
	IFF(terrorism_risk_ind = 'Y' and IsBlackListCoverage = 'N', sum(DirectWrittenPremium)) AS PRWTERR,
	-- *INF*: sum(DirectWrittenPremium)
	sum(DirectWrittenPremium) AS PRWTOT,
	EarnedPremium,
	-- *INF*: IIF(terrorism_risk_ind='Y' and IsBlackListCoverage='N',SUM(EarnedPremium))
	IFF(terrorism_risk_ind = 'Y' and IsBlackListCoverage = 'N', SUM(EarnedPremium)) AS PRETERR,
	-- *INF*: sum(EarnedPremium)
	sum(EarnedPremium) AS PRETOT,
	ReinsuranceEarnedPremium,
	-- *INF*: sum(ReinsuranceEarnedPremium)
	sum(ReinsuranceEarnedPremium) AS GREINSPREM,
	terrorism_risk_ind,
	ExtractDate,
	AuditID,
	IsBlackListCoverage3 AS IsBlackListCoverage,
	CoverageLimitValueGLPolicyOverrideState3 AS CoverageLimitValueGLPolicyOverrideState,
	-- *INF*: IIF(terrorism_risk_ind='Y' and IsBlackListCoverage='N',MAX(CoverageLimitValueGLPolicyOverrideState))
	IFF(
	    terrorism_risk_ind = 'Y' and IsBlackListCoverage = 'N',
	    MAX(CoverageLimitValueGLPolicyOverrideState)
	) AS LIMITSTERR_Override,
	-- *INF*: max(CoverageLimitValueGLPolicyOverrideState)
	max(CoverageLimitValueGLPolicyOverrideState) AS LIMITSTOT_Override
	FROM EXP_LiabilityCheckPolicyLimits
	GROUP BY pol_key, TableCode1, YEAR, STABBR, COCODE, COTYPE, LOB, ZIP, POLCAT, COVERAGE, IND_CODE_TYPE, CODE, POLTYPE, COVTYPE, Limit, DirectWrittenPremium, EarnedPremium, ReinsuranceEarnedPremium, terrorism_risk_ind, IsBlackListCoverage
),
AGG_LiabilityPreZipPolicyLevelTotals AS (
	SELECT
	pol_key,
	TableCode1,
	YEAR,
	STABBR,
	COCODE,
	COTYPE,
	LOB,
	ZIP,
	POLCAT,
	COVERAGE,
	IND_CODE_TYPE,
	CODE,
	POLTYPE,
	COVTYPE,
	1 AS PolicyNumber,
	LIMITSTERR AS in_LIMITSTERR,
	-- *INF*: max(in_LIMITSTERR)
	max(in_LIMITSTERR) AS o_LIMITSTERR,
	LIMITSTOT AS in_LIMITSTOT,
	-- *INF*: max(in_LIMITSTOT)
	max(in_LIMITSTOT) AS o_LIMITSTOT,
	PRWTERR AS in_PRWTERR,
	-- *INF*: sum(in_PRWTERR)
	sum(in_PRWTERR) AS o_PRWTERR,
	PRWTOT AS in_PRWTOT,
	-- *INF*: sum(in_PRWTOT)
	sum(in_PRWTOT) AS o_PRWTOT,
	PRETERR AS in_PRETERR,
	-- *INF*: sum(in_PRETERR)
	sum(in_PRETERR) AS o_PRETERR,
	PRETOT AS in_PRETOT,
	-- *INF*: sum(in_PRETOT)
	sum(in_PRETOT) AS o_PRETOT,
	GREINSPREM AS in_GREINSPREM,
	-- *INF*: sum(in_GREINSPREM)
	sum(in_GREINSPREM) AS o_GREINSPREM,
	ExtractDate,
	AuditID,
	LIMITSTERR_Override AS in_LIMITSTERR_Override,
	-- *INF*: max(in_LIMITSTERR_Override)
	max(in_LIMITSTERR_Override) AS o_LIMITSTERR_Override,
	LIMITSTOT_Override AS in_LIMITSTOT_Override,
	-- *INF*: max(in_LIMITSTOT_Override)
	max(in_LIMITSTOT_Override) AS o_LIMITSTOT_Override
	FROM AGG_LiabilityPreZipPolicyLevelCalculateByBlacklistFlag
	GROUP BY pol_key, TableCode1, YEAR, STABBR, COCODE, COTYPE, LOB, ZIP, POLCAT, COVERAGE, IND_CODE_TYPE, CODE, POLTYPE, COVTYPE
),
AGG_LiabilityZIP AS (
	SELECT
	TableCode1 AS TableCode11,
	YEAR AS YEAR1,
	STABBR AS STABBR1,
	COCODE AS COCODE1,
	COTYPE AS COTYPE1,
	LOB AS LOB1,
	ZIP AS ZIP1,
	POLCAT AS POLCAT1,
	COVERAGE AS COVERAGE1,
	IND_CODE_TYPE AS IND_CODE_TYPE1,
	CODE AS CODE1,
	POLTYPE AS POLTYPE1,
	COVTYPE AS COVTYPE1,
	ExtractDate,
	AuditID,
	PolicyNumber,
	-- *INF*: sum(PolicyNumber)
	sum(PolicyNumber) AS POLNUM_Count,
	o_LIMITSTERR AS in_LIMITSTERR,
	o_LIMITSTOT AS in_LIMITSTOT,
	o_PRWTERR AS in_PRWTERR,
	o_PRWTOT AS in_PRWTOT,
	o_GREINSPREM AS in_GREINSPREM,
	o_PRETERR AS in_PRETERR,
	o_PRETOT AS in_PRETOT,
	-- *INF*: sum(in_LIMITSTERR)
	sum(in_LIMITSTERR) AS out_LIMITSTERR,
	-- *INF*: sum(in_LIMITSTOT)
	sum(in_LIMITSTOT) AS out_LIMITSTOT,
	-- *INF*: sum(in_PRWTERR)
	sum(in_PRWTERR) AS out_PRWTERR,
	-- *INF*: sum(in_PRWTOT)
	sum(in_PRWTOT) AS out_PRWTOT,
	-- *INF*: sum(in_GREINSPREM)
	sum(in_GREINSPREM) AS out_GREINSPREM,
	-- *INF*: sum(in_PRETERR)
	sum(in_PRETERR) AS out_PRETERR,
	-- *INF*: sum(in_PRETOT)
	sum(in_PRETOT) AS out_PRETOT
	FROM AGG_LiabilityPreZipPolicyLevelTotals
	GROUP BY TableCode11, YEAR1, STABBR1, COCODE1, COTYPE1, LOB1, ZIP1, POLCAT1, COVERAGE1, IND_CODE_TYPE1, CODE1, POLTYPE1, COVTYPE1
),
EXP_LiabilityZipOutput AS (
	SELECT
	'NaicTerrorismLiabilityZip' || @{pipeline().parameters.YEAR}||'.csv' AS Filename,
	TableCode11 AS i_TableCode,
	i_TableCode || 'D' AS TableCode,
	YEAR1 AS YEAR,
	COCODE1 AS COCODE,
	COTYPE1 AS COTYPE,
	LOB1 AS LOB,
	STABBR1 AS STABBR,
	ZIP1 AS ZIP,
	POLCAT1 AS POLCAT,
	COVERAGE1 AS COVERAGE,
	IND_CODE_TYPE1 AS IND_CODE_TYPE,
	CODE1 AS CODE,
	POLTYPE1 AS POLTYPE,
	COVTYPE1 AS COVTYPE,
	POLNUM_Count AS POLNUM,
	out_PRWTERR AS PRWTERR,
	out_PRWTOT AS PRWTOT,
	out_LIMITSTERR AS LIMITSTERR,
	out_LIMITSTOT AS LIMITSTOT,
	ExtractDate,
	AuditID
	FROM AGG_LiabilityZIP
),
NAICTerrorismZipLevelExtract_Liability AS (
	INSERT INTO NAICTerrorismZipLevelExtract
	(AuditId, CreatedDate, ModifiedDate, TableCode, Year, CoCode, CoType, Lob, Stabbr, Zip, PolCat, IndCodeType, Code, PolType, CovType, PrwTerr, PrwTot, Coverage, LimitsTerr, LimitsTot, PolNum)
	SELECT 
	AuditID AS AUDITID, 
	ExtractDate AS CREATEDDATE, 
	ExtractDate AS MODIFIEDDATE, 
	TABLECODE, 
	YEAR AS YEAR, 
	COCODE AS COCODE, 
	COTYPE AS COTYPE, 
	LOB AS LOB, 
	STABBR AS STABBR, 
	ZIP AS ZIP, 
	POLCAT AS POLCAT, 
	IND_CODE_TYPE AS INDCODETYPE, 
	CODE AS CODE, 
	POLTYPE AS POLTYPE, 
	COVTYPE AS COVTYPE, 
	PRWTERR AS PRWTERR, 
	PRWTOT AS PRWTOT, 
	COVERAGE AS COVERAGE, 
	LIMITSTERR AS LIMITSTERR, 
	LIMITSTOT AS LIMITSTOT, 
	POLNUM AS POLNUM
	FROM EXP_LiabilityZipOutput
),
EXP_InlandMarineZipOutput AS (
	SELECT
	'NaicTerrorismInlandMarineZip' || @{pipeline().parameters.YEAR}||'.csv' AS Filename,
	TableCode11 AS i_TableCode,
	i_TableCode || 'D' AS TableCode,
	YEAR1 AS YEAR,
	COCODE1 AS COCODE,
	COTYPE1 AS COTYPE,
	LOB1 AS LOB,
	STABBR1 AS STABBR,
	ZIP1 AS ZIP,
	POLCAT1 AS POLCAT,
	IND_CODE_TYPE1 AS IND_CODE_TYPE,
	CODE1 AS CODE,
	POLTYPE1 AS POLTYPE,
	COVTYPE1 AS COVTYPE,
	PRWTERR,
	PRWTOT,
	LIMITSTERR,
	LIMITSTOT,
	ExtractDate,
	AuditID,
	o_PolNum
	FROM AGG_InlandMarineZIP
),
NAICTerrorismZipLevelExtract_InlandMarine AS (
	INSERT INTO NAICTerrorismZipLevelExtract
	(AuditId, CreatedDate, ModifiedDate, TableCode, Year, CoCode, CoType, Lob, Stabbr, Zip, PolCat, IndCodeType, Code, PolType, CovType, PrwTerr, PrwTot, LimitsTerr, LimitsTot, PolNum)
	SELECT 
	AuditID AS AUDITID, 
	ExtractDate AS CREATEDDATE, 
	ExtractDate AS MODIFIEDDATE, 
	TABLECODE, 
	YEAR AS YEAR, 
	COCODE AS COCODE, 
	COTYPE AS COTYPE, 
	LOB AS LOB, 
	STABBR AS STABBR, 
	ZIP AS ZIP, 
	POLCAT AS POLCAT, 
	IND_CODE_TYPE AS INDCODETYPE, 
	CODE AS CODE, 
	POLTYPE AS POLTYPE, 
	COVTYPE AS COVTYPE, 
	PRWTERR AS PRWTERR, 
	PRWTOT AS PRWTOT, 
	LIMITSTERR AS LIMITSTERR, 
	LIMITSTOT AS LIMITSTOT, 
	o_PolNum AS POLNUM
	FROM EXP_InlandMarineZipOutput
),
AGG_LiabilityPreStatePolicyLevelTotals AS (
	SELECT
	pol_key,
	TableCode1,
	YEAR,
	STABBR,
	COCODE,
	o_PRWTERR AS in_PRWTERR,
	-- *INF*: sum(in_PRWTERR)
	sum(in_PRWTERR) AS out_PRWTERR,
	o_PRWTOT AS in_PRWTOT,
	-- *INF*: sum(in_PRWTOT)
	sum(in_PRWTOT) AS out_PRWTOT,
	o_PRETERR AS in_PRETERR,
	-- *INF*: sum(in_PRETERR)
	sum(in_PRETERR) AS out_PRETERR,
	o_PRETOT AS in_PRETOT,
	-- *INF*: sum(in_PRETOT)
	sum(in_PRETOT) AS out_PRETOT,
	o_LIMITSTERR_Override AS in_LIMITSTERR,
	-- *INF*: max(in_LIMITSTERR)
	max(in_LIMITSTERR) AS out_LIMITSTERR,
	o_LIMITSTOT_Override AS in_LIMITSTOT,
	-- *INF*: max(in_LIMITSTOT)
	max(in_LIMITSTOT) AS out_LIMITSTOT,
	o_GREINSPREM AS in_GREINSPREM,
	-- *INF*: sum(in_GREINSPREM)
	sum(in_GREINSPREM) AS out_GREINSPREM,
	ExtractDate,
	AuditID
	FROM AGG_LiabilityPreZipPolicyLevelTotals
	GROUP BY pol_key, TableCode1, YEAR, STABBR, COCODE
),
AGG_LiabilityState AS (
	SELECT
	TableCode1 AS TableCode11,
	YEAR AS YEAR1,
	STABBR AS STABBR1,
	COCODE AS COCODE1,
	out_PRWTERR AS i_PRWTERR,
	-- *INF*: sum(i_PRWTERR)
	sum(i_PRWTERR) AS o_PRWTERR,
	out_PRWTOT AS i_PRWTOT,
	-- *INF*: sum(i_PRWTOT)
	sum(i_PRWTOT) AS o_PRWTOT,
	out_LIMITSTERR AS i_LIMITSTERR,
	-- *INF*: sum(i_LIMITSTERR)
	sum(i_LIMITSTERR) AS o_LIMITSERR,
	out_LIMITSTOT AS i_LIMITSTOT,
	-- *INF*: sum(i_LIMITSTOT)
	sum(i_LIMITSTOT) AS o_LIMITSTOT,
	out_GREINSPREM AS i_GREINSPREM,
	-- *INF*: sum(i_GREINSPREM)
	sum(i_GREINSPREM) AS o_GREINSPREM,
	out_PRETERR AS i_PRETERR,
	-- *INF*: sum(i_PRETERR)
	sum(i_PRETERR) AS o_PRETERR,
	out_PRETOT AS i_PRETOT,
	-- *INF*: sum(i_PRETOT)
	sum(i_PRETOT) AS o_PRETOT,
	ExtractDate,
	AuditID
	FROM AGG_LiabilityPreStatePolicyLevelTotals
	GROUP BY TableCode11, YEAR1, STABBR1, COCODE1
),
EXP_LiabilityStateOutput AS (
	SELECT
	'NaicTerrorismLiabilityState' || @{pipeline().parameters.YEAR}||'.csv' AS Filename,
	TableCode11,
	TableCode11 || 'S' AS TableCode,
	YEAR1 AS YEAR,
	COCODE1 AS COCODE,
	STABBR1 AS STABBR,
	o_PRWTERR AS PRWTERR,
	o_PRWTOT AS PRWTOT,
	o_PRETERR AS PRETERR,
	o_PRETOT AS PRETOT,
	o_GREINSPREM AS GREINSPREM,
	o_LIMITSERR AS LIMITSTERR,
	o_LIMITSTOT AS LIMITSTOT,
	ExtractDate,
	AuditID
	FROM AGG_LiabilityState
),
NAICTerrorismStateLevelExtract_Liability AS (
	INSERT INTO NAICTerrorismStateLevelExtract
	(AuditId, CreatedDate, ModifiedDate, TableCode, Year, CoCode, Stabbr, PrwTerr, PrwTot, PreTerr, PreTot, GreinsPrem, LimitsTerr, LIMITSTot)
	SELECT 
	AuditID AS AUDITID, 
	ExtractDate AS CREATEDDATE, 
	ExtractDate AS MODIFIEDDATE, 
	TABLECODE, 
	YEAR AS YEAR, 
	COCODE AS COCODE, 
	STABBR AS STABBR, 
	PRWTERR AS PRWTERR, 
	PRWTOT AS PRWTOT, 
	PRETERR AS PRETERR, 
	PRETOT AS PRETOT, 
	GREINSPREM AS GREINSPREM, 
	LIMITSTERR AS LIMITSTERR, 
	LIMITSTOT AS LIMITSTOT
	FROM EXP_LiabilityStateOutput
),
NaicTerrorismInlandMarineZipExtractFile AS (
	INSERT INTO NaicTerrorismInlandMarineZipExtractFile
	(FileName, TableCode, YEAR, COCODE, COTYPE, LOB, STABBR, ZIP, POLCAT, IND_CODE_TYPE, CODE, POLTYPE, COVTYPE, POLNUM, PRWTERR, PRWTOT, LIMITSTERR, LIMITSTOT)
	SELECT 
	Filename AS FILENAME, 
	TABLECODE, 
	YEAR, 
	COCODE, 
	COTYPE, 
	LOB, 
	STABBR, 
	ZIP, 
	POLCAT, 
	IND_CODE_TYPE, 
	CODE, 
	POLTYPE, 
	COVTYPE, 
	o_PolNum AS POLNUM, 
	PRWTERR, 
	PRWTOT, 
	LIMITSTERR, 
	LIMITSTOT
	FROM EXP_InlandMarineZipOutput
),
EXP_PropertyBreakout AS (
	SELECT
	pol_key,
	RCStatCoverageAKId,
	SourceSystemID,
	TableCode AS TableCode1,
	YEAR,
	STABBR,
	COCODE,
	COTYPE,
	LOB,
	ZIP,
	POLCAT,
	COVERAGE,
	IND_CODE_TYPE,
	CODE,
	POLTYPE,
	COVTYPE,
	POLNUM,
	ESTNUM,
	Limit,
	DirectWrittenPremium,
	EarnedPremium,
	TableCodeInsuranceLine,
	ReinsurancePercent,
	ReinsuranceEarnedPremium,
	terrorism_risk_ind,
	ExtractDate,
	AuditID,
	IsBlackListCoverage,
	LimitHashKey
	FROM RTR_RouteByLines_Property
),
SRT_PropertyHashKey AS (
	SELECT
	pol_key, 
	LimitHashKey, 
	Limit, 
	TableCode1, 
	YEAR, 
	STABBR, 
	COCODE, 
	COTYPE, 
	LOB, 
	ZIP, 
	POLCAT, 
	COVERAGE, 
	IND_CODE_TYPE, 
	CODE, 
	POLTYPE, 
	COVTYPE, 
	POLNUM, 
	ESTNUM, 
	DirectWrittenPremium, 
	EarnedPremium, 
	ReinsuranceEarnedPremium, 
	terrorism_risk_ind, 
	ExtractDate, 
	AuditID, 
	IsBlackListCoverage
	FROM EXP_PropertyBreakout
	ORDER BY pol_key ASC, LimitHashKey ASC, Limit DESC
),
EXP_PropertyZeroOutLimitFoDupCache AS (
	SELECT
	pol_key,
	TableCode1,
	YEAR,
	STABBR,
	COCODE,
	COTYPE,
	LOB,
	ZIP,
	POLCAT,
	COVERAGE,
	IND_CODE_TYPE,
	CODE,
	POLTYPE,
	COVTYPE,
	POLNUM,
	ESTNUM,
	DirectWrittenPremium,
	EarnedPremium,
	ReinsuranceEarnedPremium,
	terrorism_risk_ind,
	ExtractDate,
	AuditID,
	IsBlackListCoverage,
	Limit,
	LimitHashKey,
	-- *INF*: IIF(LimitHashKey=v_previous_hash_key,'Y','N')
	IFF(LimitHashKey = v_previous_hash_key, 'Y', 'N') AS v_hashKey,
	-- *INF*: IIF(v_hashKey='Y',0,Limit)
	IFF(v_hashKey = 'Y', 0, Limit) AS o_Limit,
	LimitHashKey AS v_previous_hash_key
	FROM SRT_PropertyHashKey
),
AGG_PropertyPolicyTerroismLevel AS (
	SELECT
	pol_key,
	TableCode1,
	YEAR,
	STABBR,
	COCODE,
	COTYPE,
	LOB,
	ZIP,
	POLCAT,
	COVERAGE,
	IND_CODE_TYPE,
	CODE,
	POLTYPE,
	COVTYPE,
	ESTNUM,
	terrorism_risk_ind,
	IsBlackListCoverage,
	o_Limit AS Limit,
	DirectWrittenPremium,
	EarnedPremium,
	ReinsuranceEarnedPremium,
	ExtractDate,
	AuditID,
	-- *INF*: sum(Limit)
	sum(Limit) AS LIMITS_SE_SF_SUM,
	-- *INF*: IIF(terrorism_risk_ind='Y' and IsBlackListCoverage='N',sum(DirectWrittenPremium))
	IFF(terrorism_risk_ind = 'Y' and IsBlackListCoverage = 'N', sum(DirectWrittenPremium)) AS PRWTERR,
	-- *INF*: sum(DirectWrittenPremium)
	sum(DirectWrittenPremium) AS PRWTOT,
	-- *INF*: IIF(terrorism_risk_ind='Y'  and IsBlackListCoverage='N',sum(Limit))
	IFF(terrorism_risk_ind = 'Y' and IsBlackListCoverage = 'N', sum(Limit)) AS TIVTERR,
	-- *INF*: sum(Limit)
	sum(Limit) AS TIVTOT,
	-- *INF*: IIF(terrorism_risk_ind='Y'  and IsBlackListCoverage='N',sum(EarnedPremium))
	IFF(terrorism_risk_ind = 'Y' and IsBlackListCoverage = 'N', sum(EarnedPremium)) AS PRETERR,
	-- *INF*: sum(EarnedPremium)
	sum(EarnedPremium) AS PRETOT,
	-- *INF*: sum(ReinsuranceEarnedPremium)
	sum(ReinsuranceEarnedPremium) AS GREINSPREM
	FROM EXP_PropertyZeroOutLimitFoDupCache
	GROUP BY pol_key, TableCode1, YEAR, STABBR, COCODE, COTYPE, LOB, ZIP, POLCAT, COVERAGE, IND_CODE_TYPE, CODE, POLTYPE, COVTYPE, ESTNUM, terrorism_risk_ind, IsBlackListCoverage
),
SRT_PropertyByPolicyAndZip AS (
	SELECT
	pol_key, 
	ZIP, 
	TableCode1, 
	YEAR, 
	STABBR, 
	COCODE, 
	COTYPE, 
	LOB, 
	POLCAT, 
	COVERAGE, 
	IND_CODE_TYPE, 
	CODE, 
	POLTYPE, 
	COVTYPE, 
	ESTNUM, 
	ExtractDate, 
	AuditID, 
	LIMITS_SE_SF_SUM, 
	PRWTERR, 
	PRWTOT, 
	TIVTERR, 
	TIVTOT, 
	PRETERR, 
	PRETOT, 
	GREINSPREM
	FROM AGG_PropertyPolicyTerroismLevel
	ORDER BY pol_key ASC, ZIP ASC
),
AGG_PropertyZIP AS (
	SELECT
	TableCode1 AS TableCode11,
	YEAR AS YEAR1,
	STABBR AS STABBR1,
	COCODE AS COCODE1,
	COTYPE AS COTYPE1,
	LOB AS LOB1,
	ZIP AS ZIP1,
	POLCAT AS POLCAT1,
	COVERAGE AS COVERAGE1,
	IND_CODE_TYPE AS IND_CODE_TYPE1,
	CODE AS CODE1,
	POLTYPE AS POLTYPE1,
	COVTYPE AS COVTYPE1,
	ExtractDate,
	AuditID,
	pol_key,
	ESTNUM AS ESTNUM1,
	-- *INF*: DECODE(TRUE,
	-- pol_key=v_previous_pol_key and ZIP1=v_previous_zip,v_EstNum,
	-- pol_key != v_previous_pol_key and ZIP1=v_previous_zip,v_EstNum + ESTNUM1,
	-- ESTNUM1
	-- )
	-- 
	DECODE(
	    TRUE,
	    pol_key = v_previous_pol_key and ZIP1 = v_previous_zip, v_EstNum,
	    pol_key != v_previous_pol_key and ZIP1 = v_previous_zip, v_EstNum + ESTNUM1,
	    ESTNUM1
	) AS v_EstNum,
	ZIP1 AS v_previous_zip,
	pol_key AS v_previous_pol_key,
	v_EstNum AS ESTNUM_Count,
	LIMITS_SE_SF_SUM AS in_LIMITS_SE_SF_SUM,
	PRWTERR AS in_PRWTERR,
	PRWTOT AS in_PRWTOT,
	TIVTERR AS in_TIVTERR,
	TIVTOT AS in_TIVTOT,
	PRETERR AS in_PRETERR,
	PRETOT AS in_PRETOT,
	GREINSPREM AS in_GREINSPREM,
	-- *INF*: sum(in_PRWTERR)
	sum(in_PRWTERR) AS PRWTERR,
	-- *INF*: sum(in_PRWTOT)
	sum(in_PRWTOT) AS PRWTOT,
	-- *INF*: sum(in_TIVTERR)
	sum(in_TIVTERR) AS TIVTERR,
	-- *INF*: sum(in_TIVTOT)
	sum(in_TIVTOT) AS TIVTOT,
	-- *INF*: sum(in_LIMITS_SE_SF_SUM)
	sum(in_LIMITS_SE_SF_SUM) AS LIMITS_SE_SF_SUM,
	-- *INF*: sum(in_PRETERR)
	sum(in_PRETERR) AS PRETERR,
	-- *INF*: sum(in_PRETOT)
	sum(in_PRETOT) AS PRETOT,
	-- *INF*: sum(in_GREINSPREM)
	sum(in_GREINSPREM) AS GREINSPREM
	FROM SRT_PropertyByPolicyAndZip
	GROUP BY TableCode11, YEAR1, STABBR1, COCODE1, COTYPE1, LOB1, ZIP1, POLCAT1, COVERAGE1, IND_CODE_TYPE1, CODE1, POLTYPE1, COVTYPE1
),
AGG_PropertyState AS (
	SELECT
	TableCode11,
	YEAR1,
	STABBR1,
	COCODE1,
	PRWTERR AS i_PRWTERR,
	-- *INF*: sum(i_PRWTERR)
	sum(i_PRWTERR) AS o_PRWTERR,
	PRWTOT AS i_PRWTOT,
	-- *INF*: sum(i_PRWTOT)
	sum(i_PRWTOT) AS o_PRWTOT,
	TIVTERR AS i_TIVTERR,
	-- *INF*: sum(i_TIVTERR)
	sum(i_TIVTERR) AS o_TIVTERR,
	TIVTOT AS i_TIVTOT,
	-- *INF*: sum(i_TIVTOT)
	sum(i_TIVTOT) AS o_TIVTOT,
	GREINSPREM AS i_GREINSPREM,
	-- *INF*: sum(i_GREINSPREM)
	sum(i_GREINSPREM) AS o_GREINSPREM,
	PRETERR AS i_PRETERR,
	-- *INF*: sum(i_PRETERR)
	sum(i_PRETERR) AS o_PRETERR,
	PRETOT AS i_PRETOT,
	-- *INF*: sum(i_PRETOT)
	sum(i_PRETOT) AS o_PRETOT,
	ExtractDate,
	AuditID
	FROM AGG_PropertyZIP
	GROUP BY TableCode11, YEAR1, STABBR1, COCODE1
),
EXP_PropertyStateOutput AS (
	SELECT
	'NaicTerrorismPropertyState' || @{pipeline().parameters.YEAR}||'.csv' AS Filename,
	TableCode11,
	TableCode11 || 'S' AS TableCode,
	YEAR1 AS YEAR,
	COCODE1 AS COCODE,
	STABBR1 AS STABBR,
	o_PRWTERR AS PRWTERR,
	o_PRWTOT AS PRWTOT,
	o_PRETERR AS PRETERR,
	o_PRETOT AS PRETOT,
	o_GREINSPREM AS GREINSPREM,
	o_TIVTERR AS TIVTERR,
	o_TIVTOT AS TIVTOT,
	ExtractDate,
	AuditID
	FROM AGG_PropertyState
),
NaicTerrorismPropertyStateExtractFile AS (
	INSERT INTO NaicTerrorismPropertyStateExtractFile
	(FileName, TableCode, YEAR, COCODE, STABBR, PRWTERR, PRWTOT, PRETERR, PRETOT, GREINSPREM, TIVTERR, TIVTOT)
	SELECT 
	Filename AS FILENAME, 
	TABLECODE, 
	YEAR, 
	COCODE, 
	STABBR, 
	PRWTERR, 
	PRWTOT, 
	PRETERR, 
	PRETOT, 
	GREINSPREM, 
	TIVTERR, 
	TIVTOT
	FROM EXP_PropertyStateOutput
),
WorkNAICTerrorismSubTotal AS (

	------------ PRE SQL ----------
	delete from WorkNAICTerrorismSubTotal where AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} and Year=@{pipeline().parameters.YEAR}
	-------------------------------


	INSERT INTO WorkNAICTerrorismSubTotal
	(AuditId, CreatedDate, ModifiedDate, pol_key, RatingStatCoverageID, SourceSystemID, TableCode, Year, Stabbr, CoCode, CoType, Lob, Zip, PolCat, Coverage, IndCodeType, Code, PolType, CovType, PolNum, EstNum, Limit, DirectWrittenPremium, EarnedPremium, TableCodeInsuranceLine, ReinsurancePercent, ReinsuranceEarnedPremium, TerrorismRiskInd, BlackListCoverageFlag, CoverageLimitValueGLStateOverride, LimitHashKey, Deductible)
	SELECT 
	AuditID AS AUDITID, 
	ExtractDate AS CREATEDDATE, 
	ExtractDate AS MODIFIEDDATE, 
	pol_key1 AS POL_KEY, 
	RCStatCoverageAKId1 AS RATINGSTATCOVERAGEID, 
	SOURCESYSTEMID, 
	TableCode1 AS TABLECODE, 
	YEAR AS YEAR, 
	STABBR AS STABBR, 
	COCODE AS COCODE, 
	COTYPE AS COTYPE, 
	LOB AS LOB, 
	ZIP AS ZIP, 
	POLCAT AS POLCAT, 
	COVERAGE AS COVERAGE, 
	IND_CODE_TYPE AS INDCODETYPE, 
	o_CODE AS CODE, 
	POLTYPE AS POLTYPE, 
	COVTYPE AS COVTYPE, 
	POLNUM AS POLNUM, 
	ESTNUM AS ESTNUM, 
	o_CoverageLimitValue_SubTotal AS LIMIT, 
	DIRECTWRITTENPREMIUM, 
	EARNEDPREMIUM, 
	TABLECODEINSURANCELINE, 
	REINSURANCEPERCENT, 
	REINSURANCEEARNEDPREMIUM, 
	terrorism_risk_ind AS TERRORISMRISKIND, 
	o_BlackListCoverage AS BLACKLISTCOVERAGEFLAG, 
	o_CoverageLimitValue_GL_State_Override AS COVERAGELIMITVALUEGLSTATEOVERRIDE, 
	LIMITHASHKEY, 
	o_CoverageDeductibleValue AS DEDUCTIBLE
	FROM EXP_JoinerOutput
),
NAICTerrorismStateLevelExtract_InlandMarine AS (
	INSERT INTO NAICTerrorismStateLevelExtract
	(AuditId, CreatedDate, ModifiedDate, TableCode, Year, CoCode, Stabbr, PrwTerr, PrwTot, PreTerr, PreTot, GreinsPrem, TivTerr, TivTot)
	SELECT 
	AuditID AS AUDITID, 
	ExtractDate AS CREATEDDATE, 
	ExtractDate AS MODIFIEDDATE, 
	TABLECODE, 
	YEAR AS YEAR, 
	COCODE AS COCODE, 
	STABBR AS STABBR, 
	PRWTERR AS PRWTERR, 
	PRWTOT AS PRWTOT, 
	PRETERR AS PRETERR, 
	PRETOT AS PRETOT, 
	GREINSPREM AS GREINSPREM, 
	TIVTERR AS TIVTERR, 
	TIVTOT AS TIVTOT
	FROM EXP_InlandMarineStateOutput
),
EXP_PropertyZipOutput AS (
	SELECT
	'NaicTerrorismPropertyZip' || @{pipeline().parameters.YEAR} ||'.csv' AS FileName,
	TableCode11 AS i_TableCode,
	i_TableCode || 'D' AS TableCode,
	YEAR1 AS YEAR,
	COCODE1 AS COCODE,
	COTYPE1 AS COTYPE,
	LOB1 AS LOB,
	STABBR1 AS STABBR,
	ZIP1 AS ZIP,
	POLCAT1 AS POLCAT,
	COVERAGE1 AS COVERAGE,
	IND_CODE_TYPE1 AS IND_CODE_TYPE,
	CODE1 AS CODE,
	LIMITS_SE_SF_SUM AS SumLIMIT_SE_SF,
	-- *INF*: DECODE(TRUE,
	-- SumLIMIT_SE_SF >= 100000000,'F',
	-- SumLIMIT_SE_SF >=99999999,'E',
	-- SumLIMIT_SE_SF >= 5000000, 'D',
	-- SumLIMIT_SE_SF >= 1000000,'C',
	-- SumLIMIT_SE_SF >= 500000,'B',
	-- 'A'
	-- )
	-- 
	-- -- Default is A, else go in descending order to set value, SE and SF will be same
	DECODE(
	    TRUE,
	    SumLIMIT_SE_SF >= 100000000, 'F',
	    SumLIMIT_SE_SF >= 99999999, 'E',
	    SumLIMIT_SE_SF >= 5000000, 'D',
	    SumLIMIT_SE_SF >= 1000000, 'C',
	    SumLIMIT_SE_SF >= 500000, 'B',
	    'A'
	) AS LIMIT_SE_SF,
	POLTYPE1 AS POLTYPE,
	COVTYPE1 AS COVTYPE,
	ESTNUM_Count AS ESTNUM,
	PRWTERR,
	PRWTOT,
	TIVTERR,
	TIVTOT,
	ExtractDate,
	AuditID
	FROM AGG_PropertyZIP
),
NaicTerrorismPropertyZipExtractFile AS (
	INSERT INTO NaicTerrorismPropertyZipExtractFile
	(FileName, TableCode, YEAR, COCODE, COTYPE, LOB, STABBR, ZIP, POLCAT, COVERAGE, IND_CODE_TYPE, CODE, LIMITSE, LIMITSF, POLTYPE, COVTYPE, ESTNUM, PRWTERR, PRWTOT, TIVTERR, TIVTOT)
	SELECT 
	FILENAME, 
	TABLECODE, 
	YEAR, 
	COCODE, 
	COTYPE, 
	LOB, 
	STABBR, 
	ZIP, 
	POLCAT, 
	COVERAGE, 
	IND_CODE_TYPE, 
	CODE, 
	LIMIT_SE_SF AS LIMITSE, 
	LIMIT_SE_SF AS LIMITSF, 
	POLTYPE, 
	COVTYPE, 
	ESTNUM, 
	PRWTERR, 
	PRWTOT, 
	TIVTERR, 
	TIVTOT
	FROM EXP_PropertyZipOutput
),
NAICTerrorismZipLevelExtract_Property AS (

	------------ PRE SQL ----------
	Delete From NAICTerrorismZipLevelExtract where AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} and Year=@{pipeline().parameters.YEAR}
	-------------------------------


	INSERT INTO NAICTerrorismZipLevelExtract
	(AuditId, CreatedDate, ModifiedDate, TableCode, Year, CoCode, CoType, Lob, Stabbr, Zip, PolCat, IndCodeType, Code, PolType, CovType, PrwTerr, PrwTot, Coverage, LimitsE, LimitsF, EstNum, TivTerr, TivTot)
	SELECT 
	AuditID AS AUDITID, 
	ExtractDate AS CREATEDDATE, 
	ExtractDate AS MODIFIEDDATE, 
	TABLECODE, 
	YEAR AS YEAR, 
	COCODE AS COCODE, 
	COTYPE AS COTYPE, 
	LOB AS LOB, 
	STABBR AS STABBR, 
	ZIP AS ZIP, 
	POLCAT AS POLCAT, 
	IND_CODE_TYPE AS INDCODETYPE, 
	CODE AS CODE, 
	POLTYPE AS POLTYPE, 
	COVTYPE AS COVTYPE, 
	PRWTERR AS PRWTERR, 
	PRWTOT AS PRWTOT, 
	COVERAGE AS COVERAGE, 
	LIMIT_SE_SF AS LIMITSE, 
	LIMIT_SE_SF AS LIMITSF, 
	ESTNUM AS ESTNUM, 
	TIVTERR AS TIVTERR, 
	TIVTOT AS TIVTOT
	FROM EXP_PropertyZipOutput
),
NaicTerrorismLiabilityStateExtractFile AS (
	INSERT INTO NaicTerrorismLiabilityStateExtractFile
	(FileName, TableCode, YEAR, COCODE, STABBR, PRWTERR, PRWTOT, PRETERR, PRETOT, GREINSPREM, LIMITSTERR, LIMITSTOT)
	SELECT 
	Filename AS FILENAME, 
	TABLECODE, 
	YEAR, 
	COCODE, 
	STABBR, 
	PRWTERR, 
	PRWTOT, 
	PRETERR, 
	PRETOT, 
	GREINSPREM, 
	LIMITSTERR, 
	LIMITSTOT
	FROM EXP_LiabilityStateOutput
),
NAICTerrorismStateLevelExtract_Property AS (

	------------ PRE SQL ----------
	Delete From NAICTerrorismStateLevelExtract where AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} and Year=@{pipeline().parameters.YEAR}
	-------------------------------


	INSERT INTO NAICTerrorismStateLevelExtract
	(AuditId, CreatedDate, ModifiedDate, TableCode, Year, CoCode, Stabbr, PrwTerr, PrwTot, PreTerr, PreTot, GreinsPrem, TivTerr, TivTot)
	SELECT 
	AuditID AS AUDITID, 
	ExtractDate AS CREATEDDATE, 
	ExtractDate AS MODIFIEDDATE, 
	TABLECODE, 
	YEAR AS YEAR, 
	COCODE AS COCODE, 
	STABBR AS STABBR, 
	PRWTERR AS PRWTERR, 
	PRWTOT AS PRWTOT, 
	PRETERR AS PRETERR, 
	PRETOT AS PRETOT, 
	GREINSPREM AS GREINSPREM, 
	TIVTERR AS TIVTERR, 
	TIVTOT AS TIVTOT
	FROM EXP_PropertyStateOutput
),
NaicTerrorismLiabilityZipExtractFile AS (
	INSERT INTO NaicTerrorismLiabilityZipExtractFile
	(FileName, TableCode, YEAR, COCODE, COTYPE, LOB, STABBR, ZIP, POLCAT, COVERAGE, IND_CODE_TYPE, CODE, POLTYPE, COVTYPE, POLNUM, PRWTERR, PRWTOT, LIMITSERR, LIMITSTOT)
	SELECT 
	Filename AS FILENAME, 
	TABLECODE, 
	YEAR, 
	COCODE, 
	COTYPE, 
	LOB, 
	STABBR, 
	ZIP, 
	POLCAT, 
	COVERAGE, 
	IND_CODE_TYPE, 
	CODE, 
	POLTYPE, 
	COVTYPE, 
	POLNUM, 
	PRWTERR, 
	PRWTOT, 
	LIMITSTERR AS LIMITSERR, 
	LIMITSTOT
	FROM EXP_LiabilityZipOutput
),
AGG_debugLimitRules AS (
	SELECT
	in_TableCode1 AS TableCode1,
	CoverageLimitType,
	LimitRule,
	'NaicTerrorismAggLimitRules' || @{pipeline().parameters.YEAR}|| '.csv' AS FileName
	FROM EXP_ApplyLimitRules
	QUALIFY ROW_NUMBER() OVER (PARTITION BY TableCode1, CoverageLimitType, LimitRule ORDER BY NULL) = 1
),
NaicTerrorismAggLimitRulesFlatFile AS (
	INSERT INTO NaicTerrorismAggLimitRulesFlatFile
	(FileName, TableCode1, CoverageLimitType, LimitRule)
	SELECT 
	FILENAME, 
	TABLECODE1, 
	COVERAGELIMITTYPE, 
	LIMITRULE
	FROM AGG_debugLimitRules
),
FIL_MissingCoverages AS (
	SELECT
	SourceSystemID, 
	InsuranceLineCode, 
	InsuranceLineDescription, 
	InsuranceReferenceLineOfBusinessDescription, 
	CoverageCode, 
	CoverageDescription, 
	DctRiskTypeCode, 
	PropertyCoverageCode, 
	BureauCode1, 
	PolicyOfferingCode, 
	ProductCode, 
	InsuranceReferenceLineOfBusinessCode, 
	o_Coverage AS COVERAGE, 
	PolCat AS POLCAT, 
	TableCodeInsuranceLine
	FROM EXP_InputFromControlTable
	WHERE (SUBSTR(COVERAGE,1,3)='N/A' and TableCodeInsuranceLine !='InlandMarine') or POLCAT='N/A'
),
AGG_InvalidCoverageLimits AS (
	SELECT
	SourceSystemID,
	InsuranceLineCode,
	InsuranceLineDescription,
	InsuranceReferenceLineOfBusinessDescription,
	CoverageCode,
	CoverageDescription,
	DctRiskTypeCode,
	PropertyCoverageCode,
	DctCoverageTypeCode,
	BureauCode1,
	PolicyOfferingCode,
	ProductCode,
	InsuranceReferenceLineOfBusinessCode,
	COVERAGE,
	POLCAT,
	TableCodeInsuranceLine,
	'NaicTerrorismInvalidPropertyLiabilityCoverages' || @{pipeline().parameters.YEAR}|| '.csv' AS FileName
	FROM FIL_MissingCoverages
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SourceSystemID, InsuranceLineCode, InsuranceLineDescription, InsuranceReferenceLineOfBusinessDescription, CoverageCode, CoverageDescription, DctRiskTypeCode, PropertyCoverageCode, DctCoverageTypeCode, BureauCode1, PolicyOfferingCode, ProductCode, InsuranceReferenceLineOfBusinessCode, COVERAGE, POLCAT, TableCodeInsuranceLine ORDER BY NULL) = 1
),
NaicTerrorismInvalidPropertyLiabilityCoveragesFlatFile1 AS (
	INSERT INTO NaicTerrorismInvalidPropertyLiabilityCoveragesFlatFile
	(FileName, SourceSystemID, InsuranceLineCode, InsuranceLineDescription, InsuranceReferenceLineOfBusinessDescription, CoverageCode, CoverageDescription, DctRiskTypeCode, PropertyCoverageCode, DctCoverageTypeCode, BureauCode1, PolicyOfferingCode, ProductCode, InsuranceReferenceLineOfBusinessCode, COVERAGE, POLCAT, TableCodeInsuranceLine)
	SELECT 
	FILENAME, 
	SOURCESYSTEMID, 
	INSURANCELINECODE, 
	INSURANCELINEDESCRIPTION, 
	INSURANCEREFERENCELINEOFBUSINESSDESCRIPTION, 
	COVERAGECODE, 
	COVERAGEDESCRIPTION, 
	DCTRISKTYPECODE, 
	PROPERTYCOVERAGECODE, 
	DCTCOVERAGETYPECODE, 
	BUREAUCODE1, 
	POLICYOFFERINGCODE, 
	PRODUCTCODE, 
	INSURANCEREFERENCELINEOFBUSINESSCODE, 
	COVERAGE, 
	POLCAT, 
	TABLECODEINSURANCELINE
	FROM AGG_InvalidCoverageLimits
),