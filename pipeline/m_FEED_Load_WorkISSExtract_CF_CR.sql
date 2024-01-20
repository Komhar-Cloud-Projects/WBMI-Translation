WITH
LKP_SupReferenceData AS (
	SELECT
	ToCode,
	FromCode
	FROM (
		SELECT 
			ToCode,
			FromCode
		FROM SupReferenceData
		WHERE FromDomain='DC_CF_RISK' and ToDomain='ISSCoverageCodes'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY FromCode ORDER BY ToCode) = 1
),
LKP_SupConstructionCode AS (
	SELECT
	StandardConstructionCodeDescription,
	ConstructionCode
	FROM (
		Select  LTRIM(RTRIM(ConstructionCode)) as ConstructionCode,
		     StandardConstructionCodeDescription as StandardConstructionCodeDescription
		From @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupConstructionCode
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ConstructionCode ORDER BY StandardConstructionCodeDescription) = 1
),
LKP_WorkISSExtract_ConstructionCode_First AS (
	SELECT
	ConstructionCode,
	PolicyKey,
	TypeBureauCode
	FROM (
		select ConstructionCode as ConstructionCode,
		PolicyKey as PolicyKey,
		TypeBureauCode as TypeBureauCode
		from (
		select  ISS.ConstructionCode,
		ISS.PolicyKey,
		ISS.TypeBureauCode,
		row_number() over (partition by  ISS.PolicyKey,ISS.TypeBureauCode order by RL.LocationUnitNumber, SC.SubLocationUnitNumber) rn
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ISSCommercialPropertyExtract ISS
		join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC
		on ISS.EDWPremiumMasterCalculationPKID=PMC.PremiumMasterCalculationId
		join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
		on PMC.RiskLocationAKID=RL.RiskLocationAKID and RL.SourceSystemID='PMS'
		join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
		on PMC.StatisticalCoverageAKID=SC.StatisticalCoverageAKID and SC.SourceSystemID='PMS'
		where ISS.EDWPremiumMasterCalculationPKId<>-1 and ISS.PremiumMasterRunDate between 
		 DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0)--first day of last Quarter   
		 AND
		 DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0))--Last day of last Quarter
		and ISS.ConstructionCode<>'00'
		
		union all
		
		select  ISS.ConstructionCode,
		ISS.PolicyKey,
		ISS.TypeBureauCode,
		row_number() over (partition by  ISS.PolicyKey,ISS.TypeBureauCode order by CCD.loc_unit_num,CCD.sub_loc_unit_num) rn
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ISSCommercialPropertyExtract ISS
		join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation LMC
		on ISS.EDWLossMasterCalculationPKID=LMC.loss_master_calculation_id
		join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
		on LMC.claimant_cov_det_ak_id=CCD.claimant_cov_det_ak_id and CCD.crrnt_snpsht_flag=1
		where ISS.EDWLossMasterCalculationPKId<>-1 and ISS.LossMasterRunDate between 
		 DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0)--first day of last Quarter   
		 AND
		 DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0))--Last day of last Quarter
		and len(ISS.PolicyKey)=12
		and ISS.ConstructionCode<>'00'
		) Src
		where rn=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,TypeBureauCode ORDER BY ConstructionCode) = 1
),
LKP_WorkISSExtract_ISOFireProtectionCode_First AS (
	SELECT
	ISOFireProtectionCode,
	PolicyKey,
	TypeBureauCode
	FROM (
		select ISOFireProtectionCode as ISOFireProtectionCode,
		PolicyKey as PolicyKey,
		TypeBureauCode as TypeBureauCode
		from (
		select  ISS.ISOFireProtectionCode,
		ISS.PolicyKey,
		ISS.TypeBureauCode,
		row_number() over (partition by  ISS.PolicyKey,ISS.TypeBureauCode order by RL.LocationUnitNumber, SC.SubLocationUnitNumber) rn
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ISSCommercialPropertyExtract ISS
		join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC
		on ISS.EDWPremiumMasterCalculationPKID=PMC.PremiumMasterCalculationId
		join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
		on PMC.RiskLocationAKID=RL.RiskLocationAKID and RL.SourceSystemID='PMS'
		join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
		on PMC.StatisticalCoverageAKID=SC.StatisticalCoverageAKID and SC.SourceSystemID='PMS'
		where ISS.EDWPremiumMasterCalculationPKId<>-1 and ISS.PremiumMasterRunDate between 
		 DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0)--first day of last Quarter   
		 AND
		 DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0))--Last day of last Quarter
		and ISS.ISOFireProtectionCode<>'00'
		
		union all
		
		select  ISS.ISOFireProtectionCode,
		ISS.PolicyKey,
		ISS.TypeBureauCode,
		row_number() over (partition by  ISS.PolicyKey,ISS.TypeBureauCode order by CCD.loc_unit_num,CCD.sub_loc_unit_num) rn
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ISSCommercialPropertyExtract ISS
		join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation LMC
		on ISS.EDWLossMasterCalculationPKID=LMC.loss_master_calculation_id
		join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
		on LMC.claimant_cov_det_ak_id=CCD.claimant_cov_det_ak_id and CCD.crrnt_snpsht_flag=1
		where ISS.EDWLossMasterCalculationPKId<>-1 and ISS.LossMasterRunDate between 
		 DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0)--first day of last Quarter   
		 AND
		 DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0))--Last day of last Quarter
		and len(ISS.PolicyKey)=12
		and ISS.ISOFireProtectionCode<>'00'
		) Src
		where rn=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,TypeBureauCode ORDER BY ISOFireProtectionCode) = 1
),
LKP_Policy_SBOP AS (
	SELECT
	ProgramAKId,
	pol_key
	FROM (
		select distinct a.pol_key as pol_key,a.ProgramAKId  as ProgramAKId from
		(
		select   p.pol_key as pol_key , pc.policyakid  as PolicyAKID,P.ProgramAKId as ProgramAKId
		From @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy p inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		ON
		p.pol_ak_id=pc.policyakid
		and 
		sourcesystemid='DCT' 
		and 
		PC.InsuranceLine ='SBOPGeneralLiability') a  inner join
		(
		select   p.pol_key as pol_key , pc.policyakid  as PolicyAKID,P.ProgramAKId as ProgramAKId
		From @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy p inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		ON
		p.pol_ak_id=pc.policyakid
		and 
		sourcesystemid='DCT' 
		and 
		 PC.InsuranceLine ='SBOPProperty') b 
		ON  a.PolicyAKID=b.PolicyAKID
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY ProgramAKId) = 1
),
LKP_Policy AS (
	SELECT
	pol_key
	FROM (
		select p.pol_key as Pol_Key 
		from 
		@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy p inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on
		p.pol_ak_id=pc.PolicyAKID
		where pc.InsuranceLine like 'SBOP%'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_key) = 1
),
LKP_archCFDCOccpancyType AS (
	SELECT
	OccupancyType,
	PolicyNumber
	FROM (
		select distinct db.PolicyNumber as PolicyNumber,do.OccupancyType as OccupancyType From  ArchDCCFlocationStaging dloc inner join
		 (SELECT distinct dp.PolicyNumber+PolicyVersionFormatted as PolicyNumber,db.CFlocationid as CFlocationid,db.Sessionid as 
		 sessionid,db.CFBuildingId,dl.type as type,db.description as description
		From VWArchWorkDCTPolicy dp inner join 
		  archDCLineStaging dl
		on
		dp.PolicyId=dl.PolicyId 
		and dp.Sessionid=dl.Sessionid
		 inner join  ArchDCCFBuildingStage db
		on dl.LineId=db.LineId
		and dl.Sessionid=db.Sessionid
		) db
		on dloc.CF_locationid = db.CFlocationid AND  dloc.Sessionid=db.Sessionid
		INNER JOIN archDCCFRiskStaging dr
		on dr.CF_BuildingId=db.CFBuildingId
		and dr.Sessionid=db.Sessionid
		 inner join archDCCFOccupancyStaging do
		on do.CF_RiskId=dr.CF_RiskId 
		and do.Sessionid=dr.Sessionid  
		where  db.Type in('CommercialAuto','CommercialUmbrella','Property')
		and dloc.description='Primary Location'
		and db.description like 'Building #1%' 
		and do.OccupancyType is not null
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber ORDER BY OccupancyType) = 1
),
LKP_archGLDCOccpancyType AS (
	SELECT
	OccupancyType,
	PolicyNumber
	FROM (
		select distinct dr.PolicyNumber as PolicyNumber,dcgl.OccupancyTypeMonoline as  OccupancyType
		 from archDCGLOccupancyStaging dcgl inner join (Select distinct dp.PolicyNumber+PolicyVersionFormatted as PolicyNumber, dr.GL_RiskId,dr.SessionId,dl.type
		 From  VWArchWorkDCTPolicy dp inner join   archDCLineStaging dl
		 on
		 dp.PolicyId=dl.PolicyId and
		  dp.SessionId=dl.SessionId 
		  -- and dp.AuditId=dl.AuditId 
		 inner join  archDCGLRiskStaging dr
		 on dr.LineId=dl.LineId and
		 dr.SessionId=dl.SessionId 
		 --and dr.AuditId=dl.AuditId
		 )  dr
		 on dcgl.GL_RiskId=dr.GL_RiskId and
		 dcgl.SessionId=dr.SessionId 
		 --and dcgl.AuditId=dr.AuditId
		 where dr.type='GeneralLiability' and dcgl.OccupancyTypeMonoline is not null
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber ORDER BY OccupancyType) = 1
),
LKP_Policy_GL_PR AS (
	SELECT
	pol_key
	FROM (
		select distinct a.pol_key  as pol_key from
		(
		select   p.pol_key as pol_key , pc.policyakid  as PolicyAKID
		From @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy p inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		ON
		p.pol_ak_id=pc.policyakid
		and 
		sourcesystemid='DCT' 
		and 
		PC.InsuranceLine ='GeneralLiability') a  inner join
		(
		select   p.pol_key as pol_key , pc.policyakid  as PolicyAKID
		From @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy p inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		ON
		p.pol_ak_id=pc.policyakid
		and 
		sourcesystemid='DCT' 
		and 
		 PC.InsuranceLine ='Property') b 
		ON  a.PolicyAKID=b.PolicyAKID
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_key) = 1
),
LKP_SupPackageModificationAdjustmentGroup AS (
	SELECT
	PackageModificationAdjustmentGroupDescription,
	SourceSystemId,
	PackageModificationAdjustmentGroupCode
	FROM (
		SELECT 
			PackageModificationAdjustmentGroupDescription,
			SourceSystemId,
			PackageModificationAdjustmentGroupCode
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupPackageModificationAdjustmentGroup
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SourceSystemId,PackageModificationAdjustmentGroupCode ORDER BY PackageModificationAdjustmentGroupDescription) = 1
),
LKP_Deductible_Property_WindORHail AS (
	SELECT
	CoverageDeductibleValue,
	PolicyKey,
	RatingCoverageAKId
	FROM (
		select PMC.PolicyKey as PolicyKey,PMC.RatingCoverageAKId as RatingCoverageAKId,CD.CoverageDeductibleValue as CoverageDeductibleValue
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductibleBridge CDB
		on PMC.PremiumTransactionAKID=CDB.PremiumTransactionAKId
		and PMC.SourceSystemID='DCT'
		and CDB.SourceSystemID='DCT'
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductible CD
		on CDB.CoverageDeductibleId=CD.CoverageDeductibleId
		and CD.SourceSystemID='DCT'
		where CD.CoverageDeductibleValue<>'0'
		and PMC.PremiumMasterSubLine in ('020','027','120')
		and (CD.CoverageDeductibleType like '%Wind%' or CD.CoverageDeductibleType like '%Hail%')
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,RatingCoverageAKId ORDER BY CoverageDeductibleValue) = 1
),
LKP_Deductible_Property AS (
	SELECT
	CoverageDeductibleValue,
	PolicyKey,
	RatingCoverageAKId
	FROM (
		select PMC.Policykey as Policykey,PMC.RatingCoverageAKId as RatingCoverageAKId,Cd.CoverageDeductibleValue as CoverageDeductibleValue
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductibleBridge CDB
		on PMC.PremiumTransactionAKID=CDB.PremiumTransactionAKId
		and PMC.SourceSystemID='DCT'
		and CDB.SourceSystemID='DCT'
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductible CD
		on CDB.CoverageDeductibleId=CD.CoverageDeductibleId
		and CD.SourceSystemID='DCT'
		where PMC.PremiumMasterSubLine in ('020','027','120','010','015','016','017','018','029','035','045','055','110')
		and (CD.CoverageDeductibleType not like '%Wind%' and CD.CoverageDeductibleType not like '%Hail%')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,RatingCoverageAKId ORDER BY CoverageDeductibleValue) = 1
),
LKP_Update_ConstructionCode AS (
	SELECT
	ConstructionCode,
	PolicyKey
	FROM (
		SELECT PolicyKey AS PolicyKey, 
		ConstructionCode AS ConstructionCode
		FROM (
			SELECT DISTINCT POL.pol_key AS PolicyKey, 
			RL.LocationUnitNumber AS LocationNumber, 
			RC.SubLocationUnitNumber AS BuildingNumber, 
			PT.ConstructionCode AS ConstructionCode,
			(CASE 
				WHEN RC.SubLocationUnitNumber <> '000'
				THEN 
				ROW_NUMBER() OVER (PARTITION BY POL.pol_key
				ORDER BY RL.LocationUnitNumber, RC.SubLocationUnitNumber)
				ELSE 0
			END) 
			AS RowNumber
			FROM PremiumTransaction AS PT WITH (NOLOCK)
			INNER JOIN  RatingCoverage AS RC WITH (NOLOCK)
			ON PT.RatingCoverageAKID=RC.RatingCoverageAKID
			AND RC.EffectiveDate=PT.EffectiveDate 
			INNER JOIN PolicyCoverage AS PC WITH (NOLOCK)
			ON PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
			AND PC.CurrentSnapshotFlag=1
			INNER JOIN RiskLocation AS RL WITH (NOLOCK)
			ON PC.RiskLocationAKID=RL.RiskLocationAKID
			AND RL.CurrentSnapshotFlag=1
			INNER JOIN V2.policy AS POL WITH (NOLOCK)
			ON POL.pol_ak_id=PC.PolicyAKID
			AND POL.crrnt_snpsht_flag=1
			INNER JOIN PremiumMasterCalculation AS PMC WITH (NOLOCK)
			ON PMC.PremiumTransactionAKID = PT.PremiumTransactionAKID
			AND PMC.CurrentSnapshotFlag = 1
			WHERE RC.SubLocationUnitNumber <> '000'
			AND LEN(POL.pol_key) <> 12
			AND PT.ConstructionCode NOT IN ('N/A', '00')
			AND (PMC.PremiumMasterRunDate BETWEEN DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0)
		 	AND 
			DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0))
		) AS CorrectConstructionCode
		WHERE RowNumber = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY ConstructionCode) = 1
),
LKP_Update_ClassCode AS (
	SELECT
	ClassCode,
	PolicyKey
	FROM (
		SELECT PolicyKey AS PolicyKey, 
		ClassCode AS ClassCode
		FROM (
			SELECT DISTINCT POL.pol_key AS PolicyKey, 
			RL.LocationUnitNumber AS LocationNumber, 
			RC.SubLocationUnitNumber AS BuildingNumber, 
			RC.ClassCode AS ClassCode,
			(CASE 
				WHEN RC.SubLocationUnitNumber <> '000'
				THEN 
				ROW_NUMBER() OVER (PARTITION BY POL.pol_key
				ORDER BY RL.LocationUnitNumber, RC.SubLocationUnitNumber)
				ELSE 0
			END) 
			AS RowNumber
			FROM PremiumTransaction AS PT WITH (NOLOCK)
			INNER JOIN  RatingCoverage AS RC WITH (NOLOCK)
			ON PT.RatingCoverageAKID=RC.RatingCoverageAKID
			AND RC.EffectiveDate=PT.EffectiveDate 
			INNER JOIN PolicyCoverage AS PC WITH (NOLOCK)
			ON PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
			AND PC.CurrentSnapshotFlag=1
			INNER JOIN RiskLocation AS RL WITH (NOLOCK)
			ON PC.RiskLocationAKID=RL.RiskLocationAKID
			AND RL.CurrentSnapshotFlag=1
			INNER JOIN V2.policy AS POL WITH (NOLOCK)
			ON POL.pol_ak_id=PC.PolicyAKID
			AND POL.crrnt_snpsht_flag=1
			INNER JOIN PremiumMasterCalculation AS PMC WITH (NOLOCK)
			ON PMC.PremiumTransactionAKID = PT.PremiumTransactionAKID
			AND PMC.CurrentSnapshotFlag = 1
			WHERE RC.SubLocationUnitNumber <> '000'
			AND LEN(POL.pol_key) <> 12
			AND RC.ClassCode NOT IN ('N/A', '0000', '99999')
			AND (PMC.PremiumMasterRunDate BETWEEN DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0)
		 	AND 
			DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0))
		) AS CorrectClassCode
		WHERE RowNumber = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY ClassCode) = 1
),
SQ_Loss AS (
	DECLARE @StartTime DATETIME
	DECLARE @EndTime DATETIME
	
	SET @StartTime = DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0) 
	SET @EndTime = DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0))
	
	SELECT distinct
	LMC.loss_master_calculation_id,
	CD.clndr_date,
	POL.prim_bus_class_code,
	RL.StateProvinceCode,
	RL.RatingCounty,
	RL.RatingCity,
	OCC.claim_loss_date,
	LMC.sub_line_code,
	LMC.class_code,
	CT.cause_of_loss,
	RL.RiskTerritory,
	POL.pol_eff_date,
	POL.pol_key,
	OCC.claim_occurrence_num,
	CPO.claimant_num,
	(case when LMC.trans_kind_code = 'D' then  LMC.paid_loss_amt else 0 end) as paid_loss_amt,
	(Case when LMC.financialtypecode = 'D' and LMC.trans_kind_code = 'D' Then LMF.outstanding_amt Else 0 End) as outstanding_amt,
	CCD.pms_type_bureau_code,
	SC.RiskUnitGroup,
	CCD.PolicySourceID,
	'N/A' AS RiskType,
	'N/A' AS CoverageType,
	SC.StatisticalCoverageAKID,
	-1 AS RatingCoverageAKID,
	POL.pol_exp_date,
	OCC.s3p_claim_num,
	CT.claim_trans_id,
	CCD.claimant_cov_det_ak_id,
	ASL.asl_num,
	PC.InsuranceLine,
	POL.pol_num,
	LMC.statistical_code1,
	CASE WHEN CT.trans_date<@StartTime THEN CD.clndr_date ELSE CT.trans_date END AS trans_date,
	'N/A' as RatingTerritoryCode
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_fact LMF with (nolock)
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim ASL with (nolock)
	on LMF.asl_dim_id=ASL.asl_dim_id
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim CD with (nolock) 
	ON LMF.loss_master_run_date_id = CD.clndr_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation LMC with (nolock)
	on LMC.loss_master_calculation_id=LMF.edw_loss_master_calculation_pk_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction CT with (nolock)
	on LMC.claim_trans_ak_id=CT.claim_trans_ak_id
	and LMC.crrnt_snpsht_flag=1
	and CT.crrnt_snpsht_flag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD with (nolock)
	ON CT.claimant_cov_det_ak_id= CCD.claimant_cov_det_ak_id
	AND CCD.crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO with (nolock)
	ON CPO.claim_party_occurrence_ak_id=CCD.claim_party_occurrence_ak_id
	AND CPO.Crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence OCC with (nolock)
	ON CPO.claim_occurrence_ak_id= OCC.claim_occurrence_ak_id
	AND  OCC.crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim PD with(nolock)
	on LMF.pol_dim_id=PD.pol_dim_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL with (nolock)
	on POL.pol_id=PD.edw_pol_pk_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG with (nolock)
	on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
	and ISG.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC with (nolock)
	on SC.StatisticalCoverageAKID=CCD.StatisticalCoverageAKID
	and SC.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC with (nolock)
	on PC.PolicyCoverageAKID=SC.PolicyCoverageAKID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL with (nolock)
	on PC.RiskLocationAKID=RL.RiskLocationAKID
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRC with (nolock)
	on IRC.InsuranceReferenceCoverageDimId=LMF.InsuranceReferenceCoverageDimId
	and NOT (IRC.InsuranceLineCode='CF' and IRC.CoverageCode='BOILER - BRK')
	where CCD.pms_type_bureau_code IN ('CF','CR','BT','FT')
	AND LMC.trans_kind_code='D'
	AND ISG.InsuranceSegmentCode IN ('1','2')
	AND CD.clndr_date between @StartTime AND @EndTime
	AND (LMC.paid_loss_amt<>0 or LMC.outstanding_amt<>0)
	AND LMF.audit_id > 0 
	--and 1=2
	@{pipeline().parameters.WHERE_CLAUSE_2}
	
	--DCT
	union all
	SELECT distinct
	LMC.loss_master_calculation_id,
	CD.clndr_date,
	POL.prim_bus_class_code,
	RL.StateProvinceCode,
	RL.RatingCounty,
	RL.RatingCity,
	OCC.claim_loss_date,
	LMC.sub_line_code,
	LMC.class_code,
	CT.cause_of_loss,
	RL.RiskTerritory,
	POL.pol_eff_date,
	POL.pol_key,
	OCC.claim_occurrence_num,
	CPO.claimant_num,
	(case when LMC.trans_kind_code = 'D' then  LMC.paid_loss_amt else 0 end) as paid_loss_amt,
	(Case when LMC.financialtypecode  = 'D' and LMC.trans_kind_code = 'D' Then LMF.outstanding_amt Else 0 End) as outstanding_amt,
	CCD.pms_type_bureau_code,
	'N/A' AS RiskUnitGroup,
	CCD.PolicySourceID,
	LTRIM(RTRIM(RC.RiskType)) AS RiskType,
	LTRIM(RTRIM(RC.CoverageType)) AS CoverageType,
	-1 as StatisticalCoverageAKID,
	RC.RatingCoverageAKID,
	POL.pol_exp_date,
	OCC.s3p_claim_num,
	CT.claim_trans_id,
	CCD.claimant_cov_det_ak_id,
	ASL.asl_num,
	SIL.StandardInsuranceLineCode InsuranceLine,
	POL.pol_num,
	LMC.statistical_code1,
	CASE WHEN CT.trans_date<@StartTime THEN CD.clndr_date ELSE CT.trans_date END AS trans_date,
	ISNULL(PTRR.RatingTerritoryCode,'N/A') as RatingTerritoryCode
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_fact LMF with (nolock)
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim ASL with (nolock)
	on LMF.asl_dim_id=ASL.asl_dim_id
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim CD with (nolock) 
	ON LMF.loss_master_run_date_id = CD.clndr_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation LMC with (nolock)
	on LMC.loss_master_calculation_id=LMF.edw_loss_master_calculation_pk_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction CT with (nolock)
	on LMC.claim_trans_ak_id=CT.claim_trans_ak_id
	and LMC.crrnt_snpsht_flag=1
	and CT.crrnt_snpsht_flag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD with (nolock)
	ON CT.claimant_cov_det_ak_id= CCD.claimant_cov_det_ak_id
	AND CCD.crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO with (nolock)
	ON CPO.claim_party_occurrence_ak_id=CCD.claim_party_occurrence_ak_id
	AND CPO.Crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence OCC with (nolock)
	ON CPO.claim_occurrence_ak_id= OCC.claim_occurrence_ak_id
	AND  OCC.crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim PD with(nolock)
	on LMF.pol_dim_id=PD.pol_dim_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL with (nolock)
	on POL.pol_id=PD.edw_pol_pk_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG with (nolock)
	on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
	and ISG.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC with (nolock)
	on CCD.RatingCoverageAKID=RC.RatingCoverageAKID
	and (case when LMC.trans_offset_onset_ind='O' and LMC.pms_acct_entered_date != '1800-01-01 01:00:00.000'
	then LMC.pms_acct_entered_date
	else DATEADD(D,1,LMC.loss_master_run_date)  end) between RC.EffectiveDate and RC.ExpirationDate 
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC with (nolock)
	on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
	and PC.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL with (nolock)
	on SIL.source_sys_id='DCT' and SIL.ins_line_code=PC.InsuranceLine
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL with (nolock)
	on PC.RiskLocationAKID=RL.RiskLocationAKID
	and RL.CurrentSnapshotFlag=1 
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRC with (nolock)
	on IRC.InsuranceReferenceCoverageDimId=LMF.InsuranceReferenceCoverageDimId
	and NOT (IRC.InsuranceLineCode='CF' and IRC.CoverageCode='BOILER - BRK')
	left join PremiumTransactionRatingRisk PTRR on PTRR.PremiumTransactionAKID=LMC.PremiumTransactionAKID
	where PC.Insuranceline in ('SBOPProperty','Crime','Property')
	AND LMC.trans_kind_code='D'
	AND ISG.InsuranceSegmentCode IN ('1','2')
	AND CD.clndr_date between @StartTime AND @EndTime
	AND (LMC.paid_loss_amt<>0 or LMC.outstanding_amt<>0)
	AND RC.CoverageType NOT IN('DataCompromise','EquipmentBreakdown','RatingGroup','MineSubsidence')
	AND RC.SublineCode<>'920'
	AND LMF.audit_id > 0 
	--and 1=2
	@{pipeline().parameters.WHERE_CLAUSE_2}
),
AGG_RemoveDuplicate AS (
	SELECT
	loss_master_calculation_id,
	loss_master_run_date,
	prim_bus_class_code,
	StateProvinceCode,
	RatingCounty,
	RatingCity,
	claim_loss_date,
	sub_line_code,
	class_code,
	cause_of_loss,
	RiskTerritory,
	pol_eff_date,
	pol_key,
	claim_occurrence_num,
	claimant_num,
	paid_loss_amt,
	outstanding_amt,
	pms_type_bureau_code,
	RiskUnitGroup,
	PolicySourceID,
	RiskType,
	CoverageType,
	StatisticalCoverageAKID,
	RatingCoverageAKID,
	pol_exp_date,
	s3p_claim_num,
	claim_trans_id,
	claim_coverage_detail_ak_id,
	asl_num,
	InsuranceLine,
	pol_num,
	statistical_code1,
	trans_date,
	RatingTerritoryCode
	FROM SQ_Loss
	QUALIFY ROW_NUMBER() OVER (PARTITION BY loss_master_calculation_id ORDER BY NULL) = 1
),
EXP_Type_Bureau_code AS (
	SELECT
	pms_type_bureau_code,
	loss_master_run_date AS i_loss_master_run_date,
	-- *INF*: TO_CHAR(i_loss_master_run_date, 'YYYYMMDD')
	TO_CHAR(i_loss_master_run_date, 'YYYYMMDD') AS o_loss_master_run_datekey
	FROM AGG_RemoveDuplicate
),
LKP_ISSWorkTable_Loss AS (
	SELECT
	EDWLossMasterCalculationPKId
	FROM (
		SELECT 
			EDWLossMasterCalculationPKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ISSCommercialPropertyExtract
		WHERE CONVERT (DATE,LossMasterRunDate)
		between 
		 DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0) 
		 AND
		 DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0))
		 and 
		 EDWLossMasterCalculationPKId<>-1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWLossMasterCalculationPKId ORDER BY EDWLossMasterCalculationPKId) = 1
),
SRT_Sort_data AS (
	SELECT
	AGG_RemoveDuplicate.loss_master_calculation_id, 
	AGG_RemoveDuplicate.claim_coverage_detail_ak_id, 
	EXP_Type_Bureau_code.o_loss_master_run_datekey AS loss_master_run_datekey, 
	AGG_RemoveDuplicate.loss_master_run_date, 
	AGG_RemoveDuplicate.prim_bus_class_code, 
	AGG_RemoveDuplicate.StateProvinceCode, 
	AGG_RemoveDuplicate.RatingCounty, 
	AGG_RemoveDuplicate.RatingCity, 
	AGG_RemoveDuplicate.claim_loss_date, 
	AGG_RemoveDuplicate.sub_line_code, 
	AGG_RemoveDuplicate.class_code, 
	AGG_RemoveDuplicate.cause_of_loss, 
	AGG_RemoveDuplicate.RiskTerritory, 
	AGG_RemoveDuplicate.pol_eff_date, 
	AGG_RemoveDuplicate.pol_key, 
	AGG_RemoveDuplicate.claim_occurrence_num, 
	AGG_RemoveDuplicate.claimant_num, 
	AGG_RemoveDuplicate.paid_loss_amt, 
	AGG_RemoveDuplicate.outstanding_amt, 
	EXP_Type_Bureau_code.pms_type_bureau_code, 
	AGG_RemoveDuplicate.RiskUnitGroup, 
	AGG_RemoveDuplicate.PolicySourceID, 
	AGG_RemoveDuplicate.RiskType, 
	AGG_RemoveDuplicate.CoverageType, 
	AGG_RemoveDuplicate.StatisticalCoverageAKID, 
	AGG_RemoveDuplicate.RatingCoverageAKID, 
	AGG_RemoveDuplicate.pol_exp_date, 
	AGG_RemoveDuplicate.s3p_claim_num, 
	AGG_RemoveDuplicate.claim_trans_id, 
	LKP_ISSWorkTable_Loss.EDWLossMasterCalculationPKId, 
	AGG_RemoveDuplicate.asl_num, 
	AGG_RemoveDuplicate.InsuranceLine, 
	AGG_RemoveDuplicate.pol_num, 
	AGG_RemoveDuplicate.statistical_code1, 
	AGG_RemoveDuplicate.trans_date, 
	AGG_RemoveDuplicate.RatingTerritoryCode
	FROM LKP_ISSWorkTable_Loss
	ORDER BY loss_master_calculation_id ASC, claim_coverage_detail_ak_id ASC, loss_master_run_datekey ASC
),
LKP_InceptionToDatePaidLossAmount AS (
	SELECT
	InceptionToDatePaidLossAmount,
	pol_key,
	edw_claimant_cov_det_ak_id,
	trans_date,
	loss_master_calculation_id
	FROM (
		--Altered for US403701
		
		DECLARE @StartTime DATETIME
		DECLARE @EndTime DATETIME
		
		
		
		
		
		
		
		SET @StartTime = DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0)
		SET @EndTime = DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0))
		
		
		
		
		
		
		
		SELECT DISTINCT
		--Old logic for OutstandingAmount records
		--CASE WHEN InceptionToDatePaidLossAmount=0 and PaidLossAmount=0 THEN LAST_VALUE(InceptionToDatePaidLossAmount) OVER (partition by pol_key,edw_claimant_cov_det_ak_id,year(trans_date), month(trans_date) order by trans_date rows between unbounded preceding and unbounded following )
		--CASE WHEN InceptionToDatePaidLossAmount=0 and PaidLossAmount=0 THEN SUM(PaidLossAmount) OVER (order by loss_master_run_date)
		CASE WHEN InceptionToDatePaidLossAmount=0 and PaidLossAmount=0 THEN SUM(PaidLossAmount) OVER (partition by pol_key,edw_claimant_cov_det_ak_id,claim_num order by loss_master_run_date)
		ELSE InceptionToDatePaidLossAmount END AS InceptionToDatePaidLossAmount,
		pol_key AS pol_key,
		edw_claimant_cov_det_ak_id AS edw_claimant_cov_det_ak_id,
		trans_date AS trans_date
		,loss_master_calculation_id AS loss_master_calculation_id
		FROM
		(
		SELECT distinct SUM(InceptionToDatePaidLossAmount) over (partition by pol_key,edw_claimant_cov_det_ak_id,claim_num order by edw_claimant_cov_det_ak_id,trans_date,claim_trans_pk_id) AS InceptionToDatePaidLossAmount,
		edw_claimant_cov_det_ak_id AS edw_claimant_cov_det_ak_id,
		trans_date AS trans_date,
		InceptionToDatePaidLossAmount AS PaidLossAmount,
		pol_key
		,loss_master_calculation_id
		,loss_master_run_date
		,claim_num
		FROM (
		SELECT f.direct_loss_paid_including_recoveries AS InceptionToDatePaidLossAmount,
		--f.direct_loss_outstanding_excluding_recoveries AS OutstandingAmount, --US-403701 Commenting out since we don't need it
		edw_claimant_cov_det_ak_id AS edw_claimant_cov_det_ak_id,
		ct.trans_date,
		p.pol_key AS pol_key,
		o.claim_num AS claim_num,
		lmc.loss_master_calculation_id,
		lmc.loss_master_run_date,
		lmc.claim_trans_pk_id
		from @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_loss_transaction_fact f
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_dim d
		on f.claimant_cov_dim_id = d.claimant_cov_dim_id
		inner join vw_claim_transaction ct
		on f.edw_claim_trans_pk_id=ct.claim_trans_id
		and ct.trans_date<'2001-01-01'
		join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim p
		on f.pol_dim_id=p.pol_dim_id
		join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim O
		on F.claim_occurrence_dim_id=o.claim_occurrence_dim_id
		--Join added for US-403701
		left join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.vwLossMasterFact lmf
		on lmf.claimant_cov_dim_id = d.claimant_cov_dim_id
		join loss_master_calculation lmc
		on lmc.loss_master_calculation_id = lmf.edw_loss_master_calculation_pk_id
		UNION ALL
		SELECT f.DirectLossPaidIR AS InceptionToDatePaidLossAmount,
		--f.DirectLossOutstandingER AS OutstandingAmount, --US-403701 Commenting out since we don't need it
		edw_claimant_cov_det_ak_id AS edw_claimant_cov_det_ak_id,
		ct.trans_date,
		p.pol_key AS pol_key,
		o.claim_num AS claim_num,
		lmc.loss_master_calculation_id,
		lmc.loss_master_run_date,
		lmc.claim_trans_pk_id
		from @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.vwLossMasterFact f
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_dim d
		on f.claimant_cov_dim_id = d.claimant_cov_dim_id
		inner join vw_claim_transaction ct
		on f.edw_claim_trans_pk_id=ct.claim_trans_id
		and ct.trans_date>='2001-01-01'
		join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim p
		on f.pol_dim_id=p.pol_dim_id
		join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim O
		on F.claim_occurrence_dim_id=o.claim_occurrence_dim_id
		--Join added for US-403701
		join loss_master_calculation lmc
		on lmc.loss_master_calculation_id = f.edw_loss_master_calculation_pk_id
		) T
		) T
		WHERE cast(trans_date as date)<=@EndTime
		ORDER BY pol_key,edw_claimant_cov_det_ak_id,trans_date
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key,edw_claimant_cov_det_ak_id,trans_date,loss_master_calculation_id ORDER BY InceptionToDatePaidLossAmount DESC) = 1
),
FIL_Exists_Loss AS (
	SELECT
	SRT_Sort_data.EDWLossMasterCalculationPKId AS LKP_LossMasterCalculationId, 
	SRT_Sort_data.loss_master_calculation_id, 
	SRT_Sort_data.loss_master_run_date, 
	SRT_Sort_data.prim_bus_class_code, 
	SRT_Sort_data.StateProvinceCode, 
	SRT_Sort_data.RatingCounty, 
	SRT_Sort_data.RatingCity, 
	SRT_Sort_data.claim_loss_date, 
	SRT_Sort_data.sub_line_code, 
	SRT_Sort_data.class_code AS ClassCode, 
	SRT_Sort_data.cause_of_loss, 
	SRT_Sort_data.RiskTerritory, 
	SRT_Sort_data.pol_eff_date, 
	SRT_Sort_data.pol_key, 
	SRT_Sort_data.claim_occurrence_num, 
	SRT_Sort_data.claimant_num, 
	SRT_Sort_data.paid_loss_amt, 
	SRT_Sort_data.outstanding_amt, 
	SRT_Sort_data.pms_type_bureau_code AS TypeBureauCode, 
	SRT_Sort_data.RiskUnitGroup, 
	SRT_Sort_data.PolicySourceID, 
	SRT_Sort_data.RiskType, 
	SRT_Sort_data.CoverageType, 
	SRT_Sort_data.StatisticalCoverageAKID, 
	SRT_Sort_data.RatingCoverageAKID, 
	SRT_Sort_data.pol_exp_date, 
	SRT_Sort_data.s3p_claim_num, 
	SRT_Sort_data.claim_trans_id, 
	SRT_Sort_data.claim_coverage_detail_ak_id, 
	LKP_InceptionToDatePaidLossAmount.InceptionToDatePaidLossAmount AS out_CumulativeInceptiontoDatePaidLoss, 
	SRT_Sort_data.asl_num, 
	SRT_Sort_data.InsuranceLine, 
	SRT_Sort_data.pol_num, 
	SRT_Sort_data.statistical_code1, 
	SRT_Sort_data.RatingTerritoryCode
	FROM SRT_Sort_data
	LEFT JOIN LKP_InceptionToDatePaidLossAmount
	ON LKP_InceptionToDatePaidLossAmount.pol_key = SRT_Sort_data.pol_key AND LKP_InceptionToDatePaidLossAmount.edw_claimant_cov_det_ak_id = SRT_Sort_data.claim_coverage_detail_ak_id AND LKP_InceptionToDatePaidLossAmount.trans_date <= SRT_Sort_data.trans_date AND LKP_InceptionToDatePaidLossAmount.loss_master_calculation_id = SRT_Sort_data.loss_master_calculation_id
	WHERE ISNULL(LKP_LossMasterCalculationId) AND  
(paid_loss_amt != 0 or outstanding_amt!=0)
),
EXP_Cleansing_Loss AS (
	SELECT
	loss_master_calculation_id AS i_loss_master_calculation_id,
	loss_master_run_date AS i_loss_master_run_date,
	prim_bus_class_code AS i_prim_bus_class_code,
	StateProvinceCode AS i_StateProvinceCode,
	RatingCounty AS i_RatingCounty,
	RatingCity AS i_RatingCity,
	claim_loss_date AS i_claim_loss_date,
	sub_line_code AS i_sub_line_code,
	ClassCode AS i_ClassCode,
	cause_of_loss AS i_cause_of_loss,
	RiskTerritory AS i_RiskTerritory,
	pol_eff_date AS i_pol_eff_date,
	pol_key AS i_pol_key,
	claim_occurrence_num AS i_claim_occurrence_num,
	claimant_num AS i_claimant_num,
	paid_loss_amt AS i_paid_loss_amt,
	outstanding_amt AS i_outstanding_amt,
	TypeBureauCode AS i_TypeBureauCode,
	RiskUnitGroup AS i_RiskUnitGroup,
	PolicySourceID AS i_PolicySourceID,
	RiskType AS i_RiskType,
	CoverageType AS i_CoverageType,
	StatisticalCoverageAKID AS i_StatisticalCoverageAKID,
	pol_exp_date AS i_pol_exp_date,
	s3p_claim_num,
	out_CumulativeInceptiontoDatePaidLoss AS i_CumulativeInceptiontoDatePaidLoss,
	asl_num AS i_asl_num,
	claim_coverage_detail_ak_id,
	i_loss_master_calculation_id AS o_loss_master_calculation_id,
	i_loss_master_run_date AS o_loss_master_run_date,
	-- *INF*: RTRIM(LTRIM(i_pol_key))
	RTRIM(LTRIM(i_pol_key)) AS o_pol_key,
	-- *INF*: RTRIM(LTRIM(i_prim_bus_class_code))
	RTRIM(LTRIM(i_prim_bus_class_code)) AS o_prim_bus_class_code,
	-- *INF*: RTRIM(LTRIM(i_StateProvinceCode))
	RTRIM(LTRIM(i_StateProvinceCode)) AS o_StateProvinceCode,
	-- *INF*: RTRIM(LTRIM(i_RatingCounty))
	RTRIM(LTRIM(i_RatingCounty)) AS o_RatingCounty,
	-- *INF*: LTRIM(RTRIM(i_RatingCity))
	LTRIM(RTRIM(i_RatingCity)) AS o_RatingCity,
	i_claim_loss_date AS o_claim_loss_date,
	-- *INF*: RTRIM(LTRIM(i_sub_line_code))
	RTRIM(LTRIM(i_sub_line_code)) AS o_sub_line_code,
	-- *INF*: RTRIM(LTRIM(i_ClassCode))
	RTRIM(LTRIM(i_ClassCode)) AS o_ClassCode,
	-- *INF*: RTRIM(LTRIM(i_cause_of_loss))
	RTRIM(LTRIM(i_cause_of_loss)) AS o_cause_of_loss,
	-- *INF*: RTRIM(LTRIM(i_RiskTerritory))
	RTRIM(LTRIM(i_RiskTerritory)) AS o_RiskTerritory,
	i_pol_eff_date AS o_pol_eff_date,
	-- *INF*: RTRIM(LTRIM(i_claim_occurrence_num))
	RTRIM(LTRIM(i_claim_occurrence_num)) AS o_claim_occurrence_num,
	-- *INF*: RTRIM(LTRIM(i_claimant_num))
	RTRIM(LTRIM(i_claimant_num)) AS o_claimant_num,
	-- *INF*: IIF(ISNULL(i_paid_loss_amt), 0, i_paid_loss_amt)
	-- 
	-- ---i_paid_loss_amt
	IFF(i_paid_loss_amt IS NULL, 0, i_paid_loss_amt) AS o_PaidLossAmount,
	-- *INF*: IIF(ISNULL(i_outstanding_amt), 0, i_outstanding_amt)
	-- 
	-- --i_outstanding_amt
	IFF(i_outstanding_amt IS NULL, 0, i_outstanding_amt) AS o_OutstandingLossAmount,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_TypeBureauCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_TypeBureauCode) AS o_TypeBureauCode,
	-- *INF*: RTRIM(LTRIM(i_RiskUnitGroup))
	RTRIM(LTRIM(i_RiskUnitGroup)) AS o_RiskUnitGroup,
	-- *INF*: LTRIM(RTRIM(i_PolicySourceID))
	LTRIM(RTRIM(i_PolicySourceID)) AS o_PolicySourceID,
	-- *INF*: LTRIM(RTRIM(i_RiskType))
	LTRIM(RTRIM(i_RiskType)) AS o_RiskType,
	-- *INF*: LTRIM(RTRIM(i_CoverageType))
	LTRIM(RTRIM(i_CoverageType)) AS o_CoverageType,
	i_StatisticalCoverageAKID AS o_StatisticalCoverageAKID,
	i_pol_exp_date AS o_pol_exp_date,
	-- *INF*: RTRIM(LTRIM(s3p_claim_num))
	RTRIM(LTRIM(s3p_claim_num)) AS o_s3p_claim_num,
	i_CumulativeInceptiontoDatePaidLoss AS o_CumulativeInceptiontoDatePaidLoss,
	i_asl_num AS o_AnnualStatementLineNumber,
	InsuranceLine AS i_InsuranceLine,
	-- *INF*: LTRIM(RTRIM(i_InsuranceLine))
	LTRIM(RTRIM(i_InsuranceLine)) AS o_InsuranceLine,
	pol_num,
	statistical_code1 AS i_statistical_code1,
	-- *INF*: DECODE(TRUE,
	-- isnull(i_statistical_code1),'N/A',
	-- ltrim(rtrim(i_statistical_code1))='','N/A',
	-- length(rtrim(ltrim(i_statistical_code1))) < 5,'N/A',
	-- i_statistical_code1)
	DECODE(
	    TRUE,
	    i_statistical_code1 IS NULL, 'N/A',
	    ltrim(rtrim(i_statistical_code1)) = '', 'N/A',
	    length(rtrim(ltrim(i_statistical_code1))) < 5, 'N/A',
	    i_statistical_code1
	) AS v_statistical_code1,
	-- *INF*: IIF(v_statistical_code1 != 'N/A',substr(v_statistical_code1,3,1),'N/A')
	IFF(v_statistical_code1 != 'N/A', substr(v_statistical_code1, 3, 1), 'N/A') AS o_pms_const_code,
	-- *INF*: IIF(v_statistical_code1 != 'N/A',substr(v_statistical_code1,4,2),'N/A')
	IFF(v_statistical_code1 != 'N/A', substr(v_statistical_code1, 4, 2), 'N/A') AS o_pms_iso_ppc_code,
	RatingCoverageAKID,
	RatingTerritoryCode
	FROM FIL_Exists_Loss
),
EXP_GetCoverageAKID AS (
	SELECT
	StatisticalCoverageAKID AS i_StatisticalCoverageAKID,
	RatingCoverageAKID AS i_RatingCoverageAKID,
	-- *INF*: iif(i_StatisticalCoverageAKID=-1,i_RatingCoverageAKID,i_StatisticalCoverageAKID)
	IFF(i_StatisticalCoverageAKID = - 1, i_RatingCoverageAKID, i_StatisticalCoverageAKID) AS o_CoverageAKID
	FROM FIL_Exists_Loss
),
LKP_PremiumTransaction AS (
	SELECT
	PackageModificationAdjustmentGroupCode,
	ConstructionCode,
	IsoFireProtectionCode,
	BureauCode1,
	BureauCode2,
	BureauCode4,
	DeductibleAmount,
	in_CoverageAKID,
	CoverageAKID
	FROM (
		DECLARE @StartTime DATETIME
		DECLARE @EndTime DATETIME
		
		SET @StartTime = DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0) 
		SET @EndTime = DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0))
		--PROD-15129 added deductible amount in lookup
		SELECT distinct 
		PremiumTransaction.PackageModificationAdjustmentGroupCode as PackageModificationAdjustmentGroupCode, 
		PremiumTransaction.ConstructionCode as ConstructionCode, 
		CoverageDetailCommercialProperty.IsoFireProtectionCode as IsoFireProtectionCode, 
		LTRIM(RTRIM(BureauCode1)) as BureauCode1,
		LTRIM(RTRIM(BureauCode2)) as BureauCode2,
		LTRIM(RTRIM(BureauCode4)) as BureauCode4,
		PremiumTransaction.DeductibleAmount as Deductibleamount,
		PremiumTransaction.StatisticalCoverageAKID as CoverageAKID
		FROM 
		loss_master_calculation with (nolock)
		inner join dbo.claim_transaction
		on claim_transaction.claim_trans_ak_id=loss_master_calculation.claim_trans_ak_id
		and loss_master_calculation.crrnt_snpsht_flag=1
		and claim_transaction.crrnt_snpsht_flag=1
		inner join dbo.claimant_coverage_detail
		on claimant_coverage_detail.claimant_cov_det_ak_id=claim_transaction.claimant_cov_det_ak_id
		and claimant_coverage_detail.crrnt_snpsht_flag=1
		inner join dbo.StatisticalCoverage
		on claimant_coverage_detail.StatisticalCoverageAKID=StatisticalCoverage.StatisticalCoverageAKID
		and  CONVERT (DATE,loss_master_calculation.loss_master_run_date)
		BETWEEN @StartTime AND @EndTime
		inner join PremiumTransaction with (nolock) on StatisticalCoverage.StatisticalCoverageAKID = PremiumTransaction.StatisticalCoverageAKID 
		inner join BureauStatisticalCode with (nolock) on BureauStatisticalCode.PremiumTransactionAKID = PremiumTransaction.PremiumTransactionAKID
		left join dbo.CoverageDetailCommercialProperty with (nolock)
		on CoverageDetailCommercialProperty.PremiumTransactionID=PremiumTransaction.PremiumTransactionID
		and CoverageDetailCommercialProperty.CurrentSnapshotFlag=1
		left join claim_occurrence co with (nolock)
		on loss_master_calculation.claim_occurrence_ak_id=co.claim_occurrence_ak_id
		and co.claim_loss_date between PremiumTransaction.EffectiveDate and PremiumTransaction.ExpirationDate
		WHERE PremiumTransaction.SourceSystemID='PMS'
		
		union all
		SELECT distinct 
		PremiumTransaction.PackageModificationAdjustmentGroupCode as PackageModificationAdjustmentGroupCode, 
		PremiumTransaction.ConstructionCode as ConstructionCode, 
		CoverageDetailCommercialProperty.IsoFireProtectionCode as IsoFireProtectionCode, 
		null as BureauCode1,
		null as BureauCode2,
		null as BureauCode4,
		NULL as Deductibleamount,
		PremiumTransaction.RatingCoverageAKId as RatingCoverageAKId 
		FROM 
		loss_master_calculation with (nolock) inner join 
		RatingCoverage with (nolock) on loss_master_calculation.RatingCoverageAKId = RatingCoverage.RatingCoverageAKId 
		and CONVERT (DATE,loss_master_calculation.loss_master_run_date)
		BETWEEN
		@StartTime  AND @EndTime
		inner join PremiumTransaction with (nolock) 
		on RatingCoverage.RatingCoverageAKId = PremiumTransaction.RatingCoverageAKId 
		and RatingCoverage.EffectiveDate=PremiumTransaction.EffectiveDate
		left join dbo.CoverageDetailCommercialProperty with (nolock)
		on CoverageDetailCommercialProperty.PremiumTransactionID=PremiumTransaction.PremiumTransactionID
		and CoverageDetailCommercialProperty.CurrentSnapshotFlag=1
		left join claim_occurrence co with (nolock)
		on loss_master_calculation.claim_occurrence_ak_id=co.claim_occurrence_ak_id
		and co.claim_loss_date between PremiumTransaction.EffectiveDate and PremiumTransaction.ExpirationDate
		WHERE PremiumTransaction.SourceSystemID='DCT'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageAKID ORDER BY PackageModificationAdjustmentGroupCode) = 1
),
EXP_Transform_Prior_to_lookup AS (
	SELECT
	LKP_PremiumTransaction.PackageModificationAdjustmentGroupCode,
	FIL_Exists_Loss.StatisticalCoverageAKID AS in_StatisticalCoverageAKID,
	-- *INF*: IIF(in_StatisticalCoverageAKID=-1,'DCT','PMS')
	IFF(in_StatisticalCoverageAKID = - 1, 'DCT', 'PMS') AS out_SourceSystem
	FROM FIL_Exists_Loss
	LEFT JOIN LKP_PremiumTransaction
	ON LKP_PremiumTransaction.CoverageAKID = EXP_GetCoverageAKID.o_CoverageAKID
),
EXP_Reset_Pms_ConstCode_IsoPPC AS (
	SELECT
	EXP_Cleansing_Loss.o_TypeBureauCode AS i_TypeBureauCode,
	EXP_Cleansing_Loss.o_pms_const_code AS i_pms_const_code,
	EXP_Cleansing_Loss.o_pms_iso_ppc_code AS i_pms_iso_ppc_code,
	EXP_Transform_Prior_to_lookup.out_SourceSystem AS i_SourceSystem,
	LKP_PremiumTransaction.ConstructionCode AS lkp_ConstructionCode,
	LKP_PremiumTransaction.IsoFireProtectionCode AS lkp_IsoFireProtectionCode,
	-- *INF*: IIF(i_SourceSystem='PMS' and i_pms_const_code != 'N/A' and ltrim(rtrim(i_TypeBureauCode))='CF',i_pms_const_code,lkp_ConstructionCode)
	IFF(
	    i_SourceSystem = 'PMS' and i_pms_const_code != 'N/A' and ltrim(rtrim(i_TypeBureauCode)) = 'CF',
	    i_pms_const_code,
	    lkp_ConstructionCode
	) AS v_const_code,
	-- *INF*: IIF(i_SourceSystem='PMS' and i_pms_iso_ppc_code != 'N/A' and ltrim(rtrim(i_TypeBureauCode))='CF',i_pms_iso_ppc_code,lkp_IsoFireProtectionCode)
	IFF(
	    i_SourceSystem = 'PMS'
	    and i_pms_iso_ppc_code != 'N/A'
	    and ltrim(rtrim(i_TypeBureauCode)) = 'CF',
	    i_pms_iso_ppc_code,
	    lkp_IsoFireProtectionCode
	) AS v_iso_code,
	v_const_code AS o_ConsturctionCode,
	v_iso_code AS o_IsoFireProtectionCode
	FROM EXP_Cleansing_Loss
	 -- Manually join with EXP_Transform_Prior_to_lookup
	LEFT JOIN LKP_PremiumTransaction
	ON LKP_PremiumTransaction.CoverageAKID = EXP_GetCoverageAKID.o_CoverageAKID
),
EXP_Logic_Loss AS (
	SELECT
	EXP_Cleansing_Loss.o_loss_master_calculation_id AS loss_master_calculation_id,
	EXP_Cleansing_Loss.claim_coverage_detail_ak_id,
	-1 AS PremiumMasterCalculationID,
	-- *INF*: TO_DATE('1800-01-01','YYYY/MM/DD')
	TO_TIMESTAMP('1800-01-01', 'YYYY/MM/DD') AS PremiumMasterRunDate,
	EXP_Cleansing_Loss.o_loss_master_run_date AS loss_master_run_date,
	EXP_Cleansing_Loss.o_pol_key AS pol_key,
	EXP_Cleansing_Loss.o_prim_bus_class_code AS prim_bus_class_code,
	EXP_Cleansing_Loss.o_StateProvinceCode AS StateProvinceCode,
	EXP_Cleansing_Loss.o_RatingCounty AS RatingCounty,
	EXP_Cleansing_Loss.o_RatingCity AS RatingCity,
	EXP_Cleansing_Loss.o_claim_loss_date AS claim_loss_date,
	EXP_Cleansing_Loss.o_sub_line_code AS sub_line_code,
	-- *INF*: DECODE(TRUE,sub_line_code='025','027',sub_line_code)
	DECODE(
	    TRUE,
	    sub_line_code = '025', '027',
	    sub_line_code
	) AS sub_line_code_out,
	EXP_Cleansing_Loss.o_ClassCode AS ClassCode,
	'N/A' AS PremiumMasterClassCode_out,
	-- *INF*: IIF(ISNULL(ClassCode) or LENGTH(ClassCode)=0,'N/A',ClassCode)
	IFF(ClassCode IS NULL or LENGTH(ClassCode) = 0, 'N/A', ClassCode) AS LossMasterClassCode_out,
	EXP_Cleansing_Loss.o_cause_of_loss AS cause_of_loss,
	-- *INF*: IIF(ISNULL(cause_of_loss) OR LENGTH(cause_of_loss)=0,'N/A',cause_of_loss)
	IFF(cause_of_loss IS NULL OR LENGTH(cause_of_loss) = 0, 'N/A', cause_of_loss) AS cause_of_loss_out,
	EXP_Cleansing_Loss.o_RiskTerritory AS RiskTerritory,
	-- *INF*: RiskTerritory
	-- 
	-- --SUBSTR(RiskTerritory,2,2)
	RiskTerritory AS TerritoryCode_out,
	EXP_Cleansing_Loss.o_pol_eff_date AS pol_eff_date,
	EXP_Cleansing_Loss.o_claim_occurrence_num AS i_claim_occurrence_num,
	EXP_Cleansing_Loss.o_s3p_claim_num AS i_s3p_claim_num,
	EXP_Cleansing_Loss.o_InsuranceLine AS i_InsuranceLine,
	EXP_Cleansing_Loss.pol_num AS i_pol_num,
	EXP_Cleansing_Loss.o_claimant_num AS claimant_num,
	-- *INF*: IIF(ISNULL(i_claim_occurrence_num) OR IS_SPACES(i_claim_occurrence_num) OR LENGTH(i_claim_occurrence_num)=0 OR i_claim_occurrence_num='N/A' OR IS_NUMBER(i_claim_occurrence_num)=0, 0, TO_INTEGER(i_claim_occurrence_num))
	IFF(
	    i_claim_occurrence_num IS NULL
	    or LENGTH(i_claim_occurrence_num)>0
	    and TRIM(i_claim_occurrence_num)=''
	    or LENGTH(i_claim_occurrence_num) = 0
	    or i_claim_occurrence_num = 'N/A'
	    or REGEXP_LIKE(i_claim_occurrence_num, '^[0-9]+$') = 0,
	    0,
	    CAST(i_claim_occurrence_num AS INTEGER)
	) AS v_claim_occurrence_num,
	-- *INF*: LTRIM(RTRIM(i_pol_num))||TO_CHAR(claim_loss_date,'YYMMDD') ||SUBSTR(i_claim_occurrence_num,2,2)
	-- 
	-- --DECODE(TRUE,
	-- --i_InsuranceLine='CF',
	-- --IIF(TRUNC(pol_eff_date, 'MM')  <= TO_DATE('2003-10', 'YYYY-MM'), i_pol_num|| TO_CHAR(ADD_TO_DATE(claim_loss_date, 'DD', v_claim_occurrence_num), 'YYYYMMDD'), i_s3p_claim_num),
	-- --i_InsuranceLine='CR',
	-- --IIF(TRUNC(pol_eff_date, 'MM')  <= TO_DATE('2003-10', 'YYYY-MM'), i_pol_num|| TO_CHAR(ADD_TO_DATE(claim_loss_date, 'DD', v_claim_occurrence_num), 'YYYYMMDD'), i_s3p_claim_num)
	-- --i_pol_num || TO_CHAR(claim_loss_date,'YYMMDD') || SUBSTR(i_claim_occurrence_num,2,2)
	-- --)
	LTRIM(RTRIM(i_pol_num)) || TO_CHAR(claim_loss_date, 'YYMMDD') || SUBSTR(i_claim_occurrence_num, 2, 2) AS v_ClaimNumber,
	-- *INF*: RTRIM(LTRIM(v_ClaimNumber))
	-- --SUBSTR(pol_key,4,7)||TO_CHAR(claim_loss_date,'YYMMDD')||SUBSTR(i_claim_occurrence_num,2,2)
	-- 
	-- --SUBSTR(pol_key,4,7)||SUBSTR(GET_DATE_PART(claim_loss_date,'Y'),-2,2)||LPAD(GET_DATE_PART(claim_loss_date,'MM'),2,'0')||LPAD(GET_DATE_PART(claim_loss_date,'D'),2,'0')||SUBSTR(claim_occurrence_num,2,2)
	RTRIM(LTRIM(v_ClaimNumber)) AS ClaimNum,
	0.00 AS PremiumMasterPremium,
	EXP_Cleansing_Loss.o_PaidLossAmount AS PaidLossAmount,
	EXP_Cleansing_Loss.o_OutstandingLossAmount AS OutstandingLossAmount,
	EXP_Cleansing_Loss.o_TypeBureauCode AS TypeBureauCode,
	EXP_Cleansing_Loss.o_RiskUnitGroup AS RiskUnitGroup,
	EXP_Cleansing_Loss.o_PolicySourceID AS PolicySourceID,
	EXP_Cleansing_Loss.o_RiskType AS RiskType,
	EXP_Cleansing_Loss.o_CoverageType AS CoverageType,
	LKP_PremiumTransaction.DeductibleAmount,
	-- *INF*: IIF(RatingCoverageAKID<>-1 and IN(sub_line_code,'020','027','120'),:LKP.LKP_DEDUCTIBLE_PROPERTY_WINDORHAIL(pol_key,RatingCoverageAKID),DeductibleAmount)
	IFF(
	    RatingCoverageAKID <> - 1 and sub_line_code IN ('020','027','120'),
	    LKP_DEDUCTIBLE_PROPERTY_WINDORHAIL_pol_key_RatingCoverageAKID.CoverageDeductibleValue,
	    DeductibleAmount
	) AS v_Deductible_Property_WindORHail,
	-- *INF*: IIF(RatingCoverageAKID<>-1 and IN(sub_line_code,'020','027','120','010','015','016','017','018','029','035','045','055','110'),:LKP.LKP_DEDUCTIBLE_PROPERTY(pol_key,RatingCoverageAKID),DeductibleAmount)
	IFF(
	    RatingCoverageAKID <> - 1
	    and sub_line_code IN ('020','027','120','010','015','016','017','018','029','035','045','055','110'),
	    LKP_DEDUCTIBLE_PROPERTY_pol_key_RatingCoverageAKID.CoverageDeductibleValue,
	    DeductibleAmount
	) AS v_Deductible_Property,
	-- *INF*: DECODE(TRUE, IN(sub_line_code,'070','090','170','190','930','931'),NULL, ISNULL(v_Deductible_Property_WindORHail),v_Deductible_Property, v_Deductible_Property_WindORHail)
	DECODE(
	    TRUE,
	    sub_line_code IN ('070','090','170','190','930','931'), NULL,
	    v_Deductible_Property_WindORHail IS NULL, v_Deductible_Property,
	    v_Deductible_Property_WindORHail
	) AS v_Deductible_DCT,
	-- *INF*: DECODE(TRUE, 
	-- RatingCoverageAKID=-1 and (LENGTH(DeductibleAmount)=0 OR ISNULL(DeductibleAmount) OR DeductibleAmount='FullCoverage'),'0', RatingCoverageAKID=-1,DeductibleAmount, 
	-- v_Deductible_DCT)  
	-- 
	-- --IIF(LENGTH(DeductibleAmount)=0 OR ISNULL(DeductibleAmount) OR DeductibleAmount='FullCoverage','0',DeductibleAmount)
	DECODE(
	    TRUE,
	    RatingCoverageAKID = - 1 and (LENGTH(DeductibleAmount) = 0 OR DeductibleAmount IS NULL OR DeductibleAmount = 'FullCoverage'), '0',
	    RatingCoverageAKID = - 1, DeductibleAmount,
	    v_Deductible_DCT
	) AS DeductibleAmount_out,
	LKP_PremiumTransaction.BureauCode1,
	LKP_PremiumTransaction.BureauCode2,
	LKP_PremiumTransaction.BureauCode4,
	EXP_Reset_Pms_ConstCode_IsoPPC.o_ConsturctionCode AS in_ConstructionCode,
	EXP_Reset_Pms_ConstCode_IsoPPC.o_IsoFireProtectionCode AS i_IsoFireProtectionCode,
	-- *INF*: DECODE(TRUE,PolicySourceID='PMS',
	-- IIF(ISNULL(BureauCode1) OR LENGTH(BureauCode1)=0 OR(TypeBureauCode  != 'CR' and TypeBureauCode  !='BT' and TypeBureauCode != 'FT' and TypeBureauCode != 'CF'),'N/A',BureauCode1) ,
	-- IIF( ((INSTR(RiskType,'BIEE')>0 or INSTR(RiskType, 'TIME') >0) AND  NOT IN(CoverageType, 'TerrorismRisk', 'TerrorismFireOnly')) OR RiskType='N/A', :LKP.LKP_SUPREFERENCEDATA(CoverageType),
	-- :LKP.LKP_SUPREFERENCEDATA(RiskType))
	-- )
	-- 
	DECODE(
	    TRUE,
	    PolicySourceID = 'PMS', IFF(
	        BureauCode1 IS NULL
	        or LENGTH(BureauCode1) = 0
	        or (TypeBureauCode != 'CR'
	        and TypeBureauCode != 'BT'
	        and TypeBureauCode != 'FT'
	        and TypeBureauCode != 'CF'),
	        'N/A',
	        BureauCode1
	    ),
	    IFF(
	        ((REGEXP_INSTR(RiskType, 'BIEE') > 0
	        or REGEXP_INSTR(RiskType, 'TIME') > 0)
	        and NOT CoverageType IN ('TerrorismRisk','TerrorismFireOnly'))
	        or RiskType = 'N/A',
	        LKP_SUPREFERENCEDATA_CoverageType.ToCode,
	        LKP_SUPREFERENCEDATA_RiskType.ToCode
	    )
	) AS v_CoverageCode,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(v_CoverageCode),'N/A',
	-- v_CoverageCode<>'N/A',LPAD(v_CoverageCode,2,'0'),
	-- 'N/A')
	DECODE(
	    TRUE,
	    v_CoverageCode IS NULL, 'N/A',
	    v_CoverageCode <> 'N/A', LPAD(v_CoverageCode, 2, '0'),
	    'N/A'
	) AS o_CoverageCode,
	-- *INF*: :LKP.LKP_SUPCONSTRUCTIONCODE(LTRIM(RTRIM(in_ConstructionCode)))
	LKP_SUPCONSTRUCTIONCODE_LTRIM_RTRIM_in_ConstructionCode.StandardConstructionCodeDescription AS v_ConstructionCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_ConstructionCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(v_ConstructionCode) AS ConstructionCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_IsoFireProtectionCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_IsoFireProtectionCode) AS IsoFireProtectionCode,
	EXP_Transform_Prior_to_lookup.PackageModificationAdjustmentGroupCode AS i_PackageModificationAdjustmentGroupCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_PackageModificationAdjustmentGroupCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_PackageModificationAdjustmentGroupCode) AS PackageModificationAdjustmentGroupCode,
	EXP_Cleansing_Loss.o_pol_exp_date AS pol_exp_date,
	EXP_Cleansing_Loss.o_CumulativeInceptiontoDatePaidLoss AS CumulativeInceptiontoDatePaidLoss,
	EXP_Cleansing_Loss.o_AnnualStatementLineNumber AS AnnualStatementLineNumber,
	'N/A' AS o_LocationNumber,
	'N/A' AS o_BuildingNumber,
	EXP_Cleansing_Loss.RatingCoverageAKID,
	EXP_Cleansing_Loss.RatingTerritoryCode
	FROM EXP_Cleansing_Loss
	 -- Manually join with EXP_Reset_Pms_ConstCode_IsoPPC
	 -- Manually join with EXP_Transform_Prior_to_lookup
	LEFT JOIN LKP_PremiumTransaction
	ON LKP_PremiumTransaction.CoverageAKID = EXP_GetCoverageAKID.o_CoverageAKID
	LEFT JOIN LKP_DEDUCTIBLE_PROPERTY_WINDORHAIL LKP_DEDUCTIBLE_PROPERTY_WINDORHAIL_pol_key_RatingCoverageAKID
	ON LKP_DEDUCTIBLE_PROPERTY_WINDORHAIL_pol_key_RatingCoverageAKID.PolicyKey = pol_key
	AND LKP_DEDUCTIBLE_PROPERTY_WINDORHAIL_pol_key_RatingCoverageAKID.RatingCoverageAKId = RatingCoverageAKID

	LEFT JOIN LKP_DEDUCTIBLE_PROPERTY LKP_DEDUCTIBLE_PROPERTY_pol_key_RatingCoverageAKID
	ON LKP_DEDUCTIBLE_PROPERTY_pol_key_RatingCoverageAKID.PolicyKey = pol_key
	AND LKP_DEDUCTIBLE_PROPERTY_pol_key_RatingCoverageAKID.RatingCoverageAKId = RatingCoverageAKID

	LEFT JOIN LKP_SUPREFERENCEDATA LKP_SUPREFERENCEDATA_CoverageType
	ON LKP_SUPREFERENCEDATA_CoverageType.FromCode = CoverageType

	LEFT JOIN LKP_SUPREFERENCEDATA LKP_SUPREFERENCEDATA_RiskType
	ON LKP_SUPREFERENCEDATA_RiskType.FromCode = RiskType

	LEFT JOIN LKP_SUPCONSTRUCTIONCODE LKP_SUPCONSTRUCTIONCODE_LTRIM_RTRIM_in_ConstructionCode
	ON LKP_SUPCONSTRUCTIONCODE_LTRIM_RTRIM_in_ConstructionCode.ConstructionCode = LTRIM(RTRIM(in_ConstructionCode))

),
SQ_Premium AS (
	DECLARE @StartTime DATETIME
	DECLARE @EndTime DATETIME
	
	SET @StartTime = DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0) 
	SET @EndTime = DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0))
	
	
	--PMS
	SELECT distinct
	PMC.PremiumMasterCalculationID,
	CD.clndr_date,
	POL.pol_key,
	POL.prim_bus_class_code,
	RL.StateProvinceCode,
	RL.RatingCounty,
	RL.RatingCity,
	PT.PremiumTransactionBookedDate,
	PMC.PremiumMasterSubLine,
	SC.ClassCode,
	RL.RiskTerritory,
	POL.pol_eff_date,
	PMC.PremiumMasterPremium,
	PMC.PremiumMasterTypeBureauCode TypeBureauCode,
	SC.RiskUnitGroup,
	PT.SourceSystemID,
	PMC.PremiumMasterTransactionCode,
	PMC.PremiumMasterReasonAmendedCode,
	'N/A' AS RiskType,
	'N/A' AS CoverageType,
	PT.DeductibleAmount,
	case when PC.TypeBureauCode='CF' and PT.ConstructionCode='N/A' then bsc.BureauCode3 else PT.ConstructionCode end as ConstructionCode, 
	ISNULL(CASE
	WHEN PC.TypeBureauCode='CF' THEN bsc.BureauCode4 ELSE CDCP.IsoFireProtectionCode
	END, 'N/A') as IsoFireProtectionCode,
	PT.PackageModificationAdjustmentGroupCode,
	bsc.BureauCode1,
	bsc.BureauCode2,
	bsc.BureauCode4,
	POL.pol_exp_date,
	ASL.asl_num,
	PT.PremiumTransactionEffectiveDate,
	RL.Locationunitnumber,
	--prod-12020 adding this for LocationNumber
	SC.SubLocationUnitNumber as BuildingNumber,
	--prod-12020 adding this for LocationNumber
	'PMS' as DataType,
	PMC.PolicyKey,
	PMC.RatingCoverageAkid,
	'N/A' as RatingTerritoryCode
	from @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact PMF with (nolock)
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim ASL with (nolock)
	on PMF.AnnualStatementLineDimId=ASL.asl_dim_id
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim CD with (nolock) 
	ON PMF.PremiumMasterRunDateID = CD.clndr_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC with (nolock)
	on PMF.EDWPremiumMasterCalculationPKID=PMC.PremiumMasterCalculationId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT with (nolock)
	on PT.PremiumTransactionAKID=PMC.PremiumTransactionAKID
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialProperty CDCP with (nolock)
	on CDCP.PremiumTransactionID=PT.PremiumTransactionID AND CDCP.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC with (nolock)
	on SC.StatisticalCoverageAKID=PT.StatisticalCoverageAKID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC with (nolock)
	on PC.PolicyCoverageAKID=SC.PolicyCoverageAKID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL with (nolock)
	on PC.RiskLocationAKID=RL.RiskLocationAKID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL with (nolock)
	on POL.pol_ak_id=RL.PolicyAKID
	and POL.crrnt_snpsht_flag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG with (nolock)
	on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
	and ISG.CurrentSnapshotFlag=1 
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.BureauStatisticalCode bsc with (nolock)
	on bsc.PremiumTransactionAKID = PT.PremiumTransactionAKID 
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRC with (nolock)
	on IRC.InsuranceReferenceCoverageDimId=PMF.InsuranceReferenceCoverageDimId
	and NOT (IRC.InsuranceLineCode='CF' and IRC.CoverageCode='BOILER - BRK')
	where CD.clndr_date BETWEEN @StartTime AND @EndTime
	AND PMC.PremiumMasterTypeBureauCode IN ('CF','CR','BT','FT') 
	AND PT.SourceSystemID='PMS'
	AND ISG.InsuranceSegmentCode IN ('1','2')
	--AND RL.StateProvinceCode IN ('12','13','14','15','16','21','22','24','34','48')
	AND PMC.PremiumMasterPremium <>0
	AND PMC.PremiumMasterPremiumType='D'
	AND PMC.PremiumMasterReasonAmendedCode not in ('CWO', 'COL') 
	@{pipeline().parameters.WHERE_CLAUSE_1}
	
	UNION ALL
	--DCT
	SELECT distinct
	PMC.PremiumMasterCalculationID,
	CD.clndr_date,
	POL.pol_key,
	POL.prim_bus_class_code,
	RL.StateProvinceCode,
	RL.RatingCounty,
	RL.RatingCity,
	PT.PremiumTransactionBookedDate,
	PMC.PremiumMasterSubLine,
	RC.ClassCode,
	RL.RiskTerritory,
	POL.pol_eff_date,
	PMC.PremiumMasterPremium,
	PMC.PremiumMasterTypeBureauCode TypeBureauCode,
	'N/A' AS RiskUnitGroup,
	PT.SourceSystemID,
	PMC.PremiumMasterTransactionCode,
	PMC.PremiumMasterReasonAmendedCode,
	RC.RiskType,
	RC.CoverageType,
	NULL as DeductibleAmount,
	PT.ConstructionCode,
	ISNULL(CDCP.IsoFireProtectionCode,'N/A') as IsoFireProtectionCode,
	PT.PackageModificationAdjustmentGroupCode,
	'N/A' AS BureauCode1,
	'N/A' AS BureauCode2,
	'N/A' AS BureauCode4,
	POL.pol_exp_date,
	ASL.asl_num,
	PT.PremiumTransactionEffectiveDate,
	RL.Locationunitnumber,
	--prod-12020 adding this for LocationNumber
	RC.sublocationunitnumber as BuildingNumber,
	--prod-12020 adding this for LocationNumber
	'DCT' as DataType,
	PMC.PolicyKey,
	PMC.RatingCoverageAkid,
	ISNULL(PTRR.RatingTerritoryCode,'N/A') as RatingTerritoryCode
	from @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact PMF with (nolock)
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim ASL with (nolock)
	on PMF.AnnualStatementLineDimId=ASL.asl_dim_id
	INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim CD with (nolock) 
	ON PMF.PremiumMasterRunDateID = CD.clndr_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC with (nolock)
	on PMF.EDWPremiumMasterCalculationPKID=PMC.PremiumMasterCalculationId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT with (nolock)
	on PT.PremiumTransactionAKID=PMC.PremiumTransactionAKID
	and PMC.CurrentSnapshotFlag=1
	and PT.CurrentSnapshotFlag=1
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialProperty CDCP with (nolock)
	on CDCP.PremiumTransactionID=PT.PremiumTransactionID AND CDCP.CurrentSnapshotFlag=1
	join  @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC with (nolock)
	on PMC.RatingCoverageAKID=RC.RatingCoverageAKID
	and RC.EffectiveDate=PT.EffectiveDate 
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC with (nolock)
	on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
	and PC.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL with (nolock)
	on PC.RiskLocationAKID=RL.RiskLocationAKID
	and RL.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL with (nolock)
	on POL.pol_ak_id=PMC.PolicyAKID
	and POL.crrnt_snpsht_flag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG with (nolock)
	on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
	and ISG.CurrentSnapshotFlag=1 
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRC with (nolock)
	on IRC.InsuranceReferenceCoverageDimId=PMF.InsuranceReferenceCoverageDimId
	and NOT (IRC.InsuranceLineCode='CF' and IRC.CoverageCode='BOILER - BRK')
	left join PremiumTransactionRatingRisk PTRR with (nolock) 
	on PTRR.PremiumTransactionID=PT.PremiumTransactionID
	where 
	CD.clndr_date between @StartTime AND @EndTime 
	AND PC.Insuranceline in ('SBOPProperty','Crime','Property')
	AND PT.SourceSystemID='DCT'
	AND ISG.InsuranceSegmentCode IN ('1','2')
	--AND RL.StateProvinceCode IN ('12','13','14','15','16','21','22','24','34','48')
	AND PMC.PremiumMasterPremium <>0
	AND PMC.PremiumMasterPremiumType='D'
	AND RC.CoverageType NOT IN('DataCompromise','EquipmentBreakdown','RatingGroup','MineSubsidence')
	AND RC.SublineCode <> '920'
	--AND NOT EXISTS (SELECT 1 FROM WorkBlanketPremiumBreakOut WHERE WorkBlanketPremiumBreakOut.BlanketPremiumTransactionAKId=PT.PremiumTransactionAKId)
	and irc.CoverageDescriptionNOTIN( 'Blanket Building and Contents')
	AND PMC.PremiumMasterReasonAmendedCode not in ('CWO', 'CWB') 
	@{pipeline().parameters.WHERE_CLAUSE_1}
	
	UNION ALL
	--BreakOut
	SELECT distinct
	PMC.PremiumMasterCalculationID,
	CONVERT(DATE,PMC.PremiumMasterRunDate) as PremiumMasterRunDate,
	POL.pol_key,
	POL.prim_bus_class_code,
	RL.StateProvinceCode,
	RL.RatingCounty,
	RL.RatingCity,
	PT.PremiumTransactionBookedDate,
	PMC.PremiumMasterSubLine,
	RC.ClassCode,
	RL.RiskTerritory,
	POL.pol_eff_date,
	pmf.PremiumMasterPremium,
	--BreakOut.BreakOutPremium,
	PMC.PremiumMasterTypeBureauCode,
	'N/A' AS RiskUnitGroup,
	PT.SourceSystemID,
	PMC.PremiumMasterTransactionCode,
	PMC.PremiumMasterReasonAmendedCode,
	RC.RiskType,
	RC.CoverageType,
	NULL DeductibleAmount,
	PT.ConstructionCode,
	ISNULL(CDCP.IsoFireProtectionCode,'N/A') as IsoFireProtectionCode,
	PT.PackageModificationAdjustmentGroupCode,
	'N/A' AS BureauCode1,
	'N/A' AS BureauCode2,
	'N/A' AS BureauCode4,
	POL.pol_exp_date,
	ASL.asl_num,
	PT.PremiumTransactionEffectiveDate,
	RL.Locationunitnumber,
	--prod-12020 adding this for LocationNumber
	RC.sublocationunitnumber as BuildingNumber,
	--prod-12020 adding this for LocationNumber
	'Blanket' as DataType,
	PMC.PolicyKey,
	PMC.RatingCoverageAkid,
	ISNULL(PTRR.RatingTerritoryCode,'N/A') as RatingTerritoryCode
	from --WorkBlanketPremiumBreakOut BreakOut
	--join 
	PremiumTransaction PT with (nolock)
	--on PT.PremiumTransactionAKID=BreakOut.PremiumTransactionAKID
	join PremiumMasterCalculation PMC with (nolock)
	on PT.PremiumTransactionAKID=PMC.PremiumTransactionAKID 
	and PMC.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.dbo.premiummasterfact PMF 
	on PMF.EDWPremiumMasterCalculationPKID=PMC.PremiumMasterCalculationId
	--join AnnualStatementLine asl
	--on BreakOut.AnnualStatementLineId=asl.AnnualStatementLineId 
	join  @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.dbo.asl_dim asl
	on pmf.AnnualStatementLineDimID=asl.asl_dim_id                 
	left join CoverageDetailCommercialProperty CDCP with (nolock)
	on CDCP.PremiumTransactionID=PT.PremiumTransactionID AND CDCP.CurrentSnapshotFlag=1
	join RatingCoverage RC with (nolock)
	on PT.RatingCoverageAKID=RC.RatingCoverageAKID
	and RC.EffectiveDate=PT.EffectiveDate 
	join PolicyCoverage PC with (nolock)
	on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
	and PC.CurrentSnapshotFlag=1
	join RiskLocation RL with (nolock)
	on PC.RiskLocationAKID=RL.RiskLocationAKID
	and RL.CurrentSnapshotFlag=1
	join V2.policy POL with (nolock)
	on POL.pol_ak_id=PMC.PolicyAKID
	and POL.crrnt_snpsht_flag=1
	join InsuranceSegment ISG with (nolock)
	on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
	and ISG.CurrentSnapshotFlag=1 
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.dbo.InsuranceReferenceCoverageDim irc
	on pmf.InsuranceReferenceCoverageDimId=irc.InsuranceReferenceCoverageDimId
	left join PremiumTransactionRatingRisk PTRR with (nolock) 
	on PTRR.PremiumTransactionID=PT.PremiumTransactionID
	where CONVERT(DATE,PMC.PremiumMasterRunDate) between @StartTime AND @EndTime
	AND 
	PC.Insuranceline in ('SBOPProperty','Crime','Property')
	AND PT.SourceSystemID='DCT'
	AND ISG.InsuranceSegmentCode IN ('1','2')
	--AND RL.StateProvinceCode IN ('12','13','14','15','16','21','22','24','34','48')
	--AND BreakOut.BreakOutPremium <>0
	AND PMC.PremiumMasterPremiumType='D'
	AND PMC.PremiumMasterReasonAmendedCode NOT IN ('CWO', 'CWB')
	AND RC.CoverageType IN('RatingGroup')
	AND RC.SublineCode <> '920'
	and asl.asl_num_descriptIN( 'FIRE','ALLIED LINES','EARTHQUAKE')
	and irc.CoverageDescriptionIN( 'Blanket Building and Contents') 
	@{pipeline().parameters.WHERE_CLAUSE_1}
),
LKP_ISSWorkTable_Premium AS (
	SELECT
	EDWPremiumMasterCalculationPKId
	FROM (
		SELECT 
			EDWPremiumMasterCalculationPKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ISSCommercialPropertyExtract
		WHERE CONVERT (DATE,PremiumMasterRunDate)
		between 
		 DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0)  
		 AND
		 DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0))
		 and EDWPremiumMasterCalculationPKId<>-1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPremiumMasterCalculationPKId ORDER BY EDWPremiumMasterCalculationPKId) = 1
),
FIL_Exists_Premium AS (
	SELECT
	LKP_ISSWorkTable_Premium.EDWPremiumMasterCalculationPKId AS LKP_PremiumMasterCalculationID, 
	SQ_Premium.PremiumMasterCalculationID, 
	SQ_Premium.PremiumMasterRunDate, 
	SQ_Premium.pol_key, 
	SQ_Premium.prim_bus_class_code, 
	SQ_Premium.StateProvinceCode, 
	SQ_Premium.RatingCounty, 
	SQ_Premium.RatingCity, 
	SQ_Premium.PremiumTransactionBookedDate, 
	SQ_Premium.PremiumMasterSubLine, 
	SQ_Premium.ClassCode, 
	SQ_Premium.RiskTerritory, 
	SQ_Premium.pol_eff_date, 
	SQ_Premium.PremiumMasterPremium, 
	SQ_Premium.TypeBureauCode, 
	SQ_Premium.RiskUnitGroup, 
	SQ_Premium.SourceSystemID, 
	SQ_Premium.PremiumMasterTransactionCode, 
	SQ_Premium.PremiumMasterReasonAmendedCode, 
	SQ_Premium.RiskType, 
	SQ_Premium.CoverageType, 
	SQ_Premium.DeductibleAmount, 
	SQ_Premium.ConstructionCode, 
	SQ_Premium.IsoFireProtectionCode, 
	SQ_Premium.PackageModificationAdjustmentGroupCode, 
	SQ_Premium.BureauCode1, 
	SQ_Premium.BureauCode2, 
	SQ_Premium.BureauCode4, 
	SQ_Premium.pol_exp_date, 
	SQ_Premium.asl_num, 
	SQ_Premium.PremiumTransactionEffectiveDate, 
	SQ_Premium.DataType, 
	SQ_Premium.LocationNumber, 
	SQ_Premium.BuildingNumber, 
	SQ_Premium.PolicyKey, 
	SQ_Premium.RatingCoverageAKId, 
	SQ_Premium.RatingTerritoryCode
	FROM SQ_Premium
	LEFT JOIN LKP_ISSWorkTable_Premium
	ON LKP_ISSWorkTable_Premium.EDWPremiumMasterCalculationPKId = SQ_Premium.PremiumMasterCalculationID
	WHERE ISNULL(LKP_PremiumMasterCalculationID)  OR  IN(PremiumMasterReasonAmendedCode, 'COL' , 'CWO')
),
EXP_Cleansing_Premium AS (
	SELECT
	PremiumMasterCalculationID AS i_PremiumMasterCalculationID,
	PremiumMasterRunDate AS i_PremiumMasterRunDate,
	pol_key AS i_pol_key,
	prim_bus_class_code AS i_prim_bus_class_code,
	StateProvinceCode AS i_StateProvinceCode,
	RatingCounty AS i_RatingCounty,
	RatingCity AS i_RatingCity,
	PremiumTransactionBookedDate AS i_PremiumTransactionBookedDate,
	PremiumMasterSubLine AS i_PremiumMasterSubLine,
	ClassCode AS i_ClassCode,
	RiskTerritory AS i_RiskTerritory,
	pol_eff_date AS i_pol_eff_date,
	PremiumMasterPremium AS i_PremiumMasterPremium,
	TypeBureauCode AS i_TypeBureauCode,
	RiskUnitGroup AS i_RiskUnitGroup,
	SourceSystemID AS i_SourceSystemID,
	PremiumMasterTransactionCode AS i_PremiumMasterTransactionCode,
	PremiumMasterReasonAmendedCode AS i_PremiumMasterReasonAmendedCode,
	RiskType AS i_RiskType,
	CoverageType AS i_CoverageType,
	DeductibleAmount AS i_DeductibleAmount,
	ConstructionCode AS i_ConstructionCode,
	IsoFireProtectionCode AS i_IsoFireProtectionCode,
	PackageModificationAdjustmentGroupCode AS i_PackageModificationAdjustmentGroupCode,
	BureauCode1 AS i_BureauCode1,
	pol_exp_date AS i_pol_exp_date,
	asl_num AS i_asl_num,
	i_PremiumMasterCalculationID AS o_PremiumMasterCalculationID,
	i_PremiumMasterRunDate AS o_PremiumMasterRunDate,
	-- *INF*: RTRIM(LTRIM(i_pol_key))
	RTRIM(LTRIM(i_pol_key)) AS o_pol_key,
	-- *INF*: RTRIM(LTRIM(i_prim_bus_class_code))
	RTRIM(LTRIM(i_prim_bus_class_code)) AS o_prim_bus_class_code,
	-- *INF*: RTRIM(LTRIM(i_StateProvinceCode))
	RTRIM(LTRIM(i_StateProvinceCode)) AS o_StateProvinceCode,
	-- *INF*: RTRIM(LTRIM(i_RatingCounty))
	RTRIM(LTRIM(i_RatingCounty)) AS o_RatingCounty,
	-- *INF*: LTRIM(RTRIM(i_RatingCity))
	LTRIM(RTRIM(i_RatingCity)) AS o_RatingCity,
	i_PremiumTransactionBookedDate AS o_PremiumTransactionBookedDate,
	-- *INF*: RTRIM(LTRIM(i_PremiumMasterSubLine))
	RTRIM(LTRIM(i_PremiumMasterSubLine)) AS o_PremiumMasterSubLine,
	-- *INF*: RTRIM(LTRIM(i_ClassCode))
	RTRIM(LTRIM(i_ClassCode)) AS o_ClassCode,
	-- *INF*: RTRIM(LTRIM(i_RiskTerritory))
	RTRIM(LTRIM(i_RiskTerritory)) AS o_RiskTerritory,
	i_pol_eff_date AS o_pol_eff_date,
	-- *INF*: IIF( IN(i_PremiumMasterTransactionCode, '10','11','12','13','14','15','18','19','20','21','22','23','24','25','28','29','30','31','57','67') AND  NOT IN(i_PremiumMasterReasonAmendedCode, 'COL' , 'CWO'), i_PremiumMasterPremium, 0)
	IFF(
	    i_PremiumMasterTransactionCode IN ('10','11','12','13','14','15','18','19','20','21','22','23','24','25','28','29','30','31','57','67')
	    and NOT i_PremiumMasterReasonAmendedCode IN ('COL','CWO'),
	    i_PremiumMasterPremium,
	    0
	) AS o_PremiumMasterPremium,
	-- *INF*: RTRIM(LTRIM(i_TypeBureauCode))
	RTRIM(LTRIM(i_TypeBureauCode)) AS o_TypeBureauCode,
	-- *INF*: RTRIM(LTRIM(i_RiskUnitGroup))
	RTRIM(LTRIM(i_RiskUnitGroup)) AS o_RiskUnitGroup,
	-- *INF*: LTRIM(RTRIM(i_SourceSystemID))
	LTRIM(RTRIM(i_SourceSystemID)) AS o_SourceSystemID,
	-- *INF*: LTRIM(RTRIM(i_RiskType))
	LTRIM(RTRIM(i_RiskType)) AS o_RiskType,
	-- *INF*: LTRIM(RTRIM(i_CoverageType))
	LTRIM(RTRIM(i_CoverageType)) AS o_CoverageType,
	-- *INF*: LTRIM(RTRIM(i_DeductibleAmount))
	LTRIM(RTRIM(i_DeductibleAmount)) AS o_DeductibleAmount,
	-- *INF*: RTRIM(LTRIM(i_ConstructionCode))
	RTRIM(LTRIM(i_ConstructionCode)) AS o_ConstructionCode,
	-- *INF*: RTRIM(LTRIM(i_IsoFireProtectionCode))
	RTRIM(LTRIM(i_IsoFireProtectionCode)) AS o_IsoFireProtectionCode,
	-- *INF*: LTRIM(RTRIM(i_PackageModificationAdjustmentGroupCode))
	LTRIM(RTRIM(i_PackageModificationAdjustmentGroupCode)) AS o_PackageModificationAdjustmentGroupCode,
	-- *INF*: RTRIM(LTRIM(i_BureauCode1))
	RTRIM(LTRIM(i_BureauCode1)) AS o_BureauCode1,
	BureauCode2,
	BureauCode4,
	i_pol_exp_date AS o_pol_exp_date,
	i_asl_num AS o_AnnualStatementLineNumber,
	PremiumTransactionEffectiveDate,
	DataType,
	LocationNumber,
	BuildingNumber,
	PolicyKey,
	RatingCoverageAKId,
	RatingTerritoryCode
	FROM FIL_Exists_Premium
),
EXP_Logic_Premium AS (
	SELECT
	-1 AS LossMasterCalculationId,
	o_PremiumMasterCalculationID AS PremiumMasterCalculationID,
	o_PremiumMasterRunDate AS PremiumMasterRunDate,
	-- *INF*: TO_DATE('1800-01-01','YYYY/MM/DD')
	TO_TIMESTAMP('1800-01-01', 'YYYY/MM/DD') AS loss_master_run_date,
	o_pol_key AS pol_key,
	o_prim_bus_class_code AS prim_bus_class_code,
	o_StateProvinceCode AS StateProvinceCode,
	o_RatingCounty AS RatingCounty,
	o_RatingCity AS RatingCity,
	o_PremiumTransactionBookedDate AS PremiumTransactionBookedDate,
	o_PremiumMasterSubLine AS PremiumMasterSubLine,
	-- *INF*: DECODE(TRUE,PremiumMasterSubLine='025','027',PremiumMasterSubLine)
	DECODE(
	    TRUE,
	    PremiumMasterSubLine = '025', '027',
	    PremiumMasterSubLine
	) AS sub_line_code_out,
	o_ClassCode AS ClassCode,
	-- *INF*: IIF(ISNULL(ClassCode) OR LENGTH(ClassCode)=0,'N/A',ClassCode)
	IFF(ClassCode IS NULL OR LENGTH(ClassCode) = 0, 'N/A', ClassCode) AS PremiumMasterClassCode,
	'N/A' AS LossMasterClassCode,
	'N/A' AS Cause_of_Loss,
	o_RiskTerritory AS RiskTerritory,
	-- *INF*: RiskTerritory
	-- 
	-- --SUBSTR(RiskTerritory,2,2)
	RiskTerritory AS TerritoryCode,
	o_pol_eff_date AS pol_eff_date,
	'N/A' AS ClaimNum,
	'N/A' AS ClaimantNum,
	o_PremiumMasterPremium AS PremiumMasterPremium,
	-- *INF*: ROUND(PremiumMasterPremium,2)
	ROUND(PremiumMasterPremium, 2) AS PremiumMasterPremium_out,
	0.00 AS PaidLossAmt,
	0.00 AS OutstandingAmt,
	o_TypeBureauCode AS TypeBureauCode,
	o_RiskUnitGroup AS RiskUnitGroup,
	o_SourceSystemID AS SourceSystemID,
	o_RiskType AS RiskType,
	o_CoverageType AS CoverageType,
	o_DeductibleAmount AS DeductibleAmount,
	-- *INF*: IIF(RatingCoverageAKId<>-1 and IN(PremiumMasterSubLine,'020','027','120'),:LKP.LKP_DEDUCTIBLE_PROPERTY_WINDORHAIL(PolicyKey,RatingCoverageAKId),DeductibleAmount)
	IFF(
	    RatingCoverageAKId <> - 1 and PremiumMasterSubLine IN ('020','027','120'),
	    LKP_DEDUCTIBLE_PROPERTY_WINDORHAIL_PolicyKey_RatingCoverageAKId.CoverageDeductibleValue,
	    DeductibleAmount
	) AS v_Deductible_Property_WindORHail,
	-- *INF*: IIF(RatingCoverageAKId<>-1 and IN(PremiumMasterSubLine,'020','027','120','010','015','016','017','018','029','035','045','055','110'),:LKP.LKP_DEDUCTIBLE_PROPERTY(PolicyKey,RatingCoverageAKId),DeductibleAmount)
	IFF(
	    RatingCoverageAKId <> - 1
	    and PremiumMasterSubLine IN ('020','027','120','010','015','016','017','018','029','035','045','055','110'),
	    LKP_DEDUCTIBLE_PROPERTY_PolicyKey_RatingCoverageAKId.CoverageDeductibleValue,
	    DeductibleAmount
	) AS v_Deductible_Property,
	-- *INF*: DECODE(TRUE,
	-- IN(PremiumMasterSubLine,'070','090','170','190','930','931'),NULL,
	-- ISNULL(v_Deductible_Property_WindORHail),v_Deductible_Property,
	-- v_Deductible_Property_WindORHail)
	DECODE(
	    TRUE,
	    PremiumMasterSubLine IN ('070','090','170','190','930','931'), NULL,
	    v_Deductible_Property_WindORHail IS NULL, v_Deductible_Property,
	    v_Deductible_Property_WindORHail
	) AS v_Deductible_DCT,
	-- *INF*: DECODE(TRUE,
	-- RatingCoverageAKId=-1 and (LENGTH(DeductibleAmount)=0 OR ISNULL(DeductibleAmount) OR DeductibleAmount='FullCoverage'),'0',
	-- RatingCoverageAKId=-1,DeductibleAmount,
	-- v_Deductible_DCT)
	-- 
	-- --IIF(LENGTH(DeductibleAmount)=0 OR ISNULL(DeductibleAmount) OR DeductibleAmount='FullCoverage','0',DeductibleAmount)
	DECODE(
	    TRUE,
	    RatingCoverageAKId = - 1 and (LENGTH(DeductibleAmount) = 0 OR DeductibleAmount IS NULL OR DeductibleAmount = 'FullCoverage'), '0',
	    RatingCoverageAKId = - 1, DeductibleAmount,
	    v_Deductible_DCT
	) AS DeductibleAmount_out,
	o_BureauCode1 AS BureauCode1,
	BureauCode2,
	BureauCode4,
	-- *INF*: DECODE(TRUE,
	-- SourceSystemID='PMS',IIF(ISNULL(BureauCode1) OR LENGTH(BureauCode1)=0 OR(TypeBureauCode  != 'CR' and TypeBureauCode  !='BT' and TypeBureauCode != 'FT' and TypeBureauCode != 'CF'),'N/A',BureauCode1) ,
	-- ((INSTR(RiskType,'BIEE')>0  OR  INSTR(RiskType,'TIME')>0) AND  NOT IN(CoverageType, 'TerrorismRisk', 'TerrorismFireOnly')) OR RiskType='N/A',
	-- :LKP.LKP_SUPREFERENCEDATA(CoverageType),
	-- :LKP.LKP_SUPREFERENCEDATA(RiskType)
	-- )
	DECODE(
	    TRUE,
	    SourceSystemID = 'PMS', IFF(
	        BureauCode1 IS NULL
	        or LENGTH(BureauCode1) = 0
	        or (TypeBureauCode != 'CR'
	        and TypeBureauCode != 'BT'
	        and TypeBureauCode != 'FT'
	        and TypeBureauCode != 'CF'),
	        'N/A',
	        BureauCode1
	    ),
	    ((REGEXP_INSTR(RiskType, 'BIEE') > 0 OR REGEXP_INSTR(RiskType, 'TIME') > 0) AND NOT CoverageType IN ('TerrorismRisk','TerrorismFireOnly')) OR RiskType = 'N/A', LKP_SUPREFERENCEDATA_CoverageType.ToCode,
	    LKP_SUPREFERENCEDATA_RiskType.ToCode
	) AS v_CoverageCode,
	DataType,
	-- *INF*: DECODE(TRUE,
	-- DataType='Blanket','03',
	-- ISNULL(v_CoverageCode),'N/A',
	-- v_CoverageCode<>'N/A',LPAD(v_CoverageCode,2,'0'),
	-- 'N/A')
	DECODE(
	    TRUE,
	    DataType = 'Blanket', '03',
	    v_CoverageCode IS NULL, 'N/A',
	    v_CoverageCode <> 'N/A', LPAD(v_CoverageCode, 2, '0'),
	    'N/A'
	) AS o_CoverageCode,
	o_ConstructionCode AS in_ConstructionCode,
	-- *INF*: :LKP.LKP_SUPCONSTRUCTIONCODE(LTRIM(RTRIM(in_ConstructionCode)))
	LKP_SUPCONSTRUCTIONCODE_LTRIM_RTRIM_in_ConstructionCode.StandardConstructionCodeDescription AS v_ConstructionCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_ConstructionCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(v_ConstructionCode) AS ConstructionCode_out,
	o_IsoFireProtectionCode AS IsoFireProtectionCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(IsoFireProtectionCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(IsoFireProtectionCode) AS IsoFireProtectionCode_out,
	o_PackageModificationAdjustmentGroupCode AS i_PackageModificationAdjustmentGroupCode,
	o_pol_exp_date AS pol_exp_date,
	o_AnnualStatementLineNumber AS AnnualStatementLineNumber,
	PremiumTransactionEffectiveDate,
	LocationNumber,
	BuildingNumber,
	PolicyKey,
	RatingCoverageAKId,
	RatingTerritoryCode
	FROM EXP_Cleansing_Premium
	LEFT JOIN LKP_DEDUCTIBLE_PROPERTY_WINDORHAIL LKP_DEDUCTIBLE_PROPERTY_WINDORHAIL_PolicyKey_RatingCoverageAKId
	ON LKP_DEDUCTIBLE_PROPERTY_WINDORHAIL_PolicyKey_RatingCoverageAKId.PolicyKey = PolicyKey
	AND LKP_DEDUCTIBLE_PROPERTY_WINDORHAIL_PolicyKey_RatingCoverageAKId.RatingCoverageAKId = RatingCoverageAKId

	LEFT JOIN LKP_DEDUCTIBLE_PROPERTY LKP_DEDUCTIBLE_PROPERTY_PolicyKey_RatingCoverageAKId
	ON LKP_DEDUCTIBLE_PROPERTY_PolicyKey_RatingCoverageAKId.PolicyKey = PolicyKey
	AND LKP_DEDUCTIBLE_PROPERTY_PolicyKey_RatingCoverageAKId.RatingCoverageAKId = RatingCoverageAKId

	LEFT JOIN LKP_SUPREFERENCEDATA LKP_SUPREFERENCEDATA_CoverageType
	ON LKP_SUPREFERENCEDATA_CoverageType.FromCode = CoverageType

	LEFT JOIN LKP_SUPREFERENCEDATA LKP_SUPREFERENCEDATA_RiskType
	ON LKP_SUPREFERENCEDATA_RiskType.FromCode = RiskType

	LEFT JOIN LKP_SUPCONSTRUCTIONCODE LKP_SUPCONSTRUCTIONCODE_LTRIM_RTRIM_in_ConstructionCode
	ON LKP_SUPCONSTRUCTIONCODE_LTRIM_RTRIM_in_ConstructionCode.ConstructionCode = LTRIM(RTRIM(in_ConstructionCode))

),
Union AS (
	SELECT LossMasterCalculationId, PremiumMasterCalculationID, PremiumMasterRunDate, loss_master_run_date, pol_key, prim_bus_class_code, StateProvinceCode, sub_line_code_out AS sub_line_code, PremiumMasterClassCode, LossMasterClassCode, Cause_of_Loss, TerritoryCode, pol_eff_date, ClaimNum, ClaimantNum, PremiumMasterPremium_out AS PremiumMasterPremium, PaidLossAmt, OutstandingAmt, TypeBureauCode, RiskUnitGroup, SourceSystemID, RiskType, DeductibleAmount_out AS DeductibleAmount, o_CoverageCode AS CoverageCode, ConstructionCode_out AS ConstructionCode, IsoFireProtectionCode_out AS IsoFireProtectionCode, i_PackageModificationAdjustmentGroupCode AS PackageModificationAdjustmentGroupCode, pol_exp_date AS PolicyExpirationDate, AnnualStatementLineNumber, BureauCode1, BureauCode2, BureauCode4, RatingCounty, PremiumTransactionEffectiveDate, RatingCity, LocationNumber AS o_LocationNumber, BuildingNumber AS o_BuildingNumber, RatingTerritoryCode
	FROM EXP_Logic_Premium
	UNION
	SELECT loss_master_calculation_id AS LossMasterCalculationId, PremiumMasterCalculationID, PremiumMasterRunDate, loss_master_run_date, pol_key, prim_bus_class_code, StateProvinceCode, sub_line_code_out AS sub_line_code, PremiumMasterClassCode_out AS PremiumMasterClassCode, LossMasterClassCode_out AS LossMasterClassCode, cause_of_loss_out AS Cause_of_Loss, TerritoryCode_out AS TerritoryCode, pol_eff_date, ClaimNum, claimant_num AS ClaimantNum, PremiumMasterPremium, PaidLossAmount AS PaidLossAmt, OutstandingLossAmount AS OutstandingAmt, TypeBureauCode, RiskUnitGroup, PolicySourceID AS SourceSystemID, RiskType, DeductibleAmount_out AS DeductibleAmount, o_CoverageCode AS CoverageCode, ConstructionCode, IsoFireProtectionCode, PackageModificationAdjustmentGroupCode, pol_exp_date AS PolicyExpirationDate, CumulativeInceptiontoDatePaidLoss AS InceptionToDatePaidLossAmount, claim_coverage_detail_ak_id AS ClaimCoverageID, AnnualStatementLineNumber, BureauCode1, BureauCode2, BureauCode4, RatingCounty, RatingCity, o_LocationNumber, o_BuildingNumber, RatingTerritoryCode
	FROM EXP_Logic_Loss
),
EXP_ConstCode_IsoPC_Rules AS (
	SELECT
	sub_line_code AS i_sub_line_code,
	ConstructionCode AS i_ConstructionCode,
	IsoFireProtectionCode AS i_IsoFireProtectionCode,
	-- *INF*: DECODE
	-- (TRUE,
	-- in(i_ConstructionCode,'N/A',null),'00',
	-- i_ConstructionCode='B','2',
	-- i_ConstructionCode
	-- )
	DECODE(
	    TRUE,
	    i_ConstructionCode IN ('N/A',null), '00',
	    i_ConstructionCode = 'B', '2',
	    i_ConstructionCode
	) AS v_ConstructionCode,
	-- *INF*: DECODE(TRUE,
	-- i_IsoFireProtectionCode='N/A' and in(i_sub_line_code,'010','015','016','017','018'),'10',
	-- i_IsoFireProtectionCode='N/A', '00',
	-- i_IsoFireProtectionCode='1', '01',
	-- in (i_IsoFireProtectionCode,'2','20'),'02',
	-- in (i_IsoFireProtectionCode,'3','13','30'),'03',
	-- i_IsoFireProtectionCode='4','04',
	-- i_IsoFireProtectionCode='5','05',
	-- i_IsoFireProtectionCode='6','06',
	-- i_IsoFireProtectionCode='7','07',
	-- i_IsoFireProtectionCode='8','08',
	-- in(i_IsoFireProtectionCode,'9','92','97'),'09',
	-- i_IsoFireProtectionCode='12','10',
	-- in(i_IsoFireProtectionCode,'OR','O4'),'04',
	-- i_IsoFireProtectionCode='8B','19',
	-- i_IsoFireProtectionCode='96','06',
	-- INSTR(i_IsoFireProtectionCode,'X'),'09',
	-- INSTR(i_IsoFireProtectionCode,'Y'),'8B',
	-- i_IsoFireProtectionCode
	-- )
	DECODE(
	    TRUE,
	    i_IsoFireProtectionCode = 'N/A' and i_sub_line_code IN ('010','015','016','017','018'), '10',
	    i_IsoFireProtectionCode = 'N/A', '00',
	    i_IsoFireProtectionCode = '1', '01',
	    i_IsoFireProtectionCode IN ('2','20'), '02',
	    i_IsoFireProtectionCode IN ('3','13','30'), '03',
	    i_IsoFireProtectionCode = '4', '04',
	    i_IsoFireProtectionCode = '5', '05',
	    i_IsoFireProtectionCode = '6', '06',
	    i_IsoFireProtectionCode = '7', '07',
	    i_IsoFireProtectionCode = '8', '08',
	    i_IsoFireProtectionCode IN ('9','92','97'), '09',
	    i_IsoFireProtectionCode = '12', '10',
	    i_IsoFireProtectionCode IN ('OR','O4'), '04',
	    i_IsoFireProtectionCode = '8B', '19',
	    i_IsoFireProtectionCode = '96', '06',
	    REGEXP_INSTR(i_IsoFireProtectionCode, 'X'), '09',
	    REGEXP_INSTR(i_IsoFireProtectionCode, 'Y'), '8B',
	    i_IsoFireProtectionCode
	) AS v_IsoFireProtectionCode,
	-- *INF*: DECODE(TRUE,
	-- in(i_sub_line_code,'015','016','017','018'),v_IsoFireProtectionCode,'00')
	DECODE(
	    TRUE,
	    i_sub_line_code IN ('015','016','017','018'), v_IsoFireProtectionCode,
	    '00'
	) AS v_IsoFireProtectionCode_FIRE,
	v_ConstructionCode AS o_ConstructionCode,
	v_IsoFireProtectionCode_FIRE AS o_IsoFireProtectionCode
	FROM Union
),
LKP_RiskTypeWithMaxPremium AS (
	SELECT
	RiskType,
	pol_key
	FROM (
		select pol.pol_key AS pol_key,
		rc.RiskType as RiskType
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction pt 
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage rc
		on pt.RatingCoverageAKId=rc.RatingCoverageAKID
		and pt.EffectiveDate=rc.EffectiveDate
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage pc
		on rc.PolicyCoverageAKID=pc.PolicyCoverageAKID
		and pc.CurrentSnapshotFlag=1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy pol
		on pc.PolicyAKID=pol.pol_ak_id
		and pol.crrnt_snpsht_flag=1
		and pol.source_sys_id='DCT'
		where rc.RiskType<>'N/A'
		and pc.TypeBureauCode='Crime'
		order by pol.pol_key,pt.PremiumTransactionAmount desc
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY RiskType) = 1
),
EXP_Values AS (
	SELECT
	Union.LossMasterCalculationId AS i_LossMasterCalculationId,
	Union.PremiumMasterCalculationID AS i_PremiumMasterCalculationID,
	Union.PremiumMasterRunDate AS i_PremiumMasterRunDate,
	Union.loss_master_run_date AS i_LossMasterRunDate,
	Union.pol_key AS i_pol_key,
	Union.prim_bus_class_code AS i_prim_bus_class_code,
	Union.StateProvinceCode AS i_StateProvinceCode,
	Union.RatingCounty AS i_RatingCounty,
	Union.RatingCity AS i_RatingCity,
	Union.sub_line_code AS i_sub_line_code,
	Union.PremiumMasterClassCode AS i_PremiumMasterClassCode,
	Union.LossMasterClassCode AS i_LossMasterClassCode,
	Union.Cause_of_Loss AS i_Cause_of_Loss,
	Union.TerritoryCode AS i_TerritoryCode,
	Union.pol_eff_date AS i_pol_eff_date,
	Union.ClaimNum AS i_ClaimNum,
	Union.ClaimantNum AS i_ClaimantNum,
	Union.PremiumMasterPremium AS i_PremiumMasterPremium,
	Union.PaidLossAmt AS i_PaidLossAmt,
	Union.OutstandingAmt AS i_OutstandingAmt,
	Union.TypeBureauCode AS i_TypeBureauCode,
	Union.SourceSystemID AS i_SourceSystemID,
	Union.RiskType AS i_RiskType,
	Union.DeductibleAmount AS i_DeductibleAmount,
	Union.CoverageCode AS i_CoverageCode,
	EXP_ConstCode_IsoPC_Rules.o_ConstructionCode AS i_ConstructionCode,
	EXP_ConstCode_IsoPC_Rules.o_IsoFireProtectionCode AS i_IsoFireProtectionCode,
	Union.PackageModificationAdjustmentGroupCode AS i_PackageModificationAdjustmentGroupCode,
	Union.PolicyExpirationDate AS i_PolicyExpirationDate,
	Union.InceptionToDatePaidLossAmount AS i_InceptionToDatePaidLossAmount,
	Union.ClaimCoverageID AS i_ClaimCoverageID,
	Union.AnnualStatementLineNumber AS i_AnnualStatementLineNumber,
	Union.BureauCode1 AS i_BureauCode1,
	Union.BureauCode2 AS i_BureauCode2,
	Union.BureauCode4 AS i_BureauCode4,
	Union.PremiumTransactionEffectiveDate AS i_PremiumTransactionEffectiveDate,
	Union.RatingTerritoryCode AS i_RatingTerritoryCode,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	SYSDATE AS o_CreatedDate,
	i_PremiumMasterCalculationID AS o_PremiumMasterCalculationID,
	i_LossMasterCalculationId AS o_LossMasterCalculationId,
	-- *INF*: DECODE(TRUE, 
	-- i_AnnualStatementLineNumber = '12', '930',
	-- i_PremiumMasterCalculationID  !=  -1 AND NOT IN(i_sub_line_code, '090', '070', '930') AND LocationNumber = '0001' AND BuildingNumber = '000' AND i_CoverageCode = '03', '035', 
	-- i_sub_line_code)
	DECODE(
	    TRUE,
	    i_AnnualStatementLineNumber = '12', '930',
	    i_PremiumMasterCalculationID != - 1 AND NOT i_sub_line_code IN ('090','070','930') AND LocationNumber = '0001' AND BuildingNumber = '000' AND i_CoverageCode = '03', '035',
	    i_sub_line_code
	) AS v_SublineCode,
	v_SublineCode AS o_SublineCode,
	i_TypeBureauCode AS o_TypeBureauCode,
	-- *INF*: IIF(IN(i_TypeBureauCode,'CF','Property','SBOPProperty')=1,'08','03')
	-- 
	-- 
	-- 
	-- --IIF(i_TypeBureauCode='CF','08','03')
	IFF(i_TypeBureauCode IN ('CF','Property','SBOPProperty') = 1, '08', '03') AS o_BureauLineOfInsurance,
	'0731' AS o_BureauCompanyNumber,
	i_StateProvinceCode AS o_StateProvinceCode,
	i_PremiumMasterRunDate AS o_PremiumMasterRunDate,
	i_LossMasterRunDate AS o_LossMasterRunDate,
	i_pol_key AS o_pol_key,
	-- *INF*: DECODE(TRUE,
	-- i_PremiumMasterCalculationID  != -1 AND IN(v_SublineCode, '090', '070', '930'), '0000',
	-- i_PremiumMasterClassCode)
	DECODE(
	    TRUE,
	    i_PremiumMasterCalculationID != - 1 AND v_SublineCode IN ('090','070','930'), '0000',
	    i_PremiumMasterClassCode
	) AS o_PremiumMasterClassCode,
	-- *INF*: DECODE(TRUE, 
	-- i_LossMasterCalculationId  != -1 AND IN(v_SublineCode, '090', '070', '930'), '0000', 
	-- i_LossMasterClassCode)
	DECODE(
	    TRUE,
	    i_LossMasterCalculationId != - 1 AND v_SublineCode IN ('090','070','930'), '0000',
	    i_LossMasterClassCode
	) AS o_LossMasterClassCode,
	i_ClaimNum AS o_ClaimNum,
	i_ClaimantNum AS o_ClaimantNum,
	-- *INF*: DECODE(True,
	-- i_StateProvinceCode='12'  AND IN( i_RatingCounty,'COOK',0)  AND IN(i_RatingCity,'CHICAGO',0),'01',
	-- i_StateProvinceCode='12'  AND IN(i_RatingCounty,'COOK',0)  AND NOT IN(i_RatingCity,'CHICAGO',0),'02',
	-- i_StateProvinceCode='12'  AND  IN(i_RatingCity,'CHICAGO',0),'01',
	-- i_StateProvinceCode='12','03',
	-- -- changed for EDWP-4697
	-- --i_StateProvinceCode='13' and IN( i_TerritoryCode,'099'),'94',
	-- --i_StateProvinceCode='13' and IN( i_TerritoryCode,'018'),'93',
	-- --i_StateProvinceCode='15' and IN( i_TerritoryCode,'091'),'11',
	-- --i_StateProvinceCode='16' and IN( i_TerritoryCode,'022'),'32',
	-- --i_StateProvinceCode='21''99',
	-- --i_StateProvinceCode='21' and IN( i_TerritoryCode,'094'),'17' ,
	--  --i_StateProvinceCode='22' and IN( i_TerritoryCode,'091'),'15',
	-- --i_StateProvinceCode='22' and IN( i_TerritoryCode,'093'),'17',
	-- --i_StateProvinceCode='22' and IN( i_TerritoryCode,'042'),'18',
	-- --i_StateProvinceCode='24' and IN( i_TerritoryCode,'07'),'99',
	-- --i_StateProvinceCode='24' and IN( i_TerritoryCode,'099'),'99',
	-- --i_StateProvinceCode='34' and IN( i_TerritoryCode,'067'),'99',
	-- --i_StateProvinceCode='34' and IN( i_TerritoryCode,'099'),'99',
	-- --i_StateProvinceCode='34' and IN( i_TerritoryCode,'011'),'63',
	-- --i_StateProvinceCode='34' and IN( i_TerritoryCode,'048'),'64',
	-- --i_StateProvinceCode='34' and IN( i_TerritoryCode,'069'),'65',
	-- --i_StateProvinceCode='34' and IN( i_TerritoryCode,'060'),'67',
	-- --i_StateProvinceCode='48' and IN( i_TerritoryCode,'025'),'23',  
	-- --i_TerritoryCode='999','99',
	-- --i_StateProvinceCode='14','99',
	-- --i_StateProvinceCode='21','04',
	-- --Luna add default value for PMS 5/15 
	-- '99')
	-- 
	-- 
	-- 
	DECODE(
	    True,
	    i_StateProvinceCode = '12' AND i_RatingCounty IN ('COOK',0) AND i_RatingCity IN ('CHICAGO',0), '01',
	    i_StateProvinceCode = '12' AND i_RatingCounty IN ('COOK',0) AND NOT i_RatingCity IN ('CHICAGO',0), '02',
	    i_StateProvinceCode = '12' AND i_RatingCity IN ('CHICAGO',0), '01',
	    i_StateProvinceCode = '12', '03',
	    '99'
	) AS v_Risk_Territory_PMS,
	-- *INF*: DECODE(True,
	-- i_AnnualStatementLineNumber='12','N/A', -- We need to hardcode Earthquake to N/A
	-- i_RatingTerritoryCode !='N/A',i_RatingTerritoryCode,
	-- --i_StateProvinceCode='12'  AND IN( i_RatingCounty,'COOK',0)  AND IN(i_RatingCity,'CHICAGO',0),'01',
	-- --i_StateProvinceCode='12'  AND IN(i_RatingCounty,'COOK',0)  AND NOT IN(i_RatingCity,'CHICAGO',0),'02',
	-- --i_StateProvinceCode='12'  AND  IN(i_RatingCity,'CHICAGO',0),'01',
	-- --i_StateProvinceCode='12','03'
	-- --Change made for EDWP-4697
	-- --i_StateProvinceCode='15' and IN(i_RatingCounty,'Wyandotte') ,'11',
	-- --i_StateProvinceCode='16' and IN(i_RatingCounty,'Jefferson') ,'32',
	-- --i_StateProvinceCode='21' and IN(i_RatingCounty,'Detroit'), '03',
	-- --i_StateProvinceCode='21' and IN(i_RatingCounty,'Macomb','Oakland'), '04',
	-- --i_StateProvinceCode='21' and IN(i_RatingCounty,'Wayne') ,'17',
	-- --i_StateProvinceCode='22' and IN(i_RatingCounty,'Anoka') ,'15',
	-- --i_StateProvinceCode='22' and IN(i_RatingCounty,'Dakota') ,'16',
	-- --i_StateProvinceCode='22' and IN(i_RatingCounty,'Hennepin'),'17',
	-- --i_StateProvinceCode='22' and IN(i_RatingCounty,'Ramsey') ,'18',
	-- --i_StateProvinceCode='22' and IN(i_RatingCounty,'Washington'),'16',
	-- --i_StateProvinceCode='24' and IN(i_RatingCounty,'Jackson') ,'07',
	-- --i_StateProvinceCode='24' and IN(i_RatingCounty,'St. Louis City','St. Louis'),'14',
	-- --i_StateProvinceCode='26' and IN(i_RatingCounty,'Douglas') ,'13',
	-- --i_StateProvinceCode='34' and IN(i_RatingCounty,'Cuyahoga') ,'61',
	-- --i_StateProvinceCode='34' and IN(i_RatingCounty,'Franklin') ,'62',
	-- --i_StateProvinceCode='34' and IN(i_RatingCounty,'Hamilton') ,'63',
	-- --i_StateProvinceCode='34' and IN(i_RatingCounty,'Lucas'),'64',
	-- --i_StateProvinceCode='34' and IN(i_RatingCounty,'Mahoning') ,'65',
	-- --i_StateProvinceCode='34' and IN(i_RatingCounty,'Summit') ,'67',
	-- --i_StateProvinceCode='48' and IN(i_RatingCounty,'Milwaukee') ,'23'
	-- '99')
	-- 
	DECODE(
	    True,
	    i_AnnualStatementLineNumber = '12', 'N/A',
	    i_RatingTerritoryCode != 'N/A', i_RatingTerritoryCode,
	    '99'
	) AS v_Risk_Territory_DCT,
	-- *INF*: IIF(i_SourceSystemID='PMS',v_Risk_Territory_PMS,v_Risk_Territory_DCT)
	IFF(i_SourceSystemID = 'PMS', v_Risk_Territory_PMS, v_Risk_Territory_DCT) AS o_RiskTerritoryCode,
	i_pol_eff_date AS o_PolicyEffectiveDate,
	-- *INF*: DECODE(TRUE,IN(i_Cause_of_Loss,'11','21','31','41','51','61','71','81','91'),'01',
	-- IN(i_Cause_of_Loss,'12','22','32','42','52','62','72','82','92','97'),'02',
	-- IN(i_Cause_of_Loss,'05','15','25','35','45','55','65','75','85','95'),'03',
	-- IN(i_Cause_of_Loss,'14','24','34','44','54','64','74','84','94'),'04',
	-- IN(i_Cause_of_Loss,'08','18','28','38','48','58','68','88','98'),'05',
	-- IN(i_Cause_of_Loss,'16','26','36','46','56','66','76','86','96'),'06',
	-- IN(i_Cause_of_Loss,'17','27','37','47','57','67','87'),'07',
	-- IN(i_Cause_of_Loss,'03','13','23','33','43','53','63','73','83','93'),'08',
	-- IN(i_Cause_of_Loss,'19','29','39','49','59','69','77','79','89','99'),'09',i_Cause_of_Loss)
	DECODE(
	    TRUE,
	    i_Cause_of_Loss IN ('11','21','31','41','51','61','71','81','91'), '01',
	    i_Cause_of_Loss IN ('12','22','32','42','52','62','72','82','92','97'), '02',
	    i_Cause_of_Loss IN ('05','15','25','35','45','55','65','75','85','95'), '03',
	    i_Cause_of_Loss IN ('14','24','34','44','54','64','74','84','94'), '04',
	    i_Cause_of_Loss IN ('08','18','28','38','48','58','68','88','98'), '05',
	    i_Cause_of_Loss IN ('16','26','36','46','56','66','76','86','96'), '06',
	    i_Cause_of_Loss IN ('17','27','37','47','57','67','87'), '07',
	    i_Cause_of_Loss IN ('03','13','23','33','43','53','63','73','83','93'), '08',
	    i_Cause_of_Loss IN ('19','29','39','49','59','69','77','79','89','99'), '09',
	    i_Cause_of_Loss
	) AS o_CauseOfLoss,
	-- *INF*: DECODE(TRUE,
	-- IN(v_SublineCode, '090', '070', '930'), '0000000', 
	-- ISNULL(i_DeductibleAmount),'N/A',
	-- LPAD(i_DeductibleAmount, 7, '0')
	-- )
	-- --IIF(ISNULL(i_DeductibleAmount),'0000000',LPAD(i_DeductibleAmount, 7, '0'))
	DECODE(
	    TRUE,
	    v_SublineCode IN ('090','070','930'), '0000000',
	    i_DeductibleAmount IS NULL, 'N/A',
	    LPAD(i_DeductibleAmount, 7, '0')
	) AS o_DeductibleAmount,
	-- *INF*: DECODE(TRUE, 
	-- IN(v_SublineCode, '090', '070', '930'), 'N/A', 
	-- i_CoverageCode)
	DECODE(
	    TRUE,
	    v_SublineCode IN ('090','070','930'), 'N/A',
	    i_CoverageCode
	) AS o_CoverageCode,
	-- *INF*: DECODE(TRUE, 
	-- IN(v_SublineCode, '090', '070', '930'), '00',
	-- i_ConstructionCode)
	DECODE(
	    TRUE,
	    v_SublineCode IN ('090','070','930'), '00',
	    i_ConstructionCode
	) AS o_ConstructionCode,
	i_IsoFireProtectionCode AS o_ISOFireProtectionCode,
	LKP_RiskTypeWithMaxPremium.RiskType AS i_RiskTypeCrimeWithMaxPremiumDCT,
	-- *INF*: IIF(i_RiskType='N/A',i_RiskTypeCrimeWithMaxPremiumDCT,i_RiskType)
	IFF(i_RiskType = 'N/A', i_RiskTypeCrimeWithMaxPremiumDCT, i_RiskType) AS v_RiskTypeDCT,
	-- *INF*: DECODE(TRUE,
	-- --i_TypeBureauCode='CF' , 
	-- --DECODE(TRUE,
	-- --i_RiskType='BLDG','01',
	-- --i_RiskType='PP','02',
	-- --'N/A'
	-- --),
	-- 
	-- i_TypeBureauCode= 'CR' ,
	-- DECODE(TRUE,
	-- v_RiskTypeDCT='ClientsProperty','200',
	-- v_RiskTypeDCT='ClientsProperty_ETF','400',
	-- v_RiskTypeDCT='ComputerFraud','200',
	-- v_RiskTypeDCT='ComputerFraud_G','300',
	-- v_RiskTypeDCT='EmployeeTheft','200',
	-- v_RiskTypeDCT='EmployeeTheft_ETF','400',
	-- v_RiskTypeDCT='EmployeeTheftNameOrPosition','200',
	-- v_RiskTypeDCT='EmployeeTheftNameOrPosition_ETF','400',
	-- v_RiskTypeDCT='EmployeeTheftNameOrPosition_G','300',
	-- v_RiskTypeDCT='EmployeeTheftNameOrPosition_GETF','400',
	-- v_RiskTypeDCT='EmployeeTheftPerEmployee','300',
	-- v_RiskTypeDCT='EmployeeTheftPerEmployee_GETF','400',
	-- v_RiskTypeDCT='EmployeeTheftPerLoss','300',
	-- v_RiskTypeDCT='EmployeeTheftPerLoss_GETF','400',
	-- v_RiskTypeDCT='ForgeryAndAlteration','200',
	-- v_RiskTypeDCT='ForgeryAndAlteration_ETF','400',
	-- v_RiskTypeDCT='ForgeryAndAlteration_G','300',
	-- v_RiskTypeDCT='ForgeryAndAlteration_GETF','400',
	-- v_RiskTypeDCT='FundsTransfer','200',
	-- v_RiskTypeDCT='FundsTransfer_G','300',
	-- v_RiskTypeDCT='GuestPropertyInsidePremises','200',
	-- v_RiskTypeDCT='GuestPropertySafeDeposit','200',
	-- v_RiskTypeDCT='InsideRobbery','200',
	-- v_RiskTypeDCT='InsideRobbery_G','300',
	-- v_RiskTypeDCT='InsideRobberyOther','200',
	-- v_RiskTypeDCT='InsideRobberyOther_G','300',
	-- v_RiskTypeDCT='InsideRobberySecurities','200',
	-- v_RiskTypeDCT='InsideRobberySecurities_G','300',
	-- v_RiskTypeDCT='InsideTheftMoney','200',
	-- v_RiskTypeDCT='InsideTheftMoney_G','300',
	-- v_RiskTypeDCT='InsideTheftProperty','200',
	-- v_RiskTypeDCT='InsideTheftProperty_G','300',
	-- v_RiskTypeDCT='MoneyOrders','200',
	-- v_RiskTypeDCT='OutsidePremises','200',
	-- v_RiskTypeDCT='OutsidePremises_G','300',
	-- 'N/A'
	-- ),
	-- 'N/A')
	DECODE(
	    TRUE,
	    i_TypeBureauCode = 'CR', DECODE(
	        TRUE,
	        v_RiskTypeDCT = 'ClientsProperty', '200',
	        v_RiskTypeDCT = 'ClientsProperty_ETF', '400',
	        v_RiskTypeDCT = 'ComputerFraud', '200',
	        v_RiskTypeDCT = 'ComputerFraud_G', '300',
	        v_RiskTypeDCT = 'EmployeeTheft', '200',
	        v_RiskTypeDCT = 'EmployeeTheft_ETF', '400',
	        v_RiskTypeDCT = 'EmployeeTheftNameOrPosition', '200',
	        v_RiskTypeDCT = 'EmployeeTheftNameOrPosition_ETF', '400',
	        v_RiskTypeDCT = 'EmployeeTheftNameOrPosition_G', '300',
	        v_RiskTypeDCT = 'EmployeeTheftNameOrPosition_GETF', '400',
	        v_RiskTypeDCT = 'EmployeeTheftPerEmployee', '300',
	        v_RiskTypeDCT = 'EmployeeTheftPerEmployee_GETF', '400',
	        v_RiskTypeDCT = 'EmployeeTheftPerLoss', '300',
	        v_RiskTypeDCT = 'EmployeeTheftPerLoss_GETF', '400',
	        v_RiskTypeDCT = 'ForgeryAndAlteration', '200',
	        v_RiskTypeDCT = 'ForgeryAndAlteration_ETF', '400',
	        v_RiskTypeDCT = 'ForgeryAndAlteration_G', '300',
	        v_RiskTypeDCT = 'ForgeryAndAlteration_GETF', '400',
	        v_RiskTypeDCT = 'FundsTransfer', '200',
	        v_RiskTypeDCT = 'FundsTransfer_G', '300',
	        v_RiskTypeDCT = 'GuestPropertyInsidePremises', '200',
	        v_RiskTypeDCT = 'GuestPropertySafeDeposit', '200',
	        v_RiskTypeDCT = 'InsideRobbery', '200',
	        v_RiskTypeDCT = 'InsideRobbery_G', '300',
	        v_RiskTypeDCT = 'InsideRobberyOther', '200',
	        v_RiskTypeDCT = 'InsideRobberyOther_G', '300',
	        v_RiskTypeDCT = 'InsideRobberySecurities', '200',
	        v_RiskTypeDCT = 'InsideRobberySecurities_G', '300',
	        v_RiskTypeDCT = 'InsideTheftMoney', '200',
	        v_RiskTypeDCT = 'InsideTheftMoney_G', '300',
	        v_RiskTypeDCT = 'InsideTheftProperty', '200',
	        v_RiskTypeDCT = 'InsideTheftProperty_G', '300',
	        v_RiskTypeDCT = 'MoneyOrders', '200',
	        v_RiskTypeDCT = 'OutsidePremises', '200',
	        v_RiskTypeDCT = 'OutsidePremises_G', '300',
	        'N/A'
	    ),
	    'N/A'
	) AS v_PolicyForm_DCT,
	-- *INF*: DECODE(TRUE,
	-- i_TypeBureauCode = 'CR',
	-- DECODE(TRUE,
	-- IN(i_BureauCode4,'01','02','03'),'200',
	-- IN(i_BureauCode4,'11','12','13'),'300',
	-- IN(i_BureauCode4,'21','22','23','27','28'),'400',
	-- '200'),
	-- 
	-- i_TypeBureauCode = 'BT',
	-- DECODE(TRUE,
	-- IN(i_BureauCode1||i_BureauCode2,'01','02','03','04','05','06','09'),'120',
	-- IN(i_BureauCode1||i_BureauCode2,'07','08'),'190',
	-- IN(i_BureauCode1||i_BureauCode2,'10','20','26'),'170',
	-- IN(i_BureauCode1||i_BureauCode2,'11','12','13','14','15','16','17','18','19','21','22','23','24','25','29','50'),'110',
	-- IN(i_BureauCode1||i_BureauCode2,'31','32','39'),'130',
	-- IN(i_BureauCode1||i_BureauCode2,'42','43','44','45'),'140',
	-- IN(i_BureauCode1||i_BureauCode2,'45'),'111',
	-- IN(i_BureauCode1||i_BureauCode2,'47'),'121',
	-- IN(i_BureauCode1||i_BureauCode2,'48'),'131',
	-- IN(i_BureauCode1||i_BureauCode2,'49','41','42','43','44'),'140',
	-- IN(i_BureauCode1||i_BureauCode2,'51','59'),'150',
	-- IN(i_BureauCode1||i_BureauCode2,'52','53'),'141',
	-- IN(i_BureauCode1||i_BureauCode2,'54','55'),'151',
	-- IN(i_BureauCode1||i_BureauCode2,'56'),'161',
	-- IN(i_BureauCode1||i_BureauCode2,'57'),'171',
	-- IN(i_BureauCode1||i_BureauCode2,'58'),'181',
	-- IN(i_BureauCode1||i_BureauCode2,'60'),'191',
	-- IN(i_BureauCode1||i_BureauCode2,'61'),'160',
	-- IN(i_BureauCode1||i_BureauCode2,'62','63'),'192',
	-- IN(i_BureauCode1||i_BureauCode2,'64'),'112',
	-- IN(i_BureauCode1||i_BureauCode2,'67','68'),'142',
	-- IN(i_BureauCode1||i_BureauCode2,'69'),'152',
	-- IN(i_BureauCode1||i_BureauCode2,'70','71','72'),'180',
	-- IN(i_BureauCode1||i_BureauCode2,'73'),'122',
	-- '199'
	-- ),
	-- i_TypeBureauCode = 'FT','199',
	-- 'N/A')
	DECODE(
	    TRUE,
	    i_TypeBureauCode = 'CR', DECODE(
	        TRUE,
	        i_BureauCode4 IN ('01','02','03'), '200',
	        i_BureauCode4 IN ('11','12','13'), '300',
	        i_BureauCode4 IN ('21','22','23','27','28'), '400',
	        '200'
	    ),
	    i_TypeBureauCode = 'BT', DECODE(
	        TRUE,
	        i_BureauCode1 || i_BureauCode2 IN ('01','02','03','04','05','06','09'), '120',
	        i_BureauCode1 || i_BureauCode2 IN ('07','08'), '190',
	        i_BureauCode1 || i_BureauCode2 IN ('10','20','26'), '170',
	        i_BureauCode1 || i_BureauCode2 IN ('11','12','13','14','15','16','17','18','19','21','22','23','24','25','29','50'), '110',
	        i_BureauCode1 || i_BureauCode2 IN ('31','32','39'), '130',
	        i_BureauCode1 || i_BureauCode2 IN ('42','43','44','45'), '140',
	        i_BureauCode1 || i_BureauCode2 IN ('45'), '111',
	        i_BureauCode1 || i_BureauCode2 IN ('47'), '121',
	        i_BureauCode1 || i_BureauCode2 IN ('48'), '131',
	        i_BureauCode1 || i_BureauCode2 IN ('49','41','42','43','44'), '140',
	        i_BureauCode1 || i_BureauCode2 IN ('51','59'), '150',
	        i_BureauCode1 || i_BureauCode2 IN ('52','53'), '141',
	        i_BureauCode1 || i_BureauCode2 IN ('54','55'), '151',
	        i_BureauCode1 || i_BureauCode2 IN ('56'), '161',
	        i_BureauCode1 || i_BureauCode2 IN ('57'), '171',
	        i_BureauCode1 || i_BureauCode2 IN ('58'), '181',
	        i_BureauCode1 || i_BureauCode2 IN ('60'), '191',
	        i_BureauCode1 || i_BureauCode2 IN ('61'), '160',
	        i_BureauCode1 || i_BureauCode2 IN ('62','63'), '192',
	        i_BureauCode1 || i_BureauCode2 IN ('64'), '112',
	        i_BureauCode1 || i_BureauCode2 IN ('67','68'), '142',
	        i_BureauCode1 || i_BureauCode2 IN ('69'), '152',
	        i_BureauCode1 || i_BureauCode2 IN ('70','71','72'), '180',
	        i_BureauCode1 || i_BureauCode2 IN ('73'), '122',
	        '199'
	    ),
	    i_TypeBureauCode = 'FT', '199',
	    'N/A'
	) AS v_PolicyForm_PMS,
	-- *INF*: DECODE(TRUE,
	-- i_SourceSystemID='PMS',v_PolicyForm_PMS,
	-- IN(i_SourceSystemID,'DCT','DUC'),v_PolicyForm_DCT
	-- )
	DECODE(
	    TRUE,
	    i_SourceSystemID = 'PMS', v_PolicyForm_PMS,
	    i_SourceSystemID IN ('DCT','DUC'), v_PolicyForm_DCT
	) AS v_PolicyForm,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_PolicyForm)
	UDF_DEFAULT_VALUE_FOR_STRINGS(v_PolicyForm) AS o_PolicyForm,
	i_PremiumMasterPremium AS o_PremiumMasterDirectWrittenPremiumAmount,
	i_PaidLossAmt AS o_PaidLossAmount,
	i_OutstandingAmt AS o_OutstandingLossAmount,
	i_PolicyExpirationDate AS o_PolicyExpirationDate,
	-- *INF*: IIF(ISNULL(i_InceptionToDatePaidLossAmount), 0, i_InceptionToDatePaidLossAmount)
	IFF(i_InceptionToDatePaidLossAmount IS NULL, 0, i_InceptionToDatePaidLossAmount) AS o_InceptionToDatePaidLossAmount,
	-- *INF*: IIF(ISNULL(i_ClaimCoverageID), -1, i_ClaimCoverageID)
	IFF(i_ClaimCoverageID IS NULL, - 1, i_ClaimCoverageID) AS o_ClaimCoverageID,
	i_AnnualStatementLineNumber AS o_AnnualStatementLineNumber,
	-- *INF*: IIF(ISNULL(i_PremiumTransactionEffectiveDate),TO_DATE('18000101','YYYYMMDD'),i_PremiumTransactionEffectiveDate)
	IFF(
	    i_PremiumTransactionEffectiveDate IS NULL, TO_TIMESTAMP('18000101', 'YYYYMMDD'),
	    i_PremiumTransactionEffectiveDate
	) AS o_PremiumTransactionEffectiveDate,
	-- *INF*: DECODE(TRUE,	
	-- i_SourceSystemID='DUC','DCT',
	-- i_SourceSystemID='DCT','DCT',
	-- 'PMS')
	DECODE(
	    TRUE,
	    i_SourceSystemID = 'DUC', 'DCT',
	    i_SourceSystemID = 'DCT', 'DCT',
	    'PMS'
	) AS v_DCT_SRC_ID,
	-- *INF*: IIF(v_DCT_SRC_ID='DCT',:LKP.LKP_Policy(ltrim(rtrim(i_pol_key))),'B')
	IFF(v_DCT_SRC_ID = 'DCT', LKP_POLICY_ltrim_rtrim_i_pol_key.pol_key, 'B') AS v_Policy_SBOP,
	-- *INF*: LTRIM(RTRIM(v_Policy_SBOP))
	LTRIM(RTRIM(v_Policy_SBOP)) AS o_Policy_SBOP,
	-- *INF*: IIF(v_Policy_SBOP<>'B' and v_DCT_SRC_ID='DCT',
	-- to_char(:LKP.LKP_Policy_SBOP(i_pol_key)))
	IFF(
	    v_Policy_SBOP <> 'B' and v_DCT_SRC_ID = 'DCT',
	    to_char(LKP_POLICY_SBOP_i_pol_key.ProgramAKId)
	) AS v_SBOP_GL_PR,
	-- *INF*: decode(true,
	-- v_SBOP_GL_PR='38','Institutional',
	-- v_SBOP_GL_PR='39','Service',
	-- v_SBOP_GL_PR='81','Service',
	-- isnull(v_SBOP_GL_PR),'N/A')
	-- 
	-- -- For Programid 38 Description is Childcare
	-- -- For Programid 39 Description is  Personal apperance
	-- -- For Programid 81 Description is Circuit Workout
	-- -- Other than Program Code 38,39 and 81, we are making as N/A
	decode(
	    true,
	    v_SBOP_GL_PR = '38', 'Institutional',
	    v_SBOP_GL_PR = '39', 'Service',
	    v_SBOP_GL_PR = '81', 'Service',
	    v_SBOP_GL_PR IS NULL, 'N/A'
	) AS v_Program_Desc_SBOP,
	-- *INF*: IIF(v_DCT_SRC_ID='DCT' and v_Program_Desc_SBOP='N/A',
	-- :lkp.LKP_Policy_GL_PR(i_pol_key))
	IFF(v_DCT_SRC_ID = 'DCT' and v_Program_Desc_SBOP = 'N/A',) AS v_Non_SBOP_GL_PR,
	-- *INF*: :LKP.LKP_archCFDCOccpancyType(ltrim(rtrim(i_pol_key)))
	--  
	LKP_ARCHCFDCOCCPANCYTYPE_ltrim_rtrim_i_pol_key.OccupancyType AS v_CF_Occupancy_Type,
	-- *INF*: IIF(ISNULL(v_CF_Occupancy_Type),:LKP.LKP_archGLDCOccpancyType(ltrim(rtrim(i_pol_key))),v_CF_Occupancy_Type)
	IFF(
	    v_CF_Occupancy_Type IS NULL, LKP_ARCHGLDCOCCPANCYTYPE_ltrim_rtrim_i_pol_key.OccupancyType,
	    v_CF_Occupancy_Type
	) AS v_GL_OCcupancy_Type,
	-- *INF*: IIF(v_DCT_SRC_ID='DCT' and not isnull(v_Non_SBOP_GL_PR),
	-- v_GL_OCcupancy_Type,'N/A')
	IFF(v_DCT_SRC_ID = 'DCT' and v_Non_SBOP_GL_PR IS NOT NULL, v_GL_OCcupancy_Type, 'N/A') AS Non_SBOP_OccupancyType,
	-- *INF*: IIF(v_DCT_SRC_ID='DCT' and NOT isnull(v_SBOP_GL_PR),v_Program_Desc_SBOP,
	-- IIF (v_DCT_SRC_ID='DCT' and NOT isnull(v_Non_SBOP_GL_PR),:LKP.LKP_SupPackageModificationAdjustmentGroup(v_DCT_SRC_ID,Non_SBOP_OccupancyType),
	-- IIF (v_DCT_SRC_ID='PMS',:LKP.LKP_SupPackageModificationAdjustmentGroup(v_DCT_SRC_ID,i_PackageModificationAdjustmentGroupCode),'N/A')))
	IFF(
	    v_DCT_SRC_ID = 'DCT' and v_SBOP_GL_PR IS NOT NULL, v_Program_Desc_SBOP,
	    IFF(
	        v_DCT_SRC_ID = 'DCT'
	    and v_Non_SBOP_GL_PR IS NOT NULL,
	        LKP_SUPPACKAGEMODIFICATIONADJUSTMENTGROUP_v_DCT_SRC_ID_Non_SBOP_OccupancyType.PackageModificationAdjustmentGroupDescription,
	        IFF(
	                v_DCT_SRC_ID = 'PMS',
	                LKP_SUPPACKAGEMODIFICATIONADJUSTMENTGROUP_v_DCT_SRC_ID_i_PackageModificationAdjustmentGroupCode.PackageModificationAdjustmentGroupDescription,
	                'N/A'
	            )
	    )
	) AS v_Occupancy_Desc,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_Occupancy_Desc)
	UDF_DEFAULT_VALUE_FOR_STRINGS(v_Occupancy_Desc) AS o_PackageModificationAdjustmentGroupDescription,
	Union.o_LocationNumber AS LocationNumber,
	Union.o_BuildingNumber AS BuildingNumber
	FROM EXP_ConstCode_IsoPC_Rules
	 -- Manually join with Union
	LEFT JOIN LKP_RiskTypeWithMaxPremium
	ON LKP_RiskTypeWithMaxPremium.pol_key = Union.pol_key
	LEFT JOIN LKP_POLICY LKP_POLICY_ltrim_rtrim_i_pol_key
	ON LKP_POLICY_ltrim_rtrim_i_pol_key.pol_key = ltrim(rtrim(i_pol_key))

	LEFT JOIN LKP_POLICY_SBOP LKP_POLICY_SBOP_i_pol_key
	ON LKP_POLICY_SBOP_i_pol_key.pol_key = i_pol_key

	LEFT JOIN LKP_POLICY_GL_PR LKP_POLICY_GL_PR_i_pol_key
	ON LKP_POLICY_GL_PR_i_pol_key. = i_pol_key

	LEFT JOIN LKP_ARCHCFDCOCCPANCYTYPE LKP_ARCHCFDCOCCPANCYTYPE_ltrim_rtrim_i_pol_key
	ON LKP_ARCHCFDCOCCPANCYTYPE_ltrim_rtrim_i_pol_key.PolicyNumber = ltrim(rtrim(i_pol_key))

	LEFT JOIN LKP_ARCHGLDCOCCPANCYTYPE LKP_ARCHGLDCOCCPANCYTYPE_ltrim_rtrim_i_pol_key
	ON LKP_ARCHGLDCOCCPANCYTYPE_ltrim_rtrim_i_pol_key.PolicyNumber = ltrim(rtrim(i_pol_key))

	LEFT JOIN LKP_SUPPACKAGEMODIFICATIONADJUSTMENTGROUP LKP_SUPPACKAGEMODIFICATIONADJUSTMENTGROUP_v_DCT_SRC_ID_Non_SBOP_OccupancyType
	ON LKP_SUPPACKAGEMODIFICATIONADJUSTMENTGROUP_v_DCT_SRC_ID_Non_SBOP_OccupancyType.SourceSystemId = v_DCT_SRC_ID
	AND LKP_SUPPACKAGEMODIFICATIONADJUSTMENTGROUP_v_DCT_SRC_ID_Non_SBOP_OccupancyType.PackageModificationAdjustmentGroupCode = Non_SBOP_OccupancyType

	LEFT JOIN LKP_SUPPACKAGEMODIFICATIONADJUSTMENTGROUP LKP_SUPPACKAGEMODIFICATIONADJUSTMENTGROUP_v_DCT_SRC_ID_i_PackageModificationAdjustmentGroupCode
	ON LKP_SUPPACKAGEMODIFICATIONADJUSTMENTGROUP_v_DCT_SRC_ID_i_PackageModificationAdjustmentGroupCode.SourceSystemId = v_DCT_SRC_ID
	AND LKP_SUPPACKAGEMODIFICATIONADJUSTMENTGROUP_v_DCT_SRC_ID_i_PackageModificationAdjustmentGroupCode.PackageModificationAdjustmentGroupCode = i_PackageModificationAdjustmentGroupCode

),
TGT_ISSCommercialPropertyExtract AS (

	------------ PRE SQL ----------
	@{pipeline().parameters.DELETE_PRESQL}
	-------------------------------


	INSERT INTO ISSCommercialPropertyExtract
	(AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, DeductibleAmount, CoverageCode, ConstructionCode, ISOFireProtectionCode, SublineCode, PackageModificationAdjustmentGroupDescription, PolicyForm, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, TransactionEffectiveDate, LocationNumber, BuildingNumber)
	SELECT 
	o_AuditID AS AUDITID, 
	o_CreatedDate AS CREATEDDATE, 
	o_PremiumMasterCalculationID AS EDWPREMIUMMASTERCALCULATIONPKID, 
	o_LossMasterCalculationId AS EDWLOSSMASTERCALCULATIONPKID, 
	o_TypeBureauCode AS TYPEBUREAUCODE, 
	o_BureauLineOfInsurance AS BUREAULINEOFINSURANCE, 
	o_BureauCompanyNumber AS BUREAUCOMPANYNUMBER, 
	o_StateProvinceCode AS STATEPROVINCECODE, 
	o_PremiumMasterRunDate AS PREMIUMMASTERRUNDATE, 
	o_LossMasterRunDate AS LOSSMASTERRUNDATE, 
	o_pol_key AS POLICYKEY, 
	o_PremiumMasterClassCode AS PREMIUMMASTERCLASSCODE, 
	o_LossMasterClassCode AS LOSSMASTERCLASSCODE, 
	o_ClaimNum AS CLAIMNUMBER, 
	o_ClaimantNum AS CLAIMANTNUMBER, 
	o_RiskTerritoryCode AS RISKTERRITORYCODE, 
	o_PolicyEffectiveDate AS POLICYEFFECTIVEDATE, 
	o_CauseOfLoss AS CAUSEOFLOSS, 
	o_DeductibleAmount AS DEDUCTIBLEAMOUNT, 
	o_CoverageCode AS COVERAGECODE, 
	o_ConstructionCode AS CONSTRUCTIONCODE, 
	o_ISOFireProtectionCode AS ISOFIREPROTECTIONCODE, 
	o_SublineCode AS SUBLINECODE, 
	o_PackageModificationAdjustmentGroupDescription AS PACKAGEMODIFICATIONADJUSTMENTGROUPDESCRIPTION, 
	o_PolicyForm AS POLICYFORM, 
	o_PremiumMasterDirectWrittenPremiumAmount AS PREMIUMMASTERDIRECTWRITTENPREMIUMAMOUNT, 
	o_PaidLossAmount AS PAIDLOSSAMOUNT, 
	o_OutstandingLossAmount AS OUTSTANDINGLOSSAMOUNT, 
	o_PolicyExpirationDate AS POLICYEXPIRATIONDATE, 
	o_InceptionToDatePaidLossAmount AS INCEPTIONTODATEPAIDLOSSAMOUNT, 
	o_ClaimCoverageID AS CLAIMANTCOVERAGEDETAILID, 
	o_AnnualStatementLineNumber AS ANNUALSTATEMENTLINENUMBER, 
	o_PremiumTransactionEffectiveDate AS TRANSACTIONEFFECTIVEDATE, 
	LOCATIONNUMBER, 
	BUILDINGNUMBER
	FROM EXP_Values
),
SQ_ISSCommercialPropertyExtract_UpdateForPMS_WithFirstAvailableLocation AS (
	select  ISS.ISSCommercialPropertyExtractId,
	ISS.TypeBureauCode,
	ISS.PolicyKey,
	ISS.ConstructionCode,
	ISS.ISOFireProtectionCode
	from @{pipeline().parameters.TARGET_TABLE_OWNER}.ISSCommercialPropertyExtract ISS
	where (case when ISS.EDWPremiumMasterCalculationPKId<>-1 then ISS.PremiumMasterRunDate else ISS.LossMasterRunDate end) between 
	 DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0)--first day of last Quarter   
	 AND
	 DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0))--Last day of last Quarter
	and len(ISS.PolicyKey)=12
	and (ISS.ConstructionCode='00' or ISS.ISOFireProtectionCode='00')
	and 1=2
),
EXP_UpdateForPMS_WithFirstAvailableLocation AS (
	SELECT
	ISSCommercialPropertyExtractId AS WorkISSExtractId,
	TypeBureauCode AS i_TypeBureauCode,
	PolicyKey AS i_PolicyKey,
	ConstructionCode AS i_ConstructionCode,
	ISOFireProtectionCode AS i_ISOFireProtectionCode,
	-- *INF*: IIF(i_ConstructionCode='00', :LKP.LKP_WORKISSEXTRACT_CONSTRUCTIONCODE_FIRST(i_PolicyKey, i_TypeBureauCode), null)
	IFF(
	    i_ConstructionCode = '00',
	    LKP_WORKISSEXTRACT_CONSTRUCTIONCODE_FIRST_i_PolicyKey_i_TypeBureauCode.ConstructionCode,
	    null
	) AS v_ConstructionCode,
	-- *INF*: IIF(i_ISOFireProtectionCode='00', :LKP.LKP_WORKISSEXTRACT_ISOFIREPROTECTIONCODE_FIRST(i_PolicyKey, i_TypeBureauCode), null)
	IFF(
	    i_ISOFireProtectionCode = '00',
	    LKP_WORKISSEXTRACT_ISOFIREPROTECTIONCODE_FIRST_i_PolicyKey_i_TypeBureauCode.ISOFireProtectionCode,
	    null
	) AS v_ISOFireProtectionCode,
	-- *INF*: IIF(ISNULL(v_ConstructionCode), i_ConstructionCode, v_ConstructionCode)
	IFF(v_ConstructionCode IS NULL, i_ConstructionCode, v_ConstructionCode) AS o_ConstructionCode,
	-- *INF*: IIF(ISNULL(v_ISOFireProtectionCode), i_ISOFireProtectionCode, v_ISOFireProtectionCode)
	IFF(v_ISOFireProtectionCode IS NULL, i_ISOFireProtectionCode, v_ISOFireProtectionCode) AS o_ISOFireProtectionCode
	FROM SQ_ISSCommercialPropertyExtract_UpdateForPMS_WithFirstAvailableLocation
	LEFT JOIN LKP_WORKISSEXTRACT_CONSTRUCTIONCODE_FIRST LKP_WORKISSEXTRACT_CONSTRUCTIONCODE_FIRST_i_PolicyKey_i_TypeBureauCode
	ON LKP_WORKISSEXTRACT_CONSTRUCTIONCODE_FIRST_i_PolicyKey_i_TypeBureauCode.PolicyKey = i_PolicyKey
	AND LKP_WORKISSEXTRACT_CONSTRUCTIONCODE_FIRST_i_PolicyKey_i_TypeBureauCode.TypeBureauCode = i_TypeBureauCode

	LEFT JOIN LKP_WORKISSEXTRACT_ISOFIREPROTECTIONCODE_FIRST LKP_WORKISSEXTRACT_ISOFIREPROTECTIONCODE_FIRST_i_PolicyKey_i_TypeBureauCode
	ON LKP_WORKISSEXTRACT_ISOFIREPROTECTIONCODE_FIRST_i_PolicyKey_i_TypeBureauCode.PolicyKey = i_PolicyKey
	AND LKP_WORKISSEXTRACT_ISOFIREPROTECTIONCODE_FIRST_i_PolicyKey_i_TypeBureauCode.TypeBureauCode = i_TypeBureauCode

),
FIL_Valid AS (
	SELECT
	WorkISSExtractId, 
	o_ConstructionCode AS ConstructionCode, 
	o_ISOFireProtectionCode AS ISOFireProtectionCode
	FROM EXP_UpdateForPMS_WithFirstAvailableLocation
	WHERE ConstructionCode<>'00' OR ISOFireProtectionCode<>'00'
),
UPD_UpdateForPMS_WithFirstAvailableLocation AS (
	SELECT
	WorkISSExtractId, 
	ConstructionCode, 
	ISOFireProtectionCode
	FROM FIL_Valid
),
TGT_ISSCommercialPropertyExtract_UpdateForPMS_WithFirstAvailableLocation AS (
	MERGE INTO ISSCommercialPropertyExtract AS T
	USING UPD_UpdateForPMS_WithFirstAvailableLocation AS S
	ON T.ISSCommercialPropertyExtractId = S.WorkISSExtractId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ConstructionCode = S.ConstructionCode, T.ISOFireProtectionCode = S.ISOFireProtectionCode
),
SQ_ISSCommercialPropertyExtract_Update_ConstructionCode AS (
	DECLARE @StartDate AS DATETIME = (SELECT DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0))	--First day of last Quarter 
	DECLARE @EndDate AS DATETIME = (SELECT DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0)));	--Last day of last Quarter
	
	--INCORRECT DATA
	WITH IncorrectConstructionCode
	AS
	(
		SELECT DISTINCT ISS.ISSCommercialPropertyExtractId AS ISSCommercialPropertyExtractId, 
		ISS.PolicyKey AS PolicyKey,
		ISS.ConstructionCode AS CurrentConstructionCode
		FROM ISSCommercialPropertyExtract AS ISS WITH (NOLOCK)
		WHERE ISS.EDWPremiumMasterCalculationPKId <> -1
		AND ISS.SublineCode NOT IN ('090', '070', '930')
		AND ISS.LocationNumber='0001'
		AND ISS.BuildingNumber = '000'
		AND LEN(ISS.PolicyKey) <> 12
		AND ISS.TypeBureauCode = 'CF'
		AND ISS.ConstructionCode IN ('N/A', '00')
		AND (ISS.PremiumMasterRunDate BETWEEN  @StartDate AND @EndDate)	
	)
	
	SELECT DISTINCT IncorrectConstructionCode.ISSCommercialPropertyExtractId AS ISSCommercialPropertyExtractId, 
	IncorrectConstructionCode.PolicyKey AS PolicyKey, 
	ISNULL(CorrectConstructionCode.ConstructionCode, '0') AS ConstructionCode
	FROM IncorrectConstructionCode AS IncorrectConstructionCode
	LEFT JOIN(
		SELECT ISS.ISSCommercialPropertyExtractId AS ISSCommercialPropertyExtractId, 
		ISS.PolicyKey AS PolicyKey, 
		ISS.ConstructionCode AS ConstructionCode,
		ISS.LocationNumber AS LocationNumber, 
		ISS.BuildingNumber AS BuildingNumber,
		(CASE 
			WHEN ISS.BuildingNumber <> '000'
			THEN 
			ROW_NUMBER() OVER (PARTITION BY ISS.PolicyKey
			ORDER BY ISS.LocationNumber, ISS.BuildingNumber)
			ELSE 0
		END) 
		AS RowNumber  
		FROM ISSCommercialPropertyExtract AS ISS WITH (NOLOCK)
		INNER JOIN IncorrectConstructionCode AS IncorrectConstructionCode ON IncorrectConstructionCode.PolicyKey = ISS.PolicyKey
		WHERE ISS.EDWPremiumMasterCalculationPKId <> -1
		AND ISS.BuildingNumber <> '000'
		AND LEN(ISS.PolicyKey) <> 12
		AND ISS.TypeBureauCode = 'CF'
		AND ISS.ConstructionCode NOT IN ('N/A', '00')
		AND (ISS.PremiumMasterRunDate BETWEEN  @StartDate AND @EndDate)	
	) AS CorrectConstructionCode
	ON IncorrectConstructionCode.PolicyKey = CorrectConstructionCode.PolicyKey
	AND CorrectConstructionCode.RowNumber = 1
	ORDER BY ISSCommercialPropertyExtractId
),
EXP_Update_ConstructionCode AS (
	SELECT
	ISSCommercialPropertyExtractId,
	PolicyKey,
	ConstructionCode AS i_ConstructionCode,
	-- *INF*: IIF(IN(i_ConstructionCode, '0', '00', 'N/A' ), :LKP.LKP_UPDATE_CONSTRUCTIONCODE(PolicyKey), i_ConstructionCode)
	IFF(
	    i_ConstructionCode IN ('0','00','N/A'),
	    LKP_UPDATE_CONSTRUCTIONCODE_PolicyKey.ConstructionCode,
	    i_ConstructionCode
	) AS v_ConstructionCode,
	-- *INF*: IIF(NOT ISNULL(v_ConstructionCode), v_ConstructionCode, '0')
	IFF(v_ConstructionCode IS NOT NULL, v_ConstructionCode, '0') AS o_ConstructionCode
	FROM SQ_ISSCommercialPropertyExtract_Update_ConstructionCode
	LEFT JOIN LKP_UPDATE_CONSTRUCTIONCODE LKP_UPDATE_CONSTRUCTIONCODE_PolicyKey
	ON LKP_UPDATE_CONSTRUCTIONCODE_PolicyKey.PolicyKey = PolicyKey

),
FIL_Update_ConstructionCode AS (
	SELECT
	ISSCommercialPropertyExtractId, 
	PolicyKey, 
	o_ConstructionCode AS ConstructionCode
	FROM EXP_Update_ConstructionCode
	WHERE ConstructionCode<>'0'
),
UPD_Update_ConstructionCode AS (
	SELECT
	ISSCommercialPropertyExtractId, 
	ConstructionCode
	FROM FIL_Update_ConstructionCode
),
ISSCommercialPropertyExtract_Update_ConstructionCode AS (
	MERGE INTO ISSCommercialPropertyExtract AS T
	USING UPD_Update_ConstructionCode AS S
	ON T.ISSCommercialPropertyExtractId = S.ISSCommercialPropertyExtractId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ConstructionCode = S.ConstructionCode
),
SQ_ISSCommercialPropertyExtract_Update_ClassCode AS (
	DECLARE @StartDate AS DATETIME = (SELECT DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.FIRST_DAY_OF_THE_QUARTER},0))	--First day of last Quarter 
	DECLARE @EndDate AS DATETIME = (SELECT DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE())+@{pipeline().parameters.LAST_DAY_OF_THE_QUARTER}, 0)));	--Last day of last Quarter
	
	--INCORRECT DATA
	WITH IncorrectClassCode
	AS
	(
		SELECT DISTINCT ISS.ISSCommercialPropertyExtractId AS ISSCommercialPropertyExtractId, 
		ISS.PolicyKey AS PolicyKey,
		ISS.PremiumMasterClassCode AS CurrentClassCode
		FROM ISSCommercialPropertyExtract AS ISS WITH (NOLOCK)
		WHERE ISS.EDWPremiumMasterCalculationPKId <> -1
		AND ISS.SublineCode NOT IN ('090', '070', '930')
		AND ISS.LocationNumber='0001'
		AND ISS.BuildingNumber = '000'
		AND ISS.CoverageCode = '03'
		AND LEN(ISS.PolicyKey) <> 12
		AND ISS.TypeBureauCode = 'CF'
		AND ISS.PremiumMasterClassCode IN ('N/A', '0000', '99999')
		AND (ISS.PremiumMasterRunDate BETWEEN @StartDate AND @EndDate)
	)
	
	SELECT DISTINCT	IncorrectClassCode.ISSCommercialPropertyExtractId AS ISSCommercialPropertyExtractId, 
	IncorrectClassCode.PolicyKey AS PolicyKey, 
	ISNULL(CorrectClassCode.ClassCode, '0') AS CorrectClassCode
	FROM IncorrectClassCode AS IncorrectClassCode
	LEFT JOIN(
		SELECT DISTINCT ISS.ISSCommercialPropertyExtractId AS ISSCommercialPropertyExtractId, 
		ISS.PolicyKey AS PolicyKey, 
		ISS.PremiumMasterClassCode AS ClassCode,
		ISS.LocationNumber AS LocationNumber, 
		ISS.BuildingNumber AS BuildingNumber,
		(CASE 
			WHEN ISS.BuildingNumber <> '000'
			THEN 
			ROW_NUMBER() OVER (PARTITION BY ISS.PolicyKey
			ORDER BY ISS.LocationNumber, ISS.BuildingNumber)
			ELSE 0
		END) 
		AS RowNumber  
		FROM ISSCommercialPropertyExtract AS ISS WITH (NOLOCK)
		INNER JOIN IncorrectClassCode AS IncorrectClassCode ON IncorrectClassCode.PolicyKey = ISS.PolicyKey
		WHERE ISS.EDWPremiumMasterCalculationPKId <> -1
		AND ISS.BuildingNumber <> '000'
		AND LEN(ISS.PolicyKey) <> 12
		AND ISS.TypeBureauCode = 'CF'
		AND ISS.PremiumMasterClassCode NOT IN ('N/A', '0000', '99999')
		AND (ISS.PremiumMasterRunDate BETWEEN @StartDate AND @EndDate)
	) AS CorrectClassCode
	ON IncorrectClassCode.PolicyKey = CorrectClassCode.PolicyKey
	AND CorrectClassCode.RowNumber = 1
	ORDER BY ISSCommercialPropertyExtractId
),
EXP_Update_ClassCode AS (
	SELECT
	ISSCommercialPropertyExtractId,
	PolicyKey,
	PremiumMasterClassCode AS i_PremiumMasterClassCode,
	-- *INF*: IIF(IN(i_PremiumMasterClassCode, '0', 'N/A', '0000', '99999'), :LKP.LKP_UPDATE_CLASSCODE(PolicyKey), i_PremiumMasterClassCode)
	IFF(
	    i_PremiumMasterClassCode IN ('0','N/A','0000','99999'),
	    LKP_UPDATE_CLASSCODE_PolicyKey.ClassCode,
	    i_PremiumMasterClassCode
	) AS v_PremiumMasterClassCode,
	-- *INF*: IIF(NOT ISNULL(v_PremiumMasterClassCode), v_PremiumMasterClassCode, '0')
	IFF(v_PremiumMasterClassCode IS NOT NULL, v_PremiumMasterClassCode, '0') AS o_PremiumMasterClassCode
	FROM SQ_ISSCommercialPropertyExtract_Update_ClassCode
	LEFT JOIN LKP_UPDATE_CLASSCODE LKP_UPDATE_CLASSCODE_PolicyKey
	ON LKP_UPDATE_CLASSCODE_PolicyKey.PolicyKey = PolicyKey

),
FIL_Update_ClassCode AS (
	SELECT
	ISSCommercialPropertyExtractId, 
	PolicyKey, 
	o_PremiumMasterClassCode AS PremiumMasterClassCode
	FROM EXP_Update_ClassCode
	WHERE PremiumMasterClassCode<>'0'
),
UPD_Update_ClassCode AS (
	SELECT
	ISSCommercialPropertyExtractId, 
	PremiumMasterClassCode
	FROM FIL_Update_ClassCode
),
ISSCommercialPropertyExtract_Update_ClassCode AS (
	MERGE INTO ISSCommercialPropertyExtract AS T
	USING UPD_Update_ClassCode AS S
	ON T.ISSCommercialPropertyExtractId = S.ISSCommercialPropertyExtractId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.PremiumMasterClassCode = S.PremiumMasterClassCode
),