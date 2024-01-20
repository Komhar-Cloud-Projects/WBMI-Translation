WITH
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
LKP_Premium_RetroactiveDate_Policy AS (
	SELECT
	RetroactiveDate,
	PolicyKey
	FROM (
		SELECT CTE.RetroactiveDate AS RetroactiveDate,CTE.PolicyKey as PolicyKey
		FROM
		(
		--PMS
		
		SELECT CTE.RetroactiveDate AS RetroactiveDate,CTE.PolicyKey as PolicyKey
		FROM
		(
		Select POL.POL_KEY AS PolicyKey ,MAX(T.RetroactiveDate) as RetroactiveDate
		FROM
		@{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact PMF
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC
		on PMF.EDWPremiumMasterCalculationPKID=PMC.PremiumMasterCalculationId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		on PT.PremiumTransactionAKID=PMC.PremiumTransactionAKID
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
		on SC.StatisticalCoverageAKID=PT.StatisticalCoverageAKID
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on PC.PolicyCoverageAKID=SC.PolicyCoverageAKID
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
		on PC.RiskLocationAKID=RL.RiskLocationAKID
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL
		on POL.pol_ak_id=RL.PolicyAKID
		and POL.crrnt_snpsht_flag=1
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG
		on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
		and ISG.CurrentSnapshotFlag=1
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRC
		on IRC.InsuranceReferenceCoverageDimId=PMF.InsuranceReferenceCoverageDimId
		INNER JOIN
		(
		select PremiumTransactionID, RetroactiveDate  from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto
		Union all
		 select PremiumTransactionID, RetroactiveDate from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailGeneralLiability 
		union all
		select PremiumTransactionID, RetroactiveDate  from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialProperty
		union all
		select  PremiumTransactionID, RetroactiveDate  from  @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialUmbrella 
		) T
		on T.PremiumTransactionID = PT.PremiumTransactionID
		where 
		(DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,PMC.PremiumMasterRunDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1))
		AND (PMC.PremiumMasterTypeBureauCode IN ('GL') or (PMC.PremiumMasterTypeBureauCode = 'AL' AND IRC.CoverageCode = 'EPLI') 
		or (PMC.PremiumMasterTypeBureauCode = 'BE' AND IRC.CoverageCode = 'EPLI'))
		AND PT.SourceSystemID='PMS'
		AND ISG.InsuranceSegmentCode IN ('1','2')
		--AND RL.StateProvinceCode IN ('12','13','14','15','16','21','22','24','34','48') (Line removed - RFC 126190)
		AND PMC.PremiumMasterPremium <>0
		AND PMC.PremiumMasterPremiumType='D'
		AND PMC.PremiumMasterTransactionCode IN ('10','11','12','13','14','15','18','19','20','21','22','23','24','25','28','29','57','67') 
		AND PMC.PremiumMasterReasonAmendedCode NOT IN ('COL' , 'CWO')
		GROUP BY 
		POL.Pol_Key
		)CTE
		
		
		UNION ALL
		
		---DCT
		SELECT CTE.RetroactiveDate AS RetroactiveDate,CTE.PolicyKey as PolicyKey
		FROM
		(
		Select POL.POL_KEY AS PolicyKey ,MAX(T.RetroactiveDate) as RetroactiveDate
		FROM 
		@{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact PMF
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC
		on PMF.EDWPremiumMasterCalculationPKID=PMC.PremiumMasterCalculationId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		on PT.PremiumTransactionAKID=PMC.PremiumTransactionAKID
		and PMC.CurrentSnapshotFlag=1
		and PT.CurrentSnapshotFlag=1
		inner join  @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
		on PT.RatingCoverageAKID=RC.RatingCoverageAKID
		and RC.EffectiveDate=PT.EffectiveDate 
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
		and PC.CurrentSnapshotFlag=1
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
		on PC.RiskLocationAKID=RL.RiskLocationAKID
		and RL.CurrentSnapshotFlag=1
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL
		on POL.pol_ak_id=RL.PolicyAKID
		and POL.crrnt_snpsht_flag=1
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG
		on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
		and ISG.CurrentSnapshotFlag=1
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRC
		on IRC.InsuranceReferenceCoverageDimId=PMF.InsuranceReferenceCoverageDimId
		INNER JOIN
		(
		select PremiumTransactionID, RetroactiveDate  from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto
		Union all
		 select PremiumTransactionID, RetroactiveDate from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailGeneralLiability 
		union all
		select PremiumTransactionID, RetroactiveDate  from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialProperty
		union all
		select  PremiumTransactionID, RetroactiveDate  from  @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialUmbrella 
		) T
		on T.PremiumTransactionID = PT.PremiumTransactionID
		where 
		(DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,PMC.PremiumMasterRunDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1))
		AND 
		(
		  ( PC.TypeBureauCode IN 
		     (select type_bureau_code from sup_type_bureau_code where StandardTypeBureauCode = 'GL' AND crrnt_snpsht_flag=1) 
		    and PMC.PremiumMasterSubLine !='317'  
		  ) 
		 or (PMC.PremiumMasterSubLine ='317') 
		 or PC.InsuranceLine in ('CommercialUmbrella','DirectorsAndOffsCondos','EmploymentPracticesLiab','ExcessLiability','SBOPGeneralLiability')
		 or (PC.InsuranceLine='CommercialAuto' and RC.CoverageType='WB516CA')
		 or (PC.InsuranceLine='BusinessOwners' and RC.CoverageType='EmploymentPracticesLiability')
		)
		AND PT.SourceSystemID='DCT'
		AND ISG.InsuranceSegmentCode IN ('1','2')
		--AND RL.StateProvinceCode IN ('12','13','14','15','16','21','22','24','34','48') (Line removed - RFC 126190)
		AND PMC.PremiumMasterPremium <>0
		AND PMC.PremiumMasterPremiumType='D'
		AND PMC.PremiumMasterTransactionCode IN ('10','11','12','13','14','15','18','19','20','21','22','23','24','25','28','29','30','31','57','67') 
		AND PMC.PremiumMasterReasonAmendedCode NOT IN ('CWO', 'CWB')
		GROUP BY 
		POL.Pol_Key
		)CTE
		)CTE ---
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY RetroactiveDate) = 1
),
LKP_Loss_RetroactiveDate_Policy AS (
	SELECT
	RetroactiveDate,
	PolicyKey
	FROM (
		SELECT CTE.RetroactiveDate AS RetroactiveDate,CTE.PolicyKey as PolicyKey
		FROM
		(
		--PMS
		
		SELECT CTE.RetroactiveDate AS RetroactiveDate,CTE.PolicyKey as PolicyKey
		FROM
		(
		Select POL.POL_KEY AS PolicyKey ,MAX(T.RetroactiveDate) as RetroactiveDate
		FROM
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation LMC
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction CT
		on LMC.claim_trans_ak_id=CT.claim_trans_ak_id
		and LMC.crrnt_snpsht_flag=1
		and CT.crrnt_snpsht_flag=1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
		ON CT.claimant_cov_det_ak_id= CCD.claimant_cov_det_ak_id
		AND CCD.crrnt_snpsht_flag = 1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
		on SC.StatisticalCoverageAKID=CCD.StatisticalCoverageAKID
		and SC.CurrentSnapshotFlag=1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on PC.PolicyCoverageAKID=SC.PolicyCoverageAKID
		join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL with (nolock)
		on POL.pol_ak_id=PC.PolicyAKID
		and POL.crrnt_snpsht_flag=1 AND PC.CurrentSnapshotFlag=1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
		on PC.RiskLocationAKID=RL.RiskLocationAKID
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG
		on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
		and ISG.CurrentSnapshotFlag=1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		on SC.StatisticalCoverageAKID = PT.StatisticalCoverageAKID
		INNER JOIN
		(
		select PremiumTransactionID, RetroactiveDate  from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto
		Union all
		 select PremiumTransactionID, RetroactiveDate from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailGeneralLiability 
		union all
		select PremiumTransactionID, RetroactiveDate  from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialProperty
		union all
		select  PremiumTransactionID, RetroactiveDate  from  @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialUmbrella 
		) T
		on T.PremiumTransactionID = PT.PremiumTransactionID
		where CCD.pms_type_bureau_code='GL'
		AND LMC.trans_kind_code='D'
		AND ISG.InsuranceSegmentCode IN ('1','2')
		AND (DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,LMC.loss_master_run_date),-1) =DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1))
		--AND RL.StateProvinceCode IN ('12','13','14','15','16','21','22','24','34','48') (Line removed - RFC 126190)
		GROUP BY 
		POL.POL_KEY
		)CTE
		
		
		UNION ALL
		
		---DCT
		
		SELECT CTE.RetroactiveDate AS RetroactiveDate,CTE.PolicyKey as PolicyKey
		FROM
		(
		Select POL.POL_KEY AS PolicyKey ,MAX(T.RetroactiveDate) as RetroactiveDate
		FROM 
		 @{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation LMC
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction CT
		on LMC.claim_trans_ak_id=CT.claim_trans_ak_id
		and LMC.crrnt_snpsht_flag=1
		and CT.crrnt_snpsht_flag=1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
		ON CT.claimant_cov_det_ak_id= CCD.claimant_cov_det_ak_id
		AND CCD.crrnt_snpsht_flag = 1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
		on CCD.RatingCoverageAKID=RC.RatingCoverageAKID
		and (case when LMC.trans_offset_onset_ind='O' 
		then LMC.pms_acct_entered_date
		else DATEADD(D,1,LMC.loss_master_run_date)  end) between RC.EffectiveDate and RC.ExpirationDate 
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
		and PC.CurrentSnapshotFlag=1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL with (nolock)
		ON POL.pol_ak_id=PC.PolicyAKID
		and POL.crrnt_snpsht_flag=1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
		on PC.RiskLocationAKID=RL.RiskLocationAKID
		and RL.CurrentSnapshotFlag=1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG
		on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
		and ISG.CurrentSnapshotFlag=1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		on RC.RatingCoverageAKID = PT.RatingCoverageAKID
		and RC.EffectiveDate=PT.EffectiveDate
		INNER JOIN
		(
		select PremiumTransactionID, RetroactiveDate  from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto
		Union all
		 select PremiumTransactionID, RetroactiveDate from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailGeneralLiability 
		union all
		select PremiumTransactionID, RetroactiveDate  from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialProperty
		union all
		select  PremiumTransactionID, RetroactiveDate  from  @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialUmbrella 
		) T
		on T.PremiumTransactionID = PT.PremiumTransactionID
		where 
		(
		(CCD.pms_type_bureau_code IN ( select type_bureau_code from sup_type_bureau_code where StandardTypeBureauCode = 'GL'  AND crrnt_snpsht_flag=1) and LMC.sub_line_code !='317')
		or 
		(LMC.sub_line_code='317')
		or PC.InsuranceLine in ('CommercialUmbrella','DirectorsAndOffsCondos','EmploymentPracticesLiab','ExcessLiability','SBOPGeneralLiability')
		or (PC.InsuranceLine='CommercialAuto' and RC.CoverageType='WB516CA')
		or (PC.InsuranceLine='BusinessOwners' and RC.CoverageType='EmploymentPracticesLiability')
		)
		AND LMC.trans_kind_code='D'
		AND ISG.InsuranceSegmentCode IN ('1','2')
		AND (DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,LMC.loss_master_run_date),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1))
		--AND RL.StateProvinceCode IN ('12','13','14','15','16','21','22','24','34','48') (Line removed - RFC 126190)
		GROUP BY 
		POL.POL_KEY
		)CTE
		)CTE---
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY RetroactiveDate) = 1
),
LKP_archGLDCOccpancyType AS (
	SELECT
	OccupancyType,
	PolicyNumber
	FROM (
		select distinct dr.PolicyNumber as PolicyNumber,dcgl.OccupancyTypeMonoline as  OccupancyType
		 from archDCGLOccupancyStaging dcgl 
		inner join 
		(Select distinct dp.PolicyNumber+PolicyVersionFormatted as PolicyNumber, dr.GL_RiskId,dr.SessionId,dl.type
		 From  VWArchWorkDCTPolicy dp inner join   archDCLineStaging dl
		 on
		 dp.PolicyId=dl.PolicyId and
		  dp.SessionId=dl.SessionId 
		 inner join  archDCGLRiskStaging dr
		 on dr.LineId=dl.LineId and
		 dr.SessionId=dl.SessionId 
		 )  dr
		 on dcgl.GL_RiskId=dr.GL_RiskId and
		 dcgl.SessionId=dr.SessionId 
		 where dr.type='GeneralLiability' and dcgl.OccupancyTypeMonoline is not null
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber ORDER BY OccupancyType) = 1
),
LKP_archCFDCOccpancyType AS (
	SELECT
	OccupancyType,
	PolicyNumber
	FROM (
		select distinct db.PolicyNumber as PolicyNumber,do.OccupancyType as OccupancyType From  ArchDCCFlocationStaging dloc 
		inner join
		(SELECT distinct dp.PolicyNumber+PolicyVersionFormatted as PolicyNumber,db.CFlocationid as CFlocationid,
		db.Sessionid as sessionid,db.CFBuildingId,dl.type as type,db.description as description
		From VWArchWorkDCTPolicy dp inner join 
		dbo.archDCLineStaging dl
		on
		dp.PolicyId=dl.PolicyId 
		and dp.Sessionid=dl.Sessionid
		 inner join  dbo.ArchDCCFBuildingStage db
		on dl.LineId=db.LineId
		and dl.Sessionid=db.Sessionid
		) db
		on dloc.CF_locationid = db.CFlocationid AND  dloc.Sessionid=db.Sessionid
		INNER JOIN dbo.archDCCFRiskStaging dr
		on dr.CF_BuildingId=db.CFBuildingId
		and dr.Sessionid=db.Sessionid
		 inner join dbo.archDCCFOccupancyStaging do
		on do.CF_RiskId=dr.CF_RiskId 
		and do.Sessionid=dr.Sessionid  
		where  db.Type in('CommercialAuto','CommercialUmbrella','Property')
		and dloc.description='Primary Location'
		and db.description like 'Building #1%' 
		and do.OccupancyType is not null
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
		PC.sourcesystemid='DCT'  and p.crrnt_snpsht_flag =1 and pc.currentsnapshotflag =1
		and 
		PC.InsuranceLine ='GeneralLiability') a 
		inner join
		(
		select   p.pol_key as pol_key , pc.policyakid  as PolicyAKID
		From @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy p inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		ON
		p.pol_ak_id=pc.policyakid and  PC.sourcesystemid='DCT'  and p.crrnt_snpsht_flag =1 and PC.currentsnapshotflag =1
		and PC.InsuranceLine ='Property') b 
		ON  a.PolicyAKID=b.PolicyAKID
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_key) = 1
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
		and  p.crrnt_snpsht_flag =1 and pc.currentsnapshotflag =1 and PC.sourcesystemid='DCT' and 
		PC.InsuranceLine ='SBOPGeneralLiability') a 
		 inner join
		(
		select   p.pol_key as pol_key , pc.policyakid  as PolicyAKID,P.ProgramAKId as ProgramAKId
		From @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy p inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		ON
		p.pol_ak_id=pc.policyakid
		and p.crrnt_snpsht_flag =1 and PC.currentsnapshotflag =1 and PC.sourcesystemid='DCT' 
		and PC.InsuranceLine ='SBOPProperty') b 
		ON  a.PolicyAKID=b.PolicyAKID
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY ProgramAKId) = 1
),
LKP_Policy AS (
	SELECT
	pol_key
	FROM (
		select p.pol_key as Pol_Key 
		from @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy p inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage pc
		on
		p.pol_ak_id=pc.PolicyAKID and p.crrnt_snpsht_flag =1 and pc.currentsnapshotflag =1
		where pc.InsuranceLine like 'SBOP%'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_key) = 1
),
LKP_SupClassiFicationGeneralLiability AS (
	SELECT
	RatingBasis,
	ClassCode,
	RatingStateCode
	FROM (
		SELECT 
			RatingBasis,
			ClassCode,
			RatingStateCode
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupClassificationGeneralLiability
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode,RatingStateCode ORDER BY RatingBasis) = 1
),
SQ_Premium AS (
	--PMS
	SELECT distinct
	PMC.PremiumMasterCalculationID,
	PMC.PremiumMasterRunDate,
	POL.pol_key,
	POL.prim_bus_class_code,
	RL.StateProvinceCode,
	PT.PremiumTransactionBookedDate,
	PMC.PremiumMasterSubLine,
	CASE WHEN SC.ClassCode='000000' then '44444' else SC.ClassCode end as classcode, 
	'N/A' as RiskTerritory,
	POL.pol_eff_date,
	PMC.PremiumMasterPremium,
	PMC.PremiumMasterTypeBureauCode,
	SC.RiskUnitGroup,
	PT.SourceSystemID,
	PMC.PremiumMasterTransactionCode,
	PMC.PremiumMasterReasonAmendedCode,
	'N/A' AS RiskType,
	CASE WHEN PC.TypeBureauCode='GL' THEN bsc.BureauCode3 ELSE PT.ConstructionCode END as ConstructionCode,
	'N/A' as IsoFireProtectionCode,
	PT.PackageModificationAdjustmentGroupCode,
	bsc.BureauCode1,
	'N/A' AS BureauCode2,
	'N/A' AS BureauCode4,
	POL.pol_exp_date,
	ASL.asl_num,
	ISG.InsuranceSegmentCode,
	PremiumMasterExposure,
	IRC.CoverageCode,
	RL.ZipPostalCode,
	ISNULL(CDGL.RetroactiveDate,'1800-01-01') as RetroactiveDate,
	case when CDGL.LiabilityFormCode is null and SC.SublineCode in ('641', '313') then '3' else CDGL.LiabilityFormCode end as LiabilityFormCode,
	PT.PremiumTransactionEffectiveDate,
	SC.SublineCode,
	'N/A' as CoverageType,
	PC.InsuranceLine,
	'N/A' as CoverageVersion,
	PT.NumberOfEmployee,
	PT.PremiumTransactionId,
	'N/A' as SubCoverageTypeCode,
	null as RatingCoverageAKID,
	RL.LocationUnitNumber
	from @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact PMF
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim ASL
	on PMF.AnnualStatementLineDimId=ASL.asl_dim_id
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC
	on PMF.EDWPremiumMasterCalculationPKID=PMC.PremiumMasterCalculationId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	on PT.PremiumTransactionAKID=PMC.PremiumTransactionAKID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
	on SC.StatisticalCoverageAKID=PT.StatisticalCoverageAKID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on PC.PolicyCoverageAKID=SC.PolicyCoverageAKID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	on PC.RiskLocationAKID=RL.RiskLocationAKID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL
	on POL.pol_ak_id=RL.PolicyAKID
	and POL.crrnt_snpsht_flag=1
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG
	on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
	and ISG.CurrentSnapshotFlag=1
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailGeneralLiability CDGL
	on CDGL.PremiumTransactionId=PT.PremiumTransactionId
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.BureauStatisticalCode bsc
	on bsc.PremiumTransactionAKID = PT.PremiumTransactionAKID 
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRC
	on IRC.InsuranceReferenceCoverageDimId=PMF.InsuranceReferenceCoverageDimId
	where 
	(DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,PMC.PremiumMasterRunDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1))
	AND (PMC.PremiumMasterTypeBureauCode IN ('GL') or (PMC.PremiumMasterTypeBureauCode = 'AL' AND IRC.CoverageCode = 'EPLI') 
	or (PMC.PremiumMasterTypeBureauCode = 'BE' AND IRC.CoverageCode = 'EPLI'))
	AND PT.SourceSystemID='PMS'
	AND ISG.InsuranceSegmentCode IN ('1','2')
	AND PMC.PremiumMasterPremium <>0
	AND PMC.PremiumMasterPremiumType='D'
	AND PMC.PremiumMasterTransactionCode IN ('10','11','12','13','14','15','18','19','20','21','22','23','24','25','28','29','57','67') 
	AND PMC.PremiumMasterReasonAmendedCode NOT IN ('COL' , 'CWO') @{pipeline().parameters.WHERE_CLAUSE_1}
	
	UNION ALL
	--DCT
	SELECT distinct
	PMC.PremiumMasterCalculationID,
	PMC.PremiumMasterRunDate,
	POL.pol_key,
	POL.prim_bus_class_code,
	RL.StateProvinceCode,
	PT.PremiumTransactionBookedDate,
	PMC.PremiumMasterSubLine,
	RC.ClassCode,
	ISNULL(PTRR.RatingTerritoryCode,'N/A') as RiskTerritory,
	POL.pol_eff_date,
	PMC.PremiumMasterPremium,
	CASE WHEN PMC.PremiumMasterSubLine='317' THEN 'GL' ELSE PMC.PremiumMasterTypeBureauCode END as PremiumMasterTypeBureauCode ,
	'N/A' AS RiskUnitGroup,
	PT.SourceSystemID,
	PMC.PremiumMasterTransactionCode,
	PMC.PremiumMasterReasonAmendedCode,
	RC.RiskType,
	PT.ConstructionCode,
	'N/A' as IsoFireProtectionCode,
	PT.PackageModificationAdjustmentGroupCode,
	'N/A' AS BureauCode1,
	'N/A' AS BureauCode2,
	'N/A' AS BureauCode4,
	POL.pol_exp_date,
	ASL.asl_num,
	ISG.InsuranceSegmentCode,
	PremiumMasterExposure,
	IRC.CoverageCode,
	RL.ZipPostalCode,
	ISNULL(CDGL.RetroactiveDate,'1800-01-01') as RetroactiveDate,
	case when PC.InsuranceLine in ('CommercialUmbrella') then '3' else CDGL.LiabilityFormCode end as LiabilityFormCode,
	PT.PremiumTransactionEffectiveDate,
	RC.SublineCode,
	RC.CoverageType,
	PC.InsuranceLine,
	RC.CoverageVersion,
	PT.NumberOfEmployee,
	PT.PremiumTransactionId,
	RC.SubCoverageTypeCode,
	RC.RatingCoverageAKID,
	RL.LocationUnitNumber
	from @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact PMF
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim ASL
	on PMF.AnnualStatementLineDimId=ASL.asl_dim_id
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC
	on PMF.EDWPremiumMasterCalculationPKID=PMC.PremiumMasterCalculationId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	on PT.PremiumTransactionAKID=PMC.PremiumTransactionAKID
	and PMC.CurrentSnapshotFlag=1
	and PT.CurrentSnapshotFlag=1
	inner join  @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	on PT.RatingCoverageAKID=RC.RatingCoverageAKID
	and RC.EffectiveDate=PT.EffectiveDate 
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
	and PC.CurrentSnapshotFlag=1
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	on PC.RiskLocationAKID=RL.RiskLocationAKID
	and RL.CurrentSnapshotFlag=1
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL
	on POL.pol_ak_id=RL.PolicyAKID
	and POL.crrnt_snpsht_flag=1
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG
	on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
	and ISG.CurrentSnapshotFlag=1
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailGeneralLiability CDGL
	on CDGL.PremiumTransactionId=PT.PremiumTransactionId
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRC
	on IRC.InsuranceReferenceCoverageDimId=PMF.InsuranceReferenceCoverageDimId
	left join PremiumTransactionRatingRisk PTRR  with (nolock) 
	on PTRR.PremiumTransactionAKID=PT.PremiumTransactionAKID
	where 
	(DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,PMC.PremiumMasterRunDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1))
	AND 
	(
	  ( PC.TypeBureauCode IN 
	     (select type_bureau_code from sup_type_bureau_code where StandardTypeBureauCode = 'GL' AND crrnt_snpsht_flag=1) 
	    and PMC.PremiumMasterSubLine !='317'  
	  ) 
	 or (PMC.PremiumMasterSubLine ='317') 
	 or PC.InsuranceLine in ('CommercialUmbrella','DirectorsAndOffsCondos','EmploymentPracticesLiab','ExcessLiability','SBOPGeneralLiability')
	 or (PC.InsuranceLine='CommercialAuto' and RC.CoverageType in  ('WB516CA','WB516CANC'))
	 or (PC.InsuranceLine='BusinessOwners' and RC.CoverageType='EmploymentPracticesLiability')
	)
	AND PT.SourceSystemID='DCT'
	AND ISG.InsuranceSegmentCode IN ('1','2')
	AND PMC.PremiumMasterPremium <>0
	AND PMC.PremiumMasterPremiumType='D'
	AND PMC.PremiumMasterTransactionCode IN ('10','11','12','13','14','15','18','19','20','21','22','23','24','25','28','29','30','31','57','67') 
	AND PMC.PremiumMasterReasonAmendedCode NOT IN ('CWO', 'CWB')  @{pipeline().parameters.WHERE_CLAUSE_1}
),
LKP_RetroactiveDate AS (
	SELECT
	RetroactiveDate,
	i_PremiumTransactionID,
	PremiumTransactionID
	FROM (
		select t.PremiumTransactionID as PremiumTransactionID
		, t.RetroactiveDate as RetroactiveDate 
		from
		(
		select PremiumTransactionID, RetroactiveDate  from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto
		Union all
		 select PremiumTransactionID, RetroactiveDate from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailGeneralLiability 
		union all
		select PremiumTransactionID, RetroactiveDate  from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialProperty
		union all
		select  PremiumTransactionID, RetroactiveDate  from  @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialUmbrella 
		) T
		inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT 
		on T.PremiumTransactionID = PT.PremiumTransactionID
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY RetroactiveDate) = 1
),
LKP_TargetTable_Premium AS (
	SELECT
	EDWPremiumMasterCalculationPKId
	FROM (
		SELECT
		GL.EDWPremiumMasterCalculationPKId as EDWPremiumMasterCalculationPKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ISSGeneralLiabilityExtract GL
		WHERE DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GL.PremiumMasterRunDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1)
		AND GL.EDWPremiumMasterCalculationPKId<>-1
		
		UNION ALL
		
		SELECT
		PL.EDWPremiumMasterCalculationPKId as EDWPremiumMasterCalculationPKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ISSProfessionalLiabilityExtract PL
		WHERE DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,PL.PremiumMasterRunDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1)
		AND PL.EDWPremiumMasterCalculationPKId<>-1
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPremiumMasterCalculationPKId ORDER BY EDWPremiumMasterCalculationPKId) = 1
),
FIL_Exists_Premium AS (
	SELECT
	LKP_TargetTable_Premium.EDWPremiumMasterCalculationPKId AS LKP_PremiumMasterCalculationID, 
	SQ_Premium.PremiumMasterCalculationID, 
	SQ_Premium.PremiumMasterRunDate, 
	SQ_Premium.pol_key, 
	SQ_Premium.prim_bus_class_code, 
	SQ_Premium.StateProvinceCode, 
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
	SQ_Premium.ConstructionCode, 
	SQ_Premium.IsoFireProtectionCode, 
	SQ_Premium.PackageModificationAdjustmentGroupCode, 
	SQ_Premium.BureauCode1, 
	SQ_Premium.BureauCode2, 
	SQ_Premium.BureauCode4, 
	SQ_Premium.pol_exp_date, 
	SQ_Premium.asl_num, 
	SQ_Premium.InsuranceSegmentCode, 
	SQ_Premium.PremiumMasterExposure, 
	SQ_Premium.CoverageCode, 
	SQ_Premium.ZipPostalCode, 
	SQ_Premium.RetroactiveDate, 
	SQ_Premium.LiabilityFormCode, 
	SQ_Premium.PremiumTransactionEffectiveDate, 
	SQ_Premium.SublineCode, 
	SQ_Premium.CoverageType, 
	SQ_Premium.InsuranceLine, 
	SQ_Premium.CoverageVersion, 
	SQ_Premium.NumberOfEmployee, 
	LKP_RetroactiveDate.RetroactiveDate AS lkp_RetroactiveDate, 
	SQ_Premium.SubCoverageTypeCode, 
	SQ_Premium.RatingCoverageAKID, 
	SQ_Premium.LocationUnitNumber
	FROM SQ_Premium
	LEFT JOIN LKP_RetroactiveDate
	ON LKP_RetroactiveDate.PremiumTransactionID = SQ_Premium.PremiumTransactionID
	LEFT JOIN LKP_TargetTable_Premium
	ON LKP_TargetTable_Premium.EDWPremiumMasterCalculationPKId = SQ_Premium.PremiumMasterCalculationID
	WHERE ISNULL(LKP_PremiumMasterCalculationID)
-------------------Filter Premium is 0-------------------------
AND 
ROUND(PremiumMasterPremium,2)<>0
-----------------------------------------------------------------------
),
EXP_Cleansing_Premium AS (
	SELECT
	PremiumMasterCalculationID AS i_PremiumMasterCalculationID,
	PremiumMasterRunDate AS i_PremiumMasterRunDate,
	pol_key AS i_pol_key,
	prim_bus_class_code AS i_prim_bus_class_code,
	StateProvinceCode AS i_StateProvinceCode,
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
	i_PremiumTransactionBookedDate AS o_PremiumTransactionBookedDate,
	-- *INF*: RTRIM(LTRIM(i_PremiumMasterSubLine))
	RTRIM(LTRIM(i_PremiumMasterSubLine)) AS o_PremiumMasterSubLine,
	-- *INF*: RTRIM(LTRIM(i_ClassCode))
	RTRIM(LTRIM(i_ClassCode)) AS o_ClassCode,
	-- *INF*: RTRIM(LTRIM(i_RiskTerritory))
	RTRIM(LTRIM(i_RiskTerritory)) AS o_RiskTerritory,
	i_pol_eff_date AS o_pol_eff_date,
	-- *INF*: i_PremiumMasterPremium
	-- 
	-- 
	-- --IIF( IN(i_PremiumMasterTransactionCode, '10','11','12','13','14','15','18','19','20','21','22','23','24','25','28','29','57','67') AND  NOT IN(i_PremiumMasterReasonAmendedCode, 'COL' , 'CWO'), i_PremiumMasterPremium, 0)
	i_PremiumMasterPremium AS o_PremiumMasterPremium,
	-- *INF*: RTRIM(LTRIM(i_TypeBureauCode))
	RTRIM(LTRIM(i_TypeBureauCode)) AS o_TypeBureauCode,
	-- *INF*: RTRIM(LTRIM(i_RiskUnitGroup))
	RTRIM(LTRIM(i_RiskUnitGroup)) AS o_RiskUnitGroup,
	-- *INF*: LTRIM(RTRIM(i_SourceSystemID))
	LTRIM(RTRIM(i_SourceSystemID)) AS o_SourceSystemID,
	-- *INF*: LTRIM(RTRIM(i_RiskType))
	LTRIM(RTRIM(i_RiskType)) AS o_RiskType,
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
	InsuranceSegmentCode,
	PremiumMasterExposure,
	CoverageCode,
	ZipPostalCode AS i_ZipPostalCode,
	LiabilityFormCode AS i_LiabilityFormCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ZipPostalCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_ZipPostalCode) AS o_ZipPostalCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_LiabilityFormCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_LiabilityFormCode) AS o_LiabilityFormCode,
	PremiumTransactionEffectiveDate,
	SublineCode,
	CoverageType,
	InsuranceLine,
	CoverageVersion,
	NumberOfEmployee,
	lkp_RetroactiveDate,
	-- *INF*: IIF(SublineCode='345',:LKP.Lkp_Premium_RetroactiveDate_Policy(LTRIM(RTRIM(i_pol_key))),lkp_RetroactiveDate)
	-- 
	-- --- This variable is being used to get the Retroactive date at Policy level for Condo D& O Policies
	IFF(
	    SublineCode = '345',
	    LKP_PREMIUM_RETROACTIVEDATE_POLICY_LTRIM_RTRIM_i_pol_key.RetroactiveDate,
	    lkp_RetroactiveDate
	) AS v_RetroactiveDate,
	-- *INF*: IIF(ISNULL(v_RetroactiveDate),TO_DATE('18000101000000' , 'YYYYMMDDHH24MISS') , v_RetroactiveDate)
	IFF(
	    v_RetroactiveDate IS NULL, TO_TIMESTAMP('18000101000000', 'YYYYMMDDHH24MISS'),
	    v_RetroactiveDate
	) AS RetroactiveDate,
	SubCoverageTypeCode,
	RatingCoverageAKID,
	LocationUnitNumber,
	-- *INF*: LTRIM(RTRIM(LocationUnitNumber))
	LTRIM(RTRIM(LocationUnitNumber)) AS o_LocationUnitNumber
	FROM FIL_Exists_Premium
	LEFT JOIN LKP_PREMIUM_RETROACTIVEDATE_POLICY LKP_PREMIUM_RETROACTIVEDATE_POLICY_LTRIM_RTRIM_i_pol_key
	ON LKP_PREMIUM_RETROACTIVEDATE_POLICY_LTRIM_RTRIM_i_pol_key.PolicyKey = LTRIM(RTRIM(i_pol_key))

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
	o_PremiumTransactionBookedDate AS PremiumTransactionBookedDate,
	o_PremiumMasterSubLine AS PremiumMasterSubLine,
	-- *INF*: DECODE(TRUE,PremiumMasterSubLine='025','027',PremiumMasterSubLine)
	DECODE(
	    TRUE,
	    PremiumMasterSubLine = '025', '027',
	    PremiumMasterSubLine
	) AS sub_line_code_out,
	o_ClassCode AS ClassCode,
	-- *INF*: IIF(ISNULL(ClassCode) OR IS_SPACES(ClassCode) OR LENGTH(ClassCode)=0
	-- OR IN(ClassCode, 'N/A','TBD'),
	-- '44444',
	-- ClassCode
	-- )
	-- 
	-- --IIF(ISNULL(ClassCode) OR LENGTH(ClassCode)=0,'N/A',ClassCode)
	IFF(
	    ClassCode IS NULL
	    or LENGTH(ClassCode)>0
	    and TRIM(ClassCode)=''
	    or LENGTH(ClassCode) = 0
	    or ClassCode IN ('N/A','TBD'),
	    '44444',
	    ClassCode
	) AS PremiumMasterClassCode,
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
	o_BureauCode1 AS BureauCode1,
	BureauCode2,
	BureauCode4,
	o_ConstructionCode AS ConstructionCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(ConstructionCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(ConstructionCode) AS ConstructionCode_out,
	o_IsoFireProtectionCode AS IsoFireProtectionCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(IsoFireProtectionCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(IsoFireProtectionCode) AS IsoFireProtectionCode_out,
	o_PackageModificationAdjustmentGroupCode AS i_PackageModificationAdjustmentGroupCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_PackageModificationAdjustmentGroupCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_PackageModificationAdjustmentGroupCode) AS PackageModificationAdjustmentGroupCode_out,
	o_pol_exp_date AS pol_exp_date,
	o_AnnualStatementLineNumber AS AnnualStatementLineNumber,
	InsuranceSegmentCode,
	PremiumMasterExposure,
	CoverageCode AS i_CoverageCode,
	i_CoverageCode AS CoverageCode,
	o_LiabilityFormCode AS i_LiabilityFormCode,
	SublineCode AS i_SublineCode,
	CoverageType,
	InsuranceLine,
	CoverageVersion AS i_CoverageVersion,
	RetroactiveDate AS i_RetroactiveDate,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(i_LiabilityFormCode))='6','3',
	-- (InsuranceLine='GeneralLiability' AND CoverageType='WB516GL') OR (InsuranceLine='CommercialAuto' AND IN(CoverageType,'WB516CA','WB516CANC')) OR 
	-- (InsuranceLine='SBOPGeneralLiability' AND IN(CoverageType,'NS0279','NS0313')) OR 
	-- CoverageType='EmploymentPracticesLiability' OR i_CoverageCode = 'EPLI' OR InsuranceLine='DirectorsAndOffsCondos',
	-- DECODE(TRUE,
	-- TO_CHAR(GET_DATE_PART(i_RetroactiveDate, 'YYYY'))<>'1800','1',
	-- TO_CHAR(GET_DATE_PART(i_RetroactiveDate, 'YYYY'))='1800','4',
	-- i_LiabilityFormCode
	-- ),
	-- i_LiabilityFormCode)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(i_LiabilityFormCode)) = '6', '3',
	    (InsuranceLine = 'GeneralLiability' AND CoverageType = 'WB516GL') OR (InsuranceLine = 'CommercialAuto' AND CoverageType IN ('WB516CA','WB516CANC')) OR (InsuranceLine = 'SBOPGeneralLiability' AND CoverageType IN ('NS0279','NS0313')) OR CoverageType = 'EmploymentPracticesLiability' OR i_CoverageCode = 'EPLI' OR InsuranceLine = 'DirectorsAndOffsCondos', DECODE(
	        TRUE,
	        TO_CHAR(DATE_PART(i_RetroactiveDate, 'YYYY')) <> '1800', '1',
	        TO_CHAR(DATE_PART(i_RetroactiveDate, 'YYYY')) = '1800', '4',
	        i_LiabilityFormCode
	    ),
	    i_LiabilityFormCode
	) AS v_TypeofPolicycontract,
	v_TypeofPolicycontract AS o_TypeofPolicycontract,
	-- *INF*: TO_DATE('1800-01-01' , 'YYYY-MM-DD')
	TO_TIMESTAMP('1800-01-01', 'YYYY-MM-DD') AS claim_loss_date,
	-- *INF*: IIF(ISNULL(i_RetroactiveDate) OR TO_CHAR(GET_DATE_PART(i_RetroactiveDate, 'YYYY'))='1800' OR IN(ltrim(rtrim(v_TypeofPolicycontract)),'3','4','5'), 'N/A', TO_CHAR(GET_DATE_PART(i_RetroactiveDate, 'YYYY')))
	IFF(
	    i_RetroactiveDate IS NULL
	    or TO_CHAR(DATE_PART(i_RetroactiveDate, 'YYYY')) = '1800'
	    or ltrim(rtrim(v_TypeofPolicycontract)) IN ('3','4','5'),
	    'N/A',
	    TO_CHAR(DATE_PART(i_RetroactiveDate, 'YYYY'))
	) AS v_ClaimsEntryYear,
	-- *INF*: v_ClaimsEntryYear
	-- 
	-- --'N/A'
	v_ClaimsEntryYear AS ClaimsEntryYear,
	0 AS PaidAllocatedlossAdjustmentExpenseAmount,
	0 AS OutstandingAllocatedLossAdjustmentExpenseAmount1,
	o_ZipPostalCode AS ZipPostalCode,
	PremiumTransactionEffectiveDate,
	NumberOfEmployee,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_SublineCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_SublineCode) AS o_SublineCode,
	SubCoverageTypeCode AS i_SubCoverageTypeCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_SubCoverageTypeCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_SubCoverageTypeCode) AS o_SubCoverageTypeCode,
	RatingCoverageAKID,
	o_LocationUnitNumber
	FROM EXP_Cleansing_Premium
),
EXP_Exposure AS (
	SELECT
	SourceSystemID AS i_SourceSystemID,
	PremiumMasterExposure AS i_PremiumMasterExposure,
	CoverageType AS i_CoverageType,
	InsuranceLine AS i_InsuranceLine,
	NumberOfEmployee AS i_NumberOfEmployee,
	o_SublineCode AS i_SublineCode,
	o_SubCoverageTypeCode AS i_SubCoverageTypeCode,
	PremiumMasterClassCode AS i_ClassCode,
	StateProvinceCode AS i_StateProvinceCode,
	-- *INF*: IIF(NOT ISNULL(:LKP.LKP_SUPCLASSIFICATIONGENERALLIABILITY(i_ClassCode,i_StateProvinceCode)),
	-- :LKP.LKP_SUPCLASSIFICATIONGENERALLIABILITY(i_ClassCode,i_StateProvinceCode), :LKP.LKP_SUPCLASSIFICATIONGENERALLIABILITY(i_ClassCode,'99'))
	IFF(
	    LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_StateProvinceCode.RatingBasis IS NOT NULL,
	    LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_StateProvinceCode.RatingBasis,
	    LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_99.RatingBasis
	) AS v_lkp_RatingBasis,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_lkp_RatingBasis)
	UDF_DEFAULT_VALUE_FOR_STRINGS(v_lkp_RatingBasis) AS v_RatingBasis,
	-- *INF*: DECODE(TRUE,
	-- i_SubCoverageTypeCode='TerrorismPremium',0,
	-- (i_InsuranceLine='GeneralLiability' AND i_CoverageType='WB516GL') OR (i_InsuranceLine='CommercialAuto' AND IN(i_CoverageType,'WB516CA','WB516CANC')) OR (i_InsuranceLine='SBOPGeneralLiability' AND IN(i_CoverageType,'NS0279','NS0313')) OR i_CoverageType='EmploymentPracticesLiability',i_NumberOfEmployee,
	-- i_SourceSystemID='DCT' AND v_lkp_RatingBasis='M',i_PremiumMasterExposure/1000,
	-- i_SourceSystemID='DCT' AND v_lkp_RatingBasis='C',i_PremiumMasterExposure/100,
	-- i_PremiumMasterExposure)
	DECODE(
	    TRUE,
	    i_SubCoverageTypeCode = 'TerrorismPremium', 0,
	    (i_InsuranceLine = 'GeneralLiability' AND i_CoverageType = 'WB516GL') OR (i_InsuranceLine = 'CommercialAuto' AND i_CoverageType IN ('WB516CA','WB516CANC')) OR (i_InsuranceLine = 'SBOPGeneralLiability' AND i_CoverageType IN ('NS0279','NS0313')) OR i_CoverageType = 'EmploymentPracticesLiability', i_NumberOfEmployee,
	    i_SourceSystemID = 'DCT' AND v_lkp_RatingBasis = 'M', i_PremiumMasterExposure / 1000,
	    i_SourceSystemID = 'DCT' AND v_lkp_RatingBasis = 'C', i_PremiumMasterExposure / 100,
	    i_PremiumMasterExposure
	) AS o_PremiumMasterExposure
	FROM EXP_Logic_Premium
	LEFT JOIN LKP_SUPCLASSIFICATIONGENERALLIABILITY LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_StateProvinceCode
	ON LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_StateProvinceCode.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_StateProvinceCode.RatingStateCode = i_StateProvinceCode

	LEFT JOIN LKP_SUPCLASSIFICATIONGENERALLIABILITY LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_99
	ON LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_99.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_99.RatingStateCode = '99'

),
SQ_Loss AS (
	SELECT distinct
	LMC.loss_master_calculation_id,
	LMC.loss_master_run_date as loss_master_run_date,
	POL.prim_bus_class_code,
	RL.StateProvinceCode,
	OCC.claim_loss_date,
	LMC.sub_line_code,
	LMC.class_code,
	CT.cause_of_loss,
	'N/A' as RiskTerritory ,
	POL.pol_eff_date,
	POL.pol_key,
	OCC.claim_occurrence_num,
	CPO.claimant_num,
	(case when LMC.trans_kind_code = 'D' then  LMC.paid_loss_amt else 0 end) as paid_loss_amt,
	(case when LMC.financialtypecode = 'D' and LMC.trans_kind_code = 'D' Then LMC.outstanding_amt Else 0 End ) as outstanding_amt,
	CCD.pms_type_bureau_code,
	SC.RiskUnitGroup,
	CCD.PolicySourceID,
	'N/A' AS RiskType,
	SC.StatisticalCoverageAKID as CoverageAKID,
	POL.pol_exp_date,
	OCC.s3p_claim_num,
	CT.claim_trans_id,
	CCD.claimant_cov_det_ak_id,
	ASL.asl_num,
	PC.InsuranceLine,
	POL.pol_num,
	LMC.statistical_code1,
	ISG.InsuranceSegmentCode,
	IRC.CoverageCode,
	(case when LMC.financialtypecode = 'E' and LMC.trans_kind_code = 'D' then LMF.eom_unpaid_loss_adjust_exp else 0 End) as DirectALAEOutstandingER,
	(case when LMC.trans_kind_code = 'D' then  LMF.paid_exp_amt else 0 end) as DirectALAEPaidIR,
	SC.MajorPerilCode,
	RL.ZipPostalCode,
	POL.pms_pol_lob_code,
	PremiumTransaction.PackageModificationAdjustmentGroupCode as PackageModificationAdjustmentGroupCode, 
	PremiumTransaction.ConstructionCode as ConstructionCode, 
	CoverageDetailCommercialProperty.IsoFireProtectionCode as IsoFireProtectionCode, 
	LTRIM(RTRIM(BureauCode1)) as BureauCode1,
	LTRIM(RTRIM(BureauCode2)) as BureauCode2,
	LTRIM(RTRIM(BureauCode4)) as BureauCode4,
	case when CoverageDetailGeneralLiability.LiabilityFormCode is null and SC.SublineCode in ('641', '313') then '3' else CoverageDetailGeneralLiability.LiabilityFormCode end as LiabilityFormCode,
	ISNULL(CoverageDetailGeneralLiability.RetroactiveDate,'1800-01-01') as RetroactiveDate,
	CASE WHEN CT.trans_date<DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},0) THEN LMC.loss_master_run_date ELSE CT.trans_date END AS trans_date,
	SC.SublineCode,
	'N/A' as CoverageType,
	'N/A' as CoverageVersion,
	PremiumTransaction.PremiumTransactionID
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_fact LMF
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim ASL
	on LMF.asl_dim_id=ASL.asl_dim_id
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim CD
	on LMF.loss_master_run_date_id=CD.clndr_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation LMC
	on LMC.loss_master_calculation_id=LMF.edw_loss_master_calculation_pk_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction CT
	on LMC.claim_trans_ak_id=CT.claim_trans_ak_id
	and LMC.crrnt_snpsht_flag=1
	and CT.crrnt_snpsht_flag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
	ON CT.claimant_cov_det_ak_id= CCD.claimant_cov_det_ak_id
	AND CCD.crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO
	ON CPO.claim_party_occurrence_ak_id=CCD.claim_party_occurrence_ak_id
	AND CPO.Crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence OCC
	ON CPO.claim_occurrence_ak_id= OCC.claim_occurrence_ak_id
	AND  OCC.crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim PD with(nolock)
	on LMF.pol_dim_id=PD.pol_dim_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL with (nolock)
	on POL.pol_id=PD.edw_pol_pk_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG
	on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
	and ISG.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
	on SC.StatisticalCoverageAKID=CCD.StatisticalCoverageAKID
	and SC.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on PC.PolicyCoverageAKID=SC.PolicyCoverageAKID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	on PC.RiskLocationAKID=RL.RiskLocationAKID
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRC
	on IRC.InsuranceReferenceCoverageDimId=LMF.InsuranceReferenceCoverageDimId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}. PremiumTransaction
	on SC.StatisticalCoverageAKID = PremiumTransaction.StatisticalCoverageAKID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.BureauStatisticalCode
	on BureauStatisticalCode.PremiumTransactionAKID = PremiumTransaction.PremiumTransactionAKID
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialProperty
	on CoverageDetailCommercialProperty.PremiumTransactionID=PremiumTransaction.PremiumTransactionID
	and CoverageDetailCommercialProperty.CurrentSnapshotFlag=1
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailGeneralLiability
	on CoverageDetailGeneralLiability.PremiumTransactionID=PremiumTransaction.PremiumTransactionID
	and CoverageDetailGeneralLiability.CurrentSnapshotFlag=1
	where 
	(CCD.pms_type_bureau_code IN ('GL') or (CCD.pms_type_bureau_code = 'AL' AND IRC.CoverageCode = 'EPLI') 
	or (CCD.pms_type_bureau_code = 'BE' AND IRC.CoverageCode = 'EPLI'))
	AND LMC.trans_kind_code='D'
	AND ISG.InsuranceSegmentCode IN ('1','2')
	AND (DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,LMC.loss_master_run_date),-1) =DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1))
	AND (LMC.paid_loss_amt<>0 or LMC.outstanding_amt<>0 or LMF.eom_unpaid_loss_adjust_exp <>0 or LMF.paid_exp_amt<>0) 
	AND LMF.audit_id<>-9  @{pipeline().parameters.WHERE_CLAUSE_2}
	
	
	--DCT
	union all
	SELECT distinct
	LMC.loss_master_calculation_id,
	LMC.loss_master_run_date as loss_master_run_date,
	POL.prim_bus_class_code,
	RL.StateProvinceCode,
	OCC.claim_loss_date,
	LMC.sub_line_code,
	LMC.class_code,
	CT.cause_of_loss,
	ISNULL(PTRR.RatingTerritoryCode,'N/A') as RiskTerritory,
	POL.pol_eff_date,
	POL.pol_key,
	OCC.claim_occurrence_num,
	CPO.claimant_num,
	(case when LMC.trans_kind_code = 'D' then  LMC.paid_loss_amt else 0 end) as paid_loss_amt,
	(Case when LMC.financialtypecode  = 'D' and LMC.trans_kind_code = 'D' Then LMC.outstanding_amt Else 0 End) as outstanding_amt,
	Case when LMC.sub_line_code='317' Then 'GeneralLiability' Else CCD.pms_type_bureau_code End pms_type_bureau_code,
	'N/A' AS RiskUnitGroup,
	CCD.PolicySourceID,
	LTRIM(RTRIM(RC.RiskType)) AS RiskType,
	RC.RatingCoverageAKID as CoverageAKID,
	POL.pol_exp_date,
	OCC.s3p_claim_num,
	CT.claim_trans_id,
	CCD.claimant_cov_det_ak_id,
	ASL.asl_num,
	PC.InsuranceLine,
	POL.pol_num,
	LMC.statistical_code1,
	ISG.InsuranceSegmentCode,
	IRC.CoverageCode,
	(case when LMC.financialtypecode = 'E' and LMC.trans_kind_code = 'D' then LMF.eom_unpaid_loss_adjust_exp else 0 End) as DirectALAEOutstandingER,
	(case when LMC.trans_kind_code = 'D' then  LMF.paid_exp_amt else 0 end) as DirectALAEPaidIR,
	CCD.major_peril_code MajorPerilCode,
	RL.ZipPostalCode,
	POL.pms_pol_lob_code,
	null as PackageModificationAdjustmentGroupCode, 
	'N/A' as ConstructionCode, 
	'N/A' as IsoFireProtectionCode, 
	null as BureauCode1,
	null as BureauCode2,
	null as BureauCode4,
	null as LiabilityFormCode,
	null as RetroactiveDate,
	CASE WHEN CT.trans_date < DATEADD(qq,DATEDIFF(qq,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},0) THEN LMC.loss_master_run_date ELSE CT.trans_date END AS trans_date,
	RC.SublineCode,
	RC.CoverageType,
	RC.CoverageVersion,
	null as PremiumTransactionID
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_fact LMF
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim ASL
	on LMF.asl_dim_id=ASL.asl_dim_id
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim CD
	on LMF.loss_master_run_date_id=CD.clndr_id
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRC
	on IRC.InsuranceReferenceCoverageDimId=LMF.InsuranceReferenceCoverageDimId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation LMC
	on LMC.loss_master_calculation_id=LMF.edw_loss_master_calculation_pk_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction CT
	on LMC.claim_trans_ak_id=CT.claim_trans_ak_id
	and LMC.crrnt_snpsht_flag=1
	and CT.crrnt_snpsht_flag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
	ON CT.claimant_cov_det_ak_id= CCD.claimant_cov_det_ak_id
	AND CCD.crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO
	ON CPO.claim_party_occurrence_ak_id=CCD.claim_party_occurrence_ak_id
	AND CPO.Crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence OCC
	ON CPO.claim_occurrence_ak_id= OCC.claim_occurrence_ak_id
	AND  OCC.crrnt_snpsht_flag = 1
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim PD with(nolock)
	on LMF.pol_dim_id=PD.pol_dim_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL with (nolock)
	on POL.pol_id=PD.edw_pol_pk_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG
	on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
	and ISG.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	on CCD.RatingCoverageAKID=RC.RatingCoverageAKID
	and (case when LMC.trans_offset_onset_ind='O' and LMC.pms_acct_entered_date != '1800-01-01 01:00:00.000' 
	then LMC.pms_acct_entered_date
	else DATEADD(D,1,LMC.loss_master_run_date)  end) between RC.EffectiveDate and RC.ExpirationDate 
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
	and PC.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	on PC.RiskLocationAKID=RL.RiskLocationAKID
	and RL.CurrentSnapshotFlag=1 
	left join PremiumTransactionRatingRisk PTRR with (nolock) on PTRR.PremiumTransactionAKID=LMC.PremiumTransactionAKID
	where 
	(
	(CCD.pms_type_bureau_code IN ( select type_bureau_code from sup_type_bureau_code where StandardTypeBureauCode = 'GL'  AND crrnt_snpsht_flag=1) and LMC.sub_line_code !='317')
	or 
	(LMC.sub_line_code='317')
	or PC.InsuranceLine in ('CommercialUmbrella','DirectorsAndOffsCondos','EmploymentPracticesLiab','ExcessLiability','SBOPGeneralLiability')
	or (PC.InsuranceLine='CommercialAuto' and RC.CoverageType in  ('WB516CA','WB516CANC'))
	or (PC.InsuranceLine='BusinessOwners' and RC.CoverageType='EmploymentPracticesLiability')
	)
	AND LMC.trans_kind_code='D'
	AND ISG.InsuranceSegmentCode IN ('1','2')
	AND (DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,LMC.loss_master_run_date),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1))
	AND (LMC.paid_loss_amt<>0 or LMC.outstanding_amt<>0 or LMF.eom_unpaid_loss_adjust_exp <>0 or LMF.paid_exp_amt<>0) 
	AND LMF.audit_id<>-9   @{pipeline().parameters.WHERE_CLAUSE_2}
),
AGG_RemoveDuplicate AS (
	SELECT
	loss_master_calculation_id,
	loss_master_run_date,
	prim_bus_class_code,
	StateProvinceCode,
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
	CoverageAKID,
	pol_exp_date,
	s3p_claim_num,
	claim_trans_id,
	claim_coverage_detail_ak_id,
	asl_num,
	InsuranceLine,
	pol_num,
	statistical_code1,
	InsuranceSegmentCode,
	CoverageCode,
	DirectALAEOutstandingER,
	DirectALAEPaidIR,
	MajorPerilCode,
	ZipPostalCode,
	pms_pol_lob_code,
	PackageModificationAdjustmentGroupCode,
	ConstructionCode,
	IsoFireProtectionCode,
	BureauCode1,
	BureauCode2,
	BureauCode4,
	LiabilityFormCode,
	RetroactiveDate,
	trans_date,
	SublineCode,
	CoverageType,
	CoverageVersion,
	PremiumTransactionID
	FROM SQ_Loss
	QUALIFY ROW_NUMBER() OVER (PARTITION BY loss_master_calculation_id ORDER BY NULL) = 1
),
LKP_PremiumTransactionAttibutes AS (
	SELECT
	PremiumTransactionID,
	PackageModificationAdjustmentGroupCode,
	LiabilityFormCode,
	RetroactiveDate,
	RatingCoverageAKID
	FROM (
		SELECT 
		PT.PremiumTransactionID as PremiumTransactionID,
		PT.PackageModificationAdjustmentGroupCode as PackageModificationAdjustmentGroupCode,
		CASE when PC.InsuranceLine in ('CommercialUmbrella') then '3' else CDGL.LiabilityFormCode end as LiabilityFormCode,
		ISNULL(CDGL.RetroactiveDate,'1800-01-01') as RetroactiveDate,
		PT.RatingCoverageAKID as RatingCoverageAKID
		from 
		DBO.PremiumTransaction PT
		INNER JOIN dbo.RatingCoverage RC ON PT.RatingCoverageAKID = RC.RatingCoverageAKID AND RC.EffectiveDate=PT.EffectiveDate
		INNER JOIN dbo.PolicyCoverage PC ON PC.PolicyCoverageAKID=RC.PolicyCoverageAKID and PC.CurrentSnapshotFlag=1
		INNER JOIN dbo.RiskLocation RL ON PC.RiskLocationAKID=RL.RiskLocationAKID and RL.CurrentSnapshotFlag=1 
		INNER JOIN V2.policy P on P.pol_ak_id = RL.Policyakid and P.crrnt_snpsht_flag = 1
		left outer join DBO.CoverageDetailCommercialProperty CDCP on CDCP.PremiumTransactionID=PT.PremiumTransactionID and CDCP.CurrentSnapshotFlag=1 
		left outer join DBO.CoverageDetailGeneralLiability CDGL on CDGL.PremiumTransactionID=PT.PremiumTransactionID and CDGL.CurrentSnapshotFlag=1
		WHERE PT.sourcesystemid = 'DCT' AND RC.sourcesystemid = 'DCT'
		AND p.pol_ak_id IN  (SELECT distinct LMC.pol_ak_id from DBO.loss_master_calculation LMC WHERE 
		(DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,LMC.loss_master_run_date),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1)))
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingCoverageAKID ORDER BY PremiumTransactionID) = 1
),
LKP_TargetTable_Loss AS (
	SELECT
	EDWLossMasterCalculationPKId
	FROM (
		SELECT
		GL.EDWLossMasterCalculationPKId as EDWLossMasterCalculationPKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ISSGeneralLiabilityExtract GL
		WHERE DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GL.LossMasterRunDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1)
		AND GL.EDWLossMasterCalculationPKId<>-1
		
		UNION ALL
		
		SELECT
		PL.EDWLossMasterCalculationPKId as EDWLossMasterCalculationPKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ISSProfessionalLiabilityExtract PL
		WHERE DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,PL.LossMasterRunDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1)
		AND PL.EDWLossMasterCalculationPKId<>-1
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWLossMasterCalculationPKId ORDER BY EDWLossMasterCalculationPKId) = 1
),
EXP_Calculate_ClaimNumber AS (
	SELECT
	AGG_RemoveDuplicate.pol_key,
	AGG_RemoveDuplicate.claim_coverage_detail_ak_id,
	AGG_RemoveDuplicate.loss_master_run_date,
	-- *INF*: TO_CHAR(loss_master_run_date, 'YYYYMMDD')
	TO_CHAR(loss_master_run_date, 'YYYYMMDD') AS loss_master_run_datekey,
	AGG_RemoveDuplicate.loss_master_calculation_id,
	AGG_RemoveDuplicate.prim_bus_class_code,
	AGG_RemoveDuplicate.StateProvinceCode,
	AGG_RemoveDuplicate.claim_loss_date,
	AGG_RemoveDuplicate.sub_line_code,
	AGG_RemoveDuplicate.class_code,
	AGG_RemoveDuplicate.cause_of_loss,
	AGG_RemoveDuplicate.RiskTerritory,
	AGG_RemoveDuplicate.pol_eff_date,
	AGG_RemoveDuplicate.claim_occurrence_num,
	AGG_RemoveDuplicate.claimant_num,
	AGG_RemoveDuplicate.paid_loss_amt,
	AGG_RemoveDuplicate.outstanding_amt,
	AGG_RemoveDuplicate.pms_type_bureau_code,
	AGG_RemoveDuplicate.RiskUnitGroup,
	AGG_RemoveDuplicate.PolicySourceID,
	AGG_RemoveDuplicate.RiskType,
	AGG_RemoveDuplicate.CoverageAKID,
	AGG_RemoveDuplicate.pol_exp_date,
	AGG_RemoveDuplicate.s3p_claim_num,
	AGG_RemoveDuplicate.claim_trans_id,
	LKP_TargetTable_Loss.EDWLossMasterCalculationPKId,
	AGG_RemoveDuplicate.asl_num,
	AGG_RemoveDuplicate.InsuranceLine,
	AGG_RemoveDuplicate.pol_num,
	AGG_RemoveDuplicate.statistical_code1,
	AGG_RemoveDuplicate.InsuranceSegmentCode,
	AGG_RemoveDuplicate.CoverageCode,
	AGG_RemoveDuplicate.DirectALAEOutstandingER,
	AGG_RemoveDuplicate.DirectALAEPaidIR,
	AGG_RemoveDuplicate.MajorPerilCode,
	AGG_RemoveDuplicate.ZipPostalCode,
	-- *INF*: IIF(ISNULL(claim_occurrence_num) OR IS_SPACES(claim_occurrence_num) OR LENGTH(claim_occurrence_num)=0 OR claim_occurrence_num='N/A' OR IS_NUMBER(claim_occurrence_num)=0, 0, TO_INTEGER(claim_occurrence_num))
	IFF(
	    claim_occurrence_num IS NULL
	    or LENGTH(claim_occurrence_num)>0
	    and TRIM(claim_occurrence_num)=''
	    or LENGTH(claim_occurrence_num) = 0
	    or claim_occurrence_num = 'N/A'
	    or REGEXP_LIKE(claim_occurrence_num, '^[0-9]+$') = 0,
	    0,
	    CAST(claim_occurrence_num AS INTEGER)
	) AS v_claim_occurrence_num,
	-- *INF*: LTRIM(RTRIM(pol_num))||TO_CHAR(claim_loss_date,'YYMMDD') ||SUBSTR(claim_occurrence_num,2,2)
	LTRIM(RTRIM(pol_num)) || TO_CHAR(claim_loss_date, 'YYMMDD') || SUBSTR(claim_occurrence_num, 2, 2) AS o_ClaimNum,
	AGG_RemoveDuplicate.pms_pol_lob_code,
	AGG_RemoveDuplicate.PackageModificationAdjustmentGroupCode,
	AGG_RemoveDuplicate.ConstructionCode,
	AGG_RemoveDuplicate.IsoFireProtectionCode,
	AGG_RemoveDuplicate.BureauCode1,
	AGG_RemoveDuplicate.BureauCode2,
	AGG_RemoveDuplicate.BureauCode4,
	AGG_RemoveDuplicate.LiabilityFormCode,
	AGG_RemoveDuplicate.RetroactiveDate,
	AGG_RemoveDuplicate.trans_date,
	AGG_RemoveDuplicate.SublineCode,
	AGG_RemoveDuplicate.CoverageType,
	AGG_RemoveDuplicate.CoverageVersion,
	AGG_RemoveDuplicate.PremiumTransactionID,
	LKP_PremiumTransactionAttibutes.PremiumTransactionID AS lkp_PremiumTransactionID,
	-- *INF*: IIF(IN(PolicySourceID,'DCT','DUC'),lkp_PremiumTransactionID, PremiumTransactionID)
	IFF(PolicySourceID IN ('DCT','DUC'), lkp_PremiumTransactionID, PremiumTransactionID) AS o_PremiumTransactionID,
	LKP_PremiumTransactionAttibutes.PackageModificationAdjustmentGroupCode AS lkp_PackageModificationAdjustmentGroupCode,
	-- *INF*: IIF(IN(PolicySourceID,'DCT','DUC'),lkp_PackageModificationAdjustmentGroupCode, PackageModificationAdjustmentGroupCode)
	IFF(
	    PolicySourceID IN ('DCT','DUC'), lkp_PackageModificationAdjustmentGroupCode,
	    PackageModificationAdjustmentGroupCode
	) AS o_PackageModificationAdjustmentGroupCode,
	LKP_PremiumTransactionAttibutes.LiabilityFormCode AS lkp_LiabilityFormCode,
	-- *INF*: IIF(IN(PolicySourceID,'DCT','DUC'), lkp_LiabilityFormCode, LiabilityFormCode)
	IFF(PolicySourceID IN ('DCT','DUC'), lkp_LiabilityFormCode, LiabilityFormCode) AS o_LiabilityFormCode,
	LKP_PremiumTransactionAttibutes.RetroactiveDate AS lkp_RetroactiveDate_DCT,
	-- *INF*: IIF(IN(PolicySourceID,'DCT','DUC'),lkp_RetroactiveDate_DCT, RetroactiveDate)
	IFF(PolicySourceID IN ('DCT','DUC'), lkp_RetroactiveDate_DCT, RetroactiveDate) AS o_RetroactiveDate
	FROM AGG_RemoveDuplicate
	LEFT JOIN LKP_PremiumTransactionAttibutes
	ON LKP_PremiumTransactionAttibutes.RatingCoverageAKID = AGG_RemoveDuplicate.CoverageAKID
	LEFT JOIN LKP_TargetTable_Loss
	ON LKP_TargetTable_Loss.EDWLossMasterCalculationPKId = AGG_RemoveDuplicate.loss_master_calculation_id
),
LKP_RetroactiveDate_Loss AS (
	SELECT
	RetroactiveDate,
	i_PremiumTransactionID,
	PremiumTransactionID
	FROM (
		select t.PremiumTransactionID as PremiumTransactionID
		, t.RetroactiveDate as RetroactiveDate 
		from
		(
		select PremiumTransactionID, RetroactiveDate  from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto
		Union all
		 select PremiumTransactionID, RetroactiveDate from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailGeneralLiability 
		union all
		select PremiumTransactionID, RetroactiveDate  from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialProperty
		union all
		select  PremiumTransactionID, RetroactiveDate  from  @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialUmbrella 
		) T
		inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT 
		on T.PremiumTransactionID = PT.PremiumTransactionID
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY RetroactiveDate) = 1
),
EXP_Default AS (
	SELECT
	EXP_Calculate_ClaimNumber.pol_key,
	EXP_Calculate_ClaimNumber.claim_coverage_detail_ak_id,
	EXP_Calculate_ClaimNumber.loss_master_run_date,
	EXP_Calculate_ClaimNumber.loss_master_run_datekey,
	EXP_Calculate_ClaimNumber.loss_master_calculation_id,
	EXP_Calculate_ClaimNumber.prim_bus_class_code,
	EXP_Calculate_ClaimNumber.StateProvinceCode,
	EXP_Calculate_ClaimNumber.claim_loss_date,
	EXP_Calculate_ClaimNumber.sub_line_code,
	EXP_Calculate_ClaimNumber.class_code,
	EXP_Calculate_ClaimNumber.cause_of_loss,
	EXP_Calculate_ClaimNumber.RiskTerritory,
	EXP_Calculate_ClaimNumber.pol_eff_date,
	EXP_Calculate_ClaimNumber.claim_occurrence_num,
	EXP_Calculate_ClaimNumber.claimant_num,
	EXP_Calculate_ClaimNumber.paid_loss_amt,
	EXP_Calculate_ClaimNumber.outstanding_amt,
	EXP_Calculate_ClaimNumber.pms_type_bureau_code,
	EXP_Calculate_ClaimNumber.RiskUnitGroup,
	EXP_Calculate_ClaimNumber.PolicySourceID,
	EXP_Calculate_ClaimNumber.RiskType,
	EXP_Calculate_ClaimNumber.CoverageAKID,
	EXP_Calculate_ClaimNumber.pol_exp_date,
	EXP_Calculate_ClaimNumber.s3p_claim_num,
	EXP_Calculate_ClaimNumber.claim_trans_id,
	EXP_Calculate_ClaimNumber.EDWLossMasterCalculationPKId,
	EXP_Calculate_ClaimNumber.asl_num,
	EXP_Calculate_ClaimNumber.InsuranceLine,
	EXP_Calculate_ClaimNumber.pol_num,
	EXP_Calculate_ClaimNumber.statistical_code1,
	EXP_Calculate_ClaimNumber.InsuranceSegmentCode,
	EXP_Calculate_ClaimNumber.CoverageCode,
	EXP_Calculate_ClaimNumber.DirectALAEOutstandingER,
	EXP_Calculate_ClaimNumber.DirectALAEPaidIR,
	EXP_Calculate_ClaimNumber.MajorPerilCode,
	EXP_Calculate_ClaimNumber.ZipPostalCode,
	EXP_Calculate_ClaimNumber.o_ClaimNum,
	EXP_Calculate_ClaimNumber.pms_pol_lob_code,
	EXP_Calculate_ClaimNumber.o_PackageModificationAdjustmentGroupCode AS PackageModificationAdjustmentGroupCode,
	EXP_Calculate_ClaimNumber.ConstructionCode,
	EXP_Calculate_ClaimNumber.IsoFireProtectionCode,
	EXP_Calculate_ClaimNumber.BureauCode1,
	EXP_Calculate_ClaimNumber.BureauCode2,
	EXP_Calculate_ClaimNumber.BureauCode4,
	EXP_Calculate_ClaimNumber.o_LiabilityFormCode AS LiabilityFormCode,
	EXP_Calculate_ClaimNumber.o_RetroactiveDate AS RetroactiveDate,
	LKP_RetroactiveDate_Loss.RetroactiveDate AS lkp_RetroactiveDate,
	-- *INF*: IIF(sub_line_code='345',:LKP.LKP_LOSS_RETROACTIVEDATE_POLICY(LTRIM(RTRIM(pol_key))),lkp_RetroactiveDate)
	IFF(
	    sub_line_code = '345', LKP_LOSS_RETROACTIVEDATE_POLICY_LTRIM_RTRIM_pol_key.RetroactiveDate,
	    lkp_RetroactiveDate
	) AS v_RetroactiveDate,
	-- *INF*: IIF(ISNULL(v_RetroactiveDate),TO_DATE('18000101000000' , 'YYYYMMDDHH24MISS') , v_RetroactiveDate)
	IFF(
	    v_RetroactiveDate IS NULL, TO_TIMESTAMP('18000101000000', 'YYYYMMDDHH24MISS'),
	    v_RetroactiveDate
	) AS o_RetroactiveDate,
	EXP_Calculate_ClaimNumber.trans_date,
	EXP_Calculate_ClaimNumber.SublineCode,
	EXP_Calculate_ClaimNumber.CoverageType,
	EXP_Calculate_ClaimNumber.CoverageVersion
	FROM EXP_Calculate_ClaimNumber
	LEFT JOIN LKP_RetroactiveDate_Loss
	ON LKP_RetroactiveDate_Loss.PremiumTransactionID = EXP_Calculate_ClaimNumber.o_PremiumTransactionID
	LEFT JOIN LKP_LOSS_RETROACTIVEDATE_POLICY LKP_LOSS_RETROACTIVEDATE_POLICY_LTRIM_RTRIM_pol_key
	ON LKP_LOSS_RETROACTIVEDATE_POLICY_LTRIM_RTRIM_pol_key.PolicyKey = LTRIM(RTRIM(pol_key))

),
SRT_Sort_data AS (
	SELECT
	pol_key, 
	o_ClaimNum AS ClaimNum, 
	loss_master_run_date, 
	loss_master_run_datekey, 
	claim_coverage_detail_ak_id, 
	loss_master_calculation_id, 
	prim_bus_class_code, 
	StateProvinceCode, 
	claim_loss_date, 
	sub_line_code, 
	class_code, 
	cause_of_loss, 
	RiskTerritory, 
	pol_eff_date, 
	claim_occurrence_num, 
	claimant_num, 
	paid_loss_amt, 
	outstanding_amt, 
	pms_type_bureau_code, 
	RiskUnitGroup, 
	PolicySourceID, 
	RiskType, 
	CoverageAKID, 
	pol_exp_date, 
	s3p_claim_num, 
	claim_trans_id, 
	EDWLossMasterCalculationPKId, 
	asl_num, 
	InsuranceLine, 
	pol_num, 
	statistical_code1, 
	InsuranceSegmentCode, 
	CoverageCode, 
	DirectALAEOutstandingER, 
	DirectALAEPaidIR, 
	MajorPerilCode, 
	ZipPostalCode, 
	pms_pol_lob_code, 
	PackageModificationAdjustmentGroupCode, 
	ConstructionCode, 
	IsoFireProtectionCode, 
	BureauCode1, 
	BureauCode2, 
	BureauCode4, 
	LiabilityFormCode, 
	o_RetroactiveDate AS RetroactiveDate, 
	trans_date, 
	SublineCode, 
	CoverageType, 
	CoverageVersion
	FROM EXP_Default
	ORDER BY pol_key ASC, ClaimNum ASC, loss_master_run_date ASC, claim_coverage_detail_ak_id ASC
),
LKP_InceptionToDatePaidLossAmount AS (
	SELECT
	InceptionToDatePaidLossAmount,
	pol_key,
	edw_claimant_cov_det_ak_id,
	trans_date,
	loss_master_calculation_id
	FROM (
		SELECT DISTINCT
		CASE WHEN InceptionToDatePaidLossAmount=0 and PaidLossAmount=0 THEN SUM(PaidLossAmount) OVER (partition by pol_key,edw_claimant_cov_det_ak_id,claim_num order by loss_master_run_date)
		ELSE InceptionToDatePaidLossAmount END AS InceptionToDatePaidLossAmount,
		pol_key AS pol_key,
		edw_claimant_cov_det_ak_id AS edw_claimant_cov_det_ak_id,
		trans_date AS trans_date
		,loss_master_calculation_id AS loss_master_calculation_id
		FROM
		(
		SELECT DISTINCT SUM(InceptionToDatePaidLossAmount) over (partition by pol_key,edw_claimant_cov_det_ak_id,claim_num order by edw_claimant_cov_det_ak_id,trans_date,claim_trans_pk_id) AS InceptionToDatePaidLossAmount,
		edw_claimant_cov_det_ak_id AS edw_claimant_cov_det_ak_id,
		trans_date AS trans_date,
		InceptionToDatePaidLossAmount AS PaidLossAmount,
		pol_key
		,loss_master_calculation_id
		,loss_master_run_date
		,claim_num
		FROM (
		SELECT  f.direct_loss_paid_including_recoveries AS InceptionToDatePaidLossAmount,  
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
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.vw_claim_transaction ct
		on f.edw_claim_trans_pk_id=ct.claim_trans_id
		and ct.trans_date<'2001-01-01'
		join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim p
		on f.pol_dim_id=p.pol_dim_id
		join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim O 
		on F.claim_occurrence_dim_id=o.claim_occurrence_dim_id
		left join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.vwLossMasterFact lmf
		on lmf.claimant_cov_dim_id = d.claimant_cov_dim_id
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation lmc
		on lmc.loss_master_calculation_id = lmf.edw_loss_master_calculation_pk_id
		UNION ALL
		SELECT f.DirectLossPaidIR AS InceptionToDatePaidLossAmount,  
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
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.vw_claim_transaction ct
		on f.edw_claim_trans_pk_id=ct.claim_trans_id
		and ct.trans_date>='2001-01-01'
		join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim p
		on f.pol_dim_id=p.pol_dim_id
		join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim O 
		on F.claim_occurrence_dim_id=o.claim_occurrence_dim_id
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation lmc
		on lmc.loss_master_calculation_id = f.edw_loss_master_calculation_pk_id
		) T
		) T
		WHERE cast(trans_date as date)<=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS} ,-1) 
		ORDER BY pol_key,edw_claimant_cov_det_ak_id,trans_date
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key,edw_claimant_cov_det_ak_id,trans_date,loss_master_calculation_id ORDER BY InceptionToDatePaidLossAmount DESC) = 1
),
FIL_Exists_Loss AS (
	SELECT
	SRT_Sort_data.EDWLossMasterCalculationPKId AS LKP_LossMasterCalculationId, 
	SRT_Sort_data.pol_key, 
	SRT_Sort_data.ClaimNum, 
	SRT_Sort_data.loss_master_run_date, 
	SRT_Sort_data.loss_master_calculation_id, 
	SRT_Sort_data.prim_bus_class_code, 
	SRT_Sort_data.StateProvinceCode, 
	SRT_Sort_data.claim_loss_date, 
	SRT_Sort_data.sub_line_code, 
	SRT_Sort_data.class_code AS ClassCode, 
	SRT_Sort_data.cause_of_loss, 
	SRT_Sort_data.RiskTerritory, 
	SRT_Sort_data.pol_eff_date, 
	SRT_Sort_data.claim_occurrence_num, 
	SRT_Sort_data.claimant_num, 
	SRT_Sort_data.paid_loss_amt, 
	SRT_Sort_data.outstanding_amt, 
	SRT_Sort_data.pms_type_bureau_code AS TypeBureauCode, 
	SRT_Sort_data.RiskUnitGroup, 
	SRT_Sort_data.PolicySourceID, 
	SRT_Sort_data.RiskType, 
	SRT_Sort_data.CoverageAKID, 
	SRT_Sort_data.pol_exp_date, 
	SRT_Sort_data.s3p_claim_num, 
	SRT_Sort_data.claim_trans_id, 
	SRT_Sort_data.claim_coverage_detail_ak_id, 
	LKP_InceptionToDatePaidLossAmount.InceptionToDatePaidLossAmount AS out_CumulativeInceptiontoDatePaidLoss, 
	SRT_Sort_data.asl_num, 
	SRT_Sort_data.InsuranceLine, 
	SRT_Sort_data.pol_num, 
	SRT_Sort_data.statistical_code1, 
	SRT_Sort_data.InsuranceSegmentCode, 
	SRT_Sort_data.CoverageCode, 
	SRT_Sort_data.DirectALAEOutstandingER, 
	SRT_Sort_data.DirectALAEPaidIR, 
	SRT_Sort_data.MajorPerilCode, 
	SRT_Sort_data.ZipPostalCode, 
	SRT_Sort_data.pms_pol_lob_code, 
	SRT_Sort_data.PackageModificationAdjustmentGroupCode, 
	SRT_Sort_data.ConstructionCode, 
	SRT_Sort_data.IsoFireProtectionCode, 
	SRT_Sort_data.BureauCode1, 
	SRT_Sort_data.BureauCode2, 
	SRT_Sort_data.BureauCode4, 
	SRT_Sort_data.LiabilityFormCode, 
	SRT_Sort_data.RetroactiveDate, 
	SRT_Sort_data.SublineCode, 
	SRT_Sort_data.CoverageType, 
	SRT_Sort_data.CoverageVersion
	FROM SRT_Sort_data
	LEFT JOIN LKP_InceptionToDatePaidLossAmount
	ON LKP_InceptionToDatePaidLossAmount.pol_key = SRT_Sort_data.pol_key AND LKP_InceptionToDatePaidLossAmount.edw_claimant_cov_det_ak_id = SRT_Sort_data.claim_coverage_detail_ak_id AND LKP_InceptionToDatePaidLossAmount.trans_date <= SRT_Sort_data.trans_date AND LKP_InceptionToDatePaidLossAmount.loss_master_calculation_id = SRT_Sort_data.loss_master_calculation_id
	WHERE ISNULL(LKP_LossMasterCalculationId) AND  
(paid_loss_amt != 0 or outstanding_amt!=0 or DirectALAEPaidIR!=0  or DirectALAEOutstandingER !=0)
and TO_CHAR(loss_master_run_date, 'YYYY') ||TO_CHAR(loss_master_run_date, 'QQ')=
TO_CHAR( ADD_TO_DATE(sysdate, 'MM', 3*@{pipeline().parameters.NO_OF_QUARTERS}), 'YYYY') ||TO_CHAR( ADD_TO_DATE(sysdate, 'MM', 3*@{pipeline().parameters.NO_OF_QUARTERS}), 'QQ')
),
EXP_Cleansing_Loss AS (
	SELECT
	pol_key AS i_pol_key,
	ClaimNum AS i_ClaimNum,
	loss_master_run_date AS i_loss_master_run_date,
	loss_master_calculation_id AS i_loss_master_calculation_id,
	prim_bus_class_code AS i_prim_bus_class_code,
	StateProvinceCode AS i_StateProvinceCode,
	claim_loss_date AS i_claim_loss_date,
	sub_line_code AS i_sub_line_code,
	ClassCode AS i_ClassCode,
	cause_of_loss AS i_cause_of_loss,
	RiskTerritory AS i_RiskTerritory,
	pol_eff_date AS i_pol_eff_date,
	claim_occurrence_num AS i_claim_occurrence_num,
	claimant_num AS i_claimant_num,
	paid_loss_amt AS i_paid_loss_amt,
	outstanding_amt AS i_outstanding_amt,
	TypeBureauCode AS i_TypeBureauCode,
	RiskUnitGroup AS i_RiskUnitGroup,
	PolicySourceID AS i_PolicySourceID,
	RiskType AS i_RiskType,
	CoverageAKID AS i_CoverageAKID,
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
	i_CoverageAKID AS o_StatisticalCoverageAKID,
	i_pol_exp_date AS o_pol_exp_date,
	-- *INF*: RTRIM(LTRIM(s3p_claim_num))
	RTRIM(LTRIM(s3p_claim_num)) AS o_s3p_claim_num,
	i_CumulativeInceptiontoDatePaidLoss AS o_CumulativeInceptiontoDatePaidLoss,
	i_asl_num AS o_AnnualStatementLineNumber,
	InsuranceLine AS i_InsuranceLine,
	-- *INF*: LTRIM(RTRIM(i_InsuranceLine))
	LTRIM(RTRIM(i_InsuranceLine)) AS o_InsuranceLine,
	pol_num,
	i_ClaimNum AS o_ClaimNum,
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
	InsuranceSegmentCode,
	DirectALAEPaidIR AS direct_alae_paid_including_recoveries1,
	DirectALAEOutstandingER AS direct_alae_outstanding_excluding_recoveries,
	CoverageCode,
	RetroactiveDate,
	LiabilityFormCode,
	ZipPostalCode AS i_ZipPostalCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ZipPostalCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_ZipPostalCode) AS o_ZipPostalCode,
	MajorPerilCode,
	pms_pol_lob_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(pms_pol_lob_code)
	UDF_DEFAULT_VALUE_FOR_STRINGS(pms_pol_lob_code) AS v_pms_pol_lob_code,
	-- *INF*: IIF(v_pms_pol_lob_code  != 'N/A' , v_pms_pol_lob_code  , 
	-- decode(i_InsuranceLine, 'BusinessOwners' , 'BOP',
	-- 'CommercialAuto' , 'CPP',
	-- 'CommercialUmbrella' , 'CPP',
	-- 'Crime' , 'CPP',
	-- 'DirectorsAndOfficersNFP' , 'CPP',
	-- 'DirectorsAndOffsCondos' , 'CPP',
	-- 'EmploymentPracticesLiab' , 'CPP',
	-- 'ExcessLiability' , 'CPP',
	-- 'GamesOfChance' , 'CPP',
	-- 'GeneralLiability' , 'CPP',
	-- 'HoleInOne' , 'CPP',
	-- 'InlandMarine' , 'CPP',
	-- 'Property' , 'CPP',
	-- 'SBOPGeneralLiability' , 'CPP',
	-- 'SBOPProperty' , 'CPP',
	-- i_InsuranceLine))
	IFF(
	    v_pms_pol_lob_code != 'N/A', v_pms_pol_lob_code,
	    decode(
	        i_InsuranceLine,
	        'BusinessOwners', 'BOP',
	        'CommercialAuto', 'CPP',
	        'CommercialUmbrella', 'CPP',
	        'Crime', 'CPP',
	        'DirectorsAndOfficersNFP', 'CPP',
	        'DirectorsAndOffsCondos', 'CPP',
	        'EmploymentPracticesLiab', 'CPP',
	        'ExcessLiability', 'CPP',
	        'GamesOfChance', 'CPP',
	        'GeneralLiability', 'CPP',
	        'HoleInOne', 'CPP',
	        'InlandMarine', 'CPP',
	        'Property', 'CPP',
	        'SBOPGeneralLiability', 'CPP',
	        'SBOPProperty', 'CPP',
	        i_InsuranceLine
	    )
	) AS o_pms_pol_lob_code,
	i_CoverageAKID AS o_RatingCoverageAKID
	FROM FIL_Exists_Loss
),
EXP_Transform_Prior_to_lookup AS (
	SELECT
	PackageModificationAdjustmentGroupCode,
	PolicySourceID AS in_PolicySourceID,
	-- *INF*: IIF(in_PolicySourceID='DUC','DCT','PMS')
	-- 
	-- 
	-- --IIF(in_StatisticalCoverageAKID=-1,'DCT','PMS')
	IFF(in_PolicySourceID = 'DUC', 'DCT', 'PMS') AS out_SourceSystem
	FROM FIL_Exists_Loss
),
EXP_Reset_Pms_ConstCode_IsoPPC AS (
	SELECT
	EXP_Cleansing_Loss.o_TypeBureauCode AS i_TypeBureauCode,
	EXP_Cleansing_Loss.o_pms_const_code AS i_pms_const_code,
	EXP_Cleansing_Loss.o_pms_iso_ppc_code AS i_pms_iso_ppc_code,
	EXP_Transform_Prior_to_lookup.out_SourceSystem AS i_SourceSystem,
	FIL_Exists_Loss.ConstructionCode AS lkp_ConstructionCode,
	FIL_Exists_Loss.IsoFireProtectionCode AS lkp_IsoFireProtectionCode,
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
	 -- Manually join with FIL_Exists_Loss
),
LKP_CauseOfLoss AS (
	SELECT
	BureauCauseOfLoss,
	CauseOfLoss,
	LineOfBusiness,
	MajorPeril
	FROM (
		SELECT 
			BureauCauseOfLoss,
			CauseOfLoss,
			LineOfBusiness,
			MajorPeril
		FROM sup_CauseOfLoss
		WHERE CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CauseOfLoss,LineOfBusiness,MajorPeril ORDER BY BureauCauseOfLoss) = 1
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
	-- *INF*: IIF(ISNULL(ClassCode) OR IS_SPACES(ClassCode) OR LENGTH(ClassCode)=0
	-- OR IN(ClassCode, 'N/A','TBD'),
	-- '44444',
	-- ClassCode
	-- )
	-- 
	-- --ClassCode
	IFF(
	    ClassCode IS NULL
	    or LENGTH(ClassCode)>0
	    and TRIM(ClassCode)=''
	    or LENGTH(ClassCode) = 0
	    or ClassCode IN ('N/A','TBD'),
	    '44444',
	    ClassCode
	) AS LossMasterClassCode_out,
	LKP_CauseOfLoss.BureauCauseOfLoss AS cause_of_loss,
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
	-- *INF*: DECODE(TRUE,
	-- i_InsuranceLine='CF',
	-- IIF(TRUNC(pol_eff_date, 'MM')  <= TO_DATE('2003-10', 'YYYY-MM'), TO_CHAR(ADD_TO_DATE(claim_loss_date, 'DD', v_claim_occurrence_num), 'YYYYMMDD'), i_s3p_claim_num),
	-- i_InsuranceLine='CR',
	-- i_pol_num || TO_CHAR(claim_loss_date,'YYMMDD') || SUBSTR(i_claim_occurrence_num,2,2),
	-- --i_InsuranceLine='GL',
	-- i_s3p_claim_num
	-- )
	DECODE(
	    TRUE,
	    i_InsuranceLine = 'CF', IFF(
	        CAST(TRUNC(pol_eff_date, 'MONTH') AS TIMESTAMP_NTZ(0)) <= TO_TIMESTAMP('2003-10', 'YYYY-MM'),
	        TO_CHAR(DATEADD(DAY,v_claim_occurrence_num,claim_loss_date), 'YYYYMMDD'),
	        i_s3p_claim_num
	    ),
	    i_InsuranceLine = 'CR', i_pol_num || TO_CHAR(claim_loss_date, 'YYMMDD') || SUBSTR(i_claim_occurrence_num, 2, 2),
	    i_s3p_claim_num
	) AS v_ClaimNumber,
	EXP_Cleansing_Loss.o_ClaimNum AS i_ClaimNum,
	i_ClaimNum AS ClaimNum,
	0.00 AS PremiumMasterPremium,
	EXP_Cleansing_Loss.o_PaidLossAmount AS PaidLossAmount,
	EXP_Cleansing_Loss.o_OutstandingLossAmount AS OutstandingLossAmount,
	EXP_Cleansing_Loss.o_TypeBureauCode AS TypeBureauCode,
	EXP_Cleansing_Loss.o_RiskUnitGroup AS RiskUnitGroup,
	EXP_Cleansing_Loss.o_PolicySourceID AS PolicySourceID,
	EXP_Cleansing_Loss.o_RiskType AS RiskType,
	FIL_Exists_Loss.BureauCode1,
	FIL_Exists_Loss.BureauCode2,
	FIL_Exists_Loss.BureauCode4,
	EXP_Reset_Pms_ConstCode_IsoPPC.o_ConsturctionCode AS i_ConstructionCode,
	EXP_Reset_Pms_ConstCode_IsoPPC.o_IsoFireProtectionCode AS i_IsoFireProtectionCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ConstructionCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_ConstructionCode) AS ConstructionCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_IsoFireProtectionCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_IsoFireProtectionCode) AS IsoFireProtectionCode,
	EXP_Transform_Prior_to_lookup.PackageModificationAdjustmentGroupCode AS i_PackageModificationAdjustmentGroupCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_PackageModificationAdjustmentGroupCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_PackageModificationAdjustmentGroupCode) AS PackageModificationAdjustmentGroupCode,
	EXP_Cleansing_Loss.o_pol_exp_date AS pol_exp_date,
	EXP_Cleansing_Loss.o_CumulativeInceptiontoDatePaidLoss AS CumulativeInceptiontoDatePaidLoss,
	EXP_Cleansing_Loss.o_AnnualStatementLineNumber AS AnnualStatementLineNumber,
	EXP_Cleansing_Loss.InsuranceSegmentCode,
	EXP_Cleansing_Loss.RetroactiveDate AS i_RetroactiveDate,
	FIL_Exists_Loss.SublineCode AS i_SublineCode,
	FIL_Exists_Loss.CoverageType AS i_CoverageType,
	FIL_Exists_Loss.CoverageVersion AS i_CoverageVersion,
	EXP_Cleansing_Loss.LiabilityFormCode AS i_LiabilityFormCode,
	-- *INF*: in_CoverageCode
	-- 
	-- --DECODE(TRUE,PolicySourceID='PMS',IIF(ISNULL(BureauCode1) OR LENGTH(BureauCode1)=0 OR(TypeBureauCode  != 'CR' and TypeBureauCode  !='BT' and --TypeBureauCode != 'FT' and TypeBureauCode != 'CF'),'N/A',BureauCode1) ,DECODE(TRUE,RiskType='BLDG','01',RiskType='PP','02','N/A'))
	-- 
	-- 
	-- --IIF(PolicySourceID='PMS',iif(isnull(lkp_BureauCode1),'N/A',lkp_BureauCode1),DECODE(TRUE,RiskType='BLDG','01',RiskType='PP','02','N/A'))
	in_CoverageCode AS CoverageCode,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(i_LiabilityFormCode))='6','3',
	-- (i_InsuranceLine='GeneralLiability' AND i_CoverageType='WB516GL') OR (i_InsuranceLine='CommercialAuto' AND IN(i_CoverageType,'WB516CA','WB516CANC')) OR i_CoverageType='EmploymentPracticesLiability' OR in_CoverageCode = 'EPLI' OR i_InsuranceLine='DirectorsAndOffsCondos'
	-- ,DECODE(TRUE,
	-- TO_CHAR(GET_DATE_PART(i_RetroactiveDate, 'YYYY'))<>'1800','1',
	-- TO_CHAR(GET_DATE_PART(i_RetroactiveDate, 'YYYY'))='1800','4',
	-- i_LiabilityFormCode
	-- ),
	-- i_LiabilityFormCode)
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(i_LiabilityFormCode)) = '6', '3',
	    (i_InsuranceLine = 'GeneralLiability' AND i_CoverageType = 'WB516GL') OR (i_InsuranceLine = 'CommercialAuto' AND i_CoverageType IN ('WB516CA','WB516CANC')) OR i_CoverageType = 'EmploymentPracticesLiability' OR in_CoverageCode = 'EPLI' OR i_InsuranceLine = 'DirectorsAndOffsCondos', DECODE(
	        TRUE,
	        TO_CHAR(DATE_PART(i_RetroactiveDate, 'YYYY')) <> '1800', '1',
	        TO_CHAR(DATE_PART(i_RetroactiveDate, 'YYYY')) = '1800', '4',
	        i_LiabilityFormCode
	    ),
	    i_LiabilityFormCode
	) AS v_TypeofPolicycontract,
	-- *INF*: IIF(ISNULL(i_RetroactiveDate) OR TO_CHAR(GET_DATE_PART(i_RetroactiveDate, 'YYYY'))='1800' OR IN(ltrim(rtrim(v_TypeofPolicycontract)),'3','4','5'), 'N/A', TO_CHAR(GET_DATE_PART(i_RetroactiveDate, 'YYYY')))
	IFF(
	    i_RetroactiveDate IS NULL
	    or TO_CHAR(DATE_PART(i_RetroactiveDate, 'YYYY')) = '1800'
	    or ltrim(rtrim(v_TypeofPolicycontract)) IN ('3','4','5'),
	    'N/A',
	    TO_CHAR(DATE_PART(i_RetroactiveDate, 'YYYY'))
	) AS v_ClaimsEntryYear,
	-- *INF*: v_ClaimsEntryYear
	-- 
	-- --'N/A'
	v_ClaimsEntryYear AS o_ClaimsEntryYear,
	EXP_Cleansing_Loss.direct_alae_paid_including_recoveries1 AS direct_alae_paid_including_recoveries,
	EXP_Cleansing_Loss.direct_alae_outstanding_excluding_recoveries,
	EXP_Cleansing_Loss.CoverageCode AS in_CoverageCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_TypeofPolicycontract)
	UDF_DEFAULT_VALUE_FOR_STRINGS(v_TypeofPolicycontract) AS o_TypeofPolicycontract,
	EXP_Cleansing_Loss.o_ZipPostalCode AS ZipPostalCode,
	EXP_Cleansing_Loss.o_RatingCoverageAKID AS RatingCoverageAKID,
	'N/A' AS LocationUnitNumber
	FROM EXP_Cleansing_Loss
	 -- Manually join with EXP_Reset_Pms_ConstCode_IsoPPC
	 -- Manually join with EXP_Transform_Prior_to_lookup
	 -- Manually join with FIL_Exists_Loss
	LEFT JOIN LKP_CauseOfLoss
	ON LKP_CauseOfLoss.CauseOfLoss = EXP_Cleansing_Loss.o_cause_of_loss AND LKP_CauseOfLoss.LineOfBusiness = EXP_Cleansing_Loss.o_pms_pol_lob_code AND LKP_CauseOfLoss.MajorPeril = EXP_Cleansing_Loss.MajorPerilCode
),
Union AS (
	SELECT LossMasterCalculationId, PremiumMasterCalculationID, PremiumMasterRunDate, loss_master_run_date, pol_key, prim_bus_class_code, StateProvinceCode, sub_line_code_out AS sub_line_code, PremiumMasterClassCode, LossMasterClassCode, Cause_of_Loss, TerritoryCode, pol_eff_date, ClaimNum, ClaimantNum, PremiumMasterPremium_out AS PremiumMasterPremium, PaidLossAmt, OutstandingAmt, TypeBureauCode, RiskUnitGroup, SourceSystemID, RiskType, CoverageCode, ConstructionCode_out AS ConstructionCode, IsoFireProtectionCode_out AS IsoFireProtectionCode, PackageModificationAdjustmentGroupCode_out AS PackageModificationAdjustmentGroupCode, pol_exp_date AS PolicyExpirationDate, AnnualStatementLineNumber, BureauCode1, BureauCode2, BureauCode4, InsuranceSegmentCode, o_PremiumMasterExposure AS PremiumMasterExposure, claim_loss_date, o_TypeofPolicycontract AS TypeofPolicycontract, ClaimsEntryYear, PaidAllocatedlossAdjustmentExpenseAmount, OutstandingAllocatedLossAdjustmentExpenseAmount1 AS OutstandingAllocatedLossAdjustmentExpenseAmount, ZipPostalCode, PremiumTransactionEffectiveDate, RatingCoverageAKID, o_LocationUnitNumber AS LocationUnitNumber
	FROM EXP_Exposure
	-- Manually join with EXP_Logic_Premium
	UNION
	SELECT loss_master_calculation_id AS LossMasterCalculationId, PremiumMasterCalculationID, PremiumMasterRunDate, loss_master_run_date, pol_key, prim_bus_class_code, StateProvinceCode, sub_line_code_out AS sub_line_code, PremiumMasterClassCode_out AS PremiumMasterClassCode, LossMasterClassCode_out AS LossMasterClassCode, cause_of_loss_out AS Cause_of_Loss, TerritoryCode_out AS TerritoryCode, pol_eff_date, ClaimNum, claimant_num AS ClaimantNum, PremiumMasterPremium, PaidLossAmount AS PaidLossAmt, OutstandingLossAmount AS OutstandingAmt, TypeBureauCode, RiskUnitGroup, PolicySourceID AS SourceSystemID, RiskType, CoverageCode, ConstructionCode, IsoFireProtectionCode, PackageModificationAdjustmentGroupCode, pol_exp_date AS PolicyExpirationDate, CumulativeInceptiontoDatePaidLoss AS InceptionToDatePaidLossAmount, claim_coverage_detail_ak_id AS ClaimCoverageID, AnnualStatementLineNumber, BureauCode1, BureauCode2, BureauCode4, claim_loss_date, o_TypeofPolicycontract AS TypeofPolicycontract, o_ClaimsEntryYear AS ClaimsEntryYear, direct_alae_paid_including_recoveries AS PaidAllocatedlossAdjustmentExpenseAmount, direct_alae_outstanding_excluding_recoveries AS OutstandingAllocatedLossAdjustmentExpenseAmount, ZipPostalCode, RatingCoverageAKID, LocationUnitNumber
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
	    i_IsoFireProtectionCode
	) AS v_IsoFireProtectionCode,
	v_ConstructionCode AS o_ConstructionCode,
	v_IsoFireProtectionCode AS o_IsoFireProtectionCode
	FROM Union
),
LKP_RatingCoverage AS (
	SELECT
	RatingCoverageAKID,
	SublineCode,
	i_RatingCoverageAKID
	FROM (
		SELECT distinct RatingCoverage.SublineCode as SublineCode, RatingCoverage.RatingCoverageAKID as RatingCoverageAKID FROM RatingCoverage
		where Sublinecode not in ('N/A', '0')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingCoverageAKID ORDER BY RatingCoverageAKID) = 1
),
LKP_RiskTerritory AS (
	SELECT
	LocationUnitNumber,
	RiskTerritory,
	PolicyKey
	FROM (
		SELECT DISTINCT RL.LocationUnitNumber as LocationUnitNumber,
		RL.RiskTerritory as RiskTerritory,
		POL.pol_key as PolicyKey 
		FROM dbo.RiskLocation RL 
		inner join V2.policy POL  on POL.pol_ak_id=RL.PolicyAKID and POL.crrnt_snpsht_flag=1 and RL.CurrentSnapshotFlag=1
		inner join dbo.PolicyCoverage PC on RL.RiskLocationAKID = PC.RiskLocationAKID and PC.CurrentSnapshotFlag=1  
		WHERE RL.LocationUnitNumber not in ( '0000', '000') and RL.RiskTerritory <> 'N/A'
		AND (PC.TypeBureauCode = 'GL' or PC.InsuranceLine = 'GeneralLiability')
		ORDER BY PolicyKey , LocationUnitNumber
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY LocationUnitNumber) = 1
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
	Union.sub_line_code AS i_sub_line_code,
	Union.PremiumMasterClassCode AS i_PremiumMasterClassCode,
	Union.LossMasterClassCode AS i_LossMasterClassCode,
	Union.Cause_of_Loss AS i_Cause_of_Loss,
	LKP_RiskTerritory.LocationUnitNumber AS i_LocationUnitNumber,
	Union.TerritoryCode AS i_src_TerritoryCode,
	LKP_RiskTerritory.RiskTerritory AS i_TerritoryCode,
	Union.pol_eff_date AS i_pol_eff_date,
	Union.ClaimNum AS i_ClaimNum,
	Union.ClaimantNum AS i_ClaimantNum,
	Union.PremiumMasterPremium AS i_PremiumMasterPremium,
	Union.PaidLossAmt AS i_PaidLossAmt,
	Union.OutstandingAmt AS i_OutstandingAmt,
	Union.TypeBureauCode AS i_TypeBureauCode,
	Union.SourceSystemID AS i_SourceSystemID,
	Union.RiskType AS i_RiskType,
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
	Union.InsuranceSegmentCode AS i_InsuranceSegmentCode,
	Union.PremiumMasterExposure AS i_PremiumMasterExposure,
	Union.claim_loss_date AS i_claim_loss_date,
	Union.TypeofPolicycontract AS i_TypeofPolicycontract,
	Union.ClaimsEntryYear AS i_ClaimsEntryYear,
	Union.PaidAllocatedlossAdjustmentExpenseAmount AS i_PaidAllocatedlossAdjustmentExpenseAmount,
	Union.OutstandingAllocatedLossAdjustmentExpenseAmount AS i_OutstandingAllocatedLossAdjustmentExpenseAmount,
	Union.ZipPostalCode AS i_ZipPostalCode,
	Union.PremiumTransactionEffectiveDate AS i_PremiumTransactionEffectiveDate,
	LKP_RatingCoverage.SublineCode AS i_SublineCode_lkp,
	Union.LocationUnitNumber AS i_LocationNumber,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	SYSDATE AS o_CreatedDate,
	i_PremiumMasterCalculationID AS o_PremiumMasterCalculationID,
	i_LossMasterCalculationId AS o_LossMasterCalculationId,
	i_TypeBureauCode AS o_TypeBureauCode,
	-- *INF*: -- Setting ProfessionalLiability to 27 and GeneralLiability to 26
	-- IIF(v_SublineCode='317', '27', '26')
	IFF(v_SublineCode = '317', '27', '26') AS o_BureauLineOfInsurance,
	'0731' AS o_BureauCompanyNumber,
	i_StateProvinceCode AS o_StateProvinceCode,
	i_PremiumMasterRunDate AS o_PremiumMasterRunDate,
	i_LossMasterRunDate AS o_LossMasterRunDate,
	i_pol_key AS o_pol_key,
	i_PremiumMasterClassCode AS o_PremiumMasterClassCode,
	i_LossMasterClassCode AS o_LossMasterClassCode,
	i_ClaimNum AS o_ClaimNum,
	i_ClaimantNum AS o_ClaimantNum,
	-- *INF*: IIF(length(i_pol_key)=12,RTRIM(LTRIM(i_TerritoryCode)),i_src_TerritoryCode)
	-- -- if PMS then use the lookup, else use the Duck value from the SQ
	IFF(length(i_pol_key) = 12, RTRIM(LTRIM(i_TerritoryCode)), i_src_TerritoryCode) AS o_RiskTerritoryCode,
	i_pol_eff_date AS o_PolicyEffectiveDate,
	-- *INF*: IIF(LTRIM(RTRIM(i_sub_line_code))='N/A',
	-- :UDF.DEFAULT_VALUE_FOR_STRINGS(i_SublineCode_lkp),
	-- LTRIM(RTRIM(i_sub_line_code)))
	IFF(
	    LTRIM(RTRIM(i_sub_line_code)) = 'N/A', UDF_DEFAULT_VALUE_FOR_STRINGS(i_SublineCode_lkp),
	    LTRIM(RTRIM(i_sub_line_code))
	) AS v_SublineCode,
	-- *INF*: IIF(in(i_sub_line_code ,'332','334','335') and in (i_Cause_of_Loss,'11','12'),'16',
	-- IIF(in(i_sub_line_code ,'332','334','335') and in (i_Cause_of_Loss,'21','22'),'26',
	-- IIF(in(i_sub_line_code ,'325','345'),'90', 
	-- IIF(i_sub_line_code ='336' and in (i_Cause_of_Loss,'20','16'),'12',
	-- IIF(i_sub_line_code ='336' and i_Cause_of_Loss = '26','22',
	-- i_Cause_of_Loss)))))
	-- --i_Cause_of_Loss
	-- --DECODE(TRUE,IN(i_Cause_of_Loss,'11','21','31','41','51','61','71','81','91'),'01',
	-- --IN(i_Cause_of_Loss,'12','22','32','42','52','62','72','82','92','97'),'02',
	-- --IN(i_Cause_of_Loss,'05','15','25','35','45','55','65','75','85','95'),'03',
	-- --IN(i_Cause_of_Loss,'14','24','34','44','54','64','74','84','94'),'04',
	-- --IN(i_Cause_of_Loss,'08','18','28','38','48','58','68','88','98'),'05',
	-- --IN(i_Cause_of_Loss,'16','26','36','46','56','66','76','86','96'),'06',
	-- --IN(i_Cause_of_Loss,'17','27','37','47','57','67','87'),'07',
	-- --IN(i_Cause_of_Loss,'03','13','23','33','43','53','63','73','83','93'),'08',
	-- --IN(i_Cause_of_Loss,'19','29','39','49','59','69','77','79','89','99'),'09',i_Cause_of_Loss)
	IFF(
	    i_sub_line_code IN ('332','334','335') and i_Cause_of_Loss IN ('11','12'), '16',
	    IFF(
	        i_sub_line_code IN ('332','334','335')
	    and i_Cause_of_Loss IN ('21','22'), '26',
	        IFF(
	            i_sub_line_code IN ('325','345'), '90',
	            IFF(
	                i_sub_line_code = '336'
	            and i_Cause_of_Loss IN ('20','16'), '12',
	                IFF(
	                    i_sub_line_code = '336'
	                and i_Cause_of_Loss = '26', '22',
	                    i_Cause_of_Loss
	                )
	            )
	        )
	    )
	) AS v_CauseOfLoss_LOSS,
	-- *INF*: --PROD-7566 Added output port to pass 'N/A' values for premium records
	-- IIF(i_PremiumMasterCalculationID<>-1,'N/A',v_CauseOfLoss_LOSS)
	IFF(i_PremiumMasterCalculationID <> - 1, 'N/A', v_CauseOfLoss_LOSS) AS o_CauseOfLoss,
	v_SublineCode AS o_SublineCode,
	i_CoverageCode AS o_CoverageCode,
	-- *INF*: 'N/A'
	-- --i_ConstructionCode
	'N/A' AS o_ConstructionCode,
	-- *INF*: 'N/A'
	-- --i_IsoFireProtectionCode
	'N/A' AS o_ISOFireProtectionCode,
	-- *INF*: 'N/A'
	-- --DECODE(TRUE,
	-- --i_TypeBureauCode='Property',
	-- --DECODE(TRUE,
	-- --i_RiskType='BLDG','01',
	-- --i_RiskType='PP','02',
	-- --'N/A'
	-- --),
	-- 
	-- --i_TypeBureauCode='Crime',
	-- --DECODE(TRUE,
	-- --i_RiskType='ClientsProperty','200',
	-- --i_RiskType='ClientsProperty_ETF','400',
	-- --i_RiskType='ComputerFraud','200',
	-- --i_RiskType='ComputerFraud_G','300',
	-- --i_RiskType='EmployeeTheft','200',
	-- --i_RiskType='EmployeeTheft_ETF','400',
	-- --i_RiskType='EmployeeTheftNameOrPosition','200',
	-- --i_RiskType='EmployeeTheftNameOrPosition_ETF','400',
	-- --i_RiskType='EmployeeTheftNameOrPosition_G','300',
	-- --i_RiskType='EmployeeTheftNameOrPosition_GETF','400',
	-- --i_RiskType='EmployeeTheftPerEmployee','300',
	-- --i_RiskType='EmployeeTheftPerEmployee_GETF','400',
	-- --i_RiskType='EmployeeTheftPerLoss','300',
	-- --i_RiskType='EmployeeTheftPerLoss_GETF','400',
	-- --i_RiskType='ForgeryAndAlteration','200',
	-- --i_RiskType='ForgeryAndAlteration_ETF','400',
	-- --i_RiskType='ForgeryAndAlteration_G','300',
	-- --i_RiskType='ForgeryAndAlteration_GETF','400',
	-- --i_RiskType='FundsTransfer','200',
	-- --i_RiskType='FundsTransfer_G','300',
	-- --i_RiskType='GuestPropertyInsidePremises','200',
	-- --i_RiskType='GuestPropertySafeDeposit','200',
	-- --i_RiskType='InsideRobbery','200',
	-- --i_RiskType='InsideRobbery_G','300',
	-- --i_RiskType='InsideRobberyOther','200',
	-- --i_RiskType='InsideRobberyOther_G','300',
	-- --i_RiskType='InsideRobberySecurities','200',
	-- --i_RiskType='InsideRobberySecurities_G','300',
	-- --i_RiskType='InsideTheftMoney','200',
	-- --i_RiskType='InsideTheftMoney_G','300',
	-- --i_RiskType='InsideTheftProperty','200',
	-- --i_RiskType='InsideTheftProperty_G','300',
	-- --i_RiskType='MoneyOrders','200',
	-- --i_RiskType='OutsidePremises','200',
	-- --i_RiskType='OutsidePremises_G','300',
	-- --'N/A'
	-- --),
	-- --'N/A')
	'N/A' AS v_PolicyForm_DCT,
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
	-- *INF*: 'N/A'
	-- --:UDF.DEFAULT_VALUE_FOR_STRINGS(v_PolicyForm)
	'N/A' AS o_PolicyForm,
	i_PremiumMasterPremium AS o_PremiumMasterDirectWrittenPremiumAmount,
	i_PaidLossAmt AS o_PaidLossAmount,
	i_OutstandingAmt AS o_OutstandingLossAmount,
	i_PolicyExpirationDate AS o_PolicyExpirationDate,
	-- *INF*: IIF(ISNULL(i_InceptionToDatePaidLossAmount), 0, i_InceptionToDatePaidLossAmount)
	-- 
	IFF(i_InceptionToDatePaidLossAmount IS NULL, 0, i_InceptionToDatePaidLossAmount) AS v_InceptionToDatePaidLossAmount,
	-- *INF*: DECODE(True,
	-- i_PaidAllocatedlossAdjustmentExpenseAmount<>0, 0,
	-- i_OutstandingAllocatedLossAdjustmentExpenseAmount<>0,0,
	-- v_InceptionToDatePaidLossAmount
	-- )
	DECODE(
	    True,
	    i_PaidAllocatedlossAdjustmentExpenseAmount <> 0, 0,
	    i_OutstandingAllocatedLossAdjustmentExpenseAmount <> 0, 0,
	    v_InceptionToDatePaidLossAmount
	) AS o_InceptionToDatePaidLossAmount,
	-- *INF*: IIF(ISNULL(i_ClaimCoverageID), -1, i_ClaimCoverageID)
	IFF(i_ClaimCoverageID IS NULL, - 1, i_ClaimCoverageID) AS o_ClaimCoverageID,
	i_AnnualStatementLineNumber AS o_AnnualStatementLineNumber,
	-- *INF*: IIF(ISNULL(i_PremiumMasterExposure), 0,i_PremiumMasterExposure )
	-- 
	IFF(i_PremiumMasterExposure IS NULL, 0, i_PremiumMasterExposure) AS v_PremiumMasterExposure,
	-- *INF*: IIF(v_PremiumMasterExposure > 0 AND i_PremiumMasterPremium < 0, (v_PremiumMasterExposure * -1), v_PremiumMasterExposure)
	IFF(
	    v_PremiumMasterExposure > 0 AND i_PremiumMasterPremium < 0, (v_PremiumMasterExposure * - 1),
	    v_PremiumMasterExposure
	) AS o_PremiumMasterExposure,
	-- *INF*: IIF(ISNULL(i_OutstandingAllocatedLossAdjustmentExpenseAmount), 0, i_OutstandingAllocatedLossAdjustmentExpenseAmount)
	IFF(
	    i_OutstandingAllocatedLossAdjustmentExpenseAmount IS NULL, 0,
	    i_OutstandingAllocatedLossAdjustmentExpenseAmount
	) AS o_OutstandingAllocatedLossAdjustmentExpenseAmount,
	i_claim_loss_date AS o_claim_loss_date,
	i_TypeofPolicycontract AS o_TypeofPolicycontract,
	i_ClaimsEntryYear AS o_ClaimsEntryYear,
	i_PaidAllocatedlossAdjustmentExpenseAmount AS o_PaidAllocatedlossAdjustmentExpenseAmount,
	i_ZipPostalCode AS o_ZipPostalCode,
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
	UDF_DEFAULT_VALUE_FOR_STRINGS(v_Occupancy_Desc) AS PackageModificationAdjustmentGroupDescription,
	-- *INF*: ltrim(rtrim(i_LocationNumber))
	ltrim(rtrim(i_LocationNumber)) AS o_LocationNumber
	FROM EXP_ConstCode_IsoPC_Rules
	 -- Manually join with Union
	LEFT JOIN LKP_RatingCoverage
	ON LKP_RatingCoverage.RatingCoverageAKID = Union.RatingCoverageAKID
	LEFT JOIN LKP_RiskTerritory
	ON LKP_RiskTerritory.PolicyKey = Union.pol_key
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
FIL_GL_PL AS (
	SELECT
	o_AuditID AS AuditID, 
	o_CreatedDate AS CreatedDate, 
	o_PremiumMasterCalculationID AS PremiumMasterCalculationID, 
	o_LossMasterCalculationId AS LossMasterCalculationId, 
	o_TypeBureauCode AS TypeBureauCode, 
	o_BureauLineOfInsurance AS BureauLineOfInsurance, 
	o_BureauCompanyNumber AS BureauCompanyNumber, 
	o_StateProvinceCode AS StateProvinceCode, 
	o_PremiumMasterRunDate AS PremiumMasterRunDate, 
	o_LossMasterRunDate AS LossMasterRunDate, 
	o_pol_key AS pol_key, 
	o_PremiumMasterClassCode AS PremiumMasterClassCode, 
	o_LossMasterClassCode AS LossMasterClassCode, 
	o_ClaimNum AS ClaimNum, 
	o_ClaimantNum AS ClaimantNum, 
	o_RiskTerritoryCode AS RiskTerritoryCode, 
	o_PolicyEffectiveDate AS PolicyEffectiveDate, 
	o_CauseOfLoss AS CauseOfLoss, 
	o_SublineCode AS SublineCode, 
	o_CoverageCode AS CoverageCode, 
	o_ConstructionCode AS ConstructionCode, 
	o_ISOFireProtectionCode AS ISOFireProtectionCode, 
	PackageModificationAdjustmentGroupDescription, 
	o_PolicyForm AS PolicyForm, 
	o_PremiumMasterDirectWrittenPremiumAmount AS PremiumMasterDirectWrittenPremiumAmount, 
	o_PaidLossAmount AS PaidLossAmount, 
	o_OutstandingLossAmount AS OutstandingLossAmount, 
	o_PolicyExpirationDate AS PolicyExpirationDate, 
	o_InceptionToDatePaidLossAmount AS InceptionToDatePaidLossAmount, 
	o_ClaimCoverageID AS ClaimCoverageID, 
	o_AnnualStatementLineNumber AS AnnualStatementLineNumber, 
	o_PremiumMasterExposure AS PremiumMasterExposure, 
	o_OutstandingAllocatedLossAdjustmentExpenseAmount AS OutstandingAllocatedLossAdjustmentExpenseAmount, 
	o_claim_loss_date AS claim_loss_date, 
	o_TypeofPolicycontract AS TypeofPolicycontract, 
	o_ClaimsEntryYear AS ClaimsEntryYear, 
	o_PaidAllocatedlossAdjustmentExpenseAmount AS PaidAllocatedlossAdjustmentExpenseAmount, 
	o_ZipPostalCode AS ZipPostalCode, 
	o_PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate, 
	o_LocationNumber
	FROM EXP_Values
	WHERE SUBSTR(pol_key,1,2) <>'HH'
),
RTR_GL_PL AS (
	SELECT
	AuditID,
	CreatedDate,
	PremiumMasterCalculationID,
	LossMasterCalculationId,
	TypeBureauCode,
	BureauLineOfInsurance,
	BureauCompanyNumber,
	StateProvinceCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	pol_key,
	PremiumMasterClassCode,
	LossMasterClassCode,
	ClaimNum,
	ClaimantNum,
	RiskTerritoryCode,
	PolicyEffectiveDate,
	CauseOfLoss,
	SublineCode,
	CoverageCode,
	ConstructionCode,
	ISOFireProtectionCode,
	PackageModificationAdjustmentGroupDescription,
	PolicyForm,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimCoverageID,
	AnnualStatementLineNumber,
	PremiumMasterExposure,
	OutstandingAllocatedLossAdjustmentExpenseAmount,
	claim_loss_date,
	TypeofPolicycontract,
	ClaimsEntryYear,
	PaidAllocatedlossAdjustmentExpenseAmount,
	ZipPostalCode,
	PremiumTransactionEffectiveDate,
	o_LocationNumber AS LocationNumber
	FROM FIL_GL_PL
),
RTR_GL_PL_PL AS (SELECT * FROM RTR_GL_PL WHERE SublineCode='317'),
RTR_GL_PL_DEFAULT1 AS (SELECT * FROM RTR_GL_PL WHERE NOT ( (SublineCode='317') )),
ISSGeneralLiabilityExtract AS (

	------------ PRE SQL ----------
	@{pipeline().parameters.DELETE_PRESQL_GL}
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ISSGeneralLiabilityExtract
	(AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, SublineCode, PackageModificationAdjustmentGroupDescription, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, WrittenExposure, OutstandingAllocatedLossAdjustmentExpenseAmount, Claimlossdate, TypeofPolicycontract, ClaimsEntryYear, PaidAllocatedlossAdjustmentExpenseAmount, ZipPostalCode, TransactionEffectiveDate, LocationNumber)
	SELECT 
	AuditID AS AUDITID, 
	CREATEDDATE, 
	PremiumMasterCalculationID AS EDWPREMIUMMASTERCALCULATIONPKID, 
	LossMasterCalculationId AS EDWLOSSMASTERCALCULATIONPKID, 
	TYPEBUREAUCODE, 
	BUREAULINEOFINSURANCE, 
	BUREAUCOMPANYNUMBER, 
	STATEPROVINCECODE, 
	PREMIUMMASTERRUNDATE, 
	LOSSMASTERRUNDATE, 
	pol_key AS POLICYKEY, 
	PREMIUMMASTERCLASSCODE, 
	LOSSMASTERCLASSCODE, 
	ClaimNum AS CLAIMNUMBER, 
	ClaimantNum AS CLAIMANTNUMBER, 
	RISKTERRITORYCODE, 
	POLICYEFFECTIVEDATE, 
	CAUSEOFLOSS, 
	SUBLINECODE, 
	PACKAGEMODIFICATIONADJUSTMENTGROUPDESCRIPTION, 
	PREMIUMMASTERDIRECTWRITTENPREMIUMAMOUNT, 
	PAIDLOSSAMOUNT, 
	OUTSTANDINGLOSSAMOUNT, 
	POLICYEXPIRATIONDATE, 
	INCEPTIONTODATEPAIDLOSSAMOUNT, 
	ClaimCoverageID AS CLAIMANTCOVERAGEDETAILID, 
	ANNUALSTATEMENTLINENUMBER, 
	PremiumMasterExposure AS WRITTENEXPOSURE, 
	OUTSTANDINGALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	claim_loss_date AS CLAIMLOSSDATE, 
	TYPEOFPOLICYCONTRACT, 
	CLAIMSENTRYYEAR, 
	PAIDALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	ZIPPOSTALCODE, 
	PremiumTransactionEffectiveDate AS TRANSACTIONEFFECTIVEDATE, 
	LOCATIONNUMBER
	FROM RTR_GL_PL_DEFAULT1
),
ISSProfessionalLiabilityExtract AS (

	------------ PRE SQL ----------
	@{pipeline().parameters.DELETE_PRESQL_PL}
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ISSProfessionalLiabilityExtract
	(AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, SublineCode, PackageModificationAdjustmentGroupDescription, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, WrittenExposure, OutstandingAllocatedLossAdjustmentExpenseAmount, Claimlossdate, TypeofPolicycontract, ClaimsEntryYear, PaidAllocatedlossAdjustmentExpenseAmount, ZipPostalCode, TransactionEffectiveDate, LocationNumber)
	SELECT 
	AuditID AS AUDITID, 
	CREATEDDATE, 
	PremiumMasterCalculationID AS EDWPREMIUMMASTERCALCULATIONPKID, 
	LossMasterCalculationId AS EDWLOSSMASTERCALCULATIONPKID, 
	TYPEBUREAUCODE, 
	BUREAULINEOFINSURANCE, 
	BUREAUCOMPANYNUMBER, 
	STATEPROVINCECODE, 
	PREMIUMMASTERRUNDATE, 
	LOSSMASTERRUNDATE, 
	pol_key AS POLICYKEY, 
	PREMIUMMASTERCLASSCODE, 
	LOSSMASTERCLASSCODE, 
	ClaimNum AS CLAIMNUMBER, 
	ClaimantNum AS CLAIMANTNUMBER, 
	RISKTERRITORYCODE, 
	POLICYEFFECTIVEDATE, 
	CAUSEOFLOSS, 
	SUBLINECODE, 
	PACKAGEMODIFICATIONADJUSTMENTGROUPDESCRIPTION, 
	PREMIUMMASTERDIRECTWRITTENPREMIUMAMOUNT, 
	PAIDLOSSAMOUNT, 
	OUTSTANDINGLOSSAMOUNT, 
	POLICYEXPIRATIONDATE, 
	INCEPTIONTODATEPAIDLOSSAMOUNT, 
	ClaimCoverageID AS CLAIMANTCOVERAGEDETAILID, 
	ANNUALSTATEMENTLINENUMBER, 
	PremiumMasterExposure AS WRITTENEXPOSURE, 
	OUTSTANDINGALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	claim_loss_date AS CLAIMLOSSDATE, 
	TYPEOFPOLICYCONTRACT, 
	CLAIMSENTRYYEAR, 
	PAIDALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	ZIPPOSTALCODE, 
	PremiumTransactionEffectiveDate AS TRANSACTIONEFFECTIVEDATE, 
	LOCATIONNUMBER
	FROM RTR_GL_PL_PL
),