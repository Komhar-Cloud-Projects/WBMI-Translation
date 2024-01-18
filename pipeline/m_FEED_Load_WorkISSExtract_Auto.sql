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
LKP_Policy AS (
	SELECT
	pol_key
	FROM (
		select p.pol_key as Pol_Key 
		from @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy p inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on p.pol_ak_id=pc.PolicyAKID
		and PC.sourcesystemid='DCT' and p.crrnt_snpsht_flag=1 and PC.CurrentSnapshotFlag=1
		where pc.InsuranceLine like 'SBOP%'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_key) = 1
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
		p.pol_ak_id=pc.policyakid and PC.sourcesystemid='DCT' and p.crrnt_snpsht_flag=1 and PC.CurrentSnapshotFlag=1
		and PC.InsuranceLine ='GeneralLiability') a  
		inner join
		(
		select   p.pol_key as pol_key , pc.policyakid  as PolicyAKID
		From @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy p inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		ON
		p.pol_ak_id=pc.policyakid
		and PC.sourcesystemid='DCT' and p.crrnt_snpsht_flag=1 and PC.CurrentSnapshotFlag=1
		and PC.InsuranceLine ='Property') b 
		ON  a.PolicyAKID=b.PolicyAKID
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_key) = 1
),
LKP_archGLDCOccpancyType AS (
	SELECT
	OccupancyType,
	PolicyNumber
	FROM (
		select distinct dcgl.OccupancyTypeMonoline as  OccupancyType, dr.PolicyNumber as PolicyNumber
		 from archDCGLOccupancyStaging dcgl inner join (Select distinct dp.PolicyNumber+PolicyVersionFormatted as PolicyNumber,  max(dr.GL_RiskId) as GL_RiskId,
		 max(dr.SessionId) as SessionId,dl.type
		 From  VWArchWorkDCTPolicy dp inner join   archDCLineStaging dl
		 on
		 dp.PolicyId=dl.PolicyId and
		  dp.SessionId=dl.SessionId 
		  -- and dp.AuditId=dl.AuditId 
		 inner join  archDCGLRiskStaging dr
		 on dr.LineId=dl.LineId and
		 dr.SessionId=dl.SessionId 
		 --and dr.AuditId=dl.AuditId
		 where 
		 dl.type='GeneralLiability'
		 group by
		 dp.PolicyNumber+PolicyVersionFormatted, dr.SessionId,dl.type
		 )  dr
		 on dcgl.GL_RiskId=dr.GL_RiskId and
		 dcgl.SessionId=dr.SessionId 
		 --and dcgl.AuditId=dr.AuditId
		 where dcgl.OccupancyTypeMonoline is not null
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber ORDER BY OccupancyType) = 1
),
LKP_archCFDCOccpancyType AS (
	SELECT
	OccupancyType,
	PolicyNumber
	FROM (
		select distinct 
		do.OccupancyType as OccupancyType, db.PolicyNumber as PolicyNumber
		From  ArchDCCFlocationStaging dloc inner join
		 (
		SELECT distinct dp.PolicyNumber+PolicyVersionFormatted as PolicyNumber,max(db.CFlocationid) as CFlocationid, max(db.Sessionid) as  sessionid, max(db.CFBuildingId) as CFBuildingId,dl.type as type
		From VWArchWorkDCTPolicy dp inner join 
		  archDCLineStaging dl
		on
		dp.PolicyId=dl.PolicyId 
		and dp.Sessionid=dl.Sessionid
		 inner join ArchDCCFBuildingStage db
		on dl.LineId=db.LineId
		and dl.Sessionid=db.Sessionid
		where  dl.Type in('CommercialAuto','CommercialUmbrella','Property') and db.description like 'Building #1%' 
		group by dp.PolicyNumber+PolicyVersionFormatted, dl.type
		) db
		on dloc.CF_locationid = db.CFlocationid AND  dloc.Sessionid=db.Sessionid and dloc.description='Primary Location'
		INNER JOIN archDCCFRiskStaging dr
		on dr.CF_BuildingId=db.CFBuildingId
		and dr.Sessionid=db.Sessionid
		inner join archDCCFOccupancyStaging do
		on do.CF_RiskId=dr.CF_RiskId 
		and do.Sessionid=dr.Sessionid  and do.OccupancyType is not null
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber ORDER BY OccupancyType) = 1
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
		and PC.sourcesystemid='DCT' and p.crrnt_snpsht_flag=1 and PC.CurrentSnapshotFlag=1
		and PC.InsuranceLine ='SBOPGeneralLiability') a  
		inner join
		(
		select   p.pol_key as pol_key , pc.policyakid  as PolicyAKID,P.ProgramAKId as ProgramAKId
		From @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy p inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		ON p.pol_ak_id=pc.policyakid
		and PC.sourcesystemid='DCT' and p.crrnt_snpsht_flag=1 and PC.CurrentSnapshotFlag=1
		and PC.InsuranceLine ='SBOPProperty') b 
		ON  a.PolicyAKID=b.PolicyAKID
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY ProgramAKId) = 1
),
LKP_Get_Parent_CoverageGuid AS (
	SELECT
	archWorkDCTCoverageTransactionId,
	CoverageGUID,
	PolicyKey,
	ExtractDate,
	in_pol_key,
	in_CoverageGUID
	FROM (
		SELECT
		WorkDCTCoverageTransaction.archWorkDCTCoverageTransactionId as archWorkDCTCoverageTransactionId ,
		WorkDCTCoverageTransaction.CoverageGUID as CoverageGUID,
		WorkDCTPolicy.PolicyNumber+ WorkDCTPolicy.PolicyVersionFormatted as PolicyKey,
		WorkDCTCoverageTransaction.ExtractDate as ExtractDate
		FROM 
		@{pipeline().parameters.STAGING_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.archWorkDCTCoverageTransaction WorkDCTCoverageTransaction
		
		inner join 
		( select distinct LocationAssociationId,CoverageId,LineId,SessionId From
		@{pipeline().parameters.STAGING_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.archWorkDCTTransactionInsuranceLineLocationBridge WorkDCTTransactionInsuranceLineLocationBridge
		where 
		DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,WorkDCTTransactionInsuranceLineLocationBridge.ExtractDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1) 
		) WorkDCTTransactionInsuranceLineLocationBridge
		on WorkDCTCoverageTransaction.CoverageId=WorkDCTTransactionInsuranceLineLocationBridge.CoverageId
		and
		WorkDCTCoverageTransaction.SessionId=WorkDCTTransactionInsuranceLineLocationBridge.SessionId
		
		inner join @{pipeline().parameters.STAGING_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.archWorkDCTLocation WorkDCTLocation
		on WorkDCTTransactionInsuranceLineLocationBridge.LocationAssociationId=WorkDCTLocation.LocationAssociationId
		and
		DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,WorkDCTLocation.ExtractDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1) 
		
		inner join 
		(select distinct LineId, SessionId,PolicyId from 
		@{pipeline().parameters.STAGING_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.archWorkDCTInsuranceLine WorkDCTInsuranceLine
		where LineType='CommercialAuto'
		and 
		DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,WorkDCTInsuranceLine.ExtractDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1) 
		) WorkDCTInsuranceLine
		on
		WorkDCTInsuranceLine.LineId=WorkDCTTransactionInsuranceLineLocationBridge.LineId
		
		inner join @{pipeline().parameters.STAGING_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.vwarchWorkDCTPolicy WorkDCTPolicy
		on WorkDCTInsuranceLine.PolicyId=WorkDCTPolicy.PolicyId 
		and WorkDCTPolicy.PolicyStatus<>'Quote'
		and WorkDCTPolicy.TransactionState='committed'
		and WorkDCTPolicy.TransactionType  NOT IN ('RescindNonRenew','Reporting','VoidReporting','Information','Dividend','RevisedDividend','VoidDividend','NonRenew','RescindCancelPending','CancelPending')
		and 
		DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,WorkDCTPolicy.ExtractDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1) 
		
		WHERE
		DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,WorkDCTCoverageTransaction.ExtractDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1) and WorkDCTCoverageTransaction.CoverageDeleteFlag!=1
		ORDER by WorkDCTCoverageTransaction.ExtractDate --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageGUID,PolicyKey ORDER BY archWorkDCTCoverageTransactionId DESC) = 1
),
LKP_Alt_CoverageGuid AS (
	SELECT
	CoverageGuid,
	archWorkDCTCoverageTransactionid,
	in_archWorkDCTCoverageTransactionId
	FROM (
		select
		Distinct first_value(B.Id) over(partition by B.Id, A.ExtractDate   order by A.ExtractDate desc) as CoverageGuid,
		C.archWorkDCTCoverageTransactionid as archWorkDCTCoverageTransactionid
		from 
		@{pipeline().parameters.STAGING_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.archWorkDCTCoverageTransaction C
		inner join
		@{pipeline().parameters.STAGING_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.archDCCoverageStaging A  
		on A.CoverageId=C.CoverageId and A.AuditId=C.Auditid and A.SessionId=C.sessionid
		and DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,A.ExtractDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1)
		inner join 
		@{pipeline().parameters.STAGING_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.archDCCoverageStaging B 
		on  A.ObjectId=B.CoverageId and A.SessionId=B.SessionId and A.AuditId=B.AuditId and B.objectid=C.ParentCoverageObjectId
		and DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,B.ExtractDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1)
		where
		DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,C.ExtractDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY archWorkDCTCoverageTransactionid ORDER BY CoverageGuid) = 1
),
LKP_CoverageDeductible_Premium_Alt_Combined_Output AS (
	SELECT
	CombinedCoverageDeductibleInfo,
	CoverageGUID,
	pol_key
	FROM (
		SELECT DISTINCT
		CD.CoverageDeductibleValue + '|' + CD.CoverageDeductibleType as CombinedCoverageDeductibleInfo,
		CD.CoverageDeductibleType AS CoverageDeductibleType,
		CD.CoverageDeductibleValue AS CoverageDeductibleValue,
		RC.CoverageGuid AS CoverageGUID,
		P.pol_key as pol_key
		FROM
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductible CD
		INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductibleBridge CDB
		ON CD.CoverageDeductibleId = CDB.CoverageDeductibleId
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		ON CDB.PremiumTransactionAKId = PT.PremiumTransactionAKID
		AND PT.SourceSystemID='DCT'
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
		ON PT.RatingCoverageAKId = RC.RatingCoverageAKID
		and PT.effectivedate=RC.effectivedate
		Inner Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC 
		ON PC.PolicyCoverageAKID=RC.PolicyCoverageAKID and PC.CurrentSnapshotFlag=1
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P 
		on P.pol_ak_id=PC.PolicyAKID and P.crrnt_snpsht_flag=1
		inner JOIN
		(
		select PolicyAKID as pol_ak_id from PremiumMasterCalculation where
		DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,PremiumMasterCalculation.PremiumMasterRunDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1)
		UNION
		select pol_ak_id as pol_ak_id from loss_master_calculation where
		DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,loss_master_calculation.loss_master_run_date),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1)
		) Calc on calc.pol_ak_id=P.pol_ak_id
		--WHERE CD.CoverageDeductibleType in ('CollisionDeductible','ComprehensiveDeductible')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageGUID,pol_key ORDER BY CombinedCoverageDeductibleInfo DESC) = 1
),
LKP_CoverageDeductible_Premium_Alt_LOSS_Output AS (
	SELECT
	CombinedCoverageDeductibleInfo,
	CoverageGUID,
	pol_key,
	EffectiveDate,
	ExpirationDate
	FROM (
		SELECT DISTINCT
		CD.CoverageDeductibleValue + '|' + CD.CoverageDeductibleType as CombinedCoverageDeductibleInfo,
		CD.CoverageDeductibleType AS CoverageDeductibleType,
		CD.CoverageDeductibleValue AS CoverageDeductibleValue,
		RC.CoverageGuid AS CoverageGUID,
		P.pol_key as pol_key,
		-- use lower of date, based loosely on how coveragedetaildim takes the lower value
		CASE 
		WHEN RC.RatingCoverageEffectiveDate < PT.PremiumTransactionEffectiveDate THEN RC.RatingCoverageEffectiveDate
		ELSE PT.PremiumTransactionEffectiveDate 
		END as  EffectiveDate, 
		CASE
		WHEN RC.RatingCoverageExpirationDate < PT.PremiumTransactionExpirationDate THEN RC.RatingCoverageExpirationDate
		ELSE PT.PremiumTransactionExpirationDate 
		END as ExpirationDate
		FROM
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductible CD
		INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductibleBridge CDB
		ON CD.CoverageDeductibleId = CDB.CoverageDeductibleId
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		ON CDB.PremiumTransactionAKId = PT.PremiumTransactionAKID
		AND PT.SourceSystemID='DCT'
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
		ON PT.RatingCoverageAKId = RC.RatingCoverageAKID
		and PT.effectivedate=RC.effectivedate
		Inner Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC 
		ON PC.PolicyCoverageAKID=RC.PolicyCoverageAKID and PC.CurrentSnapshotFlag=1
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P 
		on P.pol_ak_id=PC.PolicyAKID and P.crrnt_snpsht_flag=1
		INNER JOIN
		(
		select PolicyAKID as pol_ak_id from PremiumMasterCalculation where
		DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,PremiumMasterCalculation.PremiumMasterRunDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1)
		UNION
		select pol_ak_id as pol_ak_id from loss_master_calculation where
		DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,loss_master_calculation.loss_master_run_date),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1)
		) Calc on calc.pol_ak_id=P.pol_ak_id
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageGUID,pol_key,EffectiveDate,ExpirationDate ORDER BY CombinedCoverageDeductibleInfo DESC) = 1
),
LKP_GetAdditionalLimitValue AS (
	SELECT
	AdditionalLimit,
	in_PremiumTransactionAKID,
	PremiumTransactionAKId
	FROM (
		SELECT DISTINCT PIP.AdditionalLimit  as AdditionalLimit ,
		AWPT.PremiumTransactionAKId as PremiumTransactionAKId
		FROM
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.dbo.ArchWorkPremiumTransaction AWPT with (nolock)
		INNER JOIN
		@{pipeline().parameters.STAGING_DATABASE_NAME}.dbo.archWorkDCTCoverageTransaction ACT with (nolock) on AWPT.PremiumTransactionStageId=ACT.CoverageId
		INNER JOIN
		@{pipeline().parameters.STAGING_DATABASE_NAME}.dbo.archDCCACoveragePIPStage PIP with (nolock)
		ON ACT.CoverageId=PIP.CoverageId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId ORDER BY AdditionalLimit DESC) = 1
),
LKP_CoverageLimitOverride AS (
	SELECT
	CoverageLimitValue,
	PremiumTransactionAKID,
	CoverageLimitType
	FROM (
		select 
		CL.CoverageLimitValue as CoverageLimitValue, 
		CL.CoverageLimitType as CoverageLimitType,
		CLB.PremiumTransactionAKId as PremiumTransactionAKId 
		FROM
		CoverageLimitBridge CLB with (NOLOCK)
		 INNER JOIN 
		 CoverageLimit CL on CLB.CoverageLimitId=CL.CoverageLimitId
		 WHERE 
		CL.CoverageLimitType in ('PersonalInjuryProtectionBasicLimit','PersonalInjuryProtectionExcessLimit')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKID,CoverageLimitType ORDER BY CoverageLimitValue) = 1
),
LKP_CoverageDeductible_DCT AS (
	SELECT
	CoverageDeductibleValue,
	PremiumTransactionAKID
	FROM (
		SELECT DISTINCT CD.CoverageDeductibleValue AS CoverageDeductibleValue,
		PT.PremiumTransactionAKID AS PremiumTransactionAKID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductibleBridge CDB 
		INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductible cd on CDB.CoverageDeductibleId=cd.CoverageDeductibleId
		INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT ON PT.PremiumTransactionAKID = CDB.PremiumTransactionAKID
		INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC on PT.RatingCoverageAKID=RC.RatingCoverageAKID and RC.EffectiveDate=PT.EffectiveDate  
		INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID and PC.CurrentSnapshotFlag=1
		WHERE CDB.SourceSystemID = 'DCT' AND PT.SourceSystemID = 'DCT'
		AND PC.Insuranceline = 'CommercialAuto'
		AND CASE WHEN RC.CoverageType in ('OTC','Collision') AND CoverageDeductibleType in ('ComprehensiveDeductible','CollisionDeductible') THEN 1 
		         WHEN RC.CoverageType in ('OTC','Collision') AND CoverageDeductibleType NOT in ('ComprehensiveDeductible','CollisionDeductible') THEN 0
		         WHEN RC.CoverageType in ('CollisionBroadColl','CollisionLmtdColl') AND CoverageDeductibleType in ('ComprehensiveDeductible', 'CollisionDeductible', 'BroadCollisionStandard', 'LimitedCollisionStandard') THEN 1 
		 	WHEN RC.CoverageType in ('CollisionBroadColl','CollisionLmtdColl') AND CoverageDeductibleType NOT in ('ComprehensiveDeductible', 'CollisionDeductible', 'BroadCollisionStandard', 'LimitedCollisionStandard') THEN 0 
		    ELSE 1 END = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKID ORDER BY CoverageDeductibleValue DESC) = 1
),
SQ_Loss AS (
	DECLARE @Quarterstartdate as datetime, 
	        @Quarterenddate as datetime
	
	SET @Quarterstartdate = DATEADD(qq, DATEDIFF(qq, 0, GETDATE()) + @{pipeline().parameters.NO_OF_QUARTERS}, 0)             
	SET @Quarterenddate =  DATEADD(ss, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE()) +1 + @{pipeline().parameters.NO_OF_QUARTERS}, 0)) 
	
	-- this is used for DCT to get unique coverage deductible when there are multiple
	;
	with LMFIDList as
	(
	select 
	Distinct 
	RC.CoverageGUID,
	isnull(COV.CoverageDeductibleValue,'0') as CoverageDeductibleValue, 
	isnull(COV.CoverageDeductibleType,'N/A') as CoverageDeductibleType,
	LMC.PremiumTransactionAKID as PremiumTransactionAKID,
	POL.pol_key as PolicyKey,
	ISNULL(CDCA.VehicleNumber,'0')VehicleNumber,
	ISNULL(CDCA.IncludeUIM,'N/A') IncludeUIM,
	ISNULL(CDCA.CoordinationOfBenefits,'N/A') CoordinationOfBenefits,
	iif(ISNULL(CDCA.CoveredByWorkersCompensationFlag,0) = 1,'T','F') as CoveredByWorkersCompensationFlag,
	ISNULL(CDCA.MedicalExpensesOption,'N/A') MedicalExpensesOption,
	ISNULL(CDCA.SubjectToNoFault,'N/A') SubjectToNoFault,
	ISNULL(RC.CoverageType,'N/A') DCT_CoverageType
	,CDCA.AdditionalLimitKS
	,CDCA.AdditionalLimitKY
	,CDCA.AdditionalLimitMN
	,CDCA.FullGlassIndicator
	from 
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation LMC 
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_fact LMF
	  on LMC.loss_master_calculation_id=LMF.edw_loss_master_calculation_pk_id
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC 
	  on LMC.RatingCoverageAKId=RC.RatingCoverageAKID and RC.CurrentSnapshotFlag=1
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC 
	  on PC.PolicyCoverageAKID=LMC.PolicyCoverageAKID and PC.CurrentSnapshotFlag=1
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL with (nolock)
	 on PC.PolicyAKID=POL.pol_ak_id and POL.crrnt_snpsht_flag=1
	left JOIN  
	  ( select CDB.PremiumTransactionAKID, CD.CoverageDeductibleValue, CD.CoverageDeductibleType, ROW_NUMBER() over (Partition by   CDB.PremiumTransactionAKID order by CDB.CreatedDate DESC,CD.CoverageDeductibleValue  Desc)
	  as rn
	from
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductibleBridge CDB
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductible CD 
	  on CDB.CoverageDeductibleId=CD.CoverageDeductibleId
	  and  CD.CoverageDeductibleType in ('CollisionDeductible','ComprehensiveDeductible')
	) Cov 
	  on LMC.PremiumTransactionAKID=COV.PremiumTransactionAKID and Cov.rn=1
	
	--- Added this piece as part of VSTS-91092
	  left JOIN  
	  ( select CDCA.IncludeUIM,CDCA.VehicleNumber,PT.PremiumTransactionAKID,CoordinationOfBenefits,CoveredByWorkersCompensationFlag,MedicalExpensesOption,SubjectToNoFault, CDCA.AdditionalLimitKS, CDCA.AdditionalLimitKY, CDCA.AdditionalLimitMN ,CDCA.FullGlassIndicator
	from
	DBO.CoverageDetailCommercialAuto CDCA
	inner join DBO.PremiumTransaction PT 
	  on CDCA.PremiumTransactionID=PT.PremiumTransactionID
	  ) CDCA 
	  on LMC.PremiumTransactionAKID=CDCA.PremiumTransactionAKID
	--- Added this piece as part of VSTS-91092
	
	WHERE
	PC.InsuranceLine in ('CommercialAuto')
	AND RC.CoverageType<>'WB516CA' 
	AND LMC.trans_kind_code='D'
	AND LMC.loss_master_run_date between @Quarterstartdate and @Quarterenddate
	AND LMF.audit_id<>-9
	And (LMC.paid_loss_amt<>0 or LMC.outstanding_amt<>0 or LMF.eom_unpaid_loss_adjust_exp <>0 or LMF.paid_exp_amt<>0)
	@{pipeline().parameters.WHERE_CLAUSE_2} 
	)
	
	
	SELECT distinct 
	LMC.loss_master_calculation_id,
	LMC.loss_master_run_date,
	POL.prim_bus_class_code,
	RL.StateProvinceCode,
	OCC.claim_loss_date,
	LMC.sub_line_code,
	LMC.class_code  as class_code ,
	CT.cause_of_loss,
	RL.RiskTerritory,
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
	SC.StatisticalCoverageAKID,
	-1 as RatingCoverageAKID,
	POL.pol_exp_date,
	OCC.s3p_claim_num,
	CT.claim_trans_id,
	CCD.claimant_cov_det_ak_id,
	case when sub_asl_num IS NULL then asl_num
	when ltrim(rtrim(sub_asl_num))= 'N/A' then asl_num 
	else sub_asl_num 
	end as ASL_NUM,
	PC.InsuranceLine,
	POL.pol_num,
	LMC.statistical_code1,
	ISG.InsuranceSegmentCode,
	RL.ZipPostalCode,
	LMC.exposure,
	IRC.CoverageCode,
	(case when LMC.trans_kind_code = 'D' then  LMF.paid_exp_amt else 0 end) as DirectALAEPaidIR,
	(case when LMC.financialtypecode = 'E' and LMC.trans_kind_code = 'D' then LMF.eom_unpaid_loss_adjust_exp else 0 End) as DirectALAEOutstandingER,
	SC.MajorPerilCode,
	POL.pms_pol_lob_code,
	SC.CoverageGUID,
	IRC.CoverageGroupCode,
	CASE WHEN CT.trans_date < @Quarterstartdate THEN LMC.loss_master_run_date ELSE CT.trans_date END AS trans_date,
	POL.source_sys_id,
	ISNULL(CD.CoverageDeductibleValue,'0') as CoverageDeductibleValue,
	RIGHT (RTRIM(CoverageLimitValue) ,5)  CoverageLimitValue,
	FIRST_VALUE(cl.CoverageLimitType) OVER (partition by clb.PremiumTransactionAKID order by clb.CreatedDate desc, cl.CoverageLimitType desc) as CoverageLimitType,
	LMC.PremiumTransactionAKID,
	CCD.reserve_ctgry,
	'0' VehicleNumber,
	'N/A' IncludeUIM,
	'N/A' CoordinationOfBenefits,
	'F' CoveredByWorkersCompensationFlag,
	'N/A' MedicalExpensesOption,
	'N/A' SubjectToNoFault,
	'N/A' DCT_CoverageType
	,'-1' AdditionalLimitKS
	,'-1' AdditionalLimitKY
	,'-1' AdditionalLimitMN
	,'0' FullGlassIndicator
	FROM 
	@{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_fact LMF
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim ASL
	on LMF.asl_dim_id=ASL.asl_dim_id
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
	LEFT JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductibleBridge CDB
	ON CDB.PremiumTransactionAKId = LMC.PremiumTransactionAKID
	LEFT JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductible CD 
	ON CD.CoverageDeductibleId = CDB.CoverageDeductibleId
	AND CD.CoverageDeductibleType in ('CollisionDeductible','ComprehensiveDeductible')
	LEFT JOIN
	(@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge clb
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit cl
	on clb.CoverageLimitId=cl.CoverageLimitId
	)
	on LMC.PremiumTransactionAKID=clb.PremiumTransactionAKID
	and CoverageLimitValue <> 'BasicPIP'
	WHERE CCD.pms_type_bureau_code IN('AL','AN','AP') 
	AND LMC.trans_kind_code='D'
	AND LMC.loss_master_run_date between @Quarterstartdate and @Quarterenddate
	--AND RL.StateProvinceCode IN ('12','13','14','15','16','21','22','24','34','48') (line removed - RFC 126190)
	And (LMC.paid_loss_amt<>0 or LMC.outstanding_amt<>0 or LMF.eom_unpaid_loss_adjust_exp <>0 or LMF.paid_exp_amt<>0) 
	AND LMF.audit_id<>-9
	@{pipeline().parameters.WHERE_CLAUSE_2}
	
	--DCT
	union all
	
	SELECT distinct 
	LMC.loss_master_calculation_id,
	LMC.loss_master_run_date,
	POL.prim_bus_class_code,
	RL.StateProvinceCode,
	OCC.claim_loss_date,
	LMC.sub_line_code,
	LMC.class_code class_code,
	CT.cause_of_loss,
	RL.RiskTerritory,
	POL.pol_eff_date,
	POL.pol_key,
	OCC.claim_occurrence_num,
	CPO.claimant_num,
	(case when LMC.trans_kind_code = 'D' then  LMC.paid_loss_amt else 0 end) as paid_loss_amt,
	(case when LMC.financialtypecode = 'D' and LMC.trans_kind_code = 'D' Then LMC.outstanding_amt Else 0 End ) as outstanding_amt,
	CCD.pms_type_bureau_code,
	'N/A' AS RiskUnitGroup,
	CCD.PolicySourceID,
	LTRIM(RTRIM(RC.RiskType)) AS RiskType,
	-1 as StatisticalCoverageAKID,
	RC.RatingCoverageAKID,
	POL.pol_exp_date,
	OCC.s3p_claim_num,
	CT.claim_trans_id,
	CCD.claimant_cov_det_ak_id,
	case when sub_asl_num IS NULL then asl_num
	when ltrim(rtrim(sub_asl_num))= 'N/A' then asl_num 
	else sub_asl_num 
	end as ASL_NUM,
	PC.InsuranceLine,
	POL.pol_num,
	LMC.statistical_code1,
	ISG.InsuranceSegmentCode,
	RL.ZipPostalCode,
	LMC.exposure,
	IRC.CoverageCode,
	(case when LMC.trans_kind_code = 'D' then  LMF.paid_exp_amt else 0 end) as DirectALAEPaidIR,
	(case when LMC.financialtypecode = 'E' and LMC.trans_kind_code = 'D' then LMF.eom_unpaid_loss_adjust_exp else 0 End) as DirectALAEOutstandingER,
	CCD.major_peril_code MajorPerilCode,
	POL.pms_pol_lob_code,
	RC.CoverageGUID,
	IRC.CoverageGroupCode,
	CASE WHEN CT.trans_date< @Quarterstartdate THEN LMC.loss_master_run_date ELSE CT.trans_date END AS trans_date,
	POL.source_sys_id,
	ISNULL(CTE.CoverageDeductibleValue,'0') as CoverageDeductibleValue,
	RIGHT (RTRIM(FIRST_VALUE(cl.CoverageLimitValue) OVER (partition by clb.PremiumTransactionAKID order by clb.CreatedDate desc, cl.CoverageLimitValue desc)),5) as CoverageLimitValue,
	FIRST_VALUE(cl.CoverageLimitType) OVER (partition by clb.PremiumTransactionAKID order by clb.CreatedDate desc, cl.CoverageLimitType desc) as CoverageLimitType,
	LMC.PremiumTransactionAKID,
	CCD.reserve_ctgry,
	CTE.VehicleNumber,
	CTE.IncludeUIM,
	CTE.CoordinationOfBenefits,
	CTE.CoveredByWorkersCompensationFlag,
	CTE.MedicalExpensesOption,
	CTE.SubjectToNoFault,
	CTE.DCT_CoverageType
	,CTE.AdditionalLimitKS
	,CTE.AdditionalLimitKY
	,CTE.AdditionalLimitMN
	,CTE.FullGlassIndicator
	FROM 
	@{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_fact LMF
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim ASL
	on LMF.asl_dim_id=ASL.asl_dim_id
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
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRC
	on IRC.InsuranceReferenceCoverageDimId=LMF.InsuranceReferenceCoverageDimId
	left join LMFIDList CTE on
	CTE.CoverageGuid=RC.CoverageGuid and CTE.PremiumTransactionAKID=LMC.PremiumTransactionAKID
	and CTE.PolicyKey=POL.pol_key
	left join
	(@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge clb
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit cl
	on clb.CoverageLimitId=cl.CoverageLimitId
	)
	on LMC.PremiumTransactionAKID=clb.PremiumTransactionAKID
	and CoverageLimitValue <> 'BasicPI'
	
	where PC.InsuranceLine in ('CommercialAuto')
	AND RC.CoverageType<>'WB516CA' 
	AND LMC.trans_kind_code='D'
	AND LMC.loss_master_run_date between @Quarterstartdate and @Quarterenddate
	--AND RL.StateProvinceCode IN ('12','13','14','15','16','21','22','24','34','48') (line removed - RFC 126190)
	And (LMC.paid_loss_amt<>0 or LMC.outstanding_amt<>0 or LMF.eom_unpaid_loss_adjust_exp <>0 or LMF.paid_exp_amt<>0) 
	AND LMF.audit_id<>-9
	@{pipeline().parameters.WHERE_CLAUSE_2}
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
	InsuranceSegmentCode,
	ZipPostalCode,
	exposure,
	CoverageCode,
	DirectALAEPaidIR,
	DirectALAEOutstandingER,
	MajorPerilCode,
	pms_pol_lob_code,
	CoverageGUID,
	CoverageGroupCode,
	trans_date,
	source_sys_id,
	CoverageDeductibleValue,
	CoverageLimitValue,
	CoverageLimitType,
	PremiumTransactionAKID,
	reserve_ctgry,
	VehicleNumber,
	IncludeUIM,
	CoordinationOfBenefits,
	CoveredByWorkersCompensation,
	MedicalExpenseOption AS MedicalExpensesOption,
	SubjectToNoFault,
	DCT_CoverageType,
	AdditionalLimitKS,
	AdditionalLimitKY,
	AdditionalLimitMN,
	FullGlassIndicator
	FROM SQ_Loss
	QUALIFY ROW_NUMBER() OVER (PARTITION BY loss_master_calculation_id ORDER BY NULL) = 1
),
EXP_GetParentCoveageGUID_Loss AS (
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
	InsuranceSegmentCode,
	ZipPostalCode,
	exposure,
	CoverageCode,
	DirectALAEPaidIR,
	DirectALAEOutstandingER,
	MajorPerilCode,
	pms_pol_lob_code,
	CoverageGUID,
	CoverageGroupCode,
	trans_date,
	source_sys_id,
	CoverageDeductibleValue,
	CoverageLimitValue,
	CoverageLimitType,
	PremiumTransactionAKID,
	reserve_ctgry,
	VehicleNumber,
	IncludeUIM,
	CoordinationOfBenefits,
	CoveredByWorkersCompensation,
	MedicalExpensesOption,
	SubjectToNoFault,
	DCT_CoverageType,
	-- *INF*: SUBSTR(pol_key, 0, LENGTH(pol_key)-2)
	SUBSTR(pol_key, 0, LENGTH(pol_key) - 2) AS o_PolicyNumber,
	-- *INF*: SUBSTR(pol_key, -2, 2)
	SUBSTR(pol_key, - 2, 2) AS o_PolicyVersionFormatted,
	AdditionalLimitKS,
	AdditionalLimitKY,
	AdditionalLimitMN,
	FullGlassIndicator
	FROM AGG_RemoveDuplicate
),
RTR_Split_Out_Missing_DCT_Deductibles AS (
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
	InsuranceSegmentCode,
	ZipPostalCode,
	exposure,
	CoverageCode,
	DirectALAEPaidIR,
	DirectALAEOutstandingER,
	MajorPerilCode,
	pms_pol_lob_code,
	CoverageGUID,
	CoverageGroupCode,
	trans_date,
	source_sys_id,
	CoverageDeductibleValue,
	CoverageLimitValue,
	CoverageLimitType,
	PremiumTransactionAKID,
	reserve_ctgry,
	VehicleNumber,
	IncludeUIM,
	CoordinationOfBenefits,
	CoveredByWorkersCompensation,
	MedicalExpensesOption,
	SubjectToNoFault,
	DCT_CoverageType,
	AdditionalLimitKS,
	AdditionalLimitKY,
	AdditionalLimitMN,
	FullGlassIndicator
	FROM EXP_GetParentCoveageGUID_Loss
),
RTR_Split_Out_Missing_DCT_Deductibles_DCTCoverageLookup AS (SELECT * FROM RTR_Split_Out_Missing_DCT_Deductibles WHERE source_sys_id ='DCT' and CoverageDeductibleValue = '0'),
RTR_Split_Out_Missing_DCT_Deductibles_DEFAULT1 AS (SELECT * FROM RTR_Split_Out_Missing_DCT_Deductibles WHERE NOT ( (source_sys_id ='DCT' and CoverageDeductibleValue = '0') )),
EXP_DoDeductibleLookupsLoss AS (
	SELECT
	pol_key,
	CoverageGUID,
	-- *INF*: :LKP.LKP_GET_PARENT_COVERAGEGUID(pol_key,CoverageGUID)
	LKP_GET_PARENT_COVERAGEGUID_pol_key_CoverageGUID.archWorkDCTCoverageTransactionId AS v_archWorkDCTCoverageTransactionId,
	-- *INF*: IIF(NOT ISNULL(v_archWorkDCTCoverageTransactionId),:LKP.LKP_ALT_COVERAGEGUID(v_archWorkDCTCoverageTransactionId),NULL)
	IFF(
	    v_archWorkDCTCoverageTransactionId IS NOT NULL,
	    LKP_ALT_COVERAGEGUID_v_archWorkDCTCoverageTransactionId.CoverageGuid,
	    NULL
	) AS v_altCoverageGuid,
	v_altCoverageGuid AS o_altCoverageGuid,
	claim_loss_date
	FROM RTR_Split_Out_Missing_DCT_Deductibles_DCTCoverageLookup
	LEFT JOIN LKP_GET_PARENT_COVERAGEGUID LKP_GET_PARENT_COVERAGEGUID_pol_key_CoverageGUID
	ON LKP_GET_PARENT_COVERAGEGUID_pol_key_CoverageGUID.CoverageGUID = pol_key
	AND LKP_GET_PARENT_COVERAGEGUID_pol_key_CoverageGUID.PolicyKey = CoverageGUID

	LEFT JOIN LKP_ALT_COVERAGEGUID LKP_ALT_COVERAGEGUID_v_archWorkDCTCoverageTransactionId
	ON LKP_ALT_COVERAGEGUID_v_archWorkDCTCoverageTransactionId.archWorkDCTCoverageTransactionid = v_archWorkDCTCoverageTransactionId

),
EXP_DeductibleAmount_Loss AS (
	SELECT
	pol_key AS in_pol_key,
	CoverageGUID AS in_CoverageGUID,
	o_altCoverageGuid AS in_altCoverageGuid,
	claim_loss_date AS in_claim_loss_date,
	-- *INF*: --:LKP.LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_COMBINED_OUTPUT(in_altCoverageGuid,in_pol_key)
	-- 
	-- IIF(ISNULL(in_altCoverageGuid),:LKP.LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_LOSS_OUTPUT(in_CoverageGUID,in_pol_key,in_claim_loss_date),NULL)
	IFF(
	    in_altCoverageGuid IS NULL,
	    LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_LOSS_OUTPUT_in_CoverageGUID_in_pol_key_in_claim_loss_date.CombinedCoverageDeductibleInfo,
	    NULL
	) AS v_LKPCoverageDeductibleValue,
	-- *INF*: SUBSTR(v_LKPCoverageDeductibleValue,0,INSTR(v_LKPCoverageDeductibleValue,'|')-1)
	SUBSTR(v_LKPCoverageDeductibleValue, 0, REGEXP_INSTR(v_LKPCoverageDeductibleValue, '|') - 1) AS v_CoverageDeductibleValue_Value,
	-- *INF*: IIF(ISNULL(v_CoverageDeductibleValue_Value),'0',v_CoverageDeductibleValue_Value)
	-- 
	-- 
	IFF(v_CoverageDeductibleValue_Value IS NULL, '0', v_CoverageDeductibleValue_Value) AS o_DeductibleAmount
	FROM EXP_DoDeductibleLookupsLoss
	LEFT JOIN LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_LOSS_OUTPUT LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_LOSS_OUTPUT_in_CoverageGUID_in_pol_key_in_claim_loss_date
	ON LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_LOSS_OUTPUT_in_CoverageGUID_in_pol_key_in_claim_loss_date.CoverageGUID = in_CoverageGUID
	AND LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_LOSS_OUTPUT_in_CoverageGUID_in_pol_key_in_claim_loss_date.pol_key = in_pol_key
	AND LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_LOSS_OUTPUT_in_CoverageGUID_in_pol_key_in_claim_loss_date.EffectiveDate = in_claim_loss_date

),
Union_Loss_Remerge AS (
	SELECT loss_master_calculation_id, loss_master_run_date, prim_bus_class_code, StateProvinceCode, claim_loss_date, sub_line_code, class_code, cause_of_loss, RiskTerritory, pol_eff_date, pol_key, claim_occurrence_num, claimant_num, paid_loss_amt, outstanding_amt, pms_type_bureau_code, RiskUnitGroup, PolicySourceID, RiskType, StatisticalCoverageAKID, RatingCoverageAKID, pol_exp_date, s3p_claim_num, claim_trans_id, claim_coverage_detail_ak_id, asl_num, InsuranceLine, pol_num, statistical_code1, InsuranceSegmentCode, ZipPostalCode, exposure, CoverageCode, DirectALAEPaidIR, DirectALAEOutstandingER, MajorPerilCode, pms_pol_lob_code, CoverageGroupCode, trans_date, CoverageDeductibleValue AS DeductibleAmount, CoverageLimitValue AS CoverageLimitValue1, CoverageLimitType AS CoverageLimitType1, PremiumTransactionAKID AS PremiumTransactionAKID1, reserve_ctgry, VehicleNumber, IncludeUIM, CoordinationOfBenefits, CoveredByWorkersCompensation, MedicalExpensesOption, SubjectToNoFault, DCT_CoverageType, AdditionalLimitKS AS AdditionalLimitKS1, AdditionalLimitKY AS AdditionalLimitKY1, AdditionalLimitMN AS AdditionalLimitMN1, FullGlassIndicator AS FullGlassIndicator1
	FROM RTR_Split_Out_Missing_DCT_Deductibles_DEFAULT1
	UNION
	SELECT loss_master_calculation_id, loss_master_run_date, prim_bus_class_code, StateProvinceCode, claim_loss_date, sub_line_code, class_code, cause_of_loss, RiskTerritory, pol_eff_date, pol_key, claim_occurrence_num, claimant_num, paid_loss_amt, outstanding_amt, pms_type_bureau_code, RiskUnitGroup, PolicySourceID, RiskType, StatisticalCoverageAKID, RatingCoverageAKID, pol_exp_date, s3p_claim_num, claim_trans_id, claim_coverage_detail_ak_id, asl_num, InsuranceLine, pol_num, statistical_code AS statistical_code1, InsuranceSegmentCode, ZipPostalCode, exposure, CoverageCode, DirectALAEPaidIR, DirectALAEOutstandingER, MajorPerilCode, pms_pol_lob_code, CoverageGroupCode, trans_date, o_DeductibleAmount AS DeductibleAmount, CoverageLimitValue AS CoverageLimitValue1, CoverageLimitType AS CoverageLimitType1, PremiumTransactionAKID AS PremiumTransactionAKID1, reserve_ctgry, VehicleNumber, IncludeUIM, CoordinationOfBenefits, CoveredByWorkersCompensation, MedicalExpensesOption, SubjectToNoFault, DCT_CoverageType, AdditionalLimitKS AS AdditionalLimitKS1, AdditionalLimitKY AS AdditionalLimitKY1, AdditionalLimitMN AS AdditionalLimitMN1, FullGlassIndicator AS FullGlassIndicator1
	FROM EXP_DeductibleAmount_Loss
	-- Manually join with RTR_Split_Out_Missing_DCT_Deductibles_DCTCoverageLookup
),
EXP_Calculate_ClaimNumber AS (
	SELECT
	loss_master_calculation_id,
	loss_master_run_date,
	-- *INF*: TO_CHAR(loss_master_run_date, 'YYYYMMDD')
	TO_CHAR(loss_master_run_date, 'YYYYMMDD') AS loss_master_run_datekey,
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
	InsuranceSegmentCode,
	ZipPostalCode,
	exposure,
	CoverageCode,
	DirectALAEPaidIR,
	DirectALAEOutstandingER,
	MajorPerilCode,
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
	pms_pol_lob_code,
	CoverageGroupCode,
	trans_date,
	DeductibleAmount,
	CoverageLimitValue1,
	CoverageLimitType1,
	PremiumTransactionAKID1,
	reserve_ctgry,
	VehicleNumber,
	IncludeUIM,
	CoordinationOfBenefits,
	CoveredByWorkersCompensation,
	MedicalExpensesOption,
	SubjectToNoFault,
	DCT_CoverageType,
	AdditionalLimitKS1 AS i_AdditionalLimitKS,
	AdditionalLimitKY1 AS i_AdditionalLimitKY,
	AdditionalLimitMN1 AS i_AdditionalLimitMN,
	-- *INF*: IIF(ISNULL(i_AdditionalLimitKS), -1, i_AdditionalLimitKS)
	IFF(i_AdditionalLimitKS IS NULL, - 1, i_AdditionalLimitKS) AS o_AdditionalLimitKS,
	-- *INF*: IIF(ISNULL(i_AdditionalLimitKY), -1, i_AdditionalLimitKY)
	IFF(i_AdditionalLimitKY IS NULL, - 1, i_AdditionalLimitKY) AS o_AdditionalLimitKY,
	-- *INF*: IIF(ISNULL(i_AdditionalLimitMN), -1, i_AdditionalLimitMN)
	IFF(i_AdditionalLimitMN IS NULL, - 1, i_AdditionalLimitMN) AS o_AdditionalLimitMN,
	FullGlassIndicator1
	FROM Union_Loss_Remerge
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
	StatisticalCoverageAKID, 
	RatingCoverageAKID, 
	pol_exp_date, 
	s3p_claim_num, 
	claim_trans_id, 
	asl_num, 
	InsuranceLine, 
	pol_num, 
	statistical_code1, 
	InsuranceSegmentCode, 
	ZipPostalCode, 
	exposure, 
	CoverageCode, 
	DirectALAEPaidIR, 
	DirectALAEOutstandingER, 
	MajorPerilCode, 
	pms_pol_lob_code, 
	DeductibleAmount, 
	CoverageGroupCode, 
	trans_date, 
	CoverageLimitValue1, 
	CoverageLimitType1, 
	PremiumTransactionAKID1, 
	reserve_ctgry, 
	VehicleNumber, 
	IncludeUIM, 
	CoordinationOfBenefits, 
	CoveredByWorkersCompensation, 
	MedicalExpensesOption, 
	SubjectToNoFault, 
	DCT_CoverageType, 
	o_AdditionalLimitKS AS AdditionalLimitKS, 
	o_AdditionalLimitKY AS AdditionalLimitKY, 
	o_AdditionalLimitMN AS AdditionalLimitMN, 
	FullGlassIndicator1
	FROM EXP_Calculate_ClaimNumber
	ORDER BY pol_key ASC, ClaimNum ASC, loss_master_run_date ASC, claim_coverage_detail_ak_id ASC
),
LKP_ISSWorkTable_Loss AS (
	SELECT
	EDWLossMasterCalculationPKId
	FROM (
		SELECT 
			EDWLossMasterCalculationPKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ISSCommercialAutoExtract
		WHERE DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,LossMasterRunDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1) and
		EDWLossMasterCalculationPKId<>-1
		
		--YEAR(LossMasterRunDate)=YEAR(dateadd(year,@{pipeline().parameters.NO_OF_YEARS},GETDATE())) and EDWLossMasterCalculationPKId<>-1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWLossMasterCalculationPKId ORDER BY EDWLossMasterCalculationPKId) = 1
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
		SELECT distinct SUM(InceptionToDatePaidLossAmount) over (partition by pol_key,edw_claimant_cov_det_ak_id,claim_num order by edw_claimant_cov_det_ak_id,trans_date,claim_trans_pk_id) AS InceptionToDatePaidLossAmount,
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
	LKP_ISSWorkTable_Loss.EDWLossMasterCalculationPKId AS LKP_LossMasterCalculationId, 
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
	SRT_Sort_data.InsuranceSegmentCode, 
	SRT_Sort_data.ZipPostalCode, 
	SRT_Sort_data.exposure, 
	SRT_Sort_data.CoverageCode, 
	SRT_Sort_data.DirectALAEPaidIR, 
	SRT_Sort_data.DirectALAEOutstandingER, 
	SRT_Sort_data.MajorPerilCode, 
	SRT_Sort_data.pms_pol_lob_code, 
	SRT_Sort_data.DeductibleAmount, 
	SRT_Sort_data.CoverageGroupCode, 
	SRT_Sort_data.CoverageLimitValue1, 
	SRT_Sort_data.CoverageLimitType1, 
	SRT_Sort_data.PremiumTransactionAKID1, 
	SRT_Sort_data.reserve_ctgry, 
	SRT_Sort_data.VehicleNumber, 
	SRT_Sort_data.IncludeUIM, 
	SRT_Sort_data.CoordinationOfBenefits, 
	SRT_Sort_data.CoveredByWorkersCompensation, 
	SRT_Sort_data.MedicalExpensesOption, 
	SRT_Sort_data.SubjectToNoFault, 
	SRT_Sort_data.DCT_CoverageType, 
	SRT_Sort_data.AdditionalLimitKS, 
	SRT_Sort_data.AdditionalLimitKY, 
	SRT_Sort_data.AdditionalLimitMN, 
	SRT_Sort_data.FullGlassIndicator1
	FROM SRT_Sort_data
	LEFT JOIN LKP_ISSWorkTable_Loss
	ON LKP_ISSWorkTable_Loss.EDWLossMasterCalculationPKId = SRT_Sort_data.loss_master_calculation_id
	LEFT JOIN LKP_InceptionToDatePaidLossAmount
	ON LKP_InceptionToDatePaidLossAmount.pol_key = SRT_Sort_data.pol_key AND LKP_InceptionToDatePaidLossAmount.edw_claimant_cov_det_ak_id = SRT_Sort_data.claim_coverage_detail_ak_id AND LKP_InceptionToDatePaidLossAmount.trans_date <= SRT_Sort_data.trans_date AND LKP_InceptionToDatePaidLossAmount.loss_master_calculation_id = SRT_Sort_data.loss_master_calculation_id
	WHERE ISNULL(LKP_LossMasterCalculationId) AND  
(paid_loss_amt != 0 or outstanding_amt!=0 or DirectALAEOutstandingER !=0 or DirectALAEPaidIR!=0)
and TO_CHAR(loss_master_run_date, 'YYYY') ||TO_CHAR(loss_master_run_date, 'QQ')=
TO_CHAR( ADD_TO_DATE(sysdate, 'MM', 3*@{pipeline().parameters.NO_OF_QUARTERS}), 'YYYY') ||TO_CHAR( ADD_TO_DATE(sysdate, 'MM', 3*@{pipeline().parameters.NO_OF_QUARTERS}), 'QQ')
),
EXP_GetCoverageAKID AS (
	SELECT
	StatisticalCoverageAKID AS i_StatisticalCoverageAKID,
	RatingCoverageAKID AS i_RatingCoverageAKID,
	-- *INF*: IIF(
	-- i_StatisticalCoverageAKID=-1,i_RatingCoverageAKID,i_StatisticalCoverageAKID
	-- )
	IFF(i_StatisticalCoverageAKID = - 1, i_RatingCoverageAKID, i_StatisticalCoverageAKID) AS o_CoverageAKID,
	-- *INF*: IIF(
	-- i_StatisticalCoverageAKID=-1,'DCT','PMS'
	-- )
	IFF(i_StatisticalCoverageAKID = - 1, 'DCT', 'PMS') AS o_SourceSystemID
	FROM FIL_Exists_Loss
),
LKP_PremiumTransaction AS (
	SELECT
	CoverageAKID,
	PackageModificationAdjustmentGroupCode,
	ConstructionCode,
	IsoFireProtectionCode,
	BureauCode1,
	BureauCode2,
	BureauCode4,
	VehicleYear,
	TerminalZoneCode,
	DeductibleBasis,
	PIPBureaucoverageCode,
	RatingZoneCode
	FROM (
		DECLARE @QuarterEndDate DateTime
		
		SET @QuarterEndDate = DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1)
		
		
		SELECT 
		PackageModificationAdjustmentGroupCode as PackageModificationAdjustmentGroupCode,
		ConstructionCode as ConstructionCode,
		IsoFireProtectionCode as IsoFireProtectionCode,
		BureauCode1 as BureauCode1,
		BureauCode2 as BureauCode2, 
		BureauCode4 as BureauCode4,  
		VehicleYear as VehicleYear,
		TerminalZoneCode  as TerminalZoneCode,
		DeductibleBasis  as DeductibleBasis,
		PIPBureaucoverageCode as PIPBureaucoverageCode,
		RatingZoneCode as RatingZoneCode,
		CoverageAKID as CoverageAKID  
		FROM
		(
		SELECT distinct 
		PremiumTransaction.StatisticalCoverageAKID AS CoverageAKID,
		PremiumTransaction.PackageModificationAdjustmentGroupCode AS PackageModificationAdjustmentGroupCode,  
		PremiumTransaction.ConstructionCode AS ConstructionCode,  
		'NOT NEED' AS IsoFireProtectionCode,
		  LTRIM(RTRIM(BureauCode1)) AS BureauCode1,  LTRIM(RTRIM(BureauCode2)) AS BureauCode2,  LTRIM(RTRIM(BureauCode4)) AS BureauCode4,  
		  CA.VehicleYear AS VehicleYear,  
		  'N/A' AS TerminalZoneCode,  
		  PremiumTransaction.DeductibleBasis as DeductibleBasis,
		CA.PIPBureaucoverageCode as PIPBureaucoverageCode,
		'N/A' AS RatingZoneCode
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation with (nolock)
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage with (nolock) on loss_master_calculation.StatisticalCoverageAKID = StatisticalCoverage.StatisticalCoverageAKID 
		and DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,loss_master_calculation.loss_master_run_date),-1)=@QuarterEndDate
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction with (nolock) on StatisticalCoverage.StatisticalCoverageAKID = PremiumTransaction.StatisticalCoverageAKID 
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.BureauStatisticalCode with (nolock) on BureauStatisticalCode.PremiumTransactionAKID = PremiumTransaction.PremiumTransactionAKID
		INNER  join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CA
		on CA.PremiumTransactionID=PremiumTransaction.PremiumTransactionID
		and CA.CurrentSnapshotFlag=1
		WHERE PremiumTransaction.SourceSystemID='PMS'
		union all
		SELECT distinct 
		PremiumTransaction.RatingCoverageAKID as CoverageAKID,
		PremiumTransaction.PackageModificationAdjustmentGroupCode as PackageModificationAdjustmentGroupCode, 
		PremiumTransaction.ConstructionCode as ConstructionCode, 
		'NOT NEED' as IsoFireProtectionCode, 
		null as BureauCode1,
		null as BureauCode2,
		null as BureauCode4,
		CA.VehicleYear as VehicleYear,
		CA.TerminalZoneCode as TerminalZoneCode,
		PremiumTransaction.DeductibleBasis as DeductibleBasis,
		CA.PIPBureaucoverageCode as PIPBureaucoverageCode,
		CA.RatingZoneCode as RatingZoneCode
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation with (nolock) inner join 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage with (nolock) on loss_master_calculation.RatingCoverageAKID = RatingCoverage.RatingCoverageAKID
		and DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,loss_master_calculation.loss_master_run_date),-1)=@QuarterEndDate
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction with (nolock) on RatingCoverage.RatingCoverageAKID = PremiumTransaction.RatingCoverageAKID 
		and RatingCoverage.EffectiveDate=PremiumTransaction.EffectiveDate
		INNER  join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CA
		on CA.PremiumTransactionID=PremiumTransaction.PremiumTransactionID
		and CA.CurrentSnapshotFlag=1
		WHERE PremiumTransaction.SourceSystemID='DCT'
		) as A 
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageAKID ORDER BY CoverageAKID) = 1
),
EXP_Cleansing_Loss AS (
	SELECT
	FIL_Exists_Loss.pol_key AS i_pol_key,
	FIL_Exists_Loss.ClaimNum AS i_ClaimNum,
	FIL_Exists_Loss.loss_master_run_date AS i_loss_master_run_date,
	FIL_Exists_Loss.loss_master_calculation_id AS i_loss_master_calculation_id,
	FIL_Exists_Loss.prim_bus_class_code AS i_prim_bus_class_code,
	FIL_Exists_Loss.StateProvinceCode AS i_StateProvinceCode,
	FIL_Exists_Loss.claim_loss_date AS i_claim_loss_date,
	FIL_Exists_Loss.sub_line_code AS i_sub_line_code,
	FIL_Exists_Loss.ClassCode AS i_ClassCode,
	FIL_Exists_Loss.cause_of_loss AS i_cause_of_loss,
	FIL_Exists_Loss.RiskTerritory AS i_RiskTerritory,
	FIL_Exists_Loss.pol_eff_date AS i_pol_eff_date,
	FIL_Exists_Loss.claim_occurrence_num AS i_claim_occurrence_num,
	FIL_Exists_Loss.claimant_num AS i_claimant_num,
	FIL_Exists_Loss.paid_loss_amt AS i_paid_loss_amt,
	FIL_Exists_Loss.outstanding_amt AS i_outstanding_amt,
	FIL_Exists_Loss.TypeBureauCode AS i_TypeBureauCode,
	FIL_Exists_Loss.RiskUnitGroup AS i_RiskUnitGroup,
	FIL_Exists_Loss.PolicySourceID AS i_PolicySourceID,
	FIL_Exists_Loss.RiskType AS i_RiskType,
	FIL_Exists_Loss.StatisticalCoverageAKID,
	FIL_Exists_Loss.pol_exp_date AS i_pol_exp_date,
	FIL_Exists_Loss.s3p_claim_num,
	FIL_Exists_Loss.out_CumulativeInceptiontoDatePaidLoss AS i_CumulativeInceptiontoDatePaidLoss,
	FIL_Exists_Loss.asl_num AS i_asl_num,
	LKP_PremiumTransaction.PIPBureaucoverageCode,
	FIL_Exists_Loss.CoverageCode AS i_CoverageCode,
	LKP_PremiumTransaction.DeductibleBasis,
	FIL_Exists_Loss.claim_coverage_detail_ak_id,
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
	StatisticalCoverageAKID AS o_StatisticalCoverageAKID,
	i_pol_exp_date AS o_pol_exp_date,
	-- *INF*: RTRIM(LTRIM(s3p_claim_num))
	RTRIM(LTRIM(s3p_claim_num)) AS o_s3p_claim_num,
	-- *INF*: DECODE(True,
	-- direct_alae_paid_including_recoveries<>0, 0,
	-- direct_loss_outstanding_excluding_recoveries<>0,0,
	-- i_CumulativeInceptiontoDatePaidLoss
	-- )
	DECODE(
	    True,
	    direct_alae_paid_including_recoveries <> 0, 0,
	    direct_loss_outstanding_excluding_recoveries <> 0, 0,
	    i_CumulativeInceptiontoDatePaidLoss
	) AS o_CumulativeInceptiontoDatePaidLoss,
	i_asl_num AS o_AnnualStatementLineNumber,
	FIL_Exists_Loss.InsuranceLine AS i_InsuranceLine,
	-- *INF*: LTRIM(RTRIM(i_InsuranceLine))
	LTRIM(RTRIM(i_InsuranceLine)) AS o_InsuranceLine,
	FIL_Exists_Loss.pol_num,
	i_ClaimNum AS o_ClaimNum,
	FIL_Exists_Loss.statistical_code1 AS i_statistical_code1,
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
	FIL_Exists_Loss.InsuranceSegmentCode,
	FIL_Exists_Loss.ZipPostalCode AS i_ZipPostalCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ZipPostalCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_ZipPostalCode) AS o_ZipPostalCode,
	FIL_Exists_Loss.exposure AS i_exposure,
	-- *INF*: IIF(ISNULL(i_exposure),0,i_exposure)
	IFF(i_exposure IS NULL, 0, i_exposure) AS o_exposure,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(PIPBureaucoverageCode) OR PIPBureaucoverageCode='N/A', i_CoverageCode,
	-- DCT_CoverageType <> 'PIP',i_CoverageCode,
	-- PIPBureaucoverageCode)
	-- 
	-- -- old logic
	-- --IIF(ISNULL(PIPBureaucoverageCode) OR PIPBureaucoverageCode='N/A',
	-- --i_CoverageCode,PIPBureaucoverageCode)
	-- 
	DECODE(
	    TRUE,
	    PIPBureaucoverageCode IS NULL OR PIPBureaucoverageCode = 'N/A', i_CoverageCode,
	    DCT_CoverageType <> 'PIP', i_CoverageCode,
	    PIPBureaucoverageCode
	) AS o_CoverageCode,
	LKP_PremiumTransaction.VehicleYear,
	LKP_PremiumTransaction.TerminalZoneCode,
	FIL_Exists_Loss.DirectALAEPaidIR AS direct_alae_paid_including_recoveries,
	FIL_Exists_Loss.DirectALAEOutstandingER AS direct_loss_outstanding_excluding_recoveries,
	FIL_Exists_Loss.MajorPerilCode,
	FIL_Exists_Loss.pms_pol_lob_code,
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
	) AS o_Iob,
	FIL_Exists_Loss.DeductibleAmount,
	FIL_Exists_Loss.CoverageGroupCode,
	FIL_Exists_Loss.CoverageLimitValue1 AS CoverageLimitValue,
	FIL_Exists_Loss.CoverageLimitType1 AS CoverageLimitType,
	FIL_Exists_Loss.PremiumTransactionAKID1,
	FIL_Exists_Loss.reserve_ctgry,
	FIL_Exists_Loss.VehicleNumber,
	FIL_Exists_Loss.IncludeUIM,
	FIL_Exists_Loss.CoordinationOfBenefits,
	FIL_Exists_Loss.CoveredByWorkersCompensation,
	FIL_Exists_Loss.MedicalExpensesOption,
	FIL_Exists_Loss.SubjectToNoFault,
	FIL_Exists_Loss.DCT_CoverageType,
	FIL_Exists_Loss.AdditionalLimitKS,
	FIL_Exists_Loss.AdditionalLimitKY,
	FIL_Exists_Loss.AdditionalLimitMN,
	LKP_PremiumTransaction.RatingZoneCode,
	FIL_Exists_Loss.FullGlassIndicator1
	FROM FIL_Exists_Loss
	LEFT JOIN LKP_PremiumTransaction
	ON LKP_PremiumTransaction.CoverageAKID = EXP_GetCoverageAKID.o_CoverageAKID
),
EXP_Transform_Prior_to_lookup AS (
	SELECT
	LKP_PremiumTransaction.PackageModificationAdjustmentGroupCode,
	FIL_Exists_Loss.StatisticalCoverageAKID,
	-- *INF*: IIF(StatisticalCoverageAKID=-1,'DCT','PMS')
	IFF(StatisticalCoverageAKID = - 1, 'DCT', 'PMS') AS out_SourceSystem
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
LKP_ExcessAttendantCare_Coverage_Loss AS (
	SELECT
	lu_PolicyKey,
	i_PolicyKey,
	i_EffectiveDate,
	i_ExpirationDate,
	lu_EffectiveDate,
	lu_ExpirationDate
	FROM (
		select distinct POL.pol_key, RC.EffectiveDate, RC.ExpirationDate 
		
		from v2.policy POL
		
		inner join PolicyCoverage PC
		on PC.PolicyAKID = POL.pol_ak_id
		and PC.CurrentSnapshotFlag = 1
		
		inner join RatingCoverage RC
		on RC.PolicyCoverageAKID = PC.PolicyCoverageAKID
		and RC.CoverageType = 'ExcessAttendantCare'
		and RC.CurrentSnapshotFlag = 1
		
		where POL.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY lu_PolicyKey,lu_EffectiveDate,lu_ExpirationDate ORDER BY lu_PolicyKey) = 1
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
	-- *INF*: --Fix for EDWP-4028
	-- --DECODE(TRUE, 
	-- --IN(StateProvinceCode,'15', '16', '21', '22') AND --sub_line_code='613','01',
	-- --IN(StateProvinceCode,'14','13', '12', '34', '24', '48') AND
	-- --sub_line_code='611','00',
	-- --IN(sub_line_code,'618','648'),'00',
	-- --sub_line_code='615','01',
	-- --IN(StateProvinceCode,'15', '16', '21', '22') AND
	-- --IN(sub_line_code,'620', '621','622', '623', '641','645'),'01',
	-- --IN(StateProvinceCode,'14','13','12','34','24','48') AND
	-- --IN(sub_line_code,'641','620', '621', '622', '623'),'00',
	-- --'N/A')
	-- 
	-- 
	-- DECODE(TRUE,
	-- IN(i_CoverageCode,'COLL','COMPRH','COMRPD','CALNSECOMP','PLSPAK - BRD','CALNSECOL','TOWLABOR'),'00',
	-- IN(StateProvinceCode,'15','16','21','22')  AND  IN(i_CoverageCode,'ADLINS','BIPD','EMPLESSOR','FELEMPL',
	-- 'INJLEASEWRKS','LSECONCRN','MANU','MCCA','MEDPAY',
	-- 'MINPREM','PIP','POLLUTION','RACEXCL','RAILOPTS','UIM',
	-- 'UM','LOSSUSEEXP','LIMMEXCOV','PPI','671','672','681','682','695'),'01',
	-- '00')
	-- 
	-- 
	DECODE(
	    TRUE,
	    i_CoverageCode IN ('COLL','COMPRH','COMRPD','CALNSECOMP','PLSPAK - BRD','CALNSECOL','TOWLABOR'), '00',
	    StateProvinceCode IN ('15','16','21','22') AND i_CoverageCode IN ('ADLINS','BIPD','EMPLESSOR','FELEMPL','INJLEASEWRKS','LSECONCRN','MANU','MCCA','MEDPAY','MINPREM','PIP','POLLUTION','RACEXCL','RAILOPTS','UIM','UM','LOSSUSEEXP','LIMMEXCOV','PPI','671','672','681','682','695'), '01',
	    '00'
	) AS sub_line_code_out,
	EXP_Cleansing_Loss.o_ClassCode AS ClassCode,
	'N/A' AS PremiumMasterClassCode_out,
	-- *INF*: IIF(ISNULL(ClassCode) OR IS_SPACES(ClassCode) OR LENGTH(ClassCode)=0 OR IN(ClassCode, 'N/A','TBD'),
	-- '00000',
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
	    '00000',
	    ClassCode
	) AS LossMasterClassCode_out,
	LKP_CauseOfLoss.BureauCauseOfLoss AS cause_of_loss,
	-- *INF*: iif(in(i_CoverageCode,'COMRLIAB','COMRLIABUM','COMRLIABUIM','COMRLIABMEDICAL','COMRLIABPIP')
	-- ,'00',cause_of_loss) 
	IFF(
	    i_CoverageCode IN ('COMRLIAB','COMRLIABUM','COMRLIABUIM','COMRLIABMEDICAL','COMRLIABPIP'),
	    '00',
	    cause_of_loss
	) AS cause_of_loss_out,
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
	EXP_Cleansing_Loss.o_claimant_num AS claimant_num,
	0.00 AS PremiumMasterPremium,
	EXP_Cleansing_Loss.o_PaidLossAmount AS PaidLossAmount,
	EXP_Cleansing_Loss.o_OutstandingLossAmount AS OutstandingLossAmount,
	EXP_Cleansing_Loss.o_TypeBureauCode AS TypeBureauCode,
	EXP_Cleansing_Loss.o_RiskUnitGroup AS RiskUnitGroup,
	EXP_Cleansing_Loss.o_PolicySourceID AS PolicySourceID,
	EXP_Cleansing_Loss.o_RiskType AS RiskType,
	EXP_Cleansing_Loss.DeductibleAmount,
	LKP_PremiumTransaction.BureauCode1,
	LKP_PremiumTransaction.BureauCode2,
	LKP_PremiumTransaction.BureauCode4,
	EXP_Reset_Pms_ConstCode_IsoPPC.o_ConsturctionCode AS i_ConstructionCode,
	EXP_Reset_Pms_ConstCode_IsoPPC.o_IsoFireProtectionCode AS i_IsoFireProtectionCode,
	EXP_Cleansing_Loss.o_CoverageCode AS i_CoverageCode,
	EXP_Cleansing_Loss.CoordinationOfBenefits,
	EXP_Cleansing_Loss.CoveredByWorkersCompensation,
	EXP_Cleansing_Loss.MedicalExpensesOption,
	EXP_Cleansing_Loss.SubjectToNoFault,
	EXP_Cleansing_Loss.DCT_CoverageType,
	EXP_Cleansing_Loss.AdditionalLimitKS,
	EXP_Cleansing_Loss.AdditionalLimitKY,
	EXP_Cleansing_Loss.AdditionalLimitMN,
	LKP_ExcessAttendantCare_Coverage_Loss.lu_PolicyKey,
	-- *INF*: DECODE(TRUE,
	-- DCT_CoverageType = 'PIP' AND IN(TypeBureauCode,'AL','AN','AP','N/A','CommercialAuto'), '1',
	-- '0')
	DECODE(
	    TRUE,
	    DCT_CoverageType = 'PIP' AND TypeBureauCode IN ('AL','AN','AP','N/A','CommercialAuto'), '1',
	    '0'
	) AS v_CLFile_PIP,
	-- *INF*: DECODE(TRUE, i_CoverageCode = 'COMRLIAB' AND reserve_ctgry = '2','051',
	-- i_CoverageCode = 'COMRLIAB' AND reserve_ctgry = '3','054',
	-- i_CoverageCode = 'COMRLIAB' AND reserve_ctgry = '1','051',
	-- i_CoverageCode = 'COMRLIABUM' AND reserve_ctgry = '2','056',
	-- i_CoverageCode = 'COMRLIABUM' AND reserve_ctgry = '3','057',
	-- i_CoverageCode = 'COMRLIABUM' AND reserve_ctgry = '1','056',
	-- i_CoverageCode = 'COMRLIABUIM' AND reserve_ctgry = '2','052',
	-- i_CoverageCode = 'COMRLIABUIM' AND reserve_ctgry = '3','059',
	-- i_CoverageCode = 'COMRLIABUIM' AND reserve_ctgry = '1','052',
	-- i_CoverageCode = 'COMRLIABMEDICAL' ,'053',
	-- i_CoverageCode = 'COMRLIABPIP' ,'058',
	-- i_CoverageCode)
	DECODE(
	    TRUE,
	    i_CoverageCode = 'COMRLIAB' AND reserve_ctgry = '2', '051',
	    i_CoverageCode = 'COMRLIAB' AND reserve_ctgry = '3', '054',
	    i_CoverageCode = 'COMRLIAB' AND reserve_ctgry = '1', '051',
	    i_CoverageCode = 'COMRLIABUM' AND reserve_ctgry = '2', '056',
	    i_CoverageCode = 'COMRLIABUM' AND reserve_ctgry = '3', '057',
	    i_CoverageCode = 'COMRLIABUM' AND reserve_ctgry = '1', '056',
	    i_CoverageCode = 'COMRLIABUIM' AND reserve_ctgry = '2', '052',
	    i_CoverageCode = 'COMRLIABUIM' AND reserve_ctgry = '3', '059',
	    i_CoverageCode = 'COMRLIABUIM' AND reserve_ctgry = '1', '052',
	    i_CoverageCode = 'COMRLIABMEDICAL', '053',
	    i_CoverageCode = 'COMRLIABPIP', '058',
	    i_CoverageCode
	) AS v_CoverageCode,
	-- *INF*: DECODE(TRUE,
	-- StateProvinceCode = '15' AND IN (AdditionalLimitKS,-1, 0) AND v_CLFile_PIP = '1', '681',
	-- StateProvinceCode = '15' AND NOT IN (AdditionalLimitKS,-1, 0) AND v_CLFile_PIP = '1', '682',
	-- v_CoverageCode)
	DECODE(
	    TRUE,
	    StateProvinceCode = '15' AND AdditionalLimitKS IN (- 1,0) AND v_CLFile_PIP = '1', '681',
	    StateProvinceCode = '15' AND NOT AdditionalLimitKS IN (- 1,0) AND v_CLFile_PIP = '1', '682',
	    v_CoverageCode
	) AS v_KS_PIP_CoverageCode,
	-- *INF*: DECODE(TRUE,
	-- StateProvinceCode = '16' AND IN (AdditionalLimitKS,-1, 0) AND CoveredByWorkersCompensation <> 'T' AND v_CLFile_PIP = '1', '681',
	-- StateProvinceCode = '16' AND IN (AdditionalLimitKS,-1, 0) AND CoveredByWorkersCompensation = 'T' AND v_CLFile_PIP = '1', '671',			   
	-- StateProvinceCode = '16' AND NOT IN (AdditionalLimitKY, -1, 0) AND CoveredByWorkersCompensation <> 'T' AND v_CLFile_PIP = '1', '682', 			
	-- StateProvinceCode = '16' AND  NOT IN (AdditionalLimitKY, -1, 0) AND CoveredByWorkersCompensation = 'T' AND v_CLFile_PIP = '1', '672',
	-- v_CoverageCode)
	DECODE(
	    TRUE,
	    StateProvinceCode = '16' AND AdditionalLimitKS IN (- 1,0) AND CoveredByWorkersCompensation <> 'T' AND v_CLFile_PIP = '1', '681',
	    StateProvinceCode = '16' AND AdditionalLimitKS IN (- 1,0) AND CoveredByWorkersCompensation = 'T' AND v_CLFile_PIP = '1', '671',
	    StateProvinceCode = '16' AND NOT AdditionalLimitKY IN (- 1,0) AND CoveredByWorkersCompensation <> 'T' AND v_CLFile_PIP = '1', '682',
	    StateProvinceCode = '16' AND NOT AdditionalLimitKY IN (- 1,0) AND CoveredByWorkersCompensation = 'T' AND v_CLFile_PIP = '1', '672',
	    v_CoverageCode
	) AS v_KY_PIP_CoverageCode,
	PIPBureaucoverageCode AS v_MN_PIP_CoverageCode,
	-- *INF*: DECODE(TRUE,
	-- MedicalExpensesOption='Rejected','689',
	-- IN(MedicalExpensesOption,'Unlimited','SelectedLimit','Excluded', 'N/A')
	--    AND CoveredByWorkersCompensation = 'T','671',
	-- IN(MedicalExpensesOption,'Unlimited','SelectedLimit','Excluded', 'N/A')
	--    AND CoveredByWorkersCompensation = 'F' 
	--    AND IN(CoordinationOfBenefits,'None','0','N/A'),'681',
	-- IN(MedicalExpensesOption,'Unlimited','SelectedLimit','Excluded', 'N/A')
	--    AND CoveredByWorkersCompensation = 'F' 
	--    AND CoordinationOfBenefits = 'MedicalExpensess','691',
	-- IN(MedicalExpensesOption,'Unlimited','SelectedLimit','Excluded', 'N/A')
	--    AND CoveredByWorkersCompensation = 'F' 
	--    AND CoordinationOfBenefits = 'WorkLoss','692',
	-- IN(MedicalExpensesOption,'Unlimited','SelectedLimit','Excluded', 'N/A')
	--    AND CoveredByWorkersCompensation = 'F' 
	--    AND CoordinationOfBenefits = 'MedicalWorkLoss','693',
	-- NOT ISNULL(lu_PolicyKey),'683',
	-- '685')
	DECODE(
	    TRUE,
	    MedicalExpensesOption = 'Rejected', '689',
	    MedicalExpensesOption IN ('Unlimited','SelectedLimit','Excluded','N/A') AND CoveredByWorkersCompensation = 'T', '671',
	    MedicalExpensesOption IN ('Unlimited','SelectedLimit','Excluded','N/A') AND CoveredByWorkersCompensation = 'F' AND CoordinationOfBenefits IN ('None','0','N/A'), '681',
	    MedicalExpensesOption IN ('Unlimited','SelectedLimit','Excluded','N/A') AND CoveredByWorkersCompensation = 'F' AND CoordinationOfBenefits = 'MedicalExpensess', '691',
	    MedicalExpensesOption IN ('Unlimited','SelectedLimit','Excluded','N/A') AND CoveredByWorkersCompensation = 'F' AND CoordinationOfBenefits = 'WorkLoss', '692',
	    MedicalExpensesOption IN ('Unlimited','SelectedLimit','Excluded','N/A') AND CoveredByWorkersCompensation = 'F' AND CoordinationOfBenefits = 'MedicalWorkLoss', '693',
	    lu_PolicyKey IS NOT NULL, '683',
	    '685'
	) AS v_MI_PIP_CoverageCode,
	-- *INF*: DECODE(TRUE,
	-- -- MI
	-- StateProvinceCode = '21' AND SubjectToNoFault = 'Yes' AND DCT_CoverageType = 'ExcessAttendantCare' and pol_eff_date > TO_DATE('2020-07-01 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),'683',
	-- 
	-- StateProvinceCode = '21' AND v_CLFile_PIP='1', v_MI_PIP_CoverageCode,
	-- -- StateProvinceCode = '21' AND SubjectToNoFault = 'Yes' AND DCT_CoverageType = 'PIP' and pol_eff_date > TO_DATE('2020-07-01 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),v_MI_PIP_CoverageCode,
	-- 
	-- -- KS
	-- StateProvinceCode = '15'  AND v_CLFile_PIP='1', v_KS_PIP_CoverageCode,
	-- 
	-- -- KY
	-- StateProvinceCode = '16' AND v_CLFile_PIP='1', v_KY_PIP_CoverageCode,
	-- 
	-- --MN
	-- StateProvinceCode = '22' AND v_CLFile_PIP='1', v_MN_PIP_CoverageCode,
	-- 
	-- 
	-- -- Default
	-- v_CoverageCode)
	DECODE(
	    TRUE,
	    StateProvinceCode = '21' AND SubjectToNoFault = 'Yes' AND DCT_CoverageType = 'ExcessAttendantCare' and pol_eff_date > TO_TIMESTAMP('2020-07-01 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), '683',
	    StateProvinceCode = '21' AND v_CLFile_PIP = '1', v_MI_PIP_CoverageCode,
	    StateProvinceCode = '15' AND v_CLFile_PIP = '1', v_KS_PIP_CoverageCode,
	    StateProvinceCode = '16' AND v_CLFile_PIP = '1', v_KY_PIP_CoverageCode,
	    StateProvinceCode = '22' AND v_CLFile_PIP = '1', v_MN_PIP_CoverageCode,
	    v_CoverageCode
	) AS o_CoverageCode,
	v_CLFile_PIP AS o_CLFile_PIP,
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
	EXP_Cleansing_Loss.o_ZipPostalCode AS ZipPostalCode,
	EXP_Cleansing_Loss.o_exposure AS exposure,
	EXP_Cleansing_Loss.VehicleYear,
	EXP_Cleansing_Loss.TerminalZoneCode,
	EXP_Cleansing_Loss.direct_alae_paid_including_recoveries,
	EXP_Cleansing_Loss.direct_loss_outstanding_excluding_recoveries,
	EXP_Cleansing_Loss.DeductibleBasis AS DeductibleBasis2,
	EXP_Cleansing_Loss.CoverageGroupCode,
	'0' AS Old_VehicleNumber,
	EXP_Cleansing_Loss.VehicleNumber,
	EXP_Cleansing_Loss.CoverageLimitValue,
	EXP_Cleansing_Loss.PIPBureaucoverageCode,
	EXP_Cleansing_Loss.CoverageLimitType,
	EXP_Cleansing_Loss.PremiumTransactionAKID1,
	EXP_Cleansing_Loss.reserve_ctgry,
	EXP_Cleansing_Loss.IncludeUIM,
	EXP_Cleansing_Loss.RatingZoneCode,
	EXP_Cleansing_Loss.FullGlassIndicator1
	FROM EXP_Cleansing_Loss
	 -- Manually join with EXP_Reset_Pms_ConstCode_IsoPPC
	 -- Manually join with EXP_Transform_Prior_to_lookup
	LEFT JOIN LKP_CauseOfLoss
	ON LKP_CauseOfLoss.CauseOfLoss = EXP_Cleansing_Loss.o_cause_of_loss AND LKP_CauseOfLoss.LineOfBusiness = EXP_Cleansing_Loss.o_Iob AND LKP_CauseOfLoss.MajorPeril = EXP_Cleansing_Loss.MajorPerilCode
	LEFT JOIN LKP_ExcessAttendantCare_Coverage_Loss
	ON LKP_ExcessAttendantCare_Coverage_Loss.lu_PolicyKey = EXP_Cleansing_Loss.o_pol_key AND LKP_ExcessAttendantCare_Coverage_Loss.lu_EffectiveDate <= EXP_Cleansing_Loss.o_pol_exp_date AND LKP_ExcessAttendantCare_Coverage_Loss.lu_ExpirationDate >= EXP_Cleansing_Loss.o_pol_eff_date
	LEFT JOIN LKP_PremiumTransaction
	ON LKP_PremiumTransaction.CoverageAKID = EXP_GetCoverageAKID.o_CoverageAKID
),
SQ_Premium AS (
	DECLARE @Quarterstartdate as datetime, 
	        @Quarterenddate as datetime
	
	SET @Quarterstartdate = DATEADD(qq, DATEDIFF(qq, 0, GETDATE()) + @{pipeline().parameters.NO_OF_QUARTERS}, 0)                 
	SET @Quarterenddate =  DATEADD(ss, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE()) +1 + @{pipeline().parameters.NO_OF_QUARTERS}, 0)) 
	
	SELECT distinct  
	PMC.PremiumMasterCalculationID,
	PMC.PremiumMasterRunDate,
	POL.pol_key,
	POL.prim_bus_class_code,
	RL.StateProvinceCode,
	PT.PremiumTransactionBookedDate,
	PMC.PremiumMasterSubLine,
	SC.ClassCode as classcode, 
	RL.RiskTerritory,
	POL.pol_eff_date,
	PMC.PremiumMasterPremium,
	PMC.PremiumMasterTypeBureauCode,
	SC.RiskUnitGroup,
	PT.SourceSystemID,
	PMC.PremiumMasterTransactionCode,
	PMC.PremiumMasterReasonAmendedCode,
	'N/A' AS RiskType,
	SC.CoverageGUID,
	Case when cd.CoverageDeductibleValue = 0 or cd.CoverageDeductibleValue is null then cd_1.CoverageDeductibleType Else cd.CoverageDeductibleType end as CoverageDeductibleType,
	Case when cd.CoverageDeductibleValue = 0 or cd.CoverageDeductibleValue is null then cd_1.CoverageDeductibleValue Else cd.CoverageDeductibleValue end as CoverageDeductibleValue ,
	CASE WHEN PC.TypeBureauCode='GL' THEN bsc.BureauCode3 ELSE PT.ConstructionCode END as ConstructionCode,
	'N/A' as IsoFireProtectionCode,
	PT.PackageModificationAdjustmentGroupCode,
	bsc.BureauCode1,
	'N/A' AS BureauCode2,
	'N/A' AS BureauCode4,
	POL.pol_exp_date,
	CASE WHEN sub_asl_num IS NULL THEN asl_num WHEN ltrim(rtrim(sub_asl_num))= 'N/A' THEN asl_num ELSE sub_asl_num END AS ASL_NUM,
	ISG.InsuranceSegmentCode,
	RL.ZipPostalCode,
	PremiumMasterExposure,
	CASE WHEN CA.PIPBureaucoverageCode IS NULL OR PIPBureaucoverageCode='N/A' THEN LTRIM(RTRIM(IRC.CoverageCode)) ELSE CA.PIPBureaucoverageCode END AS CoverageCode,
	ISNULL(CA.VehicleYear,'N/A'),
	RIGHT (RTRIM(CoverageLimitValue) ,5)  CoverageLimitValue,
	CA.TerminalZoneCode,
	PT.DeductibleBasis,
	CA.PIPBureaucoverageCode,
	PT.PremiumTransactionEffectiveDate,
	IRC.CoverageGroupCode,
	CA.Vehiclenumber,
	FIRST_VALUE(cl.CoverageLimitType) OVER (partition by clb.PremiumTransactionAKID order by clb.CreatedDate desc, cl.CoverageLimitType desc) as CoverageLimitType,
	PT.PremiumTransactionAKID,
	ISNULL(CA.IncludeUIM,'N/A') IncludeUIM
	,'N/A' as SubjectToNoFault
	,'N/A' as CoordinationOfBenefits
	,'N/A' as CoveredByWorkersCompensationFlag
	,'N/A' as MedicalExpensesOption
	,'N/A' as DCT_CoverageType
	,CA.AdditionalLimitKS
	,CA.AdditionalLimitKY
	,CA.AdditionalLimitMN
	,CA.RatingZoneCode
	,CA.FullGlassIndicator
	from @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact PMF
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim ASL
	on PMF.AnnualStatementLineDimId=ASL.asl_dim_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC
	on PMF.EDWPremiumMasterCalculationPKID=PMC.PremiumMasterCalculationId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	on PT.PremiumTransactionAKID=PMC.PremiumTransactionAKID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
	on SC.StatisticalCoverageAKID=PT.StatisticalCoverageAKID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on PC.PolicyCoverageAKID=SC.PolicyCoverageAKID
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	on PC.RiskLocationAKID=RL.RiskLocationAKID
	join V2.policy POL
	on POL.pol_ak_id=RL.PolicyAKID
	and POL.crrnt_snpsht_flag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG
	on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
	and ISG.CurrentSnapshotFlag=1 
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.BureauStatisticalCode bsc
	on bsc.PremiumTransactionAKID = PT.PremiumTransactionAKID 
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRC
	on IRC.InsuranceReferenceCoverageDimId=PMF.InsuranceReferenceCoverageDimId
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CA
	on CA.PremiumTransactionID=PT.PremiumTransactionID
	and CA.CurrentSnapshotFlag=1
	left join (@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductibleBridge cdb
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductible cd
	on cdb.CoverageDeductibleId=cd.CoverageDeductibleId)
	on PT.PremiumTransactionAKID=cdb.PremiumTransactionAKId
	left join
	(
	SELECT DISTINCT
	CD.CoverageDeductibleType AS CoverageDeductibleType,
	CD.CoverageDeductibleValue AS CoverageDeductibleValue,
	SC.CoverageGUID AS CoverageGUID
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductible CD
	INNER JOIN  @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductibleBridge CDB
	ON CD.CoverageDeductibleId = CDB.CoverageDeductibleId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	ON CDB.PremiumTransactionAKId = PT.PremiumTransactionAKID
	AND PT.SourceSystemID='PMS'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
	ON PT.StatisticalCoverageAKID= SC.StatisticalCoverageAKID
	WHERE CD.CoverageDeductibleType in ('CollisionDeductible','ComprehensiveDeductible')
	) cd_1 on cd_1.CoverageGuid=SC.CoverageGuid
	left join
	(@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge clb
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit cl
	on clb.CoverageLimitId=cl.CoverageLimitId
	) on PT.PremiumTransactionAKID=clb.PremiumTransactionAKID and CoverageLimitValue <> 'BasicPIP'
	WHERE PMC.PremiumMasterRunDate between @Quarterstartdate and @Quarterenddate
	AND PMC.PremiumMasterTypeBureauCode IN ('AL','AN','AP') 
	AND PT.SourceSystemID='PMS'
	---AND RL.StateProvinceCode IN ('12','13','14','15','16','21','22','24','34','48') (line removed - RFC 126190)
	AND PMC.PremiumMasterPremiumType='D'
	AND PMC.PremiumMasterTransactionCode IN ('10','11','12','13','14','15','18','19','20','21','22','23','24','25','28','29','57','67') 
	AND PMC.PremiumMasterReasonAmendedCode NOT IN ( 'COL' , 'CWO')
	AND PMC.PremiumMasterPremium <>  0
	@{pipeline().parameters.WHERE_CLAUSE_1}
	
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
	LTRIM(RTRIM(REPLACE(CASE WHEN CHARINDEX(',',RC.ClassCode)<>0 and RC.CoverageType like 'NonOwned%' then SUBSTRING(RC.ClassCode,CHARINDEX(',',RC.ClassCode),6) 
	WHEN CHARINDEX(',',RC.ClassCode)<>0 and NOT(RC.CoverageType like 'NonOwned%') then SUBSTRING(RC.ClassCode,1,CHARINDEX(',',RC.ClassCode)-1) 
	ELSE RC.ClassCode END,',',''))) AS ClassCode ,
	RL.RiskTerritory,
	POL.pol_eff_date,
	PMC.PremiumMasterPremium,
	PC.TypeBureauCode,
	'N/A' AS RiskUnitGroup,
	PT.SourceSystemID,
	PMC.PremiumMasterTransactionCode,
	PMC.PremiumMasterReasonAmendedCode,
	RC.RiskType,
	RC.CoverageGUID,
	null as CoverageDeductibleType,
	null as CoverageDeductibleValue,
	PT.ConstructionCode,
	'N/A' as IsoFireProtectionCode,
	PT.PackageModificationAdjustmentGroupCode,
	'N/A' AS BureauCode1,
	'N/A' AS BureauCode2,
	'N/A' AS BureauCode4,
	POL.pol_exp_date,
	CASE WHEN sub_asl_num IS NULL THEN asl_num WHEN ltrim(rtrim(sub_asl_num))= 'N/A' THEN asl_num ELSE sub_asl_num END AS ASL_NUM,
	ISG.InsuranceSegmentCode,
	RL.ZipPostalCode,
	PremiumMasterExposure,
	CASE WHEN RC.CoverageType = 'PIP' THEN 
	CASE WHEN CA.PIPBureaucoverageCode IS NULL OR PIPBureaucoverageCode='N/A' THEN LTRIM(RTRIM(IRC.CoverageCode)) ELSE CA.PIPBureaucoverageCode END ELSE LTRIM(RTRIM(IRC.CoverageCode)) END  AS CoverageCode,
	ISNULL(CA.VehicleYear,'N/A') as VehicleYear,
	RIGHT (RTRIM(FIRST_VALUE(cl.CoverageLimitValue) OVER (partition by clb.PremiumTransactionAKID order by clb.CreatedDate desc, cl.CoverageLimitValue desc)),5) as CoverageLimitValue,
	CA.TerminalZoneCode,
	PT.DeductibleBasis,
	CA.PIPBureaucoverageCode,
	PT.PremiumTransactionEffectiveDate,
	IRC.CoverageGroupCode,
	CA.Vehiclenumber,
	FIRST_VALUE(cl.CoverageLimitType) OVER (partition by clb.PremiumTransactionAKID order by clb.CreatedDate desc, cl.CoverageLimitType desc) as CoverageLimitType,
	PT.PremiumTransactionAKID,
	ISNULL(CA.IncludeUIM,'N/A') IncludeUIM
	
	,ISNULL(CA.SubjectToNoFault,'N/A') as SubjectToNoFault
	,ISNULL(CA.CoordinationOfBenefits,'N/A') as CoordinationOfBenefits
	,iif(ISNULL(CA.CoveredByWorkersCompensationFlag,0) = 1,'T','F') as CoveredByWorkersCompensationFlag
	,ISNULL(CA.MedicalExpensesOption,'N/A') as MedicalExpensesOption
	,RC.CoverageType as DCT_CoverageType
	,CA.AdditionalLimitKS
	,CA.AdditionalLimitKY
	,CA.AdditionalLimitMN
	,CA.RatingZoneCode
	,CA.FullGlassIndicator
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact PMF
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim ASL
	on PMF.AnnualStatementLineDimId=ASL.asl_dim_id
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC
	on PMF.EDWPremiumMasterCalculationPKID=PMC.PremiumMasterCalculationId
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	on PT.PremiumTransactionAKID=PMC.PremiumTransactionAKID
	and PMC.CurrentSnapshotFlag=1
	and PT.CurrentSnapshotFlag=1
	join  @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	on PT.RatingCoverageAKID=RC.RatingCoverageAKID
	and RC.EffectiveDate=PT.EffectiveDate 
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
	and PC.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	on PC.RiskLocationAKID=RL.RiskLocationAKID
	and RL.CurrentSnapshotFlag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL
	on POL.pol_ak_id=RL.PolicyAKID
	and POL.crrnt_snpsht_flag=1
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG
	on POL.InsuranceSegmentAKId=ISG.InsuranceSegmentAKId
	and ISG.CurrentSnapshotFlag=1 
	join @{pipeline().parameters.SOURCE_DATABASE_NAME_DATAMART}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceCoverageDim IRC
	on IRC.InsuranceReferenceCoverageDimId=PMF.InsuranceReferenceCoverageDimId
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CA
	on CA.PremiumTransactionID=PT.PremiumTransactionID
	and CA.CurrentSnapshotFlag=1
	--left join (@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductibleBridge cdb
	--join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDeductible cd
	--on cdb.CoverageDeductibleId=cd.CoverageDeductibleId)
	--on PT.PremiumTransactionAKID=cdb.PremiumTransactionAKId
	left join
	(@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimitBridge clb
	join @{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageLimit cl
	on clb.CoverageLimitId=cl.CoverageLimitId
	)
	on PT.PremiumTransactionAKID=clb.PremiumTransactionAKID
	and CoverageLimitValue <> 'BasicPIP'
	where PMC.PremiumMasterRunDate between @Quarterstartdate and @Quarterenddate
	AND PC.InsuranceLine = 'CommercialAuto'
	AND RC.CoverageType <> 'WB516CA' 
	AND PT.SourceSystemID='DCT'
	--AND RL.StateProvinceCode IN ('12','13','14','15','16','21','22','24','34','48') (line removed - RFC 126190)
	AND PMC.PremiumMasterPremiumType='D'
	AND PMC.PremiumMasterTransactionCode IN ('10','11','12','13','14','15','18','19','20','21','22','23','24','25','28','29','30','31','57','67') 
	AND PMC.PremiumMasterReasonAmendedCode NOT IN ('CWO', 'CWB')
	AND PMC.PremiumMasterPremium <>0
	@{pipeline().parameters.WHERE_CLAUSE_1}
),
EXP_Premium_Input AS (
	SELECT
	PremiumMasterCalculationID,
	PremiumMasterRunDate,
	pol_key,
	prim_bus_class_code,
	StateProvinceCode,
	PremiumTransactionBookedDate,
	PremiumMasterSubLine,
	ClassCode,
	RiskTerritory,
	pol_eff_date,
	PremiumMasterPremium,
	TypeBureauCode,
	RiskUnitGroup,
	SourceSystemID,
	PremiumMasterTransactionCode,
	PremiumMasterReasonAmendedCode,
	RiskType,
	CoverageGUID,
	CoverageDeductibleType,
	DeductibleAmount,
	ConstructionCode,
	IsoFireProtectionCode,
	PackageModificationAdjustmentGroupCode,
	BureauCode1,
	BureauCode2,
	BureauCode4,
	pol_exp_date,
	asl_num,
	InsuranceSegmentCode,
	ZipPostalCode,
	PremiumMasterExposure,
	CoverageCode,
	VehicleYear,
	CoverageLimitValue,
	TerminalZoneCode,
	DeductibleBasis,
	PIPBureaucoverageCode,
	PremiumTransactionEffectiveDate,
	CoverageGroupCode,
	VehicleNumber,
	CoverageLimitType,
	PremiumTransactionAKID,
	IncludeUIM,
	SubjectToNoFault,
	CoordinationOfBenefits,
	CoveredByWorkersCompensation,
	MedicalExpensesOption,
	DCT_CoverageType,
	AdditionalLimitKS,
	AdditionalLimitKY,
	AdditionalLimitMN,
	RatingZoneCode,
	FullGlassIndicator
	FROM SQ_Premium
),
SRT_Premium_Sort_Order AS (
	SELECT
	pol_key, 
	CoverageGUID, 
	ClassCode, 
	CoverageCode, 
	PremiumMasterPremium, 
	PremiumMasterCalculationID, 
	PremiumMasterRunDate, 
	prim_bus_class_code, 
	StateProvinceCode, 
	PremiumTransactionBookedDate, 
	PremiumMasterSubLine, 
	RiskTerritory, 
	pol_eff_date, 
	TypeBureauCode, 
	RiskUnitGroup, 
	SourceSystemID, 
	PremiumMasterTransactionCode, 
	PremiumMasterReasonAmendedCode, 
	RiskType, 
	CoverageDeductibleType, 
	DeductibleAmount, 
	ConstructionCode, 
	IsoFireProtectionCode, 
	PackageModificationAdjustmentGroupCode, 
	BureauCode1, 
	BureauCode2, 
	BureauCode4, 
	pol_exp_date, 
	asl_num, 
	InsuranceSegmentCode, 
	ZipPostalCode, 
	PremiumMasterExposure, 
	VehicleYear, 
	CoverageLimitValue, 
	TerminalZoneCode, 
	DeductibleBasis, 
	PIPBureaucoverageCode, 
	PremiumTransactionEffectiveDate, 
	CoverageGroupCode, 
	VehicleNumber, 
	CoverageLimitType, 
	PremiumTransactionAKID, 
	IncludeUIM, 
	SubjectToNoFault, 
	CoordinationOfBenefits, 
	CoveredByWorkersCompensation, 
	MedicalExpensesOption, 
	DCT_CoverageType, 
	AdditionalLimitKS, 
	AdditionalLimitKY, 
	AdditionalLimitMN, 
	RatingZoneCode, 
	FullGlassIndicator
	FROM EXP_Premium_Input
	ORDER BY pol_key ASC, CoverageGUID ASC, ClassCode ASC, CoverageCode ASC, PremiumMasterCalculationID ASC
),
LKP_ISSWorkTable_Premium AS (
	SELECT
	EDWPremiumMasterCalculationPKId
	FROM (
		SELECT 
			EDWPremiumMasterCalculationPKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ISSCommercialAutoExtract
		WHERE DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,PremiumMasterRunDate),-1)=DATEADD(QUARTER,1+DATEDIFF(QUARTER,0,GETDATE())+@{pipeline().parameters.NO_OF_QUARTERS},-1) and
		EDWPremiumMasterCalculationPKId<>-1
		
		--YEAR(PremiumMasterRunDate)=YEAR(dateadd(year,@{pipeline().parameters.NO_OF_YEARS},GETDATE())) and EDWPremiumMasterCalculationPKId<>-1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPremiumMasterCalculationPKId ORDER BY EDWPremiumMasterCalculationPKId) = 1
),
FIL_Exists_Premium AS (
	SELECT
	LKP_ISSWorkTable_Premium.EDWPremiumMasterCalculationPKId AS LKP_PremiumMasterCalculationID, 
	SRT_Premium_Sort_Order.PremiumMasterCalculationID, 
	SRT_Premium_Sort_Order.PremiumMasterRunDate, 
	SRT_Premium_Sort_Order.pol_key, 
	SRT_Premium_Sort_Order.prim_bus_class_code, 
	SRT_Premium_Sort_Order.StateProvinceCode, 
	SRT_Premium_Sort_Order.PremiumTransactionBookedDate, 
	SRT_Premium_Sort_Order.PremiumMasterSubLine, 
	SRT_Premium_Sort_Order.ClassCode, 
	SRT_Premium_Sort_Order.RiskTerritory, 
	SRT_Premium_Sort_Order.pol_eff_date, 
	SRT_Premium_Sort_Order.PremiumMasterPremium, 
	SRT_Premium_Sort_Order.TypeBureauCode, 
	SRT_Premium_Sort_Order.RiskUnitGroup, 
	SRT_Premium_Sort_Order.SourceSystemID, 
	SRT_Premium_Sort_Order.PremiumMasterTransactionCode, 
	SRT_Premium_Sort_Order.PremiumMasterReasonAmendedCode, 
	SRT_Premium_Sort_Order.RiskType, 
	SRT_Premium_Sort_Order.CoverageDeductibleType, 
	SRT_Premium_Sort_Order.DeductibleAmount, 
	SRT_Premium_Sort_Order.ConstructionCode, 
	SRT_Premium_Sort_Order.IsoFireProtectionCode, 
	SRT_Premium_Sort_Order.PackageModificationAdjustmentGroupCode, 
	SRT_Premium_Sort_Order.BureauCode1, 
	SRT_Premium_Sort_Order.BureauCode2, 
	SRT_Premium_Sort_Order.BureauCode4, 
	SRT_Premium_Sort_Order.pol_exp_date, 
	SRT_Premium_Sort_Order.asl_num, 
	SRT_Premium_Sort_Order.InsuranceSegmentCode, 
	SRT_Premium_Sort_Order.ZipPostalCode, 
	SRT_Premium_Sort_Order.PremiumMasterExposure, 
	SRT_Premium_Sort_Order.CoverageCode, 
	SRT_Premium_Sort_Order.VehicleYear, 
	SRT_Premium_Sort_Order.CoverageLimitValue, 
	SRT_Premium_Sort_Order.TerminalZoneCode, 
	SRT_Premium_Sort_Order.DeductibleBasis, 
	SRT_Premium_Sort_Order.PIPBureaucoverageCode, 
	SRT_Premium_Sort_Order.PremiumTransactionEffectiveDate, 
	SRT_Premium_Sort_Order.CoverageGroupCode, 
	SRT_Premium_Sort_Order.CoverageGUID, 
	SRT_Premium_Sort_Order.VehicleNumber, 
	SRT_Premium_Sort_Order.CoverageLimitType, 
	SRT_Premium_Sort_Order.PremiumTransactionAKID, 
	SRT_Premium_Sort_Order.IncludeUIM, 
	SRT_Premium_Sort_Order.SubjectToNoFault, 
	SRT_Premium_Sort_Order.CoordinationOfBenefits, 
	SRT_Premium_Sort_Order.CoveredByWorkersCompensation, 
	SRT_Premium_Sort_Order.MedicalExpensesOption, 
	SRT_Premium_Sort_Order.DCT_CoverageType, 
	SRT_Premium_Sort_Order.AdditionalLimitKS, 
	SRT_Premium_Sort_Order.AdditionalLimitKY, 
	SRT_Premium_Sort_Order.AdditionalLimitMN, 
	SRT_Premium_Sort_Order.RatingZoneCode, 
	SRT_Premium_Sort_Order.FullGlassIndicator
	FROM SRT_Premium_Sort_Order
	LEFT JOIN LKP_ISSWorkTable_Premium
	ON LKP_ISSWorkTable_Premium.EDWPremiumMasterCalculationPKId = SRT_Premium_Sort_Order.PremiumMasterCalculationID
	WHERE ISNULL(LKP_PremiumMasterCalculationID) and PremiumMasterPremium !=0
--EDWP-4232 temprary solution-------------------------------------
--AND ROUND(PremiumMasterPremium,2)<>0
--EDWP-4232 temprary solution-------------------------------------
),
EXP_GetParentCoveageGUID_Premium AS (
	SELECT
	PremiumMasterCalculationID,
	PremiumMasterRunDate,
	pol_key,
	prim_bus_class_code,
	StateProvinceCode,
	PremiumTransactionBookedDate,
	PremiumMasterSubLine,
	ClassCode,
	RiskTerritory,
	pol_eff_date,
	PremiumMasterPremium,
	TypeBureauCode,
	RiskUnitGroup,
	SourceSystemID,
	PremiumMasterTransactionCode,
	PremiumMasterReasonAmendedCode,
	RiskType,
	CoverageDeductibleType,
	DeductibleAmount,
	ConstructionCode,
	IsoFireProtectionCode,
	PackageModificationAdjustmentGroupCode,
	BureauCode1,
	BureauCode2,
	BureauCode4,
	pol_exp_date,
	asl_num,
	InsuranceSegmentCode,
	ZipPostalCode,
	PremiumMasterExposure,
	CoverageCode,
	VehicleYear,
	CoverageLimitValue,
	TerminalZoneCode,
	DeductibleBasis,
	PIPBureaucoverageCode,
	PremiumTransactionEffectiveDate,
	CoverageGroupCode,
	CoverageGUID,
	VehicleNumber,
	CoverageLimitType,
	PremiumTransactionAKID,
	IncludeUIM,
	SubjectToNoFault,
	CoordinationOfBenefits,
	CoveredByWorkersCompensation,
	MedicalExpensesOption,
	DCT_CoverageType,
	-- *INF*: SUBSTR(pol_key, 0, LENGTH(pol_key)-2)
	SUBSTR(pol_key, 0, LENGTH(pol_key) - 2) AS o_PolicyNumber,
	-- *INF*: SUBSTR(pol_key, -2, 2)
	SUBSTR(pol_key, - 2, 2) AS o_PolicyVersionFormatted,
	AdditionalLimitKS,
	AdditionalLimitKY,
	AdditionalLimitMN,
	RatingZoneCode,
	FullGlassIndicator
	FROM FIL_Exists_Premium
),
RTR_Separate_Missing_Deductible_Records AS (
	SELECT
	PremiumMasterCalculationID,
	PremiumMasterRunDate,
	pol_key,
	prim_bus_class_code,
	StateProvinceCode,
	PremiumTransactionBookedDate,
	PremiumMasterSubLine,
	ClassCode,
	RiskTerritory,
	pol_eff_date,
	PremiumMasterPremium,
	TypeBureauCode,
	RiskUnitGroup,
	SourceSystemID,
	PremiumMasterTransactionCode,
	PremiumMasterReasonAmendedCode,
	RiskType,
	CoverageDeductibleType,
	DeductibleAmount,
	ConstructionCode,
	IsoFireProtectionCode,
	PackageModificationAdjustmentGroupCode,
	BureauCode1,
	BureauCode2,
	BureauCode4,
	pol_exp_date,
	asl_num,
	InsuranceSegmentCode,
	ZipPostalCode,
	PremiumMasterExposure,
	CoverageCode,
	VehicleYear,
	CoverageLimitValue,
	TerminalZoneCode,
	DeductibleBasis,
	PIPBureaucoverageCode,
	PremiumTransactionEffectiveDate,
	CoverageGroupCode,
	CoverageGUID,
	VehicleNumber,
	CoverageLimitType,
	PremiumTransactionAKID,
	IncludeUIM,
	SubjectToNoFault,
	CoordinationOfBenefits,
	CoveredByWorkersCompensation,
	MedicalExpensesOption,
	DCT_CoverageType,
	AdditionalLimitKS,
	AdditionalLimitKY,
	AdditionalLimitMN,
	RatingZoneCode,
	FullGlassIndicator
	FROM EXP_GetParentCoveageGUID_Premium
),
RTR_Separate_Missing_Deductible_Records_MissingDeductibleAmount AS (SELECT * FROM RTR_Separate_Missing_Deductible_Records WHERE ISNULL(DeductibleAmount) OR DeductibleAmount='0' OR DeductibleAmount='N/A'),
RTR_Separate_Missing_Deductible_Records_DEFAULT1 AS (SELECT * FROM RTR_Separate_Missing_Deductible_Records WHERE NOT ( (ISNULL(DeductibleAmount) OR DeductibleAmount='0' OR DeductibleAmount='N/A') )),
EXP_DoDeductibleLookupsPremium AS (
	SELECT
	pol_key,
	CoverageGUID,
	-- *INF*: :LKP.LKP_GET_PARENT_COVERAGEGUID(pol_key,CoverageGUID)
	LKP_GET_PARENT_COVERAGEGUID_pol_key_CoverageGUID.archWorkDCTCoverageTransactionId AS v_archWorkDCTCoverageTransactionId,
	-- *INF*: IIF(NOT ISNULL(v_archWorkDCTCoverageTransactionId),:LKP.LKP_ALT_COVERAGEGUID(v_archWorkDCTCoverageTransactionId),NULL)
	IFF(
	    v_archWorkDCTCoverageTransactionId IS NOT NULL,
	    LKP_ALT_COVERAGEGUID_v_archWorkDCTCoverageTransactionId.CoverageGuid,
	    NULL
	) AS v_altCoverageGuid,
	-- *INF*: --IIF(NOT ISNULL(v_altCoverageGuid) ,:LKP.LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_COMBINED_OUTPUT(v_altCoverageGuid,pol_key),NULL)
	-- 
	-- IIF(ISNULL(v_altCoverageGuid),:LKP.LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_COMBINED_OUTPUT(CoverageGUID,pol_key),:LKP.LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_COMBINED_OUTPUT(v_altCoverageGuid,pol_key))
	IFF(
	    v_altCoverageGuid IS NULL,
	    LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_COMBINED_OUTPUT_CoverageGUID_pol_key.CombinedCoverageDeductibleInfo,
	    LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_COMBINED_OUTPUT_v_altCoverageGuid_pol_key.CombinedCoverageDeductibleInfo
	) AS v_combinedCoverageDeductibleValueType,
	-- *INF*: IIF(NOT ISNULL(v_combinedCoverageDeductibleValueType), SUBSTR(v_combinedCoverageDeductibleValueType,0,INSTR(v_combinedCoverageDeductibleValueType,'|')-1),'')
	IFF(
	    v_combinedCoverageDeductibleValueType IS NOT NULL,
	    SUBSTR(v_combinedCoverageDeductibleValueType, 0, REGEXP_INSTR(v_combinedCoverageDeductibleValueType, '|') - 1),
	    ''
	) AS o_splitCoverageDeductibleValue,
	-- *INF*: IIF(NOT ISNULL(v_combinedCoverageDeductibleValueType), SUBSTR(v_combinedCoverageDeductibleValueType,INSTR(v_combinedCoverageDeductibleValueType,'|')+1),'')
	IFF(
	    v_combinedCoverageDeductibleValueType IS NOT NULL,
	    SUBSTR(v_combinedCoverageDeductibleValueType, REGEXP_INSTR(v_combinedCoverageDeductibleValueType, '|') + 1),
	    ''
	) AS o_splitCoverageDeductibleType
	FROM RTR_Separate_Missing_Deductible_Records_MissingDeductibleAmount
	LEFT JOIN LKP_GET_PARENT_COVERAGEGUID LKP_GET_PARENT_COVERAGEGUID_pol_key_CoverageGUID
	ON LKP_GET_PARENT_COVERAGEGUID_pol_key_CoverageGUID.CoverageGUID = pol_key
	AND LKP_GET_PARENT_COVERAGEGUID_pol_key_CoverageGUID.PolicyKey = CoverageGUID

	LEFT JOIN LKP_ALT_COVERAGEGUID LKP_ALT_COVERAGEGUID_v_archWorkDCTCoverageTransactionId
	ON LKP_ALT_COVERAGEGUID_v_archWorkDCTCoverageTransactionId.archWorkDCTCoverageTransactionid = v_archWorkDCTCoverageTransactionId

	LEFT JOIN LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_COMBINED_OUTPUT LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_COMBINED_OUTPUT_CoverageGUID_pol_key
	ON LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_COMBINED_OUTPUT_CoverageGUID_pol_key.CoverageGUID = CoverageGUID
	AND LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_COMBINED_OUTPUT_CoverageGUID_pol_key.pol_key = pol_key

	LEFT JOIN LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_COMBINED_OUTPUT LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_COMBINED_OUTPUT_v_altCoverageGuid_pol_key
	ON LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_COMBINED_OUTPUT_v_altCoverageGuid_pol_key.CoverageGUID = v_altCoverageGuid
	AND LKP_COVERAGEDEDUCTIBLE_PREMIUM_ALT_COMBINED_OUTPUT_v_altCoverageGuid_pol_key.pol_key = pol_key

),
Union_Rejoin_Premium AS (
	SELECT PremiumMasterCalculationID AS PremiumMasterCalculationID2, PremiumMasterRunDate AS PremiumMasterRunDate2, pol_key AS pol_key2, prim_bus_class_code AS prim_bus_class_code2, StateProvinceCode AS StateProvinceCode2, PremiumTransactionBookedDate AS PremiumTransactionBookedDate2, PremiumMasterSubLine AS PremiumMasterSubLine2, ClassCode AS ClassCode2, RiskTerritory AS RiskTerritory2, pol_eff_date AS pol_eff_date2, PremiumMasterPremium AS PremiumMasterPremium2, TypeBureauCode AS TypeBureauCode2, RiskUnitGroup AS RiskUnitGroup2, SourceSystemID AS SourceSystemID2, PremiumMasterTransactionCode AS PremiumMasterTransactionCode2, PremiumMasterReasonAmendedCode AS PremiumMasterReasonAmendedCode2, RiskType AS RiskType2, CoverageDeductibleType AS CoverageDeductibleType2, DeductibleAmount AS DeductibleAmount2, ConstructionCode AS ConstructionCode2, IsoFireProtectionCode AS IsoFireProtectionCode2, PackageModificationAdjustmentGroupCode AS PackageModificationAdjustmentGroupCode2, BureauCode1 AS BureauCode12, BureauCode AS BureauCode22, BureauCode4 AS BureauCode42, pol_exp_date AS pol_exp_date2, asl_num AS asl_num2, InsuranceSegmentCode AS InsuranceSegmentCode2, ZipPostalCode AS ZipPostalCode2, PremiumMasterExposure AS PremiumMasterExposure2, CoverageCode AS CoverageCode2, VehicleYear AS VehicleYear2, CoverageLimitValue AS CoverageLimitValue2, TerminalZoneCode AS TerminalZoneCode2, DeductibleBasis AS DeductibleBasis2, PIPBureaucoverageCode AS PIPBureaucoverageCode2, PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate2, CoverageGroupCode AS CoverageGroupCode2, CoverageGUID AS CoverageGUID2, VehicleNumber AS VehicleNumber1, CoverageLimitType AS CoverageLimitType2, PremiumTransactionAKID AS PremiumTransactionAKID1, IncludeUIM, SubjectToNoFault, CoordinationOfBenefits, CoveredByWorkersCompensation, MedicalExpensesOption AS MedicalExpenseOption, DCT_CoverageType, AdditionalLimitKS AS AdditionalLimitKS1, AdditionalLimitKY AS AdditionalLimitKY1, AdditionalLimitMN AS AdditionalLimitMN1, RatingZoneCode, FullGlassIndicator AS FullGlassIndicator1
	FROM RTR_Separate_Missing_Deductible_Records_DEFAULT1
	UNION
	SELECT PremiumMasterCalculationID AS PremiumMasterCalculationID2, PremiumMasterRunDate AS PremiumMasterRunDate2, pol_key AS pol_key2, prim_bus_class_code AS prim_bus_class_code2, StateProvinceCode AS StateProvinceCode2, PremiumTransactionBookedDate AS PremiumTransactionBookedDate2, PremiumMasterSubLine AS PremiumMasterSubLine2, ClassCode AS ClassCode2, RiskTerritory AS RiskTerritory2, pol_eff_date AS pol_eff_date2, PremiumMasterPremium AS PremiumMasterPremium2, TypeBureauCode AS TypeBureauCode2, RiskUnitGroup AS RiskUnitGroup2, SourceSystemID AS SourceSystemID2, PremiumMasterTransactionCode AS PremiumMasterTransactionCode2, PremiumMasterReasonAmendedCode AS PremiumMasterReasonAmendedCode2, RiskType AS RiskType2, o_splitCoverageDeductibleType AS CoverageDeductibleType2, o_splitCoverageDeductibleValue AS DeductibleAmount2, ConstructionCode AS ConstructionCode2, IsoFireProtectionCode AS IsoFireProtectionCode2, PackageModificationAdjustmentGroupCode AS PackageModificationAdjustmentGroupCode2, BureauCode AS BureauCode12, BureauCode2 AS BureauCode22, BureauCode4 AS BureauCode42, pol_exp_date AS pol_exp_date2, asl_num AS asl_num2, InsuranceSegmentCode AS InsuranceSegmentCode2, ZipPostalCode AS ZipPostalCode2, PremiumMasterExposure AS PremiumMasterExposure2, CoverageCode AS CoverageCode2, VehicleYear AS VehicleYear2, CoverageLimitValue AS CoverageLimitValue2, TerminalZoneCode AS TerminalZoneCode2, DeductibleBasis AS DeductibleBasis2, PIPBureaucoverageCode AS PIPBureaucoverageCode2, PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate2, CoverageGroupCode AS CoverageGroupCode2, CoverageGUID AS CoverageGUID2, VehicleNumber AS VehicleNumber1, CoverageLimitType AS CoverageLimitType2, PremiumTransactionAKID AS PremiumTransactionAKID1, IncludeUIM, SubjectToNoFault, CoordinationOfBenefits, CoveredByWorkersCompensation, MedicalExpensesOption AS MedicalExpenseOption, DCT_CoverageType, AdditionalLimitKS AS AdditionalLimitKS1, AdditionalLimitKY AS AdditionalLimitKY1, AdditionalLimitMN AS AdditionalLimitMN1, RatingZoneCode, FullGlassIndicator AS FullGlassIndicator1
	FROM EXP_DoDeductibleLookupsPremium
	-- Manually join with RTR_Separate_Missing_Deductible_Records_MissingDeductibleAmount
),
EXP_Cleansing_Premium AS (
	SELECT
	PremiumMasterCalculationID2 AS i_PremiumMasterCalculationID,
	PremiumMasterRunDate2 AS i_PremiumMasterRunDate,
	pol_key2 AS i_pol_key,
	prim_bus_class_code2 AS i_prim_bus_class_code,
	StateProvinceCode2 AS i_StateProvinceCode,
	PremiumTransactionBookedDate2 AS i_PremiumTransactionBookedDate,
	PremiumMasterSubLine2 AS i_PremiumMasterSubLine,
	ClassCode2 AS i_ClassCode,
	RiskTerritory2 AS i_RiskTerritory,
	pol_eff_date2 AS i_pol_eff_date,
	PremiumMasterPremium2 AS i_PremiumMasterPremium,
	TypeBureauCode2 AS i_TypeBureauCode,
	RiskUnitGroup2 AS i_RiskUnitGroup,
	SourceSystemID2 AS i_SourceSystemID,
	PremiumMasterTransactionCode2 AS i_PremiumMasterTransactionCode,
	PremiumMasterReasonAmendedCode2 AS i_PremiumMasterReasonAmendedCode,
	RiskType2 AS i_RiskType,
	CoverageDeductibleType2 AS i_CoverageDeductibleType,
	DeductibleAmount2 AS i_DeductibleAmount,
	ConstructionCode2 AS i_ConstructionCode,
	IsoFireProtectionCode2 AS i_IsoFireProtectionCode,
	PackageModificationAdjustmentGroupCode2 AS i_PackageModificationAdjustmentGroupCode,
	BureauCode12 AS i_BureauCode1,
	pol_exp_date2 AS i_pol_exp_date,
	asl_num2 AS i_asl_num,
	PIPBureaucoverageCode2 AS PIPBureaucoverageCode,
	CoverageLimitType2 AS CoverageLimitType,
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
	-- --IIF( IN(i_PremiumMasterTransactionCode, '10','11','12','13','14','15','18','19','20','21','22','23','24','25','28','29','57','67') AND  NOT IN(i_PremiumMasterReasonAmendedCode, 'COL' , 'CWO'), i_PremiumMasterPremium, 0)
	i_PremiumMasterPremium AS o_PremiumMasterPremium,
	-- *INF*: IIF(ISNULL(i_TypeBureauCode),'N/A',RTRIM(LTRIM(i_TypeBureauCode)))
	IFF(i_TypeBureauCode IS NULL, 'N/A', RTRIM(LTRIM(i_TypeBureauCode))) AS o_TypeBureauCode,
	-- *INF*: RTRIM(LTRIM(i_RiskUnitGroup))
	RTRIM(LTRIM(i_RiskUnitGroup)) AS o_RiskUnitGroup,
	-- *INF*: LTRIM(RTRIM(i_SourceSystemID))
	LTRIM(RTRIM(i_SourceSystemID)) AS o_SourceSystemID,
	-- *INF*: LTRIM(RTRIM(i_RiskType))
	LTRIM(RTRIM(i_RiskType)) AS o_RiskType,
	i_DeductibleAmount AS o_DeductibleAmount,
	-- *INF*: RTRIM(LTRIM(i_ConstructionCode))
	RTRIM(LTRIM(i_ConstructionCode)) AS o_ConstructionCode,
	-- *INF*: RTRIM(LTRIM(i_IsoFireProtectionCode))
	RTRIM(LTRIM(i_IsoFireProtectionCode)) AS o_IsoFireProtectionCode,
	-- *INF*: LTRIM(RTRIM(i_PackageModificationAdjustmentGroupCode))
	LTRIM(RTRIM(i_PackageModificationAdjustmentGroupCode)) AS o_PackageModificationAdjustmentGroupCode,
	-- *INF*: RTRIM(LTRIM(i_BureauCode1))
	RTRIM(LTRIM(i_BureauCode1)) AS o_BureauCode1,
	BureauCode22 AS BureauCode2,
	BureauCode42 AS BureauCode4,
	i_pol_exp_date AS o_pol_exp_date,
	i_asl_num AS o_AnnualStatementLineNumber,
	InsuranceSegmentCode2 AS InsuranceSegmentCode,
	ZipPostalCode2 AS i_ZipPostalCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ZipPostalCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_ZipPostalCode) AS o_ZipPostalCode,
	PremiumMasterExposure2 AS PremiumMasterExposure,
	CoverageCode2 AS i_CoverageCode,
	-- *INF*: iif(in(i_CoverageCode,'COMRLIAB','COMRLIABUM','COMRLIABUIM','COMRLIABMEDICAL','COMRLIABPIP'),'050',i_CoverageCode)
	IFF(
	    i_CoverageCode IN ('COMRLIAB','COMRLIABUM','COMRLIABUIM','COMRLIABMEDICAL','COMRLIABPIP'),
	    '050',
	    i_CoverageCode
	) AS o_CoverageCode,
	VehicleYear2 AS VehicleYear,
	CoverageLimitValue2 AS CoverageLimitValue,
	TerminalZoneCode2 AS TerminalZoneCode,
	DeductibleBasis2 AS DeductibleBasis,
	PremiumTransactionEffectiveDate2 AS PremiumTransactionEffectiveDate,
	CoverageGroupCode2 AS CoverageGroupCode,
	VehicleNumber1 AS VehicleNumber,
	-- *INF*: IIF(ISNULL(VehicleNumber),'0',LTRIM(RTRIM(VehicleNumber)))
	IFF(VehicleNumber IS NULL, '0', LTRIM(RTRIM(VehicleNumber))) AS o_VehicleNumber,
	PremiumTransactionAKID1,
	IncludeUIM,
	SubjectToNoFault,
	CoordinationOfBenefits,
	CoveredByWorkersCompensation,
	MedicalExpenseOption,
	DCT_CoverageType,
	AdditionalLimitKS1 AS i_AdditionalLimitKS,
	AdditionalLimitKY1 AS i_AdditionalLimitKY,
	AdditionalLimitMN1 AS i_AdditionalLimitMN,
	-- *INF*: IIF(ISNULL(i_AdditionalLimitKS), -1, i_AdditionalLimitKS)
	IFF(i_AdditionalLimitKS IS NULL, - 1, i_AdditionalLimitKS) AS o_AdditionalLimitKS,
	-- *INF*: IIF(ISNULL(i_AdditionalLimitKY), -1, i_AdditionalLimitKY)
	IFF(i_AdditionalLimitKY IS NULL, - 1, i_AdditionalLimitKY) AS o_AdditionalLimitKY,
	-- *INF*: IIF(ISNULL(i_AdditionalLimitMN), -1, i_AdditionalLimitMN)
	IFF(i_AdditionalLimitMN IS NULL, - 1, i_AdditionalLimitMN) AS o_AdditionalLimitMN,
	RatingZoneCode,
	FullGlassIndicator1 AS FullGlassIndicator
	FROM Union_Rejoin_Premium
),
LKP_ExcessAttendantCare_Coverage_Prem AS (
	SELECT
	lu_PolicyKey,
	i_PolicyKey,
	i_EffectiveDate,
	i_ExpirationDate,
	lu_EffectiveDate,
	lu_ExpirationDate
	FROM (
		select distinct POL.pol_key, RC.EffectiveDate, RC.ExpirationDate 
		
		from v2.policy POL
		
		inner join PolicyCoverage PC
		on PC.PolicyAKID = POL.pol_ak_id
		and PC.CurrentSnapshotFlag = 1
		
		inner join RatingCoverage RC
		on RC.PolicyCoverageAKID = PC.PolicyCoverageAKID
		and RC.CoverageType = 'ExcessAttendantCare'
		and RC.CurrentSnapshotFlag = 1
		
		where POL.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY lu_PolicyKey,lu_EffectiveDate,lu_ExpirationDate ORDER BY lu_PolicyKey) = 1
),
EXP_Logic_Premium AS (
	SELECT
	-1 AS LossMasterCalculationId,
	EXP_Cleansing_Premium.o_PremiumMasterCalculationID AS PremiumMasterCalculationID,
	EXP_Cleansing_Premium.o_PremiumMasterRunDate AS PremiumMasterRunDate,
	-- *INF*: TO_DATE('1800-01-01','YYYY/MM/DD')
	TO_TIMESTAMP('1800-01-01', 'YYYY/MM/DD') AS loss_master_run_date,
	EXP_Cleansing_Premium.o_pol_key AS pol_key,
	EXP_Cleansing_Premium.o_prim_bus_class_code AS prim_bus_class_code,
	EXP_Cleansing_Premium.o_StateProvinceCode AS StateProvinceCode,
	EXP_Cleansing_Premium.o_PremiumTransactionBookedDate AS PremiumTransactionBookedDate,
	EXP_Cleansing_Premium.o_PremiumMasterSubLine AS PremiumMasterSubLine,
	-- *INF*: --Fix for EDWP-4028
	-- --DECODE(TRUE, 
	-- --IN(StateProvinceCode,'15', '16', '21', '22') AND --PremiumMasterSubLine='613','01',
	-- --IN(StateProvinceCode,'14','13', '12', '34', '24', '48') AND
	-- ---PremiumMasterSubLine='611','00',
	-- --IN(PremiumMasterSubLine,'618','648'),'00',
	-- --PremiumMasterSubLine='615','01',
	-- --IN(StateProvinceCode,'15', '16', '21', '22') AND
	-- --IN(PremiumMasterSubLine,'620', '621','622', '623', '641','645'),'01',
	-- --IN(StateProvinceCode,'14','13','12','34','24','48') AND
	-- --IN(PremiumMasterSubLine,'641','620', '621', '622', '623'),'00',
	-- --'N/A')
	-- --4352
	-- DECODE(TRUE,
	-- IN(i_CoverageCode,'COLL','COMPRH','COMRPD','CALNSECOMP','PLSPAK - BRD','CALNSECOL','TOWLABOR'),'00',
	-- IN(StateProvinceCode,'15','16','21','22')  AND  IN(i_CoverageCode,'ADLINS','BIPD','EMPLESSOR','FELEMPL',
	-- 'INJLEASEWRKS','LSECONCRN','MANU','MCCA','MEDPAY',
	-- 'MINPREM','PIP','POLLUTION','RACEXCL','RAILOPTS','UIM',
	-- 'UM','LOSSUSEEXP','LIMMEXCOV','PPI','671','672','681','682','695'),'01',
	-- '00')
	DECODE(
	    TRUE,
	    i_CoverageCode IN ('COLL','COMPRH','COMRPD','CALNSECOMP','PLSPAK - BRD','CALNSECOL','TOWLABOR'), '00',
	    StateProvinceCode IN ('15','16','21','22') AND i_CoverageCode IN ('ADLINS','BIPD','EMPLESSOR','FELEMPL','INJLEASEWRKS','LSECONCRN','MANU','MCCA','MEDPAY','MINPREM','PIP','POLLUTION','RACEXCL','RAILOPTS','UIM','UM','LOSSUSEEXP','LIMMEXCOV','PPI','671','672','681','682','695'), '01',
	    '00'
	) AS sub_line_code_out,
	EXP_Cleansing_Premium.o_ClassCode AS ClassCode,
	-- *INF*: IIF(ISNULL(ClassCode) OR IS_SPACES(ClassCode) OR LENGTH(ClassCode)=0
	-- OR IN(ClassCode, 'N/A','TBD'),
	-- '00000',
	-- ClassCode
	-- )
	IFF(
	    ClassCode IS NULL
	    or LENGTH(ClassCode)>0
	    and TRIM(ClassCode)=''
	    or LENGTH(ClassCode) = 0
	    or ClassCode IN ('N/A','TBD'),
	    '00000',
	    ClassCode
	) AS v_PremiumMasterClassCode,
	-- *INF*: IIF(ISNULL(ClassCode) OR IS_SPACES(ClassCode) OR LENGTH(ClassCode)=0
	-- OR IN(ClassCode, 'N/A','TBD'),
	-- '00000',
	-- ClassCode
	-- )
	-- 
	-- 
	-- --IIF(ISNULL(ClassCode) OR LENGTH(ClassCode)=0,'N/A',ClassCode)
	IFF(
	    ClassCode IS NULL
	    or LENGTH(ClassCode)>0
	    and TRIM(ClassCode)=''
	    or LENGTH(ClassCode) = 0
	    or ClassCode IN ('N/A','TBD'),
	    '00000',
	    ClassCode
	) AS PremiumMasterClassCode,
	'N/A' AS LossMasterClassCode,
	'N/A' AS Cause_of_Loss,
	EXP_Cleansing_Premium.o_RiskTerritory AS RiskTerritory,
	-- *INF*: RiskTerritory
	-- 
	-- --SUBSTR(RiskTerritory,2,2)
	RiskTerritory AS TerritoryCode,
	EXP_Cleansing_Premium.o_pol_eff_date AS pol_eff_date,
	'N/A' AS ClaimNum,
	'N/A' AS ClaimantNum,
	EXP_Cleansing_Premium.o_PremiumMasterPremium AS PremiumMasterPremium,
	-- *INF*: ROUND(PremiumMasterPremium,2)
	ROUND(PremiumMasterPremium, 2) AS PremiumMasterPremium_out,
	0.00 AS PaidLossAmt,
	0.00 AS OutstandingAmt,
	EXP_Cleansing_Premium.o_TypeBureauCode AS TypeBureauCode,
	EXP_Cleansing_Premium.o_RiskUnitGroup AS RiskUnitGroup,
	EXP_Cleansing_Premium.o_SourceSystemID AS SourceSystemID,
	EXP_Cleansing_Premium.o_RiskType AS RiskType,
	EXP_Cleansing_Premium.o_DeductibleAmount AS DeductibleAmount,
	-- *INF*: :LKP.LKP_COVERAGEDEDUCTIBLE_DCT(PremiumTransactionAKID1)
	LKP_COVERAGEDEDUCTIBLE_DCT_PremiumTransactionAKID1.CoverageDeductibleValue AS v_DeductibleAmount_DCT,
	-- *INF*: IIF(SourceSystemID = 'DCT', v_DeductibleAmount_DCT, DeductibleAmount)
	IFF(SourceSystemID = 'DCT', v_DeductibleAmount_DCT, DeductibleAmount) AS v_DeductibleAmount,
	-- *INF*: IIF(LENGTH(v_DeductibleAmount)=0 OR ISNULL(v_DeductibleAmount) OR v_DeductibleAmount='FullCoverage' OR DeductibleBasis='0','0',v_DeductibleAmount)
	IFF(
	    LENGTH(v_DeductibleAmount) = 0
	    or v_DeductibleAmount IS NULL
	    or v_DeductibleAmount = 'FullCoverage'
	    or DeductibleBasis = '0',
	    '0',
	    v_DeductibleAmount
	) AS DeductibleAmount_out,
	EXP_Cleansing_Premium.o_BureauCode1 AS BureauCode1,
	EXP_Cleansing_Premium.BureauCode2,
	EXP_Cleansing_Premium.BureauCode4,
	LKP_ExcessAttendantCare_Coverage_Prem.lu_PolicyKey,
	EXP_Cleansing_Premium.SubjectToNoFault,
	EXP_Cleansing_Premium.CoordinationOfBenefits,
	EXP_Cleansing_Premium.CoveredByWorkersCompensation,
	EXP_Cleansing_Premium.MedicalExpenseOption,
	EXP_Cleansing_Premium.DCT_CoverageType,
	EXP_Cleansing_Premium.o_AdditionalLimitKS AS AdditionalLimitKS,
	EXP_Cleansing_Premium.o_AdditionalLimitKY AS AdditionalLimitKY,
	EXP_Cleansing_Premium.o_AdditionalLimitMN AS AdditionalLimitMN,
	-- *INF*: DECODE(TRUE,
	-- MedicalExpenseOption='Rejected','689',
	-- IN(MedicalExpenseOption,'Unlimited','SelectedLimit','Excluded', 'N/A')
	--    AND CoveredByWorkersCompensation = 'T','671',
	-- IN(MedicalExpenseOption,'Unlimited','SelectedLimit','Excluded', 'N/A')
	--    AND CoveredByWorkersCompensation = 'F' 
	--    AND IN(CoordinationOfBenefits,'None','0','N/A'),'681',
	-- IN(MedicalExpenseOption,'Unlimited','SelectedLimit','Excluded', 'N/A')
	--    AND CoveredByWorkersCompensation = 'F' 
	--    AND CoordinationOfBenefits = 'MedicalExpenses','691',
	-- IN(MedicalExpenseOption,'Unlimited','SelectedLimit','Excluded', 'N/A')
	--    AND CoveredByWorkersCompensation = 'F' 
	--    AND CoordinationOfBenefits = 'WorkLoss','692',
	-- IN(MedicalExpenseOption,'Unlimited','SelectedLimit','Excluded', 'N/A')
	--    AND CoveredByWorkersCompensation = 'F' 
	--    AND CoordinationOfBenefits = 'MedicalWorkLoss','693',
	-- NOT ISNULL(lu_PolicyKey),'683',
	-- '685')
	DECODE(
	    TRUE,
	    MedicalExpenseOption = 'Rejected', '689',
	    MedicalExpenseOption IN ('Unlimited','SelectedLimit','Excluded','N/A') AND CoveredByWorkersCompensation = 'T', '671',
	    MedicalExpenseOption IN ('Unlimited','SelectedLimit','Excluded','N/A') AND CoveredByWorkersCompensation = 'F' AND CoordinationOfBenefits IN ('None','0','N/A'), '681',
	    MedicalExpenseOption IN ('Unlimited','SelectedLimit','Excluded','N/A') AND CoveredByWorkersCompensation = 'F' AND CoordinationOfBenefits = 'MedicalExpenses', '691',
	    MedicalExpenseOption IN ('Unlimited','SelectedLimit','Excluded','N/A') AND CoveredByWorkersCompensation = 'F' AND CoordinationOfBenefits = 'WorkLoss', '692',
	    MedicalExpenseOption IN ('Unlimited','SelectedLimit','Excluded','N/A') AND CoveredByWorkersCompensation = 'F' AND CoordinationOfBenefits = 'MedicalWorkLoss', '693',
	    lu_PolicyKey IS NOT NULL, '683',
	    '685'
	) AS v_MI_PIP_CoverageCode,
	-- *INF*: DECODE(TRUE,
	-- DCT_CoverageType = 'PIP' AND IN(TypeBureauCode,'AL','AN','AP','N/A','CommercialAuto'), '1',
	-- '0')
	DECODE(
	    TRUE,
	    DCT_CoverageType = 'PIP' AND TypeBureauCode IN ('AL','AN','AP','N/A','CommercialAuto'), '1',
	    '0'
	) AS v_CLFile_PIP,
	EXP_Cleansing_Premium.o_CoverageCode AS i_CoverageCode,
	-- *INF*: DECODE(TRUE,
	-- StateProvinceCode = '15' AND IN (AdditionalLimitKS,-1, 0) AND v_CLFile_PIP = '1', '681',
	-- StateProvinceCode = '15' AND NOT IN (AdditionalLimitKS,-1, 0) AND v_CLFile_PIP = '1', '682',
	-- i_CoverageCode)
	DECODE(
	    TRUE,
	    StateProvinceCode = '15' AND AdditionalLimitKS IN (- 1,0) AND v_CLFile_PIP = '1', '681',
	    StateProvinceCode = '15' AND NOT AdditionalLimitKS IN (- 1,0) AND v_CLFile_PIP = '1', '682',
	    i_CoverageCode
	) AS v_KS_PIP_CoverageCode,
	-- *INF*: DECODE(TRUE,
	-- StateProvinceCode = '16' AND IN (AdditionalLimitKY, -1, 0) AND CoveredByWorkersCompensation <> 'T' AND v_CLFile_PIP = '1', '681',
	-- StateProvinceCode = '16' AND  IN (AdditionalLimitKY, -1, 0) AND CoveredByWorkersCompensation = 'T' AND v_CLFile_PIP = '1', '671',	
	-- StateProvinceCode = '16' AND NOT IN (AdditionalLimitKY, -1, 0) AND CoveredByWorkersCompensation <> 'T' AND v_CLFile_PIP = '1', '682',
	-- StateProvinceCode = '16' AND  NOT IN (AdditionalLimitKY, -1, 0) AND CoveredByWorkersCompensation = 'T' AND v_CLFile_PIP = '1', '672',
	-- i_CoverageCode)
	DECODE(
	    TRUE,
	    StateProvinceCode = '16' AND AdditionalLimitKY IN (- 1,0) AND CoveredByWorkersCompensation <> 'T' AND v_CLFile_PIP = '1', '681',
	    StateProvinceCode = '16' AND AdditionalLimitKY IN (- 1,0) AND CoveredByWorkersCompensation = 'T' AND v_CLFile_PIP = '1', '671',
	    StateProvinceCode = '16' AND NOT AdditionalLimitKY IN (- 1,0) AND CoveredByWorkersCompensation <> 'T' AND v_CLFile_PIP = '1', '682',
	    StateProvinceCode = '16' AND NOT AdditionalLimitKY IN (- 1,0) AND CoveredByWorkersCompensation = 'T' AND v_CLFile_PIP = '1', '672',
	    i_CoverageCode
	) AS v_KY_PIP_CoverageCode,
	PIPBureaucoverageCode AS v_MN_PIP_CoverageCode,
	-- *INF*: DECODE(TRUE,
	-- 
	-- -- MI
	-- StateProvinceCode = '21' AND SubjectToNoFault = 'Yes' AND DCT_CoverageType = 'ExcessAttendantCare' and pol_eff_date > TO_DATE('2020-07-01 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),'683',
	-- 
	-- StateProvinceCode = '21' AND v_CLFile_PIP='1', v_MI_PIP_CoverageCode,
	-- -- StateProvinceCode = '21' AND SubjectToNoFault = 'Yes' AND DCT_CoverageType = 'PIP' and pol_eff_date > TO_DATE('2020-07-01 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),v_MI_PIP_CoverageCode,
	-- 
	-- -- KS
	-- StateProvinceCode = '15'  AND v_CLFile_PIP='1', v_KS_PIP_CoverageCode,
	-- 
	-- -- KY
	-- StateProvinceCode = '16' AND v_CLFile_PIP='1', v_KY_PIP_CoverageCode,
	-- 
	-- --MN
	-- StateProvinceCode = '22' AND v_CLFile_PIP='1', v_MN_PIP_CoverageCode,
	-- 
	-- -- Default
	-- i_CoverageCode)
	DECODE(
	    TRUE,
	    StateProvinceCode = '21' AND SubjectToNoFault = 'Yes' AND DCT_CoverageType = 'ExcessAttendantCare' and pol_eff_date > TO_TIMESTAMP('2020-07-01 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), '683',
	    StateProvinceCode = '21' AND v_CLFile_PIP = '1', v_MI_PIP_CoverageCode,
	    StateProvinceCode = '15' AND v_CLFile_PIP = '1', v_KS_PIP_CoverageCode,
	    StateProvinceCode = '16' AND v_CLFile_PIP = '1', v_KY_PIP_CoverageCode,
	    StateProvinceCode = '22' AND v_CLFile_PIP = '1', v_MN_PIP_CoverageCode,
	    i_CoverageCode
	) AS o_CoverageCode,
	v_CLFile_PIP AS o_CLFile_PIP,
	EXP_Cleansing_Premium.o_ConstructionCode AS ConstructionCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(ConstructionCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(ConstructionCode) AS ConstructionCode_out,
	EXP_Cleansing_Premium.o_IsoFireProtectionCode AS IsoFireProtectionCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(IsoFireProtectionCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(IsoFireProtectionCode) AS IsoFireProtectionCode_out,
	EXP_Cleansing_Premium.o_PackageModificationAdjustmentGroupCode AS i_PackageModificationAdjustmentGroupCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_PackageModificationAdjustmentGroupCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_PackageModificationAdjustmentGroupCode) AS PackageModificationAdjustmentGroupCode_out,
	EXP_Cleansing_Premium.o_pol_exp_date AS pol_exp_date,
	EXP_Cleansing_Premium.o_AnnualStatementLineNumber AS AnnualStatementLineNumber,
	EXP_Cleansing_Premium.InsuranceSegmentCode,
	EXP_Cleansing_Premium.o_ZipPostalCode AS ZipPostalCode,
	EXP_Cleansing_Premium.PremiumMasterExposure,
	EXP_Cleansing_Premium.VehicleYear,
	EXP_Cleansing_Premium.CoverageLimitValue,
	EXP_Cleansing_Premium.TerminalZoneCode,
	EXP_Cleansing_Premium.DeductibleBasis,
	EXP_Cleansing_Premium.PremiumTransactionEffectiveDate,
	EXP_Cleansing_Premium.CoverageGroupCode,
	EXP_Cleansing_Premium.o_VehicleNumber,
	EXP_Cleansing_Premium.PIPBureaucoverageCode,
	EXP_Cleansing_Premium.CoverageLimitType,
	EXP_Cleansing_Premium.PremiumTransactionAKID1,
	EXP_Cleansing_Premium.IncludeUIM,
	EXP_Cleansing_Premium.RatingZoneCode,
	EXP_Cleansing_Premium.FullGlassIndicator
	FROM EXP_Cleansing_Premium
	LEFT JOIN LKP_ExcessAttendantCare_Coverage_Prem
	ON LKP_ExcessAttendantCare_Coverage_Prem.lu_PolicyKey = EXP_Cleansing_Premium.o_pol_key AND LKP_ExcessAttendantCare_Coverage_Prem.lu_EffectiveDate <= EXP_Cleansing_Premium.o_pol_exp_date AND LKP_ExcessAttendantCare_Coverage_Prem.lu_ExpirationDate >= EXP_Cleansing_Premium.o_pol_eff_date
	LEFT JOIN LKP_COVERAGEDEDUCTIBLE_DCT LKP_COVERAGEDEDUCTIBLE_DCT_PremiumTransactionAKID1
	ON LKP_COVERAGEDEDUCTIBLE_DCT_PremiumTransactionAKID1.PremiumTransactionAKID = PremiumTransactionAKID1

),
Union AS (
	SELECT LossMasterCalculationId, PremiumMasterCalculationID, PremiumMasterRunDate, loss_master_run_date, pol_key, prim_bus_class_code, StateProvinceCode, sub_line_code_out AS sub_line_code, PremiumMasterClassCode, LossMasterClassCode, Cause_of_Loss, TerritoryCode, pol_eff_date, ClaimNum, ClaimantNum, PremiumMasterPremium_out AS PremiumMasterPremium, PaidLossAmt, OutstandingAmt, TypeBureauCode, RiskUnitGroup, SourceSystemID, RiskType, DeductibleAmount_out AS DeductibleAmount, o_CoverageCode AS CoverageCode, ConstructionCode_out AS ConstructionCode, IsoFireProtectionCode_out AS IsoFireProtectionCode, PackageModificationAdjustmentGroupCode_out AS PackageModificationAdjustmentGroupCode, pol_exp_date AS PolicyExpirationDate, AnnualStatementLineNumber, BureauCode1, BureauCode2, BureauCode4, InsuranceSegmentCode, PremiumMasterExposure, ZipPostalCode, VehicleYear, CoverageLimitValue, TerminalZoneCode, DeductibleBasis, PremiumTransactionEffectiveDate, CoverageGroupCode, o_VehicleNumber AS VehicleNumber, PIPBureaucoverageCode, CoverageLimitType, PremiumTransactionAKID1, IncludeUIM, AdditionalLimitKS, AdditionalLimitKY, AdditionalLimitMN, o_CLFile_PIP, RatingZoneCode, FullGlassIndicator
	FROM EXP_Logic_Premium
	UNION
	SELECT loss_master_calculation_id AS LossMasterCalculationId, PremiumMasterCalculationID, PremiumMasterRunDate, loss_master_run_date, pol_key, prim_bus_class_code, StateProvinceCode, sub_line_code_out AS sub_line_code, PremiumMasterClassCode_out AS PremiumMasterClassCode, LossMasterClassCode_out AS LossMasterClassCode, cause_of_loss_out AS Cause_of_Loss, TerritoryCode_out AS TerritoryCode, pol_eff_date, ClaimNum, claimant_num AS ClaimantNum, PremiumMasterPremium, PaidLossAmount AS PaidLossAmt, OutstandingLossAmount AS OutstandingAmt, TypeBureauCode, RiskUnitGroup, PolicySourceID AS SourceSystemID, RiskType, DeductibleAmount, o_CoverageCode AS CoverageCode, ConstructionCode, IsoFireProtectionCode, PackageModificationAdjustmentGroupCode, pol_exp_date AS PolicyExpirationDate, CumulativeInceptiontoDatePaidLoss AS InceptionToDatePaidLossAmount, claim_coverage_detail_ak_id AS ClaimCoverageID, AnnualStatementLineNumber, BureauCode1, BureauCode2, BureauCode4, InsuranceSegmentCode, exposure AS PremiumMasterExposure, ZipPostalCode, VehicleYear, claim_loss_date, direct_alae_paid_including_recoveries, direct_loss_outstanding_excluding_recoveries, CoverageLimitValue, TerminalZoneCode, DeductibleBasis2 AS DeductibleBasis, CoverageGroupCode, VehicleNumber, PIPBureaucoverageCode, CoverageLimitType, PremiumTransactionAKID1, IncludeUIM, AdditionalLimitKS, AdditionalLimitKY, AdditionalLimitMN, o_CLFile_PIP, RatingZoneCode, FullGlassIndicator1 AS FullGlassIndicator
	FROM EXP_Logic_Loss
),
EXP_CombinedCoverageLimitAndDeductibleRules AS (
	SELECT
	StateProvinceCode,
	DeductibleAmount,
	CoverageCode,
	CoverageLimitType,
	CoverageLimitValue,
	DeductibleBasis,
	PIPBureaucoverageCode,
	PremiumTransactionAKID1 AS PremiumTransactionAKID,
	PremiumMasterCalculationID,
	AnnualStatementLineNumber,
	AdditionalLimitKS,
	AdditionalLimitKY,
	AdditionalLimitMN,
	-- *INF*: DECODE(TRUE,
	-- in(StateProvinceCode,'16') AND in(PIPBureaucoverageCode,'682','672'),:LKP.LKP_COVERAGELIMITOVERRIDE(PremiumTransactionAKID,'PersonalInjuryProtectionExcessLimit'),
	-- 
	-- in(StateProvinceCode,'22') AND v_AdditionalLimit_lookup_translated = '1',:LKP.LKP_COVERAGELIMITOVERRIDE(PremiumTransactionAKID,'PersonalInjuryProtectionExcessLimit'),
	-- 
	-- CoverageLimitValue)
	DECODE(
	    TRUE,
	    StateProvinceCode IN ('16') AND PIPBureaucoverageCode IN ('682','672'), LKP_COVERAGELIMITOVERRIDE_PremiumTransactionAKID_PersonalInjuryProtectionExcessLimit.CoverageLimitValue,
	    StateProvinceCode IN ('22') AND v_AdditionalLimit_lookup_translated = '1', LKP_COVERAGELIMITOVERRIDE_PremiumTransactionAKID_PersonalInjuryProtectionExcessLimit.CoverageLimitValue,
	    CoverageLimitValue
	) AS v_CoverageOverrideValue,
	o_CLFile_PIP AS i_CLFile_PIP,
	-- *INF*: DECODE(TRUE,
	-- StateProvinceCode = '16' AND IN (AdditionalLimitKY,-1, 0) AND i_CLFile_PIP = '1', '0',
	-- StateProvinceCode = '16' AND AdditionalLimitKY = 1 AND i_CLFile_PIP = '1', '10000', 
	-- StateProvinceCode = '16' AND AdditionalLimitKY = 2 AND i_CLFile_PIP = '1', '20000',
	-- StateProvinceCode = '16' AND AdditionalLimitKY = 3 AND i_CLFile_PIP = '1', '30000',
	-- StateProvinceCode = '16' AND AdditionalLimitKY = 4 AND i_CLFile_PIP = '1', '40000',
	-- StateProvinceCode = '16' AND AdditionalLimitKY = 5 AND i_CLFile_PIP = '1', '65000',
	-- StateProvinceCode = '16' AND AdditionalLimitKY = 6 AND i_CLFile_PIP = '1', '90000',
	-- StateProvinceCode='16' AND IN(PIPBureaucoverageCode,'672','682'),v_CoverageOverrideValue,
	-- '0')
	DECODE(
	    TRUE,
	    StateProvinceCode = '16' AND AdditionalLimitKY IN (- 1,0) AND i_CLFile_PIP = '1', '0',
	    StateProvinceCode = '16' AND AdditionalLimitKY = 1 AND i_CLFile_PIP = '1', '10000',
	    StateProvinceCode = '16' AND AdditionalLimitKY = 2 AND i_CLFile_PIP = '1', '20000',
	    StateProvinceCode = '16' AND AdditionalLimitKY = 3 AND i_CLFile_PIP = '1', '30000',
	    StateProvinceCode = '16' AND AdditionalLimitKY = 4 AND i_CLFile_PIP = '1', '40000',
	    StateProvinceCode = '16' AND AdditionalLimitKY = 5 AND i_CLFile_PIP = '1', '65000',
	    StateProvinceCode = '16' AND AdditionalLimitKY = 6 AND i_CLFile_PIP = '1', '90000',
	    StateProvinceCode = '16' AND PIPBureaucoverageCode IN ('672','682'), v_CoverageOverrideValue,
	    '0'
	) AS v_KY_PIP_CoverageLimitValue,
	-- *INF*: DECODE(TRUE,
	-- StateProvinceCode = '22' AND AdditionalLimitMN= 1 AND i_CLFile_PIP = '1', '30000', 
	-- StateProvinceCode = '22' AND AdditionalLimitMN= 2 AND i_CLFile_PIP = '1', '40000',
	-- StateProvinceCode = '22' AND AdditionalLimitMN= 3 AND i_CLFile_PIP = '1', '50000',
	-- StateProvinceCode = '22' AND AdditionalLimitMN= 4 AND i_CLFile_PIP = '1', '50000',
	-- StateProvinceCode = '22' AND AdditionalLimitMN= 5 AND i_CLFile_PIP = '1', '75000',
	-- StateProvinceCode = '22' AND AdditionalLimitMN= 6 AND i_CLFile_PIP = '1', '100000',
	-- StateProvinceCode = '22' AND v_AdditionalLimit_lookup_translated = '1', v_CoverageOverrideValue,
	-- StateProvinceCode = '22' AND IN (AdditionalLimitMN,-1, 0) AND i_CLFile_PIP = '1', '20000',
	-- '0')
	DECODE(
	    TRUE,
	    StateProvinceCode = '22' AND AdditionalLimitMN = 1 AND i_CLFile_PIP = '1', '30000',
	    StateProvinceCode = '22' AND AdditionalLimitMN = 2 AND i_CLFile_PIP = '1', '40000',
	    StateProvinceCode = '22' AND AdditionalLimitMN = 3 AND i_CLFile_PIP = '1', '50000',
	    StateProvinceCode = '22' AND AdditionalLimitMN = 4 AND i_CLFile_PIP = '1', '50000',
	    StateProvinceCode = '22' AND AdditionalLimitMN = 5 AND i_CLFile_PIP = '1', '75000',
	    StateProvinceCode = '22' AND AdditionalLimitMN = 6 AND i_CLFile_PIP = '1', '100000',
	    StateProvinceCode = '22' AND v_AdditionalLimit_lookup_translated = '1', v_CoverageOverrideValue,
	    StateProvinceCode = '22' AND AdditionalLimitMN IN (- 1,0) AND i_CLFile_PIP = '1', '20000',
	    '0'
	) AS v_MN_PIP_CoverageLimitValue,
	-- *INF*: DECODE(TRUE,
	-- StateProvinceCode = '16' AND IN (AdditionalLimitKY,-1, 0) AND i_CLFile_PIP = '1', 'D',
	-- '0')
	DECODE(
	    TRUE,
	    StateProvinceCode = '16' AND AdditionalLimitKY IN (- 1,0) AND i_CLFile_PIP = '1', 'D',
	    '0'
	) AS v_KY_PIP_DeductibleBasis,
	-- *INF*: DECODE(TRUE,
	-- StateProvinceCode='21' AND IN(CoverageCode, '683', '685', '689') AND i_CLFile_PIP = '1','0',
	-- StateProvinceCode='21' AND IN(CoverageCode,'671','681','691','692','693') AND i_CLFile_PIP = '1','D',	
	-- '0')
	DECODE(
	    TRUE,
	    StateProvinceCode = '21' AND CoverageCode IN ('683','685','689') AND i_CLFile_PIP = '1', '0',
	    StateProvinceCode = '21' AND CoverageCode IN ('671','681','691','692','693') AND i_CLFile_PIP = '1', 'D',
	    '0'
	) AS v_MI_PIP_DeductibleBasis,
	-- *INF*: IIF(IN(StateProvinceCode,'22') AND  IS_NUMBER(PIPBureaucoverageCode), :LKP.LKP_GETADDITIONALLIMITVALUE(PremiumTransactionAKID))
	IFF(
	    StateProvinceCode IN ('22') AND REGEXP_LIKE(PIPBureaucoverageCode, '^[0-9]+$'),
	    LKP_GETADDITIONALLIMITVALUE_PremiumTransactionAKID.AdditionalLimit
	) AS v_AdditionalLimit_lookup,
	-- *INF*: DECODE(TRUE,
	-- in(v_AdditionalLimit_lookup,'F','N','0'),'0',
	-- in(v_AdditionalLimit_lookup,'T','Y','1'),'1',
	-- '0')
	-- 
	-- -- figure out all the ways sql can translate a bit type to a string and account for it or default to 0
	DECODE(
	    TRUE,
	    v_AdditionalLimit_lookup IN ('F','N','0'), '0',
	    v_AdditionalLimit_lookup IN ('T','Y','1'), '1',
	    '0'
	) AS v_AdditionalLimit_lookup_translated,
	-- *INF*: DECODE(TRUE,
	-- CoverageCode='PPI','D',
	-- IN(CoverageCode,'BPIPNAME','MCCAI','MCCANI'),'0',
	-- CoverageCode = 'COMPRH' AND IN(CoverageGroupCode, 'COMPRH','DOC') AND i_FullGlassIndicator = 'T', 'F',
	-- StateProvinceCode='15' AND i_CLFile_PIP = '1', '0',
	-- StateProvinceCode='16' AND i_CLFile_PIP = '1', v_KY_PIP_DeductibleBasis,
	-- StateProvinceCode='16' AND IN(PIPBureaucoverageCode,'671','681'),'D',
	-- StateProvinceCode='21' AND i_CLFile_PIP = '1', v_MI_PIP_DeductibleBasis,
	-- StateProvinceCode='21' AND IS_NUMBER(PIPBureaucoverageCode),'D',
	-- StateProvinceCode='22' AND i_CLFile_PIP = '1', '0',
	-- IS_NUMBER(PIPBureaucoverageCode),'0',
	-- DeductibleBasis != 'F', DeductibleBasis,
	-- '0')
	DECODE(
	    TRUE,
	    CoverageCode = 'PPI', 'D',
	    CoverageCode IN ('BPIPNAME','MCCAI','MCCANI'), '0',
	    CoverageCode = 'COMPRH' AND CoverageGroupCode IN ('COMPRH','DOC') AND i_FullGlassIndicator = 'T', 'F',
	    StateProvinceCode = '15' AND i_CLFile_PIP = '1', '0',
	    StateProvinceCode = '16' AND i_CLFile_PIP = '1', v_KY_PIP_DeductibleBasis,
	    StateProvinceCode = '16' AND PIPBureaucoverageCode IN ('671','681'), 'D',
	    StateProvinceCode = '21' AND i_CLFile_PIP = '1', v_MI_PIP_DeductibleBasis,
	    StateProvinceCode = '21' AND REGEXP_LIKE(PIPBureaucoverageCode, '^[0-9]+$'), 'D',
	    StateProvinceCode = '22' AND i_CLFile_PIP = '1', '0',
	    REGEXP_LIKE(PIPBureaucoverageCode, '^[0-9]+$'), '0',
	    DeductibleBasis != 'F', DeductibleBasis,
	    '0'
	) AS v_DeductibleBasis,
	-- *INF*: DECODE(TRUE,
	-- -- only calc for premium
	-- -- PremiumMasterCalculationID = -1,'N/A',
	-- 
	-- --MN
	-- 
	-- StateProvinceCode='22' AND 
	-- --CoverageLimitType='PersonalInjuryProtectionBasicLimit' AND 
	-- v_AdditionalLimit_lookup_translated = '0' AND
	-- IS_NUMBER(PIPBureaucoverageCode),'20000',
	-- 
	-- StateProvinceCode='22' AND i_CLFile_PIP = '1', v_MN_PIP_CoverageLimitValue,
	-- 
	-- StateProvinceCode='22' AND 
	-- --CoverageLimitType='PersonalInjuryProtectionExcessLimit' AND 
	-- v_AdditionalLimit_lookup_translated =  '1' AND
	-- IS_NUMBER(PIPBureaucoverageCode),v_CoverageOverrideValue,
	-- 
	-- -- KY
	-- StateProvinceCode='16' AND i_CLFile_PIP = '1', v_KY_PIP_CoverageLimitValue,
	-- 
	-- -- KS
	-- StateProvinceCode = '15' AND i_CLFile_PIP = '1', '0',
	-- 
	-- -- Default
	-- '0'
	-- )
	DECODE(
	    TRUE,
	    StateProvinceCode = '22' AND v_AdditionalLimit_lookup_translated = '0' AND REGEXP_LIKE(PIPBureaucoverageCode, '^[0-9]+$'), '20000',
	    StateProvinceCode = '22' AND i_CLFile_PIP = '1', v_MN_PIP_CoverageLimitValue,
	    StateProvinceCode = '22' AND v_AdditionalLimit_lookup_translated = '1' AND REGEXP_LIKE(PIPBureaucoverageCode, '^[0-9]+$'), v_CoverageOverrideValue,
	    StateProvinceCode = '16' AND i_CLFile_PIP = '1', v_KY_PIP_CoverageLimitValue,
	    StateProvinceCode = '15' AND i_CLFile_PIP = '1', '0',
	    '0'
	) AS v_CoverageLimitValueRules,
	-- *INF*: DECODE(TRUE,
	-- in(v_DeductibleBasis,'0','N/A'),'0',
	-- v_DeductibleBasis='D' AND CoverageCode='COMRPD','9999999',
	-- DeductibleAmount)
	DECODE(
	    TRUE,
	    v_DeductibleBasis IN ('0','N/A'), '0',
	    v_DeductibleBasis = 'D' AND CoverageCode = 'COMRPD', '9999999',
	    DeductibleAmount
	) AS v_DeductibleAmount,
	v_DeductibleBasis AS o_DeductibleBasis,
	v_CoverageLimitValueRules AS o_CoverageLimitValue,
	v_DeductibleAmount AS o_DeductibleAmount,
	TypeBureauCode,
	CoverageGroupCode,
	FullGlassIndicator AS i_FullGlassIndicator
	FROM Union
	LEFT JOIN LKP_COVERAGELIMITOVERRIDE LKP_COVERAGELIMITOVERRIDE_PremiumTransactionAKID_PersonalInjuryProtectionExcessLimit
	ON LKP_COVERAGELIMITOVERRIDE_PremiumTransactionAKID_PersonalInjuryProtectionExcessLimit.PremiumTransactionAKID = PremiumTransactionAKID
	AND LKP_COVERAGELIMITOVERRIDE_PremiumTransactionAKID_PersonalInjuryProtectionExcessLimit.CoverageLimitType = 'PersonalInjuryProtectionExcessLimit'

	LEFT JOIN LKP_GETADDITIONALLIMITVALUE LKP_GETADDITIONALLIMITVALUE_PremiumTransactionAKID
	ON LKP_GETADDITIONALLIMITVALUE_PremiumTransactionAKID.PremiumTransactionAKId = PremiumTransactionAKID

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
LKP_RiskTerritory AS (
	SELECT
	RiskTerritory,
	PolicyKey
	FROM (
		SELECT RL.RiskTerritory as RiskTerritory,
		POL.pol_key as PolicyKey
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL 
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL on POL.pol_ak_id=RL.PolicyAKID AND RL.CurrentSnapshotFlag=1 and POL.crrnt_snpsht_flag=1
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC on RL.RisklocationAKID =PC.RisklocationAKID and PC.CurrentSnapshotFlag=1  
		WHERE (PC.TypeBureauCode in ('AL','AN','AP') or PC.InsuranceLine = 'CommercialAuto')
		and RL.LocationUnitNumber not in ( '0000', '000')
		and RL.RiskTerritory <> 'N/A'
		ORDER BY POL.pol_key,
		RL.LocationUnitNumber,
		RL.RiskLocationAKID
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY RiskTerritory) = 1
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
	LKP_RiskTerritory.RiskTerritory AS i_TerritoryCode,
	Union.pol_eff_date AS i_pol_eff_date,
	Union.ClaimNum AS i_ClaimNum,
	Union.ClaimantNum AS i_ClaimantNum,
	Union.PremiumMasterPremium AS i_PremiumMasterPremium,
	Union.PaidLossAmt AS i_PaidLossAmt,
	Union.OutstandingAmt AS i_OutstandingAmt,
	EXP_CombinedCoverageLimitAndDeductibleRules.TypeBureauCode AS i_TypeBureauCode,
	Union.SourceSystemID AS i_SourceSystemID,
	Union.RiskType AS i_RiskType,
	EXP_CombinedCoverageLimitAndDeductibleRules.o_DeductibleAmount AS i_DeductibleAmount,
	Union.CoverageCode AS i_CoverageCode,
	EXP_ConstCode_IsoPC_Rules.o_ConstructionCode AS i_ConstructionCode,
	EXP_ConstCode_IsoPC_Rules.o_IsoFireProtectionCode AS i_IsoFireProtectionCode,
	Union.PackageModificationAdjustmentGroupCode AS i_PackageModificationAdjustmentGroupCode,
	Union.PolicyExpirationDate AS i_PolicyExpirationDate,
	Union.InceptionToDatePaidLossAmount AS i_InceptionToDatePaidLossAmount,
	Union.ClaimCoverageID AS i_ClaimCoverageID,
	EXP_CombinedCoverageLimitAndDeductibleRules.AnnualStatementLineNumber AS i_AnnualStatementLineNumber,
	Union.BureauCode1 AS i_BureauCode1,
	Union.BureauCode2 AS i_BureauCode2,
	Union.BureauCode4 AS i_BureauCode4,
	Union.InsuranceSegmentCode AS i_InsuranceSegmentCode,
	Union.ZipPostalCode AS i_ZipPostalCode,
	Union.PremiumMasterExposure AS i_PremiumMasterExposure,
	Union.VehicleYear AS i_VehicleYear,
	Union.claim_loss_date AS i_claim_loss_date,
	Union.direct_alae_paid_including_recoveries AS i_direct_alae_paid_including_recoveries,
	Union.direct_loss_outstanding_excluding_recoveries AS i_direct_loss_outstanding_excluding_recoveries,
	EXP_CombinedCoverageLimitAndDeductibleRules.o_CoverageLimitValue AS i_CoverageLimitValue,
	Union.TerminalZoneCode AS i_TerminalZoneCode,
	EXP_CombinedCoverageLimitAndDeductibleRules.o_DeductibleBasis AS i_DeductibleBasis,
	Union.PremiumTransactionEffectiveDate AS i_PremiumTransactionEffectiveDate,
	EXP_CombinedCoverageLimitAndDeductibleRules.CoverageGroupCode AS i_CoverageGroupCode,
	Union.VehicleNumber AS i_VehicleNumber,
	Union.IncludeUIM AS i_IncludeUIM,
	Union.RatingZoneCode AS i_RatingZoneCode,
	-- *INF*: DECODE(TRUE,
	-- i_TypeBureauCode='Property',
	-- DECODE(TRUE,
	-- i_RiskType='BLDG','01',
	-- i_RiskType='PP','02',
	-- 'N/A'
	-- ),
	-- 
	-- i_TypeBureauCode='Crime',
	-- DECODE(TRUE,
	-- i_RiskType='ClientsProperty','200',
	-- i_RiskType='ClientsProperty_ETF','400',
	-- i_RiskType='ComputerFraud','200',
	-- i_RiskType='ComputerFraud_G','300',
	-- i_RiskType='EmployeeTheft','200',
	-- i_RiskType='EmployeeTheft_ETF','400',
	-- i_RiskType='EmployeeTheftNameOrPosition','200',
	-- i_RiskType='EmployeeTheftNameOrPosition_ETF','400',
	-- i_RiskType='EmployeeTheftNameOrPosition_G','300',
	-- i_RiskType='EmployeeTheftNameOrPosition_GETF','400',
	-- i_RiskType='EmployeeTheftPerEmployee','300',
	-- i_RiskType='EmployeeTheftPerEmployee_GETF','400',
	-- i_RiskType='EmployeeTheftPerLoss','300',
	-- i_RiskType='EmployeeTheftPerLoss_GETF','400',
	-- i_RiskType='ForgeryAndAlteration','200',
	-- i_RiskType='ForgeryAndAlteration_ETF','400',
	-- i_RiskType='ForgeryAndAlteration_G','300',
	-- i_RiskType='ForgeryAndAlteration_GETF','400',
	-- i_RiskType='FundsTransfer','200',
	-- i_RiskType='FundsTransfer_G','300',
	-- i_RiskType='GuestPropertyInsidePremises','200',
	-- i_RiskType='GuestPropertySafeDeposit','200',
	-- i_RiskType='InsideRobbery','200',
	-- i_RiskType='InsideRobbery_G','300',
	-- i_RiskType='InsideRobberyOther','200',
	-- i_RiskType='InsideRobberyOther_G','300',
	-- i_RiskType='InsideRobberySecurities','200',
	-- i_RiskType='InsideRobberySecurities_G','300',
	-- i_RiskType='InsideTheftMoney','200',
	-- i_RiskType='InsideTheftMoney_G','300',
	-- i_RiskType='InsideTheftProperty','200',
	-- i_RiskType='InsideTheftProperty_G','300',
	-- i_RiskType='MoneyOrders','200',
	-- i_RiskType='OutsidePremises','200',
	-- i_RiskType='OutsidePremises_G','300',
	-- 'N/A'
	-- ),
	-- 'N/A')
	DECODE(
	    TRUE,
	    i_TypeBureauCode = 'Property', DECODE(
	        TRUE,
	        i_RiskType = 'BLDG', '01',
	        i_RiskType = 'PP', '02',
	        'N/A'
	    ),
	    i_TypeBureauCode = 'Crime', DECODE(
	        TRUE,
	        i_RiskType = 'ClientsProperty', '200',
	        i_RiskType = 'ClientsProperty_ETF', '400',
	        i_RiskType = 'ComputerFraud', '200',
	        i_RiskType = 'ComputerFraud_G', '300',
	        i_RiskType = 'EmployeeTheft', '200',
	        i_RiskType = 'EmployeeTheft_ETF', '400',
	        i_RiskType = 'EmployeeTheftNameOrPosition', '200',
	        i_RiskType = 'EmployeeTheftNameOrPosition_ETF', '400',
	        i_RiskType = 'EmployeeTheftNameOrPosition_G', '300',
	        i_RiskType = 'EmployeeTheftNameOrPosition_GETF', '400',
	        i_RiskType = 'EmployeeTheftPerEmployee', '300',
	        i_RiskType = 'EmployeeTheftPerEmployee_GETF', '400',
	        i_RiskType = 'EmployeeTheftPerLoss', '300',
	        i_RiskType = 'EmployeeTheftPerLoss_GETF', '400',
	        i_RiskType = 'ForgeryAndAlteration', '200',
	        i_RiskType = 'ForgeryAndAlteration_ETF', '400',
	        i_RiskType = 'ForgeryAndAlteration_G', '300',
	        i_RiskType = 'ForgeryAndAlteration_GETF', '400',
	        i_RiskType = 'FundsTransfer', '200',
	        i_RiskType = 'FundsTransfer_G', '300',
	        i_RiskType = 'GuestPropertyInsidePremises', '200',
	        i_RiskType = 'GuestPropertySafeDeposit', '200',
	        i_RiskType = 'InsideRobbery', '200',
	        i_RiskType = 'InsideRobbery_G', '300',
	        i_RiskType = 'InsideRobberyOther', '200',
	        i_RiskType = 'InsideRobberyOther_G', '300',
	        i_RiskType = 'InsideRobberySecurities', '200',
	        i_RiskType = 'InsideRobberySecurities_G', '300',
	        i_RiskType = 'InsideTheftMoney', '200',
	        i_RiskType = 'InsideTheftMoney_G', '300',
	        i_RiskType = 'InsideTheftProperty', '200',
	        i_RiskType = 'InsideTheftProperty_G', '300',
	        i_RiskType = 'MoneyOrders', '200',
	        i_RiskType = 'OutsidePremises', '200',
	        i_RiskType = 'OutsidePremises_G', '300',
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
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	SYSDATE AS o_CreatedDate,
	i_PremiumMasterCalculationID AS o_PremiumMasterCalculationID,
	i_LossMasterCalculationId AS o_LossMasterCalculationId,
	i_TypeBureauCode AS o_TypeBureauCode,
	-- *INF*: IIF(IN(i_StateProvinceCode,'12','32'),'55','01')
	IFF(i_StateProvinceCode IN ('12','32'), '55', '01') AS o_BureauLineOfInsurance,
	-- *INF*: --Fix for EDWP-3967
	-- '0731'
	-- 
	-- --'0761'
	'0731' AS o_BureauCompanyNumber,
	i_StateProvinceCode AS o_StateProvinceCode,
	i_PremiumMasterRunDate AS o_PremiumMasterRunDate,
	i_LossMasterRunDate AS o_LossMasterRunDate,
	i_pol_key AS o_pol_key,
	-- *INF*: LTRIM(RTRIM(i_PremiumMasterClassCode))
	LTRIM(RTRIM(i_PremiumMasterClassCode)) AS o_PremiumMasterClassCode,
	-- *INF*: LTRIM(RTRIM(i_LossMasterClassCode))
	LTRIM(RTRIM(i_LossMasterClassCode)) AS o_LossMasterClassCode,
	i_ClaimNum AS o_ClaimNum,
	i_ClaimantNum AS o_ClaimantNum,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_TerritoryCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_TerritoryCode) AS o_RiskTerritoryCode,
	i_pol_eff_date AS o_PolicyEffectiveDate,
	i_Cause_of_Loss AS o_CauseOfLoss,
	-- *INF*: IIF(ISNULL(i_DeductibleAmount),'0000000',LPAD(i_DeductibleAmount, 7, '0'))
	IFF(i_DeductibleAmount IS NULL, '0000000', LPAD(i_DeductibleAmount, 7, '0')) AS o_DeductibleAmount,
	i_CoverageCode AS o_CoverageCode,
	-- *INF*: 'N/A'
	-- --i_ConstructionCode
	'N/A' AS o_ConstructionCode,
	-- *INF*: 'N/A'
	-- --i_IsoFireProtectionCode
	'N/A' AS o_ISOFireProtectionCode,
	i_sub_line_code AS o_SublineCode,
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
	i_ZipPostalCode AS o_ZipPostalCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_DeductibleBasis)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_DeductibleBasis) AS o_DeductibleIndicatorCode,
	-- *INF*: IIF(IN(i_TypeBureauCode,'RL','RN','RP'),i_VehicleYear,'N/A')
	IFF(i_TypeBureauCode IN ('RL','RN','RP'), i_VehicleYear, 'N/A') AS o_VehicleYear,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_CoverageLimitValue)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_CoverageLimitValue) AS o_CoverageLimitValue,
	'N/A' AS o_PolicyUpperLimit,
	-- *INF*: IIF(ISNULL(i_TerminalZoneCode),'N/A',i_TerminalZoneCode)
	IFF(i_TerminalZoneCode IS NULL, 'N/A', i_TerminalZoneCode) AS o_TerminalZoneCode,
	-- *INF*: IIF(ISNULL(i_PremiumMasterExposure), 0,i_PremiumMasterExposure )
	-- 
	IFF(i_PremiumMasterExposure IS NULL, 0, i_PremiumMasterExposure) AS o_PremiumMasterExposure,
	-- *INF*: IIF(ISNULL(i_direct_alae_paid_including_recoveries),0,i_direct_alae_paid_including_recoveries)
	IFF(
	    i_direct_alae_paid_including_recoveries IS NULL, 0, i_direct_alae_paid_including_recoveries
	) AS o_PaidAllocatedLossAdjustmentExpenseAmount,
	-- *INF*: IIF(ISNULL(i_direct_loss_outstanding_excluding_recoveries),0,i_direct_loss_outstanding_excluding_recoveries)
	IFF(
	    i_direct_loss_outstanding_excluding_recoveries IS NULL, 0,
	    i_direct_loss_outstanding_excluding_recoveries
	) AS o_OutstandingAllocatedLossAdjustmentExpenseAmount,
	-- *INF*: IIF(ISNULL(i_claim_loss_date),TO_DATE('18000101','YYYYMMDD'),i_claim_loss_date)
	IFF(i_claim_loss_date IS NULL, TO_TIMESTAMP('18000101', 'YYYYMMDD'), i_claim_loss_date) AS o_ClaimLossDate,
	-- *INF*: IIF(ISNULL(i_PremiumTransactionEffectiveDate),TO_DATE('18000101','YYYYMMDD'),i_PremiumTransactionEffectiveDate)
	IFF(
	    i_PremiumTransactionEffectiveDate IS NULL, TO_TIMESTAMP('18000101', 'YYYYMMDD'),
	    i_PremiumTransactionEffectiveDate
	) AS o_PremiumTransactionEffectiveDate,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_CoverageGroupCode)
	UDF_DEFAULT_VALUE_FOR_STRINGS(i_CoverageGroupCode) AS o_CoverageGroupCode,
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
	i_VehicleNumber AS o_VehicleNumber,
	i_IncludeUIM AS o_IncludeUIM,
	-- *INF*: IIF(ISNULL(i_RatingZoneCode),'N/A',i_RatingZoneCode)
	IFF(i_RatingZoneCode IS NULL, 'N/A', i_RatingZoneCode) AS o_RatingZoneCode
	FROM EXP_CombinedCoverageLimitAndDeductibleRules
	 -- Manually join with EXP_ConstCode_IsoPC_Rules
	 -- Manually join with Union
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
FIL_ASL AS (
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
	o_DeductibleAmount AS DeductibleAmount, 
	o_CoverageCode AS CoverageCode, 
	o_SublineCode AS SublineCode, 
	PackageModificationAdjustmentGroupDescription, 
	o_PremiumMasterDirectWrittenPremiumAmount AS PremiumMasterDirectWrittenPremiumAmount, 
	o_PaidLossAmount AS PaidLossAmount, 
	o_OutstandingLossAmount AS OutstandingLossAmount, 
	o_PolicyExpirationDate AS PolicyExpirationDate, 
	o_InceptionToDatePaidLossAmount AS InceptionToDatePaidLossAmount, 
	o_ClaimCoverageID AS ClaimCoverageID, 
	o_AnnualStatementLineNumber AS AnnualStatementLineNumber, 
	o_ZipPostalCode AS ZipPostalCode, 
	o_DeductibleIndicatorCode AS DeductibleIndicatorCode, 
	o_CoverageLimitValue AS CoverageLimitValue, 
	o_PolicyUpperLimit AS PolicyUpperLimit, 
	o_TerminalZoneCode AS TerminalZoneCode, 
	o_PremiumMasterExposure AS PremiumMasterExposure, 
	o_PaidAllocatedLossAdjustmentExpenseAmount AS PaidAllocatedLossAdjustmentExpenseAmount, 
	o_OutstandingAllocatedLossAdjustmentExpenseAmount AS OutstandingAllocatedLossAdjustmentExpenseAmount, 
	o_ClaimLossDate AS ClaimLossDate, 
	o_PremiumTransactionEffectiveDate AS PremiumTransactionEffectiveDate, 
	o_CoverageGroupCode AS CoverageGroupCode, 
	o_VehicleNumber AS VehicleNumber, 
	o_IncludeUIM AS IncludeUIM, 
	o_RatingZoneCode AS RatingZoneCode
	FROM EXP_Values
	WHERE LTRIM(RTRIM(AnnualStatementLineNumber))<> '17.2' AND CoverageCode<>'EMPLESSOR' AND INSTR(PremiumMasterClassCode,',')=0 AND PremiumMasterClassCode<>'99999'
),
ISSCommercialAutoExtract AS (

	------------ PRE SQL ----------
	@{pipeline().parameters.DELETE_PRESQL}
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ISSCommercialAutoExtract
	(AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, DeductibleAmount, CoverageCode, SublineCode, PackageModificationAdjustmentGroupDescription, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, ZipPostalCode, DeductibleIndicatorCode, PolicyLowerLimit, PolicyUpperLimit, TerminalZoneCode, WrittenExposure, PaidAllocatedLossAdjustmentExpenseAmount, OutstandingAllocatedLossAdjustmentExpenseAmount, ClaimLossDate, TransactionEffectiveDate, CoverageGroupCode, VehicleNumber, IncludeUIM, RatingZoneCode)
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
	DEDUCTIBLEAMOUNT, 
	COVERAGECODE, 
	SUBLINECODE, 
	PACKAGEMODIFICATIONADJUSTMENTGROUPDESCRIPTION, 
	PREMIUMMASTERDIRECTWRITTENPREMIUMAMOUNT, 
	PAIDLOSSAMOUNT, 
	OUTSTANDINGLOSSAMOUNT, 
	POLICYEXPIRATIONDATE, 
	INCEPTIONTODATEPAIDLOSSAMOUNT, 
	ClaimCoverageID AS CLAIMANTCOVERAGEDETAILID, 
	ANNUALSTATEMENTLINENUMBER, 
	ZIPPOSTALCODE, 
	DEDUCTIBLEINDICATORCODE, 
	CoverageLimitValue AS POLICYLOWERLIMIT, 
	POLICYUPPERLIMIT, 
	TERMINALZONECODE, 
	PremiumMasterExposure AS WRITTENEXPOSURE, 
	PAIDALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	OUTSTANDINGALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	CLAIMLOSSDATE, 
	PremiumTransactionEffectiveDate AS TRANSACTIONEFFECTIVEDATE, 
	COVERAGEGROUPCODE, 
	VEHICLENUMBER, 
	INCLUDEUIM, 
	RATINGZONECODE
	FROM FIL_ASL
),