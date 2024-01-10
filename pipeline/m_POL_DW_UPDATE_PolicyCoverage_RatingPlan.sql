WITH
LKP_InsuranceReference AS (
	SELECT
	InsuranceReferenceDimId,
	EnterpriseGroupCode,
	InsuranceReferenceLegalEntityCode,
	StrategicProfitCenterCode,
	InsuranceSegmentCode,
	PolicyOfferingCode,
	ProductCode,
	InsuranceReferenceLineOfBusinessCode
	FROM (
		SELECT 
			InsuranceReferenceDimId,
			EnterpriseGroupCode,
			InsuranceReferenceLegalEntityCode,
			StrategicProfitCenterCode,
			InsuranceSegmentCode,
			PolicyOfferingCode,
			ProductCode,
			InsuranceReferenceLineOfBusinessCode
		FROM InsuranceReferenceDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EnterpriseGroupCode,InsuranceReferenceLegalEntityCode,StrategicProfitCenterCode,InsuranceSegmentCode,PolicyOfferingCode,ProductCode,InsuranceReferenceLineOfBusinessCode ORDER BY InsuranceReferenceDimId) = 1
),
SQ_PolicyCoverage AS (
	Select PolicyCoverageID,PolicyAKID from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage
	where PolicyAKID in (select PolicyAKID from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage
	where InsuranceLine in ('WC','WorkersCompensation')
	group by PolicyAKID
	having count(distinct RatingPlanAKId)>1)
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_SRC_DataCollect AS (
	SELECT
	PolicyCoverageID,
	PolicyAKID
	FROM SQ_PolicyCoverage
),
LKP_RatingPlan_DCT AS (
	SELECT
	RatingPlanAKId,
	PolicyAKID,
	RatingPlanCode
	FROM (
		select P.pol_ak_id as PolicyAKID,R.RatingPlanAKId as RatingPlanAKId , RatingPlanCode as RatingPlanCode
		from @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
		on P.pol_ak_id=RL.PolicyAKID
		and P.crrnt_snpsht_flag=1
		and RL.CurrentSnapshotFlag=1
		and P.source_sys_id='DCT'
		and RL.SourceSystemID='DCT'
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on RL.RiskLocationAKID=PC.RiskLocationAKID
		and PC.CurrentSnapshotFlag=1
		and PC.SourceSystemID='DCT'
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
		on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		on RC.RatingCoverageAKID=PT.RatingCoverageAKId
		and RC.EffectiveDate=PT.EffectiveDate
		and PT.SourceSystemID='DCT'
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingPlan R
		on PC.RatingPlanAKId=R.RatingPlanAKId
		where PC.InsuranceLine='WorkersCompensation'
		and PT.ReasonAmendedCode not in ('COL','CWO','CWB','Claw Back')
		and P.pol_ak_id in (select PolicyAKID from PolicyCoverage
		where InsuranceLine='WorkersCompensation'
		and SourceSystemID='DCT'
		group by PolicyAKID
		having count(distinct RatingPlanAKId)>1)
		group by P.pol_ak_id,R.RatingPlanCode,R.RatingPlanAKId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID ORDER BY RatingPlanAKId DESC) = 1
),
LKP_RatingPlan_PMS AS (
	SELECT
	RatingPlanAKId,
	PolicyAKID,
	RatingPlanCode
	FROM (
		select P.pol_ak_id as PolicyAKID,R.RatingPlanAKId as RatingPlanAKId , R.RatingPlanCode as RatingPlanCode  
		from @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
		on P.pol_ak_id=RL.PolicyAKID
		and P.crrnt_snpsht_flag=1
		and RL.CurrentSnapshotFlag=1
		and P.source_sys_id='PMS'
		and RL.SourceSystemID='PMS'
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on RL.RiskLocationAKID=PC.RiskLocationAKID
		and PC.CurrentSnapshotFlag=1
		and PC.SourceSystemID='PMS'
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
		on PC.PolicyCoverageAKID=SC.PolicyCoverageAKID
		and SC.CurrentSnapshotFlag=1
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		on SC.StatisticalCoverageAKID=PT.StatisticalCoverageAKID
		and PT.SourceSystemID='PMS'
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingPlan R
		on PC.RatingPlanAKId=R.RatingPlanAKId
		where PC.InsuranceLine='WC'
		and PT.ReasonAmendedCode not in ('COL','CWO','CWB')
		and P.pol_ak_id in (select PolicyAKID from PolicyCoverage
		where InsuranceLine='WC'
		and SourceSystemID='PMS'
		group by PolicyAKID
		having count(distinct RatingPlanAKId)>1)
		group by P.pol_ak_id,R.RatingPlanCode,R.RatingPlanAKId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID ORDER BY RatingPlanAKId DESC) = 1
),
EXP_RatingPlanDerivation AS (
	SELECT
	EXP_SRC_DataCollect.PolicyCoverageID,
	EXP_SRC_DataCollect.PolicyAKID,
	LKP_RatingPlan_PMS.RatingPlanAKId AS PMS_RatingPlanAKId,
	LKP_RatingPlan_PMS.RatingPlanCode AS PMS_RatingPlanCode,
	LKP_RatingPlan_DCT.RatingPlanAKId AS DCT_RatingPlanAKId,
	LKP_RatingPlan_DCT.RatingPlanCode AS DCT_RatingPlanCode,
	-- *INF*: IIF(ISNULL(PMS_RatingPlanAKId),DCT_RatingPlanAKId,PMS_RatingPlanAKId)
	IFF(PMS_RatingPlanAKId IS NULL,
		DCT_RatingPlanAKId,
		PMS_RatingPlanAKId
	) AS o_RatingPlanAKId,
	-- *INF*: IIF(ISNULL(PMS_RatingPlanCode),DCT_RatingPlanCode,PMS_RatingPlanCode)
	IFF(PMS_RatingPlanCode IS NULL,
		DCT_RatingPlanCode,
		PMS_RatingPlanCode
	) AS o_RatingPlanCode
	FROM EXP_SRC_DataCollect
	LEFT JOIN LKP_RatingPlan_DCT
	ON LKP_RatingPlan_DCT.PolicyAKID = EXP_SRC_DataCollect.PolicyAKID
	LEFT JOIN LKP_RatingPlan_PMS
	ON LKP_RatingPlan_PMS.PolicyAKID = EXP_SRC_DataCollect.PolicyAKID
),
FIL_Exclude_Nulls AS (
	SELECT
	PolicyCoverageID, 
	o_RatingPlanAKId AS RatingPlanAKId
	FROM EXP_RatingPlanDerivation
	WHERE not ISNULL(RatingPlanAKId)
),
UPD_RatingPlanAKId_PolicyCoverage AS (
	SELECT
	PolicyCoverageID, 
	RatingPlanAKId AS o_RatingPlanAKId
	FROM FIL_Exclude_Nulls
),
PolicyCoverage AS (
	MERGE INTO PolicyCoverage AS T
	USING UPD_RatingPlanAKId_PolicyCoverage AS S
	ON T.PolicyCoverageID = S.PolicyCoverageID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.RatingPlanAKId = S.o_RatingPlanAKId
),