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
	InsuranceReferenceLineOfBusinessCode,
	RatingPlanCode
	FROM (
		SELECT 
			InsuranceReferenceDimId,
			EnterpriseGroupCode,
			InsuranceReferenceLegalEntityCode,
			StrategicProfitCenterCode,
			InsuranceSegmentCode,
			PolicyOfferingCode,
			ProductCode,
			InsuranceReferenceLineOfBusinessCode,
			RatingPlanCode
		FROM InsuranceReferenceDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EnterpriseGroupCode,InsuranceReferenceLegalEntityCode,StrategicProfitCenterCode,InsuranceSegmentCode,PolicyOfferingCode,ProductCode,InsuranceReferenceLineOfBusinessCode,RatingPlanCode ORDER BY InsuranceReferenceDimId) = 1
),
SQ_LimitFact AS (
	select P.edw_pol_ak_id,
	LF.LimitFactId,
	IRD.EnterpriseGroupCode,
	IRD.InsuranceReferenceLegalEntityCode,
	IRD.StrategicProfitCenterCode,
	IRD.InsuranceSegmentCode,
	IRD.PolicyOfferingCode,
	IRD.ProductCode,
	IRD.InsuranceReferenceLineOfBusinessCode
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim P
	inner Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.LimitFact LF
	on P.pol_dim_id=LF.PolicyDimId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IRD
	on LF.InsuranceReferenceDimId=IRD.InsuranceReferenceDimId
	where P.edw_pol_ak_id in (SELECT
		P.edw_pol_ak_id
	FROM policy_dim P
	INNER JOIN LimitFact PTCTF
		ON P.pol_dim_id = PTCTF.PolicyDimId
	INNER JOIN InsuranceReferenceDim IRD
		ON PTCTF.InsuranceReferenceDimId = IRD.InsuranceReferenceDimId
	GROUP BY P.edw_pol_ak_id
	HAVING COUNT(DISTINCT IRD.RatingPlanCode) > 1)
	order by 1
),
EXP_LimitFact AS (
	SELECT
	EDW_Pol_AK_id,
	LimitFactId,
	EnterpriseGroupCode,
	InsuranceReferenceLegalEntityCode,
	StrategicProfitCenterCode,
	InsuranceSegmentCode,
	PolicyOfferingCode,
	ProductCode,
	InsuranceReferenceLineOfBusinessCode
	FROM SQ_LimitFact
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
		and P.pol_ak_id in (SELECT
			P.edw_pol_ak_id
		FROM @{pipeline().parameters.WC_DATA_MART_DATABASE_NAME}.DBO.policy_dim P
		INNER JOIN @{pipeline().parameters.WC_DATA_MART_DATABASE_NAME}.DBO.LimitFact PTCTF
			ON P.pol_dim_id = PTCTF.PolicyDimId
		INNER JOIN @{pipeline().parameters.WC_DATA_MART_DATABASE_NAME}.DBO.InsuranceReferenceDim IRD
			ON PTCTF.InsuranceReferenceDimId = IRD.InsuranceReferenceDimId
		GROUP BY P.edw_pol_ak_id
		HAVING COUNT(DISTINCT IRD.RatingPlanCode) > 1)
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
		and P.pol_ak_id in (SELECT
			P.edw_pol_ak_id
		FROM @{pipeline().parameters.WC_DATA_MART_DATABASE_NAME}.DBO.policy_dim P
		INNER JOIN @{pipeline().parameters.WC_DATA_MART_DATABASE_NAME}.DBO.LimitFact PTCTF
			ON P.pol_dim_id = PTCTF.PolicyDimId
		INNER JOIN @{pipeline().parameters.WC_DATA_MART_DATABASE_NAME}.DBO.InsuranceReferenceDim IRD
			ON PTCTF.InsuranceReferenceDimId = IRD.InsuranceReferenceDimId
		GROUP BY P.edw_pol_ak_id
		HAVING COUNT(DISTINCT IRD.RatingPlanCode) > 1)
		group by P.pol_ak_id,R.RatingPlanCode,R.RatingPlanAKId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID ORDER BY RatingPlanAKId DESC) = 1
),
EXP_RatingPlanDerivation AS (
	SELECT
	LKP_RatingPlan_PMS.RatingPlanAKId AS PMS_RatingPlanAKId,
	LKP_RatingPlan_PMS.RatingPlanCode AS PMS_RatingPlanCode,
	LKP_RatingPlan_DCT.RatingPlanAKId AS DCT_RatingPlanAKId,
	LKP_RatingPlan_DCT.RatingPlanCode AS DCT_RatingPlanCode,
	-- *INF*: IIF(ISNULL(PMS_RatingPlanCode),DCT_RatingPlanCode,PMS_RatingPlanCode)
	IFF(PMS_RatingPlanCode IS NULL, DCT_RatingPlanCode, PMS_RatingPlanCode) AS o_RatingPlanCode
	FROM 
	LEFT JOIN LKP_RatingPlan_DCT
	ON LKP_RatingPlan_DCT.PolicyAKID = EXP_LimitFact.EDW_Pol_AK_id
	LEFT JOIN LKP_RatingPlan_PMS
	ON LKP_RatingPlan_PMS.PolicyAKID = EXP_LimitFact.EDW_Pol_AK_id
),
EXP_LmtFact_JNR AS (
	SELECT
	EXP_LimitFact.LimitFactId,
	EXP_LimitFact.EnterpriseGroupCode,
	EXP_LimitFact.InsuranceReferenceLegalEntityCode,
	EXP_LimitFact.StrategicProfitCenterCode,
	EXP_LimitFact.InsuranceSegmentCode,
	EXP_LimitFact.PolicyOfferingCode,
	EXP_LimitFact.ProductCode,
	EXP_LimitFact.InsuranceReferenceLineOfBusinessCode,
	EXP_RatingPlanDerivation.o_RatingPlanCode AS RatingPlanCode,
	-- *INF*: :LKP.LKP_INSURANCEREFERENCE(EnterpriseGroupCode,InsuranceReferenceLegalEntityCode,StrategicProfitCenterCode,InsuranceSegmentCode,PolicyOfferingCode,ProductCode,InsuranceReferenceLineOfBusinessCode,RatingPlanCode)
	LKP_INSURANCEREFERENCE_EnterpriseGroupCode_InsuranceReferenceLegalEntityCode_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_RatingPlanCode.InsuranceReferenceDimId AS o_InsuranceReferenceDimId
	FROM EXP_LimitFact
	 -- Manually join with EXP_RatingPlanDerivation
	LEFT JOIN LKP_INSURANCEREFERENCE LKP_INSURANCEREFERENCE_EnterpriseGroupCode_InsuranceReferenceLegalEntityCode_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_RatingPlanCode
	ON LKP_INSURANCEREFERENCE_EnterpriseGroupCode_InsuranceReferenceLegalEntityCode_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_RatingPlanCode.EnterpriseGroupCode = EnterpriseGroupCode
	AND LKP_INSURANCEREFERENCE_EnterpriseGroupCode_InsuranceReferenceLegalEntityCode_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_RatingPlanCode.InsuranceReferenceLegalEntityCode = InsuranceReferenceLegalEntityCode
	AND LKP_INSURANCEREFERENCE_EnterpriseGroupCode_InsuranceReferenceLegalEntityCode_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_RatingPlanCode.StrategicProfitCenterCode = StrategicProfitCenterCode
	AND LKP_INSURANCEREFERENCE_EnterpriseGroupCode_InsuranceReferenceLegalEntityCode_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_RatingPlanCode.InsuranceSegmentCode = InsuranceSegmentCode
	AND LKP_INSURANCEREFERENCE_EnterpriseGroupCode_InsuranceReferenceLegalEntityCode_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_RatingPlanCode.PolicyOfferingCode = PolicyOfferingCode
	AND LKP_INSURANCEREFERENCE_EnterpriseGroupCode_InsuranceReferenceLegalEntityCode_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_RatingPlanCode.ProductCode = ProductCode
	AND LKP_INSURANCEREFERENCE_EnterpriseGroupCode_InsuranceReferenceLegalEntityCode_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_RatingPlanCode.InsuranceReferenceLineOfBusinessCode = InsuranceReferenceLineOfBusinessCode
	AND LKP_INSURANCEREFERENCE_EnterpriseGroupCode_InsuranceReferenceLegalEntityCode_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_RatingPlanCode.RatingPlanCode = RatingPlanCode

),
FILT_LF AS (
	SELECT
	LimitFactId, 
	o_InsuranceReferenceDimId
	FROM EXP_LmtFact_JNR
	WHERE not ISNULL(o_InsuranceReferenceDimId)
),
UPD_LF AS (
	SELECT
	LimitFactId, 
	o_InsuranceReferenceDimId
	FROM FILT_LF
),
LimitFact AS (
	MERGE INTO LimitFact AS T
	USING UPD_LF AS S
	ON T.LimitFactId = S.LimitFactId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.InsuranceReferenceDimId = S.o_InsuranceReferenceDimId
),