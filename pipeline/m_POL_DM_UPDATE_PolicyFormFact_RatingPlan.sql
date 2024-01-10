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
SQ_PolicyFormFact AS (
	SELECT
		P.edw_pol_ak_id
		,PFF.PolicyFormFactId
		,IRD.EnterpriseGroupCode
		,IRD.InsuranceReferenceLegalEntityCode
		,IRD.StrategicProfitCenterCode
		,IRD.InsuranceSegmentCode
		,IRD.PolicyOfferingCode
		,IRD.ProductCode
		,IRD.InsuranceReferenceLineOfBusinessCode
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim P
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyFormFact PFF
		ON P.pol_dim_id = PFF.PolicyDimId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IRD
		ON PFF.InsuranceReferenceDimId = IRD.InsuranceReferenceDimId
	WHERE P.edw_pol_ak_id IN (SELECT
			P.edw_pol_ak_id
		FROM policy_dim P
		INNER JOIN PolicyFormFact PTCTF
			ON P.pol_dim_id = PTCTF.PolicyDimId
		INNER JOIN InsuranceReferenceDim IRD
			ON PTCTF.InsuranceReferenceDimId = IRD.InsuranceReferenceDimId
		GROUP BY P.edw_pol_ak_id
		HAVING COUNT(DISTINCT IRD.RatingPlanCode) > 1)
	@{pipeline().parameters.WHERE_CLAUSE_PFF}
	ORDER BY 1
),
EXP_PolicyFormFact AS (
	SELECT
	EDW_Pol_AK_id,
	PolicyFormFactId,
	EnterpriseGroupCode,
	InsuranceReferenceLegalEntityCode,
	StrategicProfitCenterCode,
	InsuranceSegmentCode,
	PolicyOfferingCode,
	ProductCode,
	InsuranceReferenceLineOfBusinessCode
	FROM SQ_PolicyFormFact
),
LKP_RatingPlan_DCT AS (
	SELECT
	RatingPlanAKId,
	PolicyAKID,
	RatingPlanCode
	FROM (
		SELECT
			P.pol_ak_id AS PolicyAKID
			,R.RatingPlanAKId AS RatingPlanAKId
			,RatingPlanCode AS RatingPlanCode
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
			ON P.pol_ak_id = RL.PolicyAKID
				AND P.crrnt_snpsht_flag = 1
				AND RL.CurrentSnapshotFlag = 1
				AND P.source_sys_id = 'DCT'
				AND RL.SourceSystemID = 'DCT'
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
			ON RL.RiskLocationAKID = PC.RiskLocationAKID
				AND PC.CurrentSnapshotFlag = 1
				AND PC.SourceSystemID = 'DCT'
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
			ON PC.PolicyCoverageAKID = RC.PolicyCoverageAKID
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
			ON RC.RatingCoverageAKID = PT.RatingCoverageAKId
				AND RC.EffectiveDate = PT.EffectiveDate
				AND PT.SourceSystemID = 'DCT'
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingPlan R
			ON PC.RatingPlanAKId = R.RatingPlanAKId
		WHERE PC.InsuranceLine = 'WorkersCompensation'
		AND PT.ReasonAmendedCode NOT IN ('COL', 'CWO', 'CWB', 'Claw Back')
		AND P.pol_ak_id IN (SELECT
				P.edw_pol_ak_id
			FROM @{pipeline().parameters.WC_DATA_MART_DATABASE_NAME}.DBO.policy_dim P
			INNER JOIN @{pipeline().parameters.WC_DATA_MART_DATABASE_NAME}.DBO.PolicyFormFact PTCTF
				ON P.pol_dim_id = PTCTF.PolicyDimId
			INNER JOIN @{pipeline().parameters.WC_DATA_MART_DATABASE_NAME}.DBO.InsuranceReferenceDim IRD
				ON PTCTF.InsuranceReferenceDimId = IRD.InsuranceReferenceDimId
			GROUP BY P.edw_pol_ak_id
			HAVING COUNT(DISTINCT IRD.RatingPlanCode) > 1)
		GROUP BY	P.pol_ak_id
					,R.RatingPlanCode
					,R.RatingPlanAKId
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
			INNER JOIN @{pipeline().parameters.WC_DATA_MART_DATABASE_NAME}.DBO.PolicyFormFact PTCTF
				ON P.pol_dim_id = PTCTF.PolicyDimId
			INNER JOIN @{pipeline().parameters.WC_DATA_MART_DATABASE_NAME}.DBO.InsuranceReferenceDim IRD
				ON PTCTF.InsuranceReferenceDimId = IRD.InsuranceReferenceDimId
			GROUP BY P.edw_pol_ak_id
			HAVING COUNT(DISTINCT IRD.RatingPlanCode) > 1)
		group by P.pol_ak_id,R.RatingPlanCode,R.RatingPlanAKId
		having sum(PT.PremiumTransactionAmount)<>0
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
	IFF(PMS_RatingPlanCode IS NULL,
		DCT_RatingPlanCode,
		PMS_RatingPlanCode
	) AS o_RatingPlanCode
	FROM 
	LEFT JOIN LKP_RatingPlan_DCT
	ON LKP_RatingPlan_DCT.PolicyAKID = EXP_PolicyFormFact.EDW_Pol_AK_id
	LEFT JOIN LKP_RatingPlan_PMS
	ON LKP_RatingPlan_PMS.PolicyAKID = EXP_PolicyFormFact.EDW_Pol_AK_id
),
EXP_PolFormFact_JNR AS (
	SELECT
	EXP_PolicyFormFact.PolicyFormFactId,
	EXP_PolicyFormFact.EnterpriseGroupCode,
	EXP_PolicyFormFact.InsuranceReferenceLegalEntityCode,
	EXP_PolicyFormFact.StrategicProfitCenterCode,
	EXP_PolicyFormFact.InsuranceSegmentCode,
	EXP_PolicyFormFact.PolicyOfferingCode,
	EXP_PolicyFormFact.ProductCode,
	EXP_PolicyFormFact.InsuranceReferenceLineOfBusinessCode,
	EXP_RatingPlanDerivation.o_RatingPlanCode AS RatingPlanCode,
	-- *INF*: :LKP.LKP_INSURANCEREFERENCE(EnterpriseGroupCode,InsuranceReferenceLegalEntityCode,StrategicProfitCenterCode,InsuranceSegmentCode,PolicyOfferingCode,ProductCode,InsuranceReferenceLineOfBusinessCode,RatingPlanCode)
	LKP_INSURANCEREFERENCE_EnterpriseGroupCode_InsuranceReferenceLegalEntityCode_StrategicProfitCenterCode_InsuranceSegmentCode_PolicyOfferingCode_ProductCode_InsuranceReferenceLineOfBusinessCode_RatingPlanCode.InsuranceReferenceDimId AS o_InsuranceReferenceDimId
	FROM EXP_PolicyFormFact
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
FILT_PFF AS (
	SELECT
	PolicyFormFactId, 
	o_InsuranceReferenceDimId
	FROM EXP_PolFormFact_JNR
	WHERE not ISNULL(o_InsuranceReferenceDimId)
),
UPD_PFF AS (
	SELECT
	PolicyFormFactId, 
	o_InsuranceReferenceDimId
	FROM FILT_PFF
),
PolicyFormFact AS (
	MERGE INTO PolicyFormFact AS T
	USING UPD_PFF AS S
	ON T.PolicyFormFactId = S.PolicyFormFactId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.InsuranceReferenceDimId = S.o_InsuranceReferenceDimId
),