WITH
SQ_EarnedPremiumTransactionMonthlyFact AS (
	SELECT 
	EPTF.AgencyDimID,
	EPTF.PolicyDimID,
	EPTF.ContractCustomerDimID,
	EPTF.PremiumTransactionRunDateID,
	SUM(MonthlyChangeinDirectEarnedPremium) AS MonthlyChangeinDirectEarnedPremium,
	SUM(MonthlyChangeinCededEarnedPremium) AS MonthlyChangeinCededEarnedPremium,
	SUM(MonthlyChangeInDirectUnearnedPremium) AS MonthlyChangeInDirectUnearnedPremium,
	SUM(MonthlyChangeInCededUnearnedPremium) AS MonthlyChangeInCededUnearnedPremium,
	EPTF.InsuranceReferenceDimId,
	a.SalesDivisionDimId,
	CASE WHEN PCSD.PolicyStatusDescription='Inforce'
	THEN 'Y' ELSE 'N' END AS TransactionInforceFlag
	
	FROM
	 @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.EarnedPremiumTransactionMonthlyFact EPTF
	 INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim PD
	 on EPTF.PolicyDimID=PD.pol_dim_id
	 INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim CLDRUN
	 ON EPTF.PremiumTransactionRunDateID=CLDRUN.clndr_id
	 INNER JOIN  @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V3}.AgencyDim a on a.AgencyDimId=EPTF.AgencyDimId
	 LEFT JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCurrentStatusDim PCSD
	 ON PCSD.EDWPolicyAKId=PD.edw_pol_ak_id
	 AND PCSD.RunDate=CLDRUN.clndr_date
	 WHERE CLDRUN.clndr_date>=@{pipeline().parameters.RUNDATE} @{pipeline().parameters.WHERE_CLAUSE_EP}
	 GROUP BY 
	EPTF.PremiumTransactionRunDateID,
	EPTF.AgencyDimID,
	EPTF.PolicyDimID,
	EPTF.ContractCustomerDimID,
	EPTF.InsuranceReferenceDimId,
	a.SalesDivisionDimId,
	CASE WHEN PCSD.PolicyStatusDescription='Inforce'
	THEN 'Y' ELSE 'N' END
),
EXP_GetVal_EP AS (
	SELECT
	AgencyDimID AS i_AgencyDimID,
	PolicyDimID AS i_PolicyDimID,
	ContractCustomerDimID AS i_ContractCustomerDimID,
	PremiumTransactionRunDateID AS i_PremiumTransactionRunDateID,
	MonthlyChangeinDirectEarnedPremium AS i_MonthlyChangeinDirectEarnedPremium,
	MonthlyChangeinCededEarnedPremium AS i_MonthlyChangeinCededEarnedPremium,
	MonthlyChangeInDirectUnearnedPremium AS i_MonthlyChangeInDirectUnearnedPremium,
	MonthlyChangeInCededUnearnedPremium AS i_MonthlyChangeInCededUnearnedPremium,
	InsuranceReferenceDimId AS i_InsuranceReferenceDimId,
	SalesDivisionDimId AS i_SalesDivisionDimId,
	TransactionInforceFlag AS i_TransactionInforceFlag,
	i_PremiumTransactionRunDateID AS o_PremiumTransactionRunDateID,
	i_AgencyDimID AS o_AgencyDimID,
	i_PolicyDimID AS o_PolicyDimID,
	i_ContractCustomerDimID AS o_ContractCustomerDimID,
	i_InsuranceReferenceDimId AS o_InsuranceReferenceDimId,
	i_SalesDivisionDimId AS o_SalesDivisionDimId,
	i_MonthlyChangeinDirectEarnedPremium AS o_MonthlyChangeinDirectEarnedPremium,
	i_MonthlyChangeinCededEarnedPremium AS o_MonthlyChangeinCededEarnedPremium,
	i_MonthlyChangeInDirectUnearnedPremium AS o_MonthlyDirectUnearnedPremium,
	i_MonthlyChangeInCededUnearnedPremium AS o_MonthlyCededUnearnedPremium,
	0 AS o_PremiumMasterAgencyCededWrittenCommission,
	0 AS o_PremiumMasterAgencyDirectWrittenCommission,
	0 AS o_PremiumMasterAuditPremium,
	0 AS o_PremiumMasterCededWrittenPremium,
	0 AS o_PremiumMasterCollectionWriteOffPremium,
	0 AS o_PremiumMasterDirectWrittenPremium,
	0 AS o_PremiumMasterReturnedPremium,
	i_TransactionInforceFlag AS o_TransactionInforceFlag
	FROM SQ_EarnedPremiumTransactionMonthlyFact
),
SQ_PremiumMasterFact AS (
	SELECT 
	PMF.AgencyDimID,
	PMF.PolicyDimID,
	PMF.ContractCustomerDimID,
	PMF.PremiumMasterRunDateID,
	SUM(PMF.PremiumMasterDirectWrittenPremium) AS PremiumMasterDirectWrittenPremium,
	SUM(PMF.PremiumMasterCededWrittenPremium) AS PremiumMasterCededWrittenPremium,
	SUM(PMF.PremiumMasterAgencyDirectWrittenCommission) AS PremiumMasterAgencyDirectWrittenCommission,
	SUM(PMF.PremiumMasterAgencyCededWrittenCommission) AS PremiumMasterAgencyCededWrittenCommission,
	PMF.InsuranceReferenceDimId,
	PMF.SalesDivisionDimId,
	SUM(PMF.PremiumMasterAuditPremium) AS PremiumMasterAuditPremium,
	SUM(PMF.PremiumMasterReturnedPremium) AS PremiumMasterReturnedPremium,
	SUM(PMF.PremiumMasterCollectionWriteOffPremium) AS PremiumMasterCollectionWriteOffPremium,
	CASE WHEN PCSD.PolicyStatusDescription='Inforce'
	THEN 'Y' ELSE 'N' END AS TransactionInforceFlag
	
	FROM
	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumMasterFact PMF
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim PD
	on PMF.PolicyDimID=PD.pol_dim_id
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim CLDRUN
	ON PMF.PremiumMasterRunDateID=CLDRUN.clndr_id
	LEFT JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCurrentStatusDim PCSD
	ON PCSD.EDWPolicyAKId=PD.edw_pol_ak_id
	 AND PCSD.RunDate=CLDRUN.clndr_date
	WHERE CLDRUN.clndr_date>=@{pipeline().parameters.RUNDATE} 
	and not exists (select 1 from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransactionTypeDim PTTD 
	                      where PMF.PremiumTransactionTypeDimID = PTTD.PremiumTransactionTypeDimID 
	                      and PTTD.PremiumTypeCode = 'C' AND PTTD.ReasonAmendedCode in ('CWO','COL'))
	@{pipeline().parameters.WHERE_CLAUSE_PMF}
	GROUP BY 
	PMF.PremiumMasterRunDateID,
	PMF.AgencyDimID,
	PMF.PolicyDimID,
	PMF.ContractCustomerDimID,
	PMF.InsuranceReferenceDimId,
	PMF.SalesDivisionDimId,
	CASE WHEN PCSD.PolicyStatusDescription='Inforce'
	THEN 'Y' ELSE 'N' END
),
EXP_GetVal_PM AS (
	SELECT
	AgencyDimID AS i_AgencyDimID,
	PolicyDimID AS i_PolicyDimID,
	ContractCustomerDimID AS i_ContractCustomerDimID,
	PremiumMasterRunDateID AS i_PremiumMasterRunDateID,
	PremiumMasterDirectWrittenPremium AS i_PremiumMasterDirectWrittenPremium,
	PremiumMasterCededWrittenPremium AS i_PremiumMasterCededWrittenPremium,
	PremiumMasterAgencyDirectWrittenCommission AS i_PremiumMasterAgencyDirectWrittenCommission,
	PremiumMasterAgencyCededWrittenCommission AS i_PremiumMasterAgencyCededWrittenCommission,
	InsuranceReferenceDimId AS i_InsuranceReferenceDimId,
	SalesDivisionDimId AS i_SalesDivisionDimId,
	PremiumMasterAuditPremium AS i_PremiumMasterAuditPremium,
	PremiumMasterReturnedPremium AS i_PremiumMasterReturnedPremium,
	PremiumMasterCollectionWriteOffPremium AS i_PremiumMasterCollectionWriteOffPremium,
	TransactionInforceFlag AS i_TransactionInforceFlag,
	i_PremiumMasterRunDateID AS o_PremiumMasterRunDateID,
	i_AgencyDimID AS o_AgencyDimID,
	i_PolicyDimID AS o_PolicyDimID,
	i_ContractCustomerDimID AS o_ContractCustomerDimID,
	i_InsuranceReferenceDimId AS o_InsuranceReferenceDimId,
	i_SalesDivisionDimId AS o_SalesDivisionDimId,
	0 AS o_MonthlyChangeinDirectEarnedPremium,
	0 AS o_MonthlyChangeinCededEarnedPremium,
	0 AS o_MonthlyDirectUnearnedPremium,
	0 AS o_MonthlyCededUnearnedPremium,
	i_PremiumMasterAgencyCededWrittenCommission AS o_PremiumMasterAgencyCededWrittenCommission,
	i_PremiumMasterAgencyDirectWrittenCommission AS o_PremiumMasterAgencyDirectWrittenCommission,
	i_PremiumMasterAuditPremium AS o_PremiumMasterAuditPremium,
	i_PremiumMasterCededWrittenPremium AS o_PremiumMasterCededWrittenPremium,
	i_PremiumMasterCollectionWriteOffPremium AS o_PremiumMasterCollectionWriteOffPremium,
	i_PremiumMasterDirectWrittenPremium AS o_PremiumMasterDirectWrittenPremium,
	i_PremiumMasterReturnedPremium AS o_PremiumMasterReturnedPremium,
	i_TransactionInforceFlag AS o_TransactionInforceFlag
	FROM SQ_PremiumMasterFact
),
Union_EP_PM AS (
	SELECT o_PremiumTransactionRunDateID AS SnapshotDateID, o_AgencyDimID AS AgencyDimID, o_PolicyDimID AS PolicyDimId, o_ContractCustomerDimID AS ContractCustomerDimID, o_InsuranceReferenceDimId AS InsuranceReferenceDimId, o_SalesDivisionDimId AS SalesDivisionDimID, o_MonthlyChangeinDirectEarnedPremium AS MonthlyChangeinDirectEarnedPremium, o_MonthlyChangeinCededEarnedPremium AS MonthlyChangeinCededEarnedPremium, o_MonthlyDirectUnearnedPremium AS MonthlyChangeInDirectUnearnedPremium, o_MonthlyCededUnearnedPremium AS MonthlyChangeInCededUnearnedPremium, o_PremiumMasterAgencyCededWrittenCommission AS PremiumMasterAgencyCededWrittenCommission, o_PremiumMasterAgencyDirectWrittenCommission AS PremiumMasterAgencyDirectWrittenCommission, o_PremiumMasterAuditPremium AS PremiumMasterAuditPremium, o_PremiumMasterCededWrittenPremium AS PremiumMasterCededWrittenPremium, o_PremiumMasterCollectionWriteOffPremium AS PremiumMasterCollectionWriteOffPremium, o_PremiumMasterDirectWrittenPremium AS PremiumMasterDirectWrittenPremium, o_PremiumMasterReturnedPremium AS PremiumMasterReturnedPremium, o_TransactionInforceFlag AS TransactionInforceFlag
	FROM EXP_GetVal_EP
	UNION
	SELECT o_PremiumMasterRunDateID AS SnapshotDateID, o_AgencyDimID AS AgencyDimID, o_PolicyDimID AS PolicyDimId, o_ContractCustomerDimID AS ContractCustomerDimID, o_InsuranceReferenceDimId AS InsuranceReferenceDimId, o_SalesDivisionDimId AS SalesDivisionDimID, o_MonthlyChangeinDirectEarnedPremium AS MonthlyChangeinDirectEarnedPremium, o_MonthlyChangeinCededEarnedPremium AS MonthlyChangeinCededEarnedPremium, o_MonthlyDirectUnearnedPremium AS MonthlyChangeInDirectUnearnedPremium, o_MonthlyCededUnearnedPremium AS MonthlyChangeInCededUnearnedPremium, o_PremiumMasterAgencyCededWrittenCommission AS PremiumMasterAgencyCededWrittenCommission, o_PremiumMasterAgencyDirectWrittenCommission AS PremiumMasterAgencyDirectWrittenCommission, o_PremiumMasterAuditPremium AS PremiumMasterAuditPremium, o_PremiumMasterCededWrittenPremium AS PremiumMasterCededWrittenPremium, o_PremiumMasterCollectionWriteOffPremium AS PremiumMasterCollectionWriteOffPremium, o_PremiumMasterDirectWrittenPremium AS PremiumMasterDirectWrittenPremium, o_PremiumMasterReturnedPremium AS PremiumMasterReturnedPremium, o_TransactionInforceFlag AS TransactionInforceFlag
	FROM EXP_GetVal_PM
),
SRT_Records AS (
	SELECT
	SnapshotDateID, 
	AgencyDimID, 
	PolicyDimId, 
	ContractCustomerDimID, 
	InsuranceReferenceDimId, 
	SalesDivisionDimID, 
	MonthlyChangeinDirectEarnedPremium, 
	MonthlyChangeinCededEarnedPremium, 
	MonthlyChangeInDirectUnearnedPremium, 
	MonthlyChangeInCededUnearnedPremium, 
	PremiumMasterAgencyCededWrittenCommission, 
	PremiumMasterAgencyDirectWrittenCommission, 
	PremiumMasterAuditPremium, 
	PremiumMasterCededWrittenPremium, 
	PremiumMasterCollectionWriteOffPremium, 
	PremiumMasterDirectWrittenPremium, 
	PremiumMasterReturnedPremium, 
	TransactionInforceFlag
	FROM Union_EP_PM
	ORDER BY SnapshotDateID ASC, AgencyDimID ASC, PolicyDimId ASC, ContractCustomerDimID ASC, InsuranceReferenceDimId ASC, SalesDivisionDimID ASC
),
AGG_EP_PM AS (
	SELECT
	SnapshotDateID, 
	AgencyDimID, 
	PolicyDimId, 
	ContractCustomerDimID, 
	InsuranceReferenceDimId, 
	SalesDivisionDimID, 
	MonthlyChangeinDirectEarnedPremium, 
	ROUND(SUM(MonthlyChangeinDirectEarnedPremium), 4) AS MonthlyChangeinDirectEarnedPremium_out, 
	MonthlyChangeinCededEarnedPremium, 
	ROUND(SUM(MonthlyChangeinCededEarnedPremium), 4) AS MonthlyChangeinCededEarnedPremium_out, 
	MonthlyChangeInDirectUnearnedPremium, 
	ROUND(SUM(MonthlyChangeInDirectUnearnedPremium), 4) AS MonthlyChangeInDirectUnearnedPremium_out, 
	MonthlyChangeInCededUnearnedPremium, 
	ROUND(SUM(MonthlyChangeInCededUnearnedPremium), 4) AS MonthlyChangeInCededUnearnedPremium_out, 
	PremiumMasterAgencyCededWrittenCommission, 
	ROUND(SUM(PremiumMasterAgencyCededWrittenCommission), 4) AS PremiumMasterAgencyCededWrittenCommission_out, 
	PremiumMasterAgencyDirectWrittenCommission, 
	ROUND(SUM(PremiumMasterAgencyDirectWrittenCommission), 4) AS PremiumMasterAgencyDirectWrittenCommission_out, 
	PremiumMasterAuditPremium, 
	ROUND(SUM(PremiumMasterAuditPremium), 4) AS PremiumMasterAuditPremium_out, 
	PremiumMasterCededWrittenPremium, 
	ROUND(SUM(PremiumMasterCededWrittenPremium), 4) AS PremiumMasterCededWrittenPremium_out, 
	PremiumMasterCollectionWriteOffPremium, 
	ROUND(SUM(PremiumMasterCollectionWriteOffPremium), 4) AS PremiumMasterCollectionWriteOffPremium_out, 
	PremiumMasterDirectWrittenPremium, 
	ROUND(SUM(PremiumMasterDirectWrittenPremium), 4) AS PremiumMasterDirectWrittenPremium_out, 
	PremiumMasterReturnedPremium, 
	ROUND(SUM(PremiumMasterReturnedPremium), 4) AS PremiumMasterReturnedPremium_out, 
	TransactionInforceFlag
	FROM SRT_Records
	GROUP BY SnapshotDateID, AgencyDimID, PolicyDimId, ContractCustomerDimID, InsuranceReferenceDimId, SalesDivisionDimID
),
LKP_InsuranceReferenceDim AS (
	SELECT
	ProductCode,
	InsuranceReferenceLineOfBusinessCode,
	StrategicProfitCenterCode,
	InsuranceReferenceDimId
	FROM (
		SELECT 
			ProductCode,
			InsuranceReferenceLineOfBusinessCode,
			StrategicProfitCenterCode,
			InsuranceReferenceDimId
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceReferenceDimId ORDER BY ProductCode) = 1
),
LKP_Policy_Dim AS (
	SELECT
	pol_key,
	ProgramCode,
	AssociationCode,
	pol_exp_date,
	pol_issue_code,
	pol_mod,
	pol_eff_date,
	pol_enter_date,
	edw_pol_ak_id,
	pol_dim_id
	FROM (
		SELECT 
			pol_key,
			ProgramCode,
			AssociationCode,
			pol_exp_date,
			pol_issue_code,
			pol_mod,
			pol_eff_date,
			pol_enter_date,
			edw_pol_ak_id,
			pol_dim_id
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_dim_id ORDER BY pol_key) = 1
),
LKP_calendar_dim AS (
	SELECT
	CalendarDate,
	clndr_id
	FROM (
		SELECT 
			CalendarDate,
			clndr_id
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_id ORDER BY CalendarDate) = 1
),
LKP_PolicyCurrentStatusDim AS (
	SELECT
	PolicyCancellationDate,
	PolicyStatusDescription,
	RunDate,
	EDWPolicyAKId
	FROM (
		SELECT 
			PolicyCancellationDate,
			PolicyStatusDescription,
			RunDate,
			EDWPolicyAKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCurrentStatusDim
		WHERE RunDate>=@{pipeline().parameters.RUNDATE}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RunDate,EDWPolicyAKId ORDER BY PolicyCancellationDate) = 1
),
EXP_Target AS (
	SELECT
	LKP_Policy_Dim.pol_key AS lkp_pol_key,
	LKP_Policy_Dim.ProgramCode AS lkp_ProgramCode,
	LKP_InsuranceReferenceDim.ProductCode AS lkp_ProductCode,
	LKP_InsuranceReferenceDim.InsuranceReferenceLineOfBusinessCode AS lkp_InsuranceReferenceLineOfBusinessCode,
	LKP_InsuranceReferenceDim.StrategicProfitCenterCode AS lkp_StrategicProfitCenterCode,
	LKP_Policy_Dim.AssociationCode AS lkp_assoc_code,
	LKP_Policy_Dim.pol_exp_date AS lkp_pol_exp_date,
	LKP_PolicyCurrentStatusDim.PolicyCancellationDate AS lkp_PolicyCancellationDate,
	LKP_PolicyCurrentStatusDim.PolicyStatusDescription AS lkp_PolicyStatusDescription,
	LKP_Policy_Dim.pol_issue_code AS lkp_pol_issue_code,
	LKP_Policy_Dim.pol_mod AS lkp_pol_mod,
	LKP_Policy_Dim.pol_eff_date AS lkp_pol_eff_date,
	LKP_Policy_Dim.pol_enter_date AS lkp_pol_enter_date,
	LKP_calendar_dim.CalendarDate AS lkp_CalendarDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	AGG_EP_PM.SnapshotDateID,
	AGG_EP_PM.AgencyDimID,
	AGG_EP_PM.PolicyDimId,
	AGG_EP_PM.ContractCustomerDimID,
	AGG_EP_PM.InsuranceReferenceDimId,
	AGG_EP_PM.SalesDivisionDimID,
	AGG_EP_PM.MonthlyChangeinDirectEarnedPremium_out AS MonthlyChangeinDirectEarnedPremium,
	AGG_EP_PM.MonthlyChangeinCededEarnedPremium_out AS MonthlyChangeinCededEarnedPremium,
	AGG_EP_PM.MonthlyChangeInDirectUnearnedPremium_out AS MonthlyChangeInDirectUnearnedPremium,
	AGG_EP_PM.MonthlyChangeInCededUnearnedPremium_out AS MonthlyChangeInCededUnearnedPremium,
	AGG_EP_PM.PremiumMasterAgencyCededWrittenCommission_out AS PremiumMasterAgencyCededWrittenCommission,
	AGG_EP_PM.PremiumMasterAgencyDirectWrittenCommission_out AS PremiumMasterAgencyDirectWrittenCommission,
	AGG_EP_PM.PremiumMasterAuditPremium_out AS PremiumMasterAuditPremium,
	AGG_EP_PM.PremiumMasterCededWrittenPremium_out AS PremiumMasterCededWrittenPremium,
	AGG_EP_PM.PremiumMasterCollectionWriteOffPremium_out AS PremiumMasterCollectionWriteOffPremium,
	AGG_EP_PM.PremiumMasterDirectWrittenPremium_out AS PremiumMasterDirectWrittenPremium,
	AGG_EP_PM.PremiumMasterReturnedPremium_out AS PremiumMasterReturnedPremium,
	AGG_EP_PM.TransactionInforceFlag,
	-- *INF*: LTRIM(RTRIM(lkp_pol_key))
	LTRIM(RTRIM(lkp_pol_key)) AS v_pol_key,
	-- *INF*: LTRIM(RTRIM(lkp_ProgramCode))
	LTRIM(RTRIM(lkp_ProgramCode)) AS v_ProgramCode,
	-- *INF*: LTRIM(RTRIM(lkp_ProductCode))
	LTRIM(RTRIM(lkp_ProductCode)) AS v_ProductCode,
	-- *INF*: LTRIM(RTRIM(lkp_InsuranceReferenceLineOfBusinessCode))
	LTRIM(RTRIM(lkp_InsuranceReferenceLineOfBusinessCode)) AS v_InsuranceReferenceLineOfBusinessCode,
	-- *INF*: LTRIM(RTRIM(lkp_assoc_code))
	LTRIM(RTRIM(lkp_assoc_code)) AS v_assoc_code,
	v_pol_key AS v_DistinctPolicyKeyCounter,
	v_pol_key||'/'||v_ProductCode AS v_DistinctPolicyProductCounter,
	v_pol_key||'/'||v_InsuranceReferenceLineOfBusinessCode AS v_DistinctPolicyLineOfBusinessCounter,
	v_pol_key||'/'||v_ProgramCode AS v_DistinctPolicyProgramCounter,
	v_pol_key||'/'||v_assoc_code AS v_DistinctPolicyAssociationCounter,
	lkp_CalendarDate AS v_RunDate,
	-- *INF*: IIF(TransactionInforceFlag='Y',v_DistinctPolicyKeyCounter,NULL)
	IFF(TransactionInforceFlag = 'Y', v_DistinctPolicyKeyCounter, NULL) AS o_InforceDistinctPolicyCounter,
	-- *INF*: IIF(lkp_pol_exp_date>ADD_TO_DATE(v_RunDate,'MM',-1) AND lkp_pol_exp_date<=v_RunDate,v_DistinctPolicyKeyCounter,NULL)
	IFF(lkp_pol_exp_date > ADD_TO_DATE(v_RunDate, 'MM', - 1) AND lkp_pol_exp_date <= v_RunDate, v_DistinctPolicyKeyCounter, NULL) AS o_ExpiredPolicyOfferingCounter,
	-- *INF*: IIF(lkp_PolicyStatusDescription='Cancelled' AND GET_DATE_PART(lkp_PolicyCancellationDate,'MM')=GET_DATE_PART(v_RunDate,'MM'),v_DistinctPolicyKeyCounter,NULL)
	IFF(lkp_PolicyStatusDescription = 'Cancelled' AND GET_DATE_PART(lkp_PolicyCancellationDate, 'MM') = GET_DATE_PART(v_RunDate, 'MM'), v_DistinctPolicyKeyCounter, NULL) AS o_CancelledPolicyCounter,
	-- *INF*: IIF(TransactionInforceFlag='Y' AND lkp_pol_issue_code='N' ,v_DistinctPolicyKeyCounter,NULL)
	IFF(TransactionInforceFlag = 'Y' AND lkp_pol_issue_code = 'N', v_DistinctPolicyKeyCounter, NULL) AS o_NewInforceDistinctPolicyCounter,
	-- *INF*: IIF(
	-- ((TRUNC(lkp_pol_eff_date,'MM')<=TRUNC(v_RunDate,'MM') AND TRUNC(lkp_pol_enter_date,'MM')=TRUNC(v_RunDate,'MM')) OR (TRUNC(lkp_pol_eff_date,'MM')=TRUNC(v_RunDate,'MM') AND TRUNC(lkp_pol_enter_date,'MM')<=TRUNC(v_RunDate,'MM'))) AND lkp_pol_issue_code='N',v_DistinctPolicyKeyCounter,NULL
	-- )
	IFF(( ( TRUNC(lkp_pol_eff_date, 'MM') <= TRUNC(v_RunDate, 'MM') AND TRUNC(lkp_pol_enter_date, 'MM') = TRUNC(v_RunDate, 'MM') ) OR ( TRUNC(lkp_pol_eff_date, 'MM') = TRUNC(v_RunDate, 'MM') AND TRUNC(lkp_pol_enter_date, 'MM') <= TRUNC(v_RunDate, 'MM') ) ) AND lkp_pol_issue_code = 'N', v_DistinctPolicyKeyCounter, NULL) AS o_NewPolicyCounterUsingBusinessLogic,
	-- *INF*: IIF((lkp_pol_issue_code='N') AND (lkp_PolicyStatusDescription='Cancelled' AND GET_DATE_PART(lkp_PolicyCancellationDate,'MM')=GET_DATE_PART(v_RunDate,'MM')),v_DistinctPolicyKeyCounter,NULL)
	IFF(( lkp_pol_issue_code = 'N' ) AND ( lkp_PolicyStatusDescription = 'Cancelled' AND GET_DATE_PART(lkp_PolicyCancellationDate, 'MM') = GET_DATE_PART(v_RunDate, 'MM') ), v_DistinctPolicyKeyCounter, NULL) AS o_NewOffsetCounterUsingBusinessLogic,
	-- *INF*: IIF(
	-- ((TRUNC(lkp_pol_eff_date,'MM')<=TRUNC(v_RunDate,'MM') AND TRUNC(lkp_pol_enter_date,'MM')=TRUNC(v_RunDate,'MM')) OR (TRUNC(lkp_pol_eff_date,'MM')=TRUNC(v_RunDate,'MM') AND TRUNC(lkp_pol_enter_date,'MM')<=TRUNC(v_RunDate,'MM'))) AND IN(lkp_pol_issue_code,'N' ,'R'),v_DistinctPolicyKeyCounter,NULL
	-- )
	IFF(( ( TRUNC(lkp_pol_eff_date, 'MM') <= TRUNC(v_RunDate, 'MM') AND TRUNC(lkp_pol_enter_date, 'MM') = TRUNC(v_RunDate, 'MM') ) OR ( TRUNC(lkp_pol_eff_date, 'MM') = TRUNC(v_RunDate, 'MM') AND TRUNC(lkp_pol_enter_date, 'MM') <= TRUNC(v_RunDate, 'MM') ) ) AND IN(lkp_pol_issue_code, 'N', 'R'), v_DistinctPolicyKeyCounter, NULL) AS o_IssuedPolicyCounter,
	-- *INF*: IIF(IN(lkp_pol_issue_code,'N','R') AND (lkp_PolicyStatusDescription='Cancelled' AND GET_DATE_PART(lkp_PolicyCancellationDate,'MM')=GET_DATE_PART(v_RunDate,'MM')),v_DistinctPolicyKeyCounter,NULL)
	IFF(IN(lkp_pol_issue_code, 'N', 'R') AND ( lkp_PolicyStatusDescription = 'Cancelled' AND GET_DATE_PART(lkp_PolicyCancellationDate, 'MM') = GET_DATE_PART(v_RunDate, 'MM') ), v_DistinctPolicyKeyCounter, NULL) AS o_IssuedOffsetCounterUsingBusinessLogic,
	-- *INF*: IIF(
	-- ((TRUNC(lkp_pol_eff_date,'MM')<=TRUNC(v_RunDate,'MM') AND TRUNC(lkp_pol_enter_date,'MM')=TRUNC(v_RunDate,'MM')) OR (TRUNC(lkp_pol_eff_date,'MM')=TRUNC(v_RunDate,'MM') AND TRUNC(lkp_pol_enter_date,'MM')<=TRUNC(v_RunDate,'MM'))) AND lkp_pol_issue_code='R' ,v_DistinctPolicyKeyCounter,NULL
	-- )
	IFF(( ( TRUNC(lkp_pol_eff_date, 'MM') <= TRUNC(v_RunDate, 'MM') AND TRUNC(lkp_pol_enter_date, 'MM') = TRUNC(v_RunDate, 'MM') ) OR ( TRUNC(lkp_pol_eff_date, 'MM') = TRUNC(v_RunDate, 'MM') AND TRUNC(lkp_pol_enter_date, 'MM') <= TRUNC(v_RunDate, 'MM') ) ) AND lkp_pol_issue_code = 'R', v_DistinctPolicyKeyCounter, NULL) AS o_RenewedPolicyCounter,
	-- *INF*: IIF(lkp_pol_issue_code='N',PremiumMasterDirectWrittenPremium,NULL)
	IFF(lkp_pol_issue_code = 'N', PremiumMasterDirectWrittenPremium, NULL) AS o_NewBusinessDirectWrittenPremiumUsingBusinesLogic,
	-- *INF*: IIF(lkp_pol_issue_code='N',PremiumMasterDirectWrittenPremium+PremiumMasterCollectionWriteOffPremium,NULL)
	IFF(lkp_pol_issue_code = 'N', PremiumMasterDirectWrittenPremium + PremiumMasterCollectionWriteOffPremium, NULL) AS o_NewBusinessProductionPremiumUsingBusinesLogic,
	-- *INF*: IIF(lkp_pol_issue_code='R',PremiumMasterDirectWrittenPremium,NULL)
	IFF(lkp_pol_issue_code = 'R', PremiumMasterDirectWrittenPremium, NULL) AS o_RenewedDirectWrittenPremium,
	-- *INF*: IIF(lkp_PolicyStatusDescription='Cancelled' AND GET_DATE_PART(lkp_PolicyCancellationDate,'MM')=GET_DATE_PART(v_RunDate,'MM'),PremiumMasterDirectWrittenPremium,NULL)
	IFF(lkp_PolicyStatusDescription = 'Cancelled' AND GET_DATE_PART(lkp_PolicyCancellationDate, 'MM') = GET_DATE_PART(v_RunDate, 'MM'), PremiumMasterDirectWrittenPremium, NULL) AS o_CancelledDirectWrittenPremium,
	-- *INF*: IIF(lkp_pol_exp_date>ADD_TO_DATE(v_RunDate,'MM',-1) AND lkp_pol_exp_date<=v_RunDate,PremiumMasterDirectWrittenPremium,NULL)
	IFF(lkp_pol_exp_date > ADD_TO_DATE(v_RunDate, 'MM', - 1) AND lkp_pol_exp_date <= v_RunDate, PremiumMasterDirectWrittenPremium, NULL) AS o_ExpiredDirectWrittenPremium
	FROM AGG_EP_PM
	LEFT JOIN LKP_InsuranceReferenceDim
	ON LKP_InsuranceReferenceDim.InsuranceReferenceDimId = AGG_EP_PM.InsuranceReferenceDimId
	LEFT JOIN LKP_PolicyCurrentStatusDim
	ON LKP_PolicyCurrentStatusDim.RunDate = LKP_calendar_dim.CalendarDate AND LKP_PolicyCurrentStatusDim.EDWPolicyAKId = LKP_Policy_Dim.edw_pol_ak_id
	LEFT JOIN LKP_Policy_Dim
	ON LKP_Policy_Dim.pol_dim_id = AGG_EP_PM.PolicyDimId
	LEFT JOIN LKP_calendar_dim
	ON LKP_calendar_dim.clndr_id = AGG_EP_PM.SnapshotDateID
),
LKP_PremiumMonthlySummaryFact AS (
	SELECT
	PremiumMonthlySummaryFactID,
	SnapshotDateID,
	AgencyDimID,
	PolicyDimId,
	ContractCustomerDimID,
	InsuranceReferenceDimId,
	SalesDivisionDimID
	FROM (
		SELECT 
		PremiumMonthlySummaryFactID AS PremiumMonthlySummaryFactID,
		SnapshotDateID AS SnapshotDateID,
		AgencyDimID AS AgencyDimID,
		PolicyDimId AS PolicyDimId,
		ContractCustomerDimID AS ContractCustomerDimID,
		InsuranceReferenceDimId AS InsuranceReferenceDimId,
		SalesDivisionDimID AS SalesDivisionDimID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumMonthlySummaryFact PMSF
		INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim CLDRUN
		ON PMSF.SnapshotDateID=CLDRUN.clndr_id
		AND CLDRUN.clndr_date>=@{pipeline().parameters.RUNDATE}
		@{pipeline().parameters.WHERE_CLAUSE_LKP}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SnapshotDateID,AgencyDimID,PolicyDimId,ContractCustomerDimID,InsuranceReferenceDimId,SalesDivisionDimID ORDER BY PremiumMonthlySummaryFactID) = 1
),
RTR_INSERT_UPDATE AS (
	SELECT
	LKP_PremiumMonthlySummaryFact.PremiumMonthlySummaryFactID,
	EXP_Target.AuditID,
	EXP_Target.SnapshotDateID,
	EXP_Target.AgencyDimID,
	EXP_Target.PolicyDimId,
	EXP_Target.ContractCustomerDimID,
	EXP_Target.InsuranceReferenceDimId,
	EXP_Target.SalesDivisionDimID,
	EXP_Target.MonthlyChangeinDirectEarnedPremium,
	EXP_Target.MonthlyChangeinCededEarnedPremium,
	EXP_Target.MonthlyChangeInDirectUnearnedPremium,
	EXP_Target.MonthlyChangeInCededUnearnedPremium,
	EXP_Target.PremiumMasterAgencyCededWrittenCommission,
	EXP_Target.PremiumMasterAgencyDirectWrittenCommission,
	EXP_Target.PremiumMasterAuditPremium,
	EXP_Target.PremiumMasterCededWrittenPremium,
	EXP_Target.PremiumMasterCollectionWriteOffPremium,
	EXP_Target.PremiumMasterDirectWrittenPremium,
	EXP_Target.PremiumMasterReturnedPremium,
	EXP_Target.TransactionInforceFlag,
	EXP_Target.o_InforceDistinctPolicyCounter AS InforceDistinctPolicyCounter,
	EXP_Target.o_ExpiredPolicyOfferingCounter AS ExpiredPolicyOfferingCounter,
	EXP_Target.o_CancelledPolicyCounter AS CancelledPolicyCounter,
	EXP_Target.o_NewInforceDistinctPolicyCounter AS NewInforceDistinctPolicyCounter,
	EXP_Target.o_NewPolicyCounterUsingBusinessLogic AS NewPolicyCounterUsingBusinessLogic,
	EXP_Target.o_NewOffsetCounterUsingBusinessLogic AS NewOffsetCounterUsingBusinessLogic,
	EXP_Target.o_IssuedPolicyCounter AS IssuedPolicyCounter,
	EXP_Target.o_IssuedOffsetCounterUsingBusinessLogic AS IssuedOffsetCounterUsingBusinessLogic,
	EXP_Target.o_RenewedPolicyCounter AS RenewedPolicyCounter,
	EXP_Target.o_NewBusinessDirectWrittenPremiumUsingBusinesLogic AS NewBusinessDirectWrittenPremiumUsingBusinesLogic,
	EXP_Target.o_NewBusinessProductionPremiumUsingBusinesLogic AS NewBusinessProductionPremiumUsingBusinesLogic,
	EXP_Target.o_RenewedDirectWrittenPremium AS RenewedDirectWrittenPremium,
	EXP_Target.o_CancelledDirectWrittenPremium AS CancelledDirectWrittenPremium,
	EXP_Target.o_ExpiredDirectWrittenPremium AS ExpiredDirectWrittenPremium
	FROM EXP_Target
	LEFT JOIN LKP_PremiumMonthlySummaryFact
	ON LKP_PremiumMonthlySummaryFact.SnapshotDateID = EXP_Target.SnapshotDateID AND LKP_PremiumMonthlySummaryFact.AgencyDimID = EXP_Target.AgencyDimID AND LKP_PremiumMonthlySummaryFact.PolicyDimId = EXP_Target.PolicyDimId AND LKP_PremiumMonthlySummaryFact.ContractCustomerDimID = EXP_Target.ContractCustomerDimID AND LKP_PremiumMonthlySummaryFact.InsuranceReferenceDimId = EXP_Target.InsuranceReferenceDimId AND LKP_PremiumMonthlySummaryFact.SalesDivisionDimID = EXP_Target.SalesDivisionDimID
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE ISNULL(PremiumMonthlySummaryFactID)),
RTR_INSERT_UPDATE_DEFAULT1 AS (SELECT * FROM RTR_INSERT_UPDATE WHERE NOT ( (ISNULL(PremiumMonthlySummaryFactID)) )),
PremiumMonthlySummaryFact_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumMonthlySummaryFact
	(AuditId, SnapshotDateId, InsuranceReferenceDimId, PolicyDimId, ContractCustomerDimId, SalesDivisionDimId, AgencyDimId, TransactionInforceFlag, MonthlyChangeInDirectEarnedPremium, PremiumMasterDirectWrittenPremium, MonthlyChangeinCededEarnedPremium, MonthlyChangeInDirectUnearnedPremium, MonthlyChangeInCededUnearnedPremium, PremiumMasterAuditPremium, PremiumMasterCededWrittenPremium, PremiumMasterCollectionWriteOffPremium, InforceDistinctPolicyCounter, ExpiredPolicyOfferingCounter, CancelledPolicyCounter, NewInforceDistinctPolicyCounter, NewPolicyCounterUsingBusinessLogic, NewOffsetCounterUsingBusinessLogic, IssuedPolicyCounter, IssuedOffsetCounterUsingBusinessLogic, RenewedPolicyCounter, NewBusinessDirectWrittenPremiumUsingBusinesLogic, NewBusinessProductionPremiumUsingBusinesLogic, RenewedDirectWrittenPremium, CancelledDirectWrittenPremium, ExpiredDirectWrittenPremium)
	SELECT 
	AuditID AS AUDITID, 
	SnapshotDateID AS SNAPSHOTDATEID, 
	INSURANCEREFERENCEDIMID, 
	POLICYDIMID, 
	ContractCustomerDimID AS CONTRACTCUSTOMERDIMID, 
	SalesDivisionDimID AS SALESDIVISIONDIMID, 
	AgencyDimID AS AGENCYDIMID, 
	TRANSACTIONINFORCEFLAG, 
	MonthlyChangeinDirectEarnedPremium AS MONTHLYCHANGEINDIRECTEARNEDPREMIUM, 
	PREMIUMMASTERDIRECTWRITTENPREMIUM, 
	MONTHLYCHANGEINCEDEDEARNEDPREMIUM, 
	MONTHLYCHANGEINDIRECTUNEARNEDPREMIUM, 
	MONTHLYCHANGEINCEDEDUNEARNEDPREMIUM, 
	PREMIUMMASTERAUDITPREMIUM, 
	PREMIUMMASTERCEDEDWRITTENPREMIUM, 
	PREMIUMMASTERCOLLECTIONWRITEOFFPREMIUM, 
	INFORCEDISTINCTPOLICYCOUNTER, 
	EXPIREDPOLICYOFFERINGCOUNTER, 
	CANCELLEDPOLICYCOUNTER, 
	NEWINFORCEDISTINCTPOLICYCOUNTER, 
	NEWPOLICYCOUNTERUSINGBUSINESSLOGIC, 
	NEWOFFSETCOUNTERUSINGBUSINESSLOGIC, 
	ISSUEDPOLICYCOUNTER, 
	ISSUEDOFFSETCOUNTERUSINGBUSINESSLOGIC, 
	RENEWEDPOLICYCOUNTER, 
	NEWBUSINESSDIRECTWRITTENPREMIUMUSINGBUSINESLOGIC, 
	NEWBUSINESSPRODUCTIONPREMIUMUSINGBUSINESLOGIC, 
	RENEWEDDIRECTWRITTENPREMIUM, 
	CANCELLEDDIRECTWRITTENPREMIUM, 
	EXPIREDDIRECTWRITTENPREMIUM
	FROM RTR_INSERT_UPDATE_INSERT
),
UPD_Target AS (
	SELECT
	PremiumMonthlySummaryFactID, 
	AuditID, 
	MonthlyChangeinDirectEarnedPremium, 
	MonthlyChangeinCededEarnedPremium, 
	MonthlyChangeInDirectUnearnedPremium, 
	MonthlyChangeInCededUnearnedPremium, 
	PremiumMasterAgencyCededWrittenCommission, 
	PremiumMasterAgencyDirectWrittenCommission, 
	PremiumMasterAuditPremium, 
	PremiumMasterCededWrittenPremium, 
	PremiumMasterCollectionWriteOffPremium, 
	PremiumMasterDirectWrittenPremium, 
	PremiumMasterReturnedPremium, 
	TransactionInforceFlag, 
	InforceDistinctPolicyCounter, 
	ExpiredPolicyOfferingCounter, 
	CancelledPolicyCounter, 
	NewInforceDistinctPolicyCounter, 
	NewPolicyCounterUsingBusinessLogic, 
	NewOffsetCounterUsingBusinessLogic, 
	IssuedPolicyCounter, 
	IssuedOffsetCounterUsingBusinessLogic, 
	RenewedPolicyCounter, 
	NewBusinessDirectWrittenPremiumUsingBusinesLogic, 
	NewBusinessProductionPremiumUsingBusinesLogic, 
	RenewedDirectWrittenPremium, 
	CancelledDirectWrittenPremium, 
	ExpiredDirectWrittenPremium
	FROM RTR_INSERT_UPDATE_DEFAULT1
),
PremiumMonthlySummaryFact_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumMonthlySummaryFact AS T
	USING UPD_Target AS S
	ON T.PremiumMonthlySummaryFactId = S.PremiumMonthlySummaryFactID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.TransactionInforceFlag = S.TransactionInforceFlag, T.MonthlyChangeInDirectEarnedPremium = S.MonthlyChangeinDirectEarnedPremium, T.PremiumMasterDirectWrittenPremium = S.PremiumMasterDirectWrittenPremium, T.MonthlyChangeinCededEarnedPremium = S.MonthlyChangeinCededEarnedPremium, T.MonthlyChangeInDirectUnearnedPremium = S.MonthlyChangeInDirectUnearnedPremium, T.MonthlyChangeInCededUnearnedPremium = S.MonthlyChangeInCededUnearnedPremium, T.PremiumMasterAuditPremium = S.PremiumMasterAuditPremium, T.PremiumMasterCededWrittenPremium = S.PremiumMasterCededWrittenPremium, T.PremiumMasterCollectionWriteOffPremium = S.PremiumMasterCollectionWriteOffPremium, T.InforceDistinctPolicyCounter = S.InforceDistinctPolicyCounter, T.ExpiredPolicyOfferingCounter = S.ExpiredPolicyOfferingCounter, T.CancelledPolicyCounter = S.CancelledPolicyCounter, T.NewInforceDistinctPolicyCounter = S.NewInforceDistinctPolicyCounter, T.NewPolicyCounterUsingBusinessLogic = S.NewPolicyCounterUsingBusinessLogic, T.NewOffsetCounterUsingBusinessLogic = S.NewOffsetCounterUsingBusinessLogic, T.IssuedPolicyCounter = S.IssuedPolicyCounter, T.IssuedOffsetCounterUsingBusinessLogic = S.IssuedOffsetCounterUsingBusinessLogic, T.RenewedPolicyCounter = S.RenewedPolicyCounter, T.NewBusinessDirectWrittenPremiumUsingBusinesLogic = S.NewBusinessDirectWrittenPremiumUsingBusinesLogic, T.NewBusinessProductionPremiumUsingBusinesLogic = S.NewBusinessProductionPremiumUsingBusinesLogic, T.RenewedDirectWrittenPremium = S.RenewedDirectWrittenPremium, T.CancelledDirectWrittenPremium = S.CancelledDirectWrittenPremium, T.ExpiredDirectWrittenPremium = S.ExpiredDirectWrittenPremium
),