WITH
SQ_ClaimFreeAward AS (
	SELECT
		ClaimFreeAward.PolicyAKId,
		ClaimFreeAward.ClaimFreeAwardAmount,
		ClaimFreeAward.ClaimFreeAwardTransactionEnteredDate,
		ClaimFreeAward.ClaimFreeAwardRunDate,
		StrategicProfitCenter.StrategicProfitCenterCode,
		policy.contract_cust_ak_id,
		policy.sup_state_id,
		policy.AgencyAKId
	FROM ClaimFreeAward
	INNER JOIN StrategicProfitCenter
	INNER JOIN policy
	ON ClaimFreeAward.PolicyAKId=policy.pol_ak_id and policy.crrnt_snpsht_flag=1 and policy.StrategicProfitCenterAKId=StrategicProfitCenter.StrategicProfitCenterAKId and StrategicProfitCenter.CurrentSnapshotFlag=1
	WHERE ClaimFreeAward.CurrentSnapshotFlag=1 AND
	ClaimFreeAward.ClaimFreeAwardType in ('CHECK', 'LIQ-CHK', 'MANUAL-DISB') AND
	ClaimFreeAward.ModifiedDate >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_GetValues AS (
	SELECT
	PolicyAKId,
	ClaimFreeAwardAmount,
	ClaimFreeAwardTransactionEnteredDate,
	ClaimFreeAwardRunDate,
	StrategicProfitCenterCode,
	contract_cust_ak_id,
	sup_state_id,
	AgencyAKId,
	1 AS o_DividendTypeDimID_lkp_key
	FROM SQ_ClaimFreeAward
),
LKP_DividendTypeDim AS (
	SELECT
	DividendTypeDimId,
	DividendTypeDimId_lkp_key
	FROM (
		SELECT 
		DividendTypeDimId as DividendTypeDimId, 
		1 as DividendTypeDimId_lkp_key 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.DividendTypeDim
		WHERE DividendCategory = 'CFA' and CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY DividendTypeDimId_lkp_key ORDER BY DividendTypeDimId DESC) = 1
),
LKP_InsuranceReferenceDim AS (
	SELECT
	InsuranceReferenceDimId,
	StrategicProfitCenterCode
	FROM (
		SELECT 
			InsuranceReferenceDimId,
			StrategicProfitCenterCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceDim
		WHERE EnterpriseGroupCode = '1' and InsuranceReferenceCoverageTypeCode = 'N/A' and InsuranceReferenceLegalEntityCode = '1' and InsuranceReferenceLineOfBusinessCode = 'N/A' and InsuranceSegmentCode = 'N/A' and PolicyOfferingCode = 'N/A' and ProductCode = 'N/A'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterCode ORDER BY InsuranceReferenceDimId DESC) = 1
),
LKP_SalesDivisionDim AS (
	SELECT
	SalesDivisionDimID,
	AgencyAKID
	FROM (
		Select A.AgencyAKID AS AgencyAKID, 
		SDD.SalesDivisionDimID AS SalesDivisionDimID
		FROM 
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency A,
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RegionalSalesManager RSM,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.SalesDivisionDim SDD
		WHERE A.CurrentSnapshotFlag =1
		AND RSM.RegionalSalesManagerAKID = A.RegionalSalesManagerAKID
		AND RSM.CurrentSnapshotFlag = 1
		AND RSM.SalesDirectorAKID = SDD.EDWSalesDirectorAKID
		AND A.SalesTerritoryAKID = SDD.EDWSalesTerritoryAKID
		AND RSM.RegionalSalesManagerAKID = SDD.EDWRegionalSalesManagerAKID
		AND SDD.CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID ORDER BY SalesDivisionDimID DESC) = 1
),
LKP_sup_state AS (
	SELECT
	state_code,
	i_sup_state_id,
	sup_state_id
	FROM (
		SELECT 
			state_code,
			i_sup_state_id,
			sup_state_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_state
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_state_id ORDER BY state_code DESC) = 1
),
LKP_StateDim AS (
	SELECT
	StateDimId,
	StateAbbreviation
	FROM (
		SELECT @{pipeline().parameters.TARGET_TABLE_OWNER}.StateDim.StateDimId as StateDimId, LTRIM(RTRIM(@{pipeline().parameters.TARGET_TABLE_OWNER}.StateDim.StateAbbreviation)) as StateAbbreviation FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.StateDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StateAbbreviation ORDER BY StateDimId DESC) = 1
),
LKP_calendar_dim_ClaimFreeAwardRunDate AS (
	SELECT
	clndr_id,
	clndr_date
	FROM (
		SELECT 
			clndr_id,
			clndr_date
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_date ORDER BY clndr_id DESC) = 1
),
LKP_calendar_dim_TransactionEnteredDate AS (
	SELECT
	clndr_id,
	clndr_date
	FROM (
		SELECT 
			clndr_id,
			clndr_date
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_date ORDER BY clndr_id DESC) = 1
),
mplt_PolicyDimID_PremiumMaster AS (WITH
	Input AS (
		
	),
	EXP_Default AS (
		SELECT
		IN_PolicyAKID AS PolicyAKID,
		IN_Trans_Date
		FROM Input
	),
	LKP_V2_Policy AS (
		SELECT
		contract_cust_ak_id,
		agencyakid,
		pol_status_code,
		strtgc_bus_dvsn_ak_id,
		IN_Trans_Date,
		pol_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT policy.contract_cust_ak_id as contract_cust_ak_id, policy.agencyakid as agencyakid, policy.pol_status_code as pol_status_code, policy.strtgc_bus_dvsn_ak_id as strtgc_bus_dvsn_ak_id, policy.pol_ak_id as pol_ak_id, policy.eff_from_date as eff_from_date, policy.eff_to_date as eff_to_date FROM 
			V2.policy
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,eff_from_date,eff_to_date ORDER BY contract_cust_ak_id DESC) = 1
	),
	LKP_PolicyDimID AS (
		SELECT
		pol_dim_id,
		pol_key,
		pol_eff_date,
		pol_exp_date,
		pms_pol_lob_code,
		ClassOfBusinessCode,
		IN_Trans_Date,
		edw_pol_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				pol_dim_id,
				pol_key,
				pol_eff_date,
				pol_exp_date,
				pms_pol_lob_code,
				ClassOfBusinessCode,
				IN_Trans_Date,
				edw_pol_ak_id,
				eff_from_date,
				eff_to_date
			FROM policy_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_ak_id,eff_from_date,eff_to_date ORDER BY pol_dim_id DESC) = 1
	),
	LKP_V3_AgencyDimID AS (
		SELECT
		agency_dim_id,
		edw_agency_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT AgencyDim.AgencyDimID as agency_dim_id, AgencyDim.EDWAgencyAKID as edw_agency_ak_id, AgencyDim.EffectiveDate as eff_from_date, AgencyDim.ExpirationDate as eff_to_date
			 FROM V3.AgencyDim as AgencyDim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_agency_ak_id,eff_from_date,eff_to_date ORDER BY agency_dim_id DESC) = 1
	),
	LKP_ContractCustomerDim AS (
		SELECT
		contract_cust_dim_id,
		IN_Trans_Date,
		edw_contract_cust_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				contract_cust_dim_id,
				IN_Trans_Date,
				edw_contract_cust_ak_id,
				eff_from_date,
				eff_to_date
			FROM contract_customer_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_contract_cust_ak_id,eff_from_date,eff_to_date ORDER BY contract_cust_dim_id DESC) = 1
	),
	lkp_StrategicBusinessDivisionDIM AS (
		SELECT
		strtgc_bus_dvsn_dim_id,
		edw_strtgc_bus_dvsn_ak_id
		FROM (
			SELECT strategic_business_division_dim.strtgc_bus_dvsn_dim_id as strtgc_bus_dvsn_dim_id, strategic_business_division_dim.edw_strtgc_bus_dvsn_ak_id as edw_strtgc_bus_dvsn_ak_id 
			FROM strategic_business_division_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_strtgc_bus_dvsn_ak_id ORDER BY strtgc_bus_dvsn_dim_id DESC) = 1
	),
	EXP_Values AS (
		SELECT
		LKP_V3_AgencyDimID.agency_dim_id,
		LKP_ContractCustomerDim.contract_cust_dim_id,
		LKP_PolicyDimID.pol_dim_id,
		LKP_V2_Policy.pol_status_code,
		LKP_PolicyDimID.pol_eff_date,
		LKP_PolicyDimID.pol_exp_date,
		lkp_StrategicBusinessDivisionDIM.strtgc_bus_dvsn_dim_id,
		LKP_PolicyDimID.pol_key,
		LKP_PolicyDimID.pms_pol_lob_code,
		LKP_PolicyDimID.ClassOfBusinessCode
		FROM 
		LEFT JOIN LKP_ContractCustomerDim
		ON LKP_ContractCustomerDim.edw_contract_cust_ak_id = LKP_V2_Policy.contract_cust_ak_id AND LKP_ContractCustomerDim.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_ContractCustomerDim.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN LKP_PolicyDimID
		ON LKP_PolicyDimID.edw_pol_ak_id = EXP_Default.PolicyAKID AND LKP_PolicyDimID.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_PolicyDimID.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN LKP_V2_Policy
		ON LKP_V2_Policy.pol_ak_id = EXP_Default.PolicyAKID AND LKP_V2_Policy.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_V2_Policy.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN LKP_V3_AgencyDimID
		ON LKP_V3_AgencyDimID.edw_agency_ak_id = LKP_V2_Policy.agencyakid AND LKP_V3_AgencyDimID.eff_from_date <= EXP_Default.IN_Trans_Date AND LKP_V3_AgencyDimID.eff_to_date >= EXP_Default.IN_Trans_Date
		LEFT JOIN lkp_StrategicBusinessDivisionDIM
		ON lkp_StrategicBusinessDivisionDIM.edw_strtgc_bus_dvsn_ak_id = LKP_V2_Policy.strtgc_bus_dvsn_ak_id
	),
	Output AS (
		SELECT
		agency_dim_id, 
		contract_cust_dim_id, 
		pol_dim_id, 
		pol_status_code, 
		pol_eff_date, 
		pol_exp_date, 
		strtgc_bus_dvsn_dim_id, 
		pol_key, 
		pms_pol_lob_code, 
		ClassOfBusinessCode
		FROM EXP_Values
	),
),
EXP_GetDimIds AS (
	SELECT
	LKP_calendar_dim_TransactionEnteredDate.clndr_id AS lkp_DividendTransactionEnteredDateId,
	LKP_calendar_dim_ClaimFreeAwardRunDate.clndr_id AS lkp_ClaimFreeAwardRunDateID,
	LKP_InsuranceReferenceDim.InsuranceReferenceDimId AS lkp_InsuranceReferenceDimId,
	mplt_PolicyDimID_PremiumMaster.agency_dim_id AS lkp_AgencyDimID,
	LKP_SalesDivisionDim.SalesDivisionDimID AS lkp_SalesDivisionDimID,
	mplt_PolicyDimID_PremiumMaster.pol_dim_id AS lkp_pol_dim_id,
	mplt_PolicyDimID_PremiumMaster.contract_cust_dim_id AS lkp_contract_cust_dim_id,
	LKP_DividendTypeDim.DividendTypeDimId AS lkp_DividendTypeDimId,
	LKP_StateDim.StateDimId AS lkp_StateDimId,
	EXP_GetValues.ClaimFreeAwardAmount,
	-- *INF*: IIF(ISNULL(lkp_InsuranceReferenceDimId),-1,lkp_InsuranceReferenceDimId)
	IFF(lkp_InsuranceReferenceDimId IS NULL,
		- 1,
		lkp_InsuranceReferenceDimId
	) AS o_StrategicProfitCenterDimId,
	-- *INF*: IIF(ISNULL(lkp_AgencyDimID),-1,lkp_AgencyDimID)
	IFF(lkp_AgencyDimID IS NULL,
		- 1,
		lkp_AgencyDimID
	) AS o_AgencyDimId,
	-- *INF*: IIF(ISNULL(lkp_SalesDivisionDimID),-1,lkp_SalesDivisionDimID)
	IFF(lkp_SalesDivisionDimID IS NULL,
		- 1,
		lkp_SalesDivisionDimID
	) AS o_SalesDivisionDimId,
	-- *INF*: IIF(ISNULL(lkp_pol_dim_id),-1,lkp_pol_dim_id)
	IFF(lkp_pol_dim_id IS NULL,
		- 1,
		lkp_pol_dim_id
	) AS o_PolicyDimId,
	-- *INF*: IIF(ISNULL(lkp_contract_cust_dim_id),-1,lkp_contract_cust_dim_id)
	IFF(lkp_contract_cust_dim_id IS NULL,
		- 1,
		lkp_contract_cust_dim_id
	) AS o_ContractCustomerDimId,
	-- *INF*: IIF(ISNULL(lkp_DividendTypeDimId),-1,lkp_DividendTypeDimId)
	IFF(lkp_DividendTypeDimId IS NULL,
		- 1,
		lkp_DividendTypeDimId
	) AS o_DividendTypeDimId,
	-- *INF*: IIF(ISNULL(lkp_StateDimId),-1,lkp_StateDimId)
	IFF(lkp_StateDimId IS NULL,
		- 1,
		lkp_StateDimId
	) AS o_StateDimId,
	-- *INF*: IIF(ISNULL(lkp_DividendTransactionEnteredDateId),-1,lkp_DividendTransactionEnteredDateId)
	IFF(lkp_DividendTransactionEnteredDateId IS NULL,
		- 1,
		lkp_DividendTransactionEnteredDateId
	) AS o_DividendTransactionEnteredDateId,
	-- *INF*: IIF(ISNULL(lkp_ClaimFreeAwardRunDateID),-1,lkp_ClaimFreeAwardRunDateID)
	IFF(lkp_ClaimFreeAwardRunDateID IS NULL,
		- 1,
		lkp_ClaimFreeAwardRunDateID
	) AS o_ClaimFreeAwardRunDateID
	FROM EXP_GetValues
	 -- Manually join with mplt_PolicyDimID_PremiumMaster
	LEFT JOIN LKP_DividendTypeDim
	ON LKP_DividendTypeDim.DividendTypeDimId_lkp_key = EXP_GetValues.o_DividendTypeDimID_lkp_key
	LEFT JOIN LKP_InsuranceReferenceDim
	ON LKP_InsuranceReferenceDim.StrategicProfitCenterCode = EXP_GetValues.StrategicProfitCenterCode
	LEFT JOIN LKP_SalesDivisionDim
	ON LKP_SalesDivisionDim.AgencyAKID = EXP_GetValues.AgencyAKId
	LEFT JOIN LKP_StateDim
	ON LKP_StateDim.StateAbbreviation = LKP_sup_state.state_code
	LEFT JOIN LKP_calendar_dim_ClaimFreeAwardRunDate
	ON LKP_calendar_dim_ClaimFreeAwardRunDate.clndr_date = EXP_GetValues.ClaimFreeAwardRunDate
	LEFT JOIN LKP_calendar_dim_TransactionEnteredDate
	ON LKP_calendar_dim_TransactionEnteredDate.clndr_date = EXP_GetValues.ClaimFreeAwardTransactionEnteredDate
),
AGG_SUM AS (
	SELECT
	o_StrategicProfitCenterDimId AS StrategicProfitCenterDimId,
	o_AgencyDimId AS AgencyDimId,
	o_SalesDivisionDimId AS SalesDivisionDimId,
	o_PolicyDimId AS PolicyDimId,
	o_ContractCustomerDimId AS ContractCustomerDimId,
	o_DividendTypeDimId AS DividendTypeDimId,
	o_StateDimId AS StateDimId,
	o_DividendTransactionEnteredDateId AS DividendTransactionEnteredDateId,
	o_ClaimFreeAwardRunDateID AS ClaimFreeAwardRunDateID,
	ClaimFreeAwardAmount,
	-- *INF*: ROUND(SUM(ClaimFreeAwardAmount),2)
	ROUND(SUM(ClaimFreeAwardAmount
		), 2
	) AS SUM_ClaimFreeAwardAmount
	FROM EXP_GetDimIds
	GROUP BY PolicyDimId, DividendTypeDimId, StateDimId, DividendTransactionEnteredDateId, ClaimFreeAwardRunDateID
),
EXP_Default AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	StrategicProfitCenterDimId,
	AgencyDimId,
	SalesDivisionDimId,
	PolicyDimId,
	ContractCustomerDimId,
	DividendTypeDimId,
	StateDimId,
	DividendTransactionEnteredDateId,
	ClaimFreeAwardRunDateID AS DividendRunDateId,
	0.0 AS DividendPayableAmount,
	SUM_ClaimFreeAwardAmount AS DividendPaidAmount,
	-1 AS PolicyAuditDimID
	FROM AGG_SUM
),
LKP_DividendFact AS (
	SELECT
	DividendFactId,
	PolicyDimId,
	DividendTypeDimId,
	StateDimId,
	DividendTransactionEnteredDateId,
	DividendRunDateId
	FROM (
		SELECT 
			DividendFactId,
			PolicyDimId,
			DividendTypeDimId,
			StateDimId,
			DividendTransactionEnteredDateId,
			DividendRunDateId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.DividendFact
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyDimId,DividendTypeDimId,StateDimId,DividendTransactionEnteredDateId,DividendRunDateId ORDER BY DividendFactId DESC) = 1
),
RTR_Target AS (
	SELECT
	LKP_DividendFact.DividendFactId,
	EXP_Default.o_AuditId AS AuditId,
	EXP_Default.StrategicProfitCenterDimId,
	EXP_Default.AgencyDimId,
	EXP_Default.SalesDivisionDimId,
	EXP_Default.PolicyDimId,
	EXP_Default.ContractCustomerDimId,
	EXP_Default.DividendTypeDimId,
	EXP_Default.StateDimId,
	EXP_Default.DividendTransactionEnteredDateId,
	EXP_Default.DividendRunDateId,
	EXP_Default.DividendPayableAmount,
	EXP_Default.DividendPaidAmount,
	EXP_Default.PolicyAuditDimID
	FROM EXP_Default
	LEFT JOIN LKP_DividendFact
	ON LKP_DividendFact.PolicyDimId = EXP_Default.PolicyDimId AND LKP_DividendFact.DividendTypeDimId = EXP_Default.DividendTypeDimId AND LKP_DividendFact.StateDimId = EXP_Default.StateDimId AND LKP_DividendFact.DividendTransactionEnteredDateId = EXP_Default.DividendTransactionEnteredDateId AND LKP_DividendFact.DividendRunDateId = EXP_Default.DividendRunDateId
),
RTR_Target_NEW AS (SELECT * FROM RTR_Target WHERE ISNULL(DividendFactId)),
RTR_Target_UPDATE AS (SELECT * FROM RTR_Target WHERE NOT ISNULL(DividendFactId)),
DividendFact_INSERT AS (
	INSERT INTO DividendFact
	(AuditId, StrategicProfitCenterDimId, AgencyDimId, SalesDivisionDimId, PolicyDimId, ContractCustomerDimId, DividendTypeDimId, StateDimId, DividendTransactionEnteredDateId, DividendRunDateId, DividendPayableAmount, DividendPaidAmount, PolicyAuditDimId)
	SELECT 
	AUDITID, 
	STRATEGICPROFITCENTERDIMID, 
	AGENCYDIMID, 
	SALESDIVISIONDIMID, 
	POLICYDIMID, 
	CONTRACTCUSTOMERDIMID, 
	DIVIDENDTYPEDIMID, 
	STATEDIMID, 
	DIVIDENDTRANSACTIONENTEREDDATEID, 
	DIVIDENDRUNDATEID, 
	DIVIDENDPAYABLEAMOUNT, 
	DIVIDENDPAIDAMOUNT, 
	PolicyAuditDimID AS POLICYAUDITDIMID
	FROM RTR_Target_NEW
),
UPD_UPDATE AS (
	SELECT
	DividendFactId, 
	AuditId, 
	StrategicProfitCenterDimId, 
	AgencyDimId, 
	SalesDivisionDimId, 
	PolicyDimId, 
	ContractCustomerDimId, 
	DividendTypeDimId, 
	StateDimId, 
	DividendTransactionEnteredDateId, 
	DividendRunDateId, 
	DividendPayableAmount AS DividendPayableAmount3, 
	DividendPaidAmount
	FROM RTR_Target_UPDATE
),
DividendFact_UPDATE AS (
	MERGE INTO DividendFact AS T
	USING UPD_UPDATE AS S
	ON T.DividendFactId = S.DividendFactId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId, T.StrategicProfitCenterDimId = S.StrategicProfitCenterDimId, T.AgencyDimId = S.AgencyDimId, T.SalesDivisionDimId = S.SalesDivisionDimId, T.PolicyDimId = S.PolicyDimId, T.ContractCustomerDimId = S.ContractCustomerDimId, T.DividendTypeDimId = S.DividendTypeDimId, T.StateDimId = S.StateDimId, T.DividendTransactionEnteredDateId = S.DividendTransactionEnteredDateId, T.DividendRunDateId = S.DividendRunDateId, T.DividendPayableAmount = S.DividendPayableAmount3, T.DividendPaidAmount = S.DividendPaidAmount
),