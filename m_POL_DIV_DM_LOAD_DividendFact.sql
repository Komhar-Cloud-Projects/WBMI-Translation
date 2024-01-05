WITH
SQ_Dividend AS (
	SELECT
		Dividend.PolicyAKId,
		Dividend.DividendPayableAmount,
		Dividend.DividendTransactionEnteredDate,
		Dividend.DividendRunDate,
		Dividend.DividendPlan,
		Dividend.DividendType,
		Dividend.SupStateId,
		Dividend.DividendPaidAmount,
		StrategicProfitCenter.StrategicProfitCenterCode,
		policy.contract_cust_ak_id,
		policy.AgencyAKId,
		InsuranceSegment.InsuranceSegmentCode,
		PolicyAudit.PolicyAuditAKId,
		PolicyAudit.EffectiveDate,
		Dividend.SourceSystemId
	FROM StrategicProfitCenter
	INNER JOIN policy
	INNER JOIN Dividend
	INNER JOIN PolicyAudit
	INNER JOIN InsuranceSegment
	ON { @{pipeline().parameters.SOURCE_TABLE_OWNER}.Dividend
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy
	ON @{pipeline().parameters.SOURCE_TABLE_OWNER}.Dividend.PolicyAKId=@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy.pol_ak_id 
	and @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy.crrnt_snpsht_flag=1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter
	ON @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy.StrategicProfitCenterAKId=@{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter.StrategicProfitCenterAKId 
	and @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment
	ON @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy.InsuranceSegmentAKId=@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment.InsuranceSegmentAKId 
	and @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment.CurrentSnapshotFlag=1
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyAudit
	ON @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy.pol_ak_id=@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyAudit.PolicyAKId
	AND @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyAudit.InsuranceLine='WorkersCompensation'
	AND @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyAudit.CurrentSnapshotFlag=1
	 }
	WHERE Dividend.CurrentSnapshotFlag=1 AND
	Dividend.ModifiedDate >= '@{pipeline().parameters.SELECTION_START_TS}'
	@{pipeline().parameters.WHERE_CLAUSE}
),
LKP_PolicyAuditDim AS (
	SELECT
	PolicyAuditDimId,
	EDWPolicyAuditAKId,
	EffectiveDate
	FROM (
		SELECT 
			PolicyAuditDimId,
			EDWPolicyAuditAKId,
			EffectiveDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyAuditDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPolicyAuditAKId,EffectiveDate ORDER BY PolicyAuditDimId) = 1
),
EXP_GetValues AS (
	SELECT
	LKP_PolicyAuditDim.PolicyAuditDimId AS i_PolicyAuditDimId,
	SQ_Dividend.PolicyAKId,
	SQ_Dividend.DividendPayableAmount,
	SQ_Dividend.DividendTransactionEnteredDate,
	SQ_Dividend.DividendRunDate,
	SQ_Dividend.DividendPlan,
	SQ_Dividend.DividendType,
	SQ_Dividend.SupStateId,
	SQ_Dividend.DividendPaidAmount,
	SQ_Dividend.StrategicProfitCenterCode,
	SQ_Dividend.contract_cust_ak_id,
	SQ_Dividend.AgencyAKId,
	SQ_Dividend.InsuranceSegmentCode,
	-- *INF*: IIF(ISNULL(i_PolicyAuditDimId),-1,i_PolicyAuditDimId)
	IFF(i_PolicyAuditDimId IS NULL, - 1, i_PolicyAuditDimId) AS o_PolicyAuditDimId,
	SQ_Dividend.SourceSystemId
	FROM SQ_Dividend
	LEFT JOIN LKP_PolicyAuditDim
	ON LKP_PolicyAuditDim.EDWPolicyAuditAKId = SQ_Dividend.PolicyAuditAKId AND LKP_PolicyAuditDim.EffectiveDate = SQ_Dividend.EffectiveDate
),
LKP_DividendTypeDim AS (
	SELECT
	DividendTypeDimId,
	DividendPlan,
	DividendType
	FROM (
		SELECT 
			DividendTypeDimId,
			DividendPlan,
			DividendType
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.DividendTypeDim
		WHERE CurrentSnapshotFlag=1 and DividendCategory <> 'CFA'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY DividendPlan,DividendType ORDER BY DividendTypeDimId DESC) = 1
),
LKP_InsuranceReferenceDim AS (
	SELECT
	InsuranceReferenceDimId,
	StrategicProfitCenterCode,
	InsuranceSegmentCode
	FROM (
		SELECT 
			InsuranceReferenceDimId,
			StrategicProfitCenterCode,
			InsuranceSegmentCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceDim
		WHERE EnterpriseGroupCode = '1' and InsuranceReferenceCoverageTypeCode = 'N/A' and InsuranceReferenceLegalEntityCode = '1' and InsuranceReferenceLineOfBusinessCode = '100' and PolicyOfferingCode = '100' and ProductCode = '100'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterCode,InsuranceSegmentCode ORDER BY InsuranceReferenceDimId DESC) = 1
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
	i_SupStateId,
	sup_state_id
	FROM (
		SELECT 
			state_code,
			i_SupStateId,
			sup_state_id
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state
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
LKP_calendar_dim_DividendRunDate AS (
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
LKP_calendar_dim_DividendTransactionEnteredDate AS (
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
	LKP_InsuranceReferenceDim.InsuranceReferenceDimId AS i_InsuranceReferenceDimId,
	mplt_PolicyDimID_PremiumMaster.agency_dim_id AS i_AgencyDimID,
	LKP_SalesDivisionDim.SalesDivisionDimID AS i_SalesDivisionDimId,
	mplt_PolicyDimID_PremiumMaster.pol_dim_id AS i_pol_dim_id,
	mplt_PolicyDimID_PremiumMaster.contract_cust_dim_id AS i_contract_cust_dim_id,
	LKP_DividendTypeDim.DividendTypeDimId AS i_DividendTypeDimId,
	LKP_StateDim.StateDimId AS i_StateDimId,
	LKP_calendar_dim_DividendTransactionEnteredDate.clndr_id AS i_DividendTransactionEnteredDateId,
	LKP_calendar_dim_DividendRunDate.clndr_id AS i_DividendRunDateId,
	EXP_GetValues.DividendPayableAmount,
	EXP_GetValues.DividendPaidAmount,
	EXP_GetValues.o_PolicyAuditDimId AS PolicyAuditDimId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	-- *INF*: IIF(ISNULL(i_InsuranceReferenceDimId), -1, i_InsuranceReferenceDimId)
	IFF(i_InsuranceReferenceDimId IS NULL, - 1, i_InsuranceReferenceDimId) AS o_StrategicProfitCenterDimId,
	-- *INF*: IIF(ISNULL(i_AgencyDimID), -1, i_AgencyDimID)
	IFF(i_AgencyDimID IS NULL, - 1, i_AgencyDimID) AS o_AgencyDimId,
	-- *INF*: IIF(ISNULL(i_SalesDivisionDimId),-1,i_SalesDivisionDimId)
	IFF(i_SalesDivisionDimId IS NULL, - 1, i_SalesDivisionDimId) AS o_SalesDivisionDimId,
	-- *INF*: IIF(ISNULL(i_pol_dim_id), -1, i_pol_dim_id)
	IFF(i_pol_dim_id IS NULL, - 1, i_pol_dim_id) AS o_PolicyDimId,
	-- *INF*: IIF(ISNULL(i_contract_cust_dim_id), -1, i_contract_cust_dim_id)
	IFF(i_contract_cust_dim_id IS NULL, - 1, i_contract_cust_dim_id) AS o_ContractCustomerDimId,
	-- *INF*: IIF(ISNULL(i_DividendTypeDimId),-1,i_DividendTypeDimId)
	IFF(i_DividendTypeDimId IS NULL, - 1, i_DividendTypeDimId) AS o_DividendTypeDimId,
	-- *INF*: IIF(ISNULL(i_StateDimId),-1,i_StateDimId)
	IFF(i_StateDimId IS NULL, - 1, i_StateDimId) AS o_StateDimId,
	-- *INF*: IIF(ISNULL(i_DividendTransactionEnteredDateId),-1,i_DividendTransactionEnteredDateId)
	IFF(i_DividendTransactionEnteredDateId IS NULL, - 1, i_DividendTransactionEnteredDateId) AS o_DividendTransactionEnteredDateId,
	-- *INF*: IIF(ISNULL(i_DividendRunDateId),-1,i_DividendRunDateId)
	IFF(i_DividendRunDateId IS NULL, - 1, i_DividendRunDateId) AS o_DividendRunDateId,
	DividendPayableAmount AS o_DividendPayableAmount,
	EXP_GetValues.PolicyAKId,
	EXP_GetValues.SourceSystemId
	FROM EXP_GetValues
	 -- Manually join with mplt_PolicyDimID_PremiumMaster
	LEFT JOIN LKP_DividendTypeDim
	ON LKP_DividendTypeDim.DividendPlan = EXP_GetValues.DividendPlan AND LKP_DividendTypeDim.DividendType = EXP_GetValues.DividendType
	LEFT JOIN LKP_InsuranceReferenceDim
	ON LKP_InsuranceReferenceDim.StrategicProfitCenterCode = EXP_GetValues.StrategicProfitCenterCode AND LKP_InsuranceReferenceDim.InsuranceSegmentCode = EXP_GetValues.InsuranceSegmentCode
	LEFT JOIN LKP_SalesDivisionDim
	ON LKP_SalesDivisionDim.AgencyAKID = EXP_GetValues.AgencyAKId
	LEFT JOIN LKP_StateDim
	ON LKP_StateDim.StateAbbreviation = LKP_sup_state.state_code
	LEFT JOIN LKP_calendar_dim_DividendRunDate
	ON LKP_calendar_dim_DividendRunDate.clndr_date = EXP_GetValues.DividendRunDate
	LEFT JOIN LKP_calendar_dim_DividendTransactionEnteredDate
	ON LKP_calendar_dim_DividendTransactionEnteredDate.clndr_date = EXP_GetValues.DividendTransactionEnteredDate
),
LKP_DividendFact_DCT AS (
	SELECT
	DividendFactId,
	i_PolicyAKId,
	i_StateDimId,
	edw_pol_ak_id,
	StateDimId
	FROM (
		SELECT 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.DividendFact.DividendFactId as DividendFactId, @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim.edw_pol_ak_id as edw_pol_ak_id, @{pipeline().parameters.TARGET_TABLE_OWNER}.DividendFact.StateDimId as StateDimId 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.DividendFact
		JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim
		ON DividendFact.PolicyDimId = policy_dim.pol_dim_id
		JOIN
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.Dividend
		ON Dividend.PolicyAKID = policy_dim.edw_pol_ak_id
		AND Dividend.CurrentSnapshotFlag=1
		AND
		Dividend.ModifiedDate >= '@{pipeline().parameters.SELECTION_START_TS}'
		AND Dividend.SourceSystemId = 'DCT'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_ak_id,StateDimId ORDER BY DividendFactId DESC) = 1
),
LKP_DividendFact_PMS AS (
	SELECT
	DividendFactId,
	i_PolicyDimId,
	i_DividendTypeDimId,
	i_StateDimId,
	i_DividendTransactionEnteredDateId,
	i_DividendRunDateId,
	i_DividendPayableAmount,
	i_DividendPaidAmount,
	PolicyDimId,
	DividendTypeDimId,
	StateDimId,
	DividendTransactionEnteredDateId,
	DividendRunDateId,
	DividendPayableAmount,
	DividendPaidAmount
	FROM (
		SELECT 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.DividendFact.DividendFactId as DividendFactId, @{pipeline().parameters.TARGET_TABLE_OWNER}.DividendFact.PolicyDimId as PolicyDimId, @{pipeline().parameters.TARGET_TABLE_OWNER}.DividendFact.DividendTypeDimId as DividendTypeDimId, @{pipeline().parameters.TARGET_TABLE_OWNER}.DividendFact.StateDimId as StateDimId, @{pipeline().parameters.TARGET_TABLE_OWNER}.DividendFact.DividendTransactionEnteredDateId as DividendTransactionEnteredDateId, @{pipeline().parameters.TARGET_TABLE_OWNER}.DividendFact.DividendRunDateId as DividendRunDateId ,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.DividendFact.DividendPayableAmount as DividendPayableAmount,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.DividendFact.DividendPaidAmount as DividendPaidAmount
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.DividendFact
		JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim
		ON DividendFact.PolicyDimId = policy_dim.pol_dim_id
		JOIN
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.Dividend
		ON Dividend.PolicyAKID = policy_dim.edw_pol_ak_id
		AND Dividend.CurrentSnapshotFlag=1
		AND
		Dividend.ModifiedDate >= '@{pipeline().parameters.SELECTION_START_TS}'
		AND Dividend.SourceSystemId = 'PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyDimId,DividendTypeDimId,StateDimId,DividendTransactionEnteredDateId,DividendRunDateId,DividendPayableAmount,DividendPaidAmount ORDER BY DividendFactId DESC) = 1
),
RTR_Target AS (
	SELECT
	LKP_DividendFact_PMS.DividendFactId AS DividendFactId_PMS,
	EXP_GetDimIds.o_AuditId AS AuditId,
	EXP_GetDimIds.o_StrategicProfitCenterDimId AS StrategicProfitCenterDimId,
	EXP_GetDimIds.o_AgencyDimId AS AgencyDimId,
	EXP_GetDimIds.o_SalesDivisionDimId AS SalesDivisionDimId,
	EXP_GetDimIds.o_PolicyDimId AS PolicyDimId,
	EXP_GetDimIds.o_ContractCustomerDimId AS ContractCustomerDimId,
	EXP_GetDimIds.o_DividendTypeDimId AS DividendTypeDimId,
	EXP_GetDimIds.o_StateDimId AS StateDimId,
	EXP_GetDimIds.o_DividendTransactionEnteredDateId AS DividendTransactionEnteredDateId,
	EXP_GetDimIds.o_DividendRunDateId AS DividendRunDateId,
	EXP_GetDimIds.o_DividendPayableAmount AS DividendPayableAmount,
	EXP_GetDimIds.DividendPaidAmount,
	EXP_GetDimIds.PolicyAuditDimId,
	EXP_GetDimIds.SourceSystemId,
	LKP_DividendFact_DCT.DividendFactId AS DividendFactId_DCT
	FROM EXP_GetDimIds
	LEFT JOIN LKP_DividendFact_DCT
	ON LKP_DividendFact_DCT.edw_pol_ak_id = EXP_GetDimIds.PolicyAKId AND LKP_DividendFact_DCT.StateDimId = EXP_GetDimIds.o_StateDimId
	LEFT JOIN LKP_DividendFact_PMS
	ON LKP_DividendFact_PMS.PolicyDimId = EXP_GetDimIds.o_PolicyDimId AND LKP_DividendFact_PMS.DividendTypeDimId = EXP_GetDimIds.o_DividendTypeDimId AND LKP_DividendFact_PMS.StateDimId = EXP_GetDimIds.o_StateDimId AND LKP_DividendFact_PMS.DividendTransactionEnteredDateId = EXP_GetDimIds.o_DividendTransactionEnteredDateId AND LKP_DividendFact_PMS.DividendRunDateId = EXP_GetDimIds.o_DividendRunDateId AND LKP_DividendFact_PMS.DividendPayableAmount = EXP_GetDimIds.o_DividendPayableAmount AND LKP_DividendFact_PMS.DividendPaidAmount = EXP_GetDimIds.DividendPaidAmount
),
RTR_Target_NEW_PMS AS (SELECT * FROM RTR_Target WHERE SourceSystemId = 'PMS' AND ISNULL(DividendFactId_PMS)),
RTR_Target_UPDATE_PMS AS (SELECT * FROM RTR_Target WHERE SourceSystemId = 'PMS' AND (NOT ISNULL(DividendFactId_PMS))),
RTR_Target_NEW_DCT AS (SELECT * FROM RTR_Target WHERE SourceSystemId = 'DCT' AND ISNULL(DividendFactId_DCT)),
RTR_Target_UPDATE_DCT AS (SELECT * FROM RTR_Target WHERE SourceSystemId = 'DCT' AND (NOT ISNULL(DividendFactId_DCT))),
UPD_UPDATE_DCT AS (
	SELECT
	DividendFactId_DCT AS DividendFactId, 
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
	DividendPayableAmount, 
	DividendPaidAmount AS DividendPaidAmount3, 
	PolicyAuditDimId AS PolicyAuditDimId3
	FROM RTR_Target_UPDATE_DCT
),
DividendFact_UPDATE_DCT AS (
	MERGE INTO DividendFact AS T
	USING UPD_UPDATE_DCT AS S
	ON T.DividendFactId = S.DividendFactId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId, T.StrategicProfitCenterDimId = S.StrategicProfitCenterDimId, T.AgencyDimId = S.AgencyDimId, T.SalesDivisionDimId = S.SalesDivisionDimId, T.PolicyDimId = S.PolicyDimId, T.ContractCustomerDimId = S.ContractCustomerDimId, T.DividendTypeDimId = S.DividendTypeDimId, T.StateDimId = S.StateDimId, T.DividendTransactionEnteredDateId = S.DividendTransactionEnteredDateId, T.DividendRunDateId = S.DividendRunDateId, T.DividendPayableAmount = S.DividendPayableAmount, T.DividendPaidAmount = S.DividendPaidAmount3, T.PolicyAuditDimId = S.PolicyAuditDimId3
),
DividendFact_INSERT_DCT AS (
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
	POLICYAUDITDIMID
	FROM RTR_Target_NEW_DCT
),
DividendFact_INSERT_PMS AS (
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
	POLICYAUDITDIMID
	FROM RTR_Target_NEW_PMS
),
UPD_UPDATE_PMS AS (
	SELECT
	DividendFactId_PMS AS DividendFactId, 
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
	DividendPayableAmount, 
	DividendPaidAmount AS DividendPaidAmount3, 
	PolicyAuditDimId AS PolicyAuditDimId3
	FROM RTR_Target_UPDATE_PMS
),
DividendFact_UPDATE_PMS AS (
	MERGE INTO DividendFact AS T
	USING UPD_UPDATE_PMS AS S
	ON T.DividendFactId = S.DividendFactId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId, T.StrategicProfitCenterDimId = S.StrategicProfitCenterDimId, T.AgencyDimId = S.AgencyDimId, T.SalesDivisionDimId = S.SalesDivisionDimId, T.PolicyDimId = S.PolicyDimId, T.ContractCustomerDimId = S.ContractCustomerDimId, T.DividendTypeDimId = S.DividendTypeDimId, T.StateDimId = S.StateDimId, T.DividendTransactionEnteredDateId = S.DividendTransactionEnteredDateId, T.DividendRunDateId = S.DividendRunDateId, T.DividendPayableAmount = S.DividendPayableAmount, T.DividendPaidAmount = S.DividendPaidAmount3, T.PolicyAuditDimId = S.PolicyAuditDimId3
),