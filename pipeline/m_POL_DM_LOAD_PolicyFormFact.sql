WITH
LKP_Calender_Dim AS (
	SELECT
	clndr_id,
	clndr_date
	FROM (
		SELECT 
			clndr_id,
			clndr_date
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_date ORDER BY clndr_id) = 1
),
SQ_PolicyForm AS (
	SELECT
		PolicyFormId,
		AuditId AS AuditID,
		SourceSystemID,
		CreatedDate,
		ModifiedDate,
		PolicyAKID,
		FormID,
		FormTransactionCreatedDate,
		FormTransactionEffectiveDate,
		FormTransactionType,
		FormAddRemoveFlag,
		FormAddedDate
	FROM PolicyForm
	WHERE ModifiedDate >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_Std_Values AS (
	SELECT
	PolicyFormId,
	PolicyAKID,
	FormID,
	FormTransactionCreatedDate,
	FormTransactionEffectiveDate,
	-- *INF*: Trunc(FormTransactionCreatedDate)
	Trunc(FormTransactionCreatedDate) AS v_TransactionCreatedDate,
	-- *INF*: TRUNC(FormTransactionEffectiveDate)
	TRUNC(FormTransactionEffectiveDate) AS v_TransactionEffectiveDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(v_TransactionCreatedDate)
	LKP_CALENDER_DIM_v_TransactionCreatedDate.clndr_id AS o_TransactionCreatedDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(v_TransactionEffectiveDate)
	LKP_CALENDER_DIM_v_TransactionEffectiveDate.clndr_id AS o_TransactionEffectiveDate,
	FormAddRemoveFlag,
	FormAddedDate,
	-- *INF*: Trunc(FormAddedDate)
	Trunc(FormAddedDate) AS v_FormAddedDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(v_FormAddedDate)
	LKP_CALENDER_DIM_v_FormAddedDate.clndr_id AS o_FormAddedDate
	FROM SQ_PolicyForm
	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_v_TransactionCreatedDate
	ON LKP_CALENDER_DIM_v_TransactionCreatedDate.clndr_date = v_TransactionCreatedDate

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_v_TransactionEffectiveDate
	ON LKP_CALENDER_DIM_v_TransactionEffectiveDate.clndr_date = v_TransactionEffectiveDate

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_v_FormAddedDate
	ON LKP_CALENDER_DIM_v_FormAddedDate.clndr_date = v_FormAddedDate

),
LKP_V2_Policy AS (
	SELECT
	contract_cust_ak_id,
	AgencyAKId,
	StrategicProfitCenterCode,
	InsuranceSegmentCode,
	PolicyOfferingCode,
	FormTransactionCreatedDate,
	pol_ak_id
	FROM (
		SELECT p.contract_cust_ak_id as contract_cust_ak_id, p.AgencyAKId as AgencyAKId, spc.StrategicProfitCenterCode as StrategicProfitCenterCode, ins.InsuranceSegmentCode as InsuranceSegmentCode, po.PolicyOfferingCode as PolicyOfferingCode, p.pol_ak_id as pol_ak_id 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy p
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter spc on spc.StrategicProfitCenterAKId = p.StrategicProfitCenterAKId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ins on ins.InsuranceSegmentAKId = p.InsuranceSegmentAKId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering po on po.PolicyOfferingAKId = p.PolicyOfferingAKId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id ORDER BY contract_cust_ak_id) = 1
),
LKP_AgencyDim AS (
	SELECT
	AgencyDimID,
	EDWAgencyAKID,
	EffectiveDate,
	ExpirationDate
	FROM (
		SELECT 
			AgencyDimID,
			EDWAgencyAKID,
			EffectiveDate,
			ExpirationDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V3}.AgencyDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyAKID,EffectiveDate,ExpirationDate ORDER BY AgencyDimID) = 1
),
LKP_FormDim AS (
	SELECT
	FormDimId,
	FormId
	FROM (
		select FormDim.FormDimID as FormDimID,
		Form.FormId as FormId
		from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.Form 
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyForm
		on PolicyForm.FormID=Form.FormID
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.FormDim
		on Form.FormName=FormDim.FormName
		and Form.FormNumber=FormDim.FormNumber
		and Form.FormEditionDate=FormDim.FormEditionDate
		and Form.FormEffectiveDate=FormDim.FormEffectiveDate
		and Form.FormExpirationDate=FormDim.FormExpirationDate
		where PolicyForm.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY FormId ORDER BY FormDimId) = 1
),
LKP_InsuranceReferenceDimId AS (
	SELECT
	InsuranceReferenceDimId,
	StrategicProfitCenterCode,
	InsuranceSegmentCode,
	PolicyOfferingCode
	FROM (
		SELECT InsuranceReferenceDim.InsuranceReferenceDimId as InsuranceReferenceDimId, 
		InsuranceReferenceDim.StrategicProfitCenterCode as StrategicProfitCenterCode,
		InsuranceReferenceDim.InsuranceSegmentCode as InsuranceSegmentCode,
		InsuranceReferenceDim.PolicyOfferingCode as PolicyOfferingCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceDim
		WHERE InsuranceReferenceDim.ProductCode = '000'
		and InsuranceReferenceDim.InsuranceReferenceLineOfBusinessCode = '000'
		and InsuranceReferenceDim.AccountingProductCode = 'N/A'
		and InsuranceReferenceDim.EnterpriseGroupCode = '1'
		and InsuranceReferenceDim.InsuranceReferenceLegalEntityCode = '1'
		and InsuranceReferenceDim.RatingPlanCode = 'N/A'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterCode,InsuranceSegmentCode,PolicyOfferingCode ORDER BY InsuranceReferenceDimId) = 1
),
LKP_contract_customer_dim AS (
	SELECT
	contract_cust_dim_id,
	edw_contract_cust_ak_id,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
			contract_cust_dim_id,
			edw_contract_cust_ak_id,
			eff_from_date,
			eff_to_date
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_contract_cust_ak_id,eff_from_date,eff_to_date ORDER BY contract_cust_dim_id) = 1
),
LKP_policy_dim AS (
	SELECT
	pol_dim_id,
	edw_pol_ak_id,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
			pol_dim_id,
			edw_pol_ak_id,
			eff_from_date,
			eff_to_date
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_ak_id,eff_from_date,eff_to_date ORDER BY pol_dim_id) = 1
),
EXP_Get_Values AS (
	SELECT
	EXP_Std_Values.PolicyFormId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_ID,
	LKP_FormDim.FormDimId AS FormDimID,
	-- *INF*: DECODE(TRUE, IsNull(FormDimID),-1,FormDimID)
	DECODE(TRUE,
	FormDimID IS NULL, - 1,
	FormDimID) AS o_FormDimID,
	LKP_policy_dim.pol_dim_id AS PolicyDimID,
	-- *INF*: DECODE(TRUE, IsNull(PolicyDimID),-1,PolicyDimID)
	DECODE(TRUE,
	PolicyDimID IS NULL, - 1,
	PolicyDimID) AS o_PolicyDimID,
	LKP_contract_customer_dim.contract_cust_dim_id AS ContractCustomerDimID,
	-- *INF*: DECODE(TRUE, IsNull(ContractCustomerDimID),-1,ContractCustomerDimID)
	DECODE(TRUE,
	ContractCustomerDimID IS NULL, - 1,
	ContractCustomerDimID) AS o_ContractCustomerDimID,
	LKP_AgencyDim.AgencyDimID,
	-- *INF*: DECODE(TRUE, IsNull(AgencyDimID),-1,AgencyDimID)
	DECODE(TRUE,
	AgencyDimID IS NULL, - 1,
	AgencyDimID) AS o_AgencyDimID,
	LKP_InsuranceReferenceDimId.InsuranceReferenceDimId,
	-- *INF*: DECODE(TRUE, IsNull(InsuranceReferenceDimId),-1,InsuranceReferenceDimId)
	DECODE(TRUE,
	InsuranceReferenceDimId IS NULL, - 1,
	InsuranceReferenceDimId) AS o_InsuranceReferenceDimId,
	EXP_Std_Values.o_TransactionCreatedDate AS TransactionDateID,
	EXP_Std_Values.o_TransactionEffectiveDate AS TransactionEffectiveDateID,
	EXP_Std_Values.FormAddRemoveFlag,
	EXP_Std_Values.o_FormAddedDate AS FormAddedDate
	FROM EXP_Std_Values
	LEFT JOIN LKP_AgencyDim
	ON LKP_AgencyDim.EDWAgencyAKID = LKP_V2_Policy.AgencyAKId AND LKP_AgencyDim.EffectiveDate <= LKP_V2_Policy.FormTransactionCreatedDate AND LKP_AgencyDim.ExpirationDate >= LKP_V2_Policy.FormTransactionCreatedDate
	LEFT JOIN LKP_FormDim
	ON LKP_FormDim.FormId = EXP_Std_Values.FormID
	LEFT JOIN LKP_InsuranceReferenceDimId
	ON LKP_InsuranceReferenceDimId.StrategicProfitCenterCode = LKP_V2_Policy.StrategicProfitCenterCode AND LKP_InsuranceReferenceDimId.InsuranceSegmentCode = LKP_V2_Policy.InsuranceSegmentCode AND LKP_InsuranceReferenceDimId.PolicyOfferingCode = LKP_V2_Policy.PolicyOfferingCode
	LEFT JOIN LKP_contract_customer_dim
	ON LKP_contract_customer_dim.edw_contract_cust_ak_id = LKP_V2_Policy.contract_cust_ak_id AND LKP_contract_customer_dim.eff_from_date <= LKP_V2_Policy.FormTransactionCreatedDate AND LKP_contract_customer_dim.eff_to_date >= LKP_V2_Policy.FormTransactionCreatedDate
	LEFT JOIN LKP_policy_dim
	ON LKP_policy_dim.edw_pol_ak_id = EXP_Std_Values.PolicyAKID AND LKP_policy_dim.eff_from_date <= EXP_Std_Values.FormTransactionCreatedDate AND LKP_policy_dim.eff_to_date >= EXP_Std_Values.FormTransactionCreatedDate
),
LKP_PolicyFormFact AS (
	SELECT
	PolicyFormFactId,
	EDWPolicyFormPKId
	FROM (
		SELECT @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyFormFact.PolicyFormFactId as PolicyFormFactId, @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyFormFact.EDWPolicyFormPKId as EDWPolicyFormPKId FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyFormFact
		Where EXISTS(SELECT 1 FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyForm 
		WHERE EDWPolicyFormPKId = PolicyFormId and ModifiedDate >= '@{pipeline().parameters.SELECTION_START_TS}')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPolicyFormPKId ORDER BY PolicyFormFactId) = 1
),
FIL_Insert_PolicyFormFact AS (
	SELECT
	LKP_PolicyFormFact.PolicyFormFactId, 
	EXP_Get_Values.Audit_ID, 
	EXP_Get_Values.PolicyFormId, 
	EXP_Get_Values.o_FormDimID AS FormDimID, 
	EXP_Get_Values.o_PolicyDimID AS PolicyDimID, 
	EXP_Get_Values.o_ContractCustomerDimID AS ContractCustomerDimID, 
	EXP_Get_Values.o_AgencyDimID AS AgencyDimID, 
	EXP_Get_Values.o_InsuranceReferenceDimId AS InsuranceReferenceDimId, 
	EXP_Get_Values.TransactionDateID, 
	EXP_Get_Values.TransactionEffectiveDateID, 
	EXP_Get_Values.FormAddRemoveFlag, 
	EXP_Get_Values.FormAddedDate
	FROM EXP_Get_Values
	LEFT JOIN LKP_PolicyFormFact
	ON LKP_PolicyFormFact.EDWPolicyFormPKId = EXP_Get_Values.PolicyFormId
	WHERE ISNULL(PolicyFormFactId)
),
PolicyFormFact AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyFormFact
	(AuditId, EDWPolicyFormPKId, FormDimId, PolicyDimId, ContractCustomerDimId, AgencyDimId, InsuranceReferenceDimId, FormTransactionCreatedDateId, FormTransactionEffectiveDateId, FormAddRemoveFlag, FormAddedDateId)
	SELECT 
	Audit_ID AS AUDITID, 
	PolicyFormId AS EDWPOLICYFORMPKID, 
	FormDimID AS FORMDIMID, 
	PolicyDimID AS POLICYDIMID, 
	ContractCustomerDimID AS CONTRACTCUSTOMERDIMID, 
	AgencyDimID AS AGENCYDIMID, 
	INSURANCEREFERENCEDIMID, 
	TransactionDateID AS FORMTRANSACTIONCREATEDDATEID, 
	TransactionEffectiveDateID AS FORMTRANSACTIONEFFECTIVEDATEID, 
	FORMADDREMOVEFLAG, 
	FormAddedDate AS FORMADDEDDATEID
	FROM FIL_Insert_PolicyFormFact
),