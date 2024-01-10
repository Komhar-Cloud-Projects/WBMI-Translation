WITH
LKP_pol_issue_code AS (
	SELECT
	pol_issue_code,
	pol_key
	FROM (
		select   pol_issue_code as pol_issue_code,
		pol_key  as pol_key
		from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Policy
		 where source_sys_id='DCT' and crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_issue_code) = 1
),
LKP_strategic_business_division AS (
	SELECT
	strtgc_bus_dvsn_ak_id,
	strtgc_bus_dvsn_code
	FROM (
		SELECT 
			strtgc_bus_dvsn_ak_id,
			strtgc_bus_dvsn_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.strategic_business_division
		WHERE crrnt_snpsht_flag = 1 AND source_sys_id = 'PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY strtgc_bus_dvsn_code ORDER BY strtgc_bus_dvsn_ak_id) = 1
),
LKP_policy_Contract_Customer AS (
	SELECT
	contract_cust_ak_id,
	Pol_key
	FROM (
		select B.contract_cust_ak_id AS contract_cust_ak_id,
		B.contract_key AS Pol_key 
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer B
		where B.source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and B.crrnt_snpsht_flag=1
		and exists ( select 1 from @{pipeline().parameters.STAGING_DATABASE}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLParty WCT where WCT.PolicyNumber+ISNULL(RIGHT('00'+CONVERT(varchar(3),PolicyVersion),2),'00')=B.contract_key)
		order by B.contract_key,B.created_date Desc
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Pol_key ORDER BY contract_cust_ak_id) = 1
),
SQ_WorkDCTPLPolicy AS (
	SELECT distinct P.PolicyNumber, 
	P.PolicyVersion, 
	P.PolicySymbol, 
	P.TransactionTypeKey, 
	P.TransactionTypeCode, 
	P.PolicyStatusKey, 
	P.PolicyStatusCode, 
	P.TransactionCreatedDate, 
	P.PolicyEffectiveDate, 
	P.PolicyExpirationDate, 
	P.AgencyCode, 
	P.InceptionDate, 
	P.BCCode, 
	P.PolicyCancellationInd, 
	P.PolicyCancellationDate, 
	P.PolicyCancellationReasonCode, 
	P.PolicyState, 
	P.ServiceCenterSupportCode, 
	P.PolicyTerm, 
	P.TerrorismRiskInd, 
	P.PriorPolicyNumber, 
	P.PolicyIssueCode, 
	P.PolicyAge, 
	P.IndustryRiskGradeCode, 
	P.BusinessSegmentCode, 
	P.Userid, 
	P.ClassOfBusiness, 
	P.SupBusinessClassCode, 
	P.AutomaticRenewalIndicator, 
	P.AssociationCode, 
	P.PolicyIssueCodeOverride, 
	P.PolicyOfferingCode,
	P.StrategicProfitCenterCode,
	P.AutomatedUnderwritingIndicator,
	P.CustomerServicingCd,
	P.ProducerCode,
	C.TransactionEffectiveDate
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy P
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLCoverage C
	on P.PolicyKey=C.PolicyKey
	and P.StartDate=C.StartDate
	and not exists(select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy P2 where P2.LineageId=P.LineageId and P2.PolicyStatusKey='ClaimFreeAward')
	@{pipeline().parameters.WHERE}
	order by P.PolicyNumber,P.PolicyVersion,P.TransactionCreatedDate
),
AGG_Remove_Duplicates AS (
	SELECT
	PolicyNumber,
	PolicyVersion,
	PolicySymbol,
	TransactionTypeKey,
	TransactionTypeCode,
	PolicyStatusKey,
	PolicyStatusCode,
	TransactionCreatedDate,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	AgencyCode,
	InceptionDate,
	BCCode,
	PolicyCancellationInd,
	PolicyCancellationDate,
	PolicyCancellationReasonCode,
	PolicyState,
	ServiceCenterSupportCode,
	PolicyTerm,
	TerrorismRiskInd,
	PriorPolicyNumber,
	PolicyIssueCode,
	PolicyAge,
	IndustryRiskGradeCode,
	BusinessSegmentCode,
	Userid,
	ClassOfBusiness,
	SupBusinessClassCode,
	AutomaticRenewalIndicator,
	AssociationCode,
	PolicyIssueCodeOverride,
	PolicyOfferingCode,
	StrategicProfitCenterCode,
	AutomatedUnderwritingIndicator,
	CustomerServicingCd,
	ProducerCode,
	TransactionEffectiveDate
	FROM SQ_WorkDCTPLPolicy
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber, PolicyVersion, TransactionCreatedDate ORDER BY NULL) = 1
),
EXP_Src_DataCollect AS (
	SELECT
	PolicyNumber,
	PolicyVersion,
	-- *INF*: IIF(ISNULL(ltrim(rtrim(PolicyVersion))) or Length(ltrim(rtrim(PolicyVersion)))=0 or IS_SPACES(PolicyVersion),'00',PolicyVersion)
	IFF(ltrim(rtrim(PolicyVersion)) IS NULL OR Length(ltrim(rtrim(PolicyVersion))) = 0 OR IS_SPACES(PolicyVersion), '00', PolicyVersion) AS v_PolicyVersion,
	v_PolicyVersion AS o_PolicyVersion,
	PolicySymbol,
	PolicyNumber || v_PolicyVersion AS v_PolicyKey,
	v_PolicyKey AS o_Policykey,
	TransactionTypeKey,
	TransactionTypeCode,
	PolicyStatusKey,
	PolicyStatusCode,
	TransactionCreatedDate,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	AgencyCode,
	InceptionDate,
	BCCode,
	PolicyCancellationInd,
	PolicyCancellationDate,
	PolicyCancellationReasonCode,
	PolicyState,
	ServiceCenterSupportCode,
	PolicyTerm,
	TerrorismRiskInd,
	PriorPolicyNumber,
	PolicyIssueCode,
	PolicyAge,
	IndustryRiskGradeCode,
	BusinessSegmentCode,
	Userid,
	ClassOfBusiness,
	SupBusinessClassCode,
	AutomaticRenewalIndicator,
	AssociationCode,
	PolicyIssueCodeOverride,
	PolicyOfferingCode,
	StrategicProfitCenterCode,
	'1' AS o_InsuranceSegmentCode,
	'' AS o_CustomerCare,
	'0' AS o_PolicyIssueCodeOverride,
	'N/A' AS o_SurchargeExemptCode,
	'N/A' AS o_Program,
	'0' AS o_IsRollover,
	'0' AS o_MailPolicyToInsured,
	AutomatedUnderwritingIndicator,
	CustomerServicingCd,
	ProducerCode,
	TransactionEffectiveDate
	FROM AGG_Remove_Duplicates
),
LKP_Agency AS (
	SELECT
	AgencyCode,
	AgencyAKID
	FROM (
		SELECT 
			AgencyCode,
			AgencyAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Agency
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode ORDER BY AgencyCode) = 1
),
LKP_AgencyEmployee_GetAgencyEmployeeAKID AS (
	SELECT
	AgencyEmployeeAKID,
	ProducerCode,
	UserID,
	in_AgencyAKID,
	AgencyEmployeeCode,
	AgencyAKID
	FROM (
		SELECT 
			AgencyEmployeeAKID,
			ProducerCode,
			UserID,
			in_AgencyAKID,
			AgencyEmployeeCode,
			AgencyAKID
		FROM AgencyEmployee
		WHERE CurrentSnapshotFlag=1 and UserID != 'N/A'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY UserID,AgencyEmployeeCode,AgencyAKID ORDER BY AgencyEmployeeAKID) = 1
),
LKP_Entered_Date AS (
	SELECT
	TransactionCreatedDate,
	PolicyNumber,
	PolicyVersion
	FROM (
		select 
		MIN(TransactionCreatedDate) as TransactionCreatedDate, 
		PolicyNumber as PolicyNumber, 
		PolicyVersion as PolicyVersion  
		FROM 
		WorkDCTPLPolicy with (nolock)
		group by PolicyNumber, PolicyVersion
		
		Union ALL
		
		select 
		MIN(TransactionCreatedDate) as TransactionCreatedDate, 
		PolicyNumber as PolicyNumber, 
		PolicyVersion as PolicyVersion  
		FROM 
		archWorkDCTPLPolicy with (nolock)
		group by PolicyNumber, PolicyVersion
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber,PolicyVersion ORDER BY TransactionCreatedDate) = 1
),
LKP_InsuranceSegment AS (
	SELECT
	InsuranceSegmentAKId,
	InsuranceSegmentCode
	FROM (
		SELECT 
			InsuranceSegmentAKId,
			InsuranceSegmentCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceSegment
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceSegmentCode ORDER BY InsuranceSegmentAKId) = 1
),
LKP_PolicyOffering AS (
	SELECT
	PolicyOfferingAKId,
	PolicyOfferingCode
	FROM (
		SELECT 
			PolicyOfferingAKId,
			PolicyOfferingCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyOffering
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyOfferingCode ORDER BY PolicyOfferingAKId) = 1
),
LKP_StrategicProfitCenterAKId AS (
	SELECT
	StrategicProfitCenterAKId,
	StrategicProfitCenterCode
	FROM (
		SELECT 
			StrategicProfitCenterAKId,
			StrategicProfitCenterCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.StrategicProfitCenter
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterCode ORDER BY StrategicProfitCenterAKId) = 1
),
LKP_agency_ak_id AS (
	SELECT
	agency_ak_id,
	agency_key
	FROM (
		SELECT 
			agency_ak_id,
			agency_key
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.agency
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY agency_key ORDER BY agency_ak_id) = 1
),
LKP_policy AS (
	SELECT
	pol_id,
	pol_ak_id,
	eff_from_date,
	contract_cust_ak_id,
	agency_ak_id,
	AgencyAKID,
	pol_key,
	mco,
	pol_co_num,
	pol_eff_date,
	pol_exp_date,
	orig_incptn_date,
	prim_bus_class_code,
	reins_code,
	pms_pol_lob_code,
	pol_co_line_code,
	pol_cancellation_ind,
	pol_cancellation_date,
	pol_cancellation_rsn_code,
	state_of_domicile_code,
	wbconnect_upload_code,
	serv_center_support_code,
	pol_term,
	terrorism_risk_ind,
	prior_pol_key,
	pol_status_code,
	pol_issue_code,
	pol_age,
	industry_risk_grade_code,
	uw_review_yr,
	mvr_request_code,
	renl_code,
	amend_num,
	anniversary_rerate_code,
	pol_audit_frqncy,
	final_audit_code,
	zip_ind,
	guarantee_ind,
	variation_code,
	county,
	non_smoker_disc_code,
	renl_disc,
	renl_safe_driver_disc_count,
	nonrenewal_flag_date,
	audit_complt_date,
	orig_acct_date,
	pol_enter_date,
	excess_claim_code,
	pol_status_on_pif,
	target_mrkt_code,
	pkg_code,
	pol_kind_code,
	bus_seg_code,
	pif_upload_audit_ind,
	err_flag_bal_txn,
	err_flag_bal_reins,
	producer_code_ak_id,
	prdcr_code,
	ClassOfBusiness,
	strtgc_bus_dvsn_ak_id,
	ErrorFlagBalancePremiumTransaction,
	RenewalPolicyNumber,
	RenewalPolicySymbol,
	RenewalPolicyMod,
	BillingType,
	sup_bus_class_code_id,
	sup_pol_term_id,
	sup_pol_status_code_id,
	sup_pol_issue_code_id,
	sup_pol_audit_frqncy_id,
	sup_industry_risk_grade_code_id,
	sup_state_id,
	PolicyOfferingAKId,
	producer_code_id,
	SurchargeExemptCode,
	SupSurchargeExemptID,
	StrategicProfitCenterAKId,
	InsuranceSegmentAKId,
	ProgramAKId,
	ObligeeName,
	AutomatedUnderwritingServicesIndicator,
	AutomaticRenewalIndicator,
	AssociationCode,
	RolloverPolicyIndicator,
	RolloverPriorCarrier,
	MailToInsuredFlag,
	AgencyEmployeeAKId,
	PolicyIssueCodeOverride,
	eff_to_date
	FROM (
		SELECT 
			pol_id,
			pol_ak_id,
			eff_from_date,
			contract_cust_ak_id,
			agency_ak_id,
			AgencyAKID,
			pol_key,
			mco,
			pol_co_num,
			pol_eff_date,
			pol_exp_date,
			orig_incptn_date,
			prim_bus_class_code,
			reins_code,
			pms_pol_lob_code,
			pol_co_line_code,
			pol_cancellation_ind,
			pol_cancellation_date,
			pol_cancellation_rsn_code,
			state_of_domicile_code,
			wbconnect_upload_code,
			serv_center_support_code,
			pol_term,
			terrorism_risk_ind,
			prior_pol_key,
			pol_status_code,
			pol_issue_code,
			pol_age,
			industry_risk_grade_code,
			uw_review_yr,
			mvr_request_code,
			renl_code,
			amend_num,
			anniversary_rerate_code,
			pol_audit_frqncy,
			final_audit_code,
			zip_ind,
			guarantee_ind,
			variation_code,
			county,
			non_smoker_disc_code,
			renl_disc,
			renl_safe_driver_disc_count,
			nonrenewal_flag_date,
			audit_complt_date,
			orig_acct_date,
			pol_enter_date,
			excess_claim_code,
			pol_status_on_pif,
			target_mrkt_code,
			pkg_code,
			pol_kind_code,
			bus_seg_code,
			pif_upload_audit_ind,
			err_flag_bal_txn,
			err_flag_bal_reins,
			producer_code_ak_id,
			prdcr_code,
			ClassOfBusiness,
			strtgc_bus_dvsn_ak_id,
			ErrorFlagBalancePremiumTransaction,
			RenewalPolicyNumber,
			RenewalPolicySymbol,
			RenewalPolicyMod,
			BillingType,
			sup_bus_class_code_id,
			sup_pol_term_id,
			sup_pol_status_code_id,
			sup_pol_issue_code_id,
			sup_pol_audit_frqncy_id,
			sup_industry_risk_grade_code_id,
			sup_state_id,
			PolicyOfferingAKId,
			producer_code_id,
			SurchargeExemptCode,
			SupSurchargeExemptID,
			StrategicProfitCenterAKId,
			InsuranceSegmentAKId,
			ProgramAKId,
			ObligeeName,
			AutomatedUnderwritingServicesIndicator,
			AutomaticRenewalIndicator,
			AssociationCode,
			RolloverPolicyIndicator,
			RolloverPriorCarrier,
			MailToInsuredFlag,
			AgencyEmployeeAKId,
			PolicyIssueCodeOverride,
			eff_to_date
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and exists ( select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy WCT where WCT.PolicyNumber=pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol_mod)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key,eff_from_date,eff_to_date ORDER BY pol_id) = 1
),
LKP_sup_reason_amended_code AS (
	SELECT
	StandardReasonAmendedCode,
	rsn_amended_code
	FROM (
		SELECT 
			StandardReasonAmendedCode,
			rsn_amended_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_reason_amended_code
		WHERE crrnt_snpsht_flag = 1 AND source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY rsn_amended_code ORDER BY StandardReasonAmendedCode) = 1
),
EXP_values AS (
	SELECT
	LKP_Agency.AgencyAKID AS in_AgencyAKID,
	LKP_agency_ak_id.agency_ak_id,
	LKP_PolicyOffering.PolicyOfferingAKId AS in_PolicyOfferingAKId,
	LKP_StrategicProfitCenterAKId.StrategicProfitCenterAKId AS in_StrategicProfitCenterAKId,
	LKP_InsuranceSegment.InsuranceSegmentAKId AS in_InsuranceSegmentAKId,
	EXP_Src_DataCollect.TransactionEffectiveDate AS in_TransactionEffectiveDate,
	EXP_Src_DataCollect.PolicyEffectiveDate AS in_EffectiveDate,
	EXP_Src_DataCollect.PolicyExpirationDate AS in_ExpirationDate,
	EXP_Src_DataCollect.PolicyTerm AS in_Term,
	EXP_Src_DataCollect.PolicyState AS in_PrimaryRatingState,
	EXP_Src_DataCollect.PolicyCancellationDate AS in_CancellationDate,
	LKP_Entered_Date.TransactionCreatedDate AS in_TransactionDate,
	EXP_Src_DataCollect.PriorPolicyNumber AS in_PreviousPolicyNumber,
	EXP_Src_DataCollect.InceptionDate AS in_InceptionDate,
	EXP_Src_DataCollect.TransactionTypeCode AS Type,
	EXP_Src_DataCollect.TerrorismRiskInd AS in_Terrorism,
	EXP_Src_DataCollect.o_Program AS in_Program,
	EXP_Src_DataCollect.IndustryRiskGradeCode AS in_RiskGrade,
	EXP_Src_DataCollect.BCCode AS in_BCCCode,
	EXP_Src_DataCollect.o_SurchargeExemptCode AS in_SurchargeExemptCode,
	EXP_Src_DataCollect.AgencyCode AS in_Reference,
	EXP_Src_DataCollect.PolicyStatusCode,
	EXP_Src_DataCollect.PolicySymbol AS in_PolicySymbol,
	EXP_Src_DataCollect.PolicyNumber AS in_PolicyNumber,
	EXP_Src_DataCollect.o_PolicyVersion AS in_PolicyVersion,
	EXP_Src_DataCollect.AutomaticRenewalIndicator AS in_AutomaticRenewalIndicator,
	EXP_Src_DataCollect.AssociationCode AS in_Association,
	EXP_Src_DataCollect.PolicyStatusCode AS in_Status,
	EXP_Src_DataCollect.TransactionCreatedDate AS in_CreatedDate,
	LKP_sup_reason_amended_code.StandardReasonAmendedCode AS in_cancellation_rsn_code,
	EXP_Src_DataCollect.PolicyIssueCode,
	EXP_Src_DataCollect.o_PolicyIssueCodeOverride AS in_PolicyIssueCodeOverride,
	in_CreatedDate AS out_CreatedDate,
	in_CreatedDate AS v_CurrentDate,
	in_PolicySymbol AS v_pol_sym,
	in_PolicyNumber AS v_PolicyNumber,
	in_PolicyVersion AS v_PolicyVersion,
	-- *INF*: rtrim(ltrim(v_PolicyNumber))||rtrim(ltrim(v_PolicyVersion))
	-- 
	-- --Per Rob M, this needs to be PolicyNumber and PolicyVersion only.  No GUIDs.  No Symbols. 01/24/2014
	-- --in_Id||v_PolicyVersion
	-- --v_cust_num||v_PolicyNumber||v_PolicyVersion
	rtrim(ltrim(v_PolicyNumber)) || rtrim(ltrim(v_PolicyVersion)) AS v_policy_key,
	-- *INF*: IIF(ISNULL(Type) OR IS_SPACES(Type) OR LENGTH(Type)=0,'N/A',LTRIM(RTRIM(Type)))
	IFF(Type IS NULL OR IS_SPACES(Type) OR LENGTH(Type) = 0, 'N/A', LTRIM(RTRIM(Type))) AS v_Type,
	-- *INF*: IIF(v_policy_key=v_prev_policy_key ,v_seq+1,1)
	IFF(v_policy_key = v_prev_policy_key, v_seq + 1, 1) AS v_seq,
	-- *INF*: rtrim(ltrim(in_Id))||rtrim(ltrim(v_PolicyVersion))
	-- 
	-- --Per Rob M, this needs to be PolicyNumber and PolicyVersion only.  No GUIDs.  No Symbols. 01/24/2014
	-- --in_Id||v_PolicyVersion
	-- --v_cust_num||v_PolicyNumber||v_PolicyVersion
	rtrim(ltrim(in_Id)) || rtrim(ltrim(v_PolicyVersion)) AS v_policy_id_key,
	-- *INF*: IIF(ISNULL(in_CancellationDate),
	-- TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),in_CancellationDate)
	IFF(in_CancellationDate IS NULL, TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), in_CancellationDate) AS v_CancellationDate,
	-- *INF*: IIF(v_Type<> 'Cancel',TO_DATE('21001231235959','YYYYMMDDHH24MISS'), LEAST(v_CancellationDate,in_TransactionEffectiveDate))
	-- 
	-- --IIF(in_Status<>'Cancelled',TO_DATE('21001231235959','YYYYMMDDHH24MISS'), LEAST(v_CancellationDate,in_TransactionEffectiveDate))
	IFF(v_Type <> 'Cancel', TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'), LEAST(v_CancellationDate, in_TransactionEffectiveDate)) AS v_pol_cancellation_date,
	-- *INF*: IIF( v_seq=1,
	-- 
	-- iif(NOT IN(v_Type,'Reinstate','Cancel') and NOT ISNULL(lkp_pol_ak_id)  
	-- 	AND to_char	(lkp_pol_cancellation_date,'YYYYMM')<>'210012',
	-- lkp_pol_cancellation_date,v_pol_cancellation_date),
	-- 
	-- 
	-- 
	-- IIF( v_seq<>1 and IN(v_Type,'Reinstate','Cancel'),v_pol_cancellation_date,
	-- v_pol_cancellation_date_persist)
	-- )
	--  
	-- 
	-- 
	-- --This expression is stores the cancellation date for the records between Cancel and Reinstate so that it assignes the same date for the records between cancel and reinstate.
	-- --For the first record is not reinstate and the target has cancel within the same date range then it treats the first record same as target(cancel)
	IFF(v_seq = 1, IFF(NOT IN(v_Type, 'Reinstate', 'Cancel') AND NOT lkp_pol_ak_id IS NULL AND to_char(lkp_pol_cancellation_date, 'YYYYMM') <> '210012', lkp_pol_cancellation_date, v_pol_cancellation_date), IFF(v_seq <> 1 AND IN(v_Type, 'Reinstate', 'Cancel'), v_pol_cancellation_date, v_pol_cancellation_date_persist)) AS v_pol_cancellation_date_persist,
	-- *INF*: IIF(v_pol_cancellation_date_persist<TO_DATE('21001231','YYYYMMDD'),  'Y','N' )
	-- --prod 11524 
	IFF(v_pol_cancellation_date_persist < TO_DATE('21001231', 'YYYYMMDD'), 'Y', 'N') AS v_pol_cancellation_ind,
	-- *INF*: in_PrimaryRatingState
	-- 
	-- --IIF(ISNULL(in_PrimaryRatingState) OR IS_SPACES(in_PrimaryRatingState) OR LENGTH(in_PrimaryRatingState)=0,'N/A',in_PrimaryRatingState)
	in_PrimaryRatingState AS v_PrimaryRatingState,
	-- *INF*: IIF(ISNULL(in_Term), 0, in_Term)
	IFF(in_Term IS NULL, 0, in_Term) AS v_Term,
	-- *INF*: IIF(v_Term<100, LPAD(TO_CHAR(v_Term), 3, '0'), TO_CHAR(v_Term))
	IFF(v_Term < 100, LPAD(TO_CHAR(v_Term), 3, '0'), TO_CHAR(v_Term)) AS v_pol_term,
	-- *INF*: IIF(ISNULL(in_PreviousPolicyNumber) OR IS_SPACES(in_PreviousPolicyNumber) OR LENGTH(in_PreviousPolicyNumber)=0,'N/A',in_PreviousPolicyNumber)
	IFF(in_PreviousPolicyNumber IS NULL OR IS_SPACES(in_PreviousPolicyNumber) OR LENGTH(in_PreviousPolicyNumber) = 0, 'N/A', in_PreviousPolicyNumber) AS v_PreviousPolicyNumber,
	-- *INF*: IIF(ISNULL(in_EffectiveDate),TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS') , in_EffectiveDate)
	IFF(in_EffectiveDate IS NULL, TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), in_EffectiveDate) AS v_EffectiveDate,
	-- *INF*: IIF(ISNULL(in_ExpirationDate),TO_DATE('2100-12-31 23:59:59 ','YYYY-MM-DD HH24:MI:SS') , in_ExpirationDate)
	IFF(in_ExpirationDate IS NULL, TO_DATE('2100-12-31 23:59:59 ', 'YYYY-MM-DD HH24:MI:SS'), in_ExpirationDate) AS v_ExpirationDate,
	-- *INF*: IIF(ISNULL(in_InceptionDate),TO_DATE('2100-12-31 23:59:59 ','YYYY-MM-DD HH24:MI:SS') , in_InceptionDate)
	IFF(in_InceptionDate IS NULL, TO_DATE('2100-12-31 23:59:59 ', 'YYYY-MM-DD HH24:MI:SS'), in_InceptionDate) AS v_InceptionDate,
	-- *INF*: Decode(TRUE,
	-- UPPER(PolicyStatusCode)='INFORCE','I',
	-- UPPER(PolicyStatusCode)='CANCELLED','C',
	-- in_CreatedDate>=v_ExpirationDate,'N' ,
	-- in_CreatedDate<v_EffectiveDate AND (v_pol_cancellation_date_persist>in_CreatedDate OR v_pol_cancellation_date_persist>v_EffectiveDate),'F',
	-- in_CreatedDate>v_ExpirationDate,'N',
	-- v_pol_cancellation_date<>TO_DATE('21001231235959','YYYYMMDDHH24MISS') or v_pol_cancellation_date_persist<>TO_DATE('21001231235959','YYYYMMDDHH24MISS'),'C',
	-- in_CreatedDate>=v_EffectiveDate,'I', 'N/A' 
	-- )
	-- 
	-- --IIF( v_EffectiveDate<=in_CreatedDate AND 
	-- --	      in_CreatedDate< (iif (v_ExpirationDate< v_pol_cancellation_date_persist, v_ExpirationDate,v_pol_cancellation_date_persist)),'I' ,
	-- --iif(v_pol_cancellation_date_persist<=in_CreatedDate OR (v_pol_cancellation_date_persist=v_EffectiveDate AND in_CreatedDate<=v_EffectiveDate),'C',
	-- --iif(in_CreatedDate>=v_ExpirationDate,'N' ,
	-- --iif(in_CreatedDate<v_EffectiveDate AND (v_pol_cancellation_date_persist>in_CreatedDate OR v_pol_cancellation_date_persist>v_EffectiveDate),'F', 'N/A' 
	-- --)))
	-- --)
	Decode(TRUE,
		UPPER(PolicyStatusCode) = 'INFORCE', 'I',
		UPPER(PolicyStatusCode) = 'CANCELLED', 'C',
		in_CreatedDate >= v_ExpirationDate, 'N',
		in_CreatedDate < v_EffectiveDate AND ( v_pol_cancellation_date_persist > in_CreatedDate OR v_pol_cancellation_date_persist > v_EffectiveDate ), 'F',
		in_CreatedDate > v_ExpirationDate, 'N',
		v_pol_cancellation_date <> TO_DATE('21001231235959', 'YYYYMMDDHH24MISS') OR v_pol_cancellation_date_persist <> TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'), 'C',
		in_CreatedDate >= v_EffectiveDate, 'I',
		'N/A') AS v_pol_status_code,
	-- *INF*: :LKP.LKP_pol_issue_code(v_policy_key)
	LKP_POL_ISSUE_CODE_v_policy_key.pol_issue_code AS v_lkp_pol_issue_code,
	-- *INF*: DECODE(TRUE, v_seq=1 and NOT ISNULL(v_lkp_pol_issue_code),v_lkp_pol_issue_code,
	--  -- this check needs to happen first to see if the Policykey is already in v2.Policy then just use the pol_issue_code of it.
	--    v_seq<>1,v_pol_issue_code,
	-- -- if the Policykey from the current record and the previous record from the run are same then use the pol_issue_code that has already been generated for the prior record,
	--   IN(Type,'New') and ISNULL(in_PreviousPolicyNumber),'N',
	--   IN(Type,'Rewrite','Reissue'),:LKP.LKP_pol_issue_code(Prior_Policy_key),
	--  --This rule is contingent on if the Prior policykey is deduced correctly. With the new fix for the work tables we are ensuring this. If there exists one in IDO then we populate it else it will be defaulted to R in out put port of issue code
	--   IN(Type,'Renew'),'R',
	--   ISNULL(in_PreviousPolicyNumber),'N'
	--  ) 
	-- --Note: This logic is obsolete after 5/1/2018. Get the Policy Issue code from v_pol_issue_code_new port
	DECODE(TRUE,
		v_seq = 1 AND NOT v_lkp_pol_issue_code IS NULL, v_lkp_pol_issue_code,
		v_seq <> 1, v_pol_issue_code,
		IN(Type, 'New') AND in_PreviousPolicyNumber IS NULL, 'N',
		IN(Type, 'Rewrite', 'Reissue'), LKP_POL_ISSUE_CODE_Prior_Policy_key.pol_issue_code,
		IN(Type, 'Renew'), 'R',
		in_PreviousPolicyNumber IS NULL, 'N') AS v_pol_issue_code_old,
	-- *INF*: IIF(ISNULL(PolicyIssueCode) OR IS_SPACES(PolicyIssueCode) OR LTRIM(RTRIM(PolicyIssueCode))='',v_lkp_pol_issue_code,
	-- IIF(LTRIM(RTRIM(PolicyIssueCode))='New', 'N','R'
	-- )
	-- )
	IFF(PolicyIssueCode IS NULL OR IS_SPACES(PolicyIssueCode) OR LTRIM(RTRIM(PolicyIssueCode)) = '', v_lkp_pol_issue_code, IFF(LTRIM(RTRIM(PolicyIssueCode)) = 'New', 'N', 'R')) AS v_pol_issue_code_new,
	-- *INF*: IIF(TO_DATE(TO_CHAR(v_EffectiveDate,'YYYY-MM-DD'),'YYYY-MM-DD') < TO_DATE('2018-05-01','YYYY-MM-DD') OR ISNULL(PolicyIssueCode) OR LTRIM(RTRIM(PolicyIssueCode))='',
	-- v_pol_issue_code_old,
	-- v_pol_issue_code_new
	-- )
	-- 
	-- --Note: This logic is obsolete after 5/1/2018. Get the Policy Issue code from v_pol_issue_code_new port
	IFF(TO_DATE(TO_CHAR(v_EffectiveDate, 'YYYY-MM-DD'), 'YYYY-MM-DD') < TO_DATE('2018-05-01', 'YYYY-MM-DD') OR PolicyIssueCode IS NULL OR LTRIM(RTRIM(PolicyIssueCode)) = '', v_pol_issue_code_old, v_pol_issue_code_new) AS v_pol_issue_code,
	-- *INF*: IIF(
	--   ISNULL(in_InceptionDate),
	--   -1,
	--   ABS(DATE_DIFF(TRUNC(in_InceptionDate,'YYYY'),TRUNC(v_CurrentDate,'YYYY'),'YYYY'))
	-- )
	IFF(in_InceptionDate IS NULL, - 1, ABS(DATE_DIFF(TRUNC(in_InceptionDate, 'YYYY'), TRUNC(v_CurrentDate, 'YYYY'), 'YYYY'))) AS v_pol_age,
	-- *INF*: DECODE(TRUE, in_Status = 'InForce' and Type = 'Renew', '3',
	-- IN(in_Status,'InForce', 'Quote', 'Application', 'Bound', 'Cancel-Pending') and v_CurrentDate>=v_EffectiveDate and v_CurrentDate<v_ExpirationDate, '1',
	-- IN(in_Status,'Cancelled', 'PolicyDeclined'), '9',
	-- in_Status='NonRenewed', '7',
	-- 'N/A')
	DECODE(TRUE,
		in_Status = 'InForce' AND Type = 'Renew', '3',
		IN(in_Status, 'InForce', 'Quote', 'Application', 'Bound', 'Cancel-Pending') AND v_CurrentDate >= v_EffectiveDate AND v_CurrentDate < v_ExpirationDate, '1',
		IN(in_Status, 'Cancelled', 'PolicyDeclined'), '9',
		in_Status = 'NonRenewed', '7',
		'N/A') AS v_renl_code,
	-- *INF*: --IIF(ISNULL(in_AuditPeriod) OR IS_SPACES(in_AuditPeriod) OR LENGTH(in_AuditPeriod)=0,'N/A',in_AuditPeriod)
	'' AS v_AuditPeriod,
	-- *INF*: IIF(ISNULL(in_County) OR IS_SPACES(in_County) OR LENGTH(in_County)=0,'N/A',in_County)
	IFF(in_County IS NULL OR IS_SPACES(in_County) OR LENGTH(in_County) = 0, 'N/A', in_County) AS v_County,
	v_pol_sym AS out_pol_sym,
	v_PolicyNumber AS out_PolicyNumber,
	v_PolicyVersion AS out_PolicyVersion,
	-1 AS out_producer_code_id,
	-- *INF*: rtrim(ltrim(v_PolicyNumber))||rtrim(ltrim(v_PolicyVersion))
	-- --LTRIM(RTRIM(v_policy_key))
	rtrim(ltrim(v_PolicyNumber)) || rtrim(ltrim(v_PolicyVersion)) AS out_policy_key,
	-- *INF*: LTRIM(RTRIM(v_policy_id_key))
	LTRIM(RTRIM(v_policy_id_key)) AS out_policy_id_key,
	'05' AS out_mco,
	'N/A' AS out_pol_co_num,
	v_EffectiveDate AS out_EffectiveDate,
	v_ExpirationDate AS out_ExpirationDate,
	v_InceptionDate AS out_InceptionDate,
	-- *INF*: IIF(ISNULL(in_BCCCode) OR IS_SPACES(in_BCCCode) OR LENGTH(in_BCCCode)=0,'N/A',in_BCCCode)
	IFF(in_BCCCode IS NULL OR IS_SPACES(in_BCCCode) OR LENGTH(in_BCCCode) = 0, 'N/A', in_BCCCode) AS out_prim_bus_class_code,
	'N/A' AS out_reins_code,
	'N/A' AS out_pms_pol_lob_code,
	'N/A' AS out_pol_co_line_code,
	EXP_Src_DataCollect.PolicyCancellationInd AS pol_cancellation_ind,
	in_CancellationDate AS out_pol_cancellation_date,
	-- *INF*: DECODE(TRUE, ISNULL(LTRIM(RTRIM(in_cancellation_rsn_code))),'N/A',
	-- LTRIM(RTRIM(in_cancellation_rsn_code))
	-- )
	DECODE(TRUE,
		LTRIM(RTRIM(in_cancellation_rsn_code)) IS NULL, 'N/A',
		LTRIM(RTRIM(in_cancellation_rsn_code))) AS out_pol_cancellation_rsn_code,
	v_PrimaryRatingState AS out_state_code,
	EXP_Src_DataCollect.o_CustomerCare AS CustomerCare,
	'N/A' AS out_wbconnect_upload_code,
	EXP_Src_DataCollect.CustomerServicingCd AS serv_center_support_code,
	v_pol_term AS out_pol_term,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(in_Terrorism) OR IS_SPACES(in_Terrorism) OR LENGTH(in_Terrorism)=0,'N/A',
	-- in_Terrorism='1','Y',
	-- in_Terrorism='0','N',
	-- 'N/A')
	DECODE(TRUE,
		in_Terrorism IS NULL OR IS_SPACES(in_Terrorism) OR LENGTH(in_Terrorism) = 0, 'N/A',
		in_Terrorism = '1', 'Y',
		in_Terrorism = '0', 'N',
		'N/A') AS out_terrorism_risk_ind,
	v_pol_status_code AS out_pol_status_code,
	-- *INF*: IIF(ISNULL(v_pol_issue_code),'R',v_pol_issue_code)
	IFF(v_pol_issue_code IS NULL, 'R', v_pol_issue_code) AS out_pol_issue_code,
	v_pol_age AS out_pol_age,
	-- *INF*: IIF(ISNULL(in_RiskGrade) OR IS_SPACES(in_RiskGrade) OR LENGTH(in_RiskGrade)=0,'N/A',in_RiskGrade)
	IFF(in_RiskGrade IS NULL OR IS_SPACES(in_RiskGrade) OR LENGTH(in_RiskGrade) = 0, 'N/A', in_RiskGrade) AS out_industry_risk_grade_code,
	'N/A' AS out_uw_review_yr,
	'N/A' AS out_mvr_request_code,
	v_renl_code AS out_renl_code,
	'N/A' AS out_amend_num,
	'N/A' AS out_anniversary_rerate_code,
	'No audit is needed on policy or discontinued products only' AS out_pol_audit_frqncy,
	'N/A' AS out_final_audit_code,
	'N/A' AS out_zip_ind,
	'N/A' AS out_guarantee_ind,
	'N/A' AS out_variation_code,
	v_County AS out_county,
	'N/A' AS out_non_smoker_disc_code,
	0 AS out_renl_disc,
	-1 AS out_renl_safe_driver_disc_count,
	-- *INF*: TO_DATE('2100-12-31 23:59:59 ','YYYY-MM-DD HH24:MI:SS')
	TO_DATE('2100-12-31 23:59:59 ', 'YYYY-MM-DD HH24:MI:SS') AS out_nonrenewal_flag_date,
	-- *INF*: TO_DATE('2100-12-31 23:59:59 ','YYYY-MM-DD HH24:MI:SS')
	TO_DATE('2100-12-31 23:59:59 ', 'YYYY-MM-DD HH24:MI:SS') AS out_audit_complt_date,
	-- *INF*: TO_DATE('2100-12-31 23:59:59 ','YYYY-MM-DD HH24:MI:SS')
	TO_DATE('2100-12-31 23:59:59 ', 'YYYY-MM-DD HH24:MI:SS') AS out_orig_acct_date,
	-- *INF*: IIF(NOT ISNULL(in_TransactionDate),in_TransactionDate,TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'))
	IFF(NOT in_TransactionDate IS NULL, in_TransactionDate, TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) AS out_pol_enter_date,
	'N/A' AS out_excess_claim_code,
	'N/A' AS out_pol_status_on_pif,
	'N/A' AS out_target_mrkt_code,
	'N/A' AS out_pkg_code,
	'N/A' AS out_pol_kind_code,
	EXP_Src_DataCollect.BusinessSegmentCode AS in_bus_seg_code,
	-- *INF*: IIF(NOT ISNULL(in_bus_seg_code),in_bus_seg_code,'N/A')
	IFF(NOT in_bus_seg_code IS NULL, in_bus_seg_code, 'N/A') AS out_bus_seg_code,
	'N/A' AS out_pif_upload_audit_ind,
	0 AS out_err_flag_bal_txn,
	0 AS out_err_flag_bal_reins,
	in_Reference AS out_Reference,
	'N/A' AS out_ClassOfBusiness,
	0 AS out_ErrorFlagBalancePremiumTransaction,
	-- *INF*: IIF( Type='Renew' and NOT(ISNULL(in_PreviousPolicyNumber) OR IS_SPACES(in_PreviousPolicyNumber) OR LENGTH(in_PreviousPolicyNumber)=0)  , SUBSTR(in_PreviousPolicyNumber, 1, 7),'N/A')
	IFF(Type = 'Renew' AND NOT ( in_PreviousPolicyNumber IS NULL OR IS_SPACES(in_PreviousPolicyNumber) OR LENGTH(in_PreviousPolicyNumber) = 0 ), SUBSTR(in_PreviousPolicyNumber, 1, 7), 'N/A') AS out_RenewalPolicyNumber,
	'000' AS out_RenewalPolicySymbol,
	-- *INF*: IIF( Type='Renew' and NOT(ISNULL(in_PreviousPolicyNumber) OR IS_SPACES(in_PreviousPolicyNumber) OR LENGTH(in_PreviousPolicyNumber)=0)  , SUBSTR(in_PreviousPolicyNumber, 8, 2),'N/A')
	IFF(Type = 'Renew' AND NOT ( in_PreviousPolicyNumber IS NULL OR IS_SPACES(in_PreviousPolicyNumber) OR LENGTH(in_PreviousPolicyNumber) = 0 ), SUBSTR(in_PreviousPolicyNumber, 8, 2), 'N/A') AS out_RenewalPolicyMod,
	'N/A' AS out_BillingType,
	'1' AS out_strtgc_bus_dvsn_code,
	in_AgencyAKID AS out_AgencyAKID,
	-- *INF*: IIF(ISNULL(in_PolicyOfferingAKId),26,in_PolicyOfferingAKId)
	IFF(in_PolicyOfferingAKId IS NULL, 26, in_PolicyOfferingAKId) AS out_PolicyOfferingAKId,
	-- *INF*: IIF(ISNULL(in_Program) OR IS_SPACES(in_Program) OR LENGTH(in_Program)=0,'N/A',LTRIM(RTRIM(in_Program)))
	IFF(in_Program IS NULL OR IS_SPACES(in_Program) OR LENGTH(in_Program) = 0, 'N/A', LTRIM(RTRIM(in_Program))) AS out_ProgramCode,
	-- *INF*: IIF(ISNULL(in_SurchargeExemptCode),'N/A',in_SurchargeExemptCode)
	IFF(in_SurchargeExemptCode IS NULL, 'N/A', in_SurchargeExemptCode) AS out_SurchargeExemptCode,
	-- *INF*: IIF(ISNULL(in_StrategicProfitCenterAKId),-1,in_StrategicProfitCenterAKId)
	IFF(in_StrategicProfitCenterAKId IS NULL, - 1, in_StrategicProfitCenterAKId) AS out_StrategicProfitCenterAKId,
	-- *INF*: IIF(ISNULL(in_InsuranceSegmentAKId),-1,in_InsuranceSegmentAKId)
	IFF(in_InsuranceSegmentAKId IS NULL, - 1, in_InsuranceSegmentAKId) AS out_InsuranceSegmentAKId,
	'N/A' AS out_ObligeeName,
	EXP_Src_DataCollect.AutomatedUnderwritingIndicator AS AutomatedUnderwritingServicesIndicator,
	-- *INF*: IIF(in_AutomaticRenewalIndicator = 'T', '1', '0')
	IFF(in_AutomaticRenewalIndicator = 'T', '1', '0') AS out_AutomaticRenewalIndicator,
	-- *INF*: --Fix for defect 3860
	--  iif(isnull(in_Association) 
	-- or IS_SPACES(in_Association) 
	-- or LENGTH(in_Association)=0,'N/A',in_Association)
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(in_Association IS NULL OR IS_SPACES(in_Association) OR LENGTH(in_Association) = 0, 'N/A', in_Association) AS out_Association,
	EXP_Src_DataCollect.PriorPolicyNumber AS Prior_Policy_key,
	-- *INF*: IIF(ISNULL(Prior_Policy_key),'N/A',substr(Prior_Policy_key,1,3))
	IFF(Prior_Policy_key IS NULL, 'N/A', substr(Prior_Policy_key, 1, 3)) AS RenewalPolicySymbol,
	-- *INF*: IIF(ISNULL(Prior_Policy_key),'N/A',substr(Prior_Policy_key,4,7))
	IFF(Prior_Policy_key IS NULL, 'N/A', substr(Prior_Policy_key, 4, 7)) AS RenewalPolicyNumber,
	-- *INF*: IIF(ISNULL(Prior_Policy_key),'N/A',substr(Prior_Policy_key,11,2))
	IFF(Prior_Policy_key IS NULL, 'N/A', substr(Prior_Policy_key, 11, 2)) AS RenewalPolicyMod,
	EXP_Src_DataCollect.o_IsRollover AS in_IsRollover,
	-- *INF*: DECODE(in_IsRollover, 'T',1,'F',0,0)
	DECODE(in_IsRollover,
		'T', 1,
		'F', 0,
		0) AS out_IsRollover,
	-- *INF*: IIF(ISNULL(in_PriorCarrierName),'N/A', SUBSTR(LTRIM(RTRIM(in_PriorCarrierName)),1,50))
	IFF(in_PriorCarrierName IS NULL, 'N/A', SUBSTR(LTRIM(RTRIM(in_PriorCarrierName)), 1, 50)) AS v_PriorCarrierName,
	-- *INF*: IIF(ISNULL(in_PirorCarrierNameOther),'N/A',SUBSTR(LTRIM(RTRIM(in_PirorCarrierNameOther)),1,50))
	IFF(in_PirorCarrierNameOther IS NULL, 'N/A', SUBSTR(LTRIM(RTRIM(in_PirorCarrierNameOther)), 1, 50)) AS v_PriorCarrierNameOther,
	-- *INF*:  --The chage is implemented for SM 784755 
	-- IIF(v_PriorCarrierName='Other', v_PriorCarrierNameOther,v_PriorCarrierName)
	-- 
	-- --IIF(v_PriorCarrierName='Other', in_PirorCarrierNameOther,v_PriorCarrierName)
	IFF(v_PriorCarrierName = 'Other', v_PriorCarrierNameOther, v_PriorCarrierName) AS out_RolloverPriorCarrier,
	EXP_Src_DataCollect.o_MailPolicyToInsured AS in_MailPolicyToInsured,
	-- *INF*: DECODE(TRUE,
	-- in_MailPolicyToInsured='T','1',
	-- in_MailPolicyToInsured='1','1',
	-- '0')
	DECODE(TRUE,
		in_MailPolicyToInsured = 'T', '1',
		in_MailPolicyToInsured = '1', '1',
		'0') AS out_MailPolicyToInsured,
	LKP_AgencyEmployee_GetAgencyEmployeeAKID.AgencyEmployeeAKID AS in_AgencyEmployeeAKID,
	-- *INF*: IIF(ISNULL(in_AgencyEmployeeAKID),-1,in_AgencyEmployeeAKID)
	IFF(in_AgencyEmployeeAKID IS NULL, - 1, in_AgencyEmployeeAKID) AS out_AgencyEmployeeAKID,
	EXP_Src_DataCollect.ProducerCode AS in_ProducerCode,
	-- *INF*: IIF(ISNULL(in_ProducerCode),'N/A',in_ProducerCode)
	IFF(in_ProducerCode IS NULL, 'N/A', in_ProducerCode) AS out_ProducerCode,
	LKP_policy.pol_ak_id AS lkp_pol_ak_id,
	LKP_policy.pol_cancellation_ind AS lkp_pol_cancellation_ind,
	LKP_policy.pol_cancellation_date AS lkp_pol_cancellation_date,
	LKP_policy.pol_status_code AS lkp_pol_status_code,
	LKP_policy.eff_from_date AS lkp_eff_from_date,
	-- *INF*: DECODE(TRUE,
	-- in_PolicyIssueCodeOverride='T','1',
	-- in_PolicyIssueCodeOverride='1','1',
	-- '0')
	DECODE(TRUE,
		in_PolicyIssueCodeOverride = 'T', '1',
		in_PolicyIssueCodeOverride = '1', '1',
		'0') AS o_PolicyIssueCodeOverride,
	v_policy_key AS v_prev_policy_key
	FROM EXP_Src_DataCollect
	LEFT JOIN LKP_Agency
	ON LKP_Agency.AgencyCode = EXP_Src_DataCollect.AgencyCode
	LEFT JOIN LKP_AgencyEmployee_GetAgencyEmployeeAKID
	ON LKP_AgencyEmployee_GetAgencyEmployeeAKID.UserID = EXP_Src_DataCollect.ProducerCode AND LKP_AgencyEmployee_GetAgencyEmployeeAKID.AgencyEmployeeCode = EXP_Src_DataCollect.ProducerCode AND LKP_AgencyEmployee_GetAgencyEmployeeAKID.AgencyAKID = LKP_Agency.AgencyAKID
	LEFT JOIN LKP_Entered_Date
	ON LKP_Entered_Date.PolicyNumber = EXP_Src_DataCollect.PolicyNumber AND LKP_Entered_Date.PolicyVersion = EXP_Src_DataCollect.o_PolicyVersion
	LEFT JOIN LKP_InsuranceSegment
	ON LKP_InsuranceSegment.InsuranceSegmentCode = EXP_Src_DataCollect.o_InsuranceSegmentCode
	LEFT JOIN LKP_PolicyOffering
	ON LKP_PolicyOffering.PolicyOfferingCode = EXP_Src_DataCollect.PolicyOfferingCode
	LEFT JOIN LKP_StrategicProfitCenterAKId
	ON LKP_StrategicProfitCenterAKId.StrategicProfitCenterCode = EXP_Src_DataCollect.StrategicProfitCenterCode
	LEFT JOIN LKP_agency_ak_id
	ON LKP_agency_ak_id.agency_key = EXP_Src_DataCollect.AgencyCode
	LEFT JOIN LKP_policy
	ON LKP_policy.pol_key = EXP_Src_DataCollect.o_Policykey AND LKP_policy.eff_from_date <= EXP_Src_DataCollect.TransactionCreatedDate AND LKP_policy.eff_to_date >= EXP_Src_DataCollect.TransactionCreatedDate
	LEFT JOIN LKP_sup_reason_amended_code
	ON LKP_sup_reason_amended_code.rsn_amended_code = EXP_Src_DataCollect.PolicyCancellationReasonCode
	LEFT JOIN LKP_POL_ISSUE_CODE LKP_POL_ISSUE_CODE_v_policy_key
	ON LKP_POL_ISSUE_CODE_v_policy_key.pol_key = v_policy_key

	LEFT JOIN LKP_POL_ISSUE_CODE LKP_POL_ISSUE_CODE_Prior_Policy_key
	ON LKP_POL_ISSUE_CODE_Prior_Policy_key.pol_key = Prior_Policy_key

),
LKP_Association AS (
	SELECT
	AssociationCode
	FROM (
		SELECT 
			AssociationCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Association
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AssociationCode ORDER BY AssociationCode) = 1
),
LKP_Program AS (
	SELECT
	ProgramAKId,
	ProgramCode
	FROM (
		SELECT 
			ProgramAKId,
			ProgramCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Program
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProgramCode ORDER BY ProgramAKId) = 1
),
LKP_SupSurchargeExempt AS (
	SELECT
	SupSurchargeExemptId,
	SurchargeExemptCode
	FROM (
		SELECT 
			SupSurchargeExemptId,
			SurchargeExemptCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupSurchargeExempt
		WHERE CurrentSnapshotFlag=1 AND SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SurchargeExemptCode ORDER BY SupSurchargeExemptId) = 1
),
LKP_contract_customer_key AS (
	SELECT
	contract_cust_ak_id,
	contract_key
	FROM (
		SELECT 
		contract_customer.contract_cust_ak_id as contract_cust_ak_id, 
		ltrim(rtrim(contract_customer.contract_key)) as contract_key 
		FROM 
		contract_customer
		WHERE contract_customer.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY contract_key ORDER BY contract_cust_ak_id DESC) = 1
),
LKP_producer_code AS (
	SELECT
	prdcr_code_ak_id,
	agency_key,
	producer_code
	FROM (
		SELECT 
			prdcr_code_ak_id,
			agency_key,
			producer_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.producer_code
		WHERE crrnt_snpsht_flag = 1 AND source_sys_id = '@{pipeline().parameters.AGENCY_SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY agency_key,producer_code ORDER BY prdcr_code_ak_id) = 1
),
LKP_sup_business_classification_code AS (
	SELECT
	sup_bus_class_code_id,
	bus_class_code
	FROM (
		SELECT 
			sup_bus_class_code_id,
			bus_class_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_business_classification_code
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY bus_class_code ORDER BY sup_bus_class_code_id) = 1
),
LKP_sup_industry_risk_grade_code AS (
	SELECT
	sup_industry_risk_grade_code_id,
	industry_risk_grade_code
	FROM (
		SELECT 
			sup_industry_risk_grade_code_id,
			industry_risk_grade_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_industry_risk_grade_code
		WHERE crrnt_snpsht_flag = 1 AND source_sys_id = 'PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY industry_risk_grade_code ORDER BY sup_industry_risk_grade_code_id) = 1
),
LKP_sup_policy_audit_frequency AS (
	SELECT
	sup_pol_audit_frqncy_id,
	pol_audit_frqncy_descript
	FROM (
		SELECT 
			sup_pol_audit_frqncy_id,
			pol_audit_frqncy_descript
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_policy_audit_frequency
		WHERE crrnt_snpsht_flag = 1 AND source_sys_id = 'PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_audit_frqncy_descript ORDER BY sup_pol_audit_frqncy_id) = 1
),
LKP_sup_policy_issue_code AS (
	SELECT
	sup_pol_issue_code_id,
	pol_issue_code
	FROM (
		SELECT 
			sup_pol_issue_code_id,
			pol_issue_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_policy_issue_code
		WHERE crrnt_snpsht_flag = 1 AND source_sys_id = 'PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_issue_code ORDER BY sup_pol_issue_code_id) = 1
),
LKP_sup_policy_status_code AS (
	SELECT
	sup_pol_status_code_id,
	pol_status_code
	FROM (
		SELECT 
			sup_pol_status_code_id,
			pol_status_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_policy_status_code
		WHERE crrnt_snpsht_flag = 1 AND source_sys_id = 'PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_status_code ORDER BY sup_pol_status_code_id) = 1
),
LKP_sup_policy_term AS (
	SELECT
	sup_pol_term_id,
	pol_term
	FROM (
		SELECT 
			sup_pol_term_id,
			pol_term
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_policy_term
		WHERE crrnt_snpsht_flag = 1 AND source_sys_id = 'PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_term ORDER BY sup_pol_term_id) = 1
),
LKP_sup_state AS (
	SELECT
	state_abbrev,
	sup_state_id,
	state_code
	FROM (
		SELECT 
			state_abbrev,
			sup_state_id,
			state_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state
		WHERE crrnt_snpsht_flag = 1 AND source_sys_id = 'EXCEED'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY state_abbrev) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_policy.pol_id AS lkp_pol_id,
	LKP_policy.pol_ak_id AS lkp_pol_ak_id,
	LKP_policy.eff_from_date AS lkp_eff_from_date,
	LKP_policy.contract_cust_ak_id AS lkp_contract_cust_ak_id,
	LKP_policy.agency_ak_id AS lkp_agency_ak_id,
	LKP_policy.AgencyAKID AS lkp_AgencyAKID,
	LKP_policy.pol_key AS lkp_pol_key,
	LKP_policy.mco AS lkp_mco,
	LKP_policy.pol_co_num AS lkp_pol_co_num,
	LKP_policy.pol_eff_date AS lkp_pol_eff_date,
	LKP_policy.pol_exp_date AS lkp_pol_exp_date,
	LKP_policy.orig_incptn_date AS lkp_orig_incptn_date,
	LKP_policy.prim_bus_class_code AS lkp_prim_bus_class_code,
	LKP_policy.reins_code AS lkp_reins_code,
	LKP_policy.pms_pol_lob_code AS lkp_pms_pol_lob_code,
	LKP_policy.pol_co_line_code AS lkp_pol_co_line_code,
	LKP_policy.pol_cancellation_ind AS lkp_pol_cancellation_ind,
	LKP_policy.pol_cancellation_date AS lkp_pol_cancellation_date,
	LKP_policy.pol_cancellation_rsn_code AS lkp_pol_cancellation_rsn_code,
	LKP_policy.state_of_domicile_code AS lkp_state_of_domicile_code,
	LKP_policy.wbconnect_upload_code AS lkp_wbconnect_upload_code,
	LKP_policy.serv_center_support_code AS lkp_serv_center_support_code,
	LKP_policy.pol_term AS lkp_pol_term,
	LKP_policy.terrorism_risk_ind AS lkp_terrorism_risk_ind,
	LKP_policy.prior_pol_key AS lkp_prior_pol_key,
	LKP_policy.pol_status_code AS lkp_pol_status_code,
	LKP_policy.pol_issue_code AS lkp_pol_issue_code,
	LKP_policy.pol_age AS lkp_pol_age,
	LKP_policy.industry_risk_grade_code AS lkp_industry_risk_grade_code,
	LKP_policy.uw_review_yr AS lkp_uw_review_yr,
	LKP_policy.mvr_request_code AS lkp_mvr_request_code,
	LKP_policy.renl_code AS lkp_renl_code,
	LKP_policy.amend_num AS lkp_amend_num,
	LKP_policy.anniversary_rerate_code AS lkp_anniversary_rerate_code,
	LKP_policy.pol_audit_frqncy AS lkp_pol_audit_frqncy,
	LKP_policy.final_audit_code AS lkp_final_audit_code,
	LKP_policy.zip_ind AS lkp_zip_ind,
	LKP_policy.guarantee_ind AS lkp_guarantee_ind,
	LKP_policy.variation_code AS lkp_variation_code,
	LKP_policy.county AS lkp_county,
	LKP_policy.non_smoker_disc_code AS lkp_non_smoker_disc_code,
	LKP_policy.renl_disc AS lkp_renl_disc,
	LKP_policy.renl_safe_driver_disc_count AS lkp_renl_safe_driver_disc_count,
	LKP_policy.nonrenewal_flag_date AS lkp_nonrenewal_flag_date,
	LKP_policy.audit_complt_date AS lkp_audit_complt_date,
	LKP_policy.orig_acct_date AS lkp_orig_acct_date,
	LKP_policy.pol_enter_date AS lkp_pol_enter_date,
	LKP_policy.excess_claim_code AS lkp_excess_claim_code,
	LKP_policy.pol_status_on_pif AS lkp_pol_status_on_pif,
	LKP_policy.target_mrkt_code AS lkp_target_mrkt_code,
	LKP_policy.pkg_code AS lkp_pkg_code,
	LKP_policy.pol_kind_code AS lkp_pol_kind_code,
	LKP_policy.bus_seg_code AS lkp_bus_seg_code,
	LKP_policy.pif_upload_audit_ind AS lkp_pif_upload_audit_ind,
	LKP_policy.err_flag_bal_txn AS lkp_err_flag_bal_txn,
	LKP_policy.err_flag_bal_reins AS lkp_err_flag_bal_reins,
	LKP_policy.producer_code_ak_id AS lkp_producer_code_ak_id,
	LKP_policy.prdcr_code AS lkp_prdcr_code,
	LKP_policy.ClassOfBusiness AS lkp_ClassOfBusiness,
	LKP_policy.strtgc_bus_dvsn_ak_id AS lkp_strtgc_bus_dvsn_ak_id,
	LKP_policy.ErrorFlagBalancePremiumTransaction AS lkp_ErrorFlagBalancePremiumTransaction,
	LKP_policy.RenewalPolicyNumber AS lkp_RenewalPolicyNumber,
	LKP_policy.RenewalPolicySymbol AS lkp_RenewalPolicySymbol,
	LKP_policy.RenewalPolicyMod AS lkp_RenewalPolicyMod,
	LKP_policy.BillingType AS lkp_BillingType,
	LKP_policy.sup_bus_class_code_id AS lkp_sup_bus_class_code_id,
	LKP_policy.sup_pol_term_id AS lkp_sup_pol_term_id,
	LKP_policy.sup_pol_status_code_id AS lkp_sup_pol_status_code_id,
	LKP_policy.sup_pol_issue_code_id AS lkp_sup_pol_issue_code_id,
	LKP_policy.sup_pol_audit_frqncy_id AS lkp_sup_pol_audit_frqncy_id,
	LKP_policy.sup_industry_risk_grade_code_id AS lkp_sup_industry_risk_grade_code_id,
	LKP_policy.sup_state_id AS lkp_sup_state_id,
	LKP_policy.PolicyOfferingAKId AS lkp_PolicyOfferingAKId,
	LKP_policy.producer_code_id AS lkp_producer_code_id,
	LKP_policy.SurchargeExemptCode AS lkp_SurchargeExemptCode,
	LKP_policy.SupSurchargeExemptID AS lkp_SupSurchargeExemptID,
	LKP_policy.StrategicProfitCenterAKId AS lkp_StrategicProfitCenterAKId,
	LKP_policy.InsuranceSegmentAKId AS lkp_InsuranceSegmentAKId,
	LKP_policy.ProgramAKId AS lkp_ProgramAKId,
	LKP_policy.ObligeeName AS lkp_ObligeeName,
	LKP_policy.AutomatedUnderwritingServicesIndicator AS lkp_AutomatedUnderwritingServicesIndicator,
	LKP_policy.AutomaticRenewalIndicator AS lkp_AutomaticRenewalIndicator,
	LKP_policy.AssociationCode AS lkp_AssociationCode,
	LKP_contract_customer_key.contract_cust_ak_id AS in_contract_cust_ak_id,
	LKP_producer_code.prdcr_code_ak_id AS in_producer_code_ak_id,
	LKP_sup_business_classification_code.sup_bus_class_code_id AS in_sup_bus_class_code_id,
	LKP_sup_policy_term.sup_pol_term_id AS in_sup_pol_term_id,
	LKP_sup_policy_status_code.sup_pol_status_code_id AS in_sup_pol_status_code_id,
	LKP_sup_policy_issue_code.sup_pol_issue_code_id AS in_sup_pol_issue_code_id,
	LKP_sup_policy_audit_frequency.sup_pol_audit_frqncy_id AS in_sup_pol_audit_frqncy_id,
	LKP_sup_industry_risk_grade_code.sup_industry_risk_grade_code_id AS in_sup_industry_risk_grade_code_id,
	LKP_sup_state.sup_state_id AS in_sup_state_id,
	EXP_values.Type AS in_TransactionType,
	EXP_values.out_AgencyAKID AS in_AgencyAKID,
	EXP_values.out_PolicyOfferingAKId AS in_PolicyOfferingAKId,
	LKP_Program.ProgramAKId AS in_ProgramAKId,
	EXP_values.out_strtgc_bus_dvsn_code AS in_strtgc_bus_dvsn_code,
	EXP_values.out_StrategicProfitCenterAKId AS in_StrategicProfitCenterAKId,
	EXP_values.out_InsuranceSegmentAKId AS in_InsuranceSegmentAKId,
	LKP_Association.AssociationCode AS in_AssociationCode,
	EXP_values.AssociationDiscountFactor AS in_AssociationDiscountFactor,
	EXP_values.out_CreatedDate AS in_CreatedDate,
	EXP_values.out_Association AS in_Association,
	EXP_values.out_AgencyEmployeeAKID AS AgencyEmployeeAKId,
	EXP_values.out_policy_key AS pol_key,
	EXP_values.out_mco AS mco,
	EXP_values.out_pol_co_num AS pol_co_num,
	EXP_values.out_EffectiveDate AS pol_eff_date,
	EXP_values.out_ExpirationDate AS pol_exp_date,
	EXP_values.out_InceptionDate AS orig_incptn_date,
	EXP_values.out_prim_bus_class_code AS prim_bus_class_code,
	EXP_values.out_reins_code AS reins_code,
	EXP_values.out_pms_pol_lob_code AS pms_pol_lob_code,
	EXP_values.out_pol_co_line_code AS pol_co_line_code,
	EXP_values.pol_cancellation_ind,
	EXP_values.out_pol_cancellation_date AS pol_cancellation_date,
	EXP_values.out_pol_cancellation_rsn_code AS pol_cancellation_rsn_code,
	LKP_sup_state.state_abbrev AS state_of_domicile_code,
	EXP_values.out_wbconnect_upload_code AS wbconnect_upload_code,
	EXP_values.serv_center_support_code,
	EXP_values.out_pol_term AS pol_term,
	EXP_values.out_terrorism_risk_ind AS terrorism_risk_ind,
	EXP_values.PolicyStatusCode AS pol_status_code,
	EXP_values.PolicyIssueCode AS pol_issue_code,
	EXP_values.out_pol_age AS pol_age,
	EXP_values.out_industry_risk_grade_code AS industry_risk_grade_code,
	EXP_values.out_uw_review_yr AS uw_review_yr,
	EXP_values.out_mvr_request_code AS mvr_request_code,
	EXP_values.out_renl_code AS renl_code,
	EXP_values.out_amend_num AS amend_num,
	EXP_values.out_anniversary_rerate_code AS anniversary_rerate_code,
	EXP_values.out_pol_audit_frqncy AS pol_audit_frqncy,
	EXP_values.out_final_audit_code AS final_audit_code,
	EXP_values.out_zip_ind AS zip_ind,
	EXP_values.out_guarantee_ind AS guarantee_ind,
	EXP_values.out_variation_code AS variation_code,
	EXP_values.out_county AS county,
	EXP_values.out_non_smoker_disc_code AS non_smoker_disc_code,
	EXP_values.out_renl_disc AS renl_disc,
	EXP_values.out_renl_safe_driver_disc_count AS renl_safe_driver_disc_count,
	EXP_values.out_nonrenewal_flag_date AS nonrenewal_flag_date,
	EXP_values.out_audit_complt_date AS audit_complt_date,
	EXP_values.out_orig_acct_date AS orig_acct_date,
	EXP_values.out_pol_enter_date AS pol_enter_date,
	EXP_values.out_excess_claim_code AS excess_claim_code,
	EXP_values.out_pol_status_on_pif AS pol_status_on_pif,
	EXP_values.out_target_mrkt_code AS target_mrkt_code,
	EXP_values.out_pkg_code AS pkg_code,
	EXP_values.out_pol_kind_code AS pol_kind_code,
	EXP_values.out_bus_seg_code AS bus_seg_code,
	EXP_values.out_pif_upload_audit_ind AS pif_upload_audit_ind,
	EXP_values.out_err_flag_bal_txn AS err_flag_bal_txn,
	EXP_values.out_err_flag_bal_reins AS err_flag_bal_reins,
	EXP_values.out_ProducerCode AS prdcr_code,
	EXP_values.out_ClassOfBusiness AS ClassOfBusiness,
	EXP_values.out_ErrorFlagBalancePremiumTransaction AS ErrorFlagBalancePremiumTransaction,
	EXP_values.out_BillingType AS BillingType,
	EXP_values.out_pol_sym AS pol_sym,
	EXP_values.out_PolicyNumber AS PolicyNumber,
	EXP_values.out_PolicyVersion AS PolicyVersion,
	EXP_values.out_producer_code_id AS producer_code_id,
	EXP_values.out_SurchargeExemptCode AS SurchargeExemptCode,
	LKP_SupSurchargeExempt.SupSurchargeExemptId,
	EXP_values.out_ObligeeName AS ObligeeName,
	EXP_values.AutomatedUnderwritingServicesIndicator,
	EXP_values.out_AutomaticRenewalIndicator AS AutomaticRenewalIndicator,
	EXP_values.Prior_Policy_key,
	EXP_values.RenewalPolicySymbol,
	EXP_values.RenewalPolicyNumber,
	EXP_values.RenewalPolicyMod,
	EXP_values.agency_ak_id,
	-- *INF*: :LKP.LKP_POLICY_CONTRACT_CUSTOMER(pol_key)
	LKP_POLICY_CONTRACT_CUSTOMER_pol_key.contract_cust_ak_id AS v_LKP_Policy_Contract_Cust_Ak_Id,
	-- *INF*: Decode(TRUE,
	-- (ISNULL(lkp_contract_cust_ak_id)=0 and lkp_contract_cust_ak_id<>-1),lkp_contract_cust_ak_id,
	-- ISNULL(v_LKP_Policy_Contract_Cust_Ak_Id)=0,v_LKP_Policy_Contract_Cust_Ak_Id,
	-- in_contract_cust_ak_id)
	-- 
	-- --Decode(TRUE,
	-- --ISNULL(lkp_contract_cust_ak_id)=0,lkp_contract_cust_ak_id,
	-- --ISNULL(v_LKP_Policy_Contract_Cust_Ak_Id)=--0,v_LKP_Policy_Contract_Cust_Ak_Id,
	-- --in_contract_cust_ak_id)
	Decode(TRUE,
		( lkp_contract_cust_ak_id IS NULL = 0 AND lkp_contract_cust_ak_id <> - 1 ), lkp_contract_cust_ak_id,
		v_LKP_Policy_Contract_Cust_Ak_Id IS NULL = 0, v_LKP_Policy_Contract_Cust_Ak_Id,
		in_contract_cust_ak_id) AS v_contract_cust_ak_id,
	-- *INF*: IIF(ISNULL(in_ProgramAKId),-1,in_ProgramAKId)
	IFF(in_ProgramAKId IS NULL, - 1, in_ProgramAKId) AS v_ProgramAKId,
	-- *INF*: IIF(
	--   ISNULL(in_AgencyAKID),
	--   -1,
	--   in_AgencyAKID
	-- )
	IFF(in_AgencyAKID IS NULL, - 1, in_AgencyAKID) AS v_AgencyAKID,
	-- *INF*: IIF(
	--   ISNULL(in_producer_code_ak_id),
	--   -1,
	--   in_producer_code_ak_id
	-- )
	IFF(in_producer_code_ak_id IS NULL, - 1, in_producer_code_ak_id) AS v_producer_code_ak_id,
	-- *INF*: IIF(
	--   ISNULL(in_sup_bus_class_code_id),
	--   -1,
	--   in_sup_bus_class_code_id
	-- )
	IFF(in_sup_bus_class_code_id IS NULL, - 1, in_sup_bus_class_code_id) AS v_sup_bus_class_code_id,
	-- *INF*: :LKP.LKP_STRATEGIC_BUSINESS_DIVISION(in_strtgc_bus_dvsn_code)
	LKP_STRATEGIC_BUSINESS_DIVISION_in_strtgc_bus_dvsn_code.strtgc_bus_dvsn_ak_id AS v_strtgc_bus_dvsn_ak_id,
	-- *INF*: IIF(
	--   ISNULL(in_sup_pol_term_id),
	--   -1,
	--   in_sup_pol_term_id
	-- )
	IFF(in_sup_pol_term_id IS NULL, - 1, in_sup_pol_term_id) AS v_sup_pol_term_id,
	-- *INF*: IIF(
	--   ISNULL(in_sup_pol_status_code_id),
	--   -1,
	--   in_sup_pol_status_code_id
	-- )
	IFF(in_sup_pol_status_code_id IS NULL, - 1, in_sup_pol_status_code_id) AS v_sup_pol_status_code_id,
	-- *INF*: IIF(
	--   ISNULL(in_sup_pol_issue_code_id),
	--   -1,
	--   in_sup_pol_issue_code_id
	-- )
	IFF(in_sup_pol_issue_code_id IS NULL, - 1, in_sup_pol_issue_code_id) AS v_sup_pol_issue_code_id,
	-- *INF*: IIF(
	--   ISNULL(in_sup_pol_audit_frqncy_id),
	--   -1,
	--   in_sup_pol_audit_frqncy_id
	-- )
	IFF(in_sup_pol_audit_frqncy_id IS NULL, - 1, in_sup_pol_audit_frqncy_id) AS v_sup_pol_audit_frqncy_id,
	-- *INF*: IIF(
	--   ISNULL(in_sup_industry_risk_grade_code_id),
	--   -1,
	--   in_sup_industry_risk_grade_code_id
	-- )
	IFF(in_sup_industry_risk_grade_code_id IS NULL, - 1, in_sup_industry_risk_grade_code_id) AS v_sup_industry_risk_grade_code_id,
	-- *INF*: IIF(
	--   ISNULL(in_sup_state_id),
	--   -1,
	--   in_sup_state_id
	-- )
	IFF(in_sup_state_id IS NULL, - 1, in_sup_state_id) AS v_sup_state_id,
	-- *INF*: IIF(ISNULL(SupSurchargeExemptId),-1,SupSurchargeExemptId)
	IFF(SupSurchargeExemptId IS NULL, - 1, SupSurchargeExemptId) AS v_SupSurchargeExemptId,
	-- *INF*: DECODE(lkp_AutomaticRenewalIndicator, 'T', '1', 'F', '0', NULL)
	DECODE(lkp_AutomaticRenewalIndicator,
		'T', '1',
		'F', '0',
		NULL) AS v_lkp_AutomaticRenewalIndicator,
	-- *INF*: iif(isnull(in_AssociationCode ) or IS_SPACES(in_AssociationCode ) or LENGTH(in_AssociationCode )=0,'N/A',in_AssociationCode )
	-- 
	-- 
	IFF(in_AssociationCode IS NULL OR IS_SPACES(in_AssociationCode) OR LENGTH(in_AssociationCode) = 0, 'N/A', in_AssociationCode) AS v_Association,
	LKP_policy.RolloverPolicyIndicator AS lkp_RolloverPolicyIndicator,
	-- *INF*: DECODE(lkp_RolloverPolicyIndicator, 'T', '1', 'F', '0', '0')
	DECODE(lkp_RolloverPolicyIndicator,
		'T', '1',
		'F', '0',
		'0') AS v_lkp_RolloverPolicyIndicator,
	LKP_policy.RolloverPriorCarrier AS lkp_RolloverPriorCarrier,
	-- *INF*: IIF(ISNULL(lkp_RolloverPriorCarrier),'N/A',lkp_RolloverPriorCarrier)
	IFF(lkp_RolloverPriorCarrier IS NULL, 'N/A', lkp_RolloverPriorCarrier) AS v_lkp_RolloverPriorCarrier,
	LKP_policy.MailToInsuredFlag AS lkp_MailPolicyToInsured,
	-- *INF*: DECODE(TRUE,
	-- lkp_MailPolicyToInsured='T','1',
	-- lkp_MailPolicyToInsured='1','1',
	-- '0')
	DECODE(TRUE,
		lkp_MailPolicyToInsured = 'T', '1',
		lkp_MailPolicyToInsured = '1', '1',
		'0') AS v_lkp_MailPolicyToInsured,
	LKP_policy.AgencyEmployeeAKId AS lkp_AgencyEmployeeAKId,
	LKP_policy.PolicyIssueCodeOverride AS lkp_PolicyIssueCodeOverride,
	-- *INF*: DECODE(TRUE,
	-- lkp_PolicyIssueCodeOverride='T','1',
	-- lkp_PolicyIssueCodeOverride='1','1',
	-- '0')
	DECODE(TRUE,
		lkp_PolicyIssueCodeOverride = 'T', '1',
		lkp_PolicyIssueCodeOverride = '1', '1',
		'0') AS v_lkp_PolicyIssueCodeOverride,
	EXP_values.out_IsRollover AS IsRollover,
	EXP_values.out_RolloverPriorCarrier AS RolloverPriorCarrier,
	EXP_values.out_MailPolicyToInsured AS MailPolicyToInsured,
	EXP_values.o_PolicyIssueCodeOverride AS in_PolicyIssueCodeOverride,
	-- *INF*: MD5(TO_CHAR(v_contract_cust_ak_id)||
	-- TO_CHAR(agency_ak_id) || 
	-- TO_CHAR(v_AgencyAKID)||
	-- TO_CHAR(mco)||
	-- TO_CHAR(pol_co_num)||
	-- TO_CHAR(pol_eff_date)||
	-- TO_CHAR(pol_exp_date)||
	-- TO_CHAR(orig_incptn_date)||
	-- TO_CHAR(prim_bus_class_code)||
	-- TO_CHAR(reins_code)||
	-- TO_CHAR(pms_pol_lob_code)||
	-- TO_CHAR(pol_co_line_code)||
	-- TO_CHAR(pol_cancellation_ind)||
	-- TO_CHAR(pol_cancellation_date)||
	-- TO_CHAR(pol_cancellation_rsn_code)||
	-- TO_CHAR(state_of_domicile_code)||
	-- TO_CHAR(wbconnect_upload_code)||
	-- TO_CHAR(serv_center_support_code)||
	-- TO_CHAR(pol_term)||
	-- TO_CHAR(terrorism_risk_ind)||
	-- TO_CHAR(Prior_Policy_key)||
	-- TO_CHAR(pol_status_code)||
	-- TO_CHAR(pol_issue_code)||
	-- TO_CHAR(pol_age)||
	-- TO_CHAR(industry_risk_grade_code)||
	-- TO_CHAR(uw_review_yr)||
	-- TO_CHAR(mvr_request_code)||
	-- TO_CHAR(renl_code)||
	-- TO_CHAR(amend_num)||
	-- TO_CHAR(anniversary_rerate_code)||
	-- TO_CHAR(pol_audit_frqncy)||
	-- TO_CHAR(final_audit_code)||
	-- TO_CHAR(zip_ind)||
	-- TO_CHAR(guarantee_ind)||
	-- TO_CHAR(variation_code)||
	-- TO_CHAR(county)||
	-- TO_CHAR(non_smoker_disc_code)||
	-- TO_CHAR(renl_disc)||
	-- TO_CHAR(renl_safe_driver_disc_count)||
	-- TO_CHAR(nonrenewal_flag_date)||
	-- TO_CHAR(audit_complt_date)||
	-- TO_CHAR(orig_acct_date)||
	-- TO_CHAR(pol_enter_date)||
	-- TO_CHAR(excess_claim_code)||
	-- TO_CHAR(pol_status_on_pif)||
	-- TO_CHAR(target_mrkt_code)||
	-- TO_CHAR(pkg_code)||
	-- TO_CHAR(pol_kind_code)||
	-- TO_CHAR(bus_seg_code)||
	-- TO_CHAR(pif_upload_audit_ind)||
	-- TO_CHAR(err_flag_bal_txn)||
	-- TO_CHAR(err_flag_bal_reins)||
	-- TO_CHAR(v_producer_code_ak_id)||
	-- TO_CHAR(prdcr_code)||
	-- TO_CHAR(ClassOfBusiness)||
	-- TO_CHAR(v_strtgc_bus_dvsn_ak_id)||
	-- TO_CHAR(ErrorFlagBalancePremiumTransaction)||
	-- TO_CHAR(RenewalPolicyNumber)||
	-- TO_CHAR(RenewalPolicySymbol)||
	-- TO_CHAR(RenewalPolicyMod)||
	-- TO_CHAR(BillingType)||
	-- TO_CHAR(v_sup_bus_class_code_id)||
	-- TO_CHAR(v_sup_pol_term_id)||
	-- TO_CHAR(v_sup_pol_status_code_id)||
	-- TO_CHAR(v_sup_pol_issue_code_id)||
	-- TO_CHAR(v_sup_pol_audit_frqncy_id)||
	-- TO_CHAR(v_sup_industry_risk_grade_code_id)||
	-- TO_CHAR(v_sup_state_id)||
	-- TO_CHAR(in_PolicyOfferingAKId)||
	-- TO_CHAR(producer_code_id)||
	-- TO_CHAR(SurchargeExemptCode)||
	-- TO_CHAR(v_SupSurchargeExemptId )||
	-- TO_CHAR(v_Association)||
	-- TO_CHAR(in_StrategicProfitCenterAKId)||
	-- TO_CHAR(in_InsuranceSegmentAKId)||
	-- TO_CHAR(v_ProgramAKId)||
	-- TO_CHAR(ObligeeName)||
	-- TO_CHAR(AutomatedUnderwritingServicesIndicator)||
	-- TO_CHAR(AutomaticRenewalIndicator))
	MD5(TO_CHAR(v_contract_cust_ak_id) || TO_CHAR(agency_ak_id) || TO_CHAR(v_AgencyAKID) || TO_CHAR(mco) || TO_CHAR(pol_co_num) || TO_CHAR(pol_eff_date) || TO_CHAR(pol_exp_date) || TO_CHAR(orig_incptn_date) || TO_CHAR(prim_bus_class_code) || TO_CHAR(reins_code) || TO_CHAR(pms_pol_lob_code) || TO_CHAR(pol_co_line_code) || TO_CHAR(pol_cancellation_ind) || TO_CHAR(pol_cancellation_date) || TO_CHAR(pol_cancellation_rsn_code) || TO_CHAR(state_of_domicile_code) || TO_CHAR(wbconnect_upload_code) || TO_CHAR(serv_center_support_code) || TO_CHAR(pol_term) || TO_CHAR(terrorism_risk_ind) || TO_CHAR(Prior_Policy_key) || TO_CHAR(pol_status_code) || TO_CHAR(pol_issue_code) || TO_CHAR(pol_age) || TO_CHAR(industry_risk_grade_code) || TO_CHAR(uw_review_yr) || TO_CHAR(mvr_request_code) || TO_CHAR(renl_code) || TO_CHAR(amend_num) || TO_CHAR(anniversary_rerate_code) || TO_CHAR(pol_audit_frqncy) || TO_CHAR(final_audit_code) || TO_CHAR(zip_ind) || TO_CHAR(guarantee_ind) || TO_CHAR(variation_code) || TO_CHAR(county) || TO_CHAR(non_smoker_disc_code) || TO_CHAR(renl_disc) || TO_CHAR(renl_safe_driver_disc_count) || TO_CHAR(nonrenewal_flag_date) || TO_CHAR(audit_complt_date) || TO_CHAR(orig_acct_date) || TO_CHAR(pol_enter_date) || TO_CHAR(excess_claim_code) || TO_CHAR(pol_status_on_pif) || TO_CHAR(target_mrkt_code) || TO_CHAR(pkg_code) || TO_CHAR(pol_kind_code) || TO_CHAR(bus_seg_code) || TO_CHAR(pif_upload_audit_ind) || TO_CHAR(err_flag_bal_txn) || TO_CHAR(err_flag_bal_reins) || TO_CHAR(v_producer_code_ak_id) || TO_CHAR(prdcr_code) || TO_CHAR(ClassOfBusiness) || TO_CHAR(v_strtgc_bus_dvsn_ak_id) || TO_CHAR(ErrorFlagBalancePremiumTransaction) || TO_CHAR(RenewalPolicyNumber) || TO_CHAR(RenewalPolicySymbol) || TO_CHAR(RenewalPolicyMod) || TO_CHAR(BillingType) || TO_CHAR(v_sup_bus_class_code_id) || TO_CHAR(v_sup_pol_term_id) || TO_CHAR(v_sup_pol_status_code_id) || TO_CHAR(v_sup_pol_issue_code_id) || TO_CHAR(v_sup_pol_audit_frqncy_id) || TO_CHAR(v_sup_industry_risk_grade_code_id) || TO_CHAR(v_sup_state_id) || TO_CHAR(in_PolicyOfferingAKId) || TO_CHAR(producer_code_id) || TO_CHAR(SurchargeExemptCode) || TO_CHAR(v_SupSurchargeExemptId) || TO_CHAR(v_Association) || TO_CHAR(in_StrategicProfitCenterAKId) || TO_CHAR(in_InsuranceSegmentAKId) || TO_CHAR(v_ProgramAKId) || TO_CHAR(ObligeeName) || TO_CHAR(AutomatedUnderwritingServicesIndicator) || TO_CHAR(AutomaticRenewalIndicator)) AS v_HashKey,
	-- *INF*: DECODE(TRUE,
	-- pol_key=v_prev_pol_key AND v_HashKey=v_prev_HashKey, 'NOCHANGE',
	-- ISNULL(lkp_pol_ak_id), 'NEW',
	-- in_CreatedDate=lkp_eff_from_date, 'UPDATE', --prod11524 this is to update the records if they are coming to edw again due to data repair...
	-- ( (pol_key=v_prev_pol_key AND v_HashKey<>v_prev_HashKey) OR ---prod11524 added this line to handle the changes to be inserted. In above conditions, the changes are ignored by NOCHANGE
	-- (lkp_contract_cust_ak_id	!=	v_contract_cust_ak_id
	-- OR 	lkp_AgencyAKID	!=	v_AgencyAKID
	-- OR      lkp_agency_ak_id != agency_ak_id
	-- OR	lkp_mco	!=	mco
	-- OR 	lkp_pol_co_num	!=	pol_co_num
	-- OR 	lkp_pol_eff_date	!=	pol_eff_date
	-- OR 	lkp_pol_exp_date	!=	pol_exp_date
	-- OR 	lkp_orig_incptn_date	!=	orig_incptn_date
	-- OR 	lkp_prim_bus_class_code	!=	prim_bus_class_code
	-- OR 	lkp_reins_code	!=	reins_code
	-- OR 	lkp_pms_pol_lob_code	!=	pms_pol_lob_code
	-- OR 	lkp_pol_co_line_code	!=	pol_co_line_code
	-- OR 	lkp_pol_cancellation_ind	!=	pol_cancellation_ind
	-- OR 	lkp_pol_cancellation_date	!=	pol_cancellation_date
	-- OR 	lkp_pol_cancellation_rsn_code	!=	pol_cancellation_rsn_code
	-- OR 	lkp_state_of_domicile_code	!=	state_of_domicile_code
	-- OR 	lkp_wbconnect_upload_code	!=	wbconnect_upload_code
	-- OR 	lkp_serv_center_support_code	!=	serv_center_support_code
	-- OR 	lkp_pol_term	!=	pol_term
	-- OR 	lkp_terrorism_risk_ind	!=	terrorism_risk_ind
	-- OR 	lkp_prior_pol_key	!=	Prior_Policy_key
	-- OR 	lkp_pol_status_code	!=	pol_status_code
	-- OR 	lkp_pol_issue_code	!=	pol_issue_code
	-- OR 	lkp_pol_age	!=	pol_age
	-- OR 	lkp_industry_risk_grade_code	!=	industry_risk_grade_code
	-- OR 	lkp_uw_review_yr	!=	uw_review_yr
	-- OR 	lkp_mvr_request_code	!=	mvr_request_code
	-- OR 	lkp_renl_code	!=	renl_code
	-- OR 	lkp_amend_num	!=	amend_num
	-- OR 	lkp_anniversary_rerate_code	!=	anniversary_rerate_code
	-- OR 	lkp_pol_audit_frqncy	!=	pol_audit_frqncy
	-- OR 	lkp_final_audit_code	!=	final_audit_code
	-- OR 	lkp_zip_ind	!=	zip_ind
	-- OR 	lkp_guarantee_ind	!=	guarantee_ind
	-- OR 	lkp_variation_code	!=	variation_code
	-- OR 	lkp_county	!=	county
	-- OR 	lkp_non_smoker_disc_code	!=	non_smoker_disc_code
	-- OR 	lkp_renl_disc	!=	renl_disc
	-- OR 	lkp_renl_safe_driver_disc_count	!=	renl_safe_driver_disc_count
	-- OR 	lkp_nonrenewal_flag_date	!=	nonrenewal_flag_date
	-- OR 	lkp_audit_complt_date	!=	audit_complt_date
	-- OR 	lkp_orig_acct_date	!=	orig_acct_date
	-- OR 	lkp_pol_enter_date	!=	pol_enter_date
	-- OR 	lkp_excess_claim_code	!=	excess_claim_code
	-- OR 	lkp_pol_status_on_pif	!=	pol_status_on_pif
	-- OR 	lkp_target_mrkt_code	!=	target_mrkt_code
	-- OR 	lkp_pkg_code	!=	pkg_code
	-- OR 	lkp_pol_kind_code	!=	pol_kind_code
	-- OR 	lkp_bus_seg_code	!=	bus_seg_code
	-- OR 	lkp_pif_upload_audit_ind	!=	pif_upload_audit_ind
	-- OR 	lkp_err_flag_bal_txn	!=	err_flag_bal_txn
	-- OR 	lkp_err_flag_bal_reins	!=	err_flag_bal_reins
	-- OR 	lkp_producer_code_ak_id	!=	v_producer_code_ak_id
	-- OR 	lkp_prdcr_code	!=	prdcr_code
	-- OR 	lkp_ClassOfBusiness	!=	ClassOfBusiness
	-- OR 	lkp_strtgc_bus_dvsn_ak_id	!=	v_strtgc_bus_dvsn_ak_id
	-- OR 	lkp_ErrorFlagBalancePremiumTransaction	!=	ErrorFlagBalancePremiumTransaction
	-- OR 	LTRIM(RTRIM(lkp_RenewalPolicyNumber))	!=	RenewalPolicyNumber
	-- OR 	LTRIM(RTRIM(lkp_RenewalPolicySymbol))	!=	RenewalPolicySymbol
	-- OR 	lkp_RenewalPolicyMod	!=	RenewalPolicyMod
	-- OR 	lkp_BillingType	!=	BillingType
	-- OR   	lkp_sup_bus_class_code_id	!=	v_sup_bus_class_code_id
	-- OR 	lkp_sup_pol_term_id	!=	v_sup_pol_term_id
	-- OR 	lkp_sup_pol_status_code_id	!=	v_sup_pol_status_code_id
	-- OR 	lkp_sup_pol_issue_code_id	!=	v_sup_pol_issue_code_id
	-- OR 	lkp_sup_pol_audit_frqncy_id	!=	v_sup_pol_audit_frqncy_id
	-- OR 	lkp_sup_industry_risk_grade_code_id	!=	v_sup_industry_risk_grade_code_id
	-- OR 	lkp_sup_state_id	!=	v_sup_state_id
	-- OR 	lkp_PolicyOfferingAKId	!=	in_PolicyOfferingAKId
	-- OR      lkp_producer_code_id!=producer_code_id
	-- OR      lkp_SurchargeExemptCode!=SurchargeExemptCode
	-- OR      lkp_SupSurchargeExemptID!=v_SupSurchargeExemptId 
	-- OR      lkp_AssociationCode!=v_Association
	-- OR      lkp_StrategicProfitCenterAKId!=in_StrategicProfitCenterAKId OR
	-- lkp_InsuranceSegmentAKId !=in_InsuranceSegmentAKId  OR 
	-- lkp_ProgramAKId != v_ProgramAKId OR lkp_ObligeeName  != ObligeeName OR lkp_AutomatedUnderwritingServicesIndicator  != AutomatedUnderwritingServicesIndicator OR v_lkp_AutomaticRenewalIndicator != AutomaticRenewalIndicator OR v_lkp_RolloverPolicyIndicator!=IsRollover OR
	-- v_lkp_RolloverPriorCarrier!=RolloverPriorCarrier OR 
	-- v_lkp_MailPolicyToInsured != MailPolicyToInsured OR
	-- AgencyEmployeeAKId != lkp_AgencyEmployeeAKId OR
	-- v_lkp_PolicyIssueCodeOverride != in_PolicyIssueCodeOverride)),'NEW',
	--  'NOCHANGE'
	--   )
	DECODE(TRUE,
		pol_key = v_prev_pol_key AND v_HashKey = v_prev_HashKey, 'NOCHANGE',
		lkp_pol_ak_id IS NULL, 'NEW',
		in_CreatedDate = lkp_eff_from_date, 'UPDATE',
		( ( pol_key = v_prev_pol_key AND v_HashKey <> v_prev_HashKey ) OR ( lkp_contract_cust_ak_id != v_contract_cust_ak_id OR lkp_AgencyAKID != v_AgencyAKID OR lkp_agency_ak_id != agency_ak_id OR lkp_mco != mco OR lkp_pol_co_num != pol_co_num OR lkp_pol_eff_date != pol_eff_date OR lkp_pol_exp_date != pol_exp_date OR lkp_orig_incptn_date != orig_incptn_date OR lkp_prim_bus_class_code != prim_bus_class_code OR lkp_reins_code != reins_code OR lkp_pms_pol_lob_code != pms_pol_lob_code OR lkp_pol_co_line_code != pol_co_line_code OR lkp_pol_cancellation_ind != pol_cancellation_ind OR lkp_pol_cancellation_date != pol_cancellation_date OR lkp_pol_cancellation_rsn_code != pol_cancellation_rsn_code OR lkp_state_of_domicile_code != state_of_domicile_code OR lkp_wbconnect_upload_code != wbconnect_upload_code OR lkp_serv_center_support_code != serv_center_support_code OR lkp_pol_term != pol_term OR lkp_terrorism_risk_ind != terrorism_risk_ind OR lkp_prior_pol_key != Prior_Policy_key OR lkp_pol_status_code != pol_status_code OR lkp_pol_issue_code != pol_issue_code OR lkp_pol_age != pol_age OR lkp_industry_risk_grade_code != industry_risk_grade_code OR lkp_uw_review_yr != uw_review_yr OR lkp_mvr_request_code != mvr_request_code OR lkp_renl_code != renl_code OR lkp_amend_num != amend_num OR lkp_anniversary_rerate_code != anniversary_rerate_code OR lkp_pol_audit_frqncy != pol_audit_frqncy OR lkp_final_audit_code != final_audit_code OR lkp_zip_ind != zip_ind OR lkp_guarantee_ind != guarantee_ind OR lkp_variation_code != variation_code OR lkp_county != county OR lkp_non_smoker_disc_code != non_smoker_disc_code OR lkp_renl_disc != renl_disc OR lkp_renl_safe_driver_disc_count != renl_safe_driver_disc_count OR lkp_nonrenewal_flag_date != nonrenewal_flag_date OR lkp_audit_complt_date != audit_complt_date OR lkp_orig_acct_date != orig_acct_date OR lkp_pol_enter_date != pol_enter_date OR lkp_excess_claim_code != excess_claim_code OR lkp_pol_status_on_pif != pol_status_on_pif OR lkp_target_mrkt_code != target_mrkt_code OR lkp_pkg_code != pkg_code OR lkp_pol_kind_code != pol_kind_code OR lkp_bus_seg_code != bus_seg_code OR lkp_pif_upload_audit_ind != pif_upload_audit_ind OR lkp_err_flag_bal_txn != err_flag_bal_txn OR lkp_err_flag_bal_reins != err_flag_bal_reins OR lkp_producer_code_ak_id != v_producer_code_ak_id OR lkp_prdcr_code != prdcr_code OR lkp_ClassOfBusiness != ClassOfBusiness OR lkp_strtgc_bus_dvsn_ak_id != v_strtgc_bus_dvsn_ak_id OR lkp_ErrorFlagBalancePremiumTransaction != ErrorFlagBalancePremiumTransaction OR LTRIM(RTRIM(lkp_RenewalPolicyNumber)) != RenewalPolicyNumber OR LTRIM(RTRIM(lkp_RenewalPolicySymbol)) != RenewalPolicySymbol OR lkp_RenewalPolicyMod != RenewalPolicyMod OR lkp_BillingType != BillingType OR lkp_sup_bus_class_code_id != v_sup_bus_class_code_id OR lkp_sup_pol_term_id != v_sup_pol_term_id OR lkp_sup_pol_status_code_id != v_sup_pol_status_code_id OR lkp_sup_pol_issue_code_id != v_sup_pol_issue_code_id OR lkp_sup_pol_audit_frqncy_id != v_sup_pol_audit_frqncy_id OR lkp_sup_industry_risk_grade_code_id != v_sup_industry_risk_grade_code_id OR lkp_sup_state_id != v_sup_state_id OR lkp_PolicyOfferingAKId != in_PolicyOfferingAKId OR lkp_producer_code_id != producer_code_id OR lkp_SurchargeExemptCode != SurchargeExemptCode OR lkp_SupSurchargeExemptID != v_SupSurchargeExemptId OR lkp_AssociationCode != v_Association OR lkp_StrategicProfitCenterAKId != in_StrategicProfitCenterAKId OR lkp_InsuranceSegmentAKId != in_InsuranceSegmentAKId OR lkp_ProgramAKId != v_ProgramAKId OR lkp_ObligeeName != ObligeeName OR lkp_AutomatedUnderwritingServicesIndicator != AutomatedUnderwritingServicesIndicator OR v_lkp_AutomaticRenewalIndicator != AutomaticRenewalIndicator OR v_lkp_RolloverPolicyIndicator != IsRollover OR v_lkp_RolloverPriorCarrier != RolloverPriorCarrier OR v_lkp_MailPolicyToInsured != MailPolicyToInsured OR AgencyEmployeeAKId != lkp_AgencyEmployeeAKId OR v_lkp_PolicyIssueCodeOverride != in_PolicyIssueCodeOverride ) ), 'NEW',
		'NOCHANGE') AS v_changed_flag,
	pol_key AS v_prev_pol_key,
	v_HashKey AS v_prev_HashKey,
	v_producer_code_ak_id AS out_producer_code_ak_id,
	v_sup_bus_class_code_id AS out_sup_bus_class_code_id,
	v_strtgc_bus_dvsn_ak_id AS out_strtgc_bus_dvsn_ak_id,
	1 AS out_crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS out_audit_id,
	-- *INF*: --Fix for Defect 3111 - Duck Creek not following eff_from_date time format in v2.policy.
	-- in_CreatedDate
	in_CreatedDate AS out_eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS out_eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS out_source_sys_id,
	SYSDATE AS out_created_date,
	SYSDATE AS out_modified_date,
	v_sup_pol_status_code_id AS out_sup_pol_status_code_id,
	v_sup_pol_issue_code_id AS out_sup_pol_issue_code_id,
	v_sup_pol_audit_frqncy_id AS out_sup_pol_audit_frqncy_id,
	v_sup_industry_risk_grade_code_id AS out_sup_industry_risk_grade_code_id,
	v_sup_state_id AS out_sup_state_id,
	in_PolicyOfferingAKId AS out_PolicyOfferingAKId,
	v_changed_flag AS out_changed_flag,
	v_contract_cust_ak_id AS out_contract_cust_ak_id,
	v_AgencyAKID AS out_AgencyAKID,
	v_sup_pol_term_id AS out_sup_pol_term_id,
	in_StrategicProfitCenterAKId AS out_StrategicProfitCenterAKId,
	in_InsuranceSegmentAKId AS out_InsuranceSegmentAKId,
	v_ProgramAKId AS out_ProgramAKId,
	v_SupSurchargeExemptId AS out_SupSurchargeExemptId1,
	-- *INF*: IIF(ISNULL(in_AssociationDiscountFactor), 0, in_AssociationDiscountFactor)
	IFF(in_AssociationDiscountFactor IS NULL, 0, in_AssociationDiscountFactor) AS AssociationDiscountFactor,
	v_Association AS Association,
	in_PolicyIssueCodeOverride AS out_PolicyIssueCodeOverride,
	0 AS DCBillFlag,
	'N/A' AS IssuedUWID,
	'N/A' AS IssuedUnderwriter
	FROM EXP_values
	LEFT JOIN LKP_Association
	ON LKP_Association.AssociationCode = EXP_values.out_Association
	LEFT JOIN LKP_Program
	ON LKP_Program.ProgramCode = EXP_values.out_ProgramCode
	LEFT JOIN LKP_SupSurchargeExempt
	ON LKP_SupSurchargeExempt.SurchargeExemptCode = EXP_values.out_SurchargeExemptCode
	LEFT JOIN LKP_contract_customer_key
	ON LKP_contract_customer_key.contract_key = EXP_values.out_policy_key
	LEFT JOIN LKP_policy
	ON LKP_policy.pol_key = EXP_Src_DataCollect.o_Policykey AND LKP_policy.eff_from_date <= EXP_Src_DataCollect.TransactionCreatedDate AND LKP_policy.eff_to_date >= EXP_Src_DataCollect.TransactionCreatedDate
	LEFT JOIN LKP_producer_code
	ON LKP_producer_code.agency_key = EXP_values.out_Reference AND LKP_producer_code.producer_code = EXP_values.out_ProducerCode
	LEFT JOIN LKP_sup_business_classification_code
	ON LKP_sup_business_classification_code.bus_class_code = EXP_values.out_prim_bus_class_code
	LEFT JOIN LKP_sup_industry_risk_grade_code
	ON LKP_sup_industry_risk_grade_code.industry_risk_grade_code = EXP_values.out_industry_risk_grade_code
	LEFT JOIN LKP_sup_policy_audit_frequency
	ON LKP_sup_policy_audit_frequency.pol_audit_frqncy_descript = EXP_values.out_pol_audit_frqncy
	LEFT JOIN LKP_sup_policy_issue_code
	ON LKP_sup_policy_issue_code.pol_issue_code = EXP_values.PolicyIssueCode
	LEFT JOIN LKP_sup_policy_status_code
	ON LKP_sup_policy_status_code.pol_status_code = EXP_values.PolicyStatusCode
	LEFT JOIN LKP_sup_policy_term
	ON LKP_sup_policy_term.pol_term = EXP_values.out_pol_term
	LEFT JOIN LKP_sup_state
	ON LKP_sup_state.state_code = EXP_values.out_state_code
	LEFT JOIN LKP_POLICY_CONTRACT_CUSTOMER LKP_POLICY_CONTRACT_CUSTOMER_pol_key
	ON LKP_POLICY_CONTRACT_CUSTOMER_pol_key.Pol_key = pol_key

	LEFT JOIN LKP_STRATEGIC_BUSINESS_DIVISION LKP_STRATEGIC_BUSINESS_DIVISION_in_strtgc_bus_dvsn_code
	ON LKP_STRATEGIC_BUSINESS_DIVISION_in_strtgc_bus_dvsn_code.strtgc_bus_dvsn_code = in_strtgc_bus_dvsn_code

),
RTR_INSERT_UPDATE AS (
	SELECT
	lkp_pol_id,
	lkp_pol_ak_id,
	out_changed_flag AS changed_flag,
	out_crrnt_snpsht_flag AS crrnt_snpsht_flag,
	out_audit_id AS audit_id,
	out_eff_from_date AS eff_from_date,
	out_eff_to_date AS eff_to_date,
	out_source_sys_id AS source_sys_id,
	out_created_date AS created_date,
	out_modified_date AS modified_date,
	out_contract_cust_ak_id AS contract_cust_ak_id,
	agency_ak_id,
	out_AgencyAKID AS AgencyAKID,
	pol_sym,
	PolicyNumber,
	PolicyVersion,
	pol_key AS policy_key,
	mco,
	pol_co_num,
	pol_eff_date AS EffectiveDate,
	pol_exp_date AS ExpirationDate,
	orig_incptn_date AS InceptionDate,
	prim_bus_class_code,
	reins_code,
	pms_pol_lob_code,
	pol_co_line_code,
	pol_cancellation_ind,
	pol_cancellation_date,
	pol_cancellation_rsn_code,
	state_of_domicile_code,
	wbconnect_upload_code,
	serv_center_support_code,
	pol_term,
	terrorism_risk_ind,
	pol_status_code,
	pol_issue_code,
	pol_age,
	industry_risk_grade_code,
	uw_review_yr,
	mvr_request_code,
	renl_code,
	amend_num,
	anniversary_rerate_code,
	pol_audit_frqncy,
	final_audit_code,
	zip_ind,
	guarantee_ind,
	variation_code,
	county,
	non_smoker_disc_code,
	renl_disc,
	renl_safe_driver_disc_count,
	nonrenewal_flag_date,
	audit_complt_date,
	orig_acct_date,
	pol_enter_date AS TransactionDateTime,
	excess_claim_code,
	pol_status_on_pif,
	target_mrkt_code,
	pkg_code,
	pol_kind_code,
	bus_seg_code,
	pif_upload_audit_ind,
	err_flag_bal_txn,
	err_flag_bal_reins,
	out_producer_code_ak_id AS producer_code_ak_id,
	prdcr_code,
	ClassOfBusiness,
	out_strtgc_bus_dvsn_ak_id AS strtgc_bus_dvsn_ak_id,
	ErrorFlagBalancePremiumTransaction,
	RenewalPolicyNumber,
	RenewalPolicySymbol,
	RenewalPolicyMod,
	BillingType,
	producer_code_id,
	out_sup_bus_class_code_id AS sup_bus_class_code_id,
	out_sup_pol_term_id AS sup_pol_term_id,
	out_sup_pol_status_code_id AS sup_pol_status_code_id,
	out_sup_pol_issue_code_id AS sup_pol_issue_code_id,
	out_sup_pol_audit_frqncy_id AS sup_pol_audit_frqncy_id,
	out_sup_industry_risk_grade_code_id AS sup_industry_risk_grade_code_id,
	out_sup_state_id AS sup_state_id,
	SurchargeExemptCode,
	SupSurchargeExemptId,
	out_StrategicProfitCenterAKId AS StrategicProfitCenterAKId,
	out_InsuranceSegmentAKId AS InsuranceSegmentAKId,
	out_PolicyOfferingAKId AS PolicyOfferingAKId,
	out_ProgramAKId AS ProgramAKId,
	ObligeeName,
	AutomatedUnderwritingServicesIndicator,
	AutomaticRenewalIndicator,
	AssociationDiscountFactor,
	Association,
	Prior_Policy_key,
	IsRollover,
	RolloverPriorCarrier,
	MailPolicyToInsured,
	AgencyEmployeeAKId AS AgencyEmployeeAKID,
	out_PolicyIssueCodeOverride AS PolicyIssueCodeOverride,
	DCBillFlag,
	IssuedUWID,
	IssuedUnderwriter
	FROM EXP_Detect_Changes
),
RTR_INSERT_UPDATE_Insert AS (SELECT * FROM RTR_INSERT_UPDATE WHERE changed_flag='NEW'),
RTR_INSERT_UPDATE_Update AS (SELECT * FROM RTR_INSERT_UPDATE WHERE changed_flag='UPDATE'),
UPD_CodeChange AS (
	SELECT
	lkp_pol_id AS pol_id, 
	modified_date, 
	agency_ak_id, 
	mco, 
	pol_co_num, 
	EffectiveDate AS pol_eff_date, 
	ExpirationDate AS pol_exp_date, 
	InceptionDate AS orig_incptn_date, 
	prim_bus_class_code, 
	reins_code, 
	pms_pol_lob_code, 
	pol_co_line_code, 
	pol_cancellation_ind, 
	pol_cancellation_date, 
	pol_cancellation_rsn_code, 
	state_of_domicile_code, 
	wbconnect_upload_code, 
	serv_center_support_code, 
	pol_term, 
	terrorism_risk_ind, 
	Prior_Policy_key AS prior_pol_key, 
	pol_status_code, 
	pol_issue_code, 
	pol_age, 
	industry_risk_grade_code, 
	uw_review_yr, 
	mvr_request_code, 
	renl_code, 
	amend_num, 
	anniversary_rerate_code, 
	pol_audit_frqncy, 
	final_audit_code, 
	zip_ind, 
	guarantee_ind, 
	variation_code, 
	county, 
	non_smoker_disc_code, 
	renl_disc, 
	renl_safe_driver_disc_count, 
	nonrenewal_flag_date, 
	audit_complt_date, 
	orig_acct_date, 
	TransactionDateTime AS pol_enter_date, 
	excess_claim_code, 
	pol_status_on_pif, 
	target_mrkt_code, 
	pkg_code, 
	pol_kind_code, 
	bus_seg_code, 
	pif_upload_audit_ind, 
	err_flag_bal_txn, 
	err_flag_bal_reins, 
	producer_code_ak_id, 
	prdcr_code, 
	ClassOfBusiness, 
	strtgc_bus_dvsn_ak_id, 
	ErrorFlagBalancePremiumTransaction, 
	RenewalPolicyNumber, 
	RenewalPolicySymbol, 
	RenewalPolicyMod, 
	BillingType, 
	producer_code_id, 
	sup_bus_class_code_id, 
	sup_pol_term_id, 
	sup_pol_status_code_id, 
	sup_pol_issue_code_id, 
	sup_pol_audit_frqncy_id, 
	sup_industry_risk_grade_code_id, 
	sup_state_id, 
	SurchargeExemptCode, 
	SupSurchargeExemptId AS SupSurchargeExemptID, 
	StrategicProfitCenterAKId, 
	InsuranceSegmentAKId, 
	PolicyOfferingAKId, 
	ProgramAKId, 
	AgencyAKID AS AgencyAKId, 
	ObligeeName, 
	AutomatedUnderwritingServicesIndicator, 
	AutomaticRenewalIndicator, 
	Association AS AssociationCode, 
	IsRollover, 
	RolloverPriorCarrier, 
	MailPolicyToInsured, 
	AgencyEmployeeAKID, 
	PolicyIssueCodeOverride AS PolicyIssueCodeOverride3, 
	IssuedUWID, 
	IssuedUnderwriter
	FROM RTR_INSERT_UPDATE_Update
),
TGT_Policy_Update_SameTransactionCreatedDate AS (

	------------ PRE SQL ----------
	exec [spSetIndexStatus] @Enable = 0, @Schema = 'V2', @TableName = 'policy', @IndexWildcard = 'Ak3Policy'
	-------------------------------


	MERGE INTO v2.policy AS T
	USING UPD_CodeChange AS S
	ON T.pol_id = S.pol_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.modified_date = S.modified_date, T.agency_ak_id = S.agency_ak_id, T.mco = S.mco, T.pol_co_num = S.pol_co_num, T.pol_eff_date = S.pol_eff_date, T.pol_exp_date = S.pol_exp_date, T.orig_incptn_date = S.orig_incptn_date, T.prim_bus_class_code = S.prim_bus_class_code, T.reins_code = S.reins_code, T.pms_pol_lob_code = S.pms_pol_lob_code, T.pol_co_line_code = S.pol_co_line_code, T.pol_cancellation_ind = S.pol_cancellation_ind, T.pol_cancellation_date = S.pol_cancellation_date, T.pol_cancellation_rsn_code = S.pol_cancellation_rsn_code, T.state_of_domicile_code = S.state_of_domicile_code, T.wbconnect_upload_code = S.wbconnect_upload_code, T.serv_center_support_code = S.serv_center_support_code, T.pol_term = S.pol_term, T.terrorism_risk_ind = S.terrorism_risk_ind, T.prior_pol_key = S.prior_pol_key, T.pol_status_code = S.pol_status_code, T.pol_issue_code = S.pol_issue_code, T.pol_age = S.pol_age, T.industry_risk_grade_code = S.industry_risk_grade_code, T.uw_review_yr = S.uw_review_yr, T.mvr_request_code = S.mvr_request_code, T.renl_code = S.renl_code, T.amend_num = S.amend_num, T.anniversary_rerate_code = S.anniversary_rerate_code, T.pol_audit_frqncy = S.pol_audit_frqncy, T.final_audit_code = S.final_audit_code, T.zip_ind = S.zip_ind, T.guarantee_ind = S.guarantee_ind, T.variation_code = S.variation_code, T.county = S.county, T.non_smoker_disc_code = S.non_smoker_disc_code, T.renl_disc = S.renl_disc, T.renl_safe_driver_disc_count = S.renl_safe_driver_disc_count, T.nonrenewal_flag_date = S.nonrenewal_flag_date, T.audit_complt_date = S.audit_complt_date, T.orig_acct_date = S.orig_acct_date, T.pol_enter_date = S.pol_enter_date, T.excess_claim_code = S.excess_claim_code, T.pol_status_on_pif = S.pol_status_on_pif, T.target_mrkt_code = S.target_mrkt_code, T.pkg_code = S.pkg_code, T.pol_kind_code = S.pol_kind_code, T.bus_seg_code = S.bus_seg_code, T.pif_upload_audit_ind = S.pif_upload_audit_ind, T.err_flag_bal_txn = S.err_flag_bal_txn, T.err_flag_bal_reins = S.err_flag_bal_reins, T.producer_code_ak_id = S.producer_code_ak_id, T.prdcr_code = S.prdcr_code, T.ClassOfBusiness = S.ClassOfBusiness, T.strtgc_bus_dvsn_ak_id = S.strtgc_bus_dvsn_ak_id, T.ErrorFlagBalancePremiumTransaction = S.ErrorFlagBalancePremiumTransaction, T.RenewalPolicyNumber = S.RenewalPolicyNumber, T.RenewalPolicySymbol = S.RenewalPolicySymbol, T.RenewalPolicyMod = S.RenewalPolicyMod, T.BillingType = S.BillingType, T.producer_code_id = S.producer_code_id, T.sup_bus_class_code_id = S.sup_bus_class_code_id, T.sup_pol_term_id = S.sup_pol_term_id, T.sup_pol_status_code_id = S.sup_pol_status_code_id, T.sup_pol_issue_code_id = S.sup_pol_issue_code_id, T.sup_pol_audit_frqncy_id = S.sup_pol_audit_frqncy_id, T.sup_industry_risk_grade_code_id = S.sup_industry_risk_grade_code_id, T.sup_state_id = S.sup_state_id, T.SurchargeExemptCode = S.SurchargeExemptCode, T.SupSurchargeExemptID = S.SupSurchargeExemptID, T.StrategicProfitCenterAKId = S.StrategicProfitCenterAKId, T.InsuranceSegmentAKId = S.InsuranceSegmentAKId, T.PolicyOfferingAKId = S.PolicyOfferingAKId, T.ProgramAKId = S.ProgramAKId, T.AgencyAKId = S.AgencyAKId, T.ObligeeName = S.ObligeeName, T.AutomatedUnderwritingServicesIndicator = S.AutomatedUnderwritingServicesIndicator, T.AutomaticRenewalIndicator = S.AutomaticRenewalIndicator, T.AssociationCode = S.AssociationCode, T.RolloverPolicyIndicator = S.IsRollover, T.RolloverPriorCarrier = S.RolloverPriorCarrier, T.MailToInsuredFlag = S.MailPolicyToInsured, T.AgencyEmployeeAKId = S.AgencyEmployeeAKID, T.PolicyIssueCodeOverride = S.PolicyIssueCodeOverride3, T.IssuedUWID = S.IssuedUWID, T.IssuedUnderwriter = S.IssuedUnderwriter
),
SEQ_policy_cus_ak_id AS (
	CREATE SEQUENCE SEQ_policy_cus_ak_id
	START = 0
	INCREMENT = 1;
),
SRT_SetRecordOrderForAkid AS (
	SELECT
	lkp_pol_ak_id, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date, 
	contract_cust_ak_id, 
	agency_ak_id, 
	AgencyAKID, 
	pol_sym, 
	PolicyNumber, 
	PolicyVersion, 
	policy_key, 
	mco, 
	pol_co_num, 
	EffectiveDate, 
	ExpirationDate, 
	InceptionDate, 
	prim_bus_class_code, 
	reins_code, 
	pms_pol_lob_code, 
	pol_co_line_code, 
	pol_cancellation_ind, 
	pol_cancellation_date, 
	pol_cancellation_rsn_code, 
	state_of_domicile_code, 
	wbconnect_upload_code, 
	serv_center_support_code, 
	pol_term, 
	terrorism_risk_ind, 
	pol_status_code, 
	pol_issue_code, 
	pol_age, 
	industry_risk_grade_code, 
	uw_review_yr, 
	mvr_request_code, 
	renl_code, 
	amend_num, 
	anniversary_rerate_code, 
	pol_audit_frqncy, 
	final_audit_code, 
	zip_ind, 
	guarantee_ind, 
	variation_code, 
	county, 
	non_smoker_disc_code, 
	renl_disc, 
	renl_safe_driver_disc_count, 
	nonrenewal_flag_date, 
	audit_complt_date, 
	orig_acct_date, 
	TransactionDateTime, 
	excess_claim_code, 
	pol_status_on_pif, 
	target_mrkt_code, 
	pkg_code, 
	pol_kind_code, 
	bus_seg_code, 
	pif_upload_audit_ind, 
	err_flag_bal_txn, 
	err_flag_bal_reins, 
	producer_code_ak_id, 
	prdcr_code, 
	ClassOfBusiness, 
	strtgc_bus_dvsn_ak_id, 
	ErrorFlagBalancePremiumTransaction, 
	RenewalPolicyNumber, 
	RenewalPolicySymbol, 
	RenewalPolicyMod, 
	BillingType, 
	producer_code_id, 
	sup_bus_class_code_id, 
	sup_pol_term_id, 
	sup_pol_status_code_id, 
	sup_pol_issue_code_id, 
	sup_pol_audit_frqncy_id, 
	sup_industry_risk_grade_code_id, 
	sup_state_id, 
	SurchargeExemptCode, 
	SupSurchargeExemptId, 
	StrategicProfitCenterAKId, 
	InsuranceSegmentAKId, 
	PolicyOfferingAKId, 
	ProgramAKId, 
	ObligeeName, 
	AutomatedUnderwritingServicesIndicator, 
	AutomaticRenewalIndicator, 
	Association, 
	Prior_Policy_key, 
	IsRollover, 
	RolloverPriorCarrier, 
	MailPolicyToInsured, 
	AgencyEmployeeAKID, 
	PolicyIssueCodeOverride, 
	DCBillFlag, 
	IssuedUWID, 
	IssuedUnderwriter
	FROM RTR_INSERT_UPDATE_Insert
	ORDER BY policy_key ASC
),
EXP_policy_ak_id AS (
	SELECT
	SEQ_policy_cus_ak_id.NEXTVAL AS in_NEXTVAL,
	lkp_pol_ak_id,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	policy_key,
	-- *INF*: DECODE(TRUE,
	-- policy_key=v_prev_policy_key, v_NEXTVAL,
	-- ISNULL(lkp_pol_ak_id), in_NEXTVAL,
	-- lkp_pol_ak_id
	-- )
	DECODE(TRUE,
		policy_key = v_prev_policy_key, v_NEXTVAL,
		lkp_pol_ak_id IS NULL, in_NEXTVAL,
		lkp_pol_ak_id) AS v_NEXTVAL,
	policy_key AS v_prev_policy_key,
	modified_date,
	v_NEXTVAL AS out_pol_ak_id,
	contract_cust_ak_id,
	agency_ak_id,
	AgencyAKID,
	pol_sym,
	PolicyNumber,
	PolicyVersion,
	mco,
	pol_co_num,
	EffectiveDate,
	ExpirationDate,
	InceptionDate,
	prim_bus_class_code,
	reins_code,
	pms_pol_lob_code,
	pol_co_line_code,
	pol_cancellation_ind,
	pol_cancellation_date,
	pol_cancellation_rsn_code,
	state_of_domicile_code,
	wbconnect_upload_code,
	serv_center_support_code,
	pol_term,
	terrorism_risk_ind,
	pol_status_code,
	pol_issue_code,
	pol_age,
	industry_risk_grade_code,
	uw_review_yr,
	mvr_request_code,
	renl_code,
	amend_num,
	anniversary_rerate_code,
	pol_audit_frqncy,
	final_audit_code,
	zip_ind,
	guarantee_ind,
	variation_code,
	county,
	non_smoker_disc_code,
	renl_disc,
	renl_safe_driver_disc_count,
	nonrenewal_flag_date,
	audit_complt_date,
	orig_acct_date,
	TransactionDateTime,
	excess_claim_code,
	pol_status_on_pif,
	target_mrkt_code,
	pkg_code,
	pol_kind_code,
	bus_seg_code,
	pif_upload_audit_ind,
	err_flag_bal_txn,
	err_flag_bal_reins,
	producer_code_ak_id,
	prdcr_code,
	ClassOfBusiness,
	strtgc_bus_dvsn_ak_id,
	ErrorFlagBalancePremiumTransaction,
	RenewalPolicyNumber,
	RenewalPolicySymbol,
	RenewalPolicyMod,
	BillingType,
	producer_code_id,
	sup_bus_class_code_id,
	sup_pol_term_id,
	sup_pol_status_code_id,
	sup_pol_issue_code_id,
	sup_pol_audit_frqncy_id,
	sup_industry_risk_grade_code_id,
	sup_state_id,
	SurchargeExemptCode,
	SupSurchargeExemptId,
	StrategicProfitCenterAKId,
	InsuranceSegmentAKId,
	PolicyOfferingAKId,
	ProgramAKId,
	-1 AS o_UnderwritingAssociateAKId,
	ObligeeName,
	AutomatedUnderwritingServicesIndicator,
	AutomaticRenewalIndicator,
	Association,
	Prior_Policy_key,
	IsRollover,
	RolloverPriorCarrier,
	MailPolicyToInsured,
	AgencyEmployeeAKID,
	PolicyIssueCodeOverride AS PolicyIssueCodeOverride1,
	DCBillFlag,
	IssuedUWID,
	IssuedUnderwriter
	FROM SRT_SetRecordOrderForAkid
),
TGT_Policy_INSERT AS (

	------------ PRE SQL ----------
	exec [spSetIndexStatus] @Enable = 0, @Schema = 'V2', @TableName = 'policy', @IndexWildcard = 'Ak3Policy'
	-------------------------------


	INSERT INTO v2.policy
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, pol_ak_id, contract_cust_ak_id, agency_ak_id, pol_sym, pol_num, pol_mod, pol_key, mco, pol_co_num, pol_eff_date, pol_exp_date, orig_incptn_date, prim_bus_class_code, reins_code, pms_pol_lob_code, pol_co_line_code, pol_cancellation_ind, pol_cancellation_date, pol_cancellation_rsn_code, state_of_domicile_code, wbconnect_upload_code, serv_center_support_code, pol_term, terrorism_risk_ind, prior_pol_key, pol_status_code, pol_issue_code, pol_age, industry_risk_grade_code, uw_review_yr, mvr_request_code, renl_code, amend_num, anniversary_rerate_code, pol_audit_frqncy, final_audit_code, zip_ind, guarantee_ind, variation_code, county, non_smoker_disc_code, renl_disc, renl_safe_driver_disc_count, nonrenewal_flag_date, audit_complt_date, orig_acct_date, pol_enter_date, excess_claim_code, pol_status_on_pif, target_mrkt_code, pkg_code, pol_kind_code, bus_seg_code, pif_upload_audit_ind, err_flag_bal_txn, err_flag_bal_reins, producer_code_ak_id, prdcr_code, ClassOfBusiness, strtgc_bus_dvsn_ak_id, ErrorFlagBalancePremiumTransaction, RenewalPolicyNumber, RenewalPolicySymbol, RenewalPolicyMod, BillingType, producer_code_id, sup_bus_class_code_id, sup_pol_term_id, sup_pol_status_code_id, sup_pol_issue_code_id, sup_pol_audit_frqncy_id, sup_industry_risk_grade_code_id, sup_state_id, SurchargeExemptCode, SupSurchargeExemptID, StrategicProfitCenterAKId, InsuranceSegmentAKId, PolicyOfferingAKId, ProgramAKId, AgencyAKId, UnderwritingAssociateAKId, ObligeeName, AutomatedUnderwritingServicesIndicator, AutomaticRenewalIndicator, AssociationCode, RolloverPolicyIndicator, RolloverPriorCarrier, MailToInsuredFlag, AgencyEmployeeAKId, PolicyIssueCodeOverride, DCBillFlag, IssuedUWID, IssuedUnderwriter)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	out_pol_ak_id AS POL_AK_ID, 
	CONTRACT_CUST_AK_ID, 
	AGENCY_AK_ID, 
	POL_SYM, 
	PolicyNumber AS POL_NUM, 
	PolicyVersion AS POL_MOD, 
	policy_key AS POL_KEY, 
	MCO, 
	POL_CO_NUM, 
	EffectiveDate AS POL_EFF_DATE, 
	ExpirationDate AS POL_EXP_DATE, 
	InceptionDate AS ORIG_INCPTN_DATE, 
	PRIM_BUS_CLASS_CODE, 
	REINS_CODE, 
	PMS_POL_LOB_CODE, 
	POL_CO_LINE_CODE, 
	POL_CANCELLATION_IND, 
	POL_CANCELLATION_DATE, 
	POL_CANCELLATION_RSN_CODE, 
	STATE_OF_DOMICILE_CODE, 
	WBCONNECT_UPLOAD_CODE, 
	SERV_CENTER_SUPPORT_CODE, 
	POL_TERM, 
	TERRORISM_RISK_IND, 
	Prior_Policy_key AS PRIOR_POL_KEY, 
	POL_STATUS_CODE, 
	POL_ISSUE_CODE, 
	POL_AGE, 
	INDUSTRY_RISK_GRADE_CODE, 
	UW_REVIEW_YR, 
	MVR_REQUEST_CODE, 
	RENL_CODE, 
	AMEND_NUM, 
	ANNIVERSARY_RERATE_CODE, 
	POL_AUDIT_FRQNCY, 
	FINAL_AUDIT_CODE, 
	ZIP_IND, 
	GUARANTEE_IND, 
	VARIATION_CODE, 
	COUNTY, 
	NON_SMOKER_DISC_CODE, 
	RENL_DISC, 
	RENL_SAFE_DRIVER_DISC_COUNT, 
	NONRENEWAL_FLAG_DATE, 
	AUDIT_COMPLT_DATE, 
	ORIG_ACCT_DATE, 
	TransactionDateTime AS POL_ENTER_DATE, 
	EXCESS_CLAIM_CODE, 
	POL_STATUS_ON_PIF, 
	TARGET_MRKT_CODE, 
	PKG_CODE, 
	POL_KIND_CODE, 
	BUS_SEG_CODE, 
	PIF_UPLOAD_AUDIT_IND, 
	ERR_FLAG_BAL_TXN, 
	ERR_FLAG_BAL_REINS, 
	PRODUCER_CODE_AK_ID, 
	PRDCR_CODE, 
	CLASSOFBUSINESS, 
	STRTGC_BUS_DVSN_AK_ID, 
	ERRORFLAGBALANCEPREMIUMTRANSACTION, 
	RENEWALPOLICYNUMBER, 
	RENEWALPOLICYSYMBOL, 
	RENEWALPOLICYMOD, 
	BILLINGTYPE, 
	PRODUCER_CODE_ID, 
	SUP_BUS_CLASS_CODE_ID, 
	SUP_POL_TERM_ID, 
	SUP_POL_STATUS_CODE_ID, 
	SUP_POL_ISSUE_CODE_ID, 
	SUP_POL_AUDIT_FRQNCY_ID, 
	SUP_INDUSTRY_RISK_GRADE_CODE_ID, 
	SUP_STATE_ID, 
	SURCHARGEEXEMPTCODE, 
	SupSurchargeExemptId AS SUPSURCHARGEEXEMPTID, 
	STRATEGICPROFITCENTERAKID, 
	INSURANCESEGMENTAKID, 
	POLICYOFFERINGAKID, 
	PROGRAMAKID, 
	AgencyAKID AS AGENCYAKID, 
	o_UnderwritingAssociateAKId AS UNDERWRITINGASSOCIATEAKID, 
	OBLIGEENAME, 
	AUTOMATEDUNDERWRITINGSERVICESINDICATOR, 
	AUTOMATICRENEWALINDICATOR, 
	Association AS ASSOCIATIONCODE, 
	IsRollover AS ROLLOVERPOLICYINDICATOR, 
	ROLLOVERPRIORCARRIER, 
	MailPolicyToInsured AS MAILTOINSUREDFLAG, 
	AgencyEmployeeAKID AS AGENCYEMPLOYEEAKID, 
	PolicyIssueCodeOverride1 AS POLICYISSUECODEOVERRIDE, 
	DCBILLFLAG, 
	ISSUEDUWID, 
	ISSUEDUNDERWRITER
	FROM EXP_policy_ak_id
),
SQ_policy AS (
	SELECT 
		a.pol_id, 
		a.eff_from_date,
		a.eff_to_date, 
		a.pol_ak_id 
	FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy a
	WHERE EXISTS
		( SELECT pol_ak_id  FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy b
		WHERE CRRNT_SNPSHT_FLAG = 1 AND source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND b.pol_ak_id=a.pol_ak_id GROUP BY pol_ak_id HAVING count(*) > 1) 
	AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	ORDER BY a.pol_ak_id ,a.eff_from_date DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	pol_ak_id AS in_pol_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	pol_id,
	-- *INF*: DECODE(TRUE,
	-- in_pol_ak_id = v_prev_pol_ak_id ,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(TRUE,
		in_pol_ak_id = v_prev_pol_ak_id, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	in_pol_ak_id AS v_prev_pol_ak_id,
	eff_from_date AS v_prev_eff_from_date,
	0 AS out_crrnt_snpsht_flag,
	v_eff_to_date AS out_eff_to_date,
	SYSDATE AS out_modified_date
	FROM SQ_policy
),
FIL_FirstRowInAKGroup AS (
	SELECT
	orig_eff_to_date AS in_orig_eff_to_date, 
	pol_id, 
	out_crrnt_snpsht_flag AS crrnt_snpsht_flag, 
	out_eff_to_date AS eff_to_date, 
	out_modified_date AS modified_date
	FROM EXP_Lag_eff_from_date
	WHERE in_orig_eff_to_date != eff_to_date
),
EXPIRE_policy AS (
	SELECT
	pol_id, 
	crrnt_snpsht_flag, 
	eff_to_date, 
	modified_date
	FROM FIL_FirstRowInAKGroup
),
TGT_Policy_EXPIRE AS (

	------------ PRE SQL ----------
	UPDATE A
	SET A.eff_from_date='1800-1-1'
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy A
	WHERE NOT EXISTS (
	SELECT 1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy B
	WHERE A.pol_ak_id=B.pol_ak_id
	AND B.source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	and B.eff_from_date<A.eff_from_date)
	AND A.eff_from_date>'1800-1-1'
	AND A.source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	-------------------------------


	MERGE INTO v2.policy AS T
	USING EXPIRE_policy AS S
	ON T.pol_id = S.pol_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date

	------------ POST SQL ----------
	exec [spSetIndexStatus] @Enable = 1, @Schema = 'V2', @TableName = 'policy', @IndexWildcard = 'Ak3Policy'
	-------------------------------


),