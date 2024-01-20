WITH
LKP_Policy AS (
	SELECT
	pol_ak_id,
	pol_key
	FROM (
		SELECT A.pol_ak_id as pol_ak_id, A.pol_key as pol_key FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy A
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter B
		on A.StrategicProfitCenterAKId=B.StrategicProfitCenterAKId
		where A.source_sys_id='DCT'
		and A. crrnt_snpsht_flag=1
		and B.StrategicProfitCenterAbbreviation='WB - PL'
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id) = 1
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
SQ_policy AS (
	select A.Pol_id
	      ,9999 audit_id
	      ,getdate() modified_date
	      ,B.contract_cust_ak_id
	      ,B.agency_ak_id
	      ,B.mco
	      ,B.pol_co_num
	      ,B.pol_eff_date
	      ,B.pol_exp_date
	      ,B.orig_incptn_date
	      ,B.prim_bus_class_code
	      ,B.reins_code
	      ,B.pms_pol_lob_code
	      ,B.pol_co_line_code
	      ,B.pol_cancellation_ind
	      ,B.pol_cancellation_date
	      ,B.pol_cancellation_rsn_code
	      ,B.state_of_domicile_code
	      ,B.wbconnect_upload_code
	      ,B.serv_center_support_code
	      ,B.pol_term
	      ,B.terrorism_risk_ind
	      ,B.prior_pol_key
	      ,B.pol_status_code
	      ,B.pol_issue_code
	      ,B.pol_age
	      ,B.industry_risk_grade_code
	      ,B.uw_review_yr
	      ,B.mvr_request_code
	      ,B.renl_code
	      ,B.amend_num
	      ,B.anniversary_rerate_code
	      ,B.pol_audit_frqncy
	      ,B.final_audit_code
	      ,B.zip_ind
	      ,B.guarantee_ind
	      ,B.variation_code
	      ,B.county
	      ,B.non_smoker_disc_code
	      ,B.renl_disc
	      ,B.renl_safe_driver_disc_count
	      ,B.nonrenewal_flag_date
	      ,B.audit_complt_date
	      ,B.orig_acct_date
	      ,B.pol_enter_date
	      ,B.excess_claim_code
	      ,B.pol_status_on_pif
	      ,B.target_mrkt_code
	      ,B.pkg_code
	      ,B.pol_kind_code
	      ,B.bus_seg_code
	      ,B.pif_upload_audit_ind
	      ,B.err_flag_bal_txn
	      ,B.err_flag_bal_reins
	      ,B.producer_code_ak_id
	      ,B.prdcr_code
	      ,B.ClassOfBusiness
	      ,B.strtgc_bus_dvsn_ak_id
	      ,B.ErrorFlagBalancePremiumTransaction
	      ,B.RenewalPolicyNumber
	      ,B.RenewalPolicySymbol
	      ,B.RenewalPolicyMod
	      ,B.BillingType
	      ,B.producer_code_id
	      ,B.sup_bus_class_code_id
	      ,B.sup_pol_term_id
	      ,B.sup_pol_status_code_id
	      ,B.sup_pol_issue_code_id
	      ,B.sup_pol_audit_frqncy_id
	      ,B.sup_industry_risk_grade_code_id
	      ,B.sup_state_id
	      ,B.SurchargeExemptCode
	      ,B.SupSurchargeExemptID
	      ,B.StrategicProfitCenterAKId
	      ,B.InsuranceSegmentAKId
	      ,B.PolicyOfferingAKId
	      ,B.ProgramAKId
	      ,B.AgencyAKId
	      ,B.UnderwritingAssociateAKId
	      ,B.ObligeeName
	      ,B.AutomatedUnderwritingServicesIndicator
	      ,B.AutomaticRenewalIndicator
	      ,B.AssociationCode
	      ,B.RolloverPolicyIndicator
	      ,B.RolloverPriorCarrier
	      ,B.MailToInsuredFlag
	      ,B.AgencyEmployeeAKId
	      ,B.PolicyIssueCodeOverride
	      ,B.PoolCode
	      ,B.DCBillFlag
	from @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Policy B
	on A.pol_ak_id=B.pol_ak_id
	and A.audit_id=999
	and B.audit_id not in (999,9999)
	inner join (
	select C.Pol_ak_id,min(C.pol_id) Min_pol_id
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy A
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.StrategicProfitCenter B
	on A.StrategicProfitCenterAKId=B.StrategicProfitCenterAKId
	and A.audit_id=999
	and A.source_sys_id='DCT'
	and B.StrategicProfitCenterAbbreviation='WB - PL'
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy C
	on A.pol_ak_id=C.pol_ak_id
	and C.audit_id<>999
	and C.source_sys_id='DCT'
	group by C.Pol_ak_id) C
	on B.Pol_id=C.Min_pol_id
),
UPD_PolicyData AS (
	SELECT
	pol_id, 
	audit_id, 
	modified_date, 
	contract_cust_ak_id, 
	agency_ak_id, 
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
	producer_code_id, 
	sup_bus_class_code_id, 
	sup_pol_term_id, 
	sup_pol_status_code_id, 
	sup_pol_issue_code_id, 
	sup_pol_audit_frqncy_id, 
	sup_industry_risk_grade_code_id, 
	sup_state_id, 
	SurchargeExemptCode, 
	SupSurchargeExemptID, 
	StrategicProfitCenterAKId, 
	InsuranceSegmentAKId, 
	PolicyOfferingAKId, 
	ProgramAKId, 
	AgencyAKId, 
	UnderwritingAssociateAKId, 
	ObligeeName, 
	AutomatedUnderwritingServicesIndicator, 
	AutomaticRenewalIndicator, 
	AssociationCode, 
	RolloverPolicyIndicator, 
	RolloverPriorCarrier, 
	MailToInsuredFlag, 
	AgencyEmployeeAKId, 
	PolicyIssueCodeOverride, 
	DCBillFlag, 
	PoolCode
	FROM SQ_policy
),
policy_Update AS (
	MERGE INTO @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy AS T
	USING UPD_PolicyData AS S
	ON T.pol_id = S.pol_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.audit_id = S.audit_id, T.modified_date = S.modified_date, T.contract_cust_ak_id = S.contract_cust_ak_id, T.agency_ak_id = S.agency_ak_id, T.mco = S.mco, T.pol_co_num = S.pol_co_num, T.pol_eff_date = S.pol_eff_date, T.pol_exp_date = S.pol_exp_date, T.orig_incptn_date = S.orig_incptn_date, T.prim_bus_class_code = S.prim_bus_class_code, T.reins_code = S.reins_code, T.pms_pol_lob_code = S.pms_pol_lob_code, T.pol_co_line_code = S.pol_co_line_code, T.pol_cancellation_ind = S.pol_cancellation_ind, T.pol_cancellation_date = S.pol_cancellation_date, T.pol_cancellation_rsn_code = S.pol_cancellation_rsn_code, T.state_of_domicile_code = S.state_of_domicile_code, T.wbconnect_upload_code = S.wbconnect_upload_code, T.serv_center_support_code = S.serv_center_support_code, T.pol_term = S.pol_term, T.terrorism_risk_ind = S.terrorism_risk_ind, T.prior_pol_key = S.prior_pol_key, T.pol_status_code = S.pol_status_code, T.pol_issue_code = S.pol_issue_code, T.pol_age = S.pol_age, T.industry_risk_grade_code = S.industry_risk_grade_code, T.uw_review_yr = S.uw_review_yr, T.mvr_request_code = S.mvr_request_code, T.renl_code = S.renl_code, T.amend_num = S.amend_num, T.anniversary_rerate_code = S.anniversary_rerate_code, T.pol_audit_frqncy = S.pol_audit_frqncy, T.final_audit_code = S.final_audit_code, T.zip_ind = S.zip_ind, T.guarantee_ind = S.guarantee_ind, T.variation_code = S.variation_code, T.county = S.county, T.non_smoker_disc_code = S.non_smoker_disc_code, T.renl_disc = S.renl_disc, T.renl_safe_driver_disc_count = S.renl_safe_driver_disc_count, T.nonrenewal_flag_date = S.nonrenewal_flag_date, T.audit_complt_date = S.audit_complt_date, T.orig_acct_date = S.orig_acct_date, T.pol_enter_date = S.pol_enter_date, T.excess_claim_code = S.excess_claim_code, T.pol_status_on_pif = S.pol_status_on_pif, T.target_mrkt_code = S.target_mrkt_code, T.pkg_code = S.pkg_code, T.pol_kind_code = S.pol_kind_code, T.bus_seg_code = S.bus_seg_code, T.pif_upload_audit_ind = S.pif_upload_audit_ind, T.err_flag_bal_txn = S.err_flag_bal_txn, T.err_flag_bal_reins = S.err_flag_bal_reins, T.producer_code_ak_id = S.producer_code_ak_id, T.prdcr_code = S.prdcr_code, T.ClassOfBusiness = S.ClassOfBusiness, T.strtgc_bus_dvsn_ak_id = S.strtgc_bus_dvsn_ak_id, T.ErrorFlagBalancePremiumTransaction = S.ErrorFlagBalancePremiumTransaction, T.RenewalPolicyNumber = S.RenewalPolicyNumber, T.RenewalPolicySymbol = S.RenewalPolicySymbol, T.RenewalPolicyMod = S.RenewalPolicyMod, T.BillingType = S.BillingType, T.producer_code_id = S.producer_code_id, T.sup_bus_class_code_id = S.sup_bus_class_code_id, T.sup_pol_term_id = S.sup_pol_term_id, T.sup_pol_status_code_id = S.sup_pol_status_code_id, T.sup_pol_issue_code_id = S.sup_pol_issue_code_id, T.sup_pol_audit_frqncy_id = S.sup_pol_audit_frqncy_id, T.sup_industry_risk_grade_code_id = S.sup_industry_risk_grade_code_id, T.sup_state_id = S.sup_state_id, T.SurchargeExemptCode = S.SurchargeExemptCode, T.SupSurchargeExemptID = S.SupSurchargeExemptID, T.StrategicProfitCenterAKId = S.StrategicProfitCenterAKId, T.InsuranceSegmentAKId = S.InsuranceSegmentAKId, T.PolicyOfferingAKId = S.PolicyOfferingAKId, T.ProgramAKId = S.ProgramAKId, T.AgencyAKId = S.AgencyAKId, T.UnderwritingAssociateAKId = S.UnderwritingAssociateAKId, T.ObligeeName = S.ObligeeName, T.AutomatedUnderwritingServicesIndicator = S.AutomatedUnderwritingServicesIndicator, T.AutomaticRenewalIndicator = S.AutomaticRenewalIndicator, T.AssociationCode = S.AssociationCode, T.RolloverPolicyIndicator = S.RolloverPolicyIndicator, T.RolloverPriorCarrier = S.RolloverPriorCarrier, T.MailToInsuredFlag = S.MailToInsuredFlag, T.AgencyEmployeeAKId = S.AgencyEmployeeAKId, T.PolicyIssueCodeOverride = S.PolicyIssueCodeOverride, T.PoolCode = S.PoolCode, T.DCBillFlag = S.DCBillFlag
),
SQ_claim_coverage_stage AS (
	select B.cvr_policy_key,B.cvr_pol_nbr,B.cvr_pol_mod_nbr,B.cvr_policy_eff_dt Pol_eff_date,B.cvr_polisy_exp_dt Pol_exp_date
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLM_COV_PKG_STAGE  A 
	inner JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_coverage_stage B
	ON A.CCP_CLAIM_NBR = B.cvr_claim_nbr
	and cvr_policy_src_id='DUC'
	and A.ccp_pol_cov_id like 'P|%'
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_tab_stage C
	on A.ccp_claim_nbr=C.clm_claim_nbr
	@{pipeline().parameters.WHERE}
	group by B.cvr_policy_key,B.cvr_pol_nbr,B.cvr_pol_mod_nbr,B.cvr_policy_eff_dt,B.cvr_polisy_exp_dt
),
EXP_Source_Data_Collect AS (
	SELECT
	cvr_policy_key,
	cvr_pol_nbr,
	cvr_pol_mod_nbr,
	-- *INF*: IIF(ISNULL(cvr_policy_key),'',LTRIM(RTRIM(cvr_policy_key)))
	IFF(cvr_policy_key IS NULL, '', LTRIM(RTRIM(cvr_policy_key))) AS v_Pol_key,
	-- *INF*: IIF(ISNULL(cvr_pol_nbr),'',LTRIM(RTRIM(cvr_pol_nbr)))
	IFF(cvr_pol_nbr IS NULL, '', LTRIM(RTRIM(cvr_pol_nbr))) AS v_PolicyNumber,
	-- *INF*: IIF(ISNULL(cvr_pol_mod_nbr),'00',LTRIM(RTRIM(cvr_pol_mod_nbr)))
	IFF(cvr_pol_mod_nbr IS NULL, '00', LTRIM(RTRIM(cvr_pol_mod_nbr))) AS v_PolicyVersion,
	-- *INF*: IIF(v_Pol_key='',
	-- IIF(v_PolicyNumber='','',v_PolicyNumber||v_PolicyVersion),v_Pol_key)
	IFF(
	    v_Pol_key = '',
	    IFF(
	        v_PolicyNumber = '', '', v_PolicyNumber || v_PolicyVersion
	    ),
	    v_Pol_key
	) AS v_PolicyKey,
	-- *INF*: :LKP.LKP_POLICY(v_PolicyKey)
	LKP_POLICY_v_PolicyKey.pol_ak_id AS v_lkp_PolicyAkid,
	v_lkp_PolicyAkid AS lkp_PolicyAkid,
	v_PolicyNumber AS PolicyNumber,
	v_PolicyVersion AS PolicyVersion,
	v_PolicyKey AS PolicyKey,
	pol_eff_date AS Pol_eff_date,
	pol_exp_date AS Pol_exp_date,
	-- *INF*: IIF(ISNULL(:LKP.LKP_INSURANCESEGMENT('1')),-1,:LKP.LKP_INSURANCESEGMENT('1'))
	IFF(
	    LKP_INSURANCESEGMENT__1.InsuranceSegmentAKId IS NULL, - 1,
	    LKP_INSURANCESEGMENT__1.InsuranceSegmentAKId
	) AS o_InsuranceSegmentAkid,
	-- *INF*: IIF(ISNULL(:LKP.LKP_STRATEGICPROFITCENTERAKID('1')),-1,:LKP.LKP_STRATEGICPROFITCENTERAKID('1'))
	IFF(
	    LKP_STRATEGICPROFITCENTERAKID__1.StrategicProfitCenterAKId IS NULL, - 1,
	    LKP_STRATEGICPROFITCENTERAKID__1.StrategicProfitCenterAKId
	) AS o_StrategicProfitAkid
	FROM SQ_claim_coverage_stage
	LEFT JOIN LKP_POLICY LKP_POLICY_v_PolicyKey
	ON LKP_POLICY_v_PolicyKey.pol_key = v_PolicyKey

	LEFT JOIN LKP_INSURANCESEGMENT LKP_INSURANCESEGMENT__1
	ON LKP_INSURANCESEGMENT__1.InsuranceSegmentCode = '1'

	LEFT JOIN LKP_STRATEGICPROFITCENTERAKID LKP_STRATEGICPROFITCENTERAKID__1
	ON LKP_STRATEGICPROFITCENTERAKID__1.StrategicProfitCenterCode = '1'

),
FIL_Missing_Policy AS (
	SELECT
	lkp_PolicyAkid, 
	PolicyNumber, 
	PolicyVersion, 
	PolicyKey, 
	Pol_eff_date, 
	Pol_exp_date, 
	o_InsuranceSegmentAkid AS InsuranceSegmentAkid, 
	o_StrategicProfitAkid AS StrategicProfitAkid
	FROM EXP_Source_Data_Collect
	WHERE ISNULL(lkp_PolicyAkid)=1
),
SEQ_policy_cus_ak_id AS (
	CREATE SEQUENCE SEQ_policy_cus_ak_id
	START = 0
	INCREMENT = 1;
),
EXP_DefaultingValues AS (
	SELECT
	1 AS crrnt_snpsht_flag,
	999 AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	SEQ_policy_cus_ak_id.NEXTVAL AS pol_ak_id,
	-1 AS contract_cust_ak_id,
	-1 AS agency_ak_id,
	'000' AS pol_sym,
	PolicyNumber AS pol_num,
	PolicyVersion AS pol_mod,
	PolicyKey AS pol_key,
	'05' AS mco,
	'N/A' AS pol_co_num,
	Pol_eff_date AS pol_eff_date,
	Pol_exp_date AS pol_exp_date,
	SYSDATE AS orig_incptn_date,
	'N/A' AS prim_bus_class_code,
	'N/A' AS reins_code,
	'N/A' AS pms_pol_lob_code,
	'N/A' AS pol_co_line_code,
	'0' AS pol_cancellation_ind,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS pol_cancellation_date,
	'N/A' AS pol_cancellation_rsn_code,
	'N/A' AS state_of_domicile_code,
	'N/A' AS wbconnect_upload_code,
	'N/A' AS serv_center_support_code,
	'N/A' AS pol_term,
	'N/A' AS terrorism_risk_ind,
	'N/A' AS prior_pol_key,
	'N/A' AS pol_status_code,
	'N/A' AS pol_issue_code,
	'N/A' AS pol_age,
	'N/A' AS industry_risk_grade_code,
	'N/A' AS uw_review_yr,
	'N/A' AS mvr_request_code,
	'N/A' AS renl_code,
	'N/A' AS amend_num,
	'N/A' AS anniversary_rerate_code,
	'N/A' AS pol_audit_frqncy,
	'N/A' AS final_audit_code,
	'N/A' AS zip_ind,
	'N/A' AS guarantee_ind,
	'N/A' AS variation_code,
	'N/A' AS county,
	'N/A' AS non_smoker_disc_code,
	0 AS renl_disc,
	0 AS renl_safe_driver_disc_count,
	-- *INF*: TO_DATE('2100-12-31 23:59:59 ','YYYY-MM-DD HH24:MI:SS')
	TO_TIMESTAMP('2100-12-31 23:59:59 ', 'YYYY-MM-DD HH24:MI:SS') AS nonrenewal_flag_date,
	-- *INF*: TO_DATE('2100-12-31 23:59:59 ','YYYY-MM-DD HH24:MI:SS')
	TO_TIMESTAMP('2100-12-31 23:59:59 ', 'YYYY-MM-DD HH24:MI:SS') AS audit_complt_date,
	-- *INF*: TO_DATE('2100-12-31 23:59:59 ','YYYY-MM-DD HH24:MI:SS')
	TO_TIMESTAMP('2100-12-31 23:59:59 ', 'YYYY-MM-DD HH24:MI:SS') AS orig_acct_date,
	SYSDATE AS pol_enter_date,
	'N/A' AS excess_claim_code,
	'N/A' AS pol_status_on_pif,
	'N/A' AS target_mrkt_code,
	'N/A' AS pkg_code,
	'N/A' AS pol_kind_code,
	'N/A' AS bus_seg_code,
	'N/A' AS pif_upload_audit_ind,
	0 AS err_flag_bal_txn,
	0 AS err_flag_bal_reins,
	-1 AS producer_code_ak_id,
	'N/A' AS prdcr_code,
	'N/A' AS ClassOfBusiness,
	'1' AS strtgc_bus_dvsn_ak_id,
	0 AS ErrorFlagBalancePremiumTransaction,
	'N/A' AS RenewalPolicyNumber,
	'N/A' AS RenewalPolicySymbol,
	'N/A' AS RenewalPolicyMod,
	'N/A' AS BillingType,
	-1 AS producer_code_id,
	-1 AS sup_bus_class_code_id,
	-1 AS sup_pol_term_id,
	-1 AS sup_pol_status_code_id,
	-1 AS sup_pol_issue_code_id,
	-1 AS sup_pol_audit_frqncy_id,
	-1 AS sup_industry_risk_grade_code_id,
	-1 AS sup_state_id,
	'N/A' AS SurchargeExemptCode,
	-1 AS SupSurchargeExemptID,
	StrategicProfitAkid AS StrategicProfitCenterAKId,
	InsuranceSegmentAkid AS InsuranceSegmentAKId,
	-1 AS PolicyOfferingAKId,
	-1 AS ProgramAKId,
	-1 AS AgencyAKId,
	-1 AS UnderwritingAssociateAKId,
	'N/A' AS ObligeeName,
	'0' AS AutomatedUnderwritingServicesIndicator,
	'0' AS AutomaticRenewalIndicator,
	'N/A' AS AssociationCode,
	'0' AS RolloverPolicyIndicator,
	'N/A' AS RolloverPriorCarrier,
	'0' AS MailToInsuredFlag,
	-1 AS AgencyEmployeeAKId,
	'0' AS PolicyIssueCodeOverride,
	'N/A' AS PoolCode,
	'0' AS DCBillFlag
	FROM FIL_Missing_Policy
),
policy AS (
	INSERT INTO @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, pol_ak_id, contract_cust_ak_id, agency_ak_id, pol_sym, pol_num, pol_mod, pol_key, mco, pol_co_num, pol_eff_date, pol_exp_date, orig_incptn_date, prim_bus_class_code, reins_code, pms_pol_lob_code, pol_co_line_code, pol_cancellation_ind, pol_cancellation_date, pol_cancellation_rsn_code, state_of_domicile_code, wbconnect_upload_code, serv_center_support_code, pol_term, terrorism_risk_ind, prior_pol_key, pol_status_code, pol_issue_code, pol_age, industry_risk_grade_code, uw_review_yr, mvr_request_code, renl_code, amend_num, anniversary_rerate_code, pol_audit_frqncy, final_audit_code, zip_ind, guarantee_ind, variation_code, county, non_smoker_disc_code, renl_disc, renl_safe_driver_disc_count, nonrenewal_flag_date, audit_complt_date, orig_acct_date, pol_enter_date, excess_claim_code, pol_status_on_pif, target_mrkt_code, pkg_code, pol_kind_code, bus_seg_code, pif_upload_audit_ind, err_flag_bal_txn, err_flag_bal_reins, producer_code_ak_id, prdcr_code, ClassOfBusiness, strtgc_bus_dvsn_ak_id, ErrorFlagBalancePremiumTransaction, RenewalPolicyNumber, RenewalPolicySymbol, RenewalPolicyMod, BillingType, producer_code_id, sup_bus_class_code_id, sup_pol_term_id, sup_pol_status_code_id, sup_pol_issue_code_id, sup_pol_audit_frqncy_id, sup_industry_risk_grade_code_id, sup_state_id, SurchargeExemptCode, SupSurchargeExemptID, StrategicProfitCenterAKId, InsuranceSegmentAKId, PolicyOfferingAKId, ProgramAKId, AgencyAKId, UnderwritingAssociateAKId, ObligeeName, AutomatedUnderwritingServicesIndicator, AutomaticRenewalIndicator, AssociationCode, RolloverPolicyIndicator, RolloverPriorCarrier, MailToInsuredFlag, AgencyEmployeeAKId, PolicyIssueCodeOverride, PoolCode, DCBillFlag)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	POL_AK_ID, 
	CONTRACT_CUST_AK_ID, 
	AGENCY_AK_ID, 
	POL_SYM, 
	POL_NUM, 
	POL_MOD, 
	POL_KEY, 
	MCO, 
	POL_CO_NUM, 
	POL_EFF_DATE, 
	POL_EXP_DATE, 
	ORIG_INCPTN_DATE, 
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
	PRIOR_POL_KEY, 
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
	POL_ENTER_DATE, 
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
	SUPSURCHARGEEXEMPTID, 
	STRATEGICPROFITCENTERAKID, 
	INSURANCESEGMENTAKID, 
	POLICYOFFERINGAKID, 
	PROGRAMAKID, 
	AGENCYAKID, 
	UNDERWRITINGASSOCIATEAKID, 
	OBLIGEENAME, 
	AUTOMATEDUNDERWRITINGSERVICESINDICATOR, 
	AUTOMATICRENEWALINDICATOR, 
	ASSOCIATIONCODE, 
	ROLLOVERPOLICYINDICATOR, 
	ROLLOVERPRIORCARRIER, 
	MAILTOINSUREDFLAG, 
	AGENCYEMPLOYEEAKID, 
	POLICYISSUECODEOVERRIDE, 
	POOLCODE, 
	DCBILLFLAG
	FROM EXP_DefaultingValues
),