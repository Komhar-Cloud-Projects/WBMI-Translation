WITH
LKP_All_Support_tables AS (
	SELECT
	descript,
	source_sys_id,
	tablename,
	code
	FROM (
		SELECT 
			bill_plan_use_code_descript as descript,
			'sup_bill_plan_use_code' AS tablename ,
			LTRIM(RTRIM(bill_plan_use_code)) as code
		FROM 	sup_bill_plan_use_code 
		WHERE   sup_bill_plan_use_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			bill_type_code_descript as descript,
			'sup_bill_type_code' AS tablename ,
			LTRIM(RTRIM(bill_type_code)) as code
		FROM 	sup_bill_type_code 
		WHERE   sup_bill_type_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			bill_class_code_descript as descript,
			'sup_bill_class_code' AS tablename ,
			LTRIM(RTRIM(bill_class_code)) as code
		FROM 	sup_bill_class_code 
		WHERE   sup_bill_class_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			bill_trans_code_descript as descript,
			'sup_bill_transaction_code' AS tablename ,
			LTRIM(RTRIM(bill_trans_code)) as code	
		FROM 	sup_bill_transaction_code 
		WHERE   sup_bill_transaction_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			ars_type_code_descript as descript,
			'sup_bill_ars_type_code' AS tablename ,
			LTRIM(RTRIM(ars_type_code)) as code
		FROM 	sup_bill_ars_type_code 
		WHERE   sup_bill_ars_type_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			payby_code_descript as descript,
			'sup_payby_code' AS tablename ,
			LTRIM(RTRIM(payby_code)) as code
		FROM 	sup_payby_code 
		WHERE   sup_payby_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			mrktng_pkg_description as descript,
			'sup_marketing_package_code' AS tablename ,
			LTRIM(RTRIM(mrktng_pkg_code)) as code
		FROM 	sup_marketing_package_code
		WHERE   sup_marketing_package_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			excess_claim_code_descript as descript,
			'sup_excess_claim_code' AS tablename ,
			LTRIM(RTRIM(excess_claim_code)) as code
		FROM 	sup_excess_claim_code
		WHERE   sup_excess_claim_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			non_smoker_disc_code_descript as descript,
			'sup_non_smoker_discount_code' AS tablename ,
			LTRIM(RTRIM(non_smoker_disc_code)) as code
		FROM 	sup_non_smoker_discount_code
		WHERE   sup_non_smoker_discount_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			variation_code_descript as descript,
			'sup_policy_variation_code' AS tablename ,
			LTRIM(RTRIM(variation_code)) as code	
		FROM 	sup_policy_variation_code
		WHERE   sup_policy_variation_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			pol_audit_frqncy_descript as descript,
			'sup_policy_audit_frequency' AS tablename ,
			LTRIM(RTRIM(pol_audit_frqncy)) as code
		FROM 	sup_policy_audit_frequency
		WHERE   sup_policy_audit_frequency.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			renl_code_descript as descript,
			'sup_policy_renewal_code' AS tablename ,
			LTRIM(RTRIM(renl_code)) as code
		FROM 	sup_policy_renewal_code
		WHERE   sup_policy_renewal_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			mvr_request_code_descript as descript,
			'sup_mvr_request_code' AS tablename ,
			LTRIM(RTRIM(mvr_request_code)) as code	
		FROM 	sup_mvr_request_code
		WHERE   sup_mvr_request_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			industry_risk_grade_code_descript as descript,
			'sup_industry_risk_grade_code' AS tablename ,
			LTRIM(RTRIM(industry_risk_grade_code)) as code	
		FROM 	sup_industry_risk_grade_code
		WHERE   sup_industry_risk_grade_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			pol_issue_code_descript as descript,
			'sup_policy_issue_code' AS tablename ,
			LTRIM(RTRIM(pol_issue_code)) as code
		FROM 	sup_policy_issue_code
		WHERE   sup_policy_issue_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT
			pol_status_code_descript as descript, 
			'sup_policy_status_code' AS tablename ,
			LTRIM(RTRIM(pol_status_code)) as code
		FROM 	sup_policy_status_code
		WHERE   sup_policy_status_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT
			pol_term_descript as descript, 
			'sup_policy_term' AS tablename ,
			LTRIM(RTRIM(pol_term)) as code
		FROM 	sup_policy_term
		WHERE   sup_policy_term.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			serv_center_support_code_descript as descript,
			'sup_service_center_support_code' AS tablename ,
			LTRIM(RTRIM(serv_center_support_code)) as code
		FROM 	sup_service_center_support_code
		WHERE   sup_service_center_support_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			wbconnect_upload_code_descript as descript,
			'sup_wbconnect_upload_code' AS tablename ,
			LTRIM(RTRIM(wbconnect_upload_code)) as code
		FROM 	sup_wbconnect_upload_code
		WHERE   sup_wbconnect_upload_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			state_descript as descript,
			'sup_state' AS tablename ,
			LTRIM(RTRIM(state_abbrev)) as code
		FROM 	sup_state
		WHERE   sup_state.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			state_descript as descript,
			'sup_state' AS tablename ,
			LTRIM(RTRIM(state_code)) as code
		FROM 	sup_state
		WHERE   sup_state.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			state_code as descript,
			'sup_state_abbrev' AS tablename ,
			LTRIM(RTRIM(state_abbrev)) as code
		FROM 	sup_state
		WHERE   sup_state.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			pol_co_num_descript as descript,
			'sup_policy_company_number' AS tablename ,
			LTRIM(RTRIM(pol_co_num)) as code
		FROM 	sup_policy_company_number
		WHERE   sup_policy_company_number.crrnt_snpsht_flag=1
		
		UNION ALL
		SELECT 
			pol_co_line_code_descript as descript,
			'sup_policy_company_line_code' AS tablename ,
			LTRIM(RTRIM(pol_co_line_code)) as code
		FROM 	sup_policy_company_line_code
		WHERE   sup_policy_company_line_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			reins_code_descript as descript,
			'sup_reinsurance_code' AS tablename ,
			LTRIM(RTRIM(reins_code)) as code
		FROM 	sup_reinsurance_code
		WHERE   sup_reinsurance_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			bus_class_code_descript as descript,
			'sup_business_classification_code' AS tablename ,
			LTRIM(RTRIM(bus_class_code)) as code
		FROM 	sup_business_classification_code
		WHERE   sup_business_classification_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			target_mrkt_code_descript as descript,
			'sup_target_market_code' AS tablename ,
			LTRIM(RTRIM(target_mrkt_code)) as code
		FROM 	sup_target_market_code
		WHERE   sup_target_market_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			assoc_prog_code_descript as descript,
			'sup_association_program_code' AS tablename ,
			LTRIM(RTRIM(assoc_prog_code)) as code
		FROM 	sup_association_program_code
		WHERE   sup_association_program_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			lgl_ent_code_descript as descript,
			'sup_legal_entity_code' AS tablename ,
			LTRIM(RTRIM(lgl_ent_code)) as code
		FROM 	sup_legal_entity_code
		WHERE   sup_legal_entity_code.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			cdt_rating_score_descript as descript,
			'sup_credit_rating_score' AS tablename ,
			LTRIM(RTRIM(cdt_rating_score)) as code
		FROM 	sup_credit_rating_score
		WHERE   sup_credit_rating_score.crrnt_snpsht_flag=1
		UNION ALL
		SELECT 
			sic_code_descript as descript,
			'sup_sic_code' AS tablename ,
			LTRIM(RTRIM(sic_code)) as code
		FROM 	sup_sic_code
		WHERE   sup_sic_code.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY source_sys_id,tablename,code ORDER BY descript DESC) = 1
),
LKP_AgencyEmployeeDim_DCT AS (
	SELECT
	AgencyEmployeeDimID,
	AgencyCode,
	AgencyEmployeeUserID
	FROM (
		select a.AgencyEmployeeDimID as AgencyEmployeeDimID,
		LTRIM(RTRIM(c.AgencyCode)) as AgencyCode,
		LTRIM(RTRIM(a.AgencyEmployeeUserID)) as AgencyEmployeeUserID
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyEmployeeDim a
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyEmployee b
		on a.EDWAgencyEmployeeAKID=b.AgencyEmployeeAKID
		and a.CurrentSnapshotFlag=1
		and b.CurrentSnapshotFlag=1
		join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency c
		on b.AgencyAKID=c.AgencyAKID
		and c.CurrentSnapshotFlag=1
		where a.AgencyEmployeeUserID<>'N/A'
		order by c.AgencyCode,a.AgencyEmployeeUserID,a.AgencyEmployeeDimID desc
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyCode,AgencyEmployeeUserID ORDER BY AgencyEmployeeDimID) = 1
),
SQ_policy AS (
	SELECT
		pol_id,
		crrnt_snpsht_flag,
		audit_id,
		eff_from_date,
		eff_to_date,
		source_sys_id,
		created_date,
		modified_date,
		pol_ak_id,
		contract_cust_ak_id,
		AgencyAKId AS agencyakid,
		pol_sym,
		pol_num,
		pol_mod,
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
		producer_code_id,
		sup_bus_class_code_id,
		sup_pol_term_id,
		sup_pol_status_code_id,
		sup_pol_issue_code_id,
		sup_pol_audit_frqncy_id,
		sup_industry_risk_grade_code_id,
		sup_state_id,
		SupSurchargeExemptID,
		InsuranceSegmentAKId,
		ProgramAKId,
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
		IssuedUWID,
		IssuedUnderwriter
	FROM policy
	WHERE (policy.created_date>='@{pipeline().parameters.SELECTION_START_TS}' OR policy.modified_date>='@{pipeline().parameters.SELECTION_START_TS}')
),
EXP_values AS (
	SELECT
	pol_id,
	eff_from_date,
	pol_ak_id,
	pol_sym,
	pol_num,
	pol_mod,
	pol_key,
	mco,
	pol_co_num AS in_pol_co_num,
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
	prior_pol_key AS in_prior_pol_key,
	-- *INF*: iif(substr(in_prior_pol_key,1,1)='+','N/A',iif(substr(in_prior_pol_key,14,1)='+','N/A',in_prior_pol_key))
	IFF(substr(in_prior_pol_key, 1, 1) = '+', 'N/A', IFF(substr(in_prior_pol_key, 14, 1) = '+', 'N/A', in_prior_pol_key)) AS prior_pol_key,
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
	target_mrkt_code AS target_mrkt_code1,
	-- *INF*: :UDF.LTRIM_RTRIM(target_mrkt_code1)
	:UDF.LTRIM_RTRIM(target_mrkt_code1) AS target_mrkt_code,
	pkg_code AS pkg_code1,
	-- *INF*: :UDF.LTRIM_RTRIM(pkg_code1)
	:UDF.LTRIM_RTRIM(pkg_code1) AS pkg_code,
	pol_kind_code AS pol_kind_code1,
	-- *INF*: :UDF.LTRIM_RTRIM(pol_kind_code1)
	:UDF.LTRIM_RTRIM(pol_kind_code1) AS pol_kind_code,
	bus_seg_code AS bus_seg_code1,
	-- *INF*: :UDF.LTRIM_RTRIM(bus_seg_code1)
	:UDF.LTRIM_RTRIM(bus_seg_code1) AS bus_seg_code,
	pif_upload_audit_ind AS pif_upload_audit_ind1,
	-- *INF*: :UDF.LTRIM_RTRIM(pif_upload_audit_ind1)
	:UDF.LTRIM_RTRIM(pif_upload_audit_ind1) AS pif_upload_audit_ind,
	err_flag_bal_txn,
	err_flag_bal_reins,
	prdcr_code,
	ClassOfBusiness,
	ErrorFlagBalancePremiumTransaction,
	RenewalPolicyNumber,
	RenewalPolicySymbol,
	RenewalPolicyMod,
	BillingType,
	source_sys_id,
	sup_bus_class_code_id,
	sup_pol_term_id,
	sup_pol_status_code_id,
	sup_pol_issue_code_id,
	sup_pol_audit_frqncy_id,
	sup_industry_risk_grade_code_id,
	sup_state_id,
	SupSurchargeExemptID,
	InsuranceSegmentAKId,
	ProgramAKId,
	UnderwritingAssociateAKId,
	agencyakid AS AgencyAKId,
	ObligeeName,
	AutomatedUnderwritingServicesIndicator,
	AutomaticRenewalIndicator AS i_AutomaticRenewalIndicator,
	-- *INF*: IIF(i_AutomaticRenewalIndicator='T', '1', '0')
	IFF(i_AutomaticRenewalIndicator = 'T', '1', '0') AS o_AutomaticRenewalIndicator,
	AssociationCode,
	RolloverPolicyIndicator AS i_RolloverPolicyIndicator,
	-- *INF*: IIF(i_RolloverPolicyIndicator='T','1','0')
	IFF(i_RolloverPolicyIndicator = 'T', '1', '0') AS o_RolloverPolicyIndicator,
	RolloverPriorCarrier,
	MailToInsuredFlag AS i_MailToInsuredFlag,
	-- *INF*: DECODE(TRUE,
	-- i_MailToInsuredFlag='T','1',
	-- i_MailToInsuredFlag='1','1',
	-- '0')
	DECODE(TRUE,
	i_MailToInsuredFlag = 'T', '1',
	i_MailToInsuredFlag = '1', '1',
	'0') AS o_MailToInsuredFlag,
	AgencyEmployeeAKId,
	PolicyIssueCodeOverride AS i_PolicyIssueCodeOverride,
	-- *INF*: IIF(i_PolicyIssueCodeOverride='T','Y','N')
	IFF(i_PolicyIssueCodeOverride = 'T', 'Y', 'N') AS o_PolicyIssueCodeOverride,
	DCBillFlag,
	IssuedUWID,
	IssuedUnderwriter
	FROM SQ_policy
),
LKP_AgencyEmployeeDim AS (
	SELECT
	AgencyEmployeeDimId,
	EDWAgencyEmployeeAKId,
	AgencyEmployeeAKId
	FROM (
		SELECT 
			AgencyEmployeeDimId,
			EDWAgencyEmployeeAKId,
			AgencyEmployeeAKId
		FROM AgencyEmployeeDim
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyEmployeeAKId ORDER BY AgencyEmployeeDimId) = 1
),
LKP_Agency_V2 AS (
	SELECT
	AgencyCode,
	AgencyAKID
	FROM (
		SELECT 
			AgencyCode,
			AgencyAKID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID ORDER BY AgencyCode) = 1
),
LKP_AssociationDescription AS (
	SELECT
	AssociationDescription,
	AssociationCode
	FROM (
		SELECT 
			AssociationDescription,
			AssociationCode
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Association
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AssociationCode ORDER BY AssociationDescription) = 1
),
LKP_AssociationDiscountFactor_DCT AS (
	SELECT
	AssociationDiscountFactor,
	PolicyKey,
	AssociationCode
	FROM (
		select DISTINCT (case when wbp.AssociationDiscountFactor is not null then wbp.AssociationDiscountFactor
		     when WBIM.AssociationFactor is not null then WBIM.AssociationFactor
			 when wbbp.AssociationFactorliability is not null then wbbp.AssociationFactorliability
			 when wbbp.AssociationFactorProperty is not null then wbbp.AssociationFactorProperty
			 when loc.AssociationFactor is not null then loc.AssociationFactor
			 else 0 end ) as AssociationDiscountFactor,
		LTRIM(RTRIM(DCP.PolicyNumber))+ISNULL(WBP.PolicyVersionFormatted,'00') as PolicyKey,
		LTRIM(RTRIM(WBP.Association)) AS AssociationCode
		from DCPolicyStaging DCP
		JOIN WBPolicyStaging WBP
		ON DCP.PolicyId=WBP.PolicyId 
		and DCP.SessionId=WBP.SessionId
		join DCTransactionStaging DCT
		on dct.SessionId = dcp.SessionId
		and dct.HistoryID in (select max(historyid) from DCTransactionStaging where SessionId= dct.SessionId)
		and dct.state = 'committed'
		JOIN DCLineStaging DCL
		on dcp.PolicyId=dcl.PolicyId
		left join DCIMLineStage DCIM
		on dcim.LineId = dcl.LineId
		left join WBIMLineStage wbim
		on dcim.IM_LineId = wbim.IM_LineId
		left join DCBPlineStage DCBP
		on dcl.LineId = dcbp.LineId
		left join WBBPLineStage wbbp
		on wbbp.BP_LineId = dcbp.BP_LineId
		left hash join (
		select dcl.SessionId, wbgla.AssociationFactor 
		from DCLocationStaging dcl
		join DCLocationAssociationStaging dcla
		on dcl.SessionId = dcla.SessionId
		and dcla.LocationId = dcl.LocationId
		join DCGLLocationStaging dcgl1
		on dcgl1.GL_LocationId = dcla.ObjectId
		and dcla.ObjectName = 'DC_GL_Location'
		join WBLocationAccountStage wbla
		on dcl.LocationId = wbla.LocationId
		join WBCLLocationAccountStage wbcla
		on wbla.WBLocationAccountId = wbcla.WBLocationAccountId
		join WBGLLocationAccountStage wbgla
		on wbcla.WBCLLocationAccountId = wbgla.WB_CL_LocationAccountId) loc
		on dcl.SessionId = loc.SessionId
		--and dcl.type in ('GeneralLiability','SBOPGeneralLiability')
		
		WHERE WBP.Association='ABC'
		AND (WBP.AssociationDiscountFactor IS NOT NULL
		OR WBIM.AssociationFactor IS NOT NULL
		OR WBBP.AssociationFactorLiability IS NOT NULL
		OR WBBP.AssociationFactorProperty IS NOT NULL
		OR loc.AssociationFactor IS NOT NULL
		)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,AssociationCode ORDER BY AssociationDiscountFactor) = 1
),
LKP_AssociationDiscountPercentage AS (
	SELECT
	AssociationDiscountPercentage,
	PolicyKey,
	AssociationCode
	FROM (
		SELECT
		pol_key AS PolicyKey,
		AssociationCode AS AssociationCode,
		CASE WHEN InsuranceLineList='CF,,' THEN PackagePropertyPercentage
		  WHEN InsuranceLineList='CF,CR,IM' THEN PackageCrimeInlandMarinePercentage
		 WHEN InsuranceLineList='CR,IM,' THEN PackageNoPropertyCrimePercentage
		 ELSE 0 END AS AssociationDiscountPercentage
		FROM
		(
		select distinct
		pol_key, 
		MAX(case when seq=1 then InsuranceLine else '' end)+','+MAX(case when seq=2 then InsuranceLine else '' end)
		 +','+MAX(case when seq=3 then InsuranceLine else '' end) as InsuranceLineList,
		MAX(AssociationCode) as AssociationCode,
		MAX(AssociationDescription) as AssociationDescription,
		MAX(PackagePropertyPercentage) as PackagePropertyPercentage,
		MAX(PackageCrimeInlandMarinePercentage) as PackageCrimeInlandMarinePercentage,
		MAX(PackageNoPropertyCrimePercentage) as PackageNoPropertyCrimePercentage
		from (select pol_key,
		InsuranceLine,
		ROW_NUMBER() over (PARTITION BY pol_key order by insuranceline) seq,
		AssociationCode,
		AssociationDescription,
		PackagePropertyPercentage,
		PackageCrimeInlandMarinePercentage,
		PackageNoPropertyCrimePercentage
		from (SELECT  distinct 
		pol.pol_key,
		pc.InsuranceLine,
		assoc.AssociationCode,
		assoc.AssociationDescription,
		assoc.PackagePropertyPercentage,
		assoc.PackageCrimeInlandMarinePercentage,
		assoc.PackageNoPropertyCrimePercentage
		 FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy pol 
		JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage pc
		 ON pol.pol_ak_id=pc.PolicyAKID
		 AND pol.crrnt_snpsht_flag=1 
		JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Association assoc 
		on pol.AssociationCode=assoc.AssociationCode and assoc.CurrentSnapshotFlag=1
		WHERE pol.AssociationCode IN ('AE','AG') 
		and pc.InsuranceLine in ('CF','IM','CR')) source) order_source
		group by pol_key
		)T
		
		order by PolicyKey,AssociationCode
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,AssociationCode ORDER BY AssociationDiscountPercentage) = 1
),
LKP_BusinessClassDim AS (
	SELECT
	i_pol_eff_date,
	BusinessClassDimId,
	CurrentSnapshotFlag,
	AuditId,
	EffectiveDate,
	ExpirationDate,
	SourceSystemId,
	CreatedDate,
	ModifiedDate,
	BusinessClassCode,
	BusinessClassDescription,
	BusinessSegmentCode,
	BusinessSegmentDescription,
	StrategicBusinessGroupCode,
	StrategicBusinessGroupDescription
	FROM (
		SELECT 
			i_pol_eff_date,
			BusinessClassDimId,
			CurrentSnapshotFlag,
			AuditId,
			EffectiveDate,
			ExpirationDate,
			SourceSystemId,
			CreatedDate,
			ModifiedDate,
			BusinessClassCode,
			BusinessClassDescription,
			BusinessSegmentCode,
			BusinessSegmentDescription,
			StrategicBusinessGroupCode,
			StrategicBusinessGroupDescription
		FROM BusinessClassDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EffectiveDate,ExpirationDate,BusinessClassCode ORDER BY i_pol_eff_date) = 1
),
LKP_InsuranceSegment AS (
	SELECT
	InsuranceSegmentCode,
	InsuranceSegmentDescription,
	InsuranceSegmentAKId
	FROM (
		SELECT 
			InsuranceSegmentCode,
			InsuranceSegmentDescription,
			InsuranceSegmentAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceSegmentAKId ORDER BY InsuranceSegmentCode) = 1
),
LKP_Program AS (
	SELECT
	ProgramCode,
	ProgramDescription,
	ProgramAKId
	FROM (
		SELECT 
			ProgramCode,
			ProgramDescription,
			ProgramAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Program
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProgramAKId ORDER BY ProgramCode) = 1
),
LKP_QuoteDim AS (
	SELECT
	QuoteChannel,
	QuoteChannelOrigin,
	QuoteNumber
	FROM (
		SELECT 
			QuoteChannel,
			QuoteChannelOrigin,
			QuoteNumber
		FROM QuoteDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY QuoteNumber ORDER BY QuoteChannel) = 1
),
LKP_SupSurchargeExempt AS (
	SELECT
	StandardSurchargeExemptCode,
	StandardSurchargeExemptDescription,
	SupSurchargeExemptId
	FROM (
		SELECT 
			StandardSurchargeExemptCode,
			StandardSurchargeExemptDescription,
			SupSurchargeExemptId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupSurchargeExempt
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SupSurchargeExemptId ORDER BY StandardSurchargeExemptCode) = 1
),
LKP_UnderWritingDivisionDim AS (
	SELECT
	UnderwritingDivisionDimID,
	EDWUnderwritingAssociateAKID
	FROM (
		SELECT 
			UnderwritingDivisionDimID,
			EDWUnderwritingAssociateAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingDivisionDim
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWUnderwritingAssociateAKID ORDER BY UnderwritingDivisionDimID) = 1
),
LKP_UserId AS (
	SELECT
	UserID,
	PolicyNumber,
	PolicyVersion
	FROM (
		select LTRIM(RTRIM(substring(name,CHARINDEX('\',name)+1,len(Name)))) as UserID,
		DCP.PolicyNumber as PolicyNumber,
		ISNULL(WBP.PolicyVersionFormatted,'00') as PolicyVersion
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPolicyStaging DCP
		inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBPolicyStaging WBP
		on DCP.PolicyId=WBP.PolicyId
		and DCP.SessionId=WBP.SessionId
		inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBProducerStage WBProducer
		on DCP.PolicyId=WBProducer.PolicyId
		and DCP.SessionId=WBP.SessionId
		order by DCP.PolicyNumber,WBP.PolicyVersion,DCP.SessionId desc--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber,PolicyVersion ORDER BY UserID) = 1
),
LKP_sup_business_classification_code AS (
	SELECT
	StandardBusinessClassCode,
	StandardBusinessClassCodeDescription,
	sup_bus_class_code_id
	FROM (
		SELECT 
			StandardBusinessClassCode,
			StandardBusinessClassCodeDescription,
			sup_bus_class_code_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_business_classification_code
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_bus_class_code_id ORDER BY StandardBusinessClassCode DESC) = 1
),
LKP_sup_industry_risk_grade_code AS (
	SELECT
	StandardIndustryRiskGradeCode,
	StandardIndustryRiskGradeCodeDescription,
	sup_industry_risk_grade_code_id
	FROM (
		SELECT 
			StandardIndustryRiskGradeCode,
			StandardIndustryRiskGradeCodeDescription,
			sup_industry_risk_grade_code_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_industry_risk_grade_code
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_industry_risk_grade_code_id ORDER BY StandardIndustryRiskGradeCode DESC) = 1
),
LKP_sup_policy_audit_frequency AS (
	SELECT
	StandardPolicyAuditFrequency,
	StandardPolicyAuditFrequencyDescription,
	sup_pol_audit_frqncy_id
	FROM (
		SELECT 
			StandardPolicyAuditFrequency,
			StandardPolicyAuditFrequencyDescription,
			sup_pol_audit_frqncy_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_policy_audit_frequency
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_pol_audit_frqncy_id ORDER BY StandardPolicyAuditFrequency DESC) = 1
),
LKP_sup_policy_issue_code AS (
	SELECT
	StandardPolicyIssueCode,
	StandardPolicyIssueCodeDescription,
	sup_pol_issue_code_id
	FROM (
		SELECT 
			StandardPolicyIssueCode,
			StandardPolicyIssueCodeDescription,
			sup_pol_issue_code_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_policy_issue_code
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_pol_issue_code_id ORDER BY StandardPolicyIssueCode DESC) = 1
),
LKP_sup_policy_status_code AS (
	SELECT
	StandardPolicyStatusCode,
	StandardPolicyStatusCodeDescription,
	sup_pol_status_code_id
	FROM (
		SELECT 
			StandardPolicyStatusCode,
			StandardPolicyStatusCodeDescription,
			sup_pol_status_code_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_policy_status_code
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_pol_status_code_id ORDER BY StandardPolicyStatusCode DESC) = 1
),
LKP_sup_policy_term AS (
	SELECT
	StandardPolicyTerm,
	StandardPolicyTermDescription,
	sup_pol_term_id
	FROM (
		SELECT 
			StandardPolicyTerm,
			StandardPolicyTermDescription,
			sup_pol_term_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_policy_term
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_pol_term_id ORDER BY StandardPolicyTerm DESC) = 1
),
LKP_sup_state AS (
	SELECT
	state_code,
	state_descript,
	sup_state_id
	FROM (
		SELECT 
			state_code,
			state_descript,
			sup_state_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_state
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_state_id ORDER BY state_code DESC) = 1
),
EXP_sup_description AS (
	SELECT
	EXP_values.source_sys_id AS i_source_sys_id,
	EXP_values.pol_id,
	EXP_values.pol_ak_id,
	EXP_values.pol_sym,
	EXP_values.pol_num,
	EXP_values.pol_mod,
	EXP_values.pol_key,
	EXP_values.mco,
	EXP_values.prdcr_code,
	LKP_InsuranceSegment.InsuranceSegmentCode AS StandardInsuranceSegmentCode,
	LKP_InsuranceSegment.InsuranceSegmentDescription AS StandardInsuranceSegmentDescription,
	LKP_sup_business_classification_code.StandardBusinessClassCode,
	LKP_sup_business_classification_code.StandardBusinessClassCodeDescription,
	LKP_sup_policy_term.StandardPolicyTerm,
	LKP_sup_policy_term.StandardPolicyTermDescription,
	LKP_sup_policy_status_code.StandardPolicyStatusCode,
	LKP_sup_policy_status_code.StandardPolicyStatusCodeDescription,
	LKP_sup_policy_issue_code.StandardPolicyIssueCode,
	LKP_sup_policy_issue_code.StandardPolicyIssueCodeDescription,
	LKP_sup_industry_risk_grade_code.StandardIndustryRiskGradeCode,
	LKP_sup_industry_risk_grade_code.StandardIndustryRiskGradeCodeDescription,
	LKP_sup_policy_audit_frequency.StandardPolicyAuditFrequency,
	LKP_sup_policy_audit_frequency.StandardPolicyAuditFrequencyDescription,
	LKP_sup_state.state_code AS StandardStateAbbreviation,
	LKP_sup_state.state_descript AS StandardStateDescription,
	LKP_SupSurchargeExempt.StandardSurchargeExemptCode,
	LKP_SupSurchargeExempt.StandardSurchargeExemptDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(prdcr_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(prdcr_code) AS producer_code,
	EXP_values.in_pol_co_num AS pol_co_num,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM(i_source_sys_id), 'sup_policy_company_number',:UDF.LTRIM_RTRIM(pol_co_num))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_company_number_UDF_LTRIM_RTRIM_pol_co_num.descript AS v_pol_co_num_descript,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_pol_co_num_descript)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_pol_co_num_descript) AS pol_co_num_descript,
	EXP_values.pol_eff_date,
	EXP_values.pol_exp_date,
	EXP_values.orig_incptn_date,
	EXP_values.prim_bus_class_code,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM('DCT'),'sup_business_classification_code',:UDF.LTRIM_RTRIM(prim_bus_class_code))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_DCT_sup_business_classification_code_UDF_LTRIM_RTRIM_prim_bus_class_code.descript AS v_prim_bus_class_code_descript,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_prim_bus_class_code_descript)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_prim_bus_class_code_descript) AS prim_bus_class_code_descript,
	EXP_values.reins_code,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM(i_source_sys_id),'sup_reinsurance_code',:UDF.LTRIM_RTRIM(reins_code))
	-- 
	-- 
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_reinsurance_code_UDF_LTRIM_RTRIM_reins_code.descript AS v_reins_code_descript,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_reins_code_descript)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_reins_code_descript) AS reins_code_descript,
	EXP_values.pms_pol_lob_code,
	'N/A' AS pms_pol_lob_code_descript,
	EXP_values.pol_co_line_code,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM(i_source_sys_id),'sup_policy_company_line_code',:UDF.LTRIM_RTRIM(pol_co_line_code))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_company_line_code_UDF_LTRIM_RTRIM_pol_co_line_code.descript AS v_pol_co_line_code_descript,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_pol_co_line_code_descript)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_pol_co_line_code_descript) AS pol_co_line_code_descript,
	EXP_values.pol_cancellation_ind,
	EXP_values.pol_cancellation_date,
	EXP_values.pol_cancellation_rsn_code,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM(i_source_sys_id),'sup_reason_amended_code',:UDF.LTRIM_RTRIM(pol_cancellation_rsn_code))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_reason_amended_code_UDF_LTRIM_RTRIM_pol_cancellation_rsn_code.descript AS v_pol_cancellation_rsn_code_descript,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_pol_cancellation_rsn_code_descript)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_pol_cancellation_rsn_code_descript) AS pol_cancellation_rsn_code_descript,
	EXP_values.state_of_domicile_code,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM(i_source_sys_id),'sup_state_abbrev',:UDF.LTRIM_RTRIM(state_of_domicile_code))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_state_abbrev_UDF_LTRIM_RTRIM_state_of_domicile_code.descript AS v_state_of_domicile_code_abbrev,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_state_of_domicile_code_abbrev)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_state_of_domicile_code_abbrev) AS state_of_domicile_code_abbrev,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM('EXCEED'),'sup_state',:UDF.LTRIM_RTRIM(state_of_domicile_code))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_EXCEED_sup_state_UDF_LTRIM_RTRIM_state_of_domicile_code.descript AS v_state_of_domicile_code_descript,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_state_of_domicile_code_descript)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_state_of_domicile_code_descript) AS state_of_domicile_code_descript,
	EXP_values.wbconnect_upload_code,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM(i_source_sys_id),'sup_wbconnect_upload_code',:UDF.LTRIM_RTRIM(wbconnect_upload_code))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_wbconnect_upload_code_UDF_LTRIM_RTRIM_wbconnect_upload_code.descript AS v_wbconnect_upload_code_descript,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_wbconnect_upload_code_descript)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_wbconnect_upload_code_descript) AS wbconnect_upload_code_descript,
	EXP_values.serv_center_support_code,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM(i_source_sys_id),'sup_service_center_support_code',:UDF.LTRIM_RTRIM(serv_center_support_code))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_service_center_support_code_UDF_LTRIM_RTRIM_serv_center_support_code.descript AS v_serv_center_support_code_descript,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_serv_center_support_code_descript)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_serv_center_support_code_descript) AS serv_center_support_code_descript,
	EXP_values.pol_term,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM('PMS'),'sup_policy_term',:UDF.LTRIM_RTRIM(pol_term))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_policy_term_UDF_LTRIM_RTRIM_pol_term.descript AS v_pol_term_descript,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_pol_term_descript)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_pol_term_descript) AS pol_term_descript,
	EXP_values.terrorism_risk_ind,
	EXP_values.prior_pol_key,
	EXP_values.pol_status_code,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM(i_source_sys_id),'sup_policy_status_code',:UDF.LTRIM_RTRIM(pol_status_code))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_status_code_UDF_LTRIM_RTRIM_pol_status_code.descript AS v_pol_status_code_descript,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_pol_status_code_descript)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_pol_status_code_descript) AS pol_status_code_descript,
	'N/A' AS pol_count_type_code,
	EXP_values.pol_issue_code,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM('PMS'),'sup_policy_issue_code',:UDF.LTRIM_RTRIM(pol_issue_code))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_policy_issue_code_UDF_LTRIM_RTRIM_pol_issue_code.descript AS v_pol_issue_code_descript,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_pol_issue_code_descript)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_pol_issue_code_descript) AS pol_issue_code_descript,
	EXP_values.pol_age,
	EXP_values.industry_risk_grade_code,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM('PMS'),'sup_industry_risk_grade_code',:UDF.LTRIM_RTRIM(industry_risk_grade_code))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_industry_risk_grade_code_UDF_LTRIM_RTRIM_industry_risk_grade_code.descript AS v_industry_risk_grade_code_descript,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_industry_risk_grade_code_descript)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_industry_risk_grade_code_descript) AS industry_risk_grade_code_descript,
	EXP_values.uw_review_yr,
	EXP_values.mvr_request_code,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM(i_source_sys_id),'sup_mvr_request_code',:UDF.LTRIM_RTRIM(mvr_request_code))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_mvr_request_code_UDF_LTRIM_RTRIM_mvr_request_code.descript AS v_mvr_request_code_descript,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_mvr_request_code_descript)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_mvr_request_code_descript) AS mvr_request_code_descript,
	EXP_values.renl_code,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM(i_source_sys_id),'sup_policy_renewal_code',:UDF.LTRIM_RTRIM(renl_code))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_renewal_code_UDF_LTRIM_RTRIM_renl_code.descript AS v_renl_code_descript,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_renl_code_descript)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_renl_code_descript) AS renl_code_descript,
	EXP_values.amend_num,
	EXP_values.anniversary_rerate_code,
	EXP_values.pol_audit_frqncy,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM('PMS'),'sup_policy_audit_frequency',:UDF.LTRIM_RTRIM(pol_audit_frqncy))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_policy_audit_frequency_UDF_LTRIM_RTRIM_pol_audit_frqncy.descript AS v_pol_audit_frqncy_descript,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_pol_audit_frqncy_descript)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_pol_audit_frqncy_descript) AS pol_audit_frqncy_descript,
	EXP_values.final_audit_code,
	EXP_values.zip_ind,
	EXP_values.guarantee_ind,
	EXP_values.variation_code,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM(i_source_sys_id),'sup_policy_variation_code',:UDF.LTRIM_RTRIM(variation_code))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_variation_code_UDF_LTRIM_RTRIM_variation_code.descript AS v_variation_ind_descript,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_variation_ind_descript)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_variation_ind_descript) AS variation_ind_descript,
	EXP_values.county,
	EXP_values.non_smoker_disc_code,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM(i_source_sys_id),'sup_non_smoker_discount_code',:UDF.LTRIM_RTRIM(non_smoker_disc_code))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_non_smoker_discount_code_UDF_LTRIM_RTRIM_non_smoker_disc_code.descript AS v_non_smoker_disc_code_descript,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_non_smoker_disc_code_descript)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_non_smoker_disc_code_descript) AS non_smoker_disc_code_descript,
	EXP_values.renl_disc,
	EXP_values.renl_safe_driver_disc_count,
	EXP_values.nonrenewal_flag_date,
	EXP_values.audit_complt_date,
	EXP_values.orig_acct_date,
	EXP_values.pol_enter_date,
	EXP_values.excess_claim_code,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM(i_source_sys_id),'sup_excess_claim_code',:UDF.LTRIM_RTRIM(excess_claim_code))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_excess_claim_code_UDF_LTRIM_RTRIM_excess_claim_code.descript AS v_excess_claim_code_descript,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_excess_claim_code_descript)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_excess_claim_code_descript) AS excess_claim_code_descript,
	EXP_values.pol_status_on_pif,
	EXP_values.target_mrkt_code,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM(i_source_sys_id),'sup_target_market_code',:UDF.LTRIM_RTRIM(target_mrkt_code))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_target_market_code_UDF_LTRIM_RTRIM_target_mrkt_code.descript AS v_target_mrkt_code_descript,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_target_mrkt_code_descript)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_target_mrkt_code_descript) AS target_mrkt_code_descript,
	EXP_values.pkg_code,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM(i_source_sys_id),'sup_marketing_package_code',:UDF.LTRIM_RTRIM(pkg_code))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_marketing_package_code_UDF_LTRIM_RTRIM_pkg_code.descript AS v_mrktng_pkg_description,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_mrktng_pkg_description)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_mrktng_pkg_description) AS mrktng_pkg_description,
	EXP_values.pol_kind_code,
	'N/A' AS pol_kind_code_description,
	EXP_values.bus_seg_code,
	-- *INF*: IIF(pol_sym='000',bus_seg_code,'N/A')
	IFF(pol_sym = '000', bus_seg_code, 'N/A') AS bus_seg_code_description,
	EXP_values.pif_upload_audit_ind,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	EXP_values.eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	EXP_values.err_flag_bal_txn,
	EXP_values.err_flag_bal_reins,
	-- *INF*: Decode(TRUE,
	-- substr(pol_sym,1,1)='N','N',
	-- in(substr(pol_sym,1,1),'R','S','T') and pms_pol_lob_code='WCP','N',
	-- in(substr(pol_sym,1,1),'A','J','L'),'A',
	-- 'W')
	-- 
	Decode(TRUE,
	substr(pol_sym, 1, 1) = 'N', 'N',
	in(substr(pol_sym, 1, 1), 'R', 'S', 'T') AND pms_pol_lob_code = 'WCP', 'N',
	in(substr(pol_sym, 1, 1), 'A', 'J', 'L'), 'A',
	'W') AS reporting_dvsn_code,
	EXP_values.ClassOfBusiness,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(ClassOfBusiness)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(ClassOfBusiness) AS class_of_business,
	-- *INF*: :LKP.LKP_ALL_SUPPORT_TABLES(:UDF.LTRIM_RTRIM(i_source_sys_id),'sup_association_program_code',:UDF.LTRIM_RTRIM(ClassOfBusiness))
	LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_association_program_code_UDF_LTRIM_RTRIM_ClassOfBusiness.descript AS v_ClassOfBusinessCodeDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_ClassOfBusinessCodeDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_ClassOfBusinessCodeDescription) AS ClassOfBusinessCodeDescription,
	'N/A' AS policy_type,
	'N/A' AS prog_code,
	'N/A' AS prog_code_desc,
	EXP_values.ErrorFlagBalancePremiumTransaction,
	EXP_values.RenewalPolicyNumber,
	EXP_values.RenewalPolicySymbol,
	EXP_values.RenewalPolicyMod,
	EXP_values.BillingType,
	LKP_Program.ProgramCode AS i_ProgramCode,
	LKP_Program.ProgramDescription AS i_ProgramDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(StandardInsuranceSegmentCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(StandardInsuranceSegmentCode) AS o_StandardInsuranceSegmentCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(StandardInsuranceSegmentDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(StandardInsuranceSegmentDescription) AS o_StandardInsuranceSegmentDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(StandardBusinessClassCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(StandardBusinessClassCode) AS o_StandardBusinessClassCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(StandardBusinessClassCodeDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(StandardBusinessClassCodeDescription) AS o_StandardBusinessClassCodeDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(StandardPolicyTerm)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(StandardPolicyTerm) AS o_StandardPolicyTerm,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(StandardPolicyTermDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(StandardPolicyTermDescription) AS o_StandardPolicyTermDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(StandardPolicyStatusCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(StandardPolicyStatusCode) AS o_StandardPolicyStatusCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(StandardPolicyStatusCodeDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(StandardPolicyStatusCodeDescription) AS o_StandardPolicyStatusCodeDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(StandardPolicyIssueCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(StandardPolicyIssueCode) AS o_StandardPolicyIssueCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(StandardPolicyIssueCodeDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(StandardPolicyIssueCodeDescription) AS o_StandardPolicyIssueCodeDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(StandardIndustryRiskGradeCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(StandardIndustryRiskGradeCode) AS o_StandardIndustryRiskGradeCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(StandardIndustryRiskGradeCodeDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(StandardIndustryRiskGradeCodeDescription) AS o_StandardIndustryRiskGradeCodeDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(StandardPolicyAuditFrequency)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(StandardPolicyAuditFrequency) AS o_StandardPolicyAuditFrequency,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(StandardPolicyAuditFrequencyDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(StandardPolicyAuditFrequencyDescription) AS o_StandardPolicyAuditFrequencyDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(StandardStateAbbreviation)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(StandardStateAbbreviation) AS o_StandardStateAbbreviation,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(StandardStateDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(StandardStateDescription) AS o_StandardStateDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(StandardSurchargeExemptCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(StandardSurchargeExemptCode) AS o_StandardSurchargeExemptCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(StandardSurchargeExemptDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(StandardSurchargeExemptDescription) AS o_StandardSurchargeExemptDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ProgramCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_ProgramCode) AS o_ProgramCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ProgramDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_ProgramDescription) AS o_ProgramDescription,
	LKP_UnderWritingDivisionDim.UnderwritingDivisionDimID,
	LKP_UserId.UserID AS i_UserID,
	LKP_Agency_V2.AgencyCode AS i_AgencyCode,
	-- *INF*: LTRIM(RTRIM(i_AgencyCode))
	LTRIM(RTRIM(i_AgencyCode)) AS v_AgencyCode,
	LKP_AgencyEmployeeDim.AgencyEmployeeDimId AS i_AgencyEmployeeDimID,
	-- *INF*: IIF(ISNULL(i_AgencyEmployeeDimID),-1,i_AgencyEmployeeDimID)
	IFF(i_AgencyEmployeeDimID IS NULL, - 1, i_AgencyEmployeeDimID) AS v_AgencyEmployeeDimID,
	v_AgencyEmployeeDimID AS o_AgencyEmployeeDimID,
	EXP_values.ObligeeName,
	EXP_values.AutomatedUnderwritingServicesIndicator,
	EXP_values.o_AutomaticRenewalIndicator AS AutomaticRenewalIndicator,
	EXP_values.AssociationCode,
	LKP_AssociationDescription.AssociationDescription AS i_AssociationDescription,
	-- *INF*: IIF(ISNULL(i_AssociationDescription),'N/A',i_AssociationDescription)
	IFF(i_AssociationDescription IS NULL, 'N/A', i_AssociationDescription) AS o_AssociationDescription,
	LKP_AssociationDiscountPercentage.AssociationDiscountPercentage AS i_AssociationDiscountPercentage_PMS,
	-- *INF*: IIF(ISNULL(i_AssociationDiscountPercentage_PMS),0,i_AssociationDiscountPercentage_PMS)
	IFF(i_AssociationDiscountPercentage_PMS IS NULL, 0, i_AssociationDiscountPercentage_PMS) AS v_AssociationDiscountPercentage_PMS,
	LKP_AssociationDiscountFactor_DCT.AssociationDiscountFactor AS i_AssociationDiscountFactor_DCT,
	-- *INF*: IIF(ISNULL(i_AssociationDiscountFactor_DCT),0,i_AssociationDiscountFactor_DCT)
	IFF(i_AssociationDiscountFactor_DCT IS NULL, 0, i_AssociationDiscountFactor_DCT) AS v_AssociationDiscountFactor_DCT,
	-- *INF*: DECODE(TRUE,
	-- i_source_sys_id='PMS',v_AssociationDiscountPercentage_PMS,
	-- i_source_sys_id='DCT',v_AssociationDiscountFactor_DCT,0
	-- )
	DECODE(TRUE,
	i_source_sys_id = 'PMS', v_AssociationDiscountPercentage_PMS,
	i_source_sys_id = 'DCT', v_AssociationDiscountFactor_DCT,
	0) AS o_AssociationDiscountPercentage,
	'N/A' AS o_ExpiringPriorPolicyKey,
	LKP_BusinessClassDim.BusinessClassDimId AS i_BusinessClassDimId,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_INTEGERS(i_BusinessClassDimId)
	:UDF.DEFAULT_VALUE_FOR_INTEGERS(i_BusinessClassDimId) AS o_BusinessClassDimId,
	EXP_values.o_RolloverPolicyIndicator AS RolloverPolicyIndicator,
	EXP_values.RolloverPriorCarrier,
	EXP_values.o_MailToInsuredFlag AS MailToInsuredFlag,
	EXP_values.o_PolicyIssueCodeOverride AS PolicyIssueCodeOverride,
	EXP_values.DCBillFlag,
	-- *INF*: DECODE(TRUE,
	-- DCBillFlag='T',1,
	-- 0)
	DECODE(TRUE,
	DCBillFlag = 'T', 1,
	0) AS out_DCBillFlag,
	EXP_values.IssuedUWID AS i_IssuedUWID,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_IssuedUWID)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_IssuedUWID) AS o_IssuedUWID,
	EXP_values.IssuedUnderwriter AS i_IssuedUnderwriter,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_IssuedUnderwriter)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_IssuedUnderwriter) AS o_IssuedUnderwriter,
	LKP_QuoteDim.QuoteChannel,
	-- *INF*: iif(isnull(QuoteChannel), 'N/A',QuoteChannel)
	IFF(QuoteChannel IS NULL, 'N/A', QuoteChannel) AS o_QuoteChannel,
	LKP_QuoteDim.QuoteChannelOrigin,
	-- *INF*: iif(isnull(QuoteChannelOrigin), 'N/A',QuoteChannelOrigin)
	IFF(QuoteChannelOrigin IS NULL, 'N/A', QuoteChannelOrigin) AS o_QuoteChannelOrigin
	FROM EXP_values
	LEFT JOIN LKP_AgencyEmployeeDim
	ON LKP_AgencyEmployeeDim.EDWAgencyEmployeeAKId = EXP_values.AgencyEmployeeAKId
	LEFT JOIN LKP_Agency_V2
	ON LKP_Agency_V2.AgencyAKID = EXP_values.AgencyAKId
	LEFT JOIN LKP_AssociationDescription
	ON LKP_AssociationDescription.AssociationCode = EXP_values.AssociationCode
	LEFT JOIN LKP_AssociationDiscountFactor_DCT
	ON LKP_AssociationDiscountFactor_DCT.PolicyKey = EXP_values.pol_key AND LKP_AssociationDiscountFactor_DCT.AssociationCode = EXP_values.AssociationCode
	LEFT JOIN LKP_AssociationDiscountPercentage
	ON LKP_AssociationDiscountPercentage.PolicyKey = EXP_values.pol_key AND LKP_AssociationDiscountPercentage.AssociationCode = EXP_values.AssociationCode
	LEFT JOIN LKP_BusinessClassDim
	ON LKP_BusinessClassDim.EffectiveDate <= EXP_values.pol_eff_date AND LKP_BusinessClassDim.ExpirationDate >= EXP_values.pol_eff_date AND LKP_BusinessClassDim.BusinessClassCode = EXP_values.prim_bus_class_code
	LEFT JOIN LKP_InsuranceSegment
	ON LKP_InsuranceSegment.InsuranceSegmentAKId = EXP_values.InsuranceSegmentAKId
	LEFT JOIN LKP_Program
	ON LKP_Program.ProgramAKId = EXP_values.ProgramAKId
	LEFT JOIN LKP_QuoteDim
	ON LKP_QuoteDim.QuoteNumber = EXP_values.pol_num
	LEFT JOIN LKP_SupSurchargeExempt
	ON LKP_SupSurchargeExempt.SupSurchargeExemptId = EXP_values.SupSurchargeExemptID
	LEFT JOIN LKP_UnderWritingDivisionDim
	ON LKP_UnderWritingDivisionDim.EDWUnderwritingAssociateAKID = EXP_values.UnderwritingAssociateAKId
	LEFT JOIN LKP_UserId
	ON LKP_UserId.PolicyNumber = EXP_values.pol_num AND LKP_UserId.PolicyVersion = EXP_values.pol_mod
	LEFT JOIN LKP_sup_business_classification_code
	ON LKP_sup_business_classification_code.sup_bus_class_code_id = EXP_values.sup_bus_class_code_id
	LEFT JOIN LKP_sup_industry_risk_grade_code
	ON LKP_sup_industry_risk_grade_code.sup_industry_risk_grade_code_id = EXP_values.sup_industry_risk_grade_code_id
	LEFT JOIN LKP_sup_policy_audit_frequency
	ON LKP_sup_policy_audit_frequency.sup_pol_audit_frqncy_id = EXP_values.sup_pol_audit_frqncy_id
	LEFT JOIN LKP_sup_policy_issue_code
	ON LKP_sup_policy_issue_code.sup_pol_issue_code_id = EXP_values.sup_pol_issue_code_id
	LEFT JOIN LKP_sup_policy_status_code
	ON LKP_sup_policy_status_code.sup_pol_status_code_id = EXP_values.sup_pol_status_code_id
	LEFT JOIN LKP_sup_policy_term
	ON LKP_sup_policy_term.sup_pol_term_id = EXP_values.sup_pol_term_id
	LEFT JOIN LKP_sup_state
	ON LKP_sup_state.sup_state_id = EXP_values.sup_state_id
	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_company_number_UDF_LTRIM_RTRIM_pol_co_num
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_company_number_UDF_LTRIM_RTRIM_pol_co_num.source_sys_id = :UDF.LTRIM_RTRIM(i_source_sys_id)
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_company_number_UDF_LTRIM_RTRIM_pol_co_num.tablename = 'sup_policy_company_number'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_company_number_UDF_LTRIM_RTRIM_pol_co_num.code = :UDF.LTRIM_RTRIM(pol_co_num)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_DCT_sup_business_classification_code_UDF_LTRIM_RTRIM_prim_bus_class_code
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_DCT_sup_business_classification_code_UDF_LTRIM_RTRIM_prim_bus_class_code.source_sys_id = :UDF.LTRIM_RTRIM('DCT')
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_DCT_sup_business_classification_code_UDF_LTRIM_RTRIM_prim_bus_class_code.tablename = 'sup_business_classification_code'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_DCT_sup_business_classification_code_UDF_LTRIM_RTRIM_prim_bus_class_code.code = :UDF.LTRIM_RTRIM(prim_bus_class_code)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_reinsurance_code_UDF_LTRIM_RTRIM_reins_code
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_reinsurance_code_UDF_LTRIM_RTRIM_reins_code.source_sys_id = :UDF.LTRIM_RTRIM(i_source_sys_id)
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_reinsurance_code_UDF_LTRIM_RTRIM_reins_code.tablename = 'sup_reinsurance_code'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_reinsurance_code_UDF_LTRIM_RTRIM_reins_code.code = :UDF.LTRIM_RTRIM(reins_code)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_company_line_code_UDF_LTRIM_RTRIM_pol_co_line_code
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_company_line_code_UDF_LTRIM_RTRIM_pol_co_line_code.source_sys_id = :UDF.LTRIM_RTRIM(i_source_sys_id)
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_company_line_code_UDF_LTRIM_RTRIM_pol_co_line_code.tablename = 'sup_policy_company_line_code'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_company_line_code_UDF_LTRIM_RTRIM_pol_co_line_code.code = :UDF.LTRIM_RTRIM(pol_co_line_code)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_reason_amended_code_UDF_LTRIM_RTRIM_pol_cancellation_rsn_code
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_reason_amended_code_UDF_LTRIM_RTRIM_pol_cancellation_rsn_code.source_sys_id = :UDF.LTRIM_RTRIM(i_source_sys_id)
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_reason_amended_code_UDF_LTRIM_RTRIM_pol_cancellation_rsn_code.tablename = 'sup_reason_amended_code'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_reason_amended_code_UDF_LTRIM_RTRIM_pol_cancellation_rsn_code.code = :UDF.LTRIM_RTRIM(pol_cancellation_rsn_code)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_state_abbrev_UDF_LTRIM_RTRIM_state_of_domicile_code
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_state_abbrev_UDF_LTRIM_RTRIM_state_of_domicile_code.source_sys_id = :UDF.LTRIM_RTRIM(i_source_sys_id)
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_state_abbrev_UDF_LTRIM_RTRIM_state_of_domicile_code.tablename = 'sup_state_abbrev'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_state_abbrev_UDF_LTRIM_RTRIM_state_of_domicile_code.code = :UDF.LTRIM_RTRIM(state_of_domicile_code)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_EXCEED_sup_state_UDF_LTRIM_RTRIM_state_of_domicile_code
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_EXCEED_sup_state_UDF_LTRIM_RTRIM_state_of_domicile_code.source_sys_id = :UDF.LTRIM_RTRIM('EXCEED')
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_EXCEED_sup_state_UDF_LTRIM_RTRIM_state_of_domicile_code.tablename = 'sup_state'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_EXCEED_sup_state_UDF_LTRIM_RTRIM_state_of_domicile_code.code = :UDF.LTRIM_RTRIM(state_of_domicile_code)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_wbconnect_upload_code_UDF_LTRIM_RTRIM_wbconnect_upload_code
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_wbconnect_upload_code_UDF_LTRIM_RTRIM_wbconnect_upload_code.source_sys_id = :UDF.LTRIM_RTRIM(i_source_sys_id)
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_wbconnect_upload_code_UDF_LTRIM_RTRIM_wbconnect_upload_code.tablename = 'sup_wbconnect_upload_code'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_wbconnect_upload_code_UDF_LTRIM_RTRIM_wbconnect_upload_code.code = :UDF.LTRIM_RTRIM(wbconnect_upload_code)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_service_center_support_code_UDF_LTRIM_RTRIM_serv_center_support_code
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_service_center_support_code_UDF_LTRIM_RTRIM_serv_center_support_code.source_sys_id = :UDF.LTRIM_RTRIM(i_source_sys_id)
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_service_center_support_code_UDF_LTRIM_RTRIM_serv_center_support_code.tablename = 'sup_service_center_support_code'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_service_center_support_code_UDF_LTRIM_RTRIM_serv_center_support_code.code = :UDF.LTRIM_RTRIM(serv_center_support_code)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_policy_term_UDF_LTRIM_RTRIM_pol_term
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_policy_term_UDF_LTRIM_RTRIM_pol_term.source_sys_id = :UDF.LTRIM_RTRIM('PMS')
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_policy_term_UDF_LTRIM_RTRIM_pol_term.tablename = 'sup_policy_term'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_policy_term_UDF_LTRIM_RTRIM_pol_term.code = :UDF.LTRIM_RTRIM(pol_term)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_status_code_UDF_LTRIM_RTRIM_pol_status_code
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_status_code_UDF_LTRIM_RTRIM_pol_status_code.source_sys_id = :UDF.LTRIM_RTRIM(i_source_sys_id)
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_status_code_UDF_LTRIM_RTRIM_pol_status_code.tablename = 'sup_policy_status_code'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_status_code_UDF_LTRIM_RTRIM_pol_status_code.code = :UDF.LTRIM_RTRIM(pol_status_code)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_policy_issue_code_UDF_LTRIM_RTRIM_pol_issue_code
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_policy_issue_code_UDF_LTRIM_RTRIM_pol_issue_code.source_sys_id = :UDF.LTRIM_RTRIM('PMS')
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_policy_issue_code_UDF_LTRIM_RTRIM_pol_issue_code.tablename = 'sup_policy_issue_code'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_policy_issue_code_UDF_LTRIM_RTRIM_pol_issue_code.code = :UDF.LTRIM_RTRIM(pol_issue_code)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_industry_risk_grade_code_UDF_LTRIM_RTRIM_industry_risk_grade_code
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_industry_risk_grade_code_UDF_LTRIM_RTRIM_industry_risk_grade_code.source_sys_id = :UDF.LTRIM_RTRIM('PMS')
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_industry_risk_grade_code_UDF_LTRIM_RTRIM_industry_risk_grade_code.tablename = 'sup_industry_risk_grade_code'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_industry_risk_grade_code_UDF_LTRIM_RTRIM_industry_risk_grade_code.code = :UDF.LTRIM_RTRIM(industry_risk_grade_code)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_mvr_request_code_UDF_LTRIM_RTRIM_mvr_request_code
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_mvr_request_code_UDF_LTRIM_RTRIM_mvr_request_code.source_sys_id = :UDF.LTRIM_RTRIM(i_source_sys_id)
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_mvr_request_code_UDF_LTRIM_RTRIM_mvr_request_code.tablename = 'sup_mvr_request_code'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_mvr_request_code_UDF_LTRIM_RTRIM_mvr_request_code.code = :UDF.LTRIM_RTRIM(mvr_request_code)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_renewal_code_UDF_LTRIM_RTRIM_renl_code
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_renewal_code_UDF_LTRIM_RTRIM_renl_code.source_sys_id = :UDF.LTRIM_RTRIM(i_source_sys_id)
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_renewal_code_UDF_LTRIM_RTRIM_renl_code.tablename = 'sup_policy_renewal_code'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_renewal_code_UDF_LTRIM_RTRIM_renl_code.code = :UDF.LTRIM_RTRIM(renl_code)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_policy_audit_frequency_UDF_LTRIM_RTRIM_pol_audit_frqncy
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_policy_audit_frequency_UDF_LTRIM_RTRIM_pol_audit_frqncy.source_sys_id = :UDF.LTRIM_RTRIM('PMS')
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_policy_audit_frequency_UDF_LTRIM_RTRIM_pol_audit_frqncy.tablename = 'sup_policy_audit_frequency'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_PMS_sup_policy_audit_frequency_UDF_LTRIM_RTRIM_pol_audit_frqncy.code = :UDF.LTRIM_RTRIM(pol_audit_frqncy)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_variation_code_UDF_LTRIM_RTRIM_variation_code
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_variation_code_UDF_LTRIM_RTRIM_variation_code.source_sys_id = :UDF.LTRIM_RTRIM(i_source_sys_id)
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_variation_code_UDF_LTRIM_RTRIM_variation_code.tablename = 'sup_policy_variation_code'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_policy_variation_code_UDF_LTRIM_RTRIM_variation_code.code = :UDF.LTRIM_RTRIM(variation_code)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_non_smoker_discount_code_UDF_LTRIM_RTRIM_non_smoker_disc_code
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_non_smoker_discount_code_UDF_LTRIM_RTRIM_non_smoker_disc_code.source_sys_id = :UDF.LTRIM_RTRIM(i_source_sys_id)
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_non_smoker_discount_code_UDF_LTRIM_RTRIM_non_smoker_disc_code.tablename = 'sup_non_smoker_discount_code'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_non_smoker_discount_code_UDF_LTRIM_RTRIM_non_smoker_disc_code.code = :UDF.LTRIM_RTRIM(non_smoker_disc_code)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_excess_claim_code_UDF_LTRIM_RTRIM_excess_claim_code
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_excess_claim_code_UDF_LTRIM_RTRIM_excess_claim_code.source_sys_id = :UDF.LTRIM_RTRIM(i_source_sys_id)
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_excess_claim_code_UDF_LTRIM_RTRIM_excess_claim_code.tablename = 'sup_excess_claim_code'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_excess_claim_code_UDF_LTRIM_RTRIM_excess_claim_code.code = :UDF.LTRIM_RTRIM(excess_claim_code)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_target_market_code_UDF_LTRIM_RTRIM_target_mrkt_code
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_target_market_code_UDF_LTRIM_RTRIM_target_mrkt_code.source_sys_id = :UDF.LTRIM_RTRIM(i_source_sys_id)
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_target_market_code_UDF_LTRIM_RTRIM_target_mrkt_code.tablename = 'sup_target_market_code'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_target_market_code_UDF_LTRIM_RTRIM_target_mrkt_code.code = :UDF.LTRIM_RTRIM(target_mrkt_code)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_marketing_package_code_UDF_LTRIM_RTRIM_pkg_code
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_marketing_package_code_UDF_LTRIM_RTRIM_pkg_code.source_sys_id = :UDF.LTRIM_RTRIM(i_source_sys_id)
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_marketing_package_code_UDF_LTRIM_RTRIM_pkg_code.tablename = 'sup_marketing_package_code'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_marketing_package_code_UDF_LTRIM_RTRIM_pkg_code.code = :UDF.LTRIM_RTRIM(pkg_code)

	LEFT JOIN LKP_ALL_SUPPORT_TABLES LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_association_program_code_UDF_LTRIM_RTRIM_ClassOfBusiness
	ON LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_association_program_code_UDF_LTRIM_RTRIM_ClassOfBusiness.source_sys_id = :UDF.LTRIM_RTRIM(i_source_sys_id)
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_association_program_code_UDF_LTRIM_RTRIM_ClassOfBusiness.tablename = 'sup_association_program_code'
	AND LKP_ALL_SUPPORT_TABLES__UDF_LTRIM_RTRIM_i_source_sys_id_sup_association_program_code_UDF_LTRIM_RTRIM_ClassOfBusiness.code = :UDF.LTRIM_RTRIM(ClassOfBusiness)

),
LKP_policy_dim AS (
	SELECT
	pol_dim_id,
	edw_pol_pk_id
	FROM (
		SELECT 
			pol_dim_id,
			edw_pol_pk_id
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_pk_id ORDER BY pol_dim_id DESC) = 1
),
RTR_INS_UPD AS (
	SELECT
	LKP_policy_dim.pol_dim_id AS lkp_pol_dim_id,
	EXP_sup_description.pol_id,
	EXP_sup_description.pol_ak_id,
	EXP_sup_description.pol_sym,
	EXP_sup_description.pol_num,
	EXP_sup_description.pol_mod,
	EXP_sup_description.pol_key,
	EXP_sup_description.mco,
	EXP_sup_description.producer_code,
	EXP_sup_description.pol_co_num,
	EXP_sup_description.pol_co_num_descript,
	EXP_sup_description.pol_eff_date,
	EXP_sup_description.pol_exp_date,
	EXP_sup_description.orig_incptn_date,
	EXP_sup_description.prim_bus_class_code,
	EXP_sup_description.prim_bus_class_code_descript,
	EXP_sup_description.reins_code,
	EXP_sup_description.reins_code_descript,
	EXP_sup_description.pms_pol_lob_code,
	EXP_sup_description.pms_pol_lob_code_descript,
	EXP_sup_description.pol_co_line_code,
	EXP_sup_description.pol_co_line_code_descript,
	EXP_sup_description.pol_cancellation_ind,
	EXP_sup_description.pol_cancellation_date,
	EXP_sup_description.pol_cancellation_rsn_code,
	EXP_sup_description.pol_cancellation_rsn_code_descript,
	EXP_sup_description.state_of_domicile_code,
	EXP_sup_description.state_of_domicile_code_abbrev,
	EXP_sup_description.state_of_domicile_code_descript,
	EXP_sup_description.wbconnect_upload_code,
	EXP_sup_description.wbconnect_upload_code_descript,
	EXP_sup_description.serv_center_support_code,
	EXP_sup_description.serv_center_support_code_descript,
	EXP_sup_description.pol_term,
	EXP_sup_description.pol_term_descript,
	EXP_sup_description.terrorism_risk_ind,
	EXP_sup_description.prior_pol_key,
	EXP_sup_description.pol_status_code,
	EXP_sup_description.pol_status_code_descript,
	EXP_sup_description.pol_count_type_code,
	EXP_sup_description.pol_issue_code,
	EXP_sup_description.pol_issue_code_descript,
	EXP_sup_description.pol_age,
	EXP_sup_description.industry_risk_grade_code,
	EXP_sup_description.industry_risk_grade_code_descript,
	EXP_sup_description.uw_review_yr,
	EXP_sup_description.mvr_request_code,
	EXP_sup_description.mvr_request_code_descript,
	EXP_sup_description.renl_code,
	EXP_sup_description.renl_code_descript,
	EXP_sup_description.amend_num,
	EXP_sup_description.anniversary_rerate_code,
	EXP_sup_description.pol_audit_frqncy,
	EXP_sup_description.pol_audit_frqncy_descript,
	EXP_sup_description.final_audit_code,
	EXP_sup_description.zip_ind,
	EXP_sup_description.guarantee_ind,
	EXP_sup_description.variation_code,
	EXP_sup_description.variation_ind_descript,
	EXP_sup_description.county,
	EXP_sup_description.non_smoker_disc_code,
	EXP_sup_description.non_smoker_disc_code_descript,
	EXP_sup_description.renl_disc,
	EXP_sup_description.renl_safe_driver_disc_count,
	EXP_sup_description.nonrenewal_flag_date,
	EXP_sup_description.audit_complt_date,
	EXP_sup_description.orig_acct_date,
	EXP_sup_description.pol_enter_date,
	EXP_sup_description.excess_claim_code,
	EXP_sup_description.excess_claim_code_descript,
	EXP_sup_description.pol_status_on_pif,
	EXP_sup_description.target_mrkt_code,
	EXP_sup_description.target_mrkt_code_descript,
	EXP_sup_description.pkg_code,
	EXP_sup_description.mrktng_pkg_description,
	EXP_sup_description.pol_kind_code,
	EXP_sup_description.pol_kind_code_description,
	EXP_sup_description.bus_seg_code,
	EXP_sup_description.bus_seg_code_description,
	EXP_sup_description.pif_upload_audit_ind,
	EXP_sup_description.crrnt_snpsht_flag,
	EXP_sup_description.audit_id,
	EXP_sup_description.eff_from_date,
	EXP_sup_description.eff_to_date,
	EXP_sup_description.created_date,
	EXP_sup_description.modified_date,
	EXP_sup_description.err_flag_bal_txn,
	EXP_sup_description.err_flag_bal_reins,
	EXP_sup_description.reporting_dvsn_code,
	EXP_sup_description.class_of_business,
	EXP_sup_description.ClassOfBusinessCodeDescription,
	EXP_sup_description.policy_type,
	EXP_sup_description.ErrorFlagBalancePremiumTransaction,
	EXP_sup_description.RenewalPolicyNumber,
	EXP_sup_description.RenewalPolicySymbol,
	EXP_sup_description.RenewalPolicyMod,
	EXP_sup_description.BillingType,
	EXP_sup_description.o_StandardInsuranceSegmentCode AS StandardInsuranceSegmentCode,
	EXP_sup_description.o_StandardInsuranceSegmentDescription AS StandardInsuranceSegmentDescription,
	EXP_sup_description.o_StandardBusinessClassCode AS StandardBusinessClassCode,
	EXP_sup_description.o_StandardBusinessClassCodeDescription AS StandardBusinessClassCodeDescription,
	EXP_sup_description.o_StandardPolicyTerm AS StandardPolicyTerm,
	EXP_sup_description.o_StandardPolicyTermDescription AS StandardPolicyTermDescription,
	EXP_sup_description.o_StandardPolicyStatusCode AS StandardPolicyStatusCode,
	EXP_sup_description.o_StandardPolicyStatusCodeDescription AS StandardPolicyStatusCodeDescription,
	EXP_sup_description.o_StandardPolicyIssueCode AS StandardPolicyIssueCode,
	EXP_sup_description.o_StandardPolicyIssueCodeDescription AS StandardPolicyIssueCodeDescription,
	EXP_sup_description.o_StandardIndustryRiskGradeCode AS StandardIndustryRiskGradeCode,
	EXP_sup_description.o_StandardIndustryRiskGradeCodeDescription AS StandardIndustryRiskGradeCodeDescription,
	EXP_sup_description.o_StandardPolicyAuditFrequency AS StandardPolicyAuditFrequency,
	EXP_sup_description.o_StandardPolicyAuditFrequencyDescription AS StandardPolicyAuditFrequencyDescription,
	EXP_sup_description.o_StandardStateAbbreviation AS StandardStateAbbreviation,
	EXP_sup_description.o_StandardStateDescription AS StandardStateDescription,
	EXP_sup_description.o_StandardSurchargeExemptCode AS StandardSurchargeExemptCode,
	EXP_sup_description.o_StandardSurchargeExemptDescription AS StandardSurchargeExemptDescription,
	EXP_sup_description.o_ProgramCode AS ProgramCode,
	EXP_sup_description.o_ProgramDescription AS ProgramDescription,
	EXP_sup_description.UnderwritingDivisionDimID,
	EXP_sup_description.o_AgencyEmployeeDimID AS AgencyEmployeeDimId,
	EXP_sup_description.ObligeeName,
	EXP_sup_description.AutomatedUnderwritingServicesIndicator,
	EXP_sup_description.AutomaticRenewalIndicator,
	EXP_sup_description.AssociationCode,
	EXP_sup_description.o_AssociationDescription AS AssociationDescription,
	EXP_sup_description.o_AssociationDiscountPercentage AS AssociationDiscountPercentage,
	EXP_sup_description.o_ExpiringPriorPolicyKey AS ExpiringPriorPolicyKey,
	EXP_sup_description.o_BusinessClassDimId AS BusinessClassDimId,
	EXP_sup_description.RolloverPolicyIndicator,
	EXP_sup_description.RolloverPriorCarrier,
	EXP_sup_description.MailToInsuredFlag,
	EXP_sup_description.PolicyIssueCodeOverride,
	EXP_sup_description.out_DCBillFlag AS DCBillFlag,
	EXP_sup_description.o_IssuedUWID AS IssuedUWID,
	EXP_sup_description.o_IssuedUnderwriter AS IssuedUnderwriter,
	EXP_sup_description.o_QuoteChannel AS QuoteChannel,
	EXP_sup_description.o_QuoteChannelOrigin AS QuoteChannelOrigin
	FROM EXP_sup_description
	LEFT JOIN LKP_policy_dim
	ON LKP_policy_dim.edw_pol_pk_id = EXP_sup_description.pol_id
),
RTR_INS_UPD_INSERT AS (SELECT * FROM RTR_INS_UPD WHERE ISNULL(lkp_pol_dim_id)),
RTR_INS_UPD_DEFAULT1 AS (SELECT * FROM RTR_INS_UPD WHERE NOT ( (ISNULL(lkp_pol_dim_id)) )),
TGT_policy_dim_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, edw_pol_pk_id, edw_pol_ak_id, pol_sym, pol_num, pol_mod, pol_key, mco, producer_code, pol_co_num, pol_co_num_descript, pol_eff_date, pol_exp_date, orig_incptn_date, prim_bus_class_code, prim_bus_class_code_descript, reins_code, reins_code_descript, pms_pol_lob_code, pms_pol_lob_code_descript, pol_co_line_code, pol_co_line_code_descript, pol_cancellation_ind, pol_cancellation_date, pol_cancellation_rsn_code, pol_cancellation_rsn_code_descript, state_of_domicile_code, state_of_domicile_abbrev, state_of_domicile_code_descript, wbconnect_upload_code, wbconnect_upload_code_descript, serv_center_support_code, serv_center_support_code_descript, pol_term, pol_term_descript, terrorism_risk_ind, prior_pol_key, pol_status_code, pol_status_code_descript, pol_count_type_code, pol_issue_code, pol_issue_code_descript, pol_age, industry_risk_grade_code, industry_risk_grade_code_descript, uw_review_yr, mvr_request_code, mvr_request_code_descript, renl_code, renl_code_descript, amend_num, anniversary_rerate_code, pol_audit_frqncy, pol_audit_frqncy_descript, final_audit_code, zip_ind, guarantee_ind, variation_code, variation_code_descript, county, non_smoker_disc_code, non_smoker_disc_code_descript, renl_disc, renl_safe_driver_disc_count, nonrenewal_flag_date, audit_complt_date, orig_acct_date, pol_enter_date, excess_claim_code, excess_claim_code_descript, pol_status_on_pif, target_mrkt_code, target_mrkt_code_descript, pkg_code, pkg_code_descript, pol_kind_code, bus_seg_code, bus_seg_code_descript, pif_upload_audit_ind, err_flag_bal_txn, err_flag_bal_reins, reporting_dvsn_code, ClassOfBusinessCode, ClassOfBusinessCodeDescription, ErrorFlagBalancePremiumTransaction, RenewalPolicyNumber, RenewalPolicySymbol, RenewalPolicyMod, BillingType, SurchargeExemptCode, SurchargeExemptDescription, ProgramCode, ProgramDescription, UnderwritingDivisionDimId, AgencyEmployeeDimID, ObligeeName, AutomatedUnderwritingServicesIndicator, AutomaticRenewalIndicator, AssociationCode, AssociationDescription, AssociationDiscountPercentage, ExpiringPriorPolicyKey, BusinessClassDimId, RolloverPolicyIndicator, RolloverPriorCarrier, MailToInsuredFlag, PolicyIssueCodeOverrideInd, DCBillFlag, IssuedUWID, IssuedUnderwriter, QuoteChannel, QuoteChannelOrigin)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	pol_id AS EDW_POL_PK_ID, 
	pol_ak_id AS EDW_POL_AK_ID, 
	POL_SYM, 
	POL_NUM, 
	POL_MOD, 
	POL_KEY, 
	MCO, 
	PRODUCER_CODE, 
	POL_CO_NUM, 
	POL_CO_NUM_DESCRIPT, 
	POL_EFF_DATE, 
	POL_EXP_DATE, 
	ORIG_INCPTN_DATE, 
	StandardBusinessClassCode AS PRIM_BUS_CLASS_CODE, 
	StandardBusinessClassCodeDescription AS PRIM_BUS_CLASS_CODE_DESCRIPT, 
	REINS_CODE, 
	REINS_CODE_DESCRIPT, 
	PMS_POL_LOB_CODE, 
	PMS_POL_LOB_CODE_DESCRIPT, 
	POL_CO_LINE_CODE, 
	POL_CO_LINE_CODE_DESCRIPT, 
	POL_CANCELLATION_IND, 
	POL_CANCELLATION_DATE, 
	POL_CANCELLATION_RSN_CODE, 
	POL_CANCELLATION_RSN_CODE_DESCRIPT, 
	STATE_OF_DOMICILE_CODE, 
	StandardStateAbbreviation AS STATE_OF_DOMICILE_ABBREV, 
	StandardStateDescription AS STATE_OF_DOMICILE_CODE_DESCRIPT, 
	WBCONNECT_UPLOAD_CODE, 
	WBCONNECT_UPLOAD_CODE_DESCRIPT, 
	SERV_CENTER_SUPPORT_CODE, 
	SERV_CENTER_SUPPORT_CODE_DESCRIPT, 
	StandardPolicyTerm AS POL_TERM, 
	StandardPolicyTermDescription AS POL_TERM_DESCRIPT, 
	TERRORISM_RISK_IND, 
	PRIOR_POL_KEY, 
	StandardPolicyStatusCode AS POL_STATUS_CODE, 
	StandardPolicyStatusCodeDescription AS POL_STATUS_CODE_DESCRIPT, 
	POL_COUNT_TYPE_CODE, 
	StandardPolicyIssueCode AS POL_ISSUE_CODE, 
	StandardPolicyIssueCodeDescription AS POL_ISSUE_CODE_DESCRIPT, 
	POL_AGE, 
	StandardIndustryRiskGradeCode AS INDUSTRY_RISK_GRADE_CODE, 
	StandardIndustryRiskGradeCodeDescription AS INDUSTRY_RISK_GRADE_CODE_DESCRIPT, 
	UW_REVIEW_YR, 
	MVR_REQUEST_CODE, 
	MVR_REQUEST_CODE_DESCRIPT, 
	RENL_CODE, 
	RENL_CODE_DESCRIPT, 
	AMEND_NUM, 
	ANNIVERSARY_RERATE_CODE, 
	StandardPolicyAuditFrequency AS POL_AUDIT_FRQNCY, 
	StandardPolicyAuditFrequencyDescription AS POL_AUDIT_FRQNCY_DESCRIPT, 
	FINAL_AUDIT_CODE, 
	ZIP_IND, 
	GUARANTEE_IND, 
	VARIATION_CODE, 
	variation_ind_descript AS VARIATION_CODE_DESCRIPT, 
	COUNTY, 
	NON_SMOKER_DISC_CODE, 
	NON_SMOKER_DISC_CODE_DESCRIPT, 
	RENL_DISC, 
	RENL_SAFE_DRIVER_DISC_COUNT, 
	NONRENEWAL_FLAG_DATE, 
	AUDIT_COMPLT_DATE, 
	ORIG_ACCT_DATE, 
	POL_ENTER_DATE, 
	EXCESS_CLAIM_CODE, 
	EXCESS_CLAIM_CODE_DESCRIPT, 
	POL_STATUS_ON_PIF, 
	TARGET_MRKT_CODE, 
	TARGET_MRKT_CODE_DESCRIPT, 
	PKG_CODE, 
	mrktng_pkg_description AS PKG_CODE_DESCRIPT, 
	POL_KIND_CODE, 
	BUS_SEG_CODE, 
	bus_seg_code_description AS BUS_SEG_CODE_DESCRIPT, 
	PIF_UPLOAD_AUDIT_IND, 
	ERR_FLAG_BAL_TXN, 
	ERR_FLAG_BAL_REINS, 
	REPORTING_DVSN_CODE, 
	class_of_business AS CLASSOFBUSINESSCODE, 
	CLASSOFBUSINESSCODEDESCRIPTION, 
	ERRORFLAGBALANCEPREMIUMTRANSACTION, 
	RENEWALPOLICYNUMBER, 
	RENEWALPOLICYSYMBOL, 
	RENEWALPOLICYMOD, 
	BILLINGTYPE, 
	StandardSurchargeExemptCode AS SURCHARGEEXEMPTCODE, 
	StandardSurchargeExemptDescription AS SURCHARGEEXEMPTDESCRIPTION, 
	PROGRAMCODE, 
	PROGRAMDESCRIPTION, 
	UnderwritingDivisionDimID AS UNDERWRITINGDIVISIONDIMID, 
	AgencyEmployeeDimId AS AGENCYEMPLOYEEDIMID, 
	OBLIGEENAME, 
	AUTOMATEDUNDERWRITINGSERVICESINDICATOR, 
	AUTOMATICRENEWALINDICATOR, 
	ASSOCIATIONCODE, 
	ASSOCIATIONDESCRIPTION, 
	ASSOCIATIONDISCOUNTPERCENTAGE, 
	EXPIRINGPRIORPOLICYKEY, 
	BUSINESSCLASSDIMID, 
	ROLLOVERPOLICYINDICATOR, 
	ROLLOVERPRIORCARRIER, 
	MAILTOINSUREDFLAG, 
	PolicyIssueCodeOverride AS POLICYISSUECODEOVERRIDEIND, 
	DCBILLFLAG, 
	ISSUEDUWID, 
	ISSUEDUNDERWRITER, 
	QUOTECHANNEL, 
	QUOTECHANNELORIGIN
	FROM RTR_INS_UPD_INSERT
),
UPD_policy_dim AS (
	SELECT
	lkp_pol_dim_id AS lkp_pol_dim_id2, 
	pol_id, 
	pol_ak_id, 
	pol_sym, 
	pol_num, 
	pol_mod, 
	pol_key, 
	mco, 
	producer_code AS producer_code2, 
	pol_co_num, 
	pol_co_num_descript, 
	pol_eff_date, 
	pol_exp_date, 
	orig_incptn_date, 
	prim_bus_class_code, 
	prim_bus_class_code_descript, 
	reins_code, 
	reins_code_descript AS reins_code_descript2, 
	pms_pol_lob_code, 
	pms_pol_lob_code_descript AS pms_pol_lob_code_descript2, 
	pol_co_line_code, 
	pol_co_line_code_descript, 
	pol_cancellation_ind, 
	pol_cancellation_date, 
	pol_cancellation_rsn_code, 
	pol_cancellation_rsn_code_descript AS pol_cancellation_rsn_code_descript2, 
	state_of_domicile_code, 
	state_of_domicile_code_abbrev, 
	state_of_domicile_code_descript AS state_of_domicile_code_descript2, 
	wbconnect_upload_code, 
	wbconnect_upload_code_descript AS wbconnect_upload_code_descript2, 
	serv_center_support_code, 
	serv_center_support_code_descript, 
	pol_term, 
	pol_term_descript AS pol_term_descript2, 
	terrorism_risk_ind, 
	prior_pol_key AS prior_pol_key2, 
	pol_status_code, 
	pol_status_code_descript, 
	pol_count_type_code AS pol_count_type_code2, 
	pol_issue_code, 
	pol_issue_code_descript, 
	pol_age, 
	industry_risk_grade_code, 
	industry_risk_grade_code_descript, 
	uw_review_yr, 
	mvr_request_code, 
	mvr_request_code_descript AS mvr_request_code_descript2, 
	renl_code, 
	renl_code_descript AS renl_code_descript2, 
	amend_num, 
	anniversary_rerate_code, 
	pol_audit_frqncy, 
	pol_audit_frqncy_descript AS pol_audit_frqncy_descript2, 
	final_audit_code, 
	zip_ind, 
	guarantee_ind, 
	variation_code, 
	variation_ind_descript, 
	county, 
	non_smoker_disc_code, 
	non_smoker_disc_code_descript AS non_smoker_disc_code_descript2, 
	renl_disc, 
	renl_safe_driver_disc_count, 
	nonrenewal_flag_date, 
	audit_complt_date, 
	orig_acct_date, 
	pol_enter_date, 
	excess_claim_code, 
	excess_claim_code_descript AS excess_claim_code_descript2, 
	pol_status_on_pif, 
	target_mrkt_code AS target_mrkt_code2, 
	target_mrkt_code_descript AS target_mrkt_code_descript2, 
	pkg_code AS pkg_code2, 
	mrktng_pkg_description, 
	pol_kind_code AS pol_kind_code2, 
	pol_kind_code_description AS pol_kind_code_description2, 
	bus_seg_code AS bus_seg_code2, 
	bus_seg_code_description AS bus_seg_code_description2, 
	pif_upload_audit_ind AS pif_upload_audit_ind2, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag2, 
	audit_id AS audit_id2, 
	eff_from_date, 
	eff_to_date AS eff_to_date2, 
	created_date AS created_date2, 
	modified_date AS modified_date2, 
	err_flag_bal_txn AS err_flag_bal_txn2, 
	err_flag_bal_reins AS err_flag_bal_reins2, 
	reporting_dvsn_code AS reporting_dvsn_code2, 
	class_of_business AS class_of_business2, 
	ClassOfBusinessCodeDescription AS ClassOfBusinessCodeDescription2, 
	ErrorFlagBalancePremiumTransaction AS ErrorFlagBalancePremiumTransaction2, 
	RenewalPolicyNumber AS RenewalPolicyNumber1, 
	RenewalPolicySymbol AS RenewalPolicySymbol1, 
	RenewalPolicyMod AS RenewalPolicyMod2, 
	BillingType AS BillingType2, 
	StandardInsuranceSegmentCode AS StandardInsuranceSegmentCode2, 
	StandardInsuranceSegmentDescription AS StandardInsuranceSegmentDescription2, 
	StandardBusinessClassCode AS StandardBusinessClassCode2, 
	StandardBusinessClassCodeDescription AS StandardBusinessClassCodeDescription2, 
	StandardPolicyTerm AS StandardPolicyTerm2, 
	StandardPolicyTermDescription AS StandardPolicyTermDescription2, 
	StandardPolicyStatusCode AS StandardPolicyStatusCode2, 
	StandardPolicyStatusCodeDescription AS StandardPolicyStatusCodeDescription2, 
	StandardPolicyIssueCode AS StandardPolicyIssueCode2, 
	StandardPolicyIssueCodeDescription AS StandardPolicyIssueCodeDescription2, 
	StandardIndustryRiskGradeCode AS StandardIndustryRiskGradeCode2, 
	StandardIndustryRiskGradeCodeDescription AS StandardIndustryRiskGradeCodeDescription2, 
	StandardPolicyAuditFrequency AS StandardPolicyAuditFrequency2, 
	StandardPolicyAuditFrequencyDescription AS StandardPolicyAuditFrequencyDescription2, 
	StandardStateAbbreviation AS StandardStateAbbreviation2, 
	StandardStateDescription AS StandardStateDescription2, 
	StandardSurchargeExemptCode AS StandardSurchargeExemptCode2, 
	StandardSurchargeExemptDescription AS StandardSurchargeExemptDescription2, 
	ProgramCode AS ProgramCode2, 
	ProgramDescription AS ProgramDescription2, 
	UnderwritingDivisionDimID AS UnderwritingDivisionDimID2, 
	AgencyEmployeeDimId AS AgencyEmployeeDimId2, 
	ObligeeName AS ObligeeName2, 
	AutomatedUnderwritingServicesIndicator AS AutomatedUnderwritingServicesIndicator2, 
	AutomaticRenewalIndicator AS AutomaticRenewalIndicator2, 
	AssociationCode, 
	AssociationDescription, 
	AssociationDiscountPercentage, 
	BusinessClassDimId AS BusinessClassDimId2, 
	RolloverPolicyIndicator AS RolloverPolicyIndicator2, 
	RolloverPriorCarrier AS RolloverPriorCarrier2, 
	MailToInsuredFlag AS MailToInsuredFlag2, 
	PolicyIssueCodeOverride AS PolicyIssueCodeOverride2, 
	DCBillFlag, 
	IssuedUWID AS IssuedUWID2, 
	IssuedUnderwriter AS IssuedUnderwriter2, 
	QuoteChannel AS QuoteChannel2, 
	QuoteChannelOrigin AS QuoteChannelOrigin2
	FROM RTR_INS_UPD_DEFAULT1
),
TGT_policy_dim_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim AS T
	USING UPD_policy_dim AS S
	ON T.pol_dim_id = S.lkp_pol_dim_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.eff_from_date = S.eff_from_date, T.modified_date = S.modified_date2, T.mco = S.mco, T.producer_code = S.producer_code2, T.pol_co_num = S.pol_co_num, T.pol_co_num_descript = S.pol_co_num_descript, T.pol_eff_date = S.pol_eff_date, T.pol_exp_date = S.pol_exp_date, T.orig_incptn_date = S.orig_incptn_date, T.prim_bus_class_code = S.StandardBusinessClassCode2, T.prim_bus_class_code_descript = S.StandardBusinessClassCodeDescription2, T.reins_code = S.reins_code, T.reins_code_descript = S.reins_code_descript2, T.pms_pol_lob_code = S.pms_pol_lob_code, T.pms_pol_lob_code_descript = S.pms_pol_lob_code_descript2, T.pol_co_line_code = S.pol_co_line_code, T.pol_co_line_code_descript = S.pol_co_line_code_descript, T.pol_cancellation_ind = S.pol_cancellation_ind, T.pol_cancellation_date = S.pol_cancellation_date, T.pol_cancellation_rsn_code = S.pol_cancellation_rsn_code, T.pol_cancellation_rsn_code_descript = S.pol_cancellation_rsn_code_descript2, T.state_of_domicile_code = S.state_of_domicile_code, T.state_of_domicile_abbrev = S.StandardStateAbbreviation2, T.state_of_domicile_code_descript = S.StandardStateDescription2, T.wbconnect_upload_code = S.wbconnect_upload_code, T.wbconnect_upload_code_descript = S.wbconnect_upload_code_descript2, T.serv_center_support_code = S.serv_center_support_code, T.serv_center_support_code_descript = S.serv_center_support_code_descript, T.pol_term = S.StandardPolicyTerm2, T.pol_term_descript = S.StandardPolicyTermDescription2, T.terrorism_risk_ind = S.terrorism_risk_ind, T.prior_pol_key = S.prior_pol_key2, T.pol_status_code = S.StandardPolicyStatusCode2, T.pol_status_code_descript = S.StandardPolicyStatusCodeDescription2, T.pol_count_type_code = S.pol_count_type_code2, T.pol_issue_code = S.StandardPolicyIssueCode2, T.pol_issue_code_descript = S.StandardPolicyIssueCodeDescription2, T.pol_age = S.pol_age, T.industry_risk_grade_code = S.StandardIndustryRiskGradeCode2, T.industry_risk_grade_code_descript = S.StandardIndustryRiskGradeCodeDescription2, T.uw_review_yr = S.uw_review_yr, T.mvr_request_code = S.mvr_request_code, T.mvr_request_code_descript = S.mvr_request_code_descript2, T.renl_code = S.renl_code, T.renl_code_descript = S.renl_code_descript2, T.amend_num = S.amend_num, T.anniversary_rerate_code = S.anniversary_rerate_code, T.pol_audit_frqncy = S.StandardPolicyAuditFrequency2, T.pol_audit_frqncy_descript = S.StandardPolicyAuditFrequencyDescription2, T.final_audit_code = S.final_audit_code, T.zip_ind = S.zip_ind, T.guarantee_ind = S.guarantee_ind, T.variation_code = S.variation_code, T.variation_code_descript = S.variation_ind_descript, T.county = S.county, T.non_smoker_disc_code = S.non_smoker_disc_code, T.non_smoker_disc_code_descript = S.non_smoker_disc_code_descript2, T.renl_disc = S.renl_disc, T.renl_safe_driver_disc_count = S.renl_safe_driver_disc_count, T.nonrenewal_flag_date = S.nonrenewal_flag_date, T.audit_complt_date = S.audit_complt_date, T.orig_acct_date = S.orig_acct_date, T.pol_enter_date = S.pol_enter_date, T.excess_claim_code = S.excess_claim_code, T.excess_claim_code_descript = S.excess_claim_code_descript2, T.pol_status_on_pif = S.pol_status_on_pif, T.target_mrkt_code = S.target_mrkt_code2, T.target_mrkt_code_descript = S.target_mrkt_code_descript2, T.pkg_code = S.pkg_code2, T.pkg_code_descript = S.mrktng_pkg_description, T.pol_kind_code = S.pol_kind_code2, T.bus_seg_code = S.bus_seg_code2, T.bus_seg_code_descript = S.bus_seg_code_description2, T.pif_upload_audit_ind = S.pif_upload_audit_ind2, T.err_flag_bal_txn = S.err_flag_bal_txn2, T.err_flag_bal_reins = S.err_flag_bal_reins2, T.reporting_dvsn_code = S.reporting_dvsn_code2, T.ClassOfBusinessCode = S.class_of_business2, T.ClassOfBusinessCodeDescription = S.ClassOfBusinessCodeDescription2, T.ErrorFlagBalancePremiumTransaction = S.ErrorFlagBalancePremiumTransaction2, T.RenewalPolicyNumber = S.RenewalPolicyNumber1, T.RenewalPolicySymbol = S.RenewalPolicySymbol1, T.RenewalPolicyMod = S.RenewalPolicyMod2, T.BillingType = S.BillingType2, T.SurchargeExemptCode = S.StandardSurchargeExemptCode2, T.SurchargeExemptDescription = S.StandardSurchargeExemptDescription2, T.ProgramCode = S.ProgramCode2, T.ProgramDescription = S.ProgramDescription2, T.UnderwritingDivisionDimId = S.UnderwritingDivisionDimID2, T.AgencyEmployeeDimID = S.AgencyEmployeeDimId2, T.ObligeeName = S.ObligeeName2, T.AutomatedUnderwritingServicesIndicator = S.AutomatedUnderwritingServicesIndicator2, T.AutomaticRenewalIndicator = S.AutomaticRenewalIndicator2, T.AssociationCode = S.AssociationCode, T.AssociationDescription = S.AssociationDescription, T.AssociationDiscountPercentage = S.AssociationDiscountPercentage, T.BusinessClassDimId = S.BusinessClassDimId2, T.RolloverPolicyIndicator = S.RolloverPolicyIndicator2, T.RolloverPriorCarrier = S.RolloverPriorCarrier2, T.MailToInsuredFlag = S.MailToInsuredFlag2, T.PolicyIssueCodeOverrideInd = S.PolicyIssueCodeOverride2, T.DCBillFlag = S.DCBillFlag, T.IssuedUWID = S.IssuedUWID2, T.IssuedUnderwriter = S.IssuedUnderwriter2, T.QuoteChannel = S.QuoteChannel2, T.QuoteChannelOrigin = S.QuoteChannelOrigin2
),
SQ_policy_dim AS (
	SELECT 
		pol_dim_id, 
		eff_from_date, 
		eff_to_date, 
		edw_pol_ak_id 
	FROM
		@{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim
	WHERE edw_pol_ak_id   IN 
		   (SELECT edw_pol_ak_id  FROM @{pipeline().parameters.TARGET_TABLE_OWNER}. policy_dim
	           WHERE crrnt_snpsht_flag = 1 GROUP BY edw_pol_ak_id   HAVING count(*) > 1)
	ORDER BY edw_pol_ak_id,eff_from_date  DESC
	
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	pol_dim_id,
	eff_from_date AS in_eff_from_date,
	eff_to_date AS orig_eff_to_date,
	edw_pol_ak_id,
	-- *INF*: DECODE(TRUE,
	-- edw_pol_ak_id = v_prev_edw_pol_ak_id  ,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(TRUE,
	edw_pol_ak_id = v_prev_edw_pol_ak_id, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	edw_pol_ak_id AS v_prev_edw_pol_ak_id,
	in_eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_policy_dim
),
FIL_FirstRowInAKGroup AS (
	SELECT
	pol_dim_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_policy_dim_expire AS (
	SELECT
	pol_dim_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_FirstRowInAKGroup
),
TGT_policy_dim_EXP_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim AS T
	USING UPD_policy_dim_expire AS S
	ON T.pol_dim_id = S.pol_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
SQ_policy_dim_updated AS (
	SELECT
		pol_dim_id,
		crrnt_snpsht_flag,
		audit_id,
		eff_from_date,
		eff_to_date,
		created_date,
		modified_date,
		edw_pol_pk_id,
		edw_pol_ak_id,
		pol_sym,
		pol_num,
		pol_mod,
		pol_key,
		mco,
		producer_code,
		pol_co_num,
		pol_co_num_descript,
		pol_eff_date,
		pol_exp_date,
		orig_incptn_date,
		prim_bus_class_code,
		prim_bus_class_code_descript,
		reins_code,
		reins_code_descript,
		pms_pol_lob_code,
		pms_pol_lob_code_descript,
		pol_co_line_code,
		pol_co_line_code_descript,
		pol_cancellation_ind,
		pol_cancellation_date,
		pol_cancellation_rsn_code,
		pol_cancellation_rsn_code_descript,
		state_of_domicile_code,
		state_of_domicile_abbrev,
		state_of_domicile_code_descript,
		wbconnect_upload_code,
		wbconnect_upload_code_descript,
		serv_center_support_code,
		serv_center_support_code_descript,
		pol_term,
		pol_term_descript,
		terrorism_risk_ind,
		prior_pol_key,
		pol_status_code,
		pol_status_code_descript,
		pol_count_type_code,
		pol_issue_code,
		pol_issue_code_descript,
		pol_age,
		industry_risk_grade_code,
		industry_risk_grade_code_descript,
		uw_review_yr,
		mvr_request_code,
		mvr_request_code_descript,
		renl_code,
		renl_code_descript,
		amend_num,
		anniversary_rerate_code,
		pol_audit_frqncy,
		pol_audit_frqncy_descript,
		final_audit_code,
		zip_ind,
		guarantee_ind,
		variation_code,
		variation_code_descript,
		county,
		non_smoker_disc_code,
		non_smoker_disc_code_descript,
		renl_disc,
		renl_safe_driver_disc_count,
		nonrenewal_flag_date,
		audit_complt_date,
		orig_acct_date,
		pol_enter_date,
		excess_claim_code,
		excess_claim_code_descript,
		pol_status_on_pif,
		target_mrkt_code,
		target_mrkt_code_descript,
		pkg_code,
		pkg_code_descript,
		pol_kind_code,
		bus_seg_code,
		bus_seg_code_descript,
		pif_upload_audit_ind,
		err_flag_bal_txn,
		err_flag_bal_reins,
		reporting_dvsn_code,
		ClassOfBusinessCode,
		ClassOfBusinessCodeDescription,
		ErrorFlagBalancePremiumTransaction,
		RenewalPolicyNumber,
		RenewalPolicySymbol,
		RenewalPolicyMod,
		BillingType,
		SurchargeExemptCode,
		SurchargeExemptDescription,
		ProgramCode,
		ProgramDescription,
		UnderwritingDivisionDimId,
		AgencyEmployeeDimID,
		ObligeeName,
		AutomatedUnderwritingServicesIndicator,
		AutomaticRenewalIndicator,
		AssociationCode,
		AssociationDescription,
		AssociationDiscountPercentage
	FROM policy_dim
	WHERE policy_dim.crrnt_snpsht_flag=1 AND policy_dim.modified_date>='@{pipeline().parameters.SELECTION_START_TS}'
),
UPD_Policy_Dim_UnderwritngDivisionDimId AS (
	SELECT
	edw_pol_ak_id, 
	UnderwritingDivisionDimId
	FROM SQ_policy_dim_updated
),
policy_dim_UNDERWRITER AS (
	UPDATE @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim SET UnderwritingDivisionDimId = S.UnderwritingDivisionDimId WHERE edw_pol_ak_id = S.edw_pol_ak_id
	FROM UPD_Policy_Dim_UnderwritngDivisionDimId S
),
SQ_policy_dim_updateExpiringPriorPolicy AS (
	select p.pol_key
	from @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim p 
	where  p.crrnt_snpsht_flag=1
		and (p.created_date>='@{pipeline().parameters.SELECTION_START_TS}' OR p.modified_date>='@{pipeline().parameters.SELECTION_START_TS}')
),
LKP_GetPriorPolicyKey_V2_Policy AS (
	SELECT
	PolicyKey,
	PreviousPolicyKey,
	in_PolicyKey
	FROM (
		select distinct P.pol_key as PolicyKey, P.prior_pol_key as PreviousPolicyKey 
		from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P with (nolock)
		where (P.created_date>='@{pipeline().parameters.SELECTION_START_TS}' OR P.modified_date>='@{pipeline().parameters.SELECTION_START_TS}')
		and P.prior_pol_key <> 'N/A' 
		and P.prior_pol_key is not null 
		and P.prior_pol_key <> ''
		and P.prior_pol_key <> P.pol_key
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY PolicyKey) = 1
),
LKP_Policy_Dim_DeriveExpiringPriorPolicyKey AS (
	SELECT
	pol_key,
	ExpiringPriorPolicyKey,
	PolicyKey
	FROM (
		Select P1.pol_key as ExpiringPriorPolicyKey,ISNULL(P2.pol_key,'N/A')  as PolicyKey from (SELECT  distinct
			 b.pol_key 
			,b.pol_num
			,b.pol_mod
		    ,dense_RANK() OVER (PARTITION BY b.pol_num order by b.pol_mod) as ranked  
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim  a
			inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim b on b.pol_num = a.pol_num and b.crrnt_snpsht_flag = 1
		where b.pol_cancellation_date <> b.pol_eff_date
		   and (a.created_date>='@{pipeline().parameters.SELECTION_START_TS}' OR a.modified_date>='@{pipeline().parameters.SELECTION_START_TS}')
		) P1
			Left join (SELECT  distinct
			 b.pol_key 
			,b.pol_num
			,b.pol_mod
		    ,dense_RANK() OVER (PARTITION BY b.pol_num order by b.pol_mod) as ranked  
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim  a
			inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim b on b.pol_num = a.pol_num and b.crrnt_snpsht_flag = 1
		where b.pol_cancellation_date <> b.pol_eff_date
		   and (a.created_date>='@{pipeline().parameters.SELECTION_START_TS}' OR a.modified_date>='@{pipeline().parameters.SELECTION_START_TS}')
		) P2 on P1.pol_num = P2.pol_num
		where
			P1.ranked = P2.Ranked -1
		order by 1,2
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY pol_key) = 1
),
LKP_Policy_Dim_GetExpiringPriorPolicyKey AS (
	SELECT
	pol_key,
	PolicyKey,
	ExpiringPriorPolicyKey
	FROM (
		SELECT Distinct
		 P2.pol_key     as PolicyKey
		,P2.ExpiringPriorPolicyKey as ExpiringPriorPolicyKey
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim P1
		inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim P2 on P1.Pol_Key = P2.Pol_Key
		where P2.ExpiringPriorPolicyKey <> 'N/A'
		and P2.ExpiringPriorPolicyKey is not  null
		and (P1.created_date>='@{pipeline().parameters.SELECTION_START_TS}' OR P1.modified_date>='@{pipeline().parameters.SELECTION_START_TS}')
		order by P2.pol_key
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY pol_key) = 1
),
Exp_CollectdifferentExpPolicyKey AS (
	SELECT
	SQ_policy_dim_updateExpiringPriorPolicy.pol_key AS PolicyKey,
	LKP_Policy_Dim_GetExpiringPriorPolicyKey.ExpiringPriorPolicyKey AS lkp_GetExpiringPriorPolicyKey_Step1,
	LKP_Policy_Dim_DeriveExpiringPriorPolicyKey.ExpiringPriorPolicyKey AS lkp_DeriveExpiringPriorPolicyKey_Step2,
	LKP_GetPriorPolicyKey_V2_Policy.PreviousPolicyKey AS lkp_GetPriorPolicyKey_V2_Policy_Step4,
	-- *INF*: IIF(ISNULL(lkp_GetPriorPolicyKey_V2_Policy_Step4) OR lkp_GetPriorPolicyKey_V2_Policy_Step4='' OR IS_SPACES(lkp_GetPriorPolicyKey_V2_Policy_Step4),'N/A',lkp_GetPriorPolicyKey_V2_Policy_Step4)
	IFF(lkp_GetPriorPolicyKey_V2_Policy_Step4 IS NULL OR lkp_GetPriorPolicyKey_V2_Policy_Step4 = '' OR IS_SPACES(lkp_GetPriorPolicyKey_V2_Policy_Step4), 'N/A', lkp_GetPriorPolicyKey_V2_Policy_Step4) AS o_GetPriorPolicyKey_V2_Policy_Step4,
	-- *INF*: DECODE(
	-- True,
	-- (Not isnull(lkp_GetExpiringPriorPolicyKey_Step1) AND lkp_GetExpiringPriorPolicyKey_Step1  != 'N/A' AND lkp_GetExpiringPriorPolicyKey_Step1 <> '' and NOT IS_SPACES(lkp_GetExpiringPriorPolicyKey_Step1 ) ), lkp_GetExpiringPriorPolicyKey_Step1,
	-- 
	-- (not isnull(lkp_DeriveExpiringPriorPolicyKey_Step2) and lkp_DeriveExpiringPriorPolicyKey_Step2  !=  'N/A' AND lkp_DeriveExpiringPriorPolicyKey_Step2 <> '' and NOT IS_SPACES(lkp_DeriveExpiringPriorPolicyKey_Step2) ), lkp_DeriveExpiringPriorPolicyKey_Step2
	-- ,'N/A')
	DECODE(True,
	( NOT lkp_GetExpiringPriorPolicyKey_Step1 IS NULL AND lkp_GetExpiringPriorPolicyKey_Step1 != 'N/A' AND lkp_GetExpiringPriorPolicyKey_Step1 <> '' AND NOT IS_SPACES(lkp_GetExpiringPriorPolicyKey_Step1) ), lkp_GetExpiringPriorPolicyKey_Step1,
	( NOT lkp_DeriveExpiringPriorPolicyKey_Step2 IS NULL AND lkp_DeriveExpiringPriorPolicyKey_Step2 != 'N/A' AND lkp_DeriveExpiringPriorPolicyKey_Step2 <> '' AND NOT IS_SPACES(lkp_DeriveExpiringPriorPolicyKey_Step2) ), lkp_DeriveExpiringPriorPolicyKey_Step2,
	'N/A') AS o_ExpiringPriorPolicyKey
	FROM SQ_policy_dim_updateExpiringPriorPolicy
	LEFT JOIN LKP_GetPriorPolicyKey_V2_Policy
	ON LKP_GetPriorPolicyKey_V2_Policy.PolicyKey = SQ_policy_dim_updateExpiringPriorPolicy.pol_key
	LEFT JOIN LKP_Policy_Dim_DeriveExpiringPriorPolicyKey
	ON LKP_Policy_Dim_DeriveExpiringPriorPolicyKey.PolicyKey = SQ_policy_dim_updateExpiringPriorPolicy.pol_key
	LEFT JOIN LKP_Policy_Dim_GetExpiringPriorPolicyKey
	ON LKP_Policy_Dim_GetExpiringPriorPolicyKey.PolicyKey = SQ_policy_dim_updateExpiringPriorPolicy.pol_key
),
SQ_policy_dim_updateExpiringPriorPolicyCancelRewrite AS (
	WITH ExpiringPolicyKey
	AS (
		SELECT DISTINCT C.cust_num AS CustomerNumber
			,S.StrategicProfitCenterAbbreviation
			,ISG.InsuranceSegmentAbbreviation
			,PO.PolicyOfferingAbbreviation
			,P.pol_key AS PolicyKey
			,P.pol_eff_date AS PolicyEffDate
			,CASE 
				WHEN P.pol_cancellation_date < P.pol_exp_date
					THEN P.pol_cancellation_date
				ELSE P.pol_exp_date
				END AS MinPolExpDtPolCancelDt
			,DENSE_RANK() OVER (
				PARTITION BY C.Cust_num
				,s.StrategicProfitCenterAbbreviation
				,isg.InsuranceSegmentAbbreviation
				,po.PolicyOfferingAbbreviation ORDER BY p.pol_eff_date
					,CASE 
						WHEN P.pol_cancellation_date < P.pol_exp_date
							THEN P.pol_cancellation_date
						ELSE P.pol_exp_date
						END
				) AS PolicyExpirationRenewOrder
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P1 WITH (NOLOCK)
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Policy P WITH (NOLOCK) ON P1.pol_ak_id = P.pol_ak_id
			AND P.crrnt_snpsht_flag = 1
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer C WITH (NOLOCK) ON C.contract_cust_ak_id = P.contract_cust_ak_id
			AND P.crrnt_snpsht_flag = 1
			AND C.crrnt_snpsht_flag = 1
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering PO WITH (NOLOCK) ON PO.PolicyOfferingAKId = P.PolicyOfferingAKId
			AND P.crrnt_snpsht_flag = 1
			AND PO.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter S WITH (NOLOCK) ON S.StrategicProfitCenterAKId = P.StrategicProfitCenterAKId
			AND P.crrnt_snpsht_flag = 1
			AND S.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISG WITH (NOLOCK) ON ISG.InsuranceSegmentAKId = P.InsuranceSegmentAKId
			AND P.crrnt_snpsht_flag = 1
			AND ISG.CurrentSnapshotFlag = 1
		WHERE  P.pol_cancellation_date <> P.pol_eff_date
				AND P.pol_ak_id IN (
				SELECT distinct P3.pol_ak_id
				FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P3 WITH (NOLOCK)
				INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer C2 WITH (NOLOCK) ON C2.contract_cust_ak_id = P3.contract_cust_ak_id
					--AND P3.crrnt_snpsht_flag = 1
					--AND C2.crrnt_snpsht_flag = 1
				WHERE p3.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
					OR p3.modified_date >= '@{pipeline().parameters.SELECTION_START_TS}'
					AND c2.cust_num = c.cust_num
				)
			
		)
	
	SELECT DISTINCT 
		T1.PolicyKey
	 	,CASE WHEN T2.MinPolExpDtPolCancelDt=T1.PolicyEffDate THEN ISNULL(T2.PolicyKey, 'N/A') END as ExpiringPolicyKey
		
		
	FROM ExpiringPolicyKey T1
	LEFT JOIN ExpiringPolicyKey T2 ON T1.CustomerNumber = T2.CustomerNumber
		AND T1.StrategicProfitCenterAbbreviation = T2.StrategicProfitCenterAbbreviation
		AND T1.PolicyOfferingAbbreviation = T2.PolicyOfferingAbbreviation
		AND T1.InsuranceSegmentAbbreviation = T2.InsuranceSegmentAbbreviation
		AND T2.PolicyExpirationRenewOrder = T1.PolicyExpirationRenewOrder - 1
	ORDER BY policyKey
),
Jnr_ExpiringPolicyderivationJoiner AS (SELECT
	Exp_CollectdifferentExpPolicyKey.PolicyKey AS Pol_Key, 
	Exp_CollectdifferentExpPolicyKey.o_GetPriorPolicyKey_V2_Policy_Step4 AS GetPriorPolicyKey_V2_Policy_Step4, 
	Exp_CollectdifferentExpPolicyKey.o_ExpiringPriorPolicyKey AS ExpiringPriorPolicyKey_Step1_2, 
	SQ_policy_dim_updateExpiringPriorPolicyCancelRewrite.pol_key AS Pol_Key_Step3, 
	SQ_policy_dim_updateExpiringPriorPolicyCancelRewrite.ExpiringPolicyKey AS ExpiringPriorPolicyKey_Step3
	FROM SQ_policy_dim_updateExpiringPriorPolicyCancelRewrite
	INNER JOIN Exp_CollectdifferentExpPolicyKey
	ON Exp_CollectdifferentExpPolicyKey.PolicyKey = SQ_policy_dim_updateExpiringPriorPolicyCancelRewrite.pol_key
),
Exp_FinalDerivationOfExpiringPolicyKey AS (
	SELECT
	Pol_Key AS PolicyKey,
	GetPriorPolicyKey_V2_Policy_Step4 AS ExpiringPriorPolicyKey_Step4,
	ExpiringPriorPolicyKey_Step1_2,
	ExpiringPriorPolicyKey_Step3,
	-- *INF*: DECODE(
	-- True,
	-- 
	-- (Not isnull(ExpiringPriorPolicyKey_Step1_2) and ExpiringPriorPolicyKey_Step1_2 != 'N/A' AND ExpiringPriorPolicyKey_Step1_2 <> '' and NOT IS_SPACES(ExpiringPriorPolicyKey_Step1_2) ), ExpiringPriorPolicyKey_Step1_2,
	-- 
	-- (not isnull(ExpiringPriorPolicyKey_Step3) and ExpiringPriorPolicyKey_Step3  !=  'N/A' AND ExpiringPriorPolicyKey_Step3 <> '' and NOT IS_SPACES(ExpiringPriorPolicyKey_Step3) ), ExpiringPriorPolicyKey_Step3,
	-- 
	-- (not isnull(ExpiringPriorPolicyKey_Step4) and ExpiringPriorPolicyKey_Step4  !=  'N/A' AND ExpiringPriorPolicyKey_Step4 <> '' and NOT IS_SPACES(ExpiringPriorPolicyKey_Step4)), ExpiringPriorPolicyKey_Step4,
	-- 
	-- 'N/A')
	DECODE(True,
	( NOT ExpiringPriorPolicyKey_Step1_2 IS NULL AND ExpiringPriorPolicyKey_Step1_2 != 'N/A' AND ExpiringPriorPolicyKey_Step1_2 <> '' AND NOT IS_SPACES(ExpiringPriorPolicyKey_Step1_2) ), ExpiringPriorPolicyKey_Step1_2,
	( NOT ExpiringPriorPolicyKey_Step3 IS NULL AND ExpiringPriorPolicyKey_Step3 != 'N/A' AND ExpiringPriorPolicyKey_Step3 <> '' AND NOT IS_SPACES(ExpiringPriorPolicyKey_Step3) ), ExpiringPriorPolicyKey_Step3,
	( NOT ExpiringPriorPolicyKey_Step4 IS NULL AND ExpiringPriorPolicyKey_Step4 != 'N/A' AND ExpiringPriorPolicyKey_Step4 <> '' AND NOT IS_SPACES(ExpiringPriorPolicyKey_Step4) ), ExpiringPriorPolicyKey_Step4,
	'N/A') AS o_ExpiringPriorPolicyKey
	FROM Jnr_ExpiringPolicyderivationJoiner
),
UPD_Policy_Dim_UpdateExpiringPriorPolicy AS (
	SELECT
	PolicyKey AS pol_key, 
	o_ExpiringPriorPolicyKey AS ExpiringPriorPolicyKey
	FROM Exp_FinalDerivationOfExpiringPolicyKey
),
policy_dim_ExpiringPriorPolicy AS (
	UPDATE @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim SET ExpiringPriorPolicyKey = S.ExpiringPriorPolicyKey WHERE pol_key = S.pol_key
	FROM UPD_Policy_Dim_UpdateExpiringPriorPolicy S
),