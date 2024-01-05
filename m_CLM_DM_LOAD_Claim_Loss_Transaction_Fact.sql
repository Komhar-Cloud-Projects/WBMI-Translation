WITH
LKP_Claim_Transaction AS (
	SELECT
	claimant_cov_det_ak_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability,
	financial_type_code,
	trans_code,
	trans_date
	FROM (
		SELECT 
			claimant_cov_det_ak_id,
			cause_of_loss,
			reserve_ctgry,
			type_disability,
			financial_type_code,
			trans_code,
			trans_date
		FROM claim_transaction
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,cause_of_loss,reserve_ctgry,type_disability,financial_type_code,trans_code,trans_date ORDER BY claimant_cov_det_ak_id) = 1
),
LKP_calender_dim AS (
	SELECT
	clndr_id,
	clndr_date
	FROM (
		SELECT 
			clndr_id,
			clndr_date
		FROM calendar_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_date ORDER BY clndr_id) = 1
),
LKP_Claim_payment_dim AS (
	SELECT
	claim_pay_dim_id,
	edw_claim_pay_ak_id,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
			claim_pay_dim_id,
			edw_claim_pay_ak_id,
			eff_from_date,
			eff_to_date
		FROM claim_payment_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_pay_ak_id,eff_from_date,eff_to_date ORDER BY claim_pay_dim_id DESC) = 1
),
LKP_claim_master_1099_list_dim AS (
	SELECT
	claim_master_1099_list_dim_id,
	edw_claim_master_1099_list_ak_id
	FROM (
		SELECT 
		claim_master_1099_list_dim.claim_master_1099_list_dim_id as claim_master_1099_list_dim_id, 
		claim_master_1099_list_dim.edw_claim_master_1099_list_ak_id as edw_claim_master_1099_list_ak_id 
		FROM 
		claim_master_1099_list_dim
		where
		claim_master_1099_list_dim.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_master_1099_list_ak_id ORDER BY claim_master_1099_list_dim_id) = 1
),
LKP_claim_rep_dim_alt AS (
	SELECT
	claim_rep_dim_id,
	claimant_cov_det_ak_id
	FROM (
		SELECT 
		crd.claim_rep_dim_id as claim_rep_dim_id,
		crd.eff_from_date as eff_from_date,
		crd.eff_to_date as eff_to_date,
		ccd.claimant_cov_det_ak_id as claimant_cov_det_ak_id
		FROM   RPT_EDM..claim_transaction CT
		join RPT_EDM..claimant_coverage_detail ccd on ct.claimant_cov_det_ak_id = ccd.claimant_cov_det_ak_id and ccd.crrnt_snpsht_flag = 1 
		and ccd.eff_from_date <= '12/31/2100 23:59:59' and ccd.eff_to_date >= '12/31/2100 23:59:59'
		join RPT_EDM..claim_party_occurrence cpo on cpo.claim_party_occurrence_ak_id = ccd.claim_party_occurrence_ak_id and cpo.crrnt_snpsht_flag = 1
		and cpo.eff_from_date <= '12/31/2100 23:59:59' and cpo.eff_to_date >= '12/31/2100 23:59:59'
		join RPT_EDM..claim_occurrence co on co.claim_occurrence_ak_id = cpo.claim_occurrence_ak_id and co.crrnt_snpsht_flag = 1
		and co.eff_from_date <= '12/31/2100 23:59:59' and co.eff_to_date >= '12/31/2100 23:59:59'
		join RPT_EDM..claim_representative_occurrence cro on cro.claim_occurrence_ak_id = co.claim_occurrence_ak_id and cro.crrnt_snpsht_flag =1
		and cro.eff_from_date <= '12/31/2100 23:59:59' and cro.eff_to_date >= '12/31/2100 23:59:59' and LTRIM(RTRIM(cro.claim_rep_role_code))  = 'H'
		join WC_Data_Mart..claim_representative_dim crd on cro.claim_rep_ak_id = crd.edw_claim_rep_ak_id
		and crd.eff_from_date <= '12/31/2100 23:59:59' and crd.eff_to_date >= '12/31/2100 23:59:59'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id ORDER BY claim_rep_dim_id DESC) = 1
),
SQ_claim_transaction AS (
	SELECT CT.claim_trans_id,
	       CT.claimant_cov_det_ak_id,
	       CT.claim_pay_ak_id,
	       CT.cause_of_loss,
	       CT.reserve_ctgry,
	       CT.type_disability,
	       CT.offset_onset_ind,
	       CT.financial_type_code,
	       CT.s3p_trans_code, 
	       CT.pms_trans_code,
	       CT.trans_code,
	       CT.trans_date,
	       CT.pms_acct_entered_date,
	       CT.trans_base_type_code,
	       CT.trans_ctgry_code,
	       CT.trans_amt,
	       CT.trans_hist_amt,
	       CT.trans_rsn,
	       CT.reprocess_date,
	       CT.trans_entry_oper_id,
	       CT.source_sys_id,
	       CT.tax_id,
	       CT.claim_master_1099_list_ak_id
	FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_transaction CT
	WHERE CT.trans_offset_onset_ind in ('N','N/A')
	AND CT.claim_trans_id % @{pipeline().parameters.NUM_OF_PARTITIONS} = 1
	
	--- We need to pull only Onset Transactions into Claim_Loss_Transaction_Fact
),
EXP_Default AS (
	SELECT
	claim_trans_id,
	claimant_cov_det_ak_id,
	claim_pay_ak_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability,
	offset_onset_ind,
	financial_type_code,
	trans_code,
	trans_date,
	pms_acct_entered_date,
	trans_base_type_code,
	trans_ctgry_code,
	trans_amt,
	trans_hist_amt,
	trans_rsn,
	reprocess_date,
	trans_entry_oper_id,
	source_sys_id,
	tax_id,
	claim_master_1099_list_ak_id,
	s3p_trans_code,
	pms_trans_code
	FROM SQ_claim_transaction
),
EXP_get_values AS (
	SELECT
	claim_trans_id,
	claimant_cov_det_ak_id,
	claim_pay_ak_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability,
	offset_onset_ind,
	financial_type_code,
	trans_code,
	trans_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS v_trans_date,
	-- *INF*: SET_DATE_PART(
	--   SET_DATE_PART(
	--     SET_DATE_PART(LAST_DAY(trans_date),'HH',23),
	--   'MI',59),
	-- 'SS',59)
	SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(LAST_DAY(trans_date), 'HH', 23), 'MI', 59), 'SS', 59) AS OUT_trans_date_month_end,
	v_trans_date AS trans_date_out,
	pms_acct_entered_date,
	trans_base_type_code,
	trans_ctgry_code,
	trans_amt,
	trans_hist_amt,
	trans_rsn,
	reprocess_date,
	trans_entry_oper_id,
	source_sys_id,
	-- *INF*: :LKP.LKP_CLAIM_PAYMENT_DIM(claim_pay_ak_id, v_trans_date)
	LKP_CLAIM_PAYMENT_DIM_claim_pay_ak_id_v_trans_date.claim_pay_dim_id AS v_claim_payment_dim_id,
	v_claim_payment_dim_id AS claim_payment_dim_id,
	-- *INF*: :LKP.LKP_CLAIM_PAYMENT_DIM(claim_pay_ak_id, trans_date)
	LKP_CLAIM_PAYMENT_DIM_claim_pay_ak_id_trans_date.claim_pay_dim_id AS v_claim_payment_dim_hist_id,
	v_claim_payment_dim_hist_id AS claim_payment_dim_hist_id,
	tax_id AS IN_tax_id,
	claim_master_1099_list_ak_id AS IN_claim_master_1099_list_ak_id,
	-- *INF*: LTRIM(RTRIM(IN_tax_id))
	LTRIM(RTRIM(IN_tax_id)) AS v_tax_id,
	-- *INF*: :LKP.LKP_CLAIM_MASTER_1099_LIST_DIM(IN_claim_master_1099_list_ak_id)
	LKP_CLAIM_MASTER_1099_LIST_DIM_IN_claim_master_1099_list_ak_id.claim_master_1099_list_dim_id AS v_claim_master_1099_list_dim_id,
	v_tax_id AS tax_id,
	v_claim_master_1099_list_dim_id AS claim_master_1099_list_dim_id,
	'D' AS trans_kind_code,
	s3p_trans_code,
	pms_trans_code
	FROM EXP_Default
	LEFT JOIN LKP_CLAIM_PAYMENT_DIM LKP_CLAIM_PAYMENT_DIM_claim_pay_ak_id_v_trans_date
	ON LKP_CLAIM_PAYMENT_DIM_claim_pay_ak_id_v_trans_date.edw_claim_pay_ak_id = claim_pay_ak_id
	AND LKP_CLAIM_PAYMENT_DIM_claim_pay_ak_id_v_trans_date.eff_from_date = v_trans_date

	LEFT JOIN LKP_CLAIM_PAYMENT_DIM LKP_CLAIM_PAYMENT_DIM_claim_pay_ak_id_trans_date
	ON LKP_CLAIM_PAYMENT_DIM_claim_pay_ak_id_trans_date.edw_claim_pay_ak_id = claim_pay_ak_id
	AND LKP_CLAIM_PAYMENT_DIM_claim_pay_ak_id_trans_date.eff_from_date = trans_date

	LEFT JOIN LKP_CLAIM_MASTER_1099_LIST_DIM LKP_CLAIM_MASTER_1099_LIST_DIM_IN_claim_master_1099_list_ak_id
	ON LKP_CLAIM_MASTER_1099_LIST_DIM_IN_claim_master_1099_list_ak_id.edw_claim_master_1099_list_ak_id = IN_claim_master_1099_list_ak_id

),
LKP_ClaimFeature AS (
	SELECT
	ClaimRepresentativeAkId,
	FeatureRepresentativeAssignedDate,
	claimant_cov_det_ak_id
	FROM (
		SELECT CF.ClaimRepresentativeAkId as ClaimRepresentativeAkId, 
		CF.FeatureRepresentativeAssignedDate as FeatureRepresentativeAssignedDate, 
		CCD.claimant_cov_det_ak_id as claimant_cov_det_ak_id 
		FROM ClaimFeature CF
		INNER JOIN dbo.claimant_coverage_detail CCD
		ON CF.ClaimPartyOccurrenceAKId = CCD.claim_party_occurrence_ak_id
			and CF.MajorPerilCode = CCD.major_peril_code
			and CF.CauseOfLoss = CCD.cause_of_loss
			and CF.ReserveCategory = CCD.reserve_ctgry
			and CF.CurrentSnapshotFlag = 1
			and CCD.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id ORDER BY ClaimRepresentativeAkId DESC) = 1
),
LKP_claim_financial_type_dim AS (
	SELECT
	claim_financial_type_dim_id,
	financial_type_code
	FROM (
		SELECT 
			claim_financial_type_dim_id,
			financial_type_code
		FROM claim_financial_type_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY financial_type_code ORDER BY claim_financial_type_dim_id) = 1
),
LKP_claim_rep_dim_id AS (
	SELECT
	claim_rep_dim_id,
	edw_claim_rep_ak_id
	FROM (
		SELECT 
			claim_rep_dim_id,
			edw_claim_rep_ak_id
		FROM claim_representative_dim
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_rep_ak_id ORDER BY claim_rep_dim_id) = 1
),
LKP_claim_subrogation_dim AS (
	SELECT
	claim_subrogation_dim_id,
	referred_to_subrogation_date,
	pay_start_date,
	closure_date,
	edw_claimant_cov_det_ak_id,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
			claim_subrogation_dim_id,
			referred_to_subrogation_date,
			pay_start_date,
			closure_date,
			edw_claimant_cov_det_ak_id,
			eff_from_date,
			eff_to_date
		FROM claim_subrogation_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claimant_cov_det_ak_id,eff_from_date,eff_to_date ORDER BY claim_subrogation_dim_id) = 1
),
LKP_claim_transaction_type_dim AS (
	SELECT
	claim_trans_type_dim_id,
	trans_ctgry_code,
	trans_code,
	s3p_trans_code,
	pms_trans_code,
	trans_base_type_code,
	trans_rsn,
	trans_kind_code,
	offset_onset_ind,
	type_disability
	FROM (
		SELECT 
			claim_trans_type_dim_id,
			trans_ctgry_code,
			trans_code,
			s3p_trans_code,
			pms_trans_code,
			trans_base_type_code,
			trans_rsn,
			trans_kind_code,
			offset_onset_ind,
			type_disability
		FROM claim_transaction_type_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY trans_ctgry_code,trans_code,s3p_trans_code,pms_trans_code,trans_base_type_code,trans_rsn,trans_kind_code,offset_onset_ind,type_disability ORDER BY claim_trans_type_dim_id DESC) = 1
),
mplt_ClaimReserveDim AS (WITH
	INPUT AS (
		
	),
	EXP_Get_Values AS (
		SELECT
		claimant_coverage_detail_ak_id AS ClaimantCoverageDetailAkId,
		financial_type_code AS in_FinancialTypeCode,
		-- *INF*: RTRIM(in_FinancialTypeCode)
		RTRIM(in_FinancialTypeCode) AS out_FinancialTypeCode
		FROM INPUT
	),
	LKP_Existing_Reserve AS (
		SELECT
		ClaimReserveDimId,
		ReserveOpenDate,
		ReserveCloseDate,
		ReserveReopenDate,
		ReserveCloseAfterReopenDate,
		FirstPaymentDate,
		EDWClaimantCoverageDetailAKId,
		FinancialTypeCode
		FROM (
			SELECT 
				ClaimReserveDimId,
				ReserveOpenDate,
				ReserveCloseDate,
				ReserveReopenDate,
				ReserveCloseAfterReopenDate,
				FirstPaymentDate,
				EDWClaimantCoverageDetailAKId,
				FinancialTypeCode
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.ClaimReserveDim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWClaimantCoverageDetailAKId,FinancialTypeCode ORDER BY ClaimReserveDimId DESC) = 1
	),
	OUTPUT AS (
		SELECT
		ClaimReserveDimId, 
		ReserveOpenDate, 
		ReserveCloseDate, 
		ReserveReopenDate, 
		ReserveCloseAfterReopenDate, 
		FirstPaymentDate
		FROM LKP_Existing_Reserve
	),
),
mplt_claimant_coverage_dim_hist_id AS (WITH
	INPUT AS (
		
	),
	EXP_get_values AS (
		SELECT
		IN_claimant_cov_det_ak_id,
		IN_trans_date
		FROM INPUT
	),
	LKP_CLAIMANT_COV_DIM AS (
		SELECT
		claimant_cov_dim_id,
		edw_claimant_cov_det_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claimant_cov_dim_id,
				edw_claimant_cov_det_ak_id,
				eff_from_date,
				eff_to_date
			FROM claimant_coverage_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claimant_cov_det_ak_id,eff_from_date,eff_to_date ORDER BY claimant_cov_dim_id DESC) = 1
	),
	OUTPUT AS (
		SELECT
		claimant_cov_dim_id
		FROM LKP_CLAIMANT_COV_DIM
	),
),
mplt_claimant_coverage_dim_id AS (WITH
	INPUT AS (
		
	),
	EXP_get_values AS (
		SELECT
		IN_claimant_cov_det_ak_id,
		IN_trans_date
		FROM INPUT
	),
	LKP_CLAIMANT_COV_DIM AS (
		SELECT
		claimant_cov_dim_id,
		edw_claimant_cov_det_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claimant_cov_dim_id,
				edw_claimant_cov_det_ak_id,
				eff_from_date,
				eff_to_date
			FROM claimant_coverage_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claimant_cov_det_ak_id,eff_from_date,eff_to_date ORDER BY claimant_cov_dim_id DESC) = 1
	),
	OUTPUT AS (
		SELECT
		claimant_cov_dim_id
		FROM LKP_CLAIMANT_COV_DIM
	),
),
mplt_coverage_dim_hist_id AS (WITH
	INPUT AS (
		
	),
	EXP_get_values AS (
		SELECT
		IN_claimant_cov_det_ak_id,
		IN_trans_date
		FROM INPUT
	),
	LKP_coverage_dim AS (
		SELECT
		cov_dim_id,
		edw_claimant_cov_det_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				cov_dim_id,
				edw_claimant_cov_det_ak_id,
				eff_from_date,
				eff_to_date
			FROM coverage_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claimant_cov_det_ak_id,eff_from_date,eff_to_date ORDER BY cov_dim_id DESC) = 1
	),
	OUTPUT AS (
		SELECT
		cov_dim_id
		FROM LKP_coverage_dim
	),
),
mplt_coverage_dim_id AS (WITH
	INPUT AS (
		
	),
	EXP_get_values AS (
		SELECT
		IN_claimant_cov_det_ak_id,
		IN_trans_date
		FROM INPUT
	),
	LKP_coverage_dim AS (
		SELECT
		cov_dim_id,
		edw_claimant_cov_det_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				cov_dim_id,
				edw_claimant_cov_det_ak_id,
				eff_from_date,
				eff_to_date
			FROM coverage_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claimant_cov_det_ak_id,eff_from_date,eff_to_date ORDER BY cov_dim_id DESC) = 1
	),
	OUTPUT AS (
		SELECT
		cov_dim_id
		FROM LKP_coverage_dim
	),
),
EXP_Cov_Level_dim_ids AS (
	SELECT
	mplt_claimant_coverage_dim_id.claimant_cov_dim_id,
	mplt_claimant_coverage_dim_hist_id.claimant_cov_dim_id AS claimant_cov_dim_hist_id,
	mplt_coverage_dim_id.cov_dim_id,
	mplt_coverage_dim_hist_id.cov_dim_id AS cov_dim_hist_id,
	LKP_claim_transaction_type_dim.claim_trans_type_dim_id,
	LKP_claim_financial_type_dim.claim_financial_type_dim_id,
	EXP_get_values.claim_payment_dim_id,
	EXP_get_values.claim_payment_dim_hist_id,
	EXP_get_values.tax_id,
	EXP_get_values.claim_master_1099_list_dim_id,
	LKP_claim_subrogation_dim.claim_subrogation_dim_id,
	EXP_get_values.trans_date,
	EXP_get_values.OUT_trans_date_month_end AS trans_date_month_end,
	EXP_get_values.claim_trans_id,
	EXP_get_values.claimant_cov_det_ak_id AS IN_claimant_cov_det_ak_id,
	EXP_get_values.cause_of_loss AS IN_cause_of_loss,
	EXP_get_values.reserve_ctgry AS IN_reserve_ctgry,
	EXP_get_values.type_disability AS IN_type_disability,
	EXP_get_values.financial_type_code,
	EXP_get_values.trans_code,
	EXP_get_values.trans_ctgry_code,
	EXP_get_values.trans_amt,
	EXP_get_values.trans_hist_amt,
	EXP_get_values.source_sys_id,
	LKP_claim_subrogation_dim.referred_to_subrogation_date,
	LKP_claim_subrogation_dim.pay_start_date,
	LKP_claim_subrogation_dim.closure_date,
	EXP_get_values.claim_pay_ak_id,
	EXP_get_values.reprocess_date,
	EXP_get_values.trans_date_out,
	EXP_get_values.pms_acct_entered_date,
	mplt_ClaimReserveDim.ClaimReserveDimId,
	LKP_claim_rep_dim_id.claim_rep_dim_id AS FeatureRepresentativeDimId,
	LKP_ClaimFeature.FeatureRepresentativeAssignedDate
	FROM EXP_get_values
	 -- Manually join with mplt_ClaimReserveDim
	 -- Manually join with mplt_claimant_coverage_dim_hist_id
	 -- Manually join with mplt_claimant_coverage_dim_id
	 -- Manually join with mplt_coverage_dim_hist_id
	 -- Manually join with mplt_coverage_dim_id
	LEFT JOIN LKP_ClaimFeature
	ON LKP_ClaimFeature.claimant_cov_det_ak_id = EXP_get_values.claimant_cov_det_ak_id
	LEFT JOIN LKP_claim_financial_type_dim
	ON LKP_claim_financial_type_dim.financial_type_code = EXP_get_values.financial_type_code
	LEFT JOIN LKP_claim_rep_dim_id
	ON LKP_claim_rep_dim_id.edw_claim_rep_ak_id = LKP_ClaimFeature.ClaimRepresentativeAkId
	LEFT JOIN LKP_claim_subrogation_dim
	ON LKP_claim_subrogation_dim.edw_claimant_cov_det_ak_id = EXP_get_values.claimant_cov_det_ak_id AND LKP_claim_subrogation_dim.eff_from_date <= EXP_get_values.trans_date_out AND LKP_claim_subrogation_dim.eff_to_date >= EXP_get_values.trans_date_out
	LEFT JOIN LKP_claim_transaction_type_dim
	ON LKP_claim_transaction_type_dim.trans_ctgry_code = EXP_get_values.trans_ctgry_code AND LKP_claim_transaction_type_dim.trans_code = EXP_get_values.trans_code AND LKP_claim_transaction_type_dim.s3p_trans_code = EXP_get_values.s3p_trans_code AND LKP_claim_transaction_type_dim.pms_trans_code = EXP_get_values.pms_trans_code AND LKP_claim_transaction_type_dim.trans_base_type_code = EXP_get_values.trans_base_type_code AND LKP_claim_transaction_type_dim.trans_rsn = EXP_get_values.trans_rsn AND LKP_claim_transaction_type_dim.trans_kind_code = EXP_get_values.trans_kind_code AND LKP_claim_transaction_type_dim.offset_onset_ind = EXP_get_values.offset_onset_ind AND LKP_claim_transaction_type_dim.type_disability = EXP_get_values.type_disability
),
mplt_Claim_Payment_Category_type_Dim_hist_id AS (WITH
	Input AS (
		
	),
	EXP_Get_Values AS (
		SELECT
		IN_Claim_Pay_AK_ID,
		IN_trans_date
		FROM Input
	),
	LKP_Claim_Payment_Category AS (
		SELECT
		claim_pay_ctgry_ak_id,
		claim_pay_ak_id,
		claim_pay_ctgry_type,
		claim_pay_ctgry_lump_sum_ind,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT claim_payment_category.claim_pay_ctgry_ak_id as claim_pay_ctgry_ak_id, claim_payment_category.claim_pay_ctgry_type as claim_pay_ctgry_type, claim_payment_category.claim_pay_ctgry_lump_sum_ind as claim_pay_ctgry_lump_sum_ind, claim_payment_category.claim_pay_ak_id as claim_pay_ak_id, claim_payment_category.eff_from_date as eff_from_date, claim_payment_category.eff_to_date as eff_to_date FROM claim_payment_category
			WHERE crrnt_snpsht_flag = 1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_ak_id,eff_from_date,eff_to_date ORDER BY claim_pay_ctgry_ak_id) = 1
	),
	LKP_Claim_Pay_Ctgry_Dim_id AS (
		SELECT
		claim_pay_ctgry_type_dim_id,
		claim_pay_ctgry_type,
		claim_pay_ctgry_type_descript,
		eff_from_date,
		eff_to_date,
		claim_pay_ctgry_lump_sum_ind,
		IN_claim_pay_ctgry_type,
		IN_claim_pay_ctgry_lump_sum_id,
		IN_trans_date
		FROM (
			SELECT 
				claim_pay_ctgry_type_dim_id,
				claim_pay_ctgry_type,
				claim_pay_ctgry_type_descript,
				eff_from_date,
				eff_to_date,
				claim_pay_ctgry_lump_sum_ind,
				IN_claim_pay_ctgry_type,
				IN_claim_pay_ctgry_lump_sum_id,
				IN_trans_date
			FROM claim_payment_category_type_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_ctgry_type,claim_pay_ctgry_lump_sum_ind,eff_from_date,eff_to_date ORDER BY claim_pay_ctgry_type_dim_id) = 1
	),
	Output AS (
		SELECT
		LKP_Claim_Pay_Ctgry_Dim_id.claim_pay_ctgry_type_dim_id, 
		LKP_Claim_Payment_Category.claim_pay_ctgry_ak_id, 
		LKP_Claim_Payment_Category.claim_pay_ak_id
		FROM 
		LEFT JOIN LKP_Claim_Pay_Ctgry_Dim_id
		ON LKP_Claim_Pay_Ctgry_Dim_id.claim_pay_ctgry_type = LKP_Claim_Payment_Category.claim_pay_ctgry_type AND LKP_Claim_Pay_Ctgry_Dim_id.claim_pay_ctgry_lump_sum_ind = LKP_Claim_Payment_Category.claim_pay_ctgry_lump_sum_ind AND LKP_Claim_Pay_Ctgry_Dim_id.eff_from_date <= EXP_Get_Values.IN_trans_date AND LKP_Claim_Pay_Ctgry_Dim_id.eff_to_date >= EXP_Get_Values.IN_trans_date
		LEFT JOIN LKP_Claim_Payment_Category
		ON LKP_Claim_Payment_Category.claim_pay_ak_id = EXP_Get_Values.IN_Claim_Pay_AK_ID AND LKP_Claim_Payment_Category.eff_from_date <= EXP_Get_Values.IN_trans_date AND LKP_Claim_Payment_Category.eff_to_date >= EXP_Get_Values.IN_trans_date
	),
),
mplt_Claim_Payment_Category_type_Dim_id AS (WITH
	Input AS (
		
	),
	EXP_Get_Values AS (
		SELECT
		IN_Claim_Pay_AK_ID,
		IN_trans_date
		FROM Input
	),
	LKP_Claim_Payment_Category AS (
		SELECT
		claim_pay_ctgry_ak_id,
		claim_pay_ak_id,
		claim_pay_ctgry_type,
		claim_pay_ctgry_lump_sum_ind,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT claim_payment_category.claim_pay_ctgry_ak_id as claim_pay_ctgry_ak_id, claim_payment_category.claim_pay_ctgry_type as claim_pay_ctgry_type, claim_payment_category.claim_pay_ctgry_lump_sum_ind as claim_pay_ctgry_lump_sum_ind, claim_payment_category.claim_pay_ak_id as claim_pay_ak_id, claim_payment_category.eff_from_date as eff_from_date, claim_payment_category.eff_to_date as eff_to_date FROM claim_payment_category
			WHERE crrnt_snpsht_flag = 1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_ak_id,eff_from_date,eff_to_date ORDER BY claim_pay_ctgry_ak_id) = 1
	),
	LKP_Claim_Pay_Ctgry_Dim_id AS (
		SELECT
		claim_pay_ctgry_type_dim_id,
		claim_pay_ctgry_type,
		claim_pay_ctgry_type_descript,
		eff_from_date,
		eff_to_date,
		claim_pay_ctgry_lump_sum_ind,
		IN_claim_pay_ctgry_type,
		IN_claim_pay_ctgry_lump_sum_id,
		IN_trans_date
		FROM (
			SELECT 
				claim_pay_ctgry_type_dim_id,
				claim_pay_ctgry_type,
				claim_pay_ctgry_type_descript,
				eff_from_date,
				eff_to_date,
				claim_pay_ctgry_lump_sum_ind,
				IN_claim_pay_ctgry_type,
				IN_claim_pay_ctgry_lump_sum_id,
				IN_trans_date
			FROM claim_payment_category_type_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_ctgry_type,claim_pay_ctgry_lump_sum_ind,eff_from_date,eff_to_date ORDER BY claim_pay_ctgry_type_dim_id) = 1
	),
	Output AS (
		SELECT
		LKP_Claim_Pay_Ctgry_Dim_id.claim_pay_ctgry_type_dim_id, 
		LKP_Claim_Payment_Category.claim_pay_ctgry_ak_id, 
		LKP_Claim_Payment_Category.claim_pay_ak_id
		FROM 
		LEFT JOIN LKP_Claim_Pay_Ctgry_Dim_id
		ON LKP_Claim_Pay_Ctgry_Dim_id.claim_pay_ctgry_type = LKP_Claim_Payment_Category.claim_pay_ctgry_type AND LKP_Claim_Pay_Ctgry_Dim_id.claim_pay_ctgry_lump_sum_ind = LKP_Claim_Payment_Category.claim_pay_ctgry_lump_sum_ind AND LKP_Claim_Pay_Ctgry_Dim_id.eff_from_date <= EXP_Get_Values.IN_trans_date AND LKP_Claim_Pay_Ctgry_Dim_id.eff_to_date >= EXP_Get_Values.IN_trans_date
		LEFT JOIN LKP_Claim_Payment_Category
		ON LKP_Claim_Payment_Category.claim_pay_ak_id = EXP_Get_Values.IN_Claim_Pay_AK_ID AND LKP_Claim_Payment_Category.eff_from_date <= EXP_Get_Values.IN_trans_date AND LKP_Claim_Payment_Category.eff_to_date >= EXP_Get_Values.IN_trans_date
	),
),
mplt_Claimant_dim_hist_id AS (WITH
	INPUT AS (
		
	),
	EXP_get_values AS (
		SELECT
		IN_claimant_cov_det_ak_id,
		IN_trans_date
		FROM INPUT
	),
	LKP_Claimant_coverage_detail AS (
		SELECT
		claim_party_occurrence_ak_id,
		claimant_cov_det_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_party_occurrence_ak_id,
				claimant_cov_det_ak_id,
				eff_from_date,
				eff_to_date
			FROM claimant_coverage_detail
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,eff_from_date,eff_to_date ORDER BY claim_party_occurrence_ak_id DESC) = 1
	),
	LKP_CLAIMANT_DIM AS (
		SELECT
		claimant_dim_id,
		edw_claim_party_occurrence_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT claimant_dim.claimant_dim_id as claimant_dim_id, claimant_dim.edw_claim_party_occurrence_ak_id as edw_claim_party_occurrence_ak_id, claimant_dim.eff_from_date as eff_from_date, claimant_dim.eff_to_date as eff_to_date 
			FROM claimant_dim
			WHERE edw_claim_party_occurrence_ak_id IN
			(select claim_party_occurrence_ak_id from @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_party_occurrence where claim_party_role_code in ('CMT','CLMT'))
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_party_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claimant_dim_id DESC) = 1
	),
	OUTPUT AS (
		SELECT
		claimant_dim_id
		FROM LKP_CLAIMANT_DIM
	),
),
mplt_Claimant_dim_id AS (WITH
	INPUT AS (
		
	),
	EXP_get_values AS (
		SELECT
		IN_claimant_cov_det_ak_id,
		IN_trans_date
		FROM INPUT
	),
	LKP_Claimant_coverage_detail AS (
		SELECT
		claim_party_occurrence_ak_id,
		claimant_cov_det_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_party_occurrence_ak_id,
				claimant_cov_det_ak_id,
				eff_from_date,
				eff_to_date
			FROM claimant_coverage_detail
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,eff_from_date,eff_to_date ORDER BY claim_party_occurrence_ak_id DESC) = 1
	),
	LKP_CLAIMANT_DIM AS (
		SELECT
		claimant_dim_id,
		edw_claim_party_occurrence_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT claimant_dim.claimant_dim_id as claimant_dim_id, claimant_dim.edw_claim_party_occurrence_ak_id as edw_claim_party_occurrence_ak_id, claimant_dim.eff_from_date as eff_from_date, claimant_dim.eff_to_date as eff_to_date 
			FROM claimant_dim
			WHERE edw_claim_party_occurrence_ak_id IN
			(select claim_party_occurrence_ak_id from @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_party_occurrence where claim_party_role_code in ('CMT','CLMT'))
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_party_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claimant_dim_id DESC) = 1
	),
	OUTPUT AS (
		SELECT
		claimant_dim_id
		FROM LKP_CLAIMANT_DIM
	),
),
EXP_set_claimant_dim_ids AS (
	SELECT
	mplt_Claimant_dim_id.claimant_dim_id,
	mplt_Claimant_dim_hist_id.claimant_dim_id AS claimant_dim_hist_id,
	EXP_Cov_Level_dim_ids.claimant_cov_dim_id,
	EXP_Cov_Level_dim_ids.claimant_cov_dim_hist_id,
	EXP_Cov_Level_dim_ids.cov_dim_id,
	EXP_Cov_Level_dim_ids.cov_dim_hist_id,
	EXP_Cov_Level_dim_ids.claim_trans_type_dim_id,
	EXP_Cov_Level_dim_ids.claim_financial_type_dim_id,
	EXP_Cov_Level_dim_ids.claim_payment_dim_id,
	EXP_Cov_Level_dim_ids.claim_payment_dim_hist_id,
	EXP_Cov_Level_dim_ids.tax_id,
	EXP_Cov_Level_dim_ids.claim_master_1099_list_dim_id,
	mplt_Claim_Payment_Category_type_Dim_id.claim_pay_ctgry_type_dim_id,
	mplt_Claim_Payment_Category_type_Dim_hist_id.claim_pay_ctgry_type_dim_id AS claim_pay_ctgry_type_dim_hist_id,
	EXP_Cov_Level_dim_ids.claim_subrogation_dim_id,
	EXP_Cov_Level_dim_ids.trans_date,
	EXP_Cov_Level_dim_ids.trans_date_month_end,
	EXP_Cov_Level_dim_ids.reprocess_date,
	EXP_Cov_Level_dim_ids.claim_trans_id,
	EXP_Cov_Level_dim_ids.IN_claimant_cov_det_ak_id,
	EXP_Cov_Level_dim_ids.IN_cause_of_loss,
	EXP_Cov_Level_dim_ids.IN_reserve_ctgry,
	EXP_Cov_Level_dim_ids.IN_type_disability,
	EXP_Cov_Level_dim_ids.financial_type_code,
	EXP_Cov_Level_dim_ids.trans_code,
	EXP_Cov_Level_dim_ids.trans_ctgry_code,
	EXP_Cov_Level_dim_ids.trans_amt,
	EXP_Cov_Level_dim_ids.trans_hist_amt,
	EXP_Cov_Level_dim_ids.source_sys_id,
	EXP_Cov_Level_dim_ids.referred_to_subrogation_date,
	EXP_Cov_Level_dim_ids.pay_start_date,
	EXP_Cov_Level_dim_ids.closure_date,
	EXP_Cov_Level_dim_ids.trans_date_out,
	EXP_Cov_Level_dim_ids.pms_acct_entered_date,
	EXP_Cov_Level_dim_ids.ClaimReserveDimId,
	EXP_Cov_Level_dim_ids.FeatureRepresentativeDimId,
	EXP_Cov_Level_dim_ids.FeatureRepresentativeAssignedDate
	FROM EXP_Cov_Level_dim_ids
	 -- Manually join with mplt_Claim_Payment_Category_type_Dim_hist_id
	 -- Manually join with mplt_Claim_Payment_Category_type_Dim_id
	 -- Manually join with mplt_Claimant_dim_hist_id
	 -- Manually join with mplt_Claimant_dim_id
),
mplt_Claim_occurrence_dim_id AS (WITH
	LKP_claim_representative AS (
		SELECT
		claim_rep_wbconnect_user_id,
		claim_rep_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
			CASE claim_representative.claim_rep_wbconnect_user_id  WHEN 'N/A' THEN claim_representative.claim_rep_key 
			ELSE claim_representative.claim_rep_wbconnect_user_id END AS claim_rep_wbconnect_user_id, claim_representative.claim_rep_ak_id as claim_rep_ak_id, claim_representative.eff_from_date as eff_from_date, claim_representative.eff_to_date as eff_to_date FROM claim_representative
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_ak_id,eff_from_date,eff_to_date ORDER BY claim_rep_wbconnect_user_id DESC) = 1
	),
	LKP_claim_rep_dim AS (
		SELECT
		claim_rep_dim_id,
		claim_rep_wbconnect_user_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_rep_dim_id,
				claim_rep_wbconnect_user_id,
				eff_from_date,
				eff_to_date
			FROM claim_representative_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_wbconnect_user_id,eff_from_date,eff_to_date ORDER BY claim_rep_dim_id DESC) = 1
	),
	INPUT AS (
		
	),
	EXP_get_values AS (
		SELECT
		IN_claimant_cov_det_ak_id,
		IN_trans_date
		FROM INPUT
	),
	LKP_Claimant_coverage_detail AS (
		SELECT
		claim_party_occurrence_ak_id,
		claimant_cov_det_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_party_occurrence_ak_id,
				claimant_cov_det_ak_id,
				eff_from_date,
				eff_to_date
			FROM claimant_coverage_detail
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,eff_from_date,eff_to_date ORDER BY claim_party_occurrence_ak_id DESC) = 1
	),
	LKP_Claim_Party_occurrence AS (
		SELECT
		claim_occurrence_ak_id,
		claim_case_ak_id,
		claim_party_occurrence_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT claim_party_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, claim_party_occurrence.claim_case_ak_id as claim_case_ak_id, claim_party_occurrence.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, claim_party_occurrence.eff_from_date as eff_from_date, claim_party_occurrence.eff_to_date as eff_to_date 
			FROM claim_party_occurrence
			WHERE
			claim_party_occurrence.claim_party_role_code in ('CMT','CLMT')
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_occurrence_ak_id) = 1
	),
	LKP_Claim_Rep_Occurrence_PLH AS (
		SELECT
		claim_rep_ak_id,
		eff_from_date,
		eff_to_date,
		claim_occurrence_ak_id
		FROM (
			SELECT claim_representative_occurrence.claim_rep_ak_id as claim_rep_ak_id, claim_representative_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, claim_representative_occurrence.eff_from_date as eff_from_date, claim_representative_occurrence.eff_to_date as eff_to_date FROM claim_representative_occurrence
			WHERE LTRIM(RTRIM(claim_representative_occurrence.claim_rep_role_code))  = 'PLH'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_rep_ak_id DESC) = 1
	),
	LKP_Claim_Case AS (
		SELECT
		claim_case_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_case_ak_id,
				eff_from_date,
				eff_to_date
			FROM claim_case
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_case_ak_id,eff_from_date,eff_to_date ORDER BY claim_case_ak_id) = 1
	),
	EXP_get_reserve_calc_ids AS (
		SELECT
		LKP_Claim_Party_occurrence.claim_occurrence_ak_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_ak_id), -1, claim_occurrence_ak_id)
		IFF(claim_occurrence_ak_id IS NULL, - 1, claim_occurrence_ak_id) AS claim_occurrence_ak_id_out,
		EXP_get_values.IN_trans_date
		FROM EXP_get_values
		LEFT JOIN LKP_Claim_Party_occurrence
		ON LKP_Claim_Party_occurrence.claim_party_occurrence_ak_id = LKP_Claimant_coverage_detail.claim_party_occurrence_ak_id AND LKP_Claim_Party_occurrence.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Party_occurrence.eff_to_date >= EXP_get_values.IN_trans_date
	),
	LKP_claim_occurrence_Date AS (
		SELECT
		claim_occurrence_id,
		pol_key_ak_id,
		claim_loss_date,
		claim_discovery_date,
		claim_cat_start_date,
		claim_cat_end_date,
		claim_created_by_key,
		claim_occurrence_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_occurrence_id,
				pol_key_ak_id,
				claim_loss_date,
				claim_discovery_date,
				claim_cat_start_date,
				claim_cat_end_date,
				claim_created_by_key,
				claim_occurrence_ak_id,
				eff_from_date,
				eff_to_date
			FROM claim_occurrence
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_occurrence_id DESC) = 1
	),
	LKP_Claim_Rep_Occurrence_Handler AS (
		SELECT
		claim_rep_ak_id,
		claim_assigned_date,
		eff_from_date,
		eff_to_date,
		claim_occurrence_ak_id
		FROM (
			SELECT claim_representative_occurrence.claim_rep_ak_id as claim_rep_ak_id, claim_representative_occurrence.claim_assigned_date as claim_assigned_date, claim_representative_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, claim_representative_occurrence.eff_from_date as eff_from_date, claim_representative_occurrence.eff_to_date as eff_to_date FROM claim_representative_occurrence
			WHERE LTRIM(RTRIM(claim_representative_occurrence.claim_rep_role_code))  = 'H'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_rep_ak_id DESC) = 1
	),
	LKP_Claim_Rep_Occurrence_Examiner AS (
		SELECT
		claim_rep_ak_id,
		eff_from_date,
		eff_to_date,
		claim_occurrence_ak_id
		FROM (
			SELECT claim_representative_occurrence.claim_rep_ak_id as claim_rep_ak_id, claim_representative_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, claim_representative_occurrence.eff_from_date as eff_from_date, claim_representative_occurrence.eff_to_date as eff_to_date FROM claim_representative_occurrence
			WHERE LTRIM(RTRIM(claim_representative_occurrence.claim_rep_role_code))  = 'E'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_rep_ak_id DESC) = 1
	),
	LKP_claim_occurrence_dim AS (
		SELECT
		claim_occurrence_dim_id,
		source_claim_rpted_date,
		claim_rpted_date,
		claim_scripted_date,
		claim_open_date,
		claim_close_date,
		claim_reopen_date,
		claim_closed_after_reopen_date,
		claim_notice_only_date,
		edw_claim_occurrence_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_occurrence_dim_id,
				source_claim_rpted_date,
				claim_rpted_date,
				claim_scripted_date,
				claim_open_date,
				claim_close_date,
				claim_reopen_date,
				claim_closed_after_reopen_date,
				claim_notice_only_date,
				edw_claim_occurrence_ak_id,
				eff_from_date,
				eff_to_date
			FROM claim_occurrence_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_occurrence_dim_id DESC) = 1
	),
	LKP_Claim_Created_by_rep_ak_id AS (
		SELECT
		claim_rep_ak_id,
		claim_rep_wbconnect_user_id,
		claim_rep_key,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_rep_ak_id,
				claim_rep_wbconnect_user_id,
				claim_rep_key,
				eff_from_date,
				eff_to_date
			FROM claim_representative
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_key,eff_from_date,eff_to_date ORDER BY claim_rep_ak_id) = 1
	),
	LKP_Claim_Case_Dim AS (
		SELECT
		claim_case_dim_id,
		edw_claim_case_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_case_dim_id,
				edw_claim_case_ak_id,
				eff_from_date,
				eff_to_date
			FROM claim_case_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_case_ak_id,eff_from_date,eff_to_date ORDER BY claim_case_dim_id) = 1
	),
	LKP_V2_policy AS (
		SELECT
		pol_id,
		contract_cust_ak_id,
		agency_ak_id,
		AgencyAKID,
		StrategicProfitCenterAKId,
		InsuranceSegmentAKId,
		PolicyOfferingAKId,
		pol_eff_date,
		pol_exp_date,
		pol_sym,
		pol_num,
		pol_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT policy.pol_id as pol_id, policy.contract_cust_ak_id as contract_cust_ak_id, 
			policy.agency_ak_id as agency_ak_id,
			policy.AgencyAKID as AgencyAKID, 
			policy.StrategicProfitCenterAKId as StrategicProfitCenterAKId,
			policy.InsuranceSegmentAKId as InsuranceSegmentAKId,
			policy.PolicyOfferingAKId as PolicyOfferingAKId,
			policy.pol_eff_date as pol_eff_date, policy.pol_exp_date as pol_exp_date, policy.pol_sym as pol_sym, policy.pol_num as pol_num, policy.pol_ak_id as pol_ak_id, policy.eff_from_date as eff_from_date, policy.eff_to_date as eff_to_date 
			FROM 
			v2.policy policy
			WHERE 
			policy.pol_ak_id IN (select distinct pol_key_ak_id from claim_occurrence)
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,eff_from_date,eff_to_date ORDER BY pol_id DESC) = 1
	),
	LKP_contract_customer_dim AS (
		SELECT
		contract_cust_dim_id,
		edw_contract_cust_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT contract_customer_dim.contract_cust_dim_id as contract_cust_dim_id, contract_customer_dim.edw_contract_cust_ak_id as edw_contract_cust_ak_id, contract_customer_dim.eff_from_date as eff_from_date, contract_customer_dim.eff_to_date as eff_to_date 
			FROM contract_customer_dim
			WHERE edw_contract_cust_ak_id IN
			(
			SELECT DISTINCT CC.contract_cust_ak_id 
			FROM @{pipeline().parameters.DB_NAME_EDW}.dbo.contract_customer CC, @{pipeline().parameters.DB_NAME_EDW}.V2.policy P, @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_occurrence CO
			WHERE CC.contract_cust_ak_id = P.contract_cust_ak_id
			AND CO.pol_key_ak_id = P.pol_ak_id
			AND P.crrnt_snpsht_flag = 1
			AND CC.crrnt_snpsht_flag = 1
			)
			
			--- 2/12/2014 : Modified the Lookup Query to join on AK ID values instead of Natural Key
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_contract_cust_ak_id,eff_from_date,eff_to_date ORDER BY contract_cust_dim_id DESC) = 1
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
			FROM V3.AgencyDim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyAKID,EffectiveDate,ExpirationDate ORDER BY AgencyDimID DESC) = 1
	),
	LKP_V2_Agency AS (
		SELECT
		SalesTerritoryAKID,
		RegionalSalesManagerAKID,
		AgencyAKID,
		EffectiveDate,
		ExpirationDate
		FROM (
			SELECT 
				SalesTerritoryAKID,
				RegionalSalesManagerAKID,
				AgencyAKID,
				EffectiveDate,
				ExpirationDate
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID,EffectiveDate,ExpirationDate ORDER BY SalesTerritoryAKID) = 1
	),
	LKP_agency_dim AS (
		SELECT
		agency_dim_id,
		edw_agency_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				agency_dim_id,
				edw_agency_ak_id,
				eff_from_date,
				eff_to_date
			FROM V2.agency_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_agency_ak_id,eff_from_date,eff_to_date ORDER BY agency_dim_id DESC) = 1
	),
	LKP_Policy_Dim AS (
		SELECT
		pol_dim_id,
		edw_pol_pk_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
			policy_dim.pol_dim_id as pol_dim_id, 
			policy_dim.edw_pol_pk_id as edw_pol_pk_id, 
			policy_dim.eff_from_date as eff_from_date, 
			policy_dim.eff_to_date as eff_to_date 
			FROM policy_dim
			WHERE edw_pol_pk_id IN 
			(SELECT policy.pol_id as pol_id FROM @{pipeline().parameters.DB_NAME_EDW}.v2.policy policy
			WHERE policy.pol_ak_id IN (select distinct pol_key_ak_id from @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_occurrence))
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_pk_id,eff_from_date,eff_to_date ORDER BY pol_dim_id DESC) = 1
	),
	EXP_Lkp_Dim_ids AS (
		SELECT
		EXP_get_reserve_calc_ids.IN_trans_date,
		LKP_Claim_Rep_Occurrence_Handler.claim_rep_ak_id AS claim_rep_primary_rep_ak_id,
		-- *INF*: :LKP.LKP_CLAIM_REPRESENTATIVE(claim_rep_primary_rep_ak_id, IN_trans_date)
		LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_rep_ak_id_IN_trans_date.claim_rep_wbconnect_user_id AS v_claim_rep_primary_rep_wbconnect_user_id,
		LKP_Claim_Rep_Occurrence_Examiner.claim_rep_ak_id AS claim_rep_examiner_ak_id,
		-- *INF*: :LKP.LKP_CLAIM_REPRESENTATIVE(claim_rep_examiner_ak_id, IN_trans_date)
		LKP_CLAIM_REPRESENTATIVE_claim_rep_examiner_ak_id_IN_trans_date.claim_rep_wbconnect_user_id AS v_claim_rep_examiner_wbconnect_user_id,
		LKP_Claim_Rep_Occurrence_PLH.claim_rep_ak_id AS claim_rep_primary_lit_handler_ak_id,
		-- *INF*: :LKP.LKP_CLAIM_REPRESENTATIVE(claim_rep_primary_lit_handler_ak_id, IN_trans_date)
		LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_lit_handler_ak_id_IN_trans_date.claim_rep_wbconnect_user_id AS v_claim_rep_primary_lit_handler_wbconnect_user_id,
		-- *INF*: :LKP.LKP_CLAIM_REP_DIM(claim_rep_primary_rep_ak_id, v_claim_rep_primary_rep_wbconnect_user_id, IN_trans_date)
		LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date.claim_rep_dim_id AS claim_rep_dim_prim_claim_rep_id,
		-- *INF*: :LKP.LKP_CLAIM_REP_DIM(claim_rep_examiner_ak_id, v_claim_rep_examiner_wbconnect_user_id, IN_trans_date)
		LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date.claim_rep_dim_id AS claim_rep_dim_examiner_id,
		-- *INF*: :LKP.LKP_CLAIM_REP_DIM(claim_rep_primary_lit_handler_ak_id, v_claim_rep_primary_lit_handler_wbconnect_user_id, IN_trans_date)
		LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date.claim_rep_dim_id AS claim_rep_dim_prim_litigation_handler_id,
		LKP_Claim_Created_by_rep_ak_id.claim_rep_ak_id,
		LKP_Claim_Created_by_rep_ak_id.claim_rep_wbconnect_user_id,
		-- *INF*: :LKP.LKP_CLAIM_REP_DIM(claim_rep_ak_id,claim_rep_wbconnect_user_id,IN_trans_date)
		LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date.claim_rep_dim_id AS claim_created_by_id
		FROM EXP_get_reserve_calc_ids
		LEFT JOIN LKP_Claim_Created_by_rep_ak_id
		ON LKP_Claim_Created_by_rep_ak_id.claim_rep_key = LKP_claim_occurrence_Date.claim_created_by_key AND LKP_Claim_Created_by_rep_ak_id.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Created_by_rep_ak_id.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_Examiner
		ON LKP_Claim_Rep_Occurrence_Examiner.claim_occurrence_ak_id = LKP_Claim_Party_occurrence.claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_Examiner.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_Examiner.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_Handler
		ON LKP_Claim_Rep_Occurrence_Handler.claim_occurrence_ak_id = LKP_Claim_Party_occurrence.claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_Handler.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_Handler.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_PLH
		ON LKP_Claim_Rep_Occurrence_PLH.claim_occurrence_ak_id = LKP_Claim_Party_occurrence.claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_PLH.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_PLH.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_CLAIM_REPRESENTATIVE LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_rep_ak_id_IN_trans_date
		ON LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_rep_ak_id_IN_trans_date.claim_rep_ak_id = claim_rep_primary_rep_ak_id
		AND LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_rep_ak_id_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REPRESENTATIVE LKP_CLAIM_REPRESENTATIVE_claim_rep_examiner_ak_id_IN_trans_date
		ON LKP_CLAIM_REPRESENTATIVE_claim_rep_examiner_ak_id_IN_trans_date.claim_rep_ak_id = claim_rep_examiner_ak_id
		AND LKP_CLAIM_REPRESENTATIVE_claim_rep_examiner_ak_id_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REPRESENTATIVE LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_lit_handler_ak_id_IN_trans_date
		ON LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_lit_handler_ak_id_IN_trans_date.claim_rep_ak_id = claim_rep_primary_lit_handler_ak_id
		AND LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_lit_handler_ak_id_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REP_DIM LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date
		ON LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date.claim_rep_wbconnect_user_id = claim_rep_primary_rep_ak_id
		AND LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date.eff_from_date = v_claim_rep_primary_rep_wbconnect_user_id
		AND LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date.eff_to_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REP_DIM LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date
		ON LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date.claim_rep_wbconnect_user_id = claim_rep_examiner_ak_id
		AND LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date.eff_from_date = v_claim_rep_examiner_wbconnect_user_id
		AND LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date.eff_to_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REP_DIM LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date
		ON LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date.claim_rep_wbconnect_user_id = claim_rep_primary_lit_handler_ak_id
		AND LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date.eff_from_date = v_claim_rep_primary_lit_handler_wbconnect_user_id
		AND LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date.eff_to_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REP_DIM LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date
		ON LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date.claim_rep_wbconnect_user_id = claim_rep_ak_id
		AND LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date.eff_from_date = claim_rep_wbconnect_user_id
		AND LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date.eff_to_date = IN_trans_date
	
	),
	LKP_RegionalSalesManager AS (
		SELECT
		SalesDirectorAKID,
		RegionalSalesManagerAKID,
		EffectiveDate,
		ExpirationDate
		FROM (
			SELECT 
				SalesDirectorAKID,
				RegionalSalesManagerAKID,
				EffectiveDate,
				ExpirationDate
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.RegionalSalesManager
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY RegionalSalesManagerAKID,EffectiveDate,ExpirationDate ORDER BY SalesDirectorAKID) = 1
	),
	OUTPUT AS (
		SELECT
		LKP_claim_occurrence_dim.claim_occurrence_dim_id, 
		LKP_claim_occurrence_Date.claim_loss_date, 
		LKP_claim_occurrence_Date.claim_discovery_date, 
		LKP_claim_occurrence_dim.claim_scripted_date, 
		LKP_claim_occurrence_dim.source_claim_rpted_date, 
		LKP_claim_occurrence_dim.claim_rpted_date AS claim_occurrence_rpted_date, 
		LKP_claim_occurrence_dim.claim_open_date, 
		LKP_claim_occurrence_dim.claim_close_date, 
		LKP_claim_occurrence_dim.claim_reopen_date, 
		LKP_claim_occurrence_dim.claim_closed_after_reopen_date, 
		LKP_claim_occurrence_dim.claim_notice_only_date, 
		LKP_claim_occurrence_Date.claim_cat_start_date, 
		LKP_claim_occurrence_Date.claim_cat_end_date, 
		LKP_Claim_Rep_Occurrence_Handler.claim_assigned_date AS claim_rep_assigned_date, 
		LKP_Claim_Rep_Occurrence_Handler.eff_to_date AS claim_rep_unassigned_date, 
		EXP_Lkp_Dim_ids.claim_rep_dim_prim_claim_rep_id, 
		EXP_Lkp_Dim_ids.claim_rep_dim_examiner_id, 
		EXP_Lkp_Dim_ids.claim_rep_dim_prim_litigation_handler_id, 
		LKP_Policy_Dim.pol_dim_id AS pol_key_dim_id, 
		LKP_V2_policy.pol_eff_date, 
		LKP_V2_policy.pol_exp_date, 
		LKP_AgencyDim.AgencyDimID, 
		EXP_Lkp_Dim_ids.claim_created_by_id, 
		LKP_Claim_Case_Dim.claim_case_dim_id, 
		LKP_contract_customer_dim.contract_cust_dim_id, 
		LKP_V2_policy.pol_sym, 
		LKP_V2_policy.pol_num, 
		LKP_V2_policy.AgencyAKID, 
		LKP_V2_Agency.SalesTerritoryAKID, 
		LKP_V2_Agency.RegionalSalesManagerAKID, 
		LKP_RegionalSalesManager.SalesDirectorAKID, 
		LKP_V2_policy.StrategicProfitCenterAKId, 
		LKP_V2_policy.InsuranceSegmentAKId, 
		LKP_V2_policy.PolicyOfferingAKId, 
		LKP_agency_dim.agency_dim_id, 
		LKP_claim_occurrence_Date.pol_key_ak_id AS PolicyAkid
		FROM EXP_Lkp_Dim_ids
		LEFT JOIN LKP_AgencyDim
		ON LKP_AgencyDim.EDWAgencyAKID = LKP_V2_policy.AgencyAKID AND LKP_AgencyDim.EffectiveDate <= EXP_get_values.IN_trans_date AND LKP_AgencyDim.ExpirationDate >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Case_Dim
		ON LKP_Claim_Case_Dim.edw_claim_case_ak_id = LKP_Claim_Case.claim_case_ak_id AND LKP_Claim_Case_Dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Case_Dim.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_Handler
		ON LKP_Claim_Rep_Occurrence_Handler.claim_occurrence_ak_id = LKP_Claim_Party_occurrence.claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_Handler.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_Handler.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Policy_Dim
		ON LKP_Policy_Dim.edw_pol_pk_id = LKP_V2_policy.pol_id AND LKP_Policy_Dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Policy_Dim.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_RegionalSalesManager
		ON LKP_RegionalSalesManager.RegionalSalesManagerAKID = LKP_V2_Agency.RegionalSalesManagerAKID AND LKP_RegionalSalesManager.EffectiveDate <= EXP_get_values.IN_trans_date AND LKP_RegionalSalesManager.ExpirationDate >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_V2_Agency
		ON LKP_V2_Agency.AgencyAKID = LKP_V2_policy.AgencyAKID AND LKP_V2_Agency.EffectiveDate <= EXP_get_values.IN_trans_date AND LKP_V2_Agency.ExpirationDate >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_V2_policy
		ON LKP_V2_policy.pol_ak_id = LKP_claim_occurrence_Date.pol_key_ak_id AND LKP_V2_policy.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_V2_policy.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_agency_dim
		ON LKP_agency_dim.edw_agency_ak_id = LKP_V2_policy.agency_ak_id AND LKP_agency_dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_agency_dim.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_claim_occurrence_Date
		ON LKP_claim_occurrence_Date.claim_occurrence_ak_id = LKP_Claim_Party_occurrence.claim_occurrence_ak_id AND LKP_claim_occurrence_Date.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_claim_occurrence_Date.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_claim_occurrence_dim
		ON LKP_claim_occurrence_dim.edw_claim_occurrence_ak_id = EXP_get_reserve_calc_ids.claim_occurrence_ak_id_out AND LKP_claim_occurrence_dim.eff_from_date <= EXP_get_reserve_calc_ids.IN_trans_date AND LKP_claim_occurrence_dim.eff_to_date >= EXP_get_reserve_calc_ids.IN_trans_date
		LEFT JOIN LKP_contract_customer_dim
		ON LKP_contract_customer_dim.edw_contract_cust_ak_id = LKP_V2_policy.contract_cust_ak_id AND LKP_contract_customer_dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_contract_customer_dim.eff_to_date >= EXP_get_values.IN_trans_date
	),
),
LKP_CoverageDetailDim AS (
	SELECT
	CoverageDetailDimId,
	PolicyAkid,
	claim_trans_id
	FROM (
		SELECT CDD.CoverageDetailDimId as CoverageDetailDimId,
		CT.claim_trans_id as claim_trans_id,
		PC.PolicyAKID as PolicyAKID
		FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction CT
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
		on CCD.claimant_cov_det_ak_id = CT.claimant_cov_det_ak_id and CCD.crrnt_snpsht_flag = 1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
		on CCD.RatingCoverageAKId=RC.RatingCoverageAKID
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		on RC.RatingCoverageAKID=PT.RatingCoverageAKId
		and RC.EffectiveDate=PT.EffectiveDate
		and PT.SourceSystemID='DCT'
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on RC.PolicyCoverageAKID=PC.PolicyCoverageAKID
		and PC.CurrentSnapshotFlag=1
		and PC.SourceSystemID='DCT'
		join @{pipeline().parameters.DB_NAME_DATAMART}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
		on PT.PremiumTransactionID=CDD.EDWPremiumTransactionPKId
		WHERE CT.trans_offset_onset_ind in ('N','N/A')
		
		Union
		
		SELECT CDD.CoverageDetailDimId as CoverageDetailDimId,
		CT.claim_trans_id as claim_trans_id,
		PC.PolicyAKID as PolicyAKID
		FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction CT
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
		on CCD.claimant_cov_det_ak_id = CT.claimant_cov_det_ak_id and CCD.crrnt_snpsht_flag = 1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
		on CCD.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		on SC.StatisticalCoverageAKID=PT.StatisticalCoverageAKID
		and PT.SourceSystemID='PMS'
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on SC.PolicyCoverageAKID=PC.PolicyCoverageAKID
		and PC.CurrentSnapshotFlag=1
		and PC.SourceSystemID='PMS'
		join @{pipeline().parameters.DB_NAME_DATAMART}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
		on PT.PremiumTransactionID=CDD.EDWPremiumTransactionPKId and CDD.ExpirationDate='2100-12-31 23:59:59'
		WHERE CT.trans_offset_onset_ind in ('N','N/A')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_trans_id,PolicyAkid ORDER BY CoverageDetailDimId DESC) = 1
),
LKP_InsuranceReferenceCoverageDim AS (
	SELECT
	InsuranceReferenceCoverageDimId,
	claimant_cov_det_ak_id
	FROM (
		SELECT T.claimant_cov_det_ak_id AS claimant_cov_det_ak_id,
		       T.InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId,
		T.EffectiveDate as EffectiveDate,
		T.ExpirationDate as ExpirationDate
		       FROM
		( SELECT CCD.claimant_cov_det_ak_id,
		       IRC.InsuranceReferenceCoverageDimId,
		RC.RatingCoverageEffectiveDate EffectiveDate,
		RC.RatingCoverageExpirationDate ExpirationDate
		FROM  	@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD,
				@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence  CPO,
				@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence CO,
				@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC,
				@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC,
				@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL,
				@{pipeline().parameters.DB_NAME_DATAMART}.@{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceCoverageDim IRC
		WHERE  SIL.ins_line_code = PC.InsuranceLine
				AND CCD.claim_party_occurrence_ak_id=CPO.claim_party_occurrence_ak_id and CPO.crrnt_snpsht_flag=1 
				AND CPO.claim_occurrence_ak_id=CO.claim_occurrence_ak_id and CO.crrnt_snpsht_flag=1
		       AND CCD.RatingCoverageAKId = rc.RatingCoverageAKID
			   AND RC.PolicyCoverageAKID = PC.PolicyCoverageAKID
		       AND co.pol_key_ak_id=pc.policyakid
		       AND IRC.DctCoverageTypeCode= RC.CoverageType 
		       AND IRC.DctRiskTypeCode = RC.RiskType 
		       AND IRC.InsuranceLineCode = SIL.StandardInsuranceLineCode
		       AND IRC.DctPerilGroup=RC.PerilGroup
		       AND IRC.DctSubCoverageTypeCode=RC.SubCoverageTypeCode
		       AND IRC.DctCoverageVersion=RC.CoverageVersion
		       AND NOT (IRC.DctRiskTypeCode='N/A' AND IRC.DctCoverageTypeCode='N/A' AND IRC.DctPerilGroup='N/A' AND IRC.DctSubCoverageTypeCode='N/A' AND  IRC.DctCoverageVersion='N/A')
		       AND CCD.crrnt_snpsht_flag = 1
		       AND PC.CurrentSnapshotFlag = 1
		       AND SIL.crrnt_snpsht_flag = 1
		UNION ALL
		SELECT T2.claimant_cov_det_ak_id,
		       IRC.InsuranceReferenceCoverageDimId,
		'1800-1-1' EffectiveDate,
		'2100-12-31 23:59:59' ExpirationDate
		FROM (SELECT claimant_cov_det_ak_id,
					 COALESCE(StandardInsuranceLineCode,'N/A') AS StandardInsuranceLineCode,
					 COALESCE(MajorPerilCode,'N/A') AS MajorPerilCode,
							 CASE WHEN (StandardInsuranceLineCode IN ('GL') AND (MajorPerilCode NOT IN ('540','599','919')
				        OR ClassCode NOT IN ( '11111','22222','22250','92100','17000','17001','17002','80051','80052','80053','80054','80055','80056','80057','80058')))
				        OR (StandardInsuranceLineCode IN( 'WC','IM','CG','CA'))
					 OR (StandardInsuranceLineCode='N/A' AND TypeBureauCode in ('CF','B2','BB','BE','BF','BM','BT','FT','GL','GS','IM','MS','PF','PH','PI','PL','PQ','WC','WP','NB','RL','RN','RP','BC','N/A'))
					 THEN 'N/A' ELSE COALESCE(RiskUnit,'N/A') END AS RiskUnit,
					 CASE WHEN StandardInsuranceLineCode='CR' 
					 OR (StandardInsuranceLineCode='N/A' AND TypeBureauCode in ('CF','B2','BB','BE','BF','BM','BT','FT','GL','GS','IM','MS','PF','PH','PI','PL','PQ','WC','WP','NB','RL','RN','RP','BC','N/A'))
					 THEN 'N/A' ELSE COALESCE(RiskUnitGroup,'N/A') END AS RiskUnitGroup,
		 			CASE WHEN REPLACE(ProductTypeCode ,'0','')=''  OR StandardInsuranceLineCode<>'GL' 
				        THEN 'N/A' ELSE COALESCE(REPLACE(ProductTypeCode ,'0',''),'N/A') END AS ProductTypeCode 
			 FROM (
		SELECT CCD.claimant_cov_det_ak_id,
				     PC.TypeBureauCode,
					 			 CASE WHEN REPLACE(SC.RiskUnit,'0','')='' OR PATINDEX('%[^0-9a-zA-Z]%',REPLACE(SC.RiskUnit,' ',''))<>0 THEN 'N/A' ELSE LTRIM(RTRIM(SC.RiskUnit)) END AS RiskUnit,
					 CASE WHEN REPLACE(SC.RiskUnitGroup,'0','')='' OR PATINDEX('%[^0-9a-zA-Z]%',REPLACE(SC.RiskUnitGroup,' ',''))<>0 THEN 'N/A' ELSE LTRIM(RTRIM(SC.RiskUnitGroup)) END AS RiskUnitGroup,
					 CASE WHEN REPLACE(SC.MajorPerilCode,'0','')='' OR PATINDEX('%[^0-9a-zA-Z]%',REPLACE(SC.MajorPerilCode,' ',''))<>0 THEN 'N/A' ELSE LTRIM(RTRIM(SC.MajorPerilCode)) END MajorPerilCode,
					 CASE WHEN (REPLACE(SIL.StandardInsuranceLineCode,'0','')='' OR PATINDEX('%[^0-9a-zA-Z]%',REPLACE(SIL.StandardInsuranceLineCode,' ',''))<>0 OR SIL.StandardInsuranceLineCode='N/A') 
					 AND (PC.TypeBureauCode IN ('AL','AN','AP') OR SC.MajorPerilCode IN ('930','931')) THEN 'CA' WHEN REPLACE(SIL.StandardInsuranceLineCode,'0','')='' OR PATINDEX('%[^0-9a-zA-Z]%',REPLACE(SIL.StandardInsuranceLineCode,' ',''))<>0 THEN 'N/A' ELSE LTRIM(RTRIM(SIL.StandardInsuranceLineCode)) END AS StandardInsuranceLineCode,
		            SC.ClassCode,
		 	 SUBSTRING(sc.RiskUnitSequenceNumber,2,1) as ProductTypeCode
				FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD,
					   @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC,
					   @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC,
					   @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SIL
				WHERE  CCD.StatisticalCoverageAKID = SC.StatisticalCoverageAKID
					   AND PC.PolicyCoverageAKID = SC.PolicyCoverageAKID
					   AND SC.SourcesystemID = 'PMS'
					   AND SIL.ins_line_code = PC.InsuranceLine
					   AND CCD.crrnt_snpsht_flag = 1
					   AND SIL.crrnt_snpsht_flag = 1)T1) T2,    
		       @{pipeline().parameters.DB_NAME_DATAMART}.@{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceCoverageDim IRC
		WHERE  IRC.PmsRiskUnitCode = T2.RiskUnit
		       AND IRC.PmsRiskUnitGroupCode = T2.RiskUnitGroup
		       AND IRC.PmsMajorPerilCode = T2.MajorPerilCode 
		       AND IRC.InsuranceLineCode = T2.StandardInsuranceLineCode
		       AND IRC.PmsProductTypeCode = T2.ProductTypeCode
		       AND IRC.DctRiskTypeCode='N/A' 
		       AND IRC.DctCoverageTypeCode='N/A'
		       AND IRC.DctPerilGroup='N/A'
			AND IRC.DctSubCoverageTypeCode='N/A' 
			AND  IRC.DctCoverageVersion='N/A') T
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id ORDER BY InsuranceReferenceCoverageDimId) = 1
),
LKP_InsuranceReferenceDimId AS (
	SELECT
	InsuranceReferenceDimId,
	claimant_cov_det_ak_id
	FROM (
		SELECT CCD.claimant_cov_det_ak_id AS claimant_cov_det_ak_id,
		IRD.InsuranceReferenceDimId AS InsuranceReferenceDimId
		FROM (
		select CCD.claimant_cov_det_ak_id,
		CCD.Claim_party_occurrence_ak_id,
		CCD.ProductAKId,
		CCD.InsuranceReferenceLineOfBusinessAKID,
		PC.RatingPlanAKId
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
		on CCD.StatisticalCoverageAKID=SC.StatisticalCoverageAKId
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on SC.PolicyCoverageAKId=PC.PolicyCoverageAKID
		where SC.SourceSystemId='PMS' and CCD.crrnt_snpsht_flag =1
		
		union all
		select CCD.claimant_cov_det_ak_id,
		CCD.Claim_party_occurrence_ak_id,
		CCD.ProductAKId,
		CCD.InsuranceReferenceLineOfBusinessAKID,
		PC.RatingPlanAKId
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
		Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence  CPO
		on CCD.claim_party_occurrence_ak_id=CPO.claim_party_occurrence_ak_id
		and CPO.crrnt_snpsht_flag=1
		Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence CO
		on CPO.claim_occurrence_ak_id=CO.claim_occurrence_ak_id
		and CO.crrnt_snpsht_flag=1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
		on CCD.RatingCoverageAKID=RC.RatingCoverageAKId 
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
		on RC.PolicyCoverageAKId=PC.PolicyCoverageAKID 
		and co.pol_key_ak_id=pc.policyakid
		and PC.CurrentSnapshotFlag=1
		where CCD.crrnt_snpsht_flag =1
		
		union all
		select CCD.claimant_cov_det_ak_id,
		CCD.Claim_party_occurrence_ak_id,
		CCD.ProductAKId,
		CCD.InsuranceReferenceLineOfBusinessAKID,
		null RatingPlanAKId
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD
		where CCD.crrnt_snpsht_flag =1 and CCD.StatisticalCoverageAKID=-1 and CCD.RatingCoverageAKID=-1
		 ) CCD
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO
		on CCD.Claim_party_occurrence_ak_id = CPO.claim_party_occurrence_ak_id
		and CPO.crrnt_snpsht_flag = 1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence CO
		on CPO.Claim_Occurrence_ak_id = CO.claim_occurrence_ak_id
		and CO.crrnt_snpsht_flag = 1
		join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
		on CO.pol_key_ak_id = P.pol_ak_id
		and P.crrnt_snpsht_flag = 1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering PO
		on P.PolicyOfferingAKId = PO.PolicyOfferingAKID
		and PO.CurrentSnapshotFlag =1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment ISe
		on P.InsuranceSegmentAKID = ISe.InsuranceSegmentAKID
		and ISe.CurrentSnapshotFlag = 1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter SPC
		on P.StrategicProfitCenterAKId = SPC.StrategicProfitCenterAKId
		and SPC.CurrentSnapshotFlag = 1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.EnterpriseGroup EG
		on SPC.EnterPriseGroupId = EG.EnterpriseGroupId
		and EG.CurrentSnapshotFlag = 1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLegalEntity IRE
		on SPC.InsuranceReferenceLegalEntityId = IRE.InsuranceReferenceLegalEntityId
		and IRE.CurrentSnapshotFlag = 1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product Pr
		on CCD.ProductAKId = Pr.ProductAKId
		and Pr.CurrentSnapshotFlag = 1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness IRLOB
		on CCD.InsuranceReferenceLineOfBusinessAKID = IRLOB.InsuranceReferenceLineOfBusinessAKId
		and IRLOB.CurrentSnapshotFlag =1
		left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingPlan RPDT
		on CCD.RatingPlanAKId=RPDT.RatingPlanAKId and RPDT.CurrentSnapshotFlag=1
		join @{pipeline().parameters.DB_NAME_DATAMART}.@{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceDim IRD
		on isnull(Pr.ProductCode, '000') = IRD.ProductCode 
		and isnull(PO.PolicyOfferingCode, '000') = IRD.PolicyOfferingCode 
		and isnull(ISe.InsuranceSegmentCode, 'N/A') = IRD.InsuranceSegmentCode 
		and isnull(SPC.StrategicProfitCenterCode, '6') = IRD.StrategicProfitCenterCode 
		and isnull(IRLOB.InsuranceReferenceLineOfBusinessCode, '000') = IRD.InsuranceReferenceLineOfBusinessCode 
		and isnull(EG.EnterpriseGroupCode, '1')=IRD.EnterpriseGroupCode 
		and isnull(IRE.InsuranceReferenceLegalEntityCode, '1')=IRD.InsuranceReferenceLegalEntityCode
		and isnull(RPDT.RatingPlanCode, '1')=IRD.RatingPlanCode
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id ORDER BY InsuranceReferenceDimId) = 1
),
LKP_SalesDivisionDim AS (
	SELECT
	SalesDivisionDimID,
	AgencyAKID
	FROM (
		Select A.AgencyAKID AS AgencyAKID, 
		SDD.SalesDivisionDimID AS SalesDivisionDimID
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency A,
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.RegionalSalesManager RSM,
		@{pipeline().parameters.DB_NAME_DATAMART}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SalesDivisionDim SDD
		WHERE A.CurrentSnapshotFlag =1
		AND RSM.RegionalSalesManagerAKID = A.RegionalSalesManagerAKID
		AND RSM.CurrentSnapshotFlag = 1
		AND RSM.SalesDirectorAKID = SDD.EDWSalesDirectorAKID
		AND A.SalesTerritoryAKID = SDD.EDWSalesTerritoryAKID
		AND RSM.RegionalSalesManagerAKID = SDD.EDWRegionalSalesManagerAKID
		AND SDD.CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID ORDER BY SalesDivisionDimID) = 1
),
mplt_Claim_Rep_Dim_Hist_Id_Payment_Entry_Operator AS (WITH
	INPUT AS (
		
	),
	Evaluate_Inputs AS (
		SELECT
		payment_dim_id,
		transaction_date
		FROM INPUT
	),
	LKP_Claim_Payment_Dim AS (
		SELECT
		pay_entry_oper_id,
		transaction_date,
		claim_pay_dim_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				pay_entry_oper_id,
				transaction_date,
				claim_pay_dim_id,
				eff_from_date,
				eff_to_date
			FROM claim_payment_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_dim_id,eff_from_date,eff_to_date ORDER BY pay_entry_oper_id DESC) = 1
	),
	EXP_Prepare_Results AS (
		SELECT
		pay_entry_oper_id,
		-- *INF*: IIF(isnull(pay_entry_oper_id),pay_entry_oper_id,ltrim(rtrim(pay_entry_oper_id)))
		-- 
		-- -- if nothing do nothing, else trim 
		IFF(pay_entry_oper_id IS NULL, pay_entry_oper_id, ltrim(rtrim(pay_entry_oper_id))) AS pay_entry_oper_id_out,
		transaction_date
		FROM LKP_Claim_Payment_Dim
	),
	Claim_Rep_Dim AS (
		SELECT
		claim_rep_dim_id,
		claim_rep_wbconnect_user_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT targetRow.claim_rep_dim_id AS claim_rep_dim_id,
				anymatch.claim_rep_wbconnect_user_id AS claim_rep_wbconnect_user_id,
				targetRow.eff_from_date AS eff_from_date,
				targetRow.eff_to_date AS eff_to_date
			FROM claim_representative_dim anymatch
			JOIN claim_representative_dim targetRow on anymatch.edw_claim_rep_ak_id = targetRow.edw_claim_rep_ak_id 
			order by anymatch.eff_from_date desc--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_wbconnect_user_id,eff_from_date,eff_to_date ORDER BY claim_rep_dim_id) = 1
	),
	Prepare_Output AS (
		SELECT
		claim_rep_dim_id,
		-- *INF*: IIF(isnull(claim_rep_dim_id),-1,claim_rep_dim_id)
		IFF(claim_rep_dim_id IS NULL, - 1, claim_rep_dim_id) AS claim_rep_dim_id_OUT
		FROM Claim_Rep_Dim
	),
	OUTPUT AS (
		SELECT
		claim_rep_dim_id_OUT AS Claim_Rep_Dim_Id
		FROM Prepare_Output
	),
),
mplt_Claim_Rep_Dim_Id_Payment_Entry_Operator AS (WITH
	INPUT AS (
		
	),
	Evaluate_Inputs AS (
		SELECT
		payment_dim_id,
		transaction_date
		FROM INPUT
	),
	LKP_Claim_Payment_Dim AS (
		SELECT
		pay_entry_oper_id,
		transaction_date,
		claim_pay_dim_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				pay_entry_oper_id,
				transaction_date,
				claim_pay_dim_id,
				eff_from_date,
				eff_to_date
			FROM claim_payment_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_dim_id,eff_from_date,eff_to_date ORDER BY pay_entry_oper_id DESC) = 1
	),
	EXP_Prepare_Results AS (
		SELECT
		pay_entry_oper_id,
		-- *INF*: IIF(isnull(pay_entry_oper_id),pay_entry_oper_id,ltrim(rtrim(pay_entry_oper_id)))
		-- 
		-- -- if nothing do nothing, else trim 
		IFF(pay_entry_oper_id IS NULL, pay_entry_oper_id, ltrim(rtrim(pay_entry_oper_id))) AS pay_entry_oper_id_out,
		transaction_date
		FROM LKP_Claim_Payment_Dim
	),
	Claim_Rep_Dim AS (
		SELECT
		claim_rep_dim_id,
		claim_rep_wbconnect_user_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT targetRow.claim_rep_dim_id AS claim_rep_dim_id,
				anymatch.claim_rep_wbconnect_user_id AS claim_rep_wbconnect_user_id,
				targetRow.eff_from_date AS eff_from_date,
				targetRow.eff_to_date AS eff_to_date
			FROM claim_representative_dim anymatch
			JOIN claim_representative_dim targetRow on anymatch.edw_claim_rep_ak_id = targetRow.edw_claim_rep_ak_id 
			order by anymatch.eff_from_date desc--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_wbconnect_user_id,eff_from_date,eff_to_date ORDER BY claim_rep_dim_id) = 1
	),
	Prepare_Output AS (
		SELECT
		claim_rep_dim_id,
		-- *INF*: IIF(isnull(claim_rep_dim_id),-1,claim_rep_dim_id)
		IFF(claim_rep_dim_id IS NULL, - 1, claim_rep_dim_id) AS claim_rep_dim_id_OUT
		FROM Claim_Rep_Dim
	),
	OUTPUT AS (
		SELECT
		claim_rep_dim_id_OUT AS Claim_Rep_Dim_Id
		FROM Prepare_Output
	),
),
mplt_Claim_occurrence_dim_hist_id AS (WITH
	LKP_claim_representative AS (
		SELECT
		claim_rep_wbconnect_user_id,
		claim_rep_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
			CASE claim_representative.claim_rep_wbconnect_user_id  WHEN 'N/A' THEN claim_representative.claim_rep_key 
			ELSE claim_representative.claim_rep_wbconnect_user_id END AS claim_rep_wbconnect_user_id, claim_representative.claim_rep_ak_id as claim_rep_ak_id, claim_representative.eff_from_date as eff_from_date, claim_representative.eff_to_date as eff_to_date FROM claim_representative
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_ak_id,eff_from_date,eff_to_date ORDER BY claim_rep_wbconnect_user_id DESC) = 1
	),
	LKP_claim_rep_dim AS (
		SELECT
		claim_rep_dim_id,
		claim_rep_wbconnect_user_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_rep_dim_id,
				claim_rep_wbconnect_user_id,
				eff_from_date,
				eff_to_date
			FROM claim_representative_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_wbconnect_user_id,eff_from_date,eff_to_date ORDER BY claim_rep_dim_id DESC) = 1
	),
	INPUT AS (
		
	),
	EXP_get_values AS (
		SELECT
		IN_claimant_cov_det_ak_id,
		IN_trans_date
		FROM INPUT
	),
	LKP_Claimant_coverage_detail AS (
		SELECT
		claim_party_occurrence_ak_id,
		claimant_cov_det_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_party_occurrence_ak_id,
				claimant_cov_det_ak_id,
				eff_from_date,
				eff_to_date
			FROM claimant_coverage_detail
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,eff_from_date,eff_to_date ORDER BY claim_party_occurrence_ak_id DESC) = 1
	),
	LKP_Claim_Party_occurrence AS (
		SELECT
		claim_occurrence_ak_id,
		claim_case_ak_id,
		claim_party_occurrence_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT claim_party_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, claim_party_occurrence.claim_case_ak_id as claim_case_ak_id, claim_party_occurrence.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, claim_party_occurrence.eff_from_date as eff_from_date, claim_party_occurrence.eff_to_date as eff_to_date 
			FROM claim_party_occurrence
			WHERE
			claim_party_occurrence.claim_party_role_code in ('CMT','CLMT')
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_occurrence_ak_id) = 1
	),
	LKP_Claim_Rep_Occurrence_PLH AS (
		SELECT
		claim_rep_ak_id,
		eff_from_date,
		eff_to_date,
		claim_occurrence_ak_id
		FROM (
			SELECT claim_representative_occurrence.claim_rep_ak_id as claim_rep_ak_id, claim_representative_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, claim_representative_occurrence.eff_from_date as eff_from_date, claim_representative_occurrence.eff_to_date as eff_to_date FROM claim_representative_occurrence
			WHERE LTRIM(RTRIM(claim_representative_occurrence.claim_rep_role_code))  = 'PLH'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_rep_ak_id DESC) = 1
	),
	LKP_Claim_Case AS (
		SELECT
		claim_case_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_case_ak_id,
				eff_from_date,
				eff_to_date
			FROM claim_case
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_case_ak_id,eff_from_date,eff_to_date ORDER BY claim_case_ak_id) = 1
	),
	EXP_get_reserve_calc_ids AS (
		SELECT
		LKP_Claim_Party_occurrence.claim_occurrence_ak_id,
		-- *INF*: IIF(ISNULL(claim_occurrence_ak_id), -1, claim_occurrence_ak_id)
		IFF(claim_occurrence_ak_id IS NULL, - 1, claim_occurrence_ak_id) AS claim_occurrence_ak_id_out,
		EXP_get_values.IN_trans_date
		FROM EXP_get_values
		LEFT JOIN LKP_Claim_Party_occurrence
		ON LKP_Claim_Party_occurrence.claim_party_occurrence_ak_id = LKP_Claimant_coverage_detail.claim_party_occurrence_ak_id AND LKP_Claim_Party_occurrence.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Party_occurrence.eff_to_date >= EXP_get_values.IN_trans_date
	),
	LKP_claim_occurrence_Date AS (
		SELECT
		claim_occurrence_id,
		pol_key_ak_id,
		claim_loss_date,
		claim_discovery_date,
		claim_cat_start_date,
		claim_cat_end_date,
		claim_created_by_key,
		claim_occurrence_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_occurrence_id,
				pol_key_ak_id,
				claim_loss_date,
				claim_discovery_date,
				claim_cat_start_date,
				claim_cat_end_date,
				claim_created_by_key,
				claim_occurrence_ak_id,
				eff_from_date,
				eff_to_date
			FROM claim_occurrence
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_occurrence_id DESC) = 1
	),
	LKP_Claim_Rep_Occurrence_Handler AS (
		SELECT
		claim_rep_ak_id,
		claim_assigned_date,
		eff_from_date,
		eff_to_date,
		claim_occurrence_ak_id
		FROM (
			SELECT claim_representative_occurrence.claim_rep_ak_id as claim_rep_ak_id, claim_representative_occurrence.claim_assigned_date as claim_assigned_date, claim_representative_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, claim_representative_occurrence.eff_from_date as eff_from_date, claim_representative_occurrence.eff_to_date as eff_to_date FROM claim_representative_occurrence
			WHERE LTRIM(RTRIM(claim_representative_occurrence.claim_rep_role_code))  = 'H'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_rep_ak_id DESC) = 1
	),
	LKP_Claim_Rep_Occurrence_Examiner AS (
		SELECT
		claim_rep_ak_id,
		eff_from_date,
		eff_to_date,
		claim_occurrence_ak_id
		FROM (
			SELECT claim_representative_occurrence.claim_rep_ak_id as claim_rep_ak_id, claim_representative_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, claim_representative_occurrence.eff_from_date as eff_from_date, claim_representative_occurrence.eff_to_date as eff_to_date FROM claim_representative_occurrence
			WHERE LTRIM(RTRIM(claim_representative_occurrence.claim_rep_role_code))  = 'E'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_rep_ak_id DESC) = 1
	),
	LKP_claim_occurrence_dim AS (
		SELECT
		claim_occurrence_dim_id,
		source_claim_rpted_date,
		claim_rpted_date,
		claim_scripted_date,
		claim_open_date,
		claim_close_date,
		claim_reopen_date,
		claim_closed_after_reopen_date,
		claim_notice_only_date,
		edw_claim_occurrence_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_occurrence_dim_id,
				source_claim_rpted_date,
				claim_rpted_date,
				claim_scripted_date,
				claim_open_date,
				claim_close_date,
				claim_reopen_date,
				claim_closed_after_reopen_date,
				claim_notice_only_date,
				edw_claim_occurrence_ak_id,
				eff_from_date,
				eff_to_date
			FROM claim_occurrence_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_occurrence_dim_id DESC) = 1
	),
	LKP_Claim_Created_by_rep_ak_id AS (
		SELECT
		claim_rep_ak_id,
		claim_rep_wbconnect_user_id,
		claim_rep_key,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_rep_ak_id,
				claim_rep_wbconnect_user_id,
				claim_rep_key,
				eff_from_date,
				eff_to_date
			FROM claim_representative
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_key,eff_from_date,eff_to_date ORDER BY claim_rep_ak_id) = 1
	),
	LKP_Claim_Case_Dim AS (
		SELECT
		claim_case_dim_id,
		edw_claim_case_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				claim_case_dim_id,
				edw_claim_case_ak_id,
				eff_from_date,
				eff_to_date
			FROM claim_case_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_case_ak_id,eff_from_date,eff_to_date ORDER BY claim_case_dim_id) = 1
	),
	LKP_V2_policy AS (
		SELECT
		pol_id,
		contract_cust_ak_id,
		agency_ak_id,
		AgencyAKID,
		StrategicProfitCenterAKId,
		InsuranceSegmentAKId,
		PolicyOfferingAKId,
		pol_eff_date,
		pol_exp_date,
		pol_sym,
		pol_num,
		pol_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT policy.pol_id as pol_id, policy.contract_cust_ak_id as contract_cust_ak_id, 
			policy.agency_ak_id as agency_ak_id,
			policy.AgencyAKID as AgencyAKID, 
			policy.StrategicProfitCenterAKId as StrategicProfitCenterAKId,
			policy.InsuranceSegmentAKId as InsuranceSegmentAKId,
			policy.PolicyOfferingAKId as PolicyOfferingAKId,
			policy.pol_eff_date as pol_eff_date, policy.pol_exp_date as pol_exp_date, policy.pol_sym as pol_sym, policy.pol_num as pol_num, policy.pol_ak_id as pol_ak_id, policy.eff_from_date as eff_from_date, policy.eff_to_date as eff_to_date 
			FROM 
			v2.policy policy
			WHERE 
			policy.pol_ak_id IN (select distinct pol_key_ak_id from claim_occurrence)
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,eff_from_date,eff_to_date ORDER BY pol_id DESC) = 1
	),
	LKP_contract_customer_dim AS (
		SELECT
		contract_cust_dim_id,
		edw_contract_cust_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT contract_customer_dim.contract_cust_dim_id as contract_cust_dim_id, contract_customer_dim.edw_contract_cust_ak_id as edw_contract_cust_ak_id, contract_customer_dim.eff_from_date as eff_from_date, contract_customer_dim.eff_to_date as eff_to_date 
			FROM contract_customer_dim
			WHERE edw_contract_cust_ak_id IN
			(
			SELECT DISTINCT CC.contract_cust_ak_id 
			FROM @{pipeline().parameters.DB_NAME_EDW}.dbo.contract_customer CC, @{pipeline().parameters.DB_NAME_EDW}.V2.policy P, @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_occurrence CO
			WHERE CC.contract_cust_ak_id = P.contract_cust_ak_id
			AND CO.pol_key_ak_id = P.pol_ak_id
			AND P.crrnt_snpsht_flag = 1
			AND CC.crrnt_snpsht_flag = 1
			)
			
			--- 2/12/2014 : Modified the Lookup Query to join on AK ID values instead of Natural Key
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_contract_cust_ak_id,eff_from_date,eff_to_date ORDER BY contract_cust_dim_id DESC) = 1
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
			FROM V3.AgencyDim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyAKID,EffectiveDate,ExpirationDate ORDER BY AgencyDimID DESC) = 1
	),
	LKP_V2_Agency AS (
		SELECT
		SalesTerritoryAKID,
		RegionalSalesManagerAKID,
		AgencyAKID,
		EffectiveDate,
		ExpirationDate
		FROM (
			SELECT 
				SalesTerritoryAKID,
				RegionalSalesManagerAKID,
				AgencyAKID,
				EffectiveDate,
				ExpirationDate
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID,EffectiveDate,ExpirationDate ORDER BY SalesTerritoryAKID) = 1
	),
	LKP_agency_dim AS (
		SELECT
		agency_dim_id,
		edw_agency_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				agency_dim_id,
				edw_agency_ak_id,
				eff_from_date,
				eff_to_date
			FROM V2.agency_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_agency_ak_id,eff_from_date,eff_to_date ORDER BY agency_dim_id DESC) = 1
	),
	LKP_Policy_Dim AS (
		SELECT
		pol_dim_id,
		edw_pol_pk_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
			policy_dim.pol_dim_id as pol_dim_id, 
			policy_dim.edw_pol_pk_id as edw_pol_pk_id, 
			policy_dim.eff_from_date as eff_from_date, 
			policy_dim.eff_to_date as eff_to_date 
			FROM policy_dim
			WHERE edw_pol_pk_id IN 
			(SELECT policy.pol_id as pol_id FROM @{pipeline().parameters.DB_NAME_EDW}.v2.policy policy
			WHERE policy.pol_ak_id IN (select distinct pol_key_ak_id from @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_occurrence))
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_pk_id,eff_from_date,eff_to_date ORDER BY pol_dim_id DESC) = 1
	),
	EXP_Lkp_Dim_ids AS (
		SELECT
		EXP_get_reserve_calc_ids.IN_trans_date,
		LKP_Claim_Rep_Occurrence_Handler.claim_rep_ak_id AS claim_rep_primary_rep_ak_id,
		-- *INF*: :LKP.LKP_CLAIM_REPRESENTATIVE(claim_rep_primary_rep_ak_id, IN_trans_date)
		LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_rep_ak_id_IN_trans_date.claim_rep_wbconnect_user_id AS v_claim_rep_primary_rep_wbconnect_user_id,
		LKP_Claim_Rep_Occurrence_Examiner.claim_rep_ak_id AS claim_rep_examiner_ak_id,
		-- *INF*: :LKP.LKP_CLAIM_REPRESENTATIVE(claim_rep_examiner_ak_id, IN_trans_date)
		LKP_CLAIM_REPRESENTATIVE_claim_rep_examiner_ak_id_IN_trans_date.claim_rep_wbconnect_user_id AS v_claim_rep_examiner_wbconnect_user_id,
		LKP_Claim_Rep_Occurrence_PLH.claim_rep_ak_id AS claim_rep_primary_lit_handler_ak_id,
		-- *INF*: :LKP.LKP_CLAIM_REPRESENTATIVE(claim_rep_primary_lit_handler_ak_id, IN_trans_date)
		LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_lit_handler_ak_id_IN_trans_date.claim_rep_wbconnect_user_id AS v_claim_rep_primary_lit_handler_wbconnect_user_id,
		-- *INF*: :LKP.LKP_CLAIM_REP_DIM(claim_rep_primary_rep_ak_id, v_claim_rep_primary_rep_wbconnect_user_id, IN_trans_date)
		LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date.claim_rep_dim_id AS claim_rep_dim_prim_claim_rep_id,
		-- *INF*: :LKP.LKP_CLAIM_REP_DIM(claim_rep_examiner_ak_id, v_claim_rep_examiner_wbconnect_user_id, IN_trans_date)
		LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date.claim_rep_dim_id AS claim_rep_dim_examiner_id,
		-- *INF*: :LKP.LKP_CLAIM_REP_DIM(claim_rep_primary_lit_handler_ak_id, v_claim_rep_primary_lit_handler_wbconnect_user_id, IN_trans_date)
		LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date.claim_rep_dim_id AS claim_rep_dim_prim_litigation_handler_id,
		LKP_Claim_Created_by_rep_ak_id.claim_rep_ak_id,
		LKP_Claim_Created_by_rep_ak_id.claim_rep_wbconnect_user_id,
		-- *INF*: :LKP.LKP_CLAIM_REP_DIM(claim_rep_ak_id,claim_rep_wbconnect_user_id,IN_trans_date)
		LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date.claim_rep_dim_id AS claim_created_by_id
		FROM EXP_get_reserve_calc_ids
		LEFT JOIN LKP_Claim_Created_by_rep_ak_id
		ON LKP_Claim_Created_by_rep_ak_id.claim_rep_key = LKP_claim_occurrence_Date.claim_created_by_key AND LKP_Claim_Created_by_rep_ak_id.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Created_by_rep_ak_id.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_Examiner
		ON LKP_Claim_Rep_Occurrence_Examiner.claim_occurrence_ak_id = LKP_Claim_Party_occurrence.claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_Examiner.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_Examiner.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_Handler
		ON LKP_Claim_Rep_Occurrence_Handler.claim_occurrence_ak_id = LKP_Claim_Party_occurrence.claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_Handler.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_Handler.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_PLH
		ON LKP_Claim_Rep_Occurrence_PLH.claim_occurrence_ak_id = LKP_Claim_Party_occurrence.claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_PLH.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_PLH.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_CLAIM_REPRESENTATIVE LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_rep_ak_id_IN_trans_date
		ON LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_rep_ak_id_IN_trans_date.claim_rep_ak_id = claim_rep_primary_rep_ak_id
		AND LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_rep_ak_id_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REPRESENTATIVE LKP_CLAIM_REPRESENTATIVE_claim_rep_examiner_ak_id_IN_trans_date
		ON LKP_CLAIM_REPRESENTATIVE_claim_rep_examiner_ak_id_IN_trans_date.claim_rep_ak_id = claim_rep_examiner_ak_id
		AND LKP_CLAIM_REPRESENTATIVE_claim_rep_examiner_ak_id_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REPRESENTATIVE LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_lit_handler_ak_id_IN_trans_date
		ON LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_lit_handler_ak_id_IN_trans_date.claim_rep_ak_id = claim_rep_primary_lit_handler_ak_id
		AND LKP_CLAIM_REPRESENTATIVE_claim_rep_primary_lit_handler_ak_id_IN_trans_date.eff_from_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REP_DIM LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date
		ON LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date.claim_rep_wbconnect_user_id = claim_rep_primary_rep_ak_id
		AND LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date.eff_from_date = v_claim_rep_primary_rep_wbconnect_user_id
		AND LKP_CLAIM_REP_DIM_claim_rep_primary_rep_ak_id_v_claim_rep_primary_rep_wbconnect_user_id_IN_trans_date.eff_to_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REP_DIM LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date
		ON LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date.claim_rep_wbconnect_user_id = claim_rep_examiner_ak_id
		AND LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date.eff_from_date = v_claim_rep_examiner_wbconnect_user_id
		AND LKP_CLAIM_REP_DIM_claim_rep_examiner_ak_id_v_claim_rep_examiner_wbconnect_user_id_IN_trans_date.eff_to_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REP_DIM LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date
		ON LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date.claim_rep_wbconnect_user_id = claim_rep_primary_lit_handler_ak_id
		AND LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date.eff_from_date = v_claim_rep_primary_lit_handler_wbconnect_user_id
		AND LKP_CLAIM_REP_DIM_claim_rep_primary_lit_handler_ak_id_v_claim_rep_primary_lit_handler_wbconnect_user_id_IN_trans_date.eff_to_date = IN_trans_date
	
		LEFT JOIN LKP_CLAIM_REP_DIM LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date
		ON LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date.claim_rep_wbconnect_user_id = claim_rep_ak_id
		AND LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date.eff_from_date = claim_rep_wbconnect_user_id
		AND LKP_CLAIM_REP_DIM_claim_rep_ak_id_claim_rep_wbconnect_user_id_IN_trans_date.eff_to_date = IN_trans_date
	
	),
	LKP_RegionalSalesManager AS (
		SELECT
		SalesDirectorAKID,
		RegionalSalesManagerAKID,
		EffectiveDate,
		ExpirationDate
		FROM (
			SELECT 
				SalesDirectorAKID,
				RegionalSalesManagerAKID,
				EffectiveDate,
				ExpirationDate
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.RegionalSalesManager
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY RegionalSalesManagerAKID,EffectiveDate,ExpirationDate ORDER BY SalesDirectorAKID) = 1
	),
	OUTPUT AS (
		SELECT
		LKP_claim_occurrence_dim.claim_occurrence_dim_id, 
		LKP_claim_occurrence_Date.claim_loss_date, 
		LKP_claim_occurrence_Date.claim_discovery_date, 
		LKP_claim_occurrence_dim.claim_scripted_date, 
		LKP_claim_occurrence_dim.source_claim_rpted_date, 
		LKP_claim_occurrence_dim.claim_rpted_date AS claim_occurrence_rpted_date, 
		LKP_claim_occurrence_dim.claim_open_date, 
		LKP_claim_occurrence_dim.claim_close_date, 
		LKP_claim_occurrence_dim.claim_reopen_date, 
		LKP_claim_occurrence_dim.claim_closed_after_reopen_date, 
		LKP_claim_occurrence_dim.claim_notice_only_date, 
		LKP_claim_occurrence_Date.claim_cat_start_date, 
		LKP_claim_occurrence_Date.claim_cat_end_date, 
		LKP_Claim_Rep_Occurrence_Handler.claim_assigned_date AS claim_rep_assigned_date, 
		LKP_Claim_Rep_Occurrence_Handler.eff_to_date AS claim_rep_unassigned_date, 
		EXP_Lkp_Dim_ids.claim_rep_dim_prim_claim_rep_id, 
		EXP_Lkp_Dim_ids.claim_rep_dim_examiner_id, 
		EXP_Lkp_Dim_ids.claim_rep_dim_prim_litigation_handler_id, 
		LKP_Policy_Dim.pol_dim_id AS pol_key_dim_id, 
		LKP_V2_policy.pol_eff_date, 
		LKP_V2_policy.pol_exp_date, 
		LKP_AgencyDim.AgencyDimID, 
		EXP_Lkp_Dim_ids.claim_created_by_id, 
		LKP_Claim_Case_Dim.claim_case_dim_id, 
		LKP_contract_customer_dim.contract_cust_dim_id, 
		LKP_V2_policy.pol_sym, 
		LKP_V2_policy.pol_num, 
		LKP_V2_policy.AgencyAKID, 
		LKP_V2_Agency.SalesTerritoryAKID, 
		LKP_V2_Agency.RegionalSalesManagerAKID, 
		LKP_RegionalSalesManager.SalesDirectorAKID, 
		LKP_V2_policy.StrategicProfitCenterAKId, 
		LKP_V2_policy.InsuranceSegmentAKId, 
		LKP_V2_policy.PolicyOfferingAKId, 
		LKP_agency_dim.agency_dim_id, 
		LKP_claim_occurrence_Date.pol_key_ak_id AS PolicyAkid
		FROM EXP_Lkp_Dim_ids
		LEFT JOIN LKP_AgencyDim
		ON LKP_AgencyDim.EDWAgencyAKID = LKP_V2_policy.AgencyAKID AND LKP_AgencyDim.EffectiveDate <= EXP_get_values.IN_trans_date AND LKP_AgencyDim.ExpirationDate >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Case_Dim
		ON LKP_Claim_Case_Dim.edw_claim_case_ak_id = LKP_Claim_Case.claim_case_ak_id AND LKP_Claim_Case_Dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Case_Dim.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Claim_Rep_Occurrence_Handler
		ON LKP_Claim_Rep_Occurrence_Handler.claim_occurrence_ak_id = LKP_Claim_Party_occurrence.claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_Handler.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Claim_Rep_Occurrence_Handler.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_Policy_Dim
		ON LKP_Policy_Dim.edw_pol_pk_id = LKP_V2_policy.pol_id AND LKP_Policy_Dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_Policy_Dim.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_RegionalSalesManager
		ON LKP_RegionalSalesManager.RegionalSalesManagerAKID = LKP_V2_Agency.RegionalSalesManagerAKID AND LKP_RegionalSalesManager.EffectiveDate <= EXP_get_values.IN_trans_date AND LKP_RegionalSalesManager.ExpirationDate >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_V2_Agency
		ON LKP_V2_Agency.AgencyAKID = LKP_V2_policy.AgencyAKID AND LKP_V2_Agency.EffectiveDate <= EXP_get_values.IN_trans_date AND LKP_V2_Agency.ExpirationDate >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_V2_policy
		ON LKP_V2_policy.pol_ak_id = LKP_claim_occurrence_Date.pol_key_ak_id AND LKP_V2_policy.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_V2_policy.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_agency_dim
		ON LKP_agency_dim.edw_agency_ak_id = LKP_V2_policy.agency_ak_id AND LKP_agency_dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_agency_dim.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_claim_occurrence_Date
		ON LKP_claim_occurrence_Date.claim_occurrence_ak_id = LKP_Claim_Party_occurrence.claim_occurrence_ak_id AND LKP_claim_occurrence_Date.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_claim_occurrence_Date.eff_to_date >= EXP_get_values.IN_trans_date
		LEFT JOIN LKP_claim_occurrence_dim
		ON LKP_claim_occurrence_dim.edw_claim_occurrence_ak_id = EXP_get_reserve_calc_ids.claim_occurrence_ak_id_out AND LKP_claim_occurrence_dim.eff_from_date <= EXP_get_reserve_calc_ids.IN_trans_date AND LKP_claim_occurrence_dim.eff_to_date >= EXP_get_reserve_calc_ids.IN_trans_date
		LEFT JOIN LKP_contract_customer_dim
		ON LKP_contract_customer_dim.edw_contract_cust_ak_id = LKP_V2_policy.contract_cust_ak_id AND LKP_contract_customer_dim.eff_from_date <= EXP_get_values.IN_trans_date AND LKP_contract_customer_dim.eff_to_date >= EXP_get_values.IN_trans_date
	),
),
mplt_Strategic_Business_Division_Dim AS (WITH
	INPUT_Strategic_Business_Division AS (
		
	),
	EXP_inputs AS (
		SELECT
		policy_symbol,
		policy_number,
		policy_eff_date AS policy_eff_date_in,
		-- *INF*: IIF(:UDF.DEFAULT_VALUE_FOR_STRINGS(policy_symbol)='N/A','N/A',substr(policy_symbol,1,1))
		IFF(:UDF.DEFAULT_VALUE_FOR_STRINGS(policy_symbol) = 'N/A', 'N/A', substr(policy_symbol, 1, 1)) AS policy_symbol_position_1,
		-- *INF*: IIF(:UDF.DEFAULT_VALUE_FOR_STRINGS(policy_number)='N/A','N/A',substr(policy_number,1,1))
		IFF(:UDF.DEFAULT_VALUE_FOR_STRINGS(policy_number) = 'N/A', 'N/A', substr(policy_number, 1, 1)) AS policy_number_position_1,
		-- *INF*: IIF(isnull(policy_eff_date_in),SYSDATE,policy_eff_date_in)
		IFF(policy_eff_date_in IS NULL, SYSDATE, policy_eff_date_in) AS policy_eff_date
		FROM INPUT_Strategic_Business_Division
	),
	LKP_strategic_business_division_dim AS (
		SELECT
		strtgc_bus_dvsn_dim_id,
		crrnt_snpsht_flag,
		audit_id,
		eff_from_date,
		eff_to_date,
		created_date,
		modified_date,
		edw_strtgc_bus_dvsn_ak_id,
		pol_sym_1,
		pol_num_1,
		pol_eff_date,
		pol_exp_date,
		strtgc_bus_dvsn_code,
		strtgc_bus_dvsn_code_descript,
		policy_symbol_position_IN,
		policy_number_position_IN,
		policy_eff_date_IN
		FROM (
			SELECT 
				strtgc_bus_dvsn_dim_id,
				crrnt_snpsht_flag,
				audit_id,
				eff_from_date,
				eff_to_date,
				created_date,
				modified_date,
				edw_strtgc_bus_dvsn_ak_id,
				pol_sym_1,
				pol_num_1,
				pol_eff_date,
				pol_exp_date,
				strtgc_bus_dvsn_code,
				strtgc_bus_dvsn_code_descript,
				policy_symbol_position_IN,
				policy_number_position_IN,
				policy_eff_date_IN
			FROM strategic_business_division_dim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_sym_1,pol_num_1,pol_eff_date,pol_exp_date ORDER BY strtgc_bus_dvsn_dim_id) = 1
	),
	EXP_check_outputs AS (
		SELECT
		strtgc_bus_dvsn_dim_id,
		edw_strtgc_bus_dvsn_ak_id,
		strtgc_bus_dvsn_code,
		strtgc_bus_dvsn_code_descript,
		-- *INF*: IIF(isnull(strtgc_bus_dvsn_dim_id),-1,strtgc_bus_dvsn_dim_id)
		IFF(strtgc_bus_dvsn_dim_id IS NULL, - 1, strtgc_bus_dvsn_dim_id) AS strtgc_bus_dvsn_id_out,
		-- *INF*: IIF(isnull(edw_strtgc_bus_dvsn_ak_id),-1,edw_strtgc_bus_dvsn_ak_id)
		IFF(edw_strtgc_bus_dvsn_ak_id IS NULL, - 1, edw_strtgc_bus_dvsn_ak_id) AS edw_strtgc_bus_dvsn_ak_id_out,
		-- *INF*: IIF(isnull(strtgc_bus_dvsn_code),'N/A',strtgc_bus_dvsn_code)
		IFF(strtgc_bus_dvsn_code IS NULL, 'N/A', strtgc_bus_dvsn_code) AS strtgc_bus_dvsn_code_out,
		-- *INF*: IIF(isnull(strtgc_bus_dvsn_code_descript),'N/A',strtgc_bus_dvsn_code_descript)
		IFF(strtgc_bus_dvsn_code_descript IS NULL, 'N/A', strtgc_bus_dvsn_code_descript) AS strtgc_bus_dvsn_code_descript_out
		FROM LKP_strategic_business_division_dim
	),
	OUTPUT_return_Strategic_Business_Division AS (
		SELECT
		strtgc_bus_dvsn_id_out AS strtgc_bus_dvsn_dim_id, 
		edw_strtgc_bus_dvsn_ak_id_out AS edw_strtgc_bus_dvsn_ak_id, 
		strtgc_bus_dvsn_code_out AS strtgc_bus_dvsn_code, 
		strtgc_bus_dvsn_code_descript_out AS strtgc_bus_dvsn_code_descript
		FROM EXP_check_outputs
	),
),
EXP_set_default_dim_ids AS (
	SELECT
	mplt_Claim_occurrence_dim_id.claim_occurrence_dim_id,
	-- *INF*: iif(isnull(claim_occurrence_dim_id),-1,claim_occurrence_dim_id)
	IFF(claim_occurrence_dim_id IS NULL, - 1, claim_occurrence_dim_id) AS claim_occurrence_dim_id_out,
	mplt_Claim_occurrence_dim_hist_id.claim_occurrence_dim_id AS claim_occurrence_dim_hist_id,
	-- *INF*: iif(isnull(claim_occurrence_dim_hist_id),-1,claim_occurrence_dim_hist_id)
	IFF(claim_occurrence_dim_hist_id IS NULL, - 1, claim_occurrence_dim_hist_id) AS claim_occurrence_dim_hist_id_out,
	EXP_set_claimant_dim_ids.claimant_dim_id,
	-- *INF*: iif(isnull(claimant_dim_id),-1,claimant_dim_id)
	IFF(claimant_dim_id IS NULL, - 1, claimant_dim_id) AS claimant_dim_id_out,
	EXP_set_claimant_dim_ids.claimant_dim_hist_id,
	-- *INF*: iif(isnull(claimant_dim_hist_id),-1,claimant_dim_hist_id)
	IFF(claimant_dim_hist_id IS NULL, - 1, claimant_dim_hist_id) AS claimant_dim_hist_id_out,
	EXP_set_claimant_dim_ids.claimant_cov_dim_id,
	-- *INF*: iif(isnull(claimant_cov_dim_id),-1,claimant_cov_dim_id)
	IFF(claimant_cov_dim_id IS NULL, - 1, claimant_cov_dim_id) AS claimant_cov_dim_id_out,
	EXP_set_claimant_dim_ids.claimant_cov_dim_hist_id,
	-- *INF*: iif(isnull(claimant_cov_dim_hist_id),-1,claimant_cov_dim_hist_id)
	IFF(claimant_cov_dim_hist_id IS NULL, - 1, claimant_cov_dim_hist_id) AS claimant_cov_dim_hist_id_out,
	EXP_set_claimant_dim_ids.cov_dim_id,
	-- *INF*: iif(isnull(cov_dim_id),-1,cov_dim_id)
	IFF(cov_dim_id IS NULL, - 1, cov_dim_id) AS cov_dim_id_out,
	EXP_set_claimant_dim_ids.cov_dim_hist_id,
	-- *INF*: iif(isnull(cov_dim_hist_id),-1,cov_dim_hist_id)
	IFF(cov_dim_hist_id IS NULL, - 1, cov_dim_hist_id) AS cov_dim_hist_id_out,
	EXP_set_claimant_dim_ids.claim_trans_type_dim_id,
	-- *INF*: iif(isnull(claim_trans_type_dim_id),-1,claim_trans_type_dim_id)
	IFF(claim_trans_type_dim_id IS NULL, - 1, claim_trans_type_dim_id) AS claim_trans_type_dim_id_out,
	EXP_set_claimant_dim_ids.claim_financial_type_dim_id,
	-- *INF*: iif(isnull(claim_financial_type_dim_id),-1,claim_financial_type_dim_id)
	IFF(claim_financial_type_dim_id IS NULL, - 1, claim_financial_type_dim_id) AS claim_financial_type_dim_id_out,
	-- *INF*: :LKP.LKP_CLAIM_REP_DIM_ALT(trans_date,IN_claimant_cov_det_ak_id)
	LKP_CLAIM_REP_DIM_ALT_trans_date_IN_claimant_cov_det_ak_id.claim_rep_dim_id AS claim_rep_dim_prim_claim_rep_id_alt,
	-- *INF*: IIF(ISNULL(claim_rep_dim_prim_claim_rep_id_alt),-1,claim_rep_dim_prim_claim_rep_id_alt)
	IFF(claim_rep_dim_prim_claim_rep_id_alt IS NULL, - 1, claim_rep_dim_prim_claim_rep_id_alt) AS claim_rep_dim_prim_claim_rep_id_alt_out,
	mplt_Claim_occurrence_dim_id.claim_rep_dim_prim_claim_rep_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_prim_claim_rep_id), IIF(source_sys_id = 'DCT',claim_rep_dim_prim_claim_rep_id_alt_out,-1), claim_rep_dim_prim_claim_rep_id)
	IFF(claim_rep_dim_prim_claim_rep_id IS NULL, IFF(source_sys_id = 'DCT', claim_rep_dim_prim_claim_rep_id_alt_out, - 1), claim_rep_dim_prim_claim_rep_id) AS claim_rep_prim_claim_rep_dim_id_out,
	mplt_Claim_occurrence_dim_hist_id.claim_rep_dim_prim_claim_rep_id AS claim_rep_prim_claim_rep_dim_hist_id,
	-- *INF*: IIF(ISNULL(claim_rep_prim_claim_rep_dim_hist_id), -1, claim_rep_prim_claim_rep_dim_hist_id)
	IFF(claim_rep_prim_claim_rep_dim_hist_id IS NULL, - 1, claim_rep_prim_claim_rep_dim_hist_id) AS claim_rep_prim_claim_rep_dim_hist_id_out,
	mplt_Claim_occurrence_dim_id.claim_rep_dim_examiner_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_examiner_id), -1, claim_rep_dim_examiner_id)
	IFF(claim_rep_dim_examiner_id IS NULL, - 1, claim_rep_dim_examiner_id) AS claim_rep_dim_examiner_id_out,
	mplt_Claim_occurrence_dim_hist_id.claim_rep_dim_examiner_id AS claim_rep_dim_examiner_hist_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_examiner_hist_id), -1, claim_rep_dim_examiner_hist_id)
	IFF(claim_rep_dim_examiner_hist_id IS NULL, - 1, claim_rep_dim_examiner_hist_id) AS claim_rep_dim_examiner_hist_id_out,
	mplt_Claim_occurrence_dim_id.claim_rep_dim_prim_litigation_handler_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_prim_litigation_handler_id), -1, claim_rep_dim_prim_litigation_handler_id)
	IFF(claim_rep_dim_prim_litigation_handler_id IS NULL, - 1, claim_rep_dim_prim_litigation_handler_id) AS claim_rep_dim_prim_litigation_handler_id_out,
	mplt_Claim_occurrence_dim_hist_id.claim_rep_dim_prim_litigation_handler_id AS claim_rep_dim_prim_litigation_handler_hist_id,
	-- *INF*: IIF(ISNULL(claim_rep_dim_prim_litigation_handler_hist_id), -1, claim_rep_dim_prim_litigation_handler_hist_id)
	IFF(claim_rep_dim_prim_litigation_handler_hist_id IS NULL, - 1, claim_rep_dim_prim_litigation_handler_hist_id) AS claim_rep_dim_prim_litigation_handler_hist_id_out,
	mplt_Claim_occurrence_dim_id.pol_key_dim_id,
	-- *INF*: IIF(ISNULL(pol_key_dim_id), -1, pol_key_dim_id)
	IFF(pol_key_dim_id IS NULL, - 1, pol_key_dim_id) AS pol_key_dim_id_out,
	mplt_Claim_occurrence_dim_hist_id.pol_key_dim_id AS pol_key_dim_hist_id,
	-- *INF*: IIF(ISNULL(pol_key_dim_hist_id), -1, pol_key_dim_hist_id)
	IFF(pol_key_dim_hist_id IS NULL, - 1, pol_key_dim_hist_id) AS pol_key_dim_hist_id_out,
	mplt_Claim_occurrence_dim_id.agency_dim_id,
	-- *INF*: IIF(ISNULL(agency_dim_id), -1, agency_dim_id)
	IFF(agency_dim_id IS NULL, - 1, agency_dim_id) AS agency_dim_id_out,
	mplt_Claim_occurrence_dim_hist_id.agency_dim_id AS agency_dim_hist_id,
	-- *INF*: IIF(ISNULL(agency_dim_hist_id), -1, agency_dim_hist_id)
	IFF(agency_dim_hist_id IS NULL, - 1, agency_dim_hist_id) AS agency_dim_hist_id_out,
	EXP_set_claimant_dim_ids.claim_payment_dim_id,
	-- *INF*: IIF(ISNULL(claim_payment_dim_id), -1, claim_payment_dim_id)
	IFF(claim_payment_dim_id IS NULL, - 1, claim_payment_dim_id) AS claim_payment_dim_id_out,
	EXP_set_claimant_dim_ids.claim_payment_dim_hist_id,
	-- *INF*: IIF(ISNULL(claim_payment_dim_hist_id), -1, claim_payment_dim_hist_id)
	IFF(claim_payment_dim_hist_id IS NULL, - 1, claim_payment_dim_hist_id) AS claim_payment_dim_hist_id_out,
	EXP_set_claimant_dim_ids.tax_id,
	EXP_set_claimant_dim_ids.claim_master_1099_list_dim_id AS in_claim_master_1099_list_dim_id,
	-- *INF*: iif(isnull(in_claim_master_1099_list_dim_id),-1,in_claim_master_1099_list_dim_id)
	IFF(in_claim_master_1099_list_dim_id IS NULL, - 1, in_claim_master_1099_list_dim_id) AS claim_master_1099_list_dim_id,
	EXP_set_claimant_dim_ids.claim_pay_ctgry_type_dim_id,
	-- *INF*: IIF(ISNULL(claim_pay_ctgry_type_dim_id),-1,claim_pay_ctgry_type_dim_id)
	IFF(claim_pay_ctgry_type_dim_id IS NULL, - 1, claim_pay_ctgry_type_dim_id) AS claim_pay_ctgry_type_dim_id_out,
	EXP_set_claimant_dim_ids.claim_pay_ctgry_type_dim_hist_id,
	-- *INF*: IIF(ISNULL(claim_pay_ctgry_type_dim_hist_id),-1,claim_pay_ctgry_type_dim_hist_id)
	IFF(claim_pay_ctgry_type_dim_hist_id IS NULL, - 1, claim_pay_ctgry_type_dim_hist_id) AS claim_pay_ctgry_type_dim_hist_id_out,
	mplt_Claim_occurrence_dim_id.claim_created_by_id,
	-- *INF*: IIF(ISNULL(claim_created_by_id),-1,claim_created_by_id)
	IFF(claim_created_by_id IS NULL, - 1, claim_created_by_id) AS claim_created_by_dim_id,
	mplt_Claim_occurrence_dim_id.claim_case_dim_id,
	-- *INF*: IIF(ISNULL(claim_case_dim_id),-1,claim_case_dim_id)
	IFF(claim_case_dim_id IS NULL, - 1, claim_case_dim_id) AS claim_case_dim_id_out,
	mplt_Claim_occurrence_dim_hist_id.claim_case_dim_id AS claim_case_dim_hist_id,
	-- *INF*: IIF(ISNULL(claim_case_dim_hist_id),-1,claim_case_dim_hist_id)
	IFF(claim_case_dim_hist_id IS NULL, - 1, claim_case_dim_hist_id) AS claim_case_dim_hist_id_out,
	EXP_set_claimant_dim_ids.claim_subrogation_dim_id,
	-- *INF*: IIF(ISNULL(claim_subrogation_dim_id), -1, claim_subrogation_dim_id)
	IFF(claim_subrogation_dim_id IS NULL, - 1, claim_subrogation_dim_id) AS claim_subrogation_dim_id_out,
	-1 AS claim_trans_oper_dim_id,
	EXP_set_claimant_dim_ids.trans_date,
	EXP_set_claimant_dim_ids.reprocess_date,
	mplt_Claim_occurrence_dim_id.claim_loss_date,
	mplt_Claim_occurrence_dim_id.claim_discovery_date,
	mplt_Claim_occurrence_dim_id.source_claim_rpted_date,
	mplt_Claim_occurrence_dim_id.claim_scripted_date,
	mplt_Claim_occurrence_dim_id.claim_occurrence_rpted_date,
	mplt_Claim_occurrence_dim_id.claim_open_date,
	mplt_Claim_occurrence_dim_id.claim_close_date,
	mplt_Claim_occurrence_dim_id.claim_reopen_date,
	mplt_Claim_occurrence_dim_id.claim_closed_after_reopen_date,
	mplt_Claim_occurrence_dim_id.claim_notice_only_date,
	mplt_Claim_occurrence_dim_id.claim_cat_start_date,
	mplt_Claim_occurrence_dim_id.claim_cat_end_date,
	mplt_Claim_occurrence_dim_id.claim_rep_assigned_date,
	mplt_Claim_occurrence_dim_id.claim_rep_unassigned_date,
	EXP_set_claimant_dim_ids.claim_trans_id,
	mplt_Claim_occurrence_dim_id.pol_eff_date,
	mplt_Claim_occurrence_dim_id.pol_exp_date,
	EXP_set_claimant_dim_ids.IN_claimant_cov_det_ak_id,
	EXP_set_claimant_dim_ids.IN_cause_of_loss,
	EXP_set_claimant_dim_ids.IN_reserve_ctgry,
	EXP_set_claimant_dim_ids.IN_type_disability,
	EXP_set_claimant_dim_ids.financial_type_code,
	EXP_set_claimant_dim_ids.trans_code,
	EXP_set_claimant_dim_ids.trans_ctgry_code,
	EXP_set_claimant_dim_ids.trans_amt,
	EXP_set_claimant_dim_ids.trans_hist_amt,
	EXP_set_claimant_dim_ids.source_sys_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	'000000000000000' AS err_flag,
	-1 AS default_dim_id,
	EXP_set_claimant_dim_ids.referred_to_subrogation_date,
	EXP_set_claimant_dim_ids.pay_start_date,
	EXP_set_claimant_dim_ids.closure_date,
	mplt_Claim_occurrence_dim_id.contract_cust_dim_id,
	-- *INF*: IIF(ISNULL(contract_cust_dim_id),-1,contract_cust_dim_id)
	IFF(contract_cust_dim_id IS NULL, - 1, contract_cust_dim_id) AS contract_cust_dim_id_out,
	mplt_Claim_occurrence_dim_hist_id.contract_cust_dim_id AS contract_cust_dim_hist_id,
	-- *INF*: IIF(ISNULL(contract_cust_dim_hist_id),-1,contract_cust_dim_hist_id)
	IFF(contract_cust_dim_hist_id IS NULL, - 1, contract_cust_dim_hist_id) AS contract_cust_dim_hist_id_out,
	EXP_set_claimant_dim_ids.pms_acct_entered_date,
	mplt_Strategic_Business_Division_Dim.strtgc_bus_dvsn_dim_id,
	mplt_Claim_occurrence_dim_hist_id.claim_loss_date AS claim_loss_date_hist_id,
	mplt_Claim_occurrence_dim_hist_id.claim_discovery_date AS claim_discovery_date_hist_id,
	mplt_Claim_occurrence_dim_hist_id.claim_scripted_date AS claim_scripted_date_hist_id,
	mplt_Claim_occurrence_dim_hist_id.source_claim_rpted_date AS source_claim_rpted_date_hist_id,
	mplt_Claim_occurrence_dim_hist_id.claim_occurrence_rpted_date AS claim_occurrence_rpted_date_hist_id,
	mplt_Claim_occurrence_dim_hist_id.claim_open_date AS claim_open_date_hist_id,
	mplt_Claim_occurrence_dim_hist_id.claim_close_date AS claim_close_date_hist_id,
	mplt_Claim_occurrence_dim_hist_id.claim_reopen_date AS claim_reopen_date_hist_id,
	mplt_Claim_occurrence_dim_hist_id.claim_closed_after_reopen_date AS claim_closed_after_reopen_date_hist_id,
	mplt_Claim_occurrence_dim_hist_id.claim_notice_only_date AS claim_notice_only_date_hist_id,
	mplt_Claim_occurrence_dim_hist_id.claim_cat_start_date AS claim_cat_start_date_hist_id,
	mplt_Claim_occurrence_dim_hist_id.claim_cat_end_date AS claim_cat_end_date_hist_id,
	mplt_Claim_occurrence_dim_hist_id.claim_rep_assigned_date AS claim_rep_assigned_date_hist_id,
	mplt_Claim_occurrence_dim_hist_id.claim_rep_unassigned_date AS claim_rep_unassigned_date_hist_id,
	mplt_Claim_occurrence_dim_hist_id.pol_eff_date AS pol_eff_date_hist_id,
	mplt_Claim_occurrence_dim_hist_id.pol_exp_date AS pol_exp_date_hist_id,
	mplt_Claim_occurrence_dim_hist_id.claim_created_by_id AS claim_created_by_id_hist_id,
	mplt_Claim_occurrence_dim_hist_id.pol_sym AS pol_sym_hist_id,
	mplt_Claim_occurrence_dim_hist_id.pol_num AS pol_num_hist_id,
	mplt_Claim_Rep_Dim_Id_Payment_Entry_Operator.Claim_Rep_Dim_Id AS payment_entry_operator_id,
	-- *INF*: IIF(isnull(payment_entry_operator_id),-1,payment_entry_operator_id)
	IFF(payment_entry_operator_id IS NULL, - 1, payment_entry_operator_id) AS payment_entry_operator_id_out,
	mplt_Claim_Rep_Dim_Hist_Id_Payment_Entry_Operator.Claim_Rep_Dim_Id AS payment_entry_operator_hist_id,
	-- *INF*: IIF(isnull(payment_entry_operator_hist_id),-1,payment_entry_operator_hist_id)
	IFF(payment_entry_operator_hist_id IS NULL, - 1, payment_entry_operator_hist_id) AS payment_entry_operator_hist_id_out,
	LKP_InsuranceReferenceDimId.InsuranceReferenceDimId AS I_InsuranceReferenceDimId,
	-- *INF*: IIF(ISNULL(I_InsuranceReferenceDimId), -1 , I_InsuranceReferenceDimId )
	IFF(I_InsuranceReferenceDimId IS NULL, - 1, I_InsuranceReferenceDimId) AS O_InsuranceReferenceDimId,
	mplt_Claim_occurrence_dim_id.AgencyDimID AS I_AgencyDimID,
	-- *INF*: IIF( ISNULL(I_AgencyDimID), -1, I_AgencyDimID)
	IFF(I_AgencyDimID IS NULL, - 1, I_AgencyDimID) AS O_AgencyDimID,
	LKP_SalesDivisionDim.SalesDivisionDimID AS I_SalesDivisionDimID,
	-- *INF*: IIF(ISNULL(I_SalesDivisionDimID), -1, I_SalesDivisionDimID)
	IFF(I_SalesDivisionDimID IS NULL, - 1, I_SalesDivisionDimID) AS O_SalesDivisionDimID,
	LKP_InsuranceReferenceCoverageDim.InsuranceReferenceCoverageDimId AS I_InsuranceReferenceCoverageDimId,
	-- *INF*: IIF(ISNULL(I_InsuranceReferenceCoverageDimId), -1, I_InsuranceReferenceCoverageDimId)
	IFF(I_InsuranceReferenceCoverageDimId IS NULL, - 1, I_InsuranceReferenceCoverageDimId) AS O_InsuranceReferenceCoverageDetailDimID,
	LKP_CoverageDetailDim.CoverageDetailDimId AS I_CoverageDetailDimId,
	-- *INF*: IIF( ISNULL( I_CoverageDetailDimId), -1, I_CoverageDetailDimId)
	IFF(I_CoverageDetailDimId IS NULL, - 1, I_CoverageDetailDimId) AS O_CoverageDetailDimId,
	mplt_Claim_occurrence_dim_id.AgencyAKID,
	mplt_Claim_occurrence_dim_id.SalesTerritoryAKID,
	mplt_Claim_occurrence_dim_id.RegionalSalesManagerAKID,
	mplt_Claim_occurrence_dim_id.SalesDirectorAKID,
	mplt_Claim_occurrence_dim_id.StrategicProfitCenterAKId,
	mplt_Claim_occurrence_dim_id.InsuranceSegmentAKId,
	mplt_Claim_occurrence_dim_id.PolicyOfferingAKId,
	mplt_Claim_occurrence_dim_hist_id.AgencyDimID,
	-- *INF*: IIF(ISNULL(AgencyDimID), -1, AgencyDimID)
	IFF(AgencyDimID IS NULL, - 1, AgencyDimID) AS o_agency_dim_hist_id,
	mplt_Claim_occurrence_dim_hist_id.AgencyAKID AS AgencyAKID1,
	mplt_Claim_occurrence_dim_hist_id.SalesTerritoryAKID AS SalesTerritoryAKID1,
	mplt_Claim_occurrence_dim_hist_id.RegionalSalesManagerAKID AS RegionalSalesManagerAKID1,
	mplt_Claim_occurrence_dim_hist_id.SalesDirectorAKID AS SalesDirectorAKID1,
	mplt_Claim_occurrence_dim_hist_id.StrategicProfitCenterAKId AS StrategicProfitCenterAKId1,
	mplt_Claim_occurrence_dim_hist_id.InsuranceSegmentAKId AS InsuranceSegmentAKId1,
	mplt_Claim_occurrence_dim_hist_id.PolicyOfferingAKId AS PolicyOfferingAKId1,
	EXP_set_claimant_dim_ids.ClaimReserveDimId AS in_ClaimReserveDimId,
	-- *INF*: IIF(ISNULL(in_ClaimReserveDimId),-1,in_ClaimReserveDimId)
	IFF(in_ClaimReserveDimId IS NULL, - 1, in_ClaimReserveDimId) AS out_ClaimReserveDimId,
	EXP_set_claimant_dim_ids.FeatureRepresentativeDimId AS in_FeatureRepresentativeDimId,
	-- *INF*: IIF(ISNULL(in_FeatureRepresentativeDimId),-1,in_FeatureRepresentativeDimId)
	IFF(in_FeatureRepresentativeDimId IS NULL, - 1, in_FeatureRepresentativeDimId) AS out_FeatureRepresentativeDimId,
	EXP_set_claimant_dim_ids.FeatureRepresentativeAssignedDate
	FROM EXP_set_claimant_dim_ids
	 -- Manually join with mplt_Claim_Rep_Dim_Hist_Id_Payment_Entry_Operator
	 -- Manually join with mplt_Claim_Rep_Dim_Id_Payment_Entry_Operator
	 -- Manually join with mplt_Claim_occurrence_dim_hist_id
	 -- Manually join with mplt_Claim_occurrence_dim_id
	 -- Manually join with mplt_Strategic_Business_Division_Dim
	LEFT JOIN LKP_CoverageDetailDim
	ON LKP_CoverageDetailDim.claim_trans_id = EXP_set_claimant_dim_ids.claim_trans_id AND LKP_CoverageDetailDim.PolicyAkid = mplt_Claim_occurrence_dim_id.PolicyAkid
	LEFT JOIN LKP_InsuranceReferenceCoverageDim
	ON LKP_InsuranceReferenceCoverageDim.claimant_cov_det_ak_id = EXP_set_claimant_dim_ids.IN_claimant_cov_det_ak_id
	LEFT JOIN LKP_InsuranceReferenceDimId
	ON LKP_InsuranceReferenceDimId.claimant_cov_det_ak_id = EXP_set_claimant_dim_ids.IN_claimant_cov_det_ak_id
	LEFT JOIN LKP_SalesDivisionDim
	ON LKP_SalesDivisionDim.AgencyAKID = mplt_Claim_occurrence_dim_id.AgencyAKID
	LEFT JOIN LKP_CLAIM_REP_DIM_ALT LKP_CLAIM_REP_DIM_ALT_trans_date_IN_claimant_cov_det_ak_id
	ON LKP_CLAIM_REP_DIM_ALT_trans_date_IN_claimant_cov_det_ak_id.claimant_cov_det_ak_id = trans_date
	AND LKP_CLAIM_REP_DIM_ALT_trans_date_IN_claimant_cov_det_ak_id. = IN_claimant_cov_det_ak_id

),
EXP_set_default_date_ids AS (
	SELECT
	claim_occurrence_dim_id_out,
	claim_occurrence_dim_hist_id_out,
	claimant_dim_id_out,
	claimant_dim_hist_id_out,
	claimant_cov_dim_id_out,
	claimant_cov_dim_hist_id_out,
	cov_dim_id_out,
	cov_dim_hist_id_out,
	claim_trans_type_dim_id_out,
	claim_financial_type_dim_id_out,
	claim_rep_prim_claim_rep_dim_id_out,
	claim_rep_prim_claim_rep_dim_hist_id_out,
	claim_rep_dim_examiner_id_out,
	claim_rep_dim_examiner_hist_id_out,
	claim_rep_dim_prim_litigation_handler_id_out,
	claim_rep_dim_prim_litigation_handler_hist_id_out,
	pol_key_dim_id_out,
	pol_key_dim_hist_id_out,
	agency_dim_id_out,
	o_agency_dim_hist_id AS agency_dim_hist_id_out,
	claim_payment_dim_id_out,
	claim_payment_dim_hist_id_out,
	tax_id,
	claim_master_1099_list_dim_id AS claim_master_1099_list_dim_id_out,
	claim_pay_ctgry_type_dim_id_out,
	claim_pay_ctgry_type_dim_hist_id_out,
	claim_created_by_dim_id,
	claim_case_dim_id_out,
	claim_case_dim_hist_id_out,
	claim_subrogation_dim_id_out,
	claim_trans_oper_dim_id,
	trans_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(trans_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_trans_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_trans_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_trans_date_id), v_claim_trans_date_id, -1)
	IFF(NOT v_claim_trans_date_id IS NULL, v_claim_trans_date_id, - 1) AS claim_trans_date_id,
	reprocess_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(reprocess_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_reprocess_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_reprocess_date_id,
	-- *INF*: IIF(NOT ISNULL(v_reprocess_date_id), v_reprocess_date_id, -1)
	IFF(NOT v_reprocess_date_id IS NULL, v_reprocess_date_id, - 1) AS reprocess_date_id,
	claim_loss_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_loss_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_loss_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_loss_date_id), v_claim_loss_date_id, -1)
	IFF(NOT v_claim_loss_date_id IS NULL, v_claim_loss_date_id, - 1) AS claim_loss_date_id,
	claim_discovery_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_discovery_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_discovery_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_discovery_date_id), v_claim_discovery_date_id, -1)
	IFF(NOT v_claim_discovery_date_id IS NULL, v_claim_discovery_date_id, - 1) AS claim_discovery_date_id,
	source_claim_rpted_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(source_claim_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_source_claim_rpted_date_id,
	-- *INF*: IIF(ISNULL(v_source_claim_rpted_date_id),-1,v_source_claim_rpted_date_id)
	IFF(v_source_claim_rpted_date_id IS NULL, - 1, v_source_claim_rpted_date_id) AS source_claim_rpted_date_id,
	claim_scripted_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_scripted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_scripted_date_id,
	-- *INF*: IIF(ISNULL(v_claim_scripted_date_id),-1,v_claim_scripted_date_id)
	IFF(v_claim_scripted_date_id IS NULL, - 1, v_claim_scripted_date_id) AS claim_scripted_date_id,
	claim_occurrence_rpted_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_occurrence_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_occurrence_rpted_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_occurrence_rpted_date_id), v_claim_occurrence_rpted_date_id, -1)
	IFF(NOT v_claim_occurrence_rpted_date_id IS NULL, v_claim_occurrence_rpted_date_id, - 1) AS claim_occurrence_rpted_date_id,
	claim_open_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_open_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_open_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_open_date_id,
	-- *INF*: IIF(ISNULL(v_claim_open_date_id), -1, v_claim_open_date_id)
	IFF(v_claim_open_date_id IS NULL, - 1, v_claim_open_date_id) AS claim_open_date_id,
	claim_close_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_close_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_close_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_close_date_id,
	-- *INF*: IIF(ISNULL(v_claim_close_date_id), -1, v_claim_close_date_id)
	IFF(v_claim_close_date_id IS NULL, - 1, v_claim_close_date_id) AS claim_close_date_id,
	claim_reopen_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_reopen_date_id,
	-- *INF*: IIF(ISNULL(v_claim_reopen_date_id), -1, v_claim_reopen_date_id)
	IFF(v_claim_reopen_date_id IS NULL, - 1, v_claim_reopen_date_id) AS claim_reopen_date_id,
	claim_closed_after_reopen_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_closed_after_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_closed_after_reopen_date_id,
	-- *INF*: IIF(ISNULL(v_claim_closed_after_reopen_date_id), -1, v_claim_closed_after_reopen_date_id)
	IFF(v_claim_closed_after_reopen_date_id IS NULL, - 1, v_claim_closed_after_reopen_date_id) AS claim_closed_after_reopen_date_id,
	claim_notice_only_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_notice_only_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_notice_only_date_id,
	-- *INF*: IIF(ISNULL(v_claim_notice_only_date_id), -1, v_claim_notice_only_date_id)
	IFF(v_claim_notice_only_date_id IS NULL, - 1, v_claim_notice_only_date_id) AS claim_notice_only_date_id,
	claim_cat_start_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_cat_start_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_cat_start_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_cat_start_date_id), v_claim_cat_start_date_id, -1)
	IFF(NOT v_claim_cat_start_date_id IS NULL, v_claim_cat_start_date_id, - 1) AS claim_cat_start_date_id,
	claim_cat_end_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_cat_end_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_cat_end_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_cat_end_date_id), v_claim_cat_end_date_id, -1)
	IFF(NOT v_claim_cat_end_date_id IS NULL, v_claim_cat_end_date_id, - 1) AS claim_cat_end_date_id,
	claim_rep_assigned_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_rep_assigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_rep_assigned_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_rep_assigned_date_id), v_claim_rep_assigned_date_id, -1)
	IFF(NOT v_claim_rep_assigned_date_id IS NULL, v_claim_rep_assigned_date_id, - 1) AS claim_rep_assigned_date_id,
	claim_rep_unassigned_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(claim_rep_unassigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_rep_unassigned_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_rep_unassigned_date_id), v_claim_rep_unassigned_date_id, -1)
	IFF(NOT v_claim_rep_unassigned_date_id IS NULL, v_claim_rep_unassigned_date_id, - 1) AS claim_rep_unassigned_date_id,
	claim_trans_id,
	pol_eff_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(pol_eff_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pol_eff_date_id,
	-- *INF*: IIF(NOT ISNULL(v_pol_eff_date_id), v_pol_eff_date_id, -1)
	IFF(NOT v_pol_eff_date_id IS NULL, v_pol_eff_date_id, - 1) AS pol_eff_date_id,
	pol_exp_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(pol_exp_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pol_exp_date_id,
	-- *INF*: IIF(NOT ISNULL(v_pol_exp_date_id), v_pol_exp_date_id, -1 )
	IFF(NOT v_pol_exp_date_id IS NULL, v_pol_exp_date_id, - 1) AS pol_exp_date_id,
	IN_claimant_cov_det_ak_id,
	IN_cause_of_loss,
	IN_reserve_ctgry,
	IN_type_disability,
	financial_type_code,
	trans_code,
	trans_ctgry_code,
	trans_amt,
	trans_hist_amt,
	source_sys_id,
	audit_id,
	err_flag,
	default_dim_id,
	referred_to_subrogation_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(referred_to_subrogation_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_referred_to_subrogation_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_referred_to_subrogation_date_id,
	-- *INF*: IIF(ISNULL(v_referred_to_subrogation_date_id), -1, v_referred_to_subrogation_date_id)
	IFF(v_referred_to_subrogation_date_id IS NULL, - 1, v_referred_to_subrogation_date_id) AS referred_to_subrogation_date_id,
	pay_start_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(pay_start_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_pay_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pay_start_date_id,
	-- *INF*: IIF(ISNULL(v_pay_start_date_id), -1, v_pay_start_date_id)
	IFF(v_pay_start_date_id IS NULL, - 1, v_pay_start_date_id) AS pay_start_date_id,
	closure_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(closure_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_closure_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_closure_date_id,
	-- *INF*: IIF(ISNULL(v_closure_date_id), -1, v_closure_date_id)
	IFF(v_closure_date_id IS NULL, - 1, v_closure_date_id) AS closure_date_id,
	contract_cust_dim_id_out,
	contract_cust_dim_hist_id_out,
	pms_acct_entered_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(pms_acct_entered_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_pms_acct_entered_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS V_acct_entered_date,
	V_acct_entered_date AS acct_entered_date_id,
	strtgc_bus_dvsn_dim_id,
	payment_entry_operator_id_out,
	payment_entry_operator_hist_id_out,
	O_InsuranceReferenceDimId AS InsuranceReferenceDimId,
	O_AgencyDimID AS AgencyDimID,
	O_SalesDivisionDimID AS SalesDivisionDimID,
	O_InsuranceReferenceCoverageDetailDimID AS InsuranceReferenceCoverageDetailDimID,
	O_CoverageDetailDimId AS CoverageDetailDimId,
	out_ClaimReserveDimId AS ClaimReserveDimId,
	out_FeatureRepresentativeDimId AS FeatureRepresentativeDimId,
	FeatureRepresentativeAssignedDate AS in_FeatureRepresentativeAssignedDate,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(in_FeatureRepresentativeAssignedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_in_FeatureRepresentativeAssignedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS var_FeatureRepresentativeAssignedDate_id,
	-- *INF*: IIF(ISNULL(var_FeatureRepresentativeAssignedDate_id),
	-- -1,
	-- var_FeatureRepresentativeAssignedDate_id)
	IFF(var_FeatureRepresentativeAssignedDate_id IS NULL, - 1, var_FeatureRepresentativeAssignedDate_id) AS out_FeatureRepresentativeAssignedDate_id
	FROM EXP_set_default_dim_ids
	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_trans_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_trans_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(trans_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_reprocess_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_reprocess_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(reprocess_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_loss_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_discovery_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_discovery_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(source_claim_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_scripted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_scripted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_occurrence_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_open_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_open_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_open_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_close_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_close_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_close_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_closed_after_reopen_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_closed_after_reopen_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_notice_only_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_notice_only_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_cat_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_cat_start_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_cat_end_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_cat_end_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_rep_assigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_rep_assigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_claim_rep_unassigned_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(claim_rep_unassigned_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(pol_eff_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(pol_exp_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_referred_to_subrogation_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_referred_to_subrogation_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(referred_to_subrogation_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_pay_start_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_pay_start_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(pay_start_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_closure_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_closure_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(closure_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_pms_acct_entered_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_pms_acct_entered_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(pms_acct_entered_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_in_FeatureRepresentativeAssignedDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_in_FeatureRepresentativeAssignedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(in_FeatureRepresentativeAssignedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

),
EXP_set_financial_values AS (
	SELECT
	claim_occurrence_dim_id_out,
	claim_occurrence_dim_hist_id_out,
	claimant_dim_id_out,
	claimant_dim_hist_id_out,
	claimant_cov_dim_id_out,
	claimant_cov_dim_hist_id_out,
	cov_dim_id_out,
	cov_dim_hist_id_out,
	claim_trans_type_dim_id_out,
	claim_financial_type_dim_id_out,
	claim_rep_prim_claim_rep_dim_id_out,
	claim_rep_prim_claim_rep_dim_hist_id_out,
	claim_rep_dim_examiner_id_out,
	claim_rep_dim_examiner_hist_id_out,
	claim_rep_dim_prim_litigation_handler_id_out,
	claim_rep_dim_prim_litigation_handler_hist_id_out,
	pol_key_dim_id_out,
	pol_key_dim_hist_id_out,
	agency_dim_id_out,
	agency_dim_hist_id_out,
	claim_payment_dim_id_out,
	claim_payment_dim_hist_id_out,
	tax_id,
	claim_master_1099_list_dim_id_out,
	claim_pay_ctgry_type_dim_id_out,
	claim_pay_ctgry_type_dim_hist_id_out,
	claim_created_by_dim_id,
	claim_case_dim_id_out,
	claim_case_dim_hist_id_out,
	claim_subrogation_dim_id_out,
	claim_trans_oper_dim_id,
	trans_date,
	claim_trans_date_id,
	reprocess_date_id,
	claim_loss_date_id,
	claim_discovery_date_id,
	source_claim_rpted_date_id,
	claim_scripted_date_id,
	claim_occurrence_rpted_date_id,
	claim_open_date_id,
	claim_close_date_id,
	claim_reopen_date_id,
	claim_closed_after_reopen_date_id,
	claim_notice_only_date_id,
	claim_cat_start_date_id,
	claim_cat_end_date_id,
	claim_rep_assigned_date_id,
	claim_rep_unassigned_date_id,
	claim_trans_id,
	pol_eff_date_id,
	pol_exp_date_id,
	IN_claimant_cov_det_ak_id,
	IN_cause_of_loss,
	IN_reserve_ctgry,
	IN_type_disability,
	financial_type_code,
	trans_code,
	trans_ctgry_code,
	trans_amt,
	trans_hist_amt,
	source_sys_id,
	audit_id,
	err_flag,
	default_dim_id,
	referred_to_subrogation_date_id,
	pay_start_date_id,
	closure_date_id,
	contract_cust_dim_id_out,
	contract_cust_dim_hist_id_out,
	-- *INF*: IIF(financial_type_code = 'D', 
	-- DECODE(trans_code,  '20', trans_amt, 
	-- '21',trans_amt, 
	-- '22', trans_amt, 
	-- '23',trans_amt, 
	-- '24', trans_amt, 
	-- '28', trans_amt, 
	-- '29', trans_amt, 
	-- '41', 0, 
	-- '42', 0, 
	-- '43', 0, 
	-- '65',0, 
	-- '66', 0, 
	-- '90', 0, 
	-- '91', 0, 
	-- '92', 0, 0),0)
	IFF(financial_type_code = 'D', DECODE(trans_code,
	'20', trans_amt,
	'21', trans_amt,
	'22', trans_amt,
	'23', trans_amt,
	'24', trans_amt,
	'28', trans_amt,
	'29', trans_amt,
	'41', 0,
	'42', 0,
	'43', 0,
	'65', 0,
	'66', 0,
	'90', 0,
	'91', 0,
	'92', 0,
	0), 0) AS var_direct_loss_paid_excluding_recoveries,
	-- *INF*: IIF(financial_type_code = 'D', 
	-- DECODE(trans_code, '20', 0,
	-- '21', trans_amt * -1, 
	-- '22', (trans_amt  -  trans_hist_amt ) * -1, 
	-- '23', 0, 
	-- '24', 0, 
	-- '28', trans_amt * -1, 
	-- '29', 0, 
	-- '41', trans_hist_amt, 
	-- '42', trans_hist_amt, 
	-- '43', 0, 
	-- '65', trans_hist_amt, 
	-- '66', trans_hist_amt, 
	-- '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'D', '23', trans_date)), 0, trans_hist_amt), 
	-- '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'D', '23', trans_date)), 0, trans_hist_amt), 
	-- '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'D', '23', trans_date)), 0, trans_hist_amt), 0))
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(financial_type_code = 'D', DECODE(trans_code,
	'20', 0,
	'21', trans_amt * - 1,
	'22', ( trans_amt - trans_hist_amt ) * - 1,
	'23', 0,
	'24', 0,
	'28', trans_amt * - 1,
	'29', 0,
	'41', trans_hist_amt,
	'42', trans_hist_amt,
	'43', 0,
	'65', trans_hist_amt,
	'66', trans_hist_amt,
	'90', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_D_23_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt),
	'91', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_D_23_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt),
	'92', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_D_23_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt),
	0)) AS var_direct_loss_outstanding_excluding_recoveries,
	-- *INF*: IIF(financial_type_code = 'D', 
	-- DECODE(trans_code, '20', trans_amt,
	-- '21', 0, 
	-- '22', trans_hist_amt, 
	-- '23', trans_amt, 
	-- '24', trans_amt, 
	-- '28',0, 
	-- '29', trans_amt, 
	-- '41', trans_hist_amt, 
	-- '42', trans_hist_amt, 
	-- '43', 0, 
	-- '65', trans_hist_amt, 
	-- '66', trans_hist_amt, 
	-- '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'D', '23', trans_date)), 0, trans_hist_amt), 
	-- '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'D', '23', trans_date)), 0, trans_hist_amt), 
	-- '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'D', '23', trans_date)), 0, trans_hist_amt), 0))
	IFF(financial_type_code = 'D', DECODE(trans_code,
	'20', trans_amt,
	'21', 0,
	'22', trans_hist_amt,
	'23', trans_amt,
	'24', trans_amt,
	'28', 0,
	'29', trans_amt,
	'41', trans_hist_amt,
	'42', trans_hist_amt,
	'43', 0,
	'65', trans_hist_amt,
	'66', trans_hist_amt,
	'90', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_D_23_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt),
	'91', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_D_23_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt),
	'92', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_D_23_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt),
	0)) AS var_direct_loss_incurred_excluding_recoveries,
	-- *INF*: IIF(financial_type_code = 'E', 
	-- DECODE(trans_code,  '20', trans_amt, 
	-- '21',trans_amt, 
	-- '22', trans_amt, 
	-- '23',trans_amt, 
	-- '24', trans_amt, 
	-- '28', trans_amt, 
	-- '29', trans_amt,
	-- '40',0, 
	-- '41', 0, 
	-- '42', 0, 
	-- '43', 0, 
	-- '65',0, 
	-- '66', 0, 
	-- '90', 0, 
	-- '91', 0, 
	-- '92', 0, 0),0)
	IFF(financial_type_code = 'E', DECODE(trans_code,
	'20', trans_amt,
	'21', trans_amt,
	'22', trans_amt,
	'23', trans_amt,
	'24', trans_amt,
	'28', trans_amt,
	'29', trans_amt,
	'40', 0,
	'41', 0,
	'42', 0,
	'43', 0,
	'65', 0,
	'66', 0,
	'90', 0,
	'91', 0,
	'92', 0,
	0), 0) AS var_direct_alae_paid_excluding_recoveries,
	-- *INF*: IIF(financial_type_code = 'E'  and IN (source_sys_id , 'EXCEED', 'DCT'),
	-- DECODE(trans_code, '20', 0,
	-- '21', trans_amt * -1, 
	-- '22', (trans_amt -  trans_hist_amt ) * -1, 
	-- '23', 0, 
	-- '24', 0, 
	-- '28', trans_amt * -1, 
	-- '29', 0,
	-- '40',trans_hist_amt, 
	-- '41', trans_hist_amt, 
	-- '42', trans_hist_amt, 
	-- '43', 0, 
	-- '65', trans_hist_amt, 
	-- '66', trans_hist_amt, 
	-- '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'E', '23', trans_date)), 0, trans_hist_amt), 
	-- '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'E', '23', trans_date)), 0, trans_hist_amt), 
	-- '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'E', '23', trans_date)), 0, trans_hist_amt), 0),
	-- 0)
	-- 
	IFF(financial_type_code = 'E' AND IN(source_sys_id, 'EXCEED', 'DCT'), DECODE(trans_code,
	'20', 0,
	'21', trans_amt * - 1,
	'22', ( trans_amt - trans_hist_amt ) * - 1,
	'23', 0,
	'24', 0,
	'28', trans_amt * - 1,
	'29', 0,
	'40', trans_hist_amt,
	'41', trans_hist_amt,
	'42', trans_hist_amt,
	'43', 0,
	'65', trans_hist_amt,
	'66', trans_hist_amt,
	'90', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_E_23_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt),
	'91', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_E_23_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt),
	'92', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_E_23_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt),
	0), 0) AS var_direct_alae_outstanding_excluding_recoveries,
	-- *INF*: var_direct_alae_paid_excluding_recoveries + var_direct_alae_outstanding_excluding_recoveries
	-- --JIRA-PROD-4418 Use variables to calculate var_direct_alae_incurred_excluding_recoveries instead of calculating it again based on financial_type_code, source_sys_id and trans_code.
	var_direct_alae_paid_excluding_recoveries + var_direct_alae_outstanding_excluding_recoveries AS var_direct_alae_incurred_excluding_recoveries,
	-- *INF*: IIF(financial_type_code = 'B', 
	-- DECODE(trans_code,  '25',IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- '30',IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- '31', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '32', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- '33', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- '34', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- '38', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- '39', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- '41', 0, 
	-- '42', 0,  
	-- '65',0, 
	-- '66', 0, 
	-- '90', 0, 
	-- '91', 0, 
	-- '92', 0, 0),0)
	IFF(financial_type_code = 'B', DECODE(trans_code,
	'25', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'30', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'31', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'32', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'33', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'34', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'38', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'39', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'41', 0,
	'42', 0,
	'65', 0,
	'66', 0,
	'90', 0,
	'91', 0,
	'92', 0,
	0), 0) AS var_direct_subrogation_paid,
	-- *INF*: IIF(financial_type_code = 'B' and IN (source_sys_id , 'EXCEED', 'DCT'),
	-- DECODE(trans_code, '25', 0,
	-- '30',0,
	-- '31', trans_amt , 
	-- '32', (trans_amt -  trans_hist_amt ), 
	-- '33', 0, 
	-- '34', 0, 
	-- '38', trans_amt, 
	-- '39', 0, 
	-- '41', trans_hist_amt * -1, 
	-- '42', trans_hist_amt * -1, 
	-- '65', trans_hist_amt * -1, 
	-- '66', trans_hist_amt * -1, 
	-- '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'B', '33', trans_date)), 0, trans_hist_amt * -1), 
	-- '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'B', '33', trans_date)), 0, trans_hist_amt * -1), 
	-- '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'B', '33', trans_date)), 0, trans_hist_amt * -1), 0),
	-- 0)
	-- 
	IFF(financial_type_code = 'B' AND IN(source_sys_id, 'EXCEED', 'DCT'), DECODE(trans_code,
	'25', 0,
	'30', 0,
	'31', trans_amt,
	'32', ( trans_amt - trans_hist_amt ),
	'33', 0,
	'34', 0,
	'38', trans_amt,
	'39', 0,
	'41', trans_hist_amt * - 1,
	'42', trans_hist_amt * - 1,
	'65', trans_hist_amt * - 1,
	'66', trans_hist_amt * - 1,
	'90', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_B_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'91', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_B_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'92', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_B_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	0), 0) AS var_direct_subrogation_outstanding,
	-- *INF*: IIF(financial_type_code = 'B' and IN (source_sys_id , 'EXCEED', 'DCT'),
	-- DECODE(trans_code, '25', trans_amt * -1,
	-- '30',trans_amt * -1,
	-- '31', 0 , 
	-- '32', trans_hist_amt * -1, 
	-- '33', trans_amt * -1,
	-- '34', trans_amt * -1,
	-- '38', 0,
	-- '39', trans_amt * -1,
	-- '41', trans_hist_amt * -1, 
	-- '42', trans_hist_amt * -1, 
	-- '65', trans_hist_amt * -1, 
	-- '66', trans_hist_amt * -1, 
	-- '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'B', '33', trans_date)), 0, trans_hist_amt  * -1), 
	-- '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'B', '33', trans_date)), 0, trans_hist_amt * -1), 
	-- '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'B', '33', trans_date)), 0, trans_hist_amt * -1), 0),
	-- 0)
	IFF(financial_type_code = 'B' AND IN(source_sys_id, 'EXCEED', 'DCT'), DECODE(trans_code,
	'25', trans_amt * - 1,
	'30', trans_amt * - 1,
	'31', 0,
	'32', trans_hist_amt * - 1,
	'33', trans_amt * - 1,
	'34', trans_amt * - 1,
	'38', 0,
	'39', trans_amt * - 1,
	'41', trans_hist_amt * - 1,
	'42', trans_hist_amt * - 1,
	'65', trans_hist_amt * - 1,
	'66', trans_hist_amt * - 1,
	'90', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_B_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'91', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_B_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'92', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_B_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	0), 0) AS var_direct_subrogation_incurred,
	-- *INF*: IIF(financial_type_code = 'S', 
	-- DECODE(trans_code,  '25', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '30',IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '31', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '32', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '33', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '34', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '38', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '39', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),  
	-- '41', 0, 
	-- '42', 0,  
	-- '65',0, 
	-- '66', 0, 
	-- '90', 0, 
	-- '91', 0, 
	-- '92', 0, 0),0)
	IFF(financial_type_code = 'S', DECODE(trans_code,
	'25', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'30', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'31', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'32', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'33', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'34', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'38', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'39', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'41', 0,
	'42', 0,
	'65', 0,
	'66', 0,
	'90', 0,
	'91', 0,
	'92', 0,
	0), 0) AS var_direct_salvage_paid,
	-- *INF*: IIF(financial_type_code = 'S' and IN (source_sys_id , 'EXCEED', 'DCT'), 
	-- DECODE(trans_code, '25', 0,
	-- '30',0,
	-- '31', trans_amt , 
	-- '32', (trans_amt -  trans_hist_amt ), 
	-- '33', 0, 
	-- '34', 0, 
	-- '38', trans_amt, 
	-- '39', 0, 
	-- '41', trans_hist_amt * -1, 
	-- '42', trans_hist_amt * -1, 
	-- '65', trans_hist_amt * -1, 
	-- '66', trans_hist_amt * -1, 
	-- '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'S', '33', trans_date)), 0, trans_hist_amt * -1), 
	-- '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'S', '33', trans_date)), 0, trans_hist_amt * -1), 
	-- '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'S', '33', trans_date)), 0, trans_hist_amt * -1), 0),
	-- 0)
	IFF(financial_type_code = 'S' AND IN(source_sys_id, 'EXCEED', 'DCT'), DECODE(trans_code,
	'25', 0,
	'30', 0,
	'31', trans_amt,
	'32', ( trans_amt - trans_hist_amt ),
	'33', 0,
	'34', 0,
	'38', trans_amt,
	'39', 0,
	'41', trans_hist_amt * - 1,
	'42', trans_hist_amt * - 1,
	'65', trans_hist_amt * - 1,
	'66', trans_hist_amt * - 1,
	'90', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_S_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'91', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_S_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'92', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_S_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	0), 0) AS var_direct_salvage_outstanding,
	-- *INF*: IIF(financial_type_code = 'S'and IN (source_sys_id , 'EXCEED', 'DCT'),
	-- DECODE(trans_code, '25', trans_amt * -1,
	--  '25', trans_amt * -1,
	-- '31', 0, 
	-- '32', trans_hist_amt * -1, 
	-- '33', trans_amt * -1,
	-- '34',trans_amt * -1,
	-- '38',0,
	-- '39', trans_amt * -1,
	-- '41', trans_hist_amt * -1, 
	-- '42', trans_hist_amt * -1, 
	-- '65', trans_hist_amt * -1, 
	-- '66', trans_hist_amt * -1, 
	-- '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'S', '33', trans_date)), 0, trans_hist_amt * -1), 
	-- '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'S', '33', trans_date)), 0, trans_hist_amt * -1), 
	-- '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'S', '33', trans_date)), 0, trans_hist_amt * -1), 0),
	-- 0)
	IFF(financial_type_code = 'S' AND IN(source_sys_id, 'EXCEED', 'DCT'), DECODE(trans_code,
	'25', trans_amt * - 1,
	'25', trans_amt * - 1,
	'31', 0,
	'32', trans_hist_amt * - 1,
	'33', trans_amt * - 1,
	'34', trans_amt * - 1,
	'38', 0,
	'39', trans_amt * - 1,
	'41', trans_hist_amt * - 1,
	'42', trans_hist_amt * - 1,
	'65', trans_hist_amt * - 1,
	'66', trans_hist_amt * - 1,
	'90', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_S_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'91', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_S_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'92', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_S_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	0), 0) AS var_direct_salvage_incurred,
	-- *INF*: IIF(financial_type_code = 'R', 
	-- DECODE(trans_code,  '25', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '30', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '31', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '32', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '33', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '34', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '38', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '39', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),  
	-- '41', 0, 
	-- '42', 0,  
	-- '65',0, 
	-- '66', 0, 
	-- '90', 0, 
	-- '91', 0, 
	-- '92', 0, 0),0)
	IFF(financial_type_code = 'R', DECODE(trans_code,
	'25', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'30', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'31', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'32', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'33', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'34', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'38', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'39', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'41', 0,
	'42', 0,
	'65', 0,
	'66', 0,
	'90', 0,
	'91', 0,
	'92', 0,
	0), 0) AS var_direct_other_recovery_paid,
	-- *INF*: IIF(financial_type_code = 'R' and IN (source_sys_id , 'EXCEED', 'DCT'), 
	-- DECODE(trans_code, '25', 0,
	-- '30',0,
	-- '31', trans_amt , 
	-- '32', (trans_amt -  trans_hist_amt ), 
	-- '33', 0, 
	-- '34', 0, 
	-- '38', trans_amt, 
	-- '39', 0, 
	-- '41', trans_hist_amt * -1, 
	-- '42', trans_hist_amt * -1, 
	-- '65', trans_hist_amt * -1, 
	-- '66', trans_hist_amt * -1, 
	-- '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'R', '33', trans_date)), 0, trans_hist_amt * -1), 
	-- '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'R', '33', trans_date)), 0, trans_hist_amt * -1), 
	-- '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'R', '33', trans_date)), 0, trans_hist_amt * -1), 0),
	-- 0)
	IFF(financial_type_code = 'R' AND IN(source_sys_id, 'EXCEED', 'DCT'), DECODE(trans_code,
	'25', 0,
	'30', 0,
	'31', trans_amt,
	'32', ( trans_amt - trans_hist_amt ),
	'33', 0,
	'34', 0,
	'38', trans_amt,
	'39', 0,
	'41', trans_hist_amt * - 1,
	'42', trans_hist_amt * - 1,
	'65', trans_hist_amt * - 1,
	'66', trans_hist_amt * - 1,
	'90', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'91', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'92', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	0), 0) AS var_direct_other_recovery_outstanding,
	-- *INF*: IIF(financial_type_code = 'R' and IN (source_sys_id , 'EXCEED', 'DCT'), 
	-- DECODE(trans_code, '25', trans_amt * -1,
	-- '30',trans_amt * -1,
	-- '31', 0, 
	-- '32', trans_hist_amt * -1, 
	-- '33', trans_amt * -1,
	-- '34',trans_amt * -1,
	-- '38',0,
	-- '39', trans_amt * -1,
	-- '41', trans_hist_amt * -1, 
	-- '42', trans_hist_amt * -1, 
	-- '65', trans_hist_amt * -1, 
	-- '66', trans_hist_amt * -1, 
	-- '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'R', '33', trans_date)), 0, trans_hist_amt * -1), 
	-- '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'R', '33', trans_date)), 0, trans_hist_amt * -1), 
	-- '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'R', '33', trans_date)), 0, trans_hist_amt * -1), 0),
	-- 0)
	IFF(financial_type_code = 'R' AND IN(source_sys_id, 'EXCEED', 'DCT'), DECODE(trans_code,
	'25', trans_amt * - 1,
	'30', trans_amt * - 1,
	'31', 0,
	'32', trans_hist_amt * - 1,
	'33', trans_amt * - 1,
	'34', trans_amt * - 1,
	'38', 0,
	'39', trans_amt * - 1,
	'41', trans_hist_amt * - 1,
	'42', trans_hist_amt * - 1,
	'65', trans_hist_amt * - 1,
	'66', trans_hist_amt * - 1,
	'90', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'91', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'92', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	0), 0) AS var_direct_other_recovery_incurred,
	-- *INF*: IIF(financial_type_code = 'R' and IN (source_sys_id , 'EXCEED', 'DCT') and trans_ctgry_code<>'EX', 
	-- DECODE(trans_code,
	-- '31', trans_amt , 
	-- '32', (trans_amt -  trans_hist_amt ), 
	-- '38', trans_amt, 
	-- '41', trans_hist_amt * -1, 
	-- '42', trans_hist_amt * -1, 
	-- '65', trans_hist_amt * -1, 
	-- '66', trans_hist_amt * -1, 
	-- '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'R', '33', trans_date)), 0, trans_hist_amt * -1), 
	-- '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'R', '33', trans_date)), 0, trans_hist_amt * -1), 
	-- '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'R', '33', trans_date)), 0, trans_hist_amt * -1), 0),
	-- 0)
	IFF(financial_type_code = 'R' AND IN(source_sys_id, 'EXCEED', 'DCT') AND trans_ctgry_code <> 'EX', DECODE(trans_code,
	'31', trans_amt,
	'32', ( trans_amt - trans_hist_amt ),
	'38', trans_amt,
	'41', trans_hist_amt * - 1,
	'42', trans_hist_amt * - 1,
	'65', trans_hist_amt * - 1,
	'66', trans_hist_amt * - 1,
	'90', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'91', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'92', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	0), 0) AS var_direct_other_recovery_loss_outstanding,
	-- *INF*: IIF(financial_type_code = 'R' and IN (source_sys_id , 'EXCEED', 'DCT') and trans_ctgry_code='EX', 
	-- DECODE(trans_code,
	-- '31', trans_amt , 
	-- '32', (trans_amt -  trans_hist_amt ), 
	-- '38', trans_amt, 
	-- '41', trans_hist_amt * -1, 
	-- '42', trans_hist_amt * -1, 
	-- '65', trans_hist_amt * -1, 
	-- '66', trans_hist_amt * -1, 
	-- '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'R', '33', trans_date)), 0, trans_hist_amt * -1), 
	-- '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'R', '33', trans_date)), 0, trans_hist_amt * -1), 
	-- '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'R', '33', trans_date)), 0, trans_hist_amt * -1), 0),
	-- 0)
	IFF(financial_type_code = 'R' AND IN(source_sys_id, 'EXCEED', 'DCT') AND trans_ctgry_code = 'EX', DECODE(trans_code,
	'31', trans_amt,
	'32', ( trans_amt - trans_hist_amt ),
	'38', trans_amt,
	'41', trans_hist_amt * - 1,
	'42', trans_hist_amt * - 1,
	'65', trans_hist_amt * - 1,
	'66', trans_hist_amt * - 1,
	'90', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'91', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'92', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	0), 0) AS var_direct_other_recovery_alae_outstanding,
	-- *INF*: IIF(financial_type_code = 'R' and trans_ctgry_code <> 'EX', 
	-- 	DECODE(trans_code, 
	-- 	'25', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- 	'30', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- 	'31', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- 	'32', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- 	'33', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- 	'34', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- 	'38', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- 	'39', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- 	0),
	-- 0)
	-- 
	-- ----08/15/2011  Removed the filter of EXCEED data (and IN (source_sys_id , 'EXCEED', 'DCT')) 
	-- ----JIRA-PROD-4418 Added condition for trans_code '30' and return trans_amt for PMS claims
	IFF(financial_type_code = 'R' AND trans_ctgry_code <> 'EX', DECODE(trans_code,
	'25', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'30', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'31', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'32', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'33', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'34', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'38', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'39', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	0), 0) AS var_direct_other_recovery_loss_paid,
	-- *INF*: IIF(financial_type_code = 'R' and trans_ctgry_code = 'EX', 
	-- 	DECODE(trans_code,  
	-- 	'25', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- 	'30', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- 	'31', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- 	'32', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- 	'33', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- 	'34', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- 	'38', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),
	-- 	'39', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- 	 0)
	-- ,0)
	-- 
	-- --- 08/15/2011 - Removed the filter of EXCEED data  (and source_sys_id='EXCEED')
	-- ----JIRA-PROD-4418 Added condition for trans_code '30' and return trans_amt for PMS claims
	IFF(financial_type_code = 'R' AND trans_ctgry_code = 'EX', DECODE(trans_code,
	'25', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'30', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'31', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'32', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'33', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'34', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'38', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'39', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	0), 0) AS var_direct_other_recovery_alae_paid,
	-- *INF*: IIF(financial_type_code = 'R' and trans_ctgry_code <> 'EX', 
	-- DECODE(trans_code,  '25', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '30', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '31', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '32', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '33', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '34', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '38', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt), 
	-- '39', IIF(IN (source_sys_id , 'EXCEED', 'DCT'), trans_amt * -1, trans_amt),  
	-- '41', 0, 
	-- '42', 0,  
	-- '65',0, 
	-- '66', 0, 
	-- '90', 0, 
	-- '91', 0, 
	-- '92', 0, 0),0)
	IFF(financial_type_code = 'R' AND trans_ctgry_code <> 'EX', DECODE(trans_code,
	'25', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'30', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'31', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'32', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'33', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'34', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'38', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'39', IFF(IN(source_sys_id, 'EXCEED', 'DCT'), trans_amt * - 1, trans_amt),
	'41', 0,
	'42', 0,
	'65', 0,
	'66', 0,
	'90', 0,
	'91', 0,
	'92', 0,
	0), 0) AS v_net_other_recovery_recvrd_chg_amt,
	-- *INF*: IIF(financial_type_code = 'R' and IN (source_sys_id , 'EXCEED', 'DCT') and trans_ctgry_code <> 'EX', 
	-- DECODE(trans_code, '25', 0,
	-- '30',0,
	-- '31', trans_amt , 
	-- '32', (trans_amt -  trans_hist_amt ), 
	-- '33', 0, 
	-- '34', 0, 
	-- '38', trans_amt, 
	-- '39', 0, 
	-- '41', trans_hist_amt * -1, 
	-- '42', trans_hist_amt * -1, 
	-- '65', trans_hist_amt * -1, 
	-- '66', trans_hist_amt * -1, 
	-- '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'R', '33', trans_date)), 0, trans_hist_amt * -1), 
	-- '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'R', '33', trans_date)), 0, trans_hist_amt * -1), 
	-- '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'R', '33', trans_date)), 0, trans_hist_amt * -1), 0),
	-- 0)
	IFF(financial_type_code = 'R' AND IN(source_sys_id, 'EXCEED', 'DCT') AND trans_ctgry_code <> 'EX', DECODE(trans_code,
	'25', 0,
	'30', 0,
	'31', trans_amt,
	'32', ( trans_amt - trans_hist_amt ),
	'33', 0,
	'34', 0,
	'38', trans_amt,
	'39', 0,
	'41', trans_hist_amt * - 1,
	'42', trans_hist_amt * - 1,
	'65', trans_hist_amt * - 1,
	'66', trans_hist_amt * - 1,
	'90', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'91', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'92', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	0), 0) AS v_net_other_recovery_outstanding_reserve_chg_amt_OLD,
	-- *INF*: IIF(financial_type_code = 'R' and IN (source_sys_id , 'EXCEED', 'DCT') and trans_ctgry_code <> 'EX', 
	-- DECODE(trans_code, '25', trans_amt * -1,
	-- '30',trans_amt * -1,
	-- '31', 0, 
	-- '32', trans_hist_amt * -1, 
	-- '33', trans_amt * -1,
	-- '34',trans_amt * -1,
	-- '38',0,
	-- '39', trans_amt * -1,
	-- '41', trans_hist_amt * -1, 
	-- '42', trans_hist_amt * -1, 
	-- '65', trans_hist_amt * -1, 
	-- '66', trans_hist_amt * -1, 
	-- '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'R', '33', trans_date)), 0, trans_hist_amt * -1), 
	-- '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'R', '33', trans_date)), 0, trans_hist_amt * -1), 
	-- '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_TRANSACTION(IN_claimant_cov_det_ak_id,IN_cause_of_loss,IN_reserve_ctgry,IN_type_disability,'R', '33', trans_date)), 0, trans_hist_amt * -1), 0),
	-- 0)
	IFF(financial_type_code = 'R' AND IN(source_sys_id, 'EXCEED', 'DCT') AND trans_ctgry_code <> 'EX', DECODE(trans_code,
	'25', trans_amt * - 1,
	'30', trans_amt * - 1,
	'31', 0,
	'32', trans_hist_amt * - 1,
	'33', trans_amt * - 1,
	'34', trans_amt * - 1,
	'38', 0,
	'39', trans_amt * - 1,
	'41', trans_hist_amt * - 1,
	'42', trans_hist_amt * - 1,
	'65', trans_hist_amt * - 1,
	'66', trans_hist_amt * - 1,
	'90', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'91', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	'92', IFF(NOT LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.claimant_cov_det_ak_id IS NULL, 0, trans_hist_amt * - 1),
	0), 0) AS v_net_other_recovery_incurred_chg_amt,
	var_direct_loss_paid_excluding_recoveries AS direct_loss_paid_excluding_recoveries,
	var_direct_loss_outstanding_excluding_recoveries AS direct_loss_outstanding_excluding_recoveries,
	var_direct_loss_incurred_excluding_recoveries AS direct_loss_incurred_excluding_recoveries,
	var_direct_alae_paid_excluding_recoveries AS direct_alae_paid_excluding_recoveries,
	var_direct_alae_outstanding_excluding_recoveries AS direct_alae_outstanding_excluding_recoveries,
	-- *INF*: var_direct_alae_paid_excluding_recoveries + var_direct_alae_outstanding_excluding_recoveries
	-- 
	-- 
	-- --- Changed to above logic on 8/15/2011 
	-- ---var_direct_alae_incurred_excluding_recoveries
	var_direct_alae_paid_excluding_recoveries + var_direct_alae_outstanding_excluding_recoveries AS direct_alae_incurred_excluding_recoveries,
	var_direct_subrogation_paid AS direct_subrogation_paid,
	var_direct_subrogation_outstanding AS direct_subrogation_outstanding,
	-- *INF*: var_direct_subrogation_paid  +  var_direct_subrogation_outstanding
	-- 
	-- 
	-- ---var_direct_subrogation_incurred
	var_direct_subrogation_paid + var_direct_subrogation_outstanding AS direct_subrogation_incurred,
	var_direct_salvage_paid AS direct_salvage_paid,
	var_direct_salvage_outstanding AS direct_salvage_outstanding,
	-- *INF*: var_direct_salvage_paid  + var_direct_salvage_outstanding
	-- ---var_direct_salvage_incurred
	var_direct_salvage_paid + var_direct_salvage_outstanding AS direct_salvage_incurred,
	var_direct_other_recovery_paid AS direct_other_recovery_paid,
	var_direct_other_recovery_outstanding AS direct_other_recovery_outstanding,
	-- *INF*: var_direct_other_recovery_paid + var_direct_other_recovery_outstanding
	-- 
	-- ---var_direct_other_recovery_incurred
	var_direct_other_recovery_paid + var_direct_other_recovery_outstanding AS direct_other_recovery_incurred,
	var_direct_other_recovery_loss_outstanding AS direct_other_recovery_loss_outstanding,
	var_direct_other_recovery_loss_paid AS direct_other_recovery_loss_paid,
	-- *INF*: round(var_direct_other_recovery_loss_outstanding+var_direct_other_recovery_loss_paid,2)
	round(var_direct_other_recovery_loss_outstanding + var_direct_other_recovery_loss_paid, 2) AS direct_other_recovery_loss_incurred,
	var_direct_other_recovery_alae_outstanding AS direct_other_recovery_alae_outstanding,
	var_direct_other_recovery_alae_paid AS direct_other_recovery_alae_paid,
	-- *INF*: round(var_direct_other_recovery_alae_paid + var_direct_other_recovery_alae_outstanding,2)
	round(var_direct_other_recovery_alae_paid + var_direct_other_recovery_alae_outstanding, 2) AS direct_other_recovery_alae_incurred,
	-- *INF*: round(var_direct_loss_outstanding_excluding_recoveries + var_direct_subrogation_outstanding + var_direct_salvage_outstanding + var_direct_other_recovery_loss_outstanding,2)
	round(var_direct_loss_outstanding_excluding_recoveries + var_direct_subrogation_outstanding + var_direct_salvage_outstanding + var_direct_other_recovery_loss_outstanding, 2) AS direct_loss_outstanding_including_recoveries,
	-- *INF*: round(var_direct_loss_paid_excluding_recoveries + var_direct_subrogation_paid + var_direct_salvage_paid + var_direct_other_recovery_loss_paid,2)
	round(var_direct_loss_paid_excluding_recoveries + var_direct_subrogation_paid + var_direct_salvage_paid + var_direct_other_recovery_loss_paid, 2) AS direct_loss_paid_including_recoveries,
	-- *INF*: round(var_direct_loss_incurred_excluding_recoveries + var_direct_salvage_paid+var_direct_subrogation_paid +
	-- var_direct_other_recovery_loss_paid
	-- ,2)
	round(var_direct_loss_incurred_excluding_recoveries + var_direct_salvage_paid + var_direct_subrogation_paid + var_direct_other_recovery_loss_paid, 2) AS direct_loss_incurred_including_recoveries,
	-- *INF*: round(var_direct_loss_outstanding_excluding_recoveries +  var_direct_salvage_outstanding + var_direct_subrogation_outstanding +
	-- var_direct_other_recovery_loss_outstanding,2)
	round(var_direct_loss_outstanding_excluding_recoveries + var_direct_salvage_outstanding + var_direct_subrogation_outstanding + var_direct_other_recovery_loss_outstanding, 2) AS direct_loss_outstanding_out_BAD,
	-- *INF*: round(var_direct_loss_paid_excluding_recoveries + var_direct_salvage_paid+var_direct_subrogation_paid + var_direct_other_recovery_loss_paid,2)
	round(var_direct_loss_paid_excluding_recoveries + var_direct_salvage_paid + var_direct_subrogation_paid + var_direct_other_recovery_loss_paid, 2) AS direct_loss_paid_out_BAD,
	-- *INF*: round(var_direct_loss_outstanding_excluding_recoveries + var_direct_salvage_outstanding + var_direct_subrogation_incurred + var_direct_loss_incurred_excluding_recoveries,2)
	round(var_direct_loss_outstanding_excluding_recoveries + var_direct_salvage_outstanding + var_direct_subrogation_incurred + var_direct_loss_incurred_excluding_recoveries, 2) AS direct_loss_incurred_out_BAD,
	-- *INF*: round(var_direct_alae_paid_excluding_recoveries+var_direct_other_recovery_alae_paid,2)
	round(var_direct_alae_paid_excluding_recoveries + var_direct_other_recovery_alae_paid, 2) AS direct_alae_paid_including_recoveries,
	-- *INF*: round(var_direct_alae_outstanding_excluding_recoveries + var_direct_other_recovery_alae_outstanding,2)
	round(var_direct_alae_outstanding_excluding_recoveries + var_direct_other_recovery_alae_outstanding, 2) AS direct_alae_outstanding_including_recoveries,
	-- *INF*: round(var_direct_alae_incurred_excluding_recoveries + var_direct_other_recovery_alae_paid,2)
	round(var_direct_alae_incurred_excluding_recoveries + var_direct_other_recovery_alae_paid, 2) AS direct_alae_incurred_including_recoveries,
	-- *INF*: round(var_direct_salvage_paid+var_direct_subrogation_paid + var_direct_other_recovery_loss_paid,2)
	round(var_direct_salvage_paid + var_direct_subrogation_paid + var_direct_other_recovery_loss_paid, 2) AS total_direct_loss_recovery_paid,
	-- *INF*: round(var_direct_salvage_outstanding + var_direct_subrogation_outstanding + var_direct_other_recovery_loss_outstanding ,2)
	round(var_direct_salvage_outstanding + var_direct_subrogation_outstanding + var_direct_other_recovery_loss_outstanding, 2) AS total_direct_loss_recovery_outstanding,
	-- *INF*: round(var_direct_salvage_paid + 
	-- var_direct_subrogation_paid + 
	-- var_direct_other_recovery_loss_paid + 
	-- var_direct_salvage_outstanding +
	-- var_direct_subrogation_outstanding + 
	-- var_direct_other_recovery_loss_outstanding
	--  ,2)
	-- 
	round(var_direct_salvage_paid + var_direct_subrogation_paid + var_direct_other_recovery_loss_paid + var_direct_salvage_outstanding + var_direct_subrogation_outstanding + var_direct_other_recovery_loss_outstanding, 2) AS total_direct_loss_recovery_incurred,
	-- *INF*: round(var_direct_loss_paid_excluding_recoveries + var_direct_salvage_paid+var_direct_subrogation_paid + var_direct_other_recovery_loss_paid,2)
	round(var_direct_loss_paid_excluding_recoveries + var_direct_salvage_paid + var_direct_subrogation_paid + var_direct_other_recovery_loss_paid, 2) AS net_loss_paid,
	-- *INF*: round(var_direct_loss_outstanding_excluding_recoveries,2)
	round(var_direct_loss_outstanding_excluding_recoveries, 2) AS net_loss_outstanding,
	-- *INF*: round(var_direct_loss_incurred_excluding_recoveries + var_direct_salvage_paid + var_direct_subrogation_paid + var_direct_other_recovery_loss_paid,2)
	-- 
	round(var_direct_loss_incurred_excluding_recoveries + var_direct_salvage_paid + var_direct_subrogation_paid + var_direct_other_recovery_loss_paid, 2) AS net_loss_incurred,
	-- *INF*: round(var_direct_alae_paid_excluding_recoveries+var_direct_other_recovery_alae_paid,2)
	round(var_direct_alae_paid_excluding_recoveries + var_direct_other_recovery_alae_paid, 2) AS net_alae_paid,
	-- *INF*: round(var_direct_alae_outstanding_excluding_recoveries + var_direct_other_recovery_alae_outstanding,2)
	round(var_direct_alae_outstanding_excluding_recoveries + var_direct_other_recovery_alae_outstanding, 2) AS net_alae_outstanding,
	-- *INF*: round(var_direct_alae_incurred_excluding_recoveries + var_direct_other_recovery_alae_paid + var_direct_other_recovery_alae_outstanding,2)
	-- 
	round(var_direct_alae_incurred_excluding_recoveries + var_direct_other_recovery_alae_paid + var_direct_other_recovery_alae_outstanding, 2) AS net_alae_incurred,
	acct_entered_date_id,
	strtgc_bus_dvsn_dim_id
	FROM EXP_set_default_date_ids
	LEFT JOIN LKP_CLAIM_TRANSACTION LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_D_23_trans_date
	ON LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_D_23_trans_date.claimant_cov_det_ak_id = IN_claimant_cov_det_ak_id
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_D_23_trans_date.cause_of_loss = IN_cause_of_loss
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_D_23_trans_date.reserve_ctgry = IN_reserve_ctgry
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_D_23_trans_date.type_disability = IN_type_disability
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_D_23_trans_date.financial_type_code = 'D'
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_D_23_trans_date.trans_code = '23'
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_D_23_trans_date.trans_date = trans_date

	LEFT JOIN LKP_CLAIM_TRANSACTION LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_E_23_trans_date
	ON LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_E_23_trans_date.claimant_cov_det_ak_id = IN_claimant_cov_det_ak_id
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_E_23_trans_date.cause_of_loss = IN_cause_of_loss
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_E_23_trans_date.reserve_ctgry = IN_reserve_ctgry
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_E_23_trans_date.type_disability = IN_type_disability
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_E_23_trans_date.financial_type_code = 'E'
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_E_23_trans_date.trans_code = '23'
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_E_23_trans_date.trans_date = trans_date

	LEFT JOIN LKP_CLAIM_TRANSACTION LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_B_33_trans_date
	ON LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_B_33_trans_date.claimant_cov_det_ak_id = IN_claimant_cov_det_ak_id
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_B_33_trans_date.cause_of_loss = IN_cause_of_loss
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_B_33_trans_date.reserve_ctgry = IN_reserve_ctgry
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_B_33_trans_date.type_disability = IN_type_disability
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_B_33_trans_date.financial_type_code = 'B'
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_B_33_trans_date.trans_code = '33'
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_B_33_trans_date.trans_date = trans_date

	LEFT JOIN LKP_CLAIM_TRANSACTION LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_S_33_trans_date
	ON LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_S_33_trans_date.claimant_cov_det_ak_id = IN_claimant_cov_det_ak_id
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_S_33_trans_date.cause_of_loss = IN_cause_of_loss
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_S_33_trans_date.reserve_ctgry = IN_reserve_ctgry
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_S_33_trans_date.type_disability = IN_type_disability
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_S_33_trans_date.financial_type_code = 'S'
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_S_33_trans_date.trans_code = '33'
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_S_33_trans_date.trans_date = trans_date

	LEFT JOIN LKP_CLAIM_TRANSACTION LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date
	ON LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.claimant_cov_det_ak_id = IN_claimant_cov_det_ak_id
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.cause_of_loss = IN_cause_of_loss
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.reserve_ctgry = IN_reserve_ctgry
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.type_disability = IN_type_disability
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.financial_type_code = 'R'
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.trans_code = '33'
	AND LKP_CLAIM_TRANSACTION_IN_claimant_cov_det_ak_id_IN_cause_of_loss_IN_reserve_ctgry_IN_type_disability_R_33_trans_date.trans_date = trans_date

),
LKP_Claim_Loss_Transaction_Fact AS (
	SELECT
	claim_loss_trans_fact_id,
	edw_claim_trans_pk_id,
	claim_occurrence_dim_id,
	claim_occurrence_dim_hist_id,
	claimant_dim_id,
	claimant_dim_hist_id,
	claimant_cov_dim_id,
	claimant_cov_dim_hist_id,
	cov_dim_id,
	cov_dim_hist_id,
	claim_trans_type_dim_id,
	claim_financial_type_dim_id,
	claim_rep_dim_prim_claim_rep_id,
	claim_rep_dim_prim_claim_rep_hist_id,
	claim_rep_dim_examiner_id,
	claim_rep_dim_examiner_hist_id,
	claim_rep_dim_prim_litigation_handler_id,
	claim_rep_dim_prim_litigation_handler_hist_id,
	claim_rep_dim_trans_entry_oper_id,
	claim_rep_dim_trans_entry_oper_hist_id,
	claim_rep_dim_claim_created_by_id,
	pol_dim_id,
	pol_dim_hist_id,
	agency_dim_id,
	agency_dim_hist_id,
	claim_pay_dim_id,
	claim_pay_dim_hist_id,
	claim_pay_ctgry_type_dim_id,
	claim_pay_ctgry_type_dim_hist_id,
	claim_case_dim_id,
	claim_case_dim_hist_id,
	contract_cust_dim_id,
	contract_cust_dim_hist_id,
	claim_master_1099_list_dim_id,
	claim_subrogation_dim_id,
	claim_trans_date_id,
	claim_trans_reprocess_date_id,
	claim_loss_date_id,
	claim_discovery_date_id,
	claim_scripted_date_id,
	source_claim_rpted_date_id,
	claim_rpted_date_id,
	claim_open_date_id,
	claim_close_date_id,
	claim_reopen_date_id,
	claim_closed_after_reopen_date_id,
	claim_notice_only_date_id,
	claim_cat_start_date_id,
	claim_cat_end_date_id,
	claim_rep_assigned_date_id,
	claim_rep_unassigned_date_id,
	pol_eff_date_id,
	pol_exp_date_id,
	claim_subrogation_referred_to_subrogation_date_id,
	claim_subrogation_pay_start_date_id,
	claim_subrogation_closure_date_id,
	acct_entered_date_id,
	trans_amt,
	trans_hist_amt,
	tax_id,
	direct_loss_paid_excluding_recoveries,
	direct_loss_outstanding_excluding_recoveries,
	direct_loss_incurred_excluding_recoveries,
	direct_alae_paid_excluding_recoveries,
	direct_alae_outstanding_excluding_recoveries,
	direct_alae_incurred_excluding_recoveries,
	direct_loss_paid_including_recoveries,
	direct_loss_outstanding_including_recoveries,
	direct_loss_incurred_including_recoveries,
	direct_alae_paid_including_recoveries,
	direct_alae_outstanding_including_recoveries,
	direct_alae_incurred_including_recoveries,
	direct_subrogation_paid,
	direct_subrogation_outstanding,
	direct_subrogation_incurred,
	direct_salvage_paid,
	direct_salvage_outstanding,
	direct_salvage_incurred,
	direct_other_recovery_loss_paid,
	direct_other_recovery_loss_outstanding,
	direct_other_recovery_loss_incurred,
	direct_other_recovery_alae_paid,
	direct_other_recovery_alae_outstanding,
	direct_other_recovery_alae_incurred,
	total_direct_loss_recovery_paid,
	total_direct_loss_recovery_outstanding,
	total_direct_loss_recovery_incurred,
	direct_other_recovery_paid,
	direct_other_recovery_outstanding,
	direct_other_recovery_incurred,
	ceded_loss_paid,
	ceded_loss_outstanding,
	ceded_loss_incurred,
	ceded_alae_paid,
	ceded_alae_outstanding,
	ceded_alae_incurred,
	ceded_salvage_paid,
	ceded_subrogation_paid,
	ceded_other_recovery_loss_paid,
	ceded_other_recovery_alae_paid,
	total_ceded_loss_recovery_paid,
	net_loss_paid,
	net_loss_outstanding,
	net_loss_incurred,
	net_alae_paid,
	net_alae_outstanding,
	net_alae_incurred,
	strtgc_bus_dvsn_dim_id,
	ClaimReserveDimId,
	ClaimRepresentativeDimFeatureClaimRepresentativeId,
	FeatureRepresentativeAssignedDateId,
	InsuranceReferenceDimId,
	AgencyDimId,
	SalesDivisionDimId,
	InsuranceReferenceCoverageDimId,
	CoverageDetailDimId
	FROM (
		SELECT 
			claim_loss_trans_fact_id,
			edw_claim_trans_pk_id,
			claim_occurrence_dim_id,
			claim_occurrence_dim_hist_id,
			claimant_dim_id,
			claimant_dim_hist_id,
			claimant_cov_dim_id,
			claimant_cov_dim_hist_id,
			cov_dim_id,
			cov_dim_hist_id,
			claim_trans_type_dim_id,
			claim_financial_type_dim_id,
			claim_rep_dim_prim_claim_rep_id,
			claim_rep_dim_prim_claim_rep_hist_id,
			claim_rep_dim_examiner_id,
			claim_rep_dim_examiner_hist_id,
			claim_rep_dim_prim_litigation_handler_id,
			claim_rep_dim_prim_litigation_handler_hist_id,
			claim_rep_dim_trans_entry_oper_id,
			claim_rep_dim_trans_entry_oper_hist_id,
			claim_rep_dim_claim_created_by_id,
			pol_dim_id,
			pol_dim_hist_id,
			agency_dim_id,
			agency_dim_hist_id,
			claim_pay_dim_id,
			claim_pay_dim_hist_id,
			claim_pay_ctgry_type_dim_id,
			claim_pay_ctgry_type_dim_hist_id,
			claim_case_dim_id,
			claim_case_dim_hist_id,
			contract_cust_dim_id,
			contract_cust_dim_hist_id,
			claim_master_1099_list_dim_id,
			claim_subrogation_dim_id,
			claim_trans_date_id,
			claim_trans_reprocess_date_id,
			claim_loss_date_id,
			claim_discovery_date_id,
			claim_scripted_date_id,
			source_claim_rpted_date_id,
			claim_rpted_date_id,
			claim_open_date_id,
			claim_close_date_id,
			claim_reopen_date_id,
			claim_closed_after_reopen_date_id,
			claim_notice_only_date_id,
			claim_cat_start_date_id,
			claim_cat_end_date_id,
			claim_rep_assigned_date_id,
			claim_rep_unassigned_date_id,
			pol_eff_date_id,
			pol_exp_date_id,
			claim_subrogation_referred_to_subrogation_date_id,
			claim_subrogation_pay_start_date_id,
			claim_subrogation_closure_date_id,
			acct_entered_date_id,
			trans_amt,
			trans_hist_amt,
			tax_id,
			direct_loss_paid_excluding_recoveries,
			direct_loss_outstanding_excluding_recoveries,
			direct_loss_incurred_excluding_recoveries,
			direct_alae_paid_excluding_recoveries,
			direct_alae_outstanding_excluding_recoveries,
			direct_alae_incurred_excluding_recoveries,
			direct_loss_paid_including_recoveries,
			direct_loss_outstanding_including_recoveries,
			direct_loss_incurred_including_recoveries,
			direct_alae_paid_including_recoveries,
			direct_alae_outstanding_including_recoveries,
			direct_alae_incurred_including_recoveries,
			direct_subrogation_paid,
			direct_subrogation_outstanding,
			direct_subrogation_incurred,
			direct_salvage_paid,
			direct_salvage_outstanding,
			direct_salvage_incurred,
			direct_other_recovery_loss_paid,
			direct_other_recovery_loss_outstanding,
			direct_other_recovery_loss_incurred,
			direct_other_recovery_alae_paid,
			direct_other_recovery_alae_outstanding,
			direct_other_recovery_alae_incurred,
			total_direct_loss_recovery_paid,
			total_direct_loss_recovery_outstanding,
			total_direct_loss_recovery_incurred,
			direct_other_recovery_paid,
			direct_other_recovery_outstanding,
			direct_other_recovery_incurred,
			ceded_loss_paid,
			ceded_loss_outstanding,
			ceded_loss_incurred,
			ceded_alae_paid,
			ceded_alae_outstanding,
			ceded_alae_incurred,
			ceded_salvage_paid,
			ceded_subrogation_paid,
			ceded_other_recovery_loss_paid,
			ceded_other_recovery_alae_paid,
			total_ceded_loss_recovery_paid,
			net_loss_paid,
			net_loss_outstanding,
			net_loss_incurred,
			net_alae_paid,
			net_alae_outstanding,
			net_alae_incurred,
			strtgc_bus_dvsn_dim_id,
			ClaimReserveDimId,
			ClaimRepresentativeDimFeatureClaimRepresentativeId,
			FeatureRepresentativeAssignedDateId,
			InsuranceReferenceDimId,
			AgencyDimId,
			SalesDivisionDimId,
			InsuranceReferenceCoverageDimId,
			CoverageDetailDimId
		FROM claim_loss_transaction_fact
		WHERE audit_id > 0
		
		
		--- If we do not put this filter, we will insert additional transaction when trans code chnages from 90-91-92. We need to lookup only for Direct Transaction. --This way it exludes audit_id = -50 as these will have same edw_trans_pk_Id
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_trans_pk_id ORDER BY claim_loss_trans_fact_id DESC) = 1
),
EXP_DETECT_CHANGES AS (
	SELECT
	LKP_Claim_Loss_Transaction_Fact.claim_loss_trans_fact_id AS lkp_claim_loss_trans_fact_id,
	LKP_Claim_Loss_Transaction_Fact.edw_claim_trans_pk_id AS lkp_edw_claim_trans_pk_id,
	LKP_Claim_Loss_Transaction_Fact.claim_occurrence_dim_id AS lkp_claim_occurrence_dim_id,
	LKP_Claim_Loss_Transaction_Fact.claim_occurrence_dim_hist_id AS lkp_claim_occurrence_dim_hist_id,
	LKP_Claim_Loss_Transaction_Fact.claimant_dim_id AS lkp_claimant_dim_id,
	LKP_Claim_Loss_Transaction_Fact.claimant_dim_hist_id AS lkp_claimant_dim_hist_id,
	LKP_Claim_Loss_Transaction_Fact.claimant_cov_dim_id AS lkp_claimant_cov_dim_id,
	LKP_Claim_Loss_Transaction_Fact.claimant_cov_dim_hist_id AS lkp_claimant_cov_dim_hist_id,
	LKP_Claim_Loss_Transaction_Fact.cov_dim_id AS lkp_cov_dim_id,
	LKP_Claim_Loss_Transaction_Fact.cov_dim_hist_id AS lkp_cov_dim_hist_id,
	LKP_Claim_Loss_Transaction_Fact.claim_trans_type_dim_id AS lkp_claim_trans_type_dim_id,
	LKP_Claim_Loss_Transaction_Fact.claim_financial_type_dim_id AS lkp_claim_financial_type_dim_id,
	LKP_Claim_Loss_Transaction_Fact.claim_rep_dim_prim_claim_rep_id AS lkp_claim_rep_dim_prim_claim_rep_id,
	LKP_Claim_Loss_Transaction_Fact.claim_rep_dim_prim_claim_rep_hist_id AS lkp_claim_rep_dim_prim_claim_rep_hist_id,
	LKP_Claim_Loss_Transaction_Fact.claim_rep_dim_examiner_id AS lkp_claim_rep_dim_examiner_id,
	LKP_Claim_Loss_Transaction_Fact.claim_rep_dim_examiner_hist_id AS lkp_claim_rep_dim_examiner_hist_id,
	LKP_Claim_Loss_Transaction_Fact.claim_rep_dim_prim_litigation_handler_id AS lkp_claim_rep_dim_prim_litigation_handler_id,
	LKP_Claim_Loss_Transaction_Fact.claim_rep_dim_prim_litigation_handler_hist_id AS lkp_claim_rep_dim_prim_litigation_handler_hist_id,
	LKP_Claim_Loss_Transaction_Fact.claim_rep_dim_trans_entry_oper_id AS lkp_claim_rep_dim_trans_entry_oper_id,
	LKP_Claim_Loss_Transaction_Fact.claim_rep_dim_trans_entry_oper_hist_id AS lkp_claim_rep_dim_trans_entry_oper_hist_id,
	LKP_Claim_Loss_Transaction_Fact.claim_rep_dim_claim_created_by_id AS lkp_claim_rep_dim_claim_created_by_id,
	LKP_Claim_Loss_Transaction_Fact.pol_dim_id AS lkp_pol_key_dim_id,
	LKP_Claim_Loss_Transaction_Fact.pol_dim_hist_id AS lkp_pol_key_dim_hist_id,
	LKP_Claim_Loss_Transaction_Fact.agency_dim_id AS lkp_agency_dim_id,
	LKP_Claim_Loss_Transaction_Fact.agency_dim_hist_id AS lkp_agency_dim_hist_id,
	LKP_Claim_Loss_Transaction_Fact.claim_pay_dim_id AS lkp_claim_pay_dim_id,
	LKP_Claim_Loss_Transaction_Fact.claim_pay_dim_hist_id AS lkp_claim_pay_dim_hist_id,
	LKP_Claim_Loss_Transaction_Fact.claim_pay_ctgry_type_dim_id AS lkp_claim_pay_ctgry_type_dim_id,
	LKP_Claim_Loss_Transaction_Fact.claim_pay_ctgry_type_dim_hist_id AS lkp_claim_pay_ctgry_type_dim_hist_id,
	LKP_Claim_Loss_Transaction_Fact.claim_case_dim_id AS lkp_claim_case_dim_id,
	LKP_Claim_Loss_Transaction_Fact.claim_case_dim_hist_id AS lkp_claim_case_dim_hist_id,
	LKP_Claim_Loss_Transaction_Fact.contract_cust_dim_id AS lkp_contract_cust_dim_id,
	LKP_Claim_Loss_Transaction_Fact.contract_cust_dim_hist_id AS lkp_contract_cust_dim_hist_id,
	LKP_Claim_Loss_Transaction_Fact.claim_master_1099_list_dim_id AS lkp_claim_master_1099_list_dim_id,
	LKP_Claim_Loss_Transaction_Fact.claim_subrogation_dim_id AS lkp_claim_subrogation_dim_id,
	LKP_Claim_Loss_Transaction_Fact.claim_trans_date_id AS lkp_claim_trans_date_id,
	LKP_Claim_Loss_Transaction_Fact.claim_trans_reprocess_date_id AS lkp_claim_trans_reprocess_date_id,
	LKP_Claim_Loss_Transaction_Fact.claim_loss_date_id AS lkp_claim_loss_date_id,
	LKP_Claim_Loss_Transaction_Fact.claim_discovery_date_id AS lkp_claim_discovery_date_id,
	LKP_Claim_Loss_Transaction_Fact.claim_scripted_date_id AS lkp_claim_scripted_date_id,
	LKP_Claim_Loss_Transaction_Fact.source_claim_rpted_date_id AS lkp_source_claim_rpted_date_id,
	LKP_Claim_Loss_Transaction_Fact.claim_rpted_date_id AS lkp_claim_rpted_date_id,
	LKP_Claim_Loss_Transaction_Fact.claim_open_date_id AS lkp_claim_open_date_id,
	LKP_Claim_Loss_Transaction_Fact.claim_close_date_id AS lkp_claim_close_date_id,
	LKP_Claim_Loss_Transaction_Fact.claim_reopen_date_id AS lkp_claim_reopen_date_id,
	LKP_Claim_Loss_Transaction_Fact.claim_closed_after_reopen_date_id AS lkp_claim_closed_after_reopen_date_id,
	LKP_Claim_Loss_Transaction_Fact.claim_notice_only_date_id AS lkp_claim_notice_only_date_id,
	LKP_Claim_Loss_Transaction_Fact.claim_cat_start_date_id AS lkp_claim_cat_start_date_id,
	LKP_Claim_Loss_Transaction_Fact.claim_cat_end_date_id AS lkp_claim_cat_end_date_id,
	LKP_Claim_Loss_Transaction_Fact.claim_rep_assigned_date_id AS lkp_claim_rep_assigned_date_id,
	LKP_Claim_Loss_Transaction_Fact.claim_rep_unassigned_date_id AS lkp_claim_rep_unassigned_date_id,
	LKP_Claim_Loss_Transaction_Fact.pol_eff_date_id AS lkp_pol_eff_date_id,
	LKP_Claim_Loss_Transaction_Fact.pol_exp_date_id AS lkp_pol_exp_date_id,
	LKP_Claim_Loss_Transaction_Fact.claim_subrogation_referred_to_subrogation_date_id AS lkp_claim_subrogation_referred_to_subrogation_date_id,
	LKP_Claim_Loss_Transaction_Fact.claim_subrogation_pay_start_date_id AS lkp_claim_subrogation_pay_start_date_id,
	LKP_Claim_Loss_Transaction_Fact.claim_subrogation_closure_date_id AS lkp_claim_subrogation_closure_date_id,
	LKP_Claim_Loss_Transaction_Fact.acct_entered_date_id AS lkp_acct_entered_date_id,
	LKP_Claim_Loss_Transaction_Fact.trans_amt AS lkp_trans_amt,
	LKP_Claim_Loss_Transaction_Fact.trans_hist_amt AS lkp_trans_hist_amt,
	LKP_Claim_Loss_Transaction_Fact.tax_id AS lkp_tax_id,
	LKP_Claim_Loss_Transaction_Fact.direct_loss_paid_excluding_recoveries AS lkp_direct_loss_paid_excluding_recoveries,
	LKP_Claim_Loss_Transaction_Fact.direct_loss_outstanding_excluding_recoveries AS lkp_direct_loss_outstanding_excluding_recoveries,
	LKP_Claim_Loss_Transaction_Fact.direct_loss_incurred_excluding_recoveries AS lkp_direct_loss_incurred_excluding_recoveries,
	LKP_Claim_Loss_Transaction_Fact.direct_alae_paid_excluding_recoveries AS lkp_direct_alae_paid_excluding_recoveries,
	LKP_Claim_Loss_Transaction_Fact.direct_alae_outstanding_excluding_recoveries AS lkp_direct_alae_outstanding_excluding_recoveries,
	LKP_Claim_Loss_Transaction_Fact.direct_alae_incurred_excluding_recoveries AS lkp_direct_alae_incurred_excluding_recoveries,
	LKP_Claim_Loss_Transaction_Fact.direct_loss_paid_including_recoveries AS lkp_direct_loss_paid_including_recoveries,
	LKP_Claim_Loss_Transaction_Fact.direct_loss_outstanding_including_recoveries AS lkp_direct_loss_outstanding_including_recoveries,
	LKP_Claim_Loss_Transaction_Fact.direct_loss_incurred_including_recoveries AS lkp_direct_loss_incurred_including_recoveries,
	LKP_Claim_Loss_Transaction_Fact.direct_alae_paid_including_recoveries AS lkp_direct_alae_paid_including_recoveries,
	LKP_Claim_Loss_Transaction_Fact.direct_alae_outstanding_including_recoveries AS lkp_direct_alae_outstanding_including_recoveries,
	LKP_Claim_Loss_Transaction_Fact.direct_alae_incurred_including_recoveries AS lkp_direct_alae_incurred_including_recoveries,
	LKP_Claim_Loss_Transaction_Fact.direct_subrogation_paid AS lkp_direct_subrogation_paid,
	LKP_Claim_Loss_Transaction_Fact.direct_subrogation_outstanding AS lkp_direct_subrogation_outstanding,
	LKP_Claim_Loss_Transaction_Fact.direct_subrogation_incurred AS lkp_direct_subrogation_incurred,
	LKP_Claim_Loss_Transaction_Fact.direct_salvage_paid AS lkp_direct_salvage_paid,
	LKP_Claim_Loss_Transaction_Fact.direct_salvage_outstanding AS lkp_direct_salvage_outstanding,
	LKP_Claim_Loss_Transaction_Fact.direct_salvage_incurred AS lkp_direct_salvage_incurred,
	LKP_Claim_Loss_Transaction_Fact.direct_other_recovery_loss_paid AS lkp_direct_other_recovery_loss_paid,
	LKP_Claim_Loss_Transaction_Fact.direct_other_recovery_loss_outstanding AS lkp_direct_other_recovery_loss_outstanding,
	LKP_Claim_Loss_Transaction_Fact.direct_other_recovery_loss_incurred AS lkp_direct_other_recovery_loss_incurred,
	LKP_Claim_Loss_Transaction_Fact.direct_other_recovery_alae_paid AS lkp_direct_other_recovery_alae_paid,
	LKP_Claim_Loss_Transaction_Fact.direct_other_recovery_alae_outstanding AS lkp_direct_other_recovery_alae_outstanding,
	LKP_Claim_Loss_Transaction_Fact.direct_other_recovery_alae_incurred AS lkp_direct_other_recovery_alae_incurred,
	LKP_Claim_Loss_Transaction_Fact.total_direct_loss_recovery_paid AS lkp_total_direct_loss_recovery_paid,
	LKP_Claim_Loss_Transaction_Fact.total_direct_loss_recovery_outstanding AS lkp_total_direct_loss_recovery_outstanding,
	LKP_Claim_Loss_Transaction_Fact.total_direct_loss_recovery_incurred AS lkp_total_direct_loss_recovery_incurred,
	LKP_Claim_Loss_Transaction_Fact.direct_other_recovery_paid AS lkp_direct_other_recovery_paid,
	LKP_Claim_Loss_Transaction_Fact.direct_other_recovery_outstanding AS lkp_direct_other_recovery_outstanding,
	LKP_Claim_Loss_Transaction_Fact.direct_other_recovery_incurred AS lkp_direct_other_recovery_incurred,
	LKP_Claim_Loss_Transaction_Fact.ceded_loss_paid AS lkp_ceded_loss_paid,
	LKP_Claim_Loss_Transaction_Fact.ceded_loss_outstanding AS lkp_ceded_loss_outstanding,
	LKP_Claim_Loss_Transaction_Fact.ceded_loss_incurred AS lkp_ceded_loss_incurred,
	LKP_Claim_Loss_Transaction_Fact.ceded_alae_paid AS lkp_ceded_alae_paid,
	LKP_Claim_Loss_Transaction_Fact.ceded_alae_outstanding AS lkp_ceded_alae_outstanding,
	LKP_Claim_Loss_Transaction_Fact.ceded_alae_incurred AS lkp_ceded_alae_incurred,
	LKP_Claim_Loss_Transaction_Fact.ceded_salvage_paid AS lkp_ceded_salvage_paid,
	LKP_Claim_Loss_Transaction_Fact.ceded_subrogation_paid AS lkp_ceded_subrogation_paid,
	LKP_Claim_Loss_Transaction_Fact.ceded_other_recovery_loss_paid AS lkp_ceded_other_recovery_loss_paid,
	LKP_Claim_Loss_Transaction_Fact.ceded_other_recovery_alae_paid AS lkp_ceded_other_recovery_alae_paid,
	LKP_Claim_Loss_Transaction_Fact.total_ceded_loss_recovery_paid AS lkp_total_ceded_loss_recovery_paid,
	LKP_Claim_Loss_Transaction_Fact.net_loss_paid AS lkp_net_loss_paid,
	LKP_Claim_Loss_Transaction_Fact.net_loss_outstanding AS lkp_net_loss_outstanding,
	LKP_Claim_Loss_Transaction_Fact.net_loss_incurred AS lkp_net_loss_incurred,
	LKP_Claim_Loss_Transaction_Fact.net_alae_paid AS lkp_net_alae_paid,
	LKP_Claim_Loss_Transaction_Fact.net_alae_outstanding AS lkp_net_alae_outstanding,
	LKP_Claim_Loss_Transaction_Fact.net_alae_incurred AS lkp_net_alae_incurred,
	LKP_Claim_Loss_Transaction_Fact.strtgc_bus_dvsn_dim_id AS lkp_strtgc_bus_dvsn_dim_id,
	LKP_Claim_Loss_Transaction_Fact.ClaimReserveDimId AS lkp_ClaimReserveDimId,
	LKP_Claim_Loss_Transaction_Fact.ClaimRepresentativeDimFeatureClaimRepresentativeId AS lkp_ClaimRepresentativeDimFeatureClaimRepresentativeId,
	LKP_Claim_Loss_Transaction_Fact.FeatureRepresentativeAssignedDateId AS lkp_FeatureRepresentativeAssignedDateId,
	LKP_Claim_Loss_Transaction_Fact.InsuranceReferenceDimId AS lkp_InsuranceReferenceDimId,
	LKP_Claim_Loss_Transaction_Fact.AgencyDimId AS lkp_AgencyDimId,
	LKP_Claim_Loss_Transaction_Fact.SalesDivisionDimId AS lkp_SalesDivisionDimId,
	LKP_Claim_Loss_Transaction_Fact.InsuranceReferenceCoverageDimId AS lkp_InsuranceReferenceCoverageDimId,
	LKP_Claim_Loss_Transaction_Fact.CoverageDetailDimId AS lkp_CoverageDetailDimId,
	EXP_set_financial_values.claim_occurrence_dim_id_out,
	EXP_set_financial_values.claim_occurrence_dim_hist_id_out,
	EXP_set_financial_values.claimant_dim_id_out,
	EXP_set_financial_values.claimant_dim_hist_id_out,
	EXP_set_financial_values.claimant_cov_dim_id_out,
	EXP_set_financial_values.claimant_cov_dim_hist_id_out,
	EXP_set_financial_values.cov_dim_id_out,
	EXP_set_financial_values.cov_dim_hist_id_out,
	EXP_set_financial_values.claim_trans_type_dim_id_out,
	EXP_set_financial_values.claim_financial_type_dim_id_out,
	EXP_set_financial_values.claim_rep_prim_claim_rep_dim_id_out,
	EXP_set_financial_values.claim_rep_prim_claim_rep_dim_hist_id_out,
	EXP_set_financial_values.claim_rep_dim_examiner_id_out,
	EXP_set_financial_values.claim_rep_dim_examiner_hist_id_out,
	EXP_set_financial_values.claim_rep_dim_prim_litigation_handler_id_out,
	EXP_set_financial_values.claim_rep_dim_prim_litigation_handler_hist_id_out,
	EXP_set_financial_values.pol_key_dim_id_out,
	EXP_set_financial_values.pol_key_dim_hist_id_out,
	EXP_set_financial_values.agency_dim_id_out,
	EXP_set_financial_values.agency_dim_hist_id_out,
	EXP_set_financial_values.claim_payment_dim_id_out,
	EXP_set_financial_values.claim_payment_dim_hist_id_out,
	EXP_set_financial_values.tax_id,
	EXP_set_financial_values.claim_master_1099_list_dim_id_out AS claim_master_1099_list_dim_id,
	EXP_set_financial_values.claim_pay_ctgry_type_dim_id_out,
	EXP_set_financial_values.claim_pay_ctgry_type_dim_hist_id_out,
	EXP_set_financial_values.claim_created_by_dim_id,
	EXP_set_financial_values.claim_case_dim_id_out,
	EXP_set_financial_values.claim_case_dim_hist_id_out,
	EXP_set_financial_values.claim_subrogation_dim_id_out,
	EXP_set_financial_values.claim_trans_oper_dim_id,
	EXP_set_financial_values.claim_trans_date_id,
	EXP_set_financial_values.reprocess_date_id,
	EXP_set_financial_values.claim_loss_date_id,
	EXP_set_financial_values.claim_discovery_date_id,
	EXP_set_financial_values.source_claim_rpted_date_id,
	EXP_set_financial_values.claim_scripted_date_id,
	EXP_set_financial_values.claim_occurrence_rpted_date_id,
	EXP_set_financial_values.claim_open_date_id,
	EXP_set_financial_values.claim_close_date_id,
	EXP_set_financial_values.claim_reopen_date_id,
	EXP_set_financial_values.claim_closed_after_reopen_date_id,
	EXP_set_financial_values.claim_notice_only_date_id,
	EXP_set_financial_values.claim_cat_start_date_id,
	EXP_set_financial_values.claim_cat_end_date_id,
	EXP_set_financial_values.claim_rep_assigned_date_id,
	EXP_set_financial_values.claim_rep_unassigned_date_id,
	EXP_set_financial_values.claim_trans_id,
	EXP_set_financial_values.pol_eff_date_id,
	EXP_set_financial_values.pol_exp_date_id,
	EXP_set_financial_values.financial_type_code,
	EXP_set_financial_values.trans_code,
	EXP_set_financial_values.trans_amt,
	EXP_set_financial_values.trans_hist_amt,
	EXP_set_financial_values.direct_loss_paid_excluding_recoveries,
	EXP_set_financial_values.direct_loss_outstanding_excluding_recoveries,
	EXP_set_financial_values.direct_loss_incurred_excluding_recoveries,
	EXP_set_financial_values.direct_alae_paid_excluding_recoveries,
	EXP_set_financial_values.direct_alae_outstanding_excluding_recoveries,
	EXP_set_financial_values.direct_alae_incurred_excluding_recoveries,
	EXP_set_financial_values.direct_subrogation_paid,
	EXP_set_financial_values.direct_subrogation_outstanding,
	EXP_set_financial_values.direct_subrogation_incurred,
	EXP_set_financial_values.direct_salvage_paid,
	EXP_set_financial_values.direct_salvage_outstanding,
	EXP_set_financial_values.direct_salvage_incurred,
	EXP_set_financial_values.direct_other_recovery_paid,
	EXP_set_financial_values.direct_other_recovery_outstanding,
	EXP_set_financial_values.direct_other_recovery_incurred,
	EXP_set_financial_values.direct_loss_paid_including_recoveries,
	EXP_set_financial_values.direct_loss_outstanding_including_recoveries,
	EXP_set_financial_values.direct_loss_incurred_including_recoveries,
	EXP_set_financial_values.audit_id,
	EXP_set_financial_values.referred_to_subrogation_date_id,
	EXP_set_financial_values.pay_start_date_id,
	EXP_set_financial_values.closure_date_id,
	EXP_set_financial_values.contract_cust_dim_id_out,
	EXP_set_financial_values.contract_cust_dim_hist_id_out,
	SYSDATE AS modified_date,
	EXP_set_financial_values.err_flag,
	err_flag AS err_flag_out,
	EXP_set_financial_values.default_dim_id,
	EXP_set_financial_values.direct_other_recovery_loss_outstanding,
	EXP_set_financial_values.direct_other_recovery_alae_outstanding,
	EXP_set_financial_values.direct_other_recovery_loss_paid,
	EXP_set_financial_values.direct_other_recovery_alae_paid,
	EXP_set_financial_values.direct_other_recovery_loss_incurred,
	EXP_set_financial_values.direct_loss_outstanding_out_BAD AS direct_loss_outstanding_out,
	EXP_set_financial_values.direct_loss_incurred_out_BAD AS direct_loss_incurred_out,
	EXP_set_financial_values.direct_alae_paid_including_recoveries,
	EXP_set_financial_values.direct_loss_paid_out_BAD AS direct_loss_paid_out,
	EXP_set_financial_values.direct_alae_outstanding_including_recoveries,
	EXP_set_financial_values.direct_other_recovery_alae_incurred,
	EXP_set_financial_values.direct_alae_incurred_including_recoveries,
	EXP_set_financial_values.total_direct_loss_recovery_paid,
	EXP_set_financial_values.total_direct_loss_recovery_outstanding,
	EXP_set_financial_values.total_direct_loss_recovery_incurred,
	EXP_set_financial_values.net_loss_paid,
	EXP_set_financial_values.net_loss_outstanding,
	EXP_set_financial_values.net_loss_incurred,
	EXP_set_financial_values.net_alae_paid,
	EXP_set_financial_values.net_alae_outstanding,
	EXP_set_financial_values.net_alae_incurred,
	-1 AS DEFAULT_ID,
	-- *INF*: :LKP.LKP_CALENDER_DIM(TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'))
	LKP_CALENDER_DIM_TO_DATE_01_01_1800_00_00_00_MM_DD_YYYY_HH24_MI_SS.clndr_id AS DEFAULT_DATE_ID,
	'N/A' AS DEFAULT_STRING,
	0 AS DEFAULT_AMOUNT,
	SYSDATE AS SYSTEM_DATE,
	-- *INF*: IIF(ISNULL(lkp_claim_loss_trans_fact_id),'NEW',
	-- IIF(
	-- lkp_claim_occurrence_dim_id <> claim_occurrence_dim_id_out  OR
	-- lkp_claim_occurrence_dim_hist_id <> claim_occurrence_dim_hist_id_out  OR
	-- lkp_claimant_dim_id  <> claimant_dim_id_out  OR
	-- lkp_claimant_dim_hist_id  <> claimant_dim_hist_id_out  OR
	-- lkp_claimant_cov_dim_id  <> claimant_cov_dim_id_out  OR
	-- lkp_claimant_cov_dim_hist_id  <> claimant_cov_dim_hist_id_out  OR
	-- lkp_cov_dim_id  <> cov_dim_id_out  OR
	-- lkp_cov_dim_hist_id  <> cov_dim_hist_id_out  OR
	-- lkp_claim_trans_type_dim_id  <> claim_trans_type_dim_id_out  OR
	-- lkp_claim_financial_type_dim_id  <> claim_financial_type_dim_id_out  OR
	-- lkp_claim_rep_dim_prim_claim_rep_id <> claim_rep_prim_claim_rep_dim_id_out  OR
	-- lkp_claim_rep_dim_prim_claim_rep_hist_id  <> claim_rep_prim_claim_rep_dim_hist_id_out  OR
	-- lkp_claim_rep_dim_examiner_id  <> claim_rep_dim_examiner_id_out  OR
	-- lkp_claim_rep_dim_examiner_hist_id  <> claim_rep_dim_examiner_hist_id_out  OR
	-- lkp_claim_rep_dim_prim_litigation_handler_id  <> claim_rep_dim_prim_litigation_handler_id_out  OR
	-- lkp_claim_rep_dim_prim_litigation_handler_hist_id  <> claim_rep_dim_prim_litigation_handler_hist_id_out  OR
	-- lkp_claim_rep_dim_trans_entry_oper_id  <> payment_entry_operator_id_out OR
	-- lkp_claim_rep_dim_trans_entry_oper_hist_id  <> payment_entry_operator_hist_id_out  OR
	-- lkp_claim_rep_dim_claim_created_by_id  <> claim_created_by_dim_id OR
	-- lkp_pol_key_dim_id  <> pol_key_dim_id_out  OR
	-- lkp_pol_key_dim_hist_id  <> pol_key_dim_hist_id_out  OR
	-- lkp_agency_dim_id  <> agency_dim_id_out  OR
	-- lkp_agency_dim_hist_id  <> agency_dim_hist_id_out  OR
	-- lkp_claim_pay_dim_id  <> claim_payment_dim_id_out  OR
	-- lkp_claim_pay_dim_hist_id  <> claim_payment_dim_hist_id_out  OR
	-- lkp_claim_pay_ctgry_type_dim_id  <> claim_pay_ctgry_type_dim_id_out  OR
	-- lkp_claim_pay_ctgry_type_dim_hist_id  <> claim_pay_ctgry_type_dim_hist_id_out  OR
	-- lkp_claim_master_1099_list_dim_id  <> claim_master_1099_list_dim_id  OR
	-- lkp_claim_trans_date_id  <> claim_trans_date_id  OR
	-- lkp_claim_trans_reprocess_date_id  <> reprocess_date_id  OR
	-- lkp_claim_loss_date_id  <> claim_loss_date_id  OR
	-- lkp_claim_discovery_date_id  <> claim_discovery_date_id  OR
	-- lkp_claim_scripted_date_id  <> claim_scripted_date_id  OR
	-- lkp_source_claim_rpted_date_id  <> source_claim_rpted_date_id  OR
	-- lkp_claim_rpted_date_id  <> claim_occurrence_rpted_date_id  OR
	-- lkp_claim_open_date_id  <> claim_open_date_id  OR
	-- lkp_claim_close_date_id  <> claim_close_date_id  OR
	-- lkp_claim_reopen_date_id  <> claim_reopen_date_id  OR
	-- lkp_claim_closed_after_reopen_date_id  <> claim_closed_after_reopen_date_id  OR
	-- lkp_claim_notice_only_date_id  <> claim_notice_only_date_id  OR
	-- lkp_claim_cat_start_date_id  <> claim_cat_start_date_id  OR
	-- lkp_claim_cat_end_date_id  <> claim_cat_end_date_id  OR
	-- lkp_claim_rep_assigned_date_id  <> claim_rep_assigned_date_id  OR
	-- lkp_claim_rep_unassigned_date_id  <> claim_rep_unassigned_date_id  OR
	-- lkp_pol_eff_date_id  <> pol_eff_date_id  OR
	-- lkp_pol_exp_date_id  <> pol_exp_date_id  OR
	-- ROUND(abs(lkp_trans_amt  -  trans_amt ),2) > .001 OR
	-- ROUND(abs(lkp_trans_hist_amt - trans_hist_amt),2) >.001  OR
	-- lkp_claim_case_dim_id <> claim_case_dim_id_out OR
	-- lkp_claim_case_dim_hist_id <> claim_case_dim_hist_id_out OR
	-- lkp_claim_subrogation_dim_id <> claim_subrogation_dim_id_out OR
	-- lkp_claim_subrogation_pay_start_date_id <> pay_start_date_id OR
	-- lkp_claim_subrogation_closure_date_id <> closure_date_id OR
	-- lkp_claim_subrogation_referred_to_subrogation_date_id <> referred_to_subrogation_date_id OR
	-- LTRIM(RTRIM(lkp_tax_id))  <>LTRIM(RTRIM(tax_id))  OR
	-- ROUND(ABS(lkp_direct_loss_paid_excluding_recoveries - direct_loss_paid_excluding_recoveries),2) >0.01  OR
	-- ROUND(ABS(lkp_direct_loss_outstanding_excluding_recoveries - direct_loss_outstanding_excluding_recoveries),2) >0.01   OR
	-- ROUND(ABS(lkp_direct_loss_incurred_excluding_recoveries - direct_loss_incurred_excluding_recoveries),2) >0.01   OR
	-- ROUND(ABS(lkp_direct_alae_paid_excluding_recoveries  - direct_alae_paid_excluding_recoveries),2) >0.01 OR
	-- ROUND(ABS(lkp_direct_alae_outstanding_excluding_recoveries - direct_alae_outstanding_excluding_recoveries),2) >0.01  OR
	-- ROUND(ABS(lkp_direct_alae_incurred_excluding_recoveries - direct_alae_incurred_excluding_recoveries),2) >0.01  OR
	-- ROUND(ABS(lkp_direct_subrogation_paid - direct_subrogation_paid),2) >0.01 OR
	-- ROUND(ABS(lkp_direct_subrogation_outstanding - direct_subrogation_outstanding),2) >0.01 OR
	-- ROUND(ABS(lkp_direct_subrogation_incurred - direct_subrogation_incurred),2) >0.01 OR
	-- ROUND(ABS(lkp_direct_salvage_paid - direct_salvage_paid),2) >0.01 OR
	-- ROUND(ABS(lkp_direct_salvage_outstanding - direct_salvage_outstanding),2) >0.01 OR
	-- ROUND(ABS(lkp_direct_salvage_incurred - direct_salvage_incurred),2) >0.01 OR
	-- ROUND(ABS(lkp_direct_other_recovery_paid - direct_other_recovery_paid),2) >0.01 OR
	-- ROUND(ABS(lkp_direct_other_recovery_outstanding - direct_other_recovery_outstanding),2) >0.01  OR
	-- ROUND(ABS(lkp_direct_other_recovery_incurred - direct_other_recovery_incurred),2) >0.01   OR
	-- ROUND(ABS(lkp_direct_loss_paid_including_recoveries - direct_loss_paid_including_recoveries),2)  >0.01  OR
	-- ROUND(ABS(lkp_direct_loss_outstanding_including_recoveries - direct_loss_outstanding_including_recoveries),2) >0.01  OR
	-- ROUND(ABS(lkp_direct_loss_incurred_including_recoveries - direct_loss_incurred_including_recoveries),2) >0.01 OR
	-- lkp_contract_cust_dim_id <> contract_cust_dim_id_out OR
	-- lkp_contract_cust_dim_hist_id <> contract_cust_dim_hist_id_out  OR
	-- ROUND(ABS(lkp_direct_alae_paid_including_recoveries - direct_alae_paid_including_recoveries),2) > 0.01 OR
	-- ROUND(ABS(lkp_direct_alae_outstanding_including_recoveries -  direct_alae_outstanding_including_recoveries),2) > 0.01 OR
	-- ROUND(ABS(lkp_direct_alae_incurred_including_recoveries - direct_alae_incurred_including_recoveries),2) > 0.01 OR
	-- ROUND(ABS(lkp_direct_other_recovery_loss_paid - direct_other_recovery_loss_paid),2) > 0.01 OR
	-- ROUND(ABS(lkp_direct_other_recovery_loss_outstanding - direct_other_recovery_loss_outstanding),2) > 0.01 OR
	-- ROUND(ABS(lkp_direct_other_recovery_loss_incurred -direct_other_recovery_loss_incurred),2) > 0.01 OR
	-- ROUND(ABS(lkp_direct_other_recovery_alae_paid - direct_other_recovery_alae_paid),2) > 0.01 OR
	-- ROUND(ABS(lkp_direct_other_recovery_alae_outstanding - direct_other_recovery_alae_outstanding),2) > 0.01 OR
	-- ROUND(ABS(lkp_direct_other_recovery_alae_incurred - direct_other_recovery_alae_incurred),2) > 0.01 OR
	-- ROUND(ABS(lkp_total_direct_loss_recovery_paid - total_direct_loss_recovery_paid),2) > 0.01 OR
	-- ROUND(ABS(lkp_total_direct_loss_recovery_outstanding - total_direct_loss_recovery_outstanding),2) > 0.01 OR
	-- ROUND(ABS(lkp_total_direct_loss_recovery_incurred - total_direct_loss_recovery_incurred),2) > 0.01 OR
	-- ROUND(ABS(lkp_net_loss_paid -  net_loss_paid),2) > 0.01 OR
	-- ROUND(ABS(lkp_net_loss_outstanding -  net_loss_outstanding),2) > 0.01 OR
	-- ROUND(ABS(lkp_net_loss_incurred -  net_loss_incurred),2) > 0.01 OR
	-- ROUND(ABS(lkp_net_alae_paid -  net_alae_paid),2) > 0.01 OR
	-- ROUND(ABS(lkp_net_alae_outstanding -  net_alae_outstanding),2) > 0.01 OR
	-- ROUND(ABS(lkp_net_alae_incurred -  net_alae_incurred),2) > 0.01 OR
	-- lkp_strtgc_bus_dvsn_dim_id <> strtgc_bus_dvsn_dim_id OR
	-- lkp_ClaimReserveDimId <> ClaimReserveDimId OR
	-- lkp_ClaimRepresentativeDimFeatureClaimRepresentativeId <> FeatureRepresentativeDimId OR
	-- lkp_FeatureRepresentativeAssignedDateId <> FeatureRepresentativeAssignedDate_id OR
	-- lkp_InsuranceReferenceDimId <>InsuranceReferenceDimId OR lkp_AgencyDimId <>AgencyDimID OR 
	-- lkp_SalesDivisionDimId<>SalesDivisionDimID OR lkp_InsuranceReferenceCoverageDimId<>InsuranceReferenceCoverageDetailDimID OR 
	-- lkp_CoverageDetailDimId<>CoverageDetailDimId
	-- ,
	-- 'UPDATE','NOCHANGE')) 
	IFF(lkp_claim_loss_trans_fact_id IS NULL, 'NEW', IFF(lkp_claim_occurrence_dim_id <> claim_occurrence_dim_id_out OR lkp_claim_occurrence_dim_hist_id <> claim_occurrence_dim_hist_id_out OR lkp_claimant_dim_id <> claimant_dim_id_out OR lkp_claimant_dim_hist_id <> claimant_dim_hist_id_out OR lkp_claimant_cov_dim_id <> claimant_cov_dim_id_out OR lkp_claimant_cov_dim_hist_id <> claimant_cov_dim_hist_id_out OR lkp_cov_dim_id <> cov_dim_id_out OR lkp_cov_dim_hist_id <> cov_dim_hist_id_out OR lkp_claim_trans_type_dim_id <> claim_trans_type_dim_id_out OR lkp_claim_financial_type_dim_id <> claim_financial_type_dim_id_out OR lkp_claim_rep_dim_prim_claim_rep_id <> claim_rep_prim_claim_rep_dim_id_out OR lkp_claim_rep_dim_prim_claim_rep_hist_id <> claim_rep_prim_claim_rep_dim_hist_id_out OR lkp_claim_rep_dim_examiner_id <> claim_rep_dim_examiner_id_out OR lkp_claim_rep_dim_examiner_hist_id <> claim_rep_dim_examiner_hist_id_out OR lkp_claim_rep_dim_prim_litigation_handler_id <> claim_rep_dim_prim_litigation_handler_id_out OR lkp_claim_rep_dim_prim_litigation_handler_hist_id <> claim_rep_dim_prim_litigation_handler_hist_id_out OR lkp_claim_rep_dim_trans_entry_oper_id <> payment_entry_operator_id_out OR lkp_claim_rep_dim_trans_entry_oper_hist_id <> payment_entry_operator_hist_id_out OR lkp_claim_rep_dim_claim_created_by_id <> claim_created_by_dim_id OR lkp_pol_key_dim_id <> pol_key_dim_id_out OR lkp_pol_key_dim_hist_id <> pol_key_dim_hist_id_out OR lkp_agency_dim_id <> agency_dim_id_out OR lkp_agency_dim_hist_id <> agency_dim_hist_id_out OR lkp_claim_pay_dim_id <> claim_payment_dim_id_out OR lkp_claim_pay_dim_hist_id <> claim_payment_dim_hist_id_out OR lkp_claim_pay_ctgry_type_dim_id <> claim_pay_ctgry_type_dim_id_out OR lkp_claim_pay_ctgry_type_dim_hist_id <> claim_pay_ctgry_type_dim_hist_id_out OR lkp_claim_master_1099_list_dim_id <> claim_master_1099_list_dim_id OR lkp_claim_trans_date_id <> claim_trans_date_id OR lkp_claim_trans_reprocess_date_id <> reprocess_date_id OR lkp_claim_loss_date_id <> claim_loss_date_id OR lkp_claim_discovery_date_id <> claim_discovery_date_id OR lkp_claim_scripted_date_id <> claim_scripted_date_id OR lkp_source_claim_rpted_date_id <> source_claim_rpted_date_id OR lkp_claim_rpted_date_id <> claim_occurrence_rpted_date_id OR lkp_claim_open_date_id <> claim_open_date_id OR lkp_claim_close_date_id <> claim_close_date_id OR lkp_claim_reopen_date_id <> claim_reopen_date_id OR lkp_claim_closed_after_reopen_date_id <> claim_closed_after_reopen_date_id OR lkp_claim_notice_only_date_id <> claim_notice_only_date_id OR lkp_claim_cat_start_date_id <> claim_cat_start_date_id OR lkp_claim_cat_end_date_id <> claim_cat_end_date_id OR lkp_claim_rep_assigned_date_id <> claim_rep_assigned_date_id OR lkp_claim_rep_unassigned_date_id <> claim_rep_unassigned_date_id OR lkp_pol_eff_date_id <> pol_eff_date_id OR lkp_pol_exp_date_id <> pol_exp_date_id OR ROUND(abs(lkp_trans_amt - trans_amt), 2) > .001 OR ROUND(abs(lkp_trans_hist_amt - trans_hist_amt), 2) > .001 OR lkp_claim_case_dim_id <> claim_case_dim_id_out OR lkp_claim_case_dim_hist_id <> claim_case_dim_hist_id_out OR lkp_claim_subrogation_dim_id <> claim_subrogation_dim_id_out OR lkp_claim_subrogation_pay_start_date_id <> pay_start_date_id OR lkp_claim_subrogation_closure_date_id <> closure_date_id OR lkp_claim_subrogation_referred_to_subrogation_date_id <> referred_to_subrogation_date_id OR LTRIM(RTRIM(lkp_tax_id)) <> LTRIM(RTRIM(tax_id)) OR ROUND(ABS(lkp_direct_loss_paid_excluding_recoveries - direct_loss_paid_excluding_recoveries), 2) > 0.01 OR ROUND(ABS(lkp_direct_loss_outstanding_excluding_recoveries - direct_loss_outstanding_excluding_recoveries), 2) > 0.01 OR ROUND(ABS(lkp_direct_loss_incurred_excluding_recoveries - direct_loss_incurred_excluding_recoveries), 2) > 0.01 OR ROUND(ABS(lkp_direct_alae_paid_excluding_recoveries - direct_alae_paid_excluding_recoveries), 2) > 0.01 OR ROUND(ABS(lkp_direct_alae_outstanding_excluding_recoveries - direct_alae_outstanding_excluding_recoveries), 2) > 0.01 OR ROUND(ABS(lkp_direct_alae_incurred_excluding_recoveries - direct_alae_incurred_excluding_recoveries), 2) > 0.01 OR ROUND(ABS(lkp_direct_subrogation_paid - direct_subrogation_paid), 2) > 0.01 OR ROUND(ABS(lkp_direct_subrogation_outstanding - direct_subrogation_outstanding), 2) > 0.01 OR ROUND(ABS(lkp_direct_subrogation_incurred - direct_subrogation_incurred), 2) > 0.01 OR ROUND(ABS(lkp_direct_salvage_paid - direct_salvage_paid), 2) > 0.01 OR ROUND(ABS(lkp_direct_salvage_outstanding - direct_salvage_outstanding), 2) > 0.01 OR ROUND(ABS(lkp_direct_salvage_incurred - direct_salvage_incurred), 2) > 0.01 OR ROUND(ABS(lkp_direct_other_recovery_paid - direct_other_recovery_paid), 2) > 0.01 OR ROUND(ABS(lkp_direct_other_recovery_outstanding - direct_other_recovery_outstanding), 2) > 0.01 OR ROUND(ABS(lkp_direct_other_recovery_incurred - direct_other_recovery_incurred), 2) > 0.01 OR ROUND(ABS(lkp_direct_loss_paid_including_recoveries - direct_loss_paid_including_recoveries), 2) > 0.01 OR ROUND(ABS(lkp_direct_loss_outstanding_including_recoveries - direct_loss_outstanding_including_recoveries), 2) > 0.01 OR ROUND(ABS(lkp_direct_loss_incurred_including_recoveries - direct_loss_incurred_including_recoveries), 2) > 0.01 OR lkp_contract_cust_dim_id <> contract_cust_dim_id_out OR lkp_contract_cust_dim_hist_id <> contract_cust_dim_hist_id_out OR ROUND(ABS(lkp_direct_alae_paid_including_recoveries - direct_alae_paid_including_recoveries), 2) > 0.01 OR ROUND(ABS(lkp_direct_alae_outstanding_including_recoveries - direct_alae_outstanding_including_recoveries), 2) > 0.01 OR ROUND(ABS(lkp_direct_alae_incurred_including_recoveries - direct_alae_incurred_including_recoveries), 2) > 0.01 OR ROUND(ABS(lkp_direct_other_recovery_loss_paid - direct_other_recovery_loss_paid), 2) > 0.01 OR ROUND(ABS(lkp_direct_other_recovery_loss_outstanding - direct_other_recovery_loss_outstanding), 2) > 0.01 OR ROUND(ABS(lkp_direct_other_recovery_loss_incurred - direct_other_recovery_loss_incurred), 2) > 0.01 OR ROUND(ABS(lkp_direct_other_recovery_alae_paid - direct_other_recovery_alae_paid), 2) > 0.01 OR ROUND(ABS(lkp_direct_other_recovery_alae_outstanding - direct_other_recovery_alae_outstanding), 2) > 0.01 OR ROUND(ABS(lkp_direct_other_recovery_alae_incurred - direct_other_recovery_alae_incurred), 2) > 0.01 OR ROUND(ABS(lkp_total_direct_loss_recovery_paid - total_direct_loss_recovery_paid), 2) > 0.01 OR ROUND(ABS(lkp_total_direct_loss_recovery_outstanding - total_direct_loss_recovery_outstanding), 2) > 0.01 OR ROUND(ABS(lkp_total_direct_loss_recovery_incurred - total_direct_loss_recovery_incurred), 2) > 0.01 OR ROUND(ABS(lkp_net_loss_paid - net_loss_paid), 2) > 0.01 OR ROUND(ABS(lkp_net_loss_outstanding - net_loss_outstanding), 2) > 0.01 OR ROUND(ABS(lkp_net_loss_incurred - net_loss_incurred), 2) > 0.01 OR ROUND(ABS(lkp_net_alae_paid - net_alae_paid), 2) > 0.01 OR ROUND(ABS(lkp_net_alae_outstanding - net_alae_outstanding), 2) > 0.01 OR ROUND(ABS(lkp_net_alae_incurred - net_alae_incurred), 2) > 0.01 OR lkp_strtgc_bus_dvsn_dim_id <> strtgc_bus_dvsn_dim_id OR lkp_ClaimReserveDimId <> ClaimReserveDimId OR lkp_ClaimRepresentativeDimFeatureClaimRepresentativeId <> FeatureRepresentativeDimId OR lkp_FeatureRepresentativeAssignedDateId <> FeatureRepresentativeAssignedDate_id OR lkp_InsuranceReferenceDimId <> InsuranceReferenceDimId OR lkp_AgencyDimId <> AgencyDimID OR lkp_SalesDivisionDimId <> SalesDivisionDimID OR lkp_InsuranceReferenceCoverageDimId <> InsuranceReferenceCoverageDetailDimID OR lkp_CoverageDetailDimId <> CoverageDetailDimId, 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	EXP_set_financial_values.acct_entered_date_id,
	EXP_set_financial_values.strtgc_bus_dvsn_dim_id,
	EXP_set_financial_values.claim_created_by_dim_id AS claim_created_by_dim_id1,
	EXP_set_default_date_ids.payment_entry_operator_id_out,
	EXP_set_default_date_ids.payment_entry_operator_hist_id_out,
	EXP_set_default_date_ids.InsuranceReferenceDimId,
	EXP_set_default_date_ids.AgencyDimID,
	EXP_set_default_date_ids.SalesDivisionDimID,
	EXP_set_default_date_ids.InsuranceReferenceCoverageDetailDimID,
	EXP_set_default_date_ids.CoverageDetailDimId,
	EXP_set_default_date_ids.ClaimReserveDimId,
	EXP_set_default_date_ids.FeatureRepresentativeDimId,
	EXP_set_default_date_ids.out_FeatureRepresentativeAssignedDate_id AS FeatureRepresentativeAssignedDate_id,
	SYSDATE AS ModifiedDate
	FROM EXP_set_default_date_ids
	 -- Manually join with EXP_set_financial_values
	LEFT JOIN LKP_Claim_Loss_Transaction_Fact
	ON LKP_Claim_Loss_Transaction_Fact.edw_claim_trans_pk_id = EXP_set_financial_values.claim_trans_id
	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TO_DATE_01_01_1800_00_00_00_MM_DD_YYYY_HH24_MI_SS
	ON LKP_CALENDER_DIM_TO_DATE_01_01_1800_00_00_00_MM_DD_YYYY_HH24_MI_SS.clndr_date = TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')

),
RTR_claim_loss_transaction_fact AS (
	SELECT
	lkp_claim_loss_trans_fact_id AS claim_loss_trans_fact_id,
	claim_occurrence_dim_id_out AS claim_occurrence_dim_id,
	claim_occurrence_dim_hist_id_out AS claim_occurrence_dim_hist_id,
	claimant_dim_id_out AS claimant_dim_id,
	claimant_dim_hist_id_out AS claimant_dim_hist_id,
	claimant_cov_dim_id_out AS claimant_cov_dim_id,
	claimant_cov_dim_hist_id_out AS claimant_cov_dim_hist_id,
	cov_dim_id_out AS cov_dim_id,
	cov_dim_hist_id_out AS cov_dim_hist_id,
	claim_trans_type_dim_id_out AS claim_trans_type_dim_id,
	claim_financial_type_dim_id_out AS claim_financial_type_dim_id,
	claim_rep_prim_claim_rep_dim_id_out AS claim_rep_dim_prim_claim_rep_id,
	claim_rep_prim_claim_rep_dim_hist_id_out AS claim_rep_dim_prim_claim_rep_hist_id,
	claim_rep_dim_examiner_id_out,
	claim_rep_dim_examiner_hist_id_out,
	claim_rep_dim_prim_litigation_handler_id_out,
	claim_rep_dim_prim_litigation_handler_hist_id_out,
	claim_created_by_dim_id1 AS claim_created_by_dim_id,
	claim_trans_oper_dim_id,
	pol_key_dim_id_out AS pol_key_dim_id,
	pol_key_dim_hist_id_out AS pol_key_dim_hist_id,
	agency_dim_id_out AS agency_dim_id,
	agency_dim_hist_id_out AS agency_dim_hist_id,
	claim_payment_dim_id_out,
	claim_payment_dim_hist_id_out,
	claim_pay_ctgry_type_dim_id_out,
	claim_pay_ctgry_type_dim_hist_id_out,
	claim_master_1099_list_dim_id,
	claim_case_dim_id_out,
	claim_case_dim_hist_id_out,
	claim_trans_date_id,
	reprocess_date_id,
	claim_loss_date_id,
	claim_discovery_date_id,
	source_claim_rpted_date_id,
	claim_scripted_date_id,
	claim_occurrence_rpted_date_id,
	claim_open_date_id,
	claim_close_date_id,
	claim_reopen_date_id,
	claim_closed_after_reopen_date_id,
	claim_notice_only_date_id,
	claim_cat_start_date_id,
	claim_cat_end_date_id,
	claim_rep_assigned_date_id,
	claim_rep_unassigned_date_id,
	claim_trans_id,
	pol_eff_date_id,
	pol_exp_date_id,
	financial_type_code,
	trans_code,
	trans_amt,
	trans_hist_amt,
	direct_loss_paid_excluding_recoveries,
	direct_loss_outstanding_excluding_recoveries,
	direct_loss_incurred_excluding_recoveries,
	direct_alae_paid_excluding_recoveries,
	direct_alae_outstanding_excluding_recoveries,
	direct_alae_incurred_excluding_recoveries,
	direct_subrogation_paid,
	direct_subrogation_outstanding,
	direct_subrogation_incurred,
	direct_salvage_paid,
	direct_salvage_outstanding,
	direct_salvage_incurred,
	direct_other_recovery_paid,
	direct_other_recovery_outstanding,
	direct_other_recovery_incurred,
	direct_loss_paid_including_recoveries,
	direct_loss_outstanding_including_recoveries,
	direct_loss_incurred_including_recoveries,
	audit_id,
	err_flag_out AS err_flag,
	default_dim_id,
	tax_id,
	changed_flag,
	modified_date,
	claim_created_by_dim_id AS claim_created_by_dim_id4,
	claim_subrogation_dim_id_out,
	referred_to_subrogation_date_id,
	pay_start_date_id,
	closure_date_id,
	contract_cust_dim_id_out,
	contract_cust_dim_hist_id_out,
	direct_other_recovery_loss_outstanding,
	direct_other_recovery_alae_outstanding,
	direct_other_recovery_loss_paid,
	direct_other_recovery_alae_paid,
	direct_other_recovery_loss_incurred,
	direct_loss_outstanding_out,
	direct_loss_incurred_out,
	direct_alae_paid_including_recoveries,
	direct_loss_paid_out,
	direct_alae_outstanding_including_recoveries,
	direct_other_recovery_alae_incurred,
	direct_alae_incurred_including_recoveries,
	total_direct_loss_recovery_paid,
	total_direct_loss_recovery_outstanding,
	total_direct_loss_recovery_incurred,
	net_loss_paid,
	net_loss_outstanding,
	net_loss_incurred,
	net_alae_paid,
	net_alae_outstanding,
	net_alae_incurred,
	DEFAULT_ID,
	DEFAULT_DATE_ID,
	DEFAULT_STRING,
	DEFAULT_AMOUNT,
	SYSTEM_DATE,
	acct_entered_date_id,
	strtgc_bus_dvsn_dim_id,
	payment_entry_operator_id_out,
	payment_entry_operator_hist_id_out,
	InsuranceReferenceDimId,
	AgencyDimID,
	SalesDivisionDimID,
	InsuranceReferenceCoverageDetailDimID,
	CoverageDetailDimId,
	ClaimReserveDimId,
	FeatureRepresentativeDimId,
	FeatureRepresentativeAssignedDate_id,
	ModifiedDate
	FROM EXP_DETECT_CHANGES
),
RTR_claim_loss_transaction_fact_INSERT AS (SELECT * FROM RTR_claim_loss_transaction_fact WHERE changed_flag = 'NEW'

--ISNULL(claim_loss_trans_fact_id)),
RTR_claim_loss_transaction_fact_UPDATE AS (SELECT * FROM RTR_claim_loss_transaction_fact WHERE changed_flag ='UPDATE'),
UPD_claim_loss_transaction_fact_insert AS (
	SELECT
	claim_occurrence_dim_id AS claim_occurrence_dim_id1, 
	claim_occurrence_dim_hist_id AS claim_occurrence_dim_hist_id1, 
	claimant_dim_id AS claimant_dim_id1, 
	claimant_dim_hist_id AS claimant_dim_hist_id1, 
	claimant_cov_dim_id AS claimant_cov_dim_id1, 
	claimant_cov_dim_hist_id AS claimant_cov_dim_hist_id1, 
	cov_dim_id AS cov_dim_id1, 
	cov_dim_hist_id AS cov_dim_hist_id1, 
	claim_trans_type_dim_id AS claim_trans_type_dim_id1, 
	claim_financial_type_dim_id AS claim_financial_type_dim_id1, 
	claim_rep_dim_prim_claim_rep_id, 
	claim_rep_dim_prim_claim_rep_hist_id, 
	claim_rep_dim_examiner_id_out AS claim_rep_dim_examiner_id_out1, 
	claim_rep_dim_examiner_hist_id_out AS claim_rep_dim_examiner_hist_id_out1, 
	claim_rep_dim_prim_litigation_handler_id_out AS claim_rep_dim_prim_litigation_handler_id_out1, 
	claim_rep_dim_prim_litigation_handler_hist_id_out AS claim_rep_dim_prim_litigation_handler_hist_id_out1, 
	pol_key_dim_id AS pol_key_dim_id1, 
	pol_key_dim_hist_id AS pol_key_dim_hist_id1, 
	claim_created_by_dim_id AS claim_created_by_dim_id1, 
	claim_trans_oper_dim_id AS claim_trans_oper_dim_id1, 
	claim_trans_date_id AS claim_trans_date_id1, 
	reprocess_date_id AS reprocess_date_id1, 
	claim_loss_date_id AS claim_loss_date_id1, 
	claim_discovery_date_id AS claim_discovery_date_id1, 
	source_claim_rpted_date_id, 
	claim_scripted_date_id, 
	claim_occurrence_rpted_date_id AS claim_occurrence_rpted_date_id1, 
	claim_open_date_id AS claim_open_date_id1, 
	claim_close_date_id AS claim_close_date_id1, 
	claim_reopen_date_id AS claim_reopen_date_id1, 
	claim_closed_after_reopen_date_id AS claim_closed_after_reopen_date_id1, 
	claim_notice_only_date_id AS claim_notice_only_date_id1, 
	claim_cat_start_date_id AS claim_cat_start_date_id1, 
	claim_cat_end_date_id AS claim_cat_end_date_id1, 
	claim_rep_assigned_date_id AS claim_rep_assigned_date_id1, 
	claim_rep_unassigned_date_id AS claim_rep_unassigned_date_id1, 
	claim_trans_id AS claim_trans_id1, 
	pol_eff_date_id AS pol_eff_date_id1, 
	pol_exp_date_id AS pol_exp_date_id1, 
	financial_type_code AS financial_type_code1, 
	trans_code AS trans_code1, 
	trans_amt AS trans_amt1, 
	trans_hist_amt AS trans_hist_amt1, 
	direct_loss_paid_excluding_recoveries, 
	direct_loss_outstanding_excluding_recoveries, 
	direct_loss_incurred_excluding_recoveries, 
	direct_alae_paid_excluding_recoveries, 
	direct_alae_outstanding_excluding_recoveries, 
	direct_alae_incurred_excluding_recoveries, 
	direct_subrogation_paid, 
	direct_subrogation_outstanding, 
	direct_subrogation_incurred, 
	direct_salvage_paid, 
	direct_salvage_outstanding, 
	direct_salvage_incurred, 
	direct_other_recovery_paid, 
	direct_other_recovery_outstanding, 
	direct_other_recovery_incurred, 
	direct_loss_paid_including_recoveries, 
	direct_loss_outstanding_including_recoveries, 
	direct_loss_incurred_including_recoveries, 
	audit_id AS audit_id1, 
	agency_dim_id AS agency_dim_id1, 
	agency_dim_hist_id AS agency_dim_hist_id1, 
	err_flag AS err_flag1, 
	default_dim_id AS default_dim_id1, 
	claim_payment_dim_id_out AS claim_payment_dim_id_out1, 
	claim_payment_dim_hist_id_out AS claim_payment_dim_hist_id_out1, 
	tax_id AS tax_id1, 
	claim_master_1099_list_dim_id AS claim_master_1099_list_dim_id1, 
	claim_pay_ctgry_type_dim_id_out AS claim_pay_ctgry_type_dim_id_out1, 
	claim_pay_ctgry_type_dim_hist_id_out AS claim_pay_ctgry_type_dim_hist_id_out1, 
	claim_created_by_dim_id4 AS claim_created_by_dim_id, 
	claim_case_dim_id_out AS claim_case_dim_id_out1, 
	claim_case_dim_hist_id_out AS claim_case_dim_hist_id_out1, 
	claim_subrogation_dim_id_out AS claim_subrogation_dim_id_out1, 
	referred_to_subrogation_date_id AS referred_to_subrogation_date_id1, 
	pay_start_date_id AS pay_start_date_id1, 
	closure_date_id AS closure_date_id1, 
	contract_cust_dim_id_out, 
	contract_cust_dim_hist_id_out, 
	direct_other_recovery_loss_outstanding, 
	direct_other_recovery_alae_outstanding, 
	direct_other_recovery_loss_paid, 
	direct_other_recovery_alae_paid, 
	direct_other_recovery_loss_incurred, 
	direct_loss_outstanding_out AS direct_loss_outstanding_out1, 
	direct_loss_incurred_out AS direct_loss_incurred_out1, 
	direct_alae_paid_including_recoveries, 
	direct_loss_paid_out AS direct_loss_paid_out1, 
	direct_alae_outstanding_including_recoveries, 
	direct_other_recovery_alae_incurred, 
	direct_alae_incurred_including_recoveries, 
	total_direct_loss_recovery_paid, 
	total_direct_loss_recovery_outstanding, 
	total_direct_loss_recovery_incurred, 
	net_loss_paid AS net_loss_paid1, 
	net_loss_outstanding AS net_loss_outstanding1, 
	net_loss_incurred AS net_loss_incurred1, 
	net_alae_paid AS net_alae_paid1, 
	net_alae_outstanding AS net_alae_outstanding1, 
	net_alae_incurred AS net_alae_incurred1, 
	DEFAULT_ID AS DEFAULT_ID1, 
	DEFAULT_DATE_ID AS DEFAULT_DATE_ID1, 
	DEFAULT_STRING AS DEFAULT_STRING1, 
	DEFAULT_AMOUNT AS DEFAULT_AMOUNT1, 
	SYSTEM_DATE AS CREATE_DATE, 
	acct_entered_date_id AS acct_entered_date_id1, 
	strtgc_bus_dvsn_dim_id AS strtgc_bus_dvsn_dim_id1, 
	payment_entry_operator_id_out AS payment_entry_operator_id_out1, 
	payment_entry_operator_hist_id_out AS payment_entry_operator_hist_id_out1, 
	InsuranceReferenceDimId AS InsuranceReferenceDimId1, 
	AgencyDimID AS AgencyDimID1, 
	SalesDivisionDimID AS SalesDivisionDimID1, 
	InsuranceReferenceCoverageDetailDimID AS InsuranceReferenceCoverageDetailDimID1, 
	CoverageDetailDimId AS CoverageDetailDimId1, 
	ClaimReserveDimId AS ClaimReserveDimId1, 
	FeatureRepresentativeDimId AS FeatureRepresentativeDimId1, 
	FeatureRepresentativeAssignedDate_id AS FeatureRepresentativeAssignedDate_id1, 
	ModifiedDate AS ModifiedDate1
	FROM RTR_claim_loss_transaction_fact_INSERT
),
claim_loss_transaction_fact_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_loss_transaction_fact
	(err_flag, audit_id, edw_claim_trans_pk_id, edw_claim_reins_trans_pk_id, claim_occurrence_dim_id, claim_occurrence_dim_hist_id, claimant_dim_id, claimant_dim_hist_id, claimant_cov_dim_id, claimant_cov_dim_hist_id, cov_dim_id, cov_dim_hist_id, claim_trans_type_dim_id, claim_financial_type_dim_id, reins_cov_dim_id, reins_cov_dim_hist_id, claim_rep_dim_prim_claim_rep_id, claim_rep_dim_prim_claim_rep_hist_id, claim_rep_dim_examiner_id, claim_rep_dim_examiner_hist_id, claim_rep_dim_prim_litigation_handler_id, claim_rep_dim_prim_litigation_handler_hist_id, claim_rep_dim_trans_entry_oper_id, claim_rep_dim_trans_entry_oper_hist_id, claim_rep_dim_claim_created_by_id, pol_dim_id, pol_dim_hist_id, agency_dim_id, agency_dim_hist_id, claim_pay_dim_id, claim_pay_dim_hist_id, claim_pay_ctgry_type_dim_id, claim_pay_ctgry_type_dim_hist_id, claim_case_dim_id, claim_case_dim_hist_id, contract_cust_dim_id, contract_cust_dim_hist_id, claim_master_1099_list_dim_id, claim_subrogation_dim_id, claim_trans_date_id, claim_trans_reprocess_date_id, claim_loss_date_id, claim_discovery_date_id, claim_scripted_date_id, source_claim_rpted_date_id, claim_rpted_date_id, claim_open_date_id, claim_close_date_id, claim_reopen_date_id, claim_closed_after_reopen_date_id, claim_notice_only_date_id, claim_cat_start_date_id, claim_cat_end_date_id, claim_rep_assigned_date_id, claim_rep_unassigned_date_id, pol_eff_date_id, pol_exp_date_id, claim_subrogation_referred_to_subrogation_date_id, claim_subrogation_pay_start_date_id, claim_subrogation_closure_date_id, acct_entered_date_id, trans_amt, trans_hist_amt, tax_id, direct_loss_paid_excluding_recoveries, direct_loss_outstanding_excluding_recoveries, direct_loss_incurred_excluding_recoveries, direct_alae_paid_excluding_recoveries, direct_alae_outstanding_excluding_recoveries, direct_alae_incurred_excluding_recoveries, direct_loss_paid_including_recoveries, direct_loss_outstanding_including_recoveries, direct_loss_incurred_including_recoveries, direct_alae_paid_including_recoveries, direct_alae_outstanding_including_recoveries, direct_alae_incurred_including_recoveries, direct_subrogation_paid, direct_subrogation_outstanding, direct_subrogation_incurred, direct_salvage_paid, direct_salvage_outstanding, direct_salvage_incurred, direct_other_recovery_loss_paid, direct_other_recovery_loss_outstanding, direct_other_recovery_loss_incurred, direct_other_recovery_alae_paid, direct_other_recovery_alae_outstanding, direct_other_recovery_alae_incurred, total_direct_loss_recovery_paid, total_direct_loss_recovery_outstanding, total_direct_loss_recovery_incurred, direct_other_recovery_paid, direct_other_recovery_outstanding, direct_other_recovery_incurred, ceded_loss_paid, ceded_loss_outstanding, ceded_loss_incurred, ceded_alae_paid, ceded_alae_outstanding, ceded_alae_incurred, ceded_salvage_paid, ceded_subrogation_paid, ceded_other_recovery_loss_paid, ceded_other_recovery_alae_paid, total_ceded_loss_recovery_paid, net_loss_paid, net_loss_outstanding, net_loss_incurred, net_alae_paid, net_alae_outstanding, net_alae_incurred, asl_dim_id, asl_prdct_code_dim_id, loss_master_dim_id, strtgc_bus_dvsn_dim_id, prdct_code_dim_id, ClaimReserveDimId, ClaimRepresentativeDimFeatureClaimRepresentativeId, FeatureRepresentativeAssignedDateId, InsuranceReferenceDimId, AgencyDimId, SalesDivisionDimId, InsuranceReferenceCoverageDimId, CoverageDetailDimId, ModifiedDate)
	SELECT 
	err_flag1 AS ERR_FLAG, 
	audit_id1 AS AUDIT_ID, 
	claim_trans_id1 AS EDW_CLAIM_TRANS_PK_ID, 
	DEFAULT_ID1 AS EDW_CLAIM_REINS_TRANS_PK_ID, 
	claim_occurrence_dim_id1 AS CLAIM_OCCURRENCE_DIM_ID, 
	claim_occurrence_dim_hist_id1 AS CLAIM_OCCURRENCE_DIM_HIST_ID, 
	claimant_dim_id1 AS CLAIMANT_DIM_ID, 
	claimant_dim_hist_id1 AS CLAIMANT_DIM_HIST_ID, 
	claimant_cov_dim_id1 AS CLAIMANT_COV_DIM_ID, 
	claimant_cov_dim_hist_id1 AS CLAIMANT_COV_DIM_HIST_ID, 
	cov_dim_id1 AS COV_DIM_ID, 
	cov_dim_hist_id1 AS COV_DIM_HIST_ID, 
	claim_trans_type_dim_id1 AS CLAIM_TRANS_TYPE_DIM_ID, 
	claim_financial_type_dim_id1 AS CLAIM_FINANCIAL_TYPE_DIM_ID, 
	DEFAULT_ID1 AS REINS_COV_DIM_ID, 
	DEFAULT_ID1 AS REINS_COV_DIM_HIST_ID, 
	CLAIM_REP_DIM_PRIM_CLAIM_REP_ID, 
	CLAIM_REP_DIM_PRIM_CLAIM_REP_HIST_ID, 
	claim_rep_dim_examiner_id_out1 AS CLAIM_REP_DIM_EXAMINER_ID, 
	claim_rep_dim_examiner_hist_id_out1 AS CLAIM_REP_DIM_EXAMINER_HIST_ID, 
	claim_rep_dim_prim_litigation_handler_id_out1 AS CLAIM_REP_DIM_PRIM_LITIGATION_HANDLER_ID, 
	claim_rep_dim_prim_litigation_handler_hist_id_out1 AS CLAIM_REP_DIM_PRIM_LITIGATION_HANDLER_HIST_ID, 
	payment_entry_operator_id_out1 AS CLAIM_REP_DIM_TRANS_ENTRY_OPER_ID, 
	payment_entry_operator_hist_id_out1 AS CLAIM_REP_DIM_TRANS_ENTRY_OPER_HIST_ID, 
	claim_created_by_dim_id AS CLAIM_REP_DIM_CLAIM_CREATED_BY_ID, 
	pol_key_dim_id1 AS POL_DIM_ID, 
	pol_key_dim_hist_id1 AS POL_DIM_HIST_ID, 
	agency_dim_id1 AS AGENCY_DIM_ID, 
	agency_dim_hist_id1 AS AGENCY_DIM_HIST_ID, 
	claim_payment_dim_id_out1 AS CLAIM_PAY_DIM_ID, 
	claim_payment_dim_hist_id_out1 AS CLAIM_PAY_DIM_HIST_ID, 
	claim_pay_ctgry_type_dim_id_out1 AS CLAIM_PAY_CTGRY_TYPE_DIM_ID, 
	claim_pay_ctgry_type_dim_hist_id_out1 AS CLAIM_PAY_CTGRY_TYPE_DIM_HIST_ID, 
	claim_case_dim_id_out1 AS CLAIM_CASE_DIM_ID, 
	claim_case_dim_hist_id_out1 AS CLAIM_CASE_DIM_HIST_ID, 
	contract_cust_dim_id_out AS CONTRACT_CUST_DIM_ID, 
	contract_cust_dim_hist_id_out AS CONTRACT_CUST_DIM_HIST_ID, 
	claim_master_1099_list_dim_id1 AS CLAIM_MASTER_1099_LIST_DIM_ID, 
	claim_subrogation_dim_id_out1 AS CLAIM_SUBROGATION_DIM_ID, 
	claim_trans_date_id1 AS CLAIM_TRANS_DATE_ID, 
	reprocess_date_id1 AS CLAIM_TRANS_REPROCESS_DATE_ID, 
	claim_loss_date_id1 AS CLAIM_LOSS_DATE_ID, 
	claim_discovery_date_id1 AS CLAIM_DISCOVERY_DATE_ID, 
	CLAIM_SCRIPTED_DATE_ID, 
	SOURCE_CLAIM_RPTED_DATE_ID, 
	claim_occurrence_rpted_date_id1 AS CLAIM_RPTED_DATE_ID, 
	claim_open_date_id1 AS CLAIM_OPEN_DATE_ID, 
	claim_close_date_id1 AS CLAIM_CLOSE_DATE_ID, 
	claim_reopen_date_id1 AS CLAIM_REOPEN_DATE_ID, 
	claim_closed_after_reopen_date_id1 AS CLAIM_CLOSED_AFTER_REOPEN_DATE_ID, 
	claim_notice_only_date_id1 AS CLAIM_NOTICE_ONLY_DATE_ID, 
	claim_cat_start_date_id1 AS CLAIM_CAT_START_DATE_ID, 
	claim_cat_end_date_id1 AS CLAIM_CAT_END_DATE_ID, 
	claim_rep_assigned_date_id1 AS CLAIM_REP_ASSIGNED_DATE_ID, 
	claim_rep_unassigned_date_id1 AS CLAIM_REP_UNASSIGNED_DATE_ID, 
	pol_eff_date_id1 AS POL_EFF_DATE_ID, 
	pol_exp_date_id1 AS POL_EXP_DATE_ID, 
	referred_to_subrogation_date_id1 AS CLAIM_SUBROGATION_REFERRED_TO_SUBROGATION_DATE_ID, 
	pay_start_date_id1 AS CLAIM_SUBROGATION_PAY_START_DATE_ID, 
	closure_date_id1 AS CLAIM_SUBROGATION_CLOSURE_DATE_ID, 
	acct_entered_date_id1 AS ACCT_ENTERED_DATE_ID, 
	trans_amt1 AS TRANS_AMT, 
	trans_hist_amt1 AS TRANS_HIST_AMT, 
	tax_id1 AS TAX_ID, 
	DIRECT_LOSS_PAID_EXCLUDING_RECOVERIES, 
	DIRECT_LOSS_OUTSTANDING_EXCLUDING_RECOVERIES, 
	DIRECT_LOSS_INCURRED_EXCLUDING_RECOVERIES, 
	DIRECT_ALAE_PAID_EXCLUDING_RECOVERIES, 
	DIRECT_ALAE_OUTSTANDING_EXCLUDING_RECOVERIES, 
	DIRECT_ALAE_INCURRED_EXCLUDING_RECOVERIES, 
	DIRECT_LOSS_PAID_INCLUDING_RECOVERIES, 
	DIRECT_LOSS_OUTSTANDING_INCLUDING_RECOVERIES, 
	DIRECT_LOSS_INCURRED_INCLUDING_RECOVERIES, 
	DIRECT_ALAE_PAID_INCLUDING_RECOVERIES, 
	DIRECT_ALAE_OUTSTANDING_INCLUDING_RECOVERIES, 
	DIRECT_ALAE_INCURRED_INCLUDING_RECOVERIES, 
	DIRECT_SUBROGATION_PAID, 
	DIRECT_SUBROGATION_OUTSTANDING, 
	DIRECT_SUBROGATION_INCURRED, 
	DIRECT_SALVAGE_PAID, 
	DIRECT_SALVAGE_OUTSTANDING, 
	DIRECT_SALVAGE_INCURRED, 
	DIRECT_OTHER_RECOVERY_LOSS_PAID, 
	DIRECT_OTHER_RECOVERY_LOSS_OUTSTANDING, 
	DIRECT_OTHER_RECOVERY_LOSS_INCURRED, 
	DIRECT_OTHER_RECOVERY_ALAE_PAID, 
	DIRECT_OTHER_RECOVERY_ALAE_OUTSTANDING, 
	DIRECT_OTHER_RECOVERY_ALAE_INCURRED, 
	TOTAL_DIRECT_LOSS_RECOVERY_PAID, 
	TOTAL_DIRECT_LOSS_RECOVERY_OUTSTANDING, 
	TOTAL_DIRECT_LOSS_RECOVERY_INCURRED, 
	DIRECT_OTHER_RECOVERY_PAID, 
	DIRECT_OTHER_RECOVERY_OUTSTANDING, 
	DIRECT_OTHER_RECOVERY_INCURRED, 
	DEFAULT_AMOUNT1 AS CEDED_LOSS_PAID, 
	DEFAULT_AMOUNT1 AS CEDED_LOSS_OUTSTANDING, 
	DEFAULT_AMOUNT1 AS CEDED_LOSS_INCURRED, 
	DEFAULT_AMOUNT1 AS CEDED_ALAE_PAID, 
	DEFAULT_AMOUNT1 AS CEDED_ALAE_OUTSTANDING, 
	DEFAULT_AMOUNT1 AS CEDED_ALAE_INCURRED, 
	DEFAULT_AMOUNT1 AS CEDED_SALVAGE_PAID, 
	DEFAULT_AMOUNT1 AS CEDED_SUBROGATION_PAID, 
	DEFAULT_AMOUNT1 AS CEDED_OTHER_RECOVERY_LOSS_PAID, 
	DEFAULT_AMOUNT1 AS CEDED_OTHER_RECOVERY_ALAE_PAID, 
	DEFAULT_AMOUNT1 AS TOTAL_CEDED_LOSS_RECOVERY_PAID, 
	net_loss_paid1 AS NET_LOSS_PAID, 
	net_loss_outstanding1 AS NET_LOSS_OUTSTANDING, 
	net_loss_incurred1 AS NET_LOSS_INCURRED, 
	net_alae_paid1 AS NET_ALAE_PAID, 
	net_alae_outstanding1 AS NET_ALAE_OUTSTANDING, 
	net_alae_incurred1 AS NET_ALAE_INCURRED, 
	DEFAULT_ID1 AS ASL_DIM_ID, 
	DEFAULT_ID1 AS ASL_PRDCT_CODE_DIM_ID, 
	DEFAULT_ID1 AS LOSS_MASTER_DIM_ID, 
	strtgc_bus_dvsn_dim_id1 AS STRTGC_BUS_DVSN_DIM_ID, 
	DEFAULT_ID1 AS PRDCT_CODE_DIM_ID, 
	ClaimReserveDimId1 AS CLAIMRESERVEDIMID, 
	FeatureRepresentativeDimId1 AS CLAIMREPRESENTATIVEDIMFEATURECLAIMREPRESENTATIVEID, 
	FeatureRepresentativeAssignedDate_id1 AS FEATUREREPRESENTATIVEASSIGNEDDATEID, 
	InsuranceReferenceDimId1 AS INSURANCEREFERENCEDIMID, 
	AgencyDimID1 AS AGENCYDIMID, 
	SalesDivisionDimID1 AS SALESDIVISIONDIMID, 
	InsuranceReferenceCoverageDetailDimID1 AS INSURANCEREFERENCECOVERAGEDIMID, 
	CoverageDetailDimId1 AS COVERAGEDETAILDIMID, 
	ModifiedDate1 AS MODIFIEDDATE
	FROM UPD_claim_loss_transaction_fact_insert
),
UPD_claim_loss_transaction_fact_update AS (
	SELECT
	claim_loss_trans_fact_id AS claim_loss_trans_fact_id2, 
	claim_occurrence_dim_id AS claim_occurrence_dim_id2, 
	claim_occurrence_dim_hist_id AS claim_occurrence_dim_hist_id2, 
	claimant_dim_id AS claimant_dim_id2, 
	claimant_dim_hist_id AS claimant_dim_hist_id2, 
	claimant_cov_dim_id AS claimant_cov_dim_id2, 
	claimant_cov_dim_hist_id AS claimant_cov_dim_hist_id2, 
	cov_dim_id AS cov_dim_id2, 
	cov_dim_hist_id AS cov_dim_hist_id2, 
	claim_trans_type_dim_id AS claim_trans_type_dim_id2, 
	claim_financial_type_dim_id AS claim_financial_type_dim_id2, 
	claim_rep_dim_prim_claim_rep_id, 
	claim_rep_dim_prim_claim_rep_hist_id, 
	claim_rep_dim_examiner_id_out AS claim_rep_dim_examiner_id_out2, 
	claim_rep_dim_examiner_hist_id_out AS claim_rep_dim_examiner_hist_id_out2, 
	claim_rep_dim_prim_litigation_handler_id_out AS claim_rep_dim_prim_litigation_handler_id_out2, 
	claim_rep_dim_prim_litigation_handler_hist_id_out AS claim_rep_dim_prim_litigation_handler_hist_id_out2, 
	claim_payment_dim_id_out AS claim_payment_dim_id_out2, 
	claim_payment_dim_hist_id_out AS claim_payment_dim_hist_id_out2, 
	pol_key_dim_id AS pol_key_dim_id2, 
	pol_key_dim_hist_id AS pol_key_dim_hist_id2, 
	agency_dim_id AS agency_dim_id2, 
	agency_dim_hist_id AS agency_dim_hist_id2, 
	claim_pay_ctgry_type_dim_id_out AS claim_pay_ctgry_type_dim_id_out2, 
	claim_pay_ctgry_type_dim_hist_id_out AS claim_pay_ctgry_type_dim_hist_id_out2, 
	claim_master_1099_list_dim_id AS claim_master_1099_list_dim_id2, 
	claim_created_by_dim_id AS claim_created_by_dim_id2, 
	claim_trans_oper_dim_id AS claim_trans_oper_dim_id2, 
	claim_trans_date_id AS claim_trans_date_id2, 
	reprocess_date_id AS reprocess_date_id2, 
	claim_loss_date_id AS claim_loss_date_id2, 
	claim_discovery_date_id AS claim_discovery_date_id2, 
	source_claim_rpted_date_id, 
	claim_scripted_date_id, 
	claim_occurrence_rpted_date_id AS claim_occurrence_rpted_date_id2, 
	claim_open_date_id AS claim_open_date_id2, 
	claim_close_date_id AS claim_close_date_id2, 
	claim_reopen_date_id AS claim_reopen_date_id2, 
	claim_closed_after_reopen_date_id AS claim_closed_after_reopen_date_id2, 
	claim_notice_only_date_id, 
	claim_cat_start_date_id AS claim_cat_start_date_id2, 
	claim_cat_end_date_id AS claim_cat_end_date_id2, 
	claim_rep_assigned_date_id AS claim_rep_assigned_date_id2, 
	claim_rep_unassigned_date_id AS claim_rep_unassigned_date_id2, 
	claim_trans_id AS claim_trans_id2, 
	pol_eff_date_id AS pol_eff_date_id2, 
	pol_exp_date_id AS pol_exp_date_id2, 
	financial_type_code AS financial_type_code2, 
	trans_code AS trans_code2, 
	trans_amt AS trans_amt2, 
	trans_hist_amt AS trans_hist_amt2, 
	direct_loss_paid_excluding_recoveries, 
	direct_loss_outstanding_excluding_recoveries, 
	direct_loss_incurred_excluding_recoveries, 
	direct_alae_paid_excluding_recoveries, 
	direct_alae_outstanding_excluding_recoveries, 
	direct_alae_incurred_excluding_recoveries, 
	direct_subrogation_paid, 
	direct_subrogation_outstanding, 
	direct_subrogation_incurred, 
	direct_salvage_paid, 
	direct_salvage_outstanding, 
	direct_salvage_incurred, 
	direct_other_recovery_paid, 
	direct_other_recovery_outstanding, 
	direct_other_recovery_incurred, 
	direct_loss_paid_including_recoveries, 
	direct_loss_outstanding_including_recoveries, 
	direct_loss_incurred_including_recoveries, 
	audit_id AS audit_id3, 
	err_flag AS err_flag2, 
	tax_id AS tax_id2, 
	default_dim_id AS default_dim_id2, 
	modified_date AS modified_date3, 
	claim_created_by_dim_id4 AS claim_created_by_dim_id43, 
	claim_case_dim_id_out AS claim_case_dim_id_out1, 
	claim_case_dim_hist_id_out AS claim_case_dim_hist_id_out1, 
	claim_subrogation_dim_id_out AS claim_subrogation_dim_id_out3, 
	referred_to_subrogation_date_id AS referred_to_subrogation_date_id3, 
	pay_start_date_id AS pay_start_date_id3, 
	closure_date_id AS closure_date_id3, 
	contract_cust_dim_id_out, 
	contract_cust_dim_hist_id_out, 
	direct_other_recovery_loss_outstanding, 
	direct_other_recovery_alae_outstanding, 
	direct_other_recovery_loss_paid, 
	direct_other_recovery_alae_paid, 
	direct_other_recovery_loss_incurred, 
	direct_loss_outstanding_out AS direct_loss_outstanding_out3, 
	direct_loss_incurred_out AS direct_loss_incurred_out3, 
	direct_alae_paid_including_recoveries, 
	direct_loss_paid_out AS direct_loss_paid_out3, 
	direct_alae_outstanding_including_recoveries, 
	direct_other_recovery_alae_incurred, 
	direct_alae_incurred_including_recoveries, 
	total_direct_loss_recovery_paid, 
	total_direct_loss_recovery_outstanding, 
	total_direct_loss_recovery_incurred, 
	net_loss_paid AS net_loss_paid2, 
	net_loss_outstanding AS net_loss_outstanding2, 
	net_loss_incurred AS net_loss_incurred2, 
	net_alae_paid AS net_alae_paid2, 
	net_alae_outstanding AS net_alae_outstanding2, 
	net_alae_incurred AS net_alae_incurred2, 
	DEFAULT_ID AS DEFAULT_ID2, 
	DEFAULT_DATE_ID AS DEFAULT_DATE_ID2, 
	DEFAULT_STRING AS DEFAULT_STRING2, 
	DEFAULT_AMOUNT AS DEFAULT_AMOUNT2, 
	SYSTEM_DATE AS SYSTEM_DATE3, 
	acct_entered_date_id AS acct_entered_date_id3, 
	strtgc_bus_dvsn_dim_id AS strtgc_bus_dvsn_dim_id3, 
	payment_entry_operator_id_out AS payment_entry_operator_id_out3, 
	payment_entry_operator_hist_id_out AS payment_entry_operator_hist_id_out3, 
	InsuranceReferenceDimId AS InsuranceReferenceDimId3, 
	AgencyDimID AS AgencyDimID3, 
	SalesDivisionDimID AS SalesDivisionDimID3, 
	InsuranceReferenceCoverageDetailDimID AS InsuranceReferenceCoverageDetailDimID3, 
	CoverageDetailDimId AS CoverageDetailDimId3, 
	ClaimReserveDimId AS ClaimReserveDimId3, 
	FeatureRepresentativeDimId AS FeatureRepresentativeDimId3, 
	FeatureRepresentativeAssignedDate_id AS FeatureRepresentativeAssignedDate_id3, 
	ModifiedDate AS ModifiedDate3
	FROM RTR_claim_loss_transaction_fact_UPDATE
),
claim_loss_transaction_fact_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_loss_transaction_fact AS T
	USING UPD_claim_loss_transaction_fact_update AS S
	ON T.claim_loss_trans_fact_id = S.claim_loss_trans_fact_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.err_flag = S.err_flag2, T.edw_claim_trans_pk_id = S.claim_trans_id2, T.claim_occurrence_dim_id = S.claim_occurrence_dim_id2, T.claim_occurrence_dim_hist_id = S.claim_occurrence_dim_hist_id2, T.claimant_dim_id = S.claimant_dim_id2, T.claimant_dim_hist_id = S.claimant_dim_hist_id2, T.claimant_cov_dim_id = S.claimant_cov_dim_id2, T.claimant_cov_dim_hist_id = S.claimant_cov_dim_hist_id2, T.cov_dim_id = S.cov_dim_id2, T.cov_dim_hist_id = S.cov_dim_hist_id2, T.claim_trans_type_dim_id = S.claim_trans_type_dim_id2, T.claim_financial_type_dim_id = S.claim_financial_type_dim_id2, T.claim_rep_dim_prim_claim_rep_id = S.claim_rep_dim_prim_claim_rep_id, T.claim_rep_dim_prim_claim_rep_hist_id = S.claim_rep_dim_prim_claim_rep_hist_id, T.claim_rep_dim_examiner_id = S.claim_rep_dim_examiner_id_out2, T.claim_rep_dim_examiner_hist_id = S.claim_rep_dim_examiner_hist_id_out2, T.claim_rep_dim_prim_litigation_handler_id = S.claim_rep_dim_prim_litigation_handler_id_out2, T.claim_rep_dim_prim_litigation_handler_hist_id = S.claim_rep_dim_prim_litigation_handler_hist_id_out2, T.claim_rep_dim_trans_entry_oper_id = S.payment_entry_operator_id_out3, T.claim_rep_dim_trans_entry_oper_hist_id = S.payment_entry_operator_hist_id_out3, T.claim_rep_dim_claim_created_by_id = S.claim_created_by_dim_id43, T.pol_dim_id = S.pol_key_dim_id2, T.pol_dim_hist_id = S.pol_key_dim_hist_id2, T.agency_dim_id = S.agency_dim_id2, T.agency_dim_hist_id = S.agency_dim_hist_id2, T.claim_pay_dim_id = S.claim_payment_dim_id_out2, T.claim_pay_dim_hist_id = S.claim_payment_dim_hist_id_out2, T.claim_pay_ctgry_type_dim_id = S.claim_pay_ctgry_type_dim_id_out2, T.claim_pay_ctgry_type_dim_hist_id = S.claim_pay_ctgry_type_dim_hist_id_out2, T.claim_case_dim_id = S.claim_case_dim_id_out1, T.claim_case_dim_hist_id = S.claim_case_dim_hist_id_out1, T.contract_cust_dim_id = S.contract_cust_dim_id_out, T.contract_cust_dim_hist_id = S.contract_cust_dim_hist_id_out, T.claim_master_1099_list_dim_id = S.claim_master_1099_list_dim_id2, T.claim_subrogation_dim_id = S.claim_subrogation_dim_id_out3, T.claim_trans_date_id = S.claim_trans_date_id2, T.claim_trans_reprocess_date_id = S.reprocess_date_id2, T.claim_loss_date_id = S.claim_loss_date_id2, T.claim_discovery_date_id = S.claim_discovery_date_id2, T.claim_scripted_date_id = S.claim_scripted_date_id, T.source_claim_rpted_date_id = S.source_claim_rpted_date_id, T.claim_rpted_date_id = S.claim_occurrence_rpted_date_id2, T.claim_open_date_id = S.claim_open_date_id2, T.claim_close_date_id = S.claim_close_date_id2, T.claim_reopen_date_id = S.claim_reopen_date_id2, T.claim_closed_after_reopen_date_id = S.claim_closed_after_reopen_date_id2, T.claim_notice_only_date_id = S.claim_notice_only_date_id, T.claim_cat_start_date_id = S.claim_cat_start_date_id2, T.claim_cat_end_date_id = S.claim_cat_end_date_id2, T.claim_rep_assigned_date_id = S.claim_rep_assigned_date_id2, T.claim_rep_unassigned_date_id = S.claim_rep_unassigned_date_id2, T.pol_eff_date_id = S.pol_eff_date_id2, T.pol_exp_date_id = S.pol_exp_date_id2, T.claim_subrogation_referred_to_subrogation_date_id = S.referred_to_subrogation_date_id3, T.claim_subrogation_pay_start_date_id = S.pay_start_date_id3, T.claim_subrogation_closure_date_id = S.closure_date_id3, T.acct_entered_date_id = S.acct_entered_date_id3, T.trans_amt = S.trans_amt2, T.trans_hist_amt = S.trans_hist_amt2, T.tax_id = S.tax_id2, T.direct_loss_paid_excluding_recoveries = S.direct_loss_paid_excluding_recoveries, T.direct_loss_outstanding_excluding_recoveries = S.direct_loss_outstanding_excluding_recoveries, T.direct_loss_incurred_excluding_recoveries = S.direct_loss_incurred_excluding_recoveries, T.direct_alae_paid_excluding_recoveries = S.direct_alae_paid_excluding_recoveries, T.direct_alae_outstanding_excluding_recoveries = S.direct_alae_outstanding_excluding_recoveries, T.direct_alae_incurred_excluding_recoveries = S.direct_alae_incurred_excluding_recoveries, T.direct_loss_paid_including_recoveries = S.direct_loss_paid_including_recoveries, T.direct_loss_outstanding_including_recoveries = S.direct_loss_outstanding_including_recoveries, T.direct_loss_incurred_including_recoveries = S.direct_loss_incurred_including_recoveries, T.direct_alae_paid_including_recoveries = S.direct_alae_paid_including_recoveries, T.direct_alae_outstanding_including_recoveries = S.direct_alae_outstanding_including_recoveries, T.direct_alae_incurred_including_recoveries = S.direct_alae_incurred_including_recoveries, T.direct_subrogation_paid = S.direct_subrogation_paid, T.direct_subrogation_outstanding = S.direct_subrogation_outstanding, T.direct_subrogation_incurred = S.direct_subrogation_incurred, T.direct_salvage_paid = S.direct_salvage_paid, T.direct_salvage_outstanding = S.direct_salvage_outstanding, T.direct_salvage_incurred = S.direct_salvage_incurred, T.direct_other_recovery_loss_paid = S.direct_other_recovery_loss_paid, T.direct_other_recovery_loss_outstanding = S.direct_other_recovery_loss_outstanding, T.direct_other_recovery_loss_incurred = S.direct_other_recovery_loss_incurred, T.direct_other_recovery_alae_paid = S.direct_other_recovery_alae_paid, T.direct_other_recovery_alae_outstanding = S.direct_other_recovery_alae_outstanding, T.direct_other_recovery_alae_incurred = S.direct_other_recovery_alae_incurred, T.total_direct_loss_recovery_paid = S.total_direct_loss_recovery_paid, T.total_direct_loss_recovery_outstanding = S.total_direct_loss_recovery_outstanding, T.total_direct_loss_recovery_incurred = S.total_direct_loss_recovery_incurred, T.direct_other_recovery_paid = S.direct_other_recovery_paid, T.direct_other_recovery_outstanding = S.direct_other_recovery_outstanding, T.direct_other_recovery_incurred = S.direct_other_recovery_incurred, T.net_loss_paid = S.net_loss_paid2, T.net_loss_outstanding = S.net_loss_outstanding2, T.net_loss_incurred = S.net_loss_incurred2, T.net_alae_paid = S.net_alae_paid2, T.net_alae_outstanding = S.net_alae_outstanding2, T.net_alae_incurred = S.net_alae_incurred2, T.ClaimReserveDimId = S.ClaimReserveDimId3, T.ClaimRepresentativeDimFeatureClaimRepresentativeId = S.FeatureRepresentativeDimId3, T.FeatureRepresentativeAssignedDateId = S.FeatureRepresentativeAssignedDate_id3, T.InsuranceReferenceDimId = S.InsuranceReferenceDimId3, T.AgencyDimId = S.AgencyDimID3, T.SalesDivisionDimId = S.SalesDivisionDimID3, T.InsuranceReferenceCoverageDimId = S.InsuranceReferenceCoverageDetailDimID3, T.CoverageDetailDimId = S.CoverageDetailDimId3, T.ModifiedDate = S.ModifiedDate3
),