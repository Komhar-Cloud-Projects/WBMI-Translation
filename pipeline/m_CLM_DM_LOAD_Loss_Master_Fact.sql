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
SQ_loss_master_calculation AS (
	SELECT loss_master_calculation.loss_master_calculation_id, 
	loss_master_calculation.source_sys_id, 
	loss_master_calculation.claim_trans_pk_id, 
	loss_master_calculation.claim_reins_trans_pk_id, 
	loss_master_calculation.claimant_cov_det_ak_id, 
	loss_master_calculation.claim_pay_ak_id, 
	loss_master_calculation.reins_cov_ak_id, 
	loss_master_calculation.trans_kind_code, 
	loss_master_calculation.variation_code, 
	loss_master_calculation.pol_type, 
	loss_master_calculation.incptn_date, 
	loss_master_calculation.loss_master_run_date, 
	loss_master_calculation.new_claim_count, 
	loss_master_calculation.outstanding_amt, 
	loss_master_calculation.paid_loss_amt, 
	loss_master_calculation.paid_exp_amt, 
	loss_master_calculation.eom_unpaid_loss_adjust_exp, 
	loss_master_calculation.orig_reserve, 
	loss_master_calculation.auto_reins_facility, 
	loss_master_calculation.statistical_brkdwn_line, 
	loss_master_calculation.statistical_code1, 
	loss_master_calculation.statistical_code2, 
	loss_master_calculation.statistical_code3, 
	loss_master_calculation.statistical_line, 
	loss_master_calculation.loss_master_cov_code, 
	loss_master_calculation.risk_state_prov_code, 
	loss_master_calculation.risk_zip_code, 
	loss_master_calculation.terr_code, 
	loss_master_calculation.tax_loc, 
	loss_master_calculation.class_code, 
	loss_master_calculation.exposure, 
	loss_master_calculation.sub_line_code, 
	loss_master_calculation.source_sar_asl, 
	loss_master_calculation.source_sar_prdct_line, 
	loss_master_calculation.source_sar_sp_use_code, 
	loss_master_calculation.pms_trans_code, 
	loss_master_calculation.trans_date,
	loss_master_calculation.pms_acct_entered_date, 
	loss_master_calculation.trans_offset_onset_ind, 
	loss_master_calculation.claim_trans_amt, 
	loss_master_calculation.claim_trans_hist_amt, 
	loss_master_calculation.FinancialTypeCode,
	loss_master_calculation.TransactionCode,
	Case when claimant_coverage_detail.policysourceid in ('PMS','ESU') or claimant_coverage_detail.source_sys_id= 'PMS' then StatisticalCoverage.CoverageGUID
	when claimant_coverage_detail.policysourceid in ('DUC','PDC') then claimant_coverage_detail.CoverageGUID end as CoverageGUID,
	claimant_coverage_detail.InsuranceReferenceLineOfBusinessAKId, 
	claimant_coverage_detail.ProductAKId, 
	Case when claimant_coverage_detail.policysourceid in ('PMS','ESU') or claimant_coverage_detail.source_sys_id = 'PMS'  then SCSIL.StandardInsuranceLineCode 
	when claimant_coverage_detail.policysourceid in ('DUC','PDC') then RCSIL.StandardInsuranceLineCode end as StandardInsuranceLineCode,
	Case when claimant_coverage_detail.policysourceid in ('PMS','ESU') or claimant_coverage_detail.source_sys_id = 'PMS' then StatisticalCoverage.ClassCode
	when claimant_coverage_detail.policysourceid in ('DUC','PDC') then RCPC.ClassCode end as ClassCode,
	Case when claimant_coverage_detail.policysourceid = 'PMS' or claimant_coverage_detail.source_sys_id = 'PMS'  then 'PMS'
	when claimant_coverage_detail.policysourceid = 'ESU' or claimant_coverage_detail.source_sys_id = 'ESU'  then 'ESU'
	when claimant_coverage_detail.policysourceid in ('DUC','PDC') then 'DUC' end as PolicySourceId,
	StatisticalCoverage.RiskUnitGroup,
	StatisticalCoverage.RiskUnit,
	StatisticalCoverage.MajorPerilCode,
	RCPC.RiskType,
	RCPC.CoverageType,
	SCPC.TypeBureauCode,
	CASE WHEN StatisticalCoverage.RiskUnitSequenceNumber='N/A' then '' ELSE 
	SUBSTRING(StatisticalCoverage.RiskUnitSequenceNumber,2,1) END as ProductTypeCode,
	RCPC.PerilGroup,
	RCPC.CoverageForm,
	RCPC.SubCoverageTypeCode,
	RCPC.CoverageVersion,
	Case when claimant_coverage_detail.policysourceid in ('PMS','ESU') or claimant_coverage_detail.source_sys_id = 'PMS'  then SCPC.RatingPlanAKId
	when claimant_coverage_detail.policysourceid in ('DUC','PDC') then RCPC.RatingPlanAKId end as RatingPlanAKId,
	ISNULL(RCPC.Policyakid,SCPC.PolicyAkid) PolicyAkid
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_calculation
	LEFT JOIN claimant_coverage_detail 
	ON
	(case when loss_master_calculation.trans_offset_onset_ind='O' 
	then loss_master_calculation.pms_acct_entered_date
	else DATEADD(D,1,loss_master_calculation.loss_master_run_date)  end) between claimant_coverage_detail.eff_from_date and claimant_coverage_detail.eff_to_date
	and claimant_coverage_detail.claimant_cov_det_ak_id=loss_master_calculation.claimant_cov_det_ak_id
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage 
	ON claimant_coverage_detail.StatisticalCoverageAKID = StatisticalCoverage.StatisticalCoverageAKID and
	(case when loss_master_calculation.trans_offset_onset_ind='O' 
	then loss_master_calculation.pms_acct_entered_date
	else DATEADD(D,1,loss_master_calculation.loss_master_run_date)  end) between StatisticalCoverage.EffectiveDate and StatisticalCoverage.ExpirationDate
	and StatisticalCoverage.SourceSystemID = 'PMS' 
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage SCPC ON SCPC.PolicyCoverageAKID = StatisticalCoverage.PolicyCoverageAKID
	and (case when loss_master_calculation.trans_offset_onset_ind='O' 
	then loss_master_calculation.pms_acct_entered_date
	else DATEADD(D,1,loss_master_calculation.loss_master_run_date)  end) between SCPC.EffectiveDate and SCPC.ExpirationDate
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line SCSIL ON SCSIL.ins_line_code = SCPC.InsuranceLine
	and (case when loss_master_calculation.trans_offset_onset_ind='O' 
	then loss_master_calculation.pms_acct_entered_date
	else DATEADD(D,1,loss_master_calculation.loss_master_run_date)  end) between SCSIL.eff_from_date and SCSIL.eff_to_date
	
	LEFT JOIN ( select RatingCoverageAKID,RatingCoverage.EffectiveDate RatingCoverageRecordEffectivadate,RatingCoverage.ExpirationDate RatingCoverageRecordExpirationdate,PolicyCoverage.Policyakid,PolicyCoverage.EffectiveDate PolicyCoverageRecordEffectivadate,PolicyCoverage.ExpirationDate PolicyCoverageRecordExpirationdate,InsuranceLine,RatingCoverage.ClassCode,RatingCoverage.RiskType,RatingCoverage.CoverageType,RatingCoverage.PerilGroup,
	RatingCoverage.CoverageForm,RatingCoverage.SubCoverageTypeCode,RatingCoverage.CoverageVersion,PolicyCoverage.RatingPlanAKId
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage 
	 inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage
	 on PolicyCoverage.PolicyCoverageAKID=RatingCoverage.PolicyCoverageAKID
	 and PolicyCoverage.CurrentSnapshotFlag=1
	 and PolicyCoverage.SourceSystemID='DCT') RCPC
	ON claimant_coverage_detail.RatingCoverageAKID = RCPC.RatingCoverageAKID
	and loss_master_calculation.pol_ak_id=RCPC.PolicyAKID
	and (case when loss_master_calculation.trans_offset_onset_ind='O' 
	then loss_master_calculation.pms_acct_entered_date
	else DATEADD(D,1,loss_master_calculation.loss_master_run_date)  end) between RCPC.RatingCoverageRecordEffectivadate and RCPC.RatingCoverageRecordExpirationdate
	and (case when  loss_master_calculation.trans_offset_onset_ind='O' 
	then loss_master_calculation.pms_acct_entered_date
	else DATEADD(D,1,loss_master_calculation.loss_master_run_date)  end) between RCPC.PolicyCoverageRecordEffectivadate and RCPC.PolicyCoverageRecordExpirationdate
	
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line RCSIL ON RCSIL.ins_line_code = RCPC.InsuranceLine and
	(case when loss_master_calculation.trans_offset_onset_ind='O' 
	then loss_master_calculation.pms_acct_entered_date
	else DATEADD(D,1,loss_master_calculation.loss_master_run_date)  end) between RCSIL.eff_from_date and RCSIL.eff_to_date
	WHERE
	 loss_master_calculation.created_date > '@{pipeline().parameters.SELECTION_START_TS}'
	@{pipeline().parameters.WHERECLAUSE}
),
EXP_Loss_Master_Calc_Input AS (
	SELECT
	loss_master_calculation_id,
	source_sys_id,
	claim_trans_pk_id,
	claim_reins_trans_pk_id,
	claimant_cov_det_ak_id,
	claim_pay_ak_id,
	reins_cov_ak_id,
	trans_kind_code,
	variation_code,
	pol_type,
	-- *INF*: IIF(isnull(pol_type) or length(rtrim(ltrim(pol_type)))=0,'N/A',rtrim(ltrim(pol_type)))
	IFF(pol_type IS NULL OR length(rtrim(ltrim(pol_type))) = 0, 'N/A', rtrim(ltrim(pol_type))) AS pol_type_out,
	incptn_date,
	loss_master_run_date,
	new_claim_count,
	outstanding_amt,
	paid_loss_amt,
	paid_exp_amt,
	eom_unpaid_loss_adjust_exp,
	orig_reserve,
	auto_reins_facility,
	statistical_brkdwn_line,
	-- *INF*: IIF(isnull(statistical_brkdwn_line) or length(ltrim(rtrim(statistical_brkdwn_line)))=0,'N/A',ltrim(rtrim(statistical_brkdwn_line)))
	-- 
	-- 
	-- -- different column lengths
	IFF(statistical_brkdwn_line IS NULL OR length(ltrim(rtrim(statistical_brkdwn_line))) = 0, 'N/A', ltrim(rtrim(statistical_brkdwn_line))) AS statistical_brkdwn_line_out,
	statistical_code1,
	-- *INF*: IIF(isnull(statistical_code1) or length(rtrim(ltrim(statistical_code1)))=0  ,'N/A',rtrim(ltrim(statistical_code1)))
	IFF(statistical_code1 IS NULL OR length(rtrim(ltrim(statistical_code1))) = 0, 'N/A', rtrim(ltrim(statistical_code1))) AS statistical_code1_out,
	statistical_code2,
	-- *INF*: IIF(isnull(statistical_code2) or length(ltrim(rtrim(statistical_code2)))=0,'N/A',ltrim(rtrim(statistical_code2))) 
	IFF(statistical_code2 IS NULL OR length(ltrim(rtrim(statistical_code2))) = 0, 'N/A', ltrim(rtrim(statistical_code2))) AS statistical_code2_out,
	statistical_code3,
	-- *INF*: IIF(isnull(statistical_code3) or length(ltrim(rtrim(statistical_code3)))=0,'N/A',ltrim(rtrim(statistical_code3)))
	IFF(statistical_code3 IS NULL OR length(ltrim(rtrim(statistical_code3))) = 0, 'N/A', ltrim(rtrim(statistical_code3))) AS statistical_code3_out,
	statistical_line,
	-- *INF*: IIF(isnull(statistical_line) or length(rtrim(ltrim(statistical_line)))=0,'N/A', rtrim(ltrim(statistical_line)))
	IFF(statistical_line IS NULL OR length(rtrim(ltrim(statistical_line))) = 0, 'N/A', rtrim(ltrim(statistical_line))) AS statistical_line_out,
	loss_master_cov_code,
	-- *INF*: IIF(isnull(loss_master_cov_code) or length(rtrim(ltrim(loss_master_cov_code)))=0,'N/A', rtrim(ltrim(loss_master_cov_code)))
	IFF(loss_master_cov_code IS NULL OR length(rtrim(ltrim(loss_master_cov_code))) = 0, 'N/A', rtrim(ltrim(loss_master_cov_code))) AS loss_master_cov_code_out,
	risk_state_prov_code,
	risk_zip_code,
	-- *INF*: IIF(isnull(risk_zip_code) or length(rtrim(ltrim(risk_zip_code)))=0,'N/A', rtrim(ltrim(risk_zip_code)))
	IFF(risk_zip_code IS NULL OR length(rtrim(ltrim(risk_zip_code))) = 0, 'N/A', rtrim(ltrim(risk_zip_code))) AS risk_zip_code_out,
	terr_code,
	tax_loc,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRING_NUMERIC(tax_loc)
	-- 
	:UDF.DEFAULT_VALUE_FOR_STRING_NUMERIC(tax_loc) AS tax_loc_out,
	class_code,
	-- *INF*: IIF(isnull(class_code) or length(rtrim(ltrim(class_code)))=0,'N/A',rtrim(ltrim(class_code)))
	IFF(class_code IS NULL OR length(rtrim(ltrim(class_code))) = 0, 'N/A', rtrim(ltrim(class_code))) AS class_code_out,
	exposure,
	sub_line_code,
	-- *INF*: IIF(isnull(sub_line_code) or length(rtrim(ltrim(sub_line_code)))=0,'N/A',rtrim(ltrim(sub_line_code)))
	IFF(sub_line_code IS NULL OR length(rtrim(ltrim(sub_line_code))) = 0, 'N/A', rtrim(ltrim(sub_line_code))) AS sub_line_code_out,
	source_sar_asl,
	-- *INF*: IIF(isnull(source_sar_asl) or length(rtrim(ltrim(source_sar_asl)))=0,'N/A',rtrim(ltrim(source_sar_asl)))
	IFF(source_sar_asl IS NULL OR length(rtrim(ltrim(source_sar_asl))) = 0, 'N/A', rtrim(ltrim(source_sar_asl))) AS source_sar_asl_out,
	source_sar_prdct_line,
	-- *INF*: IIF(isnull(source_sar_prdct_line) or length(rtrim(ltrim(source_sar_prdct_line)))=0,'N/A',rtrim(ltrim(source_sar_prdct_line)))
	IFF(source_sar_prdct_line IS NULL OR length(rtrim(ltrim(source_sar_prdct_line))) = 0, 'N/A', rtrim(ltrim(source_sar_prdct_line))) AS source_sar_prdct_line_out,
	source_sar_sp_use_code,
	-- *INF*: IIF(isnull(source_sar_sp_use_code) or length(rtrim(ltrim(source_sar_sp_use_code)))=0,'N/A',rtrim(ltrim(source_sar_sp_use_code)))
	IFF(source_sar_sp_use_code IS NULL OR length(rtrim(ltrim(source_sar_sp_use_code))) = 0, 'N/A', rtrim(ltrim(source_sar_sp_use_code))) AS source_sar_sp_use_code_out,
	pms_trans_code,
	trans_date,
	pms_acct_entered_date,
	trans_offset_onset_ind,
	-- *INF*: IIF(trans_offset_onset_ind = 'O', pms_acct_entered_date, ADD_TO_DATE(loss_master_run_date,'dd',1))
	-- 
	-- --- Above logic is very important for Loss Master generation for EDW. We had to use above so that for EXCEED Offset Transactions we can get the attributes from Dim tables as that day so we are using pms_acct_entered_date. And for other transactions we use loss_master_run_date.
	IFF(trans_offset_onset_ind = 'O', pms_acct_entered_date, ADD_TO_DATE(loss_master_run_date, 'dd', 1)) AS loss_master_run_date_plus_one,
	claim_trans_amt,
	claim_trans_hist_amt,
	FinancialTypeCode,
	-- *INF*: ltrim(rtrim(FinancialTypeCode))
	ltrim(rtrim(FinancialTypeCode)) AS in_FinancialTypeCode,
	TransactionCode,
	CoverageGUID,
	InsuranceReferenceLineOfBusinessAKId AS i_InsuranceReferenceLineOfBusinessAKId,
	ProductAKId AS i_ProductAKId,
	StandardInsuranceLineCode AS i_StandardInsuranceLineCode,
	ClassCode AS i_ClassCode,
	PolicySourceId AS i_PolicySourceId,
	RiskUnitGroup AS i_RiskUnitGroup,
	RiskUnit AS i_RiskUnit,
	MajorPerilCode AS i_MajorPerilCode,
	CoverageForm AS i_CoverageForm,
	RiskType AS i_RiskType,
	CoverageType AS i_CoverageType,
	TypeBureauCode AS i_TypeBureauCode,
	-- *INF*: IIF(ISNULL(i_CoverageForm),'N/A',i_CoverageForm)
	IFF(i_CoverageForm IS NULL, 'N/A', i_CoverageForm) AS v_CoverageForm,
	-- *INF*: IIF(ISNULL(i_RiskType), 'N/A', i_RiskType)
	IFF(i_RiskType IS NULL, 'N/A', i_RiskType) AS v_RiskType,
	-- *INF*: IIF(ISNULL(i_RiskUnit), 'N/A', i_RiskUnit)
	IFF(i_RiskUnit IS NULL, 'N/A', i_RiskUnit) AS v_RiskUnit,
	-- *INF*: IIF(ISNULL(i_StandardInsuranceLineCode), 'N/A', i_StandardInsuranceLineCode)
	IFF(i_StandardInsuranceLineCode IS NULL, 'N/A', i_StandardInsuranceLineCode) AS o_StandardInsuranceLineCode,
	-- *INF*: IIF(ISNULL(i_ClassCode), 'N/A', i_ClassCode)
	IFF(i_ClassCode IS NULL, 'N/A', i_ClassCode) AS o_ClassCode,
	-- *INF*: IIF(ISNULL(i_TypeBureauCode),'N/A',i_TypeBureauCode)
	IFF(i_TypeBureauCode IS NULL, 'N/A', i_TypeBureauCode) AS o_TypeBureauCode,
	-- *INF*: IIF(ISNULL(i_RiskUnitGroup), 'N/A', i_RiskUnitGroup)
	IFF(i_RiskUnitGroup IS NULL, 'N/A', i_RiskUnitGroup) AS o_RiskUnitGroup,
	-- *INF*: IIF(ISNULL(i_RiskUnit), 'N/A', i_RiskUnit)
	IFF(i_RiskUnit IS NULL, 'N/A', i_RiskUnit) AS o_RiskUnit,
	-- *INF*: IIF(ISNULL(i_MajorPerilCode), 'N/A', i_MajorPerilCode)
	IFF(i_MajorPerilCode IS NULL, 'N/A', i_MajorPerilCode) AS o_MajorPerilCode,
	-- *INF*: IIF(ISNULL(i_InsuranceReferenceLineOfBusinessAKId), -1, i_InsuranceReferenceLineOfBusinessAKId)
	IFF(i_InsuranceReferenceLineOfBusinessAKId IS NULL, - 1, i_InsuranceReferenceLineOfBusinessAKId) AS o_InsuranceReferenceLineOfBusinessAKId,
	-- *INF*: IIF(ISNULL(i_ProductAKId), -1, i_ProductAKId)
	IFF(i_ProductAKId IS NULL, - 1, i_ProductAKId) AS o_ProductAKId,
	-- *INF*: IIF(ISNULL(i_PolicySourceId), 'N/A', i_PolicySourceId)
	IFF(i_PolicySourceId IS NULL, 'N/A', i_PolicySourceId) AS o_PolicySourceId,
	-- *INF*: v_RiskType
	-- 
	-- --IIF(LTRIM(RTRIM(v_CoverageForm))='BusinessAuto','N/A',v_RiskType)
	v_RiskType AS o_RiskType,
	-- *INF*: IIF(ISNULL(i_CoverageType), 'N/A', i_CoverageType)
	IFF(i_CoverageType IS NULL, 'N/A', i_CoverageType) AS o_CoverageType,
	ProductTypeCode,
	PerilGroup,
	SubCoverageTypeCode,
	CoverageVersion,
	RatingPlanAKId,
	PolicyAKID,
	'N/A' AS Default_Value
	FROM SQ_loss_master_calculation
),
lkp_Claim_Reinsurance_Transaction_SRC AS (
	SELECT
	claim_reins_trans_id,
	type_disability,
	claim_reins_pms_trans_code,
	claim_reins_trans_base_type_code,
	trans_ctgry_code,
	claim_reins_trans_code,
	claim_reins_trans_amt,
	claim_reins_trans_hist_amt,
	claim_reins_trans_date,
	offset_onset_ind
	FROM (
		SELECT 
			claim_reins_trans_id,
			type_disability,
			claim_reins_pms_trans_code,
			claim_reins_trans_base_type_code,
			trans_ctgry_code,
			claim_reins_trans_code,
			claim_reins_trans_amt,
			claim_reins_trans_hist_amt,
			claim_reins_trans_date,
			offset_onset_ind
		FROM claim_reinsurance_transaction
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_reins_trans_id ORDER BY claim_reins_trans_id) = 1
),
lkp_Claim_Transaction_SRC AS (
	SELECT
	claim_trans_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability,
	offset_onset_ind,
	trans_code,
	s3p_trans_code,
	trans_date,
	trans_base_type_code,
	trans_ctgry_code,
	trans_amt,
	trans_hist_amt,
	trans_rsn,
	draft_num,
	claim_trans_pk_id
	FROM (
		SELECT 
			claim_trans_id,
			cause_of_loss,
			reserve_ctgry,
			type_disability,
			offset_onset_ind,
			trans_code,
			s3p_trans_code,
			trans_date,
			trans_base_type_code,
			trans_ctgry_code,
			trans_amt,
			trans_hist_amt,
			trans_rsn,
			draft_num,
			claim_trans_pk_id
		FROM claim_transaction
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_trans_id ORDER BY claim_trans_id) = 1
),
EXP_Determine_Transaction_Values AS (
	SELECT
	lkp_Claim_Transaction_SRC.claim_trans_id AS claim_trans_pk_id,
	lkp_Claim_Transaction_SRC.cause_of_loss,
	lkp_Claim_Transaction_SRC.reserve_ctgry,
	lkp_Claim_Transaction_SRC.type_disability,
	lkp_Claim_Transaction_SRC.offset_onset_ind,
	lkp_Claim_Transaction_SRC.trans_code,
	lkp_Claim_Transaction_SRC.s3p_trans_code,
	lkp_Claim_Transaction_SRC.trans_date,
	lkp_Claim_Transaction_SRC.trans_base_type_code,
	lkp_Claim_Transaction_SRC.trans_ctgry_code,
	lkp_Claim_Transaction_SRC.trans_amt,
	lkp_Claim_Transaction_SRC.trans_hist_amt,
	lkp_Claim_Transaction_SRC.trans_rsn,
	lkp_Claim_Transaction_SRC.draft_num AS kind_code,
	lkp_Claim_Reinsurance_Transaction_SRC.claim_reins_trans_id AS claim_reins_trans_pk_id_IN,
	lkp_Claim_Reinsurance_Transaction_SRC.type_disability AS reins_type_disability,
	lkp_Claim_Reinsurance_Transaction_SRC.claim_reins_pms_trans_code,
	lkp_Claim_Reinsurance_Transaction_SRC.claim_reins_trans_base_type_code AS reins_trans_base_type_code,
	lkp_Claim_Reinsurance_Transaction_SRC.trans_ctgry_code AS reins_trans_ctgry_code,
	lkp_Claim_Reinsurance_Transaction_SRC.claim_reins_trans_code AS reins_trans_code,
	lkp_Claim_Reinsurance_Transaction_SRC.claim_reins_trans_amt AS reins_trans_amt,
	lkp_Claim_Reinsurance_Transaction_SRC.claim_reins_trans_hist_amt AS reins_trans_hist_amt,
	lkp_Claim_Reinsurance_Transaction_SRC.claim_reins_trans_date AS reins_trans_date,
	lkp_Claim_Reinsurance_Transaction_SRC.offset_onset_ind AS reins_offset_onset_ind,
	-- *INF*: iif(isnull(claim_trans_pk_id),0,iif(claim_trans_pk_id > 0,1,0))
	IFF(claim_trans_pk_id IS NULL, 0, IFF(claim_trans_pk_id > 0, 1, 0)) AS is_claim_trans_pk_id_valid,
	-- *INF*: iif(isnull(claim_reins_trans_pk_id_IN),0,iif(claim_reins_trans_pk_id_IN > 0,1,0))
	IFF(claim_reins_trans_pk_id_IN IS NULL, 0, IFF(claim_reins_trans_pk_id_IN > 0, 1, 0)) AS is_reins_trans_pk_id_valid,
	is_claim_trans_pk_id_valid AS is_claim_trans_pk_id_valid_OUT,
	is_reins_trans_pk_id_valid AS is_reins_trans_pk_id_valid_OUT,
	-- *INF*: decode(TRUE,
	-- is_claim_trans_pk_id_valid,cause_of_loss,
	-- is_reins_trans_pk_id_valid,'N/A',
	-- '')
	decode(TRUE,
		is_claim_trans_pk_id_valid, cause_of_loss,
		is_reins_trans_pk_id_valid, 'N/A',
		'') AS cause_of_loss_OUT,
	-- *INF*: decode(TRUE,
	-- is_claim_trans_pk_id_valid,reserve_ctgry,
	-- is_reins_trans_pk_id_valid,'N/A',
	-- '')
	decode(TRUE,
		is_claim_trans_pk_id_valid, reserve_ctgry,
		is_reins_trans_pk_id_valid, 'N/A',
		'') AS reserve_ctgry_OUT,
	-- *INF*: decode(TRUE,
	-- is_claim_trans_pk_id_valid,type_disability,
	-- is_reins_trans_pk_id_valid,reins_type_disability,
	-- '')
	decode(TRUE,
		is_claim_trans_pk_id_valid, type_disability,
		is_reins_trans_pk_id_valid, reins_type_disability,
		'') AS type_disability_OUT,
	-- *INF*: decode(TRUE,
	-- is_claim_trans_pk_id_valid,trans_code,
	-- is_reins_trans_pk_id_valid,reins_trans_code,
	-- '')
	decode(TRUE,
		is_claim_trans_pk_id_valid, trans_code,
		is_reins_trans_pk_id_valid, reins_trans_code,
		'') AS trans_code_OUT,
	-- *INF*: decode(TRUE,
	-- is_claim_trans_pk_id_valid,trans_ctgry_code,
	-- is_reins_trans_pk_id_valid,reins_trans_ctgry_code,
	-- '')
	decode(TRUE,
		is_claim_trans_pk_id_valid, trans_ctgry_code,
		is_reins_trans_pk_id_valid, reins_trans_ctgry_code,
		'') AS trans_ctgry_code_OUT,
	-- *INF*: decode(TRUE,
	-- is_claim_trans_pk_id_valid,offset_onset_ind,
	-- is_reins_trans_pk_id_valid,reins_offset_onset_ind,
	-- '')
	decode(TRUE,
		is_claim_trans_pk_id_valid, offset_onset_ind,
		is_reins_trans_pk_id_valid, reins_offset_onset_ind,
		'') AS offset_onset_ind_OUT,
	-- *INF*: decode(TRUE,
	-- is_claim_trans_pk_id_valid,'D',
	-- is_reins_trans_pk_id_valid,'C',
	-- 'N/A')
	decode(TRUE,
		is_claim_trans_pk_id_valid, 'D',
		is_reins_trans_pk_id_valid, 'C',
		'N/A') AS kind_code_OUT,
	-- *INF*: decode(TRUE,
	-- is_claim_trans_pk_id_valid,trans_rsn,
	-- is_reins_trans_pk_id_valid,'N/A',
	-- '')
	-- -- ceded records are always N/A
	decode(TRUE,
		is_claim_trans_pk_id_valid, trans_rsn,
		is_reins_trans_pk_id_valid, 'N/A',
		'') AS trns_rsn_OUT,
	-- *INF*: decode(TRUE,
	-- is_claim_trans_pk_id_valid,trans_base_type_code,
	-- is_reins_trans_pk_id_valid,reins_trans_base_type_code,
	-- '')
	decode(TRUE,
		is_claim_trans_pk_id_valid, trans_base_type_code,
		is_reins_trans_pk_id_valid, reins_trans_base_type_code,
		'') AS trans_base_type_code_OUT,
	-- *INF*: decode(TRUE,
	-- is_claim_trans_pk_id_valid,:LKP.LKP_CALENDER_DIM(to_date(to_char(trans_date,'MM/DD/YYYY'),'MM/DD/YYYY')),
	-- is_reins_trans_pk_id_valid,:LKP.LKP_CALENDER_DIM(to_date(to_char(reins_trans_date,'MM/DD/YYYY'),'MM/DD/YYYY')),
	-- -1)
	decode(TRUE,
		is_claim_trans_pk_id_valid, LKP_CALENDER_DIM_to_date_to_char_trans_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id,
		is_reins_trans_pk_id_valid, LKP_CALENDER_DIM_to_date_to_char_reins_trans_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id,
		- 1) AS trans_date_id_OUT,
	-- *INF*: decode(TRUE,
	-- is_claim_trans_pk_id_valid,trans_amt,
	-- is_reins_trans_pk_id_valid,reins_trans_amt,
	-- 0)
	decode(TRUE,
		is_claim_trans_pk_id_valid, trans_amt,
		is_reins_trans_pk_id_valid, reins_trans_amt,
		0) AS trans_amt_OUT,
	-- *INF*: decode(TRUE,
	-- is_claim_trans_pk_id_valid,trans_hist_amt,
	-- is_reins_trans_pk_id_valid,reins_trans_hist_amt,
	-- 0)
	decode(TRUE,
		is_claim_trans_pk_id_valid, trans_hist_amt,
		is_reins_trans_pk_id_valid, reins_trans_hist_amt,
		0) AS trans_hist_amt_OUT,
	EXP_Loss_Master_Calc_Input.trans_kind_code AS LM_trans_kind_code,
	EXP_Loss_Master_Calc_Input.pms_trans_code
	FROM EXP_Loss_Master_Calc_Input
	LEFT JOIN lkp_Claim_Reinsurance_Transaction_SRC
	ON lkp_Claim_Reinsurance_Transaction_SRC.claim_reins_trans_id = EXP_Loss_Master_Calc_Input.claim_reins_trans_pk_id
	LEFT JOIN lkp_Claim_Transaction_SRC
	ON lkp_Claim_Transaction_SRC.claim_trans_id = EXP_Loss_Master_Calc_Input.claim_trans_pk_id
	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_trans_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_trans_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(trans_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_reins_trans_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_reins_trans_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(reins_trans_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

),
EXP_set_financial_values AS (
	SELECT
	EXP_Loss_Master_Calc_Input.trans_date,
	EXP_Loss_Master_Calc_Input.claimant_cov_det_ak_id AS IN_claimant_cov_det_ak_id,
	EXP_Determine_Transaction_Values.cause_of_loss_OUT AS IN_cause_of_loss,
	EXP_Determine_Transaction_Values.reserve_ctgry_OUT AS IN_reserve_ctgry,
	EXP_Determine_Transaction_Values.type_disability_OUT AS IN_type_disability,
	EXP_Loss_Master_Calc_Input.FinancialTypeCode AS financial_type_code,
	EXP_Loss_Master_Calc_Input.TransactionCode AS trans_code,
	EXP_Determine_Transaction_Values.trans_ctgry_code_OUT AS trans_ctgry_code,
	EXP_Loss_Master_Calc_Input.claim_trans_amt AS trans_amt,
	EXP_Loss_Master_Calc_Input.claim_trans_hist_amt AS trans_hist_amt,
	EXP_Loss_Master_Calc_Input.source_sys_id,
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
	round(var_direct_alae_incurred_excluding_recoveries + var_direct_other_recovery_alae_paid + var_direct_other_recovery_alae_outstanding, 2) AS net_alae_incurred
	FROM EXP_Determine_Transaction_Values
	 -- Manually join with EXP_Loss_Master_Calc_Input
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
LKP_CoverageDetailDim AS (
	SELECT
	CoverageDetailDimId,
	CoverageGuid,
	PolicyAKID,
	EffectiveDate,
	ExpirationDate
	FROM (
		SELECT DISTINCT CDD.CoverageDetailDimid as CoverageDetailDimid, CDD.CoverageGuid as CoverageGuid, PC.PolicyAKID as PolicyAKID, CDD.EffectiveDate as EffectiveDate, CDD.ExpirationDate as ExpirationDate  
		FROM @{pipeline().parameters.DB_NAME_DATAMART}.DBO.CoverageDetailDim CDD
		INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.DBO.PremiumTransaction PT on CDD.EDWPremiumTransactionPKId= PT.PremiumTransactionID
		INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.DBO.RatingCoverage RC on PT.RatingCoverageAKId=RC.RatingCoverageAKID and PT.EffectiveDate = RC.EffectiveDate
		INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.DBO.PolicyCoverage PC on RC.PolicyCoverageAKID=PC.PolicyCoverageAKID and PC.CurrentSnapshotFlag=1 and PC.SourceSystemID = 'DCT'
		WHERE EXISTS (SELECT DISTINCT pol_ak_id FROM @{pipeline().parameters.DB_NAME_EDW}.dbo.loss_master_calculation  WHERE created_date > '@{pipeline().parameters.SELECTION_START_TS}'
		AND PC.PolicyAKID = pol_ak_id)
		
		UNION
		
		SELECT DISTINCT CDD.CoverageDetailDimid as CoverageDetailDimid, CDD.CoverageGuid as CoverageGuid, PC.PolicyAKID as PolicyAKID, CDD.EffectiveDate as EffectiveDate, CDD.ExpirationDate as ExpirationDate  
		FROM @{pipeline().parameters.DB_NAME_DATAMART}.DBO.CoverageDetailDim CDD
		INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.DBO.PremiumTransaction PT on CDD.EDWPremiumTransactionPKId= PT.PremiumTransactionID
		INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.DBO.statisticalCoverage SC on PT.StatisticalCoverageAKID=SC.StatisticalCoverageAKID 
		INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.DBO.PolicyCoverage PC on SC.PolicyCoverageAKID=PC.PolicyCoverageAKID and PC.CurrentSnapshotFlag=1 and PC.SourceSystemID = 'PMS'
		WHERE EXISTS (SELECT DISTINCT pol_ak_id FROM @{pipeline().parameters.DB_NAME_EDW}.dbo.loss_master_calculation  WHERE created_date > '@{pipeline().parameters.SELECTION_START_TS}'
		AND PC.PolicyAKID = pol_ak_id)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageGuid,PolicyAKID,EffectiveDate,ExpirationDate ORDER BY CoverageDetailDimId DESC) = 1
),
LKP_InsuranceReferenceCoverageDim_DCT AS (
	SELECT
	InsuranceReferenceCoverageDimId,
	InsuranceLineCode,
	DctRiskTypeCode,
	DctCoverageTypeCode,
	DctPerilGroup,
	DctSubCoverageTypeCode,
	DctCoverageVersion
	FROM (
		SELECT 
			InsuranceReferenceCoverageDimId,
			InsuranceLineCode,
			DctRiskTypeCode,
			DctCoverageTypeCode,
			DctPerilGroup,
			DctSubCoverageTypeCode,
			DctCoverageVersion
		FROM InsuranceReferenceCoverageDim
		WHERE NOT (DctRiskTypeCode='N/A' AND DctCoverageTypeCode='N/A' AND DctPerilGroup='N/A' AND DctSubCoverageTypeCode='N/A' AND DctCoverageVersion='N/A')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLineCode,DctRiskTypeCode,DctCoverageTypeCode,DctPerilGroup,DctSubCoverageTypeCode,DctCoverageVersion ORDER BY InsuranceReferenceCoverageDimId) = 1
),
LKP_Loss_Master_Dim_Find_Dim_Id AS (
	SELECT
	loss_master_dim_id,
	variation_code,
	pol_type,
	auto_reins_facility,
	statistical_brkdwn_line,
	statistical_code1,
	statistical_code2,
	statistical_code3,
	loss_master_cov_code,
	risk_state_prov_code,
	risk_zip_code,
	terr_code,
	tax_loc,
	class_code,
	exposure,
	sub_line_code,
	source_sar_asl,
	source_sar_sp_use_code,
	source_statistical_line
	FROM (
		SELECT
		ltrim(rtrim( loss_master_dim.loss_master_dim_id)) 			as loss_master_dim_id,
		ltrim(rtrim(loss_master_dim.variation_code))				as variation_code, 
		ltrim(rtrim(loss_master_dim.pol_type))							as pol_type, 
		ltrim(rtrim(loss_master_dim.auto_reins_facility))	 			as auto_reins_facility, 
		ltrim(rtrim(loss_master_dim.statistical_brkdwn_line)) 		as statistical_brkdwn_line, 
		ltrim(rtrim(loss_master_dim.statistical_code1))				 as statistical_code1, 
		ltrim(rtrim(loss_master_dim.statistical_code2))	 			as statistical_code2, 
		ltrim(rtrim(loss_master_dim.statistical_code3))		 		as statistical_code3, 
		ltrim(rtrim(loss_master_dim.loss_master_cov_code))	 	as loss_master_cov_code, 
		ltrim(rtrim(loss_master_dim.risk_state_prov_code))	 		as risk_state_prov_code, 
		ltrim(rtrim(loss_master_dim.risk_zip_code))	 				as risk_zip_code, 
		ltrim(rtrim(loss_master_dim.terr_code))	 					as terr_code, 
		ltrim(rtrim(loss_master_dim.tax_loc)) 							as tax_loc, 
		ltrim(rtrim(loss_master_dim.class_code))	 					as class_code, 
		ltrim(rtrim(loss_master_dim.exposure))	 					as exposure, 
		ltrim(rtrim(loss_master_dim.sub_line_code))	 				as sub_line_code, 
		ltrim(rtrim(loss_master_dim.source_sar_asl)) 				as source_sar_asl, 
		ltrim(rtrim(loss_master_dim.source_sar_sp_use_code)) 	as source_sar_sp_use_code, 
		ltrim(rtrim(loss_master_dim.source_statistical_line)) 		as source_statistical_line 
		FROM loss_master_dim
		where crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY variation_code,pol_type,auto_reins_facility,statistical_brkdwn_line,statistical_code1,statistical_code2,statistical_code3,loss_master_cov_code,risk_state_prov_code,risk_zip_code,terr_code,tax_loc,class_code,exposure,sub_line_code,source_sar_asl,source_sar_sp_use_code,source_statistical_line ORDER BY loss_master_dim_id DESC) = 1
),
LKP_Loss_Master_Dim_Find_Dim_Id_DCT AS (
	SELECT
	loss_master_dim_id,
	variation_code,
	pol_type,
	auto_reins_facility,
	statistical_brkdwn_line,
	statistical_code1,
	statistical_code2,
	statistical_code3,
	loss_master_cov_code,
	risk_state_prov_code,
	risk_zip_code,
	terr_code,
	tax_loc,
	class_code,
	exposure,
	sub_line_code,
	source_sar_asl,
	source_sar_sp_use_code,
	source_statistical_line
	FROM (
		SELECT
		ltrim(rtrim( loss_master_dim.loss_master_dim_id)) 			as loss_master_dim_id,
		ltrim(rtrim(loss_master_dim.risk_state_prov_code))	 		as risk_state_prov_code, 
		ltrim(rtrim(loss_master_dim.risk_zip_code))	 				as risk_zip_code, 
		ltrim(rtrim(loss_master_dim.terr_code))	 					as terr_code, 
		ltrim(rtrim(loss_master_dim.tax_loc)) 							as tax_loc, 
		ltrim(rtrim(loss_master_dim.class_code))	 					as class_code, 
		ltrim(rtrim(loss_master_dim.exposure))	 					as exposure, 
		ltrim(rtrim(loss_master_dim.sub_line_code))	 				as sub_line_code, 
		ltrim(rtrim(loss_master_dim.variation_code))				as variation_code, 
		ltrim(rtrim(loss_master_dim.pol_type))							as pol_type, 
		ltrim(rtrim(loss_master_dim.auto_reins_facility))	 			as auto_reins_facility, 
		ltrim(rtrim(loss_master_dim.statistical_brkdwn_line)) 		as statistical_brkdwn_line, 
		ltrim(rtrim(loss_master_dim.statistical_code1))				 as statistical_code1, 
		ltrim(rtrim(loss_master_dim.statistical_code2))	 			as statistical_code2, 
		ltrim(rtrim(loss_master_dim.statistical_code3))		 		as statistical_code3, 
		ltrim(rtrim(loss_master_dim.loss_master_cov_code))	 	as loss_master_cov_code, 
		ltrim(rtrim(loss_master_dim.source_sar_asl)) 				as source_sar_asl, 
		ltrim(rtrim(loss_master_dim.source_sar_sp_use_code)) 	as source_sar_sp_use_code, 
		ltrim(rtrim(loss_master_dim.source_statistical_line)) 		as source_statistical_line 
		FROM loss_master_dim
		where crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY variation_code,pol_type,auto_reins_facility,statistical_brkdwn_line,statistical_code1,statistical_code2,statistical_code3,loss_master_cov_code,risk_state_prov_code,risk_zip_code,terr_code,tax_loc,class_code,exposure,sub_line_code,source_sar_asl,source_sar_sp_use_code,source_statistical_line ORDER BY loss_master_dim_id DESC) = 1
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
	QUALIFY ROW_NUMBER() OVER (PARTITION BY financial_type_code ORDER BY claim_financial_type_dim_id DESC) = 1
),
LKP_reinsurance_coverage_dim1 AS (
	SELECT
	reins_cov_dim_id,
	edw_reins_cov_ak_id,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
			reins_cov_dim_id,
			edw_reins_cov_ak_id,
			eff_from_date,
			eff_to_date
		FROM reinsurance_coverage_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_reins_cov_ak_id,eff_from_date,eff_to_date ORDER BY reins_cov_dim_id DESC) = 1
),
lkp_claim_transaction_type_dim2 AS (
	SELECT
	claim_trans_type_dim_id,
	type_disability,
	pms_trans_code,
	trans_ctgry_code,
	offset_onset_ind,
	trans_kind_code,
	trans_rsn
	FROM (
		SELECT 
			claim_trans_type_dim_id,
			type_disability,
			pms_trans_code,
			trans_ctgry_code,
			offset_onset_ind,
			trans_kind_code,
			trans_rsn
		FROM claim_transaction_type_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY type_disability,pms_trans_code,trans_ctgry_code,offset_onset_ind,trans_kind_code,trans_rsn ORDER BY claim_trans_type_dim_id DESC) = 1
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
mplt_Claim_occurence_dim_id AS (WITH
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
mplt_PMS_Coverage_Agency_InsuranceReference_DimIDs AS (WITH
	Input AS (
		
	),
	EXP_GetSource AS (
		SELECT
		AgencyAKId,
		SalesTerritoryAKID,
		RegionalSalesManagerAKID,
		SalesDirectorAKID,
		InsuranceLine,
		ClassCode,
		TypeBureauCode,
		RiskUnitGroup,
		RiskUnit,
		MajorPerilCode,
		SupSpecialClassGroupId,
		SupPackageModificationAdjustmentGroupId,
		SupIncreasedLimitGroupId,
		StrategicProfitCenterAKId,
		InsuranceSegmentAKId,
		PolicyOfferingAKId,
		InsuranceReferenceLineOfBusinessAKId,
		ProductAKId,
		trans_date,
		ProductTypeCode,
		RatingPlanAKId
		FROM Input
	),
	LKP_RatingPlan AS (
		SELECT
		RatingPlanCode,
		RatingPlanAKId,
		EffectiveDate,
		ExpirationDate
		FROM (
			SELECT 
				RatingPlanCode,
				RatingPlanAKId,
				EffectiveDate,
				ExpirationDate
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingPlan
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingPlanAKId,EffectiveDate,ExpirationDate ORDER BY RatingPlanCode) = 1
	),
	LKP_PolicyOffering AS (
		SELECT
		PolicyOfferingCode,
		PolicyOfferingAKId,
		EffectiveDate,
		ExpirationDate
		FROM (
			SELECT 
				PolicyOfferingCode,
				PolicyOfferingAKId,
				EffectiveDate,
				ExpirationDate
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyOfferingAKId,EffectiveDate,ExpirationDate ORDER BY PolicyOfferingCode) = 1
	),
	LKP_InsuranceSegment AS (
		SELECT
		InsuranceSegmentCode,
		InsuranceSegmentAKId,
		EffectiveDate,
		ExpirationDate
		FROM (
			SELECT 
				InsuranceSegmentCode,
				InsuranceSegmentAKId,
				EffectiveDate,
				ExpirationDate
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceSegmentAKId,EffectiveDate,ExpirationDate ORDER BY InsuranceSegmentCode) = 1
	),
	LKP_StrategicProfitCenter AS (
		SELECT
		EnterpriseGroupId,
		InsuranceReferenceLegalEntityId,
		StrategicProfitCenterCode,
		StrategicProfitCenterAKId,
		EffectiveDate,
		ExpirationDate
		FROM (
			SELECT 
				EnterpriseGroupId,
				InsuranceReferenceLegalEntityId,
				StrategicProfitCenterCode,
				StrategicProfitCenterAKId,
				EffectiveDate,
				ExpirationDate
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterAKId,EffectiveDate,ExpirationDate ORDER BY EnterpriseGroupId) = 1
	),
	LKP_Product AS (
		SELECT
		ProductCode,
		ProductAKId,
		EffectiveDate,
		ExpirationDate
		FROM (
			SELECT 
				ProductCode,
				ProductAKId,
				EffectiveDate,
				ExpirationDate
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ProductAKId,EffectiveDate,ExpirationDate ORDER BY ProductCode) = 1
	),
	LKP_sup_insurance_line AS (
		SELECT
		StandardInsuranceLineCode,
		ins_line_code,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
				StandardInsuranceLineCode,
				ins_line_code,
				eff_from_date,
				eff_to_date
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ins_line_code,eff_from_date,eff_to_date ORDER BY StandardInsuranceLineCode) = 1
	),
	LKP_InsuranceReferenceLineOfBusiness AS (
		SELECT
		InsuranceReferenceLineOfBusinessCode,
		InsuranceReferenceLineOfBusinessAKId,
		EffectiveDate,
		ExpirationDate
		FROM (
			SELECT 
				InsuranceReferenceLineOfBusinessCode,
				InsuranceReferenceLineOfBusinessAKId,
				EffectiveDate,
				ExpirationDate
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceReferenceLineOfBusinessAKId,EffectiveDate,ExpirationDate ORDER BY InsuranceReferenceLineOfBusinessCode) = 1
	),
	LKP_EnterpriseGroup AS (
		SELECT
		EnterpriseGroupCode,
		EnterpriseGroupId
		FROM (
			SELECT 
				EnterpriseGroupCode,
				EnterpriseGroupId
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.EnterpriseGroup
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY EnterpriseGroupId ORDER BY EnterpriseGroupCode) = 1
	),
	LKP_InsuranceReferenceLegalEntity AS (
		SELECT
		InsuranceReferenceLegalEntityCode,
		InsuranceReferenceLegalEntityId
		FROM (
			SELECT 
				InsuranceReferenceLegalEntityCode,
				InsuranceReferenceLegalEntityId
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLegalEntity
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceReferenceLegalEntityId ORDER BY InsuranceReferenceLegalEntityCode) = 1
	),
	EXP_GetCodes AS (
		SELECT
		LKP_sup_insurance_line.StandardInsuranceLineCode AS i_StandardInsuranceLineCode,
		LKP_StrategicProfitCenter.StrategicProfitCenterCode AS i_StrategicProfitCenterCode,
		LKP_InsuranceSegment.InsuranceSegmentCode AS i_InsuranceSegmentCode,
		LKP_PolicyOffering.PolicyOfferingCode AS i_PolicyOfferingCode,
		LKP_Product.ProductCode AS i_ProductCode,
		LKP_InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessCode AS i_InsuranceReferenceLineOfBusinessCode,
		LKP_EnterpriseGroup.EnterpriseGroupCode AS i_EnterpriseGroupCode,
		LKP_InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityCode AS i_InsuranceReferenceLegalEntityCode,
		EXP_GetSource.MajorPerilCode AS i_MajorPerilCode,
		-- *INF*: DECODE(TRUE, 
		-- ISNULL(i_MajorPerilCode),'N/A',
		-- IS_SPACES(i_MajorPerilCode),'N/A',
		-- LENGTH(i_MajorPerilCode)=0,'N/A', 
		-- LTRIM(RTRIM(i_MajorPerilCode)))
		DECODE(TRUE,
			i_MajorPerilCode IS NULL, 'N/A',
			IS_SPACES(i_MajorPerilCode), 'N/A',
			LENGTH(i_MajorPerilCode) = 0, 'N/A',
			LTRIM(RTRIM(i_MajorPerilCode))) AS v_MajorPerilCode,
		-- *INF*: IIF(REG_MATCH(v_MajorPerilCode,'[^0-9a-zA-Z]') OR LTRIM(v_MajorPerilCode,'0')='','N/A',v_MajorPerilCode)
		IFF(REG_MATCH(v_MajorPerilCode, '[^0-9a-zA-Z]') OR LTRIM(v_MajorPerilCode, '0') = '', 'N/A', v_MajorPerilCode) AS v_Reg_MajorPerilCode,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(i_StandardInsuranceLineCode),'N/A',
		-- IS_SPACES(i_StandardInsuranceLineCode),'N/A',
		-- LENGTH(i_StandardInsuranceLineCode)=0,'N/A',
		-- LTRIM(RTRIM(i_StandardInsuranceLineCode)))
		DECODE(TRUE,
			i_StandardInsuranceLineCode IS NULL, 'N/A',
			IS_SPACES(i_StandardInsuranceLineCode), 'N/A',
			LENGTH(i_StandardInsuranceLineCode) = 0, 'N/A',
			LTRIM(RTRIM(i_StandardInsuranceLineCode))) AS v_Default_StandardInsuranceLineCode,
		-- *INF*: IIF(REG_MATCH(v_Default_StandardInsuranceLineCode,'[^0-9a-zA-Z]') OR LTRIM(v_Default_StandardInsuranceLineCode,'0')='' ,'N/A',v_Default_StandardInsuranceLineCode)
		IFF(REG_MATCH(v_Default_StandardInsuranceLineCode, '[^0-9a-zA-Z]') OR LTRIM(v_Default_StandardInsuranceLineCode, '0') = '', 'N/A', v_Default_StandardInsuranceLineCode) AS v_Reg_StandardInsuranceLineCode,
		-- *INF*: IIF(v_Reg_StandardInsuranceLineCode='N/A' AND (IN(TypeBureauCode,'AL','AN','AP') OR IN(v_Reg_MajorPerilCode,'930','931')),'CA',v_Reg_StandardInsuranceLineCode)
		IFF(v_Reg_StandardInsuranceLineCode = 'N/A' AND ( IN(TypeBureauCode, 'AL', 'AN', 'AP') OR IN(v_Reg_MajorPerilCode, '930', '931') ), 'CA', v_Reg_StandardInsuranceLineCode) AS v_StandardInsuranceLineCode,
		-- *INF*: IIF(v_StandardInsuranceLineCode='N/A' AND IN(TypeBureauCode,'CF','B2','BB','BE','BF','BM','BT','FT','GL','GS','IM','MS','PF','PH','PI','PL','PQ','WC','WP','NB','RL','RN','RP','BC','N/A'),1,0)
		IFF(v_StandardInsuranceLineCode = 'N/A' AND IN(TypeBureauCode, 'CF', 'B2', 'BB', 'BE', 'BF', 'BM', 'BT', 'FT', 'GL', 'GS', 'IM', 'MS', 'PF', 'PH', 'PI', 'PL', 'PQ', 'WC', 'WP', 'NB', 'RL', 'RN', 'RP', 'BC', 'N/A'), 1, 0) AS v_flag,
		EXP_GetSource.AgencyAKId,
		EXP_GetSource.SalesTerritoryAKID,
		EXP_GetSource.TypeBureauCode,
		EXP_GetSource.RegionalSalesManagerAKID,
		EXP_GetSource.SalesDirectorAKID,
		EXP_GetSource.ClassCode,
		EXP_GetSource.RiskUnitGroup AS i_RiskUnitGroup,
		EXP_GetSource.RiskUnit AS i_RiskUnit,
		EXP_GetSource.trans_date,
		EXP_GetSource.ProductTypeCode AS i_ProductTypeCode,
		LKP_RatingPlan.RatingPlanCode AS i_RatingPlanCode,
		-- *INF*: IIF(IN(v_StandardInsuranceLineCode,'CR') OR v_flag=1,'N/A',i_RiskUnitGroup)
		IFF(IN(v_StandardInsuranceLineCode, 'CR') OR v_flag = 1, 'N/A', i_RiskUnitGroup) AS v_RiskUnitGroup,
		-- *INF*: DECODE(TRUE, 
		-- ISNULL(v_RiskUnitGroup),'N/A',
		-- IS_SPACES(v_RiskUnitGroup),'N/A',
		-- LENGTH(v_RiskUnitGroup)=0,'N/A', 
		-- LTRIM(RTRIM(v_RiskUnitGroup)))
		DECODE(TRUE,
			v_RiskUnitGroup IS NULL, 'N/A',
			IS_SPACES(v_RiskUnitGroup), 'N/A',
			LENGTH(v_RiskUnitGroup) = 0, 'N/A',
			LTRIM(RTRIM(v_RiskUnitGroup))) AS v_Default_RiskUnitGroup,
		-- *INF*: IIF(REG_MATCH(v_Default_RiskUnitGroup,'[^0-9a-zA-Z]') OR LTRIM(v_Default_RiskUnitGroup,'0')='','N/A',v_Default_RiskUnitGroup)
		IFF(REG_MATCH(v_Default_RiskUnitGroup, '[^0-9a-zA-Z]') OR LTRIM(v_Default_RiskUnitGroup, '0') = '', 'N/A', v_Default_RiskUnitGroup) AS o_RiskUnitGroup,
		v_Reg_MajorPerilCode AS o_MajorPerilCode,
		-- *INF*: IIF((v_StandardInsuranceLineCode='GL' AND (NOT IN(v_MajorPerilCode,'540','599','919') OR  (NOT IN( ClassCode,'11111','22222','22250','92100','17000','17001','17002','80051','80052','80053','80054','80055','80056','80057','80058'))))  OR IN(v_StandardInsuranceLineCode, 'WC','IM','CG','CA') OR  v_flag=1, 'N/A',i_RiskUnit)
		IFF(( v_StandardInsuranceLineCode = 'GL' AND ( NOT IN(v_MajorPerilCode, '540', '599', '919') OR ( NOT IN(ClassCode, '11111', '22222', '22250', '92100', '17000', '17001', '17002', '80051', '80052', '80053', '80054', '80055', '80056', '80057', '80058') ) ) ) OR IN(v_StandardInsuranceLineCode, 'WC', 'IM', 'CG', 'CA') OR v_flag = 1, 'N/A', i_RiskUnit) AS v_RiskUnit,
		-- *INF*: DECODE(TRUE, 
		-- ISNULL(v_RiskUnit),'N/A',
		-- IS_SPACES(v_RiskUnit),'N/A',
		-- LENGTH(v_RiskUnit)=0,'N/A', 
		-- LTRIM(RTRIM(v_RiskUnit)))
		DECODE(TRUE,
			v_RiskUnit IS NULL, 'N/A',
			IS_SPACES(v_RiskUnit), 'N/A',
			LENGTH(v_RiskUnit) = 0, 'N/A',
			LTRIM(RTRIM(v_RiskUnit))) AS v_Default_RiskUnit,
		-- *INF*: IIF(LTRIM(i_ProductTypeCode,'0')='' OR  v_StandardInsuranceLineCode<>'GL','N/A',LTRIM(i_ProductTypeCode,'0'))
		IFF(LTRIM(i_ProductTypeCode, '0') = '' OR v_StandardInsuranceLineCode <> 'GL', 'N/A', LTRIM(i_ProductTypeCode, '0')) AS v_ProductTypeCode,
		-- *INF*: IIF(REG_MATCH(v_Default_RiskUnit,'[^0-9a-zA-Z]') OR LTRIM(v_Default_RiskUnit,'0')='','N/A',v_Default_RiskUnit)
		IFF(REG_MATCH(v_Default_RiskUnit, '[^0-9a-zA-Z]') OR LTRIM(v_Default_RiskUnit, '0') = '', 'N/A', v_Default_RiskUnit) AS o_RiskUnit,
		-- *INF*: IIF(ISNULL(v_StandardInsuranceLineCode), 'N/A', v_StandardInsuranceLineCode)
		IFF(v_StandardInsuranceLineCode IS NULL, 'N/A', v_StandardInsuranceLineCode) AS o_StandardInsuranceLineCode,
		-- *INF*: IIF(ISNULL(i_StrategicProfitCenterCode), '6', i_StrategicProfitCenterCode)
		IFF(i_StrategicProfitCenterCode IS NULL, '6', i_StrategicProfitCenterCode) AS o_StrategicProfitCenterCode,
		-- *INF*: IIF(ISNULL(i_InsuranceSegmentCode), 'N/A', i_InsuranceSegmentCode)
		IFF(i_InsuranceSegmentCode IS NULL, 'N/A', i_InsuranceSegmentCode) AS o_InsuranceSegmentCode,
		-- *INF*: IIF(ISNULL(i_PolicyOfferingCode), '000', i_PolicyOfferingCode)
		IFF(i_PolicyOfferingCode IS NULL, '000', i_PolicyOfferingCode) AS o_PolicyOfferingCode,
		-- *INF*: IIF(ISNULL(i_ProductCode), '000', i_ProductCode)
		IFF(i_ProductCode IS NULL, '000', i_ProductCode) AS o_ProductCode,
		-- *INF*: IIF(ISNULL(i_InsuranceReferenceLineOfBusinessCode), '000', i_InsuranceReferenceLineOfBusinessCode)
		IFF(i_InsuranceReferenceLineOfBusinessCode IS NULL, '000', i_InsuranceReferenceLineOfBusinessCode) AS o_InsuranceReferenceLineOfBusinessCode,
		-- *INF*: IIF(ISNULL(i_EnterpriseGroupCode), '1', i_EnterpriseGroupCode)
		IFF(i_EnterpriseGroupCode IS NULL, '1', i_EnterpriseGroupCode) AS o_EnterpriseGroupCode,
		-- *INF*: IIF(ISNULL(i_InsuranceReferenceLegalEntityCode), '1', i_InsuranceReferenceLegalEntityCode)
		IFF(i_InsuranceReferenceLegalEntityCode IS NULL, '1', i_InsuranceReferenceLegalEntityCode) AS o_InsuranceReferenceLegalEntityCode,
		-- *INF*: DECODE(TRUE, 
		-- ISNULL(v_ProductTypeCode),'N/A',
		-- IS_SPACES(v_ProductTypeCode),'N/A',
		-- LENGTH(v_ProductTypeCode)=0,'N/A', 
		-- LTRIM(RTRIM(v_ProductTypeCode)))
		DECODE(TRUE,
			v_ProductTypeCode IS NULL, 'N/A',
			IS_SPACES(v_ProductTypeCode), 'N/A',
			LENGTH(v_ProductTypeCode) = 0, 'N/A',
			LTRIM(RTRIM(v_ProductTypeCode))) AS o_ProductTypeCode,
		-- *INF*: IIF(ISNULL(i_RatingPlanCode), '1', i_RatingPlanCode)
		IFF(i_RatingPlanCode IS NULL, '1', i_RatingPlanCode) AS o_RatingPlanCode
		FROM EXP_GetSource
		LEFT JOIN LKP_EnterpriseGroup
		ON LKP_EnterpriseGroup.EnterpriseGroupId = LKP_StrategicProfitCenter.EnterpriseGroupId
		LEFT JOIN LKP_InsuranceReferenceLegalEntity
		ON LKP_InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityId = LKP_StrategicProfitCenter.InsuranceReferenceLegalEntityId
		LEFT JOIN LKP_InsuranceReferenceLineOfBusiness
		ON LKP_InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessAKId = EXP_GetSource.InsuranceReferenceLineOfBusinessAKId AND LKP_InsuranceReferenceLineOfBusiness.EffectiveDate <= EXP_GetSource.trans_date AND LKP_InsuranceReferenceLineOfBusiness.ExpirationDate >= EXP_GetSource.trans_date
		LEFT JOIN LKP_InsuranceSegment
		ON LKP_InsuranceSegment.InsuranceSegmentAKId = EXP_GetSource.InsuranceSegmentAKId AND LKP_InsuranceSegment.EffectiveDate <= EXP_GetSource.trans_date AND LKP_InsuranceSegment.ExpirationDate >= EXP_GetSource.trans_date
		LEFT JOIN LKP_PolicyOffering
		ON LKP_PolicyOffering.PolicyOfferingAKId = EXP_GetSource.PolicyOfferingAKId AND LKP_PolicyOffering.EffectiveDate <= EXP_GetSource.trans_date AND LKP_PolicyOffering.ExpirationDate >= EXP_GetSource.trans_date
		LEFT JOIN LKP_Product
		ON LKP_Product.ProductAKId = EXP_GetSource.ProductAKId AND LKP_Product.EffectiveDate <= EXP_GetSource.trans_date AND LKP_Product.ExpirationDate >= EXP_GetSource.trans_date
		LEFT JOIN LKP_RatingPlan
		ON LKP_RatingPlan.RatingPlanAKId = EXP_GetSource.RatingPlanAKId AND LKP_RatingPlan.EffectiveDate <= EXP_GetSource.trans_date AND LKP_RatingPlan.ExpirationDate >= EXP_GetSource.trans_date
		LEFT JOIN LKP_StrategicProfitCenter
		ON LKP_StrategicProfitCenter.StrategicProfitCenterAKId = EXP_GetSource.StrategicProfitCenterAKId AND LKP_StrategicProfitCenter.EffectiveDate <= EXP_GetSource.trans_date AND LKP_StrategicProfitCenter.ExpirationDate >= EXP_GetSource.trans_date
		LEFT JOIN LKP_sup_insurance_line
		ON LKP_sup_insurance_line.ins_line_code = EXP_GetSource.InsuranceLine AND LKP_sup_insurance_line.eff_from_date <= EXP_GetSource.trans_date AND LKP_sup_insurance_line.eff_to_date >= EXP_GetSource.trans_date
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
	LKP_InsuranceReferenceDim AS (
		SELECT
		InsuranceReferenceDimId,
		StrategicProfitCenterCode,
		InsuranceSegmentCode,
		PolicyOfferingCode,
		ProductCode,
		InsuranceReferenceLineOfBusinessCode,
		EnterpriseGroupCode,
		InsuranceReferenceLegalEntityCode,
		RatingPlanCode
		FROM (
			SELECT 
				InsuranceReferenceDimId,
				StrategicProfitCenterCode,
				InsuranceSegmentCode,
				PolicyOfferingCode,
				ProductCode,
				InsuranceReferenceLineOfBusinessCode,
				EnterpriseGroupCode,
				InsuranceReferenceLegalEntityCode,
				RatingPlanCode
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceDim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterCode,InsuranceSegmentCode,PolicyOfferingCode,ProductCode,InsuranceReferenceLineOfBusinessCode,EnterpriseGroupCode,InsuranceReferenceLegalEntityCode,RatingPlanCode ORDER BY InsuranceReferenceDimId) = 1
	),
	LKP_InsuranceReferenceCoverageDim_PMS AS (
		SELECT
		InsuranceReferenceCoverageDimId,
		PmsMajorPerilCode,
		PmsProductTypeCode,
		InsuranceLineCode,
		PmsRiskUnitGroupCode,
		PmsRiskUnitCode
		FROM (
			SELECT 
				InsuranceReferenceCoverageDimId,
				PmsMajorPerilCode,
				PmsProductTypeCode,
				InsuranceLineCode,
				PmsRiskUnitGroupCode,
				PmsRiskUnitCode
			FROM InsuranceReferenceCoverageDim
			WHERE DctRiskTypeCode='N/A' AND DctCoverageTypeCode='N/A' AND DctPerilGroup='N/A' AND DctSubCoverageTypeCode='N/A' AND DctCoverageVersion='N/A'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PmsMajorPerilCode,PmsProductTypeCode,InsuranceLineCode,PmsRiskUnitGroupCode,PmsRiskUnitCode ORDER BY InsuranceReferenceCoverageDimId) = 1
	),
	LKP_SalesDivisionDim AS (
		SELECT
		SalesDivisionDimID,
		EDWSalesTerritoryAKID,
		EDWRegionalSalesManagerAKID,
		EDWSalesDirectorAKID
		FROM (
			SELECT 
				SalesDivisionDimID,
				EDWSalesTerritoryAKID,
				EDWRegionalSalesManagerAKID,
				EDWSalesDirectorAKID
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SalesDivisionDim
			WHERE CurrentSnapshotFlag=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWSalesTerritoryAKID,EDWRegionalSalesManagerAKID,EDWSalesDirectorAKID ORDER BY SalesDivisionDimID) = 1
	),
	EXP_GetDimIDs AS (
		SELECT
		LKP_InsuranceReferenceDim.InsuranceReferenceDimId AS i_InsuranceReferenceDimId,
		LKP_AgencyDim.AgencyDimID AS i_AgencyDimID,
		LKP_SalesDivisionDim.SalesDivisionDimID AS i_SalesDivisionDimID,
		LKP_InsuranceReferenceCoverageDim_PMS.InsuranceReferenceCoverageDimId AS i_InsuranceReferenceCoverageDimId_PMS,
		-- *INF*: IIF(ISNULL(i_InsuranceReferenceDimId), -1, i_InsuranceReferenceDimId)
		IFF(i_InsuranceReferenceDimId IS NULL, - 1, i_InsuranceReferenceDimId) AS o_InsuranceReferenceDimId,
		-- *INF*: IIF(ISNULL(i_AgencyDimID), -1, i_AgencyDimID)
		IFF(i_AgencyDimID IS NULL, - 1, i_AgencyDimID) AS o_AgencyDimID,
		-- *INF*: IIF(ISNULL(i_SalesDivisionDimID), -1, i_SalesDivisionDimID)
		IFF(i_SalesDivisionDimID IS NULL, - 1, i_SalesDivisionDimID) AS o_SalesDivisionDimID,
		-- *INF*: IIF(ISNULL(i_InsuranceReferenceCoverageDimId_PMS), -1,
		-- i_InsuranceReferenceCoverageDimId_PMS)
		IFF(i_InsuranceReferenceCoverageDimId_PMS IS NULL, - 1, i_InsuranceReferenceCoverageDimId_PMS) AS o_InsuranceReferenceCoverageDimId,
		-1 AS o_CoverageDetailDimId
		FROM 
		LEFT JOIN LKP_AgencyDim
		ON LKP_AgencyDim.EDWAgencyAKID = EXP_GetCodes.AgencyAKId AND LKP_AgencyDim.EffectiveDate <= EXP_GetCodes.trans_date AND LKP_AgencyDim.ExpirationDate >= EXP_GetCodes.trans_date
		LEFT JOIN LKP_InsuranceReferenceCoverageDim_PMS
		ON LKP_InsuranceReferenceCoverageDim_PMS.PmsMajorPerilCode = EXP_GetCodes.o_MajorPerilCode AND LKP_InsuranceReferenceCoverageDim_PMS.PmsProductTypeCode = EXP_GetCodes.o_ProductTypeCode AND LKP_InsuranceReferenceCoverageDim_PMS.InsuranceLineCode = EXP_GetCodes.o_StandardInsuranceLineCode AND LKP_InsuranceReferenceCoverageDim_PMS.PmsRiskUnitGroupCode = EXP_GetCodes.o_RiskUnitGroup AND LKP_InsuranceReferenceCoverageDim_PMS.PmsRiskUnitCode = EXP_GetCodes.o_RiskUnit
		LEFT JOIN LKP_InsuranceReferenceDim
		ON LKP_InsuranceReferenceDim.StrategicProfitCenterCode = EXP_GetCodes.o_StrategicProfitCenterCode AND LKP_InsuranceReferenceDim.InsuranceSegmentCode = EXP_GetCodes.o_InsuranceSegmentCode AND LKP_InsuranceReferenceDim.PolicyOfferingCode = EXP_GetCodes.o_PolicyOfferingCode AND LKP_InsuranceReferenceDim.ProductCode = EXP_GetCodes.o_ProductCode AND LKP_InsuranceReferenceDim.InsuranceReferenceLineOfBusinessCode = EXP_GetCodes.o_InsuranceReferenceLineOfBusinessCode AND LKP_InsuranceReferenceDim.EnterpriseGroupCode = EXP_GetCodes.o_EnterpriseGroupCode AND LKP_InsuranceReferenceDim.InsuranceReferenceLegalEntityCode = EXP_GetCodes.o_InsuranceReferenceLegalEntityCode AND LKP_InsuranceReferenceDim.RatingPlanCode = EXP_GetCodes.o_RatingPlanCode
		LEFT JOIN LKP_SalesDivisionDim
		ON LKP_SalesDivisionDim.EDWSalesTerritoryAKID = EXP_GetCodes.SalesTerritoryAKID AND LKP_SalesDivisionDim.EDWRegionalSalesManagerAKID = EXP_GetCodes.RegionalSalesManagerAKID AND LKP_SalesDivisionDim.EDWSalesDirectorAKID = EXP_GetCodes.SalesDirectorAKID
	),
	Output AS (
		SELECT
		o_InsuranceReferenceDimId AS InsuranceReferenceDimId, 
		o_AgencyDimID AS AgencyDimID, 
		o_SalesDivisionDimID AS SalesDivisionDimID, 
		o_InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId, 
		o_CoverageDetailDimId AS CoverageDetailDimId
		FROM EXP_GetDimIDs
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
EXP_Consolidate_Data AS (
	SELECT
	EXP_Loss_Master_Calc_Input.loss_master_calculation_id,
	EXP_Loss_Master_Calc_Input.incptn_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(incptn_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_incptn_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_incptn_date_id,
	-- *INF*: IIF(NOT ISNULL(v_incptn_date_id),v_incptn_date_id,-1)
	IFF(NOT v_incptn_date_id IS NULL, v_incptn_date_id, - 1) AS incptn_date_id_out,
	EXP_Loss_Master_Calc_Input.loss_master_run_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(loss_master_run_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_loss_master_run_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_loss_master_run_date_id,
	-- *INF*: IIF(NOT ISNULL(v_loss_master_run_date_id), v_loss_master_run_date_id, -1)
	IFF(NOT v_loss_master_run_date_id IS NULL, v_loss_master_run_date_id, - 1) AS loss_mater_run_date_id_out,
	mplt_Claim_occurence_dim_id.claim_loss_date AS mplt_claim_loss_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(mplt_claim_loss_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_mplt_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_loss_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_loss_date_id), v_claim_loss_date_id, -1)
	IFF(NOT v_claim_loss_date_id IS NULL, v_claim_loss_date_id, - 1) AS claim_loss_date_id_out,
	mplt_Claim_occurence_dim_id.source_claim_rpted_date AS mplt_source_claim_rpted_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(mplt_source_claim_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_mplt_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_mplt_source_claim_rpted_date,
	-- *INF*: IIF(NOT ISNULL(v_mplt_source_claim_rpted_date), v_mplt_source_claim_rpted_date, -1)
	IFF(NOT v_mplt_source_claim_rpted_date IS NULL, v_mplt_source_claim_rpted_date, - 1) AS source_claim_rpted_date_id,
	mplt_Claim_occurence_dim_id.claim_occurrence_rpted_date AS mplt_claim_occurrence_rpted_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(mplt_claim_occurrence_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_mplt_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_claim_occurrence_rpted_date_id,
	-- *INF*: IIF(NOT ISNULL(v_claim_occurrence_rpted_date_id), v_claim_occurrence_rpted_date_id, -1)
	IFF(NOT v_claim_occurrence_rpted_date_id IS NULL, v_claim_occurrence_rpted_date_id, - 1) AS claim_occurrence_rpted_date_id_out,
	mplt_Claim_occurence_dim_id.pol_eff_date AS mplt_pol_eff_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(mplt_pol_eff_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_mplt_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pol_eff_date_id,
	-- *INF*: IIF(NOT ISNULL(v_pol_eff_date_id), v_pol_eff_date_id, -1)
	IFF(NOT v_pol_eff_date_id IS NULL, v_pol_eff_date_id, - 1) AS pol_eff_date_id_out,
	mplt_Claim_occurence_dim_id.pol_exp_date AS mplt_pol_exp_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(to_date(to_char(mplt_pol_exp_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_to_date_to_char_mplt_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pol_exp_date_id,
	-- *INF*: IIF(NOT ISNULL(v_pol_exp_date_id), v_pol_exp_date_id, -1)
	IFF(NOT v_pol_exp_date_id IS NULL, v_pol_exp_date_id, - 1) AS pol_exp_date_id_out,
	EXP_Loss_Master_Calc_Input.claim_pay_ak_id,
	-- *INF*: :LKP.LKP_CLAIM_PAYMENT_DIM(claim_pay_ak_id,loss_master_run_date)
	LKP_CLAIM_PAYMENT_DIM_claim_pay_ak_id_loss_master_run_date.claim_pay_dim_id AS v_Internal_lkp_claim_pay_dim_id,
	-- *INF*: IIF(NOT ISNULL(v_Internal_lkp_claim_pay_dim_id), v_Internal_lkp_claim_pay_dim_id, -1)
	IFF(NOT v_Internal_lkp_claim_pay_dim_id IS NULL, v_Internal_lkp_claim_pay_dim_id, - 1) AS claim_pay_dim_id_out,
	mplt_Claim_occurence_dim_id.claim_occurrence_dim_id AS mplt_claim_occurrence_dim_id,
	-- *INF*: IIF(NOT ISNULL(mplt_claim_occurrence_dim_id), mplt_claim_occurrence_dim_id, -1)
	IFF(NOT mplt_claim_occurrence_dim_id IS NULL, mplt_claim_occurrence_dim_id, - 1) AS claim_occurrence_dim_id_out,
	EXP_Loss_Master_Calc_Input.claim_trans_pk_id,
	-- *INF*: IIF(NOT ISNULL(claim_trans_pk_id), claim_trans_pk_id, -1)
	IFF(NOT claim_trans_pk_id IS NULL, claim_trans_pk_id, - 1) AS claim_trans_pk_id_out,
	EXP_Loss_Master_Calc_Input.claim_reins_trans_pk_id,
	-- *INF*: IIF(NOT ISNULL(claim_reins_trans_pk_id), claim_reins_trans_pk_id, -1)
	IFF(NOT claim_reins_trans_pk_id IS NULL, claim_reins_trans_pk_id, - 1) AS claim_reins_trans_pk_id_out,
	mplt_Claim_occurence_dim_id.claim_rep_dim_prim_claim_rep_id AS mplt_claim_rep_dim_prim_claim_rep_id,
	-- *INF*: IIF(NOT ISNULL(mplt_claim_rep_dim_prim_claim_rep_id), mplt_claim_rep_dim_prim_claim_rep_id, -1)
	IFF(NOT mplt_claim_rep_dim_prim_claim_rep_id IS NULL, mplt_claim_rep_dim_prim_claim_rep_id, - 1) AS claim_rep_dim_prim_claim_rep_id_out,
	mplt_Claim_occurence_dim_id.claim_rep_dim_examiner_id AS mplt_claim_rep_dim_examiner_id,
	-- *INF*: IIF(NOT ISNULL(mplt_claim_rep_dim_examiner_id), mplt_claim_rep_dim_examiner_id, -1)
	IFF(NOT mplt_claim_rep_dim_examiner_id IS NULL, mplt_claim_rep_dim_examiner_id, - 1) AS claim_rep_dim_examiner_id_out,
	mplt_Claim_occurence_dim_id.pol_key_dim_id AS mplt_pol_key_dim_id,
	-- *INF*: IIF(NOT ISNULL(mplt_pol_key_dim_id), mplt_pol_key_dim_id, -1)
	IFF(NOT mplt_pol_key_dim_id IS NULL, mplt_pol_key_dim_id, - 1) AS pol_key_dim_id_out,
	mplt_Claim_occurence_dim_id.agency_dim_id AS mplt_agency_dim_id,
	-- *INF*: IIF(NOT ISNULL(mplt_agency_dim_id), mplt_agency_dim_id, -1)
	IFF(NOT mplt_agency_dim_id IS NULL, mplt_agency_dim_id, - 1) AS agency_dim_id_out,
	mplt_Claim_occurence_dim_id.claim_case_dim_id AS mplt_claim_case_dim_id,
	-- *INF*: IIF(NOT ISNULL(mplt_claim_case_dim_id), mplt_claim_case_dim_id, -1)
	IFF(NOT mplt_claim_case_dim_id IS NULL, mplt_claim_case_dim_id, - 1) AS claim_case_dim_id_out,
	mplt_Claim_occurence_dim_id.contract_cust_dim_id AS mplt_contract_cust_dim_id,
	-- *INF*: IIF(NOT ISNULL(mplt_contract_cust_dim_id), mplt_contract_cust_dim_id, -1)
	IFF(NOT mplt_contract_cust_dim_id IS NULL, mplt_contract_cust_dim_id, - 1) AS contract_cust_dim_id_out,
	LKP_reinsurance_coverage_dim1.reins_cov_dim_id AS lkp_reins_cov_dim_id,
	-- *INF*: IIF(NOT ISNULL(lkp_reins_cov_dim_id), lkp_reins_cov_dim_id, -1)
	IFF(NOT lkp_reins_cov_dim_id IS NULL, lkp_reins_cov_dim_id, - 1) AS reins_cov_dim_id_out,
	EXP_set_financial_values.source_sys_id,
	LKP_Loss_Master_Dim_Find_Dim_Id_DCT.loss_master_dim_id AS DCT_Loss_master_dim_id,
	LKP_Loss_Master_Dim_Find_Dim_Id.loss_master_dim_id AS PMS_Loss_master_dim_id,
	-- *INF*: DECODE(TRUE,
	-- IN(i_PolicySourceId,'PMS','ESU'),PMS_Loss_master_dim_id,
	-- IN(i_PolicySourceId, 'DUC','PDC'),DCT_Loss_master_dim_id,NULL)
	DECODE(TRUE,
		IN(i_PolicySourceId, 'PMS', 'ESU'), PMS_Loss_master_dim_id,
		IN(i_PolicySourceId, 'DUC', 'PDC'), DCT_Loss_master_dim_id,
		NULL) AS lkp_loss_master_dim_id,
	-- *INF*: IIF(NOT ISNULL(lkp_loss_master_dim_id), lkp_loss_master_dim_id, -1)
	IFF(NOT lkp_loss_master_dim_id IS NULL, lkp_loss_master_dim_id, - 1) AS loss_master_dim_id_out,
	mplt_coverage_dim_id.cov_dim_id AS mplt_cov_dim_id,
	-- *INF*: IIF(NOT ISNULL(mplt_cov_dim_id), mplt_cov_dim_id, -1)
	IFF(NOT mplt_cov_dim_id IS NULL, mplt_cov_dim_id, - 1) AS cov_dim_id_out,
	mplt_Claimant_dim_id.claimant_dim_id AS mplt_claimant_dim_id,
	-- *INF*: IIF(NOT ISNULL(mplt_claimant_dim_id), mplt_claimant_dim_id, -1)
	IFF(NOT mplt_claimant_dim_id IS NULL, mplt_claimant_dim_id, - 1) AS claimant_dim_id_out,
	mplt_claimant_coverage_dim_id.claimant_cov_dim_id AS mplt_claimant_cov_dim_id,
	-- *INF*: IIF(NOT ISNULL(mplt_claimant_cov_dim_id), mplt_claimant_cov_dim_id, -1)
	IFF(NOT mplt_claimant_cov_dim_id IS NULL, mplt_claimant_cov_dim_id, - 1) AS claimant_cov_dim_id_out,
	mplt_Claim_Payment_Category_type_Dim_id.claim_pay_ctgry_type_dim_id AS mplt_claim_pay_ctgry_type_dim_id,
	-- *INF*: IIF(NOT ISNULL(mplt_claim_pay_ctgry_type_dim_id), mplt_claim_pay_ctgry_type_dim_id, -1)
	IFF(NOT mplt_claim_pay_ctgry_type_dim_id IS NULL, mplt_claim_pay_ctgry_type_dim_id, - 1) AS claim_pay_ctgry_type_dim_id_out,
	lkp_claim_transaction_type_dim2.claim_trans_type_dim_id,
	-- *INF*: IIF(NOT ISNULL(claim_trans_type_dim_id), claim_trans_type_dim_id, -1)
	IFF(NOT claim_trans_type_dim_id IS NULL, claim_trans_type_dim_id, - 1) AS claim_trans_type_dim_id_out,
	EXP_Determine_Transaction_Values.trans_date_id_OUT AS trans_date_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID,
	-1 AS DEFAULT_ID,
	EXP_Loss_Master_Calc_Input.claim_trans_amt AS trans_amt,
	EXP_Loss_Master_Calc_Input.claim_trans_hist_amt AS trans_hist_amt,
	EXP_Determine_Transaction_Values.trans_code_OUT AS trans_code,
	EXP_Loss_Master_Calc_Input.pms_trans_code,
	EXP_Loss_Master_Calc_Input.new_claim_count,
	EXP_Loss_Master_Calc_Input.outstanding_amt,
	EXP_Loss_Master_Calc_Input.paid_loss_amt,
	EXP_Loss_Master_Calc_Input.paid_exp_amt,
	EXP_Loss_Master_Calc_Input.eom_unpaid_loss_adjust_exp,
	EXP_Loss_Master_Calc_Input.orig_reserve,
	-- *INF*: IIF(rtrim(ltrim(pms_trans_code))='95',eom_unpaid_loss_adjust_exp,orig_reserve)
	IFF(rtrim(ltrim(pms_trans_code)) = '95', eom_unpaid_loss_adjust_exp, orig_reserve) AS orig_reserve_extract,
	mplt_Strategic_Business_Division_Dim.strtgc_bus_dvsn_dim_id,
	EXP_Loss_Master_Calc_Input.o_PolicySourceId AS i_PolicySourceId,
	LKP_InsuranceReferenceCoverageDim_DCT.InsuranceReferenceCoverageDimId AS i_InsuranceReferenceCoverageDimId_DCT,
	mplt_PMS_Coverage_Agency_InsuranceReference_DimIDs.InsuranceReferenceDimId,
	mplt_PMS_Coverage_Agency_InsuranceReference_DimIDs.AgencyDimID AS agency_dim_id_V3,
	mplt_PMS_Coverage_Agency_InsuranceReference_DimIDs.SalesDivisionDimID,
	LKP_CoverageDetailDim.CoverageDetailDimId,
	-- *INF*: IIF(ISNULL(CoverageDetailDimId), -1, CoverageDetailDimId)
	IFF(CoverageDetailDimId IS NULL, - 1, CoverageDetailDimId) AS o_CoverageDetailDimId,
	mplt_PMS_Coverage_Agency_InsuranceReference_DimIDs.InsuranceReferenceCoverageDimId AS i_InsuranceReferenceCoverageDimId_PMS,
	-- *INF*: DECODE(TRUE,i_PolicySourceId='DUC' and NOT ISNULL(i_InsuranceReferenceCoverageDimId_DCT),i_InsuranceReferenceCoverageDimId_DCT,IN(i_PolicySourceId,'PMS','ESU') and NOT ISNULL(i_InsuranceReferenceCoverageDimId_PMS),i_InsuranceReferenceCoverageDimId_PMS,-1)
	DECODE(TRUE,
		i_PolicySourceId = 'DUC' AND NOT i_InsuranceReferenceCoverageDimId_DCT IS NULL, i_InsuranceReferenceCoverageDimId_DCT,
		IN(i_PolicySourceId, 'PMS', 'ESU') AND NOT i_InsuranceReferenceCoverageDimId_PMS IS NULL, i_InsuranceReferenceCoverageDimId_PMS,
		- 1) AS o_InsuranceReferenceCoverageDimId,
	EXP_Determine_Transaction_Values.is_claim_trans_pk_id_valid_OUT AS is_claim_trans_pk_id_valid,
	EXP_Determine_Transaction_Values.is_reins_trans_pk_id_valid_OUT AS is_reins_trans_pk_id_valid,
	EXP_set_financial_values.direct_loss_paid_excluding_recoveries AS i_DirectLossPaidExcludingRecoveries,
	EXP_set_financial_values.direct_loss_outstanding_excluding_recoveries AS i_DirectLossOutstandingExcludingRecoveries,
	EXP_set_financial_values.direct_loss_incurred_excluding_recoveries AS i_DirectLossIncurredExcludingRecoveries,
	EXP_set_financial_values.direct_alae_paid_excluding_recoveries AS i_DirectALAEPaidExcludingRecoveries,
	EXP_set_financial_values.direct_alae_outstanding_excluding_recoveries AS i_DirectALAEOutstandingExcludingRecoveries,
	EXP_set_financial_values.direct_loss_paid_including_recoveries AS i_DirectLossPaidIncludingRecoveries,
	EXP_set_financial_values.direct_loss_outstanding_including_recoveries AS i_DirectLossOutstandingIncludingRecoveries,
	EXP_set_financial_values.direct_loss_incurred_including_recoveries AS i_DirectLossIncurredIncludingRecoveries,
	EXP_set_financial_values.direct_alae_paid_including_recoveries AS i_DirectALAEPaidIncludingRecoveries,
	EXP_set_financial_values.direct_alae_incurred_including_recoveries AS i_DirectALAEIncurredIncludingRecoveries,
	EXP_set_financial_values.total_direct_loss_recovery_paid AS i_TotalDirectLossRecoveryPaid,
	-- *INF*: IIF(is_claim_trans_pk_id_valid=1,i_DirectLossPaidExcludingRecoveries,0)
	IFF(is_claim_trans_pk_id_valid = 1, i_DirectLossPaidExcludingRecoveries, 0) AS o_DirectLossPaidExcludingRecoveries,
	-- *INF*: IIF(is_claim_trans_pk_id_valid=1,i_DirectLossOutstandingExcludingRecoveries,0)
	IFF(is_claim_trans_pk_id_valid = 1, i_DirectLossOutstandingExcludingRecoveries, 0) AS o_DirectLossOutstandingExcludingRecoveries,
	-- *INF*: IIF(is_claim_trans_pk_id_valid=1,i_DirectLossIncurredExcludingRecoveries,0)
	IFF(is_claim_trans_pk_id_valid = 1, i_DirectLossIncurredExcludingRecoveries, 0) AS o_DirectLossIncurredExcludingRecoveries,
	-- *INF*: IIF(is_claim_trans_pk_id_valid=1,i_DirectALAEPaidExcludingRecoveries,0)
	IFF(is_claim_trans_pk_id_valid = 1, i_DirectALAEPaidExcludingRecoveries, 0) AS o_DirectALAEPaidExcludingRecoveries,
	-- *INF*: IIF(is_claim_trans_pk_id_valid=1,i_DirectALAEOutstandingExcludingRecoveries,0)
	IFF(is_claim_trans_pk_id_valid = 1, i_DirectALAEOutstandingExcludingRecoveries, 0) AS o_DirectALAEOutstandingExcludingRecoveries,
	-- *INF*: IIF(is_claim_trans_pk_id_valid=1,i_DirectLossPaidIncludingRecoveries,0)
	IFF(is_claim_trans_pk_id_valid = 1, i_DirectLossPaidIncludingRecoveries, 0) AS o_DirectLossPaidIncludingRecoveries,
	-- *INF*: IIF(is_claim_trans_pk_id_valid=1,i_DirectLossOutstandingIncludingRecoveries,0)
	IFF(is_claim_trans_pk_id_valid = 1, i_DirectLossOutstandingIncludingRecoveries, 0) AS o_DirectLossOutstandingIncludingRecoveries,
	-- *INF*: IIF(is_claim_trans_pk_id_valid=1,i_DirectLossIncurredIncludingRecoveries,0)
	IFF(is_claim_trans_pk_id_valid = 1, i_DirectLossIncurredIncludingRecoveries, 0) AS o_DirectLossIncurredIncludingRecoveries,
	-- *INF*: IIF(is_claim_trans_pk_id_valid=1,i_DirectALAEPaidIncludingRecoveries,0)
	IFF(is_claim_trans_pk_id_valid = 1, i_DirectALAEPaidIncludingRecoveries, 0) AS o_DirectALAEPaidIncludingRecoveries,
	-- *INF*: IIF(is_claim_trans_pk_id_valid=1,i_DirectALAEIncurredIncludingRecoveries,0)
	IFF(is_claim_trans_pk_id_valid = 1, i_DirectALAEIncurredIncludingRecoveries, 0) AS o_DirectALAEIncurredIncludingRecoveries,
	-- *INF*: IIF(is_claim_trans_pk_id_valid=1,i_TotalDirectLossRecoveryPaid,0)
	IFF(is_claim_trans_pk_id_valid = 1, i_TotalDirectLossRecoveryPaid, 0) AS o_TotalDirectLossRecoveryPaid,
	mplt_Claim_occurence_dim_id.AgencyDimID,
	0 AS o_ChangeInOutstandingAmount,
	0 AS o_ChangeInEOMUnpaidLossAdjustmentExpense,
	LKP_claim_financial_type_dim.claim_financial_type_dim_id,
	-- *INF*: IIF(ISNULL(claim_financial_type_dim_id), -1, claim_financial_type_dim_id)
	IFF(claim_financial_type_dim_id IS NULL, - 1, claim_financial_type_dim_id) AS o_ClaimFinancialTypeDimId
	FROM EXP_Determine_Transaction_Values
	 -- Manually join with EXP_Loss_Master_Calc_Input
	 -- Manually join with EXP_set_financial_values
	 -- Manually join with mplt_Claim_Payment_Category_type_Dim_id
	 -- Manually join with mplt_Claim_occurence_dim_id
	 -- Manually join with mplt_Claimant_dim_id
	 -- Manually join with mplt_PMS_Coverage_Agency_InsuranceReference_DimIDs
	 -- Manually join with mplt_Strategic_Business_Division_Dim
	 -- Manually join with mplt_claimant_coverage_dim_id
	 -- Manually join with mplt_coverage_dim_id
	LEFT JOIN LKP_CoverageDetailDim
	ON LKP_CoverageDetailDim.CoverageGuid = EXP_Loss_Master_Calc_Input.CoverageGUID AND LKP_CoverageDetailDim.PolicyAKID = EXP_Loss_Master_Calc_Input.PolicyAKID AND LKP_CoverageDetailDim.EffectiveDate <= EXP_Loss_Master_Calc_Input.loss_master_run_date_plus_one AND LKP_CoverageDetailDim.ExpirationDate >= EXP_Loss_Master_Calc_Input.loss_master_run_date_plus_one
	LEFT JOIN LKP_InsuranceReferenceCoverageDim_DCT
	ON LKP_InsuranceReferenceCoverageDim_DCT.InsuranceLineCode = EXP_Loss_Master_Calc_Input.o_StandardInsuranceLineCode AND LKP_InsuranceReferenceCoverageDim_DCT.DctRiskTypeCode = EXP_Loss_Master_Calc_Input.o_RiskType AND LKP_InsuranceReferenceCoverageDim_DCT.DctCoverageTypeCode = EXP_Loss_Master_Calc_Input.o_CoverageType AND LKP_InsuranceReferenceCoverageDim_DCT.DctPerilGroup = EXP_Loss_Master_Calc_Input.PerilGroup AND LKP_InsuranceReferenceCoverageDim_DCT.DctSubCoverageTypeCode = EXP_Loss_Master_Calc_Input.SubCoverageTypeCode AND LKP_InsuranceReferenceCoverageDim_DCT.DctCoverageVersion = EXP_Loss_Master_Calc_Input.CoverageVersion
	LEFT JOIN LKP_Loss_Master_Dim_Find_Dim_Id
	ON LKP_Loss_Master_Dim_Find_Dim_Id.variation_code = EXP_Loss_Master_Calc_Input.variation_code AND LKP_Loss_Master_Dim_Find_Dim_Id.pol_type = EXP_Loss_Master_Calc_Input.pol_type_out AND LKP_Loss_Master_Dim_Find_Dim_Id.auto_reins_facility = EXP_Loss_Master_Calc_Input.auto_reins_facility AND LKP_Loss_Master_Dim_Find_Dim_Id.statistical_brkdwn_line = EXP_Loss_Master_Calc_Input.statistical_brkdwn_line_out AND LKP_Loss_Master_Dim_Find_Dim_Id.statistical_code1 = EXP_Loss_Master_Calc_Input.statistical_code1_out AND LKP_Loss_Master_Dim_Find_Dim_Id.statistical_code2 = EXP_Loss_Master_Calc_Input.statistical_code2_out AND LKP_Loss_Master_Dim_Find_Dim_Id.statistical_code3 = EXP_Loss_Master_Calc_Input.statistical_code3_out AND LKP_Loss_Master_Dim_Find_Dim_Id.loss_master_cov_code = EXP_Loss_Master_Calc_Input.loss_master_cov_code_out AND LKP_Loss_Master_Dim_Find_Dim_Id.risk_state_prov_code = EXP_Loss_Master_Calc_Input.risk_state_prov_code AND LKP_Loss_Master_Dim_Find_Dim_Id.risk_zip_code = EXP_Loss_Master_Calc_Input.risk_zip_code_out AND LKP_Loss_Master_Dim_Find_Dim_Id.terr_code = EXP_Loss_Master_Calc_Input.terr_code AND LKP_Loss_Master_Dim_Find_Dim_Id.tax_loc = EXP_Loss_Master_Calc_Input.tax_loc_out AND LKP_Loss_Master_Dim_Find_Dim_Id.class_code = EXP_Loss_Master_Calc_Input.class_code_out AND LKP_Loss_Master_Dim_Find_Dim_Id.exposure = EXP_Loss_Master_Calc_Input.exposure AND LKP_Loss_Master_Dim_Find_Dim_Id.sub_line_code = EXP_Loss_Master_Calc_Input.sub_line_code_out AND LKP_Loss_Master_Dim_Find_Dim_Id.source_sar_asl = EXP_Loss_Master_Calc_Input.source_sar_asl_out AND LKP_Loss_Master_Dim_Find_Dim_Id.source_sar_sp_use_code = EXP_Loss_Master_Calc_Input.source_sar_sp_use_code_out AND LKP_Loss_Master_Dim_Find_Dim_Id.source_statistical_line = EXP_Loss_Master_Calc_Input.statistical_line_out
	LEFT JOIN LKP_Loss_Master_Dim_Find_Dim_Id_DCT
	ON LKP_Loss_Master_Dim_Find_Dim_Id_DCT.variation_code = EXP_Loss_Master_Calc_Input.Default_Value AND LKP_Loss_Master_Dim_Find_Dim_Id_DCT.pol_type = EXP_Loss_Master_Calc_Input.Default_Value AND LKP_Loss_Master_Dim_Find_Dim_Id_DCT.auto_reins_facility = EXP_Loss_Master_Calc_Input.Default_Value AND LKP_Loss_Master_Dim_Find_Dim_Id_DCT.statistical_brkdwn_line = EXP_Loss_Master_Calc_Input.Default_Value AND LKP_Loss_Master_Dim_Find_Dim_Id_DCT.statistical_code1 = EXP_Loss_Master_Calc_Input.Default_Value AND LKP_Loss_Master_Dim_Find_Dim_Id_DCT.statistical_code2 = EXP_Loss_Master_Calc_Input.Default_Value AND LKP_Loss_Master_Dim_Find_Dim_Id_DCT.statistical_code3 = EXP_Loss_Master_Calc_Input.Default_Value AND LKP_Loss_Master_Dim_Find_Dim_Id_DCT.loss_master_cov_code = EXP_Loss_Master_Calc_Input.Default_Value AND LKP_Loss_Master_Dim_Find_Dim_Id_DCT.risk_state_prov_code = EXP_Loss_Master_Calc_Input.risk_state_prov_code AND LKP_Loss_Master_Dim_Find_Dim_Id_DCT.risk_zip_code = EXP_Loss_Master_Calc_Input.risk_zip_code_out AND LKP_Loss_Master_Dim_Find_Dim_Id_DCT.terr_code = EXP_Loss_Master_Calc_Input.terr_code AND LKP_Loss_Master_Dim_Find_Dim_Id_DCT.tax_loc = EXP_Loss_Master_Calc_Input.tax_loc_out AND LKP_Loss_Master_Dim_Find_Dim_Id_DCT.class_code = EXP_Loss_Master_Calc_Input.class_code_out AND LKP_Loss_Master_Dim_Find_Dim_Id_DCT.exposure = EXP_Loss_Master_Calc_Input.exposure AND LKP_Loss_Master_Dim_Find_Dim_Id_DCT.sub_line_code = EXP_Loss_Master_Calc_Input.sub_line_code_out AND LKP_Loss_Master_Dim_Find_Dim_Id_DCT.source_sar_asl = EXP_Loss_Master_Calc_Input.Default_Value AND LKP_Loss_Master_Dim_Find_Dim_Id_DCT.source_sar_sp_use_code = EXP_Loss_Master_Calc_Input.Default_Value AND LKP_Loss_Master_Dim_Find_Dim_Id_DCT.source_statistical_line = EXP_Loss_Master_Calc_Input.Default_Value
	LEFT JOIN LKP_claim_financial_type_dim
	ON LKP_claim_financial_type_dim.financial_type_code = EXP_Loss_Master_Calc_Input.in_FinancialTypeCode
	LEFT JOIN LKP_reinsurance_coverage_dim1
	ON LKP_reinsurance_coverage_dim1.edw_reins_cov_ak_id = EXP_Loss_Master_Calc_Input.reins_cov_ak_id AND LKP_reinsurance_coverage_dim1.eff_from_date <= EXP_Loss_Master_Calc_Input.loss_master_run_date_plus_one AND LKP_reinsurance_coverage_dim1.eff_to_date >= EXP_Loss_Master_Calc_Input.loss_master_run_date_plus_one
	LEFT JOIN lkp_claim_transaction_type_dim2
	ON lkp_claim_transaction_type_dim2.type_disability = EXP_Determine_Transaction_Values.type_disability_OUT AND lkp_claim_transaction_type_dim2.pms_trans_code = EXP_Determine_Transaction_Values.pms_trans_code AND lkp_claim_transaction_type_dim2.trans_ctgry_code = EXP_Determine_Transaction_Values.trans_ctgry_code_OUT AND lkp_claim_transaction_type_dim2.offset_onset_ind = EXP_Determine_Transaction_Values.offset_onset_ind_OUT AND lkp_claim_transaction_type_dim2.trans_kind_code = EXP_Determine_Transaction_Values.LM_trans_kind_code AND lkp_claim_transaction_type_dim2.trans_rsn = EXP_Determine_Transaction_Values.trns_rsn_OUT
	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_incptn_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_incptn_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(incptn_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_loss_master_run_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_loss_master_run_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(loss_master_run_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_mplt_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_mplt_claim_loss_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(mplt_claim_loss_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_mplt_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_mplt_source_claim_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(mplt_source_claim_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_mplt_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_mplt_claim_occurrence_rpted_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(mplt_claim_occurrence_rpted_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_mplt_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_mplt_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(mplt_pol_eff_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_to_date_to_char_mplt_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_to_date_to_char_mplt_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = to_date(to_char(mplt_pol_exp_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CLAIM_PAYMENT_DIM LKP_CLAIM_PAYMENT_DIM_claim_pay_ak_id_loss_master_run_date
	ON LKP_CLAIM_PAYMENT_DIM_claim_pay_ak_id_loss_master_run_date.edw_claim_pay_ak_id = claim_pay_ak_id
	AND LKP_CLAIM_PAYMENT_DIM_claim_pay_ak_id_loss_master_run_date.eff_from_date = loss_master_run_date

),
EXP_order_for_target AS (
	SELECT
	AUDIT_ID,
	loss_master_calculation_id,
	claim_trans_pk_id_out,
	claim_reins_trans_pk_id_out,
	DEFAULT_ID,
	loss_master_dim_id_out,
	claim_occurrence_dim_id_out,
	claimant_dim_id_out,
	claimant_cov_dim_id_out,
	cov_dim_id_out,
	claim_trans_type_dim_id_out,
	reins_cov_dim_id_out,
	claim_rep_dim_prim_claim_rep_id_out,
	claim_rep_dim_examiner_id_out,
	pol_key_dim_id_out,
	contract_cust_dim_id_out,
	agency_dim_id_out,
	claim_pay_dim_id_out,
	claim_pay_ctgry_type_dim_id_out,
	claim_case_dim_id_out,
	trans_date_id,
	pol_eff_date_id_out,
	pol_exp_date_id_out,
	source_claim_rpted_date_id,
	claim_occurrence_rpted_date_id_out,
	claim_loss_date_id_out,
	incptn_date_id_out,
	loss_mater_run_date_id_out,
	trans_amt,
	trans_hist_amt,
	new_claim_count,
	outstanding_amt,
	paid_loss_amt,
	paid_exp_amt,
	eom_unpaid_loss_adjust_exp,
	orig_reserve,
	orig_reserve_extract,
	strtgc_bus_dvsn_dim_id,
	InsuranceReferenceDimId,
	agency_dim_id_V3 AS AgencyDimID,
	SalesDivisionDimID,
	o_InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId,
	o_CoverageDetailDimId AS CoverageDetailDimId_out,
	o_DirectLossPaidExcludingRecoveries AS DirectLossPaidExcludingRecoveries,
	o_DirectLossOutstandingExcludingRecoveries AS DirectLossOutstandingExcludingRecoveries,
	o_DirectLossIncurredExcludingRecoveries AS DirectLossIncurredExcludingRecoveries,
	o_DirectALAEPaidExcludingRecoveries AS DirectALAEPaidExcludingRecoveries,
	o_DirectALAEOutstandingExcludingRecoveries AS DirectALAEOutstandingExcludingRecoveries,
	o_DirectLossPaidIncludingRecoveries AS DirectLossPaidIncludingRecoveries,
	o_DirectLossOutstandingIncludingRecoveries AS DirectLossOutstandingIncludingRecoveries,
	o_DirectLossIncurredIncludingRecoveries AS DirectLossIncurredIncludingRecoveries,
	o_DirectALAEPaidIncludingRecoveries AS DirectALAEPaidIncludingRecoveries,
	o_DirectALAEIncurredIncludingRecoveries AS DirectALAEIncurredIncludingRecoveries,
	o_TotalDirectLossRecoveryPaid AS TotalDirectLossRecoveryPaid,
	o_ChangeInOutstandingAmount AS ChangeInOutstandingAmount,
	o_ChangeInEOMUnpaidLossAdjustmentExpense AS ChangeInEOMUnpaidLossAdjustmentExpense,
	o_ClaimFinancialTypeDimId AS ClaimFinancialTypeDimId
	FROM EXP_Consolidate_Data
),
LKP_Loss_Master_Fact AS (
	SELECT
	loss_master_fact_id,
	edw_loss_master_calculation_pk_id
	FROM (
		SELECT 
			loss_master_fact_id,
			edw_loss_master_calculation_pk_id
		FROM loss_master_fact
		WHERE audit_id <> -50 AND edw_loss_master_calculation_pk_id <> -1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_loss_master_calculation_pk_id ORDER BY loss_master_fact_id DESC) = 1
),
RTR_INSERT_UPDATE AS (
	SELECT
	LKP_Loss_Master_Fact.loss_master_fact_id,
	EXP_order_for_target.AUDIT_ID,
	EXP_order_for_target.loss_master_calculation_id,
	EXP_order_for_target.claim_trans_pk_id_out,
	EXP_order_for_target.claim_reins_trans_pk_id_out,
	EXP_order_for_target.DEFAULT_ID,
	EXP_order_for_target.loss_master_dim_id_out,
	EXP_order_for_target.claim_occurrence_dim_id_out,
	EXP_order_for_target.claimant_dim_id_out,
	EXP_order_for_target.claimant_cov_dim_id_out,
	EXP_order_for_target.cov_dim_id_out,
	EXP_order_for_target.claim_trans_type_dim_id_out,
	EXP_order_for_target.reins_cov_dim_id_out,
	EXP_order_for_target.claim_rep_dim_prim_claim_rep_id_out,
	EXP_order_for_target.claim_rep_dim_examiner_id_out,
	EXP_order_for_target.pol_key_dim_id_out,
	EXP_order_for_target.contract_cust_dim_id_out,
	EXP_order_for_target.agency_dim_id_out,
	EXP_order_for_target.claim_pay_dim_id_out,
	EXP_order_for_target.claim_pay_ctgry_type_dim_id_out,
	EXP_order_for_target.claim_case_dim_id_out,
	EXP_order_for_target.trans_date_id,
	EXP_order_for_target.pol_eff_date_id_out,
	EXP_order_for_target.pol_exp_date_id_out,
	EXP_order_for_target.source_claim_rpted_date_id,
	EXP_order_for_target.claim_occurrence_rpted_date_id_out,
	EXP_order_for_target.claim_loss_date_id_out,
	EXP_order_for_target.incptn_date_id_out,
	EXP_order_for_target.loss_mater_run_date_id_out,
	EXP_order_for_target.trans_amt,
	EXP_order_for_target.trans_hist_amt,
	EXP_order_for_target.new_claim_count,
	EXP_order_for_target.outstanding_amt,
	EXP_order_for_target.paid_loss_amt,
	EXP_order_for_target.paid_exp_amt,
	EXP_order_for_target.eom_unpaid_loss_adjust_exp,
	EXP_order_for_target.orig_reserve,
	EXP_order_for_target.orig_reserve_extract,
	EXP_order_for_target.strtgc_bus_dvsn_dim_id,
	EXP_order_for_target.InsuranceReferenceDimId,
	EXP_order_for_target.AgencyDimID,
	EXP_order_for_target.SalesDivisionDimID,
	EXP_order_for_target.InsuranceReferenceCoverageDimId,
	EXP_order_for_target.CoverageDetailDimId_out,
	EXP_order_for_target.DirectLossPaidExcludingRecoveries,
	EXP_order_for_target.DirectLossOutstandingExcludingRecoveries,
	EXP_order_for_target.DirectLossIncurredExcludingRecoveries,
	EXP_order_for_target.DirectALAEPaidExcludingRecoveries,
	EXP_order_for_target.DirectALAEOutstandingExcludingRecoveries,
	EXP_order_for_target.DirectLossPaidIncludingRecoveries,
	EXP_order_for_target.DirectLossOutstandingIncludingRecoveries,
	EXP_order_for_target.DirectLossIncurredIncludingRecoveries,
	EXP_order_for_target.DirectALAEPaidIncludingRecoveries,
	EXP_order_for_target.DirectALAEIncurredIncludingRecoveries,
	EXP_order_for_target.TotalDirectLossRecoveryPaid,
	EXP_order_for_target.ChangeInOutstandingAmount,
	EXP_order_for_target.ChangeInEOMUnpaidLossAdjustmentExpense,
	EXP_order_for_target.ClaimFinancialTypeDimId
	FROM EXP_order_for_target
	LEFT JOIN LKP_Loss_Master_Fact
	ON LKP_Loss_Master_Fact.edw_loss_master_calculation_pk_id = EXP_order_for_target.loss_master_calculation_id
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE ISNULL(loss_master_fact_id)),
RTR_INSERT_UPDATE_DEFAULT1 AS (SELECT * FROM RTR_INSERT_UPDATE WHERE NOT ( (ISNULL(loss_master_fact_id)) )),
UPD_UPDATE AS (
	SELECT
	loss_master_fact_id AS loss_master_fact_id2, 
	AUDIT_ID AS AUDIT_ID2, 
	loss_master_calculation_id AS loss_master_calculation_id2, 
	claim_trans_pk_id_out AS claim_trans_pk_id_out2, 
	claim_reins_trans_pk_id_out AS claim_reins_trans_pk_id_out2, 
	DEFAULT_ID AS DEFAULT_ID2, 
	loss_master_dim_id_out AS loss_master_dim_id_out2, 
	claim_occurrence_dim_id_out AS claim_occurrence_dim_id_out2, 
	claimant_dim_id_out AS claimant_dim_id_out2, 
	claimant_cov_dim_id_out AS claimant_cov_dim_id_out2, 
	cov_dim_id_out AS cov_dim_id_out2, 
	claim_trans_type_dim_id_out AS claim_trans_type_dim_id_out2, 
	reins_cov_dim_id_out AS reins_cov_dim_id_out2, 
	claim_rep_dim_prim_claim_rep_id_out AS claim_rep_dim_prim_claim_rep_id_out2, 
	claim_rep_dim_examiner_id_out AS claim_rep_dim_examiner_id_out2, 
	pol_key_dim_id_out AS pol_key_dim_id_out2, 
	contract_cust_dim_id_out AS contract_cust_dim_id_out2, 
	agency_dim_id_out AS agency_dim_id_out2, 
	claim_pay_dim_id_out AS claim_pay_dim_id_out2, 
	claim_pay_ctgry_type_dim_id_out AS claim_pay_ctgry_type_dim_id_out2, 
	claim_case_dim_id_out AS claim_case_dim_id_out2, 
	trans_date_id AS trans_date_id2, 
	pol_eff_date_id_out AS pol_eff_date_id_out2, 
	pol_exp_date_id_out AS pol_exp_date_id_out2, 
	source_claim_rpted_date_id AS source_claim_rpted_date_id2, 
	claim_occurrence_rpted_date_id_out AS claim_occurrence_rpted_date_id_out2, 
	claim_loss_date_id_out AS claim_loss_date_id_out2, 
	incptn_date_id_out AS incptn_date_id_out2, 
	loss_mater_run_date_id_out AS loss_mater_run_date_id_out2, 
	trans_amt AS trans_amt2, 
	trans_hist_amt AS trans_hist_amt2, 
	new_claim_count AS new_claim_count2, 
	outstanding_amt AS outstanding_amt2, 
	paid_loss_amt AS paid_loss_amt2, 
	paid_exp_amt AS paid_exp_amt2, 
	eom_unpaid_loss_adjust_exp AS eom_unpaid_loss_adjust_exp2, 
	orig_reserve AS orig_reserve2, 
	orig_reserve_extract AS orig_reserve_extract2, 
	strtgc_bus_dvsn_dim_id AS strtgc_bus_dvsn_dim_id2, 
	InsuranceReferenceDimId AS InsuranceReferenceDimId2, 
	AgencyDimID AS AgencyDimID2, 
	SalesDivisionDimID AS SalesDivisionDimID2, 
	InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId2, 
	CoverageDetailDimId_out AS CoverageDetailDimId_out2, 
	DirectLossPaidExcludingRecoveries, 
	DirectLossOutstandingExcludingRecoveries, 
	DirectLossIncurredExcludingRecoveries, 
	DirectALAEPaidExcludingRecoveries, 
	DirectALAEOutstandingExcludingRecoveries, 
	DirectLossPaidIncludingRecoveries, 
	DirectLossOutstandingIncludingRecoveries, 
	DirectLossIncurredIncludingRecoveries, 
	DirectALAEPaidIncludingRecoveries, 
	DirectALAEIncurredIncludingRecoveries, 
	TotalDirectLossRecoveryPaid
	FROM RTR_INSERT_UPDATE_DEFAULT1
),
TGT_loss_master_fact_Update AS (
	MERGE INTO loss_master_fact AS T
	USING UPD_UPDATE AS S
	ON T.loss_master_fact_id = S.loss_master_fact_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.edw_loss_master_calculation_pk_id = S.loss_master_calculation_id2, T.edw_claim_trans_pk_id = S.claim_trans_pk_id_out2, T.edw_claim_reins_trans_pk_id = S.claim_reins_trans_pk_id_out2, T.wc_stage_loss_master_pk_id = S.DEFAULT_ID2, T.loss_master_dim_id = S.loss_master_dim_id_out2, T.claim_occurrence_dim_id = S.claim_occurrence_dim_id_out2, T.claimant_dim_id = S.claimant_dim_id_out2, T.claimant_cov_dim_id = S.claimant_cov_dim_id_out2, T.cov_dim_id = S.cov_dim_id_out2, T.claim_trans_type_dim_id = S.claim_trans_type_dim_id_out2, T.reins_cov_dim_id = S.reins_cov_dim_id_out2, T.claim_rep_dim_prim_claim_rep_id = S.claim_rep_dim_prim_claim_rep_id_out2, T.claim_rep_dim_examiner_id = S.claim_rep_dim_examiner_id_out2, T.pol_dim_id = S.pol_key_dim_id_out2, T.contract_cust_dim_id = S.contract_cust_dim_id_out2, T.agency_dim_id = S.agency_dim_id_out2, T.claim_pay_dim_id = S.claim_pay_dim_id_out2, T.claim_pay_ctgry_type_dim_id = S.claim_pay_ctgry_type_dim_id_out2, T.claim_case_dim_id = S.claim_case_dim_id_out2, T.claim_trans_date_id = S.trans_date_id2, T.pol_eff_date_id = S.pol_eff_date_id_out2, T.pol_exp_date_id = S.pol_exp_date_id_out2, T.source_claim_rpted_date_id = S.source_claim_rpted_date_id2, T.claim_rpted_date_id = S.claim_occurrence_rpted_date_id_out2, T.claim_loss_date_id = S.claim_loss_date_id_out2, T.incptn_date_id = S.incptn_date_id_out2, T.loss_master_run_date_id = S.loss_mater_run_date_id_out2, T.claim_trans_amt = S.trans_amt2, T.claim_trans_hist_amt = S.trans_hist_amt2, T.new_claim_count = S.new_claim_count2, T.outstanding_amt = S.outstanding_amt2, T.paid_loss_amt = S.paid_loss_amt2, T.paid_exp_amt = S.paid_exp_amt2, T.eom_unpaid_loss_adjust_exp = S.eom_unpaid_loss_adjust_exp2, T.orig_reserve = S.orig_reserve2, T.orig_reserve_extract = S.orig_reserve_extract2, T.asl_dim_id = S.DEFAULT_ID2, T.asl_prdct_code_dim_id = S.DEFAULT_ID2, T.strtgc_bus_dvsn_dim_id = S.strtgc_bus_dvsn_dim_id2, T.prdct_code_dim_id = S.DEFAULT_ID2, T.InsuranceReferenceDimId = S.InsuranceReferenceDimId2, T.AgencyDimId = S.AgencyDimID2, T.SalesDivisionDimId = S.SalesDivisionDimID2, T.InsuranceReferenceCoverageDimId = S.InsuranceReferenceCoverageDimId2, T.CoverageDetailDimId = S.CoverageDetailDimId_out2
),
TGT_loss_master_fact_Insert AS (
	INSERT INTO loss_master_fact
	(audit_id, edw_loss_master_calculation_pk_id, edw_claim_trans_pk_id, edw_claim_reins_trans_pk_id, wc_stage_loss_master_pk_id, loss_master_dim_id, claim_occurrence_dim_id, claimant_dim_id, claimant_cov_dim_id, cov_dim_id, claim_trans_type_dim_id, reins_cov_dim_id, claim_rep_dim_prim_claim_rep_id, claim_rep_dim_examiner_id, pol_dim_id, contract_cust_dim_id, agency_dim_id, claim_pay_dim_id, claim_pay_ctgry_type_dim_id, claim_case_dim_id, claim_trans_date_id, pol_eff_date_id, pol_exp_date_id, source_claim_rpted_date_id, claim_rpted_date_id, claim_loss_date_id, incptn_date_id, loss_master_run_date_id, claim_trans_amt, claim_trans_hist_amt, new_claim_count, outstanding_amt, paid_loss_amt, paid_exp_amt, eom_unpaid_loss_adjust_exp, orig_reserve, orig_reserve_extract, asl_dim_id, asl_prdct_code_dim_id, strtgc_bus_dvsn_dim_id, prdct_code_dim_id, InsuranceReferenceDimId, AgencyDimId, SalesDivisionDimId, InsuranceReferenceCoverageDimId, CoverageDetailDimId, ChangeInOutstandingAmount, ChangeInEOMUnpaidLossAdjustmentExpense, ClaimFinancialTypeDimId)
	SELECT 
	AUDIT_ID AS AUDIT_ID, 
	loss_master_calculation_id AS EDW_LOSS_MASTER_CALCULATION_PK_ID, 
	claim_trans_pk_id_out AS EDW_CLAIM_TRANS_PK_ID, 
	claim_reins_trans_pk_id_out AS EDW_CLAIM_REINS_TRANS_PK_ID, 
	DEFAULT_ID AS WC_STAGE_LOSS_MASTER_PK_ID, 
	loss_master_dim_id_out AS LOSS_MASTER_DIM_ID, 
	claim_occurrence_dim_id_out AS CLAIM_OCCURRENCE_DIM_ID, 
	claimant_dim_id_out AS CLAIMANT_DIM_ID, 
	claimant_cov_dim_id_out AS CLAIMANT_COV_DIM_ID, 
	cov_dim_id_out AS COV_DIM_ID, 
	claim_trans_type_dim_id_out AS CLAIM_TRANS_TYPE_DIM_ID, 
	reins_cov_dim_id_out AS REINS_COV_DIM_ID, 
	claim_rep_dim_prim_claim_rep_id_out AS CLAIM_REP_DIM_PRIM_CLAIM_REP_ID, 
	claim_rep_dim_examiner_id_out AS CLAIM_REP_DIM_EXAMINER_ID, 
	pol_key_dim_id_out AS POL_DIM_ID, 
	contract_cust_dim_id_out AS CONTRACT_CUST_DIM_ID, 
	agency_dim_id_out AS AGENCY_DIM_ID, 
	claim_pay_dim_id_out AS CLAIM_PAY_DIM_ID, 
	claim_pay_ctgry_type_dim_id_out AS CLAIM_PAY_CTGRY_TYPE_DIM_ID, 
	claim_case_dim_id_out AS CLAIM_CASE_DIM_ID, 
	trans_date_id AS CLAIM_TRANS_DATE_ID, 
	pol_eff_date_id_out AS POL_EFF_DATE_ID, 
	pol_exp_date_id_out AS POL_EXP_DATE_ID, 
	SOURCE_CLAIM_RPTED_DATE_ID, 
	claim_occurrence_rpted_date_id_out AS CLAIM_RPTED_DATE_ID, 
	claim_loss_date_id_out AS CLAIM_LOSS_DATE_ID, 
	incptn_date_id_out AS INCPTN_DATE_ID, 
	loss_mater_run_date_id_out AS LOSS_MASTER_RUN_DATE_ID, 
	trans_amt AS CLAIM_TRANS_AMT, 
	trans_hist_amt AS CLAIM_TRANS_HIST_AMT, 
	NEW_CLAIM_COUNT, 
	OUTSTANDING_AMT, 
	PAID_LOSS_AMT, 
	PAID_EXP_AMT, 
	EOM_UNPAID_LOSS_ADJUST_EXP, 
	ORIG_RESERVE, 
	ORIG_RESERVE_EXTRACT, 
	DEFAULT_ID AS ASL_DIM_ID, 
	DEFAULT_ID AS ASL_PRDCT_CODE_DIM_ID, 
	STRTGC_BUS_DVSN_DIM_ID, 
	DEFAULT_ID AS PRDCT_CODE_DIM_ID, 
	INSURANCEREFERENCEDIMID, 
	AgencyDimID AS AGENCYDIMID, 
	SalesDivisionDimID AS SALESDIVISIONDIMID, 
	INSURANCEREFERENCECOVERAGEDIMID, 
	CoverageDetailDimId_out AS COVERAGEDETAILDIMID, 
	CHANGEINOUTSTANDINGAMOUNT, 
	CHANGEINEOMUNPAIDLOSSADJUSTMENTEXPENSE, 
	CLAIMFINANCIALTYPEDIMID
	FROM RTR_INSERT_UPDATE_INSERT
),