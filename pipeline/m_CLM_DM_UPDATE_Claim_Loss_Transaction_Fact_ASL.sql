WITH
SQ_claim_reinsurance_transaction AS (
	SELECT
	CRT.source_sys_id, 
	CRT.claimant_cov_det_ak_id, 
	CRT.sar_id, 
	CRT.claim_reins_financial_type_code, 
	CRT.trans_ctgry_code, 
	CLTF.claim_loss_trans_fact_id,
	CLTF.claim_trans_type_dim_id as wc_stage_pk_id,
	CLTF.InsuranceReferenceDimId,
	CLTF.AgencyDimId,
	CLTF.SalesDivisionDimId,
	CLTF.InsuranceReferenceCoverageDimId,
	CLTF.CoverageDetailDimId 
	FROM   
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_reinsurance_transaction CRT,
		@{pipeline().parameters.DB_NAME_DATAMART}.@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_loss_transaction_fact CLTF   
	WHERE  
	 	CRT.claim_reins_trans_id = CLTF.edw_claim_reins_trans_pk_id 
	       AND CRT.crrnt_snpsht_flag = 1
	       AND 
			(CLTF.asl_dim_id in (0,-1) or 
			CLTF.asl_prdct_code_dim_id in (0,-1) or 
			CLTF.loss_master_dim_id in (0,-1) or
			CLTF.prdct_code_dim_id in (0,-1)
			)
),
EXP_reinsurance_input AS (
	SELECT
	claimant_cov_det_ak_id,
	sar_id,
	claim_reins_financial_type_code,
	trans_ctgry_code,
	source_sys_id,
	claim_loss_trans_fact_id,
	-1 AS default_id,
	'N/A' AS default_value,
	0 AS default_amount,
	SYSDATE AS default_date,
	claim_trans_type_dim_id AS claim_loss_fact_trans_type_dim_id,
	InsuranceReferenceDimId,
	AgencyDimId,
	SalesDivisionDimId,
	InsuranceReferenceCoverageDimId,
	CoverageDetailDimId
	FROM SQ_claim_reinsurance_transaction
),
SQ_claim_transaction_PMSClaims AS (
	SELECT 
	CT.claimant_cov_det_ak_id, 
	CT.claim_pay_ak_id, 
	CT.sar_id, 
	CT.financial_type_code, 
	CT.trans_code, 
	CT.trans_date, 
	CT.trans_ctgry_code, 
	CT.trans_amt, 
	CT.trans_hist_amt, 
	CT.source_sys_id, 
	CLTF.claim_loss_trans_fact_id,
	CLTF.claim_trans_type_dim_id as wc_stage_pk_id,
	CLTF.InsuranceReferenceDimId,
	CLTF.AgencyDimId,
	CLTF.SalesDivisionDimId,
	CLTF.InsuranceReferenceCoverageDimId,
	CLTF.CoverageDetailDimId 
	FROM   
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction CT,
		@{pipeline().parameters.DB_NAME_DATAMART}.@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_loss_transaction_fact CLTF  ,
	     @{pipeline().parameters.DB_NAME_DATAMART}.@{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim PD 
	WHERE  
	 CT.claim_trans_id = CLTF.edw_claim_trans_pk_id 
	 AND CLTF.pol_dim_id = PD.pol_dim_id    AND CT.crrnt_snpsht_flag = 1
	      AND 
			(CLTF.asl_dim_id in (0,-1) or 
			CLTF.asl_prdct_code_dim_id in (0,-1) or 
			CLTF.loss_master_dim_id in (0,-1) or
			CLTF.prdct_code_dim_id in (0,-1)
			)
	AND PD.pol_sym <> '000' 
	---- Reading Claim transactions of PMS policies.
	
	UNION 
	
	--- Dummy Transactions mappings delete the rows from claim_loss_transaction_fact with audit_id < 0. So we need to insert Major_Peril 50 --- rows on a daily basis. Below query pull the rows from Claim_transaction and Claim_loss_Transaction_fact 
	
	SELECT 
	CT.claimant_cov_det_ak_id, 
	CT.claim_pay_ak_id, 
	CT.sar_id, 
	CT.financial_type_code, 
	CT.trans_code, 
	CT.trans_date, 
	CT.trans_ctgry_code, 
	CT.trans_amt, 
	CT.trans_hist_amt, 
	CT.source_sys_id, 
	CLTF.claim_loss_trans_fact_id,
	CLTF.claim_trans_type_dim_id as wc_stage_pk_id,
	CLTF.InsuranceReferenceDimId,
	CLTF.AgencyDimId,
	CLTF.SalesDivisionDimId,
	CLTF.InsuranceReferenceCoverageDimId,
	CLTF.CoverageDetailDimId
	FROM
	@{pipeline().parameters.DB_NAME_DATAMART}.@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_loss_transaction_fact CLTF , 
	@{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.TARGET_TABLE_OWNER}.VW_claim_transaction CT , 
	@{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail CCD
	WHERE CLTF.edw_claim_trans_pk_id = CT.claim_trans_id
	AND CT.claimant_cov_det_ak_id = CCD.claimant_cov_det_ak_id
	AND CCD.major_peril_code = '050' AND CLTF.audit_id > 0
),
EXP_source_input AS (
	SELECT
	claimant_cov_det_ak_id,
	claim_pay_ak_id,
	sar_id,
	financial_type_code,
	trans_code,
	trans_date,
	trans_ctgry_code,
	trans_amt,
	trans_hist_amt,
	source_sys_id,
	claim_loss_trans_fact_id,
	claim_trans_type_dim_id AS claim_loss_fact_trans_type_dim_id,
	InsuranceReferenceDimId,
	AgencyDimId,
	SalesDivisionDimId,
	InsuranceReferenceCoverageDimId,
	CoverageDetailDimId
	FROM SQ_claim_transaction_PMSClaims
),
Union AS (
	SELECT claimant_cov_det_ak_id, claim_pay_ak_id, sar_id, claim_loss_trans_fact_id, financial_type_code, trans_code, trans_date, trans_ctgry_code, trans_amt, trans_hist_amt, source_sys_id, claim_loss_fact_trans_type_dim_id AS claim_loss_trans_type_dim_id, InsuranceReferenceDimId, AgencyDimId, SalesDivisionDimId, InsuranceReferenceCoverageDimId, CoverageDetailDimId
	FROM EXP_source_input
	UNION
	SELECT claimant_cov_det_ak_id, default_id AS claim_pay_ak_id, sar_id, claim_loss_trans_fact_id, default_value AS financial_type_code, default_value AS trans_code, default_date AS trans_date, default_value AS trans_ctgry_code, default_amount AS trans_amt, default_amount AS trans_hist_amt, source_sys_id, claim_loss_fact_trans_type_dim_id AS claim_loss_trans_type_dim_id, InsuranceReferenceDimId, AgencyDimId, SalesDivisionDimId, InsuranceReferenceCoverageDimId, CoverageDetailDimId
	FROM EXP_reinsurance_input
),
EXP_merged_union_input_source AS (
	SELECT
	claimant_cov_det_ak_id,
	claim_pay_ak_id,
	sar_id,
	claim_loss_trans_fact_id,
	financial_type_code,
	trans_code,
	trans_date,
	trans_ctgry_code,
	trans_amt,
	trans_hist_amt,
	source_sys_id,
	claim_loss_trans_type_dim_id,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	-- 
	-- 
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS v_trans_date_12312100,
	v_trans_date_12312100 AS trans_date_12312100,
	InsuranceReferenceDimId,
	AgencyDimId,
	SalesDivisionDimId,
	InsuranceReferenceCoverageDimId,
	CoverageDetailDimId
	FROM Union
),
LKP_Claimant_Coverage_Detail_PMSClaims AS (
	SELECT
	pol_ak_id,
	pol_key,
	mco,
	pol_eff_date,
	pms_pol_lob_code,
	ClassOfBusiness,
	variation_code,
	Claim_Loss_Date,
	loc_unit_num,
	sub_loc_unit_num,
	ins_line,
	risk_unit_grp,
	risk_unit_grp_seq_num,
	risk_unit,
	risk_unit_seq_num,
	risk_type_ind,
	major_peril_code,
	major_peril_seq,
	pms_loss_disability,
	reserve_ctgry,
	cause_of_loss,
	pms_type_exposure,
	pms_type_bureau_code,
	claimant_cov_det_ak_id,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
		P.pol_ak_id as pol_ak_id,
		P.pol_key             AS pol_key,
		P.mco                    AS mco,
		P.pol_eff_date        AS pol_eff_date,
		P.pms_pol_lob_code    AS pms_pol_lob_code,
		P.ClassOfBusiness as ClassOfBusiness,
		P.variation_code      AS variation_code,
		CO.claim_loss_date as Claim_Loss_Date,  
		Ab.loc_unit_num                 AS Loc_unit_num, 
		Ab.sub_loc_unit_num             AS Sub_loc_unit_num,
		Ab.ins_line                     AS Ins_line,  
		Ab.risk_unit_grp                AS Risk_unit_grp, 
		Ab.risk_unit_grp_seq_num        AS Risk_unit_grp_seq_num,  
		Ab.risk_unit                    AS Risk_unit, 
		Ab.risk_unit_seq_num            AS Risk_unit_seq_num, 
		Ab.risk_type_ind                AS Risk_type_ind,
		Ab.major_peril_code             AS Major_peril_code,  
		Ab.major_peril_seq              AS Major_peril_seq, 
		Ab.pms_loss_disability          AS Pms_loss_disability,  
		Ab.reserve_ctgry                AS Reserve_ctgry, 
		Ab.cause_of_loss                AS Cause_of_loss, 
		Ab.pms_type_exposure            AS Pms_type_exposure,  
		Ab.pms_type_bureau_code         AS Pms_type_bureau_code, 
		Ab.claimant_cov_det_ak_id       AS Claimant_cov_det_ak_id, 
		Ab.eff_from_date                AS Eff_from_date,  
		Ab.eff_to_date                  AS Eff_to_date 
		FROM DBO.CLAIMANT_COVERAGE_DETAIL Ab 
		INNER JOIN dbo.claim_party_occurrence CPO ON CPO.claim_party_occurrence_ak_id = AB.claim_party_occurrence_ak_id and CPO.crrnt_snpsht_flag=1
		INNER JOIN dbo.claim_occurrence CO on CPO.claim_occurrence_ak_id = CO.claim_occurrence_ak_id and CO.crrnt_snpsht_flag =1
		INNER JOIN V2.policy P on P.pol_ak_id  = CO.pol_key_ak_id and P.crrnt_snpsht_flag = 1
		WHERE ab.PolicySourceID IN ('ESU','PMS')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,eff_from_date,eff_to_date ORDER BY pol_ak_id) = 1
),
EXP_policy_claim_attribute_outputs AS (
	SELECT
	LKP_Claimant_Coverage_Detail_PMSClaims.pol_ak_id,
	LKP_Claimant_Coverage_Detail_PMSClaims.pol_key,
	-- *INF*: substr(pol_key,1,3)
	substr(pol_key, 1, 3
	) AS pol_symbol,
	-- *INF*: substr(pol_key,4,7)
	substr(pol_key, 4, 7
	) AS pol_number,
	-- *INF*: substr(pol_key,11,2)
	substr(pol_key, 11, 2
	) AS pol_mod,
	LKP_Claimant_Coverage_Detail_PMSClaims.mco,
	LKP_Claimant_Coverage_Detail_PMSClaims.pol_eff_date,
	LKP_Claimant_Coverage_Detail_PMSClaims.pms_pol_lob_code,
	LKP_Claimant_Coverage_Detail_PMSClaims.ClassOfBusiness AS pif_clb,
	LKP_Claimant_Coverage_Detail_PMSClaims.variation_code,
	LKP_Claimant_Coverage_Detail_PMSClaims.Claim_Loss_Date AS claim_loss_date,
	LKP_Claimant_Coverage_Detail_PMSClaims.loc_unit_num,
	-- *INF*: IIF(loc_unit_num = 'N/A','0000',loc_unit_num)
	-- 
	-- 
	-- --Adding new rules for Personal Lines policy as the coverage EDW is incorrect.
	-- ----IIF(loc_unit_num = 'N/A','0000',loc_unit_num)
	-- 
	-- -----IIF(loc_unit_num = 'N/A','0000',
	--     --   IIF(SUBSTR(Policy_key,1,1)='H' and SUBSTR(Policy_key,4,1) = '5','0000',loc_unit_num)
	--        ---)
	IFF(loc_unit_num = 'N/A',
		'0000',
		loc_unit_num
	) AS loc_unit_num_out,
	LKP_Claimant_Coverage_Detail_PMSClaims.sub_loc_unit_num,
	-- *INF*: IIF(sub_loc_unit_num='N/A','000',sub_loc_unit_num)
	IFF(sub_loc_unit_num = 'N/A',
		'000',
		sub_loc_unit_num
	) AS sub_loc_unit_num_out,
	LKP_Claimant_Coverage_Detail_PMSClaims.ins_line,
	-- *INF*: IIF(ins_line = 'N/A','NA',ins_line)
	IFF(ins_line = 'N/A',
		'NA',
		ins_line
	) AS ins_line_out,
	LKP_Claimant_Coverage_Detail_PMSClaims.risk_unit_grp,
	-- *INF*: IIF(risk_unit_grp = 'N/A','000',risk_unit_grp)
	IFF(risk_unit_grp = 'N/A',
		'000',
		risk_unit_grp
	) AS risk_unit_grp_out,
	LKP_Claimant_Coverage_Detail_PMSClaims.risk_unit_grp_seq_num,
	-- *INF*: IIF(LENGTH(RTRIM(risk_unit_grp_seq_num))<3,LPAD(RTRIM(risk_unit_grp_seq_num),3,'0'),risk_unit_grp_seq_num)
	IFF(LENGTH(RTRIM(risk_unit_grp_seq_num
			)
		) < 3,
		LPAD(RTRIM(risk_unit_grp_seq_num
			), 3, '0'
		),
		risk_unit_grp_seq_num
	) AS v_risk_unit_grp_seq_num,
	-- *INF*: IIF(SUBSTR(v_risk_unit_grp_seq_num,1,2)='N/','NA',SUBSTR(v_risk_unit_grp_seq_num,1,2))
	IFF(SUBSTR(v_risk_unit_grp_seq_num, 1, 2
		) = 'N/',
		'NA',
		SUBSTR(v_risk_unit_grp_seq_num, 1, 2
		)
	) AS risk_unit_grp_seq_num_first2_pos,
	-- *INF*: IIF(SUBSTR(v_risk_unit_grp_seq_num,3,1)='A','N',SUBSTR(v_risk_unit_grp_seq_num,3,1))
	IFF(SUBSTR(v_risk_unit_grp_seq_num, 3, 1
		) = 'A',
		'N',
		SUBSTR(v_risk_unit_grp_seq_num, 3, 1
		)
	) AS risk_unit_grp_seq_num_last_pos,
	LKP_Claimant_Coverage_Detail_PMSClaims.risk_unit,
	-- *INF*: RTRIM(risk_unit)
	RTRIM(risk_unit
	) AS risk_unit_out,
	-- *INF*: SUBSTR(risk_unit,1,3)
	-- 
	-- 
	-- ---IIF(source_sys_id = 'PMS',SUBSTR(risk_unit,1,3),
	-- ---IIF(SUBSTR(Policy_key,1,1)='H' and SUBSTR(Policy_key,4,1) = '5',loc_unit_num,SUBSTR(risk_unit,1,3)))
	-- 
	-- ---SUBSTR(risk_unit,1,3)
	SUBSTR(risk_unit, 1, 3
	) AS v_risk_unit_first3,
	-- *INF*: IIF(LENGTH(RTRIM(LTRIM(SUBSTR(risk_unit,4,3))))<3,
	-- RPAD(RTRIM(LTRIM(SUBSTR(risk_unit,4,3))),3,'0'), RTRIM(LTRIM(SUBSTR(risk_unit,4,3)))
	-- )
	IFF(LENGTH(RTRIM(LTRIM(SUBSTR(risk_unit, 4, 3
					)
				)
			)
		) < 3,
		RPAD(RTRIM(LTRIM(SUBSTR(risk_unit, 4, 3
					)
				)
			), 3, '0'
		),
		RTRIM(LTRIM(SUBSTR(risk_unit, 4, 3
				)
			)
		)
	) AS v_risk_unit_last3,
	v_risk_unit_first3 || v_risk_unit_last3 AS v_risk_unit_complete,
	v_risk_unit_complete AS risk_unit_complete,
	LKP_Claimant_Coverage_Detail_PMSClaims.risk_unit_seq_num,
	-- *INF*: IIF(risk_unit_seq_num ='0' and risk_type_ind = 'N/A','00',
	-- IIF(LENGTH(risk_unit_seq_num)=1 and risk_unit_seq_num <> '0' and risk_type_ind = 'N/A', risk_unit_seq_num || '0',risk_unit_seq_num || risk_type_ind ))
	IFF(risk_unit_seq_num = '0' 
		AND risk_type_ind = 'N/A',
		'00',
		IFF(LENGTH(risk_unit_seq_num
			) = 1 
			AND risk_unit_seq_num <> '0' 
			AND risk_type_ind = 'N/A',
			risk_unit_seq_num || '0',
			risk_unit_seq_num || risk_type_ind
		)
	) AS risk_unit_seq_num_out,
	LKP_Claimant_Coverage_Detail_PMSClaims.risk_type_ind,
	LKP_Claimant_Coverage_Detail_PMSClaims.major_peril_code,
	-- *INF*: IIF(major_peril_code='N/A','000',major_peril_code)
	IFF(major_peril_code = 'N/A',
		'000',
		major_peril_code
	) AS major_peril_code_out,
	LKP_Claimant_Coverage_Detail_PMSClaims.major_peril_seq,
	-- *INF*: IIF(major_peril_seq='N/A','00',major_peril_seq)
	IFF(major_peril_seq = 'N/A',
		'00',
		major_peril_seq
	) AS major_peril_seq_out,
	LKP_Claimant_Coverage_Detail_PMSClaims.pms_loss_disability,
	LKP_Claimant_Coverage_Detail_PMSClaims.reserve_ctgry,
	LKP_Claimant_Coverage_Detail_PMSClaims.cause_of_loss,
	LKP_Claimant_Coverage_Detail_PMSClaims.pms_type_exposure,
	-- *INF*: IIF(pms_type_exposure = 'N/A','000',pms_type_exposure)
	IFF(pms_type_exposure = 'N/A',
		'000',
		pms_type_exposure
	) AS pms_type_exposure_out,
	LKP_Claimant_Coverage_Detail_PMSClaims.pms_type_bureau_code,
	EXP_merged_union_input_source.claimant_cov_det_ak_id,
	EXP_merged_union_input_source.claim_pay_ak_id,
	EXP_merged_union_input_source.sar_id,
	EXP_merged_union_input_source.claim_loss_trans_fact_id,
	EXP_merged_union_input_source.financial_type_code,
	EXP_merged_union_input_source.trans_code,
	EXP_merged_union_input_source.trans_date,
	EXP_merged_union_input_source.trans_ctgry_code,
	EXP_merged_union_input_source.trans_amt,
	EXP_merged_union_input_source.trans_hist_amt,
	EXP_merged_union_input_source.source_sys_id,
	EXP_merged_union_input_source.claim_loss_trans_type_dim_id,
	EXP_merged_union_input_source.InsuranceReferenceDimId,
	EXP_merged_union_input_source.AgencyDimId,
	EXP_merged_union_input_source.SalesDivisionDimId,
	EXP_merged_union_input_source.InsuranceReferenceCoverageDimId,
	EXP_merged_union_input_source.CoverageDetailDimId
	FROM EXP_merged_union_input_source
	LEFT JOIN LKP_Claimant_Coverage_Detail_PMSClaims
	ON LKP_Claimant_Coverage_Detail_PMSClaims.claimant_cov_det_ak_id = EXP_merged_union_input_source.claimant_cov_det_ak_id AND LKP_Claimant_Coverage_Detail_PMSClaims.eff_from_date <= EXP_merged_union_input_source.trans_date_12312100 AND LKP_Claimant_Coverage_Detail_PMSClaims.eff_to_date >= EXP_merged_union_input_source.trans_date_12312100
),
mplt_Coverage_Temp_Policy_Transaction_Attributes AS (WITH
	INPUT AS (
		
	),
	EXP_Values AS (
		SELECT
		pol_ak_id,
		loss_id,
		ins_line,
		-- *INF*: RTRIM(ins_line)
		RTRIM(ins_line
		) AS ins_line_out,
		loc_unit_num,
		-- *INF*: RTRIM(loc_unit_num)
		RTRIM(loc_unit_num
		) AS loc_unit_num1,
		sub_loc_unit_num,
		-- *INF*: RTRIM(sub_loc_unit_num)
		RTRIM(sub_loc_unit_num
		) AS sub_loc_unit_num1,
		risk_unit_grp,
		-- *INF*: RTRIM(risk_unit_grp)
		RTRIM(risk_unit_grp
		) AS risk_unit_grp1,
		risk_unit_grp_seq_num_First_2pos,
		-- *INF*: RTRIM(risk_unit_grp_seq_num_First_2pos)
		RTRIM(risk_unit_grp_seq_num_First_2pos
		) AS risk_unit_grp_seq_num_First_2pos1,
		risk_unit_grp_seq_num_last_pos,
		-- *INF*: RTRIM(risk_unit_grp_seq_num_last_pos)
		RTRIM(risk_unit_grp_seq_num_last_pos
		) AS risk_unit_grp_seq_num_last_pos1,
		risk_unit_complete,
		-- *INF*: RTRIM(risk_unit_complete)
		RTRIM(risk_unit_complete
		) AS risk_unit_complete1,
		risk_unit_seq_num,
		-- *INF*: RTRIM(risk_unit_seq_num)
		RTRIM(risk_unit_seq_num
		) AS risk_unit_seq_num1,
		pms_type_exposure,
		-- *INF*: RTRIM(pms_type_exposure)
		RTRIM(pms_type_exposure
		) AS pms_type_exposure1,
		major_peril_code,
		-- *INF*: RTRIM(major_peril_code)
		RTRIM(major_peril_code
		) AS major_peril_code1,
		major_peril_seq,
		-- *INF*: RTRIM(major_peril_seq)
		RTRIM(major_peril_seq
		) AS major_peril_seq1,
		Claim_loss_date
		FROM INPUT
	),
	LKP_Coverage_Temp_Policy_Transaction AS (
		SELECT
		cov_ak_id,
		temp_pol_trans_ak_id,
		pol_ak_id,
		sar_id,
		ins_line,
		loc_unit_num,
		sub_loc_unit_num,
		risk_unit_grp,
		risk_unit_grp_seq_num_First_2pos,
		risk_unit_grp_seq_num_lastpos,
		risk_unit,
		risk_unit_seq_num,
		major_peril_code,
		major_peril_seq_num,
		pms_type_exposure,
		cov_eff_date,
		type_bureau_code,
		risk_state_prov_code,
		risk_zip_code,
		terr_code,
		tax_loc,
		class_code,
		exposure,
		sub_line_code,
		source_sar_asl,
		source_sar_prdct_line,
		source_sar_sp_use_code,
		source_statistical_code,
		section_code,
		rsn_amended_code,
		part_code,
		rating_date_ind
		FROM (
			SELECT  C.cov_ak_id                     AS cov_ak_id,  
			TPT.temp_pol_trans_ak_id   AS temp_pol_trans_ak_id,  
			CASE  C.pms_type_exposure WHEN 'N/A' THEN '000' ELSE RTRIM(C.pms_type_exposure)   END AS pms_type_exposure,  
			C.type_bureau_code              AS type_bureau_code,  
			TPT.risk_state_prov_code        AS risk_state_prov_code,  
			TPT.risk_zip_code               AS risk_zip_code,  
			TPT.terr_code                   AS terr_code,  
			TPT.tax_loc                     AS tax_loc,  
			TPT.class_code                  AS class_code,  
			TPT.exposure                      AS exposure,  
			TPT.sub_line_code               AS sub_line_code,  
			TPT.source_sar_asl              AS source_sar_asl,  
			TPT.source_sar_prdct_line       AS source_sar_prdct_line,  
			TPT.source_sar_sp_use_code	 AS source_sar_sp_use_code,  
			TPT.source_statistical_code     AS source_statistical_code,  
			TPT.section_code                AS section_code,  
			TPT.rsn_amended_code            AS rsn_amended_code,  
			TPT.part_code                   AS part_code,  
			RTRIM(TPT.rating_date_ind)        AS rating_date_ind,  
			C.pol_ak_id                     AS pol_ak_id,  
			TPT.sar_id                      AS sar_id,  
			CASE C.ins_line WHEN 'N/A' THEN 'NA' ELSE RTRIM(C.ins_line) END AS ins_line,  
			CASE  C.loc_unit_num  WHEN 'N/A' THEN '0000' ELSE RTRIM(C.loc_unit_num)  END AS loc_unit_num,  
			CASE  C.sub_loc_unit_num  WHEN 'N/A' THEN '000' ELSE RTRIM(C.sub_loc_unit_num)   END AS sub_loc_unit_num,  
			CASE  C.risk_unit_grp   WHEN 'N/A' THEN '000' ELSE RTRIM(C.risk_unit_grp)    END AS risk_unit_grp,  
			CASE  SUBSTRING(C.risk_unit_grp_seq_num,1,2)   WHEN 'N/' THEN 'NA' ELSE SUBSTRING(C.risk_unit_grp_seq_num,1,2) END AS risk_unit_grp_seq_num_First_2pos,  
			CASE  SUBSTRING(C.risk_unit_grp_seq_num,3,1)   WHEN 'A' THEN 'N' ELSE SUBSTRING(C.risk_unit_grp_seq_num,3,1) END AS risk_unit_grp_seq_num_lastpos,  
			RTRIM(C.risk_unit)               AS risk_unit,  
			CASE   C.risk_unit_seq_num   WHEN 'N/A' THEN '00' ELSE  RTRIM(C.risk_unit_seq_num)    END AS risk_unit_seq_num,  
			RTRIM(C.major_peril_code)              AS major_peril_code,  
			RTRIM(C.major_peril_seq_num)           AS major_peril_seq_num,  
			C.cov_eff_date                  AS cov_eff_date 
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.coverage C 
			INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.temp_policy_transaction TPT ON C.cov_ak_id = TPT.cov_ak_id AND C.crrnt_snpsht_flag = 1 AND TPT.crrnt_snpsht_flag = 1
			INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.Claim_Occurrence CO ON CO.pol_key_ak_id = C.pol_ak_id and CO.crrnt_snpsht_flag = 1
			INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.Claim_Party_Occurrence CPO ON CPO.Claim_Occurrence_ak_id = CO.Claim_Occurrence_ak_id and CPO.crrnt_snpsht_flag = 1
			INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.Claimant_Coverage_Detail CCD ON CCD.Claim_Party_Occurrence_ak_id = CPO.Claim_Party_Occurrence_ak_id and CCD.crrnt_snpsht_flag = 1
			AND RTRIM(C.risk_unit) = CCD.Risk_unit AND RTRIM(C.major_peril_code)  = CCD.major_peril_code
			ORDER BY TPT.temp_pol_trans_ak_id   --
			
			--- Order By clause is important in this Lookup Override because how the data is retrieved is important
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,sar_id,ins_line,loc_unit_num,sub_loc_unit_num,risk_unit_grp,risk_unit_grp_seq_num_First_2pos,risk_unit_grp_seq_num_lastpos,risk_unit,risk_unit_seq_num,major_peril_code,major_peril_seq_num,cov_eff_date ORDER BY cov_ak_id DESC) = 1
	),
	EXP_Lkp_Values AS (
		SELECT
		LKP_Coverage_Temp_Policy_Transaction.temp_pol_trans_ak_id,
		LKP_Coverage_Temp_Policy_Transaction.cov_ak_id,
		LKP_Coverage_Temp_Policy_Transaction.pol_ak_id,
		LKP_Coverage_Temp_Policy_Transaction.sar_id,
		LKP_Coverage_Temp_Policy_Transaction.ins_line,
		LKP_Coverage_Temp_Policy_Transaction.loc_unit_num,
		LKP_Coverage_Temp_Policy_Transaction.sub_loc_unit_num,
		LKP_Coverage_Temp_Policy_Transaction.risk_unit_grp,
		LKP_Coverage_Temp_Policy_Transaction.risk_unit_grp_seq_num_First_2pos,
		LKP_Coverage_Temp_Policy_Transaction.risk_unit_grp_seq_num_lastpos,
		LKP_Coverage_Temp_Policy_Transaction.risk_unit,
		LKP_Coverage_Temp_Policy_Transaction.risk_unit_seq_num,
		LKP_Coverage_Temp_Policy_Transaction.major_peril_code,
		LKP_Coverage_Temp_Policy_Transaction.major_peril_seq_num,
		LKP_Coverage_Temp_Policy_Transaction.pms_type_exposure,
		LKP_Coverage_Temp_Policy_Transaction.cov_eff_date,
		LKP_Coverage_Temp_Policy_Transaction.type_bureau_code,
		LKP_Coverage_Temp_Policy_Transaction.risk_state_prov_code,
		LKP_Coverage_Temp_Policy_Transaction.risk_zip_code,
		LKP_Coverage_Temp_Policy_Transaction.terr_code,
		LKP_Coverage_Temp_Policy_Transaction.tax_loc,
		LKP_Coverage_Temp_Policy_Transaction.class_code,
		LKP_Coverage_Temp_Policy_Transaction.exposure,
		LKP_Coverage_Temp_Policy_Transaction.sub_line_code,
		LKP_Coverage_Temp_Policy_Transaction.source_sar_asl,
		LKP_Coverage_Temp_Policy_Transaction.source_sar_prdct_line,
		LKP_Coverage_Temp_Policy_Transaction.source_sar_sp_use_code,
		LKP_Coverage_Temp_Policy_Transaction.source_statistical_code,
		LKP_Coverage_Temp_Policy_Transaction.section_code,
		LKP_Coverage_Temp_Policy_Transaction.rsn_amended_code,
		LKP_Coverage_Temp_Policy_Transaction.part_code,
		LKP_Coverage_Temp_Policy_Transaction.rating_date_ind,
		EXP_Values.Claim_loss_date
		FROM EXP_Values
		LEFT JOIN LKP_Coverage_Temp_Policy_Transaction
		ON LKP_Coverage_Temp_Policy_Transaction.pol_ak_id = EXP_Values.pol_ak_id AND LKP_Coverage_Temp_Policy_Transaction.sar_id = EXP_Values.loss_id AND LKP_Coverage_Temp_Policy_Transaction.ins_line = EXP_Values.ins_line_out AND LKP_Coverage_Temp_Policy_Transaction.loc_unit_num = EXP_Values.loc_unit_num1 AND LKP_Coverage_Temp_Policy_Transaction.sub_loc_unit_num = EXP_Values.sub_loc_unit_num1 AND LKP_Coverage_Temp_Policy_Transaction.risk_unit_grp = EXP_Values.risk_unit_grp1 AND LKP_Coverage_Temp_Policy_Transaction.risk_unit_grp_seq_num_First_2pos = EXP_Values.risk_unit_grp_seq_num_First_2pos1 AND LKP_Coverage_Temp_Policy_Transaction.risk_unit_grp_seq_num_lastpos = EXP_Values.risk_unit_grp_seq_num_last_pos1 AND LKP_Coverage_Temp_Policy_Transaction.risk_unit = EXP_Values.risk_unit_complete1 AND LKP_Coverage_Temp_Policy_Transaction.risk_unit_seq_num = EXP_Values.risk_unit_seq_num1 AND LKP_Coverage_Temp_Policy_Transaction.major_peril_code = EXP_Values.major_peril_code1 AND LKP_Coverage_Temp_Policy_Transaction.major_peril_seq_num = EXP_Values.major_peril_seq1 AND LKP_Coverage_Temp_Policy_Transaction.cov_eff_date <= EXP_Values.Claim_loss_date
	),
	OUTPUT AS (
		SELECT
		cov_ak_id AS cov_ak_id_Out, 
		pol_ak_id AS pol_ak_id_Out, 
		temp_pol_trans_ak_id, 
		sar_id AS loss_id_Out, 
		ins_line AS ins_line_Out, 
		loc_unit_num AS loc_unit_num_Out, 
		sub_loc_unit_num AS sub_loc_unit_num_Out, 
		risk_unit_grp AS risk_unit_grp_Out, 
		risk_unit_grp_seq_num_First_2pos AS risk_unit_grp_seq_num_First_2pos_Out, 
		risk_unit_grp_seq_num_lastpos AS risk_unit_grp_seq_num_last_pos_Out, 
		risk_unit AS risk_unit_complete_Out, 
		risk_unit_seq_num AS risk_unit_seq_num_Out, 
		major_peril_code AS major_peril_code_Out, 
		major_peril_seq_num AS major_peril_seq_Out, 
		pms_type_exposure AS pms_type_exposure_Out, 
		cov_eff_date AS cov_eff_date_Out, 
		type_bureau_code AS type_bureau_code_Out, 
		risk_state_prov_code AS risk_state_prov_code_Out, 
		risk_zip_code AS risk_zip_code_Out, 
		terr_code AS terr_code_Out, 
		tax_loc AS tax_loc_Out, 
		class_code AS class_code_Out, 
		exposure AS exposure_Out, 
		sub_line_code AS sub_line_code_Out, 
		source_sar_asl AS source_sar_asl_Out, 
		source_sar_prdct_line AS source_sar_prdct_line_Out, 
		source_sar_sp_use_code, 
		source_statistical_code AS source_statistical_code_Out, 
		source_statistical_line AS source_statistical_line_Out, 
		section_code AS section_code_Out, 
		rsn_amended_code AS rsn_amended_code_Out, 
		part_code AS part_code_Out, 
		rating_date_ind, 
		Claim_loss_date AS Claim_loss_date_Out
		FROM EXP_Lkp_Values
	),
),
EXP_Transform_Statistical_Codes AS (
	SELECT
	source_statistical_code_Out AS statistical_code,
	type_bureau_code_Out AS Type_Bureau,
	class_code_Out AS sar_class_code,
	-- *INF*: statistical_code
	-- 
	-- --DECODE(TRUE, Type_Bureau = 'BE', ' '  || statistical_code,
	-- --Type_Bureau = 'BF', ' '  || statistical_code,
	-- --Type_Bureau = 'RP' AND major_peril = '145', ' '  || statistical_code,
	-- --Type_Bureau = 'RL' AND major_peril = '114', '  '  || statistical_code,
	-- --Type_Bureau = 'RL' AND major_peril = '119', '     '  || statistical_code,
	-- --statistical_code)
	-- 
	-- ---- Had to introduce space at the begining of the field because of LTRIM(RTRIM)) to statistical codes in Temp_Policy_transaction Table.
	statistical_code AS v_statistical_code,
	'D' AS v_stat_plan_id,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,1,1))=0,' ',SUBSTR(v_statistical_code,1,1))
	-- 
	-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 1, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 1, 1
		)
	) AS v_pos_1,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,2,1))=0,' ',SUBSTR(v_statistical_code,2,1))
	-- 
	-- ----Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 2, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 2, 1
		)
	) AS v_pos_2,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,3,1))=0,' ',SUBSTR(v_statistical_code,3,1))
	-- 
	-- ----Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 3, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 3, 1
		)
	) AS v_pos_3,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,4,1))=0,' ',SUBSTR(v_statistical_code,4,1))
	-- 
	-- ----Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 4, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 4, 1
		)
	) AS v_pos_4,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,5,1))=0,' ',SUBSTR(v_statistical_code,5,1))
	-- 
	-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 5, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 5, 1
		)
	) AS v_pos_5,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,6,1))=0,' ',SUBSTR(v_statistical_code,6,1))
	-- 
	-- 
	-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 6, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 6, 1
		)
	) AS v_pos_6,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,7,1))=0,' ',SUBSTR(v_statistical_code,7,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 7, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 7, 1
		)
	) AS v_pos_7,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,8,1))=0,' ',SUBSTR(v_statistical_code,8,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 8, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 8, 1
		)
	) AS v_pos_8,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,9,1))=0,' ',SUBSTR(v_statistical_code,9,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 9, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 9, 1
		)
	) AS v_pos_9,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,10,1))=0,' ',SUBSTR(v_statistical_code,10,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 10, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 10, 1
		)
	) AS v_pos_10,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,11,1))=0,' ',SUBSTR(v_statistical_code,11,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 11, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 11, 1
		)
	) AS v_pos_11,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,12,1))=0,' ',SUBSTR(v_statistical_code,12,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 12, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 12, 1
		)
	) AS v_pos_12,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,13,1))=0,' ',SUBSTR(v_statistical_code,13,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 13, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 13, 1
		)
	) AS v_pos_13,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,14,1))=0,' ',SUBSTR(v_statistical_code,14,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 14, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 14, 1
		)
	) AS v_pos_14,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,15,1))=0,' ',SUBSTR(v_statistical_code,15,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 15, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 15, 1
		)
	) AS v_pos_15,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,16,1))=0,' ',SUBSTR(v_statistical_code,16,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 16, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 16, 1
		)
	) AS v_pos_16,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,17,1))=0,' ',SUBSTR(v_statistical_code,17,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 17, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 17, 1
		)
	) AS v_pos_17,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,18,1))=0,' ',SUBSTR(v_statistical_code,18,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 18, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 18, 1
		)
	) AS v_pos_18,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,19,1))=0,' ',SUBSTR(v_statistical_code,19,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 19, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 19, 1
		)
	) AS v_pos_19,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,20,1))=0,' ',SUBSTR(v_statistical_code,20,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 20, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 20, 1
		)
	) AS v_pos_20,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
	-- LENGTH(SUBSTR(v_statistical_code,21,1))=0,' ',SUBSTR(v_statistical_code,21,1))
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,21,1))=0,' ',SUBSTR(v_statistical_code,21,1))
	-- 
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
	DECODE(TRUE,
		Type_Bureau = 'RP', '0',
		LENGTH(SUBSTR(v_statistical_code, 21, 1
			)
		) = 0, ' ',
		SUBSTR(v_statistical_code, 21, 1
		)
	) AS v_pos_21,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
	-- LENGTH(SUBSTR(v_statistical_code,22,1))=0,' ',SUBSTR(v_statistical_code,22,1))
	-- 
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,22,1))=0,' ',SUBSTR(v_statistical_code,22,1))
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
	DECODE(TRUE,
		Type_Bureau = 'RP', '0',
		LENGTH(SUBSTR(v_statistical_code, 22, 1
			)
		) = 0, ' ',
		SUBSTR(v_statistical_code, 22, 1
		)
	) AS v_pos_22,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
	-- LENGTH(SUBSTR(v_statistical_code,23,1))=0,' ',SUBSTR(v_statistical_code,23,1))
	-- 
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,23,1))=0,' ',SUBSTR(v_statistical_code,23,1))
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
	DECODE(TRUE,
		Type_Bureau = 'RP', '0',
		LENGTH(SUBSTR(v_statistical_code, 23, 1
			)
		) = 0, ' ',
		SUBSTR(v_statistical_code, 23, 1
		)
	) AS v_pos_23,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
	-- LENGTH(SUBSTR(v_statistical_code,24,1))=0,' ',SUBSTR(v_statistical_code,24,1))
	-- 
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,24,1))=0,' ',SUBSTR(v_statistical_code,24,1))
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
	DECODE(TRUE,
		Type_Bureau = 'RP', '0',
		LENGTH(SUBSTR(v_statistical_code, 24, 1
			)
		) = 0, ' ',
		SUBSTR(v_statistical_code, 24, 1
		)
	) AS v_pos_24,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','{',
	-- LENGTH(SUBSTR(v_statistical_code,25,1))=0,' ',SUBSTR(v_statistical_code,25,1))
	-- 
	-- --- IN COBOL "{" represents a  +ve sign and "}" is -ve sign, since this is base rate for Type_Bureau RP is a sign field so COBOL creates "{". Replicating the COBOL logic.
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '{'
	-- 
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,25,1))=0,' ',SUBSTR(v_statistical_code,25,1))
	DECODE(TRUE,
		Type_Bureau = 'RP', '{',
		LENGTH(SUBSTR(v_statistical_code, 25, 1
			)
		) = 0, ' ',
		SUBSTR(v_statistical_code, 25, 1
		)
	) AS v_pos_25,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,26,1))=0,' ',SUBSTR(v_statistical_code,26,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 26, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 26, 1
		)
	) AS v_pos_26,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,27,1))=0,' ',SUBSTR(v_statistical_code,27,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 27, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 27, 1
		)
	) AS v_pos_27,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,28,1))=0,' ',SUBSTR(v_statistical_code,28,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 28, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 28, 1
		)
	) AS v_pos_28,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,29,1))=0,' ',SUBSTR(v_statistical_code,29,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 29, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 29, 1
		)
	) AS v_pos_29,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,30,1))=0,' ',SUBSTR(v_statistical_code,30,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 30, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 30, 1
		)
	) AS v_pos_30,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,31,1))=0,' ',SUBSTR(v_statistical_code,31,1))
	-- 
	-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 31, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 31, 1
		)
	) AS v_pos_31,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,32,1))=0,' ',SUBSTR(v_statistical_code,32,1))
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 32, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 32, 1
		)
	) AS v_pos_32,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,33,1))=0,' ',SUBSTR(v_statistical_code,33,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 33, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 33, 1
		)
	) AS v_pos_33,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,34,1))=0,' ',SUBSTR(v_statistical_code,34,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 34, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 34, 1
		)
	) AS v_pos_34,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,35,1))=0,' ',SUBSTR(v_statistical_code,35,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 35, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 35, 1
		)
	) AS v_pos_35,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,36,1))=0,' ',SUBSTR(v_statistical_code,36,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 36, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 36, 1
		)
	) AS v_pos_36,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,37,1))=0,' ',SUBSTR(v_statistical_code,37,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 37, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 37, 1
		)
	) AS v_pos_37,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,38,1))=0,' ',SUBSTR(v_statistical_code,38,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 38, 1
			)
		) = 0,
		' ',
		SUBSTR(v_statistical_code, 38, 1
		)
	) AS v_pos_38,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38 
	) AS Generic,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 
	) AS v_Stat_Code_AC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 
	) AS v_Stat_Codes_AI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_26 || '       ' || v_pos_25 || v_pos_23  || v_pos_24  || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22)
	-- 
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_26 || '       ' || v_pos_25 || v_pos_23 || v_pos_24 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 
	) AS v_Stat_Codes_AL,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10  || v_pos_11|| v_pos_20 || v_pos_21  || 
	-- '             ' ||  v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19  )
	-- 
	--  -----It has a Filler of 13 spaces
	-- --- I have checked this code this is fine
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_20 || v_pos_21 || '             ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 
	) AS v_Stat_Codes_AN,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 ||
	-- '      ' || v_pos_14 || v_pos_23  || v_pos_24  || '  '  ||  v_pos_26  || v_pos_27  || v_pos_28  || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || '      ' || v_pos_14 || v_pos_23 || v_pos_24 || '  ' || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 
	) AS v_Stat_Codes_AP,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_11 || v_pos_10 || 
	--   v_pos_12 || v_pos_13 )
	-- 
	-- --- Verified the logic
	-- 
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_11 || v_pos_10 || v_pos_12 || v_pos_13 
	) AS v_Stat_Codes_A2,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_11 || v_pos_12 )
	-- 
	-- --- Verified logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_11 || v_pos_12 
	) AS v_Stat_Codes_A3,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 ||
	-- '           '  ||  v_pos_22 || v_pos_29 || '  ' || v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28)
	-- 
	-- --- Verified logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || '           ' || v_pos_22 || v_pos_29 || '  ' || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 
	) AS v_Stat_Codes_BB,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17  || v_pos_20  || v_pos_27  || v_pos_28  || v_pos_29 || '    ' ||v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26 )
	-- 
	-- 
	-- -- Verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_20 || v_pos_27 || v_pos_28 || v_pos_29 || '    ' || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 
	) AS v_Stat_Codes_BC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_5  || v_pos_6 || v_pos_7)
	-- 
	-- --- Verified logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_5 || v_pos_6 || v_pos_7 
	) AS v_Stat_Codes_BD,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 ||  v_pos_6 || v_pos_7 || '                    ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13)
	-- 
	-- 
	--  ---  Verified Logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || '                    ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 
	) AS v_Stat_Codes_BE,
	-- *INF*: ('  '  || v_pos_4  || v_pos_5 || ' ' || v_pos_14 || '  ' || v_pos_15 || v_pos_16 || '   ' ||  v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || '     ' || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22 || '   ')
	-- 
	-- 
	-- --8/22/2011 - Added 2 spaces in the beginning. In COBOL, statitistical code field is initialised to spaces at the start of reformatting. If there is no code to move certain fields then the spaces stay as it is except other fileds are layed out over spaces.
	-- --- Verified the logic
	-- 
	( '  ' || v_pos_4 || v_pos_5 || ' ' || v_pos_14 || '  ' || v_pos_15 || v_pos_16 || '   ' || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || '     ' || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || '   ' 
	) AS v_Stat_Codes_BF,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_4  || v_pos_5)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_4 || v_pos_5 
	) AS v_Stat_Codes_BP,
	-- *INF*: (v_pos_1 || v_pos_2 )
	-- 
	-- --- Verified the logic
	( v_pos_1 || v_pos_2 
	) AS v_Stat_Codes_BI,
	-- *INF*: v_pos_1
	-- 
	-- -- verified the logic
	v_pos_1 AS v_Stat_Codes_BL,
	-- *INF*: (SUBSTR(sar_class_code,1,3) || '  ' || v_pos_18  ||  v_pos_19 || v_pos_1 ||  ' ' ||  v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 
	-- || '    ' ||  v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34 || v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28 || '   ' )
	-- 
	-- --- Verfied the logic
	( SUBSTR(sar_class_code, 1, 3
		) || '  ' || v_pos_18 || v_pos_19 || v_pos_1 || ' ' || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || '    ' || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || '   ' 
	) AS v_Stat_Codes_BM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || '      '  ||  v_pos_8 || v_pos_9 || '           ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19)
	-- 
	--  ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || '      ' || v_pos_8 || v_pos_9 || '           ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 
	) AS v_Stat_Codes_BT,
	-- *INF*: (v_pos_1 || v_pos_2 || '      '  || v_pos_9 || v_pos_10 || v_pos_17 || v_pos_18 ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31)
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || '      ' || v_pos_9 || v_pos_10 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 
	) AS v_Stat_Codes_B2,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17)
	-- 
	-- ----- verified the logic
	-- 
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 
	) AS v_Stat_Codes_CC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || 
	--  v_pos_17 || v_pos_18  || ' ' ||  v_pos_20 || '              ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_17 || v_pos_18 || ' ' || v_pos_20 || '              ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 
	) AS v_Stat_Codes_CF,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- ---- Generic 
	-- -- No Change from Input copybook to Output
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38 
	) AS v_Stat_Code_CR,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_6 || v_pos_7 || ' '  || v_pos_9 || '  ' || v_pos_12 || ' ' || v_pos_14 || v_pos_15 )
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_6 || v_pos_7 || ' ' || v_pos_9 || '  ' || v_pos_12 || ' ' || v_pos_14 || v_pos_15 
	) AS v_Stat_Codes_CI,
	-- *INF*: (v_pos_1 || v_pos_4  || v_pos_6 || v_pos_7 )
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_4 || v_pos_6 || v_pos_7 
	) AS v_Stat_Codes_CL,
	-- *INF*: ('  ' || v_pos_1 || v_pos_2 || v_pos_5  || v_pos_6 || v_pos_7)
	-- 
	-- ---- verified the logic
	( '  ' || v_pos_1 || v_pos_2 || v_pos_5 || v_pos_6 || v_pos_7 
	) AS v_Stat_Codes_CP,
	-- *INF*: (v_pos_3 || v_pos_4  || v_pos_5 )
	-- 
	-- ---- verified the logic
	( v_pos_3 || v_pos_4 || v_pos_5 
	) AS v_Stat_Codes_CN,
	-- *INF*: v_pos_1
	-- 
	-- -----
	v_pos_1 AS v_Stat_Codes_EI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || '                   ' ||v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16)
	-- 
	-- ---- verified the logic
	-- --- 19 spaces
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || '                   ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 
	) AS v_Stat_Codes_EQ,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 )
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 
	) AS v_Stat_Codes_FC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 
	-- || '                  ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
	-- 
	-- ---- verified the logic
	-- ---- 18 Spaces
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || '                  ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 
	) AS v_Stat_Codes_FF,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5)
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 
	) AS v_Stat_Codes_FM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || '                   ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16)
	-- 
	-- ---- verified the logic
	-- --- 19 spaces
	-- 
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || '                   ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 
	) AS v_Stat_Codes_FO,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3)
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 
	) AS v_Stat_Codes_FP,
	-- *INF*: (v_pos_1 || v_pos_2 || ' ' || v_pos_3 || '  ' || v_pos_6 || v_pos_7 || '   ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 ||
	-- '       ' || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22 || '   ')
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || ' ' || v_pos_3 || '  ' || v_pos_6 || v_pos_7 || '   ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || '       ' || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || '   ' 
	) AS v_Stat_Codes_FT,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_12 || v_pos_13 || '                '  || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9)
	-- 
	-- ---- verified the logic
	-- -- 17 Spaces
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_12 || v_pos_13 || '                ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 
	) AS v_Stat_Codes_GI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_4  || v_pos_5  || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || '      ' || v_pos_13 || v_pos_29  || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19 ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28)
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_4 || v_pos_5 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || '      ' || v_pos_13 || v_pos_29 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 
	) AS v_Stat_Codes_GL,
	-- *INF*: (v_pos_1 || '           '  ||   v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7)
	-- 
	-- ---- verified the logic
	-- 
	( v_pos_1 || '           ' || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 
	) AS v_Stat_Codes_GP,
	-- *INF*: (v_pos_1 || '                       ' || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_11 || v_pos_12 || v_pos_13)
	-- 
	-- ---- verified the logic
	-- --- 23 spaces
	-- 
	-- 
	-- 
	( v_pos_1 || '                       ' || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_11 || v_pos_12 || v_pos_13 
	) AS v_Stat_Codes_GS,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_18  ||  v_pos_19  
	-- || ' ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || '                ')
	-- 
	-- 
	-- ---- verified the logic
	-- --- 16 Spaces at the end
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_18 || v_pos_19 || ' ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || '                ' 
	) AS v_Stat_Codes_HO,
	-- *INF*: ('        ' || v_pos_11 || v_pos_12 || '               '  || v_pos_4  || v_pos_5  || v_pos_6  || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_14 || v_pos_15 || v_pos_17)
	-- 
	-- ---- verified the logic
	( '        ' || v_pos_11 || v_pos_12 || '               ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_14 || v_pos_15 || v_pos_17 
	) AS v_Stat_Codes_IM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  || v_pos_24  || v_pos_25  || v_pos_26 || v_pos_28  || v_pos_29  || v_pos_30 || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35)
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 
	) AS v_Stat_Codes_JR,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  )
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 
	) AS v_Stat_Codes_ME,
	-- *INF*: (v_pos_1 || ' '  || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' ||  v_pos_11 || v_pos_12 || v_pos_13 || '  ' || v_pos_18  || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || '                ' ) 
	-- 
	-- --- need logic for stat-plan -id
	-- ---- 16 Spaces at the end
	( v_pos_1 || ' ' || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || '  ' || v_pos_18 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || '                ' 
	) AS v_Stat_Codes_MH,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || '                  '  || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7)
	-- 
	--  --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || '                  ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 
	) AS v_Stat_Codes_MI,
	-- *INF*: (v_pos_6 || v_pos_7 || v_pos_3 || v_pos_4  || v_pos_2 || '      ' || v_pos_1 || '        ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || ' ' ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24 )
	-- 
	--  --- verified the logic
	( v_pos_6 || v_pos_7 || v_pos_3 || v_pos_4 || v_pos_2 || '      ' || v_pos_1 || '        ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || ' ' || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 
	) AS v_Stat_Codes_ML,
	-- *INF*: -- No Stats code in the Output Copybook just the policy_type logic
	'' AS v_Stat_Codes_MP,
	-- *INF*: (SUBSTR(sar_class_code,1,3) || v_pos_17 || v_pos_18 ||  v_pos_19  || v_pos_1 || v_pos_2 || v_pos_3 || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || '   ' || '       ' || '      ' )
	-- 
	-- --- Need to look at complete logic
	-- 
	( SUBSTR(sar_class_code, 1, 3
		) || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_1 || v_pos_2 || v_pos_3 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || '   ' || '       ' || '      ' 
	) AS v_Stat_Codes_M2,
	-- *INF*: ( '                 ' || v_stat_plan_id)
	-- 
	-- ----verified the logic
	( '                 ' || v_stat_plan_id 
	) AS v_Stat_Codes_NE,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  ||  v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_19)
	-- 
	-- --- Verified the Logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_19 
	) AS v_Stat_Codes_PC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || v_pos_19  || v_pos_20  ||  v_pos_21)
	-- 
	-- --- verified the logic
	--  
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || v_pos_19 || v_pos_20 || v_pos_21 
	) AS v_Stat_Codes_PH,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38 
	) AS v_Stat_Code_PF,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38 
	) AS v_Stat_Code_PI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38 
	) AS v_Stat_Code_PL,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 ||  v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 
	) AS v_Stat_Codes_PM,
	-- *INF*: (v_pos_1 || v_pos_2)
	-- 
	-- --- verified the logic
	-- 
	( v_pos_1 || v_pos_2 
	) AS v_Stat_Codes_RB,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 
	) AS v_Stat_Codes_RG,
	-- *INF*: (v_pos_1 || v_pos_2)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 
	) AS v_Stat_Codes_RI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 
	) AS v_Stat_Codes_RL,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10 
	) AS v_Stat_Codes_RM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || ' ' || 
	-- v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21 || v_pos_22 ||  v_pos_23  || v_pos_24)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 
	) AS v_Stat_Codes_RN,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29 || v_pos_30 || v_pos_31|| v_pos_33 || v_pos_34  ||  v_pos_35  || v_pos_32)
	-- 
	-- ----
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_32 
	) AS v_Stat_Codes_RP,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5 )
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 
	) AS v_Stat_Codes_RQ,
	-- *INF*: (v_pos_1 || ' ' || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 )
	-- 
	-- --- verified the logic
	( v_pos_1 || ' ' || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 
	) AS v_Stat_Codes_SM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_8 || v_pos_11 || v_pos_9)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_8 || v_pos_11 || v_pos_9 
	) AS v_Stat_Codes_TH,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 
	-- || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19
	-- ||  v_pos_22  ||  v_pos_23  || v_pos_24 || '       ' || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36)
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_22 || v_pos_23 || v_pos_24 || '       ' || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 
	) AS v_Stat_Codes_VL,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19 
	--  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30 || ' ' || v_pos_32  ||  v_pos_33
	-- || v_pos_34  ||  v_pos_35  || v_pos_36 )
	-- 
	-- --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || ' ' || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 
	) AS v_Stat_Codes_VP,
	-- *INF*: ('   ' || v_pos_4  || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12  || ' ' || v_pos_14 || v_pos_15 || '              ' 
	-- || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34 || v_pos_35)
	-- 
	-- --- verified the logic
	( '   ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || ' ' || v_pos_14 || v_pos_15 || '              ' || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 
	) AS v_Stat_Codes_VN,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  
	-- || ' ' || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || '    ' || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- ---- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || ' ' || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || '    ' || v_pos_36 || v_pos_37 || v_pos_38 
	) AS v_Stat_Codes_VC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31)
	-- 
	--  --- verified the logic
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 
	) AS v_Stat_Codes_WC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	( v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38 
	) AS v_Stat_Code_WP,
	-- *INF*: ('   ' || v_pos_1 || v_pos_2 || '            ' || v_stat_plan_id)
	-- 
	-- --8/19/2011 Added v_stat_plan_id
	-- --- need to bring stat plan_id
	--  --- verified the logic but need stat plan id
	-- 
	( '   ' || v_pos_1 || v_pos_2 || '            ' || v_stat_plan_id 
	) AS v_Stat_Codes_WL,
	-- *INF*: DECODE(Type_Bureau, 'AC', v_Stat_Code_AC, 'AI', v_Stat_Codes_AI, 'AL', v_Stat_Codes_AL, 'AN', v_Stat_Codes_AN, 'AP', v_Stat_Codes_AP, 'A2', v_Stat_Codes_A2, 'A3', v_Stat_Codes_A3, 'BB', v_Stat_Codes_BB, 'BC', v_Stat_Codes_BC, 'BD', v_Stat_Codes_BD, 'BE', v_Stat_Codes_BE, 'BF', v_Stat_Codes_BF, 'BP', v_Stat_Codes_BP, 'BI', v_Stat_Codes_BI, 'BL', v_Stat_Codes_BL, 'BM', v_Stat_Codes_BM, 'BT', v_Stat_Codes_BT, 'B2', v_Stat_Codes_B2, 'CC', v_Stat_Codes_CC, 'CF', v_Stat_Codes_CF, 'CI', v_Stat_Codes_CI, 'CL', v_Stat_Codes_CL, 'CN', v_Stat_Codes_CN, 'CP', v_Stat_Codes_CP, 'EI', v_Stat_Codes_EI, 'EQ', v_Stat_Codes_EQ, 'FC', v_Stat_Codes_FC, 'FF', v_Stat_Codes_FF, 'FM', v_Stat_Codes_FM, 'FO', v_Stat_Codes_FO, 'FP', v_Stat_Codes_FP, 'FT', v_Stat_Codes_FT, 'GI', v_Stat_Codes_GI, 'GL', v_Stat_Codes_GL, 'GP', v_Stat_Codes_GP, 'GS', v_Stat_Codes_GS, 'HO', v_Stat_Codes_HO, 'IM', v_Stat_Codes_IM, 'JR', v_Stat_Codes_JR, 'ME', v_Stat_Codes_ME, 'MH', v_Stat_Codes_MH, 'MI', v_Stat_Codes_MI, 'ML',
	-- v_Stat_Codes_ML, 'MP', v_Stat_Codes_MP, 'M2', v_Stat_Codes_M2, 'NE', v_Stat_Codes_NE, 'PC', v_Stat_Codes_PC, 'PH', v_Stat_Codes_PH, 'PM', v_Stat_Codes_PM, 'RB', v_Stat_Codes_RB, 'RG', v_Stat_Codes_RG, 'RI', v_Stat_Codes_RI, 'RL', v_Stat_Codes_RL, 'RM', v_Stat_Codes_RM, 'RN', v_Stat_Codes_RN, 'RP', v_Stat_Codes_RP, 'RQ', v_Stat_Codes_RQ, 'SM', v_Stat_Codes_SM, 'TH', v_Stat_Codes_TH, 'VL', v_Stat_Codes_VL, 'VP', v_Stat_Codes_VP, 'VN', v_Stat_Codes_VN, 'VC', v_Stat_Codes_VC, 'WC', v_Stat_Codes_WC, 'WL', v_Stat_Codes_WL,
	-- 'CR', v_Stat_Code_CR, 'PF', v_Stat_Code_PF,'PI', v_Stat_Code_PI, 'PL', v_Stat_Code_PL,
	-- 'WP', v_Stat_Code_WP,v_statistical_code) 
	DECODE(Type_Bureau,
		'AC', v_Stat_Code_AC,
		'AI', v_Stat_Codes_AI,
		'AL', v_Stat_Codes_AL,
		'AN', v_Stat_Codes_AN,
		'AP', v_Stat_Codes_AP,
		'A2', v_Stat_Codes_A2,
		'A3', v_Stat_Codes_A3,
		'BB', v_Stat_Codes_BB,
		'BC', v_Stat_Codes_BC,
		'BD', v_Stat_Codes_BD,
		'BE', v_Stat_Codes_BE,
		'BF', v_Stat_Codes_BF,
		'BP', v_Stat_Codes_BP,
		'BI', v_Stat_Codes_BI,
		'BL', v_Stat_Codes_BL,
		'BM', v_Stat_Codes_BM,
		'BT', v_Stat_Codes_BT,
		'B2', v_Stat_Codes_B2,
		'CC', v_Stat_Codes_CC,
		'CF', v_Stat_Codes_CF,
		'CI', v_Stat_Codes_CI,
		'CL', v_Stat_Codes_CL,
		'CN', v_Stat_Codes_CN,
		'CP', v_Stat_Codes_CP,
		'EI', v_Stat_Codes_EI,
		'EQ', v_Stat_Codes_EQ,
		'FC', v_Stat_Codes_FC,
		'FF', v_Stat_Codes_FF,
		'FM', v_Stat_Codes_FM,
		'FO', v_Stat_Codes_FO,
		'FP', v_Stat_Codes_FP,
		'FT', v_Stat_Codes_FT,
		'GI', v_Stat_Codes_GI,
		'GL', v_Stat_Codes_GL,
		'GP', v_Stat_Codes_GP,
		'GS', v_Stat_Codes_GS,
		'HO', v_Stat_Codes_HO,
		'IM', v_Stat_Codes_IM,
		'JR', v_Stat_Codes_JR,
		'ME', v_Stat_Codes_ME,
		'MH', v_Stat_Codes_MH,
		'MI', v_Stat_Codes_MI,
		'ML', v_Stat_Codes_ML,
		'MP', v_Stat_Codes_MP,
		'M2', v_Stat_Codes_M2,
		'NE', v_Stat_Codes_NE,
		'PC', v_Stat_Codes_PC,
		'PH', v_Stat_Codes_PH,
		'PM', v_Stat_Codes_PM,
		'RB', v_Stat_Codes_RB,
		'RG', v_Stat_Codes_RG,
		'RI', v_Stat_Codes_RI,
		'RL', v_Stat_Codes_RL,
		'RM', v_Stat_Codes_RM,
		'RN', v_Stat_Codes_RN,
		'RP', v_Stat_Codes_RP,
		'RQ', v_Stat_Codes_RQ,
		'SM', v_Stat_Codes_SM,
		'TH', v_Stat_Codes_TH,
		'VL', v_Stat_Codes_VL,
		'VP', v_Stat_Codes_VP,
		'VN', v_Stat_Codes_VN,
		'VC', v_Stat_Codes_VC,
		'WC', v_Stat_Codes_WC,
		'WL', v_Stat_Codes_WL,
		'CR', v_Stat_Code_CR,
		'PF', v_Stat_Code_PF,
		'PI', v_Stat_Code_PI,
		'PL', v_Stat_Code_PL,
		'WP', v_Stat_Code_WP,
		v_statistical_code
	) AS V_Formatted_Stat_Codes,
	-- *INF*: SUBSTR(V_Formatted_Stat_Codes,1,25)
	SUBSTR(V_Formatted_Stat_Codes, 1, 25
	) AS Formatted_Stat_Codes,
	-- *INF*: SUBSTR(V_Formatted_Stat_Codes,26,9)
	SUBSTR(V_Formatted_Stat_Codes, 26, 9
	) AS Formatted_Stat_Codes_26_34,
	-- *INF*: SUBSTR(V_Formatted_Stat_Codes,35,4)
	SUBSTR(V_Formatted_Stat_Codes, 35, 4
	) AS Formatted_Stat_Codes_34_38,
	-- *INF*: DECODE(Type_Bureau,'AI', (v_pos_11 || v_pos_12),
	-- 'AL', (v_pos_15  ||  v_pos_16),
	-- 'AN',(v_pos_12 || v_pos_13),
	-- 'AP',(v_pos_12 || v_pos_13),
	-- 'A2',(v_pos_8 || v_pos_9),
	-- 'A3',(v_pos_8 || v_pos_9),
	-- 'BB',(v_pos_20 || v_pos_21),
	-- 'BC',(v_pos_18 || v_pos_19),
	-- 'BE', ( v_pos_4  || v_pos_5),
	-- 'BF', (v_pos_1  ||  v_pos_2),
	-- 'BP', (' '  ||  v_pos_2),
	-- 'BI', (v_pos_3 ||  v_pos_4),
	-- 'BL', (v_pos_3  ||  v_pos_4),
	-- 'BM',(v_pos_20 || v_pos_21),
	-- 'BT', (v_pos_11  ||  v_pos_12),
	-- 'B2',(v_pos_14  ||  v_pos_15),
	-- 'CF', (v_pos_8  || v_pos_9),
	-- 'CI',(v_pos_3  ||  v_pos_4),
	-- 'CN', (v_pos_1  ||  v_pos_2),
	-- 'CP', (v_pos_3  ||  v_pos_4),
	-- 'EI', (v_pos_2  ||  v_pos_3),
	-- 'EQ', (v_pos_8  || v_pos_9),
	-- 'FF', (v_pos_8  || v_pos_9),
	-- 'FI', (v_pos_1  ||  v_pos_2),
	-- 'FM', (v_pos_6  ||  v_pos_7),
	-- 'FO', (v_pos_8  || v_pos_9),
	-- 'FP', (v_pos_2  ||  v_pos_3),
	-- 'FT', (v_pos_4  ||  v_pos_5),
	-- 'GI', (v_pos_10  ||  v_pos_11),
	-- 'GL',(v_pos_20 || v_pos_21),
	-- 'GM', (v_pos_1  ||  v_pos_2),
	-- 'GP', (v_pos_8  || v_pos_9),
	-- 'GS',(v_pos_3  ||  v_pos_4),
	-- 'II', (v_pos_1  ||  v_pos_2),
	-- 'IM', (v_pos_1  ||  v_pos_2),
	-- 'MI',(v_pos_10  ||  v_pos_11),
	-- 'ML', (v_pos_16  ||  v_pos_17),
	-- 'MP', (v_pos_1  ||  v_pos_2),
	-- 'M2', (v_pos_15  ||  v_pos_16),'  ')
	-- 
	-- 
	-- 
	-- 
	DECODE(Type_Bureau,
		'AI', ( v_pos_11 || v_pos_12 
		),
		'AL', ( v_pos_15 || v_pos_16 
		),
		'AN', ( v_pos_12 || v_pos_13 
		),
		'AP', ( v_pos_12 || v_pos_13 
		),
		'A2', ( v_pos_8 || v_pos_9 
		),
		'A3', ( v_pos_8 || v_pos_9 
		),
		'BB', ( v_pos_20 || v_pos_21 
		),
		'BC', ( v_pos_18 || v_pos_19 
		),
		'BE', ( v_pos_4 || v_pos_5 
		),
		'BF', ( v_pos_1 || v_pos_2 
		),
		'BP', ( ' ' || v_pos_2 
		),
		'BI', ( v_pos_3 || v_pos_4 
		),
		'BL', ( v_pos_3 || v_pos_4 
		),
		'BM', ( v_pos_20 || v_pos_21 
		),
		'BT', ( v_pos_11 || v_pos_12 
		),
		'B2', ( v_pos_14 || v_pos_15 
		),
		'CF', ( v_pos_8 || v_pos_9 
		),
		'CI', ( v_pos_3 || v_pos_4 
		),
		'CN', ( v_pos_1 || v_pos_2 
		),
		'CP', ( v_pos_3 || v_pos_4 
		),
		'EI', ( v_pos_2 || v_pos_3 
		),
		'EQ', ( v_pos_8 || v_pos_9 
		),
		'FF', ( v_pos_8 || v_pos_9 
		),
		'FI', ( v_pos_1 || v_pos_2 
		),
		'FM', ( v_pos_6 || v_pos_7 
		),
		'FO', ( v_pos_8 || v_pos_9 
		),
		'FP', ( v_pos_2 || v_pos_3 
		),
		'FT', ( v_pos_4 || v_pos_5 
		),
		'GI', ( v_pos_10 || v_pos_11 
		),
		'GL', ( v_pos_20 || v_pos_21 
		),
		'GM', ( v_pos_1 || v_pos_2 
		),
		'GP', ( v_pos_8 || v_pos_9 
		),
		'GS', ( v_pos_3 || v_pos_4 
		),
		'II', ( v_pos_1 || v_pos_2 
		),
		'IM', ( v_pos_1 || v_pos_2 
		),
		'MI', ( v_pos_10 || v_pos_11 
		),
		'ML', ( v_pos_16 || v_pos_17 
		),
		'MP', ( v_pos_1 || v_pos_2 
		),
		'M2', ( v_pos_15 || v_pos_16 
		),
		'  '
	) AS V_Policy_Type,
	V_Policy_Type AS Policy_Type,
	-- *INF*: SUBSTR(sar_class_code,1,3)
	SUBSTR(sar_class_code, 1, 3
	) AS v_sar_class_3,
	-- *INF*: DECODE(TRUE,
	-- IN (Type_Bureau,'BP','FP','BF','FT'),V_Policy_Type)
	DECODE(TRUE,
		Type_Bureau IN ('BP','FP','BF','FT'), V_Policy_Type
	) AS v_type_policy_45,
	-- *INF*: DECODE(TRUE,
	-- Type_Bureau='BP',v_pos_2,
	-- Type_Bureau='BF',v_pos_2,
	-- Type_Bureau='FP',' ',
	-- Type_Bureau='FT',' '  )
	DECODE(TRUE,
		Type_Bureau = 'BP', v_pos_2,
		Type_Bureau = 'BF', v_pos_2,
		Type_Bureau = 'FP', ' ',
		Type_Bureau = 'FT', ' '
	) AS v_type_of_bond_6,
	-- *INF*: DECODE(TRUE,
	--  IN(Type_Bureau,'BP','BF','FP','FT'),v_sar_class_3  || v_type_policy_45 || v_type_of_bond_6,
	-- sar_class_code)
	DECODE(TRUE,
		Type_Bureau IN ('BP','BF','FP','FT'), v_sar_class_3 || v_type_policy_45 || v_type_of_bond_6,
		sar_class_code
	) AS v_hold_sar_class_code,
	v_hold_sar_class_code AS sar_class_code_out
	FROM mplt_Coverage_Temp_Policy_Transaction_Attributes
),
LKP_gtamTM08_stage AS (
	SELECT
	coverage_code,
	major_peril
	FROM (
		SELECT gtam_tm08_stage.coverage_code as coverage_code, 
		RTRIM(gtam_tm08_stage.major_peril) as major_peril 
		FROM gtam_tm08_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY major_peril ORDER BY coverage_code DESC) = 1
),
EXP_Derive_Values AS (
	SELECT
	EXP_policy_claim_attribute_outputs.pol_symbol AS Policy_Symbol,
	EXP_policy_claim_attribute_outputs.ins_line,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(ins_line)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(ins_line
	) AS ins_line_Out,
	EXP_policy_claim_attribute_outputs.loc_unit_num_out AS loc_unit_num,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(loc_unit_num)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(loc_unit_num
	) AS loc_unit_num_out,
	EXP_policy_claim_attribute_outputs.risk_unit_grp_out AS risk_unit_grp,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(risk_unit_grp)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(risk_unit_grp
	) AS risk_unit_grp_out,
	EXP_policy_claim_attribute_outputs.risk_unit_out AS risk_unit,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(risk_unit)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(risk_unit
	) AS risk_unit_out,
	EXP_policy_claim_attribute_outputs.major_peril_code_out AS major_peril_code_source,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(major_peril_code_source)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(major_peril_code_source
	) AS major_peril_code_out,
	EXP_policy_claim_attribute_outputs.cause_of_loss,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(cause_of_loss)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(cause_of_loss
	) AS cause_of_loss_out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.type_bureau_code_Out AS type_bureau_code,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.risk_state_prov_code_Out AS risk_state_prov_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(risk_state_prov_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(risk_state_prov_code
	) AS risk_state_prov_code_out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.risk_zip_code_Out AS risk_zip_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(risk_zip_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(risk_zip_code
	) AS risk_zip_code_out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.terr_code_Out AS terr_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(terr_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(terr_code
	) AS terr_code_out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.tax_loc_Out AS tax_loc,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRING_NUMERIC(tax_loc)
	:UDF.DEFAULT_VALUE_FOR_STRING_NUMERIC(tax_loc
	) AS tax_loc_Out,
	EXP_Transform_Statistical_Codes.sar_class_code_out AS class_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(class_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(class_code
	) AS class_code_out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.exposure_Out AS exposure,
	-- *INF*: IIF(isnull(exposure),0,exposure)
	IFF(exposure IS NULL,
		0,
		exposure
	) AS exposure_out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.sub_line_code_Out AS sub_line_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(sub_line_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(sub_line_code
	) AS sub_line_code_out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.source_sar_asl_Out AS source_sar_asl,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(source_sar_asl)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(source_sar_asl
	) AS source_sar_asl_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.source_sar_prdct_line_Out AS source_sar_prdct_line,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(source_sar_prdct_line)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(source_sar_prdct_line
	) AS source_sar_prdct_line_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.source_sar_sp_use_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(source_sar_sp_use_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(source_sar_sp_use_code
	) AS source_sar_sp_use_code_out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.source_statistical_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.source_statistical_line_Out AS source_statistical_line_Out_unused,
	EXP_Transform_Statistical_Codes.Formatted_Stat_Codes AS source_statistical_code1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(source_statistical_code1)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(source_statistical_code1
	) AS source_statistical_code1_out,
	EXP_Transform_Statistical_Codes.Formatted_Stat_Codes_26_34 AS source_statistical_code2,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(source_statistical_code2)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(source_statistical_code2
	) AS source_statistical_code2_out,
	EXP_Transform_Statistical_Codes.Formatted_Stat_Codes_34_38 AS source_statistical_code3,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(source_statistical_code3)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(source_statistical_code3
	) AS source_statistical_code3_out,
	-- *INF*: DECODE(TRUE,IN( type_bureau_code,'AL','LP','AI','LI','RL'), '100',
	-- IN( type_bureau_code,'GS','GM','RG'),'400',
	-- IN( type_bureau_code,'WC','WP'),'500',
	-- IN( type_bureau_code,'GL','GI','GN','RQ'),'600',
	-- IN( type_bureau_code,'FF','FM','BF','BP','FT','FP'),'711',
	-- IN( type_bureau_code,'BD'),'722',
	-- IN( type_bureau_code,'BI','BT','RB'),'800')
	DECODE(TRUE,
		type_bureau_code IN ('AL','LP','AI','LI','RL'), '100',
		type_bureau_code IN ('GS','GM','RG'), '400',
		type_bureau_code IN ('WC','WP'), '500',
		type_bureau_code IN ('GL','GI','GN','RQ'), '600',
		type_bureau_code IN ('FF','FM','BF','BP','FT','FP'), '711',
		type_bureau_code IN ('BD'), '722',
		type_bureau_code IN ('BI','BT','RB'), '800'
	) AS V_Statistical_Line,
	-- *INF*: IIF(ISNULL(V_Statistical_Line),'N/A',V_Statistical_Line)
	IFF(V_Statistical_Line IS NULL,
		'N/A',
		V_Statistical_Line
	) AS Statistical_Line,
	EXP_Transform_Statistical_Codes.Policy_Type AS policy_type,
	LKP_gtamTM08_stage.coverage_code,
	-- *INF*: IIF(ISNULL(coverage_code),'N/A',coverage_code)
	IFF(coverage_code IS NULL,
		'N/A',
		coverage_code
	) AS coverage_code_out,
	-1 AS Default_Id,
	'N/A' AS Default_NA,
	-- *INF*: SUBSTR(source_sar_prdct_line,1,2)
	SUBSTR(source_sar_prdct_line, 1, 2
	) AS statistical_brkdwn_line,
	EXP_policy_claim_attribute_outputs.mco,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(mco)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(mco
	) AS mco_out,
	EXP_policy_claim_attribute_outputs.pol_eff_date,
	EXP_policy_claim_attribute_outputs.pms_pol_lob_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(pms_pol_lob_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(pms_pol_lob_code
	) AS pms_pol_lob_code_out,
	EXP_policy_claim_attribute_outputs.pif_clb,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(pif_clb)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(pif_clb
	) AS pif_clb_out,
	EXP_policy_claim_attribute_outputs.variation_code,
	-- *INF*: IIF(IN(pms_pol_lob_code,'ACA','AFA','APA','ATA','ACJ','AFJ','APJ'),'6',variation_code)
	IFF(pms_pol_lob_code IN ('ACA','AFA','APA','ATA','ACJ','AFJ','APJ'),
		'6',
		variation_code
	) AS variation_code_out,
	EXP_policy_claim_attribute_outputs.claimant_cov_det_ak_id,
	EXP_policy_claim_attribute_outputs.pms_type_bureau_code,
	EXP_policy_claim_attribute_outputs.claim_loss_trans_fact_id AS claim_loss_transaction_fact_id,
	EXP_policy_claim_attribute_outputs.financial_type_code,
	EXP_policy_claim_attribute_outputs.trans_code,
	EXP_policy_claim_attribute_outputs.trans_date,
	EXP_policy_claim_attribute_outputs.trans_ctgry_code,
	EXP_policy_claim_attribute_outputs.trans_amt,
	EXP_policy_claim_attribute_outputs.trans_hist_amt,
	EXP_policy_claim_attribute_outputs.source_sys_id,
	EXP_policy_claim_attribute_outputs.claim_loss_trans_type_dim_id AS claim_loss_fact_trans_type_dim_id,
	EXP_policy_claim_attribute_outputs.InsuranceReferenceDimId,
	EXP_policy_claim_attribute_outputs.AgencyDimId,
	EXP_policy_claim_attribute_outputs.SalesDivisionDimId,
	EXP_policy_claim_attribute_outputs.InsuranceReferenceCoverageDimId,
	EXP_policy_claim_attribute_outputs.CoverageDetailDimId
	FROM EXP_Transform_Statistical_Codes
	 -- Manually join with EXP_policy_claim_attribute_outputs
	 -- Manually join with mplt_Coverage_Temp_Policy_Transaction_Attributes
	LEFT JOIN LKP_gtamTM08_stage
	ON LKP_gtamTM08_stage.major_peril = EXP_policy_claim_attribute_outputs.major_peril_code_out
),
EXP_converge AS (
	SELECT
	risk_state_prov_code_out AS risk_state_prov_code1,
	-- *INF*: ltrim(rtrim(risk_state_prov_code1))
	ltrim(rtrim(risk_state_prov_code1
		)
	) AS risk_state_prov_code_out,
	risk_zip_code_out AS risk_zip_code1,
	-- *INF*: rtrim(ltrim(risk_zip_code1))
	rtrim(ltrim(risk_zip_code1
		)
	) AS risk_zip_code_out,
	terr_code_out AS terr_code1,
	-- *INF*: ltrim(rtrim(terr_code1))
	ltrim(rtrim(terr_code1
		)
	) AS terr_code_out,
	tax_loc_Out AS tax_loc1,
	-- *INF*: rtrim(ltrim(tax_loc1))
	rtrim(ltrim(tax_loc1
		)
	) AS tax_loc_out,
	class_code_out AS class_code1,
	-- *INF*: rtrim(ltrim(class_code1))
	rtrim(ltrim(class_code1
		)
	) AS class_code_out,
	exposure_out AS exposure,
	sub_line_code_out AS sub_line_code1,
	-- *INF*: rtrim(ltrim(sub_line_code1))
	rtrim(ltrim(sub_line_code1
		)
	) AS sub_line_code_out,
	source_sar_asl_Out AS source_sar_asl1,
	-- *INF*: rtrim(ltrim(source_sar_asl1))
	rtrim(ltrim(source_sar_asl1
		)
	) AS source_sar_asl_out,
	source_sar_prdct_line_Out AS source_sar_prdct_line1,
	-- *INF*: rtrim(ltrim(source_sar_prdct_line1))
	rtrim(ltrim(source_sar_prdct_line1
		)
	) AS source_sar_prdct_line_out,
	source_sar_sp_use_code_out AS source_sar_sp_use_code1,
	-- *INF*: rtrim(ltrim(source_sar_sp_use_code1))
	rtrim(ltrim(source_sar_sp_use_code1
		)
	) AS source_sar_sp_use_code_out,
	source_statistical_code_Out AS source_statistical_code,
	source_statistical_code1_out AS statistical_code1,
	-- *INF*: ltrim(rtrim(statistical_code1))
	ltrim(rtrim(statistical_code1
		)
	) AS statistical_code1_out,
	source_statistical_code2_out AS statistical_code2,
	-- *INF*: ltrim(rtrim(statistical_code2))
	ltrim(rtrim(statistical_code2
		)
	) AS statistical_code2_out,
	source_statistical_code3_out AS statistical_code3,
	-- *INF*: ltrim(rtrim(statistical_code3))
	ltrim(rtrim(statistical_code3
		)
	) AS statistical_code3_out,
	Statistical_Line AS source_statistical_line,
	-- *INF*: ltrim(rtrim(source_statistical_line))
	ltrim(rtrim(source_statistical_line
		)
	) AS source_statistical_line_out,
	variation_code_out AS variation_code,
	-- *INF*: ltrim(rtrim(variation_code))
	ltrim(rtrim(variation_code
		)
	) AS variation_code_out,
	Default_NA AS auto_reins_facility,
	-- *INF*: ltrim(rtrim(auto_reins_facility))
	ltrim(rtrim(auto_reins_facility
		)
	) AS auto_reins_facility_out,
	statistical_brkdwn_line AS statistical_brkdwn_line1,
	-- *INF*: rtrim(ltrim(statistical_brkdwn_line1))
	rtrim(ltrim(statistical_brkdwn_line1
		)
	) AS statistical_brkdwn_line_out,
	policy_type AS Policy_Type,
	-- *INF*: --IIF(ISNULL(Policy_Type),'N/A',Policy_Type)
	-- -- go back to UDF
	-- :UDF.DEFAULT_VALUE_FOR_STRINGS(Policy_Type)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(Policy_Type
	) AS policy_type_out,
	coverage_code_out AS coverage_code_in,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(coverage_code_in)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(coverage_code_in
	) AS coverage_code_out
	FROM EXP_Derive_Values
),
LKP_loss_master_dim AS (
	SELECT
	loss_master_dim_id,
	risk_state_prov_code,
	risk_zip_code,
	terr_code,
	tax_loc,
	class_code,
	exposure,
	sub_line_code,
	source_sar_asl,
	source_sar_prdct_line,
	source_sar_sp_use_code,
	source_statistical_line,
	variation_code,
	pol_type,
	auto_reins_facility,
	statistical_brkdwn_line,
	statistical_code1,
	statistical_code2,
	statistical_code3,
	loss_master_cov_code
	FROM (
		SELECT loss_master_dim.loss_master_dim_id      AS loss_master_dim_id,
		       ltrim(rtrim(loss_master_dim.risk_state_prov_code))    AS risk_state_prov_code,
		       ltrim(rtrim(loss_master_dim.risk_zip_code))           AS risk_zip_code,
		       ltrim(rtrim(loss_master_dim.terr_code))               AS terr_code,
		       ltrim(rtrim(loss_master_dim.tax_loc))                 AS tax_loc,
		       ltrim(rtrim(loss_master_dim.class_code))              AS class_code,
		       loss_master_dim.exposure                AS exposure,
		       ltrim(rtrim(loss_master_dim.sub_line_code))           AS sub_line_code,
		       ltrim(rtrim(loss_master_dim.source_sar_asl))          AS source_sar_asl,
		       ltrim(rtrim(loss_master_dim.source_sar_prdct_line))   AS source_sar_prdct_line,
		       ltrim(rtrim(loss_master_dim.source_sar_sp_use_code))  AS source_sar_sp_use_code,
		       ltrim(rtrim(loss_master_dim.source_statistical_line)) AS source_statistical_line,
		       ltrim(rtrim(loss_master_dim.variation_code))          AS variation_code,
		       ltrim(rtrim(loss_master_dim.pol_type))                AS pol_type,
		       ltrim(rtrim(loss_master_dim.auto_reins_facility))     AS auto_reins_facility,
		       ltrim(rtrim(loss_master_dim.statistical_brkdwn_line)) AS statistical_brkdwn_line,
		       ltrim(rtrim(loss_master_dim.statistical_code1))       AS statistical_code1,
		       ltrim(rtrim(loss_master_dim.statistical_code2))       AS statistical_code2,
		       ltrim(rtrim(loss_master_dim.statistical_code3))       AS statistical_code3,
		       ltrim(rtrim(loss_master_dim.loss_master_cov_code))    AS loss_master_cov_code
		FROM   loss_master_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY risk_state_prov_code,risk_zip_code,terr_code,tax_loc,class_code,exposure,sub_line_code,source_sar_asl,source_sar_prdct_line,source_sar_sp_use_code,source_statistical_line,variation_code,pol_type,auto_reins_facility,statistical_brkdwn_line,statistical_code1,statistical_code2,statistical_code3,loss_master_cov_code ORDER BY loss_master_dim_id DESC) = 1
),
mplt_hierarchy_product_mapping AS (WITH
	INPUT_hierarchy_product_input AS (
		
	),
	EXP_input AS (
		SELECT
		Symbol AS symbol,
		-- *INF*: iif(symbol <> 'N/A',substr(symbol,1,2),symbol)
		IFF(symbol <> 'N/A',
			substr(symbol, 1, 2
			),
			symbol
		) AS symbol_2pos,
		Class_of_Business AS class_of_business,
		Line_of_Business AS line_of_business,
		Insurance_Line AS insurance_line,
		Type_Bureau AS type_bureau,
		Major_Peril AS major_peril,
		Class_Code AS class_code,
		Sub_Line AS sub_line,
		Risk_Unit_Group AS risk_unit_group,
		Eff_Date AS eff_date
		FROM INPUT_hierarchy_product_input
	),
	EXP_apply_product_rules AS (
		SELECT
		symbol_2pos,
		class_of_business,
		line_of_business,
		insurance_line,
		type_bureau,
		major_peril,
		class_code,
		sub_line,
		risk_unit_group,
		eff_date,
		-- *INF*: DECODE(TRUE,
		-- IN(symbol_2pos,'CP','NS') AND insurance_line='GL' AND IN(major_peril,'530','599') AND RTRIM(class_code)='99999' AND IN(sub_line,'334','336'),'320',
		-- 
		-- IN(symbol_2pos,'CP','NS') AND type_bureau='IM','550',
		-- 
		-- symbol_2pos='CP' AND insurance_line='GL' AND sub_line='365','380',
		-- 
		-- IN(symbol_2pos, 'CP','NS') AND insurance_line='GL' AND IN(major_peril,'599','919') AND IN(risk_unit_group,'345','367'),'300',
		-- 
		-- IN(symbol_2pos, 'CP','NS') AND insurance_line='GL' AND IN(major_peril,'530','540','919','599') AND RTRIM(class_code) <>'99999' AND NOT IN(risk_unit_group,'345','346','355','900','901','367','286','365'),'300',
		-- 
		-- IN(symbol_2pos,'CF','CP','NS') AND IN(insurance_line,'BM','CF','CG','CR','GS','N/A') AND NOT IN(type_bureau,'AL','AP','AN','GL','IM'),'500',
		-- 
		-- IN(symbol_2pos,'BC','BD','BG','BH','CA','CP','NB','NS','NA','XX') AND IN(insurance_line,'N/A','CA')  AND IN(type_bureau,'AL','AP','AN'),'200',
		-- 
		-- IN(symbol_2pos,'CP','NS') AND insurance_line='GL' AND risk_unit_group='355','370',
		-- 
		-- IN(symbol_2pos,'BA','BB','XX') AND IN(line_of_business,'BOP','BO') AND NOT IN(insurance_line,'CA'),'400',
		-- 
		-- symbol_2pos='CM' AND insurance_line='GL' AND IN(risk_unit_group,'901','902','903'),'360',
		-- 
		-- IN(symbol_2pos,'CP','NS') AND insurance_line='GL'  AND risk_unit_group='345','365',
		-- 
		-- IN(symbol_2pos,'CU','NU','CP','UC') AND type_bureau='GL' AND IN(major_peril,'517'),'900',
		-- 
		-- IN(symbol_2pos,'BC','BD') AND IN(insurance_line,'CF','GL','CR','IM','CG','N/A') ,'410',
		-- 
		-- symbol_2pos='CP' AND insurance_line='GL'  AND risk_unit_group='346','321',
		-- 
		-- IN(symbol_2pos,'NA','NB') AND IN(insurance_line,'CF','GL','CR','IM','CG'),'430',
		-- 
		-- IN(symbol_2pos,'BG','BH','GG') AND IN(insurance_line,'CF','GL','CR','IM','GA','CG','N/A'),'420',
		-- 
		-- symbol_2pos='NF' AND IN(class_of_business,'XN','XO','XP','XQ'),'620',
		-- 
		-- IN(symbol_2pos,'CD','CM') AND IN(risk_unit_group,'367','900'),'350',
		-- 
		-- IN(symbol_2pos,'BA','BB') AND insurance_line='GL' AND IN(risk_unit_group,'110','111'),'200',
		-- 
		-- IN(symbol_2pos,'CP','NS') AND insurance_line='GA','340',
		-- 
		-- IN(symbol_2pos,'HH','HA','HB','HX','IB','IP','PA','PX','XX') AND IN(type_bureau,'PH','PI','PL','PQ','MS'),'800',
		-- 
		-- symbol_2pos='NF' AND class_of_business = '9','510',
		-- 
		-- ----IN(Line_of_Business,'APV','ASV','FP','HP','IMP'),'810',  'Personal Lines Monoline',
		-- 
		-- symbol_2pos='BO','450',
		-- 
		-- IN(symbol_2pos,'GL','XX') AND IN(major_peril,'084','085'),'300',
		-- 
		-- symbol_2pos='NN','310',
		-- 
		-- symbol_2pos='NK','311',
		-- 
		-- symbol_2pos='NE','330',
		-- 
		-- major_peril='032','100',
		-- 
		-- symbol_2pos='NC','610',
		-- 
		-- symbol_2pos='NJ','630',
		-- 
		-- symbol_2pos='NL','640',
		-- 
		-- symbol_2pos='NM','650',
		-- 
		-- symbol_2pos='NO','660',
		-- 
		-- symbol_2pos='FF','510',
		-- 
		-- IN(symbol_2pos,'FL','FP') AND IN(type_bureau,'PF','PQ','MS'),'820',
		-- 
		-- symbol_2pos='HH' AND type_bureau='PF','820',
		-- 
		-- IN(symbol_2pos,'HH','PA','PM','PP','PS','PT','HA','XX','XA') AND IN(type_bureau,'RL','RP','RN'),'850',
		-- 
		-- IN(symbol_2pos,'HH','UP','HX','XX') AND type_bureau ='GL' AND major_peril='017','890',
		-- 
		-- '000')
		DECODE(TRUE,
			symbol_2pos IN ('CP','NS') 
			AND insurance_line = 'GL' 
			AND major_peril IN ('530','599') 
			AND RTRIM(class_code
			) = '99999' 
			AND sub_line IN ('334','336'), '320',
			symbol_2pos IN ('CP','NS') 
			AND type_bureau = 'IM', '550',
			symbol_2pos = 'CP' 
			AND insurance_line = 'GL' 
			AND sub_line = '365', '380',
			symbol_2pos IN ('CP','NS') 
			AND insurance_line = 'GL' 
			AND major_peril IN ('599','919') 
			AND risk_unit_group IN ('345','367'), '300',
			symbol_2pos IN ('CP','NS') 
			AND insurance_line = 'GL' 
			AND major_peril IN ('530','540','919','599') 
			AND RTRIM(class_code
			) <> '99999' 
			AND NOT risk_unit_group IN ('345','346','355','900','901','367','286','365'), '300',
			symbol_2pos IN ('CF','CP','NS') 
			AND insurance_line IN ('BM','CF','CG','CR','GS','N/A') 
			AND NOT type_bureau IN ('AL','AP','AN','GL','IM'), '500',
			symbol_2pos IN ('BC','BD','BG','BH','CA','CP','NB','NS','NA','XX') 
			AND insurance_line IN ('N/A','CA') 
			AND type_bureau IN ('AL','AP','AN'), '200',
			symbol_2pos IN ('CP','NS') 
			AND insurance_line = 'GL' 
			AND risk_unit_group = '355', '370',
			symbol_2pos IN ('BA','BB','XX') 
			AND line_of_business IN ('BOP','BO') 
			AND NOT insurance_line IN ('CA'), '400',
			symbol_2pos = 'CM' 
			AND insurance_line = 'GL' 
			AND risk_unit_group IN ('901','902','903'), '360',
			symbol_2pos IN ('CP','NS') 
			AND insurance_line = 'GL' 
			AND risk_unit_group = '345', '365',
			symbol_2pos IN ('CU','NU','CP','UC') 
			AND type_bureau = 'GL' 
			AND major_peril IN ('517'), '900',
			symbol_2pos IN ('BC','BD') 
			AND insurance_line IN ('CF','GL','CR','IM','CG','N/A'), '410',
			symbol_2pos = 'CP' 
			AND insurance_line = 'GL' 
			AND risk_unit_group = '346', '321',
			symbol_2pos IN ('NA','NB') 
			AND insurance_line IN ('CF','GL','CR','IM','CG'), '430',
			symbol_2pos IN ('BG','BH','GG') 
			AND insurance_line IN ('CF','GL','CR','IM','GA','CG','N/A'), '420',
			symbol_2pos = 'NF' 
			AND class_of_business IN ('XN','XO','XP','XQ'), '620',
			symbol_2pos IN ('CD','CM') 
			AND risk_unit_group IN ('367','900'), '350',
			symbol_2pos IN ('BA','BB') 
			AND insurance_line = 'GL' 
			AND risk_unit_group IN ('110','111'), '200',
			symbol_2pos IN ('CP','NS') 
			AND insurance_line = 'GA', '340',
			symbol_2pos IN ('HH','HA','HB','HX','IB','IP','PA','PX','XX') 
			AND type_bureau IN ('PH','PI','PL','PQ','MS'), '800',
			symbol_2pos = 'NF' 
			AND class_of_business = '9', '510',
			symbol_2pos = 'BO', '450',
			symbol_2pos IN ('GL','XX') 
			AND major_peril IN ('084','085'), '300',
			symbol_2pos = 'NN', '310',
			symbol_2pos = 'NK', '311',
			symbol_2pos = 'NE', '330',
			major_peril = '032', '100',
			symbol_2pos = 'NC', '610',
			symbol_2pos = 'NJ', '630',
			symbol_2pos = 'NL', '640',
			symbol_2pos = 'NM', '650',
			symbol_2pos = 'NO', '660',
			symbol_2pos = 'FF', '510',
			symbol_2pos IN ('FL','FP') 
			AND type_bureau IN ('PF','PQ','MS'), '820',
			symbol_2pos = 'HH' 
			AND type_bureau = 'PF', '820',
			symbol_2pos IN ('HH','PA','PM','PP','PS','PT','HA','XX','XA') 
			AND type_bureau IN ('RL','RP','RN'), '850',
			symbol_2pos IN ('HH','UP','HX','XX') 
			AND type_bureau = 'GL' 
			AND major_peril = '017', '890',
			'000'
		) AS v_product_code,
		v_product_code AS product_code_out
		FROM EXP_input
	),
	LKP_product_code_dim AS (
		SELECT
		prdct_code_dim_id,
		prdct_code,
		prdct_code_descript,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT 
			product_code_dim.prdct_code_dim_id as prdct_code_dim_id,
			product_code_dim.prdct_code as prdct_code, 
			product_code_dim.prdct_code_descript as prdct_code_descript, product_code_dim.eff_from_date as eff_from_date, 
			product_code_dim.eff_to_date as eff_to_date 
			FROM 
			product_code_dim
			where
			crrnt_snpsht_flag=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY prdct_code,eff_from_date,eff_to_date ORDER BY prdct_code_dim_id) = 1
	),
	EXP_validate_dim_id AS (
		SELECT
		prdct_code_dim_id,
		-- *INF*: IIF(isnull(prdct_code_dim_id),-1,prdct_code_dim_id)
		IFF(prdct_code_dim_id IS NULL,
			- 1,
			prdct_code_dim_id
		) AS prdct_code_dim_id_out,
		prdct_code_descript,
		-- *INF*: IIF(isnull(prdct_code_descript),'N/A',rtrim(ltrim(prdct_code_descript)))
		IFF(prdct_code_descript IS NULL,
			'N/A',
			rtrim(ltrim(prdct_code_descript
				)
			)
		) AS prdct_code_descript_out,
		prdct_code,
		-- *INF*: IIF(isnull(prdct_code),'N/A',rtrim(ltrim(prdct_code)))
		IFF(prdct_code IS NULL,
			'N/A',
			rtrim(ltrim(prdct_code
				)
			)
		) AS prdct_code_out
		FROM LKP_product_code_dim
	),
	OUTPUT_hierarchy_product_code AS (
		SELECT
		prdct_code_out AS code, 
		prdct_code_descript_out AS name, 
		prdct_code_dim_id_out AS prdct_code_dim_id
		FROM EXP_validate_dim_id
	),
),
EXP_prepare_ASL_input AS (
	SELECT
	EXP_Derive_Values.Policy_Symbol AS pol_sym,
	EXP_Derive_Values.mco_out AS mco,
	EXP_Derive_Values.pif_clb_out AS pif_clb,
	EXP_Derive_Values.pms_pol_lob_code_out AS pms_pol_lob_code,
	EXP_Derive_Values.major_peril_code_out AS major_peril_code,
	EXP_Derive_Values.pms_type_bureau_code AS type_bureau_code,
	EXP_Derive_Values.risk_unit_grp_out AS risk_unit_grp,
	EXP_Derive_Values.risk_unit_out AS risk_unit,
	EXP_Derive_Values.loc_unit_num_out AS loc_unit_num,
	EXP_Derive_Values.cause_of_loss_out AS cause_of_loss,
	LKP_loss_master_dim.loss_master_dim_id AS lkp_loss_master_dim_id,
	-- *INF*: IIF(isnull(lkp_loss_master_dim_id),-1,lkp_loss_master_dim_id)
	IFF(lkp_loss_master_dim_id IS NULL,
		- 1,
		lkp_loss_master_dim_id
	) AS loss_master_dim_id,
	-- *INF*: Decode(TRUE,
	-- substr(pol_sym,1,1)='N','N',
	-- in(substr(pol_sym,1,1),'R','S','T') and pms_pol_lob_code='WCP','N',
	-- in(substr(pol_sym,1,1),'A','J','L'),'A',
	-- 'W')
	Decode(TRUE,
		substr(pol_sym, 1, 1
		) = 'N', 'N',
		substr(pol_sym, 1, 1
		) IN ('R','S','T') 
		AND pms_pol_lob_code = 'WCP', 'N',
		substr(pol_sym, 1, 1
		) IN ('A','J','L'), 'A',
		'W'
	) AS reporting_dvsn_code,
	EXP_Derive_Values.sub_line_code_out,
	EXP_Derive_Values.class_code_out,
	EXP_Derive_Values.claimant_cov_det_ak_id,
	EXP_Derive_Values.claim_loss_transaction_fact_id,
	EXP_Derive_Values.financial_type_code,
	EXP_Derive_Values.trans_code,
	EXP_Derive_Values.trans_date,
	EXP_Derive_Values.trans_ctgry_code,
	EXP_Derive_Values.trans_amt,
	EXP_Derive_Values.trans_hist_amt,
	EXP_Derive_Values.source_sys_id,
	mplt_hierarchy_product_mapping.prdct_code_dim_id,
	EXP_Derive_Values.claim_loss_fact_trans_type_dim_id,
	EXP_Derive_Values.pol_eff_date,
	EXP_Derive_Values.InsuranceReferenceDimId,
	EXP_Derive_Values.AgencyDimId,
	EXP_Derive_Values.SalesDivisionDimId,
	EXP_Derive_Values.InsuranceReferenceCoverageDimId,
	EXP_Derive_Values.CoverageDetailDimId
	FROM EXP_Derive_Values
	 -- Manually join with mplt_hierarchy_product_mapping
	LEFT JOIN LKP_loss_master_dim
	ON LKP_loss_master_dim.risk_state_prov_code = EXP_converge.risk_state_prov_code_out AND LKP_loss_master_dim.risk_zip_code = EXP_converge.risk_zip_code_out AND LKP_loss_master_dim.terr_code = EXP_converge.terr_code_out AND LKP_loss_master_dim.tax_loc = EXP_converge.tax_loc_out AND LKP_loss_master_dim.class_code = EXP_converge.class_code_out AND LKP_loss_master_dim.exposure = EXP_converge.exposure AND LKP_loss_master_dim.sub_line_code = EXP_converge.sub_line_code_out AND LKP_loss_master_dim.source_sar_asl = EXP_converge.source_sar_asl_out AND LKP_loss_master_dim.source_sar_prdct_line = EXP_converge.source_sar_prdct_line_out AND LKP_loss_master_dim.source_sar_sp_use_code = EXP_converge.source_sar_sp_use_code_out AND LKP_loss_master_dim.source_statistical_line = EXP_converge.source_statistical_line_out AND LKP_loss_master_dim.variation_code = EXP_converge.variation_code_out AND LKP_loss_master_dim.pol_type = EXP_converge.policy_type_out AND LKP_loss_master_dim.auto_reins_facility = EXP_converge.auto_reins_facility_out AND LKP_loss_master_dim.statistical_brkdwn_line = EXP_converge.statistical_brkdwn_line_out AND LKP_loss_master_dim.statistical_code1 = EXP_converge.statistical_code1_out AND LKP_loss_master_dim.statistical_code2 = EXP_converge.statistical_code2_out AND LKP_loss_master_dim.statistical_code3 = EXP_converge.statistical_code3_out AND LKP_loss_master_dim.loss_master_cov_code = EXP_converge.coverage_code_out
),
mplt_ASL_Policy_Symbol_Changes AS (WITH
	IN_ASL_Policy_Symbol_Changes AS (
		
	),
	EXP_accept_inputs AS (
		SELECT
		symbol,
		line_of_business,
		master_co_number,
		major_peril,
		type_bureau
		FROM IN_ASL_Policy_Symbol_Changes
	),
	EXP_3000_check_sys38_policy AS (
		SELECT
		symbol,
		line_of_business,
		master_co_number,
		major_peril,
		type_bureau,
		-- *INF*: decode(substr(symbol,1,2) ,
		-- 'HX',1,
		-- 'PX',1,
		-- 'WM',1,
		-- 'WX',1,
		-- 'XA',1,
		-- 'XX',1, 
		-- 'DU',1,
		-- 0)
		decode(substr(symbol, 1, 2
			),
			'HX', 1,
			'PX', 1,
			'WM', 1,
			'WX', 1,
			'XA', 1,
			'XX', 1,
			'DU', 1,
			0
		) AS symbol_change,
		-- *INF*: substr(symbol,3,1)
		substr(symbol, 3, 1
		) AS v_symbol_3,
		-- *INF*: IIF(symbol_change=1,
		-- 	DECODE(TRUE,
		-- 		line_of_business='HAP' and type_bureau !='GL', 'HH',
		-- 		line_of_business='APV','PA',
		-- 		rtrim(ltrim(line_of_business))='HP' and master_co_number='06','HA',
		-- 		rtrim(ltrim(line_of_business))='HP' and master_co_number='05', 'HB',
		-- 		line_of_business='IMP','IP',
		-- 		line_of_business='HAP' and type_bureau='GL' , 'HH',
		-- 		rtrim(ltrim(line_of_business))='GL' and major_peril='017','UP', 
		-- 	substr(symbol,1,2)
		-- 	)
		--   ,substr(symbol,1,2)
		-- )
		-- 
		-- --IIF(line_of_business='HAP' and type_bureau !='GL', 'HH',
		-- --	IIF(line_of_business='APV','PA',
		-- --		IIF(rtrim(ltrim(line_of_business))='HP' and master_co_number='06','HA',
		-- --			IIF(rtrim(ltrim(line_of_business))='HP' and master_co_number='05', 'HB',
		-- --				IIF(line_of_business='IMP','IP',
		-- --					IIF(line_of_business='HAP' and type_bureau='GL' , 'HH',
		-- --						IIF(rtrim(ltrim(line_of_business))='GL' and major_peril='017','UP', 
		-- --						substr(symbol,1,2))
		-- --					)
		-- --				)
		-- --			)
		-- --		)
		-- --	)
		-- --)
		-- 
		-- 
		IFF(symbol_change = 1,
			DECODE(TRUE,
			line_of_business = 'HAP' 
				AND type_bureau != 'GL', 'HH',
			line_of_business = 'APV', 'PA',
			rtrim(ltrim(line_of_business
					)
				) = 'HP' 
				AND master_co_number = '06', 'HA',
			rtrim(ltrim(line_of_business
					)
				) = 'HP' 
				AND master_co_number = '05', 'HB',
			line_of_business = 'IMP', 'IP',
			line_of_business = 'HAP' 
				AND type_bureau = 'GL', 'HH',
			rtrim(ltrim(line_of_business
					)
				) = 'GL' 
				AND major_peril = '017', 'UP',
			substr(symbol, 1, 2
				)
			),
			substr(symbol, 1, 2
			)
		) AS v_symbol_1_2,
		-- *INF*: IIF(symbol_change=1,
		-- 	IIF(line_of_business='HAP' and type_bureau='GL', '017',major_peril)
		-- ,major_peril)
		-- 
		-- 
		IFF(symbol_change = 1,
			IFF(line_of_business = 'HAP' 
				AND type_bureau = 'GL',
				'017',
				major_peril
			),
			major_peril
		) AS v_major_peril,
		-- *INF*: IIF(symbol_change=1,
		-- 	IIF(rtrim(ltrim(line_of_business))='GL' and major_peril='017','GL',type_bureau)
		-- ,type_bureau)
		-- 
		-- 
		IFF(symbol_change = 1,
			IFF(rtrim(ltrim(line_of_business
					)
				) = 'GL' 
				AND major_peril = '017',
				'GL',
				type_bureau
			),
			type_bureau
		) AS v_type_bureau,
		-- *INF*: concat(v_symbol_1_2,v_symbol_3)
		concat(v_symbol_1_2, v_symbol_3
		) AS symbol_out,
		v_major_peril AS major_peril_out,
		v_type_bureau AS type_bureau_out
		FROM EXP_accept_inputs
	),
	OUTPUT AS (
		SELECT
		symbol_out AS symbol, 
		major_peril_out AS major_peril, 
		type_bureau_out AS type_bureau
		FROM EXP_3000_check_sys38_policy
	),
),
EXP_asl_mapplet_bridge AS (
	SELECT
	symbol1 AS symbol,
	major_peril1 AS major_peril,
	type_bureau1 AS type_bureau
	FROM mplt_ASL_Policy_Symbol_Changes
),
mplt_ASL_WBC8827B_Product_Coverage_Codes AS (WITH
	INPUT AS (
		
	),
	EXP_accept_inputs AS (
		SELECT
		symbol,
		type_bureau,
		major_peril,
		unit_number,
		location_number,
		class_of_business,
		subline,
		class_code,
		risk_unit_group,
		loss_cause,
		nsi_indicator,
		policy_effective_date,
		-- *INF*: IIF(ISNULL(policy_effective_date),TO_DATE('01/01/1800','MM/DD/YYYY'),policy_effective_date)
		IFF(policy_effective_date IS NULL,
			TO_DATE('01/01/1800', 'MM/DD/YYYY'
			),
			policy_effective_date
		) AS policy_effective_date_out,
		-- *INF*: TO_DATE('06/01/2012 00:00:00','MM/DD/YYYY HH24:MI:SS')
		-- 
		-- -- prod 3017 6/1/2012 is the date for applying new earthquake coverages
		TO_DATE('06/01/2012 00:00:00', 'MM/DD/YYYY HH24:MI:SS'
		) AS CL_EQ_EFF_Date
		FROM INPUT
	),
	EXP_evaluate_step_1 AS (
		SELECT
		symbol,
		type_bureau,
		major_peril,
		unit_number,
		location_number,
		class_of_business,
		subline,
		class_code,
		risk_unit_group,
		loss_cause,
		nsi_indicator,
		policy_effective_date_out AS policy_effective_date,
		CL_EQ_EFF_Date,
		-- *INF*: substr(risk_unit_group,1,3)
		substr(risk_unit_group, 1, 3
		) AS v_risk_unit_group_1_3,
		-- *INF*: substr(symbol,1,2)
		substr(symbol, 1, 2
		) AS v_symbol_pos_1_2,
		v_symbol_pos_1_2 AS symbol_pos_1_2_out,
		-- *INF*: IIF(v_symbol_pos_1_2='HH' and type_bureau='PF' and (in(major_peril,'081','280',@{pipeline().parameters.MP_210_211},@{pipeline().parameters.MP_249_250})),1,0)
		IFF(v_symbol_pos_1_2 = 'HH' 
			AND type_bureau = 'PF' 
			AND ( major_peril IN ('081','280',@{pipeline().parameters.MP_210_211},@{pipeline().parameters.MP_249_250}) 
			),
			1,
			0
		) AS v_home_and_highway_fire_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HH' and type_bureau='PF' and in(major_peril,@{pipeline().parameters.MP_220_230}),1,0)
		IFF(v_symbol_pos_1_2 = 'HH' 
			AND type_bureau = 'PF' 
			AND major_peril IN (@{pipeline().parameters.MP_220_230}),
			1,
			0
		) AS v_home_and_highway_allied_lines_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HH' and type_bureau='PH' and in(major_peril,'002','097','911','914'),1,0)
		IFF(v_symbol_pos_1_2 = 'HH' 
			AND type_bureau = 'PH' 
			AND major_peril IN ('002','097','911','914'),
			1,
			0
		) AS v_home_and_highway_homeowners_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HH' and  type_bureau='PI' and in( major_peril,'042','062','200','201','206'),1,0)
		IFF(v_symbol_pos_1_2 = 'HH' 
			AND type_bureau = 'PI' 
			AND major_peril IN ('042','062','200','201','206'),
			1,
			0
		) AS v_home_and_highway_inland_marine_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HH' and type_bureau='PQ' and in(major_peril,@{pipeline().parameters.MP_260_261}),1,0)
		IFF(v_symbol_pos_1_2 = 'HH' 
			AND type_bureau = 'PQ' 
			AND major_peril IN (@{pipeline().parameters.MP_260_261}),
			1,
			0
		) AS v_home_and_highway_earthquake_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HH' and type_bureau='PL',1,0) 
		IFF(v_symbol_pos_1_2 = 'HH' 
			AND type_bureau = 'PL',
			1,
			0
		) AS v_home_and_highway_personal_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HH' and  type_bureau='GL' and major_peril='017',1,0)
		IFF(v_symbol_pos_1_2 = 'HH' 
			AND type_bureau = 'GL' 
			AND major_peril = '017',
			1,
			0
		) AS v_home_and_highway_general_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HH'  and in(type_bureau,'RL','RP','RN') and in(major_peril,'150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143}),1,0) 
		IFF(v_symbol_pos_1_2 = 'HH' 
			AND type_bureau IN ('RL','RP','RN') 
			AND major_peril IN ('150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143}),
			1,
			0
		) AS v_home_and_highway_pp_auto_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HH' and type_bureau='RP' and in(major_peril,'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}),1,0)
		IFF(v_symbol_pos_1_2 = 'HH' 
			AND type_bureau = 'RP' 
			AND major_peril IN ('168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}),
			1,
			0
		) AS v_home_and_highway_pp_auto_physical_damage_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='PP' and in(type_bureau,'RL','RP','RN') and in(major_peril,'150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143}),1,0)
		IFF(v_symbol_pos_1_2 = 'PP' 
			AND type_bureau IN ('RL','RP','RN') 
			AND major_peril IN ('150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143}),
			1,
			0
		) AS v_preferred_auto_pp_auto_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='PP' and type_bureau='RP' and in(major_peril,'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}),1,0)
		IFF(v_symbol_pos_1_2 = 'PP' 
			AND type_bureau = 'RP' 
			AND major_peril IN ('168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}),
			1,
			0
		) AS v_preferred_auto_pp_auto_physical_damage_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='PA' and in(type_bureau,'RL','RP','RN') and in(major_peril,'150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143}),1,0)
		IFF(v_symbol_pos_1_2 = 'PA' 
			AND type_bureau IN ('RL','RP','RN') 
			AND major_peril IN ('150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143}),
			1,
			0
		) AS v_select_auto_pp_auto_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='PA' and type_bureau='RP' and in(major_peril,'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}),1,0)
		IFF(v_symbol_pos_1_2 = 'PA' 
			AND type_bureau = 'RP' 
			AND major_peril IN ('168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}),
			1,
			0
		) AS v_select_auto_pp_auto_physical_damage_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HB' and type_bureau='NB' and in(major_peril,@{pipeline().parameters.MP_220_230}),1,0)
		IFF(v_symbol_pos_1_2 = 'HB' 
			AND type_bureau = 'NB' 
			AND major_peril IN (@{pipeline().parameters.MP_220_230}),
			1,
			0
		) AS v_standard_homeowners_allied_lined_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HB' and  type_bureau='PH' and major_peril='002',1,0)
		IFF(v_symbol_pos_1_2 = 'HB' 
			AND type_bureau = 'PH' 
			AND major_peril = '002',
			1,
			0
		) AS v_standard_homeowners_homeowners_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HB' and type_bureau='PI' and in(major_peril,'042','044','062','200','201','206'),1,0)
		IFF(v_symbol_pos_1_2 = 'HB' 
			AND type_bureau = 'PI' 
			AND major_peril IN ('042','044','062','200','201','206'),
			1,
			0
		) AS v_standard_homeowners_inland_marine_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HB' and type_bureau='PQ' and in(major_peril,@{pipeline().parameters.MP_260_261}),1,0)
		-- 
		IFF(v_symbol_pos_1_2 = 'HB' 
			AND type_bureau = 'PQ' 
			AND major_peril IN (@{pipeline().parameters.MP_260_261}),
			1,
			0
		) AS v_standard_homeowners_earthquake_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'HB' AND type_bureau = 'PL',1,0)
		IFF(v_symbol_pos_1_2 = 'HB' 
			AND type_bureau = 'PL',
			1,
			0
		) AS v_standard_homeowners_personal_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HA' and type_bureau='NB' and in(major_peril,@{pipeline().parameters.MP_220_230}),1,0)
		-- 
		IFF(v_symbol_pos_1_2 = 'HA' 
			AND type_bureau = 'NB' 
			AND major_peril IN (@{pipeline().parameters.MP_220_230}),
			1,
			0
		) AS v_select_homeowners_allied_lines_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'HA' AND type_bureau = 'PH' AND
		--               major_peril = '002',1,0)
		IFF(v_symbol_pos_1_2 = 'HA' 
			AND type_bureau = 'PH' 
			AND major_peril = '002',
			1,
			0
		) AS v_select_homeowners_homeowners_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'HA' AND type_bureau = 'PI' AND in(major_peril,'042','044','062',                     '200','201','206'),1,0)
		IFF(v_symbol_pos_1_2 = 'HA' 
			AND type_bureau = 'PI' 
			AND major_peril IN ('042','044','062','200','201','206'),
			1,
			0
		) AS v_select_homeowners_inland_marine_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'HA' AND type_bureau = 'PQ' AND in(major_peril,@{pipeline().parameters.MP_260_261}),1,0)
		IFF(v_symbol_pos_1_2 = 'HA' 
			AND type_bureau = 'PQ' 
			AND major_peril IN (@{pipeline().parameters.MP_260_261}),
			1,
			0
		) AS v_select_homeowners_earthquake_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'HA' AND type_bureau = 'PL',1,0)
		IFF(v_symbol_pos_1_2 = 'HA' 
			AND type_bureau = 'PL',
			1,
			0
		) AS v_select_homeowners_personal_liability_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2 , 'FP' ,'FL') AND type_bureau = 'PF' AND
		--               in(major_peril ,'081',@{pipeline().parameters.MP_210_211},@{pipeline().parameters.MP_249_250}),1,0)
		IFF(v_symbol_pos_1_2 IN ('FP','FL') 
			AND type_bureau = 'PF' 
			AND major_peril IN ('081',@{pipeline().parameters.MP_210_211},@{pipeline().parameters.MP_249_250}),
			1,
			0
		) AS v_dwelling_fire_fire_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2, 'FP','FL') AND in(type_bureau,'PF','NB') AND in(major_peril,@{pipeline().parameters.MP_220_230}),1,0)
		IFF(v_symbol_pos_1_2 IN ('FP','FL') 
			AND type_bureau IN ('PF','NB') 
			AND major_peril IN (@{pipeline().parameters.MP_220_230}),
			1,
			0
		) AS v_dwelling_fire_allied_lines_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'FP','FL') AND type_bureau = 'PQ' AND in(major_peril,@{pipeline().parameters.MP_260_261}),1,0)
		IFF(v_symbol_pos_1_2 IN ('FP','FL') 
			AND type_bureau = 'PQ' 
			AND major_peril IN (@{pipeline().parameters.MP_260_261}),
			1,
			0
		) AS v_dwelling_fire_earthquake_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'IP' AND type_bureau= 'PI',1,0)
		IFF(v_symbol_pos_1_2 = 'IP' 
			AND type_bureau = 'PI',
			1,
			0
		) AS v_personal_inland_marine_inland_marine_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'IP' AND type_bureau = 'PL',1,0)
		IFF(v_symbol_pos_1_2 = 'IP' 
			AND type_bureau = 'PL',
			1,
			0
		) AS v_personal_inland_marine_personal_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'PM' AND
		--               in(type_bureau,'RL','RP','RN') AND
		--               in(major_peril,'150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143}),1,0)
		IFF(v_symbol_pos_1_2 = 'PM' 
			AND type_bureau IN ('RL','RP','RN') 
			AND major_peril IN ('150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143}),
			1,
			0
		) AS v_motorcycle_pp_auto_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'PM' AND type_bureau = 'RP' AND
		--  in(major_peril, '168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}),1,0)
		IFF(v_symbol_pos_1_2 = 'PM' 
			AND type_bureau = 'RP' 
			AND major_peril IN ('168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}),
			1,
			0
		) AS v_motorcycle_pp_auto_physical_damage_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'IB' AND type_bureau = 'PI',1,0)
		IFF(v_symbol_pos_1_2 = 'IB' 
			AND type_bureau = 'PI',
			1,
			0
		) AS v_boatowners_inland_marine_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'IB' AND type_bureau = 'PL',1,0)
		IFF(v_symbol_pos_1_2 = 'IB' 
			AND type_bureau = 'PL',
			1,
			0
		) AS v_boatowners_personal_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'PS' AND
		--               in(type_bureau,'RL', 'RP', 'RN') AND
		--              in(major_peril,'150',@{pipeline().parameters.MP_100_130}, @{pipeline().parameters.MP_140_143}),1,0)
		IFF(v_symbol_pos_1_2 = 'PS' 
			AND type_bureau IN ('RL','RP','RN') 
			AND major_peril IN ('150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143}),
			1,
			0
		) AS v_alternative_one_pp_auto_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'PS' AND type_bureau = 'RP' AND
		-- in(major_peril, '168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}),1,0)
		IFF(v_symbol_pos_1_2 = 'PS' 
			AND type_bureau = 'RP' 
			AND major_peril IN ('168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}),
			1,
			0
		) AS v_alternative_one_pp_auto_physical_damage_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'PT' AND
		--               in(type_bureau,'RL','RP','RN') AND
		--               in(major_peril,'150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143}),1,0)
		IFF(v_symbol_pos_1_2 = 'PT' 
			AND type_bureau IN ('RL','RP','RN') 
			AND major_peril IN ('150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143}),
			1,
			0
		) AS v_alternative_one_star_pp_auto_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'PT' AND type_bureau = 'RP' AND
		-- in(major_peril,'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}),1,0)
		IFF(v_symbol_pos_1_2 = 'PT' 
			AND type_bureau = 'RP' 
			AND major_peril IN ('168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}),
			1,
			0
		) AS v_alternative_one_star_pp_auto_physical_damage_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2, 'BC','BD','CP','BG','BH','GG','CA') AND
		--  in(type_bureau,'AN','AL') AND
		--  in(major_peril ,'150','599',@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},@{pipeline().parameters.MP_930_931}) AND 
		-- NOT in(subline,@{pipeline().parameters.GARAGE_SUBLINES}),1,0)
		IFF(v_symbol_pos_1_2 IN ('BC','BD','CP','BG','BH','GG','CA') 
			AND type_bureau IN ('AN','AL') 
			AND major_peril IN ('150','599',@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},@{pipeline().parameters.MP_930_931}) 
			AND NOT subline IN (@{pipeline().parameters.GARAGE_SUBLINES}),
			1,
			0
		) AS v_commercial_auto_commercial_auto_liability_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2, 'NA','NB','NS') AND
		--               in(type_bureau, 'AN','AL') AND
		--               in(major_peril,'150','599',@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},@{pipeline().parameters.MP_930_931}) AND
		--  NOT in(subline,@{pipeline().parameters.GARAGE_SUBLINES}),1,0)
		IFF(v_symbol_pos_1_2 IN ('NA','NB','NS') 
			AND type_bureau IN ('AN','AL') 
			AND major_peril IN ('150','599',@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},@{pipeline().parameters.MP_930_931}) 
			AND NOT subline IN (@{pipeline().parameters.GARAGE_SUBLINES}),
			1,
			0
		) AS v_commercial_auto_commercial_auto_liability_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BC','BD','CP','BG','BH','GG') AND
		--          in(type_bureau,'AN','AL') AND
		--          in(major_peril,'150','599',@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},@{pipeline().parameters.MP_930_931}) 	   AND
		--          in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) AND
		--          unit_number = '999' AND
		--          (is_spaces(location_number) = 1 OR rtrim(ltrim(location_number)) = '0000' ),1,0)
		IFF(v_symbol_pos_1_2 IN ('BC','BD','CP','BG','BH','GG') 
			AND type_bureau IN ('AN','AL') 
			AND major_peril IN ('150','599',@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},@{pipeline().parameters.MP_930_931}) 
			AND subline IN (@{pipeline().parameters.GARAGE_SUBLINES}) 
			AND unit_number = '999' 
			AND ( LENGTH(location_number)>0 AND TRIM(location_number)='' = 1 
				OR rtrim(ltrim(location_number
					)
				) = '0000' 
			),
			1,
			0
		) AS v_commercial_auto_commercial_auto_liability_garage_veh_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB','NS') AND
		--         in(type_bureau,'AN','AL') AND
		--         in(major_peril,'150','599',@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},@{pipeline().parameters.MP_930_931}) AND
		--         in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) AND
		--         unit_number = '999' AND
		--          (is_spaces(location_number) OR rtrim(ltrim(location_number))='0000'),1,0)
		IFF(v_symbol_pos_1_2 IN ('NA','NB','NS') 
			AND type_bureau IN ('AN','AL') 
			AND major_peril IN ('150','599',@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},@{pipeline().parameters.MP_930_931}) 
			AND subline IN (@{pipeline().parameters.GARAGE_SUBLINES}) 
			AND unit_number = '999' 
			AND ( LENGTH(location_number)>0 AND TRIM(location_number)='' 
				OR rtrim(ltrim(location_number
					)
				) = '0000' 
			),
			1,
			0
		) AS v_commercial_auto_commercial_auto_liability_garage_veh_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BC','BD','CP','BG','BH','GG','CA','GA') AND
		--               type_bureau = 'AP' AND
		--               NOT in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) AND
		--              in (major_peril,'132','147','177','178',@{pipeline().parameters.MP_145_146},@{pipeline().parameters.MP_148_160},@{pipeline().parameters.MP_163_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}),1,0)
		IFF(v_symbol_pos_1_2 IN ('BC','BD','CP','BG','BH','GG','CA','GA') 
			AND type_bureau = 'AP' 
			AND NOT subline IN (@{pipeline().parameters.GARAGE_SUBLINES}) 
			AND major_peril IN ('132','147','177','178',@{pipeline().parameters.MP_145_146},@{pipeline().parameters.MP_148_160},@{pipeline().parameters.MP_163_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}),
			1,
			0
		) AS v_commercial_auto_comm_auto_physical_damage_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB','NS') AND
		--        type_bureau = 'AP' AND
		--        NOT in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) AND
		--        in(major_peril,'132','147','177','178',@{pipeline().parameters.MP_145_146},@{pipeline().parameters.MP_148_160},@{pipeline().parameters.MP_163_166}, @{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}),1,0)
		IFF(v_symbol_pos_1_2 IN ('NA','NB','NS') 
			AND type_bureau = 'AP' 
			AND NOT subline IN (@{pipeline().parameters.GARAGE_SUBLINES}) 
			AND major_peril IN ('132','147','177','178',@{pipeline().parameters.MP_145_146},@{pipeline().parameters.MP_148_160},@{pipeline().parameters.MP_163_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}),
			1,
			0
		) AS v_commercial_auto_comm_auto_physical_damage_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2, 'BC','BD','CP','BG','BH','GG') AND
		--         type_bureau = 'AP' AND
		--         in(major_peril,'132','147','177','178',@{pipeline().parameters.MP_145_146}, @{pipeline().parameters.MP_148_160},@{pipeline().parameters.MP_163_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}) AND
		--          in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) AND
		--          unit_number = '999' AND
		--           (is_spaces(location_number)=1 OR rtrim(ltrim(location_number))= '0000'),1,0)
		IFF(v_symbol_pos_1_2 IN ('BC','BD','CP','BG','BH','GG') 
			AND type_bureau = 'AP' 
			AND major_peril IN ('132','147','177','178',@{pipeline().parameters.MP_145_146},@{pipeline().parameters.MP_148_160},@{pipeline().parameters.MP_163_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}) 
			AND subline IN (@{pipeline().parameters.GARAGE_SUBLINES}) 
			AND unit_number = '999' 
			AND ( LENGTH(location_number)>0 AND TRIM(location_number)='' = 1 
				OR rtrim(ltrim(location_number
					)
				) = '0000' 
			),
			1,
			0
		) AS v_commercial_auto_comm_auto_physical_damage_garage_veh_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2, 'NA','NB','NS') AND
		--         type_bureau = 'AP' AND
		--         in(major_peril,'132','147','177','178',@{pipeline().parameters.MP_145_146}, @{pipeline().parameters.MP_148_160}, @{pipeline().parameters.MP_163_166}, @{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}) AND
		--               in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) AND
		--              unit_number = '999' AND
		--               (is_spaces(location_number)=1 OR  rtrim(ltrim(location_number))= '0000'),1,0)
		IFF(v_symbol_pos_1_2 IN ('NA','NB','NS') 
			AND type_bureau = 'AP' 
			AND major_peril IN ('132','147','177','178',@{pipeline().parameters.MP_145_146},@{pipeline().parameters.MP_148_160},@{pipeline().parameters.MP_163_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}) 
			AND subline IN (@{pipeline().parameters.GARAGE_SUBLINES}) 
			AND unit_number = '999' 
			AND ( LENGTH(location_number)>0 AND TRIM(location_number)='' = 1 
				OR rtrim(ltrim(location_number
					)
				) = '0000' 
			),
			1,
			0
		) AS v_commercial_auto_comm_auto_physical_damage_garage_veh_nsi,
		-- *INF*: IIF(v_symbol_pos_1_2='CP' and in(type_bureau,'AN','AL') and in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) and in(major_peril,'599',@{pipeline().parameters.MP_100_130}, @{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_930_931}),1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau IN ('AN','AL') 
			AND subline IN (@{pipeline().parameters.GARAGE_SUBLINES}) 
			AND major_peril IN ('599',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_930_931}),
			1,
			0
		) AS v_garage_liability_commercial_auto_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='NS' and in(type_bureau,'AN','AL') and in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) and in(major_peril,'599',@{pipeline().parameters.MP_100_130}, @{pipeline().parameters.MP_271_274}, @{pipeline().parameters.MP_930_931}),1,0)
		IFF(v_symbol_pos_1_2 = 'NS' 
			AND type_bureau IN ('AN','AL') 
			AND subline IN (@{pipeline().parameters.GARAGE_SUBLINES}) 
			AND major_peril IN ('599',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_930_931}),
			1,
			0
		) AS v_garage_liability_commercial_auto_liability_nsi,
		-- *INF*: IIF(v_symbol_pos_1_2= 'CP' AND
		--        type_bureau = 'AP' AND 
		--       in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) AND
		--       in(major_peril,'132','177','178',@{pipeline().parameters.MP_145_159},@{pipeline().parameters.MP_165_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270})
		-- ,1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau = 'AP' 
			AND subline IN (@{pipeline().parameters.GARAGE_SUBLINES}) 
			AND major_peril IN ('132','177','178',@{pipeline().parameters.MP_145_159},@{pipeline().parameters.MP_165_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}),
			1,
			0
		) AS v_garage_liability_commercial_auto_physical_damage_wbm,
		-- *INF*: IIF (v_symbol_pos_1_2 = 'NS' AND
		--         type_bureau = 'AP' AND 
		--        in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) AND
		--        in(major_peril,'132','177','178',@{pipeline().parameters.MP_145_159},@{pipeline().parameters.MP_165_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}) ,1,0)
		IFF(v_symbol_pos_1_2 = 'NS' 
			AND type_bureau = 'AP' 
			AND subline IN (@{pipeline().parameters.GARAGE_SUBLINES}) 
			AND major_peril IN ('132','177','178',@{pipeline().parameters.MP_145_159},@{pipeline().parameters.MP_165_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}),
			1,
			0
		) AS v_garage_liability_commercial_auto_physical_damage_nsi,
		-- *INF*: IIF(in(substr(symbol,1,1),'V','W','Y') AND
		--          in(type_bureau,'WC','WP'),1,0)
		IFF(substr(symbol, 1, 1
			) IN ('V','W','Y') 
			AND type_bureau IN ('WC','WP'),
			1,
			0
		) AS v_workers_comp_total_non_pool_workers_comp_wbm,
		-- *INF*: IIF(in(substr(symbol,1,1),'R','S','T') AND
		--        in(type_bureau,'WC','WP'),1,0)
		IFF(substr(symbol, 1, 1
			) IN ('R','S','T') 
			AND type_bureau IN ('WC','WP'),
			1,
			0
		) AS v_workers_comp_total_non_pool_workers_comp_nsi,
		-- *INF*: IIF(in(substr(symbol,1,1),'A','J','L') AND
		--        in(type_bureau,'WC','WP'),1,0)
		IFF(substr(symbol, 1, 1
			) IN ('A','J','L') 
			AND type_bureau IN ('WC','WP'),
			1,
			0
		) AS v_workers_comp_total_non_pool_workers_comp_argent,
		-- *INF*:  IIF(v_symbol_pos_1_2 = 'CP' AND
		--        type_bureau = 'CF' AND
		--        in(major_peril,'415','463','490','496','498','599','919','425','426','435','455','480') AND
		--        NOT in(class_of_business,'I','O') AND
		-- 	 in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		-- 	policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau = 'CF' 
			AND major_peril IN ('415','463','490','496','498','599','919','425','426','435','455','480') 
			AND NOT class_of_business IN ('I','O') 
			AND v_risk_unit_group_1_3 IN (@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) 
			AND policy_effective_date >= CL_EQ_EFF_Date,
			1,
			0
		) AS v_commercial_property_earthquake_wbm,
		-- *INF*:  IIF(v_symbol_pos_1_2 = 'CP' AND
		--        type_bureau = 'CF' AND
		--        in(major_peril,'415','463','490','496','498','599','919') AND
		--        NOT in(class_of_business,'I','O'),1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau = 'CF' 
			AND major_peril IN ('415','463','490','496','498','599','919') 
			AND NOT class_of_business IN ('I','O'),
			1,
			0
		) AS v_commercial_property_fire_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NS' AND
		--         type_bureau = 'CF' AND
		--         in(major_peril, '415','463','490','496','498','599','919','425','426','435','455','480') AND
		-- 	  in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		-- 	  policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(v_symbol_pos_1_2 = 'NS' 
			AND type_bureau = 'CF' 
			AND major_peril IN ('415','463','490','496','498','599','919','425','426','435','455','480') 
			AND v_risk_unit_group_1_3 IN (@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) 
			AND policy_effective_date >= CL_EQ_EFF_Date,
			1,
			0
		) AS v_commercial_property_earthquake_nsi,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NS' AND
		--         type_bureau = 'CF' AND
		--         in(major_peril, '415','463','490','496','498','599','919'),1,0)
		IFF(v_symbol_pos_1_2 = 'NS' 
			AND type_bureau = 'CF' 
			AND major_peril IN ('415','463','490','496','498','599','919'),
			1,
			0
		) AS v_commercial_property_fire_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2 ,'CP','PX','SM') AND
		--         in(type_bureau,'CF','NB','GS') AND
		--          in(major_peril,'425','426','435','220','455','480','599','227') AND
		--          NOT in(class_of_business,'I','O'),1,0)
		IFF(v_symbol_pos_1_2 IN ('CP','PX','SM') 
			AND type_bureau IN ('CF','NB','GS') 
			AND major_peril IN ('425','426','435','220','455','480','599','227') 
			AND NOT class_of_business IN ('I','O'),
			1,
			0
		) AS v_commercial_property_allied_lines_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NS' AND
		--        in(type_bureau,'CF','NB','GS') AND
		--        in(major_peril,'425','426','435','220','455','480','599','227'),1,0)
		IFF(v_symbol_pos_1_2 = 'NS' 
			AND type_bureau IN ('CF','NB','GS') 
			AND major_peril IN ('425','426','435','220','455','480','599','227'),
			1,
			0
		) AS v_commercial_property_allied_lines_nsi,
		-- *INF*:  IIF(v_symbol_pos_1_2 = 'CP' AND 
		--        type_bureau = 'CF' AND
		--        in(major_peril,'415','463','490','496','498','599' ,'919','425','426','435','455','480') AND
		--        class_of_business = 'I' AND
		--        in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		--        policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau = 'CF' 
			AND major_peril IN ('415','463','490','496','498','599','919','425','426','435','455','480') 
			AND class_of_business = 'I' 
			AND v_risk_unit_group_1_3 IN (@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) 
			AND policy_effective_date >= CL_EQ_EFF_Date,
			1,
			0
		) AS v_metalworkers_earthquake_wbm,
		-- *INF*:  IIF(v_symbol_pos_1_2 = 'CP' AND 
		--        type_bureau = 'CF' AND
		--        in(major_peril,'415','463','490','496','498','599' ,'919') AND
		--        class_of_business = 'I',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau = 'CF' 
			AND major_peril IN ('415','463','490','496','498','599','919') 
			AND class_of_business = 'I',
			1,
			0
		) AS v_metalworkers_fire_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND
		--        in(type_bureau,'CF','NB','GS') AND
		--        in(major_peril,'425','426','435','455','480') AND
		--         class_of_business = 'I',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau IN ('CF','NB','GS') 
			AND major_peril IN ('425','426','435','455','480') 
			AND class_of_business = 'I',
			1,
			0
		) AS v_metalworkers_allied_lines_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND 
		--       type_bureau = 'IM' AND
		--       in(major_peril,'551','599','919') AND
		--       class_of_business = 'I',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau = 'IM' 
			AND major_peril IN ('551','599','919') 
			AND class_of_business = 'I',
			1,
			0
		) AS v_metalworkers_inland_marine_wbm,
		-- *INF*:  IIF(v_symbol_pos_1_2 = 'CP' AND type_bureau = 'GL' AND
		--        in(major_peril,'530','599','919') AND
		--        in(subline,@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350}) AND
		--        class_of_business = 'I',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau = 'GL' 
			AND major_peril IN ('530','599','919') 
			AND subline IN (@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350}) 
			AND class_of_business = 'I',
			1,
			0
		) AS v_metalworkers_general_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND 
		--       type_bureau = 'GL' AND
		--       in(major_peril,'530','550','599') AND
		--       in(subline,'336','365') AND
		--       class_of_business = 'I',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau = 'GL' 
			AND major_peril IN ('530','550','599') 
			AND subline IN ('336','365') 
			AND class_of_business = 'I',
			1,
			0
		) AS v_metalworkers_products_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CM' AND type_bureau = 'GL' AND
		--              major_peril = '540' AND
		--              subline = '336' AND 
		--              class_of_business = 'I',1,0)
		IFF(v_symbol_pos_1_2 = 'CM' 
			AND type_bureau = 'GL' 
			AND major_peril = '540' 
			AND subline = '336' 
			AND class_of_business = 'I',
			1,
			0
		) AS v_metalworkers_claims_made_product_liability_wbm,
		-- *INF*:  IIF(in(v_symbol_pos_1_2,'CP','FF') AND
		--         in(type_bureau,'FT','CR') AND
		--         in(major_peril,'566','016') AND
		--         class_of_business = 'I',1,0)
		IFF(v_symbol_pos_1_2 IN ('CP','FF') 
			AND type_bureau IN ('FT','CR') 
			AND major_peril IN ('566','016') 
			AND class_of_business = 'I',
			1,
			0
		) AS v_metalworkers_fidelity_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND
		--       in(type_bureau,'FT','BT','CR') AND
		--       in(major_peril,'565','599') AND
		--       class_of_business = 'I',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau IN ('FT','BT','CR') 
			AND major_peril IN ('565','599') 
			AND class_of_business = 'I',
			1,
			0
		) AS v_metalworkers_burglary_and_theft_wbm,
		-- *INF*:  IIF(v_symbol_pos_1_2 = 'CP' AND 
		--        type_bureau = 'CF' AND
		--        in(major_peril, '415','463','490','496','498','599','919','425','426','435','455','480') AND
		--        class_of_business = 'O' AND
		--        in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		--        policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau = 'CF' 
			AND major_peril IN ('415','463','490','496','498','599','919','425','426','435','455','480') 
			AND class_of_business = 'O' 
			AND v_risk_unit_group_1_3 IN (@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) 
			AND policy_effective_date >= CL_EQ_EFF_Date,
			1,
			0
		) AS v_woodworkers_earthquake_wbm,
		-- *INF*:  IIF(v_symbol_pos_1_2 = 'CP' AND 
		--        type_bureau = 'CF' AND
		--        in(major_peril, '415','463','490','496','498','599','919') AND
		--        class_of_business = 'O',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau = 'CF' 
			AND major_peril IN ('415','463','490','496','498','599','919') 
			AND class_of_business = 'O',
			1,
			0
		) AS v_woodworkers_fire_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND
		--      in(type_bureau,'CF','NB','GS') AND
		--      in(major_peril,'425','426','435','455','480') AND
		--      class_of_business = 'O',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau IN ('CF','NB','GS') 
			AND major_peril IN ('425','426','435','455','480') 
			AND class_of_business = 'O',
			1,
			0
		) AS v_woodworkers_allied_lines_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND 
		--       type_bureau = 'IM' AND
		--       in(major_peril,'551','599','919') AND
		--       class_of_business = 'O',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau = 'IM' 
			AND major_peril IN ('551','599','919') 
			AND class_of_business = 'O',
			1,
			0
		) AS v_woodworkers_inland_marine_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND 
		--       type_bureau = 'GL' AND
		--       in(major_peril,'530','599','919') AND
		--       in(subline,@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350}) AND
		--       class_of_business = 'O',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau = 'GL' 
			AND major_peril IN ('530','599','919') 
			AND subline IN (@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350}) 
			AND class_of_business = 'O',
			1,
			0
		) AS v_woodworkers_general_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND 
		--       type_bureau = 'GL' AND
		--      in(major_peril,'530','550','599') AND
		--      in(subline,'336','365') AND
		--      class_of_business = 'O',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau = 'GL' 
			AND major_peril IN ('530','550','599') 
			AND subline IN ('336','365') 
			AND class_of_business = 'O',
			1,
			0
		) AS v_woodworkers_products_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CM' AND 
		--       type_bureau = 'GL' AND
		--       major_peril = '540' AND
		--       subline = '336' AND 
		--       class_of_business = 'O',1,0)
		IFF(v_symbol_pos_1_2 = 'CM' 
			AND type_bureau = 'GL' 
			AND major_peril = '540' 
			AND subline = '336' 
			AND class_of_business = 'O',
			1,
			0
		) AS v_woodworkers_claims_made_product_liability_wbm,
		-- *INF*:  IIF(in(v_symbol_pos_1_2,'CP','FF') AND
		--         in(type_bureau,'FT','CR') AND
		--         in (major_peril,'566','016') AND
		--         class_of_business = 'O',1,0)
		IFF(v_symbol_pos_1_2 IN ('CP','FF') 
			AND type_bureau IN ('FT','CR') 
			AND major_peril IN ('566','016') 
			AND class_of_business = 'O',
			1,
			0
		) AS v_woodworkers_fidelity_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND
		--       in(type_bureau,'FT','BT','CR') AND
		--       in(major_peril,'565','599') AND
		--       class_of_business= 'O',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau IN ('FT','BT','CR') 
			AND major_peril IN ('565','599') 
			AND class_of_business = 'O',
			1,
			0
		) AS v_woodworkers_burglary_and_theft_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'CP','GL','SM','XX') AND
		--         type_bureau = 'GL' AND
		--        in(major_peril,'530','599','084','085','919') AND
		--        in(subline,@{pipeline().parameters.SUB_313_315},@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350}) AND
		--        NOT in(class_of_business,'I','O'),1,0)
		IFF(v_symbol_pos_1_2 IN ('CP','GL','SM','XX') 
			AND type_bureau = 'GL' 
			AND major_peril IN ('530','599','084','085','919') 
			AND subline IN (@{pipeline().parameters.SUB_313_315},@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350}) 
			AND NOT class_of_business IN ('I','O'),
			1,
			0
		) AS v_general_liability_general_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NS' AND
		--         type_bureau = 'GL' AND
		--         in(major_peril,'530','599','084','085','919') AND
		--         in(subline,@{pipeline().parameters.SUB_313_315},@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350}),1,0)
		IFF(v_symbol_pos_1_2 = 'NS' 
			AND type_bureau = 'GL' 
			AND major_peril IN ('530','599','084','085','919') 
			AND subline IN (@{pipeline().parameters.SUB_313_315},@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350}),
			1,
			0
		) AS v_general_liability_general_liability_nsi,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND
		--        type_bureau = 'GL' AND
		--        in(major_peril,'530','550','599') AND
		--        in(subline,'336','365') AND
		--        not in(class_of_business,'I','O'),1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau = 'GL' 
			AND major_peril IN ('530','550','599') 
			AND subline IN ('336','365') 
			AND NOT class_of_business IN ('I','O'),
			1,
			0
		) AS v_general_liability_products_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'BO' AND
		--        type_bureau = 'GL' AND
		--        in(major_peril,'530','550','599') AND
		--        in(subline,'336','365'),1,0)
		IFF(v_symbol_pos_1_2 = 'BO' 
			AND type_bureau = 'GL' 
			AND major_peril IN ('530','550','599') 
			AND subline IN ('336','365'),
			1,
			0
		) AS v_smart_products_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NS' AND
		--        type_bureau = 'GL' AND
		--        in(major_peril,'530','599') AND
		--        subline = '336',1,0)
		IFF(v_symbol_pos_1_2 = 'NS' 
			AND type_bureau = 'GL' 
			AND major_peril IN ('530','599') 
			AND subline = '336',
			1,
			0
		) AS v_general_liability_products_liability_nsi,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CM' AND
		--       type_bureau = 'GL' AND
		--       major_peril = '540' AND
		--       subline = '336' AND
		--       not in(class_of_business,'I','O'),1,0)
		IFF(v_symbol_pos_1_2 = 'CM' 
			AND type_bureau = 'GL' 
			AND major_peril = '540' 
			AND subline = '336' 
			AND NOT class_of_business IN ('I','O'),
			1,
			0
		) AS v_general_liability_claims_made_product_liability_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NE','NS') AND
		--             type_bureau = 'GL' AND
		--            major_peril = '540' AND
		--            subline= '336',1,0)
		IFF(v_symbol_pos_1_2 IN ('NE','NS') 
			AND type_bureau = 'GL' 
			AND major_peril = '540' 
			AND subline = '336',
			1,
			0
		) AS v_general_liability_claims_made_product_liability_nsi,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NS' AND
		--       type_bureau= 'GL' AND
		--      major_peril= '540' AND
		--      subline= '334',1,0)
		IFF(v_symbol_pos_1_2 = 'NS' 
			AND type_bureau = 'GL' 
			AND major_peril = '540' 
			AND subline = '334',
			1,
			0
		) AS v_general_liability_claims_made_general_liability_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'CP','SM') AND
		--       in(type_bureau,'FT','BT','CR') AND
		--       in(major_peril,'566','016') AND
		--       not in(class_of_business,'I','O'),1,0)
		IFF(v_symbol_pos_1_2 IN ('CP','SM') 
			AND type_bureau IN ('FT','BT','CR') 
			AND major_peril IN ('566','016') 
			AND NOT class_of_business IN ('I','O'),
			1,
			0
		) AS v_crime_fidelity_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'BO' AND
		--       in(type_bureau,'FT','BT','CR') AND
		--       in(major_peril,'566','016'),1,0)
		IFF(v_symbol_pos_1_2 = 'BO' 
			AND type_bureau IN ('FT','BT','CR') 
			AND major_peril IN ('566','016'),
			1,
			0
		) AS v_smart_fidelity_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NS' AND
		--       in(type_bureau,'FT','BT','CR') AND
		--       in(major_peril,'566','016'),1,0)
		IFF(v_symbol_pos_1_2 = 'NS' 
			AND type_bureau IN ('FT','BT','CR') 
			AND major_peril IN ('566','016'),
			1,
			0
		) AS v_crime_fidelity_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'CP','SM') AND
		--        in(type_bureau,'FT','BT','CR') AND
		--        in(major_peril,'565','599','015') AND
		--        not in(class_of_business,'I','O'),1,0)
		IFF(v_symbol_pos_1_2 IN ('CP','SM') 
			AND type_bureau IN ('FT','BT','CR') 
			AND major_peril IN ('565','599','015') 
			AND NOT class_of_business IN ('I','O'),
			1,
			0
		) AS v_crime_burglary_and_theft_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NS' AND
		--       in(type_bureau,'FT','BT','CR') AND
		--       in(major_peril,'565','599','015'),1,0)
		IFF(v_symbol_pos_1_2 = 'NS' 
			AND type_bureau IN ('FT','BT','CR') 
			AND major_peril IN ('565','599','015'),
			1,
			0
		) AS v_crime_burglary_and_theft_nsi,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND
		--       type_bureau = 'IM' AND
		--       in(major_peril,'551','599','919') AND
		--       not in(class_of_business,'I','O'),1,0)
		IFF(v_symbol_pos_1_2 = 'CP' 
			AND type_bureau = 'IM' 
			AND major_peril IN ('551','599','919') 
			AND NOT class_of_business IN ('I','O'),
			1,
			0
		) AS v_commercial_im_inland_marine_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NS' AND
		--       type_bureau = 'IM' AND
		--       in(major_peril,'551','599','919'),1,0)
		IFF(v_symbol_pos_1_2 = 'NS' 
			AND type_bureau = 'IM' 
			AND major_peril IN ('551','599','919'),
			1,
			0
		) AS v_commercial_im_inland_marine_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BA','BB') AND
		--        in(type_bureau,'BB','BC','BE','NB') AND
		--        in(major_peril,'903','904','905','908'),1,0)
		IFF(v_symbol_pos_1_2 IN ('BA','BB') 
			AND type_bureau IN ('BB','BC','BE','NB') 
			AND major_peril IN ('903','904','905','908'),
			1,
			0
		) AS v_bop_cmp_property_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BA','BB') AND
		--        type_bureau = 'BC' AND
		--        major_peril = '919',1,0)
		IFF(v_symbol_pos_1_2 IN ('BA','BB') 
			AND type_bureau = 'BC' 
			AND major_peril = '919',
			1,
			0
		) AS v_bop_unnamed_R13905,
		-- *INF*:  IIF(in(v_symbol_pos_1_2,'BA','BB','XA','XX') AND
		--         in(type_bureau,'BB','BC','BE','NB') AND
		--         in(major_peril,'599',@{pipeline().parameters.MP_901_904}),1,0)
		IFF(v_symbol_pos_1_2 IN ('BA','BB','XA','XX') 
			AND type_bureau IN ('BB','BC','BE','NB') 
			AND major_peril IN ('599',@{pipeline().parameters.MP_901_904}),
			1,
			0
		) AS v_bop_cmp_property_liability_peril_901_904_and_599_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BA','BB','XA') AND
		--        in(type_bureau,'BE','B2') AND
		--        in(major_peril,'907','065','919'),1,0)
		IFF(v_symbol_pos_1_2 IN ('BA','BB','XA') 
			AND type_bureau IN ('BE','B2') 
			AND major_peril IN ('907','065','919'),
			1,
			0
		) AS v_bop_cmp_liability_wbm,
		-- *INF*:  IIF(in(v_symbol_pos_1_2,'BC','BD') AND
		--         type_bureau = 'CF' AND
		--         in(major_peril,'415','463','490','496','498','599','919','425','426','435','455','480') AND
		-- in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		-- policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(v_symbol_pos_1_2 IN ('BC','BD') 
			AND type_bureau = 'CF' 
			AND major_peril IN ('415','463','490','496','498','599','919','425','426','435','455','480') 
			AND v_risk_unit_group_1_3 IN (@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) 
			AND policy_effective_date >= CL_EQ_EFF_Date,
			1,
			0
		) AS v_cbop_earthquake_wbm,
		-- *INF*:  IIF(in(v_symbol_pos_1_2,'BC','BD') AND
		--         type_bureau = 'CF' AND
		--         in(major_peril,'415','463','490','496','498','599','919'),1,0)
		IFF(v_symbol_pos_1_2 IN ('BC','BD') 
			AND type_bureau = 'CF' 
			AND major_peril IN ('415','463','490','496','498','599','919'),
			1,
			0
		) AS v_cbop_fire_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'BO' AND
		--       type_bureau = 'CF' AND
		--       in(major_peril,'415','463','490','496','498','599','919','425','426','435','455','480') AND
		--       in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		--       policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(v_symbol_pos_1_2 = 'BO' 
			AND type_bureau = 'CF' 
			AND major_peril IN ('415','463','490','496','498','599','919','425','426','435','455','480') 
			AND v_risk_unit_group_1_3 IN (@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) 
			AND policy_effective_date >= CL_EQ_EFF_Date,
			1,
			0
		) AS v_smart_earthquake_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'BO' AND
		--       type_bureau = 'CF' AND
		--       in(major_peril,'415','463','490','496','498','599','919'),1,0)
		IFF(v_symbol_pos_1_2 = 'BO' 
			AND type_bureau = 'CF' 
			AND major_peril IN ('415','463','490','496','498','599','919'),
			1,
			0
		) AS v_smart_fire_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB') AND
		--              type_bureau = 'CF' AND
		--              in(major_peril,'415','463','490','496','498','599','919','425','426','435','455','480') AND
		--              in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		--              policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(v_symbol_pos_1_2 IN ('NA','NB') 
			AND type_bureau = 'CF' 
			AND major_peril IN ('415','463','490','496','498','599','919','425','426','435','455','480') 
			AND v_risk_unit_group_1_3 IN (@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) 
			AND policy_effective_date >= CL_EQ_EFF_Date,
			1,
			0
		) AS v_sbop_earthquake_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB') AND
		--              type_bureau = 'CF' AND
		--              in(major_peril,'415','463','490','496','498','599','919'),1,0)
		IFF(v_symbol_pos_1_2 IN ('NA','NB') 
			AND type_bureau = 'CF' 
			AND major_peril IN ('415','463','490','496','498','599','919'),
			1,
			0
		) AS v_sbop_fire_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BC','BD') AND
		--        in(type_bureau,'CF','GS') AND
		--        in(major_peril,'425','426','435','455','480'),1,0)
		IFF(v_symbol_pos_1_2 IN ('BC','BD') 
			AND type_bureau IN ('CF','GS') 
			AND major_peril IN ('425','426','435','455','480'),
			1,
			0
		) AS v_cbop_allied_lines_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'BO' AND
		--       in(type_bureau,'CF','GS') AND
		--       in(major_peril,'425','426','435','455','480'),1,0)
		IFF(v_symbol_pos_1_2 = 'BO' 
			AND type_bureau IN ('CF','GS') 
			AND major_peril IN ('425','426','435','455','480'),
			1,
			0
		) AS v_smart_allied_lines_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB') AND
		--        in(type_bureau,'CF','GS') AND
		--        in(major_peril,'425','426','435','455','480'),1,0)
		IFF(v_symbol_pos_1_2 IN ('NA','NB') 
			AND type_bureau IN ('CF','GS') 
			AND major_peril IN ('425','426','435','455','480'),
			1,
			0
		) AS v_sbop_allied_lines_nsi,
		-- *INF*:  IIF(in(v_symbol_pos_1_2,'BC','BD') AND
		--         type_bureau = 'CF' AND
		--         major_peril = '066',1,0)
		IFF(v_symbol_pos_1_2 IN ('BC','BD') 
			AND type_bureau = 'CF' 
			AND major_peril = '066',
			1,
			0
		) AS v_cbop_cmp_property_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BC','BD') AND
		--        type_bureau = 'IM' AND
		--        in(major_peril,'551','599','919'),1,0)
		IFF(v_symbol_pos_1_2 IN ('BC','BD') 
			AND type_bureau = 'IM' 
			AND major_peril IN ('551','599','919'),
			1,
			0
		) AS v_cbop_inland_marine_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'BO' AND
		--        type_bureau = 'IM' AND
		--        in(major_peril,'551','599','919'),1,0)
		IFF(v_symbol_pos_1_2 = 'BO' 
			AND type_bureau = 'IM' 
			AND major_peril IN ('551','599','919'),
			1,
			0
		) AS v_smart_inland_marine_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB') AND
		--        type_bureau = 'IM' AND
		--        in(major_peril,'551','599','919'),1,0)
		IFF(v_symbol_pos_1_2 IN ('NA','NB') 
			AND type_bureau = 'IM' 
			AND major_peril IN ('551','599','919'),
			1,
			0
		) AS v_sbop_inland_marine_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BC','BD') AND
		--        type_bureau = 'GL' AND
		--        in(major_peril,'530','599','067','919'),1,0)
		IFF(v_symbol_pos_1_2 IN ('BC','BD') 
			AND type_bureau = 'GL' 
			AND major_peril IN ('530','599','067','919'),
			1,
			0
		) AS v_cbop_general_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'BO' AND
		--        type_bureau = 'GL' AND
		--        in(major_peril,'530','599','919'),1,0)
		IFF(v_symbol_pos_1_2 = 'BO' 
			AND type_bureau = 'GL' 
			AND major_peril IN ('530','599','919'),
			1,
			0
		) AS v_smart_general_liability_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB') AND
		--        type_bureau = 'GL' AND
		--        in(major_peril,'530','599','067','919'),1,0)
		IFF(v_symbol_pos_1_2 IN ('NA','NB') 
			AND type_bureau = 'GL' 
			AND major_peril IN ('530','599','067','919'),
			1,
			0
		) AS v_sbop_general_liability_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BC','BD') AND
		--        type_bureau = 'FT' AND
		--         major_peril = '566',1,0)
		IFF(v_symbol_pos_1_2 IN ('BC','BD') 
			AND type_bureau = 'FT' 
			AND major_peril = '566',
			1,
			0
		) AS v_cbop_fidelity_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB') AND
		--        type_bureau = 'FT' AND
		--        major_peril = '566',1,0)
		IFF(v_symbol_pos_1_2 IN ('NA','NB') 
			AND type_bureau = 'FT' 
			AND major_peril = '566',
			1,
			0
		) AS v_sbop_fidelity_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BC','BD') AND
		--        in(type_bureau,'FT','BT') AND
		--        in(major_peril,'565','599'),1,0)
		IFF(v_symbol_pos_1_2 IN ('BC','BD') 
			AND type_bureau IN ('FT','BT') 
			AND major_peril IN ('565','599'),
			1,
			0
		) AS v_cbop_burglary_and_theft_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'BO' AND
		--       in(type_bureau,'CR','FT','BT') AND
		--       in(major_peril,'565','599'),1,0)
		IFF(v_symbol_pos_1_2 = 'BO' 
			AND type_bureau IN ('CR','FT','BT') 
			AND major_peril IN ('565','599'),
			1,
			0
		) AS v_smart_burglary_and_theft_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB') AND
		--        in(type_bureau,'FT','BT') AND
		--        in(major_peril,'565','599'),1,0)
		IFF(v_symbol_pos_1_2 IN ('NA','NB') 
			AND type_bureau IN ('FT','BT') 
			AND major_peril IN ('565','599'),
			1,
			0
		) AS v_sbop_burglary_and_theft_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH') AND 
		--        type_bureau = 'CF' AND
		--        in(major_peril,'415','463','490','496','498','599','919','425','426','435','455','480',@{pipeline().parameters.MP_901_904}) AND
		--        in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		--       policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(v_symbol_pos_1_2 IN ('BG','BH') 
			AND type_bureau = 'CF' 
			AND major_peril IN ('415','463','490','496','498','599','919','425','426','435','455','480',@{pipeline().parameters.MP_901_904}) 
			AND v_risk_unit_group_1_3 IN (@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) 
			AND policy_effective_date >= CL_EQ_EFF_Date,
			1,
			0
		) AS v_gbop_earthquake_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH') AND 
		--        type_bureau = 'CF' AND
		--        in(major_peril,'415','463','490','496','498','599','919'),1,0)
		IFF(v_symbol_pos_1_2 IN ('BG','BH') 
			AND type_bureau = 'CF' 
			AND major_peril IN ('415','463','490','496','498','599','919'),
			1,
			0
		) AS v_gbop_fire_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH') AND
		--        in(type_bureau,'CF','GS') AND
		--        in(major_peril,'425' ,'426','435','455','480'),1,0)
		IFF(v_symbol_pos_1_2 IN ('BG','BH') 
			AND type_bureau IN ('CF','GS') 
			AND major_peril IN ('425','426','435','455','480'),
			1,
			0
		) AS v_gbop_allied_lines_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH') AND 
		--        type_bureau = 'IM' AND
		--        in(major_peril,'551','599','919'),1,0)
		IFF(v_symbol_pos_1_2 IN ('BG','BH') 
			AND type_bureau = 'IM' 
			AND major_peril IN ('551','599','919'),
			1,
			0
		) AS v_gbop_inland_marine_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH') AND 
		--       type_bureau = 'GL' AND
		--       in(major_peril,'530','599','919'),1,0)
		IFF(v_symbol_pos_1_2 IN ('BG','BH') 
			AND type_bureau = 'GL' 
			AND major_peril IN ('530','599','919'),
			1,
			0
		) AS v_gbop_general_liability_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH','GG','XX') AND
		--         in(type_bureau,'AN','AL') AND
		--         in(major_peril,'599',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_930_931}),1,0)
		IFF(v_symbol_pos_1_2 IN ('BG','BH','GG','XX') 
			AND type_bureau IN ('AN','AL') 
			AND major_peril IN ('599',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_930_931}),
			1,
			0
		) AS v_gbop_commercial_auto_liability_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH') AND
		--        in(type_bureau,'CF','NB','BC','BE') AND
		--        in(major_peril,@{pipeline().parameters.MP_901_904}),1,0)
		IFF(v_symbol_pos_1_2 IN ('BG','BH') 
			AND type_bureau IN ('CF','NB','BC','BE') 
			AND major_peril IN (@{pipeline().parameters.MP_901_904}),
			1,
			0
		) AS v_gbop_cmp_property_liability_peril_901_904_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH') AND
		--        type_bureau = 'BE' AND 
		--        major_peril = '907',1,0)
		IFF(v_symbol_pos_1_2 IN ('BG','BH') 
			AND type_bureau = 'BE' 
			AND major_peril = '907',
			1,
			0
		) AS v_gbop_cmp_liability_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH','GG') AND
		--        type_bureau = 'AP' AND
		--         in(major_peril,'269',@{pipeline().parameters.MP_145_160},@{pipeline().parameters.MP_165_166},@{pipeline().parameters.MP_170_173}),1,0)
		IFF(v_symbol_pos_1_2 IN ('BG','BH','GG') 
			AND type_bureau = 'AP' 
			AND major_peril IN ('269',@{pipeline().parameters.MP_145_160},@{pipeline().parameters.MP_165_166},@{pipeline().parameters.MP_170_173}),
			1,
			0
		) AS v_gbop_comm_auto_physical_damage_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH') AND
		--         in( type_bureau,'FT','BT') AND
		--         major_peril = '566',1,0)
		IFF(v_symbol_pos_1_2 IN ('BG','BH') 
			AND type_bureau IN ('FT','BT') 
			AND major_peril = '566',
			1,
			0
		) AS v_gbop_fidelity_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH') AND
		--        in(type_bureau,'FT','BT') AND
		--        in(major_peril,'565','599'),1,0)
		IFF(v_symbol_pos_1_2 IN ('BG','BH') 
			AND type_bureau IN ('FT','BT') 
			AND major_peril IN ('565','599'),
			1,
			0
		) AS v_gbop_burglary_and_theft_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'UP' AND 
		--        type_bureau = 'GL' AND
		--        major_peril = '017',1,0)
		IFF(v_symbol_pos_1_2 = 'UP' 
			AND type_bureau = 'GL' 
			AND major_peril = '017',
			1,
			0
		) AS v_personal_umbrella_general_liability_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'CP','UC','CU') AND
		--        type_bureau = 'GL' AND
		--        major_peril = '517',1,0)
		IFF(v_symbol_pos_1_2 IN ('CP','UC','CU') 
			AND type_bureau = 'GL' 
			AND major_peril = '517',
			1,
			0
		) AS v_commercial_umbrella_general_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NU' AND
		--         type_bureau = 'GL' AND
		--         major_peril = '517',1,0)
		IFF(v_symbol_pos_1_2 = 'NU' 
			AND type_bureau = 'GL' 
			AND major_peril = '517',
			1,
			0
		) AS v_commercial_umbrella_general_liability_nsi,
		-- *INF*: IIF(symbol= 'ZZZ',1,0)
		IFF(symbol = 'ZZZ',
			1,
			0
		) AS v_workers_comp_pool_total_workers_comp_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NF' AND
		--         in(major_peril,'566','599'),1,0)
		IFF(v_symbol_pos_1_2 = 'NF' 
			AND major_peril IN ('566','599'),
			1,
			0
		) AS v_fidelity_bonds_fidelity_nsi,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NF' AND
		--        major_peril = '565',1,0)
		IFF(v_symbol_pos_1_2 = 'NF' 
			AND major_peril = '565',
			1,
			0
		) AS v_fidelity_bonds_burgulary_and_theft_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NC','NJ','NL','NO','NM'),1,0)
		IFF(v_symbol_pos_1_2 IN ('NC','NJ','NL','NO','NM'),
			1,
			0
		) AS v_surety_bonds_surety_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'CD','CM') AND
		--        in(subline,'345','334') AND
		--        in(major_peril,'540','599','919'),1,0)
		IFF(v_symbol_pos_1_2 IN ('CD','CM') 
			AND subline IN ('345','334') 
			AND major_peril IN ('540','599','919'),
			1,
			0
		) AS v_d_and_o_claims_made_general_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CM',1,0)
		IFF(v_symbol_pos_1_2 = 'CM',
			1,
			0
		) AS v_epli_general_liability_claims_made_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NE',1,0)
		IFF(v_symbol_pos_1_2 = 'NE',
			1,
			0
		) AS v_epli_general_liability_claims_made_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BC','BD','BG','BH','CP') AND
		--         major_peril = '540' AND
		--         type_bureau = 'GL' AND
		--         (
		--         in(substr(class_code,1,5),'22222','22250') OR
		--         in(substr(risk_unit_group,1,3),'366','367')
		--         )
		-- 	 ,1,0)
		IFF(v_symbol_pos_1_2 IN ('BC','BD','BG','BH','CP') 
			AND major_peril = '540' 
			AND type_bureau = 'GL' 
			AND ( substr(class_code, 1, 5
				) IN ('22222','22250') 
				OR substr(risk_unit_group, 1, 3
				) IN ('366','367') 
			),
			1,
			0
		) AS v_epli_general_liability_claims_made_cpp_cbop_wbm,
		-- *INF*:  IIF(v_symbol_pos_1_2 = 'BO' AND
		--        in(major_peril,'540','541') AND
		--        type_bureau = 'GL' AND
		--       (
		--        in(substr(class_code,1,5),'22222','22250') OR
		--        in(substr(risk_unit_group,1,3),'366','367')
		--        )
		--        ,1,0)
		IFF(v_symbol_pos_1_2 = 'BO' 
			AND major_peril IN ('540','541') 
			AND type_bureau = 'GL' 
			AND ( substr(class_code, 1, 5
				) IN ('22222','22250') 
				OR substr(risk_unit_group, 1, 3
				) IN ('366','367') 
			),
			1,
			0
		) AS v_smart_general_liability_claims_made_smart_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB','NS') AND
		--        major_peril = '540' AND
		--        type_bureau = 'GL' AND
		--       (
		--        in(substr(class_code,1,5),'22222','22250') OR
		--        in(substr(risk_unit_group,1,3),'366','367')
		--        )
		--        ,1,0)
		IFF(v_symbol_pos_1_2 IN ('NA','NB','NS') 
			AND major_peril = '540' 
			AND type_bureau = 'GL' 
			AND ( substr(class_code, 1, 5
				) IN ('22222','22250') 
				OR substr(risk_unit_group, 1, 3
				) IN ('366','367') 
			),
			1,
			0
		) AS v_epli_general_liability_claims_made_cpp_sbop_nsi,
		-- *INF*: IIF(major_peril = '540' AND
		--        type_bureau = 'BE' AND
		--        in(substr(risk_unit_group,1,3),'366','367'),1,0)
		IFF(major_peril = '540' 
			AND type_bureau = 'BE' 
			AND substr(risk_unit_group, 1, 3
			) IN ('366','367'),
			1,
			0
		) AS v_epli_general_liability_claims_made_bop_wbm,
		-- *INF*: IIF(major_peril = '540' AND
		--        type_bureau = 'AL' AND
		--        in(substr(risk_unit_group,1,3),'417','418'),1,0)
		IFF(major_peril = '540' 
			AND type_bureau = 'AL' 
			AND substr(risk_unit_group, 1, 3
			) IN ('417','418'),
			1,
			0
		) AS v_epli_general_liability_claims_made_gbop_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NK','NN'),1,0)
		IFF(v_symbol_pos_1_2 IN ('NK','NN'),
			1,
			0
		) AS v_d_and_o_claims_made_general_liability_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BA','BB','BC','BD','BG','BH','BO','CP') AND
		--        in(type_bureau,'CF','BE','BM') AND
		--        in(major_peril,'570','906') AND
		--        in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		--        policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(v_symbol_pos_1_2 IN ('BA','BB','BC','BD','BG','BH','BO','CP') 
			AND type_bureau IN ('CF','BE','BM') 
			AND major_peril IN ('570','906') 
			AND v_risk_unit_group_1_3 IN (@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) 
			AND policy_effective_date >= CL_EQ_EFF_Date,
			1,
			0
		) AS v_boiler_and_machinery_earthquake_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BA','BB','BC','BD','BG','BH','BO','CP') AND
		--        in(type_bureau,'CF','BE','BM') AND
		--        in(major_peril,'570','906'),1,0)
		IFF(v_symbol_pos_1_2 IN ('BA','BB','BC','BD','BG','BH','BO','CP') 
			AND type_bureau IN ('CF','BE','BM') 
			AND major_peril IN ('570','906'),
			1,
			0
		) AS v_boiler_and_machinery_boiler_and_machinery_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB','NS') AND
		--        in(type_bureau,'CF','BE', 'BM') AND
		--        in(major_peril,'570','906') AND
		--        in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		--        policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(v_symbol_pos_1_2 IN ('NA','NB','NS') 
			AND type_bureau IN ('CF','BE','BM') 
			AND major_peril IN ('570','906') 
			AND v_risk_unit_group_1_3 IN (@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) 
			AND policy_effective_date >= CL_EQ_EFF_Date,
			1,
			0
		) AS v_boiler_and_machinery_earthquake_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB','NS') AND
		--        in(type_bureau,'CF','BE','BM') AND
		--        in(major_peril,'570','906'),1,0)
		IFF(v_symbol_pos_1_2 IN ('NA','NB','NS') 
			AND type_bureau IN ('CF','BE','BM') 
			AND major_peril IN ('570','906'),
			1,
			0
		) AS v_boiler_and_machinery_boiler_and_machinery_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'HA','HB','HH') AND
		--        type_bureau = 'MS' AND
		--        major_peril = '050',1,0)
		IFF(v_symbol_pos_1_2 IN ('HA','HB','HH') 
			AND type_bureau = 'MS' 
			AND major_peril = '050',
			1,
			0
		) AS v_mine_subsidence_homeowners_wbm,
		-- *INF*:  IIF(substr(symbol,1,1) != 'N' AND
		--        in(type_bureau,'MS','NB') AND
		--         major_peril = '050',1,0)
		IFF(substr(symbol, 1, 1
			) != 'N' 
			AND type_bureau IN ('MS','NB') 
			AND major_peril = '050',
			1,
			0
		) AS v_mine_subsidence_allied_lines_wbm,
		-- *INF*:  IIF(substr(symbol,1,1) = 'N' AND
		--        in(type_bureau,'MS','NB') AND
		--         major_peril = '050',1,0)
		IFF(substr(symbol, 1, 1
			) = 'N' 
			AND type_bureau IN ('MS','NB') 
			AND major_peril = '050',
			1,
			0
		) AS v_mine_subsidence_allied_lines_nsi,
		-- *INF*: DECODE(1,
		-- v_home_and_highway_fire_wbm,'1500',
		-- v_home_and_highway_allied_lines_wbm,'1500',
		-- v_home_and_highway_homeowners_wbm,'1500',
		-- v_home_and_highway_inland_marine_wbm,'1500',
		-- v_home_and_highway_earthquake_wbm,'1500',
		-- v_home_and_highway_personal_liability_wbm,'1500',
		-- v_home_and_highway_general_liability_wbm,'1500',
		-- v_home_and_highway_pp_auto_liability_wbm,'2000',
		-- v_home_and_highway_pp_auto_physical_damage_wbm,'2020',
		-- v_preferred_auto_pp_auto_liability_wbm,'2000',
		-- v_preferred_auto_pp_auto_physical_damage_wbm,'2020',
		-- v_select_auto_pp_auto_liability_wbm,'2000',
		-- v_select_auto_pp_auto_physical_damage_wbm,'2020',
		-- v_standard_homeowners_allied_lined_wbm,'1500',
		-- v_standard_homeowners_homeowners_wbm,'1500',
		-- v_standard_homeowners_inland_marine_wbm,'1500',
		-- v_standard_homeowners_earthquake_wbm,'1500',
		-- v_standard_homeowners_personal_liability_wbm,'1500',
		-- v_select_homeowners_allied_lines_wbm,'1500',
		-- v_select_homeowners_homeowners_wbm,'1500',
		-- v_select_homeowners_inland_marine_wbm,'1500',
		-- v_select_homeowners_earthquake_wbm,'1500',
		-- v_select_homeowners_personal_liability_wbm,'1500',
		-- v_dwelling_fire_fire_wbm,'1500',
		-- v_dwelling_fire_allied_lines_wbm,'1500',
		-- v_dwelling_fire_earthquake_wbm,'1500',
		-- v_personal_inland_marine_inland_marine_wbm,'1500',
		-- v_personal_inland_marine_personal_liability_wbm,'1500',
		-- v_motorcycle_pp_auto_liability_wbm,'2000',
		-- v_motorcycle_pp_auto_physical_damage_wbm,'2020',
		-- v_boatowners_inland_marine_wbm,'1500',
		-- v_boatowners_personal_liability_wbm,'1500',
		-- v_alternative_one_pp_auto_liability_wbm,'2000',
		-- v_alternative_one_pp_auto_physical_damage_wbm,'2020',
		-- v_alternative_one_star_pp_auto_liability_wbm,'2000',
		-- v_alternative_one_star_pp_auto_physical_damage_wbm,'2020',
		-- v_commercial_auto_commercial_auto_liability_wbm,'2050',
		-- v_commercial_auto_commercial_auto_liability_nsi,'2050',
		-- v_commercial_auto_commercial_auto_liability_garage_veh_wbm,'2050',
		-- v_commercial_auto_commercial_auto_liability_garage_veh_nsi,'2050',
		-- v_commercial_auto_comm_auto_physical_damage_wbm,'2070',
		-- v_commercial_auto_comm_auto_physical_damage_nsi,'2070',
		-- v_commercial_auto_comm_auto_physical_damage_garage_veh_wbm,'2070',
		-- v_commercial_auto_comm_auto_physical_damage_garage_veh_nsi,'2070',
		-- v_garage_liability_commercial_auto_liability_wbm,'2080',
		-- v_garage_liability_commercial_auto_liability_nsi,'2080',
		-- v_garage_liability_commercial_auto_physical_damage_wbm,'2100',
		-- v_garage_liability_commercial_auto_physical_damage_nsi,'2100',
		-- v_workers_comp_total_non_pool_workers_comp_wbm,'2150',
		-- v_workers_comp_total_non_pool_workers_comp_nsi,'2150',
		-- v_workers_comp_total_non_pool_workers_comp_argent,'2150',
		-- v_commercial_property_earthquake_wbm,'1500',
		-- v_commercial_property_fire_wbm,'1500',
		-- v_commercial_property_earthquake_nsi,'1500',
		-- v_commercial_property_fire_nsi,'1500',
		-- v_commercial_property_allied_lines_wbm,'1500',
		-- v_commercial_property_allied_lines_nsi,'1500',
		-- v_metalworkers_earthquake_wbm,'1500',
		-- v_metalworkers_fire_wbm,'1500',
		-- v_metalworkers_allied_lines_wbm,'1500',
		-- v_metalworkers_inland_marine_wbm,'1500',
		-- v_metalworkers_general_liability_wbm,'1500',
		-- v_metalworkers_products_liability_wbm,'1500',
		-- v_metalworkers_claims_made_product_liability_wbm,'1500',
		-- v_metalworkers_fidelity_wbm,'1500',
		-- v_metalworkers_burglary_and_theft_wbm,'1500',
		-- v_woodworkers_earthquake_wbm,'1500',
		-- v_woodworkers_fire_wbm,'1500',
		-- v_woodworkers_allied_lines_wbm,'1500',
		-- v_woodworkers_inland_marine_wbm,'1500',
		-- v_woodworkers_general_liability_wbm,'1500',
		-- v_woodworkers_products_liability_wbm,'1500',
		-- v_woodworkers_claims_made_product_liability_wbm,'1500',
		-- v_woodworkers_fidelity_wbm,'1500',
		-- v_woodworkers_burglary_and_theft_wbm,'1500',
		-- v_general_liability_general_liability_wbm,'1500',
		-- v_general_liability_general_liability_nsi,'1500',
		-- v_general_liability_products_liability_wbm,'1500',
		-- v_smart_products_liability_wbm,'1500',
		-- v_general_liability_products_liability_nsi,'1500',
		-- v_general_liability_claims_made_product_liability_wbm,'1500',
		-- v_general_liability_claims_made_product_liability_nsi,'1500',
		-- v_general_liability_claims_made_general_liability_nsi,'1500',
		-- v_crime_fidelity_wbm,'1500',
		-- v_smart_fidelity_wbm,'1500',
		-- v_crime_fidelity_nsi,'1500',
		-- v_crime_burglary_and_theft_wbm,'1500',
		-- v_crime_burglary_and_theft_nsi,'1500',
		-- v_commercial_im_inland_marine_wbm,'1500',
		-- v_commercial_im_inland_marine_nsi,'1500',
		-- v_bop_cmp_property_wbm,'1500',
		-- v_bop_unnamed_R13905,'1500',
		-- v_bop_cmp_property_liability_peril_901_904_and_599_wbm,'1500',
		-- v_bop_cmp_liability_wbm,'1500',
		-- v_cbop_earthquake_wbm,'1500',
		-- v_cbop_fire_wbm,'1500',
		-- v_smart_earthquake_wbm,'1500',
		-- v_smart_fire_wbm,'1500',
		-- v_sbop_earthquake_nsi,'1500',
		-- v_sbop_fire_nsi,'1500',
		-- v_cbop_allied_lines_wbm,'1500',
		-- v_smart_allied_lines_wbm,'1500',
		-- v_sbop_allied_lines_nsi,'1500',
		-- v_cbop_cmp_property_wbm,'1500',
		-- v_cbop_inland_marine_wbm,'1500',
		-- v_smart_inland_marine_wbm,'1500',
		-- v_sbop_inland_marine_nsi,'1500',
		-- v_cbop_general_liability_wbm,'1500',
		-- v_smart_general_liability_wbm,'1500',
		-- v_sbop_general_liability_nsi,'1500',
		-- v_cbop_fidelity_wbm,'1500',
		-- v_sbop_fidelity_nsi,'1500',
		-- v_cbop_burglary_and_theft_wbm,'1500',
		-- v_smart_burglary_and_theft_wbm,'1500',
		-- v_sbop_burglary_and_theft_nsi,'1500',
		-- v_gbop_earthquake_wbm,'1500',
		-- v_gbop_fire_wbm,'1500',
		-- v_gbop_allied_lines_wbm,'1500',
		-- v_gbop_inland_marine_wbm,'1500',
		-- v_gbop_general_liability_wbm,'1500',
		-- v_gbop_commercial_auto_liability_wbm,'2110',
		-- v_gbop_cmp_property_liability_peril_901_904_wbm,'1500',
		-- v_gbop_cmp_liability_wbm,'1500',
		-- v_gbop_comm_auto_physical_damage_wbm,'2130',
		-- v_gbop_fidelity_wbm,'1500',
		-- v_gbop_burglary_and_theft_wbm,'1500',
		-- v_personal_umbrella_general_liability_wbm,'1500',
		-- v_commercial_umbrella_general_liability_wbm,'1500',
		-- v_commercial_umbrella_general_liability_nsi,'1500',
		-- v_workers_comp_pool_total_workers_comp_wbm,'2150',
		-- v_fidelity_bonds_fidelity_nsi,'1500',
		-- v_fidelity_bonds_burgulary_and_theft_nsi,'1500',
		-- v_surety_bonds_surety_nsi,'1500',
		-- v_d_and_o_claims_made_general_liability_wbm,'1500',
		-- v_epli_general_liability_claims_made_wbm,'1500',
		-- v_epli_general_liability_claims_made_nsi,'1500',
		-- v_epli_general_liability_claims_made_cpp_cbop_wbm,'1500',
		-- v_smart_general_liability_claims_made_smart_wbm,'1500',
		-- v_epli_general_liability_claims_made_cpp_sbop_nsi,'1500',
		-- v_epli_general_liability_claims_made_bop_wbm,'1500',
		-- v_epli_general_liability_claims_made_gbop_wbm,'1500',
		-- v_d_and_o_claims_made_general_liability_nsi,'1500',
		-- v_boiler_and_machinery_earthquake_wbm,'1500',
		-- v_boiler_and_machinery_boiler_and_machinery_wbm,'1500',
		-- v_boiler_and_machinery_earthquake_nsi,'1500',
		-- v_boiler_and_machinery_boiler_and_machinery_nsi,'1500',
		-- v_mine_subsidence_homeowners_wbm,'1500',
		-- v_mine_subsidence_allied_lines_wbm,'1500',
		-- v_mine_subsidence_allied_lines_nsi,'1500',
		-- '1500')
		-- -- DECODES must follow the same order as the program
		DECODE(1,
			v_home_and_highway_fire_wbm, '1500',
			v_home_and_highway_allied_lines_wbm, '1500',
			v_home_and_highway_homeowners_wbm, '1500',
			v_home_and_highway_inland_marine_wbm, '1500',
			v_home_and_highway_earthquake_wbm, '1500',
			v_home_and_highway_personal_liability_wbm, '1500',
			v_home_and_highway_general_liability_wbm, '1500',
			v_home_and_highway_pp_auto_liability_wbm, '2000',
			v_home_and_highway_pp_auto_physical_damage_wbm, '2020',
			v_preferred_auto_pp_auto_liability_wbm, '2000',
			v_preferred_auto_pp_auto_physical_damage_wbm, '2020',
			v_select_auto_pp_auto_liability_wbm, '2000',
			v_select_auto_pp_auto_physical_damage_wbm, '2020',
			v_standard_homeowners_allied_lined_wbm, '1500',
			v_standard_homeowners_homeowners_wbm, '1500',
			v_standard_homeowners_inland_marine_wbm, '1500',
			v_standard_homeowners_earthquake_wbm, '1500',
			v_standard_homeowners_personal_liability_wbm, '1500',
			v_select_homeowners_allied_lines_wbm, '1500',
			v_select_homeowners_homeowners_wbm, '1500',
			v_select_homeowners_inland_marine_wbm, '1500',
			v_select_homeowners_earthquake_wbm, '1500',
			v_select_homeowners_personal_liability_wbm, '1500',
			v_dwelling_fire_fire_wbm, '1500',
			v_dwelling_fire_allied_lines_wbm, '1500',
			v_dwelling_fire_earthquake_wbm, '1500',
			v_personal_inland_marine_inland_marine_wbm, '1500',
			v_personal_inland_marine_personal_liability_wbm, '1500',
			v_motorcycle_pp_auto_liability_wbm, '2000',
			v_motorcycle_pp_auto_physical_damage_wbm, '2020',
			v_boatowners_inland_marine_wbm, '1500',
			v_boatowners_personal_liability_wbm, '1500',
			v_alternative_one_pp_auto_liability_wbm, '2000',
			v_alternative_one_pp_auto_physical_damage_wbm, '2020',
			v_alternative_one_star_pp_auto_liability_wbm, '2000',
			v_alternative_one_star_pp_auto_physical_damage_wbm, '2020',
			v_commercial_auto_commercial_auto_liability_wbm, '2050',
			v_commercial_auto_commercial_auto_liability_nsi, '2050',
			v_commercial_auto_commercial_auto_liability_garage_veh_wbm, '2050',
			v_commercial_auto_commercial_auto_liability_garage_veh_nsi, '2050',
			v_commercial_auto_comm_auto_physical_damage_wbm, '2070',
			v_commercial_auto_comm_auto_physical_damage_nsi, '2070',
			v_commercial_auto_comm_auto_physical_damage_garage_veh_wbm, '2070',
			v_commercial_auto_comm_auto_physical_damage_garage_veh_nsi, '2070',
			v_garage_liability_commercial_auto_liability_wbm, '2080',
			v_garage_liability_commercial_auto_liability_nsi, '2080',
			v_garage_liability_commercial_auto_physical_damage_wbm, '2100',
			v_garage_liability_commercial_auto_physical_damage_nsi, '2100',
			v_workers_comp_total_non_pool_workers_comp_wbm, '2150',
			v_workers_comp_total_non_pool_workers_comp_nsi, '2150',
			v_workers_comp_total_non_pool_workers_comp_argent, '2150',
			v_commercial_property_earthquake_wbm, '1500',
			v_commercial_property_fire_wbm, '1500',
			v_commercial_property_earthquake_nsi, '1500',
			v_commercial_property_fire_nsi, '1500',
			v_commercial_property_allied_lines_wbm, '1500',
			v_commercial_property_allied_lines_nsi, '1500',
			v_metalworkers_earthquake_wbm, '1500',
			v_metalworkers_fire_wbm, '1500',
			v_metalworkers_allied_lines_wbm, '1500',
			v_metalworkers_inland_marine_wbm, '1500',
			v_metalworkers_general_liability_wbm, '1500',
			v_metalworkers_products_liability_wbm, '1500',
			v_metalworkers_claims_made_product_liability_wbm, '1500',
			v_metalworkers_fidelity_wbm, '1500',
			v_metalworkers_burglary_and_theft_wbm, '1500',
			v_woodworkers_earthquake_wbm, '1500',
			v_woodworkers_fire_wbm, '1500',
			v_woodworkers_allied_lines_wbm, '1500',
			v_woodworkers_inland_marine_wbm, '1500',
			v_woodworkers_general_liability_wbm, '1500',
			v_woodworkers_products_liability_wbm, '1500',
			v_woodworkers_claims_made_product_liability_wbm, '1500',
			v_woodworkers_fidelity_wbm, '1500',
			v_woodworkers_burglary_and_theft_wbm, '1500',
			v_general_liability_general_liability_wbm, '1500',
			v_general_liability_general_liability_nsi, '1500',
			v_general_liability_products_liability_wbm, '1500',
			v_smart_products_liability_wbm, '1500',
			v_general_liability_products_liability_nsi, '1500',
			v_general_liability_claims_made_product_liability_wbm, '1500',
			v_general_liability_claims_made_product_liability_nsi, '1500',
			v_general_liability_claims_made_general_liability_nsi, '1500',
			v_crime_fidelity_wbm, '1500',
			v_smart_fidelity_wbm, '1500',
			v_crime_fidelity_nsi, '1500',
			v_crime_burglary_and_theft_wbm, '1500',
			v_crime_burglary_and_theft_nsi, '1500',
			v_commercial_im_inland_marine_wbm, '1500',
			v_commercial_im_inland_marine_nsi, '1500',
			v_bop_cmp_property_wbm, '1500',
			v_bop_unnamed_R13905, '1500',
			v_bop_cmp_property_liability_peril_901_904_and_599_wbm, '1500',
			v_bop_cmp_liability_wbm, '1500',
			v_cbop_earthquake_wbm, '1500',
			v_cbop_fire_wbm, '1500',
			v_smart_earthquake_wbm, '1500',
			v_smart_fire_wbm, '1500',
			v_sbop_earthquake_nsi, '1500',
			v_sbop_fire_nsi, '1500',
			v_cbop_allied_lines_wbm, '1500',
			v_smart_allied_lines_wbm, '1500',
			v_sbop_allied_lines_nsi, '1500',
			v_cbop_cmp_property_wbm, '1500',
			v_cbop_inland_marine_wbm, '1500',
			v_smart_inland_marine_wbm, '1500',
			v_sbop_inland_marine_nsi, '1500',
			v_cbop_general_liability_wbm, '1500',
			v_smart_general_liability_wbm, '1500',
			v_sbop_general_liability_nsi, '1500',
			v_cbop_fidelity_wbm, '1500',
			v_sbop_fidelity_nsi, '1500',
			v_cbop_burglary_and_theft_wbm, '1500',
			v_smart_burglary_and_theft_wbm, '1500',
			v_sbop_burglary_and_theft_nsi, '1500',
			v_gbop_earthquake_wbm, '1500',
			v_gbop_fire_wbm, '1500',
			v_gbop_allied_lines_wbm, '1500',
			v_gbop_inland_marine_wbm, '1500',
			v_gbop_general_liability_wbm, '1500',
			v_gbop_commercial_auto_liability_wbm, '2110',
			v_gbop_cmp_property_liability_peril_901_904_wbm, '1500',
			v_gbop_cmp_liability_wbm, '1500',
			v_gbop_comm_auto_physical_damage_wbm, '2130',
			v_gbop_fidelity_wbm, '1500',
			v_gbop_burglary_and_theft_wbm, '1500',
			v_personal_umbrella_general_liability_wbm, '1500',
			v_commercial_umbrella_general_liability_wbm, '1500',
			v_commercial_umbrella_general_liability_nsi, '1500',
			v_workers_comp_pool_total_workers_comp_wbm, '2150',
			v_fidelity_bonds_fidelity_nsi, '1500',
			v_fidelity_bonds_burgulary_and_theft_nsi, '1500',
			v_surety_bonds_surety_nsi, '1500',
			v_d_and_o_claims_made_general_liability_wbm, '1500',
			v_epli_general_liability_claims_made_wbm, '1500',
			v_epli_general_liability_claims_made_nsi, '1500',
			v_epli_general_liability_claims_made_cpp_cbop_wbm, '1500',
			v_smart_general_liability_claims_made_smart_wbm, '1500',
			v_epli_general_liability_claims_made_cpp_sbop_nsi, '1500',
			v_epli_general_liability_claims_made_bop_wbm, '1500',
			v_epli_general_liability_claims_made_gbop_wbm, '1500',
			v_d_and_o_claims_made_general_liability_nsi, '1500',
			v_boiler_and_machinery_earthquake_wbm, '1500',
			v_boiler_and_machinery_boiler_and_machinery_wbm, '1500',
			v_boiler_and_machinery_earthquake_nsi, '1500',
			v_boiler_and_machinery_boiler_and_machinery_nsi, '1500',
			v_mine_subsidence_homeowners_wbm, '1500',
			v_mine_subsidence_allied_lines_wbm, '1500',
			v_mine_subsidence_allied_lines_nsi, '1500',
			'1500'
		) AS Path_Flag_Step_1,
		-- *INF*: DECODE(1,
		-- v_home_and_highway_fire_wbm,'20',
		-- v_home_and_highway_allied_lines_wbm,'20',
		-- v_home_and_highway_homeowners_wbm,'20',
		-- v_home_and_highway_inland_marine_wbm,'20',
		-- v_home_and_highway_earthquake_wbm,'20',
		-- v_home_and_highway_personal_liability_wbm,'20',
		-- v_home_and_highway_general_liability_wbm,'20',
		-- v_home_and_highway_pp_auto_liability_wbm,'20',
		-- v_home_and_highway_pp_auto_physical_damage_wbm,'20',
		-- v_preferred_auto_pp_auto_liability_wbm,'40',
		-- v_preferred_auto_pp_auto_physical_damage_wbm,'40',
		-- v_select_auto_pp_auto_liability_wbm,'60',
		-- v_select_auto_pp_auto_physical_damage_wbm,'60',
		-- v_standard_homeowners_allied_lined_wbm,'80',
		-- v_standard_homeowners_homeowners_wbm,'80',
		-- v_standard_homeowners_inland_marine_wbm,'80',
		-- v_standard_homeowners_earthquake_wbm,'80',
		-- v_standard_homeowners_personal_liability_wbm,'80',
		-- v_select_homeowners_allied_lines_wbm,'100',
		-- v_select_homeowners_homeowners_wbm,'100',
		-- v_select_homeowners_inland_marine_wbm,'100',
		-- v_select_homeowners_earthquake_wbm,'100',
		-- v_select_homeowners_personal_liability_wbm,'100',
		-- v_dwelling_fire_fire_wbm,'120',
		-- v_dwelling_fire_allied_lines_wbm,'120',
		-- v_dwelling_fire_earthquake_wbm,'120',
		-- v_personal_inland_marine_inland_marine_wbm,'140',
		-- v_personal_inland_marine_personal_liability_wbm,'140',
		-- v_motorcycle_pp_auto_liability_wbm,'160',
		-- v_motorcycle_pp_auto_physical_damage_wbm,'160',
		-- v_boatowners_inland_marine_wbm,'180',
		-- v_boatowners_personal_liability_wbm,'180',
		-- v_alternative_one_pp_auto_liability_wbm,'200',
		-- v_alternative_one_pp_auto_physical_damage_wbm,'200',
		-- v_alternative_one_star_pp_auto_liability_wbm,'220',
		-- v_alternative_one_star_pp_auto_physical_damage_wbm,'220',
		-- v_commercial_auto_commercial_auto_liability_wbm,'240',
		-- v_commercial_auto_commercial_auto_liability_nsi,'600',
		-- v_commercial_auto_commercial_auto_liability_garage_veh_wbm,'240',
		-- v_commercial_auto_commercial_auto_liability_garage_veh_nsi,'600',
		-- v_commercial_auto_comm_auto_physical_damage_wbm,'240',
		-- v_commercial_auto_comm_auto_physical_damage_nsi,'600',
		-- v_commercial_auto_comm_auto_physical_damage_garage_veh_wbm,'240',
		-- v_commercial_auto_comm_auto_physical_damage_garage_veh_nsi,'600',
		-- v_garage_liability_commercial_auto_liability_wbm,'260',
		-- v_garage_liability_commercial_auto_liability_nsi,'620',
		-- v_garage_liability_commercial_auto_physical_damage_wbm,'260',
		-- v_garage_liability_commercial_auto_physical_damage_nsi,'620',
		-- v_workers_comp_total_non_pool_workers_comp_wbm,'280',
		-- v_workers_comp_total_non_pool_workers_comp_nsi,'640',
		-- v_workers_comp_total_non_pool_workers_comp_argent,'950',
		-- v_commercial_property_earthquake_wbm,'300',
		-- v_commercial_property_fire_wbm,'300',
		-- v_commercial_property_earthquake_nsi,'660',
		-- v_commercial_property_fire_nsi,'660',
		-- v_commercial_property_allied_lines_wbm,'300',
		-- v_commercial_property_allied_lines_nsi,'660',
		-- v_metalworkers_earthquake_wbm,'320',
		-- v_metalworkers_fire_wbm,'320',
		-- v_metalworkers_allied_lines_wbm,'320',
		-- v_metalworkers_inland_marine_wbm,'320',
		-- v_metalworkers_general_liability_wbm,'320',
		-- v_metalworkers_products_liability_wbm,'320',
		-- v_metalworkers_claims_made_product_liability_wbm,'320',
		-- v_metalworkers_fidelity_wbm,'320',
		-- v_metalworkers_burglary_and_theft_wbm,'320',
		-- v_woodworkers_earthquake_wbm,'340',
		-- v_woodworkers_fire_wbm,'340',
		-- v_woodworkers_allied_lines_wbm,'340',
		-- v_woodworkers_inland_marine_wbm,'340',
		-- v_woodworkers_general_liability_wbm,'340',
		-- v_woodworkers_products_liability_wbm,'340',
		-- v_woodworkers_claims_made_product_liability_wbm,'340',
		-- v_woodworkers_fidelity_wbm,'340',
		-- v_woodworkers_burglary_and_theft_wbm,'340',
		-- v_general_liability_general_liability_wbm,'360',
		-- v_general_liability_general_liability_nsi,'680',
		-- v_general_liability_products_liability_wbm,'360',
		-- v_smart_products_liability_wbm,'450',
		-- v_general_liability_products_liability_nsi,'680',
		-- v_general_liability_claims_made_product_liability_wbm,'360',
		-- v_general_liability_claims_made_product_liability_nsi,'680',
		-- v_general_liability_claims_made_general_liability_nsi,'680',
		-- v_crime_fidelity_wbm,'380',
		-- v_smart_fidelity_wbm,'450',
		-- v_crime_fidelity_nsi,'700',
		-- v_crime_burglary_and_theft_wbm,'380',
		-- v_crime_burglary_and_theft_nsi,'700',
		-- v_commercial_im_inland_marine_wbm,'400',
		-- v_commercial_im_inland_marine_nsi,'720',
		-- v_bop_cmp_property_wbm,'420',
		-- v_bop_unnamed_R13905,'420',
		-- v_bop_cmp_property_liability_peril_901_904_and_599_wbm,'420',
		-- v_bop_cmp_liability_wbm,'420',
		-- v_cbop_earthquake_wbm,'440',
		-- v_cbop_fire_wbm,'440',
		-- v_smart_earthquake_wbm,'450',
		-- v_smart_fire_wbm,'450',
		-- v_sbop_earthquake_nsi,'740',
		-- v_sbop_fire_nsi,'740',
		-- v_cbop_allied_lines_wbm,'440',
		-- v_smart_allied_lines_wbm,'450',
		-- v_sbop_allied_lines_nsi,'740',
		-- v_cbop_cmp_property_wbm,'440',
		-- v_cbop_inland_marine_wbm,'440',
		-- v_smart_inland_marine_wbm,'450',
		-- v_sbop_inland_marine_nsi,'740',
		-- v_cbop_general_liability_wbm,'440',
		-- v_smart_general_liability_wbm,'450',
		-- v_sbop_general_liability_nsi,'740',
		-- v_cbop_fidelity_wbm,'440',
		-- v_sbop_fidelity_nsi,'740',
		-- v_cbop_burglary_and_theft_wbm,'440',
		-- v_smart_burglary_and_theft_wbm,'450',
		-- v_sbop_burglary_and_theft_nsi,'740',
		-- v_gbop_earthquake_wbm,'460',
		-- v_gbop_fire_wbm,'460',
		-- v_gbop_allied_lines_wbm,'460',
		-- v_gbop_inland_marine_wbm,'460',
		-- v_gbop_general_liability_wbm,'460',
		-- v_gbop_commercial_auto_liability_wbm,'460',
		-- v_gbop_cmp_property_liability_peril_901_904_wbm,'460',
		-- v_gbop_cmp_liability_wbm,'460',
		-- v_gbop_comm_auto_physical_damage_wbm,'460',
		-- v_gbop_fidelity_wbm,'460',
		-- v_gbop_burglary_and_theft_wbm,'460',
		-- v_personal_umbrella_general_liability_wbm,'480',
		-- v_commercial_umbrella_general_liability_wbm,'500',
		-- v_commercial_umbrella_general_liability_nsi,'760',
		-- v_workers_comp_pool_total_workers_comp_wbm,'580',
		-- v_fidelity_bonds_fidelity_nsi,'780',
		-- v_fidelity_bonds_burgulary_and_theft_nsi,'780',
		-- v_surety_bonds_surety_nsi,'800',
		-- v_d_and_o_claims_made_general_liability_wbm,'530',
		-- v_epli_general_liability_claims_made_wbm,'520',
		-- v_epli_general_liability_claims_made_nsi,'820',
		-- v_epli_general_liability_claims_made_cpp_cbop_wbm,'520',
		-- v_smart_general_liability_claims_made_smart_wbm,'450',
		-- v_epli_general_liability_claims_made_cpp_sbop_nsi,'820',
		-- v_epli_general_liability_claims_made_bop_wbm,'520',
		-- v_epli_general_liability_claims_made_gbop_wbm,'520',
		-- v_d_and_o_claims_made_general_liability_nsi,'840',
		-- v_boiler_and_machinery_earthquake_wbm,'540',
		-- v_boiler_and_machinery_boiler_and_machinery_wbm,'540',
		-- v_boiler_and_machinery_earthquake_nsi,'860',
		-- v_boiler_and_machinery_boiler_and_machinery_nsi,'860',
		-- v_mine_subsidence_homeowners_wbm,'560',
		-- v_mine_subsidence_allied_lines_wbm,'560',
		-- v_mine_subsidence_allied_lines_nsi,'880',
		-- '999')
		-- -- DECODES must follow the same order as the program
		DECODE(1,
			v_home_and_highway_fire_wbm, '20',
			v_home_and_highway_allied_lines_wbm, '20',
			v_home_and_highway_homeowners_wbm, '20',
			v_home_and_highway_inland_marine_wbm, '20',
			v_home_and_highway_earthquake_wbm, '20',
			v_home_and_highway_personal_liability_wbm, '20',
			v_home_and_highway_general_liability_wbm, '20',
			v_home_and_highway_pp_auto_liability_wbm, '20',
			v_home_and_highway_pp_auto_physical_damage_wbm, '20',
			v_preferred_auto_pp_auto_liability_wbm, '40',
			v_preferred_auto_pp_auto_physical_damage_wbm, '40',
			v_select_auto_pp_auto_liability_wbm, '60',
			v_select_auto_pp_auto_physical_damage_wbm, '60',
			v_standard_homeowners_allied_lined_wbm, '80',
			v_standard_homeowners_homeowners_wbm, '80',
			v_standard_homeowners_inland_marine_wbm, '80',
			v_standard_homeowners_earthquake_wbm, '80',
			v_standard_homeowners_personal_liability_wbm, '80',
			v_select_homeowners_allied_lines_wbm, '100',
			v_select_homeowners_homeowners_wbm, '100',
			v_select_homeowners_inland_marine_wbm, '100',
			v_select_homeowners_earthquake_wbm, '100',
			v_select_homeowners_personal_liability_wbm, '100',
			v_dwelling_fire_fire_wbm, '120',
			v_dwelling_fire_allied_lines_wbm, '120',
			v_dwelling_fire_earthquake_wbm, '120',
			v_personal_inland_marine_inland_marine_wbm, '140',
			v_personal_inland_marine_personal_liability_wbm, '140',
			v_motorcycle_pp_auto_liability_wbm, '160',
			v_motorcycle_pp_auto_physical_damage_wbm, '160',
			v_boatowners_inland_marine_wbm, '180',
			v_boatowners_personal_liability_wbm, '180',
			v_alternative_one_pp_auto_liability_wbm, '200',
			v_alternative_one_pp_auto_physical_damage_wbm, '200',
			v_alternative_one_star_pp_auto_liability_wbm, '220',
			v_alternative_one_star_pp_auto_physical_damage_wbm, '220',
			v_commercial_auto_commercial_auto_liability_wbm, '240',
			v_commercial_auto_commercial_auto_liability_nsi, '600',
			v_commercial_auto_commercial_auto_liability_garage_veh_wbm, '240',
			v_commercial_auto_commercial_auto_liability_garage_veh_nsi, '600',
			v_commercial_auto_comm_auto_physical_damage_wbm, '240',
			v_commercial_auto_comm_auto_physical_damage_nsi, '600',
			v_commercial_auto_comm_auto_physical_damage_garage_veh_wbm, '240',
			v_commercial_auto_comm_auto_physical_damage_garage_veh_nsi, '600',
			v_garage_liability_commercial_auto_liability_wbm, '260',
			v_garage_liability_commercial_auto_liability_nsi, '620',
			v_garage_liability_commercial_auto_physical_damage_wbm, '260',
			v_garage_liability_commercial_auto_physical_damage_nsi, '620',
			v_workers_comp_total_non_pool_workers_comp_wbm, '280',
			v_workers_comp_total_non_pool_workers_comp_nsi, '640',
			v_workers_comp_total_non_pool_workers_comp_argent, '950',
			v_commercial_property_earthquake_wbm, '300',
			v_commercial_property_fire_wbm, '300',
			v_commercial_property_earthquake_nsi, '660',
			v_commercial_property_fire_nsi, '660',
			v_commercial_property_allied_lines_wbm, '300',
			v_commercial_property_allied_lines_nsi, '660',
			v_metalworkers_earthquake_wbm, '320',
			v_metalworkers_fire_wbm, '320',
			v_metalworkers_allied_lines_wbm, '320',
			v_metalworkers_inland_marine_wbm, '320',
			v_metalworkers_general_liability_wbm, '320',
			v_metalworkers_products_liability_wbm, '320',
			v_metalworkers_claims_made_product_liability_wbm, '320',
			v_metalworkers_fidelity_wbm, '320',
			v_metalworkers_burglary_and_theft_wbm, '320',
			v_woodworkers_earthquake_wbm, '340',
			v_woodworkers_fire_wbm, '340',
			v_woodworkers_allied_lines_wbm, '340',
			v_woodworkers_inland_marine_wbm, '340',
			v_woodworkers_general_liability_wbm, '340',
			v_woodworkers_products_liability_wbm, '340',
			v_woodworkers_claims_made_product_liability_wbm, '340',
			v_woodworkers_fidelity_wbm, '340',
			v_woodworkers_burglary_and_theft_wbm, '340',
			v_general_liability_general_liability_wbm, '360',
			v_general_liability_general_liability_nsi, '680',
			v_general_liability_products_liability_wbm, '360',
			v_smart_products_liability_wbm, '450',
			v_general_liability_products_liability_nsi, '680',
			v_general_liability_claims_made_product_liability_wbm, '360',
			v_general_liability_claims_made_product_liability_nsi, '680',
			v_general_liability_claims_made_general_liability_nsi, '680',
			v_crime_fidelity_wbm, '380',
			v_smart_fidelity_wbm, '450',
			v_crime_fidelity_nsi, '700',
			v_crime_burglary_and_theft_wbm, '380',
			v_crime_burglary_and_theft_nsi, '700',
			v_commercial_im_inland_marine_wbm, '400',
			v_commercial_im_inland_marine_nsi, '720',
			v_bop_cmp_property_wbm, '420',
			v_bop_unnamed_R13905, '420',
			v_bop_cmp_property_liability_peril_901_904_and_599_wbm, '420',
			v_bop_cmp_liability_wbm, '420',
			v_cbop_earthquake_wbm, '440',
			v_cbop_fire_wbm, '440',
			v_smart_earthquake_wbm, '450',
			v_smart_fire_wbm, '450',
			v_sbop_earthquake_nsi, '740',
			v_sbop_fire_nsi, '740',
			v_cbop_allied_lines_wbm, '440',
			v_smart_allied_lines_wbm, '450',
			v_sbop_allied_lines_nsi, '740',
			v_cbop_cmp_property_wbm, '440',
			v_cbop_inland_marine_wbm, '440',
			v_smart_inland_marine_wbm, '450',
			v_sbop_inland_marine_nsi, '740',
			v_cbop_general_liability_wbm, '440',
			v_smart_general_liability_wbm, '450',
			v_sbop_general_liability_nsi, '740',
			v_cbop_fidelity_wbm, '440',
			v_sbop_fidelity_nsi, '740',
			v_cbop_burglary_and_theft_wbm, '440',
			v_smart_burglary_and_theft_wbm, '450',
			v_sbop_burglary_and_theft_nsi, '740',
			v_gbop_earthquake_wbm, '460',
			v_gbop_fire_wbm, '460',
			v_gbop_allied_lines_wbm, '460',
			v_gbop_inland_marine_wbm, '460',
			v_gbop_general_liability_wbm, '460',
			v_gbop_commercial_auto_liability_wbm, '460',
			v_gbop_cmp_property_liability_peril_901_904_wbm, '460',
			v_gbop_cmp_liability_wbm, '460',
			v_gbop_comm_auto_physical_damage_wbm, '460',
			v_gbop_fidelity_wbm, '460',
			v_gbop_burglary_and_theft_wbm, '460',
			v_personal_umbrella_general_liability_wbm, '480',
			v_commercial_umbrella_general_liability_wbm, '500',
			v_commercial_umbrella_general_liability_nsi, '760',
			v_workers_comp_pool_total_workers_comp_wbm, '580',
			v_fidelity_bonds_fidelity_nsi, '780',
			v_fidelity_bonds_burgulary_and_theft_nsi, '780',
			v_surety_bonds_surety_nsi, '800',
			v_d_and_o_claims_made_general_liability_wbm, '530',
			v_epli_general_liability_claims_made_wbm, '520',
			v_epli_general_liability_claims_made_nsi, '820',
			v_epli_general_liability_claims_made_cpp_cbop_wbm, '520',
			v_smart_general_liability_claims_made_smart_wbm, '450',
			v_epli_general_liability_claims_made_cpp_sbop_nsi, '820',
			v_epli_general_liability_claims_made_bop_wbm, '520',
			v_epli_general_liability_claims_made_gbop_wbm, '520',
			v_d_and_o_claims_made_general_liability_nsi, '840',
			v_boiler_and_machinery_earthquake_wbm, '540',
			v_boiler_and_machinery_boiler_and_machinery_wbm, '540',
			v_boiler_and_machinery_earthquake_nsi, '860',
			v_boiler_and_machinery_boiler_and_machinery_nsi, '860',
			v_mine_subsidence_homeowners_wbm, '560',
			v_mine_subsidence_allied_lines_wbm, '560',
			v_mine_subsidence_allied_lines_nsi, '880',
			'999'
		) AS product_code,
		-- *INF*: DECODE(1,
		-- v_home_and_highway_fire_wbm,'20',
		-- v_home_and_highway_allied_lines_wbm,'40',
		-- v_home_and_highway_homeowners_wbm,'60',
		-- v_home_and_highway_inland_marine_wbm,'120',
		-- v_home_and_highway_earthquake_wbm,'140',
		-- v_home_and_highway_personal_liability_wbm,'200',
		-- v_home_and_highway_general_liability_wbm,'220',
		-- v_home_and_highway_pp_auto_liability_wbm,'260',
		-- v_home_and_highway_pp_auto_physical_damage_wbm,'440',
		-- v_preferred_auto_pp_auto_liability_wbm,'260',
		-- v_preferred_auto_pp_auto_physical_damage_wbm,'440',
		-- v_select_auto_pp_auto_liability_wbm,'260',
		-- v_select_auto_pp_auto_physical_damage_wbm,'440',
		-- v_standard_homeowners_allied_lined_wbm,'40',
		-- v_standard_homeowners_homeowners_wbm,'60',
		-- v_standard_homeowners_inland_marine_wbm,'120',
		-- v_standard_homeowners_earthquake_wbm,'140',
		-- v_standard_homeowners_personal_liability_wbm,'200',
		-- v_select_homeowners_allied_lines_wbm,'40',
		-- v_select_homeowners_homeowners_wbm,'60',
		-- v_select_homeowners_inland_marine_wbm,'120',
		-- v_select_homeowners_earthquake_wbm,'140',
		-- v_select_homeowners_personal_liability_wbm,'200',
		-- v_dwelling_fire_fire_wbm,'20',
		-- v_dwelling_fire_allied_lines_wbm,'40',
		-- v_dwelling_fire_earthquake_wbm,'140',
		-- v_personal_inland_marine_inland_marine_wbm,'120',
		-- v_personal_inland_marine_personal_liability_wbm,'200',
		-- v_motorcycle_pp_auto_liability_wbm,'260',
		-- v_motorcycle_pp_auto_physical_damage_wbm,'440',
		-- v_boatowners_inland_marine_wbm,'120',
		-- v_boatowners_personal_liability_wbm,'200',
		-- v_alternative_one_pp_auto_liability_wbm,'260',
		-- v_alternative_one_pp_auto_physical_damage_wbm,'440',
		-- v_alternative_one_star_pp_auto_liability_wbm,'260',
		-- v_alternative_one_star_pp_auto_physical_damage_wbm,'440',
		-- v_commercial_auto_commercial_auto_liability_wbm,'340',
		-- v_commercial_auto_commercial_auto_liability_nsi,'340',
		-- v_commercial_auto_commercial_auto_liability_garage_veh_wbm,'340',
		-- v_commercial_auto_commercial_auto_liability_garage_veh_nsi,'340',
		-- v_commercial_auto_comm_auto_physical_damage_wbm,'500',
		-- v_commercial_auto_comm_auto_physical_damage_nsi,'500',
		-- v_commercial_auto_comm_auto_physical_damage_garage_veh_wbm,'500',
		-- v_commercial_auto_comm_auto_physical_damage_garage_veh_nsi,'500',
		-- v_garage_liability_commercial_auto_liability_wbm,'340',
		-- v_garage_liability_commercial_auto_liability_nsi,'340',
		-- v_garage_liability_commercial_auto_physical_damage_wbm,'500',
		-- v_garage_liability_commercial_auto_physical_damage_nsi,'500',
		-- v_workers_comp_total_non_pool_workers_comp_wbm,'160',
		-- v_workers_comp_total_non_pool_workers_comp_nsi,'160',
		-- v_workers_comp_total_non_pool_workers_comp_argent,'160',
		-- v_commercial_property_earthquake_wbm,'140',
		-- v_commercial_property_fire_wbm,'20',
		-- v_commercial_property_earthquake_nsi,'140',
		-- v_commercial_property_fire_nsi,'20',
		-- v_commercial_property_allied_lines_wbm,'40',
		-- v_commercial_property_allied_lines_nsi,'40',
		-- v_metalworkers_earthquake_wbm,'140',
		-- v_metalworkers_fire_wbm,'20',
		-- v_metalworkers_allied_lines_wbm,'40',
		-- v_metalworkers_inland_marine_wbm,'120',
		-- v_metalworkers_general_liability_wbm,'220',
		-- v_metalworkers_products_liability_wbm,'240',
		-- v_metalworkers_claims_made_product_liability_wbm,'250',
		-- v_metalworkers_fidelity_wbm,'600',
		-- v_metalworkers_burglary_and_theft_wbm,'640',
		-- v_woodworkers_earthquake_wbm,'140',
		-- v_woodworkers_fire_wbm,'20',
		-- v_woodworkers_allied_lines_wbm,'40',
		-- v_woodworkers_inland_marine_wbm,'120',
		-- v_woodworkers_general_liability_wbm,'220',
		-- v_woodworkers_products_liability_wbm,'240',
		-- v_woodworkers_claims_made_product_liability_wbm,'250',
		-- v_woodworkers_fidelity_wbm,'600',
		-- v_woodworkers_burglary_and_theft_wbm,'640',
		-- v_general_liability_general_liability_wbm,'220',
		-- v_general_liability_general_liability_nsi,'220',
		-- v_general_liability_products_liability_wbm,'240',
		-- v_smart_products_liability_wbm,'240',
		-- v_general_liability_products_liability_nsi,'240',
		-- v_general_liability_claims_made_product_liability_wbm,'250',
		-- v_general_liability_claims_made_product_liability_nsi,'250',
		-- v_general_liability_claims_made_general_liability_nsi,'230',
		-- v_crime_fidelity_wbm,'600',
		-- v_smart_fidelity_wbm,'600',
		-- v_crime_fidelity_nsi,'600',
		-- v_crime_burglary_and_theft_wbm,'640',
		-- v_crime_burglary_and_theft_nsi,'640',
		-- v_commercial_im_inland_marine_wbm,'120',
		-- v_commercial_im_inland_marine_nsi,'120',
		-- v_bop_cmp_property_wbm,'80',
		-- v_bop_unnamed_R13905,'80',
		-- v_bop_cmp_property_liability_peril_901_904_and_599_wbm,to_char(:UDF.ASL_400_PERIL_901_904_LIABILITY(loss_cause,80)),
		-- v_bop_cmp_liability_wbm,'100',
		-- v_cbop_earthquake_wbm,'140',
		-- v_cbop_fire_wbm,'20',
		-- v_smart_earthquake_wbm,'140',
		-- v_smart_fire_wbm,'20',
		-- v_sbop_earthquake_nsi,'140',
		-- v_sbop_fire_nsi,'20',
		-- v_cbop_allied_lines_wbm,'40',
		-- v_smart_allied_lines_wbm,'40',
		-- v_sbop_allied_lines_nsi,'40',
		-- v_cbop_cmp_property_wbm,'80',
		-- v_cbop_inland_marine_wbm,'120',
		-- v_smart_inland_marine_wbm,'120',
		-- v_sbop_inland_marine_nsi,'120',
		-- v_cbop_general_liability_wbm,'220',
		-- v_smart_general_liability_wbm,'220',
		-- v_sbop_general_liability_nsi,'220',
		-- v_cbop_fidelity_wbm,'600',
		-- v_sbop_fidelity_nsi,'600',
		-- v_cbop_burglary_and_theft_wbm,'640',
		-- v_smart_burglary_and_theft_wbm,'640',
		-- v_sbop_burglary_and_theft_nsi,'640',
		-- v_gbop_earthquake_wbm,'140',
		-- v_gbop_fire_wbm,'20',
		-- v_gbop_allied_lines_wbm,'40',
		-- v_gbop_inland_marine_wbm,'120',
		-- v_gbop_general_liability_wbm,'220',
		-- v_gbop_commercial_auto_liability_wbm,'340',
		-- v_gbop_cmp_property_liability_peril_901_904_wbm,'80',
		-- v_gbop_cmp_liability_wbm,'100',
		-- v_gbop_comm_auto_physical_damage_wbm,'500',
		-- v_gbop_fidelity_wbm,'600',
		-- v_gbop_burglary_and_theft_wbm,'640',
		-- v_personal_umbrella_general_liability_wbm,'220',
		-- v_commercial_umbrella_general_liability_wbm,'220',
		-- v_commercial_umbrella_general_liability_nsi,'220',
		-- v_workers_comp_pool_total_workers_comp_wbm,'160',
		-- v_fidelity_bonds_fidelity_nsi,'600',
		-- v_fidelity_bonds_burgulary_and_theft_nsi,'640',
		-- v_surety_bonds_surety_nsi,'620',
		-- v_d_and_o_claims_made_general_liability_wbm,'230',
		-- v_epli_general_liability_claims_made_wbm,'230',
		-- v_epli_general_liability_claims_made_nsi,'230',
		-- v_epli_general_liability_claims_made_cpp_cbop_wbm,'230',
		-- v_smart_general_liability_claims_made_smart_wbm,'230',
		-- v_epli_general_liability_claims_made_cpp_sbop_nsi,'230',
		-- v_epli_general_liability_claims_made_bop_wbm,'230',
		-- v_epli_general_liability_claims_made_gbop_wbm,'230',
		-- v_d_and_o_claims_made_general_liability_nsi,'230',
		-- v_boiler_and_machinery_earthquake_wbm,'140',
		-- v_boiler_and_machinery_boiler_and_machinery_wbm,'660',
		-- v_boiler_and_machinery_earthquake_nsi,'140',
		-- v_boiler_and_machinery_boiler_and_machinery_nsi,'660',
		-- v_mine_subsidence_homeowners_wbm,'60',
		-- v_mine_subsidence_allied_lines_wbm,'40',
		-- v_mine_subsidence_allied_lines_nsi,'40',
		-- '999')
		-- -- DECODES must follow the same order as the program
		DECODE(1,
			v_home_and_highway_fire_wbm, '20',
			v_home_and_highway_allied_lines_wbm, '40',
			v_home_and_highway_homeowners_wbm, '60',
			v_home_and_highway_inland_marine_wbm, '120',
			v_home_and_highway_earthquake_wbm, '140',
			v_home_and_highway_personal_liability_wbm, '200',
			v_home_and_highway_general_liability_wbm, '220',
			v_home_and_highway_pp_auto_liability_wbm, '260',
			v_home_and_highway_pp_auto_physical_damage_wbm, '440',
			v_preferred_auto_pp_auto_liability_wbm, '260',
			v_preferred_auto_pp_auto_physical_damage_wbm, '440',
			v_select_auto_pp_auto_liability_wbm, '260',
			v_select_auto_pp_auto_physical_damage_wbm, '440',
			v_standard_homeowners_allied_lined_wbm, '40',
			v_standard_homeowners_homeowners_wbm, '60',
			v_standard_homeowners_inland_marine_wbm, '120',
			v_standard_homeowners_earthquake_wbm, '140',
			v_standard_homeowners_personal_liability_wbm, '200',
			v_select_homeowners_allied_lines_wbm, '40',
			v_select_homeowners_homeowners_wbm, '60',
			v_select_homeowners_inland_marine_wbm, '120',
			v_select_homeowners_earthquake_wbm, '140',
			v_select_homeowners_personal_liability_wbm, '200',
			v_dwelling_fire_fire_wbm, '20',
			v_dwelling_fire_allied_lines_wbm, '40',
			v_dwelling_fire_earthquake_wbm, '140',
			v_personal_inland_marine_inland_marine_wbm, '120',
			v_personal_inland_marine_personal_liability_wbm, '200',
			v_motorcycle_pp_auto_liability_wbm, '260',
			v_motorcycle_pp_auto_physical_damage_wbm, '440',
			v_boatowners_inland_marine_wbm, '120',
			v_boatowners_personal_liability_wbm, '200',
			v_alternative_one_pp_auto_liability_wbm, '260',
			v_alternative_one_pp_auto_physical_damage_wbm, '440',
			v_alternative_one_star_pp_auto_liability_wbm, '260',
			v_alternative_one_star_pp_auto_physical_damage_wbm, '440',
			v_commercial_auto_commercial_auto_liability_wbm, '340',
			v_commercial_auto_commercial_auto_liability_nsi, '340',
			v_commercial_auto_commercial_auto_liability_garage_veh_wbm, '340',
			v_commercial_auto_commercial_auto_liability_garage_veh_nsi, '340',
			v_commercial_auto_comm_auto_physical_damage_wbm, '500',
			v_commercial_auto_comm_auto_physical_damage_nsi, '500',
			v_commercial_auto_comm_auto_physical_damage_garage_veh_wbm, '500',
			v_commercial_auto_comm_auto_physical_damage_garage_veh_nsi, '500',
			v_garage_liability_commercial_auto_liability_wbm, '340',
			v_garage_liability_commercial_auto_liability_nsi, '340',
			v_garage_liability_commercial_auto_physical_damage_wbm, '500',
			v_garage_liability_commercial_auto_physical_damage_nsi, '500',
			v_workers_comp_total_non_pool_workers_comp_wbm, '160',
			v_workers_comp_total_non_pool_workers_comp_nsi, '160',
			v_workers_comp_total_non_pool_workers_comp_argent, '160',
			v_commercial_property_earthquake_wbm, '140',
			v_commercial_property_fire_wbm, '20',
			v_commercial_property_earthquake_nsi, '140',
			v_commercial_property_fire_nsi, '20',
			v_commercial_property_allied_lines_wbm, '40',
			v_commercial_property_allied_lines_nsi, '40',
			v_metalworkers_earthquake_wbm, '140',
			v_metalworkers_fire_wbm, '20',
			v_metalworkers_allied_lines_wbm, '40',
			v_metalworkers_inland_marine_wbm, '120',
			v_metalworkers_general_liability_wbm, '220',
			v_metalworkers_products_liability_wbm, '240',
			v_metalworkers_claims_made_product_liability_wbm, '250',
			v_metalworkers_fidelity_wbm, '600',
			v_metalworkers_burglary_and_theft_wbm, '640',
			v_woodworkers_earthquake_wbm, '140',
			v_woodworkers_fire_wbm, '20',
			v_woodworkers_allied_lines_wbm, '40',
			v_woodworkers_inland_marine_wbm, '120',
			v_woodworkers_general_liability_wbm, '220',
			v_woodworkers_products_liability_wbm, '240',
			v_woodworkers_claims_made_product_liability_wbm, '250',
			v_woodworkers_fidelity_wbm, '600',
			v_woodworkers_burglary_and_theft_wbm, '640',
			v_general_liability_general_liability_wbm, '220',
			v_general_liability_general_liability_nsi, '220',
			v_general_liability_products_liability_wbm, '240',
			v_smart_products_liability_wbm, '240',
			v_general_liability_products_liability_nsi, '240',
			v_general_liability_claims_made_product_liability_wbm, '250',
			v_general_liability_claims_made_product_liability_nsi, '250',
			v_general_liability_claims_made_general_liability_nsi, '230',
			v_crime_fidelity_wbm, '600',
			v_smart_fidelity_wbm, '600',
			v_crime_fidelity_nsi, '600',
			v_crime_burglary_and_theft_wbm, '640',
			v_crime_burglary_and_theft_nsi, '640',
			v_commercial_im_inland_marine_wbm, '120',
			v_commercial_im_inland_marine_nsi, '120',
			v_bop_cmp_property_wbm, '80',
			v_bop_unnamed_R13905, '80',
			v_bop_cmp_property_liability_peril_901_904_and_599_wbm, to_char(:UDF.ASL_400_PERIL_901_904_LIABILITY(loss_cause, 80
				)
			),
			v_bop_cmp_liability_wbm, '100',
			v_cbop_earthquake_wbm, '140',
			v_cbop_fire_wbm, '20',
			v_smart_earthquake_wbm, '140',
			v_smart_fire_wbm, '20',
			v_sbop_earthquake_nsi, '140',
			v_sbop_fire_nsi, '20',
			v_cbop_allied_lines_wbm, '40',
			v_smart_allied_lines_wbm, '40',
			v_sbop_allied_lines_nsi, '40',
			v_cbop_cmp_property_wbm, '80',
			v_cbop_inland_marine_wbm, '120',
			v_smart_inland_marine_wbm, '120',
			v_sbop_inland_marine_nsi, '120',
			v_cbop_general_liability_wbm, '220',
			v_smart_general_liability_wbm, '220',
			v_sbop_general_liability_nsi, '220',
			v_cbop_fidelity_wbm, '600',
			v_sbop_fidelity_nsi, '600',
			v_cbop_burglary_and_theft_wbm, '640',
			v_smart_burglary_and_theft_wbm, '640',
			v_sbop_burglary_and_theft_nsi, '640',
			v_gbop_earthquake_wbm, '140',
			v_gbop_fire_wbm, '20',
			v_gbop_allied_lines_wbm, '40',
			v_gbop_inland_marine_wbm, '120',
			v_gbop_general_liability_wbm, '220',
			v_gbop_commercial_auto_liability_wbm, '340',
			v_gbop_cmp_property_liability_peril_901_904_wbm, '80',
			v_gbop_cmp_liability_wbm, '100',
			v_gbop_comm_auto_physical_damage_wbm, '500',
			v_gbop_fidelity_wbm, '600',
			v_gbop_burglary_and_theft_wbm, '640',
			v_personal_umbrella_general_liability_wbm, '220',
			v_commercial_umbrella_general_liability_wbm, '220',
			v_commercial_umbrella_general_liability_nsi, '220',
			v_workers_comp_pool_total_workers_comp_wbm, '160',
			v_fidelity_bonds_fidelity_nsi, '600',
			v_fidelity_bonds_burgulary_and_theft_nsi, '640',
			v_surety_bonds_surety_nsi, '620',
			v_d_and_o_claims_made_general_liability_wbm, '230',
			v_epli_general_liability_claims_made_wbm, '230',
			v_epli_general_liability_claims_made_nsi, '230',
			v_epli_general_liability_claims_made_cpp_cbop_wbm, '230',
			v_smart_general_liability_claims_made_smart_wbm, '230',
			v_epli_general_liability_claims_made_cpp_sbop_nsi, '230',
			v_epli_general_liability_claims_made_bop_wbm, '230',
			v_epli_general_liability_claims_made_gbop_wbm, '230',
			v_d_and_o_claims_made_general_liability_nsi, '230',
			v_boiler_and_machinery_earthquake_wbm, '140',
			v_boiler_and_machinery_boiler_and_machinery_wbm, '660',
			v_boiler_and_machinery_earthquake_nsi, '140',
			v_boiler_and_machinery_boiler_and_machinery_nsi, '660',
			v_mine_subsidence_homeowners_wbm, '60',
			v_mine_subsidence_allied_lines_wbm, '40',
			v_mine_subsidence_allied_lines_nsi, '40',
			'999'
		) AS coverage_code,
		-- *INF*: DECODE(1,
		-- v_mine_subsidence_homeowners_wbm,1,
		-- v_mine_subsidence_allied_lines_wbm,1,
		-- v_mine_subsidence_allied_lines_nsi,1,
		-- 0)
		DECODE(1,
			v_mine_subsidence_homeowners_wbm, 1,
			v_mine_subsidence_allied_lines_wbm, 1,
			v_mine_subsidence_allied_lines_nsi, 1,
			0
		) AS mine_sub_special_out
		FROM EXP_accept_inputs
	),
	EXP_router_step_1 AS (
		SELECT
		symbol,
		type_bureau,
		major_peril,
		unit_number,
		location_number,
		class_of_business,
		subline,
		class_code,
		risk_unit_group,
		loss_cause,
		nsi_indicator,
		symbol_pos_1_2_out,
		Path_Flag_Step_1,
		product_code,
		coverage_code,
		mine_sub_special_out AS flag_mine_sub,
		-- *INF*: IIF(Path_Flag_Step_1='2000',1,0)
		IFF(Path_Flag_Step_1 = '2000',
			1,
			0
		) AS flag_2000,
		-- *INF*: IIF(Path_Flag_Step_1='2020',1,0)
		IFF(Path_Flag_Step_1 = '2020',
			1,
			0
		) AS flag_2020,
		-- *INF*: IIF(Path_Flag_Step_1='2050',1,0)
		IFF(Path_Flag_Step_1 = '2050',
			1,
			0
		) AS flag_2050,
		-- *INF*: IIF(Path_Flag_Step_1='2070',1,0)
		IFF(Path_Flag_Step_1 = '2070',
			1,
			0
		) AS flag_2070,
		-- *INF*: IIF(Path_Flag_Step_1='2080',1,0)
		IFF(Path_Flag_Step_1 = '2080',
			1,
			0
		) AS flag_2080,
		-- *INF*: IIF(Path_Flag_Step_1='2100',1,0)
		IFF(Path_Flag_Step_1 = '2100',
			1,
			0
		) AS flag_2100,
		-- *INF*: IIF(Path_Flag_Step_1='2110',1,0)
		IFF(Path_Flag_Step_1 = '2110',
			1,
			0
		) AS flag_2110,
		-- *INF*: IIF(Path_Flag_Step_1='2130',1,0)
		IFF(Path_Flag_Step_1 = '2130',
			1,
			0
		) AS flag_2130,
		-- *INF*: IIF(Path_Flag_Step_1='2150',1,0)
		IFF(Path_Flag_Step_1 = '2150',
			1,
			0
		) AS flag_2150
		FROM EXP_evaluate_step_1
	),
	EXP_2070 AS (
		SELECT
		major_peril,
		-- *INF*: IIF(in(major_peril,'132','147','177','270',@{pipeline().parameters.MP_145_146},@{pipeline().parameters.MP_148_155},@{pipeline().parameters.MP_157_160},@{pipeline().parameters.MP_163_166}),1,0)
		IFF(major_peril IN ('132','147','177','270',@{pipeline().parameters.MP_145_146},@{pipeline().parameters.MP_148_155},@{pipeline().parameters.MP_157_160},@{pipeline().parameters.MP_163_166}),
			1,
			0
		) AS v_Comp,
		-- *INF*: IIF(in(major_peril,'156','178','269',@{pipeline().parameters.MP_170_173}),1,0)
		IFF(major_peril IN ('156','178','269',@{pipeline().parameters.MP_170_173}),
			1,
			0
		) AS v_Coll,
		-- *INF*: DECODE(1,
		-- v_Comp,'520',
		-- v_Coll,'540',
		-- '999')
		DECODE(1,
			v_Comp, '520',
			v_Coll, '540',
			'999'
		) AS coverage_code_2070,
		-- *INF*: '1500'
		-- --999 path does route to 4000, but that's an infinite loop in the source code that just writes 999 anyway.
		'1500' AS Path_Flag_Step_2_2070
		FROM EXP_router_step_1
	),
	EXP_mine_sub AS (
		SELECT
		nsi_indicator,
		'C' AS kind_code_mine_sub,
		'N' AS faculative_ind_mine_sub,
		-- *INF*: IIF(nsi_indicator='W','0008','0094')
		IFF(nsi_indicator = 'W',
			'0008',
			'0094'
		) AS reins_co_number_mine_sub,
		'1500' AS Path_Flag_Step_2_mine_sub
		FROM EXP_router_step_1
	),
	EXP_2050 AS (
		SELECT
		type_bureau,
		major_peril,
		-- *INF*: IIF(type_bureau='AN' and in(major_peril,'130',@{pipeline().parameters.MP_930_931}),1,0)
		IFF(type_bureau = 'AN' 
			AND major_peril IN ('130',@{pipeline().parameters.MP_930_931}),
			1,
			0
		) AS v_PIP,
		-- *INF*: IIF(type_bureau='AL' and in(major_peril,'150',@{pipeline().parameters.MP_100_125},@{pipeline().parameters.MP_140_143}),1,0)
		IFF(type_bureau = 'AL' 
			AND major_peril IN ('150',@{pipeline().parameters.MP_100_125},@{pipeline().parameters.MP_140_143}),
			1,
			0
		) AS v_Other_Than_PIP,
		-- *INF*: DECODE(1,
		-- v_PIP,'2060',
		-- v_Other_Than_PIP,'2060',
		-- '1500')
		DECODE(1,
			v_PIP, '2060',
			v_Other_Than_PIP, '2060',
			'1500'
		) AS Path_Flag_Step_2_2050,
		-- *INF*: DECODE(1,
		-- v_PIP,'360',
		-- v_Other_Than_PIP,'380',
		-- '999')
		DECODE(1,
			v_PIP, '360',
			v_Other_Than_PIP, '380',
			'999'
		) AS coverage_code_2050
		FROM EXP_router_step_1
	),
	EXP_2020 AS (
		SELECT
		major_peril,
		-- *INF*: IIF(in(major_peril,'168','169','174','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_155},@{pipeline().parameters.MP_157_163}),1,0)
		IFF(major_peril IN ('168','169','174','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_155},@{pipeline().parameters.MP_157_163}),
			1,
			0
		) AS v_Comp,
		-- *INF*: IIF(in(major_peril,'156','178',@{pipeline().parameters.MP_170_173}),1,0)
		IFF(major_peril IN ('156','178',@{pipeline().parameters.MP_170_173}),
			1,
			0
		) AS v_Coll,
		'1500' AS Path_Flag_Step_2_2020,
		-- *INF*: DECODE(1,
		-- v_Comp,'460',
		-- v_Coll, '480',
		-- '999')
		DECODE(1,
			v_Comp, '460',
			v_Coll, '480',
			'999'
		) AS coverage_code_2020
		FROM EXP_router_step_1
	),
	EXP_Perform_2080 AS (
		SELECT
		type_bureau,
		major_peril,
		-- *INF*: IIF(type_bureau='AN' and in(major_peril,'130',@{pipeline().parameters.MP_930_931}),1,0)
		IFF(type_bureau = 'AN' 
			AND major_peril IN ('130',@{pipeline().parameters.MP_930_931}),
			1,
			0
		) AS v_GA_Comm_Auto_PIP,
		-- *INF*: IIF(type_bureau='AL' and in(major_peril,'599',@{pipeline().parameters.MP_100_125},@{pipeline().parameters.MP_271_274}),1,0)
		IFF(type_bureau = 'AL' 
			AND major_peril IN ('599',@{pipeline().parameters.MP_100_125},@{pipeline().parameters.MP_271_274}),
			1,
			0
		) AS v_GA_Comm_Auto_Other_Than_PIP,
		-- *INF*: DECODE(1,
		-- v_GA_Comm_Auto_PIP,'2090',
		-- v_GA_Comm_Auto_Other_Than_PIP,'2090',
		-- '1500')
		-- 
		DECODE(1,
			v_GA_Comm_Auto_PIP, '2090',
			v_GA_Comm_Auto_Other_Than_PIP, '2090',
			'1500'
		) AS Path_Flag_Step_2_2080,
		-- *INF*: DECODE(1,
		-- v_GA_Comm_Auto_PIP,'360',
		-- v_GA_Comm_Auto_Other_Than_PIP,'380',
		-- '999')
		DECODE(1,
			v_GA_Comm_Auto_PIP, '360',
			v_GA_Comm_Auto_Other_Than_PIP, '380',
			'999'
		) AS coverage_code_2080
		FROM EXP_router_step_1
	),
	EXP_2130 AS (
		SELECT
		major_peril,
		-- *INF*: IIF(in(major_peril,'132','177',@{pipeline().parameters.MP_145_155},@{pipeline().parameters.MP_157_160},@{pipeline().parameters.MP_165_166}),1,0)
		IFF(major_peril IN ('132','177',@{pipeline().parameters.MP_145_155},@{pipeline().parameters.MP_157_160},@{pipeline().parameters.MP_165_166}),
			1,
			0
		) AS v_Comp,
		-- *INF*: IIF(in(major_peril,'156','178','269',@{pipeline().parameters.MP_170_173}),1,0)
		IFF(major_peril IN ('156','178','269',@{pipeline().parameters.MP_170_173}),
			1,
			0
		) AS v_Coll,
		-- *INF*: DECODE(1,
		-- v_Comp,'520',
		-- v_Coll,'540',
		-- '999')
		-- 
		DECODE(1,
			v_Comp, '520',
			v_Coll, '540',
			'999'
		) AS coverage_code_2130,
		'1500' AS Path_Flag_Step_2_2130
		FROM EXP_router_step_1
	),
	EXP_2150 AS (
		SELECT
		loss_cause,
		-- *INF*: IIF(in(loss_cause,'05','75'),1,0)
		IFF(loss_cause IN ('05','75'),
			1,
			0
		) AS v_Indemnity,
		-- *INF*: IIF(in(loss_cause,'06','07'),1,0)
		IFF(loss_cause IN ('06','07'),
			1,
			0
		) AS v_Medical,
		-- *INF*: DECODE(1,
		-- v_Indemnity,'180',
		-- v_Medical,'190',
		-- '999')
		-- --this actually has no default condition in code
		DECODE(1,
			v_Indemnity, '180',
			v_Medical, '190',
			'999'
		) AS coverage_code_2150,
		'1500' AS Path_Flag_Step_2_2150
		FROM EXP_router_step_1
	),
	EXP_2000 AS (
		SELECT
		type_bureau,
		major_peril,
		-- *INF*: IIF(type_bureau='RN' and major_peril='130',1,0)
		IFF(type_bureau = 'RN' 
			AND major_peril = '130',
			1,
			0
		) AS v_PIP,
		-- *INF*: IIF(type_bureau='RL' and in(major_peril,'150',@{pipeline().parameters.MP_100_121},@{pipeline().parameters.MP_140_143}),1,0)
		IFF(type_bureau = 'RL' 
			AND major_peril IN ('150',@{pipeline().parameters.MP_100_121},@{pipeline().parameters.MP_140_143}),
			1,
			0
		) AS v_Other_than_PIP,
		-- *INF*: DECODE(1,
		-- v_PIP,'270',
		-- v_Other_than_PIP,'280',
		-- '999')
		DECODE(1,
			v_PIP, '270',
			v_Other_than_PIP, '280',
			'999'
		) AS coverage_code_2000,
		-- *INF*: DECODE(1,
		-- v_PIP,'2010',
		-- v_Other_than_PIP,'2010',
		-- '1500')
		DECODE(1,
			v_PIP, '2010',
			v_Other_than_PIP, '2010',
			'1500'
		) AS Path_Flag_Step_2_2000
		FROM EXP_router_step_1
	),
	EXP_2100 AS (
		SELECT
		major_peril,
		-- *INF*: IIF(in(major_peril,'132','177','270',@{pipeline().parameters.MP_145_146},@{pipeline().parameters.MP_148_155},@{pipeline().parameters.MP_157_160},@{pipeline().parameters.MP_165_166}),1,0)
		IFF(major_peril IN ('132','177','270',@{pipeline().parameters.MP_145_146},@{pipeline().parameters.MP_148_155},@{pipeline().parameters.MP_157_160},@{pipeline().parameters.MP_165_166}),
			1,
			0
		) AS v_Comp,
		-- *INF*: IIF(in(major_peril,'156','178','269',@{pipeline().parameters.MP_170_173}),1,0)
		IFF(major_peril IN ('156','178','269',@{pipeline().parameters.MP_170_173}),
			1,
			0
		) AS v_Coll,
		-- *INF*: DECODE(1,
		-- v_Comp,'520',
		-- v_Coll,'540',
		-- '999')
		DECODE(1,
			v_Comp, '520',
			v_Coll, '540',
			'999'
		) AS coverage_code_2100,
		'1500' AS Path_Flag_Step_2_2100
		FROM EXP_router_step_1
	),
	EXP_perform_2110 AS (
		SELECT
		type_bureau,
		major_peril,
		-- *INF*: IIF(type_bureau='AN' and in(major_peril,'130',@{pipeline().parameters.MP_930_931}),1,0)
		IFF(type_bureau = 'AN' 
			AND major_peril IN ('130',@{pipeline().parameters.MP_930_931}),
			1,
			0
		) AS v_PIP,
		-- *INF*: IIF(type_bureau='AL' and in(major_peril,'599',@{pipeline().parameters.MP_100_125},@{pipeline().parameters.MP_271_274}),1,0)
		IFF(type_bureau = 'AL' 
			AND major_peril IN ('599',@{pipeline().parameters.MP_100_125},@{pipeline().parameters.MP_271_274}),
			1,
			0
		) AS v_Other_than_PIP,
		-- *INF*: DECODE(1,
		-- v_PIP,'2120',
		-- v_Other_than_PIP,'2120',
		-- '1500')
		DECODE(1,
			v_PIP, '2120',
			v_Other_than_PIP, '2120',
			'1500'
		) AS Path_Flag_Step_2_2110,
		-- *INF*: DECODE(1,
		-- v_PIP,'360',
		-- v_Other_than_PIP,'380',
		-- '999')
		DECODE(1,
			v_PIP, '360',
			v_Other_than_PIP, '380',
			'999'
		) AS coverage_code_2110
		FROM EXP_router_step_1
	),
	EXP_union_router_Step_2 AS (
		SELECT
		EXP_router_step_1.symbol,
		EXP_router_step_1.type_bureau,
		EXP_router_step_1.major_peril,
		EXP_router_step_1.unit_number,
		EXP_router_step_1.location_number,
		EXP_router_step_1.class_of_business,
		EXP_router_step_1.subline,
		EXP_router_step_1.class_code,
		EXP_router_step_1.risk_unit_group,
		EXP_router_step_1.loss_cause,
		EXP_router_step_1.nsi_indicator,
		EXP_router_step_1.symbol_pos_1_2_out,
		EXP_router_step_1.Path_Flag_Step_1,
		EXP_router_step_1.product_code,
		EXP_router_step_1.coverage_code AS coverage_code_1,
		EXP_router_step_1.flag_2000,
		EXP_2000.coverage_code_2000,
		EXP_2000.Path_Flag_Step_2_2000,
		EXP_router_step_1.flag_2020,
		EXP_2020.Path_Flag_Step_2_2020,
		EXP_2020.coverage_code_2020,
		EXP_router_step_1.flag_2050,
		EXP_2050.Path_Flag_Step_2_2050,
		EXP_2050.coverage_code_2050,
		EXP_router_step_1.flag_2070,
		EXP_2070.coverage_code_2070,
		EXP_2070.Path_Flag_Step_2_2070,
		EXP_router_step_1.flag_2080,
		EXP_Perform_2080.Path_Flag_Step_2_2080,
		EXP_Perform_2080.coverage_code_2080,
		EXP_router_step_1.flag_2100,
		EXP_2100.coverage_code_2100,
		EXP_2100.Path_Flag_Step_2_2100,
		EXP_router_step_1.flag_2110,
		EXP_perform_2110.Path_Flag_Step_2_2110,
		EXP_perform_2110.coverage_code_2110,
		EXP_router_step_1.flag_2130,
		EXP_2130.coverage_code_2130,
		EXP_2130.Path_Flag_Step_2_2130,
		EXP_router_step_1.flag_2150,
		EXP_2150.coverage_code_2150,
		EXP_2150.Path_Flag_Step_2_2150,
		EXP_router_step_1.flag_mine_sub,
		EXP_mine_sub.kind_code_mine_sub,
		EXP_mine_sub.faculative_ind_mine_sub AS facultative_ind_mine_sub,
		EXP_mine_sub.reins_co_number_mine_sub,
		EXP_mine_sub.Path_Flag_Step_2_mine_sub,
		-- *INF*: DECODE(TRUE,
		-- flag_2000,coverage_code_2000,
		-- flag_2020,coverage_code_2020,
		-- flag_2050,coverage_code_2050,
		-- flag_2070,coverage_code_2070,
		-- flag_2080,coverage_code_2080,
		-- flag_2100,coverage_code_2100,
		-- flag_2110,coverage_code_2110,
		-- flag_2130,coverage_code_2130,
		-- flag_2150,coverage_code_2150,
		-- 'N/A')
		-- 
		DECODE(TRUE,
			flag_2000, coverage_code_2000,
			flag_2020, coverage_code_2020,
			flag_2050, coverage_code_2050,
			flag_2070, coverage_code_2070,
			flag_2080, coverage_code_2080,
			flag_2100, coverage_code_2100,
			flag_2110, coverage_code_2110,
			flag_2130, coverage_code_2130,
			flag_2150, coverage_code_2150,
			'N/A'
		) AS coverage_code_step_2,
		-- *INF*: DECODE(TRUE,
		-- flag_2000,Path_Flag_Step_2_2000,
		-- flag_2020,Path_Flag_Step_2_2020,
		-- flag_2050,Path_Flag_Step_2_2050,
		-- flag_2070,Path_Flag_Step_2_2070,
		-- flag_2080,Path_Flag_Step_2_2080,
		-- flag_2100,Path_Flag_Step_2_2100,
		-- flag_2110,Path_Flag_Step_2_2110,
		-- flag_2130,Path_Flag_Step_2_2130,
		-- flag_2150,Path_Flag_Step_2_2150,
		-- '1500')
		DECODE(TRUE,
			flag_2000, Path_Flag_Step_2_2000,
			flag_2020, Path_Flag_Step_2_2020,
			flag_2050, Path_Flag_Step_2_2050,
			flag_2070, Path_Flag_Step_2_2070,
			flag_2080, Path_Flag_Step_2_2080,
			flag_2100, Path_Flag_Step_2_2100,
			flag_2110, Path_Flag_Step_2_2110,
			flag_2130, Path_Flag_Step_2_2130,
			flag_2150, Path_Flag_Step_2_2150,
			'1500'
		) AS Path_Flag_Step_3,
		-- *INF*: IIF(Path_Flag_Step_3='2010',1,0)
		IFF(Path_Flag_Step_3 = '2010',
			1,
			0
		) AS flag_2010,
		-- *INF*: IIF(Path_Flag_Step_3='2060',1,0)
		IFF(Path_Flag_Step_3 = '2060',
			1,
			0
		) AS flag_2060,
		-- *INF*: IIF(Path_Flag_Step_3='2090',1,0)
		IFF(Path_Flag_Step_3 = '2090',
			1,
			0
		) AS flag_2090,
		-- *INF*: IIF(Path_Flag_Step_3='2120',1,0)
		IFF(Path_Flag_Step_3 = '2120',
			1,
			0
		) AS flag_2120
		FROM EXP_2000
		 -- Manually join with EXP_2020
		 -- Manually join with EXP_2050
		 -- Manually join with EXP_2070
		 -- Manually join with EXP_2100
		 -- Manually join with EXP_2130
		 -- Manually join with EXP_2150
		 -- Manually join with EXP_Perform_2080
		 -- Manually join with EXP_mine_sub
		 -- Manually join with EXP_perform_2110
		 -- Manually join with EXP_router_step_1
	),
	EXP_2120 AS (
		SELECT
		major_peril,
		loss_cause,
		-- *INF*: IIF(in(major_peril,'130',@{pipeline().parameters.MP_101_103},@{pipeline().parameters.MP_114_119},@{pipeline().parameters.MP_930_931}),1,0)
		IFF(major_peril IN ('130',@{pipeline().parameters.MP_101_103},@{pipeline().parameters.MP_114_119},@{pipeline().parameters.MP_930_931}),
			1,
			0
		) AS v_BI,
		-- *INF*: IIF(in(major_peril,@{pipeline().parameters.MP_110_112},@{pipeline().parameters.MP_120_125}),1,0)
		IFF(major_peril IN (@{pipeline().parameters.MP_110_112},@{pipeline().parameters.MP_120_125}),
			1,
			0
		) AS v_PD,
		-- *INF*: IIF(in(major_peril,'100','599',@{pipeline().parameters.MP_271_274}),1,0)
		IFF(major_peril IN ('100','599',@{pipeline().parameters.MP_271_274}),
			1,
			0
		) AS v_MP,
		-- *INF*: DECODE(1,
		-- v_BI,'400',
		-- v_PD,'420',
		-- v_MP,to_char(:UDF.ASL_300_PERIL_100_PD(loss_cause,400)),
		-- '999'
		-- )
		DECODE(1,
			v_BI, '400',
			v_PD, '420',
			v_MP, to_char(:UDF.ASL_300_PERIL_100_PD(loss_cause, 400
				)
			),
			'999'
		) AS coverage_code_2120
		FROM EXP_union_router_Step_2
	),
	EXP_2010 AS (
		SELECT
		major_peril,
		loss_cause,
		-- *INF*: IIF(in(major_peril,'130',@{pipeline().parameters.MP_101_103},@{pipeline().parameters.MP_114_119},@{pipeline().parameters.MP_140_143}),1,0)
		IFF(major_peril IN ('130',@{pipeline().parameters.MP_101_103},@{pipeline().parameters.MP_114_119},@{pipeline().parameters.MP_140_143}),
			1,
			0
		) AS v_BI,
		-- *INF*: IIF(in(major_peril,@{pipeline().parameters.MP_110_112},@{pipeline().parameters.MP_120_121}),1,0)
		IFF(major_peril IN (@{pipeline().parameters.MP_110_112},@{pipeline().parameters.MP_120_121}),
			1,
			0
		) AS v_PD,
		-- *INF*: IIF(major_peril='100',1,0)
		IFF(major_peril = '100',
			1,
			0
		) AS v_MP,
		-- *INF*: DECODE(1,
		-- v_BI,'300',
		-- v_PD,'320',
		-- v_MP,to_char(:UDF.ASL_300_PERIL_100_PD(loss_cause,300)),
		-- '999')
		-- 
		DECODE(1,
			v_BI, '300',
			v_PD, '320',
			v_MP, to_char(:UDF.ASL_300_PERIL_100_PD(loss_cause, 300
				)
			),
			'999'
		) AS coverage_code_2010
		FROM EXP_union_router_Step_2
	),
	EXP_2060 AS (
		SELECT
		major_peril,
		loss_cause,
		-- *INF*: IIF(in(major_peril,'130','150',@{pipeline().parameters.MP_101_103},@{pipeline().parameters.MP_114_119},@{pipeline().parameters.MP_140_143},@{pipeline().parameters.MP_930_931}),1,0)
		IFF(major_peril IN ('130','150',@{pipeline().parameters.MP_101_103},@{pipeline().parameters.MP_114_119},@{pipeline().parameters.MP_140_143},@{pipeline().parameters.MP_930_931}),
			1,
			0
		) AS v_BI,
		-- *INF*: IIF(in(major_peril,@{pipeline().parameters.MP_110_112},@{pipeline().parameters.MP_120_125}),1,0)
		IFF(major_peril IN (@{pipeline().parameters.MP_110_112},@{pipeline().parameters.MP_120_125}),
			1,
			0
		) AS v_PD,
		-- *INF*: IIF(in(major_peril,'100','599'),1,0)
		IFF(major_peril IN ('100','599'),
			1,
			0
		) AS v_MP,
		-- *INF*: DECODE(1,
		-- v_BI,'400',
		-- v_PD,'420',
		-- v_MP,to_char(:UDF.ASL_300_PERIL_100_PD(loss_cause,400)),
		-- '999')
		DECODE(1,
			v_BI, '400',
			v_PD, '420',
			v_MP, to_char(:UDF.ASL_300_PERIL_100_PD(loss_cause, 400
				)
			),
			'999'
		) AS coverage_code_2060
		FROM EXP_union_router_Step_2
	),
	EXP_2090 AS (
		SELECT
		major_peril,
		loss_cause,
		-- *INF*: IIF(in(major_peril,'130',@{pipeline().parameters.MP_101_103},@{pipeline().parameters.MP_114_119},@{pipeline().parameters.MP_930_931}),1,0)
		IFF(major_peril IN ('130',@{pipeline().parameters.MP_101_103},@{pipeline().parameters.MP_114_119},@{pipeline().parameters.MP_930_931}),
			1,
			0
		) AS v_GA_Comm_Auto_Bi,
		-- *INF*: IIF(in(major_peril,@{pipeline().parameters.MP_110_112},@{pipeline().parameters.MP_120_125}),1,0)
		IFF(major_peril IN (@{pipeline().parameters.MP_110_112},@{pipeline().parameters.MP_120_125}),
			1,
			0
		) AS v_GA_Comm_Auto_Pd,
		-- *INF*: IIF(in(major_peril,'100','599',@{pipeline().parameters.MP_271_274}),1,0)
		IFF(major_peril IN ('100','599',@{pipeline().parameters.MP_271_274}),
			1,
			0
		) AS v_GA_Comm_Auto,
		-- *INF*: DECODE(1,
		-- v_GA_Comm_Auto_Bi,'400',
		-- v_GA_Comm_Auto_Pd,'420',
		-- v_GA_Comm_Auto,to_char(:UDF.ASL_300_PERIL_100_PD(loss_cause,400)),
		-- '999')
		DECODE(1,
			v_GA_Comm_Auto_Bi, '400',
			v_GA_Comm_Auto_Pd, '420',
			v_GA_Comm_Auto, to_char(:UDF.ASL_300_PERIL_100_PD(loss_cause, 400
				)
			),
			'999'
		) AS coverage_code_2090
		FROM EXP_union_router_Step_2
	),
	EXP_union_output AS (
		SELECT
		EXP_union_router_Step_2.symbol,
		EXP_union_router_Step_2.type_bureau,
		EXP_union_router_Step_2.major_peril,
		EXP_union_router_Step_2.unit_number,
		EXP_union_router_Step_2.location_number,
		EXP_union_router_Step_2.class_of_business,
		EXP_union_router_Step_2.subline,
		EXP_union_router_Step_2.class_code,
		EXP_union_router_Step_2.risk_unit_group,
		EXP_union_router_Step_2.loss_cause,
		EXP_union_router_Step_2.nsi_indicator,
		EXP_union_router_Step_2.symbol_pos_1_2_out,
		EXP_union_router_Step_2.Path_Flag_Step_1,
		EXP_union_router_Step_2.product_code,
		EXP_union_router_Step_2.coverage_code_1,
		EXP_union_router_Step_2.flag_mine_sub,
		EXP_union_router_Step_2.kind_code_mine_sub,
		EXP_union_router_Step_2.facultative_ind_mine_sub,
		EXP_union_router_Step_2.reins_co_number_mine_sub,
		EXP_union_router_Step_2.Path_Flag_Step_2_mine_sub,
		EXP_union_router_Step_2.coverage_code_step_2,
		EXP_union_router_Step_2.flag_2010,
		EXP_2010.coverage_code_2010,
		EXP_union_router_Step_2.flag_2060,
		EXP_2060.coverage_code_2060,
		EXP_union_router_Step_2.flag_2090,
		EXP_2090.coverage_code_2090,
		EXP_union_router_Step_2.flag_2120,
		EXP_2120.coverage_code_2120,
		-- *INF*: DECODE(TRUE,
		-- flag_2010,coverage_code_2010,
		-- flag_2060,coverage_code_2060,
		-- flag_2090,coverage_code_2090,
		-- flag_2120,coverage_code_2120,
		-- 'N/A')
		DECODE(TRUE,
			flag_2010, coverage_code_2010,
			flag_2060, coverage_code_2060,
			flag_2090, coverage_code_2090,
			flag_2120, coverage_code_2120,
			'N/A'
		) AS coverage_code_3
		FROM EXP_2010
		 -- Manually join with EXP_2060
		 -- Manually join with EXP_2090
		 -- Manually join with EXP_2120
		 -- Manually join with EXP_union_router_Step_2
	),
	EXP_output AS (
		SELECT
		product_code,
		coverage_code_1,
		kind_code_mine_sub,
		facultative_ind_mine_sub,
		reins_co_number_mine_sub,
		coverage_code_step_2,
		-- *INF*: DECODE(TRUE,
		-- coverage_code_1 = '40','421',
		-- coverage_code_step_2)
		-- 
		-- ---- SubASLCode of 421 applies to Allied lines PMS data.
		DECODE(TRUE,
			coverage_code_1 = '40', '421',
			coverage_code_step_2
		) AS O_coverage_code_step_2,
		coverage_code_3,
		-- *INF*: DECODE(TRUE,
		-- coverage_code_1 = '40','421',
		-- coverage_code_3)
		DECODE(TRUE,
			coverage_code_1 = '40', '421',
			coverage_code_3
		) AS O_coverage_code_3
		FROM EXP_union_output
	),
	Output AS (
		SELECT
		product_code, 
		coverage_code_1, 
		O_coverage_code_step_2 AS coverage_code_2, 
		O_coverage_code_3 AS coverage_code_3, 
		kind_code_mine_sub, 
		facultative_ind_mine_sub, 
		reins_co_number_mine_sub
		FROM EXP_output
	),
),
EXP_ASL_mapplet_output AS (
	SELECT
	product_code,
	coverage_code_1,
	coverage_code_2,
	coverage_code_3,
	kind_code_mine_sub,
	facultative_ind_mine_sub,
	reins_co_number_mine_sub
	FROM mplt_ASL_WBC8827B_Product_Coverage_Codes
),
LKP_asl_dim AS (
	SELECT
	asl_dim_id,
	asl_code,
	sub_asl_code,
	sub_non_asl_code
	FROM (
		SELECT 
		asl_dim.asl_code as asl_code, 
		asl_dim.asl_dim_id as asl_dim_id, 
		asl_dim.sub_asl_code as sub_asl_code, 
		asl_dim.sub_non_asl_code as sub_non_asl_code 
		FROM asl_dim
		where 
		asl_dim.crrnt_snpsht_flag ='1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY asl_code,sub_asl_code,sub_non_asl_code ORDER BY asl_dim_id DESC) = 1
),
LKP_asl_product_code_dim AS (
	SELECT
	asl_prdct_code_dim_id,
	asl_prdct_code
	FROM (
		SELECT 
		asl_product_code_dim.asl_prdct_code_dim_id as asl_prdct_code_dim_id, 
		asl_product_code_dim.asl_prdct_code as asl_prdct_code 
		FROM 
		asl_product_code_dim
		WHERE
		crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY asl_prdct_code ORDER BY asl_prdct_code_dim_id DESC) = 1
),
EXP_ASL_lookup_output AS (
	SELECT
	EXP_prepare_ASL_input.claim_loss_transaction_fact_id AS claim_loss_trans_fact_id,
	LKP_asl_dim.asl_dim_id,
	LKP_asl_product_code_dim.asl_prdct_code_dim_id,
	EXP_prepare_ASL_input.loss_master_dim_id,
	-- *INF*: IIF(isnull(asl_dim_id),-1,asl_dim_id)
	IFF(asl_dim_id IS NULL,
		- 1,
		asl_dim_id
	) AS asl_dim_id_out,
	-- *INF*: IIF(isnull(asl_prdct_code_dim_id),-1,asl_prdct_code_dim_id)
	IFF(asl_prdct_code_dim_id IS NULL,
		- 1,
		asl_prdct_code_dim_id
	) AS asl_prdct_code_dim_id_out,
	-- *INF*: IIF(isnull(loss_master_dim_id),-1,loss_master_dim_id)
	IFF(loss_master_dim_id IS NULL,
		- 1,
		loss_master_dim_id
	) AS loss_master_dim_id_out,
	EXP_ASL_mapplet_output.kind_code_mine_sub,
	EXP_ASL_mapplet_output.facultative_ind_mine_sub,
	EXP_ASL_mapplet_output.reins_co_number_mine_sub,
	EXP_asl_mapplet_bridge.major_peril,
	EXP_prepare_ASL_input.financial_type_code,
	EXP_prepare_ASL_input.trans_code,
	EXP_prepare_ASL_input.trans_date,
	EXP_prepare_ASL_input.trans_ctgry_code,
	EXP_prepare_ASL_input.trans_amt,
	EXP_prepare_ASL_input.trans_hist_amt,
	EXP_prepare_ASL_input.source_sys_id,
	EXP_prepare_ASL_input.claimant_cov_det_ak_id,
	EXP_prepare_ASL_input.prdct_code_dim_id,
	EXP_prepare_ASL_input.claim_loss_fact_trans_type_dim_id,
	EXP_prepare_ASL_input.InsuranceReferenceDimId,
	EXP_prepare_ASL_input.AgencyDimId,
	EXP_prepare_ASL_input.SalesDivisionDimId,
	EXP_prepare_ASL_input.InsuranceReferenceCoverageDimId,
	EXP_prepare_ASL_input.CoverageDetailDimId,
	SYSDATE AS ModifiedDate
	FROM EXP_ASL_mapplet_output
	 -- Manually join with EXP_asl_mapplet_bridge
	 -- Manually join with EXP_prepare_ASL_input
	LEFT JOIN LKP_asl_dim
	ON LKP_asl_dim.asl_code = EXP_ASL_mapplet_output.coverage_code_1 AND LKP_asl_dim.sub_asl_code = EXP_ASL_mapplet_output.coverage_code_2 AND LKP_asl_dim.sub_non_asl_code = EXP_ASL_mapplet_output.coverage_code_3
	LEFT JOIN LKP_asl_product_code_dim
	ON LKP_asl_product_code_dim.asl_prdct_code = EXP_ASL_mapplet_output.product_code
),
RTR_divert_MP50 AS (
	SELECT
	claim_loss_trans_fact_id,
	asl_dim_id_out,
	asl_prdct_code_dim_id_out,
	loss_master_dim_id_out,
	kind_code_mine_sub,
	facultative_ind_mine_sub,
	reins_co_number_mine_sub,
	major_peril AS major_peril1,
	financial_type_code,
	trans_code,
	trans_date,
	trans_ctgry_code,
	trans_amt,
	trans_hist_amt,
	source_sys_id,
	claimant_cov_det_ak_id,
	prdct_code_dim_id,
	claim_loss_fact_trans_type_dim_id,
	InsuranceReferenceDimId,
	AgencyDimId,
	SalesDivisionDimId,
	InsuranceReferenceCoverageDimId,
	CoverageDetailDimId,
	ModifiedDate
	FROM EXP_ASL_lookup_output
),
RTR_divert_MP50_Update AS (SELECT * FROM RTR_divert_MP50 WHERE TRUE),
RTR_divert_MP50_MP50 AS (SELECT * FROM RTR_divert_MP50 WHERE major_peril1='050'),
EXP_mp50_input AS (
	SELECT
	claim_loss_trans_fact_id,
	asl_dim_id_out AS asl_dim_id_IN,
	asl_prdct_code_dim_id_out AS asl_prdct_code_dim_id_IN,
	loss_master_dim_id_out AS loss_master_dim_id_IN,
	kind_code_mine_sub AS kind_code_mine_sub_IN,
	facultative_ind_mine_sub AS facultative_ind_mine_sub_IN,
	reins_co_number_mine_sub AS reins_co_number_mine_sub_IN,
	major_peril1 AS major_peril_IN,
	financial_type_code AS financial_type_code_IN,
	trans_code AS trans_code_IN,
	trans_date AS trans_date_IN,
	trans_ctgry_code AS trans_ctgry_code_IN,
	trans_amt AS trans_amt_IN,
	trans_hist_amt AS trans_hist_amt_IN,
	source_sys_id AS source_sys_id_IN,
	claimant_cov_det_ak_id AS claimant_cov_det_ak_id_IN,
	prdct_code_dim_id AS prdct_code_dim_id_IN,
	claim_loss_fact_trans_type_dim_id AS claim_loss_fact_trans_type_dim_id_IN,
	InsuranceReferenceDimId,
	AgencyDimId,
	SalesDivisionDimId,
	InsuranceReferenceCoverageDimId,
	CoverageDetailDimId
	FROM RTR_divert_MP50_MP50
),
LKP_claim_loss_transaction_fact AS (
	SELECT
	claim_loss_trans_fact_id,
	err_flag,
	audit_id,
	edw_claim_trans_pk_id,
	edw_claim_reins_trans_pk_id,
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
	reins_cov_dim_id,
	reins_cov_dim_hist_id,
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
	asl_dim_id,
	asl_prdct_code_dim_id,
	loss_master_dim_id,
	strtgc_bus_dvsn_dim_id,
	prdct_code_dim_id,
	ClaimReserveDimId,
	ClaimRepresentativeDimFeatureClaimRepresentativeId,
	FeatureRepresentativeAssignedDateId
	FROM (
		SELECT CLTF.err_flag                                          AS err_flag,
		       CLTF.audit_id                                          AS audit_id,
		       CLTF.edw_claim_trans_pk_id                             AS edw_claim_trans_pk_id,
		       CLTF.edw_claim_reins_trans_pk_id                       AS edw_claim_reins_trans_pk_id,
		       CLTF.claim_occurrence_dim_id                           AS claim_occurrence_dim_id,
		       CLTF.claim_occurrence_dim_hist_id                      AS claim_occurrence_dim_hist_id,
		       CLTF.claimant_dim_id                                   AS claimant_dim_id,
		       CLTF.claimant_dim_hist_id                              AS claimant_dim_hist_id,
		       CLTF.claimant_cov_dim_id                               AS claimant_cov_dim_id,
		       CLTF.claimant_cov_dim_hist_id                          AS claimant_cov_dim_hist_id,
		       CLTF.cov_dim_id                                        AS cov_dim_id,
		       CLTF.cov_dim_hist_id                                   AS cov_dim_hist_id,
		       CLTF.claim_financial_type_dim_id                       AS claim_financial_type_dim_id,
		       CLTF.reins_cov_dim_id                                  AS reins_cov_dim_id,
		       CLTF.reins_cov_dim_hist_id                             AS reins_cov_dim_hist_id,
		       CLTF.claim_rep_dim_prim_claim_rep_id                   AS claim_rep_dim_prim_claim_rep_id,
		       CLTF.claim_rep_dim_prim_claim_rep_hist_id              AS claim_rep_dim_prim_claim_rep_hist_id,
		       CLTF.claim_rep_dim_examiner_id                         AS claim_rep_dim_examiner_id,
		       CLTF.claim_rep_dim_examiner_hist_id                    AS claim_rep_dim_examiner_hist_id,
		       CLTF.claim_rep_dim_prim_litigation_handler_id          AS claim_rep_dim_prim_litigation_handler_id,
		       CLTF.claim_rep_dim_prim_litigation_handler_hist_id     AS claim_rep_dim_prim_litigation_handler_hist_id,
		       CLTF.claim_rep_dim_trans_entry_oper_id                 AS claim_rep_dim_trans_entry_oper_id,
		       CLTF.claim_rep_dim_trans_entry_oper_hist_id            AS claim_rep_dim_trans_entry_oper_hist_id,
		       CLTF.claim_rep_dim_claim_created_by_id                 AS claim_rep_dim_claim_created_by_id,
		       CLTF.pol_dim_id                                        AS pol_dim_id,
		       CLTF.pol_dim_hist_id                                   AS pol_dim_hist_id,
		       CLTF.agency_dim_id                                     AS agency_dim_id,
		       CLTF.agency_dim_hist_id                                AS agency_dim_hist_id,
		       CLTF.claim_pay_dim_id                                  AS claim_pay_dim_id,
		       CLTF.claim_pay_dim_hist_id                             AS claim_pay_dim_hist_id,
		       CLTF.claim_pay_ctgry_type_dim_id                       AS claim_pay_ctgry_type_dim_id,
		       CLTF.claim_pay_ctgry_type_dim_hist_id                  AS claim_pay_ctgry_type_dim_hist_id,
		       CLTF.claim_case_dim_id                                 AS claim_case_dim_id,
		       CLTF.claim_case_dim_hist_id                            AS claim_case_dim_hist_id,
		       CLTF.contract_cust_dim_id                              AS contract_cust_dim_id,
		       CLTF.contract_cust_dim_hist_id                         AS contract_cust_dim_hist_id,
		       CLTF.claim_master_1099_list_dim_id                     AS claim_master_1099_list_dim_id,
		       CLTF.claim_subrogation_dim_id                          AS claim_subrogation_dim_id,
		       CLTF.claim_trans_date_id                               AS claim_trans_date_id,
		       CLTF.claim_trans_reprocess_date_id                     AS claim_trans_reprocess_date_id,
		       CLTF.claim_loss_date_id                                AS claim_loss_date_id,
		       CLTF.claim_discovery_date_id                           AS claim_discovery_date_id,
		       CLTF.claim_scripted_date_id                            AS claim_scripted_date_id,
		       CLTF.source_claim_rpted_date_id                        AS source_claim_rpted_date_id,
		       CLTF.claim_rpted_date_id                               AS claim_rpted_date_id,
		       CLTF.claim_open_date_id                                AS claim_open_date_id,
		       CLTF.claim_close_date_id                               AS claim_close_date_id,
		       CLTF.claim_reopen_date_id                              AS claim_reopen_date_id,
		       CLTF.claim_closed_after_reopen_date_id                 AS claim_closed_after_reopen_date_id,
		       CLTF.claim_notice_only_date_id                         AS claim_notice_only_date_id,
		       CLTF.claim_cat_start_date_id                           AS claim_cat_start_date_id,
		       CLTF.claim_cat_end_date_id                             AS claim_cat_end_date_id,
		       CLTF.claim_rep_assigned_date_id                        AS claim_rep_assigned_date_id,
		       CLTF.claim_rep_unassigned_date_id                      AS claim_rep_unassigned_date_id,
		       CLTF.pol_eff_date_id                                   AS pol_eff_date_id,
		       CLTF.pol_exp_date_id                                   AS pol_exp_date_id,
		       CLTF.claim_subrogation_referred_to_subrogation_date_id AS claim_subrogation_referred_to_subrogation_date_id,
		       CLTF.claim_subrogation_pay_start_date_id               AS claim_subrogation_pay_start_date_id,
		       CLTF.claim_subrogation_closure_date_id                 AS claim_subrogation_closure_date_id,
		       CLTF.acct_entered_date_id                              AS acct_entered_date_id,
		       CLTF.trans_amt                                         AS trans_amt,
		       CLTF.trans_hist_amt                                    AS trans_hist_amt,
		       CLTF.tax_id                                            AS tax_id,
		       CLTF.direct_loss_paid_excluding_recoveries             AS direct_loss_paid_excluding_recoveries,
		       CLTF.direct_loss_outstanding_excluding_recoveries      AS direct_loss_outstanding_excluding_recoveries,
		       CLTF.direct_loss_incurred_excluding_recoveries         AS direct_loss_incurred_excluding_recoveries,
		       CLTF.direct_alae_paid_excluding_recoveries             AS direct_alae_paid_excluding_recoveries,
		       CLTF.direct_alae_outstanding_excluding_recoveries      AS direct_alae_outstanding_excluding_recoveries,
		       CLTF.direct_alae_incurred_excluding_recoveries         AS direct_alae_incurred_excluding_recoveries,
		       CLTF.direct_loss_paid_including_recoveries             AS direct_loss_paid_including_recoveries,
		       CLTF.direct_loss_outstanding_including_recoveries      AS direct_loss_outstanding_including_recoveries,
		       CLTF.direct_loss_incurred_including_recoveries         AS direct_loss_incurred_including_recoveries,
		       CLTF.direct_alae_paid_including_recoveries             AS direct_alae_paid_including_recoveries,
		       CLTF.direct_alae_outstanding_including_recoveries      AS direct_alae_outstanding_including_recoveries,
		       CLTF.direct_alae_incurred_including_recoveries         AS direct_alae_incurred_including_recoveries,
		       CLTF.direct_subrogation_paid                           AS direct_subrogation_paid,
		       CLTF.direct_subrogation_outstanding                    AS direct_subrogation_outstanding,
		       CLTF.direct_subrogation_incurred                       AS direct_subrogation_incurred,
		       CLTF.direct_salvage_paid                               AS direct_salvage_paid,
		       CLTF.direct_salvage_outstanding                        AS direct_salvage_outstanding,
		       CLTF.direct_salvage_incurred                           AS direct_salvage_incurred,
		       CLTF.direct_other_recovery_loss_paid                   AS direct_other_recovery_loss_paid,
		       CLTF.direct_other_recovery_loss_outstanding            AS direct_other_recovery_loss_outstanding,
		       CLTF.direct_other_recovery_loss_incurred               AS direct_other_recovery_loss_incurred,
		       CLTF.direct_other_recovery_alae_paid                   AS direct_other_recovery_alae_paid,
		       CLTF.direct_other_recovery_alae_outstanding            AS direct_other_recovery_alae_outstanding,
		       CLTF.direct_other_recovery_alae_incurred               AS direct_other_recovery_alae_incurred,
		       CLTF.total_direct_loss_recovery_paid                   AS total_direct_loss_recovery_paid,
		       CLTF.total_direct_loss_recovery_outstanding            AS total_direct_loss_recovery_outstanding,
		       CLTF.total_direct_loss_recovery_incurred               AS total_direct_loss_recovery_incurred,
		       CLTF.direct_other_recovery_paid                        AS direct_other_recovery_paid,
		       CLTF.direct_other_recovery_outstanding                 AS direct_other_recovery_outstanding,
		       CLTF.direct_other_recovery_incurred                    AS direct_other_recovery_incurred,
		       CLTF.ceded_loss_paid                                   AS ceded_loss_paid,
		       CLTF.ceded_loss_outstanding                            AS ceded_loss_outstanding,
		       CLTF.ceded_loss_incurred                               AS ceded_loss_incurred,
		       CLTF.ceded_alae_paid                                   AS ceded_alae_paid,
		       CLTF.ceded_alae_outstanding                            AS ceded_alae_outstanding,
		       CLTF.ceded_alae_incurred                               AS ceded_alae_incurred,
		       CLTF.ceded_salvage_paid                                AS ceded_salvage_paid,
		       CLTF.ceded_subrogation_paid                            AS ceded_subrogation_paid,
		       CLTF.ceded_other_recovery_loss_paid                    AS ceded_other_recovery_loss_paid,
		       CLTF.ceded_other_recovery_alae_paid                    AS ceded_other_recovery_alae_paid,
		       CLTF.total_ceded_loss_recovery_paid                    AS total_ceded_loss_recovery_paid,
		       CLTF.net_loss_paid                                     AS net_loss_paid,
		       CLTF.net_loss_outstanding                              AS net_loss_outstanding,
		       CLTF.net_loss_incurred                                 AS net_loss_incurred,
		       CLTF.net_alae_paid                                     AS net_alae_paid,
		       CLTF.net_alae_outstanding                              AS net_alae_outstanding,
		       CLTF.net_alae_incurred                                 AS net_alae_incurred,
		       CLTF.asl_dim_id                                        AS asl_dim_id,
		       CLTF.asl_prdct_code_dim_id                             AS asl_prdct_code_dim_id,
		       CLTF.loss_master_dim_id                                AS loss_master_dim_id,
		       CLTF.strtgc_bus_dvsn_dim_id                            AS strtgc_bus_dvsn_dim_id,
		       CLTF.prdct_code_dim_id                                 AS prdct_code_dim_id,
		       CLTF.ClaimReserveDimId                                 AS ClaimReserveDimId,
			CLTF.ClaimRepresentativeDimFeatureClaimRepresentativeId AS ClaimRepresentativeDimFeatureClaimRepresentativeId,
			CLTF.FeatureRepresentativeAssignedDateId AS FeatureRepresentativeAssignedDateId,
		       CLTF.claim_loss_trans_fact_id                          AS claim_loss_trans_fact_id,
		       CLTF.claim_trans_type_dim_id                           AS claim_trans_type_dim_id
		FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_loss_transaction_fact CLTF , 
		@{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.TARGET_TABLE_OWNER}.VW_claim_transaction CT , 
		@{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail CCD
		WHERE CLTF.edw_claim_trans_pk_id = CT.claim_trans_id
		AND CT.claimant_cov_det_ak_id = CCD.claimant_cov_det_ak_id
		AND CCD.major_peril_code = '050' AND CLTF.audit_id >0
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_loss_trans_fact_id ORDER BY claim_loss_trans_fact_id DESC) = 1
),
EXP_claim_loss_transaction_fact_lookup_output AS (
	SELECT
	LKP_claim_loss_transaction_fact.claim_loss_trans_fact_id,
	LKP_claim_loss_transaction_fact.err_flag,
	LKP_claim_loss_transaction_fact.audit_id,
	LKP_claim_loss_transaction_fact.edw_claim_trans_pk_id,
	LKP_claim_loss_transaction_fact.edw_claim_reins_trans_pk_id,
	LKP_claim_loss_transaction_fact.claim_occurrence_dim_id,
	LKP_claim_loss_transaction_fact.claim_occurrence_dim_hist_id,
	LKP_claim_loss_transaction_fact.claimant_dim_id,
	LKP_claim_loss_transaction_fact.claimant_dim_hist_id,
	LKP_claim_loss_transaction_fact.claimant_cov_dim_id,
	LKP_claim_loss_transaction_fact.claimant_cov_dim_hist_id,
	LKP_claim_loss_transaction_fact.cov_dim_id,
	LKP_claim_loss_transaction_fact.cov_dim_hist_id,
	LKP_claim_loss_transaction_fact.claim_trans_type_dim_id,
	LKP_claim_loss_transaction_fact.claim_financial_type_dim_id,
	LKP_claim_loss_transaction_fact.reins_cov_dim_id,
	LKP_claim_loss_transaction_fact.reins_cov_dim_hist_id,
	LKP_claim_loss_transaction_fact.claim_rep_dim_prim_claim_rep_id,
	LKP_claim_loss_transaction_fact.claim_rep_dim_prim_claim_rep_hist_id,
	LKP_claim_loss_transaction_fact.claim_rep_dim_examiner_id,
	LKP_claim_loss_transaction_fact.claim_rep_dim_examiner_hist_id,
	LKP_claim_loss_transaction_fact.claim_rep_dim_prim_litigation_handler_id,
	LKP_claim_loss_transaction_fact.claim_rep_dim_prim_litigation_handler_hist_id,
	LKP_claim_loss_transaction_fact.claim_rep_dim_trans_entry_oper_id,
	LKP_claim_loss_transaction_fact.claim_rep_dim_trans_entry_oper_hist_id,
	LKP_claim_loss_transaction_fact.claim_rep_dim_claim_created_by_id,
	LKP_claim_loss_transaction_fact.pol_dim_id,
	LKP_claim_loss_transaction_fact.pol_dim_hist_id,
	LKP_claim_loss_transaction_fact.agency_dim_id,
	LKP_claim_loss_transaction_fact.agency_dim_hist_id,
	LKP_claim_loss_transaction_fact.claim_pay_dim_id,
	LKP_claim_loss_transaction_fact.claim_pay_dim_hist_id,
	LKP_claim_loss_transaction_fact.claim_pay_ctgry_type_dim_id,
	LKP_claim_loss_transaction_fact.claim_pay_ctgry_type_dim_hist_id,
	LKP_claim_loss_transaction_fact.claim_case_dim_id,
	LKP_claim_loss_transaction_fact.claim_case_dim_hist_id,
	LKP_claim_loss_transaction_fact.contract_cust_dim_id,
	LKP_claim_loss_transaction_fact.contract_cust_dim_hist_id,
	LKP_claim_loss_transaction_fact.claim_master_1099_list_dim_id,
	LKP_claim_loss_transaction_fact.claim_subrogation_dim_id,
	LKP_claim_loss_transaction_fact.claim_trans_date_id,
	LKP_claim_loss_transaction_fact.claim_trans_reprocess_date_id,
	LKP_claim_loss_transaction_fact.claim_loss_date_id,
	LKP_claim_loss_transaction_fact.claim_discovery_date_id,
	LKP_claim_loss_transaction_fact.claim_scripted_date_id,
	LKP_claim_loss_transaction_fact.source_claim_rpted_date_id,
	LKP_claim_loss_transaction_fact.claim_rpted_date_id,
	LKP_claim_loss_transaction_fact.claim_open_date_id,
	LKP_claim_loss_transaction_fact.claim_close_date_id,
	LKP_claim_loss_transaction_fact.claim_reopen_date_id,
	LKP_claim_loss_transaction_fact.claim_closed_after_reopen_date_id,
	LKP_claim_loss_transaction_fact.claim_notice_only_date_id,
	LKP_claim_loss_transaction_fact.claim_cat_start_date_id,
	LKP_claim_loss_transaction_fact.claim_cat_end_date_id,
	LKP_claim_loss_transaction_fact.claim_rep_assigned_date_id,
	LKP_claim_loss_transaction_fact.claim_rep_unassigned_date_id,
	LKP_claim_loss_transaction_fact.pol_eff_date_id,
	LKP_claim_loss_transaction_fact.pol_exp_date_id,
	LKP_claim_loss_transaction_fact.claim_subrogation_referred_to_subrogation_date_id,
	LKP_claim_loss_transaction_fact.claim_subrogation_pay_start_date_id,
	LKP_claim_loss_transaction_fact.claim_subrogation_closure_date_id,
	LKP_claim_loss_transaction_fact.acct_entered_date_id,
	LKP_claim_loss_transaction_fact.trans_amt,
	LKP_claim_loss_transaction_fact.trans_hist_amt,
	LKP_claim_loss_transaction_fact.tax_id,
	LKP_claim_loss_transaction_fact.direct_loss_paid_excluding_recoveries,
	LKP_claim_loss_transaction_fact.direct_loss_outstanding_excluding_recoveries,
	LKP_claim_loss_transaction_fact.direct_loss_incurred_excluding_recoveries,
	LKP_claim_loss_transaction_fact.direct_alae_paid_excluding_recoveries,
	LKP_claim_loss_transaction_fact.direct_alae_outstanding_excluding_recoveries,
	LKP_claim_loss_transaction_fact.direct_alae_incurred_excluding_recoveries,
	LKP_claim_loss_transaction_fact.direct_loss_paid_including_recoveries,
	LKP_claim_loss_transaction_fact.direct_loss_outstanding_including_recoveries,
	LKP_claim_loss_transaction_fact.direct_loss_incurred_including_recoveries,
	LKP_claim_loss_transaction_fact.direct_alae_paid_including_recoveries,
	LKP_claim_loss_transaction_fact.direct_alae_outstanding_including_recoveries,
	LKP_claim_loss_transaction_fact.direct_alae_incurred_including_recoveries,
	LKP_claim_loss_transaction_fact.direct_subrogation_paid,
	LKP_claim_loss_transaction_fact.direct_subrogation_outstanding,
	LKP_claim_loss_transaction_fact.direct_subrogation_incurred,
	LKP_claim_loss_transaction_fact.direct_salvage_paid,
	LKP_claim_loss_transaction_fact.direct_salvage_outstanding,
	LKP_claim_loss_transaction_fact.direct_salvage_incurred,
	LKP_claim_loss_transaction_fact.direct_other_recovery_loss_paid,
	LKP_claim_loss_transaction_fact.direct_other_recovery_loss_outstanding,
	LKP_claim_loss_transaction_fact.direct_other_recovery_loss_incurred,
	LKP_claim_loss_transaction_fact.direct_other_recovery_alae_paid,
	LKP_claim_loss_transaction_fact.direct_other_recovery_alae_outstanding,
	LKP_claim_loss_transaction_fact.direct_other_recovery_alae_incurred,
	LKP_claim_loss_transaction_fact.total_direct_loss_recovery_paid,
	LKP_claim_loss_transaction_fact.total_direct_loss_recovery_outstanding,
	LKP_claim_loss_transaction_fact.total_direct_loss_recovery_incurred,
	LKP_claim_loss_transaction_fact.direct_other_recovery_paid,
	LKP_claim_loss_transaction_fact.direct_other_recovery_outstanding,
	LKP_claim_loss_transaction_fact.direct_other_recovery_incurred,
	LKP_claim_loss_transaction_fact.ceded_loss_paid,
	LKP_claim_loss_transaction_fact.ceded_loss_outstanding,
	LKP_claim_loss_transaction_fact.ceded_loss_incurred,
	LKP_claim_loss_transaction_fact.ceded_alae_paid,
	LKP_claim_loss_transaction_fact.ceded_alae_outstanding,
	LKP_claim_loss_transaction_fact.ceded_alae_incurred,
	LKP_claim_loss_transaction_fact.ceded_salvage_paid,
	LKP_claim_loss_transaction_fact.ceded_subrogation_paid,
	LKP_claim_loss_transaction_fact.ceded_other_recovery_loss_paid,
	LKP_claim_loss_transaction_fact.ceded_other_recovery_alae_paid,
	LKP_claim_loss_transaction_fact.total_ceded_loss_recovery_paid,
	LKP_claim_loss_transaction_fact.net_loss_paid,
	LKP_claim_loss_transaction_fact.net_loss_outstanding,
	LKP_claim_loss_transaction_fact.net_loss_incurred,
	LKP_claim_loss_transaction_fact.net_alae_paid,
	LKP_claim_loss_transaction_fact.net_alae_outstanding,
	LKP_claim_loss_transaction_fact.net_alae_incurred,
	LKP_claim_loss_transaction_fact.asl_dim_id,
	LKP_claim_loss_transaction_fact.asl_prdct_code_dim_id,
	LKP_claim_loss_transaction_fact.loss_master_dim_id,
	LKP_claim_loss_transaction_fact.strtgc_bus_dvsn_dim_id,
	LKP_claim_loss_transaction_fact.prdct_code_dim_id,
	EXP_mp50_input.asl_dim_id_IN,
	EXP_mp50_input.asl_prdct_code_dim_id_IN,
	EXP_mp50_input.loss_master_dim_id_IN,
	EXP_mp50_input.kind_code_mine_sub_IN,
	EXP_mp50_input.facultative_ind_mine_sub_IN,
	EXP_mp50_input.reins_co_number_mine_sub_IN,
	EXP_mp50_input.major_peril_IN,
	EXP_mp50_input.financial_type_code_IN,
	EXP_mp50_input.trans_code_IN,
	EXP_mp50_input.trans_date_IN,
	EXP_mp50_input.trans_ctgry_code_IN,
	EXP_mp50_input.trans_amt_IN,
	EXP_mp50_input.trans_hist_amt_IN,
	EXP_mp50_input.source_sys_id_IN,
	EXP_mp50_input.claimant_cov_det_ak_id_IN,
	EXP_mp50_input.prdct_code_dim_id_IN,
	LKP_claim_loss_transaction_fact.ClaimReserveDimId,
	LKP_claim_loss_transaction_fact.ClaimRepresentativeDimFeatureClaimRepresentativeId,
	LKP_claim_loss_transaction_fact.FeatureRepresentativeAssignedDateId,
	EXP_mp50_input.InsuranceReferenceDimId,
	EXP_mp50_input.AgencyDimId,
	EXP_mp50_input.SalesDivisionDimId,
	EXP_mp50_input.InsuranceReferenceCoverageDimId,
	EXP_mp50_input.CoverageDetailDimId
	FROM EXP_mp50_input
	LEFT JOIN LKP_claim_loss_transaction_fact
	ON LKP_claim_loss_transaction_fact.claim_loss_trans_fact_id = EXP_mp50_input.claim_loss_trans_fact_id
),
LKP_claim_transaction_type_dim_Get_kind_code AS (
	SELECT
	claim_trans_type_dim_id,
	crrnt_snpsht_flag,
	trans_ctgry_code,
	trans_code,
	s3p_trans_code,
	pms_trans_code,
	trans_base_type_code,
	trans_kind_code,
	trans_rsn,
	type_disability,
	offset_onset_ind
	FROM (
		SELECT 
			claim_trans_type_dim_id,
			crrnt_snpsht_flag,
			trans_ctgry_code,
			trans_code,
			s3p_trans_code,
			pms_trans_code,
			trans_base_type_code,
			trans_kind_code,
			trans_rsn,
			type_disability,
			offset_onset_ind
		FROM claim_transaction_type_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_trans_type_dim_id ORDER BY claim_trans_type_dim_id DESC) = 1
),
FIL_remove_ceded_kind_code AS (
	SELECT
	LKP_claim_transaction_type_dim_Get_kind_code.crrnt_snpsht_flag AS lkp_crrnt_snpsht_flag, 
	LKP_claim_transaction_type_dim_Get_kind_code.trans_ctgry_code AS lkp_trans_ctgry_code, 
	LKP_claim_transaction_type_dim_Get_kind_code.trans_code AS lkp_trans_code, 
	LKP_claim_transaction_type_dim_Get_kind_code.s3p_trans_code AS lkp_s3p_trans_code, 
	LKP_claim_transaction_type_dim_Get_kind_code.pms_trans_code AS lkp_pms_trans_code, 
	LKP_claim_transaction_type_dim_Get_kind_code.trans_base_type_code AS lkp_trans_base_type_code, 
	LKP_claim_transaction_type_dim_Get_kind_code.trans_kind_code AS lkp_trans_kind_code, 
	LKP_claim_transaction_type_dim_Get_kind_code.trans_rsn AS lkp_trans_rsn, 
	LKP_claim_transaction_type_dim_Get_kind_code.type_disability AS lkp_type_disability, 
	LKP_claim_transaction_type_dim_Get_kind_code.offset_onset_ind AS lkp_offset_onset_ind, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_loss_trans_fact_id, 
	EXP_claim_loss_transaction_fact_lookup_output.err_flag, 
	EXP_claim_loss_transaction_fact_lookup_output.audit_id, 
	EXP_claim_loss_transaction_fact_lookup_output.edw_claim_trans_pk_id, 
	EXP_claim_loss_transaction_fact_lookup_output.edw_claim_reins_trans_pk_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_occurrence_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_occurrence_dim_hist_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claimant_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claimant_dim_hist_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claimant_cov_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claimant_cov_dim_hist_id, 
	EXP_claim_loss_transaction_fact_lookup_output.cov_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.cov_dim_hist_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_trans_type_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_financial_type_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.reins_cov_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.reins_cov_dim_hist_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_rep_dim_prim_claim_rep_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_rep_dim_prim_claim_rep_hist_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_rep_dim_examiner_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_rep_dim_examiner_hist_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_rep_dim_prim_litigation_handler_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_rep_dim_prim_litigation_handler_hist_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_rep_dim_trans_entry_oper_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_rep_dim_trans_entry_oper_hist_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_rep_dim_claim_created_by_id, 
	EXP_claim_loss_transaction_fact_lookup_output.pol_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.pol_dim_hist_id, 
	EXP_claim_loss_transaction_fact_lookup_output.agency_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.agency_dim_hist_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_pay_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_pay_dim_hist_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_pay_ctgry_type_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_pay_ctgry_type_dim_hist_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_case_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_case_dim_hist_id, 
	EXP_claim_loss_transaction_fact_lookup_output.contract_cust_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.contract_cust_dim_hist_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_master_1099_list_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_subrogation_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_trans_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_trans_reprocess_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_loss_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_discovery_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_scripted_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.source_claim_rpted_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_rpted_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_open_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_close_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_reopen_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_closed_after_reopen_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_notice_only_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_cat_start_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_cat_end_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_rep_assigned_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_rep_unassigned_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.pol_eff_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.pol_exp_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_subrogation_referred_to_subrogation_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_subrogation_pay_start_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.claim_subrogation_closure_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.acct_entered_date_id, 
	EXP_claim_loss_transaction_fact_lookup_output.trans_amt, 
	EXP_claim_loss_transaction_fact_lookup_output.trans_hist_amt, 
	EXP_claim_loss_transaction_fact_lookup_output.tax_id, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_loss_paid_excluding_recoveries, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_loss_outstanding_excluding_recoveries, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_loss_incurred_excluding_recoveries, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_alae_paid_excluding_recoveries, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_alae_outstanding_excluding_recoveries, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_alae_incurred_excluding_recoveries, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_loss_paid_including_recoveries, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_loss_outstanding_including_recoveries, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_loss_incurred_including_recoveries, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_alae_paid_including_recoveries, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_alae_outstanding_including_recoveries, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_alae_incurred_including_recoveries, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_subrogation_paid, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_subrogation_outstanding, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_subrogation_incurred, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_salvage_paid, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_salvage_outstanding, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_salvage_incurred, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_other_recovery_loss_paid, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_other_recovery_loss_outstanding, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_other_recovery_loss_incurred, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_other_recovery_alae_paid, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_other_recovery_alae_outstanding, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_other_recovery_alae_incurred, 
	EXP_claim_loss_transaction_fact_lookup_output.total_direct_loss_recovery_paid, 
	EXP_claim_loss_transaction_fact_lookup_output.total_direct_loss_recovery_outstanding, 
	EXP_claim_loss_transaction_fact_lookup_output.total_direct_loss_recovery_incurred, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_other_recovery_paid, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_other_recovery_outstanding, 
	EXP_claim_loss_transaction_fact_lookup_output.direct_other_recovery_incurred, 
	EXP_claim_loss_transaction_fact_lookup_output.ceded_loss_paid, 
	EXP_claim_loss_transaction_fact_lookup_output.ceded_loss_outstanding, 
	EXP_claim_loss_transaction_fact_lookup_output.ceded_loss_incurred, 
	EXP_claim_loss_transaction_fact_lookup_output.ceded_alae_paid, 
	EXP_claim_loss_transaction_fact_lookup_output.ceded_alae_outstanding, 
	EXP_claim_loss_transaction_fact_lookup_output.ceded_alae_incurred, 
	EXP_claim_loss_transaction_fact_lookup_output.ceded_salvage_paid, 
	EXP_claim_loss_transaction_fact_lookup_output.ceded_subrogation_paid, 
	EXP_claim_loss_transaction_fact_lookup_output.ceded_other_recovery_loss_paid, 
	EXP_claim_loss_transaction_fact_lookup_output.ceded_other_recovery_alae_paid, 
	EXP_claim_loss_transaction_fact_lookup_output.total_ceded_loss_recovery_paid, 
	EXP_claim_loss_transaction_fact_lookup_output.net_loss_paid, 
	EXP_claim_loss_transaction_fact_lookup_output.net_loss_outstanding, 
	EXP_claim_loss_transaction_fact_lookup_output.net_loss_incurred, 
	EXP_claim_loss_transaction_fact_lookup_output.net_alae_paid, 
	EXP_claim_loss_transaction_fact_lookup_output.net_alae_outstanding, 
	EXP_claim_loss_transaction_fact_lookup_output.net_alae_incurred, 
	EXP_claim_loss_transaction_fact_lookup_output.asl_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.asl_prdct_code_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.loss_master_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.strtgc_bus_dvsn_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.prdct_code_dim_id, 
	EXP_claim_loss_transaction_fact_lookup_output.asl_dim_id_IN, 
	EXP_claim_loss_transaction_fact_lookup_output.asl_prdct_code_dim_id_IN, 
	EXP_claim_loss_transaction_fact_lookup_output.loss_master_dim_id_IN, 
	EXP_claim_loss_transaction_fact_lookup_output.kind_code_mine_sub_IN, 
	EXP_claim_loss_transaction_fact_lookup_output.facultative_ind_mine_sub_IN, 
	EXP_claim_loss_transaction_fact_lookup_output.reins_co_number_mine_sub_IN, 
	EXP_claim_loss_transaction_fact_lookup_output.major_peril_IN, 
	EXP_claim_loss_transaction_fact_lookup_output.financial_type_code_IN, 
	EXP_claim_loss_transaction_fact_lookup_output.trans_code_IN, 
	EXP_claim_loss_transaction_fact_lookup_output.trans_date_IN, 
	EXP_claim_loss_transaction_fact_lookup_output.trans_ctgry_code_IN, 
	EXP_claim_loss_transaction_fact_lookup_output.trans_amt_IN, 
	EXP_claim_loss_transaction_fact_lookup_output.trans_hist_amt_IN, 
	EXP_claim_loss_transaction_fact_lookup_output.source_sys_id_IN, 
	EXP_claim_loss_transaction_fact_lookup_output.claimant_cov_det_ak_id_IN, 
	EXP_claim_loss_transaction_fact_lookup_output.prdct_code_dim_id_IN, 
	EXP_claim_loss_transaction_fact_lookup_output.ClaimReserveDimId, 
	EXP_claim_loss_transaction_fact_lookup_output.ClaimRepresentativeDimFeatureClaimRepresentativeId, 
	EXP_claim_loss_transaction_fact_lookup_output.FeatureRepresentativeAssignedDateId, 
	EXP_claim_loss_transaction_fact_lookup_output.InsuranceReferenceDimId, 
	EXP_claim_loss_transaction_fact_lookup_output.AgencyDimId, 
	EXP_claim_loss_transaction_fact_lookup_output.SalesDivisionDimId, 
	EXP_claim_loss_transaction_fact_lookup_output.InsuranceReferenceCoverageDimId, 
	EXP_claim_loss_transaction_fact_lookup_output.CoverageDetailDimId
	FROM EXP_claim_loss_transaction_fact_lookup_output
	LEFT JOIN LKP_claim_transaction_type_dim_Get_kind_code
	ON LKP_claim_transaction_type_dim_Get_kind_code.claim_trans_type_dim_id = EXP_claim_loss_transaction_fact_lookup_output.claim_trans_type_dim_id
	WHERE rtrim(ltrim(lkp_trans_kind_code))='D'
),
LKP_claim_transaction_type_dim_Get_Ceded_Record AS (
	SELECT
	claim_trans_type_dim_id,
	crrnt_snpsht_flag,
	trans_ctgry_code,
	trans_code,
	s3p_trans_code,
	pms_trans_code,
	trans_base_type_code,
	trans_rsn,
	type_disability,
	offset_onset_ind,
	trans_kind_code
	FROM (
		SELECT 
			claim_trans_type_dim_id,
			crrnt_snpsht_flag,
			trans_ctgry_code,
			trans_code,
			s3p_trans_code,
			pms_trans_code,
			trans_base_type_code,
			trans_rsn,
			type_disability,
			offset_onset_ind,
			trans_kind_code
		FROM claim_transaction_type_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY crrnt_snpsht_flag,trans_ctgry_code,trans_code,s3p_trans_code,pms_trans_code,trans_base_type_code,trans_rsn,type_disability,offset_onset_ind,trans_kind_code ORDER BY claim_trans_type_dim_id DESC) = 1
),
EXP_set_ceded_transaction_type_id AS (
	SELECT
	FIL_remove_ceded_kind_code.claim_loss_trans_fact_id,
	FIL_remove_ceded_kind_code.err_flag,
	FIL_remove_ceded_kind_code.audit_id,
	FIL_remove_ceded_kind_code.edw_claim_trans_pk_id,
	FIL_remove_ceded_kind_code.edw_claim_reins_trans_pk_id,
	FIL_remove_ceded_kind_code.claim_occurrence_dim_id,
	FIL_remove_ceded_kind_code.claim_occurrence_dim_hist_id,
	FIL_remove_ceded_kind_code.claimant_dim_id,
	FIL_remove_ceded_kind_code.claimant_dim_hist_id,
	FIL_remove_ceded_kind_code.claimant_cov_dim_id,
	FIL_remove_ceded_kind_code.claimant_cov_dim_hist_id,
	FIL_remove_ceded_kind_code.cov_dim_id,
	FIL_remove_ceded_kind_code.cov_dim_hist_id,
	LKP_claim_transaction_type_dim_Get_Ceded_Record.claim_trans_type_dim_id,
	-- *INF*: IIF(isnull(claim_trans_type_dim_id),-1,claim_trans_type_dim_id)
	IFF(claim_trans_type_dim_id IS NULL,
		- 1,
		claim_trans_type_dim_id
	) AS lkp_claim_trans_type_dim_id,
	FIL_remove_ceded_kind_code.claim_financial_type_dim_id,
	FIL_remove_ceded_kind_code.reins_cov_dim_id,
	FIL_remove_ceded_kind_code.reins_cov_dim_hist_id,
	FIL_remove_ceded_kind_code.claim_rep_dim_prim_claim_rep_id,
	FIL_remove_ceded_kind_code.claim_rep_dim_prim_claim_rep_hist_id,
	FIL_remove_ceded_kind_code.claim_rep_dim_examiner_id,
	FIL_remove_ceded_kind_code.claim_rep_dim_examiner_hist_id,
	FIL_remove_ceded_kind_code.claim_rep_dim_prim_litigation_handler_id,
	FIL_remove_ceded_kind_code.claim_rep_dim_prim_litigation_handler_hist_id,
	FIL_remove_ceded_kind_code.claim_rep_dim_trans_entry_oper_id,
	FIL_remove_ceded_kind_code.claim_rep_dim_trans_entry_oper_hist_id,
	FIL_remove_ceded_kind_code.claim_rep_dim_claim_created_by_id,
	FIL_remove_ceded_kind_code.pol_dim_id,
	FIL_remove_ceded_kind_code.pol_dim_hist_id,
	FIL_remove_ceded_kind_code.agency_dim_id,
	FIL_remove_ceded_kind_code.agency_dim_hist_id,
	FIL_remove_ceded_kind_code.claim_pay_dim_id,
	FIL_remove_ceded_kind_code.claim_pay_dim_hist_id,
	FIL_remove_ceded_kind_code.claim_pay_ctgry_type_dim_id,
	FIL_remove_ceded_kind_code.claim_pay_ctgry_type_dim_hist_id,
	FIL_remove_ceded_kind_code.claim_case_dim_id,
	FIL_remove_ceded_kind_code.claim_case_dim_hist_id,
	FIL_remove_ceded_kind_code.contract_cust_dim_id,
	FIL_remove_ceded_kind_code.contract_cust_dim_hist_id,
	FIL_remove_ceded_kind_code.claim_master_1099_list_dim_id,
	FIL_remove_ceded_kind_code.claim_subrogation_dim_id,
	FIL_remove_ceded_kind_code.claim_trans_date_id,
	FIL_remove_ceded_kind_code.claim_trans_reprocess_date_id,
	FIL_remove_ceded_kind_code.claim_loss_date_id,
	FIL_remove_ceded_kind_code.claim_discovery_date_id,
	FIL_remove_ceded_kind_code.claim_scripted_date_id,
	FIL_remove_ceded_kind_code.source_claim_rpted_date_id,
	FIL_remove_ceded_kind_code.claim_rpted_date_id,
	FIL_remove_ceded_kind_code.claim_open_date_id,
	FIL_remove_ceded_kind_code.claim_close_date_id,
	FIL_remove_ceded_kind_code.claim_reopen_date_id,
	FIL_remove_ceded_kind_code.claim_closed_after_reopen_date_id,
	FIL_remove_ceded_kind_code.claim_notice_only_date_id,
	FIL_remove_ceded_kind_code.claim_cat_start_date_id,
	FIL_remove_ceded_kind_code.claim_cat_end_date_id,
	FIL_remove_ceded_kind_code.claim_rep_assigned_date_id,
	FIL_remove_ceded_kind_code.claim_rep_unassigned_date_id,
	FIL_remove_ceded_kind_code.pol_eff_date_id,
	FIL_remove_ceded_kind_code.pol_exp_date_id,
	FIL_remove_ceded_kind_code.claim_subrogation_referred_to_subrogation_date_id,
	FIL_remove_ceded_kind_code.claim_subrogation_pay_start_date_id,
	FIL_remove_ceded_kind_code.claim_subrogation_closure_date_id,
	FIL_remove_ceded_kind_code.acct_entered_date_id,
	FIL_remove_ceded_kind_code.trans_amt,
	FIL_remove_ceded_kind_code.trans_hist_amt,
	FIL_remove_ceded_kind_code.tax_id,
	FIL_remove_ceded_kind_code.direct_loss_paid_excluding_recoveries,
	FIL_remove_ceded_kind_code.direct_loss_outstanding_excluding_recoveries,
	FIL_remove_ceded_kind_code.direct_loss_incurred_excluding_recoveries,
	FIL_remove_ceded_kind_code.direct_alae_paid_excluding_recoveries,
	FIL_remove_ceded_kind_code.direct_alae_outstanding_excluding_recoveries,
	FIL_remove_ceded_kind_code.direct_alae_incurred_excluding_recoveries,
	FIL_remove_ceded_kind_code.direct_loss_paid_including_recoveries,
	FIL_remove_ceded_kind_code.direct_loss_outstanding_including_recoveries,
	FIL_remove_ceded_kind_code.direct_loss_incurred_including_recoveries,
	FIL_remove_ceded_kind_code.direct_alae_paid_including_recoveries,
	FIL_remove_ceded_kind_code.direct_alae_outstanding_including_recoveries,
	FIL_remove_ceded_kind_code.direct_alae_incurred_including_recoveries,
	FIL_remove_ceded_kind_code.direct_subrogation_paid,
	FIL_remove_ceded_kind_code.direct_subrogation_outstanding,
	FIL_remove_ceded_kind_code.direct_subrogation_incurred,
	FIL_remove_ceded_kind_code.direct_salvage_paid,
	FIL_remove_ceded_kind_code.direct_salvage_outstanding,
	FIL_remove_ceded_kind_code.direct_salvage_incurred,
	FIL_remove_ceded_kind_code.direct_other_recovery_loss_paid,
	FIL_remove_ceded_kind_code.direct_other_recovery_loss_outstanding,
	FIL_remove_ceded_kind_code.direct_other_recovery_loss_incurred,
	FIL_remove_ceded_kind_code.direct_other_recovery_alae_paid,
	FIL_remove_ceded_kind_code.direct_other_recovery_alae_outstanding,
	FIL_remove_ceded_kind_code.direct_other_recovery_alae_incurred,
	FIL_remove_ceded_kind_code.total_direct_loss_recovery_paid,
	FIL_remove_ceded_kind_code.total_direct_loss_recovery_outstanding,
	FIL_remove_ceded_kind_code.total_direct_loss_recovery_incurred,
	FIL_remove_ceded_kind_code.direct_other_recovery_paid,
	FIL_remove_ceded_kind_code.direct_other_recovery_outstanding,
	FIL_remove_ceded_kind_code.direct_other_recovery_incurred,
	FIL_remove_ceded_kind_code.ceded_loss_paid,
	FIL_remove_ceded_kind_code.ceded_loss_outstanding,
	FIL_remove_ceded_kind_code.ceded_loss_incurred,
	FIL_remove_ceded_kind_code.ceded_alae_paid,
	FIL_remove_ceded_kind_code.ceded_alae_outstanding,
	FIL_remove_ceded_kind_code.ceded_alae_incurred,
	FIL_remove_ceded_kind_code.ceded_salvage_paid,
	FIL_remove_ceded_kind_code.ceded_subrogation_paid,
	FIL_remove_ceded_kind_code.ceded_other_recovery_loss_paid,
	FIL_remove_ceded_kind_code.ceded_other_recovery_alae_paid,
	FIL_remove_ceded_kind_code.total_ceded_loss_recovery_paid,
	FIL_remove_ceded_kind_code.net_loss_paid,
	FIL_remove_ceded_kind_code.net_loss_outstanding,
	FIL_remove_ceded_kind_code.net_loss_incurred,
	FIL_remove_ceded_kind_code.net_alae_paid,
	FIL_remove_ceded_kind_code.net_alae_outstanding,
	FIL_remove_ceded_kind_code.net_alae_incurred,
	FIL_remove_ceded_kind_code.asl_dim_id,
	FIL_remove_ceded_kind_code.asl_prdct_code_dim_id,
	FIL_remove_ceded_kind_code.loss_master_dim_id,
	FIL_remove_ceded_kind_code.strtgc_bus_dvsn_dim_id,
	FIL_remove_ceded_kind_code.prdct_code_dim_id,
	FIL_remove_ceded_kind_code.asl_dim_id_IN,
	FIL_remove_ceded_kind_code.asl_prdct_code_dim_id_IN,
	FIL_remove_ceded_kind_code.loss_master_dim_id_IN,
	FIL_remove_ceded_kind_code.kind_code_mine_sub_IN,
	FIL_remove_ceded_kind_code.facultative_ind_mine_sub_IN,
	FIL_remove_ceded_kind_code.reins_co_number_mine_sub_IN,
	FIL_remove_ceded_kind_code.major_peril_IN,
	-1 AS DEFAULT_ID,
	-50 AS DEFAULT_AUDIT,
	0 AS DEFAULT_VALUE,
	FIL_remove_ceded_kind_code.financial_type_code_IN,
	FIL_remove_ceded_kind_code.trans_code_IN,
	FIL_remove_ceded_kind_code.trans_date_IN,
	FIL_remove_ceded_kind_code.trans_ctgry_code_IN,
	FIL_remove_ceded_kind_code.trans_amt_IN,
	FIL_remove_ceded_kind_code.trans_hist_amt_IN,
	FIL_remove_ceded_kind_code.source_sys_id_IN,
	FIL_remove_ceded_kind_code.claimant_cov_det_ak_id_IN,
	FIL_remove_ceded_kind_code.prdct_code_dim_id_IN,
	FIL_remove_ceded_kind_code.ClaimReserveDimId,
	FIL_remove_ceded_kind_code.ClaimRepresentativeDimFeatureClaimRepresentativeId,
	FIL_remove_ceded_kind_code.FeatureRepresentativeAssignedDateId,
	FIL_remove_ceded_kind_code.InsuranceReferenceDimId,
	FIL_remove_ceded_kind_code.AgencyDimId,
	FIL_remove_ceded_kind_code.SalesDivisionDimId,
	FIL_remove_ceded_kind_code.InsuranceReferenceCoverageDimId,
	FIL_remove_ceded_kind_code.CoverageDetailDimId
	FROM FIL_remove_ceded_kind_code
	LEFT JOIN LKP_claim_transaction_type_dim_Get_Ceded_Record
	ON LKP_claim_transaction_type_dim_Get_Ceded_Record.crrnt_snpsht_flag = FIL_remove_ceded_kind_code.lkp_crrnt_snpsht_flag AND LKP_claim_transaction_type_dim_Get_Ceded_Record.trans_ctgry_code = FIL_remove_ceded_kind_code.lkp_trans_ctgry_code AND LKP_claim_transaction_type_dim_Get_Ceded_Record.trans_code = FIL_remove_ceded_kind_code.lkp_trans_code AND LKP_claim_transaction_type_dim_Get_Ceded_Record.s3p_trans_code = FIL_remove_ceded_kind_code.lkp_s3p_trans_code AND LKP_claim_transaction_type_dim_Get_Ceded_Record.pms_trans_code = FIL_remove_ceded_kind_code.lkp_pms_trans_code AND LKP_claim_transaction_type_dim_Get_Ceded_Record.trans_base_type_code = FIL_remove_ceded_kind_code.lkp_trans_base_type_code AND LKP_claim_transaction_type_dim_Get_Ceded_Record.trans_rsn = FIL_remove_ceded_kind_code.lkp_trans_rsn AND LKP_claim_transaction_type_dim_Get_Ceded_Record.type_disability = FIL_remove_ceded_kind_code.lkp_type_disability AND LKP_claim_transaction_type_dim_Get_Ceded_Record.offset_onset_ind = FIL_remove_ceded_kind_code.lkp_offset_onset_ind AND LKP_claim_transaction_type_dim_Get_Ceded_Record.trans_kind_code = FIL_remove_ceded_kind_code.kind_code_mine_sub_IN
),
mplt_reinsurance_claim_loss_transaction_fact_calculations AS (WITH
	LKP_claim_reinsurance_transaction AS (
		SELECT
		claimant_cov_det_ak_id,
		claim_reins_financial_type_code,
		claim_reins_trans_date
		FROM (
			SELECT claim_reinsurance_transaction.claimant_cov_det_ak_id as claimant_cov_det_ak_id, claim_reinsurance_transaction.claim_reins_financial_type_code as claim_reins_financial_type_code, claim_reinsurance_transaction.claim_reins_trans_date as claim_reins_trans_date 
			 FROM claim_reinsurance_transaction
			where claim_reins_trans_code= 23
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,claim_reins_financial_type_code,claim_reins_trans_date ORDER BY claimant_cov_det_ak_id) = 1
	),
	INPT_reinsurance_claim_loss_trans_fact_calculations AS (
		
	),
	EXP_reinsurance_claim_loss_transaction_fact_calculations_input AS (
		SELECT
		claim_reins_fin_type_code,
		claim_reins_trans_code,
		claim_reins_trans_amt,
		claim_reins_trans_hist_amt,
		claimant_cov_det_ak_id,
		claim_reins_trans_date,
		source_sys_id,
		trans_ctgry_code
		FROM INPT_reinsurance_claim_loss_trans_fact_calculations
	),
	EXP_calculations AS (
		SELECT
		claim_reins_fin_type_code AS claim_reins_financial_type_code,
		claim_reins_trans_code,
		claim_reins_trans_amt,
		claim_reins_trans_hist_amt,
		claimant_cov_det_ak_id,
		claim_reins_trans_date,
		source_sys_id,
		trans_ctgry_code,
		-- *INF*: IIF(claim_reins_financial_type_code = 'D', DECODE(claim_reins_trans_code, '20', claim_reins_trans_amt, '21',claim_reins_trans_amt, '22', claim_reins_trans_amt, '23',claim_reins_trans_amt, '24', claim_reins_trans_amt, '28', claim_reins_trans_amt, '29', claim_reins_trans_amt, '41', 0, '42', 0, '43', 0, '65',0, '66', 0, '90', 0, '91', 0, '92', 0, 0),0)
		IFF(claim_reins_financial_type_code = 'D',
			DECODE(claim_reins_trans_code,
			'20', claim_reins_trans_amt,
			'21', claim_reins_trans_amt,
			'22', claim_reins_trans_amt,
			'23', claim_reins_trans_amt,
			'24', claim_reins_trans_amt,
			'28', claim_reins_trans_amt,
			'29', claim_reins_trans_amt,
			'41', 0,
			'42', 0,
			'43', 0,
			'65', 0,
			'66', 0,
			'90', 0,
			'91', 0,
			'92', 0,
			0
			),
			0
		) AS var_ceded_loss_paid,
		-- *INF*: IIF(claim_reins_financial_type_code = 'D', DECODE(claim_reins_trans_code, '20', 0, '21', claim_reins_trans_amt * -1, '22', (claim_reins_trans_amt - claim_reins_trans_hist_amt ) * -1, '23', 0, '24', 0, '28', claim_reins_trans_amt * -1, '29', 0, '41', claim_reins_trans_hist_amt, '42', claim_reins_trans_hist_amt, '43', 0, '65', claim_reins_trans_hist_amt, '66', claim_reins_trans_hist_amt, '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'D',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'D',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'D',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), 0))
		IFF(claim_reins_financial_type_code = 'D',
			DECODE(claim_reins_trans_code,
			'20', 0,
			'21', claim_reins_trans_amt * - 1,
			'22', ( claim_reins_trans_amt - claim_reins_trans_hist_amt 
				) * - 1,
			'23', 0,
			'24', 0,
			'28', claim_reins_trans_amt * - 1,
			'29', 0,
			'41', claim_reins_trans_hist_amt,
			'42', claim_reins_trans_hist_amt,
			'43', 0,
			'65', claim_reins_trans_hist_amt,
			'66', claim_reins_trans_hist_amt,
			'90', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
					0,
					claim_reins_trans_hist_amt
				),
			'91', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
					0,
					claim_reins_trans_hist_amt
				),
			'92', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
					0,
					claim_reins_trans_hist_amt
				),
			0
			)
		) AS var_ceded_loss_outstanding,
		-- *INF*: IIF(claim_reins_financial_type_code = 'D', DECODE(claim_reins_trans_code, '20', claim_reins_trans_amt, '21', 0, '22', claim_reins_trans_hist_amt, '23', claim_reins_trans_amt, '24', claim_reins_trans_amt, '28',0, '29', claim_reins_trans_amt, '41', claim_reins_trans_hist_amt, '42', claim_reins_trans_hist_amt, '43', 0, '65', claim_reins_trans_hist_amt, '66', claim_reins_trans_hist_amt, '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'D',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'D',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'D',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), 1111))
		IFF(claim_reins_financial_type_code = 'D',
			DECODE(claim_reins_trans_code,
			'20', claim_reins_trans_amt,
			'21', 0,
			'22', claim_reins_trans_hist_amt,
			'23', claim_reins_trans_amt,
			'24', claim_reins_trans_amt,
			'28', 0,
			'29', claim_reins_trans_amt,
			'41', claim_reins_trans_hist_amt,
			'42', claim_reins_trans_hist_amt,
			'43', 0,
			'65', claim_reins_trans_hist_amt,
			'66', claim_reins_trans_hist_amt,
			'90', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
					0,
					claim_reins_trans_hist_amt
				),
			'91', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
					0,
					claim_reins_trans_hist_amt
				),
			'92', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
					0,
					claim_reins_trans_hist_amt
				),
			1111
			)
		) AS var_ceded_loss_incurred,
		-- *INF*: IIF(claim_reins_financial_type_code = 'E', DECODE(claim_reins_trans_code, '20', claim_reins_trans_amt, '21',claim_reins_trans_amt, '22', claim_reins_trans_amt, '23',claim_reins_trans_amt, '24', claim_reins_trans_amt, '28', claim_reins_trans_amt, '29', claim_reins_trans_amt, '40',0, '41', 0, '42', 0, '43', 0, '65',0, '66', 0, '90', 0, '91', 0, '92', 0, 0),0)
		IFF(claim_reins_financial_type_code = 'E',
			DECODE(claim_reins_trans_code,
			'20', claim_reins_trans_amt,
			'21', claim_reins_trans_amt,
			'22', claim_reins_trans_amt,
			'23', claim_reins_trans_amt,
			'24', claim_reins_trans_amt,
			'28', claim_reins_trans_amt,
			'29', claim_reins_trans_amt,
			'40', 0,
			'41', 0,
			'42', 0,
			'43', 0,
			'65', 0,
			'66', 0,
			'90', 0,
			'91', 0,
			'92', 0,
			0
			),
			0
		) AS var_ceded_alae_paid,
		-- *INF*: IIF(claim_reins_financial_type_code = 'E' and source_sys_id = 'EXCEED', DECODE(claim_reins_trans_code, '20', 0, '21', claim_reins_trans_amt * -1, '22', (claim_reins_trans_amt - claim_reins_trans_hist_amt ) * -1, '23', 0, '24', 0, '28', claim_reins_trans_amt * -1, '29', 0, '40',claim_reins_trans_hist_amt, '41', claim_reins_trans_hist_amt, '42', claim_reins_trans_hist_amt, '43', 0, '65', claim_reins_trans_hist_amt, '66', claim_reins_trans_hist_amt, '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'E',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'E',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'E',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), 0), 0)
		IFF(claim_reins_financial_type_code = 'E' 
			AND source_sys_id = 'EXCEED',
			DECODE(claim_reins_trans_code,
			'20', 0,
			'21', claim_reins_trans_amt * - 1,
			'22', ( claim_reins_trans_amt - claim_reins_trans_hist_amt 
				) * - 1,
			'23', 0,
			'24', 0,
			'28', claim_reins_trans_amt * - 1,
			'29', 0,
			'40', claim_reins_trans_hist_amt,
			'41', claim_reins_trans_hist_amt,
			'42', claim_reins_trans_hist_amt,
			'43', 0,
			'65', claim_reins_trans_hist_amt,
			'66', claim_reins_trans_hist_amt,
			'90', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
					0,
					claim_reins_trans_hist_amt
				),
			'91', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
					0,
					claim_reins_trans_hist_amt
				),
			'92', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
					0,
					claim_reins_trans_hist_amt
				),
			0
			),
			0
		) AS var_ceded_alae_outstanding,
		-- *INF*: IIF(claim_reins_financial_type_code = 'E' and source_sys_id = 'EXCEED', DECODE(claim_reins_trans_code, '20', claim_reins_trans_amt, '21', 0, '22', claim_reins_trans_hist_amt, '23', claim_reins_trans_amt, '24', claim_reins_trans_amt, '28',claim_reins_trans_amt, '29', claim_reins_trans_amt, '40', claim_reins_trans_hist_amt, '41', claim_reins_trans_hist_amt, '42', claim_reins_trans_hist_amt, '43', 0, '65', claim_reins_trans_hist_amt, '66', claim_reins_trans_hist_amt, '90', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'E',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), '91', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'E',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), '92', IIF(NOT ISNULL(:LKP.LKP_CLAIM_REINSURANCE_TRANSACTION(claimant_cov_det_ak_id, 'E',claim_reins_trans_date)), 0, claim_reins_trans_hist_amt), 0), 0)
		IFF(claim_reins_financial_type_code = 'E' 
			AND source_sys_id = 'EXCEED',
			DECODE(claim_reins_trans_code,
			'20', claim_reins_trans_amt,
			'21', 0,
			'22', claim_reins_trans_hist_amt,
			'23', claim_reins_trans_amt,
			'24', claim_reins_trans_amt,
			'28', claim_reins_trans_amt,
			'29', claim_reins_trans_amt,
			'40', claim_reins_trans_hist_amt,
			'41', claim_reins_trans_hist_amt,
			'42', claim_reins_trans_hist_amt,
			'43', 0,
			'65', claim_reins_trans_hist_amt,
			'66', claim_reins_trans_hist_amt,
			'90', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
					0,
					claim_reins_trans_hist_amt
				),
			'91', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
					0,
					claim_reins_trans_hist_amt
				),
			'92', IFF(LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date.claimant_cov_det_ak_id IS NOT NULL,
					0,
					claim_reins_trans_hist_amt
				),
			0
			),
			0
		) AS var_ceded_alae_incurred,
		-- *INF*: IIF(claim_reins_financial_type_code = 'S', DECODE(claim_reins_trans_code, '25', claim_reins_trans_amt * -1, '31', claim_reins_trans_amt * -1, '32', claim_reins_trans_amt * -1, '33', claim_reins_trans_amt * -1, '34', claim_reins_trans_amt * -1, '38', claim_reins_trans_amt * -1, '39', claim_reins_trans_amt * -1 , 0),0)
		IFF(claim_reins_financial_type_code = 'S',
			DECODE(claim_reins_trans_code,
			'25', claim_reins_trans_amt * - 1,
			'31', claim_reins_trans_amt * - 1,
			'32', claim_reins_trans_amt * - 1,
			'33', claim_reins_trans_amt * - 1,
			'34', claim_reins_trans_amt * - 1,
			'38', claim_reins_trans_amt * - 1,
			'39', claim_reins_trans_amt * - 1,
			0
			),
			0
		) AS var_ceded_salvage_paid,
		-- *INF*: IIF(claim_reins_financial_type_code = 'B', DECODE(claim_reins_trans_code, '25', claim_reins_trans_amt * -1, '31', claim_reins_trans_amt * -1, '32', claim_reins_trans_amt * -1, '33', claim_reins_trans_amt * -1, '34', claim_reins_trans_amt * -1, '38', claim_reins_trans_amt * -1, '39', claim_reins_trans_amt * -1 , 0),0)
		IFF(claim_reins_financial_type_code = 'B',
			DECODE(claim_reins_trans_code,
			'25', claim_reins_trans_amt * - 1,
			'31', claim_reins_trans_amt * - 1,
			'32', claim_reins_trans_amt * - 1,
			'33', claim_reins_trans_amt * - 1,
			'34', claim_reins_trans_amt * - 1,
			'38', claim_reins_trans_amt * - 1,
			'39', claim_reins_trans_amt * - 1,
			0
			),
			0
		) AS var_ceded_subrogation_paid,
		-- *INF*: IIF(claim_reins_financial_type_code = 'R' and trans_ctgry_code<>'EX', DECODE(claim_reins_trans_code, '25', claim_reins_trans_amt * -1, '31', claim_reins_trans_amt * -1, '32', claim_reins_trans_amt * -1, '33', claim_reins_trans_amt * -1, '34', claim_reins_trans_amt * -1, '38', claim_reins_trans_amt * -1, '39', claim_reins_trans_amt * -1 , 0),0)
		IFF(claim_reins_financial_type_code = 'R' 
			AND trans_ctgry_code <> 'EX',
			DECODE(claim_reins_trans_code,
			'25', claim_reins_trans_amt * - 1,
			'31', claim_reins_trans_amt * - 1,
			'32', claim_reins_trans_amt * - 1,
			'33', claim_reins_trans_amt * - 1,
			'34', claim_reins_trans_amt * - 1,
			'38', claim_reins_trans_amt * - 1,
			'39', claim_reins_trans_amt * - 1,
			0
			),
			0
		) AS var_ceded_other_recovery_loss_paid,
		-- *INF*: IIF(claim_reins_financial_type_code = 'R' and trans_ctgry_code = 'EX', DECODE(claim_reins_trans_code, '25', claim_reins_trans_amt * -1, '31', claim_reins_trans_amt * -1, '32', claim_reins_trans_amt * -1, '33', claim_reins_trans_amt * -1, '34', claim_reins_trans_amt * -1, '38', claim_reins_trans_amt * -1, '39', claim_reins_trans_amt * -1 , 0),0)
		IFF(claim_reins_financial_type_code = 'R' 
			AND trans_ctgry_code = 'EX',
			DECODE(claim_reins_trans_code,
			'25', claim_reins_trans_amt * - 1,
			'31', claim_reins_trans_amt * - 1,
			'32', claim_reins_trans_amt * - 1,
			'33', claim_reins_trans_amt * - 1,
			'34', claim_reins_trans_amt * - 1,
			'38', claim_reins_trans_amt * - 1,
			'39', claim_reins_trans_amt * - 1,
			0
			),
			0
		) AS var_ceded_other_recovery_alae_paid,
		-- *INF*: round(var_ceded_salvage_paid+var_ceded_subrogation_paid+var_ceded_other_recovery_loss_paid,2)
		round(var_ceded_salvage_paid + var_ceded_subrogation_paid + var_ceded_other_recovery_loss_paid, 2
		) AS var_total_ceded_loss_recovery_paid,
		var_ceded_loss_paid AS ceded_loss_paid,
		var_ceded_loss_outstanding AS ceded_loss_outstanding,
		var_ceded_loss_incurred AS ceded_loss_incurred,
		var_ceded_alae_paid AS ceded_alae_paid,
		var_ceded_alae_outstanding AS ceded_alae_outstanding,
		var_ceded_alae_incurred AS ceded_alae_incurred,
		var_ceded_salvage_paid AS ceded_salvage_paid,
		var_ceded_subrogation_paid AS ceded_subrogation_paid,
		var_ceded_other_recovery_loss_paid AS ceded_other_recovery_loss_paid,
		var_ceded_other_recovery_alae_paid AS ceded_other_recovery_alae_paid,
		var_total_ceded_loss_recovery_paid AS total_ceded_loss_recovery_paid,
		var_ceded_loss_paid *-1 AS net_loss_paid,
		var_ceded_loss_outstanding*-1 AS net_loss_outstanding,
		var_ceded_loss_incurred * -1 AS net_loss_incurred,
		var_ceded_alae_paid*-1 AS net_alae_paid,
		var_ceded_alae_outstanding*-1 AS net_alae_outstanding,
		var_ceded_alae_incurred*-1 AS net_alae_incurred
		FROM EXP_reinsurance_claim_loss_transaction_fact_calculations_input
		LEFT JOIN LKP_CLAIM_REINSURANCE_TRANSACTION LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date
		ON LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date.claimant_cov_det_ak_id = claimant_cov_det_ak_id
		AND LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date.claim_reins_financial_type_code = 'D'
		AND LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_D_claim_reins_trans_date.claim_reins_trans_date = claim_reins_trans_date
	
		LEFT JOIN LKP_CLAIM_REINSURANCE_TRANSACTION LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date
		ON LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date.claimant_cov_det_ak_id = claimant_cov_det_ak_id
		AND LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date.claim_reins_financial_type_code = 'E'
		AND LKP_CLAIM_REINSURANCE_TRANSACTION_claimant_cov_det_ak_id_E_claim_reins_trans_date.claim_reins_trans_date = claim_reins_trans_date
	
	),
	OUT_reinsurance_claim_loss_transaction_fact_calculations AS (
		SELECT
		claim_reins_financial_type_code, 
		claim_reins_trans_code, 
		claim_reins_trans_amt, 
		claim_reins_trans_hist_amt, 
		claimant_cov_det_ak_id, 
		claim_reins_trans_date, 
		source_sys_id, 
		trans_ctgry_code, 
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
		net_alae_incurred
		FROM EXP_calculations
	),
),
FIL_remove_if_record_exists AS (
	SELECT
	EXP_set_ceded_transaction_type_id.err_flag, 
	EXP_set_ceded_transaction_type_id.audit_id, 
	EXP_set_ceded_transaction_type_id.edw_claim_trans_pk_id, 
	EXP_set_ceded_transaction_type_id.edw_claim_reins_trans_pk_id, 
	EXP_set_ceded_transaction_type_id.claim_occurrence_dim_id, 
	EXP_set_ceded_transaction_type_id.claim_occurrence_dim_hist_id, 
	EXP_set_ceded_transaction_type_id.claimant_dim_id, 
	EXP_set_ceded_transaction_type_id.claimant_dim_hist_id, 
	EXP_set_ceded_transaction_type_id.claimant_cov_dim_id, 
	EXP_set_ceded_transaction_type_id.claimant_cov_dim_hist_id, 
	EXP_set_ceded_transaction_type_id.cov_dim_id, 
	EXP_set_ceded_transaction_type_id.cov_dim_hist_id, 
	EXP_set_ceded_transaction_type_id.lkp_claim_trans_type_dim_id, 
	EXP_set_ceded_transaction_type_id.claim_financial_type_dim_id, 
	EXP_set_ceded_transaction_type_id.reins_cov_dim_id, 
	EXP_set_ceded_transaction_type_id.reins_cov_dim_hist_id, 
	EXP_set_ceded_transaction_type_id.claim_rep_dim_prim_claim_rep_id, 
	EXP_set_ceded_transaction_type_id.claim_rep_dim_prim_claim_rep_hist_id, 
	EXP_set_ceded_transaction_type_id.claim_rep_dim_examiner_id, 
	EXP_set_ceded_transaction_type_id.claim_rep_dim_examiner_hist_id, 
	EXP_set_ceded_transaction_type_id.claim_rep_dim_prim_litigation_handler_id, 
	EXP_set_ceded_transaction_type_id.claim_rep_dim_prim_litigation_handler_hist_id, 
	EXP_set_ceded_transaction_type_id.claim_rep_dim_trans_entry_oper_id, 
	EXP_set_ceded_transaction_type_id.claim_rep_dim_trans_entry_oper_hist_id, 
	EXP_set_ceded_transaction_type_id.claim_rep_dim_claim_created_by_id, 
	EXP_set_ceded_transaction_type_id.pol_dim_id, 
	EXP_set_ceded_transaction_type_id.pol_dim_hist_id, 
	EXP_set_ceded_transaction_type_id.agency_dim_id, 
	EXP_set_ceded_transaction_type_id.agency_dim_hist_id, 
	EXP_set_ceded_transaction_type_id.claim_pay_dim_id, 
	EXP_set_ceded_transaction_type_id.claim_pay_dim_hist_id, 
	EXP_set_ceded_transaction_type_id.claim_pay_ctgry_type_dim_id, 
	EXP_set_ceded_transaction_type_id.claim_pay_ctgry_type_dim_hist_id, 
	EXP_set_ceded_transaction_type_id.claim_case_dim_id, 
	EXP_set_ceded_transaction_type_id.claim_case_dim_hist_id, 
	EXP_set_ceded_transaction_type_id.contract_cust_dim_id, 
	EXP_set_ceded_transaction_type_id.contract_cust_dim_hist_id, 
	EXP_set_ceded_transaction_type_id.claim_master_1099_list_dim_id, 
	EXP_set_ceded_transaction_type_id.claim_subrogation_dim_id, 
	EXP_set_ceded_transaction_type_id.claim_trans_date_id, 
	EXP_set_ceded_transaction_type_id.claim_trans_reprocess_date_id, 
	EXP_set_ceded_transaction_type_id.claim_loss_date_id, 
	EXP_set_ceded_transaction_type_id.claim_discovery_date_id, 
	EXP_set_ceded_transaction_type_id.claim_scripted_date_id, 
	EXP_set_ceded_transaction_type_id.source_claim_rpted_date_id, 
	EXP_set_ceded_transaction_type_id.claim_rpted_date_id, 
	EXP_set_ceded_transaction_type_id.claim_open_date_id, 
	EXP_set_ceded_transaction_type_id.claim_close_date_id, 
	EXP_set_ceded_transaction_type_id.claim_reopen_date_id, 
	EXP_set_ceded_transaction_type_id.claim_closed_after_reopen_date_id, 
	EXP_set_ceded_transaction_type_id.claim_notice_only_date_id, 
	EXP_set_ceded_transaction_type_id.claim_cat_start_date_id, 
	EXP_set_ceded_transaction_type_id.claim_cat_end_date_id, 
	EXP_set_ceded_transaction_type_id.claim_rep_assigned_date_id, 
	EXP_set_ceded_transaction_type_id.claim_rep_unassigned_date_id, 
	EXP_set_ceded_transaction_type_id.pol_eff_date_id, 
	EXP_set_ceded_transaction_type_id.pol_exp_date_id, 
	EXP_set_ceded_transaction_type_id.claim_subrogation_referred_to_subrogation_date_id, 
	EXP_set_ceded_transaction_type_id.claim_subrogation_pay_start_date_id, 
	EXP_set_ceded_transaction_type_id.claim_subrogation_closure_date_id, 
	EXP_set_ceded_transaction_type_id.acct_entered_date_id, 
	EXP_set_ceded_transaction_type_id.trans_amt, 
	EXP_set_ceded_transaction_type_id.trans_hist_amt, 
	EXP_set_ceded_transaction_type_id.tax_id, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_loss_paid_excluding_recoveries, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_loss_outstanding_excluding_recoveries, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_loss_incurred_excluding_recoveries, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_alae_paid_excluding_recoveries, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_alae_outstanding_excluding_recoveries, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_alae_incurred_excluding_recoveries, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_loss_paid_including_recoveries, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_loss_outstanding_including_recoveries, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_loss_incurred_including_recoveries, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_alae_paid_including_recoveries, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_alae_outstanding_including_recoveries, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_alae_incurred_including_recoveries, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_subrogation_paid, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_subrogation_outstanding, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_subrogation_incurred, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_salvage_paid, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_salvage_outstanding, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_salvage_incurred, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_other_recovery_loss_paid, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_other_recovery_loss_outstanding, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_other_recovery_loss_incurred, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_other_recovery_alae_paid, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_other_recovery_alae_outstanding, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_other_recovery_alae_incurred, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS total_direct_loss_recovery_paid, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS total_direct_loss_recovery_outstanding, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS total_direct_loss_recovery_incurred, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_other_recovery_paid, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_other_recovery_outstanding, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE AS direct_other_recovery_incurred, 
	mplt_reinsurance_claim_loss_transaction_fact_calculations.ceded_loss_paid, 
	mplt_reinsurance_claim_loss_transaction_fact_calculations.ceded_loss_outstanding, 
	mplt_reinsurance_claim_loss_transaction_fact_calculations.ceded_loss_incurred, 
	mplt_reinsurance_claim_loss_transaction_fact_calculations.ceded_alae_paid, 
	mplt_reinsurance_claim_loss_transaction_fact_calculations.ceded_alae_outstanding, 
	mplt_reinsurance_claim_loss_transaction_fact_calculations.ceded_alae_incurred, 
	mplt_reinsurance_claim_loss_transaction_fact_calculations.ceded_salvage_paid, 
	mplt_reinsurance_claim_loss_transaction_fact_calculations.ceded_subrogation_paid, 
	mplt_reinsurance_claim_loss_transaction_fact_calculations.ceded_other_recovery_loss_paid, 
	mplt_reinsurance_claim_loss_transaction_fact_calculations.ceded_other_recovery_alae_paid, 
	mplt_reinsurance_claim_loss_transaction_fact_calculations.total_ceded_loss_recovery_paid, 
	mplt_reinsurance_claim_loss_transaction_fact_calculations.net_loss_paid, 
	mplt_reinsurance_claim_loss_transaction_fact_calculations.net_loss_outstanding, 
	mplt_reinsurance_claim_loss_transaction_fact_calculations.net_loss_incurred, 
	mplt_reinsurance_claim_loss_transaction_fact_calculations.net_alae_paid, 
	mplt_reinsurance_claim_loss_transaction_fact_calculations.net_alae_outstanding, 
	mplt_reinsurance_claim_loss_transaction_fact_calculations.net_alae_incurred, 
	EXP_set_ceded_transaction_type_id.asl_dim_id, 
	EXP_set_ceded_transaction_type_id.asl_prdct_code_dim_id, 
	EXP_set_ceded_transaction_type_id.loss_master_dim_id, 
	EXP_set_ceded_transaction_type_id.strtgc_bus_dvsn_dim_id, 
	EXP_set_ceded_transaction_type_id.prdct_code_dim_id, 
	EXP_set_ceded_transaction_type_id.asl_dim_id_IN, 
	EXP_set_ceded_transaction_type_id.asl_prdct_code_dim_id_IN, 
	EXP_set_ceded_transaction_type_id.loss_master_dim_id_IN, 
	EXP_set_ceded_transaction_type_id.kind_code_mine_sub_IN, 
	EXP_set_ceded_transaction_type_id.facultative_ind_mine_sub_IN, 
	EXP_set_ceded_transaction_type_id.reins_co_number_mine_sub_IN, 
	EXP_set_ceded_transaction_type_id.major_peril_IN, 
	EXP_set_ceded_transaction_type_id.DEFAULT_ID, 
	EXP_set_ceded_transaction_type_id.DEFAULT_AUDIT, 
	EXP_set_ceded_transaction_type_id.DEFAULT_VALUE, 
	EXP_set_ceded_transaction_type_id.prdct_code_dim_id_IN, 
	EXP_set_ceded_transaction_type_id.ClaimReserveDimId, 
	EXP_set_ceded_transaction_type_id.ClaimRepresentativeDimFeatureClaimRepresentativeId, 
	EXP_set_ceded_transaction_type_id.FeatureRepresentativeAssignedDateId, 
	EXP_set_ceded_transaction_type_id.InsuranceReferenceDimId, 
	EXP_set_ceded_transaction_type_id.AgencyDimId, 
	EXP_set_ceded_transaction_type_id.SalesDivisionDimId, 
	EXP_set_ceded_transaction_type_id.InsuranceReferenceCoverageDimId, 
	EXP_set_ceded_transaction_type_id.CoverageDetailDimId
	FROM EXP_set_ceded_transaction_type_id
	 -- Manually join with mplt_reinsurance_claim_loss_transaction_fact_calculations
	WHERE TRUE


---isnull(claim_loss_trans_fact_id)
),
LKP_reinsurance_coverage_dim AS (
	SELECT
	reins_cov_dim_id,
	reins_co_num,
	edw_reins_cov_pk_id
	FROM (
		SELECT 
		RCD.reins_cov_dim_id 		as reins_cov_dim_id, 
		RCD.reins_co_num 			as reins_co_num, 
		RCD.edw_reins_cov_pk_id 	as edw_reins_cov_pk_id 
		
		FROM reinsurance_coverage_dim RCD
		where 
		RCD.edw_reins_cov_pk_id  < 0
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY reins_co_num,edw_reins_cov_pk_id ORDER BY reins_cov_dim_id) = 1
),
EXP_mp50_output AS (
	SELECT
	FIL_remove_if_record_exists.err_flag,
	FIL_remove_if_record_exists.audit_id,
	FIL_remove_if_record_exists.edw_claim_trans_pk_id,
	FIL_remove_if_record_exists.edw_claim_reins_trans_pk_id,
	FIL_remove_if_record_exists.claim_occurrence_dim_id,
	FIL_remove_if_record_exists.claim_occurrence_dim_hist_id,
	FIL_remove_if_record_exists.claimant_dim_id,
	FIL_remove_if_record_exists.claimant_dim_hist_id,
	FIL_remove_if_record_exists.claimant_cov_dim_id,
	FIL_remove_if_record_exists.claimant_cov_dim_hist_id,
	FIL_remove_if_record_exists.cov_dim_id,
	FIL_remove_if_record_exists.cov_dim_hist_id,
	FIL_remove_if_record_exists.lkp_claim_trans_type_dim_id,
	FIL_remove_if_record_exists.claim_financial_type_dim_id,
	FIL_remove_if_record_exists.reins_cov_dim_hist_id,
	FIL_remove_if_record_exists.claim_rep_dim_prim_claim_rep_id,
	FIL_remove_if_record_exists.claim_rep_dim_prim_claim_rep_hist_id,
	FIL_remove_if_record_exists.claim_rep_dim_examiner_id,
	FIL_remove_if_record_exists.claim_rep_dim_examiner_hist_id,
	FIL_remove_if_record_exists.claim_rep_dim_prim_litigation_handler_id,
	FIL_remove_if_record_exists.claim_rep_dim_prim_litigation_handler_hist_id,
	FIL_remove_if_record_exists.claim_rep_dim_trans_entry_oper_id,
	FIL_remove_if_record_exists.claim_rep_dim_trans_entry_oper_hist_id,
	FIL_remove_if_record_exists.claim_rep_dim_claim_created_by_id,
	FIL_remove_if_record_exists.pol_dim_id,
	FIL_remove_if_record_exists.pol_dim_hist_id,
	FIL_remove_if_record_exists.agency_dim_id,
	FIL_remove_if_record_exists.agency_dim_hist_id,
	FIL_remove_if_record_exists.claim_pay_dim_id,
	FIL_remove_if_record_exists.claim_pay_dim_hist_id,
	FIL_remove_if_record_exists.claim_pay_ctgry_type_dim_id,
	FIL_remove_if_record_exists.claim_pay_ctgry_type_dim_hist_id,
	FIL_remove_if_record_exists.claim_case_dim_id,
	FIL_remove_if_record_exists.claim_case_dim_hist_id,
	FIL_remove_if_record_exists.contract_cust_dim_id,
	FIL_remove_if_record_exists.contract_cust_dim_hist_id,
	FIL_remove_if_record_exists.claim_master_1099_list_dim_id,
	FIL_remove_if_record_exists.claim_subrogation_dim_id,
	FIL_remove_if_record_exists.claim_trans_date_id,
	FIL_remove_if_record_exists.claim_trans_reprocess_date_id,
	FIL_remove_if_record_exists.claim_loss_date_id,
	FIL_remove_if_record_exists.claim_discovery_date_id,
	FIL_remove_if_record_exists.claim_scripted_date_id,
	FIL_remove_if_record_exists.source_claim_rpted_date_id,
	FIL_remove_if_record_exists.claim_rpted_date_id,
	FIL_remove_if_record_exists.claim_open_date_id,
	FIL_remove_if_record_exists.claim_close_date_id,
	FIL_remove_if_record_exists.claim_reopen_date_id,
	FIL_remove_if_record_exists.claim_closed_after_reopen_date_id,
	FIL_remove_if_record_exists.claim_notice_only_date_id,
	FIL_remove_if_record_exists.claim_cat_start_date_id,
	FIL_remove_if_record_exists.claim_cat_end_date_id,
	FIL_remove_if_record_exists.claim_rep_assigned_date_id,
	FIL_remove_if_record_exists.claim_rep_unassigned_date_id,
	FIL_remove_if_record_exists.pol_eff_date_id,
	FIL_remove_if_record_exists.pol_exp_date_id,
	FIL_remove_if_record_exists.claim_subrogation_referred_to_subrogation_date_id,
	FIL_remove_if_record_exists.claim_subrogation_pay_start_date_id,
	FIL_remove_if_record_exists.claim_subrogation_closure_date_id,
	FIL_remove_if_record_exists.acct_entered_date_id,
	FIL_remove_if_record_exists.trans_amt,
	FIL_remove_if_record_exists.trans_hist_amt,
	FIL_remove_if_record_exists.tax_id,
	FIL_remove_if_record_exists.direct_loss_paid_excluding_recoveries,
	FIL_remove_if_record_exists.direct_loss_outstanding_excluding_recoveries,
	FIL_remove_if_record_exists.direct_loss_incurred_excluding_recoveries,
	FIL_remove_if_record_exists.direct_alae_paid_excluding_recoveries,
	FIL_remove_if_record_exists.direct_alae_outstanding_excluding_recoveries,
	FIL_remove_if_record_exists.direct_alae_incurred_excluding_recoveries,
	FIL_remove_if_record_exists.direct_loss_paid_including_recoveries,
	FIL_remove_if_record_exists.direct_loss_outstanding_including_recoveries,
	FIL_remove_if_record_exists.direct_loss_incurred_including_recoveries,
	FIL_remove_if_record_exists.direct_alae_paid_including_recoveries,
	FIL_remove_if_record_exists.direct_alae_outstanding_including_recoveries,
	FIL_remove_if_record_exists.direct_alae_incurred_including_recoveries,
	FIL_remove_if_record_exists.direct_subrogation_paid,
	FIL_remove_if_record_exists.direct_subrogation_outstanding,
	FIL_remove_if_record_exists.direct_subrogation_incurred,
	FIL_remove_if_record_exists.direct_salvage_paid,
	FIL_remove_if_record_exists.direct_salvage_outstanding,
	FIL_remove_if_record_exists.direct_salvage_incurred,
	FIL_remove_if_record_exists.direct_other_recovery_loss_paid,
	FIL_remove_if_record_exists.direct_other_recovery_loss_outstanding,
	FIL_remove_if_record_exists.direct_other_recovery_loss_incurred,
	FIL_remove_if_record_exists.direct_other_recovery_alae_paid,
	FIL_remove_if_record_exists.direct_other_recovery_alae_outstanding,
	FIL_remove_if_record_exists.direct_other_recovery_alae_incurred,
	FIL_remove_if_record_exists.total_direct_loss_recovery_paid,
	FIL_remove_if_record_exists.total_direct_loss_recovery_outstanding,
	FIL_remove_if_record_exists.total_direct_loss_recovery_incurred,
	FIL_remove_if_record_exists.direct_other_recovery_paid,
	FIL_remove_if_record_exists.direct_other_recovery_outstanding,
	FIL_remove_if_record_exists.direct_other_recovery_incurred,
	FIL_remove_if_record_exists.ceded_loss_paid,
	FIL_remove_if_record_exists.ceded_loss_outstanding,
	FIL_remove_if_record_exists.ceded_loss_incurred,
	FIL_remove_if_record_exists.ceded_alae_paid,
	FIL_remove_if_record_exists.ceded_alae_outstanding,
	FIL_remove_if_record_exists.ceded_alae_incurred,
	FIL_remove_if_record_exists.ceded_salvage_paid,
	FIL_remove_if_record_exists.ceded_subrogation_paid,
	FIL_remove_if_record_exists.ceded_other_recovery_loss_paid,
	FIL_remove_if_record_exists.ceded_other_recovery_alae_paid,
	FIL_remove_if_record_exists.total_ceded_loss_recovery_paid,
	FIL_remove_if_record_exists.net_loss_paid,
	FIL_remove_if_record_exists.net_loss_outstanding,
	FIL_remove_if_record_exists.net_loss_incurred,
	FIL_remove_if_record_exists.net_alae_paid,
	FIL_remove_if_record_exists.net_alae_outstanding,
	FIL_remove_if_record_exists.net_alae_incurred,
	FIL_remove_if_record_exists.asl_dim_id,
	FIL_remove_if_record_exists.asl_prdct_code_dim_id,
	FIL_remove_if_record_exists.loss_master_dim_id,
	FIL_remove_if_record_exists.strtgc_bus_dvsn_dim_id,
	FIL_remove_if_record_exists.prdct_code_dim_id,
	FIL_remove_if_record_exists.asl_dim_id_IN,
	FIL_remove_if_record_exists.asl_prdct_code_dim_id_IN,
	FIL_remove_if_record_exists.loss_master_dim_id_IN,
	FIL_remove_if_record_exists.kind_code_mine_sub_IN,
	FIL_remove_if_record_exists.facultative_ind_mine_sub_IN,
	FIL_remove_if_record_exists.reins_co_number_mine_sub_IN,
	FIL_remove_if_record_exists.major_peril_IN,
	FIL_remove_if_record_exists.DEFAULT_ID,
	FIL_remove_if_record_exists.DEFAULT_AUDIT,
	LKP_reinsurance_coverage_dim.reins_cov_dim_id AS lkp_reins_cov_dim_id,
	FIL_remove_if_record_exists.prdct_code_dim_id_IN,
	FIL_remove_if_record_exists.ClaimReserveDimId,
	FIL_remove_if_record_exists.ClaimRepresentativeDimFeatureClaimRepresentativeId,
	FIL_remove_if_record_exists.FeatureRepresentativeAssignedDateId,
	FIL_remove_if_record_exists.InsuranceReferenceDimId,
	FIL_remove_if_record_exists.AgencyDimId,
	FIL_remove_if_record_exists.SalesDivisionDimId,
	FIL_remove_if_record_exists.InsuranceReferenceCoverageDimId,
	FIL_remove_if_record_exists.CoverageDetailDimId,
	SYSDATE AS ModifiedDate
	FROM FIL_remove_if_record_exists
	LEFT JOIN LKP_reinsurance_coverage_dim
	ON LKP_reinsurance_coverage_dim.reins_co_num = FIL_remove_if_record_exists.reins_co_number_mine_sub_IN AND LKP_reinsurance_coverage_dim.edw_reins_cov_pk_id = FIL_remove_if_record_exists.DEFAULT_ID
),
claim_loss_transaction_fact_mp50_INSERT AS (
	INSERT INTO claim_loss_transaction_fact
	(err_flag, audit_id, edw_claim_trans_pk_id, edw_claim_reins_trans_pk_id, claim_occurrence_dim_id, claim_occurrence_dim_hist_id, claimant_dim_id, claimant_dim_hist_id, claimant_cov_dim_id, claimant_cov_dim_hist_id, cov_dim_id, cov_dim_hist_id, claim_trans_type_dim_id, claim_financial_type_dim_id, reins_cov_dim_id, reins_cov_dim_hist_id, claim_rep_dim_prim_claim_rep_id, claim_rep_dim_prim_claim_rep_hist_id, claim_rep_dim_examiner_id, claim_rep_dim_examiner_hist_id, claim_rep_dim_prim_litigation_handler_id, claim_rep_dim_prim_litigation_handler_hist_id, claim_rep_dim_trans_entry_oper_id, claim_rep_dim_trans_entry_oper_hist_id, claim_rep_dim_claim_created_by_id, pol_dim_id, pol_dim_hist_id, agency_dim_id, agency_dim_hist_id, claim_pay_dim_id, claim_pay_dim_hist_id, claim_pay_ctgry_type_dim_id, claim_pay_ctgry_type_dim_hist_id, claim_case_dim_id, claim_case_dim_hist_id, contract_cust_dim_id, contract_cust_dim_hist_id, claim_master_1099_list_dim_id, claim_subrogation_dim_id, claim_trans_date_id, claim_trans_reprocess_date_id, claim_loss_date_id, claim_discovery_date_id, claim_scripted_date_id, source_claim_rpted_date_id, claim_rpted_date_id, claim_open_date_id, claim_close_date_id, claim_reopen_date_id, claim_closed_after_reopen_date_id, claim_notice_only_date_id, claim_cat_start_date_id, claim_cat_end_date_id, claim_rep_assigned_date_id, claim_rep_unassigned_date_id, pol_eff_date_id, pol_exp_date_id, claim_subrogation_referred_to_subrogation_date_id, claim_subrogation_pay_start_date_id, claim_subrogation_closure_date_id, acct_entered_date_id, trans_amt, trans_hist_amt, tax_id, direct_loss_paid_excluding_recoveries, direct_loss_outstanding_excluding_recoveries, direct_loss_incurred_excluding_recoveries, direct_alae_paid_excluding_recoveries, direct_alae_outstanding_excluding_recoveries, direct_alae_incurred_excluding_recoveries, direct_loss_paid_including_recoveries, direct_loss_outstanding_including_recoveries, direct_loss_incurred_including_recoveries, direct_alae_paid_including_recoveries, direct_alae_outstanding_including_recoveries, direct_alae_incurred_including_recoveries, direct_subrogation_paid, direct_subrogation_outstanding, direct_subrogation_incurred, direct_salvage_paid, direct_salvage_outstanding, direct_salvage_incurred, direct_other_recovery_loss_paid, direct_other_recovery_loss_outstanding, direct_other_recovery_loss_incurred, direct_other_recovery_alae_paid, direct_other_recovery_alae_outstanding, direct_other_recovery_alae_incurred, total_direct_loss_recovery_paid, total_direct_loss_recovery_outstanding, total_direct_loss_recovery_incurred, direct_other_recovery_paid, direct_other_recovery_outstanding, direct_other_recovery_incurred, ceded_loss_paid, ceded_loss_outstanding, ceded_loss_incurred, ceded_alae_paid, ceded_alae_outstanding, ceded_alae_incurred, ceded_salvage_paid, ceded_subrogation_paid, ceded_other_recovery_loss_paid, ceded_other_recovery_alae_paid, total_ceded_loss_recovery_paid, net_loss_paid, net_loss_outstanding, net_loss_incurred, net_alae_paid, net_alae_outstanding, net_alae_incurred, asl_dim_id, asl_prdct_code_dim_id, loss_master_dim_id, strtgc_bus_dvsn_dim_id, prdct_code_dim_id, ClaimReserveDimId, ClaimRepresentativeDimFeatureClaimRepresentativeId, FeatureRepresentativeAssignedDateId, InsuranceReferenceDimId, AgencyDimId, SalesDivisionDimId, InsuranceReferenceCoverageDimId, CoverageDetailDimId, ModifiedDate)
	SELECT 
	ERR_FLAG, 
	DEFAULT_AUDIT AS AUDIT_ID, 
	EDW_CLAIM_TRANS_PK_ID, 
	EDW_CLAIM_REINS_TRANS_PK_ID, 
	CLAIM_OCCURRENCE_DIM_ID, 
	CLAIM_OCCURRENCE_DIM_HIST_ID, 
	CLAIMANT_DIM_ID, 
	CLAIMANT_DIM_HIST_ID, 
	CLAIMANT_COV_DIM_ID, 
	CLAIMANT_COV_DIM_HIST_ID, 
	COV_DIM_ID, 
	COV_DIM_HIST_ID, 
	lkp_claim_trans_type_dim_id AS CLAIM_TRANS_TYPE_DIM_ID, 
	CLAIM_FINANCIAL_TYPE_DIM_ID, 
	lkp_reins_cov_dim_id AS REINS_COV_DIM_ID, 
	DEFAULT_ID AS REINS_COV_DIM_HIST_ID, 
	CLAIM_REP_DIM_PRIM_CLAIM_REP_ID, 
	CLAIM_REP_DIM_PRIM_CLAIM_REP_HIST_ID, 
	CLAIM_REP_DIM_EXAMINER_ID, 
	CLAIM_REP_DIM_EXAMINER_HIST_ID, 
	CLAIM_REP_DIM_PRIM_LITIGATION_HANDLER_ID, 
	CLAIM_REP_DIM_PRIM_LITIGATION_HANDLER_HIST_ID, 
	CLAIM_REP_DIM_TRANS_ENTRY_OPER_ID, 
	CLAIM_REP_DIM_TRANS_ENTRY_OPER_HIST_ID, 
	CLAIM_REP_DIM_CLAIM_CREATED_BY_ID, 
	POL_DIM_ID, 
	POL_DIM_HIST_ID, 
	AGENCY_DIM_ID, 
	AGENCY_DIM_HIST_ID, 
	CLAIM_PAY_DIM_ID, 
	CLAIM_PAY_DIM_HIST_ID, 
	CLAIM_PAY_CTGRY_TYPE_DIM_ID, 
	CLAIM_PAY_CTGRY_TYPE_DIM_HIST_ID, 
	CLAIM_CASE_DIM_ID, 
	CLAIM_CASE_DIM_HIST_ID, 
	CONTRACT_CUST_DIM_ID, 
	CONTRACT_CUST_DIM_HIST_ID, 
	CLAIM_MASTER_1099_LIST_DIM_ID, 
	CLAIM_SUBROGATION_DIM_ID, 
	CLAIM_TRANS_DATE_ID, 
	CLAIM_TRANS_REPROCESS_DATE_ID, 
	CLAIM_LOSS_DATE_ID, 
	CLAIM_DISCOVERY_DATE_ID, 
	CLAIM_SCRIPTED_DATE_ID, 
	SOURCE_CLAIM_RPTED_DATE_ID, 
	CLAIM_RPTED_DATE_ID, 
	CLAIM_OPEN_DATE_ID, 
	CLAIM_CLOSE_DATE_ID, 
	CLAIM_REOPEN_DATE_ID, 
	CLAIM_CLOSED_AFTER_REOPEN_DATE_ID, 
	CLAIM_NOTICE_ONLY_DATE_ID, 
	CLAIM_CAT_START_DATE_ID, 
	CLAIM_CAT_END_DATE_ID, 
	CLAIM_REP_ASSIGNED_DATE_ID, 
	CLAIM_REP_UNASSIGNED_DATE_ID, 
	POL_EFF_DATE_ID, 
	POL_EXP_DATE_ID, 
	CLAIM_SUBROGATION_REFERRED_TO_SUBROGATION_DATE_ID, 
	CLAIM_SUBROGATION_PAY_START_DATE_ID, 
	CLAIM_SUBROGATION_CLOSURE_DATE_ID, 
	ACCT_ENTERED_DATE_ID, 
	TRANS_AMT, 
	TRANS_HIST_AMT, 
	TAX_ID, 
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
	CEDED_LOSS_PAID, 
	CEDED_LOSS_OUTSTANDING, 
	CEDED_LOSS_INCURRED, 
	CEDED_ALAE_PAID, 
	CEDED_ALAE_OUTSTANDING, 
	CEDED_ALAE_INCURRED, 
	CEDED_SALVAGE_PAID, 
	CEDED_SUBROGATION_PAID, 
	CEDED_OTHER_RECOVERY_LOSS_PAID, 
	CEDED_OTHER_RECOVERY_ALAE_PAID, 
	TOTAL_CEDED_LOSS_RECOVERY_PAID, 
	NET_LOSS_PAID, 
	NET_LOSS_OUTSTANDING, 
	NET_LOSS_INCURRED, 
	NET_ALAE_PAID, 
	NET_ALAE_OUTSTANDING, 
	NET_ALAE_INCURRED, 
	asl_dim_id_IN AS ASL_DIM_ID, 
	asl_prdct_code_dim_id_IN AS ASL_PRDCT_CODE_DIM_ID, 
	loss_master_dim_id_IN AS LOSS_MASTER_DIM_ID, 
	STRTGC_BUS_DVSN_DIM_ID, 
	prdct_code_dim_id_IN AS PRDCT_CODE_DIM_ID, 
	CLAIMRESERVEDIMID, 
	CLAIMREPRESENTATIVEDIMFEATURECLAIMREPRESENTATIVEID, 
	FEATUREREPRESENTATIVEASSIGNEDDATEID, 
	INSURANCEREFERENCEDIMID, 
	AGENCYDIMID, 
	SALESDIVISIONDIMID, 
	INSURANCEREFERENCECOVERAGEDIMID, 
	COVERAGEDETAILDIMID, 
	MODIFIEDDATE
	FROM EXP_mp50_output
),
UPD_set_update AS (
	SELECT
	claim_loss_trans_fact_id, 
	asl_dim_id_out AS asl_dim_id, 
	asl_prdct_code_dim_id_out AS asl_prdct_code_dim_id, 
	loss_master_dim_id_out AS loss_master_dim_id, 
	prdct_code_dim_id AS prdct_code_dim_id1, 
	InsuranceReferenceDimId, 
	AgencyDimId, 
	SalesDivisionDimId, 
	InsuranceReferenceCoverageDimId, 
	CoverageDetailDimId
	FROM RTR_divert_MP50_Update
),
claim_loss_transaction_fact_UPDATE_PMSClaims AS (
	MERGE INTO claim_loss_transaction_fact AS T
	USING UPD_set_update AS S
	ON T.claim_loss_trans_fact_id = S.claim_loss_trans_fact_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.asl_dim_id = S.asl_dim_id, T.asl_prdct_code_dim_id = S.asl_prdct_code_dim_id, T.loss_master_dim_id = S.loss_master_dim_id, T.prdct_code_dim_id = S.prdct_code_dim_id1, T.InsuranceReferenceDimId = S.InsuranceReferenceDimId, T.AgencyDimId = S.AgencyDimId, T.SalesDivisionDimId = S.SalesDivisionDimId, T.InsuranceReferenceCoverageDimId = S.InsuranceReferenceCoverageDimId, T.CoverageDetailDimId = S.CoverageDetailDimId, T.ModifiedDate = S.ModifiedDate3
),
SQ_EDWSource_DCTClaims AS (
	SELECT CLTF.claim_loss_trans_fact_id   AS claim_loss_trans_fact_id,
		   CT.claim_trans_id			   AS claim_trans_id,
		   AB.claimant_cov_det_ak_id       AS Claimant_cov_det_ak_id,
		   AB.claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id,
	       AB.PolicySourceID               AS Policysourceid,
	       AB.ClassCode                    AS Classcode,
	       AB.SublineCode                  AS Sublinecode,
	       RC.Exposure                     AS Exposure,
	       RC.AnnualStatementLineNumber    AS AnnualStatementLineNumber,
	       RC.SchedulePNumber              AS SchedulePNumber,
	       RC.AnnualStatementLineCode      AS AnnualStatementLineCode,
	       RC.SubAnnualStatementLineNumber AS SubAnnualStatementLineNumber,
	       RC.SubAnnualStatementLineCode   AS SubAnnualStatementLineCode,
	       RC.SubNonAnnualStatementLineCode AS SubNonAnnualStatementLineCode,
	       RL.RiskTerritory                AS Riskterritory,
	       RL.StateProvinceCode            AS Stateprovincecode,
	       RL.ZipPostalCode                AS Zippostalcode,
	       RL.TaxLocation                  AS Taxlocation,
	       SPC.StrategicProfitCenterCode   AS StrategicProfitCenterCode,
	       PDT.ProductCode                 AS ProductCode,
	       LOB.InsuranceReferenceLineOfBusinessCode AS InsuranceReferenceLineOfBusinessCode,
	AB.cause_of_loss AS cause_of_loss 
	FROM   @{pipeline().parameters.DB_NAME_DATAMART}.@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_loss_transaction_fact CLTF  INNER JOIN 
	       @{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.TARGET_TABLE_OWNER}.VW_claim_transaction CT  ON CLTF.edw_claim_trans_pk_id = CT.claim_trans_id
	       INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CLAIMANT_COVERAGE_DETAIL AB ON CT.Claimant_cov_det_ak_id = AB.Claimant_cov_det_ak_id
	       INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence CPO ON CPO.claim_party_occurrence_ak_id = AB.claim_party_occurrence_ak_id
	       INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO ON CO.claim_occurrence_ak_id = CPO.claim_occurrence_ak_id
	       INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.V2.policy P ON CO.pol_key_ak_id = P.pol_ak_id
		   left JOIN @{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC ON AB.RatingCoverageAKId = RC.RatingCoverageAKID 
		   AND RC.CurrentSnapshotFlag =1
		   left JOIN @{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.TARGET_TABLE_OWNER}.POLICYCOVERAGE Pc ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID AND Pc.CurrentSnapshotFlag =1
		   left JOIN @{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RISKLOCATION Rl ON PC.RiskLocationAKID = RL.RiskLocationAKID AND Rl.CurrentSnapshotFlag =1
		   LEFT OUTER JOIN @{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StrategicProfitCenter SPC ON P.StrategicProfitCenterAKId = SPC.StrategicProfitCenterAKId
		   LEFT OUTER JOIN @{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceLineOfBusiness LOB ON RC.InsuranceReferenceLineOfBusinessAKId = LOB.InsuranceReferenceLineOfBusinessAKId
		   LEFT OUTER JOIN @{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.TARGET_TABLE_OWNER}.Product PDT ON PDT.ProductAKId = RC.ProductAKId
	WHERE AB.PolicySourceID IN ( 'PDC', 'DUC' )
	      AND P.crrnt_snpsht_flag =1
	      AND SPC.CurrentSnapshotFlag =1
	      AND AB.crrnt_snpsht_flag = 1
	      AND CPO.crrnt_snpsht_flag = 1
	      AND CO.crrnt_snpsht_flag = 1
	AND  (CLTF.asl_dim_id in (0,-1) or CLTF.strtgc_bus_dvsn_dim_id in (0,-1) or  CLTF.loss_master_dim_id in (0,-1) or CLTF.prdct_code_dim_id in (0,-1)
			)
),
EXP_Default AS (
	SELECT
	claim_loss_trans_fact_id,
	claim_trans_id,
	claimant_cov_det_ak_id,
	claim_party_occurrence_ak_id,
	PolicySourceID,
	ClassCode,
	SublineCode,
	RiskTerritory,
	StateProvinceCode,
	ZipPostalCode,
	TaxLocation,
	Exposure,
	StrategicProfitCenterCode,
	ProductCode,
	InsuranceReferenceLineOfBusinessCode,
	'N/A' AS Default_NA,
	AnnualStatementLineNumber1 AS AnnualStatementLineNumber,
	SchedulePNumber,
	AnnualStatementLineCode,
	SubAnnualStatementLineNumber,
	SubAnnualStatementLineCode,
	SubNonAnnualStatementLineCode,
	cause_of_loss,
	-- *INF*: IIF(in(cause_of_loss,'05','75'),'1','0')
	IFF(cause_of_loss IN ('05','75'),
		'1',
		'0'
	) AS v_Indemnity,
	-- *INF*: IIF(in(cause_of_loss,'06','07'),'1','0')
	IFF(cause_of_loss IN ('06','07'),
		'1',
		'0'
	) AS v_Medical,
	-- *INF*: IIF(AnnualStatementLineCode='160',
	-- DECODE('1',v_Indemnity,'180',v_Medical,'190','999'),
	-- 		SubAnnualStatementLineCode)
	IFF(AnnualStatementLineCode = '160',
		DECODE('1',
		v_Indemnity, '180',
		v_Medical, '190',
		'999'
		),
		SubAnnualStatementLineCode
	) AS o_sub_asl_code
	FROM SQ_EDWSource_DCTClaims
),
LKP_Produc_Code_Dim AS (
	SELECT
	prdct_code_dim_id,
	prdct_code
	FROM (
		SELECT 
			prdct_code_dim_id,
			prdct_code
		FROM product_code_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY prdct_code ORDER BY prdct_code_dim_id DESC) = 1
),
LKP_asl_dim_DuckCreekClaims AS (
	SELECT
	asl_dim_id,
	asl_code,
	sub_asl_code,
	sub_non_asl_code
	FROM (
		SELECT 
		asl_dim.asl_code as asl_code, 
		asl_dim.asl_dim_id as asl_dim_id, 
		asl_dim.sub_asl_code as sub_asl_code, 
		asl_dim.sub_non_asl_code as sub_non_asl_code 
		FROM asl_dim
		where 
		asl_dim.crrnt_snpsht_flag ='1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY asl_code,sub_asl_code,sub_non_asl_code ORDER BY asl_dim_id DESC) = 1
),
LKP_loss_master_dim_DuckCreekClaims AS (
	SELECT
	loss_master_dim_id,
	risk_state_prov_code,
	risk_zip_code,
	terr_code,
	tax_loc,
	class_code,
	exposure,
	sub_line_code,
	source_sar_asl,
	source_sar_prdct_line,
	source_sar_sp_use_code,
	source_statistical_line,
	variation_code,
	pol_type,
	auto_reins_facility,
	statistical_brkdwn_line,
	statistical_code1,
	statistical_code2,
	statistical_code3,
	loss_master_cov_code
	FROM (
		SELECT loss_master_dim.loss_master_dim_id      AS loss_master_dim_id,
		       ltrim(rtrim(loss_master_dim.risk_state_prov_code))    AS risk_state_prov_code,
		       ltrim(rtrim(loss_master_dim.risk_zip_code))           AS risk_zip_code,
		       ltrim(rtrim(loss_master_dim.terr_code))               AS terr_code,
		       ltrim(rtrim(loss_master_dim.tax_loc))                 AS tax_loc,
		       ltrim(rtrim(loss_master_dim.class_code))              AS class_code,
		       loss_master_dim.exposure                AS exposure,
		       ltrim(rtrim(loss_master_dim.sub_line_code))           AS sub_line_code,
		       ltrim(rtrim(loss_master_dim.source_sar_asl))          AS source_sar_asl,
		       ltrim(rtrim(loss_master_dim.source_sar_prdct_line))   AS source_sar_prdct_line,
		       ltrim(rtrim(loss_master_dim.source_sar_sp_use_code))  AS source_sar_sp_use_code,
		       ltrim(rtrim(loss_master_dim.source_statistical_line)) AS source_statistical_line,
		       ltrim(rtrim(loss_master_dim.variation_code))          AS variation_code,
		       ltrim(rtrim(loss_master_dim.pol_type))                AS pol_type,
		       ltrim(rtrim(loss_master_dim.auto_reins_facility))     AS auto_reins_facility,
		       ltrim(rtrim(loss_master_dim.statistical_brkdwn_line)) AS statistical_brkdwn_line,
		       ltrim(rtrim(loss_master_dim.statistical_code1))       AS statistical_code1,
		       ltrim(rtrim(loss_master_dim.statistical_code2))       AS statistical_code2,
		       ltrim(rtrim(loss_master_dim.statistical_code3))       AS statistical_code3,
		       ltrim(rtrim(loss_master_dim.loss_master_cov_code))    AS loss_master_cov_code
		FROM   loss_master_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY risk_state_prov_code,risk_zip_code,terr_code,tax_loc,class_code,exposure,sub_line_code,source_sar_asl,source_sar_prdct_line,source_sar_sp_use_code,source_statistical_line,variation_code,pol_type,auto_reins_facility,statistical_brkdwn_line,statistical_code1,statistical_code2,statistical_code3,loss_master_cov_code ORDER BY loss_master_dim_id DESC) = 1
),
LKP_strategic_business_division_dim AS (
	SELECT
	strtgc_bus_dvsn_dim_id,
	strtgc_bus_dvsn_code
	FROM (
		SELECT strategic_business_division_dim.strtgc_bus_dvsn_dim_id as strtgc_bus_dvsn_dim_id, strategic_business_division_dim.strtgc_bus_dvsn_code as strtgc_bus_dvsn_code FROM strategic_business_division_dim
		WHERE
		pol_sym_1 = 'N/A'
		
		--- We are filtering the records as we need to tie to DuckCreek Claims.
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY strtgc_bus_dvsn_code ORDER BY strtgc_bus_dvsn_dim_id DESC) = 1
),
EXP_Values AS (
	SELECT
	EXP_Default.claim_loss_trans_fact_id,
	EXP_Default.claim_trans_id,
	LKP_loss_master_dim_DuckCreekClaims.loss_master_dim_id,
	-- *INF*: IIF(ISNULL(loss_master_dim_id),-1,loss_master_dim_id)
	IFF(loss_master_dim_id IS NULL,
		- 1,
		loss_master_dim_id
	) AS loss_master_dim_id_out,
	EXP_Default.AnnualStatementLineNumber,
	EXP_Default.SchedulePNumber,
	EXP_Default.SubAnnualStatementLineNumber,
	LKP_asl_dim_DuckCreekClaims.asl_dim_id,
	-- *INF*: IIF(ISNULL(asl_dim_id),-1,asl_dim_id)
	-- 
	IFF(asl_dim_id IS NULL,
		- 1,
		asl_dim_id
	) AS asl_dim_id_Out,
	LKP_strategic_business_division_dim.strtgc_bus_dvsn_dim_id,
	-- *INF*: IIF(ISNULL(strtgc_bus_dvsn_dim_id),-1,strtgc_bus_dvsn_dim_id)
	IFF(strtgc_bus_dvsn_dim_id IS NULL,
		- 1,
		strtgc_bus_dvsn_dim_id
	) AS strtgc_bus_dvsn_dim_id_Out,
	LKP_Produc_Code_Dim.prdct_code_dim_id,
	-- *INF*: IIF(ISNULL(prdct_code_dim_id),-1,prdct_code_dim_id)
	IFF(prdct_code_dim_id IS NULL,
		- 1,
		prdct_code_dim_id
	) AS prdct_code_dim_id_Out,
	SYSDATE AS ModifiedDate
	FROM EXP_Default
	LEFT JOIN LKP_Produc_Code_Dim
	ON LKP_Produc_Code_Dim.prdct_code = EXP_Default.ProductCode
	LEFT JOIN LKP_asl_dim_DuckCreekClaims
	ON LKP_asl_dim_DuckCreekClaims.asl_code = EXP_Default.AnnualStatementLineCode AND LKP_asl_dim_DuckCreekClaims.sub_asl_code = EXP_Default.o_sub_asl_code AND LKP_asl_dim_DuckCreekClaims.sub_non_asl_code = EXP_Default.SubNonAnnualStatementLineCode
	LEFT JOIN LKP_loss_master_dim_DuckCreekClaims
	ON LKP_loss_master_dim_DuckCreekClaims.risk_state_prov_code = EXP_Default.StateProvinceCode AND LKP_loss_master_dim_DuckCreekClaims.risk_zip_code = EXP_Default.ZipPostalCode AND LKP_loss_master_dim_DuckCreekClaims.terr_code = EXP_Default.RiskTerritory AND LKP_loss_master_dim_DuckCreekClaims.tax_loc = EXP_Default.TaxLocation AND LKP_loss_master_dim_DuckCreekClaims.class_code = EXP_Default.ClassCode AND LKP_loss_master_dim_DuckCreekClaims.exposure = EXP_Default.Exposure AND LKP_loss_master_dim_DuckCreekClaims.sub_line_code = EXP_Default.SublineCode AND LKP_loss_master_dim_DuckCreekClaims.source_sar_asl = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.source_sar_prdct_line = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.source_sar_sp_use_code = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.source_statistical_line = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.variation_code = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.pol_type = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.auto_reins_facility = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.statistical_brkdwn_line = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.statistical_code1 = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.statistical_code2 = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.statistical_code3 = EXP_Default.Default_NA AND LKP_loss_master_dim_DuckCreekClaims.loss_master_cov_code = EXP_Default.Default_NA
	LEFT JOIN LKP_strategic_business_division_dim
	ON LKP_strategic_business_division_dim.strtgc_bus_dvsn_code = EXP_Default.StrategicProfitCenterCode
),
AGG_Remove_Duplicate AS (
	SELECT
	claim_loss_trans_fact_id,
	asl_dim_id_Out AS asl_dim_id,
	loss_master_dim_id_out AS loss_master_dim_id,
	strtgc_bus_dvsn_dim_id_Out AS strtgc_bus_dvsn_dim_id,
	prdct_code_dim_id_Out AS prdct_code_dim_id,
	ModifiedDate
	FROM EXP_Values
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_loss_trans_fact_id, asl_dim_id, loss_master_dim_id, strtgc_bus_dvsn_dim_id, prdct_code_dim_id ORDER BY NULL) = 1
),
UPD_ASLID_LossMasterDimID AS (
	SELECT
	claim_loss_trans_fact_id, 
	asl_dim_id, 
	loss_master_dim_id, 
	strtgc_bus_dvsn_dim_id, 
	prdct_code_dim_id, 
	ModifiedDate
	FROM AGG_Remove_Duplicate
),
claim_loss_transaction_fact_UPDATE_DCTClaims AS (
	MERGE INTO claim_loss_transaction_fact AS T
	USING UPD_ASLID_LossMasterDimID AS S
	ON T.claim_loss_trans_fact_id = S.claim_loss_trans_fact_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.asl_dim_id = S.asl_dim_id, T.loss_master_dim_id = S.loss_master_dim_id, T.strtgc_bus_dvsn_dim_id = S.strtgc_bus_dvsn_dim_id, T.prdct_code_dim_id = S.prdct_code_dim_id, T.ModifiedDate = S.ModifiedDate
),