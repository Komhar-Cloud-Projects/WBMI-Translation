WITH
SQ_Sources_PMSClaims AS (
	SELECT 
	FAC.loss_master_fact_id, 
	FAC.edw_claim_trans_pk_id, 
	FAC.wc_stage_loss_master_pk_id, 
	FAC.claim_trans_type_dim_id, 
	FAC.edw_loss_master_calculation_pk_id, 
	FAC.edw_claim_reins_trans_pk_id, 
	FAC.loss_master_dim_id, 
	FAC.claim_occurrence_dim_id, 
	FAC.claimant_dim_id, 
	FAC.claimant_cov_dim_id, 
	FAC.cov_dim_id, 
	FAC.reins_cov_dim_id, 
	FAC.claim_rep_dim_prim_claim_rep_id, 
	FAC.claim_rep_dim_examiner_id, 
	FAC.pol_dim_id, 
	FAC.contract_cust_dim_id, 
	FAC.agency_dim_id, 
	FAC.claim_pay_dim_id, 
	FAC.claim_pay_ctgry_type_dim_id, 
	FAC.claim_case_dim_id, 
	FAC.claim_trans_date_id, 
	FAC.pol_eff_date_id, 
	FAC.pol_exp_date_id, 
	FAC.source_claim_rpted_date_id, 
	FAC.claim_rpted_date_id, 
	FAC.claim_loss_date_id, 
	FAC.incptn_date_id, 
	FAC.loss_master_run_date_id, 
	FAC.claim_trans_amt, 
	FAC.claim_trans_hist_amt, 
	FAC.new_claim_count, 
	FAC.outstanding_amt, 
	FAC.paid_loss_amt, 
	FAC.paid_exp_amt, 
	FAC.eom_unpaid_loss_adjust_exp, 
	FAC.orig_reserve, 
	FAC.orig_reserve_extract, 
	FAC.asl_dim_id, 
	FAC.asl_prdct_code_dim_id, 
	FAC.strtgc_bus_dvsn_dim_id,
	FAC.prdct_code_dim_id,
	FAC.InsuranceReferenceDimId,
	FAC.AgencyDimId,
	FAC.SalesDivisionDimId,
	FAC.InsuranceReferenceCoverageDimId,
	FAC.CoverageDetailDimId,
	FAC.ClaimFinancialTypeDimId,
	POL.pms_pol_lob_code, 
	POL.pol_sym, 
	POL.mco, 
	POL.reporting_dvsn_code, 
	POL.ClassOfBusinessCode,
	POL.pol_eff_date,  
	COV.type_bureau_code, 
	COV.major_peril_code, 
	COV.risk_unit, 
	COV.loc_unit_num, 
	COV.risk_unit_grp,
	COV.ins_line, 
	LM.sub_line_code, 
	LM.class_code, 
	CLMT.cause_of_loss 
	FROM
	loss_master_fact FAC, policy_dim POL, claimant_coverage_dim CLMT, loss_master_dim LM, coverage_dim COV
	WHERE
	FAC.cov_dim_id=COV.cov_dim_id AND FAC.pol_dim_id = POL.pol_dim_id AND 
	FAC.loss_master_dim_id= LM.loss_master_dim_id AND
	FAC.claimant_cov_dim_id= CLMT.claimant_cov_dim_id 
	AND
	(FAC.asl_dim_id in (0, -1,32) OR 
	 FAC.asl_prdct_code_dim_id in (0, -1,48) OR 
	 FAC.prdct_code_dim_id in (0,-1))
	AND POL.pol_sym <> '000'
	-- Reading only PMS claims data
),
EXP_Source AS (
	SELECT
	pms_pol_lob_code,
	pol_sym,
	type_bureau_code,
	major_peril_code,
	risk_unit,
	loc_unit_num,
	sub_line_code,
	class_code,
	risk_unit_grp,
	mco,
	reporting_dvsn_code,
	cause_of_loss,
	ClassOfBusinessCode,
	ins_line,
	pol_eff_date,
	loss_master_fact_id,
	edw_claim_trans_pk_id,
	wc_stage_loss_master_pk_id,
	claim_trans_type_dim_id,
	edw_loss_master_calculation_pk_id,
	edw_claim_reins_trans_pk_id,
	loss_master_dim_id,
	claim_occurrence_dim_id,
	claimant_dim_id,
	claimant_cov_dim_id,
	cov_dim_id,
	reins_cov_dim_id,
	claim_rep_dim_prim_claim_rep_id,
	claim_rep_dim_examiner_id,
	pol_dim_id,
	contract_cust_dim_id,
	agency_dim_id,
	claim_pay_dim_id,
	claim_pay_ctgry_type_dim_id,
	claim_case_dim_id,
	claim_trans_date_id,
	pol_eff_date_id,
	pol_exp_date_id,
	source_claim_rpted_date_id,
	claim_rpted_date_id,
	claim_loss_date_id,
	incptn_date_id,
	loss_master_run_date_id,
	claim_trans_amt,
	claim_trans_hist_amt,
	new_claim_count,
	outstanding_amt,
	paid_loss_amt,
	paid_exp_amt,
	eom_unpaid_loss_adjust_exp,
	orig_reserve,
	orig_reserve_extract,
	asl_dim_id,
	asl_prdct_code_dim_id,
	strtgc_bus_dvsn_dim_id,
	prdct_code_dim_id,
	InsuranceReferenceDimId,
	AgencyDimId,
	SalesDivisionDimId,
	InsuranceReferenceCoverageDimId,
	CoverageDetailDimId,
	ClaimFinancialTypeDimId
	FROM SQ_Sources_PMSClaims
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
		decode(substr(symbol, 1, 2),
		'HX', 1,
		'PX', 1,
		'WM', 1,
		'WX', 1,
		'XA', 1,
		'XX', 1,
		'DU', 1,
		0) AS symbol_change,
		-- *INF*: substr(symbol,3,1)
		substr(symbol, 3, 1) AS v_symbol_3,
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
		IFF(symbol_change = 1, DECODE(TRUE,
		line_of_business = 'HAP' AND type_bureau != 'GL', 'HH',
		line_of_business = 'APV', 'PA',
		rtrim(ltrim(line_of_business)) = 'HP' AND master_co_number = '06', 'HA',
		rtrim(ltrim(line_of_business)) = 'HP' AND master_co_number = '05', 'HB',
		line_of_business = 'IMP', 'IP',
		line_of_business = 'HAP' AND type_bureau = 'GL', 'HH',
		rtrim(ltrim(line_of_business)) = 'GL' AND major_peril = '017', 'UP',
		substr(symbol, 1, 2)), substr(symbol, 1, 2)) AS v_symbol_1_2,
		-- *INF*: IIF(symbol_change=1,
		-- 	IIF(line_of_business='HAP' and type_bureau='GL', '017',major_peril)
		-- ,major_peril)
		-- 
		-- 
		IFF(symbol_change = 1, IFF(line_of_business = 'HAP' AND type_bureau = 'GL', '017', major_peril), major_peril) AS v_major_peril,
		-- *INF*: IIF(symbol_change=1,
		-- 	IIF(rtrim(ltrim(line_of_business))='GL' and major_peril='017','GL',type_bureau)
		-- ,type_bureau)
		-- 
		-- 
		IFF(symbol_change = 1, IFF(rtrim(ltrim(line_of_business)) = 'GL' AND major_peril = '017', 'GL', type_bureau), type_bureau) AS v_type_bureau,
		-- *INF*: concat(v_symbol_1_2,v_symbol_3)
		concat(v_symbol_1_2, v_symbol_3) AS symbol_out,
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
EXP_view__symbol_mapplet_output AS (
	SELECT
	symbol1,
	type_bureau1,
	major_peril1
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
		IFF(policy_effective_date IS NULL, TO_DATE('01/01/1800', 'MM/DD/YYYY'), policy_effective_date) AS policy_effective_date_out,
		-- *INF*: TO_DATE('06/01/2012 00:00:00','MM/DD/YYYY HH24:MI:SS')
		-- 
		-- -- prod 3017 6/1/2012 is the date for applying new earthquake coverages
		TO_DATE('06/01/2012 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS CL_EQ_EFF_Date
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
		substr(risk_unit_group, 1, 3) AS v_risk_unit_group_1_3,
		-- *INF*: substr(symbol,1,2)
		substr(symbol, 1, 2) AS v_symbol_pos_1_2,
		v_symbol_pos_1_2 AS symbol_pos_1_2_out,
		-- *INF*: IIF(v_symbol_pos_1_2='HH' and type_bureau='PF' and (in(major_peril,'081','280',@{pipeline().parameters.MP_210_211},@{pipeline().parameters.MP_249_250})),1,0)
		IFF(v_symbol_pos_1_2 = 'HH' AND type_bureau = 'PF' AND ( in(major_peril, '081', '280', @{pipeline().parameters.MP_210_211}, @{pipeline().parameters.MP_249_250}) ), 1, 0) AS v_home_and_highway_fire_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HH' and type_bureau='PF' and in(major_peril,@{pipeline().parameters.MP_220_230}),1,0)
		IFF(v_symbol_pos_1_2 = 'HH' AND type_bureau = 'PF' AND in(major_peril, @{pipeline().parameters.MP_220_230}), 1, 0) AS v_home_and_highway_allied_lines_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HH' and type_bureau='PH' and in(major_peril,'002','097','911','914'),1,0)
		IFF(v_symbol_pos_1_2 = 'HH' AND type_bureau = 'PH' AND in(major_peril, '002', '097', '911', '914'), 1, 0) AS v_home_and_highway_homeowners_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HH' and  type_bureau='PI' and in( major_peril,'042','062','200','201','206'),1,0)
		IFF(v_symbol_pos_1_2 = 'HH' AND type_bureau = 'PI' AND in(major_peril, '042', '062', '200', '201', '206'), 1, 0) AS v_home_and_highway_inland_marine_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HH' and type_bureau='PQ' and in(major_peril,@{pipeline().parameters.MP_260_261}),1,0)
		IFF(v_symbol_pos_1_2 = 'HH' AND type_bureau = 'PQ' AND in(major_peril, @{pipeline().parameters.MP_260_261}), 1, 0) AS v_home_and_highway_earthquake_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HH' and type_bureau='PL',1,0) 
		IFF(v_symbol_pos_1_2 = 'HH' AND type_bureau = 'PL', 1, 0) AS v_home_and_highway_personal_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HH' and  type_bureau='GL' and major_peril='017',1,0)
		IFF(v_symbol_pos_1_2 = 'HH' AND type_bureau = 'GL' AND major_peril = '017', 1, 0) AS v_home_and_highway_general_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HH'  and in(type_bureau,'RL','RP','RN') and in(major_peril,'150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143}),1,0) 
		IFF(v_symbol_pos_1_2 = 'HH' AND in(type_bureau, 'RL', 'RP', 'RN') AND in(major_peril, '150', @{pipeline().parameters.MP_100_130}, @{pipeline().parameters.MP_140_143}), 1, 0) AS v_home_and_highway_pp_auto_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HH' and type_bureau='RP' and in(major_peril,'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}),1,0)
		IFF(v_symbol_pos_1_2 = 'HH' AND type_bureau = 'RP' AND in(major_peril, '168', '169', '912', @{pipeline().parameters.MP_145_149}, @{pipeline().parameters.MP_151_163}, @{pipeline().parameters.MP_170_178}), 1, 0) AS v_home_and_highway_pp_auto_physical_damage_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='PP' and in(type_bureau,'RL','RP','RN') and in(major_peril,'150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143}),1,0)
		IFF(v_symbol_pos_1_2 = 'PP' AND in(type_bureau, 'RL', 'RP', 'RN') AND in(major_peril, '150', @{pipeline().parameters.MP_100_130}, @{pipeline().parameters.MP_140_143}), 1, 0) AS v_preferred_auto_pp_auto_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='PP' and type_bureau='RP' and in(major_peril,'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}),1,0)
		IFF(v_symbol_pos_1_2 = 'PP' AND type_bureau = 'RP' AND in(major_peril, '168', '169', '912', @{pipeline().parameters.MP_145_149}, @{pipeline().parameters.MP_151_163}, @{pipeline().parameters.MP_170_178}), 1, 0) AS v_preferred_auto_pp_auto_physical_damage_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='PA' and in(type_bureau,'RL','RP','RN') and in(major_peril,'150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143}),1,0)
		IFF(v_symbol_pos_1_2 = 'PA' AND in(type_bureau, 'RL', 'RP', 'RN') AND in(major_peril, '150', @{pipeline().parameters.MP_100_130}, @{pipeline().parameters.MP_140_143}), 1, 0) AS v_select_auto_pp_auto_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='PA' and type_bureau='RP' and in(major_peril,'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}),1,0)
		IFF(v_symbol_pos_1_2 = 'PA' AND type_bureau = 'RP' AND in(major_peril, '168', '169', '912', @{pipeline().parameters.MP_145_149}, @{pipeline().parameters.MP_151_163}, @{pipeline().parameters.MP_170_178}), 1, 0) AS v_select_auto_pp_auto_physical_damage_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HB' and type_bureau='NB' and in(major_peril,@{pipeline().parameters.MP_220_230}),1,0)
		IFF(v_symbol_pos_1_2 = 'HB' AND type_bureau = 'NB' AND in(major_peril, @{pipeline().parameters.MP_220_230}), 1, 0) AS v_standard_homeowners_allied_lined_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HB' and  type_bureau='PH' and major_peril='002',1,0)
		IFF(v_symbol_pos_1_2 = 'HB' AND type_bureau = 'PH' AND major_peril = '002', 1, 0) AS v_standard_homeowners_homeowners_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HB' and type_bureau='PI' and in(major_peril,'042','044','062','200','201','206'),1,0)
		IFF(v_symbol_pos_1_2 = 'HB' AND type_bureau = 'PI' AND in(major_peril, '042', '044', '062', '200', '201', '206'), 1, 0) AS v_standard_homeowners_inland_marine_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HB' and type_bureau='PQ' and in(major_peril,@{pipeline().parameters.MP_260_261}),1,0)
		-- 
		IFF(v_symbol_pos_1_2 = 'HB' AND type_bureau = 'PQ' AND in(major_peril, @{pipeline().parameters.MP_260_261}), 1, 0) AS v_standard_homeowners_earthquake_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'HB' AND type_bureau = 'PL',1,0)
		IFF(v_symbol_pos_1_2 = 'HB' AND type_bureau = 'PL', 1, 0) AS v_standard_homeowners_personal_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='HA' and type_bureau='NB' and in(major_peril,@{pipeline().parameters.MP_220_230}),1,0)
		-- 
		IFF(v_symbol_pos_1_2 = 'HA' AND type_bureau = 'NB' AND in(major_peril, @{pipeline().parameters.MP_220_230}), 1, 0) AS v_select_homeowners_allied_lines_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'HA' AND type_bureau = 'PH' AND
		--               major_peril = '002',1,0)
		IFF(v_symbol_pos_1_2 = 'HA' AND type_bureau = 'PH' AND major_peril = '002', 1, 0) AS v_select_homeowners_homeowners_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'HA' AND type_bureau = 'PI' AND in(major_peril,'042','044','062',                     '200','201','206'),1,0)
		IFF(v_symbol_pos_1_2 = 'HA' AND type_bureau = 'PI' AND in(major_peril, '042', '044', '062', '200', '201', '206'), 1, 0) AS v_select_homeowners_inland_marine_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'HA' AND type_bureau = 'PQ' AND in(major_peril,@{pipeline().parameters.MP_260_261}),1,0)
		IFF(v_symbol_pos_1_2 = 'HA' AND type_bureau = 'PQ' AND in(major_peril, @{pipeline().parameters.MP_260_261}), 1, 0) AS v_select_homeowners_earthquake_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'HA' AND type_bureau = 'PL',1,0)
		IFF(v_symbol_pos_1_2 = 'HA' AND type_bureau = 'PL', 1, 0) AS v_select_homeowners_personal_liability_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2 , 'FP' ,'FL') AND type_bureau = 'PF' AND
		--               in(major_peril ,'081',@{pipeline().parameters.MP_210_211},@{pipeline().parameters.MP_249_250}),1,0)
		IFF(in(v_symbol_pos_1_2, 'FP', 'FL') AND type_bureau = 'PF' AND in(major_peril, '081', @{pipeline().parameters.MP_210_211}, @{pipeline().parameters.MP_249_250}), 1, 0) AS v_dwelling_fire_fire_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2, 'FP','FL') AND in(type_bureau,'PF','NB') AND in(major_peril,@{pipeline().parameters.MP_220_230}),1,0)
		IFF(in(v_symbol_pos_1_2, 'FP', 'FL') AND in(type_bureau, 'PF', 'NB') AND in(major_peril, @{pipeline().parameters.MP_220_230}), 1, 0) AS v_dwelling_fire_allied_lines_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'FP','FL') AND type_bureau = 'PQ' AND in(major_peril,@{pipeline().parameters.MP_260_261}),1,0)
		IFF(in(v_symbol_pos_1_2, 'FP', 'FL') AND type_bureau = 'PQ' AND in(major_peril, @{pipeline().parameters.MP_260_261}), 1, 0) AS v_dwelling_fire_earthquake_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'IP' AND type_bureau= 'PI',1,0)
		IFF(v_symbol_pos_1_2 = 'IP' AND type_bureau = 'PI', 1, 0) AS v_personal_inland_marine_inland_marine_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'IP' AND type_bureau = 'PL',1,0)
		IFF(v_symbol_pos_1_2 = 'IP' AND type_bureau = 'PL', 1, 0) AS v_personal_inland_marine_personal_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'PM' AND
		--               in(type_bureau,'RL','RP','RN') AND
		--               in(major_peril,'150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143}),1,0)
		IFF(v_symbol_pos_1_2 = 'PM' AND in(type_bureau, 'RL', 'RP', 'RN') AND in(major_peril, '150', @{pipeline().parameters.MP_100_130}, @{pipeline().parameters.MP_140_143}), 1, 0) AS v_motorcycle_pp_auto_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'PM' AND type_bureau = 'RP' AND
		--  in(major_peril, '168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}),1,0)
		IFF(v_symbol_pos_1_2 = 'PM' AND type_bureau = 'RP' AND in(major_peril, '168', '169', '912', @{pipeline().parameters.MP_145_149}, @{pipeline().parameters.MP_151_163}, @{pipeline().parameters.MP_170_178}), 1, 0) AS v_motorcycle_pp_auto_physical_damage_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'IB' AND type_bureau = 'PI',1,0)
		IFF(v_symbol_pos_1_2 = 'IB' AND type_bureau = 'PI', 1, 0) AS v_boatowners_inland_marine_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'IB' AND type_bureau = 'PL',1,0)
		IFF(v_symbol_pos_1_2 = 'IB' AND type_bureau = 'PL', 1, 0) AS v_boatowners_personal_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'PS' AND
		--               in(type_bureau,'RL', 'RP', 'RN') AND
		--              in(major_peril,'150',@{pipeline().parameters.MP_100_130}, @{pipeline().parameters.MP_140_143}),1,0)
		IFF(v_symbol_pos_1_2 = 'PS' AND in(type_bureau, 'RL', 'RP', 'RN') AND in(major_peril, '150', @{pipeline().parameters.MP_100_130}, @{pipeline().parameters.MP_140_143}), 1, 0) AS v_alternative_one_pp_auto_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'PS' AND type_bureau = 'RP' AND
		-- in(major_peril, '168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}),1,0)
		IFF(v_symbol_pos_1_2 = 'PS' AND type_bureau = 'RP' AND in(major_peril, '168', '169', '912', @{pipeline().parameters.MP_145_149}, @{pipeline().parameters.MP_151_163}, @{pipeline().parameters.MP_170_178}), 1, 0) AS v_alternative_one_pp_auto_physical_damage_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'PT' AND
		--               in(type_bureau,'RL','RP','RN') AND
		--               in(major_peril,'150',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143}),1,0)
		IFF(v_symbol_pos_1_2 = 'PT' AND in(type_bureau, 'RL', 'RP', 'RN') AND in(major_peril, '150', @{pipeline().parameters.MP_100_130}, @{pipeline().parameters.MP_140_143}), 1, 0) AS v_alternative_one_star_pp_auto_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'PT' AND type_bureau = 'RP' AND
		-- in(major_peril,'168','169','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_163},@{pipeline().parameters.MP_170_178}),1,0)
		IFF(v_symbol_pos_1_2 = 'PT' AND type_bureau = 'RP' AND in(major_peril, '168', '169', '912', @{pipeline().parameters.MP_145_149}, @{pipeline().parameters.MP_151_163}, @{pipeline().parameters.MP_170_178}), 1, 0) AS v_alternative_one_star_pp_auto_physical_damage_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2, 'BC','BD','CP','BG','BH','GG','CA') AND
		--  in(type_bureau,'AN','AL') AND
		--  in(major_peril ,'150','599',@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},@{pipeline().parameters.MP_930_931}) AND 
		-- NOT in(subline,@{pipeline().parameters.GARAGE_SUBLINES}),1,0)
		IFF(in(v_symbol_pos_1_2, 'BC', 'BD', 'CP', 'BG', 'BH', 'GG', 'CA') AND in(type_bureau, 'AN', 'AL') AND in(major_peril, '150', '599', @{pipeline().parameters.MP_271_274}, @{pipeline().parameters.MP_100_130}, @{pipeline().parameters.MP_140_143}, @{pipeline().parameters.MP_930_931}) AND NOT in(subline, @{pipeline().parameters.GARAGE_SUBLINES}), 1, 0) AS v_commercial_auto_commercial_auto_liability_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2, 'NA','NB','NS') AND
		--               in(type_bureau, 'AN','AL') AND
		--               in(major_peril,'150','599',@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},@{pipeline().parameters.MP_930_931}) AND
		--  NOT in(subline,@{pipeline().parameters.GARAGE_SUBLINES}),1,0)
		IFF(in(v_symbol_pos_1_2, 'NA', 'NB', 'NS') AND in(type_bureau, 'AN', 'AL') AND in(major_peril, '150', '599', @{pipeline().parameters.MP_271_274}, @{pipeline().parameters.MP_100_130}, @{pipeline().parameters.MP_140_143}, @{pipeline().parameters.MP_930_931}) AND NOT in(subline, @{pipeline().parameters.GARAGE_SUBLINES}), 1, 0) AS v_commercial_auto_commercial_auto_liability_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BC','BD','CP','BG','BH','GG') AND
		--          in(type_bureau,'AN','AL') AND
		--          in(major_peril,'150','599',@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},@{pipeline().parameters.MP_930_931}) 	   AND
		--          in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) AND
		--          unit_number = '999' AND
		--          (is_spaces(location_number) = 1 OR rtrim(ltrim(location_number)) = '0000' ),1,0)
		IFF(in(v_symbol_pos_1_2, 'BC', 'BD', 'CP', 'BG', 'BH', 'GG') AND in(type_bureau, 'AN', 'AL') AND in(major_peril, '150', '599', @{pipeline().parameters.MP_271_274}, @{pipeline().parameters.MP_100_130}, @{pipeline().parameters.MP_140_143}, @{pipeline().parameters.MP_930_931}) AND in(subline, @{pipeline().parameters.GARAGE_SUBLINES}) AND unit_number = '999' AND ( is_spaces(location_number) = 1 OR rtrim(ltrim(location_number)) = '0000' ), 1, 0) AS v_commercial_auto_commercial_auto_liability_garage_veh_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB','NS') AND
		--         in(type_bureau,'AN','AL') AND
		--         in(major_peril,'150','599',@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_140_143},@{pipeline().parameters.MP_930_931}) AND
		--         in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) AND
		--         unit_number = '999' AND
		--          (is_spaces(location_number) OR rtrim(ltrim(location_number))='0000'),1,0)
		IFF(in(v_symbol_pos_1_2, 'NA', 'NB', 'NS') AND in(type_bureau, 'AN', 'AL') AND in(major_peril, '150', '599', @{pipeline().parameters.MP_271_274}, @{pipeline().parameters.MP_100_130}, @{pipeline().parameters.MP_140_143}, @{pipeline().parameters.MP_930_931}) AND in(subline, @{pipeline().parameters.GARAGE_SUBLINES}) AND unit_number = '999' AND ( is_spaces(location_number) OR rtrim(ltrim(location_number)) = '0000' ), 1, 0) AS v_commercial_auto_commercial_auto_liability_garage_veh_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BC','BD','CP','BG','BH','GG','CA','GA') AND
		--               type_bureau = 'AP' AND
		--               NOT in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) AND
		--              in (major_peril,'132','147','177','178',@{pipeline().parameters.MP_145_146},@{pipeline().parameters.MP_148_160},@{pipeline().parameters.MP_163_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}),1,0)
		IFF(in(v_symbol_pos_1_2, 'BC', 'BD', 'CP', 'BG', 'BH', 'GG', 'CA', 'GA') AND type_bureau = 'AP' AND NOT in(subline, @{pipeline().parameters.GARAGE_SUBLINES}) AND in(major_peril, '132', '147', '177', '178', @{pipeline().parameters.MP_145_146}, @{pipeline().parameters.MP_148_160}, @{pipeline().parameters.MP_163_166}, @{pipeline().parameters.MP_170_173}, @{pipeline().parameters.MP_269_270}), 1, 0) AS v_commercial_auto_comm_auto_physical_damage_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB','NS') AND
		--        type_bureau = 'AP' AND
		--        NOT in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) AND
		--        in(major_peril,'132','147','177','178',@{pipeline().parameters.MP_145_146},@{pipeline().parameters.MP_148_160},@{pipeline().parameters.MP_163_166}, @{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}),1,0)
		IFF(in(v_symbol_pos_1_2, 'NA', 'NB', 'NS') AND type_bureau = 'AP' AND NOT in(subline, @{pipeline().parameters.GARAGE_SUBLINES}) AND in(major_peril, '132', '147', '177', '178', @{pipeline().parameters.MP_145_146}, @{pipeline().parameters.MP_148_160}, @{pipeline().parameters.MP_163_166}, @{pipeline().parameters.MP_170_173}, @{pipeline().parameters.MP_269_270}), 1, 0) AS v_commercial_auto_comm_auto_physical_damage_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2, 'BC','BD','CP','BG','BH','GG') AND
		--         type_bureau = 'AP' AND
		--         in(major_peril,'132','147','177','178',@{pipeline().parameters.MP_145_146}, @{pipeline().parameters.MP_148_160},@{pipeline().parameters.MP_163_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}) AND
		--          in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) AND
		--          unit_number = '999' AND
		--           (is_spaces(location_number)=1 OR rtrim(ltrim(location_number))= '0000'),1,0)
		IFF(in(v_symbol_pos_1_2, 'BC', 'BD', 'CP', 'BG', 'BH', 'GG') AND type_bureau = 'AP' AND in(major_peril, '132', '147', '177', '178', @{pipeline().parameters.MP_145_146}, @{pipeline().parameters.MP_148_160}, @{pipeline().parameters.MP_163_166}, @{pipeline().parameters.MP_170_173}, @{pipeline().parameters.MP_269_270}) AND in(subline, @{pipeline().parameters.GARAGE_SUBLINES}) AND unit_number = '999' AND ( is_spaces(location_number) = 1 OR rtrim(ltrim(location_number)) = '0000' ), 1, 0) AS v_commercial_auto_comm_auto_physical_damage_garage_veh_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2, 'NA','NB','NS') AND
		--         type_bureau = 'AP' AND
		--         in(major_peril,'132','147','177','178',@{pipeline().parameters.MP_145_146}, @{pipeline().parameters.MP_148_160}, @{pipeline().parameters.MP_163_166}, @{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}) AND
		--               in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) AND
		--              unit_number = '999' AND
		--               (is_spaces(location_number)=1 OR  rtrim(ltrim(location_number))= '0000'),1,0)
		IFF(in(v_symbol_pos_1_2, 'NA', 'NB', 'NS') AND type_bureau = 'AP' AND in(major_peril, '132', '147', '177', '178', @{pipeline().parameters.MP_145_146}, @{pipeline().parameters.MP_148_160}, @{pipeline().parameters.MP_163_166}, @{pipeline().parameters.MP_170_173}, @{pipeline().parameters.MP_269_270}) AND in(subline, @{pipeline().parameters.GARAGE_SUBLINES}) AND unit_number = '999' AND ( is_spaces(location_number) = 1 OR rtrim(ltrim(location_number)) = '0000' ), 1, 0) AS v_commercial_auto_comm_auto_physical_damage_garage_veh_nsi,
		-- *INF*: IIF(v_symbol_pos_1_2='CP' and in(type_bureau,'AN','AL') and in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) and in(major_peril,'599',@{pipeline().parameters.MP_100_130}, @{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_930_931}),1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND in(type_bureau, 'AN', 'AL') AND in(subline, @{pipeline().parameters.GARAGE_SUBLINES}) AND in(major_peril, '599', @{pipeline().parameters.MP_100_130}, @{pipeline().parameters.MP_271_274}, @{pipeline().parameters.MP_930_931}), 1, 0) AS v_garage_liability_commercial_auto_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2='NS' and in(type_bureau,'AN','AL') and in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) and in(major_peril,'599',@{pipeline().parameters.MP_100_130}, @{pipeline().parameters.MP_271_274}, @{pipeline().parameters.MP_930_931}),1,0)
		IFF(v_symbol_pos_1_2 = 'NS' AND in(type_bureau, 'AN', 'AL') AND in(subline, @{pipeline().parameters.GARAGE_SUBLINES}) AND in(major_peril, '599', @{pipeline().parameters.MP_100_130}, @{pipeline().parameters.MP_271_274}, @{pipeline().parameters.MP_930_931}), 1, 0) AS v_garage_liability_commercial_auto_liability_nsi,
		-- *INF*: IIF(v_symbol_pos_1_2= 'CP' AND
		--        type_bureau = 'AP' AND 
		--       in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) AND
		--       in(major_peril,'132','177','178',@{pipeline().parameters.MP_145_159},@{pipeline().parameters.MP_165_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270})
		-- ,1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND type_bureau = 'AP' AND in(subline, @{pipeline().parameters.GARAGE_SUBLINES}) AND in(major_peril, '132', '177', '178', @{pipeline().parameters.MP_145_159}, @{pipeline().parameters.MP_165_166}, @{pipeline().parameters.MP_170_173}, @{pipeline().parameters.MP_269_270}), 1, 0) AS v_garage_liability_commercial_auto_physical_damage_wbm,
		-- *INF*: IIF (v_symbol_pos_1_2 = 'NS' AND
		--         type_bureau = 'AP' AND 
		--        in(subline,@{pipeline().parameters.GARAGE_SUBLINES}) AND
		--        in(major_peril,'132','177','178',@{pipeline().parameters.MP_145_159},@{pipeline().parameters.MP_165_166},@{pipeline().parameters.MP_170_173},@{pipeline().parameters.MP_269_270}) ,1,0)
		IFF(v_symbol_pos_1_2 = 'NS' AND type_bureau = 'AP' AND in(subline, @{pipeline().parameters.GARAGE_SUBLINES}) AND in(major_peril, '132', '177', '178', @{pipeline().parameters.MP_145_159}, @{pipeline().parameters.MP_165_166}, @{pipeline().parameters.MP_170_173}, @{pipeline().parameters.MP_269_270}), 1, 0) AS v_garage_liability_commercial_auto_physical_damage_nsi,
		-- *INF*: IIF(in(substr(symbol,1,1),'V','W','Y') AND
		--          in(type_bureau,'WC','WP'),1,0)
		IFF(in(substr(symbol, 1, 1), 'V', 'W', 'Y') AND in(type_bureau, 'WC', 'WP'), 1, 0) AS v_workers_comp_total_non_pool_workers_comp_wbm,
		-- *INF*: IIF(in(substr(symbol,1,1),'R','S','T') AND
		--        in(type_bureau,'WC','WP'),1,0)
		IFF(in(substr(symbol, 1, 1), 'R', 'S', 'T') AND in(type_bureau, 'WC', 'WP'), 1, 0) AS v_workers_comp_total_non_pool_workers_comp_nsi,
		-- *INF*: IIF(in(substr(symbol,1,1),'A','J','L') AND
		--        in(type_bureau,'WC','WP'),1,0)
		IFF(in(substr(symbol, 1, 1), 'A', 'J', 'L') AND in(type_bureau, 'WC', 'WP'), 1, 0) AS v_workers_comp_total_non_pool_workers_comp_argent,
		-- *INF*:  IIF(v_symbol_pos_1_2 = 'CP' AND
		--        type_bureau = 'CF' AND
		--        in(major_peril,'415','463','490','496','498','599','919','425','426','435','455','480') AND
		--        NOT in(class_of_business,'I','O') AND
		-- 	 in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		-- 	policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND type_bureau = 'CF' AND in(major_peril, '415', '463', '490', '496', '498', '599', '919', '425', '426', '435', '455', '480') AND NOT in(class_of_business, 'I', 'O') AND in(v_risk_unit_group_1_3, @{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND policy_effective_date >= CL_EQ_EFF_Date, 1, 0) AS v_commercial_property_earthquake_wbm,
		-- *INF*:  IIF(v_symbol_pos_1_2 = 'CP' AND
		--        type_bureau = 'CF' AND
		--        in(major_peril,'415','463','490','496','498','599','919') AND
		--        NOT in(class_of_business,'I','O'),1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND type_bureau = 'CF' AND in(major_peril, '415', '463', '490', '496', '498', '599', '919') AND NOT in(class_of_business, 'I', 'O'), 1, 0) AS v_commercial_property_fire_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NS' AND
		--         type_bureau = 'CF' AND
		--         in(major_peril, '415','463','490','496','498','599','919','425','426','435','455','480') AND
		-- 	  in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		-- 	  policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(v_symbol_pos_1_2 = 'NS' AND type_bureau = 'CF' AND in(major_peril, '415', '463', '490', '496', '498', '599', '919', '425', '426', '435', '455', '480') AND in(v_risk_unit_group_1_3, @{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND policy_effective_date >= CL_EQ_EFF_Date, 1, 0) AS v_commercial_property_earthquake_nsi,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NS' AND
		--         type_bureau = 'CF' AND
		--         in(major_peril, '415','463','490','496','498','599','919'),1,0)
		IFF(v_symbol_pos_1_2 = 'NS' AND type_bureau = 'CF' AND in(major_peril, '415', '463', '490', '496', '498', '599', '919'), 1, 0) AS v_commercial_property_fire_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2 ,'CP','PX','SM') AND
		--         in(type_bureau,'CF','NB','GS') AND
		--          in(major_peril,'425','426','435','220','455','480','599','227') AND
		--          NOT in(class_of_business,'I','O'),1,0)
		IFF(in(v_symbol_pos_1_2, 'CP', 'PX', 'SM') AND in(type_bureau, 'CF', 'NB', 'GS') AND in(major_peril, '425', '426', '435', '220', '455', '480', '599', '227') AND NOT in(class_of_business, 'I', 'O'), 1, 0) AS v_commercial_property_allied_lines_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NS' AND
		--        in(type_bureau,'CF','NB','GS') AND
		--        in(major_peril,'425','426','435','220','455','480','599','227'),1,0)
		IFF(v_symbol_pos_1_2 = 'NS' AND in(type_bureau, 'CF', 'NB', 'GS') AND in(major_peril, '425', '426', '435', '220', '455', '480', '599', '227'), 1, 0) AS v_commercial_property_allied_lines_nsi,
		-- *INF*:  IIF(v_symbol_pos_1_2 = 'CP' AND 
		--        type_bureau = 'CF' AND
		--        in(major_peril,'415','463','490','496','498','599' ,'919','425','426','435','455','480') AND
		--        class_of_business = 'I' AND
		--        in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		--        policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND type_bureau = 'CF' AND in(major_peril, '415', '463', '490', '496', '498', '599', '919', '425', '426', '435', '455', '480') AND class_of_business = 'I' AND in(v_risk_unit_group_1_3, @{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND policy_effective_date >= CL_EQ_EFF_Date, 1, 0) AS v_metalworkers_earthquake_wbm,
		-- *INF*:  IIF(v_symbol_pos_1_2 = 'CP' AND 
		--        type_bureau = 'CF' AND
		--        in(major_peril,'415','463','490','496','498','599' ,'919') AND
		--        class_of_business = 'I',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND type_bureau = 'CF' AND in(major_peril, '415', '463', '490', '496', '498', '599', '919') AND class_of_business = 'I', 1, 0) AS v_metalworkers_fire_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND
		--        in(type_bureau,'CF','NB','GS') AND
		--        in(major_peril,'425','426','435','455','480') AND
		--         class_of_business = 'I',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND in(type_bureau, 'CF', 'NB', 'GS') AND in(major_peril, '425', '426', '435', '455', '480') AND class_of_business = 'I', 1, 0) AS v_metalworkers_allied_lines_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND 
		--       type_bureau = 'IM' AND
		--       in(major_peril,'551','599','919') AND
		--       class_of_business = 'I',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND type_bureau = 'IM' AND in(major_peril, '551', '599', '919') AND class_of_business = 'I', 1, 0) AS v_metalworkers_inland_marine_wbm,
		-- *INF*:  IIF(v_symbol_pos_1_2 = 'CP' AND type_bureau = 'GL' AND
		--        in(major_peril,'530','599','919') AND
		--        in(subline,@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350}) AND
		--        class_of_business = 'I',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND type_bureau = 'GL' AND in(major_peril, '530', '599', '919') AND in(subline, @{pipeline().parameters.SUB_325_335}, @{pipeline().parameters.SUB_342_350}) AND class_of_business = 'I', 1, 0) AS v_metalworkers_general_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND 
		--       type_bureau = 'GL' AND
		--       in(major_peril,'530','550','599') AND
		--       in(subline,'336','365') AND
		--       class_of_business = 'I',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND type_bureau = 'GL' AND in(major_peril, '530', '550', '599') AND in(subline, '336', '365') AND class_of_business = 'I', 1, 0) AS v_metalworkers_products_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CM' AND type_bureau = 'GL' AND
		--              major_peril = '540' AND
		--              subline = '336' AND 
		--              class_of_business = 'I',1,0)
		IFF(v_symbol_pos_1_2 = 'CM' AND type_bureau = 'GL' AND major_peril = '540' AND subline = '336' AND class_of_business = 'I', 1, 0) AS v_metalworkers_claims_made_product_liability_wbm,
		-- *INF*:  IIF(in(v_symbol_pos_1_2,'CP','FF') AND
		--         in(type_bureau,'FT','CR') AND
		--         in(major_peril,'566','016') AND
		--         class_of_business = 'I',1,0)
		IFF(in(v_symbol_pos_1_2, 'CP', 'FF') AND in(type_bureau, 'FT', 'CR') AND in(major_peril, '566', '016') AND class_of_business = 'I', 1, 0) AS v_metalworkers_fidelity_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND
		--       in(type_bureau,'FT','BT','CR') AND
		--       in(major_peril,'565','599') AND
		--       class_of_business = 'I',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND in(type_bureau, 'FT', 'BT', 'CR') AND in(major_peril, '565', '599') AND class_of_business = 'I', 1, 0) AS v_metalworkers_burglary_and_theft_wbm,
		-- *INF*:  IIF(v_symbol_pos_1_2 = 'CP' AND 
		--        type_bureau = 'CF' AND
		--        in(major_peril, '415','463','490','496','498','599','919','425','426','435','455','480') AND
		--        class_of_business = 'O' AND
		--        in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		--        policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND type_bureau = 'CF' AND in(major_peril, '415', '463', '490', '496', '498', '599', '919', '425', '426', '435', '455', '480') AND class_of_business = 'O' AND in(v_risk_unit_group_1_3, @{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND policy_effective_date >= CL_EQ_EFF_Date, 1, 0) AS v_woodworkers_earthquake_wbm,
		-- *INF*:  IIF(v_symbol_pos_1_2 = 'CP' AND 
		--        type_bureau = 'CF' AND
		--        in(major_peril, '415','463','490','496','498','599','919') AND
		--        class_of_business = 'O',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND type_bureau = 'CF' AND in(major_peril, '415', '463', '490', '496', '498', '599', '919') AND class_of_business = 'O', 1, 0) AS v_woodworkers_fire_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND
		--      in(type_bureau,'CF','NB','GS') AND
		--      in(major_peril,'425','426','435','455','480') AND
		--      class_of_business = 'O',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND in(type_bureau, 'CF', 'NB', 'GS') AND in(major_peril, '425', '426', '435', '455', '480') AND class_of_business = 'O', 1, 0) AS v_woodworkers_allied_lines_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND 
		--       type_bureau = 'IM' AND
		--       in(major_peril,'551','599','919') AND
		--       class_of_business = 'O',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND type_bureau = 'IM' AND in(major_peril, '551', '599', '919') AND class_of_business = 'O', 1, 0) AS v_woodworkers_inland_marine_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND 
		--       type_bureau = 'GL' AND
		--       in(major_peril,'530','599','919') AND
		--       in(subline,@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350}) AND
		--       class_of_business = 'O',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND type_bureau = 'GL' AND in(major_peril, '530', '599', '919') AND in(subline, @{pipeline().parameters.SUB_325_335}, @{pipeline().parameters.SUB_342_350}) AND class_of_business = 'O', 1, 0) AS v_woodworkers_general_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND 
		--       type_bureau = 'GL' AND
		--      in(major_peril,'530','550','599') AND
		--      in(subline,'336','365') AND
		--      class_of_business = 'O',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND type_bureau = 'GL' AND in(major_peril, '530', '550', '599') AND in(subline, '336', '365') AND class_of_business = 'O', 1, 0) AS v_woodworkers_products_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CM' AND 
		--       type_bureau = 'GL' AND
		--       major_peril = '540' AND
		--       subline = '336' AND 
		--       class_of_business = 'O',1,0)
		IFF(v_symbol_pos_1_2 = 'CM' AND type_bureau = 'GL' AND major_peril = '540' AND subline = '336' AND class_of_business = 'O', 1, 0) AS v_woodworkers_claims_made_product_liability_wbm,
		-- *INF*:  IIF(in(v_symbol_pos_1_2,'CP','FF') AND
		--         in(type_bureau,'FT','CR') AND
		--         in (major_peril,'566','016') AND
		--         class_of_business = 'O',1,0)
		IFF(in(v_symbol_pos_1_2, 'CP', 'FF') AND in(type_bureau, 'FT', 'CR') AND in(major_peril, '566', '016') AND class_of_business = 'O', 1, 0) AS v_woodworkers_fidelity_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND
		--       in(type_bureau,'FT','BT','CR') AND
		--       in(major_peril,'565','599') AND
		--       class_of_business= 'O',1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND in(type_bureau, 'FT', 'BT', 'CR') AND in(major_peril, '565', '599') AND class_of_business = 'O', 1, 0) AS v_woodworkers_burglary_and_theft_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'CP','GL','SM','XX') AND
		--         type_bureau = 'GL' AND
		--        in(major_peril,'530','599','084','085','919') AND
		--        in(subline,@{pipeline().parameters.SUB_313_315},@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350}) AND
		--        NOT in(class_of_business,'I','O'),1,0)
		IFF(in(v_symbol_pos_1_2, 'CP', 'GL', 'SM', 'XX') AND type_bureau = 'GL' AND in(major_peril, '530', '599', '084', '085', '919') AND in(subline, @{pipeline().parameters.SUB_313_315}, @{pipeline().parameters.SUB_325_335}, @{pipeline().parameters.SUB_342_350}) AND NOT in(class_of_business, 'I', 'O'), 1, 0) AS v_general_liability_general_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NS' AND
		--         type_bureau = 'GL' AND
		--         in(major_peril,'530','599','084','085','919') AND
		--         in(subline,@{pipeline().parameters.SUB_313_315},@{pipeline().parameters.SUB_325_335},@{pipeline().parameters.SUB_342_350}),1,0)
		IFF(v_symbol_pos_1_2 = 'NS' AND type_bureau = 'GL' AND in(major_peril, '530', '599', '084', '085', '919') AND in(subline, @{pipeline().parameters.SUB_313_315}, @{pipeline().parameters.SUB_325_335}, @{pipeline().parameters.SUB_342_350}), 1, 0) AS v_general_liability_general_liability_nsi,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND
		--        type_bureau = 'GL' AND
		--        in(major_peril,'530','550','599') AND
		--        in(subline,'336','365') AND
		--        not in(class_of_business,'I','O'),1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND type_bureau = 'GL' AND in(major_peril, '530', '550', '599') AND in(subline, '336', '365') AND NOT in(class_of_business, 'I', 'O'), 1, 0) AS v_general_liability_products_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'BO' AND
		--        type_bureau = 'GL' AND
		--        in(major_peril,'530','550','599') AND
		--        in(subline,'336','365'),1,0)
		IFF(v_symbol_pos_1_2 = 'BO' AND type_bureau = 'GL' AND in(major_peril, '530', '550', '599') AND in(subline, '336', '365'), 1, 0) AS v_smart_products_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NS' AND
		--        type_bureau = 'GL' AND
		--        in(major_peril,'530','599') AND
		--        subline = '336',1,0)
		IFF(v_symbol_pos_1_2 = 'NS' AND type_bureau = 'GL' AND in(major_peril, '530', '599') AND subline = '336', 1, 0) AS v_general_liability_products_liability_nsi,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CM' AND
		--       type_bureau = 'GL' AND
		--       major_peril = '540' AND
		--       subline = '336' AND
		--       not in(class_of_business,'I','O'),1,0)
		IFF(v_symbol_pos_1_2 = 'CM' AND type_bureau = 'GL' AND major_peril = '540' AND subline = '336' AND NOT in(class_of_business, 'I', 'O'), 1, 0) AS v_general_liability_claims_made_product_liability_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NE','NS') AND
		--             type_bureau = 'GL' AND
		--            major_peril = '540' AND
		--            subline= '336',1,0)
		IFF(in(v_symbol_pos_1_2, 'NE', 'NS') AND type_bureau = 'GL' AND major_peril = '540' AND subline = '336', 1, 0) AS v_general_liability_claims_made_product_liability_nsi,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NS' AND
		--       type_bureau= 'GL' AND
		--      major_peril= '540' AND
		--      subline= '334',1,0)
		IFF(v_symbol_pos_1_2 = 'NS' AND type_bureau = 'GL' AND major_peril = '540' AND subline = '334', 1, 0) AS v_general_liability_claims_made_general_liability_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'CP','SM') AND
		--       in(type_bureau,'FT','BT','CR') AND
		--       in(major_peril,'566','016') AND
		--       not in(class_of_business,'I','O'),1,0)
		IFF(in(v_symbol_pos_1_2, 'CP', 'SM') AND in(type_bureau, 'FT', 'BT', 'CR') AND in(major_peril, '566', '016') AND NOT in(class_of_business, 'I', 'O'), 1, 0) AS v_crime_fidelity_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'BO' AND
		--       in(type_bureau,'FT','BT','CR') AND
		--       in(major_peril,'566','016'),1,0)
		IFF(v_symbol_pos_1_2 = 'BO' AND in(type_bureau, 'FT', 'BT', 'CR') AND in(major_peril, '566', '016'), 1, 0) AS v_smart_fidelity_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NS' AND
		--       in(type_bureau,'FT','BT','CR') AND
		--       in(major_peril,'566','016'),1,0)
		IFF(v_symbol_pos_1_2 = 'NS' AND in(type_bureau, 'FT', 'BT', 'CR') AND in(major_peril, '566', '016'), 1, 0) AS v_crime_fidelity_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'CP','SM') AND
		--        in(type_bureau,'FT','BT','CR') AND
		--        in(major_peril,'565','599','015') AND
		--        not in(class_of_business,'I','O'),1,0)
		IFF(in(v_symbol_pos_1_2, 'CP', 'SM') AND in(type_bureau, 'FT', 'BT', 'CR') AND in(major_peril, '565', '599', '015') AND NOT in(class_of_business, 'I', 'O'), 1, 0) AS v_crime_burglary_and_theft_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NS' AND
		--       in(type_bureau,'FT','BT','CR') AND
		--       in(major_peril,'565','599','015'),1,0)
		IFF(v_symbol_pos_1_2 = 'NS' AND in(type_bureau, 'FT', 'BT', 'CR') AND in(major_peril, '565', '599', '015'), 1, 0) AS v_crime_burglary_and_theft_nsi,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CP' AND
		--       type_bureau = 'IM' AND
		--       in(major_peril,'551','599','919') AND
		--       not in(class_of_business,'I','O'),1,0)
		IFF(v_symbol_pos_1_2 = 'CP' AND type_bureau = 'IM' AND in(major_peril, '551', '599', '919') AND NOT in(class_of_business, 'I', 'O'), 1, 0) AS v_commercial_im_inland_marine_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NS' AND
		--       type_bureau = 'IM' AND
		--       in(major_peril,'551','599','919'),1,0)
		IFF(v_symbol_pos_1_2 = 'NS' AND type_bureau = 'IM' AND in(major_peril, '551', '599', '919'), 1, 0) AS v_commercial_im_inland_marine_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BA','BB') AND
		--        in(type_bureau,'BB','BC','BE','NB') AND
		--        in(major_peril,'903','904','905','908'),1,0)
		IFF(in(v_symbol_pos_1_2, 'BA', 'BB') AND in(type_bureau, 'BB', 'BC', 'BE', 'NB') AND in(major_peril, '903', '904', '905', '908'), 1, 0) AS v_bop_cmp_property_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BA','BB') AND
		--        type_bureau = 'BC' AND
		--        major_peril = '919',1,0)
		IFF(in(v_symbol_pos_1_2, 'BA', 'BB') AND type_bureau = 'BC' AND major_peril = '919', 1, 0) AS v_bop_unnamed_R13905,
		-- *INF*:  IIF(in(v_symbol_pos_1_2,'BA','BB','XA','XX') AND
		--         in(type_bureau,'BB','BC','BE','NB') AND
		--         in(major_peril,'599',@{pipeline().parameters.MP_901_904}),1,0)
		IFF(in(v_symbol_pos_1_2, 'BA', 'BB', 'XA', 'XX') AND in(type_bureau, 'BB', 'BC', 'BE', 'NB') AND in(major_peril, '599', @{pipeline().parameters.MP_901_904}), 1, 0) AS v_bop_cmp_property_liability_peril_901_904_and_599_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BA','BB','XA') AND
		--        in(type_bureau,'BE','B2') AND
		--        in(major_peril,'907','065','919'),1,0)
		IFF(in(v_symbol_pos_1_2, 'BA', 'BB', 'XA') AND in(type_bureau, 'BE', 'B2') AND in(major_peril, '907', '065', '919'), 1, 0) AS v_bop_cmp_liability_wbm,
		-- *INF*:  IIF(in(v_symbol_pos_1_2,'BC','BD') AND
		--         type_bureau = 'CF' AND
		--         in(major_peril,'415','463','490','496','498','599','919','425','426','435','455','480') AND
		-- in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		-- policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(in(v_symbol_pos_1_2, 'BC', 'BD') AND type_bureau = 'CF' AND in(major_peril, '415', '463', '490', '496', '498', '599', '919', '425', '426', '435', '455', '480') AND in(v_risk_unit_group_1_3, @{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND policy_effective_date >= CL_EQ_EFF_Date, 1, 0) AS v_cbop_earthquake_wbm,
		-- *INF*:  IIF(in(v_symbol_pos_1_2,'BC','BD') AND
		--         type_bureau = 'CF' AND
		--         in(major_peril,'415','463','490','496','498','599','919'),1,0)
		IFF(in(v_symbol_pos_1_2, 'BC', 'BD') AND type_bureau = 'CF' AND in(major_peril, '415', '463', '490', '496', '498', '599', '919'), 1, 0) AS v_cbop_fire_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'BO' AND
		--       type_bureau = 'CF' AND
		--       in(major_peril,'415','463','490','496','498','599','919','425','426','435','455','480') AND
		--       in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		--       policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(v_symbol_pos_1_2 = 'BO' AND type_bureau = 'CF' AND in(major_peril, '415', '463', '490', '496', '498', '599', '919', '425', '426', '435', '455', '480') AND in(v_risk_unit_group_1_3, @{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND policy_effective_date >= CL_EQ_EFF_Date, 1, 0) AS v_smart_earthquake_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'BO' AND
		--       type_bureau = 'CF' AND
		--       in(major_peril,'415','463','490','496','498','599','919'),1,0)
		IFF(v_symbol_pos_1_2 = 'BO' AND type_bureau = 'CF' AND in(major_peril, '415', '463', '490', '496', '498', '599', '919'), 1, 0) AS v_smart_fire_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB') AND
		--              type_bureau = 'CF' AND
		--              in(major_peril,'415','463','490','496','498','599','919','425','426','435','455','480') AND
		--              in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		--              policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(in(v_symbol_pos_1_2, 'NA', 'NB') AND type_bureau = 'CF' AND in(major_peril, '415', '463', '490', '496', '498', '599', '919', '425', '426', '435', '455', '480') AND in(v_risk_unit_group_1_3, @{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND policy_effective_date >= CL_EQ_EFF_Date, 1, 0) AS v_sbop_earthquake_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB') AND
		--              type_bureau = 'CF' AND
		--              in(major_peril,'415','463','490','496','498','599','919'),1,0)
		IFF(in(v_symbol_pos_1_2, 'NA', 'NB') AND type_bureau = 'CF' AND in(major_peril, '415', '463', '490', '496', '498', '599', '919'), 1, 0) AS v_sbop_fire_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BC','BD') AND
		--        in(type_bureau,'CF','GS') AND
		--        in(major_peril,'425','426','435','455','480'),1,0)
		IFF(in(v_symbol_pos_1_2, 'BC', 'BD') AND in(type_bureau, 'CF', 'GS') AND in(major_peril, '425', '426', '435', '455', '480'), 1, 0) AS v_cbop_allied_lines_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'BO' AND
		--       in(type_bureau,'CF','GS') AND
		--       in(major_peril,'425','426','435','455','480'),1,0)
		IFF(v_symbol_pos_1_2 = 'BO' AND in(type_bureau, 'CF', 'GS') AND in(major_peril, '425', '426', '435', '455', '480'), 1, 0) AS v_smart_allied_lines_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB') AND
		--        in(type_bureau,'CF','GS') AND
		--        in(major_peril,'425','426','435','455','480'),1,0)
		IFF(in(v_symbol_pos_1_2, 'NA', 'NB') AND in(type_bureau, 'CF', 'GS') AND in(major_peril, '425', '426', '435', '455', '480'), 1, 0) AS v_sbop_allied_lines_nsi,
		-- *INF*:  IIF(in(v_symbol_pos_1_2,'BC','BD') AND
		--         type_bureau = 'CF' AND
		--         major_peril = '066',1,0)
		IFF(in(v_symbol_pos_1_2, 'BC', 'BD') AND type_bureau = 'CF' AND major_peril = '066', 1, 0) AS v_cbop_cmp_property_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BC','BD') AND
		--        type_bureau = 'IM' AND
		--        in(major_peril,'551','599','919'),1,0)
		IFF(in(v_symbol_pos_1_2, 'BC', 'BD') AND type_bureau = 'IM' AND in(major_peril, '551', '599', '919'), 1, 0) AS v_cbop_inland_marine_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'BO' AND
		--        type_bureau = 'IM' AND
		--        in(major_peril,'551','599','919'),1,0)
		IFF(v_symbol_pos_1_2 = 'BO' AND type_bureau = 'IM' AND in(major_peril, '551', '599', '919'), 1, 0) AS v_smart_inland_marine_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB') AND
		--        type_bureau = 'IM' AND
		--        in(major_peril,'551','599','919'),1,0)
		IFF(in(v_symbol_pos_1_2, 'NA', 'NB') AND type_bureau = 'IM' AND in(major_peril, '551', '599', '919'), 1, 0) AS v_sbop_inland_marine_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BC','BD') AND
		--        type_bureau = 'GL' AND
		--        in(major_peril,'530','599','067','919'),1,0)
		IFF(in(v_symbol_pos_1_2, 'BC', 'BD') AND type_bureau = 'GL' AND in(major_peril, '530', '599', '067', '919'), 1, 0) AS v_cbop_general_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'BO' AND
		--        type_bureau = 'GL' AND
		--        in(major_peril,'530','599','919'),1,0)
		IFF(v_symbol_pos_1_2 = 'BO' AND type_bureau = 'GL' AND in(major_peril, '530', '599', '919'), 1, 0) AS v_smart_general_liability_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB') AND
		--        type_bureau = 'GL' AND
		--        in(major_peril,'530','599','067','919'),1,0)
		IFF(in(v_symbol_pos_1_2, 'NA', 'NB') AND type_bureau = 'GL' AND in(major_peril, '530', '599', '067', '919'), 1, 0) AS v_sbop_general_liability_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BC','BD') AND
		--        type_bureau = 'FT' AND
		--         major_peril = '566',1,0)
		IFF(in(v_symbol_pos_1_2, 'BC', 'BD') AND type_bureau = 'FT' AND major_peril = '566', 1, 0) AS v_cbop_fidelity_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB') AND
		--        type_bureau = 'FT' AND
		--        major_peril = '566',1,0)
		IFF(in(v_symbol_pos_1_2, 'NA', 'NB') AND type_bureau = 'FT' AND major_peril = '566', 1, 0) AS v_sbop_fidelity_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BC','BD') AND
		--        in(type_bureau,'FT','BT') AND
		--        in(major_peril,'565','599'),1,0)
		IFF(in(v_symbol_pos_1_2, 'BC', 'BD') AND in(type_bureau, 'FT', 'BT') AND in(major_peril, '565', '599'), 1, 0) AS v_cbop_burglary_and_theft_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'BO' AND
		--       in(type_bureau,'CR','FT','BT') AND
		--       in(major_peril,'565','599'),1,0)
		IFF(v_symbol_pos_1_2 = 'BO' AND in(type_bureau, 'CR', 'FT', 'BT') AND in(major_peril, '565', '599'), 1, 0) AS v_smart_burglary_and_theft_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB') AND
		--        in(type_bureau,'FT','BT') AND
		--        in(major_peril,'565','599'),1,0)
		IFF(in(v_symbol_pos_1_2, 'NA', 'NB') AND in(type_bureau, 'FT', 'BT') AND in(major_peril, '565', '599'), 1, 0) AS v_sbop_burglary_and_theft_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH') AND 
		--        type_bureau = 'CF' AND
		--        in(major_peril,'415','463','490','496','498','599','919','425','426','435','455','480',@{pipeline().parameters.MP_901_904}) AND
		--        in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		--       policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(in(v_symbol_pos_1_2, 'BG', 'BH') AND type_bureau = 'CF' AND in(major_peril, '415', '463', '490', '496', '498', '599', '919', '425', '426', '435', '455', '480', @{pipeline().parameters.MP_901_904}) AND in(v_risk_unit_group_1_3, @{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND policy_effective_date >= CL_EQ_EFF_Date, 1, 0) AS v_gbop_earthquake_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH') AND 
		--        type_bureau = 'CF' AND
		--        in(major_peril,'415','463','490','496','498','599','919'),1,0)
		IFF(in(v_symbol_pos_1_2, 'BG', 'BH') AND type_bureau = 'CF' AND in(major_peril, '415', '463', '490', '496', '498', '599', '919'), 1, 0) AS v_gbop_fire_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH') AND
		--        in(type_bureau,'CF','GS') AND
		--        in(major_peril,'425' ,'426','435','455','480'),1,0)
		IFF(in(v_symbol_pos_1_2, 'BG', 'BH') AND in(type_bureau, 'CF', 'GS') AND in(major_peril, '425', '426', '435', '455', '480'), 1, 0) AS v_gbop_allied_lines_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH') AND 
		--        type_bureau = 'IM' AND
		--        in(major_peril,'551','599','919'),1,0)
		IFF(in(v_symbol_pos_1_2, 'BG', 'BH') AND type_bureau = 'IM' AND in(major_peril, '551', '599', '919'), 1, 0) AS v_gbop_inland_marine_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH') AND 
		--       type_bureau = 'GL' AND
		--       in(major_peril,'530','599','919'),1,0)
		IFF(in(v_symbol_pos_1_2, 'BG', 'BH') AND type_bureau = 'GL' AND in(major_peril, '530', '599', '919'), 1, 0) AS v_gbop_general_liability_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH','GG','XX') AND
		--         in(type_bureau,'AN','AL') AND
		--         in(major_peril,'599',@{pipeline().parameters.MP_100_130},@{pipeline().parameters.MP_271_274},@{pipeline().parameters.MP_930_931}),1,0)
		IFF(in(v_symbol_pos_1_2, 'BG', 'BH', 'GG', 'XX') AND in(type_bureau, 'AN', 'AL') AND in(major_peril, '599', @{pipeline().parameters.MP_100_130}, @{pipeline().parameters.MP_271_274}, @{pipeline().parameters.MP_930_931}), 1, 0) AS v_gbop_commercial_auto_liability_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH') AND
		--        in(type_bureau,'CF','NB','BC','BE') AND
		--        in(major_peril,@{pipeline().parameters.MP_901_904}),1,0)
		IFF(in(v_symbol_pos_1_2, 'BG', 'BH') AND in(type_bureau, 'CF', 'NB', 'BC', 'BE') AND in(major_peril, @{pipeline().parameters.MP_901_904}), 1, 0) AS v_gbop_cmp_property_liability_peril_901_904_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH') AND
		--        type_bureau = 'BE' AND 
		--        major_peril = '907',1,0)
		IFF(in(v_symbol_pos_1_2, 'BG', 'BH') AND type_bureau = 'BE' AND major_peril = '907', 1, 0) AS v_gbop_cmp_liability_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH','GG') AND
		--        type_bureau = 'AP' AND
		--         in(major_peril,'269',@{pipeline().parameters.MP_145_160},@{pipeline().parameters.MP_165_166},@{pipeline().parameters.MP_170_173}),1,0)
		IFF(in(v_symbol_pos_1_2, 'BG', 'BH', 'GG') AND type_bureau = 'AP' AND in(major_peril, '269', @{pipeline().parameters.MP_145_160}, @{pipeline().parameters.MP_165_166}, @{pipeline().parameters.MP_170_173}), 1, 0) AS v_gbop_comm_auto_physical_damage_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH') AND
		--         in( type_bureau,'FT','BT') AND
		--         major_peril = '566',1,0)
		IFF(in(v_symbol_pos_1_2, 'BG', 'BH') AND in(type_bureau, 'FT', 'BT') AND major_peril = '566', 1, 0) AS v_gbop_fidelity_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BG','BH') AND
		--        in(type_bureau,'FT','BT') AND
		--        in(major_peril,'565','599'),1,0)
		IFF(in(v_symbol_pos_1_2, 'BG', 'BH') AND in(type_bureau, 'FT', 'BT') AND in(major_peril, '565', '599'), 1, 0) AS v_gbop_burglary_and_theft_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'UP' AND 
		--        type_bureau = 'GL' AND
		--        major_peril = '017',1,0)
		IFF(v_symbol_pos_1_2 = 'UP' AND type_bureau = 'GL' AND major_peril = '017', 1, 0) AS v_personal_umbrella_general_liability_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'CP','UC','CU') AND
		--        type_bureau = 'GL' AND
		--        major_peril = '517',1,0)
		IFF(in(v_symbol_pos_1_2, 'CP', 'UC', 'CU') AND type_bureau = 'GL' AND major_peril = '517', 1, 0) AS v_commercial_umbrella_general_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NU' AND
		--         type_bureau = 'GL' AND
		--         major_peril = '517',1,0)
		IFF(v_symbol_pos_1_2 = 'NU' AND type_bureau = 'GL' AND major_peril = '517', 1, 0) AS v_commercial_umbrella_general_liability_nsi,
		-- *INF*: IIF(symbol= 'ZZZ',1,0)
		IFF(symbol = 'ZZZ', 1, 0) AS v_workers_comp_pool_total_workers_comp_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NF' AND
		--         in(major_peril,'566','599'),1,0)
		IFF(v_symbol_pos_1_2 = 'NF' AND in(major_peril, '566', '599'), 1, 0) AS v_fidelity_bonds_fidelity_nsi,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NF' AND
		--        major_peril = '565',1,0)
		IFF(v_symbol_pos_1_2 = 'NF' AND major_peril = '565', 1, 0) AS v_fidelity_bonds_burgulary_and_theft_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NC','NJ','NL','NO','NM'),1,0)
		IFF(in(v_symbol_pos_1_2, 'NC', 'NJ', 'NL', 'NO', 'NM'), 1, 0) AS v_surety_bonds_surety_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'CD','CM') AND
		--        in(subline,'345','334') AND
		--        in(major_peril,'540','599','919'),1,0)
		IFF(in(v_symbol_pos_1_2, 'CD', 'CM') AND in(subline, '345', '334') AND in(major_peril, '540', '599', '919'), 1, 0) AS v_d_and_o_claims_made_general_liability_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'CM',1,0)
		IFF(v_symbol_pos_1_2 = 'CM', 1, 0) AS v_epli_general_liability_claims_made_wbm,
		-- *INF*: IIF(v_symbol_pos_1_2 = 'NE',1,0)
		IFF(v_symbol_pos_1_2 = 'NE', 1, 0) AS v_epli_general_liability_claims_made_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BC','BD','BG','BH','CP') AND
		--         major_peril = '540' AND
		--         type_bureau = 'GL' AND
		--         (
		--         in(substr(class_code,1,5),'22222','22250') OR
		--         in(substr(risk_unit_group,1,3),'366','367')
		--         )
		-- 	 ,1,0)
		IFF(in(v_symbol_pos_1_2, 'BC', 'BD', 'BG', 'BH', 'CP') AND major_peril = '540' AND type_bureau = 'GL' AND ( in(substr(class_code, 1, 5), '22222', '22250') OR in(substr(risk_unit_group, 1, 3), '366', '367') ), 1, 0) AS v_epli_general_liability_claims_made_cpp_cbop_wbm,
		-- *INF*:  IIF(v_symbol_pos_1_2 = 'BO' AND
		--        in(major_peril,'540','541') AND
		--        type_bureau = 'GL' AND
		--       (
		--        in(substr(class_code,1,5),'22222','22250') OR
		--        in(substr(risk_unit_group,1,3),'366','367')
		--        )
		--        ,1,0)
		IFF(v_symbol_pos_1_2 = 'BO' AND in(major_peril, '540', '541') AND type_bureau = 'GL' AND ( in(substr(class_code, 1, 5), '22222', '22250') OR in(substr(risk_unit_group, 1, 3), '366', '367') ), 1, 0) AS v_smart_general_liability_claims_made_smart_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB','NS') AND
		--        major_peril = '540' AND
		--        type_bureau = 'GL' AND
		--       (
		--        in(substr(class_code,1,5),'22222','22250') OR
		--        in(substr(risk_unit_group,1,3),'366','367')
		--        )
		--        ,1,0)
		IFF(in(v_symbol_pos_1_2, 'NA', 'NB', 'NS') AND major_peril = '540' AND type_bureau = 'GL' AND ( in(substr(class_code, 1, 5), '22222', '22250') OR in(substr(risk_unit_group, 1, 3), '366', '367') ), 1, 0) AS v_epli_general_liability_claims_made_cpp_sbop_nsi,
		-- *INF*: IIF(major_peril = '540' AND
		--        type_bureau = 'BE' AND
		--        in(substr(risk_unit_group,1,3),'366','367'),1,0)
		IFF(major_peril = '540' AND type_bureau = 'BE' AND in(substr(risk_unit_group, 1, 3), '366', '367'), 1, 0) AS v_epli_general_liability_claims_made_bop_wbm,
		-- *INF*: IIF(major_peril = '540' AND
		--        type_bureau = 'AL' AND
		--        in(substr(risk_unit_group,1,3),'417','418'),1,0)
		IFF(major_peril = '540' AND type_bureau = 'AL' AND in(substr(risk_unit_group, 1, 3), '417', '418'), 1, 0) AS v_epli_general_liability_claims_made_gbop_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NK','NN'),1,0)
		IFF(in(v_symbol_pos_1_2, 'NK', 'NN'), 1, 0) AS v_d_and_o_claims_made_general_liability_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BA','BB','BC','BD','BG','BH','BO','CP') AND
		--        in(type_bureau,'CF','BE','BM') AND
		--        in(major_peril,'570','906') AND
		--        in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		--        policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(in(v_symbol_pos_1_2, 'BA', 'BB', 'BC', 'BD', 'BG', 'BH', 'BO', 'CP') AND in(type_bureau, 'CF', 'BE', 'BM') AND in(major_peril, '570', '906') AND in(v_risk_unit_group_1_3, @{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND policy_effective_date >= CL_EQ_EFF_Date, 1, 0) AS v_boiler_and_machinery_earthquake_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'BA','BB','BC','BD','BG','BH','BO','CP') AND
		--        in(type_bureau,'CF','BE','BM') AND
		--        in(major_peril,'570','906'),1,0)
		IFF(in(v_symbol_pos_1_2, 'BA', 'BB', 'BC', 'BD', 'BG', 'BH', 'BO', 'CP') AND in(type_bureau, 'CF', 'BE', 'BM') AND in(major_peril, '570', '906'), 1, 0) AS v_boiler_and_machinery_boiler_and_machinery_wbm,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB','NS') AND
		--        in(type_bureau,'CF','BE', 'BM') AND
		--        in(major_peril,'570','906') AND
		--        in(v_risk_unit_group_1_3,@{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND
		--        policy_effective_date >= CL_EQ_EFF_Date
		-- ,1,0)
		IFF(in(v_symbol_pos_1_2, 'NA', 'NB', 'NS') AND in(type_bureau, 'CF', 'BE', 'BM') AND in(major_peril, '570', '906') AND in(v_risk_unit_group_1_3, @{pipeline().parameters.RISK_UNIT_GRP_CL_EQ}) AND policy_effective_date >= CL_EQ_EFF_Date, 1, 0) AS v_boiler_and_machinery_earthquake_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'NA','NB','NS') AND
		--        in(type_bureau,'CF','BE','BM') AND
		--        in(major_peril,'570','906'),1,0)
		IFF(in(v_symbol_pos_1_2, 'NA', 'NB', 'NS') AND in(type_bureau, 'CF', 'BE', 'BM') AND in(major_peril, '570', '906'), 1, 0) AS v_boiler_and_machinery_boiler_and_machinery_nsi,
		-- *INF*: IIF(in(v_symbol_pos_1_2,'HA','HB','HH') AND
		--        type_bureau = 'MS' AND
		--        major_peril = '050',1,0)
		IFF(in(v_symbol_pos_1_2, 'HA', 'HB', 'HH') AND type_bureau = 'MS' AND major_peril = '050', 1, 0) AS v_mine_subsidence_homeowners_wbm,
		-- *INF*:  IIF(substr(symbol,1,1) != 'N' AND
		--        in(type_bureau,'MS','NB') AND
		--         major_peril = '050',1,0)
		IFF(substr(symbol, 1, 1) != 'N' AND in(type_bureau, 'MS', 'NB') AND major_peril = '050', 1, 0) AS v_mine_subsidence_allied_lines_wbm,
		-- *INF*:  IIF(substr(symbol,1,1) = 'N' AND
		--        in(type_bureau,'MS','NB') AND
		--         major_peril = '050',1,0)
		IFF(substr(symbol, 1, 1) = 'N' AND in(type_bureau, 'MS', 'NB') AND major_peril = '050', 1, 0) AS v_mine_subsidence_allied_lines_nsi,
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
		'1500') AS Path_Flag_Step_1,
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
		'999') AS product_code,
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
		v_bop_cmp_property_liability_peril_901_904_and_599_wbm, to_char(:UDF.ASL_400_PERIL_901_904_LIABILITY(loss_cause, 80)),
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
		'999') AS coverage_code,
		-- *INF*: DECODE(1,
		-- v_mine_subsidence_homeowners_wbm,1,
		-- v_mine_subsidence_allied_lines_wbm,1,
		-- v_mine_subsidence_allied_lines_nsi,1,
		-- 0)
		DECODE(1,
		v_mine_subsidence_homeowners_wbm, 1,
		v_mine_subsidence_allied_lines_wbm, 1,
		v_mine_subsidence_allied_lines_nsi, 1,
		0) AS mine_sub_special_out
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
		IFF(Path_Flag_Step_1 = '2000', 1, 0) AS flag_2000,
		-- *INF*: IIF(Path_Flag_Step_1='2020',1,0)
		IFF(Path_Flag_Step_1 = '2020', 1, 0) AS flag_2020,
		-- *INF*: IIF(Path_Flag_Step_1='2050',1,0)
		IFF(Path_Flag_Step_1 = '2050', 1, 0) AS flag_2050,
		-- *INF*: IIF(Path_Flag_Step_1='2070',1,0)
		IFF(Path_Flag_Step_1 = '2070', 1, 0) AS flag_2070,
		-- *INF*: IIF(Path_Flag_Step_1='2080',1,0)
		IFF(Path_Flag_Step_1 = '2080', 1, 0) AS flag_2080,
		-- *INF*: IIF(Path_Flag_Step_1='2100',1,0)
		IFF(Path_Flag_Step_1 = '2100', 1, 0) AS flag_2100,
		-- *INF*: IIF(Path_Flag_Step_1='2110',1,0)
		IFF(Path_Flag_Step_1 = '2110', 1, 0) AS flag_2110,
		-- *INF*: IIF(Path_Flag_Step_1='2130',1,0)
		IFF(Path_Flag_Step_1 = '2130', 1, 0) AS flag_2130,
		-- *INF*: IIF(Path_Flag_Step_1='2150',1,0)
		IFF(Path_Flag_Step_1 = '2150', 1, 0) AS flag_2150
		FROM EXP_evaluate_step_1
	),
	EXP_2070 AS (
		SELECT
		major_peril,
		-- *INF*: IIF(in(major_peril,'132','147','177','270',@{pipeline().parameters.MP_145_146},@{pipeline().parameters.MP_148_155},@{pipeline().parameters.MP_157_160},@{pipeline().parameters.MP_163_166}),1,0)
		IFF(in(major_peril, '132', '147', '177', '270', @{pipeline().parameters.MP_145_146}, @{pipeline().parameters.MP_148_155}, @{pipeline().parameters.MP_157_160}, @{pipeline().parameters.MP_163_166}), 1, 0) AS v_Comp,
		-- *INF*: IIF(in(major_peril,'156','178','269',@{pipeline().parameters.MP_170_173}),1,0)
		IFF(in(major_peril, '156', '178', '269', @{pipeline().parameters.MP_170_173}), 1, 0) AS v_Coll,
		-- *INF*: DECODE(1,
		-- v_Comp,'520',
		-- v_Coll,'540',
		-- '999')
		DECODE(1,
		v_Comp, '520',
		v_Coll, '540',
		'999') AS coverage_code_2070,
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
		IFF(nsi_indicator = 'W', '0008', '0094') AS reins_co_number_mine_sub,
		'1500' AS Path_Flag_Step_2_mine_sub
		FROM EXP_router_step_1
	),
	EXP_2050 AS (
		SELECT
		type_bureau,
		major_peril,
		-- *INF*: IIF(type_bureau='AN' and in(major_peril,'130',@{pipeline().parameters.MP_930_931}),1,0)
		IFF(type_bureau = 'AN' AND in(major_peril, '130', @{pipeline().parameters.MP_930_931}), 1, 0) AS v_PIP,
		-- *INF*: IIF(type_bureau='AL' and in(major_peril,'150',@{pipeline().parameters.MP_100_125},@{pipeline().parameters.MP_140_143}),1,0)
		IFF(type_bureau = 'AL' AND in(major_peril, '150', @{pipeline().parameters.MP_100_125}, @{pipeline().parameters.MP_140_143}), 1, 0) AS v_Other_Than_PIP,
		-- *INF*: DECODE(1,
		-- v_PIP,'2060',
		-- v_Other_Than_PIP,'2060',
		-- '1500')
		DECODE(1,
		v_PIP, '2060',
		v_Other_Than_PIP, '2060',
		'1500') AS Path_Flag_Step_2_2050,
		-- *INF*: DECODE(1,
		-- v_PIP,'360',
		-- v_Other_Than_PIP,'380',
		-- '999')
		DECODE(1,
		v_PIP, '360',
		v_Other_Than_PIP, '380',
		'999') AS coverage_code_2050
		FROM EXP_router_step_1
	),
	EXP_2020 AS (
		SELECT
		major_peril,
		-- *INF*: IIF(in(major_peril,'168','169','174','912',@{pipeline().parameters.MP_145_149},@{pipeline().parameters.MP_151_155},@{pipeline().parameters.MP_157_163}),1,0)
		IFF(in(major_peril, '168', '169', '174', '912', @{pipeline().parameters.MP_145_149}, @{pipeline().parameters.MP_151_155}, @{pipeline().parameters.MP_157_163}), 1, 0) AS v_Comp,
		-- *INF*: IIF(in(major_peril,'156','178',@{pipeline().parameters.MP_170_173}),1,0)
		IFF(in(major_peril, '156', '178', @{pipeline().parameters.MP_170_173}), 1, 0) AS v_Coll,
		'1500' AS Path_Flag_Step_2_2020,
		-- *INF*: DECODE(1,
		-- v_Comp,'460',
		-- v_Coll, '480',
		-- '999')
		DECODE(1,
		v_Comp, '460',
		v_Coll, '480',
		'999') AS coverage_code_2020
		FROM EXP_router_step_1
	),
	EXP_Perform_2080 AS (
		SELECT
		type_bureau,
		major_peril,
		-- *INF*: IIF(type_bureau='AN' and in(major_peril,'130',@{pipeline().parameters.MP_930_931}),1,0)
		IFF(type_bureau = 'AN' AND in(major_peril, '130', @{pipeline().parameters.MP_930_931}), 1, 0) AS v_GA_Comm_Auto_PIP,
		-- *INF*: IIF(type_bureau='AL' and in(major_peril,'599',@{pipeline().parameters.MP_100_125},@{pipeline().parameters.MP_271_274}),1,0)
		IFF(type_bureau = 'AL' AND in(major_peril, '599', @{pipeline().parameters.MP_100_125}, @{pipeline().parameters.MP_271_274}), 1, 0) AS v_GA_Comm_Auto_Other_Than_PIP,
		-- *INF*: DECODE(1,
		-- v_GA_Comm_Auto_PIP,'2090',
		-- v_GA_Comm_Auto_Other_Than_PIP,'2090',
		-- '1500')
		-- 
		DECODE(1,
		v_GA_Comm_Auto_PIP, '2090',
		v_GA_Comm_Auto_Other_Than_PIP, '2090',
		'1500') AS Path_Flag_Step_2_2080,
		-- *INF*: DECODE(1,
		-- v_GA_Comm_Auto_PIP,'360',
		-- v_GA_Comm_Auto_Other_Than_PIP,'380',
		-- '999')
		DECODE(1,
		v_GA_Comm_Auto_PIP, '360',
		v_GA_Comm_Auto_Other_Than_PIP, '380',
		'999') AS coverage_code_2080
		FROM EXP_router_step_1
	),
	EXP_2130 AS (
		SELECT
		major_peril,
		-- *INF*: IIF(in(major_peril,'132','177',@{pipeline().parameters.MP_145_155},@{pipeline().parameters.MP_157_160},@{pipeline().parameters.MP_165_166}),1,0)
		IFF(in(major_peril, '132', '177', @{pipeline().parameters.MP_145_155}, @{pipeline().parameters.MP_157_160}, @{pipeline().parameters.MP_165_166}), 1, 0) AS v_Comp,
		-- *INF*: IIF(in(major_peril,'156','178','269',@{pipeline().parameters.MP_170_173}),1,0)
		IFF(in(major_peril, '156', '178', '269', @{pipeline().parameters.MP_170_173}), 1, 0) AS v_Coll,
		-- *INF*: DECODE(1,
		-- v_Comp,'520',
		-- v_Coll,'540',
		-- '999')
		-- 
		DECODE(1,
		v_Comp, '520',
		v_Coll, '540',
		'999') AS coverage_code_2130,
		'1500' AS Path_Flag_Step_2_2130
		FROM EXP_router_step_1
	),
	EXP_2150 AS (
		SELECT
		loss_cause,
		-- *INF*: IIF(in(loss_cause,'05','75'),1,0)
		IFF(in(loss_cause, '05', '75'), 1, 0) AS v_Indemnity,
		-- *INF*: IIF(in(loss_cause,'06','07'),1,0)
		IFF(in(loss_cause, '06', '07'), 1, 0) AS v_Medical,
		-- *INF*: DECODE(1,
		-- v_Indemnity,'180',
		-- v_Medical,'190',
		-- '999')
		-- --this actually has no default condition in code
		DECODE(1,
		v_Indemnity, '180',
		v_Medical, '190',
		'999') AS coverage_code_2150,
		'1500' AS Path_Flag_Step_2_2150
		FROM EXP_router_step_1
	),
	EXP_2000 AS (
		SELECT
		type_bureau,
		major_peril,
		-- *INF*: IIF(type_bureau='RN' and major_peril='130',1,0)
		IFF(type_bureau = 'RN' AND major_peril = '130', 1, 0) AS v_PIP,
		-- *INF*: IIF(type_bureau='RL' and in(major_peril,'150',@{pipeline().parameters.MP_100_121},@{pipeline().parameters.MP_140_143}),1,0)
		IFF(type_bureau = 'RL' AND in(major_peril, '150', @{pipeline().parameters.MP_100_121}, @{pipeline().parameters.MP_140_143}), 1, 0) AS v_Other_than_PIP,
		-- *INF*: DECODE(1,
		-- v_PIP,'270',
		-- v_Other_than_PIP,'280',
		-- '999')
		DECODE(1,
		v_PIP, '270',
		v_Other_than_PIP, '280',
		'999') AS coverage_code_2000,
		-- *INF*: DECODE(1,
		-- v_PIP,'2010',
		-- v_Other_than_PIP,'2010',
		-- '1500')
		DECODE(1,
		v_PIP, '2010',
		v_Other_than_PIP, '2010',
		'1500') AS Path_Flag_Step_2_2000
		FROM EXP_router_step_1
	),
	EXP_2100 AS (
		SELECT
		major_peril,
		-- *INF*: IIF(in(major_peril,'132','177','270',@{pipeline().parameters.MP_145_146},@{pipeline().parameters.MP_148_155},@{pipeline().parameters.MP_157_160},@{pipeline().parameters.MP_165_166}),1,0)
		IFF(in(major_peril, '132', '177', '270', @{pipeline().parameters.MP_145_146}, @{pipeline().parameters.MP_148_155}, @{pipeline().parameters.MP_157_160}, @{pipeline().parameters.MP_165_166}), 1, 0) AS v_Comp,
		-- *INF*: IIF(in(major_peril,'156','178','269',@{pipeline().parameters.MP_170_173}),1,0)
		IFF(in(major_peril, '156', '178', '269', @{pipeline().parameters.MP_170_173}), 1, 0) AS v_Coll,
		-- *INF*: DECODE(1,
		-- v_Comp,'520',
		-- v_Coll,'540',
		-- '999')
		DECODE(1,
		v_Comp, '520',
		v_Coll, '540',
		'999') AS coverage_code_2100,
		'1500' AS Path_Flag_Step_2_2100
		FROM EXP_router_step_1
	),
	EXP_perform_2110 AS (
		SELECT
		type_bureau,
		major_peril,
		-- *INF*: IIF(type_bureau='AN' and in(major_peril,'130',@{pipeline().parameters.MP_930_931}),1,0)
		IFF(type_bureau = 'AN' AND in(major_peril, '130', @{pipeline().parameters.MP_930_931}), 1, 0) AS v_PIP,
		-- *INF*: IIF(type_bureau='AL' and in(major_peril,'599',@{pipeline().parameters.MP_100_125},@{pipeline().parameters.MP_271_274}),1,0)
		IFF(type_bureau = 'AL' AND in(major_peril, '599', @{pipeline().parameters.MP_100_125}, @{pipeline().parameters.MP_271_274}), 1, 0) AS v_Other_than_PIP,
		-- *INF*: DECODE(1,
		-- v_PIP,'2120',
		-- v_Other_than_PIP,'2120',
		-- '1500')
		DECODE(1,
		v_PIP, '2120',
		v_Other_than_PIP, '2120',
		'1500') AS Path_Flag_Step_2_2110,
		-- *INF*: DECODE(1,
		-- v_PIP,'360',
		-- v_Other_than_PIP,'380',
		-- '999')
		DECODE(1,
		v_PIP, '360',
		v_Other_than_PIP, '380',
		'999') AS coverage_code_2110
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
		'N/A') AS coverage_code_step_2,
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
		'1500') AS Path_Flag_Step_3,
		-- *INF*: IIF(Path_Flag_Step_3='2010',1,0)
		IFF(Path_Flag_Step_3 = '2010', 1, 0) AS flag_2010,
		-- *INF*: IIF(Path_Flag_Step_3='2060',1,0)
		IFF(Path_Flag_Step_3 = '2060', 1, 0) AS flag_2060,
		-- *INF*: IIF(Path_Flag_Step_3='2090',1,0)
		IFF(Path_Flag_Step_3 = '2090', 1, 0) AS flag_2090,
		-- *INF*: IIF(Path_Flag_Step_3='2120',1,0)
		IFF(Path_Flag_Step_3 = '2120', 1, 0) AS flag_2120
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
		IFF(in(major_peril, '130', @{pipeline().parameters.MP_101_103}, @{pipeline().parameters.MP_114_119}, @{pipeline().parameters.MP_930_931}), 1, 0) AS v_BI,
		-- *INF*: IIF(in(major_peril,@{pipeline().parameters.MP_110_112},@{pipeline().parameters.MP_120_125}),1,0)
		IFF(in(major_peril, @{pipeline().parameters.MP_110_112}, @{pipeline().parameters.MP_120_125}), 1, 0) AS v_PD,
		-- *INF*: IIF(in(major_peril,'100','599',@{pipeline().parameters.MP_271_274}),1,0)
		IFF(in(major_peril, '100', '599', @{pipeline().parameters.MP_271_274}), 1, 0) AS v_MP,
		-- *INF*: DECODE(1,
		-- v_BI,'400',
		-- v_PD,'420',
		-- v_MP,to_char(:UDF.ASL_300_PERIL_100_PD(loss_cause,400)),
		-- '999'
		-- )
		DECODE(1,
		v_BI, '400',
		v_PD, '420',
		v_MP, to_char(:UDF.ASL_300_PERIL_100_PD(loss_cause, 400)),
		'999') AS coverage_code_2120
		FROM EXP_union_router_Step_2
	),
	EXP_2010 AS (
		SELECT
		major_peril,
		loss_cause,
		-- *INF*: IIF(in(major_peril,'130',@{pipeline().parameters.MP_101_103},@{pipeline().parameters.MP_114_119},@{pipeline().parameters.MP_140_143}),1,0)
		IFF(in(major_peril, '130', @{pipeline().parameters.MP_101_103}, @{pipeline().parameters.MP_114_119}, @{pipeline().parameters.MP_140_143}), 1, 0) AS v_BI,
		-- *INF*: IIF(in(major_peril,@{pipeline().parameters.MP_110_112},@{pipeline().parameters.MP_120_121}),1,0)
		IFF(in(major_peril, @{pipeline().parameters.MP_110_112}, @{pipeline().parameters.MP_120_121}), 1, 0) AS v_PD,
		-- *INF*: IIF(major_peril='100',1,0)
		IFF(major_peril = '100', 1, 0) AS v_MP,
		-- *INF*: DECODE(1,
		-- v_BI,'300',
		-- v_PD,'320',
		-- v_MP,to_char(:UDF.ASL_300_PERIL_100_PD(loss_cause,300)),
		-- '999')
		-- 
		DECODE(1,
		v_BI, '300',
		v_PD, '320',
		v_MP, to_char(:UDF.ASL_300_PERIL_100_PD(loss_cause, 300)),
		'999') AS coverage_code_2010
		FROM EXP_union_router_Step_2
	),
	EXP_2060 AS (
		SELECT
		major_peril,
		loss_cause,
		-- *INF*: IIF(in(major_peril,'130','150',@{pipeline().parameters.MP_101_103},@{pipeline().parameters.MP_114_119},@{pipeline().parameters.MP_140_143},@{pipeline().parameters.MP_930_931}),1,0)
		IFF(in(major_peril, '130', '150', @{pipeline().parameters.MP_101_103}, @{pipeline().parameters.MP_114_119}, @{pipeline().parameters.MP_140_143}, @{pipeline().parameters.MP_930_931}), 1, 0) AS v_BI,
		-- *INF*: IIF(in(major_peril,@{pipeline().parameters.MP_110_112},@{pipeline().parameters.MP_120_125}),1,0)
		IFF(in(major_peril, @{pipeline().parameters.MP_110_112}, @{pipeline().parameters.MP_120_125}), 1, 0) AS v_PD,
		-- *INF*: IIF(in(major_peril,'100','599'),1,0)
		IFF(in(major_peril, '100', '599'), 1, 0) AS v_MP,
		-- *INF*: DECODE(1,
		-- v_BI,'400',
		-- v_PD,'420',
		-- v_MP,to_char(:UDF.ASL_300_PERIL_100_PD(loss_cause,400)),
		-- '999')
		DECODE(1,
		v_BI, '400',
		v_PD, '420',
		v_MP, to_char(:UDF.ASL_300_PERIL_100_PD(loss_cause, 400)),
		'999') AS coverage_code_2060
		FROM EXP_union_router_Step_2
	),
	EXP_2090 AS (
		SELECT
		major_peril,
		loss_cause,
		-- *INF*: IIF(in(major_peril,'130',@{pipeline().parameters.MP_101_103},@{pipeline().parameters.MP_114_119},@{pipeline().parameters.MP_930_931}),1,0)
		IFF(in(major_peril, '130', @{pipeline().parameters.MP_101_103}, @{pipeline().parameters.MP_114_119}, @{pipeline().parameters.MP_930_931}), 1, 0) AS v_GA_Comm_Auto_Bi,
		-- *INF*: IIF(in(major_peril,@{pipeline().parameters.MP_110_112},@{pipeline().parameters.MP_120_125}),1,0)
		IFF(in(major_peril, @{pipeline().parameters.MP_110_112}, @{pipeline().parameters.MP_120_125}), 1, 0) AS v_GA_Comm_Auto_Pd,
		-- *INF*: IIF(in(major_peril,'100','599',@{pipeline().parameters.MP_271_274}),1,0)
		IFF(in(major_peril, '100', '599', @{pipeline().parameters.MP_271_274}), 1, 0) AS v_GA_Comm_Auto,
		-- *INF*: DECODE(1,
		-- v_GA_Comm_Auto_Bi,'400',
		-- v_GA_Comm_Auto_Pd,'420',
		-- v_GA_Comm_Auto,to_char(:UDF.ASL_300_PERIL_100_PD(loss_cause,400)),
		-- '999')
		DECODE(1,
		v_GA_Comm_Auto_Bi, '400',
		v_GA_Comm_Auto_Pd, '420',
		v_GA_Comm_Auto, to_char(:UDF.ASL_300_PERIL_100_PD(loss_cause, 400)),
		'999') AS coverage_code_2090
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
		'N/A') AS coverage_code_3
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
		coverage_code_step_2) AS O_coverage_code_step_2,
		coverage_code_3,
		-- *INF*: DECODE(TRUE,
		-- coverage_code_1 = '40','421',
		-- coverage_code_3)
		DECODE(TRUE,
		coverage_code_1 = '40', '421',
		coverage_code_3) AS O_coverage_code_3
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
EXP_view_ASL_mapplet_output AS (
	SELECT
	product_code,
	coverage_code_1,
	coverage_code_2,
	coverage_code_3
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
		asl_dim.asl_dim_id as asl_dim_id, 
		asl_dim.asl_code as asl_code, 
		asl_dim.sub_asl_code as sub_asl_code, 
		asl_dim.sub_non_asl_code as sub_non_asl_code 
		FROM asl_dim
		WHERE
		crrnt_snpsht_flag=1
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
mplt_hierarchy_product_mapping AS (WITH
	INPUT_hierarchy_product_input AS (
		
	),
	EXP_input AS (
		SELECT
		Symbol AS symbol,
		-- *INF*: iif(symbol <> 'N/A',substr(symbol,1,2),symbol)
		IFF(symbol <> 'N/A', substr(symbol, 1, 2), symbol) AS symbol_2pos,
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
		IN(symbol_2pos, 'CP', 'NS') AND insurance_line = 'GL' AND IN(major_peril, '530', '599') AND RTRIM(class_code) = '99999' AND IN(sub_line, '334', '336'), '320',
		IN(symbol_2pos, 'CP', 'NS') AND type_bureau = 'IM', '550',
		symbol_2pos = 'CP' AND insurance_line = 'GL' AND sub_line = '365', '380',
		IN(symbol_2pos, 'CP', 'NS') AND insurance_line = 'GL' AND IN(major_peril, '599', '919') AND IN(risk_unit_group, '345', '367'), '300',
		IN(symbol_2pos, 'CP', 'NS') AND insurance_line = 'GL' AND IN(major_peril, '530', '540', '919', '599') AND RTRIM(class_code) <> '99999' AND NOT IN(risk_unit_group, '345', '346', '355', '900', '901', '367', '286', '365'), '300',
		IN(symbol_2pos, 'CF', 'CP', 'NS') AND IN(insurance_line, 'BM', 'CF', 'CG', 'CR', 'GS', 'N/A') AND NOT IN(type_bureau, 'AL', 'AP', 'AN', 'GL', 'IM'), '500',
		IN(symbol_2pos, 'BC', 'BD', 'BG', 'BH', 'CA', 'CP', 'NB', 'NS', 'NA', 'XX') AND IN(insurance_line, 'N/A', 'CA') AND IN(type_bureau, 'AL', 'AP', 'AN'), '200',
		IN(symbol_2pos, 'CP', 'NS') AND insurance_line = 'GL' AND risk_unit_group = '355', '370',
		IN(symbol_2pos, 'BA', 'BB', 'XX') AND IN(line_of_business, 'BOP', 'BO') AND NOT IN(insurance_line, 'CA'), '400',
		symbol_2pos = 'CM' AND insurance_line = 'GL' AND IN(risk_unit_group, '901', '902', '903'), '360',
		IN(symbol_2pos, 'CP', 'NS') AND insurance_line = 'GL' AND risk_unit_group = '345', '365',
		IN(symbol_2pos, 'CU', 'NU', 'CP', 'UC') AND type_bureau = 'GL' AND IN(major_peril, '517'), '900',
		IN(symbol_2pos, 'BC', 'BD') AND IN(insurance_line, 'CF', 'GL', 'CR', 'IM', 'CG', 'N/A'), '410',
		symbol_2pos = 'CP' AND insurance_line = 'GL' AND risk_unit_group = '346', '321',
		IN(symbol_2pos, 'NA', 'NB') AND IN(insurance_line, 'CF', 'GL', 'CR', 'IM', 'CG'), '430',
		IN(symbol_2pos, 'BG', 'BH', 'GG') AND IN(insurance_line, 'CF', 'GL', 'CR', 'IM', 'GA', 'CG', 'N/A'), '420',
		symbol_2pos = 'NF' AND IN(class_of_business, 'XN', 'XO', 'XP', 'XQ'), '620',
		IN(symbol_2pos, 'CD', 'CM') AND IN(risk_unit_group, '367', '900'), '350',
		IN(symbol_2pos, 'BA', 'BB') AND insurance_line = 'GL' AND IN(risk_unit_group, '110', '111'), '200',
		IN(symbol_2pos, 'CP', 'NS') AND insurance_line = 'GA', '340',
		IN(symbol_2pos, 'HH', 'HA', 'HB', 'HX', 'IB', 'IP', 'PA', 'PX', 'XX') AND IN(type_bureau, 'PH', 'PI', 'PL', 'PQ', 'MS'), '800',
		symbol_2pos = 'NF' AND class_of_business = '9', '510',
		symbol_2pos = 'BO', '450',
		IN(symbol_2pos, 'GL', 'XX') AND IN(major_peril, '084', '085'), '300',
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
		IN(symbol_2pos, 'FL', 'FP') AND IN(type_bureau, 'PF', 'PQ', 'MS'), '820',
		symbol_2pos = 'HH' AND type_bureau = 'PF', '820',
		IN(symbol_2pos, 'HH', 'PA', 'PM', 'PP', 'PS', 'PT', 'HA', 'XX', 'XA') AND IN(type_bureau, 'RL', 'RP', 'RN'), '850',
		IN(symbol_2pos, 'HH', 'UP', 'HX', 'XX') AND type_bureau = 'GL' AND major_peril = '017', '890',
		'000') AS v_product_code,
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
		IFF(prdct_code_dim_id IS NULL, - 1, prdct_code_dim_id) AS prdct_code_dim_id_out,
		prdct_code_descript,
		-- *INF*: IIF(isnull(prdct_code_descript),'N/A',rtrim(ltrim(prdct_code_descript)))
		IFF(prdct_code_descript IS NULL, 'N/A', rtrim(ltrim(prdct_code_descript))) AS prdct_code_descript_out,
		prdct_code,
		-- *INF*: IIF(isnull(prdct_code),'N/A',rtrim(ltrim(prdct_code)))
		IFF(prdct_code IS NULL, 'N/A', rtrim(ltrim(prdct_code))) AS prdct_code_out
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
EXP_merge_update_data AS (
	SELECT
	LKP_asl_dim.asl_dim_id,
	LKP_asl_product_code_dim.asl_prdct_code_dim_id,
	-- *INF*: IIF(ISNULL(asl_dim_id),-1,asl_dim_id)
	IFF(asl_dim_id IS NULL, - 1, asl_dim_id) AS asl_dim_id_out,
	-- *INF*: IIF(ISNULL(asl_prdct_code_dim_id),-1,asl_prdct_code_dim_id)
	IFF(asl_prdct_code_dim_id IS NULL, - 1, asl_prdct_code_dim_id) AS asl_prdct_code_dim_id_out,
	mplt_ASL_WBC8827B_Product_Coverage_Codes.kind_code_mine_sub,
	mplt_ASL_WBC8827B_Product_Coverage_Codes.facultative_ind_mine_sub,
	mplt_ASL_WBC8827B_Product_Coverage_Codes.reins_co_number_mine_sub,
	mplt_ASL_Policy_Symbol_Changes.major_peril1 AS major_peril_code,
	EXP_Source.loss_master_fact_id AS loss_master_fact_id1,
	EXP_Source.edw_claim_trans_pk_id AS edw_claim_trans_pk_id1,
	EXP_Source.wc_stage_loss_master_pk_id AS wc_stage_loss_master_pk_id1,
	EXP_Source.claim_trans_type_dim_id AS claim_trans_type_dim_id1,
	EXP_Source.edw_loss_master_calculation_pk_id,
	EXP_Source.edw_claim_reins_trans_pk_id,
	EXP_Source.loss_master_dim_id,
	EXP_Source.claim_occurrence_dim_id,
	EXP_Source.claimant_dim_id,
	EXP_Source.claimant_cov_dim_id,
	EXP_Source.cov_dim_id,
	EXP_Source.reins_cov_dim_id,
	EXP_Source.claim_rep_dim_prim_claim_rep_id,
	EXP_Source.claim_rep_dim_examiner_id,
	EXP_Source.pol_dim_id,
	EXP_Source.contract_cust_dim_id,
	EXP_Source.agency_dim_id,
	EXP_Source.claim_pay_dim_id,
	EXP_Source.claim_pay_ctgry_type_dim_id,
	EXP_Source.claim_case_dim_id,
	EXP_Source.claim_trans_date_id,
	EXP_Source.pol_eff_date_id,
	EXP_Source.pol_exp_date_id,
	EXP_Source.source_claim_rpted_date_id,
	EXP_Source.claim_rpted_date_id,
	EXP_Source.claim_loss_date_id,
	EXP_Source.incptn_date_id,
	EXP_Source.loss_master_run_date_id,
	EXP_Source.claim_trans_amt,
	EXP_Source.claim_trans_hist_amt,
	EXP_Source.new_claim_count,
	EXP_Source.outstanding_amt,
	EXP_Source.paid_loss_amt,
	EXP_Source.paid_exp_amt,
	EXP_Source.eom_unpaid_loss_adjust_exp,
	EXP_Source.orig_reserve,
	EXP_Source.orig_reserve_extract,
	EXP_Source.asl_dim_id AS asl_dim_id1,
	EXP_Source.asl_prdct_code_dim_id AS asl_prdct_code_dim_id1,
	EXP_Source.strtgc_bus_dvsn_dim_id,
	EXP_Source.prdct_code_dim_id,
	mplt_hierarchy_product_mapping.prdct_code_dim_id AS prdct_code_dim_id1,
	EXP_Source.InsuranceReferenceDimId,
	EXP_Source.AgencyDimId,
	EXP_Source.SalesDivisionDimId,
	EXP_Source.InsuranceReferenceCoverageDimId,
	EXP_Source.CoverageDetailDimId,
	-- *INF*: 0
	-- --DEFAULT VALUE
	0 AS ChangeInOutstandingAmount,
	-- *INF*: 0
	-- --DEFAULT VALUE
	0 AS ChangeInEOMUnpaidLossAdjustmentExpense,
	EXP_Source.ClaimFinancialTypeDimId
	FROM EXP_Source
	 -- Manually join with mplt_ASL_Policy_Symbol_Changes
	 -- Manually join with mplt_ASL_WBC8827B_Product_Coverage_Codes
	 -- Manually join with mplt_hierarchy_product_mapping
	LEFT JOIN LKP_asl_dim
	ON LKP_asl_dim.asl_code = EXP_view_ASL_mapplet_output.coverage_code_1 AND LKP_asl_dim.sub_asl_code = EXP_view_ASL_mapplet_output.coverage_code_2 AND LKP_asl_dim.sub_non_asl_code = EXP_view_ASL_mapplet_output.coverage_code_3
	LEFT JOIN LKP_asl_product_code_dim
	ON LKP_asl_product_code_dim.asl_prdct_code = EXP_view_ASL_mapplet_output.product_code
),
RTR_split_special_inserts AS (
	SELECT
	asl_dim_id_out,
	asl_prdct_code_dim_id_out,
	prdct_code_dim_id1 AS prdct_code_dim_id_out,
	kind_code_mine_sub,
	facultative_ind_mine_sub,
	reins_co_number_mine_sub,
	major_peril_code,
	loss_master_fact_id1 AS loss_master_fact_id11,
	edw_claim_trans_pk_id1 AS edw_claim_trans_pk_id11,
	wc_stage_loss_master_pk_id1 AS wc_stage_loss_master_pk_id11,
	claim_trans_type_dim_id1 AS claim_trans_type_dim_id11,
	edw_loss_master_calculation_pk_id,
	edw_claim_reins_trans_pk_id,
	loss_master_dim_id,
	claim_occurrence_dim_id,
	claimant_dim_id,
	claimant_cov_dim_id,
	cov_dim_id,
	reins_cov_dim_id,
	claim_rep_dim_prim_claim_rep_id,
	claim_rep_dim_examiner_id,
	pol_dim_id,
	contract_cust_dim_id,
	agency_dim_id,
	claim_pay_dim_id,
	claim_pay_ctgry_type_dim_id,
	claim_case_dim_id,
	claim_trans_date_id,
	pol_eff_date_id,
	pol_exp_date_id,
	source_claim_rpted_date_id,
	claim_rpted_date_id,
	claim_loss_date_id,
	incptn_date_id,
	loss_master_run_date_id,
	claim_trans_amt,
	claim_trans_hist_amt,
	new_claim_count,
	outstanding_amt,
	paid_loss_amt,
	paid_exp_amt,
	eom_unpaid_loss_adjust_exp,
	orig_reserve,
	orig_reserve_extract,
	asl_dim_id1,
	asl_prdct_code_dim_id1,
	strtgc_bus_dvsn_dim_id,
	prdct_code_dim_id,
	InsuranceReferenceDimId,
	AgencyDimId,
	SalesDivisionDimId,
	InsuranceReferenceCoverageDimId,
	CoverageDetailDimId,
	ChangeInOutstandingAmount,
	ChangeInEOMUnpaidLossAdjustmentExpense,
	ClaimFinancialTypeDimId
	FROM EXP_merge_update_data
),
RTR_split_special_inserts_MineSub50 AS (SELECT * FROM RTR_split_special_inserts WHERE major_peril_code='050'),
RTR_split_special_inserts_UpdatePath AS (SELECT * FROM RTR_split_special_inserts WHERE TRUE),
EXP_inputs_for_mp50_path AS (
	SELECT
	asl_dim_id_out,
	asl_prdct_code_dim_id_out,
	prdct_code_dim_id_out,
	kind_code_mine_sub,
	reins_co_number_mine_sub,
	loss_master_fact_id AS loss_master_fact_id111,
	edw_claim_trans_pk_id AS edw_claim_trans_pk_id111,
	wc_stage_loss_master_pk_id AS wc_stage_loss_master_pk_id111,
	claim_trans_type_dim_id AS claim_trans_type_dim_id111,
	edw_loss_master_calculation_pk_id AS edw_loss_master_calculation_pk_id1,
	edw_claim_reins_trans_pk_id AS edw_claim_reins_trans_pk_id1,
	loss_master_dim_id AS loss_master_dim_id1,
	claim_occurrence_dim_id AS claim_occurrence_dim_id1,
	claimant_dim_id AS claimant_dim_id1,
	claimant_cov_dim_id AS claimant_cov_dim_id1,
	cov_dim_id AS cov_dim_id1,
	reins_cov_dim_id AS reins_cov_dim_id1,
	claim_rep_dim_prim_claim_rep_id AS claim_rep_dim_prim_claim_rep_id1,
	claim_rep_dim_examiner_id AS claim_rep_dim_examiner_id1,
	pol_dim_id AS pol_dim_id1,
	contract_cust_dim_id AS contract_cust_dim_id1,
	agency_dim_id AS agency_dim_id1,
	claim_pay_dim_id AS claim_pay_dim_id1,
	claim_pay_ctgry_type_dim_id AS claim_pay_ctgry_type_dim_id1,
	claim_case_dim_id AS claim_case_dim_id1,
	claim_trans_date_id AS claim_trans_date_id1,
	pol_eff_date_id AS pol_eff_date_id1,
	pol_exp_date_id AS pol_exp_date_id1,
	source_claim_rpted_date_id AS source_claim_rpted_date_id1,
	claim_rpted_date_id AS claim_rpted_date_id1,
	claim_loss_date_id AS claim_loss_date_id1,
	incptn_date_id AS incptn_date_id1,
	loss_master_run_date_id AS loss_master_run_date_id1,
	claim_trans_amt AS claim_trans_amt1,
	claim_trans_hist_amt AS claim_trans_hist_amt1,
	new_claim_count AS new_claim_count1,
	outstanding_amt AS outstanding_amt1,
	paid_loss_amt AS paid_loss_amt1,
	paid_exp_amt AS paid_exp_amt1,
	eom_unpaid_loss_adjust_exp AS eom_unpaid_loss_adjust_exp1,
	orig_reserve AS orig_reserve1,
	orig_reserve_extract AS orig_reserve_extract1,
	strtgc_bus_dvsn_dim_id AS strtgc_bus_dvsn_dim_id1,
	InsuranceReferenceDimId AS InsuranceReferenceDimId1,
	AgencyDimId AS AgencyDimId1,
	SalesDivisionDimId AS SalesDivisionDimId1,
	InsuranceReferenceCoverageDimId AS InsuranceReferenceCoverageDimId11,
	CoverageDetailDimId AS CoverageDetailDimId1,
	ChangeInOutstandingAmount AS ChangeInOutstandingAmount1,
	ChangeInEOMUnpaidLossAdjustmentExpense AS ChangeInEOMUnpaidLossAdjustmentExpense1,
	ClaimFinancialTypeDimId AS ClaimFinancialTypeDimId1
	FROM RTR_split_special_inserts_MineSub50
),
LKP_claim_transaction_type_dim_Get_kind_code AS (
	SELECT
	claim_trans_type_dim_id,
	crrnt_snpsht_flag,
	trans_ctgry_code,
	trans_code,
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
			pms_trans_code,
			trans_base_type_code,
			trans_kind_code,
			trans_rsn,
			type_disability,
			offset_onset_ind
		FROM claim_transaction_type_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_trans_type_dim_id ORDER BY claim_trans_type_dim_id) = 1
),
FIL_filter_Ceded_records AS (
	SELECT
	LKP_claim_transaction_type_dim_Get_kind_code.claim_trans_type_dim_id, 
	LKP_claim_transaction_type_dim_Get_kind_code.crrnt_snpsht_flag, 
	LKP_claim_transaction_type_dim_Get_kind_code.trans_ctgry_code, 
	LKP_claim_transaction_type_dim_Get_kind_code.trans_code, 
	LKP_claim_transaction_type_dim_Get_kind_code.pms_trans_code, 
	LKP_claim_transaction_type_dim_Get_kind_code.trans_base_type_code, 
	LKP_claim_transaction_type_dim_Get_kind_code.trans_kind_code, 
	LKP_claim_transaction_type_dim_Get_kind_code.trans_rsn, 
	LKP_claim_transaction_type_dim_Get_kind_code.type_disability, 
	LKP_claim_transaction_type_dim_Get_kind_code.offset_onset_ind, 
	EXP_inputs_for_mp50_path.asl_dim_id_out, 
	EXP_inputs_for_mp50_path.asl_prdct_code_dim_id_out, 
	EXP_inputs_for_mp50_path.kind_code_mine_sub, 
	EXP_inputs_for_mp50_path.reins_co_number_mine_sub, 
	EXP_inputs_for_mp50_path.loss_master_fact_id111, 
	EXP_inputs_for_mp50_path.edw_claim_trans_pk_id111, 
	EXP_inputs_for_mp50_path.wc_stage_loss_master_pk_id111, 
	EXP_inputs_for_mp50_path.claim_trans_type_dim_id111, 
	EXP_inputs_for_mp50_path.edw_loss_master_calculation_pk_id1, 
	EXP_inputs_for_mp50_path.edw_claim_reins_trans_pk_id1, 
	EXP_inputs_for_mp50_path.loss_master_dim_id1, 
	EXP_inputs_for_mp50_path.claim_occurrence_dim_id1, 
	EXP_inputs_for_mp50_path.claimant_dim_id1, 
	EXP_inputs_for_mp50_path.claimant_cov_dim_id1, 
	EXP_inputs_for_mp50_path.cov_dim_id1, 
	EXP_inputs_for_mp50_path.reins_cov_dim_id1, 
	EXP_inputs_for_mp50_path.claim_rep_dim_prim_claim_rep_id1, 
	EXP_inputs_for_mp50_path.claim_rep_dim_examiner_id1, 
	EXP_inputs_for_mp50_path.pol_dim_id1, 
	EXP_inputs_for_mp50_path.contract_cust_dim_id1, 
	EXP_inputs_for_mp50_path.agency_dim_id1, 
	EXP_inputs_for_mp50_path.claim_pay_dim_id1, 
	EXP_inputs_for_mp50_path.claim_pay_ctgry_type_dim_id1, 
	EXP_inputs_for_mp50_path.claim_case_dim_id1, 
	EXP_inputs_for_mp50_path.claim_trans_date_id1, 
	EXP_inputs_for_mp50_path.pol_eff_date_id1, 
	EXP_inputs_for_mp50_path.pol_exp_date_id1, 
	EXP_inputs_for_mp50_path.source_claim_rpted_date_id1, 
	EXP_inputs_for_mp50_path.claim_rpted_date_id1, 
	EXP_inputs_for_mp50_path.claim_loss_date_id1, 
	EXP_inputs_for_mp50_path.incptn_date_id1, 
	EXP_inputs_for_mp50_path.loss_master_run_date_id1, 
	EXP_inputs_for_mp50_path.claim_trans_amt1, 
	EXP_inputs_for_mp50_path.claim_trans_hist_amt1, 
	EXP_inputs_for_mp50_path.new_claim_count1, 
	EXP_inputs_for_mp50_path.outstanding_amt1, 
	EXP_inputs_for_mp50_path.paid_loss_amt1, 
	EXP_inputs_for_mp50_path.paid_exp_amt1, 
	EXP_inputs_for_mp50_path.eom_unpaid_loss_adjust_exp1, 
	EXP_inputs_for_mp50_path.orig_reserve1, 
	EXP_inputs_for_mp50_path.orig_reserve_extract1, 
	EXP_inputs_for_mp50_path.strtgc_bus_dvsn_dim_id1, 
	EXP_inputs_for_mp50_path.prdct_code_dim_id_out AS prdct_code_dim_id1, 
	EXP_inputs_for_mp50_path.InsuranceReferenceDimId1, 
	EXP_inputs_for_mp50_path.AgencyDimId1, 
	EXP_inputs_for_mp50_path.SalesDivisionDimId1, 
	EXP_inputs_for_mp50_path.InsuranceReferenceCoverageDimId11, 
	EXP_inputs_for_mp50_path.CoverageDetailDimId1, 
	EXP_inputs_for_mp50_path.ChangeInOutstandingAmount1, 
	EXP_inputs_for_mp50_path.ChangeInEOMUnpaidLossAdjustmentExpense1, 
	EXP_inputs_for_mp50_path.ClaimFinancialTypeDimId1
	FROM EXP_inputs_for_mp50_path
	LEFT JOIN LKP_claim_transaction_type_dim_Get_kind_code
	ON LKP_claim_transaction_type_dim_Get_kind_code.claim_trans_type_dim_id = EXP_inputs_for_mp50_path.claim_trans_type_dim_id111
	WHERE rtrim(ltrim(trans_kind_code))='D'
),
LKP_claim_transaction_type_dim_Get_Ceded_Record AS (
	SELECT
	claim_trans_type_dim_id,
	crrnt_snpsht_flag,
	trans_ctgry_code,
	trans_code,
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
			pms_trans_code,
			trans_base_type_code,
			trans_rsn,
			type_disability,
			offset_onset_ind,
			trans_kind_code
		FROM claim_transaction_type_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY crrnt_snpsht_flag,trans_ctgry_code,trans_code,pms_trans_code,trans_base_type_code,trans_rsn,type_disability,offset_onset_ind,trans_kind_code ORDER BY claim_trans_type_dim_id) = 1
),
EXP_combine_mp50_with_ceded_transaction_type_id AS (
	SELECT
	FIL_filter_Ceded_records.asl_dim_id_out,
	FIL_filter_Ceded_records.asl_prdct_code_dim_id_out,
	FIL_filter_Ceded_records.reins_co_number_mine_sub,
	LKP_claim_transaction_type_dim_Get_Ceded_Record.claim_trans_type_dim_id AS lkp_claim_trans_type_dim_id,
	-- *INF*: IIF(isnull(lkp_claim_trans_type_dim_id),-1,lkp_claim_trans_type_dim_id)
	IFF(lkp_claim_trans_type_dim_id IS NULL, - 1, lkp_claim_trans_type_dim_id) AS lkp_claim_trans_type_dim_id_out,
	-1 AS DEFAULT_ID,
	FIL_filter_Ceded_records.kind_code_mine_sub,
	FIL_filter_Ceded_records.loss_master_fact_id111,
	FIL_filter_Ceded_records.edw_claim_trans_pk_id111,
	FIL_filter_Ceded_records.wc_stage_loss_master_pk_id111,
	FIL_filter_Ceded_records.claim_trans_type_dim_id111,
	FIL_filter_Ceded_records.edw_loss_master_calculation_pk_id1,
	FIL_filter_Ceded_records.edw_claim_reins_trans_pk_id1,
	FIL_filter_Ceded_records.loss_master_dim_id1,
	FIL_filter_Ceded_records.claim_occurrence_dim_id1,
	FIL_filter_Ceded_records.claimant_dim_id1,
	FIL_filter_Ceded_records.claimant_cov_dim_id1,
	FIL_filter_Ceded_records.cov_dim_id1,
	FIL_filter_Ceded_records.reins_cov_dim_id1,
	FIL_filter_Ceded_records.claim_rep_dim_prim_claim_rep_id1,
	FIL_filter_Ceded_records.claim_rep_dim_examiner_id1,
	FIL_filter_Ceded_records.pol_dim_id1,
	FIL_filter_Ceded_records.contract_cust_dim_id1,
	FIL_filter_Ceded_records.agency_dim_id1,
	FIL_filter_Ceded_records.claim_pay_dim_id1,
	FIL_filter_Ceded_records.claim_pay_ctgry_type_dim_id1,
	FIL_filter_Ceded_records.claim_case_dim_id1,
	FIL_filter_Ceded_records.claim_trans_date_id1,
	FIL_filter_Ceded_records.pol_eff_date_id1,
	FIL_filter_Ceded_records.pol_exp_date_id1,
	FIL_filter_Ceded_records.source_claim_rpted_date_id1,
	FIL_filter_Ceded_records.claim_rpted_date_id1,
	FIL_filter_Ceded_records.claim_loss_date_id1,
	FIL_filter_Ceded_records.incptn_date_id1,
	FIL_filter_Ceded_records.loss_master_run_date_id1,
	FIL_filter_Ceded_records.claim_trans_amt1,
	FIL_filter_Ceded_records.claim_trans_hist_amt1,
	FIL_filter_Ceded_records.new_claim_count1,
	FIL_filter_Ceded_records.outstanding_amt1,
	FIL_filter_Ceded_records.paid_loss_amt1,
	FIL_filter_Ceded_records.paid_exp_amt1,
	FIL_filter_Ceded_records.eom_unpaid_loss_adjust_exp1,
	FIL_filter_Ceded_records.orig_reserve1,
	FIL_filter_Ceded_records.orig_reserve_extract1,
	-50 AS AUDIT_DEFAULT,
	FIL_filter_Ceded_records.strtgc_bus_dvsn_dim_id1,
	FIL_filter_Ceded_records.prdct_code_dim_id1,
	FIL_filter_Ceded_records.InsuranceReferenceDimId1,
	FIL_filter_Ceded_records.AgencyDimId1,
	FIL_filter_Ceded_records.SalesDivisionDimId1,
	FIL_filter_Ceded_records.InsuranceReferenceCoverageDimId11,
	FIL_filter_Ceded_records.CoverageDetailDimId1,
	FIL_filter_Ceded_records.ChangeInOutstandingAmount1,
	FIL_filter_Ceded_records.ChangeInEOMUnpaidLossAdjustmentExpense1,
	FIL_filter_Ceded_records.ClaimFinancialTypeDimId1
	FROM FIL_filter_Ceded_records
	LEFT JOIN LKP_claim_transaction_type_dim_Get_Ceded_Record
	ON LKP_claim_transaction_type_dim_Get_Ceded_Record.crrnt_snpsht_flag = FIL_filter_Ceded_records.crrnt_snpsht_flag AND LKP_claim_transaction_type_dim_Get_Ceded_Record.trans_ctgry_code = FIL_filter_Ceded_records.trans_ctgry_code AND LKP_claim_transaction_type_dim_Get_Ceded_Record.trans_code = FIL_filter_Ceded_records.trans_code AND LKP_claim_transaction_type_dim_Get_Ceded_Record.pms_trans_code = FIL_filter_Ceded_records.pms_trans_code AND LKP_claim_transaction_type_dim_Get_Ceded_Record.trans_base_type_code = FIL_filter_Ceded_records.trans_base_type_code AND LKP_claim_transaction_type_dim_Get_Ceded_Record.trans_rsn = FIL_filter_Ceded_records.trans_rsn AND LKP_claim_transaction_type_dim_Get_Ceded_Record.type_disability = FIL_filter_Ceded_records.type_disability AND LKP_claim_transaction_type_dim_Get_Ceded_Record.offset_onset_ind = FIL_filter_Ceded_records.offset_onset_ind AND LKP_claim_transaction_type_dim_Get_Ceded_Record.trans_kind_code = FIL_filter_Ceded_records.kind_code_mine_sub
),
LKP_loss_master_fact_Check_for_ceded_record AS (
	SELECT
	loss_master_fact_id,
	claim_trans_type_dim_id,
	edw_claim_trans_pk_id,
	wc_stage_loss_master_pk_id,
	edw_loss_master_calculation_pk_id
	FROM (
		SELECT 
			loss_master_fact_id,
			claim_trans_type_dim_id,
			edw_claim_trans_pk_id,
			wc_stage_loss_master_pk_id,
			edw_loss_master_calculation_pk_id
		FROM loss_master_fact
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_trans_type_dim_id,edw_claim_trans_pk_id,wc_stage_loss_master_pk_id,edw_loss_master_calculation_pk_id ORDER BY loss_master_fact_id DESC) = 1
),
FIL_remove_if_ceded_loss_master_fact_exists AS (
	SELECT
	LKP_loss_master_fact_Check_for_ceded_record.loss_master_fact_id AS lkp_loss_master_fact_id, 
	EXP_combine_mp50_with_ceded_transaction_type_id.asl_dim_id_out AS asl_dim_id_out1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.asl_prdct_code_dim_id_out AS asl_prdct_code_dim_id_out1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.reins_co_number_mine_sub AS reins_co_number_mine_sub1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.lkp_claim_trans_type_dim_id_out AS claim_trans_type_dim_ceded, 
	EXP_combine_mp50_with_ceded_transaction_type_id.DEFAULT_ID, 
	EXP_combine_mp50_with_ceded_transaction_type_id.edw_claim_trans_pk_id111, 
	EXP_combine_mp50_with_ceded_transaction_type_id.wc_stage_loss_master_pk_id111, 
	EXP_combine_mp50_with_ceded_transaction_type_id.edw_loss_master_calculation_pk_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.edw_claim_reins_trans_pk_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.loss_master_dim_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.claim_occurrence_dim_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.claimant_dim_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.claimant_cov_dim_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.cov_dim_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.claim_rep_dim_prim_claim_rep_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.claim_rep_dim_examiner_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.pol_dim_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.contract_cust_dim_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.agency_dim_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.claim_pay_dim_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.claim_pay_ctgry_type_dim_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.claim_case_dim_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.claim_trans_date_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.pol_eff_date_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.pol_exp_date_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.source_claim_rpted_date_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.claim_rpted_date_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.claim_loss_date_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.incptn_date_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.loss_master_run_date_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.claim_trans_amt1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.claim_trans_hist_amt1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.new_claim_count1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.outstanding_amt1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.paid_loss_amt1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.paid_exp_amt1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.eom_unpaid_loss_adjust_exp1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.orig_reserve1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.orig_reserve_extract1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.AUDIT_DEFAULT, 
	EXP_combine_mp50_with_ceded_transaction_type_id.strtgc_bus_dvsn_dim_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.prdct_code_dim_id1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.InsuranceReferenceDimId1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.AgencyDimId1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.SalesDivisionDimId1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.InsuranceReferenceCoverageDimId11, 
	EXP_combine_mp50_with_ceded_transaction_type_id.CoverageDetailDimId1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.ChangeInOutstandingAmount1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.ChangeInEOMUnpaidLossAdjustmentExpense1, 
	EXP_combine_mp50_with_ceded_transaction_type_id.ClaimFinancialTypeDimId1
	FROM EXP_combine_mp50_with_ceded_transaction_type_id
	LEFT JOIN LKP_loss_master_fact_Check_for_ceded_record
	ON LKP_loss_master_fact_Check_for_ceded_record.claim_trans_type_dim_id = EXP_combine_mp50_with_ceded_transaction_type_id.lkp_claim_trans_type_dim_id_out AND LKP_loss_master_fact_Check_for_ceded_record.edw_claim_trans_pk_id = EXP_combine_mp50_with_ceded_transaction_type_id.edw_claim_trans_pk_id111 AND LKP_loss_master_fact_Check_for_ceded_record.wc_stage_loss_master_pk_id = EXP_combine_mp50_with_ceded_transaction_type_id.wc_stage_loss_master_pk_id111 AND LKP_loss_master_fact_Check_for_ceded_record.edw_loss_master_calculation_pk_id = EXP_combine_mp50_with_ceded_transaction_type_id.edw_loss_master_calculation_pk_id1
	WHERE TRUE---isnull(lkp_loss_master_fact_id)
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
EXP_insert_output AS (
	SELECT
	FIL_remove_if_ceded_loss_master_fact_exists.lkp_loss_master_fact_id,
	FIL_remove_if_ceded_loss_master_fact_exists.AUDIT_DEFAULT AS audit_id,
	FIL_remove_if_ceded_loss_master_fact_exists.edw_loss_master_calculation_pk_id1 AS edw_loss_master_calculation_pk_id,
	FIL_remove_if_ceded_loss_master_fact_exists.edw_claim_trans_pk_id111 AS edw_claim_trans_pk_id,
	FIL_remove_if_ceded_loss_master_fact_exists.edw_claim_reins_trans_pk_id1 AS edw_claim_reins_trans_pk_id,
	FIL_remove_if_ceded_loss_master_fact_exists.wc_stage_loss_master_pk_id111 AS wc_stage_loss_master_pk_id,
	FIL_remove_if_ceded_loss_master_fact_exists.loss_master_dim_id1 AS loss_master_dim_id,
	FIL_remove_if_ceded_loss_master_fact_exists.claim_occurrence_dim_id1 AS claim_occurrence_dim_id,
	FIL_remove_if_ceded_loss_master_fact_exists.claimant_dim_id1 AS claimant_dim_id,
	FIL_remove_if_ceded_loss_master_fact_exists.claimant_cov_dim_id1 AS claimant_cov_dim_id,
	FIL_remove_if_ceded_loss_master_fact_exists.cov_dim_id1 AS cov_dim_id,
	LKP_reinsurance_coverage_dim.reins_cov_dim_id,
	FIL_remove_if_ceded_loss_master_fact_exists.claim_trans_type_dim_ceded AS claim_trans_type_dim_id,
	FIL_remove_if_ceded_loss_master_fact_exists.claim_rep_dim_prim_claim_rep_id1 AS claim_rep_dim_prim_claim_rep_id,
	FIL_remove_if_ceded_loss_master_fact_exists.claim_rep_dim_examiner_id1 AS claim_rep_dim_examiner_id,
	FIL_remove_if_ceded_loss_master_fact_exists.pol_dim_id1 AS pol_dim_id,
	FIL_remove_if_ceded_loss_master_fact_exists.contract_cust_dim_id1 AS contract_cust_dim_id,
	FIL_remove_if_ceded_loss_master_fact_exists.agency_dim_id1 AS agency_dim_id,
	FIL_remove_if_ceded_loss_master_fact_exists.claim_pay_dim_id1 AS claim_pay_dim_id,
	FIL_remove_if_ceded_loss_master_fact_exists.claim_pay_ctgry_type_dim_id1 AS claim_pay_ctgry_type_dim_id,
	FIL_remove_if_ceded_loss_master_fact_exists.claim_case_dim_id1 AS claim_case_dim_id,
	FIL_remove_if_ceded_loss_master_fact_exists.claim_trans_date_id1 AS claim_trans_date_id,
	FIL_remove_if_ceded_loss_master_fact_exists.pol_eff_date_id1 AS pol_eff_date_id,
	FIL_remove_if_ceded_loss_master_fact_exists.pol_exp_date_id1 AS pol_exp_date_id,
	FIL_remove_if_ceded_loss_master_fact_exists.source_claim_rpted_date_id1 AS source_claim_rpted_date_id,
	FIL_remove_if_ceded_loss_master_fact_exists.claim_rpted_date_id1 AS claim_rpted_date_id,
	FIL_remove_if_ceded_loss_master_fact_exists.claim_loss_date_id1 AS claim_loss_date_id,
	FIL_remove_if_ceded_loss_master_fact_exists.incptn_date_id1 AS incptn_date_id,
	FIL_remove_if_ceded_loss_master_fact_exists.loss_master_run_date_id1 AS loss_master_run_date_id,
	FIL_remove_if_ceded_loss_master_fact_exists.claim_trans_amt1 AS claim_trans_amt,
	FIL_remove_if_ceded_loss_master_fact_exists.claim_trans_hist_amt1 AS claim_trans_hist_amt,
	FIL_remove_if_ceded_loss_master_fact_exists.new_claim_count1 AS new_claim_count,
	FIL_remove_if_ceded_loss_master_fact_exists.outstanding_amt1 AS outstanding_amt,
	FIL_remove_if_ceded_loss_master_fact_exists.paid_loss_amt1 AS paid_loss_amt,
	FIL_remove_if_ceded_loss_master_fact_exists.paid_exp_amt1 AS paid_exp_amt,
	FIL_remove_if_ceded_loss_master_fact_exists.eom_unpaid_loss_adjust_exp1 AS eom_unpaid_loss_adjust_exp,
	FIL_remove_if_ceded_loss_master_fact_exists.orig_reserve1 AS orig_reserve,
	FIL_remove_if_ceded_loss_master_fact_exists.orig_reserve_extract1 AS orig_reserve_extract,
	FIL_remove_if_ceded_loss_master_fact_exists.asl_dim_id_out1 AS asl_dim_id,
	FIL_remove_if_ceded_loss_master_fact_exists.asl_prdct_code_dim_id_out1 AS asl_prdct_code_dim_id,
	FIL_remove_if_ceded_loss_master_fact_exists.strtgc_bus_dvsn_dim_id1,
	FIL_remove_if_ceded_loss_master_fact_exists.prdct_code_dim_id1,
	FIL_remove_if_ceded_loss_master_fact_exists.InsuranceReferenceDimId1,
	FIL_remove_if_ceded_loss_master_fact_exists.AgencyDimId1,
	FIL_remove_if_ceded_loss_master_fact_exists.SalesDivisionDimId1,
	FIL_remove_if_ceded_loss_master_fact_exists.InsuranceReferenceCoverageDimId11,
	FIL_remove_if_ceded_loss_master_fact_exists.CoverageDetailDimId1,
	FIL_remove_if_ceded_loss_master_fact_exists.ChangeInOutstandingAmount1,
	FIL_remove_if_ceded_loss_master_fact_exists.ChangeInEOMUnpaidLossAdjustmentExpense1,
	FIL_remove_if_ceded_loss_master_fact_exists.ClaimFinancialTypeDimId1
	FROM FIL_remove_if_ceded_loss_master_fact_exists
	LEFT JOIN LKP_reinsurance_coverage_dim
	ON LKP_reinsurance_coverage_dim.reins_co_num = FIL_remove_if_ceded_loss_master_fact_exists.reins_co_number_mine_sub1 AND LKP_reinsurance_coverage_dim.edw_reins_cov_pk_id = FIL_remove_if_ceded_loss_master_fact_exists.DEFAULT_ID
),
RTR_INSERT_UPDATE AS (
	SELECT
	lkp_loss_master_fact_id,
	audit_id,
	edw_loss_master_calculation_pk_id,
	edw_claim_trans_pk_id,
	edw_claim_reins_trans_pk_id,
	wc_stage_loss_master_pk_id,
	loss_master_dim_id,
	claim_occurrence_dim_id,
	claimant_dim_id,
	claimant_cov_dim_id,
	cov_dim_id,
	claim_trans_type_dim_id,
	reins_cov_dim_id,
	claim_rep_dim_prim_claim_rep_id,
	claim_rep_dim_examiner_id,
	pol_dim_id,
	contract_cust_dim_id,
	agency_dim_id,
	claim_pay_dim_id,
	claim_pay_ctgry_type_dim_id,
	claim_case_dim_id,
	claim_trans_date_id,
	pol_eff_date_id,
	pol_exp_date_id,
	source_claim_rpted_date_id,
	claim_rpted_date_id,
	claim_loss_date_id,
	incptn_date_id,
	loss_master_run_date_id,
	claim_trans_amt,
	claim_trans_hist_amt,
	new_claim_count,
	outstanding_amt,
	paid_loss_amt,
	paid_exp_amt,
	eom_unpaid_loss_adjust_exp,
	orig_reserve,
	orig_reserve_extract,
	asl_dim_id,
	asl_prdct_code_dim_id,
	strtgc_bus_dvsn_dim_id1,
	prdct_code_dim_id1,
	InsuranceReferenceDimId1,
	AgencyDimId1,
	SalesDivisionDimId1,
	InsuranceReferenceCoverageDimId11 AS InsuranceReferenceCoverageDimId1,
	CoverageDetailDimId1,
	ChangeInOutstandingAmount1,
	ChangeInEOMUnpaidLossAdjustmentExpense1,
	ClaimFinancialTypeDimId1
	FROM EXP_insert_output
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE ISNULL(lkp_loss_master_fact_id)),
RTR_INSERT_UPDATE_DEFAULT1 AS (SELECT * FROM RTR_INSERT_UPDATE WHERE NOT ( (ISNULL(lkp_loss_master_fact_id)) )),
UPD_Update_MP50 AS (
	SELECT
	lkp_loss_master_fact_id AS lkp_loss_master_fact_id2, 
	audit_id AS audit_id2, 
	edw_loss_master_calculation_pk_id AS edw_loss_master_calculation_pk_id2, 
	edw_claim_trans_pk_id AS edw_claim_trans_pk_id2, 
	edw_claim_reins_trans_pk_id AS edw_claim_reins_trans_pk_id2, 
	wc_stage_loss_master_pk_id AS wc_stage_loss_master_pk_id2, 
	loss_master_dim_id AS loss_master_dim_id2, 
	claim_occurrence_dim_id AS claim_occurrence_dim_id2, 
	claimant_dim_id AS claimant_dim_id2, 
	claimant_cov_dim_id AS claimant_cov_dim_id2, 
	cov_dim_id AS cov_dim_id2, 
	claim_trans_type_dim_id AS claim_trans_type_dim_id2, 
	reins_cov_dim_id AS reins_cov_dim_id2, 
	claim_rep_dim_prim_claim_rep_id AS claim_rep_dim_prim_claim_rep_id2, 
	claim_rep_dim_examiner_id AS claim_rep_dim_examiner_id2, 
	pol_dim_id AS pol_dim_id2, 
	contract_cust_dim_id AS contract_cust_dim_id2, 
	agency_dim_id AS agency_dim_id2, 
	claim_pay_dim_id AS claim_pay_dim_id2, 
	claim_pay_ctgry_type_dim_id AS claim_pay_ctgry_type_dim_id2, 
	claim_case_dim_id AS claim_case_dim_id2, 
	claim_trans_date_id AS claim_trans_date_id2, 
	pol_eff_date_id AS pol_eff_date_id2, 
	pol_exp_date_id AS pol_exp_date_id2, 
	source_claim_rpted_date_id AS source_claim_rpted_date_id2, 
	claim_rpted_date_id AS claim_rpted_date_id2, 
	claim_loss_date_id AS claim_loss_date_id2, 
	incptn_date_id AS incptn_date_id2, 
	loss_master_run_date_id AS loss_master_run_date_id2, 
	claim_trans_amt AS claim_trans_amt2, 
	claim_trans_hist_amt AS claim_trans_hist_amt2, 
	new_claim_count AS new_claim_count2, 
	outstanding_amt AS outstanding_amt2, 
	paid_loss_amt AS paid_loss_amt2, 
	paid_exp_amt AS paid_exp_amt2, 
	eom_unpaid_loss_adjust_exp AS eom_unpaid_loss_adjust_exp2, 
	orig_reserve AS orig_reserve2, 
	orig_reserve_extract AS orig_reserve_extract2, 
	asl_dim_id AS asl_dim_id2, 
	asl_prdct_code_dim_id AS asl_prdct_code_dim_id2, 
	strtgc_bus_dvsn_dim_id1 AS strtgc_bus_dvsn_dim_id12, 
	prdct_code_dim_id1 AS prdct_code_dim_id12
	FROM RTR_INSERT_UPDATE_DEFAULT1
),
loss_master_fact_UPDATE_MP50 AS (
	MERGE INTO loss_master_fact AS T
	USING UPD_Update_MP50 AS S
	ON T.loss_master_fact_id = S.lkp_loss_master_fact_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.audit_id = S.audit_id2, T.edw_loss_master_calculation_pk_id = S.edw_loss_master_calculation_pk_id2, T.edw_claim_trans_pk_id = S.edw_claim_trans_pk_id2, T.edw_claim_reins_trans_pk_id = S.edw_claim_reins_trans_pk_id2, T.wc_stage_loss_master_pk_id = S.wc_stage_loss_master_pk_id2, T.loss_master_dim_id = S.loss_master_dim_id2, T.claim_occurrence_dim_id = S.claim_occurrence_dim_id2, T.claimant_dim_id = S.claimant_dim_id2, T.claimant_cov_dim_id = S.claimant_cov_dim_id2, T.cov_dim_id = S.cov_dim_id2, T.claim_trans_type_dim_id = S.claim_trans_type_dim_id2, T.reins_cov_dim_id = S.reins_cov_dim_id2, T.claim_rep_dim_prim_claim_rep_id = S.claim_rep_dim_prim_claim_rep_id2, T.claim_rep_dim_examiner_id = S.claim_rep_dim_examiner_id2, T.pol_dim_id = S.pol_dim_id2, T.contract_cust_dim_id = S.contract_cust_dim_id2, T.agency_dim_id = S.agency_dim_id2, T.claim_pay_dim_id = S.claim_pay_dim_id2, T.claim_pay_ctgry_type_dim_id = S.claim_pay_ctgry_type_dim_id2, T.claim_case_dim_id = S.claim_case_dim_id2, T.claim_trans_date_id = S.claim_trans_date_id2, T.pol_eff_date_id = S.pol_eff_date_id2, T.pol_exp_date_id = S.pol_exp_date_id2, T.source_claim_rpted_date_id = S.source_claim_rpted_date_id2, T.claim_rpted_date_id = S.claim_rpted_date_id2, T.claim_loss_date_id = S.claim_loss_date_id2, T.incptn_date_id = S.incptn_date_id2, T.loss_master_run_date_id = S.loss_master_run_date_id2, T.claim_trans_amt = S.claim_trans_amt2, T.claim_trans_hist_amt = S.claim_trans_hist_amt2, T.new_claim_count = S.new_claim_count2, T.outstanding_amt = S.outstanding_amt2, T.paid_loss_amt = S.paid_loss_amt2, T.paid_exp_amt = S.paid_exp_amt2, T.eom_unpaid_loss_adjust_exp = S.eom_unpaid_loss_adjust_exp2, T.orig_reserve = S.orig_reserve2, T.orig_reserve_extract = S.orig_reserve_extract2, T.asl_dim_id = S.asl_dim_id2, T.asl_prdct_code_dim_id = S.asl_prdct_code_dim_id2, T.strtgc_bus_dvsn_dim_id = S.strtgc_bus_dvsn_dim_id12, T.prdct_code_dim_id = S.prdct_code_dim_id12
),
loss_master_fact_INSERT AS (
	INSERT INTO loss_master_fact
	(audit_id, edw_loss_master_calculation_pk_id, edw_claim_trans_pk_id, edw_claim_reins_trans_pk_id, wc_stage_loss_master_pk_id, loss_master_dim_id, claim_occurrence_dim_id, claimant_dim_id, claimant_cov_dim_id, cov_dim_id, claim_trans_type_dim_id, reins_cov_dim_id, claim_rep_dim_prim_claim_rep_id, claim_rep_dim_examiner_id, pol_dim_id, contract_cust_dim_id, agency_dim_id, claim_pay_dim_id, claim_pay_ctgry_type_dim_id, claim_case_dim_id, claim_trans_date_id, pol_eff_date_id, pol_exp_date_id, source_claim_rpted_date_id, claim_rpted_date_id, claim_loss_date_id, incptn_date_id, loss_master_run_date_id, claim_trans_amt, claim_trans_hist_amt, new_claim_count, outstanding_amt, paid_loss_amt, paid_exp_amt, eom_unpaid_loss_adjust_exp, orig_reserve, orig_reserve_extract, asl_dim_id, asl_prdct_code_dim_id, strtgc_bus_dvsn_dim_id, prdct_code_dim_id, InsuranceReferenceDimId, AgencyDimId, SalesDivisionDimId, InsuranceReferenceCoverageDimId, CoverageDetailDimId, ChangeInOutstandingAmount, ChangeInEOMUnpaidLossAdjustmentExpense, ClaimFinancialTypeDimId)
	SELECT 
	AUDIT_ID, 
	EDW_LOSS_MASTER_CALCULATION_PK_ID, 
	EDW_CLAIM_TRANS_PK_ID, 
	EDW_CLAIM_REINS_TRANS_PK_ID, 
	WC_STAGE_LOSS_MASTER_PK_ID, 
	LOSS_MASTER_DIM_ID, 
	CLAIM_OCCURRENCE_DIM_ID, 
	CLAIMANT_DIM_ID, 
	CLAIMANT_COV_DIM_ID, 
	COV_DIM_ID, 
	CLAIM_TRANS_TYPE_DIM_ID, 
	REINS_COV_DIM_ID, 
	CLAIM_REP_DIM_PRIM_CLAIM_REP_ID, 
	CLAIM_REP_DIM_EXAMINER_ID, 
	POL_DIM_ID, 
	CONTRACT_CUST_DIM_ID, 
	AGENCY_DIM_ID, 
	CLAIM_PAY_DIM_ID, 
	CLAIM_PAY_CTGRY_TYPE_DIM_ID, 
	CLAIM_CASE_DIM_ID, 
	CLAIM_TRANS_DATE_ID, 
	POL_EFF_DATE_ID, 
	POL_EXP_DATE_ID, 
	SOURCE_CLAIM_RPTED_DATE_ID, 
	CLAIM_RPTED_DATE_ID, 
	CLAIM_LOSS_DATE_ID, 
	INCPTN_DATE_ID, 
	LOSS_MASTER_RUN_DATE_ID, 
	CLAIM_TRANS_AMT, 
	CLAIM_TRANS_HIST_AMT, 
	NEW_CLAIM_COUNT, 
	OUTSTANDING_AMT, 
	PAID_LOSS_AMT, 
	PAID_EXP_AMT, 
	EOM_UNPAID_LOSS_ADJUST_EXP, 
	ORIG_RESERVE, 
	ORIG_RESERVE_EXTRACT, 
	ASL_DIM_ID, 
	ASL_PRDCT_CODE_DIM_ID, 
	STRTGC_BUS_DVSN_DIM_ID, 
	PRDCT_CODE_DIM_ID, 
	INSURANCEREFERENCEDIMID, 
	AGENCYDIMID, 
	SALESDIVISIONDIMID, 
	INSURANCEREFERENCECOVERAGEDIMID, 
	COVERAGEDETAILDIMID, 
	CHANGEINOUTSTANDINGAMOUNT, 
	CHANGEINEOMUNPAIDLOSSADJUSTMENTEXPENSE, 
	CLAIMFINANCIALTYPEDIMID
	FROM RTR_INSERT_UPDATE_INSERT
),
EXP_check_for_update AS (
	SELECT
	loss_master_fact_id11 AS loss_master_fact_id,
	asl_dim_id_out AS mplt_asl_dim_id_out,
	asl_prdct_code_dim_id_out AS mplt_asl_prdct_code_dim_id_out,
	prdct_code_dim_id_out AS mplt_prdct_code_dim_id_out,
	asl_dim_id1 AS asl_dim_id_old,
	asl_prdct_code_dim_id1 AS asl_prdct_code_dim_id_old,
	prdct_code_dim_id AS prdct_code_dim_id_old,
	-- *INF*: IIF(
	-- mplt_asl_dim_id_out != asl_dim_id_old or 
	-- mplt_asl_prdct_code_dim_id_out !=asl_prdct_code_dim_id_old or
	-- mplt_prdct_code_dim_id_out != prdct_code_dim_id_old,1,0)
	IFF(mplt_asl_dim_id_out != asl_dim_id_old OR mplt_asl_prdct_code_dim_id_out != asl_prdct_code_dim_id_old OR mplt_prdct_code_dim_id_out != prdct_code_dim_id_old, 1, 0) AS IsChanged
	FROM RTR_split_special_inserts_UpdatePath
),
FIL_ASL_updates AS (
	SELECT
	loss_master_fact_id, 
	mplt_asl_dim_id_out, 
	mplt_asl_prdct_code_dim_id_out, 
	mplt_prdct_code_dim_id_out, 
	IsChanged
	FROM EXP_check_for_update
	WHERE IsChanged=1
),
UPD_Update AS (
	SELECT
	loss_master_fact_id, 
	mplt_asl_dim_id_out AS asl_dim_id_out, 
	mplt_asl_prdct_code_dim_id_out AS asl_prdct_code_dim_id_out, 
	mplt_prdct_code_dim_id_out AS prdct_code_dim_id_out
	FROM FIL_ASL_updates
),
loss_master_fact_UPDATE_PMSClaims AS (
	MERGE INTO loss_master_fact AS T
	USING UPD_Update AS S
	ON T.loss_master_fact_id = S.loss_master_fact_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.asl_dim_id = S.asl_dim_id_out, T.asl_prdct_code_dim_id = S.asl_prdct_code_dim_id_out, T.prdct_code_dim_id = S.prdct_code_dim_id_out
),
SQ_EDWSource_DCTClaims AS (
	SELECT LMF.loss_master_fact_id   AS loss_master_fact_id,
		   CT.claim_trans_id			   AS claim_trans_id,
		   AB.claimant_cov_det_ak_id       AS Claimant_cov_det_ak_id,
		   AB.claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id,
	       AB.PolicySourceID               AS Policysourceid,
	       AB.ClassCode                    AS Classcode,
	       AB.SublineCode                  AS Sublinecode,
	       RC.Exposure                     AS Exposure,
	       RC.SchedulePNumber              AS SchedulePNumber,
		RC.AnnualStatementLineNumber AS AnnualStatementLineNumber,
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
			AB.cause_of_loss as cause_of_loss
	FROM  @{pipeline().parameters.SOURCE_TABLE_OWNER}.loss_master_fact LMF  
		   INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.VW_claim_transaction CT  ON LMF.edw_claim_trans_pk_id = CT.claim_trans_id
	       INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIMANT_COVERAGE_DETAIL AB ON CT.Claimant_cov_det_ak_id = AB.Claimant_cov_det_ak_id
		   INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC ON AB.RatingCoverageAKId = RC.RatingCoverageAKID
		   INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.POLICYCOVERAGE Pc ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID
		   INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RISKLOCATION Rl ON PC.RiskLocationAKID = RL.RiskLocationAKID
		   INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.V2.policy P ON RL.PolicyAKID = P.pol_ak_id
		   LEFT OUTER JOIN @{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter SPC ON P.StrategicProfitCenterAKId = SPC.StrategicProfitCenterAKId
		   LEFT OUTER JOIN @{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness LOB ON RC.InsuranceReferenceLineOfBusinessAKId = LOB.InsuranceReferenceLineOfBusinessAKId
		   LEFT OUTER JOIN @{pipeline().parameters.DB_NAME_EDW}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.Product PDT ON PDT.ProductAKId = RC.ProductAKId
	WHERE AB.PolicySourceID IN ( 'PDC', 'DUC' )
	      AND P.crrnt_snpsht_flag =1 AND SPC.CurrentSnapshotFlag =1 AND Rl.CurrentSnapshotFlag =1 
	      AND Pc.CurrentSnapshotFlag =1 AND RC.CurrentSnapshotFlag =1 AND AB.crrnt_snpsht_flag = 1
	      AND (LMF.asl_dim_id in (0, -1, 32) OR LMF.loss_master_dim_id = -1 OR LMF.strtgc_bus_dvsn_dim_id = -1 OR LMF.prdct_code_dim_id in (0,-1)
	              )
),
EXP_Default AS (
	SELECT
	loss_master_fact_id,
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
	SchedulePNumber,
	AnnualStatementLineNumber,
	AnnualStatementLineCode,
	SubAnnualStatementLineNumber,
	SubAnnualStatementLineCode,
	SubNonAnnualStatementLineCode,
	cause_of_loss,
	-- *INF*: IIF(in(cause_of_loss,'05','75'),'1','0')
	IFF(in(cause_of_loss, '05', '75'), '1', '0') AS v_Indemnity,
	-- *INF*: IIF(in(cause_of_loss,'06','07'),'1','0')
	IFF(in(cause_of_loss, '06', '07'), '1', '0') AS v_Medical,
	-- *INF*: IIF
	-- (AnnualStatementLineCode='160',
	-- DECODE('1',
	-- v_Indemnity,'180',
	-- v_Medical,'190',
	-- '999'),
	-- SubAnnualStatementLineCode)
	IFF(AnnualStatementLineCode = '160', DECODE('1',
	v_Indemnity, '180',
	v_Medical, '190',
	'999'), SubAnnualStatementLineCode) AS o_sub_asl_code
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
	EXP_Default.loss_master_fact_id,
	EXP_Default.claim_trans_id,
	LKP_loss_master_dim_DuckCreekClaims.loss_master_dim_id,
	-- *INF*: IIF(ISNULL(loss_master_dim_id),-1,loss_master_dim_id)
	IFF(loss_master_dim_id IS NULL, - 1, loss_master_dim_id) AS loss_master_dim_id_out,
	EXP_Default.AnnualStatementLineNumber,
	EXP_Default.SchedulePNumber,
	EXP_Default.SubAnnualStatementLineNumber,
	LKP_asl_dim_DuckCreekClaims.asl_dim_id,
	-- *INF*: IIF(ISNULL(asl_dim_id),-1,asl_dim_id)
	-- 
	IFF(asl_dim_id IS NULL, - 1, asl_dim_id) AS asl_dim_id_Out,
	LKP_strategic_business_division_dim.strtgc_bus_dvsn_dim_id,
	-- *INF*: IIF(ISNULL(strtgc_bus_dvsn_dim_id),-1,strtgc_bus_dvsn_dim_id)
	IFF(strtgc_bus_dvsn_dim_id IS NULL, - 1, strtgc_bus_dvsn_dim_id) AS strtgc_bus_dvsn_dim_id_Out,
	LKP_Produc_Code_Dim.prdct_code_dim_id,
	-- *INF*: IIF(ISNULL(prdct_code_dim_id),-1,prdct_code_dim_id)
	IFF(prdct_code_dim_id IS NULL, - 1, prdct_code_dim_id) AS prdct_code_dim_id_Out
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
UPD_ASLID_LossMasterDimID AS (
	SELECT
	loss_master_fact_id, 
	asl_dim_id_Out AS asl_dim_id, 
	loss_master_dim_id_out AS loss_master_dim_id, 
	strtgc_bus_dvsn_dim_id_Out AS strtgc_bus_dvsn_dim_id, 
	prdct_code_dim_id_Out AS prdct_code_dim_id
	FROM EXP_Values
),
TGT_loss_master_fact_UPDATE_ASL_DCTClaims AS (
	MERGE INTO loss_master_fact AS T
	USING UPD_ASLID_LossMasterDimID AS S
	ON T.loss_master_fact_id = S.loss_master_fact_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.loss_master_dim_id = S.loss_master_dim_id, T.asl_dim_id = S.asl_dim_id, T.strtgc_bus_dvsn_dim_id = S.strtgc_bus_dvsn_dim_id, T.prdct_code_dim_id = S.prdct_code_dim_id
),