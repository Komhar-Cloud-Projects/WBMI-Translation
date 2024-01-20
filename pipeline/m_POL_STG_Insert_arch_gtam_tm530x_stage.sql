WITH
SQ_gtam_tm530x_stage1 AS (
	SELECT
		gtam_tm530x_stage_id,
		table_fld,
		key_len,
		location,
		master_company_number,
		state,
		class_description_code,
		crime_indicator,
		expiration_date,
		data_len,
		pkg_mod_asgn_code,
		secondary_pma_code,
		gen_liability_prem_basis,
		gen_liability_exp_basis,
		exclusion_opt_1,
		exclusion_opt_2,
		exclusion_opt_3,
		premises_limit_table,
		products_limit_table,
		premises_guide_a_ind,
		products_guide_a_ind,
		store_keepers_min_prem_ind,
		trans_program_ind,
		special_medical_pay,
		gen_liab_footnote,
		fire_csp_code,
		contents_rate_group,
		free_footnote,
		screen_class_descr,
		decl_class_descr_source,
		old_premium_basis_premops,
		old_premium_basis_prodop,
		reg_chg_class_desc_code_or_chg_date,
		prop_class_limit,
		reg_chg_date_ind,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_tm530x_stage1
),
LKP_arch_tm530x_stage AS (
	SELECT
	arch_gtam_tm530x_stage_id,
	pkg_mod_asgn_code,
	secondary_pma_code,
	gen_liability_prem_basis,
	gen_liability_exp_basis,
	exclusion_opt_1,
	exclusion_opt_2,
	exclusion_opt_3,
	premises_limit_table,
	products_limit_table,
	premises_guide_a_ind,
	products_guide_a_ind,
	store_keepers_min_prem_ind,
	trans_program_ind,
	special_medical_pay,
	gen_liab_footnote,
	fire_csp_code,
	contents_rate_group,
	free_footnote,
	screen_class_descr,
	decl_class_descr_source,
	old_premium_basis_premops,
	old_premium_basis_prodop,
	reg_chg_class_desc_code_or_chg_date,
	prop_class_limit,
	reg_chg_date_ind,
	location,
	master_company_number,
	state,
	class_description_code,
	crime_indicator,
	expiration_date
	FROM (
		SELECT  tl.arch_gtam_tm530x_stage_id as arch_gtam_tm530x_stage_id
		       , tl.location as location
		      , tl.master_company_number as master_company_number 
		      , tl.state as state 
		      , tl.class_description_code as class_description_code
		      , tl.crime_indicator as crime_indicator
		      , tl.expiration_date as     expiration_date   
		      , tl.pkg_mod_asgn_code as  pkg_mod_asgn_code
		      , tl.secondary_pma_code as secondary_pma_code
		      , tl.gen_liability_prem_basis as gen_liability_prem_basis
		      , tl.gen_liability_exp_basis as gen_liability_exp_basis
		      , tl.exclusion_opt_1 as exclusion_opt_1
		      , tl.exclusion_opt_2 as exclusion_opt_2
		      , tl.exclusion_opt_3 as exclusion_opt_3 
		      , tl.premises_limit_table as premises_limit_table
		      , tl.products_limit_table as products_limit_table
		      , tl.premises_guide_a_ind as premises_guide_a_ind
		      , tl.products_guide_a_ind as products_guide_a_ind
		      , tl.store_keepers_min_prem_ind as store_keepers_min_prem_ind
		      , tl.trans_program_ind as trans_program_ind 
		      , tl.special_medical_pay as special_medical_pay
		      , tl.gen_liab_footnote as gen_liab_footnote
		      , tl.fire_csp_code as fire_csp_code 
		      , tl.contents_rate_group as contents_rate_group
		      , tl.free_footnote as free_footnote
		      , tl.screen_class_descr as screen_class_descr
		      , tl.decl_class_descr_source as decl_class_descr_source
		      , tl.old_premium_basis_premops as old_premium_basis_premops
		      , tl.old_premium_basis_prodop as old_premium_basis_prodop
		      , tl.reg_chg_class_desc_code_or_chg_date as reg_chg_class_desc_code_or_chg_date 
		      , tl.prop_class_limit as prop_class_limit
		      , tl.reg_chg_date_ind as reg_chg_date_ind       
		  FROM  arch_gtam_tm530x_stage tl
		  where 	tl.arch_gtam_tm530x_stage_id  In
			(Select max(b.arch_gtam_tm530x_stage_id) from arch_gtam_tm530x_stage b
			group by b.location
		      ,b.master_company_number
		      ,b.state 
		      ,b.class_description_code 
		      ,b.crime_indicator    
		      ,b.expiration_date  )
		  order by tl.location 
		      , tl.master_company_number
		      , tl.state 
		      , tl.class_description_code
		      , tl.crime_indicator
		      , tl.expiration_date--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY location,master_company_number,state,class_description_code,crime_indicator,expiration_date ORDER BY arch_gtam_tm530x_stage_id) = 1
),
EXP_arch_tm530xe_seq1_stage1 AS (
	SELECT
	SQ_gtam_tm530x_stage1.gtam_tm530x_stage_id,
	SQ_gtam_tm530x_stage1.table_fld AS Table_fld,
	SQ_gtam_tm530x_stage1.key_len AS Key_len,
	SQ_gtam_tm530x_stage1.location,
	SQ_gtam_tm530x_stage1.master_company_number,
	SQ_gtam_tm530x_stage1.state,
	SQ_gtam_tm530x_stage1.class_description_code,
	SQ_gtam_tm530x_stage1.crime_indicator,
	SQ_gtam_tm530x_stage1.expiration_date,
	SQ_gtam_tm530x_stage1.data_len,
	SQ_gtam_tm530x_stage1.pkg_mod_asgn_code,
	SQ_gtam_tm530x_stage1.secondary_pma_code,
	SQ_gtam_tm530x_stage1.gen_liability_prem_basis,
	SQ_gtam_tm530x_stage1.gen_liability_exp_basis,
	SQ_gtam_tm530x_stage1.exclusion_opt_1,
	SQ_gtam_tm530x_stage1.exclusion_opt_2,
	SQ_gtam_tm530x_stage1.exclusion_opt_3,
	SQ_gtam_tm530x_stage1.premises_limit_table,
	SQ_gtam_tm530x_stage1.products_limit_table,
	SQ_gtam_tm530x_stage1.premises_guide_a_ind,
	SQ_gtam_tm530x_stage1.products_guide_a_ind,
	SQ_gtam_tm530x_stage1.store_keepers_min_prem_ind,
	SQ_gtam_tm530x_stage1.trans_program_ind,
	SQ_gtam_tm530x_stage1.special_medical_pay,
	SQ_gtam_tm530x_stage1.gen_liab_footnote,
	SQ_gtam_tm530x_stage1.fire_csp_code,
	SQ_gtam_tm530x_stage1.contents_rate_group,
	SQ_gtam_tm530x_stage1.free_footnote,
	SQ_gtam_tm530x_stage1.screen_class_descr,
	SQ_gtam_tm530x_stage1.decl_class_descr_source,
	SQ_gtam_tm530x_stage1.old_premium_basis_premops,
	SQ_gtam_tm530x_stage1.old_premium_basis_prodop,
	SQ_gtam_tm530x_stage1.reg_chg_class_desc_code_or_chg_date,
	SQ_gtam_tm530x_stage1.prop_class_limit,
	SQ_gtam_tm530x_stage1.reg_chg_date_ind,
	SQ_gtam_tm530x_stage1.extract_date AS EXTRACT_DATE,
	SQ_gtam_tm530x_stage1.as_of_date AS AS_OF_DATE,
	SQ_gtam_tm530x_stage1.record_count AS RECORD_COUNT,
	SQ_gtam_tm530x_stage1.source_system_id AS SOURCE_SYSTEM_ID,
	LKP_arch_tm530x_stage.arch_gtam_tm530x_stage_id AS LKP_arch_gtam_tm530x_stage_id,
	LKP_arch_tm530x_stage.pkg_mod_asgn_code AS LKP_pkg_mod_asgn_code,
	LKP_arch_tm530x_stage.secondary_pma_code AS LKP_secondary_pma_code,
	LKP_arch_tm530x_stage.gen_liability_prem_basis AS LKP_gen_liability_prem_basis,
	LKP_arch_tm530x_stage.gen_liability_exp_basis AS LKP_gen_liability_exp_basis,
	LKP_arch_tm530x_stage.exclusion_opt_1 AS LKP_exclusion_opt_1,
	LKP_arch_tm530x_stage.exclusion_opt_2 AS LKP_exclusion_opt_2,
	LKP_arch_tm530x_stage.exclusion_opt_3 AS LKP_exclusion_opt_3,
	LKP_arch_tm530x_stage.premises_limit_table AS LKP_premises_limit_table,
	LKP_arch_tm530x_stage.products_limit_table AS LKP_products_limit_table,
	LKP_arch_tm530x_stage.premises_guide_a_ind AS LKP_premises_guide_a_ind,
	LKP_arch_tm530x_stage.products_guide_a_ind AS LKP_products_guide_a_ind,
	LKP_arch_tm530x_stage.store_keepers_min_prem_ind AS LKP_store_keepers_min_prem_ind,
	LKP_arch_tm530x_stage.trans_program_ind AS LKP_trans_program_ind,
	LKP_arch_tm530x_stage.special_medical_pay AS LKP_special_medical_pay,
	LKP_arch_tm530x_stage.gen_liab_footnote AS LKP_gen_liab_footnote,
	LKP_arch_tm530x_stage.fire_csp_code AS LKP_fire_csp_code,
	LKP_arch_tm530x_stage.contents_rate_group AS LKP_contents_rate_group,
	LKP_arch_tm530x_stage.free_footnote AS LKP_free_footnote,
	LKP_arch_tm530x_stage.screen_class_descr AS LKP_screen_class_descr,
	LKP_arch_tm530x_stage.decl_class_descr_source AS LKP_decl_class_descr_source,
	LKP_arch_tm530x_stage.old_premium_basis_premops AS LKP_old_premium_basis_premops,
	LKP_arch_tm530x_stage.old_premium_basis_prodop AS LKP_old_premium_basis_prodop,
	LKP_arch_tm530x_stage.reg_chg_class_desc_code_or_chg_date AS LKP_reg_chg_class_desc_code_or_chg_date,
	LKP_arch_tm530x_stage.prop_class_limit AS LKP_prop_class_limit,
	LKP_arch_tm530x_stage.reg_chg_date_ind AS LKP_reg_chg_date_ind,
	-- *INF*: iif(isnull(LKP_arch_gtam_tm530x_stage_id),'NEW',
	--     iif(
	-- rtrim(ltrim( LKP_pkg_mod_asgn_code )) <>  rtrim(ltrim( pkg_mod_asgn_code ))    
	-- OR  rtrim(ltrim( LKP_secondary_pma_code))    <> rtrim(ltrim( secondary_pma_code))     
	-- 
	-- OR  rtrim(ltrim( LKP_gen_liability_prem_basis))    <> rtrim(ltrim( gen_liability_prem_basis))     
	-- OR  rtrim(ltrim( LKP_gen_liability_exp_basis))    <> rtrim(ltrim( gen_liability_exp_basis))     
	-- OR  rtrim(ltrim( LKP_exclusion_opt_1))    <> rtrim(ltrim( exclusion_opt_1))     
	-- OR  rtrim(ltrim( LKP_exclusion_opt_2))    <> rtrim(ltrim( exclusion_opt_2))     
	-- OR  rtrim(ltrim( LKP_exclusion_opt_3))    <> rtrim(ltrim( exclusion_opt_3))     
	--  
	-- 
	-- OR  rtrim(ltrim( LKP_premises_limit_table))    <> rtrim(ltrim( premises_limit_table))   
	-- OR  rtrim(ltrim( LKP_products_limit_table ))    <> rtrim(ltrim( products_limit_table ))   
	-- OR  rtrim(ltrim( LKP_premises_guide_a_ind))    <> rtrim(ltrim( premises_guide_a_ind))   
	-- OR  rtrim(ltrim( LKP_products_guide_a_ind))    <> rtrim(ltrim( products_guide_a_ind)) 
	-- 
	-- 
	-- OR  rtrim(ltrim( LKP_store_keepers_min_prem_ind))    <> rtrim(ltrim( store_keepers_min_prem_ind))   
	-- OR  rtrim(ltrim( LKP_trans_program_ind))    <> rtrim(ltrim( trans_program_ind))   
	-- OR  rtrim(ltrim( LKP_special_medical_pay))    <> rtrim(ltrim( special_medical_pay)) 
	-- 
	-- OR  rtrim(ltrim( LKP_gen_liab_footnote)   )  <> rtrim(ltrim( gen_liab_footnote))   
	-- OR  rtrim(ltrim( LKP_fire_csp_code ))    <> rtrim(ltrim( fire_csp_code ))   
	-- OR  rtrim(ltrim( LKP_contents_rate_group))    <> rtrim(ltrim( contents_rate_group))   
	-- 
	-- OR  rtrim(ltrim( LKP_free_footnote ))    <> rtrim(ltrim( free_footnote ))   
	-- OR  rtrim(ltrim( LKP_screen_class_descr))    <> rtrim(ltrim( screen_class_descr))   
	-- 
	-- OR  rtrim(ltrim( LKP_decl_class_descr_source ))  <> rtrim(ltrim( decl_class_descr_source )) 
	-- OR  rtrim(ltrim( LKP_old_premium_basis_premops )) <> rtrim(ltrim( old_premium_basis_premops )) 
	-- OR  rtrim(ltrim( LKP_old_premium_basis_prodop ))  <> rtrim(ltrim( old_premium_basis_prodop )) 
	-- 
	-- OR  rtrim(ltrim( LKP_reg_chg_class_desc_code_or_chg_date ))    
	-- <> rtrim(ltrim( reg_chg_class_desc_code_or_chg_date )) 
	-- 
	-- OR  rtrim(ltrim( LKP_prop_class_limit ))    <> rtrim(ltrim( prop_class_limit )) 
	-- OR  rtrim(ltrim( LKP_reg_chg_date_ind ))    <> rtrim(ltrim( reg_chg_date_ind )) 
	-- 
	-- 
	-- 
	-- , 'UPDATE', 'NOCHANGE'))
	IFF(
	    LKP_arch_gtam_tm530x_stage_id IS NULL, 'NEW',
	    IFF(
	        rtrim(ltrim(LKP_pkg_mod_asgn_code)) <> rtrim(ltrim(pkg_mod_asgn_code))
	        or rtrim(ltrim(LKP_secondary_pma_code)) <> rtrim(ltrim(secondary_pma_code))
	        or rtrim(ltrim(LKP_gen_liability_prem_basis)) <> rtrim(ltrim(gen_liability_prem_basis))
	        or rtrim(ltrim(LKP_gen_liability_exp_basis)) <> rtrim(ltrim(gen_liability_exp_basis))
	        or rtrim(ltrim(LKP_exclusion_opt_1)) <> rtrim(ltrim(exclusion_opt_1))
	        or rtrim(ltrim(LKP_exclusion_opt_2)) <> rtrim(ltrim(exclusion_opt_2))
	        or rtrim(ltrim(LKP_exclusion_opt_3)) <> rtrim(ltrim(exclusion_opt_3))
	        or rtrim(ltrim(LKP_premises_limit_table)) <> rtrim(ltrim(premises_limit_table))
	        or rtrim(ltrim(LKP_products_limit_table)) <> rtrim(ltrim(products_limit_table))
	        or rtrim(ltrim(LKP_premises_guide_a_ind)) <> rtrim(ltrim(premises_guide_a_ind))
	        or rtrim(ltrim(LKP_products_guide_a_ind)) <> rtrim(ltrim(products_guide_a_ind))
	        or rtrim(ltrim(LKP_store_keepers_min_prem_ind)) <> rtrim(ltrim(store_keepers_min_prem_ind))
	        or rtrim(ltrim(LKP_trans_program_ind)) <> rtrim(ltrim(trans_program_ind))
	        or rtrim(ltrim(LKP_special_medical_pay)) <> rtrim(ltrim(special_medical_pay))
	        or rtrim(ltrim(LKP_gen_liab_footnote)) <> rtrim(ltrim(gen_liab_footnote))
	        or rtrim(ltrim(LKP_fire_csp_code)) <> rtrim(ltrim(fire_csp_code))
	        or rtrim(ltrim(LKP_contents_rate_group)) <> rtrim(ltrim(contents_rate_group))
	        or rtrim(ltrim(LKP_free_footnote)) <> rtrim(ltrim(free_footnote))
	        or rtrim(ltrim(LKP_screen_class_descr)) <> rtrim(ltrim(screen_class_descr))
	        or rtrim(ltrim(LKP_decl_class_descr_source)) <> rtrim(ltrim(decl_class_descr_source))
	        or rtrim(ltrim(LKP_old_premium_basis_premops)) <> rtrim(ltrim(old_premium_basis_premops))
	        or rtrim(ltrim(LKP_old_premium_basis_prodop)) <> rtrim(ltrim(old_premium_basis_prodop))
	        or rtrim(ltrim(LKP_reg_chg_class_desc_code_or_chg_date)) <> rtrim(ltrim(reg_chg_class_desc_code_or_chg_date))
	        or rtrim(ltrim(LKP_prop_class_limit)) <> rtrim(ltrim(prop_class_limit))
	        or rtrim(ltrim(LKP_reg_chg_date_ind)) <> rtrim(ltrim(reg_chg_date_ind)),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_Changed_Flag,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID
	FROM SQ_gtam_tm530x_stage1
	LEFT JOIN LKP_arch_tm530x_stage
	ON LKP_arch_tm530x_stage.location = SQ_gtam_tm530x_stage1.location AND LKP_arch_tm530x_stage.master_company_number = SQ_gtam_tm530x_stage1.master_company_number AND LKP_arch_tm530x_stage.state = SQ_gtam_tm530x_stage1.state AND LKP_arch_tm530x_stage.class_description_code = SQ_gtam_tm530x_stage1.class_description_code AND LKP_arch_tm530x_stage.crime_indicator = SQ_gtam_tm530x_stage1.crime_indicator AND LKP_arch_tm530x_stage.expiration_date = SQ_gtam_tm530x_stage1.expiration_date
),
FIL_Inserts AS (
	SELECT
	gtam_tm530x_stage_id, 
	Table_fld, 
	Key_len, 
	location, 
	master_company_number, 
	state, 
	class_description_code, 
	crime_indicator, 
	expiration_date, 
	data_len, 
	pkg_mod_asgn_code, 
	secondary_pma_code, 
	gen_liability_prem_basis, 
	gen_liability_exp_basis, 
	exclusion_opt_1, 
	exclusion_opt_2, 
	exclusion_opt_3, 
	premises_limit_table, 
	products_limit_table, 
	premises_guide_a_ind, 
	products_guide_a_ind, 
	store_keepers_min_prem_ind, 
	trans_program_ind, 
	special_medical_pay, 
	gen_liab_footnote, 
	fire_csp_code, 
	contents_rate_group, 
	free_footnote, 
	screen_class_descr, 
	decl_class_descr_source, 
	old_premium_basis_premops, 
	old_premium_basis_prodop, 
	reg_chg_class_desc_code_or_chg_date, 
	prop_class_limit, 
	reg_chg_date_ind, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	Changed_Flag, 
	AUDIT_ID
	FROM EXP_arch_tm530xe_seq1_stage1
	WHERE Changed_Flag = 'NEW' or Changed_Flag = 'UPDATE'
),
arch_gtam_tm530x_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_gtam_tm530x_stage
	(gtam_tm530x_stage_id, table_fld, key_len, location, master_company_number, state, class_description_code, crime_indicator, expiration_date, data_len, pkg_mod_asgn_code, secondary_pma_code, gen_liability_prem_basis, gen_liability_exp_basis, exclusion_opt_1, exclusion_opt_2, exclusion_opt_3, premises_limit_table, products_limit_table, premises_guide_a_ind, products_guide_a_ind, store_keepers_min_prem_ind, trans_program_ind, special_medical_pay, gen_liab_footnote, fire_csp_code, contents_rate_group, free_footnote, screen_class_descr, decl_class_descr_source, old_premium_basis_premops, old_premium_basis_prodop, reg_chg_class_desc_code_or_chg_date, prop_class_limit, reg_chg_date_ind, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	GTAM_TM530X_STAGE_ID, 
	Table_fld AS TABLE_FLD, 
	Key_len AS KEY_LEN, 
	LOCATION, 
	MASTER_COMPANY_NUMBER, 
	STATE, 
	CLASS_DESCRIPTION_CODE, 
	CRIME_INDICATOR, 
	EXPIRATION_DATE, 
	DATA_LEN, 
	PKG_MOD_ASGN_CODE, 
	SECONDARY_PMA_CODE, 
	GEN_LIABILITY_PREM_BASIS, 
	GEN_LIABILITY_EXP_BASIS, 
	EXCLUSION_OPT_1, 
	EXCLUSION_OPT_2, 
	EXCLUSION_OPT_3, 
	PREMISES_LIMIT_TABLE, 
	PRODUCTS_LIMIT_TABLE, 
	PREMISES_GUIDE_A_IND, 
	PRODUCTS_GUIDE_A_IND, 
	STORE_KEEPERS_MIN_PREM_IND, 
	TRANS_PROGRAM_IND, 
	SPECIAL_MEDICAL_PAY, 
	GEN_LIAB_FOOTNOTE, 
	FIRE_CSP_CODE, 
	CONTENTS_RATE_GROUP, 
	FREE_FOOTNOTE, 
	SCREEN_CLASS_DESCR, 
	DECL_CLASS_DESCR_SOURCE, 
	OLD_PREMIUM_BASIS_PREMOPS, 
	OLD_PREMIUM_BASIS_PRODOP, 
	REG_CHG_CLASS_DESC_CODE_OR_CHG_DATE, 
	PROP_CLASS_LIMIT, 
	REG_CHG_DATE_IND, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID AS AUDIT_ID
	FROM FIL_Inserts
),