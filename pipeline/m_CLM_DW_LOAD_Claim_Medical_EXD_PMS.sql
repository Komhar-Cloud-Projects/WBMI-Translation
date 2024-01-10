WITH
SQ_claim_medical_stage AS (
	SELECT
		claim_medical_stage_id,
		injured_party_id,
		medicare_hicn,
		medicare_elig,
		cms_date_of_incid,
		cause_code,
		diag_code1,
		diag_code2,
		diag_code3,
		diag_code4,
		diag_code5,
		diag_code6,
		diag_code7,
		diag_code8,
		diag_code9,
		diag_code10,
		diag_code11,
		diag_code12,
		diag_code13,
		diag_code14,
		diag_code15,
		diag_code16,
		diag_code17,
		diag_code18,
		diag_code19,
		self_insd_ind,
		self_insd_type,
		self_insd_fst_nm,
		self_insd_last_nm,
		self_insd_dba_nm,
		self_insd_lgl_nm,
		product_liab_ind,
		prod_generic_nm,
		prod_brand_nm,
		prod_manufacturer,
		prod_allege_harm,
		inj_par_rep_firm,
		exceed_claim_key,
		exceed_claimnt_key,
		pms_policy_sym,
		pms_policy_num,
		pms_policy_mod,
		pms_date_of_loss,
		pms_loss_occurence,
		pms_loss_claimant,
		cms_source_system_id,
		clmt1_rep_firm,
		clmt2_rep_firm,
		clmt3_rep_firm,
		clmt4_rep_firm,
		created_ts,
		created_user_id,
		modified_ts,
		modified_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id,
		query_requested,
		query_request_date,
		last_cms_hicn,
		icd_code_version
	FROM claim_medical_stage
),
EXP_Values AS (
	SELECT
	claim_medical_stage_id,
	injured_party_id,
	medicare_hicn,
	medicare_elig,
	cms_date_of_incid,
	cause_code,
	diag_code1 AS diag_code,
	self_insd_ind,
	self_insd_type,
	self_insd_fst_nm,
	self_insd_last_nm,
	self_insd_dba_nm,
	self_insd_lgl_nm,
	product_liab_ind,
	prod_generic_nm,
	prod_brand_nm,
	prod_manufacturer,
	prod_allege_harm,
	inj_par_rep_firm,
	exceed_claim_key,
	exceed_claimnt_key,
	pms_policy_sym,
	pms_policy_num,
	pms_policy_mod,
	pms_date_of_loss,
	pms_loss_occurence,
	pms_loss_claimant,
	cms_source_system_id,
	clmt1_rep_firm,
	clmt2_rep_firm,
	clmt3_rep_firm,
	clmt4_rep_firm,
	created_ts,
	created_user_id,
	modified_ts,
	modified_user_id,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	query_requested,
	query_request_date,
	last_cms_hicn,
	icd_code_version,
	claim_medical_stage_id AS claim_medical_stage_id1,
	injured_party_id AS injured_party_id1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(medicare_hicn)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(medicare_hicn) AS medicare_hicn1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(medicare_elig)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(medicare_elig) AS medicare_elig1,
	-- *INF*: IIF(ISNULL(cms_date_of_incid), TO_DATE('1/1/1800','MM/DD/YYYY'), cms_date_of_incid)
	IFF(cms_date_of_incid IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), cms_date_of_incid) AS cms_date_of_incid1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(cause_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(cause_code) AS cause_code1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(diag_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(diag_code) AS diag_code1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(self_insd_ind)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(self_insd_ind) AS self_insd_ind1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(self_insd_type)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(self_insd_type) AS self_insd_type1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(self_insd_fst_nm)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(self_insd_fst_nm) AS self_insd_fst_nm1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(self_insd_last_nm)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(self_insd_last_nm) AS self_insd_last_nm1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(self_insd_dba_nm)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(self_insd_dba_nm) AS self_insd_dba_nm1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(self_insd_lgl_nm)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(self_insd_lgl_nm) AS self_insd_lgl_nm1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(product_liab_ind)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(product_liab_ind) AS product_liab_ind1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(prod_generic_nm)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(prod_generic_nm) AS prod_generic_nm1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(prod_brand_nm)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(prod_brand_nm) AS prod_brand_nm1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(prod_manufacturer)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(prod_manufacturer) AS prod_manufacturer1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(prod_allege_harm)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(prod_allege_harm) AS prod_allege_harm1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(inj_par_rep_firm)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(inj_par_rep_firm) AS inj_par_rep_firm1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(cms_source_system_id)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(cms_source_system_id) AS cms_source_system_id1,
	-- *INF*: replaceChr(0,to_char(pms_date_of_loss),'/','')
	replaceChr(0, to_char(pms_date_of_loss), '/', '') AS v_pms_date_of_loss,
	-- *INF*: IIF(length(exceed_claim_key)>0 ,ltrim(rtrim(exceed_claim_key)),
	-- IIF(length(pms_policy_sym)>0,ltrim(rtrim(pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss || pms_loss_occurence)),
	-- 'N/A'))
	IFF(length(exceed_claim_key) > 0, ltrim(rtrim(exceed_claim_key)), IFF(length(pms_policy_sym) > 0, ltrim(rtrim(pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss || pms_loss_occurence)), 'N/A')) AS claim_key1,
	-- *INF*: IIF(length(exceed_claim_key)>0,ltrim(rtrim(exceed_claimnt_key)),
	-- IIF(length(pms_policy_sym)>0,ltrim(rtrim(pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss || pms_loss_occurence || pms_loss_claimant || 'CMT')),
	-- 'N/A'))
	IFF(length(exceed_claim_key) > 0, ltrim(rtrim(exceed_claimnt_key)), IFF(length(pms_policy_sym) > 0, ltrim(rtrim(pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss || pms_loss_occurence || pms_loss_claimant || 'CMT')), 'N/A')) AS claimnt_key1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(clmt1_rep_firm)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(clmt1_rep_firm) AS clmt1_rep_firm1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(clmt2_rep_firm)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(clmt2_rep_firm) AS clmt2_rep_firm1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(clmt3_rep_firm)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(clmt3_rep_firm) AS clmt3_rep_firm1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(clmt4_rep_firm)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(clmt4_rep_firm) AS clmt4_rep_firm1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(query_requested)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(query_requested) AS query_requested1,
	-- *INF*: IIF(ISNULL(query_request_date), TO_DATE('1/1/1800','MM/DD/YYYY'), query_request_date)
	IFF(query_request_date IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), query_request_date) AS query_request_date1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(last_cms_hicn)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(last_cms_hicn) AS last_cms_hicn1,
	-- *INF*: iif(icd_code_version = '0','10',icd_code_version)
	IFF(icd_code_version = '0', '10', icd_code_version) AS v_icd_code_version,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_icd_code_version)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_icd_code_version) AS icd_code_version1
	FROM SQ_claim_medical_stage
),
LKP_Claim_Party_Occurrence_AK_ID AS (
	SELECT
	claim_party_occurrence_ak_id,
	claimant_num,
	claim_party_role_code
	FROM (
		SELECT 
		CPO.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
		LTRIM(RTRIM(CO.claim_occurrence_key)) as claimant_num, 
		LTRIM(RTRIM(CP.claim_party_key)) as claim_party_role_code
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence CPO,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP, 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO 
		WHERE 
		CO.claim_occurrence_ak_id = CPO.claim_occurrence_ak_id  
		AND CP.claim_party_ak_id = CPO.claim_party_ak_id 
		AND CPO.claim_party_role_code IN ('CMT' , 'CLMT')
		AND CO.crrnt_snpsht_flag = 1
		AND CP.crrnt_snpsht_flag = 1
		AND CPO.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_num,claim_party_role_code ORDER BY claim_party_occurrence_ak_id) = 1
),
LKP_Target AS (
	SELECT
	claim_med_id,
	claim_med_ak_id,
	claim_party_occurrence_ak_id,
	injured_party_id,
	medicare_hicn,
	medicare_eligibility,
	cms_incdnt_date,
	patient_cause_code,
	patient_diag_code,
	self_insd_ind,
	self_insd_type,
	self_insd_first_name,
	self_insd_last_name,
	self_insd_dba_name,
	self_insd_lgl_name,
	prdct_liab_ind,
	prdct_generic_name,
	prdct_brand_name,
	prdct_mfr,
	prdct_alleged_harm,
	injured_party_rep_firm,
	claimant1_rep_firm,
	claimant2_rep_firm,
	claimant3_rep_firm,
	claimant4_rep_firm,
	query_requested_date,
	query_requested_ind,
	last_cms_hicn,
	ICDCodeVersion
	FROM (
		SELECT CM.claim_med_id as claim_med_id, 
		CM.claim_med_ak_id as claim_med_ak_id, 
		CM.injured_party_id as injured_party_id, 
		CM.medicare_hicn as medicare_hicn, 
		CM.medicare_eligibility as medicare_eligibility, 
		CM.cms_incdnt_date as cms_incdnt_date, 
		CM.patient_cause_code as patient_cause_code, 
		patient_diag_code as patient_diag_code,
		CM.self_insd_ind as self_insd_ind, 
		CM.self_insd_type as self_insd_type, 
		CM.self_insd_first_name as self_insd_first_name, 
		CM.self_insd_last_name as self_insd_last_name, 
		CM.self_insd_dba_name as self_insd_dba_name, 
		CM.self_insd_lgl_name as self_insd_lgl_name, 
		CM.prdct_liab_ind as prdct_liab_ind, 
		CM.prdct_generic_name as prdct_generic_name, 
		CM.prdct_brand_name as prdct_brand_name, 
		CM.prdct_mfr as prdct_mfr, 
		CM.prdct_alleged_harm as prdct_alleged_harm, 
		CM.injured_party_rep_firm as injured_party_rep_firm, 
		CM.claimant1_rep_firm as claimant1_rep_firm, 
		CM.claimant2_rep_firm as claimant2_rep_firm, 
		CM.claimant3_rep_firm as claimant3_rep_firm, 
		CM.claimant4_rep_firm as claimant4_rep_firm, 
		CM.query_requested_date as query_requested_date, 
		CM.query_requested_ind as query_requested_ind, 
		CM.last_cms_hicn as last_cms_hicn,
		CM.ICDCodeVersion as ICDCodeVersion ,
		CM.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical CM
		WHERE CM.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id ORDER BY claim_med_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	EXP_Values.claim_medical_stage_id1,
	LKP_Claim_Party_Occurrence_AK_ID.claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id1,
	EXP_Values.injured_party_id1,
	EXP_Values.medicare_hicn1,
	EXP_Values.medicare_elig1,
	EXP_Values.cms_date_of_incid1,
	EXP_Values.cause_code1,
	EXP_Values.diag_code1,
	EXP_Values.self_insd_ind1,
	EXP_Values.self_insd_type1,
	EXP_Values.self_insd_fst_nm1,
	EXP_Values.self_insd_last_nm1,
	EXP_Values.self_insd_dba_nm1,
	EXP_Values.self_insd_lgl_nm1,
	EXP_Values.product_liab_ind1,
	EXP_Values.prod_generic_nm1,
	EXP_Values.prod_brand_nm1,
	EXP_Values.prod_manufacturer1,
	EXP_Values.prod_allege_harm1,
	EXP_Values.inj_par_rep_firm1,
	EXP_Values.clmt1_rep_firm1,
	EXP_Values.clmt2_rep_firm1,
	EXP_Values.clmt3_rep_firm1,
	EXP_Values.clmt4_rep_firm1,
	EXP_Values.query_requested1,
	EXP_Values.query_request_date1,
	EXP_Values.last_cms_hicn1,
	EXP_Values.icd_code_version1,
	LKP_Target.claim_med_id,
	LKP_Target.claim_med_ak_id,
	LKP_Target.claim_party_occurrence_ak_id,
	LKP_Target.injured_party_id AS cms_document_cntl_num,
	LKP_Target.medicare_hicn,
	LKP_Target.medicare_eligibility,
	LKP_Target.cms_incdnt_date,
	LKP_Target.patient_cause_code,
	LKP_Target.patient_diag_code,
	LKP_Target.self_insd_ind,
	LKP_Target.self_insd_type,
	LKP_Target.self_insd_first_name,
	LKP_Target.self_insd_last_name,
	LKP_Target.self_insd_dba_name,
	LKP_Target.self_insd_lgl_name,
	LKP_Target.prdct_liab_ind,
	LKP_Target.prdct_generic_name,
	LKP_Target.prdct_brand_name,
	LKP_Target.prdct_mfr,
	LKP_Target.prdct_alleged_harm,
	LKP_Target.injured_party_rep_firm,
	LKP_Target.claimant1_rep_firm,
	LKP_Target.claimant2_rep_firm,
	LKP_Target.claimant3_rep_firm,
	LKP_Target.claimant4_rep_firm,
	LKP_Target.query_requested_date AS query_request_date,
	LKP_Target.query_requested_ind,
	LKP_Target.last_cms_hicn,
	LKP_Target.ICDCodeVersion,
	-- *INF*: iif(isnull(claim_med_id),'NEW',	
	-- 	iif (
	-- injured_party_id1 <> cms_document_cntl_num OR
	-- medicare_hicn1 <> medicare_hicn OR
	-- medicare_elig1 <> medicare_eligibility OR
	-- cms_date_of_incid1 <> cms_incdnt_date OR
	-- cause_code1 <> patient_cause_code OR
	-- diag_code1 <> patient_diag_code OR
	-- self_insd_ind1 <> self_insd_ind OR
	-- self_insd_type1 <> self_insd_type OR
	-- self_insd_fst_nm1 <> self_insd_first_name OR
	-- self_insd_last_nm1 <> self_insd_last_name OR
	-- self_insd_dba_nm1 <> self_insd_dba_name OR
	-- self_insd_lgl_nm1 <> self_insd_lgl_name OR
	-- product_liab_ind1 <> prdct_liab_ind OR
	-- prod_generic_nm1 <> prdct_generic_name OR
	-- prod_brand_nm1 <> prdct_brand_name OR
	-- prod_manufacturer1 <> prdct_mfr OR
	-- prod_allege_harm1 <> prdct_alleged_harm OR
	-- inj_par_rep_firm1 <> injured_party_rep_firm OR
	-- clmt1_rep_firm1 <> claimant1_rep_firm OR
	-- clmt2_rep_firm1 <> claimant2_rep_firm OR
	-- clmt3_rep_firm1 <> claimant3_rep_firm OR
	-- clmt4_rep_firm1 <> claimant4_rep_firm OR
	-- query_requested1 <> query_requested_ind OR
	-- query_request_date1 <> query_request_date OR
	-- last_cms_hicn1 <> last_cms_hicn  OR 
	-- icd_code_version1 <> ICDCodeVersion
	-- , 'UPDATE','NOCHANGE'))
	IFF(claim_med_id IS NULL, 'NEW', IFF(injured_party_id1 <> cms_document_cntl_num OR medicare_hicn1 <> medicare_hicn OR medicare_elig1 <> medicare_eligibility OR cms_date_of_incid1 <> cms_incdnt_date OR cause_code1 <> patient_cause_code OR diag_code1 <> patient_diag_code OR self_insd_ind1 <> self_insd_ind OR self_insd_type1 <> self_insd_type OR self_insd_fst_nm1 <> self_insd_first_name OR self_insd_last_nm1 <> self_insd_last_name OR self_insd_dba_nm1 <> self_insd_dba_name OR self_insd_lgl_nm1 <> self_insd_lgl_name OR product_liab_ind1 <> prdct_liab_ind OR prod_generic_nm1 <> prdct_generic_name OR prod_brand_nm1 <> prdct_brand_name OR prod_manufacturer1 <> prdct_mfr OR prod_allege_harm1 <> prdct_alleged_harm OR inj_par_rep_firm1 <> injured_party_rep_firm OR clmt1_rep_firm1 <> claimant1_rep_firm OR clmt2_rep_firm1 <> claimant2_rep_firm OR clmt3_rep_firm1 <> claimant3_rep_firm OR clmt4_rep_firm1 <> claimant4_rep_firm OR query_requested1 <> query_requested_ind OR query_request_date1 <> query_request_date OR last_cms_hicn1 <> last_cms_hicn OR icd_code_version1 <> ICDCodeVersion, 'UPDATE', 'NOCHANGE')) AS v_Changed_Flag,
	1 AS Crrnt_Snpsht_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_Id,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	SYSDATE)
	IFF(v_Changed_Flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS Eff_To_Date,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date
	FROM EXP_Values
	LEFT JOIN LKP_Claim_Party_Occurrence_AK_ID
	ON LKP_Claim_Party_Occurrence_AK_ID.claimant_num = EXP_Values.claim_key1 AND LKP_Claim_Party_Occurrence_AK_ID.claim_party_role_code = EXP_Values.claimnt_key1
	LEFT JOIN LKP_Target
	ON LKP_Target.claim_party_occurrence_ak_id = LKP_Claim_Party_Occurrence_AK_ID.claim_party_occurrence_ak_id
),
FIL_Insert AS (
	SELECT
	claim_party_occurrence_ak_id1, 
	injured_party_id1, 
	medicare_hicn1, 
	medicare_elig1, 
	cms_date_of_incid1, 
	cause_code1, 
	diag_code1, 
	self_insd_ind1, 
	self_insd_type1, 
	self_insd_fst_nm1, 
	self_insd_last_nm1, 
	self_insd_dba_nm1, 
	self_insd_lgl_nm1, 
	product_liab_ind1, 
	prod_generic_nm1, 
	prod_brand_nm1, 
	prod_manufacturer1, 
	prod_allege_harm1, 
	inj_par_rep_firm1, 
	clmt1_rep_firm1, 
	clmt2_rep_firm1, 
	clmt3_rep_firm1, 
	clmt4_rep_firm1, 
	query_requested1 AS query_requested_ind1, 
	query_request_date1, 
	claim_med_ak_id, 
	Crrnt_Snpsht_Flag, 
	Audit_Id, 
	Eff_From_Date, 
	Eff_To_Date, 
	Changed_Flag, 
	SOURCE_SYSTEM_ID, 
	Created_Date, 
	Modified_Date, 
	last_cms_hicn1, 
	icd_code_version1
	FROM EXP_Detect_Changes
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
SEQ_claim_medical AS (
	CREATE SEQUENCE SEQ_claim_medical
	START = 0
	INCREMENT = 1;
),
EXP_Insert AS (
	SELECT
	claim_party_occurrence_ak_id1,
	injured_party_id1,
	medicare_hicn1,
	medicare_elig1,
	cms_date_of_incid1,
	cause_code1,
	diag_code1,
	self_insd_ind1,
	self_insd_type1,
	self_insd_fst_nm1,
	self_insd_last_nm1,
	self_insd_dba_nm1,
	self_insd_lgl_nm1,
	product_liab_ind1,
	prod_generic_nm1,
	prod_brand_nm1,
	prod_manufacturer1,
	prod_allege_harm1,
	inj_par_rep_firm1,
	clmt1_rep_firm1,
	clmt2_rep_firm1,
	clmt3_rep_firm1,
	clmt4_rep_firm1,
	query_requested_ind1,
	query_request_date1,
	claim_med_ak_id,
	Crrnt_Snpsht_Flag,
	Audit_Id,
	Eff_From_Date,
	Eff_To_Date,
	Changed_Flag,
	SOURCE_SYSTEM_ID,
	Created_Date,
	Modified_Date,
	SEQ_claim_medical.NEXTVAL,
	-- *INF*: IIF(Changed_Flag='NEW', NEXTVAL, claim_med_ak_id)
	IFF(Changed_Flag = 'NEW', NEXTVAL, claim_med_ak_id) AS claim_med_ak_id_out,
	last_cms_hicn1,
	icd_code_version1
	FROM FIL_Insert
),
claim_medical_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, claim_med_ak_id, claim_party_occurrence_ak_id, injured_party_id, medicare_hicn, medicare_eligibility, cms_incdnt_date, patient_cause_code, patient_diag_code, self_insd_ind, self_insd_type, self_insd_first_name, self_insd_last_name, self_insd_dba_name, self_insd_lgl_name, prdct_liab_ind, prdct_generic_name, prdct_brand_name, prdct_mfr, prdct_alleged_harm, injured_party_rep_firm, claimant1_rep_firm, claimant2_rep_firm, claimant3_rep_firm, claimant4_rep_firm, query_requested_date, query_requested_ind, last_cms_hicn, ICDCodeVersion)
	SELECT 
	Crrnt_Snpsht_Flag AS CRRNT_SNPSHT_FLAG, 
	Audit_Id AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	claim_med_ak_id_out AS CLAIM_MED_AK_ID, 
	claim_party_occurrence_ak_id1 AS CLAIM_PARTY_OCCURRENCE_AK_ID, 
	injured_party_id1 AS INJURED_PARTY_ID, 
	medicare_hicn1 AS MEDICARE_HICN, 
	medicare_elig1 AS MEDICARE_ELIGIBILITY, 
	cms_date_of_incid1 AS CMS_INCDNT_DATE, 
	cause_code1 AS PATIENT_CAUSE_CODE, 
	diag_code1 AS PATIENT_DIAG_CODE, 
	self_insd_ind1 AS SELF_INSD_IND, 
	self_insd_type1 AS SELF_INSD_TYPE, 
	self_insd_fst_nm1 AS SELF_INSD_FIRST_NAME, 
	self_insd_last_nm1 AS SELF_INSD_LAST_NAME, 
	self_insd_dba_nm1 AS SELF_INSD_DBA_NAME, 
	self_insd_lgl_nm1 AS SELF_INSD_LGL_NAME, 
	product_liab_ind1 AS PRDCT_LIAB_IND, 
	prod_generic_nm1 AS PRDCT_GENERIC_NAME, 
	prod_brand_nm1 AS PRDCT_BRAND_NAME, 
	prod_manufacturer1 AS PRDCT_MFR, 
	prod_allege_harm1 AS PRDCT_ALLEGED_HARM, 
	inj_par_rep_firm1 AS INJURED_PARTY_REP_FIRM, 
	clmt1_rep_firm1 AS CLAIMANT1_REP_FIRM, 
	clmt2_rep_firm1 AS CLAIMANT2_REP_FIRM, 
	clmt3_rep_firm1 AS CLAIMANT3_REP_FIRM, 
	clmt4_rep_firm1 AS CLAIMANT4_REP_FIRM, 
	query_request_date1 AS QUERY_REQUESTED_DATE, 
	query_requested_ind1 AS QUERY_REQUESTED_IND, 
	last_cms_hicn1 AS LAST_CMS_HICN, 
	icd_code_version1 AS ICDCODEVERSION
	FROM EXP_Insert
),
SQ_claim_medical AS (
	SELECT 
	a.claim_med_id, 
	a.eff_from_date, 
	a.eff_to_date, 
	a.claim_med_ak_id 
	FROM
	 	@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical a
	WHERE 
	 a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND  
	 EXISTS (SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical b
			WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.claim_med_ak_id  = b.claim_med_ak_id 
			GROUP BY claim_med_ak_id
			HAVING COUNT(*) > 1)
	ORDER BY claim_med_ak_id, eff_from_date  DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	claim_med_id,
	claim_med_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	claim_med_ak_id = v_PREV_ROW_claim_med_ak_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
		claim_med_ak_id = v_PREV_ROW_claim_med_ak_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	claim_med_ak_id AS v_PREV_ROW_claim_med_ak_id,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_claim_medical
),
FIL_FirstRowInAKGroup AS (
	SELECT
	claim_med_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <> eff_to_date

--If these two dates equal each other we are dealing with the first row in an AK group.  This row
--does not need to be expired or updated for any reason thus it can be filtered out
-- but we must source it to capture the eff_from_date of this row 
--so that we can properly expire the subsequent row
),
UPD_Claim_Occurrence AS (
	SELECT
	claim_med_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_FirstRowInAKGroup
),
claim_medical_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical AS T
	USING UPD_Claim_Occurrence AS S
	ON T.claim_med_id = S.claim_med_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),