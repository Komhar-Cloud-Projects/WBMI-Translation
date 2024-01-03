WITH
SQ_cms_pms_relation_stage AS (
	SELECT cms_pms_relation_stage.cms_party_type, cms_pms_relation_stage.last_name, cms_pms_relation_stage.first_name, cms_pms_relation_stage.tin, cms_pms_relation_stage.address1, cms_pms_relation_stage.address2, cms_pms_relation_stage.city, cms_pms_relation_stage.state, cms_pms_relation_stage.zip_code, cms_pms_relation_stage.zip4, cms_pms_relation_stage.phone, cms_pms_relation_stage.phone_ext, claim_medical_stage.pms_policy_sym, claim_medical_stage.pms_policy_num, claim_medical_stage.pms_policy_mod, claim_medical_stage.pms_date_of_loss, claim_medical_stage.pms_loss_occurence, claim_medical_stage.pms_loss_claimant 
	FROM
	 cms_pms_relation_stage, claim_medical_stage
	WHERE
	claim_medical_stage.injured_party_id=cms_pms_relation_stage.injured_party_id
	AND cms_pms_relation_stage.cms_party_type <> 'MINJ'
	
	
	--MINJ (injured party) will be already present with CMT suffix in the claim_party_key) so need not insert these records
	--AND claim_medical_stage.cms_source_system_id = 'PMS'  (This is not required as cms_pms_relation_stage contains data only for doc_nums where PMS injured partties.)
),
EXP_Values AS (
	SELECT
	pms_policy_sym,
	pms_policy_num,
	pms_policy_mod,
	pms_date_of_loss,
	pms_loss_occurence,
	pms_loss_claimant,
	cms_party_type,
	-- *INF*: replaceChr(0,to_char(pms_date_of_loss),'/','')
	replaceChr(0, to_char(pms_date_of_loss), '/', '') AS v_pms_date_of_loss,
	pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss || pms_loss_occurence || pms_loss_claimant || cms_party_type AS claim_party_key,
	last_name AS claim_party_last_name,
	first_name AS claim_party_first_name,
	'N/A' AS claim_party_mid_name,
	'N/A' AS claim_party_name_prfx,
	'N/A' AS claim_party_name_sfx,
	claim_party_first_name || ' ' ||  claim_party_last_name AS v_claim_party_full_name,
	-- *INF*: IIF(ISNULL(v_claim_party_full_name) or IS_SPACES(v_claim_party_full_name) or LENGTH(v_claim_party_full_name) = 0,'N/A',LTRIM(RTRIM(v_claim_party_full_name)))
	IFF(v_claim_party_full_name IS NULL OR IS_SPACES(v_claim_party_full_name) OR LENGTH(v_claim_party_full_name) = 0, 'N/A', LTRIM(RTRIM(v_claim_party_full_name))) AS claim_party_full_name,
	address1,
	address2,
	address1 || ' ' ||  address2 AS v_claim_party_address,
	-- *INF*: IIF(ISNULL(v_claim_party_address) or IS_SPACES(v_claim_party_address) or LENGTH(v_claim_party_address) = 0 ,'N/A',LTRIM(RTRIM(v_claim_party_address)))
	IFF(v_claim_party_address IS NULL OR IS_SPACES(v_claim_party_address) OR LENGTH(v_claim_party_address) = 0, 'N/A', LTRIM(RTRIM(v_claim_party_address))) AS claim_party_address,
	city,
	-- *INF*: IIF(ISNULL(city) or IS_SPACES(city) or LENGTH(city) = 0 ,'N/A',LTRIM(RTRIM(city)))
	IFF(city IS NULL OR IS_SPACES(city) OR LENGTH(city) = 0, 'N/A', LTRIM(RTRIM(city))) AS claim_party_city,
	'N/A' AS claim_party_county,
	state,
	-- *INF*: IIF(ISNULL(state) or IS_SPACES(state) or LENGTH(state) = 0 ,'N/A',LTRIM(RTRIM(state)))
	IFF(state IS NULL OR IS_SPACES(state) OR LENGTH(state) = 0, 'N/A', LTRIM(RTRIM(state))) AS claim_party_state,
	zip_code,
	zip4,
	zip_code || '-' || zip4 AS v_claim_party_zip_code,
	-- *INF*: IIF(ISNULL(v_claim_party_zip_code) or IS_SPACES(v_claim_party_zip_code) or LENGTH(v_claim_party_zip_code) = 0 or v_claim_party_zip_code = '-'
	--  ,'N/A',LTRIM(RTRIM(v_claim_party_zip_code)))
	IFF(v_claim_party_zip_code IS NULL OR IS_SPACES(v_claim_party_zip_code) OR LENGTH(v_claim_party_zip_code) = 0 OR v_claim_party_zip_code = '-', 'N/A', LTRIM(RTRIM(v_claim_party_zip_code))) AS claim_party_zip_code,
	'N/A' AS addr_type,
	tin,
	-- *INF*: IIF(ISNULL(tin) or IS_SPACES(tin) or LENGTH(tin) = 0 ,'N/A',LTRIM(RTRIM(tin)))
	IFF(tin IS NULL OR IS_SPACES(tin) OR LENGTH(tin) = 0, 'N/A', LTRIM(RTRIM(tin))) AS claim_party_taxid_ssn,
	'N/A' AS tax_fed_id,
	-- *INF*: TO_DATE('12/31/2100','MM/DD/YYYY')
	TO_DATE('12/31/2100', 'MM/DD/YYYY') AS claim_party_birthdate,
	'N/A' AS claim_party_gndr,
	phone AS in_phone_num,
	-- *INF*: IIF(ISNULL(in_phone_num) or IS_SPACES(in_phone_num) or LENGTH(in_phone_num) = 0 ,'N/A',LTRIM(RTRIM(in_phone_num)))
	IFF(in_phone_num IS NULL OR IS_SPACES(in_phone_num) OR LENGTH(in_phone_num) = 0, 'N/A', LTRIM(RTRIM(in_phone_num))) AS phone_num,
	phone_ext AS in_phone_ext,
	-- *INF*: IIF(ISNULL(in_phone_ext) or IS_SPACES(in_phone_ext) or LENGTH(in_phone_ext) = 0 ,'N/A',LTRIM(RTRIM(in_phone_ext)))
	IFF(in_phone_ext IS NULL OR IS_SPACES(in_phone_ext) OR LENGTH(in_phone_ext) = 0, 'N/A', LTRIM(RTRIM(in_phone_ext))) AS phone_ext,
	'N/A' AS phone_type
	FROM SQ_cms_pms_relation_stage
),
LKP_Target AS (
	SELECT
	claim_party_id,
	claim_party_ak_id,
	claim_party_key,
	claim_party_full_name,
	claim_party_first_name,
	claim_party_last_name,
	claim_party_mid_name,
	claim_party_name_prfx,
	claim_party_name_sfx,
	claim_party_addr,
	claim_party_city,
	claim_party_county,
	claim_party_state,
	claim_party_zip,
	addr_type,
	tax_ssn_id,
	tax_fed_id,
	claim_party_birthdate,
	claim_party_gndr,
	ph_num,
	ph_extension,
	ph_type,
	in_claim_party_key
	FROM (
		SELECT 
		A.claim_party_id as claim_party_id, 
		A.claim_party_ak_id as claim_party_ak_id, 
		A.claim_party_full_name as claim_party_full_name, 
		A.claim_party_first_name as claim_party_first_name, 
		A.claim_party_last_name as claim_party_last_name, 
		A.claim_party_mid_name as claim_party_mid_name, 
		A.claim_party_name_prfx as claim_party_name_prfx, 
		A.claim_party_name_sfx as claim_party_name_sfx, 
		A.claim_party_addr as claim_party_addr, 
		A.claim_party_city as claim_party_city, 
		A.claim_party_county as claim_party_county, 
		A.claim_party_state as claim_party_state, 
		A.claim_party_zip as claim_party_zip, 
		A.addr_type as addr_type, 
		A.tax_ssn_id as tax_ssn_id, 
		A.tax_fed_id as tax_fed_id, 
		A.claim_party_birthdate as claim_party_birthdate, 
		A.claim_party_gndr as claim_party_gndr, 
		A.ph_num as ph_num, 
		A.ph_extension as ph_extension, 
		A.ph_type as ph_type,
		A.claim_party_key as claim_party_key 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party A
		WHERE A.source_sys_id ='@{pipeline().parameters.SOURCE_SYSTEM_ID}' and A.crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_key ORDER BY claim_party_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_Target.claim_party_id AS Old_claim_party_id,
	LKP_Target.claim_party_ak_id AS Old_claim_party_ak_id,
	LKP_Target.claim_party_key AS Old_claim_party_key,
	LKP_Target.claim_party_full_name AS Old_claim_party_full_name,
	LKP_Target.claim_party_first_name AS Old_claim_party_first_name,
	LKP_Target.claim_party_last_name AS Old_claim_party_last_name,
	LKP_Target.claim_party_mid_name AS Old_claim_party_mid_name,
	LKP_Target.claim_party_name_prfx AS Old_claim_party_name_prfx,
	LKP_Target.claim_party_name_sfx AS Old_claim_party_name_sfx,
	LKP_Target.claim_party_addr AS Old_claim_party_addr,
	LKP_Target.claim_party_city AS Old_claim_party_city,
	LKP_Target.claim_party_county AS Old_claim_party_county,
	LKP_Target.claim_party_state AS Old_claim_party_state,
	LKP_Target.claim_party_zip AS Old_claim_party_zip,
	LKP_Target.addr_type AS Old_addr_type,
	LKP_Target.tax_ssn_id AS Old_tax_ssn_id,
	LKP_Target.tax_fed_id AS Old_tax_fed_id,
	LKP_Target.claim_party_birthdate AS Old_claim_party_birthdate,
	LKP_Target.claim_party_gndr AS Old_claim_party_gndr,
	LKP_Target.ph_num AS Old_phone_num,
	LKP_Target.ph_extension AS Old_phone_ext,
	LKP_Target.ph_type AS Old_phone_type,
	EXP_Values.claim_party_key,
	EXP_Values.claim_party_full_name,
	EXP_Values.claim_party_first_name,
	EXP_Values.claim_party_last_name,
	EXP_Values.claim_party_mid_name,
	EXP_Values.claim_party_name_prfx,
	EXP_Values.claim_party_name_sfx,
	EXP_Values.claim_party_address,
	EXP_Values.claim_party_city,
	EXP_Values.claim_party_county,
	EXP_Values.claim_party_state,
	EXP_Values.claim_party_zip_code,
	EXP_Values.addr_type,
	EXP_Values.claim_party_taxid_ssn AS adnm_taxid_ssn,
	EXP_Values.tax_fed_id,
	EXP_Values.claim_party_birthdate,
	EXP_Values.claim_party_gndr,
	EXP_Values.phone_num,
	EXP_Values.phone_ext,
	EXP_Values.phone_type,
	-- *INF*: iif(isnull(Old_claim_party_id),'NEW',
	-- 	iif (
	-- 	(ltrim(rtrim(claim_party_full_name)) <> ltrim(rtrim(Old_claim_party_full_name))) or
	-- 	(ltrim(rtrim(claim_party_last_name)) <> ltrim(rtrim(Old_claim_party_last_name))) or
	-- 	(ltrim(rtrim(claim_party_first_name)) <> ltrim(rtrim(Old_claim_party_first_name))) or
	-- 	(ltrim(rtrim(claim_party_address)) <> ltrim(rtrim(Old_claim_party_addr))) or
	-- 	(ltrim(rtrim(claim_party_city)) <> ltrim(rtrim(Old_claim_party_city))) or
	-- 	(ltrim(rtrim(claim_party_state)) <> ltrim(rtrim(Old_claim_party_state))) or
	-- 	(ltrim(rtrim(claim_party_zip_code)) <> ltrim(rtrim(Old_claim_party_zip))) or
	-- 	(ltrim(rtrim(phone_num)) <> ltrim(rtrim(Old_phone_num))) or
	-- 	(ltrim(rtrim(phone_ext)) <> ltrim(rtrim(Old_phone_ext))) or	
	-- 	(ltrim(rtrim(phone_type)) <> ltrim(rtrim(Old_phone_type))) or
	-- 	(ltrim(rtrim(adnm_taxid_ssn)) <> ltrim(rtrim(Old_tax_ssn_id))) ,
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(Old_claim_party_id IS NULL, 'NEW', IFF(( ltrim(rtrim(claim_party_full_name)) <> ltrim(rtrim(Old_claim_party_full_name)) ) OR ( ltrim(rtrim(claim_party_last_name)) <> ltrim(rtrim(Old_claim_party_last_name)) ) OR ( ltrim(rtrim(claim_party_first_name)) <> ltrim(rtrim(Old_claim_party_first_name)) ) OR ( ltrim(rtrim(claim_party_address)) <> ltrim(rtrim(Old_claim_party_addr)) ) OR ( ltrim(rtrim(claim_party_city)) <> ltrim(rtrim(Old_claim_party_city)) ) OR ( ltrim(rtrim(claim_party_state)) <> ltrim(rtrim(Old_claim_party_state)) ) OR ( ltrim(rtrim(claim_party_zip_code)) <> ltrim(rtrim(Old_claim_party_zip)) ) OR ( ltrim(rtrim(phone_num)) <> ltrim(rtrim(Old_phone_num)) ) OR ( ltrim(rtrim(phone_ext)) <> ltrim(rtrim(Old_phone_ext)) ) OR ( ltrim(rtrim(phone_type)) <> ltrim(rtrim(Old_phone_type)) ) OR ( ltrim(rtrim(adnm_taxid_ssn)) <> ltrim(rtrim(Old_tax_ssn_id)) ), 'UPDATE', 'NOCHANGE')) AS V_changed_flag,
	'0' AS logical_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: IIF(V_changed_flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	SYSDATE)
	IFF(V_changed_flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	V_changed_flag AS Changed_flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	'N/A' AS Default_String,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS claim_party_ref_eff_from_date
	FROM EXP_Values
	LEFT JOIN LKP_Target
	ON LKP_Target.claim_party_key = EXP_Values.claim_party_key
),
FIL_Insert AS (
	SELECT
	Old_claim_party_ak_id, 
	claim_party_key, 
	claim_party_full_name, 
	claim_party_first_name, 
	claim_party_last_name, 
	claim_party_mid_name, 
	claim_party_name_prfx, 
	claim_party_name_sfx, 
	claim_party_address, 
	claim_party_city, 
	claim_party_county, 
	claim_party_state, 
	claim_party_zip_code, 
	addr_type, 
	adnm_taxid_ssn AS tax_ssn_id, 
	tax_fed_id, 
	claim_party_birthdate, 
	claim_party_gndr, 
	logical_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date, 
	Changed_flag, 
	phone_num, 
	phone_ext, 
	phone_type, 
	Default_String, 
	claim_party_ref_eff_from_date
	FROM EXP_Detect_Changes
	WHERE Changed_flag='NEW'  OR Changed_flag='UPDATE'
),
SEQ_claim_party AS (
	CREATE SEQUENCE SEQ_claim_party
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK AS (
	SELECT
	Old_claim_party_ak_id,
	-- *INF*: IIF(Changed_flag='NEW', NEXTVAL, Old_claim_party_ak_id)
	IFF(Changed_flag = 'NEW', NEXTVAL, Old_claim_party_ak_id) AS claim_party_ak_id,
	claim_party_key,
	claim_party_full_name,
	claim_party_first_name,
	claim_party_last_name,
	claim_party_mid_name,
	claim_party_name_prfx,
	claim_party_name_sfx,
	claim_party_address,
	claim_party_city,
	claim_party_county,
	claim_party_state,
	claim_party_zip_code,
	addr_type,
	tax_ssn_id,
	tax_fed_id,
	claim_party_birthdate,
	claim_party_gndr,
	logical_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	Changed_flag,
	phone_num,
	phone_ext,
	phone_type,
	Default_String,
	claim_party_ref_eff_from_date,
	-- *INF*: TO_DATE('12/31/2999','MM/DD/YYYY')
	TO_DATE('12/31/2999', 'MM/DD/YYYY') AS out_default_high_date,
	SEQ_claim_party.NEXTVAL
	FROM FIL_Insert
),
claim_party_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party
	(claim_party_ak_id, claim_party_key, claim_party_full_name, claim_party_first_name, claim_party_last_name, claim_party_mid_name, claim_party_name_prfx, claim_party_name_sfx, claim_party_addr, claim_party_city, claim_party_county, claim_party_state, claim_party_zip, addr_type, tax_ssn_id, tax_fed_id, claim_party_birthdate, claim_party_gndr, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, ph_num, ph_extension, ph_type, lgl_ent_code, claim_party_ref_eff_from_date, claim_party_death_date)
	SELECT 
	CLAIM_PARTY_AK_ID, 
	CLAIM_PARTY_KEY, 
	CLAIM_PARTY_FULL_NAME, 
	CLAIM_PARTY_FIRST_NAME, 
	CLAIM_PARTY_LAST_NAME, 
	CLAIM_PARTY_MID_NAME, 
	CLAIM_PARTY_NAME_PRFX, 
	CLAIM_PARTY_NAME_SFX, 
	claim_party_address AS CLAIM_PARTY_ADDR, 
	CLAIM_PARTY_CITY, 
	CLAIM_PARTY_COUNTY, 
	CLAIM_PARTY_STATE, 
	claim_party_zip_code AS CLAIM_PARTY_ZIP, 
	ADDR_TYPE, 
	TAX_SSN_ID, 
	TAX_FED_ID, 
	CLAIM_PARTY_BIRTHDATE, 
	CLAIM_PARTY_GNDR, 
	LOGICAL_FLAG, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	phone_num AS PH_NUM, 
	phone_ext AS PH_EXTENSION, 
	phone_type AS PH_TYPE, 
	Default_String AS LGL_ENT_CODE, 
	CLAIM_PARTY_REF_EFF_FROM_DATE, 
	out_default_high_date AS CLAIM_PARTY_DEATH_DATE
	FROM EXP_Determine_AK
),
SQ_claim_party AS (
	SELECT 
	a.claim_party_id, 
	a.claim_party_key, 
	a.eff_from_date, 
	a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party a
	WHERE a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	and EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party b
			WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.claim_party_key = b.claim_party_key
			GROUP BY claim_party_key
			HAVING COUNT(*) > 1)
	ORDER BY claim_party_key, eff_from_date  DESC--
),
EXP_Lag_eff_from_date AS (
	SELECT
	claim_party_id,
	claim_party_key,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	claim_party_key = v_PREV_ROW_party_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
	claim_party_key = v_PREV_ROW_party_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	claim_party_key AS v_PREV_ROW_party_key,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_claim_party
),
FIL_FirstRowInAKGroup AS (
	SELECT
	claim_party_id, 
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
UPD_Claim_Party AS (
	SELECT
	claim_party_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_FirstRowInAKGroup
),
claim_party_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party AS T
	USING UPD_Claim_Party AS S
	ON T.claim_party_id = S.claim_party_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),