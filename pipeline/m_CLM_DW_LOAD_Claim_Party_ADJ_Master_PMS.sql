WITH
SQ_PMS_ADJUSTER_MASTER_STAGE AS (
	SELECT 
	adnm_taxid_ssn,
	adnm_name,
	(adnm_adjustor_nbr + adnm_type_adjustor) as adnm_commnt1,
	adnm_address,
	adnm_city_state,
	adnm_zip_code
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.pms_adjuster_master_stage
),
EXP_Values AS (
	SELECT
	adnm_commnt1,
	adnm_name,
	-- *INF*: IIF(ISNULL(adnm_name) or IS_SPACES(adnm_name) or LENGTH(adnm_name) = 0,'N/A',LTRIM(RTRIM(adnm_name)))
	IFF(adnm_name IS NULL OR IS_SPACES(adnm_name) OR LENGTH(adnm_name) = 0, 'N/A', LTRIM(RTRIM(adnm_name))) AS Claim_Party_Full_Name,
	'N/A' AS claim_party_first_name,
	'N/A' AS claim_party_last_name,
	'N/A' AS claim_party_mid_name,
	'N/A' AS claim_party_name_prfx,
	'N/A' AS claim_party_name_sfx,
	adnm_address,
	-- *INF*: IIF(ISNULL(adnm_address) or IS_SPACES(adnm_address) or LENGTH(adnm_address) = 0 ,'N/A',LTRIM(RTRIM(adnm_address)))
	IFF(adnm_address IS NULL OR IS_SPACES(adnm_address) OR LENGTH(adnm_address) = 0, 'N/A', LTRIM(RTRIM(adnm_address))) AS Claim_Party_address,
	adnm_city_state,
	-- *INF*: IIF(INSTR(LTRIM(RTRIM(adnm_city_state)),' ' ,-1,1) = 0, SUBSTR(LTRIM(RTRIM(adnm_city_state)),1),
	-- SUBSTR(LTRIM(RTRIM(adnm_city_state)),1,INSTR(LTRIM(RTRIM(adnm_city_state)),' ' ,-1,1)))
	-- 
	-- 
	-- --SUBSTR(LTRIM(RTRIM(adnm_city_state)),1,INSTR(LTRIM(RTRIM(adnm_city_state)),' ' ,-1,1))
	IFF(INSTR(LTRIM(RTRIM(adnm_city_state)), ' ', - 1, 1) = 0, SUBSTR(LTRIM(RTRIM(adnm_city_state)), 1), SUBSTR(LTRIM(RTRIM(adnm_city_state)), 1, INSTR(LTRIM(RTRIM(adnm_city_state)), ' ', - 1, 1))) AS v_claim_party_city,
	-- *INF*: IIF(ISNULL(v_claim_party_city) or IS_SPACES(v_claim_party_city) or LENGTH(v_claim_party_city) = 0 ,'N/A',LTRIM(RTRIM(v_claim_party_city)))
	IFF(v_claim_party_city IS NULL OR IS_SPACES(v_claim_party_city) OR LENGTH(v_claim_party_city) = 0, 'N/A', LTRIM(RTRIM(v_claim_party_city))) AS claim_party_city_out,
	'N/A' AS claim_party_county,
	-- *INF*: IIF(INSTR(LTRIM(RTRIM(adnm_city_state)),' ',-1,1) = 0 , 'N/A', SUBSTR(LTRIM(RTRIM(adnm_city_state)),(INSTR(LTRIM(RTRIM(adnm_city_state)),' ',-1,1)+1)))
	-- 
	-- 
	-- --SUBSTR(LTRIM(RTRIM(adnm_city_state)),(INSTR(LTRIM(RTRIM(adnm_city_state)),' ',-1,1)+1))
	IFF(INSTR(LTRIM(RTRIM(adnm_city_state)), ' ', - 1, 1) = 0, 'N/A', SUBSTR(LTRIM(RTRIM(adnm_city_state)), ( INSTR(LTRIM(RTRIM(adnm_city_state)), ' ', - 1, 1) + 1 ))) AS v_claim_party_state,
	-- *INF*: IIF(ISNULL(v_claim_party_state) or IS_SPACES(v_claim_party_state) or LENGTH(v_claim_party_state) = 0 ,'N/A',LTRIM(RTRIM(v_claim_party_state)))
	IFF(v_claim_party_state IS NULL OR IS_SPACES(v_claim_party_state) OR LENGTH(v_claim_party_state) = 0, 'N/A', LTRIM(RTRIM(v_claim_party_state))) AS claim_party_state_out,
	adnm_zip_code,
	-- *INF*: IIF(ISNULL(adnm_zip_code) or IS_SPACES(adnm_zip_code) or LENGTH(adnm_zip_code) = 0 ,'N/A',LTRIM(RTRIM(adnm_zip_code)))
	IFF(adnm_zip_code IS NULL OR IS_SPACES(adnm_zip_code) OR LENGTH(adnm_zip_code) = 0, 'N/A', LTRIM(RTRIM(adnm_zip_code))) AS adnm_zip_code_out,
	'N/A' AS addr_type,
	adnm_taxid_ssn,
	-- *INF*: IIF(ISNULL(adnm_taxid_ssn) or IS_SPACES(adnm_taxid_ssn) or LENGTH(adnm_taxid_ssn) = 0 ,'N/A',LTRIM(RTRIM(adnm_taxid_ssn)))
	IFF(adnm_taxid_ssn IS NULL OR IS_SPACES(adnm_taxid_ssn) OR LENGTH(adnm_taxid_ssn) = 0, 'N/A', LTRIM(RTRIM(adnm_taxid_ssn))) AS adnm_taxid_ssn_out,
	'N/A' AS tax_fed_id,
	-- *INF*: TO_DATE('12/31/2100','MM/DD/YYYY')
	TO_DATE('12/31/2100', 'MM/DD/YYYY') AS claim_party_birthdate,
	'N/A' AS claim_party_gndr
	FROM SQ_PMS_ADJUSTER_MASTER_STAGE
),
LKP_Claim_Party AS (
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
	adnm_commnt1
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
		A.claim_party_key as claim_party_key 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party A
		WHERE A.source_sys_id ='@{pipeline().parameters.SOURCE_SYSTEM_ID}' and A.crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_key ORDER BY claim_party_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_Claim_Party.claim_party_id AS Old_claim_party_id,
	LKP_Claim_Party.claim_party_ak_id AS Old_claim_party_ak_id,
	LKP_Claim_Party.claim_party_key AS Old_claim_party_key,
	LKP_Claim_Party.claim_party_full_name AS Old_claim_party_full_name,
	LKP_Claim_Party.claim_party_first_name AS Old_claim_party_first_name,
	LKP_Claim_Party.claim_party_last_name AS Old_claim_party_last_name,
	LKP_Claim_Party.claim_party_mid_name AS Old_claim_party_mid_name,
	LKP_Claim_Party.claim_party_name_prfx AS Old_claim_party_name_prfx,
	LKP_Claim_Party.claim_party_name_sfx AS Old_claim_party_name_sfx,
	LKP_Claim_Party.claim_party_addr AS Old_claim_party_addr,
	LKP_Claim_Party.claim_party_city AS Old_claim_party_city,
	LKP_Claim_Party.claim_party_county AS Old_claim_party_county,
	LKP_Claim_Party.claim_party_state AS Old_claim_party_state,
	LKP_Claim_Party.claim_party_zip AS Old_claim_party_zip,
	LKP_Claim_Party.addr_type AS Old_addr_type,
	LKP_Claim_Party.tax_ssn_id AS Old_tax_ssn_id,
	LKP_Claim_Party.tax_fed_id AS Old_tax_fed_id,
	LKP_Claim_Party.claim_party_birthdate AS Old_claim_party_birthdate,
	LKP_Claim_Party.claim_party_gndr AS Old_claim_party_gndr,
	EXP_Values.adnm_commnt1 AS adnm_commnt,
	EXP_Values.Claim_Party_Full_Name AS adnm_name,
	EXP_Values.claim_party_first_name,
	EXP_Values.claim_party_last_name,
	EXP_Values.claim_party_mid_name,
	EXP_Values.claim_party_name_prfx,
	EXP_Values.claim_party_name_sfx,
	EXP_Values.Claim_Party_address AS adnm_address,
	EXP_Values.claim_party_city_out,
	EXP_Values.claim_party_county,
	EXP_Values.claim_party_state_out,
	EXP_Values.adnm_zip_code_out AS adnm_zip_code,
	EXP_Values.addr_type,
	EXP_Values.adnm_taxid_ssn_out AS adnm_taxid_ssn,
	EXP_Values.tax_fed_id,
	EXP_Values.claim_party_birthdate,
	EXP_Values.claim_party_gndr,
	-- *INF*: iif(isnull(Old_claim_party_id),'NEW',
	-- 	iif (
	-- 	(ltrim(rtrim(adnm_name)) <> ltrim(rtrim(Old_claim_party_full_name))) or
	-- 	(ltrim(rtrim(adnm_address)) <> ltrim(rtrim(Old_claim_party_addr))) or
	-- 	(ltrim(rtrim(claim_party_city_out)) <> ltrim(rtrim(Old_claim_party_city))) or
	-- 	(ltrim(rtrim(claim_party_state_out)) <> ltrim(rtrim(Old_claim_party_state))) or
	-- 	(ltrim(rtrim(adnm_zip_code)) <> ltrim(rtrim(Old_claim_party_zip))) or
	-- 	(ltrim(rtrim(adnm_taxid_ssn)) <> ltrim(rtrim(Old_tax_ssn_id))) ,
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(Old_claim_party_id IS NULL, 'NEW', IFF(( ltrim(rtrim(adnm_name)) <> ltrim(rtrim(Old_claim_party_full_name)) ) OR ( ltrim(rtrim(adnm_address)) <> ltrim(rtrim(Old_claim_party_addr)) ) OR ( ltrim(rtrim(claim_party_city_out)) <> ltrim(rtrim(Old_claim_party_city)) ) OR ( ltrim(rtrim(claim_party_state_out)) <> ltrim(rtrim(Old_claim_party_state)) ) OR ( ltrim(rtrim(adnm_zip_code)) <> ltrim(rtrim(Old_claim_party_zip)) ) OR ( ltrim(rtrim(adnm_taxid_ssn)) <> ltrim(rtrim(Old_tax_ssn_id)) ), 'UPDATE', 'NOCHANGE')) AS V_changed_flag,
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
	'N/A' AS Out_Default_String,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS claim_party_ref_eff_from_date
	FROM EXP_Values
	LEFT JOIN LKP_Claim_Party
	ON LKP_Claim_Party.claim_party_key = EXP_Values.adnm_commnt1
),
FIL_Insert AS (
	SELECT
	Old_claim_party_ak_id, 
	adnm_commnt AS claim_party_key, 
	adnm_name AS claim_party_full_name, 
	claim_party_first_name, 
	claim_party_last_name, 
	claim_party_mid_name, 
	claim_party_name_prfx, 
	claim_party_name_sfx, 
	adnm_address AS claim_party_addr, 
	claim_party_city_out AS claim_party_city, 
	claim_party_county, 
	claim_party_state_out AS claim_party_state, 
	adnm_zip_code AS claim_party_zip, 
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
	Out_Default_String, 
	claim_party_ref_eff_from_date
	FROM EXP_Detect_Changes
	WHERE Changed_flag='NEW'  OR Changed_flag='UPDATE'
),
SEQ_claim_party AS (
	CREATE SEQUENCE SEQ_claim_party
	START = 0
	INCREMENT = 1;
),
EXP_Determin_AK AS (
	SELECT
	Old_claim_party_ak_id,
	-- *INF*: IIF(Changed_flag='NEW', NEXTVAL,Old_claim_party_ak_id)
	IFF(Changed_flag = 'NEW', NEXTVAL, Old_claim_party_ak_id) AS claim_party_ak_id,
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
	logical_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	Changed_flag,
	Out_Default_String,
	claim_party_ref_eff_from_date,
	-- *INF*: TO_DATE('12/31/2999','MM/DD/YYYY')
	TO_DATE('12/31/2999', 'MM/DD/YYYY') AS out_default_high_date,
	SEQ_claim_party.NEXTVAL
	FROM FIL_Insert
),
claim_party AS (
	INSERT INTO claim_party
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
	CLAIM_PARTY_ADDR, 
	CLAIM_PARTY_CITY, 
	CLAIM_PARTY_COUNTY, 
	CLAIM_PARTY_STATE, 
	CLAIM_PARTY_ZIP, 
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
	Out_Default_String AS PH_NUM, 
	Out_Default_String AS PH_EXTENSION, 
	Out_Default_String AS PH_TYPE, 
	Out_Default_String AS LGL_ENT_CODE, 
	CLAIM_PARTY_REF_EFF_FROM_DATE, 
	out_default_high_date AS CLAIM_PARTY_DEATH_DATE
	FROM EXP_Determin_AK
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
claim_party_Update AS (
	MERGE INTO claim_party AS T
	USING UPD_Claim_Party AS S
	ON T.claim_party_id = S.claim_party_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),