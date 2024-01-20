WITH
SQ_claim_party AS (
	SELECT 
	CP.claim_party_id, 
	CP.claim_party_ak_id, 
	CP.claim_party_key, 
	CP.claim_party_full_name, 
	CP.claim_party_first_name, 
	CP.claim_party_last_name, 
	CP.claim_party_mid_name, 
	CP.claim_party_name_prfx, 
	CP.claim_party_name_sfx, 
	CP.claim_party_addr, 
	CP.claim_party_city, 
	CP.claim_party_county, 
	CP.claim_party_state, 
	CP.claim_party_zip, 
	CP.addr_type, 
	CP.tax_ssn_id, CP.tax_fed_id, 
	CP.claim_party_birthdate, 
	CP.claim_party_gndr, 
	CP.eff_from_date 
	FROM
	 claim_party CP
	WHERE CP.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_default AS (
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
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM SQ_claim_party
),
LKP_Claim_Party_Dim AS (
	SELECT
	claim_party_dim_id,
	edw_claim_party_pk_id
	FROM (
		SELECT 
			claim_party_dim_id,
			edw_claim_party_pk_id
		FROM claim_party_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_party_pk_id ORDER BY claim_party_dim_id) = 1
),
RTR_insert_update AS (
	SELECT
	LKP_Claim_Party_Dim.claim_party_dim_id,
	EXP_default.claim_party_id AS edw_claim_party_pk_id,
	EXP_default.claim_party_ak_id AS edw_claim_party_ak_id,
	EXP_default.claim_party_key,
	EXP_default.claim_party_full_name,
	EXP_default.claim_party_first_name,
	EXP_default.claim_party_last_name,
	EXP_default.claim_party_mid_name,
	EXP_default.claim_party_name_prfx,
	EXP_default.claim_party_name_sfx,
	EXP_default.claim_party_addr,
	EXP_default.claim_party_city,
	EXP_default.claim_party_county,
	EXP_default.claim_party_state,
	EXP_default.claim_party_zip,
	EXP_default.addr_type,
	EXP_default.tax_ssn_id,
	EXP_default.tax_fed_id,
	EXP_default.claim_party_birthdate,
	EXP_default.claim_party_gndr,
	EXP_default.crrnt_snpsht_flag,
	EXP_default.audit_id,
	EXP_default.eff_from_date,
	EXP_default.eff_to_date,
	EXP_default.created_date,
	EXP_default.modified_date
	FROM EXP_default
	LEFT JOIN LKP_Claim_Party_Dim
	ON LKP_Claim_Party_Dim.edw_claim_party_pk_id = EXP_default.claim_party_id
),
RTR_insert_update_INSERT AS (SELECT * FROM RTR_insert_update WHERE ISNULL(claim_party_dim_id)),
RTR_insert_update_DEFAULT1 AS (SELECT * FROM RTR_insert_update WHERE NOT ( (ISNULL(claim_party_dim_id)) )),
UPD_insert AS (
	SELECT
	edw_claim_party_pk_id AS edw_claim_party_pk_id1, 
	edw_claim_party_ak_id AS edw_claim_party_ak_id1, 
	claim_party_key AS claim_party_key1, 
	addr_type AS addr_type1, 
	claim_party_zip AS claim_party_zip1, 
	claim_party_state AS claim_party_state1, 
	claim_party_county AS claim_party_county1, 
	claim_party_city AS claim_party_city1, 
	claim_party_addr AS claim_party_addr1, 
	claim_party_full_name AS claim_party_full_name1, 
	claim_party_first_name AS claim_party_first_name1, 
	claim_party_last_name AS claim_party_last_name1, 
	claim_party_mid_name AS claim_party_mid_name1, 
	claim_party_name_prfx AS claim_party_name_prfx1, 
	claim_party_name_sfx AS claim_party_name_sfx1, 
	tax_ssn_id AS tax_ssn_id1, 
	tax_fed_id AS tax_fed_id1, 
	claim_party_birthdate AS claim_party_birthdate1, 
	claim_party_gndr AS claim_party_gndr1, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag1, 
	audit_id AS audit_id1, 
	eff_from_date AS eff_from_date1, 
	eff_to_date AS eff_to_date1, 
	created_date AS created_date1, 
	modified_date AS modified_date1
	FROM RTR_insert_update_INSERT
),
claim_party_dim_insert AS (
	INSERT INTO claim_party_dim
	(edw_claim_party_pk_id, edw_claim_party_ak_id, claim_party_key, addr_type, claim_party_zip, claim_party_state, claim_party_county, claim_party_city, claim_party_addr, claim_party_full_name, claim_party_first_name, claim_party_last_name, claim_party_mid_name, claim_party_name_prfx, claim_party_name_sfx, tax_ssn_id, tax_fed_id, claim_party_birthdate, claim_party_gndr, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date)
	SELECT 
	edw_claim_party_pk_id1 AS EDW_CLAIM_PARTY_PK_ID, 
	edw_claim_party_ak_id1 AS EDW_CLAIM_PARTY_AK_ID, 
	claim_party_key1 AS CLAIM_PARTY_KEY, 
	addr_type1 AS ADDR_TYPE, 
	claim_party_zip1 AS CLAIM_PARTY_ZIP, 
	claim_party_state1 AS CLAIM_PARTY_STATE, 
	claim_party_county1 AS CLAIM_PARTY_COUNTY, 
	claim_party_city1 AS CLAIM_PARTY_CITY, 
	claim_party_addr1 AS CLAIM_PARTY_ADDR, 
	claim_party_full_name1 AS CLAIM_PARTY_FULL_NAME, 
	claim_party_first_name1 AS CLAIM_PARTY_FIRST_NAME, 
	claim_party_last_name1 AS CLAIM_PARTY_LAST_NAME, 
	claim_party_mid_name1 AS CLAIM_PARTY_MID_NAME, 
	claim_party_name_prfx1 AS CLAIM_PARTY_NAME_PRFX, 
	claim_party_name_sfx1 AS CLAIM_PARTY_NAME_SFX, 
	tax_ssn_id1 AS TAX_SSN_ID, 
	tax_fed_id1 AS TAX_FED_ID, 
	claim_party_birthdate1 AS CLAIM_PARTY_BIRTHDATE, 
	claim_party_gndr1 AS CLAIM_PARTY_GNDR, 
	crrnt_snpsht_flag1 AS CRRNT_SNPSHT_FLAG, 
	audit_id1 AS AUDIT_ID, 
	eff_from_date1 AS EFF_FROM_DATE, 
	eff_to_date1 AS EFF_TO_DATE, 
	created_date1 AS CREATED_DATE, 
	modified_date1 AS MODIFIED_DATE
	FROM UPD_insert
),
UPD_update AS (
	SELECT
	claim_party_dim_id AS claim_party_dim_id2, 
	edw_claim_party_pk_id AS edw_claim_party_pk_id2, 
	edw_claim_party_ak_id AS edw_claim_party_ak_id2, 
	claim_party_key AS claim_party_key2, 
	addr_type AS addr_type2, 
	claim_party_zip AS claim_party_zip2, 
	claim_party_state AS claim_party_state2, 
	claim_party_county AS claim_party_county2, 
	claim_party_city AS claim_party_city2, 
	claim_party_addr AS claim_party_addr2, 
	claim_party_full_name AS claim_party_full_name2, 
	claim_party_first_name AS claim_party_first_name2, 
	claim_party_last_name AS claim_party_last_name2, 
	claim_party_mid_name AS claim_party_mid_name2, 
	claim_party_name_prfx AS claim_party_name_prfx2, 
	claim_party_name_sfx AS claim_party_name_sfx2, 
	tax_ssn_id AS tax_ssn_id2, 
	tax_fed_id AS tax_fed_id2, 
	claim_party_birthdate AS claim_party_birthdate2, 
	claim_party_gndr AS claim_party_gndr2, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag2, 
	audit_id AS audit_id2, 
	eff_from_date AS eff_from_date2, 
	eff_to_date AS eff_to_date2, 
	created_date AS created_date2, 
	modified_date AS modified_date2
	FROM RTR_insert_update_DEFAULT1
),
claim_party_dim_update AS (
	MERGE INTO claim_party_dim AS T
	USING UPD_update AS S
	ON T.claim_party_dim_id = S.claim_party_dim_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.edw_claim_party_pk_id = S.edw_claim_party_pk_id2, T.edw_claim_party_ak_id = S.edw_claim_party_ak_id2, T.claim_party_key = S.claim_party_key2, T.addr_type = S.addr_type2, T.claim_party_zip = S.claim_party_zip2, T.claim_party_state = S.claim_party_state2, T.claim_party_county = S.claim_party_county2, T.claim_party_city = S.claim_party_city2, T.claim_party_addr = S.claim_party_addr2, T.claim_party_full_name = S.claim_party_full_name2, T.claim_party_first_name = S.claim_party_first_name2, T.claim_party_last_name = S.claim_party_last_name2, T.claim_party_mid_name = S.claim_party_mid_name2, T.claim_party_name_prfx = S.claim_party_name_prfx2, T.claim_party_name_sfx = S.claim_party_name_sfx2, T.tax_ssn_id = S.tax_ssn_id2, T.tax_fed_id = S.tax_fed_id2, T.claim_party_birthdate = S.claim_party_birthdate2, T.claim_party_gndr = S.claim_party_gndr2, T.crrnt_snpsht_flag = S.crrnt_snpsht_flag2, T.audit_id = S.audit_id2, T.eff_from_date = S.eff_from_date2, T.eff_to_date = S.eff_to_date2, T.created_date = S.created_date2, T.modified_date = S.modified_date2
),
SQ_claim_party_dim AS (
	SELECT 
	A.CLAIM_PARTY_DIM_ID, A.EDW_CLAIM_PARTY_AK_ID, A.EFF_FROM_DATE, A.EFF_TO_DATE 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.CLAIM_PARTY_DIM A
	WHERE 
	EXISTS
	(
	SELECT  1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CLAIM_PARTY_DIM B
	WHERE CRRNT_SNPSHT_FLAG = 1 AND 
	A.EDW_CLAIM_PARTY_AK_ID =B.EDW_CLAIM_PARTY_AK_ID
	GROUP BY B.EDW_CLAIM_PARTY_AK_ID HAVING COUNT(*) > 1
	)
	ORDER BY A.EDW_CLAIM_PARTY_AK_ID,A.EFF_FROM_DATE DESC
),
EXP_Lag_eff_from_date1 AS (
	SELECT
	claim_party_dim_id,
	edw_claim_party_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	edw_claim_party_ak_id = v_PREV_ROW_claim_party_ak_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(
	    TRUE,
	    edw_claim_party_ak_id = v_PREV_ROW_claim_party_ak_id, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	edw_claim_party_ak_id AS v_PREV_ROW_claim_party_ak_id,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_claim_party_dim
),
FILTRANS AS (
	SELECT
	claim_party_dim_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date1
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_EFF_TO_DATE AS (
	SELECT
	claim_party_dim_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FILTRANS
),
claim_party_dim_expire AS (
	MERGE INTO claim_party_dim AS T
	USING UPD_EFF_TO_DATE AS S
	ON T.claim_party_dim_id = S.claim_party_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),