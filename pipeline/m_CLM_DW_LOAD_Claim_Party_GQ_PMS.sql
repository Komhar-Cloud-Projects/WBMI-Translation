WITH
SQ_PIF_42GQ_ATY_stage AS (
	SELECT 
	PIF_SYMBOL, PIF_POLICY_NUMBER, PIF_MODULE, 
	(PIF_SYMBOL + PIF_POLICY_NUMBER + PIF_MODULE + 
	CASE len(IPFCGQ_MONTH_OF_LOSS) when 1 THEN '0' + CAST(IPFCGQ_MONTH_OF_LOSS AS VARCHAR) ELSE CAST(IPFCGQ_MONTH_OF_LOSS AS VARCHAR) END + 
	CASE len(IPFCGQ_DAY_OF_LOSS) when 1 THEN '0' + CAST(IPFCGQ_DAY_OF_LOSS AS VARCHAR) ELSE CAST(IPFCGQ_DAY_OF_LOSS AS VARCHAR) END +
	CAST(IPFCGQ_YEAR_OF_LOSS AS VARCHAR) + 
	CAST(IPFCGQ_LOSS_OCCURENCE AS VARCHAR) + 
	CAST(IPFCGQ_LOSS_CLAIMANT AS VARCHAR) + 'ATTY') as IPFCGQ_REC_LENGTH, IPFCGQ_YEAR_OF_LOSS, IPFCGQ_MONTH_OF_LOSS, IPFCGQ_DAY_OF_LOSS, IPFCGQ_LOSS_OCCURENCE, IPFCGQ_LOSS_CLAIMANT, IPFCGQ_ATTORNEY_NAME_1 
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PIF_42GQ_ATY_STAGE 
	WHERE IPFCGQ_ATTORNEY_NAME_1 IS NOT NULL AND LEN(RTRIM(IPFCGQ_ATTORNEY_NAME_1)) <> 0 
	AND pif_42gq_aty_stage.logical_flag='0'
	UNION 
	SELECT 
	PIF_SYMBOL, PIF_POLICY_NUMBER, PIF_MODULE, 
	(PIF_SYMBOL + PIF_POLICY_NUMBER + PIF_MODULE + 
	CASE len(IPFCGQ_MONTH_OF_LOSS) when 1 THEN '0' + CAST(IPFCGQ_MONTH_OF_LOSS AS VARCHAR) ELSE CAST(IPFCGQ_MONTH_OF_LOSS AS VARCHAR) END + 
	CASE len(IPFCGQ_DAY_OF_LOSS) when 1 THEN '0' + CAST(IPFCGQ_DAY_OF_LOSS AS VARCHAR) ELSE CAST(IPFCGQ_DAY_OF_LOSS AS VARCHAR) END +
	CAST(IPFCGQ_YEAR_OF_LOSS AS VARCHAR) + CAST(IPFCGQ_LOSS_OCCURENCE AS VARCHAR) + 
	CAST(IPFCGQ_LOSS_CLAIMANT AS VARCHAR) + 'PLAT') as IPFCGQ_REC_LENGTH, IPFCGQ_YEAR_OF_LOSS, IPFCGQ_MONTH_OF_LOSS, IPFCGQ_DAY_OF_LOSS, IPFCGQ_LOSS_OCCURENCE, IPFCGQ_LOSS_CLAIMANT, IPFCGQ_PLAINTIFF_1
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PIF_42GQ_ATY_STAGE 
	WHERE IPFCGQ_PLAINTIFF_1 IS NOT NULL AND LEN(RTRIM(IPFCGQ_PLAINTIFF_1)) <> 0 
	AND pif_42gq_aty_stage.logical_flag='0'
	UNION 
	SELECT 
	PIF_SYMBOL, PIF_POLICY_NUMBER, PIF_MODULE, 
	(PIF_SYMBOL + PIF_POLICY_NUMBER + PIF_MODULE + 
	CASE len(IPFCGQ_MONTH_OF_LOSS) when 1 THEN '0' + CAST(IPFCGQ_MONTH_OF_LOSS AS VARCHAR) ELSE CAST(IPFCGQ_MONTH_OF_LOSS AS VARCHAR) END + 
	CASE len(IPFCGQ_DAY_OF_LOSS) when 1 THEN '0' + CAST(IPFCGQ_DAY_OF_LOSS AS VARCHAR) ELSE CAST(IPFCGQ_DAY_OF_LOSS AS VARCHAR) END +
	CAST(IPFCGQ_YEAR_OF_LOSS AS VARCHAR) + CAST(IPFCGQ_LOSS_OCCURENCE AS VARCHAR) + 
	CAST(IPFCGQ_LOSS_CLAIMANT AS VARCHAR) + 'DEFD') as IPFCGQ_REC_LENGTH, IPFCGQ_YEAR_OF_LOSS, IPFCGQ_MONTH_OF_LOSS, IPFCGQ_DAY_OF_LOSS, IPFCGQ_LOSS_OCCURENCE, IPFCGQ_LOSS_CLAIMANT, IPFCGQ_DEFENDANT_1
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PIF_42GQ_ATY_STAGE 
	WHERE IPFCGQ_DEFENDANT_1 IS NOT NULL AND LEN(RTRIM(IPFCGQ_DEFENDANT_1)) <> 0
	AND pif_42gq_aty_stage.logical_flag='0'
),
EXP_Values AS (
	SELECT
	IPFCGQ_REC_LENGTH AS CLIENT_ID,
	IPFCGQ_ATTORNEY_NAME_1 AS CICL_FULL_NM,
	PIF_SYMBOL,
	PIF_POLICY_NUMBER,
	PIF_MODULE,
	IPFCGQ_YEAR_OF_LOSS,
	IPFCGQ_MONTH_OF_LOSS,
	IPFCGQ_DAY_OF_LOSS,
	IPFCGQ_LOSS_OCCURENCE,
	IPFCGQ_LOSS_CLAIMANT
	FROM SQ_PIF_42GQ_ATY_stage
),
LKP_PIF_4578_STAGE AS (
	SELECT
	LOSS_1099_NUMBER,
	PIF_SYMBOL,
	PIF_POLICY_NUMBER,
	PIF_MODULE,
	LOSS_YEAR,
	LOSS_MONTH,
	LOSS_DAY,
	LOSS_OCCURENCE,
	LOSS_CLAIMANT
	FROM (
		SELECT 
			LOSS_1099_NUMBER,
			PIF_SYMBOL,
			PIF_POLICY_NUMBER,
			PIF_MODULE,
			LOSS_YEAR,
			LOSS_MONTH,
			LOSS_DAY,
			LOSS_OCCURENCE,
			LOSS_CLAIMANT
		FROM PIF_4578_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PIF_SYMBOL,PIF_POLICY_NUMBER,PIF_MODULE,LOSS_YEAR,LOSS_MONTH,LOSS_DAY,LOSS_OCCURENCE,LOSS_CLAIMANT ORDER BY LOSS_1099_NUMBER DESC) = 1
),
EXP_Lkp_Values AS (
	SELECT
	EXP_Values.CLIENT_ID,
	EXP_Values.CICL_FULL_NM AS in_CICL_FULL_NM,
	-- *INF*: iif(isnull(in_CICL_FULL_NM),'N/A',
	--    iif(is_spaces(in_CICL_FULL_NM),'N/A',
	--     rtrim(in_CICL_FULL_NM)))
	IFF(in_CICL_FULL_NM IS NULL, 'N/A', IFF(is_spaces(in_CICL_FULL_NM), 'N/A', rtrim(in_CICL_FULL_NM))) AS CICL_FULL_NM,
	'N/A' AS CICL_FST_NM,
	'N/A' AS CICL_LST_NM,
	'N/A' AS CICL_MDL_NM,
	'N/A' AS NM_PFX,
	'N/A' AS NM_SFX,
	'N/A' AS CICA_ADR,
	'N/A' AS CICA_CIT_NM,
	'N/A' AS CICA_CTY,
	'N/A' AS ST_CD,
	'N/A' AS CICA_PST_CD,
	'N/A' AS ADR_TYP_CD,
	LKP_PIF_4578_STAGE.LOSS_1099_NUMBER AS in_CICA_TAX_ID,
	'N/A' AS CICA_TAX_SSN_ID,
	-- *INF*: 'N/A'
	-- --iif(isnull(in_CICA_TAX_ID),'N/A',   iif(is_spaces(in_CICA_TAX_ID),'N/A',    rtrim(in_CICA_TAX_ID)))
	'N/A' AS CICA_TAX_FED_ID,
	-- *INF*: TO_DATE('12/31/2100','MM/DD/YYYY')
	-- 
	TO_DATE('12/31/2100', 'MM/DD/YYYY') AS BIRTH_DATE,
	'N/A' AS GENDER
	FROM EXP_Values
	LEFT JOIN LKP_PIF_4578_STAGE
	ON LKP_PIF_4578_STAGE.PIF_SYMBOL = EXP_Values.PIF_SYMBOL AND LKP_PIF_4578_STAGE.PIF_POLICY_NUMBER = EXP_Values.PIF_POLICY_NUMBER AND LKP_PIF_4578_STAGE.PIF_MODULE = EXP_Values.PIF_MODULE AND LKP_PIF_4578_STAGE.LOSS_YEAR = EXP_Values.IPFCGQ_YEAR_OF_LOSS AND LKP_PIF_4578_STAGE.LOSS_MONTH = EXP_Values.IPFCGQ_MONTH_OF_LOSS AND LKP_PIF_4578_STAGE.LOSS_DAY = EXP_Values.IPFCGQ_DAY_OF_LOSS AND LKP_PIF_4578_STAGE.LOSS_OCCURENCE = EXP_Values.IPFCGQ_LOSS_OCCURENCE AND LKP_PIF_4578_STAGE.LOSS_CLAIMANT = EXP_Values.IPFCGQ_LOSS_CLAIMANT
),
LKP_Claim_Party AS (
	SELECT
	claim_party_id,
	claim_party_ak_id,
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
	claim_party_key
	FROM (
		SELECT a.claim_party_id as claim_party_id, 
		a.claim_party_ak_id as claim_party_ak_id,
		a.claim_party_full_name as claim_party_full_name,
		a.claim_party_first_name as claim_party_first_name, 
		a.claim_party_last_name as claim_party_last_name, 
		a.claim_party_mid_name as claim_party_mid_name, 
		a.claim_party_name_prfx as claim_party_name_prfx, 
		a.claim_party_name_sfx as claim_party_name_sfx, 
		a.claim_party_addr as claim_party_addr, 
		a.claim_party_city as claim_party_city, 
		a.claim_party_county as claim_party_county, 
		a.claim_party_state as claim_party_state, 
		a.claim_party_zip as claim_party_zip, 
		a.addr_type as addr_type, 
		a.tax_ssn_id as tax_ssn_id, 
		a.tax_fed_id as tax_fed_id, 
		a.claim_party_birthdate as claim_party_birthdate,
		a.claim_party_gndr as claim_party_gndr,
		a.claim_party_key as claim_party_key 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party a
		WHERE  a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND a.crrnt_snpsht_flag = 1
		ORDER BY claim_party_key --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_key ORDER BY claim_party_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	EXP_Lkp_Values.CLIENT_ID,
	EXP_Lkp_Values.CICL_FULL_NM,
	EXP_Lkp_Values.CICL_FST_NM,
	EXP_Lkp_Values.CICL_LST_NM,
	EXP_Lkp_Values.CICL_MDL_NM,
	EXP_Lkp_Values.NM_PFX,
	EXP_Lkp_Values.NM_SFX,
	EXP_Lkp_Values.ADR_TYP_CD,
	EXP_Lkp_Values.CICA_ADR,
	EXP_Lkp_Values.CICA_CIT_NM,
	EXP_Lkp_Values.CICA_CTY,
	EXP_Lkp_Values.ST_CD,
	EXP_Lkp_Values.CICA_PST_CD,
	EXP_Lkp_Values.CICA_TAX_SSN_ID,
	EXP_Lkp_Values.CICA_TAX_FED_ID,
	EXP_Lkp_Values.BIRTH_DATE,
	EXP_Lkp_Values.GENDER,
	LKP_Claim_Party.claim_party_id AS lkp_claim_party_id,
	LKP_Claim_Party.claim_party_ak_id AS lkp_claim_party_ak_id,
	LKP_Claim_Party.claim_party_full_name AS lkp_claim_party_full_name,
	LKP_Claim_Party.claim_party_first_name AS lkp_claim_party_first_name,
	LKP_Claim_Party.claim_party_last_name AS lkp_claim_party_last_name,
	LKP_Claim_Party.claim_party_mid_name AS lkp_claim_party_mid_name,
	LKP_Claim_Party.claim_party_name_prfx AS lkp_claim_party_name_prfx,
	LKP_Claim_Party.claim_party_name_sfx AS lkp_claim_party_name_sfx,
	LKP_Claim_Party.claim_party_addr AS lkp_claimant_addr,
	LKP_Claim_Party.claim_party_city AS lkp_claimant_city,
	LKP_Claim_Party.claim_party_county AS lkp_claimant_county,
	LKP_Claim_Party.claim_party_state AS lkp_claimant_state,
	LKP_Claim_Party.claim_party_zip AS lkp_claimant_zip,
	LKP_Claim_Party.addr_type AS lkp_addr_type,
	LKP_Claim_Party.tax_ssn_id AS lkp_tax_ssn_id,
	LKP_Claim_Party.tax_fed_id AS lkp_tax_fed_id,
	LKP_Claim_Party.claim_party_birthdate AS lkp_claim_party_birthdate,
	LKP_Claim_Party.claim_party_gndr AS lkp_claim_party_gndr,
	'0' AS logical_flag_op,
	1 AS Crrnt_Snpsht_Flag,
	-- *INF*: iif(isnull(lkp_claim_party_id),'NEW',
	-- 	iif (
	-- 	(ltrim(rtrim(CICL_FULL_NM)) <> ltrim(rtrim(lkp_claim_party_full_name))) or
	-- 	(ltrim(rtrim(CICL_FST_NM)) <> ltrim(rtrim(lkp_claim_party_first_name))) or
	-- 	(ltrim(rtrim(CICL_LST_NM)) <> ltrim(rtrim(lkp_claim_party_last_name))) or
	-- 	(ltrim(rtrim(CICL_MDL_NM)) <> ltrim(rtrim(lkp_claim_party_mid_name))) or
	-- 	(ltrim(rtrim(NM_PFX)) <> ltrim(rtrim(lkp_claim_party_name_prfx))) or
	-- 	(ltrim(rtrim(NM_SFX)) <> ltrim(rtrim(lkp_claim_party_name_sfx))) or
	-- 	(ltrim(rtrim(CICA_ADR)) <> ltrim(rtrim(lkp_claimant_addr) )) or
	-- 	(ltrim(rtrim(CICA_CTY)) <> ltrim(rtrim(lkp_claimant_city) )) or
	-- 	(ltrim(rtrim(CICA_CIT_NM)) <>  ltrim(rtrim(lkp_claimant_county))) or
	-- 	(ltrim(rtrim(ST_CD)) <> ltrim(rtrim(lkp_claimant_state))) or
	-- 	(ltrim(rtrim(CICA_PST_CD)) <> ltrim(rtrim(lkp_claimant_zip))) or
	--       (ltrim(rtrim(CICA_TAX_FED_ID)) <>  ltrim(rtrim(lkp_tax_fed_id))) or
	--        (ltrim(rtrim(CICA_TAX_SSN_ID)) <> ltrim(rtrim(lkp_tax_ssn_id))) or
	-- 	(BIRTH_DATE <> lkp_claim_party_birthdate) or
	--       (ltrim(rtrim(GENDER)) <>  ltrim(rtrim(lkp_claim_party_gndr))) or
	-- 	(ltrim(rtrim(ADR_TYP_CD)) <> ltrim(rtrim(lkp_addr_type))) ,
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(lkp_claim_party_id IS NULL, 'NEW', IFF(( ltrim(rtrim(CICL_FULL_NM)) <> ltrim(rtrim(lkp_claim_party_full_name)) ) OR ( ltrim(rtrim(CICL_FST_NM)) <> ltrim(rtrim(lkp_claim_party_first_name)) ) OR ( ltrim(rtrim(CICL_LST_NM)) <> ltrim(rtrim(lkp_claim_party_last_name)) ) OR ( ltrim(rtrim(CICL_MDL_NM)) <> ltrim(rtrim(lkp_claim_party_mid_name)) ) OR ( ltrim(rtrim(NM_PFX)) <> ltrim(rtrim(lkp_claim_party_name_prfx)) ) OR ( ltrim(rtrim(NM_SFX)) <> ltrim(rtrim(lkp_claim_party_name_sfx)) ) OR ( ltrim(rtrim(CICA_ADR)) <> ltrim(rtrim(lkp_claimant_addr)) ) OR ( ltrim(rtrim(CICA_CTY)) <> ltrim(rtrim(lkp_claimant_city)) ) OR ( ltrim(rtrim(CICA_CIT_NM)) <> ltrim(rtrim(lkp_claimant_county)) ) OR ( ltrim(rtrim(ST_CD)) <> ltrim(rtrim(lkp_claimant_state)) ) OR ( ltrim(rtrim(CICA_PST_CD)) <> ltrim(rtrim(lkp_claimant_zip)) ) OR ( ltrim(rtrim(CICA_TAX_FED_ID)) <> ltrim(rtrim(lkp_tax_fed_id)) ) OR ( ltrim(rtrim(CICA_TAX_SSN_ID)) <> ltrim(rtrim(lkp_tax_ssn_id)) ) OR ( BIRTH_DATE <> lkp_claim_party_birthdate ) OR ( ltrim(rtrim(GENDER)) <> ltrim(rtrim(lkp_claim_party_gndr)) ) OR ( ltrim(rtrim(ADR_TYP_CD)) <> ltrim(rtrim(lkp_addr_type)) ), 'UPDATE', 'NOCHANGE')) AS v_Changed_Flag,
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
	SYSDATE AS Modified_Date,
	'N/A' AS Out_Default_String,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS claim_party_ref_eff_from_date
	FROM EXP_Lkp_Values
	LEFT JOIN LKP_Claim_Party
	ON LKP_Claim_Party.claim_party_key = EXP_Lkp_Values.CLIENT_ID
),
FIL_Insert AS (
	SELECT
	lkp_claim_party_ak_id, 
	CLIENT_ID, 
	CICL_FULL_NM, 
	CICL_FST_NM, 
	CICL_LST_NM, 
	CICL_MDL_NM, 
	NM_PFX, 
	NM_SFX, 
	ADR_TYP_CD, 
	CICA_ADR, 
	CICA_CIT_NM, 
	CICA_CTY, 
	ST_CD, 
	CICA_PST_CD, 
	CICA_TAX_SSN_ID, 
	CICA_TAX_FED_ID, 
	BIRTH_DATE, 
	GENDER, 
	logical_flag_op, 
	Crrnt_Snpsht_Flag, 
	Audit_Id, 
	Eff_From_Date, 
	Eff_To_Date, 
	SOURCE_SYSTEM_ID, 
	Created_Date, 
	Modified_Date, 
	Changed_Flag, 
	Out_Default_String, 
	claim_party_ref_eff_from_date
	FROM EXP_Detect_Changes
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
SEQ_claim_party AS (
	CREATE SEQUENCE SEQ_claim_party
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK AS (
	SELECT
	lkp_claim_party_ak_id,
	-- *INF*: IIF(Changed_Flag='NEW', NEXTVAL, lkp_claim_party_ak_id)
	IFF(Changed_Flag = 'NEW', NEXTVAL, lkp_claim_party_ak_id) AS claim_party_ak_id,
	CLIENT_ID,
	CICL_FULL_NM,
	CICL_FST_NM,
	CICL_LST_NM,
	CICL_MDL_NM,
	NM_PFX,
	NM_SFX,
	ADR_TYP_CD,
	CICA_ADR,
	CICA_CIT_NM,
	CICA_CTY,
	ST_CD,
	CICA_PST_CD,
	CICA_TAX_SSN_ID,
	CICA_TAX_FED_ID,
	BIRTH_DATE,
	GENDER,
	logical_flag_op,
	Crrnt_Snpsht_Flag,
	Audit_Id,
	Eff_From_Date,
	Eff_To_Date,
	SOURCE_SYSTEM_ID,
	Created_Date,
	Modified_Date,
	Changed_Flag,
	Out_Default_String,
	claim_party_ref_eff_from_date,
	-- *INF*: TO_DATE('12/31/2999','MM/DD/YYYY')
	TO_DATE('12/31/2999', 'MM/DD/YYYY') AS out_default_high_date,
	SEQ_claim_party.NEXTVAL
	FROM FIL_Insert
),
claim_party_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party
	(claim_party_ak_id, claim_party_key, claim_party_full_name, claim_party_first_name, claim_party_last_name, claim_party_mid_name, claim_party_name_prfx, claim_party_name_sfx, claim_party_addr, claim_party_city, claim_party_county, claim_party_state, claim_party_zip, addr_type, tax_ssn_id, tax_fed_id, claim_party_birthdate, claim_party_gndr, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, ph_num, ph_extension, ph_type, lgl_ent_code, claim_party_ref_eff_from_date, claim_party_death_date)
	SELECT 
	CLAIM_PARTY_AK_ID, 
	CLIENT_ID AS CLAIM_PARTY_KEY, 
	CICL_FULL_NM AS CLAIM_PARTY_FULL_NAME, 
	CICL_FST_NM AS CLAIM_PARTY_FIRST_NAME, 
	CICL_LST_NM AS CLAIM_PARTY_LAST_NAME, 
	CICL_MDL_NM AS CLAIM_PARTY_MID_NAME, 
	NM_PFX AS CLAIM_PARTY_NAME_PRFX, 
	NM_SFX AS CLAIM_PARTY_NAME_SFX, 
	CICA_ADR AS CLAIM_PARTY_ADDR, 
	CICA_CIT_NM AS CLAIM_PARTY_CITY, 
	CICA_CTY AS CLAIM_PARTY_COUNTY, 
	ST_CD AS CLAIM_PARTY_STATE, 
	CICA_PST_CD AS CLAIM_PARTY_ZIP, 
	ADR_TYP_CD AS ADDR_TYPE, 
	CICA_TAX_SSN_ID AS TAX_SSN_ID, 
	CICA_TAX_FED_ID AS TAX_FED_ID, 
	BIRTH_DATE AS CLAIM_PARTY_BIRTHDATE, 
	GENDER AS CLAIM_PARTY_GNDR, 
	logical_flag_op AS LOGICAL_FLAG, 
	Crrnt_Snpsht_Flag AS CRRNT_SNPSHT_FLAG, 
	Audit_Id AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	Out_Default_String AS PH_NUM, 
	Out_Default_String AS PH_EXTENSION, 
	Out_Default_String AS PH_TYPE, 
	Out_Default_String AS LGL_ENT_CODE, 
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
	WHERE 
	 a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and 
	 EXISTS(SELECT 1			
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
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party AS T
	USING UPD_Claim_Party AS S
	ON T.claim_party_id = S.claim_party_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),