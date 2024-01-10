WITH
LKP_Sup_State AS (
	SELECT
	sup_state_id,
	state_code
	FROM (
		SELECT 
		a.sup_state_id as sup_state_id, 
		a.state_code as state_code 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state a
		WHERE 
		crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY sup_state_id) = 1
),
SQ_PIF_42GJ_stage AS (
	SELECT 
	PIF_42GJ_stage.PIF_SYMBOL, 
	PIF_42GJ_stage.PIF_POLICY_NUMBER, 
	PIF_42GJ_stage.PIF_MODULE, 
	(PIF_SYMBOL + PIF_POLICY_NUMBER + PIF_MODULE + 
	CASE len(IPFC4J_LOSS_MONTH) when 1 THEN '0' + CAST(IPFC4J_LOSS_MONTH AS VARCHAR) ELSE CAST(IPFC4J_LOSS_MONTH AS VARCHAR) END + 
	CASE len(IPFC4J_LOSS_DAY) when 1 THEN '0' + CAST(IPFC4J_LOSS_DAY AS VARCHAR) ELSE CAST(IPFC4J_LOSS_DAY AS VARCHAR) END +
	CAST(IPFC4J_LOSS_YEAR AS VARCHAR) +
	CAST(IPFC4J_LOSS_OCCURENCE AS VARCHAR) + 
	CAST(IPFC4J_LOSS_CLAIMANT AS VARCHAR) + IPFC4J_USE_CODE) as  IPFC4J_ACTION_CODE, 
	PIF_42GJ_stage.IPFC4J_LOSS_YEAR, 
	PIF_42GJ_stage.IPFC4J_LOSS_MONTH, 
	PIF_42GJ_stage.IPFC4J_LOSS_DAY, 
	PIF_42GJ_stage.IPFC4J_LOSS_OCCURENCE, 
	PIF_42GJ_stage.IPFC4J_LOSS_CLAIMANT, 
	PIF_42GJ_stage.IPFC4J_ADDRESS_LINE_1, 
	PIF_42GJ_stage.IPFC4J_ADDR_LIN_2_POS_1, 
	PIF_42GJ_stage.IPFC4J_ADDR_LIN_2_POS_2_30, 
	PIF_42GJ_stage.IPFC4J_ADDRESS_LINE_3, 
	PIF_42GJ_stage.IPFC4J_ADDRESS_LINE_4, 
	PIF_42GJ_STAGE.IPFC4J_ID_NUMBER,
	PIF_42GJ_stage.IPFC4J_ZIP_BASIC, 
	PIF_42GJ_stage.IPFC4J_ZIP_EXPANDED , 
	PIF_42GJ_STAGE.LOGICAL_FLAG 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.PIF_42GJ_STAGE
	WHERE
	IPFC4J_USE_CODE IN ('CMT','DRV','PS','HS','UNL')
	AND PIF_42GJ_STAGE.LOGICAL_FLAG  IN ('0','1','5')
),
EXP_Values AS (
	SELECT
	IPFC4J_ACTION_CODE AS CLIENT_ID,
	IPFC4J_ADDRESS_LINE_1,
	IPFC4J_ADDR_LIN_2_POS_1,
	IPFC4J_ADDR_LIN_2_POS_2_30,
	-- *INF*: DECODE (IPFC4J_ADDR_LIN_2_POS_1,
	-- 'E' , IPFC4J_ADDR_LIN_2_POS_1  || ' '  || RTRIM(IPFC4J_ADDR_LIN_2_POS_2_30),
	-- 'W' , IPFC4J_ADDR_LIN_2_POS_1  || ' '  || RTRIM(IPFC4J_ADDR_LIN_2_POS_2_30),
	-- 'N' , IPFC4J_ADDR_LIN_2_POS_1  || ' '  || RTRIM(IPFC4J_ADDR_LIN_2_POS_2_30),
	-- 'S' , IPFC4J_ADDR_LIN_2_POS_1  || ' '  || RTRIM(IPFC4J_ADDR_LIN_2_POS_2_30),
	-- IPFC4J_ADDR_LIN_2_POS_1  || RTRIM(IPFC4J_ADDR_LIN_2_POS_2_30))
	-- 
	DECODE(IPFC4J_ADDR_LIN_2_POS_1,
		'E', IPFC4J_ADDR_LIN_2_POS_1 || ' ' || RTRIM(IPFC4J_ADDR_LIN_2_POS_2_30
		),
		'W', IPFC4J_ADDR_LIN_2_POS_1 || ' ' || RTRIM(IPFC4J_ADDR_LIN_2_POS_2_30
		),
		'N', IPFC4J_ADDR_LIN_2_POS_1 || ' ' || RTRIM(IPFC4J_ADDR_LIN_2_POS_2_30
		),
		'S', IPFC4J_ADDR_LIN_2_POS_1 || ' ' || RTRIM(IPFC4J_ADDR_LIN_2_POS_2_30
		),
		IPFC4J_ADDR_LIN_2_POS_1 || RTRIM(IPFC4J_ADDR_LIN_2_POS_2_30
		)
	) AS v_IPFC4J_ADDR_LIN_2,
	-- *INF*: IIF(ISNULL(in_IPFC4J_ADDRESS_LINE_3), 
	-- v_IPFC4J_ADDR_LIN_2, 
	-- v_IPFC4J_ADDR_LIN_2  || ' '
	-- )
	IFF(in_IPFC4J_ADDRESS_LINE_3 IS NULL,
		v_IPFC4J_ADDR_LIN_2,
		v_IPFC4J_ADDR_LIN_2 || ' '
	) AS v_ADDR_LIN_2,
	IPFC4J_ADDRESS_LINE_3 AS in_IPFC4J_ADDRESS_LINE_3,
	-- *INF*: IIF(ISNULL(IPFC4J_ADDRESS_LINE_4), 
	-- RTRIM(in_IPFC4J_ADDRESS_LINE_3), 
	-- RTRIM(in_IPFC4J_ADDRESS_LINE_3)  || ' '
	-- )
	IFF(IPFC4J_ADDRESS_LINE_4 IS NULL,
		RTRIM(in_IPFC4J_ADDRESS_LINE_3
		),
		RTRIM(in_IPFC4J_ADDRESS_LINE_3
		) || ' '
	) AS v_ADDRESS_LINE_3,
	IPFC4J_ADDRESS_LINE_4,
	-- *INF*: IIF(
	-- ISNULL(IPFC4J_ADDRESS_LINE_1), 'N/A',
	-- v_ADDR_LIN_2 || v_ADDRESS_LINE_3 || RTRIM(IPFC4J_ADDRESS_LINE_4))
	IFF(IPFC4J_ADDRESS_LINE_1 IS NULL,
		'N/A',
		v_ADDR_LIN_2 || v_ADDRESS_LINE_3 || RTRIM(IPFC4J_ADDRESS_LINE_4
		)
	) AS IPFC4J_ADDRESS,
	IPFC4J_ZIP_BASIC,
	IPFC4J_ZIP_EXPANDED,
	-- *INF*: IIF(
	-- (ISNULL(IPFC4J_ZIP_BASIC) OR LENGTH(RTRIM(IPFC4J_ZIP_BASIC)) = 0),
	-- 'N/A',
	-- IPFC4J_ZIP_BASIC || '-'  || to_char(IPFC4J_ZIP_EXPANDED))
	IFF(( IPFC4J_ZIP_BASIC IS NULL 
			OR LENGTH(RTRIM(IPFC4J_ZIP_BASIC
				)
			) = 0 
		),
		'N/A',
		IPFC4J_ZIP_BASIC || '-' || to_char(IPFC4J_ZIP_EXPANDED
		)
	) AS ZIP_CODE,
	PIF_SYMBOL,
	PIF_POLICY_NUMBER,
	PIF_MODULE,
	IPFC4J_LOSS_YEAR,
	IPFC4J_LOSS_MONTH,
	IPFC4J_LOSS_DAY,
	IPFC4J_LOSS_OCCURENCE,
	IPFC4J_LOSS_CLAIMANT,
	logical_flag,
	IPFC4J_ID_NUMBER
	FROM SQ_PIF_42GJ_stage
),
LKP_PIF_42GQ_CMT_stage AS (
	SELECT
	ipfcgq_claimant_birth_month,
	ipfcgq_claimant_birth_day,
	ipfcgq_claimant_birth_year,
	ipfcgq_loss_sex,
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfcgq_year_of_loss,
	ipfcgq_month_of_loss,
	ipfcgq_day_of_loss,
	ipfcgq_loss_occurence,
	ipfcgq_loss_claimant
	FROM (
		SELECT 
			ipfcgq_claimant_birth_month,
			ipfcgq_claimant_birth_day,
			ipfcgq_claimant_birth_year,
			ipfcgq_loss_sex,
			pif_symbol,
			pif_policy_number,
			pif_module,
			ipfcgq_year_of_loss,
			ipfcgq_month_of_loss,
			ipfcgq_day_of_loss,
			ipfcgq_loss_occurence,
			ipfcgq_loss_claimant
		FROM pif_42gq_cmt_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,ipfcgq_year_of_loss,ipfcgq_month_of_loss,ipfcgq_day_of_loss,ipfcgq_loss_occurence,ipfcgq_loss_claimant ORDER BY ipfcgq_claimant_birth_month) = 1
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
	EXP_Values.IPFC4J_ADDRESS_LINE_1 AS in_CICL_FULL_NM,
	-- *INF*: iif(isnull(in_CICL_FULL_NM),'N/A',
	--    iif(is_spaces(in_CICL_FULL_NM),'N/A',
	--     rtrim(in_CICL_FULL_NM)))
	IFF(in_CICL_FULL_NM IS NULL,
		'N/A',
		IFF(LENGTH(in_CICL_FULL_NM)>0 AND TRIM(in_CICL_FULL_NM)='',
			'N/A',
			rtrim(in_CICL_FULL_NM
			)
		)
	) AS CICL_FULL_NM,
	'N/A' AS CICL_FST_NM,
	'N/A' AS CICL_LST_NM,
	'N/A' AS CICL_MDL_NM,
	'N/A' AS NM_PFX,
	'N/A' AS NM_SFX,
	EXP_Values.IPFC4J_ADDRESS AS in_CICA_ADR,
	-- *INF*: iif(isnull(rtrim(in_CICA_ADR)),'N/A',
	--    iif(is_spaces(in_CICA_ADR),'N/A',
	--     rtrim(in_CICA_ADR)))
	IFF(rtrim(in_CICA_ADR
		) IS NULL,
		'N/A',
		IFF(LENGTH(in_CICA_ADR)>0 AND TRIM(in_CICA_ADR)='',
			'N/A',
			rtrim(in_CICA_ADR
			)
		)
	) AS CICA_ADR,
	'N/A' AS CICA_CIT_NM,
	'N/A' AS CICA_CTY,
	-- *INF*: IIF(ISNULL(RTRIM(in_CICA_ADR)),'N/A',
	--    IIF(IS_SPACES(in_CICA_ADR),'N/A',
	-- ltrim(SUBSTR( RTRIM(in_CICA_ADR), LENGTH(in_CICA_ADR) - 2))
	-- ))
	IFF(RTRIM(in_CICA_ADR
		) IS NULL,
		'N/A',
		IFF(LENGTH(in_CICA_ADR)>0 AND TRIM(in_CICA_ADR)='',
			'N/A',
			ltrim(SUBSTR(RTRIM(in_CICA_ADR
					), LENGTH(in_CICA_ADR
					) - 2
				)
			)
		)
	) AS v_ST_CD,
	-- *INF*: IIF(ISNULL(:LKP.lkp_sup_state (v_ST_CD)),
	-- 'N/A',
	-- v_ST_CD)
	IFF(LKP_SUP_STATE_v_ST_CD.sup_state_id IS NULL,
		'N/A',
		v_ST_CD
	) AS ST_CD,
	EXP_Values.ZIP_CODE AS in_CICA_PST_CD,
	-- *INF*: iif(isnull(rtrim(in_CICA_PST_CD)),'N/A',
	--    iif(is_spaces(in_CICA_PST_CD),'N/A',
	--     rtrim(in_CICA_PST_CD)))
	IFF(rtrim(in_CICA_PST_CD
		) IS NULL,
		'N/A',
		IFF(LENGTH(in_CICA_PST_CD)>0 AND TRIM(in_CICA_PST_CD)='',
			'N/A',
			rtrim(in_CICA_PST_CD
			)
		)
	) AS CICA_PST_CD,
	'N/A' AS ADR_TYP_CD,
	LKP_PIF_4578_STAGE.LOSS_1099_NUMBER AS in_CICL_TAX_ID,
	'N/A' AS CICL_TAX_SSN_ID,
	-- *INF*: 'N/A'
	-- --iif(isnull(rtrim(in_CICL_TAX_ID)),'N/A',   iif(is_spaces(in_CICL_TAX_ID),'N/A',    rtrim(in_CICL_TAX_ID)))
	'N/A' AS CICL_TAX_FED_ID,
	LKP_PIF_42GQ_CMT_stage.ipfcgq_claimant_birth_year AS in_ipfcgq_claimant_birth_year,
	LKP_PIF_42GQ_CMT_stage.ipfcgq_loss_sex AS in_IPFCGQ_LOSS_SEX,
	-- *INF*: IIF(
	-- (ISNULL(in_IPFCGQ_LOSS_SEX) OR IS_SPACES(in_IPFCGQ_LOSS_SEX) OR LENGTH(in_IPFCGQ_LOSS_SEX) = 0),
	--    'N/A',
	--    in_IPFCGQ_LOSS_SEX)
	IFF(( in_IPFCGQ_LOSS_SEX IS NULL 
			OR LENGTH(in_IPFCGQ_LOSS_SEX)>0 AND TRIM(in_IPFCGQ_LOSS_SEX)='' 
			OR LENGTH(in_IPFCGQ_LOSS_SEX
			) = 0 
		),
		'N/A',
		in_IPFCGQ_LOSS_SEX
	) AS IPFCGQ_LOSS_SEX,
	EXP_Values.logical_flag,
	LKP_PIF_42GQ_CMT_stage.ipfcgq_claimant_birth_month AS claimant_birth_month,
	-- *INF*: IIF(ISNULL(claimant_birth_month) OR LENGTH(to_char(claimant_birth_month)) =0, '00', 
	-- IIF(LENGTH(to_char(claimant_birth_month)) =1, '0'  || TO_CHAR(claimant_birth_month),TO_CHAR(claimant_birth_month)))
	IFF(claimant_birth_month IS NULL 
		OR LENGTH(to_char(claimant_birth_month
			)
		) = 0,
		'00',
		IFF(LENGTH(to_char(claimant_birth_month
				)
			) = 1,
			'0' || TO_CHAR(claimant_birth_month
			),
			TO_CHAR(claimant_birth_month
			)
		)
	) AS var_claimant_birth_month,
	LKP_PIF_42GQ_CMT_stage.ipfcgq_claimant_birth_day AS claimant_birth_day,
	-- *INF*: IIF(ISNULL(claimant_birth_day) OR LENGTH(to_char(claimant_birth_day)) =0, '00', 
	-- IIF(LENGTH(to_char(claimant_birth_day)) =1, '0'  || TO_CHAR(claimant_birth_day),TO_CHAR(claimant_birth_day)))
	IFF(claimant_birth_day IS NULL 
		OR LENGTH(to_char(claimant_birth_day
			)
		) = 0,
		'00',
		IFF(LENGTH(to_char(claimant_birth_day
				)
			) = 1,
			'0' || TO_CHAR(claimant_birth_day
			),
			TO_CHAR(claimant_birth_day
			)
		)
	) AS var_claimant_birth_day,
	LKP_PIF_42GQ_CMT_stage.ipfcgq_claimant_birth_year AS claimant_birth_year,
	-- *INF*: IIF(ISNULL(claimant_birth_year) OR LENGTH(to_char(claimant_birth_year)) <4, '0000', TO_CHAR(claimant_birth_year))
	IFF(claimant_birth_year IS NULL 
		OR LENGTH(to_char(claimant_birth_year
			)
		) < 4,
		'0000',
		TO_CHAR(claimant_birth_year
		)
	) AS var_claimant_birth_year,
	-- *INF*: IIF(LENGTH(var_claimant_birth_month) =1 , '0' || var_claimant_birth_month, var_claimant_birth_month)
	-- || '/' ||
	-- IIF(LENGTH(var_claimant_birth_day) = 1, '0' || var_claimant_birth_day,var_claimant_birth_day)
	-- ||  '/' ||
	-- var_claimant_birth_year
	-- 
	-- 
	-- 
	-- ---var_claimant_birth_month || '/' ||   var_claimant_birth_day || '/' ||  var_claimant_birth_year
	-- 
	IFF(LENGTH(var_claimant_birth_month
		) = 1,
		'0' || var_claimant_birth_month,
		var_claimant_birth_month
	) || '/' || IFF(LENGTH(var_claimant_birth_day
		) = 1,
		'0' || var_claimant_birth_day,
		var_claimant_birth_day
	) || '/' || var_claimant_birth_year AS var_CLAIMANT_BIRTH_DATE,
	-- *INF*: IIF(LENGTH(var_CLAIMANT_BIRTH_DATE) < 10 OR claimant_birth_year < 1800 OR 
	-- var_claimant_birth_month='00' OR var_claimant_birth_day = '00' OR var_claimant_birth_year = '0000' or NOT IS_DATE(var_CLAIMANT_BIRTH_DATE,'MM/DD/YYYY') ,
	-- TO_DATE('12/31/2100','MM/DD/YYYY'),
	-- TO_DATE(var_claimant_birth_month || '/' || var_claimant_birth_day || '/' || var_claimant_birth_year,'MM/DD/YYYY'))
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(LENGTH(var_CLAIMANT_BIRTH_DATE
		) < 10 
		OR claimant_birth_year < 1800 
		OR var_claimant_birth_month = '00' 
		OR var_claimant_birth_day = '00' 
		OR var_claimant_birth_year = '0000' 
		OR NOT IS_DATE(var_CLAIMANT_BIRTH_DATE, 'MM/DD/YYYY'
		),
		TO_DATE('12/31/2100', 'MM/DD/YYYY'
		),
		TO_DATE(var_claimant_birth_month || '/' || var_claimant_birth_day || '/' || var_claimant_birth_year, 'MM/DD/YYYY'
		)
	) AS v_CLAIMANT_BIRTH_DATE,
	v_CLAIMANT_BIRTH_DATE AS CLAIMANT_BIRTH_DATE,
	EXP_Values.IPFC4J_ID_NUMBER,
	-- *INF*: IIF(ISNULL(IPFC4J_ID_NUMBER) OR IS_SPACES(IPFC4J_ID_NUMBER) OR LENGTH(IPFC4J_ID_NUMBER)=0  
	-- OR LENGTH(IPFC4J_ID_NUMBER)<11 ,'N/A',IPFC4J_ID_NUMBER)
	IFF(IPFC4J_ID_NUMBER IS NULL 
		OR LENGTH(IPFC4J_ID_NUMBER)>0 AND TRIM(IPFC4J_ID_NUMBER)='' 
		OR LENGTH(IPFC4J_ID_NUMBER
		) = 0 
		OR LENGTH(IPFC4J_ID_NUMBER
		) < 11,
		'N/A',
		IPFC4J_ID_NUMBER
	) AS IPFC4J_ID_NUMBER_OUT
	FROM EXP_Values
	LEFT JOIN LKP_PIF_42GQ_CMT_stage
	ON LKP_PIF_42GQ_CMT_stage.pif_symbol = EXP_Values.PIF_SYMBOL AND LKP_PIF_42GQ_CMT_stage.pif_policy_number = EXP_Values.PIF_POLICY_NUMBER AND LKP_PIF_42GQ_CMT_stage.pif_module = EXP_Values.PIF_MODULE AND LKP_PIF_42GQ_CMT_stage.ipfcgq_year_of_loss = EXP_Values.IPFC4J_LOSS_YEAR AND LKP_PIF_42GQ_CMT_stage.ipfcgq_month_of_loss = EXP_Values.IPFC4J_LOSS_MONTH AND LKP_PIF_42GQ_CMT_stage.ipfcgq_day_of_loss = EXP_Values.IPFC4J_LOSS_DAY AND LKP_PIF_42GQ_CMT_stage.ipfcgq_loss_occurence = EXP_Values.IPFC4J_LOSS_OCCURENCE AND LKP_PIF_42GQ_CMT_stage.ipfcgq_loss_claimant = EXP_Values.IPFC4J_LOSS_CLAIMANT
	LEFT JOIN LKP_PIF_4578_STAGE
	ON LKP_PIF_4578_STAGE.PIF_SYMBOL = EXP_Values.PIF_SYMBOL AND LKP_PIF_4578_STAGE.PIF_POLICY_NUMBER = EXP_Values.PIF_POLICY_NUMBER AND LKP_PIF_4578_STAGE.PIF_MODULE = EXP_Values.PIF_MODULE AND LKP_PIF_4578_STAGE.LOSS_YEAR = EXP_Values.IPFC4J_LOSS_YEAR AND LKP_PIF_4578_STAGE.LOSS_MONTH = EXP_Values.IPFC4J_LOSS_MONTH AND LKP_PIF_4578_STAGE.LOSS_DAY = EXP_Values.IPFC4J_LOSS_DAY AND LKP_PIF_4578_STAGE.LOSS_OCCURENCE = EXP_Values.IPFC4J_LOSS_OCCURENCE AND LKP_PIF_4578_STAGE.LOSS_CLAIMANT = EXP_Values.IPFC4J_LOSS_CLAIMANT
	LEFT JOIN LKP_SUP_STATE LKP_SUP_STATE_v_ST_CD
	ON LKP_SUP_STATE_v_ST_CD.state_code = v_ST_CD

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
		SELECT 
		a.claim_party_id as claim_party_id, 
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
		WHERE  a. source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND a.crrnt_snpsht_flag = 1
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
	EXP_Lkp_Values.CICL_TAX_SSN_ID,
	EXP_Lkp_Values.CICL_TAX_FED_ID,
	EXP_Lkp_Values.CLAIMANT_BIRTH_DATE,
	EXP_Lkp_Values.IPFCGQ_LOSS_SEX,
	EXP_Lkp_Values.IPFC4J_ID_NUMBER_OUT AS IPFC4J_ID_NUMBER,
	LKP_Claim_Party.claim_party_id,
	LKP_Claim_Party.claim_party_ak_id AS lkp_claim_party_ak_id,
	LKP_Claim_Party.claim_party_full_name,
	LKP_Claim_Party.claim_party_first_name,
	LKP_Claim_Party.claim_party_last_name,
	LKP_Claim_Party.claim_party_mid_name,
	LKP_Claim_Party.claim_party_name_prfx,
	LKP_Claim_Party.claim_party_name_sfx,
	LKP_Claim_Party.claim_party_addr AS claimant_addr,
	LKP_Claim_Party.claim_party_city AS claimant_city,
	LKP_Claim_Party.claim_party_county AS claimant_county,
	LKP_Claim_Party.claim_party_state AS claimant_state,
	LKP_Claim_Party.claim_party_zip AS claimant_zip,
	LKP_Claim_Party.addr_type,
	LKP_Claim_Party.tax_ssn_id,
	LKP_Claim_Party.tax_fed_id,
	LKP_Claim_Party.claim_party_birthdate,
	LKP_Claim_Party.claim_party_gndr,
	EXP_Lkp_Values.logical_flag,
	1 AS Crrnt_Snpsht_Flag,
	-- *INF*: iif(isnull(claim_party_id),'NEW',
	-- 	iif (
	-- 	(ltrim(rtrim(CICL_FULL_NM)) <> ltrim(rtrim(claim_party_full_name))) or
	-- 	(ltrim(rtrim(CICL_FST_NM)) <> ltrim(rtrim(claim_party_first_name))) or
	-- 	(ltrim(rtrim(CICL_LST_NM)) <> ltrim(rtrim(claim_party_last_name))) or
	-- 	(ltrim(rtrim(CICL_MDL_NM)) <> ltrim(rtrim(claim_party_mid_name))) or
	-- 	(ltrim(rtrim(NM_PFX)) <> ltrim(rtrim(claim_party_name_prfx))) or
	-- 	(ltrim(rtrim(NM_SFX)) <> ltrim(rtrim(claim_party_name_sfx))) or
	-- 	(ltrim(rtrim(CICA_ADR)) <> ltrim(rtrim(claimant_addr) )) or
	-- 	(ltrim(rtrim(CICA_CTY)) <> ltrim(rtrim(claimant_city) )) or
	-- 	(ltrim(rtrim(CICA_CIT_NM)) <>  ltrim(rtrim(claimant_county))) or
	-- 	(ltrim(rtrim(ST_CD)) <> ltrim(rtrim(claimant_state))) or
	-- 	(ltrim(rtrim(CICA_PST_CD)) <> ltrim(rtrim(claimant_zip))) or
	-- 	(ltrim(rtrim(CICL_TAX_FED_ID)) <>  ltrim(rtrim(tax_fed_id))) or
	--       (ltrim(rtrim(IPFC4J_ID_NUMBER)) <> ltrim(rtrim(tax_ssn_id))) or
	-- 	(CLAIMANT_BIRTH_DATE <> claim_party_birthdate) or
	-- 	(ltrim(rtrim(IPFCGQ_LOSS_SEX)) <>  ltrim(rtrim(claim_party_gndr))) or      
	-- 	(ltrim(rtrim(ADR_TYP_CD)) <> ltrim(rtrim(addr_type))) ,
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(claim_party_id IS NULL,
		'NEW',
		IFF(( ltrim(rtrim(CICL_FULL_NM
					)
				) <> ltrim(rtrim(claim_party_full_name
					)
				) 
			) 
			OR ( ltrim(rtrim(CICL_FST_NM
					)
				) <> ltrim(rtrim(claim_party_first_name
					)
				) 
			) 
			OR ( ltrim(rtrim(CICL_LST_NM
					)
				) <> ltrim(rtrim(claim_party_last_name
					)
				) 
			) 
			OR ( ltrim(rtrim(CICL_MDL_NM
					)
				) <> ltrim(rtrim(claim_party_mid_name
					)
				) 
			) 
			OR ( ltrim(rtrim(NM_PFX
					)
				) <> ltrim(rtrim(claim_party_name_prfx
					)
				) 
			) 
			OR ( ltrim(rtrim(NM_SFX
					)
				) <> ltrim(rtrim(claim_party_name_sfx
					)
				) 
			) 
			OR ( ltrim(rtrim(CICA_ADR
					)
				) <> ltrim(rtrim(claimant_addr
					)
				) 
			) 
			OR ( ltrim(rtrim(CICA_CTY
					)
				) <> ltrim(rtrim(claimant_city
					)
				) 
			) 
			OR ( ltrim(rtrim(CICA_CIT_NM
					)
				) <> ltrim(rtrim(claimant_county
					)
				) 
			) 
			OR ( ltrim(rtrim(ST_CD
					)
				) <> ltrim(rtrim(claimant_state
					)
				) 
			) 
			OR ( ltrim(rtrim(CICA_PST_CD
					)
				) <> ltrim(rtrim(claimant_zip
					)
				) 
			) 
			OR ( ltrim(rtrim(CICL_TAX_FED_ID
					)
				) <> ltrim(rtrim(tax_fed_id
					)
				) 
			) 
			OR ( ltrim(rtrim(IPFC4J_ID_NUMBER
					)
				) <> ltrim(rtrim(tax_ssn_id
					)
				) 
			) 
			OR ( CLAIMANT_BIRTH_DATE <> claim_party_birthdate 
			) 
			OR ( ltrim(rtrim(IPFCGQ_LOSS_SEX
					)
				) <> ltrim(rtrim(claim_party_gndr
					)
				) 
			) 
			OR ( ltrim(rtrim(ADR_TYP_CD
					)
				) <> ltrim(rtrim(addr_type
					)
				) 
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_Id,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	SYSDATE)
	IFF(v_Changed_Flag = 'NEW',
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		SYSDATE
	) AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS Eff_To_Date,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date,
	'N/A' AS Out_Default_String,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS claim_party_ref_eff_from_date
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
	IPFC4J_ID_NUMBER, 
	CICL_TAX_FED_ID, 
	CLAIMANT_BIRTH_DATE, 
	IPFCGQ_LOSS_SEX, 
	logical_flag, 
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
	SEQ_claim_party.NEXTVAL,
	lkp_claim_party_ak_id,
	-- *INF*: IIF(Changed_Flag='NEW', NEXTVAL, lkp_claim_party_ak_id)
	IFF(Changed_Flag = 'NEW',
		NEXTVAL,
		lkp_claim_party_ak_id
	) AS claim_party_ak_id,
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
	IPFC4J_ID_NUMBER,
	CICL_TAX_FED_ID,
	CLAIMANT_BIRTH_DATE,
	IPFCGQ_LOSS_SEX,
	logical_flag,
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
	TO_DATE('12/31/2999', 'MM/DD/YYYY'
	) AS out_default_high_date
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
	CICA_CTY AS CLAIM_PARTY_CITY, 
	CICA_CIT_NM AS CLAIM_PARTY_COUNTY, 
	ST_CD AS CLAIM_PARTY_STATE, 
	CICA_PST_CD AS CLAIM_PARTY_ZIP, 
	ADR_TYP_CD AS ADDR_TYPE, 
	IPFC4J_ID_NUMBER AS TAX_SSN_ID, 
	CICL_TAX_FED_ID AS TAX_FED_ID, 
	CLAIMANT_BIRTH_DATE AS CLAIM_PARTY_BIRTHDATE, 
	IPFCGQ_LOSS_SEX AS CLAIM_PARTY_GNDR, 
	LOGICAL_FLAG, 
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
		claim_party_key = v_PREV_ROW_party_key, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
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