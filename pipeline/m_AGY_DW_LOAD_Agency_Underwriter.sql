WITH
LKP_Underwriter AS (
	SELECT
	uw_ak_id,
	uw_code
	FROM (
		SELECT u.uw_ak_id as uw_ak_id, 
		u.uw_code as uw_code FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.underwriter u
		where crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY uw_code ORDER BY uw_ak_id) = 1
),
SQ_agency_underwriter_stage AS (
	SELECT
		agency_uw_stage_id,
		state_code,
		agency_num,
		insurance_line,
		uw_assistant_flag,
		uw_code,
		agency_code,
		EXTRACT_DATE,
		AS_OF_DATE,
		RECORD_COUNT,
		SOURCE_SYSTEM_ID
	FROM agency_underwriter_stage
),
EXP_Lkp_Agency AS (
	SELECT
	state_code,
	agency_num,
	state_code || agency_num AS agency_key
	FROM SQ_agency_underwriter_stage
),
LKP_Agency AS (
	SELECT
	agency_ak_id,
	agency_state_code,
	agency_key
	FROM (
		SELECT a.agency_ak_id as agency_ak_id, a.agency_state_code as agency_state_code, a.agency_key as agency_key FROM  
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.agency a
		where crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY agency_key ORDER BY agency_ak_id) = 1
),
EXP_LkpValues AS (
	SELECT
	SQ_agency_underwriter_stage.state_code AS IN_state_code,
	SQ_agency_underwriter_stage.agency_num AS IN_agency_num,
	SQ_agency_underwriter_stage.insurance_line AS IN_insurance_line,
	SQ_agency_underwriter_stage.uw_assistant_flag AS IN_uw_assistant_flag,
	SQ_agency_underwriter_stage.uw_code AS IN_in_uw_code,
	-- *INF*: iif(isnull(IN_uw_assistant_flag),'X',iif(IS_SPACES(IN_uw_assistant_flag),'X',IN_uw_assistant_flag))
	IFF(IN_uw_assistant_flag IS NULL, 'X', IFF(IS_SPACES(IN_uw_assistant_flag), 'X', IN_uw_assistant_flag)) AS uw_assistant_flag,
	-- *INF*: iif(isnull(IN_insurance_line),'N/A',iif(IS_SPACES(IN_insurance_line),'N/A',
	-- rpad(IN_insurance_line,3)))
	IFF(IN_insurance_line IS NULL, 'N/A', IFF(IS_SPACES(IN_insurance_line), 'N/A', rpad(IN_insurance_line, 3))) AS insurance_line,
	-- *INF*: iif(isnull(IN_state_code),'N/A',iif(IS_SPACES(IN_state_code),'N/A',IN_state_code))
	IFF(IN_state_code IS NULL, 'N/A', IFF(IS_SPACES(IN_state_code), 'N/A', IN_state_code)) AS state_code,
	-- *INF*: iif(isnull(IN_agency_num),'N/A',iif(IS_SPACES(IN_agency_num),'N/A',IN_agency_num))
	IFF(IN_agency_num IS NULL, 'N/A', IFF(IS_SPACES(IN_agency_num), 'N/A', IN_agency_num)) AS agency_num,
	-- *INF*: iif(isnull(IN_in_uw_code),'N/A',iif(IS_SPACES(IN_in_uw_code),'N/A',IN_in_uw_code))
	IFF(IN_in_uw_code IS NULL, 'N/A', IFF(IS_SPACES(IN_in_uw_code), 'N/A', IN_in_uw_code)) AS in_uw_code,
	IN_state_code || IN_agency_num AS v_agency_key,
	-- *INF*: iif(isnull(state_code || agency_num),'N/A',
	-- iif(is_spaces(state_code || agency_num),'N/A',state_code || agency_num))
	IFF(state_code || agency_num IS NULL, 'N/A', IFF(is_spaces(state_code || agency_num), 'N/A', state_code || agency_num)) AS out_agency_key,
	LKP_Agency.agency_ak_id AS out_agency_ak_id,
	-- *INF*: :LKP.LKP_UNDERWRITER(IN_in_uw_code)
	LKP_UNDERWRITER_IN_in_uw_code.uw_ak_id AS out_uw_ak_id,
	SQ_agency_underwriter_stage.SOURCE_SYSTEM_ID,
	-- *INF*: rpad(IN_state_code,3)
	rpad(IN_state_code, 3) AS out_state_code,
	LKP_Agency.agency_state_code
	FROM SQ_agency_underwriter_stage
	LEFT JOIN LKP_Agency
	ON LKP_Agency.agency_key = EXP_Lkp_Agency.agency_key
	LEFT JOIN LKP_UNDERWRITER LKP_UNDERWRITER_IN_in_uw_code
	ON LKP_UNDERWRITER_IN_in_uw_code.uw_code = IN_in_uw_code

),
LKP_Agency_Underwriter AS (
	SELECT
	agency_uw_id,
	agency_uw_ak_id,
	uw_ak_id,
	agency_key,
	insurance_line,
	uw_assistant_flag,
	agency_ak_id
	FROM (
		SELECT a.agency_uw_id as agency_uw_id, 
		a.agency_uw_ak_id as agency_uw_ak_id,
		a.agency_ak_id as agency_ak_id, 
		a.uw_ak_id as uw_ak_id ,
		a.insurance_line as insurance_line, 
		a.uw_assistant_flag as uw_assistant_flag, 
		a.agency_key as agency_key
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.agency_underwriter a
		WHERE a.agency_uw_id IN(SELECT MAX(b.agency_uw_id)
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.agency_underwriter b
		WHERE crrnt_snpsht_flag=1
		GROUP BY  
			b.agency_ak_id,
		      b.insurance_line,
			  b.uw_assistant_flag )
			ORDER BY 
		     a.agency_ak_id,
			a.insurance_line,
			a.uw_assistant_flag
			
			
		--IN Subquery exists so that we only pick the MAX value of the PK for each AK Group
		--WHERE clause is always eff_to_date = '12/31/2100'
		--GROUP BY clause is always the AK
		--ORDER BY clause is always the AK.  When any comments exist in the SQL override Informatica will no longer generate an ORDER BY statemen
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY insurance_line,uw_assistant_flag,agency_ak_id ORDER BY agency_uw_id) = 1
),
SEQ_Agency_Underwriter AS (
	CREATE SEQUENCE SEQ_Agency_Underwriter
	START = 0
	INCREMENT = 1;
),
EXP_DetectChanges AS (
	SELECT
	EXP_LkpValues.out_agency_key,
	LKP_Agency_Underwriter.agency_uw_id AS agency_uw_id_old,
	LKP_Agency_Underwriter.agency_uw_ak_id,
	LKP_Agency_Underwriter.agency_key AS agency_key_old,
	LKP_Agency_Underwriter.uw_ak_id AS uw_ak_id_old,
	-- *INF*: iif(isnull(agency_uw_id_old),'NEW',
	-- 	iif(	(uw_ak_id_old<>out_uw_ak_id),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	-- 
	-- 
	-- 
	IFF(agency_uw_id_old IS NULL, 'NEW', IFF(( uw_ak_id_old <> out_uw_ak_id ), 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_changed_flag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	v_changed_flag AS changed_flag,
	sysdate AS created_date,
	sysdate AS modified_date,
	EXP_LkpValues.SOURCE_SYSTEM_ID,
	EXP_LkpValues.out_state_code,
	EXP_LkpValues.agency_state_code,
	SEQ_Agency_Underwriter.NEXTVAL,
	-- *INF*: IIF(v_changed_flag='NEW',
	-- NEXTVAL,
	-- agency_uw_ak_id)
	IFF(v_changed_flag = 'NEW', NEXTVAL, agency_uw_ak_id) AS out_Agency_uw_ak_id,
	EXP_LkpValues.out_uw_ak_id
	FROM EXP_LkpValues
	LEFT JOIN LKP_Agency_Underwriter
	ON LKP_Agency_Underwriter.insurance_line = EXP_LkpValues.insurance_line AND LKP_Agency_Underwriter.uw_assistant_flag = EXP_LkpValues.uw_assistant_flag AND LKP_Agency_Underwriter.agency_ak_id = EXP_LkpValues.out_agency_ak_id
),
FLT_Insert AS (
	SELECT
	EXP_DetectChanges.out_Agency_uw_ak_id, 
	EXP_LkpValues.insurance_line, 
	EXP_LkpValues.uw_assistant_flag, 
	EXP_DetectChanges.out_agency_key, 
	EXP_LkpValues.out_agency_ak_id, 
	EXP_DetectChanges.out_uw_ak_id, 
	EXP_DetectChanges.crrnt_snpsht_flag, 
	EXP_DetectChanges.audit_id, 
	EXP_DetectChanges.eff_from_date, 
	EXP_DetectChanges.eff_to_date, 
	EXP_DetectChanges.changed_flag, 
	EXP_DetectChanges.created_date, 
	EXP_DetectChanges.modified_date, 
	EXP_DetectChanges.SOURCE_SYSTEM_ID, 
	EXP_DetectChanges.out_state_code, 
	EXP_DetectChanges.agency_state_code
	FROM EXP_DetectChanges
	 -- Manually join with EXP_LkpValues
	WHERE (out_state_code= agency_state_code) and (changed_flag = 'NEW' or changed_flag = 'UPDATE')
),
agency_underwriter_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.agency_underwriter
	(agency_uw_ak_id, agency_ak_id, uw_ak_id, insurance_line, uw_assistant_flag, agency_key, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_system_id, created_date, modified_date)
	SELECT 
	out_Agency_uw_ak_id AS AGENCY_UW_AK_ID, 
	out_agency_ak_id AS AGENCY_AK_ID, 
	out_uw_ak_id AS UW_AK_ID, 
	INSURANCE_LINE, 
	UW_ASSISTANT_FLAG, 
	out_agency_key AS AGENCY_KEY, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FLT_Insert
),
SQ_agency_underwriter AS (
	SELECT a.agency_uw_id, 
	a.agency_ak_id,
	a.insurance_line,
	a.uw_assistant_flag, 
	a.eff_from_date, 
	a.eff_to_date 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.Agency_Underwriter a
	WHERE EXISTS(SELECT 1	from		
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.Agency_Underwriter b
		WHERE eff_to_date = '12/31/2100 23:59:59'
	     and a.agency_ak_id = b.agency_ak_id
		and a.insurance_line = b.insurance_line
		AND a.uw_assistant_flag = b.uw_assistant_flag
		GROUP BY agency_ak_id,
		    	insurance_line,
			 uw_assistant_flag
			HAVING COUNT(*) > 1)
	ORDER BY
		agency_ak_id,
	 	insurance_line,
		uw_assistant_flag,
		eff_from_date  DESC
	
	
	--ORDER BY clause is always the AK.  When any comments exist in the SQL override Informatica will no longer generate an ORDER BY statemen
),
EXP_Lag_eff_from_date AS (
	SELECT
	agency_uw_id,
	insurance_line,
	uw_assistant_flag,
	agency_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE (TRUE, 
	-- 	insurance_line = v_Prev_Row_insurance_line and
	-- 	uw_assistant_flag = v_Prev_Row_uw_assistant_flag and
	-- 	agency_ak_id = v_Prev_Row_agency_ak_id,
	-- 	 ADD_TO_DATE(v_Prev_Row_Eff_From_Date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 
	-- 
	-- 
	DECODE(TRUE,
	insurance_line = v_Prev_Row_insurance_line AND uw_assistant_flag = v_Prev_Row_uw_assistant_flag AND agency_ak_id = v_Prev_Row_agency_ak_id, ADD_TO_DATE(v_Prev_Row_Eff_From_Date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_Prev_Row_Eff_From_Date,
	agency_ak_id AS v_Prev_Row_agency_ak_id,
	insurance_line AS v_Prev_Row_insurance_line,
	uw_assistant_flag AS v_Prev_Row_uw_assistant_flag,
	0 AS crrnt_snpsht_flag,
	sysdate AS modified_date
	FROM SQ_agency_underwriter
),
FLT_FirstRowInAKGroup1 AS (
	SELECT
	agency_uw_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_Agency_underwriter AS (
	SELECT
	agency_uw_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FLT_FirstRowInAKGroup1
),
agency_underwriter_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.agency_underwriter AS T
	USING UPD_Agency_underwriter AS S
	ON T.agency_uw_id = S.agency_uw_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),