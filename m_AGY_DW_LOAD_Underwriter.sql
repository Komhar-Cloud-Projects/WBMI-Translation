WITH
SQ_underwriter_stage AS (
	SELECT
		uw_stage_id,
		uw_code,
		uw_first_name,
		uw_middle_name,
		uw_last_name,
		uw_suffix,
		uw_extension,
		routing_station,
		emp_id,
		EXTRACT_DATE,
		AS_OF_DATE,
		RECORD_COUNT,
		SOURCE_SYSTEM_ID
	FROM underwriter_stage
),
EXP_defaultvalues AS (
	SELECT
	uw_code AS in_uw_code,
	uw_first_name AS in_uw_first_name,
	uw_middle_name AS in_uw_middle_name,
	uw_last_name AS in_uw_last_name,
	uw_suffix AS in_uw_suffix,
	uw_extension AS in_uw_extension,
	routing_station AS in_routing_station,
	-- *INF*: iif(isnull(in_uw_code),'N/A',iif(IS_SPACES(in_uw_code),'N/A',in_uw_code))
	IFF(in_uw_code IS NULL, 'N/A', IFF(IS_SPACES(in_uw_code), 'N/A', in_uw_code)) AS uw_code,
	-- *INF*: iif(isnull(in_uw_first_name),'N/A',iif(IS_SPACES(in_uw_first_name),'N/A',ltrim(rtrim(in_uw_first_name))))
	IFF(in_uw_first_name IS NULL, 'N/A', IFF(IS_SPACES(in_uw_first_name), 'N/A', ltrim(rtrim(in_uw_first_name)))) AS uw_first_name,
	-- *INF*: iif(isnull(in_uw_middle_name),'N/A',iif(IS_SPACES(in_uw_middle_name),'N/A',ltrim(rtrim(in_uw_middle_name))))
	IFF(in_uw_middle_name IS NULL, 'N/A', IFF(IS_SPACES(in_uw_middle_name), 'N/A', ltrim(rtrim(in_uw_middle_name)))) AS uw_middle_name,
	-- *INF*: iif(isnull(in_uw_last_name),'N/A',iif(IS_SPACES(in_uw_last_name),'N/A',ltrim(rtrim(in_uw_last_name))))
	IFF(in_uw_last_name IS NULL, 'N/A', IFF(IS_SPACES(in_uw_last_name), 'N/A', ltrim(rtrim(in_uw_last_name)))) AS uw_last_name,
	-- *INF*: iif(isnull(in_uw_suffix),'N/A',iif(IS_SPACES(in_uw_suffix),'N/A',in_uw_suffix))
	IFF(in_uw_suffix IS NULL, 'N/A', IFF(IS_SPACES(in_uw_suffix), 'N/A', in_uw_suffix)) AS uw_suffix,
	-- *INF*: iif(isnull(in_uw_extension),'N/A ',iif(IS_SPACES(in_uw_extension),'N/A ',in_uw_extension))
	IFF(in_uw_extension IS NULL, 'N/A ', IFF(IS_SPACES(in_uw_extension), 'N/A ', in_uw_extension)) AS uw_extension,
	-- *INF*: iif(isnull(in_routing_station),'N/A',iif(IS_SPACES(in_routing_station),'N/A',in_routing_station))
	IFF(in_routing_station IS NULL, 'N/A', IFF(IS_SPACES(in_routing_station), 'N/A', in_routing_station)) AS routing_station,
	SOURCE_SYSTEM_ID
	FROM SQ_underwriter_stage
),
LKP_Underwriter AS (
	SELECT
	uw_id,
	uw_ak_id,
	uw_first_name,
	uw_mid_name,
	uw_last_name,
	uw_sfx,
	uw_extension,
	routing_station,
	uw_code
	FROM (
		SELECT a.uw_id as uw_id,
		a.uw_ak_id as uw_ak_id,
		a.uw_first_name as uw_first_name, 
		a.uw_mid_name as uw_mid_name, 
		a.uw_last_name as uw_last_name, 
		a.uw_sfx as uw_sfx, 
		a.uw_extension as uw_extension,
		a.routing_station as routing_station,
		a.uw_code as uw_code 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.underwriter a
		WHERE  a.uw_id IN (SELECT MAX(b.uw_id)
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.underwriter b
			WHERE crrnt_snpsht_flag=1
			GROUP BY b.uw_code)
		ORDER BY uw_code
		
		--IN Subquery exists so that we only pick the MAX value of the PK for each AK Group
		--WHERE clause is always eff_to_date = '12/31/2100'
		--GROUP BY clause is always the AK
		--ORDER BY clause is always the AK.  When any comments exist in the SQL override Informatica will no longer generate an ORDER BY statement
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY uw_code ORDER BY uw_id) = 1
),
SEQ_Underwriter AS (
	CREATE SEQUENCE SEQ_Underwriter
	START = 0
	INCREMENT = 1;
),
EXP_DetectChanges AS (
	SELECT
	EXP_defaultvalues.uw_first_name,
	EXP_defaultvalues.uw_middle_name,
	EXP_defaultvalues.uw_last_name,
	EXP_defaultvalues.uw_suffix,
	EXP_defaultvalues.uw_extension,
	EXP_defaultvalues.routing_station,
	LKP_Underwriter.uw_id AS uw_id_old,
	LKP_Underwriter.uw_ak_id,
	LKP_Underwriter.uw_first_name AS uw_first_name_old,
	LKP_Underwriter.uw_mid_name AS uw_mid_name_old,
	LKP_Underwriter.uw_last_name AS uw_last_name_old,
	LKP_Underwriter.uw_sfx AS uw_sfx_old,
	LKP_Underwriter.uw_extension AS uw_extension_old,
	LKP_Underwriter.routing_station AS routing_station_old,
	-- *INF*: iif(isnull(uw_id_old),'NEW',
	-- 	iif((uw_first_name <> uw_first_name_old ) or
	-- 	(uw_middle_name <> uw_mid_name_old) or
	-- 	(uw_last_name <> uw_last_name_old) or
	-- 	(uw_suffix <> uw_sfx_old ) or
	-- 	(uw_extension <> uw_extension_old ) or
	-- 	(routing_station<> routing_station_old ),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(uw_id_old IS NULL, 'NEW', IFF(( uw_first_name <> uw_first_name_old ) OR ( uw_middle_name <> uw_mid_name_old ) OR ( uw_last_name <> uw_last_name_old ) OR ( uw_suffix <> uw_sfx_old ) OR ( uw_extension <> uw_extension_old ) OR ( routing_station <> routing_station_old ), 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	-- 
	-- --sysdate normally has a time value.  We don't want the time value as our effectivity runs from day to day starting at midnight
	-- 
	-- 
	-- 
	IFF(v_changed_flag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	-- 
	-- 
	-- 
	-- 
	-- 
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	sysdate AS created_date,
	sysdate AS modified_date,
	EXP_defaultvalues.SOURCE_SYSTEM_ID,
	SEQ_Underwriter.NEXTVAL,
	-- *INF*: IIF(v_changed_flag='NEW',
	-- NEXTVAL,
	-- uw_ak_id)
	IFF(v_changed_flag = 'NEW', NEXTVAL, uw_ak_id) AS out_uw_AK_ID
	FROM EXP_defaultvalues
	LEFT JOIN LKP_Underwriter
	ON LKP_Underwriter.uw_code = EXP_defaultvalues.uw_code
),
FIL_Insert AS (
	SELECT
	EXP_DetectChanges.out_uw_AK_ID, 
	EXP_defaultvalues.uw_code, 
	EXP_DetectChanges.uw_first_name, 
	EXP_DetectChanges.uw_middle_name, 
	EXP_DetectChanges.uw_last_name, 
	EXP_DetectChanges.uw_suffix, 
	EXP_DetectChanges.uw_extension, 
	EXP_DetectChanges.routing_station, 
	EXP_DetectChanges.crrnt_snpsht_flag, 
	EXP_DetectChanges.audit_id, 
	EXP_DetectChanges.eff_from_date, 
	EXP_DetectChanges.eff_to_date, 
	EXP_DetectChanges.changed_flag, 
	EXP_DetectChanges.created_date, 
	EXP_DetectChanges.modified_date, 
	EXP_DetectChanges.SOURCE_SYSTEM_ID
	FROM EXP_DetectChanges
	 -- Manually join with EXP_defaultvalues
	WHERE changed_flag='NEW' or changed_flag='UPDATE'
),
underwriter_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.underwriter
	(uw_ak_id, uw_code, uw_first_name, uw_mid_name, uw_last_name, uw_sfx, uw_extension, routing_station, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_system_id, created_date, modified_date)
	SELECT 
	out_uw_AK_ID AS UW_AK_ID, 
	UW_CODE, 
	UW_FIRST_NAME, 
	uw_middle_name AS UW_MID_NAME, 
	UW_LAST_NAME, 
	uw_suffix AS UW_SFX, 
	UW_EXTENSION, 
	ROUTING_STATION, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_Insert
),
SQ_underwriter1 AS (
	SELECT a.uw_id, 
	a.uw_code,
	 a.eff_from_date, 
	a.eff_to_date 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.underwriter a
	WHERE EXISTS(SELECT 1			
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.underwriter b
		WHERE eff_to_date = '12/31/2100 23:59:59'
		AND a.uw_code = b.uw_code
		GROUP BY UW_CODE
		HAVING COUNT(*) > 1)
	ORDER BY uw_code, eff_from_date  DESC
	
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of eff_to_date='12/31/2100' and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK and then by the eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the
	--eff_to_date by utilizing a local variable to keep track of the eff_from date of the previous row and then ultimately
	--utilize that value minus 1 day for the next row
),
EXP_Lag_eff_from_date AS (
	SELECT
	uw_id,
	uw_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	uw_code = v_PREV_ROW_uw_code, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(TRUE,
	uw_code = v_PREV_ROW_uw_code, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	uw_code AS v_PREV_ROW_uw_code,
	0 AS crrnt_snpsht_flag,
	sysdate AS modified_date
	FROM SQ_underwriter1
),
FIL_FirstRowInAKGroup AS (
	SELECT
	uw_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <> eff_to_date

--If these two dates equal each other we are dealing with the first row in an AK group.  This row
--does not need to be expired or updated for any reason thus it can be filtered out
-- but we must source it to capture the eff_from_date of this row 
--so that we can properly expire the subsequent row
),
UPD_underwriter AS (
	SELECT
	uw_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_FirstRowInAKGroup
),
underwriter_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.underwriter AS T
	USING UPD_underwriter AS S
	ON T.uw_id = S.uw_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),