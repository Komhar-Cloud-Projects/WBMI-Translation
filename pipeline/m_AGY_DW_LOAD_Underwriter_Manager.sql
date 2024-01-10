WITH
SQ_underwriter_mgr_stage AS (
	SELECT
		uw_mgr_stage_id,
		uw_mgr_id,
		uw_mgr_first_name,
		uw_mgr_middle_name,
		uw_mgr_last_name,
		uw_mgr_suffix,
		routing_station,
		EXTRACT_DATE,
		AS_OF_DATE,
		RECORD_COUNT,
		SOURCE_SYSTEM_ID
	FROM underwriter_mgr_stage
),
EXP_DefaultValues AS (
	SELECT
	uw_mgr_id AS in_uw_mgr_id,
	uw_mgr_first_name AS in_uw_mgr_first_name,
	uw_mgr_middle_name AS in_uw_mgr_middle_name,
	uw_mgr_last_name AS in_uw_mgr_last_name,
	uw_mgr_suffix AS in_uw_mgr_suffix,
	routing_station AS in_routing_station,
	-- *INF*: iif(isnull(in_uw_mgr_id),'N/A',iif(IS_SPACES(in_uw_mgr_id),'N/A',in_uw_mgr_id))
	IFF(in_uw_mgr_id IS NULL,
		'N/A',
		IFF(LENGTH(in_uw_mgr_id)>0 AND TRIM(in_uw_mgr_id)='',
			'N/A',
			in_uw_mgr_id
		)
	) AS uw_mgr_id,
	-- *INF*: iif(isnull(in_uw_mgr_first_name),'N/A',iif(IS_SPACES(in_uw_mgr_first_name),'N/A',in_uw_mgr_first_name))
	IFF(in_uw_mgr_first_name IS NULL,
		'N/A',
		IFF(LENGTH(in_uw_mgr_first_name)>0 AND TRIM(in_uw_mgr_first_name)='',
			'N/A',
			in_uw_mgr_first_name
		)
	) AS uw_mgr_first_name,
	-- *INF*: iif(isnull(in_uw_mgr_middle_name),'N/A',iif(IS_SPACES(in_uw_mgr_middle_name),'N/A',in_uw_mgr_middle_name))
	IFF(in_uw_mgr_middle_name IS NULL,
		'N/A',
		IFF(LENGTH(in_uw_mgr_middle_name)>0 AND TRIM(in_uw_mgr_middle_name)='',
			'N/A',
			in_uw_mgr_middle_name
		)
	) AS uw_mgr_middle_name,
	-- *INF*: iif(isnull(in_uw_mgr_last_name),'N/A',iif(IS_SPACES(in_uw_mgr_last_name),'N/A',in_uw_mgr_last_name))
	IFF(in_uw_mgr_last_name IS NULL,
		'N/A',
		IFF(LENGTH(in_uw_mgr_last_name)>0 AND TRIM(in_uw_mgr_last_name)='',
			'N/A',
			in_uw_mgr_last_name
		)
	) AS uw_mgr_last_name,
	-- *INF*: iif(isnull(in_uw_mgr_suffix),'N/A',iif(IS_SPACES(in_uw_mgr_suffix),'N/A',in_uw_mgr_suffix))
	IFF(in_uw_mgr_suffix IS NULL,
		'N/A',
		IFF(LENGTH(in_uw_mgr_suffix)>0 AND TRIM(in_uw_mgr_suffix)='',
			'N/A',
			in_uw_mgr_suffix
		)
	) AS uw_mgr_suffix,
	-- *INF*: iif(isnull(in_routing_station),'N/A',iif(IS_SPACES(in_routing_station),'N/A',in_routing_station))
	IFF(in_routing_station IS NULL,
		'N/A',
		IFF(LENGTH(in_routing_station)>0 AND TRIM(in_routing_station)='',
			'N/A',
			in_routing_station
		)
	) AS routing_station,
	SOURCE_SYSTEM_ID
	FROM SQ_underwriter_mgr_stage
),
LKP_underwriter_Manager AS (
	SELECT
	uw_mgr_id,
	uw_mgr_ak_id,
	source_uw_mgr_id,
	uw_mgr_first_name,
	uw_mgr_mid_name,
	uw_mgr_last_name,
	uw_mgr_sfx,
	routing_station
	FROM (
		SELECT a.uw_mgr_id as uw_mgr_id, 
		a.uw_mgr_ak_id as uw_mgr_ak_id,
		a.uw_mgr_first_name as uw_mgr_first_name, a.uw_mgr_mid_name as uw_mgr_mid_name, a.uw_mgr_last_name as uw_mgr_last_name, 
		a.uw_mgr_sfx as uw_mgr_sfx, 
		a.routing_station as routing_station, 
		a.source_uw_mgr_id as source_uw_mgr_id FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.underwriter_manager a
		WHERE  a.uw_mgr_id IN (SELECT MAX(b.uw_mgr_id)
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.underwriter_manager b
			WHERE crrnt_snpsht_flag=1
			GROUP BY b.source_uw_mgr_id)
		ORDER BY source_uw_mgr_id
		
		--IN Subquery exists so that we only pick the MAX value of the PK for each AK Group
		--WHERE clause is always eff_to_date = '12/31/2100'
		--GROUP BY clause is always the AK
		--ORDER BY clause is always the AK.  When any comments exist in the SQL override Informatica will no longer generate an ORDER BY statemen
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY source_uw_mgr_id ORDER BY uw_mgr_id) = 1
),
SEQ_Underwriter_Manager AS (
	CREATE SEQUENCE SEQ_Underwriter_Manager
	START = 0
	INCREMENT = 1;
),
EXP_DetectChanges AS (
	SELECT
	EXP_DefaultValues.uw_mgr_id AS source_uw_mgr_id,
	EXP_DefaultValues.uw_mgr_first_name,
	EXP_DefaultValues.uw_mgr_middle_name,
	EXP_DefaultValues.uw_mgr_last_name,
	EXP_DefaultValues.uw_mgr_suffix,
	EXP_DefaultValues.routing_station,
	LKP_underwriter_Manager.uw_mgr_id AS uw_mgr_id_old,
	LKP_underwriter_Manager.uw_mgr_ak_id,
	LKP_underwriter_Manager.source_uw_mgr_id AS source_uw_mgr_id_old,
	LKP_underwriter_Manager.uw_mgr_first_name AS uw_mgr_first_name_old,
	LKP_underwriter_Manager.uw_mgr_mid_name AS uw_mgr_mid_name_old,
	LKP_underwriter_Manager.uw_mgr_last_name AS uw_mgr_last_name_old,
	LKP_underwriter_Manager.uw_mgr_sfx AS uw_mgr_sfx_old,
	LKP_underwriter_Manager.routing_station AS routing_station_old,
	-- *INF*: iif(isnull(uw_mgr_id_old),'NEW',
	-- 	iif((source_uw_mgr_id <> source_uw_mgr_id_old) or
	-- 	(uw_mgr_first_name <> uw_mgr_first_name_old ) or
	-- 	(uw_mgr_middle_name <> uw_mgr_mid_name_old) or
	-- 	(uw_mgr_last_name <> uw_mgr_last_name_old) or
	-- 	(uw_mgr_suffix <> uw_mgr_sfx_old ) or
	-- 	(routing_station <> routing_station_old ) ,
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(uw_mgr_id_old IS NULL,
		'NEW',
		IFF(( source_uw_mgr_id <> source_uw_mgr_id_old 
			) 
			OR ( uw_mgr_first_name <> uw_mgr_first_name_old 
			) 
			OR ( uw_mgr_middle_name <> uw_mgr_mid_name_old 
			) 
			OR ( uw_mgr_last_name <> uw_mgr_last_name_old 
			) 
			OR ( uw_mgr_suffix <> uw_mgr_sfx_old 
			) 
			OR ( routing_station <> routing_station_old 
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	-- 
	-- --sysdate normally has a time value.  We don't want the time value as our effectivity runs from day to day starting at midnight
	IFF(v_changed_flag = 'NEW',
		to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		sysdate
	) AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	v_changed_flag AS changed_flag,
	sysdate AS created_date,
	sysdate AS modified_date,
	EXP_DefaultValues.SOURCE_SYSTEM_ID,
	SEQ_Underwriter_Manager.NEXTVAL,
	-- *INF*: IIF(v_changed_flag='NEW',
	-- NEXTVAL,
	-- uw_mgr_ak_id)
	IFF(v_changed_flag = 'NEW',
		NEXTVAL,
		uw_mgr_ak_id
	) AS out_uw_mgr_ak_id
	FROM EXP_DefaultValues
	LEFT JOIN LKP_underwriter_Manager
	ON LKP_underwriter_Manager.source_uw_mgr_id = EXP_DefaultValues.uw_mgr_id
),
FLT_Insert AS (
	SELECT
	out_uw_mgr_ak_id, 
	source_uw_mgr_id AS uw_mgr_id, 
	uw_mgr_first_name, 
	uw_mgr_middle_name, 
	uw_mgr_last_name, 
	uw_mgr_suffix, 
	routing_station, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	changed_flag, 
	SOURCE_SYSTEM_ID, 
	created_date, 
	modified_date
	FROM EXP_DetectChanges
	WHERE changed_flag='NEW' or changed_flag='UPDATE'
),
underwriter_manager_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.underwriter_manager
	(uw_mgr_ak_id, source_uw_mgr_id, uw_mgr_first_name, uw_mgr_mid_name, uw_mgr_last_name, uw_mgr_sfx, routing_station, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_system_id, created_date, modified_date)
	SELECT 
	out_uw_mgr_ak_id AS UW_MGR_AK_ID, 
	uw_mgr_id AS SOURCE_UW_MGR_ID, 
	UW_MGR_FIRST_NAME, 
	uw_mgr_middle_name AS UW_MGR_MID_NAME, 
	UW_MGR_LAST_NAME, 
	uw_mgr_suffix AS UW_MGR_SFX, 
	ROUTING_STATION, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FLT_Insert
),
SQ_underwriter_manager AS (
	SELECT a.uw_mgr_id, 
	a.source_uw_mgr_id, a.eff_from_date, a.eff_to_date 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.underwriter_manager a 
	WHERE EXISTS(SELECT 1			
		FROM  @{pipeline().parameters.SOURCE_TABLE_OWNER}.underwriter_manager  b
		WHERE eff_to_date = '12/31/2100 23:59:59'
		AND a.source_uw_mgr_id = b.source_uw_mgr_id
		GROUP BY source_uw_mgr_id
		HAVING COUNT(*) > 1)
	ORDER BY source_uw_mgr_id, eff_from_date  DESC
	
	
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
	uw_mgr_id,
	source_uw_mgr_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- source_uw_mgr_id = v_PREV_ROW_source_uw_mgr_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 
	DECODE(TRUE,
		source_uw_mgr_id = v_PREV_ROW_source_uw_mgr_id, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	source_uw_mgr_id AS v_PREV_ROW_source_uw_mgr_id,
	0 AS crrnt_snpsht_flag,
	sysdate AS modified_date
	FROM SQ_underwriter_manager
),
FIL_FirstRowInAKGroup AS (
	SELECT
	uw_mgr_id, 
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
	uw_mgr_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_FirstRowInAKGroup
),
underwriter_manager_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.underwriter_manager AS T
	USING UPD_underwriter AS S
	ON T.uw_mgr_id = S.uw_mgr_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),