WITH
Agency_address_Stage AS (
	SELECT
		agency_address_stage_id,
		STATE_CODE,
		AGENCY_NUM,
		ADDRESS_TYPE,
		ADDRESS_LINE_1,
		ADDRESS_LINE_2,
		ADDRESS_LINE_3,
		CITY,
		POSTAL_CODE,
		ZIP_PLUS_4,
		COUNTY,
		STATE_ABBREV,
		COUNTRY,
		AGENCY_CODE,
		COUNTY_LOCATION,
		EXTRACT_DATE,
		AS_OF_DATE,
		RECORD_COUNT,
		SOURCE_SYSTEM_ID
	FROM Agency_address_Stage
	INNER JOIN Agency_address_Stage
),
EXP_AGY_address_convert AS (
	SELECT
	agency_address_stage_id,
	STATE_CODE AS IN_STATE_CODE,
	AGENCY_NUM AS IN_AGENCY_NUM,
	ADDRESS_TYPE AS in_ADDRESS_TYPE,
	ADDRESS_LINE_1,
	ADDRESS_LINE_2,
	ADDRESS_LINE_3,
	CITY AS IN_CITY,
	POSTAL_CODE AS IN_POSTAL_CODE,
	ZIP_PLUS_4 AS IN_ZIP_PLUS_4,
	COUNTY AS IN_COUNTY,
	STATE_ABBREV AS IN_STATE_ABBREV,
	COUNTRY AS IN_COUNTRY,
	AGENCY_CODE AS IN_AGENCY_CODE,
	COUNTY_LOCATION AS IN_COUNTY_LOCATION,
	-- *INF*: iif(isnull(IN_STATE_CODE),'N/A',iif(IS_SPACES(IN_STATE_CODE),'N/A',IN_STATE_CODE))
	IFF(IN_STATE_CODE IS NULL, 'N/A', IFF(IS_SPACES(IN_STATE_CODE), 'N/A', IN_STATE_CODE)) AS STATE_CODE,
	-- *INF*: iif(isnull(IN_AGENCY_NUM),'N/A',iif(IS_SPACES(IN_AGENCY_NUM),'N/A',IN_AGENCY_NUM))
	IFF(IN_AGENCY_NUM IS NULL, 'N/A', IFF(IS_SPACES(IN_AGENCY_NUM), 'N/A', IN_AGENCY_NUM)) AS AGENCY_NUM,
	-- *INF*: iif(isnull(in_ADDRESS_TYPE),'N/A',
	-- iif(IS_SPACES(in_ADDRESS_TYPE),'N/A',
	-- rpad(in_ADDRESS_TYPE,3)))
	IFF(in_ADDRESS_TYPE IS NULL, 'N/A', IFF(IS_SPACES(in_ADDRESS_TYPE), 'N/A', rpad(in_ADDRESS_TYPE, 3))) AS ADDRESS_TYPE,
	-- *INF*: iif(isnull(IN_CITY),'Not Available',iif(IS_SPACES(IN_CITY),'Not Available',IN_CITY))
	IFF(IN_CITY IS NULL, 'Not Available', IFF(IS_SPACES(IN_CITY), 'Not Available', IN_CITY)) AS CITY,
	-- *INF*: iif(isnull(IN_POSTAL_CODE),'N/A',iif(IS_SPACES(IN_POSTAL_CODE),'N/A',
	-- iif(length(IN_POSTAL_CODE)=0,'N/A',IN_POSTAL_CODE)))
	IFF(IN_POSTAL_CODE IS NULL, 'N/A', IFF(IS_SPACES(IN_POSTAL_CODE), 'N/A', IFF(length(IN_POSTAL_CODE) = 0, 'N/A', IN_POSTAL_CODE))) AS POSTAL_CODE,
	-- *INF*: iif(isnull(IN_ZIP_PLUS_4),'N/A',iif(IS_SPACES(IN_ZIP_PLUS_4),'N/A',IN_ZIP_PLUS_4))
	IFF(IN_ZIP_PLUS_4 IS NULL, 'N/A', IFF(IS_SPACES(IN_ZIP_PLUS_4), 'N/A', IN_ZIP_PLUS_4)) AS ZIP_PLUS_4,
	-- *INF*: iif(isnull(IN_COUNTY),'Not Available',iif(IS_SPACES(IN_COUNTY),'Not Available',IN_COUNTY))
	IFF(IN_COUNTY IS NULL, 'Not Available', IFF(IS_SPACES(IN_COUNTY), 'Not Available', IN_COUNTY)) AS COUNTY,
	-- *INF*: iif(isnull(IN_STATE_ABBREV),'N/A',iif(IS_SPACES(IN_STATE_ABBREV),'N/A',rpad(IN_STATE_ABBREV,3)))
	IFF(IN_STATE_ABBREV IS NULL, 'N/A', IFF(IS_SPACES(IN_STATE_ABBREV), 'N/A', rpad(IN_STATE_ABBREV, 3))) AS STATE_ABBREV,
	-- *INF*: iif(isnull(IN_COUNTRY),'Not Available',iif(IS_SPACES(IN_COUNTRY),'Not Available',IN_COUNTRY))
	IFF(IN_COUNTRY IS NULL, 'Not Available', IFF(IS_SPACES(IN_COUNTRY), 'Not Available', IN_COUNTRY)) AS COUNTRY,
	-- *INF*: iif(isnull(IN_AGENCY_CODE),'N/A',iif(IS_SPACES(IN_AGENCY_CODE),'N/A',IN_AGENCY_CODE))
	IFF(IN_AGENCY_CODE IS NULL, 'N/A', IFF(IS_SPACES(IN_AGENCY_CODE), 'N/A', IN_AGENCY_CODE)) AS AGENCY_CODE,
	-- *INF*: iif(isnull(IN_COUNTY_LOCATION),'Not Available',iif(IS_SPACES(IN_COUNTY_LOCATION),'Not Available',
	-- iif(length(IN_COUNTY_LOCATION)=0,'Not Available',IN_COUNTY_LOCATION)))
	IFF(IN_COUNTY_LOCATION IS NULL, 'Not Available', IFF(IS_SPACES(IN_COUNTY_LOCATION), 'Not Available', IFF(length(IN_COUNTY_LOCATION) = 0, 'Not Available', IN_COUNTY_LOCATION))) AS COUNTY_LOCATION,
	SOURCE_SYSTEM_ID,
	-- *INF*: iif(isnull
	-- ((ADDRESS_LINE_1 || ADDRESS_LINE_2 || ADDRESS_LINE_3)),'Not Available',
	-- iif(is_spaces((ADDRESS_LINE_1 || ADDRESS_LINE_2 || ADDRESS_LINE_3)),'Not Available',
	-- (ADDRESS_LINE_1 || ADDRESS_LINE_2 || ADDRESS_LINE_3)
	-- ))
	IFF(( ADDRESS_LINE_1 || ADDRESS_LINE_2 || ADDRESS_LINE_3 ) IS NULL, 'Not Available', IFF(is_spaces(( ADDRESS_LINE_1 || ADDRESS_LINE_2 || ADDRESS_LINE_3 )), 'Not Available', ( ADDRESS_LINE_1 || ADDRESS_LINE_2 || ADDRESS_LINE_3 ))) AS OUT_AGENCY_ADDRESS,
	IN_STATE_CODE || IN_AGENCY_NUM AS v_agency_key,
	-- *INF*: iif(isnull(STATE_CODE || AGENCY_NUM),'N/A',
	-- iif(is_spaces(STATE_CODE || AGENCY_NUM),'N/A',
	-- STATE_CODE || AGENCY_NUM))
	IFF(STATE_CODE || AGENCY_NUM IS NULL, 'N/A', IFF(is_spaces(STATE_CODE || AGENCY_NUM), 'N/A', STATE_CODE || AGENCY_NUM)) AS OUT_AGENCY_KEY
	FROM Agency_address_Stage
),
LKP_Agency AS (
	SELECT
	agency_ak_id,
	agency_state_code,
	agency_key
	FROM (
		SELECT a.agency_ak_id as agency_ak_id, 
		a.agency_state_code as agency_state_code, 
		a.agency_key as agency_key
		 FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.agency a
		where crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY agency_key ORDER BY agency_ak_id) = 1
),
LKP_Agency_Address AS (
	SELECT
	agency_address_id,
	agency_addr_ak_id,
	agency_ak_id,
	agency_address,
	city,
	postal_code,
	zip_plus_4,
	county,
	state_abbrev,
	country,
	county_location,
	agency_key,
	address_type
	FROM (
		SELECT a.agency_address_id as agency_address_id, 
		a.agency_addr_ak_id as agency_addr_ak_id,
		a.agency_address as agency_address, 
		a.city as city,
		a.postal_code as postal_code, 
		a.zip_plus_4 as zip_plus_4, 
		a.county as county, 
		a.state_abbrev as state_abbrev, 
		a.country as country, 
		a.county_location as county_location, 
		a.agency_ak_id as agency_ak_id, 
		a.address_type as address_type, 
		a.agency_key as agency_key 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.agency_address a
		WHERE a.agency_address_id IN(SELECT MAX(b.agency_address_id)
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.agency_address b
		where crrnt_snpsht_flag=1
		group by b.address_type,b.agency_key)
		order by address_type,agency_key
		
		
		--IN Subquery exists so that we only pick the MAX value of the PK for each AK Group
		--WHERE clause is always eff_to_date = '12/31/2100'
		--GROUP BY clause is always the AK
		--ORDER BY clause is always the AK.  When any comments exist in the SQL override Informatica will no longer generate an ORDER BY statement
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY agency_key,address_type ORDER BY agency_address_id) = 1
),
SEQ_Agency_Address AS (
	CREATE SEQUENCE SEQ_Agency_Address
	START = 0
	INCREMENT = 1;
),
EXP_Load_AGY_Address AS (
	SELECT
	EXP_AGY_address_convert.ADDRESS_TYPE,
	EXP_AGY_address_convert.CITY,
	EXP_AGY_address_convert.POSTAL_CODE,
	EXP_AGY_address_convert.ZIP_PLUS_4 AS in_zip_plus_4,
	-- *INF*: rpad(in_zip_plus_4,4)
	rpad(in_zip_plus_4, 4) AS v_zip_plus_4,
	v_zip_plus_4 AS ZIP_PLUS_4,
	EXP_AGY_address_convert.COUNTY,
	EXP_AGY_address_convert.STATE_ABBREV,
	EXP_AGY_address_convert.COUNTRY,
	EXP_AGY_address_convert.COUNTY_LOCATION,
	EXP_AGY_address_convert.OUT_AGENCY_ADDRESS,
	LKP_Agency.agency_ak_id AS OUT_AGENCY_ak_ID,
	EXP_AGY_address_convert.OUT_AGENCY_KEY,
	LKP_Agency_Address.agency_address_id,
	LKP_Agency_Address.agency_addr_ak_id,
	LKP_Agency_Address.city AS OLD_city,
	LKP_Agency_Address.postal_code AS OLD_postal_code,
	LKP_Agency_Address.zip_plus_4 AS OLD_zip_plus_4,
	LKP_Agency_Address.county AS OLD_county,
	LKP_Agency_Address.state_abbrev AS OLD_state_abbrev,
	LKP_Agency_Address.country AS OLD_country,
	LKP_Agency_Address.county_location AS OLD_county_location,
	LKP_Agency_Address.agency_address AS OLD_agency_address,
	LKP_Agency_Address.agency_ak_id AS OLD_agency_ak_id,
	-- *INF*: iif(isnull(agency_address_id),'NEW',
	-- 	iif(	(CITY <> OLD_city) or
	-- 	(POSTAL_CODE <> OLD_postal_code) or
	-- 	( v_zip_plus_4 <> OLD_zip_plus_4) or
	--        ( COUNTY<> OLD_county) or
	-- 	( STATE_ABBREV<> OLD_state_abbrev) or
	-- 	( COUNTRY<> OLD_country) or
	-- 	(COUNTY_LOCATION <> OLD_county_location) or 
	-- 	(OUT_AGENCY_ADDRESS <> OLD_agency_address) or
	-- 	(OUT_AGENCY_ak_ID <> OLD_agency_ak_id),
	-- 		'UPDATE',
	-- 	'NOCHANGE'))
	IFF(agency_address_id IS NULL, 'NEW', IFF(( CITY <> OLD_city ) OR ( POSTAL_CODE <> OLD_postal_code ) OR ( v_zip_plus_4 <> OLD_zip_plus_4 ) OR ( COUNTY <> OLD_county ) OR ( STATE_ABBREV <> OLD_state_abbrev ) OR ( COUNTRY <> OLD_country ) OR ( COUNTY_LOCATION <> OLD_county_location ) OR ( OUT_AGENCY_ADDRESS <> OLD_agency_address ) OR ( OUT_AGENCY_ak_ID <> OLD_agency_ak_id ), 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	1 AS Crrnt_SnapSht_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_id,
	EXP_AGY_address_convert.SOURCE_SYSTEM_ID,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	-- 
	-- --sysdate normally has a time value.  We don't want the time value as our effectivity runs from day to day starting at midnight
	IFF(v_changed_flag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS Eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS Eff_to_date,
	sysdate AS Created_date,
	sysdate AS Modified_date,
	v_changed_flag AS changed_flag,
	EXP_AGY_address_convert.IN_STATE_CODE,
	-- *INF*: RPAD(IN_STATE_CODE,3)
	RPAD(IN_STATE_CODE, 3) AS out_state_code,
	SEQ_Agency_Address.NEXTVAL,
	-- *INF*: IIF(v_changed_flag='NEW',
	-- NEXTVAL,
	-- agency_addr_ak_id)
	IFF(v_changed_flag = 'NEW', NEXTVAL, agency_addr_ak_id) AS out_agency_addr_ak_id
	FROM EXP_AGY_address_convert
	LEFT JOIN LKP_Agency
	ON LKP_Agency.agency_key = EXP_AGY_address_convert.OUT_AGENCY_KEY
	LEFT JOIN LKP_Agency_Address
	ON LKP_Agency_Address.agency_key = EXP_AGY_address_convert.OUT_AGENCY_KEY AND LKP_Agency_Address.address_type = EXP_AGY_address_convert.ADDRESS_TYPE
),
FIL_AGY_Address_Insert AS (
	SELECT
	EXP_Load_AGY_Address.out_agency_addr_ak_id, 
	EXP_Load_AGY_Address.ADDRESS_TYPE, 
	EXP_Load_AGY_Address.OUT_AGENCY_ADDRESS, 
	EXP_Load_AGY_Address.CITY, 
	EXP_Load_AGY_Address.POSTAL_CODE, 
	EXP_Load_AGY_Address.ZIP_PLUS_4, 
	EXP_Load_AGY_Address.COUNTY, 
	EXP_Load_AGY_Address.STATE_ABBREV, 
	EXP_Load_AGY_Address.COUNTRY, 
	EXP_Load_AGY_Address.OUT_AGENCY_KEY, 
	EXP_Load_AGY_Address.COUNTY_LOCATION, 
	EXP_Load_AGY_Address.OUT_AGENCY_ak_ID, 
	EXP_Load_AGY_Address.Crrnt_SnapSht_Flag AS Crrnt_SnpSht_Flag, 
	EXP_Load_AGY_Address.Audit_id, 
	EXP_Load_AGY_Address.Eff_from_date, 
	EXP_Load_AGY_Address.Eff_to_date, 
	EXP_Load_AGY_Address.SOURCE_SYSTEM_ID, 
	EXP_Load_AGY_Address.Created_date, 
	EXP_Load_AGY_Address.Modified_date, 
	EXP_Load_AGY_Address.changed_flag, 
	EXP_Load_AGY_Address.out_state_code AS IN_STATE_CODE, 
	LKP_Agency.agency_state_code
	FROM EXP_Load_AGY_Address
	LEFT JOIN LKP_Agency
	ON LKP_Agency.agency_key = EXP_AGY_address_convert.OUT_AGENCY_KEY
	WHERE (IN_STATE_CODE= agency_state_code) and 
(changed_flag = 'NEW' or changed_flag = 'UPDATE') and (OUT_AGENCY_ADDRESS <> 'Not Available')
),
Agency_address_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.agency_address
	(agency_addr_ak_id, agency_ak_id, address_type, agency_address, city, postal_code, zip_plus_4, county, state_abbrev, country, agency_key, county_location, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_system_id, created_date, modified_date)
	SELECT 
	out_agency_addr_ak_id AS AGENCY_ADDR_AK_ID, 
	OUT_AGENCY_ak_ID AS AGENCY_AK_ID, 
	ADDRESS_TYPE AS ADDRESS_TYPE, 
	OUT_AGENCY_ADDRESS AS AGENCY_ADDRESS, 
	CITY AS CITY, 
	POSTAL_CODE AS POSTAL_CODE, 
	ZIP_PLUS_4 AS ZIP_PLUS_4, 
	COUNTY AS COUNTY, 
	STATE_ABBREV AS STATE_ABBREV, 
	COUNTRY AS COUNTRY, 
	OUT_AGENCY_KEY AS AGENCY_KEY, 
	COUNTY_LOCATION AS COUNTY_LOCATION, 
	Crrnt_SnpSht_Flag AS CRRNT_SNPSHT_FLAG, 
	Audit_id AS AUDIT_ID, 
	Eff_from_date AS EFF_FROM_DATE, 
	Eff_to_date AS EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	Created_date AS CREATED_DATE, 
	Modified_date AS MODIFIED_DATE
	FROM FIL_AGY_Address_Insert
),
Agency_address_upd AS (
	SELECT a.agency_address_id,
	a.address_type, 
	a.agency_key, 
	a.eff_from_date, 
	a.eff_to_date 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.agency_address a
	WHERE EXISTS(SELECT 1			
		FROM  @{pipeline().parameters.SOURCE_TABLE_OWNER}.Agency_Address b
		WHERE eff_to_date = '12/31/2100 23:59:59'
		AND a.address_type = b.address_type
		and a.agency_key = b.agency_key
		GROUP BY address_type,agency_key
		HAVING COUNT(*) > 1)
	ORDER BY address_type, agency_key,eff_from_date  DESC
	
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of eff_to_date='12/31/2100' and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK and then by the eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the
	--eff_to_date by utilizing a local variable to keep track
),
EXP_Agy_Address_Upd_desc AS (
	SELECT
	agency_address_id,
	address_type,
	agency_key,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE (TRUE, address_type = v_PREV_ROW_address_type and agency_key = v_PREV_ROW_agency_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
	address_type = v_PREV_ROW_address_type AND agency_key = v_PREV_ROW_agency_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	address_type AS v_PREV_ROW_address_type,
	agency_key AS v_PREV_ROW_agency_key,
	0 AS Crrnt_Snpsht_flag,
	sysdate AS modified_date
	FROM Agency_address_upd
),
FIL_Agy_address_upd AS (
	SELECT
	agency_address_id, 
	orig_eff_to_date, 
	eff_to_date, 
	Crrnt_Snpsht_flag, 
	modified_date
	FROM EXP_Agy_Address_Upd_desc
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_Agy_Address AS (
	SELECT
	agency_address_id, 
	eff_to_date, 
	Crrnt_Snpsht_flag, 
	modified_date
	FROM FIL_Agy_address_upd
),
agency_address_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.agency_address AS T
	USING UPD_Agy_Address AS S
	ON T.agency_address_id = S.agency_address_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.Crrnt_Snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),