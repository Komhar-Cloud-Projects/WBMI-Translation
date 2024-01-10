WITH
SQ_DCLocationStaging AS (
	SELECT WorkDCTPolicy.SessionId, WorkDCTLocation.LocationId, WorkDCTLocation.Address1, WorkDCTLocation.Address2, WorkDCTLocation.City, WorkDCTLocation.County, WorkDCTLocation.StateProvince, WorkDCTLocation.PostalCode, WorkDCTLocation.Country, WorkDCTPolicy.PolicyGUId, WorkDCTPolicy.PolicyVersion, WorkDCTPolicy.CustomerNum ,WorkDCTPolicy.PolicyNumber 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy 
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTLocation
	on
	WorkDCTLocation.SessionId=WorkDCTPolicy.SessionId
	and
	WorkDCTLocation.LocationAssociationType='Account' 
	WHERE
	WorkDCTPolicy.PolicyStatus<>'Quote'
	and
	WorkDCTPolicy.TransactionState='committed'
	ORDER BY
	WorkDCTPolicy.SessionId,WorkDCTLocation.LocationId DESC
),
AGG_Remove_Duplicates AS (
	SELECT
	SessionId AS i_SessionId,
	LocationId AS i_LocationId,
	Address1 AS i_Address1,
	Address2 AS i_Address2,
	City AS i_City,
	County AS i_County,
	StateProvince AS i_StateProv,
	PostalCode AS i_PostalCode,
	Country AS i_Country,
	PolicyGUId AS i_Id,
	PolicyVersion AS i_PolicyVersion,
	CustomerNum,
	PolicyNumber AS i_PolicyNumber,
	i_SessionId AS o_SessionId,
	i_LocationId AS o_LocationId,
	i_Address1 AS o_Address1,
	i_Address2 AS o_Address2,
	i_City AS o_City,
	i_County AS o_County,
	i_StateProv AS o_StateProv,
	i_PostalCode AS o_PostalCode,
	i_Country AS o_Country,
	-- *INF*: IIF(ISNULL(CustomerNum) or IS_SPACES(CustomerNum) or LENGTH(CustomerNum)=0, 'N/A', LTRIM(RTRIM(CustomerNum)))
	IFF(CustomerNum IS NULL OR IS_SPACES(CustomerNum) OR LENGTH(CustomerNum) = 0, 'N/A', LTRIM(RTRIM(CustomerNum))) AS o_CustomerNumber,
	-- *INF*: IIF(ISNULL(i_Id) or IS_SPACES(i_Id) or LENGTH(i_Id)=0, 'N/A', LTRIM(RTRIM(i_Id)))
	IFF(i_Id IS NULL OR IS_SPACES(i_Id) OR LENGTH(i_Id) = 0, 'N/A', LTRIM(RTRIM(i_Id))) AS o_Id,
	-- *INF*: IIF(ISNULL(i_PolicyVersion), '00', LPAD(TO_CHAR(i_PolicyVersion), 2 , '0'))
	IFF(i_PolicyVersion IS NULL, '00', LPAD(TO_CHAR(i_PolicyVersion), 2, '0')) AS o_PolicyVersion,
	-- *INF*: IIF(ISNULL(i_PolicyNumber) or IS_SPACES(i_PolicyNumber) or LENGTH(i_PolicyNumber)=0, 'N/A', LTRIM(RTRIM(i_PolicyNumber)))
	IFF(i_PolicyNumber IS NULL OR IS_SPACES(i_PolicyNumber) OR LENGTH(i_PolicyNumber) = 0, 'N/A', LTRIM(RTRIM(i_PolicyNumber))) AS o_PolicyNumber
	FROM SQ_DCLocationStaging
	GROUP BY o_PolicyVersion, o_PolicyNumber
),
EXP_values AS (
	SELECT
	o_SessionId AS i_SessionId,
	o_LocationId AS i_LocationId,
	o_Address1 AS i_Address1,
	o_Address2 AS i_Address2,
	o_City AS i_City,
	o_County AS i_County,
	o_StateProv AS i_State,
	o_PostalCode AS i_PostalCode,
	o_Country AS i_Country,
	o_CustomerNumber AS i_CustomerNumber,
	o_Id AS i_Id,
	o_PolicyNumber AS i_PolicyNumber,
	o_PolicyVersion AS i_PolicyVersion,
	-- *INF*: i_PolicyNumber||i_PolicyVersion
	-- 
	-- --i_Id||i_PolicyVersion
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --i_CustomerNumber||i_PolicyNumber||i_PolicyVersion
	i_PolicyNumber || i_PolicyVersion AS o_contract_key,
	'MAILING' AS o_addr_type,
	-- *INF*: IIF(ISNULL(i_Address1) or IS_SPACES(i_Address1)  or LENGTH(i_Address1)=0,'N/A',LTRIM(RTRIM(i_Address1)))
	IFF(i_Address1 IS NULL OR IS_SPACES(i_Address1) OR LENGTH(i_Address1) = 0, 'N/A', LTRIM(RTRIM(i_Address1))) AS o_Address1,
	-- *INF*: IIF(ISNULL(i_Address2) or IS_SPACES(i_Address2)  or LENGTH(i_Address2)=0,'N/A',LTRIM(RTRIM(i_Address2)))
	IFF(i_Address2 IS NULL OR IS_SPACES(i_Address2) OR LENGTH(i_Address2) = 0, 'N/A', LTRIM(RTRIM(i_Address2))) AS o_Address2,
	'N/A' AS o_Address3,
	-- *INF*: IIF(ISNULL(i_City) or IS_SPACES(i_City)  or LENGTH(i_City)=0,'N/A',LTRIM(RTRIM(i_City)))
	IFF(i_City IS NULL OR IS_SPACES(i_City) OR LENGTH(i_City) = 0, 'N/A', LTRIM(RTRIM(i_City))) AS o_City,
	-- *INF*: IIF(ISNULL(i_State) or IS_SPACES(i_State)  or LENGTH(i_State)=0,'N/A',LTRIM(RTRIM(i_State)))
	IFF(i_State IS NULL OR IS_SPACES(i_State) OR LENGTH(i_State) = 0, 'N/A', LTRIM(RTRIM(i_State))) AS o_State,
	-- *INF*: IIF(ISNULL(SUBSTR(i_PostalCode,1,5)) or IS_SPACES(SUBSTR(i_PostalCode,1,5))  or LENGTH(SUBSTR(i_PostalCode,1,5))=0,'N/A',LTRIM(RTRIM(SUBSTR(i_PostalCode,1,5))))
	IFF(SUBSTR(i_PostalCode, 1, 5) IS NULL OR IS_SPACES(SUBSTR(i_PostalCode, 1, 5)) OR LENGTH(SUBSTR(i_PostalCode, 1, 5)) = 0, 'N/A', LTRIM(RTRIM(SUBSTR(i_PostalCode, 1, 5)))) AS o_PostalCode,
	-- *INF*: IIF(SUBSTR(i_PostalCode,6,1)='-',SUBSTR(i_PostalCode,7,4),'N/A')
	IFF(SUBSTR(i_PostalCode, 6, 1) = '-', SUBSTR(i_PostalCode, 7, 4), 'N/A') AS o_zip_postal_code_extension,
	-- *INF*: IIF(ISNULL(i_County) or IS_SPACES(i_County)  or LENGTH(i_County)=0,'N/A',LTRIM(RTRIM(i_County)))
	IFF(i_County IS NULL OR IS_SPACES(i_County) OR LENGTH(i_County) = 0, 'N/A', LTRIM(RTRIM(i_County))) AS o_County,
	'0000' AS o_loc_unit_num,
	-- *INF*: IIF(ISNULL(i_Country) or IS_SPACES(i_Country)  or LENGTH(i_Country)=0,'N/A',LTRIM(RTRIM(i_Country)))
	IFF(i_Country IS NULL OR IS_SPACES(i_Country) OR LENGTH(i_Country) = 0, 'N/A', LTRIM(RTRIM(i_Country))) AS o_Country,
	'N/A' AS o_no_match_flag,
	'N/A' AS o_delivery_confirmation_flag,
	'N/A' AS o_group1_match_code,
	0 AS o_latitude,
	0 AS o_longitude
	FROM AGG_Remove_Duplicates
),
LKP_contract_customer_key AS (
	SELECT
	contract_cust_ak_id,
	contract_key
	FROM (
		SELECT 
		contract_customer.contract_cust_ak_id as contract_cust_ak_id, 
		ltrim(rtrim(contract_customer.contract_key)) as contract_key 
		FROM 
		contract_customer
		WHERE contract_customer.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY contract_key ORDER BY contract_cust_ak_id DESC) = 1
),
LKP_contract_customer_address AS (
	SELECT
	contract_cust_addr_id,
	loc_unit_num,
	addr_line_1,
	addr_line_2,
	addr_line_3,
	city_name,
	state_prov_code,
	zip_postal_code,
	zip_postal_code_extension,
	county_parish_name,
	country_name,
	no_match_flag,
	delivery_confirmation_flag,
	group1_match_code,
	latitude,
	longitude,
	contract_cust_addr_ak_id,
	contract_cust_ak_id,
	addr_type
	FROM (
		SELECT 
			contract_cust_addr_id,
			loc_unit_num,
			addr_line_1,
			addr_line_2,
			addr_line_3,
			city_name,
			state_prov_code,
			zip_postal_code,
			zip_postal_code_extension,
			county_parish_name,
			country_name,
			no_match_flag,
			delivery_confirmation_flag,
			group1_match_code,
			latitude,
			longitude,
			contract_cust_addr_ak_id,
			contract_cust_ak_id,
			addr_type
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer_address
		WHERE CRRNT_SNPSHT_FLAG=1 and source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and contract_cust_ak_id in (
		select contract_cust_ak_id from @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer
		where contract_key in (
		select PolicyNumber+ISNULL(RIGHT('00'+CONVERT(varchar(3),PolicyVersion),2),'00') from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy)
		and crrnt_snpsht_flag=1)
		order by contract_cust_ak_id,addr_type,Created_date Desc
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY contract_cust_ak_id,addr_type ORDER BY contract_cust_addr_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_contract_customer_address.contract_cust_addr_id AS i_cust_addr_id,
	LKP_contract_customer_address.loc_unit_num AS i_loc_unit_num,
	LKP_contract_customer_address.addr_line_1 AS i_addr_line_1,
	LKP_contract_customer_address.addr_line_2 AS i_addr_line_2,
	LKP_contract_customer_address.addr_line_3 AS i_addr_line_3,
	LKP_contract_customer_address.city_name AS i_city,
	LKP_contract_customer_address.state_prov_code AS i_state,
	LKP_contract_customer_address.zip_postal_code AS i_zip_code,
	LKP_contract_customer_address.zip_postal_code_extension AS i_zip_postal_code_extension,
	LKP_contract_customer_address.county_parish_name AS i_county,
	LKP_contract_customer_address.country_name AS i_country,
	LKP_contract_customer_address.no_match_flag AS i_no_match_flag,
	LKP_contract_customer_address.delivery_confirmation_flag AS i_delivery_confirmation_flag,
	LKP_contract_customer_address.group1_match_code AS i_group1_match_code,
	LKP_contract_customer_address.latitude AS i_latitude,
	LKP_contract_customer_address.longitude AS i_longitude,
	-- *INF*: IIF(ISNULL(cust_addr_ak_id), 'NEW', 
	-- IIF(LTRIM(RTRIM(i_addr_line_1)) != LTRIM(RTRIM(addr_line_1)) OR
	-- LTRIM(RTRIM(i_addr_line_2)) != LTRIM(RTRIM(addr_line_2)) OR
	-- LTRIM(RTRIM(i_addr_line_3)) != LTRIM(RTRIM(addr_line_3)) OR
	-- LTRIM(RTRIM(i_city)) != LTRIM(RTRIM(city))  OR
	-- LTRIM(RTRIM(i_state)) != LTRIM(RTRIM(state)) OR
	-- LTRIM(RTRIM(i_zip_code)) != LTRIM(RTRIM(zip_postal_code)) OR
	-- LTRIM(RTRIM(i_zip_postal_code_extension)) != LTRIM(RTRIM(zip_postal_code_extension)) OR
	-- LTRIM(RTRIM(i_loc_unit_num)) != LTRIM(RTRIM(loc_unit_num)) OR
	-- LTRIM(RTRIM(i_county)) != LTRIM(RTRIM(county_parish_name)) OR	
	-- LTRIM(RTRIM(i_country)) != LTRIM(RTRIM(country)) OR
	-- LTRIM(RTRIM(i_no_match_flag)) != LTRIM(RTRIM(no_match_flag)) OR
	-- LTRIM(RTRIM(i_delivery_confirmation_flag)) != LTRIM(RTRIM(delivery_confirmation_flag)) OR
	-- LTRIM(RTRIM(i_group1_match_code)) != LTRIM(RTRIM(group1_match_code)) OR
	-- i_latitude  != latitude OR
	-- i_longitude != longitude,
	-- 'UPDATE', 'NOCHANGE'))
	-- 
	-- 
	-- --iif(NewLookupRow=1,'NEW',IIF(NewLookupRow=2,'UPDATE','NOCHANGE'))
	IFF(cust_addr_ak_id IS NULL, 'NEW', IFF(LTRIM(RTRIM(i_addr_line_1)) != LTRIM(RTRIM(addr_line_1)) OR LTRIM(RTRIM(i_addr_line_2)) != LTRIM(RTRIM(addr_line_2)) OR LTRIM(RTRIM(i_addr_line_3)) != LTRIM(RTRIM(addr_line_3)) OR LTRIM(RTRIM(i_city)) != LTRIM(RTRIM(city)) OR LTRIM(RTRIM(i_state)) != LTRIM(RTRIM(state)) OR LTRIM(RTRIM(i_zip_code)) != LTRIM(RTRIM(zip_postal_code)) OR LTRIM(RTRIM(i_zip_postal_code_extension)) != LTRIM(RTRIM(zip_postal_code_extension)) OR LTRIM(RTRIM(i_loc_unit_num)) != LTRIM(RTRIM(loc_unit_num)) OR LTRIM(RTRIM(i_county)) != LTRIM(RTRIM(county_parish_name)) OR LTRIM(RTRIM(i_country)) != LTRIM(RTRIM(country)) OR LTRIM(RTRIM(i_no_match_flag)) != LTRIM(RTRIM(no_match_flag)) OR LTRIM(RTRIM(i_delivery_confirmation_flag)) != LTRIM(RTRIM(delivery_confirmation_flag)) OR LTRIM(RTRIM(i_group1_match_code)) != LTRIM(RTRIM(group1_match_code)) OR i_latitude != latitude OR i_longitude != longitude, 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	-- *INF*: IIF(v_changed_flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(v_changed_flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS v_eff_from_date,
	LKP_contract_customer_address.contract_cust_addr_ak_id AS cust_addr_ak_id,
	EXP_values.o_addr_type AS addr_type,
	LKP_contract_customer_key.contract_cust_ak_id AS cust_ak_id,
	EXP_values.o_Address1 AS addr_line_1,
	EXP_values.o_Address2 AS addr_line_2,
	EXP_values.o_Address3 AS addr_line_3,
	EXP_values.o_City AS city,
	EXP_values.o_State AS state,
	EXP_values.o_PostalCode AS zip_postal_code,
	EXP_values.o_zip_postal_code_extension AS zip_postal_code_extension,
	EXP_values.o_County AS county_parish_name,
	EXP_values.o_loc_unit_num AS loc_unit_num,
	EXP_values.o_Country AS country,
	EXP_values.o_no_match_flag AS no_match_flag,
	EXP_values.o_delivery_confirmation_flag AS delivery_confirmation_flag,
	EXP_values.o_group1_match_code AS group1_match_code,
	EXP_values.o_latitude AS latitude,
	EXP_values.o_longitude AS longitude,
	v_changed_flag AS changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	v_eff_from_date AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM EXP_values
	LEFT JOIN LKP_contract_customer_address
	ON LKP_contract_customer_address.contract_cust_ak_id = LKP_contract_customer_key.contract_cust_ak_id AND LKP_contract_customer_address.addr_type = EXP_values.o_addr_type
	LEFT JOIN LKP_contract_customer_key
	ON LKP_contract_customer_key.contract_key = EXP_values.o_contract_key
),
FIL_Insert AS (
	SELECT
	cust_addr_ak_id, 
	addr_type, 
	cust_ak_id, 
	addr_line_1, 
	addr_line_2, 
	addr_line_3, 
	city, 
	state, 
	zip_postal_code, 
	zip_postal_code_extension, 
	county_parish_name, 
	loc_unit_num, 
	country, 
	no_match_flag, 
	delivery_confirmation_flag, 
	group1_match_code, 
	latitude, 
	longitude, 
	changed_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_system_id, 
	created_date, 
	modified_date
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW' OR changed_flag='UPDATE'
),
SEQ_customer_address AS (
	CREATE SEQUENCE SEQ_customer_address
	START = 0
	INCREMENT = 1;
),
EXP_Customer_address_ak_id AS (
	SELECT
	cust_addr_ak_id AS i_cust_addr_ak_id,
	SEQ_customer_address.NEXTVAL,
	addr_type,
	cust_ak_id,
	addr_line_1,
	addr_line_2,
	addr_line_3,
	city,
	state,
	zip_postal_code,
	zip_postal_code_extension,
	county_parish_name,
	loc_unit_num,
	country,
	no_match_flag,
	delivery_confirmation_flag,
	group1_match_code,
	latitude,
	longitude,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_system_id,
	created_date,
	modified_date,
	-- *INF*: IIF(ISNULL(i_cust_addr_ak_id),NEXTVAL,i_cust_addr_ak_id)
	IFF(i_cust_addr_ak_id IS NULL, NEXTVAL, i_cust_addr_ak_id) AS cust_addr_ak_id
	FROM FIL_Insert
),
TGT_contract_customer_address_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer_address
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, contract_cust_addr_ak_id, contract_cust_ak_id, addr_type, loc_unit_num, addr_line_1, addr_line_2, addr_line_3, city_name, state_prov_code, zip_postal_code, zip_postal_code_extension, county_parish_name, country_name, no_match_flag, delivery_confirmation_flag, group1_match_code, latitude, longitude)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	source_system_id AS SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	cust_addr_ak_id AS CONTRACT_CUST_ADDR_AK_ID, 
	cust_ak_id AS CONTRACT_CUST_AK_ID, 
	ADDR_TYPE, 
	LOC_UNIT_NUM, 
	ADDR_LINE_1, 
	ADDR_LINE_2, 
	ADDR_LINE_3, 
	city AS CITY_NAME, 
	state AS STATE_PROV_CODE, 
	ZIP_POSTAL_CODE, 
	ZIP_POSTAL_CODE_EXTENSION, 
	COUNTY_PARISH_NAME, 
	country AS COUNTRY_NAME, 
	NO_MATCH_FLAG, 
	DELIVERY_CONFIRMATION_FLAG, 
	GROUP1_MATCH_CODE, 
	LATITUDE, 
	LONGITUDE
	FROM EXP_Customer_address_ak_id
),
SQ_contract_customer_address AS (
	SELECT 
		contract_cust_addr_id,
		eff_from_date,
		eff_to_date,
		contract_cust_addr_ak_id 
	FROM
		@{pipeline().parameters.TARGET_TABLE_OWNER}. contract_customer_address a
	WHERE  EXISTS
		 (SELECT 1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer_address b
		   WHERE crrnt_snpsht_flag = 1 and source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		   AND a.contract_cust_addr_ak_id = b.contract_cust_addr_ak_id
	GROUP BY  contract_cust_addr_ak_id  HAVING count(*) > 1)
	AND source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
	ORDER BY  contract_cust_addr_ak_id ,eff_from_date  DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	eff_from_date AS i_eff_from_date,
	contract_cust_addr_ak_id AS i_cust_addr_ak_id,
	contract_cust_addr_id AS cust_addr_id,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- i_cust_addr_ak_id = v_prev_cust_addr_ak_id ,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(TRUE,
		i_cust_addr_ak_id = v_prev_cust_addr_ak_id, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	i_cust_addr_ak_id AS v_prev_cust_addr_ak_id,
	i_eff_from_date AS v_prev_eff_from_date,
	0 AS o_crrnt_snpsht_flag,
	v_eff_to_date AS o_eff_to_date,
	SYSDATE AS o_modified_date
	FROM SQ_contract_customer_address
),
FIL_FirstRowInAKGroup AS (
	SELECT
	cust_addr_id, 
	orig_eff_to_date AS i_orig_eff_to_date, 
	o_crrnt_snpsht_flag AS crrnt_snpsht_flag, 
	o_eff_to_date AS eff_to_date, 
	o_modified_date AS modified_date
	FROM EXP_Lag_eff_from_date
	WHERE i_orig_eff_to_date != eff_to_date
),
UPD_customer_address AS (
	SELECT
	cust_addr_id, 
	crrnt_snpsht_flag, 
	eff_to_date, 
	modified_date
	FROM FIL_FirstRowInAKGroup
),
TGT_contract_customer_address_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer_address AS T
	USING UPD_customer_address AS S
	ON T.contract_cust_addr_id = S.cust_addr_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),