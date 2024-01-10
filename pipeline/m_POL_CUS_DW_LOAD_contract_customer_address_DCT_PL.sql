WITH
SQ_WorkDCTPLParty AS (
	SELECT distinct P.PolicyNumber, 
	P.PolicyVersion, 
	ISNULL(P.StreetAddressLine1,'') StreetAddressLine1, 
	ISNULL(P.StreetAddressLine2,'') StreetAddressLine2, 
	ISNULL(P.StreetAddressLine3,'') StreetAddressLine3, 
	ISNULL(P.CityName,'') CityName, 
	ISNULL(P.StateName,'') StateName, 
	ISNULL(P.PostalCode,'') PostalCode, 
	ISNULL(P.PostalCodeExt,'') PostalCodeExt,
	ISNULL(P.CountyName,'') CountyName, 
	ISNULL(P.CountryName,'') CountryName
	from
	(select *,case when AddressType='Insured MailingAddress' then 1 else 2 end Customer_Record
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLParty 
	where AddressType in ('Insured MailingAddress','Insured InsuredsAddress')) P
	inner join (select PolicyKey,StartDate,min(case when AddressType='Insured MailingAddress' then 1 else 2 end) Customer_Record
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLParty P
	where AddressType in ('Insured MailingAddress','Insured InsuredsAddress')
	group by PolicyKey,StartDate) B
	on P.Policykey=B.PolicyKey
	and P.StartDate=B.STartDate
	and P.Customer_Record=B.Customer_Record
),
EXP_Src_Data_Collect AS (
	SELECT
	CountryName AS i_country,
	PolicyNumber AS i_PolicyNumber,
	PolicyVersion AS i_PolicyVersion,
	-- *INF*: i_PolicyNumber|| IIF(ISNULL(ltrim(rtrim(i_PolicyVersion))) or Length(ltrim(rtrim(i_PolicyVersion)))=0 or IS_SPACES(i_PolicyVersion),'00',i_PolicyVersion)
	i_PolicyNumber || IFF(ltrim(rtrim(i_PolicyVersion
			)
		) IS NULL 
		OR Length(ltrim(rtrim(i_PolicyVersion
				)
			)
		) = 0 
		OR LENGTH(i_PolicyVersion)>0 AND TRIM(i_PolicyVersion)='',
		'00',
		i_PolicyVersion
	) AS o_contract_key,
	'MAILING' AS o_addr_type,
	'0000' AS o_loc_unit_num,
	-- *INF*: IIF(ISNULL(i_country) or IS_SPACES(i_country)  or LENGTH(i_country)=0,'N/A',LTRIM(RTRIM(i_country)))
	IFF(i_country IS NULL 
		OR LENGTH(i_country)>0 AND TRIM(i_country)='' 
		OR LENGTH(i_country
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_country
			)
		)
	) AS o_Country,
	'N\A' AS o_no_match_flag,
	'N\A' AS o_delivery_confirmation_flag,
	'N/A' AS o_group1_match_code,
	0 AS o_latitude,
	0 AS o_longitude,
	StreetAddressLine1,
	StreetAddressLine2,
	StreetAddressLine3,
	CityName,
	StateName,
	PostalCode,
	PostalCodeExt,
	CountyName
	FROM SQ_WorkDCTPLParty
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
		select PolicyNumber+ISNULL(RIGHT('00'+CONVERT(varchar(3),PolicyVersion),2),'00') from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLParty)
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
	IFF(cust_addr_ak_id IS NULL,
		'NEW',
		IFF(LTRIM(RTRIM(i_addr_line_1
				)
			) != LTRIM(RTRIM(addr_line_1
				)
			) 
			OR LTRIM(RTRIM(i_addr_line_2
				)
			) != LTRIM(RTRIM(addr_line_2
				)
			) 
			OR LTRIM(RTRIM(i_addr_line_3
				)
			) != LTRIM(RTRIM(addr_line_3
				)
			) 
			OR LTRIM(RTRIM(i_city
				)
			) != LTRIM(RTRIM(city
				)
			) 
			OR LTRIM(RTRIM(i_state
				)
			) != LTRIM(RTRIM(state
				)
			) 
			OR LTRIM(RTRIM(i_zip_code
				)
			) != LTRIM(RTRIM(zip_postal_code
				)
			) 
			OR LTRIM(RTRIM(i_zip_postal_code_extension
				)
			) != LTRIM(RTRIM(zip_postal_code_extension
				)
			) 
			OR LTRIM(RTRIM(i_loc_unit_num
				)
			) != LTRIM(RTRIM(loc_unit_num
				)
			) 
			OR LTRIM(RTRIM(i_county
				)
			) != LTRIM(RTRIM(county_parish_name
				)
			) 
			OR LTRIM(RTRIM(i_country
				)
			) != LTRIM(RTRIM(country
				)
			) 
			OR LTRIM(RTRIM(i_no_match_flag
				)
			) != LTRIM(RTRIM(no_match_flag
				)
			) 
			OR LTRIM(RTRIM(i_delivery_confirmation_flag
				)
			) != LTRIM(RTRIM(delivery_confirmation_flag
				)
			) 
			OR LTRIM(RTRIM(i_group1_match_code
				)
			) != LTRIM(RTRIM(group1_match_code
				)
			) 
			OR i_latitude != latitude 
			OR i_longitude != longitude,
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_changed_flag,
	-- *INF*: IIF(v_changed_flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(v_changed_flag = 'NEW',
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		SYSDATE
	) AS v_eff_from_date,
	LKP_contract_customer_address.contract_cust_addr_ak_id AS cust_addr_ak_id,
	EXP_Src_Data_Collect.o_addr_type AS addr_type,
	LKP_contract_customer_key.contract_cust_ak_id AS cust_ak_id,
	EXP_Src_Data_Collect.StreetAddressLine1 AS addr_line_1,
	EXP_Src_Data_Collect.StreetAddressLine2 AS addr_line_2,
	EXP_Src_Data_Collect.StreetAddressLine3 AS addr_line_3,
	EXP_Src_Data_Collect.CityName AS city,
	EXP_Src_Data_Collect.StateName AS state,
	EXP_Src_Data_Collect.PostalCode AS zip_postal_code,
	EXP_Src_Data_Collect.PostalCodeExt AS zip_postal_code_extension,
	EXP_Src_Data_Collect.CountyName AS county_parish_name,
	EXP_Src_Data_Collect.o_loc_unit_num AS loc_unit_num,
	EXP_Src_Data_Collect.o_Country AS country,
	EXP_Src_Data_Collect.o_no_match_flag AS no_match_flag,
	EXP_Src_Data_Collect.o_delivery_confirmation_flag AS delivery_confirmation_flag,
	EXP_Src_Data_Collect.o_group1_match_code AS group1_match_code,
	EXP_Src_Data_Collect.o_latitude AS latitude,
	EXP_Src_Data_Collect.o_longitude AS longitude,
	v_changed_flag AS changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	v_eff_from_date AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM EXP_Src_Data_Collect
	LEFT JOIN LKP_contract_customer_address
	ON LKP_contract_customer_address.contract_cust_ak_id = LKP_contract_customer_key.contract_cust_ak_id AND LKP_contract_customer_address.addr_type = EXP_Src_Data_Collect.o_addr_type
	LEFT JOIN LKP_contract_customer_key
	ON LKP_contract_customer_key.contract_key = EXP_Src_Data_Collect.o_contract_key
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
	IFF(i_cust_addr_ak_id IS NULL,
		NEXTVAL,
		i_cust_addr_ak_id
	) AS cust_addr_ak_id
	FROM FIL_Insert
),
TGT_contract_customer_address_INSERT AS (
	INSERT INTO contract_customer_address
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
		i_cust_addr_ak_id = v_prev_cust_addr_ak_id, DATEADD(SECOND,- 1,v_prev_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
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
	MERGE INTO contract_customer_address AS T
	USING UPD_customer_address AS S
	ON T.contract_cust_addr_id = S.cust_addr_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),