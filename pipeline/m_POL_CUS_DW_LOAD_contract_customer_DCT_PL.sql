WITH
SQ_WorkDCTPLParty AS (
	SELECT distinct P.RoleCode, 
	P.RoleDesc, 
	P.PolicyNumber, 
	P.PolicyVersion, 
	P.CustomerNumber, 
	P.FullName, 
	P.FEIN, 
	P.BusinessName, 
	isnull(P.PrimaryPhone,'11111111111') PrimaryPhone, 
	P.PhoneExtension, 
	isnull(P.Email,'N/A') Email, 
	P.LastName, 
	P.LegalEntityCode, 
	P.FirstName, 
	P.MiddleName 
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
EXP_Source_Data_Collect AS (
	SELECT
	RoleCode,
	RoleDesc,
	PolicyNumber,
	PolicyVersion,
	-- *INF*: PolicyNumber || IIF(ISNULL(ltrim(rtrim(PolicyVersion))) or Length(ltrim(rtrim(PolicyVersion)))=0 or IS_SPACES(PolicyVersion),'00',PolicyVersion)
	PolicyNumber || IFF(ltrim(rtrim(PolicyVersion
			)
		) IS NULL 
		OR Length(ltrim(rtrim(PolicyVersion
				)
			)
		) = 0 
		OR LENGTH(PolicyVersion)>0 AND TRIM(PolicyVersion)='',
		'00',
		PolicyVersion
	) AS o_ContractKey,
	CustomerNumber,
	FullName,
	FEIN,
	BusinessName,
	-- *INF*: IIF(ISNULL(BusinessName) or IS_SPACES(BusinessName)  or LENGTH(BusinessName)=0,'N/A',LTRIM(RTRIM(BusinessName))) 
	IFF(BusinessName IS NULL 
		OR LENGTH(BusinessName)>0 AND TRIM(BusinessName)='' 
		OR LENGTH(BusinessName
		) = 0,
		'N/A',
		LTRIM(RTRIM(BusinessName
			)
		)
	) AS o_BusinessName,
	PrimaryPhone,
	-- *INF*: REPLACECHR( 0, REPLACECHR( 0, REPLACECHR( 0, PrimaryPhone, ')', '' ), '(', '' ), '-', '' )
	-- 
	REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(PrimaryPhone,')','','i'),'(','','i'),'-','','i') AS v_ph_num_full,
	v_ph_num_full AS o_ph_num_full,
	-- *INF*: REVERSE(SUBSTR(REVERSE(v_ph_num_full),7,3))
	REVERSE(SUBSTR(REVERSE(v_ph_num_full
			), 7, 3
		)
	) AS o_ph_area_code,
	-- *INF*: REVERSE(SUBSTR(REVERSE(v_ph_num_full),5,3))
	REVERSE(SUBSTR(REVERSE(v_ph_num_full
			), 5, 3
		)
	) AS o_ph_exchange,
	-- *INF*: REVERSE(SUBSTR(REVERSE(v_ph_num_full),1,4))
	REVERSE(SUBSTR(REVERSE(v_ph_num_full
			), 1, 4
		)
	) AS o_ph_num,
	PhoneExtension,
	Email,
	LastName,
	-- *INF*: IIF(ISNULL(LastName) or IS_SPACES(LastName)  or LENGTH(LastName)=0,'N/A',LTRIM(RTRIM(LastName))) 
	IFF(LastName IS NULL 
		OR LENGTH(LastName)>0 AND TRIM(LastName)='' 
		OR LENGTH(LastName
		) = 0,
		'N/A',
		LTRIM(RTRIM(LastName
			)
		)
	) AS o_LastName,
	-- *INF*: IIF(ISNULL(LastName) or IS_SPACES(LastName)  or LENGTH(LastName)=0,'N/A',SUBSTR(LTRIM(RTRIM(LastName)),1,4)) 
	IFF(LastName IS NULL 
		OR LENGTH(LastName)>0 AND TRIM(LastName)='' 
		OR LENGTH(LastName
		) = 0,
		'N/A',
		SUBSTR(LTRIM(RTRIM(LastName
				)
			), 1, 4
		)
	) AS o_sort_name,
	LegalEntityCode,
	FirstName,
	-- *INF*: IIF(ISNULL(FirstName) or IS_SPACES(FirstName)  or LENGTH(FirstName)=0,'N/A',LTRIM(RTRIM(FirstName))) 
	-- 
	IFF(FirstName IS NULL 
		OR LENGTH(FirstName)>0 AND TRIM(FirstName)='' 
		OR LENGTH(FirstName
		) = 0,
		'N/A',
		LTRIM(RTRIM(FirstName
			)
		)
	) AS o_FirstName,
	MiddleName,
	-- *INF*: IIF(ISNULL(MiddleName) or IS_SPACES(MiddleName)  or LENGTH(MiddleName)=0,'N/A',LTRIM(RTRIM(MiddleName))) 
	IFF(MiddleName IS NULL 
		OR LENGTH(MiddleName)>0 AND TRIM(MiddleName)='' 
		OR LENGTH(MiddleName
		) = 0,
		'N/A',
		LTRIM(RTRIM(MiddleName
			)
		)
	) AS o_MiddleName,
	-1 AS o_sup_lgl_ent_code_id,
	'' AS o_sic_code,
	'' AS o_naics_code,
	-1 AS o_yr_in_bus
	FROM SQ_WorkDCTPLParty
),
LKP_contract_customer AS (
	SELECT
	contract_cust_id,
	contract_cust_ak_id,
	cust_num,
	name,
	fed_tax_id,
	doing_bus_as,
	sic_code,
	naics_code,
	lgl_ent_code,
	yr_in_bus,
	ph_num_full,
	ph_area_code,
	ph_exchange,
	ph_num,
	ph_extension,
	bus_email_addr,
	sort_name,
	sup_lgl_ent_code_id,
	FirstName,
	LastName,
	MiddleName,
	contract_key,
	cust_role
	FROM (
		SELECT C.contract_cust_id as contract_cust_id, C.contract_cust_ak_id as contract_cust_ak_id, C.cust_num as cust_num, C.name as name, C.fed_tax_id as fed_tax_id, C.doing_bus_as as doing_bus_as, C.sic_code as sic_code, C.naics_code as naics_code, C.lgl_ent_code as lgl_ent_code, C.yr_in_bus as yr_in_bus, C.ph_num_full as ph_num_full, C.ph_area_code as ph_area_code, C.ph_exchange as ph_exchange, C.ph_num as ph_num, C.ph_extension as ph_extension, C.bus_email_addr as bus_email_addr, C.sort_name as sort_name, C.sup_lgl_ent_code_id as sup_lgl_ent_code_id, C.FirstName as FirstName, C.LastName as LastName, C.MiddleName as MiddleName, C.contract_key as contract_key, C.cust_role as cust_role 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer C
		where crrnt_snpsht_flag=1 
		and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		order by contract_key,eff_from_date,created_date Desc
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY contract_key,cust_role ORDER BY contract_cust_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_contract_customer.contract_cust_id AS lkp_cust_id,
	LKP_contract_customer.contract_cust_ak_id AS lkp_cust_ak_id,
	LKP_contract_customer.cust_num AS lkp_cust_num,
	LKP_contract_customer.name AS lkp_name,
	LKP_contract_customer.fed_tax_id AS lkp_fed_tax_id,
	LKP_contract_customer.doing_bus_as AS lkp_doing_bus_as,
	LKP_contract_customer.sic_code AS lkp_sic_code,
	LKP_contract_customer.naics_code AS lkp_naics_code,
	LKP_contract_customer.lgl_ent_code AS lkp_lgl_ent_code,
	LKP_contract_customer.yr_in_bus AS lkp_yr_in_bus,
	LKP_contract_customer.ph_num_full AS lkp_ph_num_full,
	LKP_contract_customer.ph_area_code AS lkp_ph_area_code,
	LKP_contract_customer.ph_exchange AS lkp_ph_exchange,
	LKP_contract_customer.ph_num AS lkp_ph_num,
	LKP_contract_customer.ph_extension AS lkp_ph_extension,
	LKP_contract_customer.bus_email_addr AS lkp_bus_email_addr,
	LKP_contract_customer.sort_name AS lkp_sort_name,
	LKP_contract_customer.sup_lgl_ent_code_id AS lkp_sup_lgl_ent_code_id,
	LKP_contract_customer.FirstName AS lkp_FirstName,
	LKP_contract_customer.LastName AS lkp_LastName,
	LKP_contract_customer.MiddleName AS lkp_MiddleName,
	EXP_Source_Data_Collect.o_sup_lgl_ent_code_id AS i_sup_lgl_ent_code_id,
	EXP_Source_Data_Collect.o_ContractKey AS contract_key,
	EXP_Source_Data_Collect.RoleCode AS cust_role,
	EXP_Source_Data_Collect.CustomerNumber AS customer_number,
	EXP_Source_Data_Collect.FullName AS name,
	EXP_Source_Data_Collect.FEIN AS fed_tax_id,
	EXP_Source_Data_Collect.o_BusinessName AS doing_bus_as,
	EXP_Source_Data_Collect.o_sic_code AS sic_code,
	EXP_Source_Data_Collect.o_naics_code AS naics_code,
	EXP_Source_Data_Collect.LegalEntityCode AS lgl_ent_code,
	EXP_Source_Data_Collect.o_yr_in_bus AS yr_in_bus,
	EXP_Source_Data_Collect.o_ph_num_full AS ph_num_full,
	EXP_Source_Data_Collect.o_ph_area_code AS ph_area_code,
	EXP_Source_Data_Collect.o_ph_exchange AS ph_exchange,
	EXP_Source_Data_Collect.o_ph_num AS ph_num,
	EXP_Source_Data_Collect.PhoneExtension AS ph_extension,
	EXP_Source_Data_Collect.Email AS bus_email_addr,
	EXP_Source_Data_Collect.o_sort_name AS sort_name,
	-- *INF*: IIF(
	--   ISNULL(i_sup_lgl_ent_code_id),
	--   -1,
	--   i_sup_lgl_ent_code_id
	-- )
	IFF(i_sup_lgl_ent_code_id IS NULL,
		- 1,
		i_sup_lgl_ent_code_id
	) AS v_lgl_ent_code_id,
	-- *INF*: IIF(ISNULL(lkp_cust_ak_id), 'NEW', IIF(
	-- lkp_name != name OR
	-- lkp_fed_tax_id != fed_tax_id OR
	-- lkp_doing_bus_as != doing_bus_as  OR
	-- lkp_sic_code != sic_code  OR
	-- lkp_naics_code != naics_code  OR
	-- lkp_lgl_ent_code != lgl_ent_code  OR
	-- lkp_yr_in_bus != yr_in_bus OR
	-- lkp_ph_num_full != ph_num_full OR
	-- lkp_ph_area_code != ph_area_code OR 
	-- lkp_ph_exchange != ph_exchange OR
	-- lkp_ph_num != ph_num OR
	-- lkp_ph_extension != ph_extension OR
	-- lkp_bus_email_addr != bus_email_addr OR
	-- LTRIM(RTRIM(lkp_sort_name)) != sort_name OR
	-- lkp_sup_lgl_ent_code_id!=v_lgl_ent_code_id OR
	-- lkp_FirstName != FirstName OR
	-- lkp_LastName != LastName OR
	-- lkp_MiddleName != MiddleName,
	-- 'UPDATE', 'NOCHANGE'))
	-- 
	-- 
	-- 
	-- --IIFNewLookupRow=1,'NEW',IIFNewLookupRow=2,'UPDATE','NOCHANGE'
	IFF(lkp_cust_ak_id IS NULL,
		'NEW',
		IFF(lkp_name != name 
			OR lkp_fed_tax_id != fed_tax_id 
			OR lkp_doing_bus_as != doing_bus_as 
			OR lkp_sic_code != sic_code 
			OR lkp_naics_code != naics_code 
			OR lkp_lgl_ent_code != lgl_ent_code 
			OR lkp_yr_in_bus != yr_in_bus 
			OR lkp_ph_num_full != ph_num_full 
			OR lkp_ph_area_code != ph_area_code 
			OR lkp_ph_exchange != ph_exchange 
			OR lkp_ph_num != ph_num 
			OR lkp_ph_extension != ph_extension 
			OR lkp_bus_email_addr != bus_email_addr 
			OR LTRIM(RTRIM(lkp_sort_name
				)
			) != sort_name 
			OR lkp_sup_lgl_ent_code_id != v_lgl_ent_code_id 
			OR lkp_FirstName != FirstName 
			OR lkp_LastName != LastName 
			OR lkp_MiddleName != MiddleName,
			'UPDATE',
			'NOCHANGE'
		)
	) AS o_changed_flag,
	v_lgl_ent_code_id AS o_lgl_ent_code_id,
	EXP_Source_Data_Collect.o_FirstName AS FirstName,
	EXP_Source_Data_Collect.o_LastName AS LastName,
	EXP_Source_Data_Collect.o_MiddleName AS MiddleName
	FROM EXP_Source_Data_Collect
	LEFT JOIN LKP_contract_customer
	ON LKP_contract_customer.contract_key = EXP_Source_Data_Collect.o_ContractKey AND LKP_contract_customer.cust_role = EXP_Source_Data_Collect.RoleCode
),
FIL_insert AS (
	SELECT
	lkp_cust_ak_id AS cust_ak_id, 
	contract_key, 
	cust_role, 
	customer_number, 
	name, 
	fed_tax_id, 
	doing_bus_as, 
	sic_code, 
	naics_code, 
	lgl_ent_code, 
	yr_in_bus, 
	ph_num_full, 
	ph_area_code, 
	ph_exchange, 
	ph_num, 
	ph_extension, 
	bus_email_addr, 
	sort_name AS pif_sort_name, 
	o_changed_flag AS changed_flag, 
	o_lgl_ent_code_id, 
	FirstName, 
	LastName, 
	MiddleName
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW'  OR changed_flag='UPDATE'
),
SEQ_customer AS (
	CREATE SEQUENCE SEQ_customer
	START = 0
	INCREMENT = 1;
),
EXP_customer_ak_id AS (
	SELECT
	SEQ_customer.NEXTVAL AS i_NEXTVAL,
	cust_ak_id AS i_cust_ak_id,
	contract_key,
	cust_role,
	customer_number,
	name,
	fed_tax_id,
	doing_bus_as,
	sic_code,
	naics_code,
	lgl_ent_code,
	yr_in_bus,
	ph_num_full,
	ph_area_code,
	ph_exchange,
	ph_num,
	ph_extension,
	bus_email_addr,
	pif_sort_name,
	changed_flag AS i_changed_flag,
	o_lgl_ent_code_id AS lgl_ent_code_id,
	FirstName,
	LastName,
	MiddleName,
	1 AS o_crrnt_snpsht_flag,
	-- *INF*: IIF(ISNULL(i_cust_ak_id),i_NEXTVAL,i_cust_ak_id)
	IFF(i_cust_ak_id IS NULL,
		i_NEXTVAL,
		i_cust_ak_id
	) AS o_cust_ak_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_audit_id,
	-- *INF*: IIF(i_changed_flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(i_changed_flag = 'NEW',
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		SYSDATE
	) AS o_eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS o_eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_source_sys_id,
	SYSDATE AS o_created_date,
	SYSDATE AS o_modified_date
	FROM FIL_insert
),
TGT_contract_customer_INSERT AS (
	INSERT INTO contract_customer
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, contract_cust_ak_id, cust_num, contract_key, cust_role, name, fed_tax_id, doing_bus_as, sic_code, naics_code, lgl_ent_code, yr_in_bus, ph_num_full, ph_area_code, ph_exchange, ph_num, ph_extension, bus_email_addr, sort_name, sup_lgl_ent_code_id, FirstName, LastName, MiddleName)
	SELECT 
	o_crrnt_snpsht_flag AS CRRNT_SNPSHT_FLAG, 
	o_audit_id AS AUDIT_ID, 
	o_eff_from_date AS EFF_FROM_DATE, 
	o_eff_to_date AS EFF_TO_DATE, 
	o_source_sys_id AS SOURCE_SYS_ID, 
	o_created_date AS CREATED_DATE, 
	o_modified_date AS MODIFIED_DATE, 
	o_cust_ak_id AS CONTRACT_CUST_AK_ID, 
	customer_number AS CUST_NUM, 
	CONTRACT_KEY, 
	CUST_ROLE, 
	NAME, 
	FED_TAX_ID, 
	DOING_BUS_AS, 
	SIC_CODE, 
	NAICS_CODE, 
	LGL_ENT_CODE, 
	YR_IN_BUS, 
	PH_NUM_FULL, 
	PH_AREA_CODE, 
	PH_EXCHANGE, 
	PH_NUM, 
	PH_EXTENSION, 
	BUS_EMAIL_ADDR, 
	pif_sort_name AS SORT_NAME, 
	lgl_ent_code_id AS SUP_LGL_ENT_CODE_ID, 
	FIRSTNAME, 
	LASTNAME, 
	MIDDLENAME
	FROM EXP_customer_ak_id
),
SQ_contract_customer AS (
	SELECT 
		contract_cust_id,
		eff_from_date,
		eff_to_date,
		contract_cust_ak_id 
	FROM
		@{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer a
	WHERE  exists 
		   (SELECT 1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer b
	           WHERE crrnt_snpsht_flag = 1 AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND a.contract_cust_ak_id=b.contract_cust_ak_id GROUP BY contract_cust_ak_id  HAVING count(*) > 1)
	AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	ORDER BY  contract_cust_ak_id , eff_from_date  DESC
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	contract_cust_id AS i_cust_id,
	eff_from_date AS i_eff_from_date,
	eff_to_date AS i_orig_eff_to_date,
	contract_cust_ak_id AS i_cust_ak_id,
	-- *INF*: DECODE(TRUE,
	-- i_cust_ak_id = v_prev_cust_ak_id  ,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),i_orig_eff_to_date)
	DECODE(TRUE,
		i_cust_ak_id = v_prev_cust_ak_id, DATEADD(SECOND,- 1,v_prev_eff_from_date),
		i_orig_eff_to_date
	) AS v_eff_to_date,
	i_cust_ak_id AS v_prev_cust_ak_id,
	i_eff_from_date AS v_prev_eff_from_date,
	i_orig_eff_to_date AS o_orig_eff_to_date,
	i_cust_id AS o_cust_id,
	0 AS o_crrnt_snpsht_flag,
	v_eff_to_date AS o_eff_to_date,
	SYSDATE AS o_modified_date
	FROM SQ_contract_customer
),
FIL_FirstRowInAKGroup AS (
	SELECT
	o_orig_eff_to_date AS i_orig_eff_to_date, 
	o_cust_id AS cust_id, 
	o_crrnt_snpsht_flag AS crrnt_snpsht_flag, 
	o_eff_to_date AS eff_to_date, 
	o_modified_date AS modified_date
	FROM EXP_Lag_eff_from_date
	WHERE i_orig_eff_to_date != eff_to_date
),
UPD_customer AS (
	SELECT
	cust_id, 
	crrnt_snpsht_flag, 
	eff_to_date, 
	modified_date
	FROM FIL_FirstRowInAKGroup
),
TGT_contract_customer_UPDATE AS (
	MERGE INTO contract_customer AS T
	USING UPD_customer AS S
	ON T.contract_cust_id = S.cust_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),