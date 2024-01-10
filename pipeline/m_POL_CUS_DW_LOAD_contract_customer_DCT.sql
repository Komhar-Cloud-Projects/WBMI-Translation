WITH
SQ_DCPartyStaging AS (
	SELECT WorkDCTPolicy.SessionId, WorkDCTPolicy.PartyId, WorkDCTPolicy.PolicyGUId, WorkDCTPolicy.PolicyVersion, WorkDCTPolicy.Name, WorkDCTPolicy.FirstName, WorkDCTPolicy.LastName, WorkDCTPolicy.MiddleName, WorkDCTPolicy.EntityType, WorkDCTPolicy.FederalEmployeeIDNumber, WorkDCTPolicy.SICCode, WorkDCTPolicy.NAICSCode, WorkDCTPolicy.PolicyStatus, WorkDCTPolicy.CustomerNum, WorkDCTPolicy.Program, WorkDCTPolicy.Association ,WorkDCTPolicy.PolicyNumber 
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
AGG_RemoveDuplicates AS (
	SELECT
	PolicyGUId AS i_Id,
	PolicyVersion AS i_PolicyVersion,
	PolicyNumber AS i_PolicyNumber,
	CustomerNum,
	SessionId,
	PartyId,
	Name,
	FirstName,
	LastName,
	MiddleName,
	EntityType,
	FederalEmployeeIDNumber,
	SICCode,
	NAICSCode,
	PolicyStatus AS Status,
	Program,
	Association,
	-- *INF*: IIF(ISNULL(CustomerNum) or IS_SPACES(CustomerNum) or LENGTH(CustomerNum)=0,'N/A',LTRIM(RTRIM(CustomerNum)))
	IFF(CustomerNum IS NULL OR IS_SPACES(CustomerNum) OR LENGTH(CustomerNum) = 0, 'N/A', LTRIM(RTRIM(CustomerNum))) AS o_CustomerNumber,
	-- *INF*: IIF(ISNULL(i_Id) or IS_SPACES(i_Id) or LENGTH(i_Id)=0,'N/A',LTRIM(RTRIM(i_Id)))
	IFF(i_Id IS NULL OR IS_SPACES(i_Id) OR LENGTH(i_Id) = 0, 'N/A', LTRIM(RTRIM(i_Id))) AS o_Id,
	-- *INF*: IIF(ISNULL(i_PolicyVersion),'00',LPAD(TO_CHAR(i_PolicyVersion),2,'0'))
	IFF(i_PolicyVersion IS NULL, '00', LPAD(TO_CHAR(i_PolicyVersion), 2, '0')) AS o_PolicyVersion,
	'Email' AS o_Email,
	-- *INF*: IIF(ISNULL(i_PolicyNumber) or IS_SPACES(i_PolicyNumber) or LENGTH(i_PolicyNumber)=0,'N/A',LTRIM(RTRIM(i_PolicyNumber)))
	IFF(i_PolicyNumber IS NULL OR IS_SPACES(i_PolicyNumber) OR LENGTH(i_PolicyNumber) = 0, 'N/A', LTRIM(RTRIM(i_PolicyNumber))) AS o_PolicyNumber
	FROM SQ_DCPartyStaging
	GROUP BY o_PolicyVersion, o_PolicyNumber
),
LKP_DCContactStaging AS (
	SELECT
	PhoneNumber,
	PhoneExtension,
	SessionId,
	PartyId
	FROM (
		SELECT 
			PhoneNumber,
			PhoneExtension,
			SessionId,
			PartyId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCContactStaging
		WHERE DCContactStaging.Type='Primary'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionId,PartyId ORDER BY PhoneNumber) = 1
),
LKP_WBPartyStaging AS (
	SELECT
	DoingBusinessAs,
	lkp_SessionId
	FROM (
		select distinct wbps.DoingBusinessAs AS DoingBusinessAs ,
		dcpas.SessionId AS lkp_SessionId
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPartyStaging dcps with (nolock)
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPartyAssociationStaging dcpas with (nolock)
		on dcpas.partyid = dcps.partyid 
		and dcpas.PartyAssociationType = 'Account'
		and dcpas.SessionId = dcps.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBPartyStaging wbps with (nolock)
		on wbps.partyid = dcps.partyid
		and wbps.SessionId = dcps.SessionId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY lkp_SessionId ORDER BY DoingBusinessAs) = 1
),
EXP_Values AS (
	SELECT
	LKP_DCContactStaging.PhoneNumber AS i_PhoneNumber,
	LKP_DCContactStaging.PhoneExtension AS i_PhoneExtension,
	LKP_WBPartyStaging.DoingBusinessAs AS i_DoingBusinessAs,
	AGG_RemoveDuplicates.Name AS i_Name,
	AGG_RemoveDuplicates.FirstName AS i_FirstName,
	AGG_RemoveDuplicates.LastName AS i_LastName,
	AGG_RemoveDuplicates.MiddleName AS i_MiddleName,
	AGG_RemoveDuplicates.EntityType AS i_EntityType,
	AGG_RemoveDuplicates.FederalEmployeeIDNumber AS i_FederalEmployeeIDNumber,
	AGG_RemoveDuplicates.SICCode AS i_SICCode,
	AGG_RemoveDuplicates.NAICSCode AS i_NAICSCode,
	AGG_RemoveDuplicates.Status AS i_Status,
	AGG_RemoveDuplicates.Program AS i_Program,
	AGG_RemoveDuplicates.Association AS i_Association,
	AGG_RemoveDuplicates.o_CustomerNumber AS i_CustomerNumber,
	AGG_RemoveDuplicates.o_Id AS i_Id,
	AGG_RemoveDuplicates.o_PolicyNumber AS i_PolicyNumber,
	AGG_RemoveDuplicates.o_PolicyVersion AS i_PolicyVersion,
	AGG_RemoveDuplicates.o_Email AS i_Email,
	-- *INF*: LTRIM(RTRIM(i_PhoneNumber))
	LTRIM(RTRIM(i_PhoneNumber)) AS v_ph_num_full_trim,
	-- *INF*: IIF(SUBSTR(v_ph_num_full_trim,1,1)='+','+','')
	IFF(SUBSTR(v_ph_num_full_trim, 1, 1) = '+', '+', '') AS v_ph_num_full_first,
	-- *INF*: IIF(ISNULL(i_PhoneNumber) or IS_SPACES(i_PhoneNumber)  or LENGTH(i_PhoneNumber)=0,'N/A',REG_REPLACE(v_ph_num_full_trim,'\D',''))
	IFF(i_PhoneNumber IS NULL OR IS_SPACES(i_PhoneNumber) OR LENGTH(i_PhoneNumber) = 0, 'N/A', REG_REPLACE(v_ph_num_full_trim, '\D', '')) AS v_ph_num_full,
	-- *INF*: IIF(ISNULL(i_FirstName) or IS_SPACES(i_FirstName)  or LENGTH(i_FirstName)=0,'N/A',LTRIM(RTRIM(i_FirstName)))
	IFF(i_FirstName IS NULL OR IS_SPACES(i_FirstName) OR LENGTH(i_FirstName) = 0, 'N/A', LTRIM(RTRIM(i_FirstName))) AS v_FirstName,
	-- *INF*: IIF(ISNULL(i_LastName) or IS_SPACES(i_LastName)  or LENGTH(i_LastName)=0,'N/A',LTRIM(RTRIM(i_LastName)))
	IFF(i_LastName IS NULL OR IS_SPACES(i_LastName) OR LENGTH(i_LastName) = 0, 'N/A', LTRIM(RTRIM(i_LastName))) AS v_LastName,
	-- *INF*: IIF(ISNULL(i_MiddleName) or IS_SPACES(i_MiddleName)  or LENGTH(i_MiddleName)=0,'N/A',LTRIM(RTRIM(i_MiddleName)))
	IFF(i_MiddleName IS NULL OR IS_SPACES(i_MiddleName) OR LENGTH(i_MiddleName) = 0, 'N/A', LTRIM(RTRIM(i_MiddleName))) AS v_MiddleName,
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
	'INSURED' AS o_cust_role,
	i_CustomerNumber AS o_customer_number,
	-- *INF*: IIF(ISNULL(i_Name) or IS_SPACES(i_Name)  or LENGTH(i_Name)=0,'N/A',LTRIM(RTRIM(i_Name)))
	IFF(i_Name IS NULL OR IS_SPACES(i_Name) OR LENGTH(i_Name) = 0, 'N/A', LTRIM(RTRIM(i_Name))) AS o_name,
	-- *INF*: IIF(ISNULL(i_FederalEmployeeIDNumber) or IS_SPACES(i_FederalEmployeeIDNumber)  or LENGTH(i_FederalEmployeeIDNumber)=0,'N/A',LTRIM(RTRIM(i_FederalEmployeeIDNumber)))
	IFF(i_FederalEmployeeIDNumber IS NULL OR IS_SPACES(i_FederalEmployeeIDNumber) OR LENGTH(i_FederalEmployeeIDNumber) = 0, 'N/A', LTRIM(RTRIM(i_FederalEmployeeIDNumber))) AS o_fed_tax_id,
	-- *INF*: IIF(ISNULL(i_DoingBusinessAs) or IS_SPACES(i_DoingBusinessAs)  or LENGTH(i_DoingBusinessAs)=0,'N/A',LTRIM(RTRIM(i_DoingBusinessAs)))
	IFF(i_DoingBusinessAs IS NULL OR IS_SPACES(i_DoingBusinessAs) OR LENGTH(i_DoingBusinessAs) = 0, 'N/A', LTRIM(RTRIM(i_DoingBusinessAs))) AS o_doing_bus_as,
	-- *INF*: IIF(ISNULL(i_SICCode) or IS_SPACES(i_SICCode)  or LENGTH(i_SICCode)=0,'N/A',LTRIM(RTRIM(i_SICCode)))
	IFF(i_SICCode IS NULL OR IS_SPACES(i_SICCode) OR LENGTH(i_SICCode) = 0, 'N/A', LTRIM(RTRIM(i_SICCode))) AS o_sic_code,
	-- *INF*: IIF(ISNULL(i_NAICSCode) or IS_SPACES(i_NAICSCode)  or LENGTH(i_NAICSCode)=0,'N/A',LTRIM(RTRIM(i_NAICSCode)))
	IFF(i_NAICSCode IS NULL OR IS_SPACES(i_NAICSCode) OR LENGTH(i_NAICSCode) = 0, 'N/A', LTRIM(RTRIM(i_NAICSCode))) AS o_naics_code,
	-- *INF*: IIF(ISNULL(i_EntityType) or IS_SPACES(i_EntityType)  or LENGTH(i_EntityType)=0,'N/A',LTRIM(RTRIM(i_EntityType)))
	IFF(i_EntityType IS NULL OR IS_SPACES(i_EntityType) OR LENGTH(i_EntityType) = 0, 'N/A', LTRIM(RTRIM(i_EntityType))) AS o_lgl_ent_code,
	-1 AS o_yr_in_bus,
	v_ph_num_full_first || v_ph_num_full AS o_ph_num_full,
	-- *INF*: IIF(v_ph_num_full='N/A' or LENGTH(v_ph_num_full)  !=  10,'N/A',v_ph_num_full_first || SUBSTR(v_ph_num_full,1,3))
	IFF(v_ph_num_full = 'N/A' OR LENGTH(v_ph_num_full) != 10, 'N/A', v_ph_num_full_first || SUBSTR(v_ph_num_full, 1, 3)) AS o_ph_area_code,
	-- *INF*: IIF(v_ph_num_full='N/A' or LENGTH(v_ph_num_full)  !=  10,'N/A',SUBSTR(v_ph_num_full,4,3))
	IFF(v_ph_num_full = 'N/A' OR LENGTH(v_ph_num_full) != 10, 'N/A', SUBSTR(v_ph_num_full, 4, 3)) AS o_ph_exchange,
	-- *INF*: IIF(v_ph_num_full='N/A' or LENGTH(v_ph_num_full) != 10,'N/A',SUBSTR(v_ph_num_full,7,4))
	IFF(v_ph_num_full = 'N/A' OR LENGTH(v_ph_num_full) != 10, 'N/A', SUBSTR(v_ph_num_full, 7, 4)) AS o_ph_num,
	-- *INF*: IIF(ISNULL(i_PhoneExtension) or IS_SPACES(i_PhoneExtension)  or LENGTH(i_PhoneExtension)=0,'N/A',LTRIM(RTRIM(i_PhoneExtension)))
	IFF(i_PhoneExtension IS NULL OR IS_SPACES(i_PhoneExtension) OR LENGTH(i_PhoneExtension) = 0, 'N/A', LTRIM(RTRIM(i_PhoneExtension))) AS o_ph_extension,
	-- *INF*: IIF(ISNULL(i_Email) or IS_SPACES(i_Email)  or LENGTH(i_Email)=0,'N/A',LTRIM(RTRIM(i_Email)))
	IFF(i_Email IS NULL OR IS_SPACES(i_Email) OR LENGTH(i_Email) = 0, 'N/A', LTRIM(RTRIM(i_Email))) AS o_bus_email_addr,
	-- *INF*: IIF(v_LastName='N/A' or ISNULL(SUBSTR(v_LastName,1,4)),'N/A',SUBSTR(v_LastName,1,4))
	IFF(v_LastName = 'N/A' OR SUBSTR(v_LastName, 1, 4) IS NULL, 'N/A', SUBSTR(v_LastName, 1, 4)) AS o_sort_name,
	v_FirstName AS o_FirstName,
	v_LastName AS o_LastName,
	v_MiddleName AS o_MiddleName
	FROM AGG_RemoveDuplicates
	LEFT JOIN LKP_DCContactStaging
	ON LKP_DCContactStaging.SessionId = AGG_RemoveDuplicates.SessionId AND LKP_DCContactStaging.PartyId = AGG_RemoveDuplicates.PartyId
	LEFT JOIN LKP_WBPartyStaging
	ON LKP_WBPartyStaging.lkp_SessionId = AGG_RemoveDuplicates.SessionId
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
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer
		WHERE crrnt_snpsht_flag=1 and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		order by contract_key,eff_from_date,created_date Desc
		--EDWP 4568
		/*and exists (
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT
		where WCT.PolicyGUId+ISNULL(WCT.PolicyVersionFormatted,'00')=contract_key)*/--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY contract_key,cust_role ORDER BY contract_cust_id) = 1
),
LKP_sup_legal_entity_code AS (
	SELECT
	sup_lgl_ent_code_id,
	lgl_ent_code
	FROM (
		SELECT 
			sup_lgl_ent_code_id,
			lgl_ent_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_legal_entity_code
		WHERE crrnt_snpsht_flag=1 and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY lgl_ent_code ORDER BY sup_lgl_ent_code_id) = 1
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
	LKP_sup_legal_entity_code.sup_lgl_ent_code_id AS i_sup_lgl_ent_code_id,
	EXP_Values.o_contract_key AS contract_key,
	EXP_Values.o_cust_role AS cust_role,
	EXP_Values.o_customer_number AS customer_number,
	EXP_Values.o_name AS name,
	EXP_Values.o_fed_tax_id AS fed_tax_id,
	EXP_Values.o_doing_bus_as AS doing_bus_as,
	EXP_Values.o_sic_code AS sic_code,
	EXP_Values.o_naics_code AS naics_code,
	EXP_Values.o_lgl_ent_code AS lgl_ent_code,
	EXP_Values.o_yr_in_bus AS yr_in_bus,
	EXP_Values.o_ph_num_full AS ph_num_full,
	EXP_Values.o_ph_area_code AS ph_area_code,
	EXP_Values.o_ph_exchange AS ph_exchange,
	EXP_Values.o_ph_num AS ph_num,
	EXP_Values.o_ph_extension AS ph_extension,
	EXP_Values.o_bus_email_addr AS bus_email_addr,
	EXP_Values.o_sort_name AS sort_name,
	-- *INF*: IIF(
	--   ISNULL(i_sup_lgl_ent_code_id),
	--   -1,
	--   i_sup_lgl_ent_code_id
	-- )
	IFF(i_sup_lgl_ent_code_id IS NULL, - 1, i_sup_lgl_ent_code_id) AS v_lgl_ent_code_id,
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
	IFF(lkp_cust_ak_id IS NULL, 'NEW', IFF(lkp_name != name OR lkp_fed_tax_id != fed_tax_id OR lkp_doing_bus_as != doing_bus_as OR lkp_sic_code != sic_code OR lkp_naics_code != naics_code OR lkp_lgl_ent_code != lgl_ent_code OR lkp_yr_in_bus != yr_in_bus OR lkp_ph_num_full != ph_num_full OR lkp_ph_area_code != ph_area_code OR lkp_ph_exchange != ph_exchange OR lkp_ph_num != ph_num OR lkp_ph_extension != ph_extension OR lkp_bus_email_addr != bus_email_addr OR LTRIM(RTRIM(lkp_sort_name)) != sort_name OR lkp_sup_lgl_ent_code_id != v_lgl_ent_code_id OR lkp_FirstName != FirstName OR lkp_LastName != LastName OR lkp_MiddleName != MiddleName, 'UPDATE', 'NOCHANGE')) AS o_changed_flag,
	v_lgl_ent_code_id AS o_lgl_ent_code_id,
	EXP_Values.o_FirstName AS FirstName,
	EXP_Values.o_LastName AS LastName,
	EXP_Values.o_MiddleName AS MiddleName
	FROM EXP_Values
	LEFT JOIN LKP_contract_customer
	ON LKP_contract_customer.contract_key = EXP_Values.o_contract_key AND LKP_contract_customer.cust_role = EXP_Values.o_cust_role
	LEFT JOIN LKP_sup_legal_entity_code
	ON LKP_sup_legal_entity_code.lgl_ent_code = EXP_Values.o_lgl_ent_code
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
	IFF(i_cust_ak_id IS NULL, i_NEXTVAL, i_cust_ak_id) AS o_cust_ak_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_audit_id,
	-- *INF*: IIF(i_changed_flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(i_changed_flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS o_eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_source_sys_id,
	SYSDATE AS o_created_date,
	SYSDATE AS o_modified_date
	FROM FIL_insert
),
TGT_contract_customer_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer
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
		i_cust_ak_id = v_prev_cust_ak_id, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
		i_orig_eff_to_date) AS v_eff_to_date,
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
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer AS T
	USING UPD_customer AS S
	ON T.contract_cust_id = S.cust_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),