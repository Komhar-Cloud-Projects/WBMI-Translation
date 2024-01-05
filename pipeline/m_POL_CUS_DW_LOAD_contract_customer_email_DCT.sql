WITH
SQ_DCLocationStaging AS (
	SELECT WorkDCTPolicy.SessionId, 
	WorkDCTLocation.LocationId,  
	WorkDCTPolicy.PolicyGUId, 
	WorkDCTPolicy.PolicyVersion, 
	WorkDCTPolicy.CustomerNum, 
	WorkDCTMail.PrimaryEmail, 
	WorkDCTMail.SecondaryEmail, 
	WorkDCTMail.EmailType,
	WorkDCTPolicy.PolicyNumber
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy 
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTLocation
	on WorkDCTLocation.SessionId=WorkDCTPolicy.SessionId
	and WorkDCTLocation.LocationAssociationType='Account' 
	join 
	(select a.sessionid, a.locationid, PrimaryEmail, SecondaryEmail,Description as EmailType 
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging a
	join  @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationStaging b 
	on a.sessionid= b.sessionid and a.locationid=b.locationid
	where replace(Description,' ','') in ('Audit', 'Owner', 'Other', 'LossPrevention') and NOT(PrimaryEmail is NULL and SecondaryEmail is NULL)
	)  WorkDCTMail
	on WorkDCTLocation.SessionId = WorkDCTMail.SessionId
	WHERE WorkDCTPolicy.PolicyStatus<>'Quote'
	and WorkDCTPolicy.TransactionState='committed'
	@{pipeline().parameters.WHERE_CLAUSE}
	ORDER BY WorkDCTPolicy.SessionId,WorkDCTMail.LocationId DESC
),
AGG_Remove_Duplicates AS (
	SELECT
	SessionId AS i_SessionId, 
	LocationId AS i_LocationId, 
	PolicyGUId AS i_Id, 
	PolicyVersion AS i_PolicyVersion, 
	CustomerNum AS i_CustomerNum, 
	PrimaryEmail AS i_PrimaryEmail, 
	SecondaryEmail AS i_SecondaryEmail, 
	EmailType AS i_EmailType, 
	PolicyNumber AS i_PolicyNumber, 
	i_SessionId AS o_SessionId, 
	i_LocationId AS o_LocationId, 
	IFF(i_CustomerNum IS NULL OR IS_SPACES(i_CustomerNum) OR LENGTH(i_CustomerNum) = 0, 'N/A', LTRIM(RTRIM(i_CustomerNum))) AS o_CustomerNumber, 
	IFF(i_Id IS NULL OR IS_SPACES(i_Id) OR LENGTH(i_Id) = 0, 'N/A', LTRIM(RTRIM(i_Id))) AS o_Id, 
	IFF(i_PolicyNumber IS NULL OR IS_SPACES(i_PolicyNumber) OR LENGTH(i_PolicyNumber) = 0, 'N/A', LTRIM(RTRIM(i_PolicyNumber))) AS o_PolicyNumber, 
	IFF(i_PolicyVersion IS NULL, '00', LPAD(TO_CHAR(i_PolicyVersion), 2, '0')) AS o_PolicyVersion, 
	IFF(i_PrimaryEmail IS NULL OR IS_SPACES(i_PrimaryEmail) OR LENGTH(i_PrimaryEmail) = 0, 'N/A', LTRIM(RTRIM(i_PrimaryEmail))) AS o_PrimaryEmail, 
	IFF(i_SecondaryEmail IS NULL OR IS_SPACES(i_SecondaryEmail) OR LENGTH(i_SecondaryEmail) = 0, 'N/A', LTRIM(RTRIM(i_SecondaryEmail))) AS o_SecondaryEmail, 
	i_EmailType AS o_EmailType
	FROM SQ_DCLocationStaging
	GROUP BY o_CustomerNumber, o_Id, o_PolicyNumber, o_PolicyVersion, o_PrimaryEmail, o_SecondaryEmail, o_EmailType
),
EXP_values AS (
	SELECT
	o_SessionId AS i_SessionId,
	o_LocationId AS i_LocationId,
	o_CustomerNumber AS i_CustomerNumber,
	o_Id AS i_Id,
	o_PolicyVersion AS i_PolicyVersion,
	o_PrimaryEmail AS i_PrimaryEmail,
	o_SecondaryEmail AS i_SecondaryEmail,
	o_EmailType AS i_EmailType,
	o_PolicyNumber AS i_PolicyNumber,
	-- *INF*: i_PolicyNumber||i_PolicyVersion
	-- 
	-- --i_Id||i_PolicyVersion
	i_PolicyNumber || i_PolicyVersion AS o_contract_key,
	i_PrimaryEmail AS PrimaryEmail,
	i_SecondaryEmail AS SecondaryEmail,
	i_EmailType AS EmailType,
	i_LocationId AS LocationId
	FROM AGG_Remove_Duplicates
),
RTRTRANS AS (
	SELECT
	o_contract_key AS contract_key,
	PrimaryEmail,
	SecondaryEmail,
	EmailType,
	LocationId
	FROM EXP_values
),
RTRTRANS_PrimaryEmail AS (SELECT * FROM RTRTRANS WHERE TRUE),
RTRTRANS_SecondaryEmail AS (SELECT * FROM RTRTRANS WHERE TRUE),
EXP_PRIMARY AS (
	SELECT
	contract_key,
	PrimaryEmail AS Email,
	EmailType,
	'Primary' AS Priority,
	LocationId AS LocationId1
	FROM RTRTRANS_PrimaryEmail
),
EXP_SECONDARY AS (
	SELECT
	contract_key,
	SecondaryEmail AS Email,
	EmailType,
	'Secondary' AS Priority,
	LocationId AS LocationId3
	FROM RTRTRANS_SecondaryEmail
),
Union AS (
	SELECT contract_key, Email, EmailType, Priority, LocationId1 AS Locationid
	FROM EXP_PRIMARY
	UNION
	SELECT contract_key, Email, EmailType, Priority, LocationId3 AS Locationid
	FROM EXP_SECONDARY
),
FIL_Invalid_Email AS (
	SELECT
	contract_key, 
	Email, 
	EmailType, 
	Priority, 
	Locationid
	FROM Union
	WHERE Email<>'N/A'
),
SRT_LocationId AS (
	SELECT
	contract_key, 
	Email, 
	EmailType, 
	Priority, 
	Locationid
	FROM FIL_Invalid_Email
	ORDER BY contract_key ASC, EmailType ASC, Priority ASC, Locationid DESC
),
AGG_ContractCustomerMailKey AS (
	SELECT
	contract_key, 
	Email, 
	EmailType, 
	Priority
	FROM SRT_LocationId
	QUALIFY ROW_NUMBER() OVER (PARTITION BY contract_key, EmailType, Priority ORDER BY NULL) = 1
),
LKP_SupEmailPriorityCode AS (
	SELECT
	SupEmailPriorityCodeId,
	EmailPriorityDescription
	FROM (
		select SupEmailPriorityCodeId as SupEmailPriorityCodeId, 
		LTRIM(RTRIM(EmailPriorityDescription)) as EmailPriorityDescription
		From SupEmailPriorityCode
		Where currentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EmailPriorityDescription ORDER BY SupEmailPriorityCodeId) = 1
),
LKP_SupEmailTypeCode AS (
	SELECT
	SupEmailTypeCodeId,
	SupEmailTypeDescription
	FROM (
		select SupEmailTypeCodeId as SupEmailTypeCodeId,
		LTRIM(RTRIM(SupEmailTypeDescription)) as SupEmailTypeDescription
		from SupEmailTypeCode
		where CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SupEmailTypeDescription ORDER BY SupEmailTypeCodeId) = 1
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
LKP_ContractCustomerEmailAddress AS (
	SELECT
	ContractCustomerEmailAddressID,
	CustomerEmailAddress,
	ContractCustomerEmailAddressAKID,
	ContractCustomerAKID,
	SupEmailTypeCodeId,
	SupEmailPriorityCodeID
	FROM (
		SELECT 
			ContractCustomerEmailAddressID,
			CustomerEmailAddress,
			ContractCustomerEmailAddressAKID,
			ContractCustomerAKID,
			SupEmailTypeCodeId,
			SupEmailPriorityCodeID
		FROM ContractCustomerEmailAddress
		WHERE SourceSystemId='DCT' and CurrentSnapshotFlag=1
		order by ContractCustomerAKID,SupEmailTypeCodeId,SupEmailPriorityCodeID,CreatedDate Desc
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ContractCustomerAKID,SupEmailTypeCodeId,SupEmailPriorityCodeID ORDER BY ContractCustomerEmailAddressID) = 1
),
SEQ_customer_email AS (
	CREATE SEQUENCE SEQ_customer_email
	START = 0
	INCREMENT = 1;
),
EXP_Detect_Changes AS (
	SELECT
	SEQ_customer_email.NEXTVAL,
	LKP_ContractCustomerEmailAddress.ContractCustomerEmailAddressID AS lkp_ContractCustomerEmailAddressID,
	LKP_ContractCustomerEmailAddress.CustomerEmailAddress AS lkp_CustomerEmailAddress,
	LKP_ContractCustomerEmailAddress.ContractCustomerEmailAddressAKID AS lkp_ContractCustomerEmailAKID,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	LKP_contract_customer_key.contract_cust_ak_id AS ContractCustomerAKID,
	LKP_SupEmailTypeCode.SupEmailTypeCodeId AS i_SupEmailTypeCodeId,
	-- *INF*: IIF(ISNULL(i_SupEmailTypeCodeId), -1, i_SupEmailTypeCodeId)
	IFF(i_SupEmailTypeCodeId IS NULL, - 1, i_SupEmailTypeCodeId) AS o_SupEmailTypeCodeId,
	LKP_SupEmailPriorityCode.SupEmailPriorityCodeId AS i_SupEmailPriorityCodeId,
	-- *INF*: IIF(ISNULL(i_SupEmailPriorityCodeId)-1,i_SupEmailPriorityCodeId)
	IFF(i_SupEmailPriorityCodeId IS NULL - 1, i_SupEmailPriorityCodeId) AS o_SupEmailPriorityCodeId,
	AGG_ContractCustomerMailKey.Email,
	-- *INF*: IIF(lkp_ContractCustomerEmailAKID=-1 OR ISNULL(lkp_ContractCustomerEmailAddressID),NEXTVAL,lkp_ContractCustomerEmailAKID)
	IFF(lkp_ContractCustomerEmailAKID = - 1 OR lkp_ContractCustomerEmailAddressID IS NULL, NEXTVAL, lkp_ContractCustomerEmailAKID) AS o_ContractCustomerEmailAKID,
	-- *INF*: DECODE(TRUE,
	-- ContractCustomerAKID=-1 or lkp_ContractCustomerEmailAddressID=-1 , 'NEW', 
	-- LTRIM(RTRIM(Email)) <> LTRIM(RTRIM(lkp_CustomerEmailAddress)),'UPDATE', 
	-- 'NOCHANGE')
	DECODE(TRUE,
	ContractCustomerAKID = - 1 OR lkp_ContractCustomerEmailAddressID = - 1, 'NEW',
	LTRIM(RTRIM(Email)) <> LTRIM(RTRIM(lkp_CustomerEmailAddress)), 'UPDATE',
	'NOCHANGE') AS v_changed_flag,
	v_changed_flag AS changed_flag
	FROM AGG_ContractCustomerMailKey
	LEFT JOIN LKP_ContractCustomerEmailAddress
	ON LKP_ContractCustomerEmailAddress.ContractCustomerAKID = LKP_contract_customer_key.contract_cust_ak_id AND LKP_ContractCustomerEmailAddress.SupEmailTypeCodeId = LKP_SupEmailTypeCode.SupEmailTypeCodeId AND LKP_ContractCustomerEmailAddress.SupEmailPriorityCodeID = LKP_SupEmailPriorityCode.SupEmailPriorityCodeId
	LEFT JOIN LKP_SupEmailPriorityCode
	ON LKP_SupEmailPriorityCode.EmailPriorityDescription = AGG_ContractCustomerMailKey.Priority
	LEFT JOIN LKP_SupEmailTypeCode
	ON LKP_SupEmailTypeCode.SupEmailTypeDescription = AGG_ContractCustomerMailKey.EmailType
	LEFT JOIN LKP_contract_customer_key
	ON LKP_contract_customer_key.contract_key = AGG_ContractCustomerMailKey.contract_key
),
RTR_InsertOrUpdate AS (
	SELECT
	lkp_ContractCustomerEmailAddressID AS ContractCustomerEmailAddressID,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_system_id,
	created_date,
	modified_date,
	ContractCustomerAKID AS cust_ak_id,
	o_SupEmailTypeCodeId AS SupEmailTypeCodeId,
	o_SupEmailPriorityCodeId AS SupEmailPriorityCodeId,
	Email,
	o_ContractCustomerEmailAKID AS ContractCustomerEmailAKID,
	changed_flag
	FROM EXP_Detect_Changes
),
RTR_InsertOrUpdate_Insert AS (SELECT * FROM RTR_InsertOrUpdate WHERE changed_flag = 'NEW'),
RTR_InsertOrUpdate_Update AS (SELECT * FROM RTR_InsertOrUpdate WHERE changed_flag = 'UPDATE'),
UPD_ContractCustomerEmailAddress AS (
	SELECT
	ContractCustomerEmailAddressID AS ContractCustomerEmailAddressID3, 
	modified_date AS modified_date3, 
	Email AS Email3
	FROM RTR_InsertOrUpdate_Update
),
Update_ContractCustomerEmailAddress AS (
	MERGE INTO ContractCustomerEmailAddress AS T
	USING UPD_ContractCustomerEmailAddress AS S
	ON T.ContractCustomerEmailAddressID = S.ContractCustomerEmailAddressID3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.modified_date3, T.CustomerEmailAddress = S.Email3
),
EXP_Customer_email_address_ak_id AS (
	SELECT
	crrnt_snpsht_flag AS crrnt_snpsht_flag1,
	audit_id AS audit_id1,
	eff_from_date AS eff_from_date1,
	eff_to_date AS eff_to_date1,
	source_system_id AS source_system_id1,
	created_date AS created_date1,
	modified_date AS modified_date1,
	cust_ak_id AS cust_ak_id1,
	SupEmailTypeCodeId AS SupEmailTypeCodeId1,
	SupEmailPriorityCodeId AS SupEmailPriorityCodeId1,
	Email AS Email1,
	ContractCustomerEmailAKID AS ContractCustomerEmailAKID1
	FROM RTR_InsertOrUpdate_Insert
),
Insert_ContractCustomerEmailAddress AS (
	INSERT INTO ContractCustomerEmailAddress
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, ContractCustomerAKID, SupEmailTypeCodeId, SupEmailPriorityCodeID, CustomerEmailAddress, ContractCustomerEmailAddressAKID)
	SELECT 
	crrnt_snpsht_flag1 AS CURRENTSNAPSHOTFLAG, 
	audit_id1 AS AUDITID, 
	eff_from_date1 AS EFFECTIVEDATE, 
	eff_to_date1 AS EXPIRATIONDATE, 
	source_system_id1 AS SOURCESYSTEMID, 
	created_date1 AS CREATEDDATE, 
	modified_date1 AS MODIFIEDDATE, 
	cust_ak_id1 AS CONTRACTCUSTOMERAKID, 
	SupEmailTypeCodeId1 AS SUPEMAILTYPECODEID, 
	SupEmailPriorityCodeId1 AS SUPEMAILPRIORITYCODEID, 
	Email1 AS CUSTOMEREMAILADDRESS, 
	ContractCustomerEmailAKID1 AS CONTRACTCUSTOMEREMAILADDRESSAKID
	FROM EXP_Customer_email_address_ak_id
),