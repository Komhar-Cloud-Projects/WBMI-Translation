WITH
SQ_WorkDCTPLParty AS (
	SELECT distinct P.PolicyNumber, 
	P.PolicyVersion, 
	ISNULL(P.Email,'N/A') Email,
	P.EmailType, 
	P.EmailPriority 
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
EXPTRANS AS (
	SELECT
	PolicyNumber AS i_PolicyNumber,
	PolicyVersion AS i_PolicyVersion,
	-- *INF*: i_PolicyNumber|| IIF(ISNULL(ltrim(rtrim(i_PolicyVersion))) or Length(ltrim(rtrim(i_PolicyVersion)))=0 or IS_SPACES(i_PolicyVersion),'00',i_PolicyVersion)
	i_PolicyNumber || IFF(ltrim(rtrim(i_PolicyVersion)) IS NULL OR Length(ltrim(rtrim(i_PolicyVersion))) = 0 OR IS_SPACES(i_PolicyVersion), '00', i_PolicyVersion) AS o_contract_key
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
	SQ_WorkDCTPLParty.Email,
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
	FROM SQ_WorkDCTPLParty
	LEFT JOIN LKP_ContractCustomerEmailAddress
	ON LKP_ContractCustomerEmailAddress.ContractCustomerAKID = LKP_contract_customer_key.contract_cust_ak_id AND LKP_ContractCustomerEmailAddress.SupEmailTypeCodeId = LKP_SupEmailTypeCode.SupEmailTypeCodeId AND LKP_ContractCustomerEmailAddress.SupEmailPriorityCodeID = LKP_SupEmailPriorityCode.SupEmailPriorityCodeId
	LEFT JOIN LKP_SupEmailPriorityCode
	ON LKP_SupEmailPriorityCode.EmailPriorityDescription = SQ_WorkDCTPLParty.EmailPriority
	LEFT JOIN LKP_SupEmailTypeCode
	ON LKP_SupEmailTypeCode.SupEmailTypeDescription = SQ_WorkDCTPLParty.EmailType
	LEFT JOIN LKP_contract_customer_key
	ON LKP_contract_customer_key.contract_key = EXPTRANS.o_contract_key
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