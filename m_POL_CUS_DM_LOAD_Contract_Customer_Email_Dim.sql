WITH
SQ_ContractCustomerEmailAddress AS (
	SELECT
		ContractCustomerEmailAddressID,
		CurrentSnapshotFlag,
		AuditId,
		EffectiveDate,
		ExpirationDate,
		SourceSystemId,
		CreatedDate,
		ModifiedDate,
		ContractCustomerAKID,
		SupEmailTypeCodeId,
		SupEmailPriorityCodeID,
		CustomerEmailAddress,
		ContractCustomerEmailAddressAKID
	FROM ContractCustomerEmailAddress
	WHERE ContractCustomerEmailAddress.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}'
),
LKP_ContractCustomerEmailAddressDim AS (
	SELECT
	ContractCustomerEmailAddressDimID,
	EDWContractCustomerEmailAddressPKID
	FROM (
		SELECT 
			ContractCustomerEmailAddressDimID,
			EDWContractCustomerEmailAddressPKID
		FROM ContractCustomerEmailAddressDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWContractCustomerEmailAddressPKID ORDER BY ContractCustomerEmailAddressDimID) = 1
),
LKP_SupEmailPriorityCode AS (
	SELECT
	EmailPriorityCode,
	EmailPriorityDescription,
	SupEmailPriorityCodeId
	FROM (
		select SupEmailPriorityCodeId as SupEmailPriorityCodeId, EmailPriorityCode as EmailPriorityCode, EmailPriorityDescription as EmailPriorityDescription
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupEmailPriorityCode
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SupEmailPriorityCodeId ORDER BY EmailPriorityCode) = 1
),
LKP_SupEmailTypeCode AS (
	SELECT
	SupEmailTypeCodeCode,
	SupEmailTypeDescription,
	SupEmailTypeCodeID
	FROM (
		select SupEmailTypeCodeID as SupEmailTypeCodeID, SupEmailTypeCodeCode as SupEmailTypeCodeCode, SupEmailTypeDescription as SupEmailTypeDescription
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupEmailTypeCode
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SupEmailTypeCodeID ORDER BY SupEmailTypeCodeCode) = 1
),
LKP_contract_customer_dim AS (
	SELECT
	contract_cust_dim_id,
	cust_num,
	edw_contract_cust_ak_id,
	eff_from_date,
	eff_to_date
	FROM (
		select edw_contract_cust_ak_id as edw_contract_cust_ak_id, cust_num as cust_num, contract_cust_dim_id as contract_cust_dim_id, eff_from_date as eff_from_date, eff_to_date as eff_to_date
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_contract_cust_ak_id,eff_from_date,eff_to_date ORDER BY contract_cust_dim_id) = 1
),
Exp_SetDefaultValue AS (
	SELECT
	LKP_ContractCustomerEmailAddressDim.ContractCustomerEmailAddressDimID,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	'DCT' AS SourceSystemId,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	LKP_contract_customer_dim.contract_cust_dim_id AS i_contract_cust_dim_id,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_INTEGERS(i_contract_cust_dim_id)
	:UDF.DEFAULT_VALUE_FOR_INTEGERS(i_contract_cust_dim_id) AS o_contract_cust_dim_id,
	SQ_ContractCustomerEmailAddress.CustomerEmailAddress AS i_CustomerEmailAddress,
	-- *INF*: IIF(ISNULL(i_CustomerEmailAddress),'N/A',i_CustomerEmailAddress)
	IFF(i_CustomerEmailAddress IS NULL, 'N/A', i_CustomerEmailAddress) AS o_CustomerEmailAddress,
	LKP_SupEmailPriorityCode.EmailPriorityCode AS i_EmailPriorityCode,
	-- *INF*: IIF(ISNULL(i_EmailPriorityCode),'N/A',i_EmailPriorityCode)
	IFF(i_EmailPriorityCode IS NULL, 'N/A', i_EmailPriorityCode) AS o_EmailPriorityCode,
	LKP_SupEmailPriorityCode.EmailPriorityDescription AS i_EmailPriorityDescription,
	-- *INF*: IIF(ISNULL(i_EmailPriorityDescription),'N/A',i_EmailPriorityDescription)
	IFF(i_EmailPriorityDescription IS NULL, 'N/A', i_EmailPriorityDescription) AS o_EmailPriorityDescription,
	LKP_SupEmailTypeCode.SupEmailTypeCodeCode AS i_SupEmailTypeCodeCode,
	-- *INF*: IIF(ISNULL(i_SupEmailTypeCodeCode),'N/A',i_SupEmailTypeCodeCode)
	IFF(i_SupEmailTypeCodeCode IS NULL, 'N/A', i_SupEmailTypeCodeCode) AS o_SupEmailTypeCodeCode,
	LKP_SupEmailTypeCode.SupEmailTypeDescription AS i_SupEmailTypeDescription,
	-- *INF*: IIF(ISNULL(i_SupEmailTypeDescription),'N/A',i_SupEmailTypeDescription)
	IFF(i_SupEmailTypeDescription IS NULL, 'N/A', i_SupEmailTypeDescription) AS o_SupEmailTypeDescription,
	LKP_contract_customer_dim.cust_num AS i_cust_num,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_cust_num)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_cust_num) AS o_cust_num,
	0 AS MasterFlag,
	SQ_ContractCustomerEmailAddress.ContractCustomerEmailAddressAKID AS i_ContractCustomerEmailAddressAKID,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_INTEGERS(i_ContractCustomerEmailAddressAKID)
	:UDF.DEFAULT_VALUE_FOR_INTEGERS(i_ContractCustomerEmailAddressAKID) AS o_ContractCustomerEmailAddressAKID,
	SQ_ContractCustomerEmailAddress.ContractCustomerEmailAddressID AS i_ContractCustomerEmailAddressID,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_INTEGERS(i_ContractCustomerEmailAddressID)
	:UDF.DEFAULT_VALUE_FOR_INTEGERS(i_ContractCustomerEmailAddressID) AS o_ContractCustomerEmailAddressID
	FROM SQ_ContractCustomerEmailAddress
	LEFT JOIN LKP_ContractCustomerEmailAddressDim
	ON LKP_ContractCustomerEmailAddressDim.EDWContractCustomerEmailAddressPKID = SQ_ContractCustomerEmailAddress.ContractCustomerEmailAddressID
	LEFT JOIN LKP_SupEmailPriorityCode
	ON LKP_SupEmailPriorityCode.SupEmailPriorityCodeId = SQ_ContractCustomerEmailAddress.SupEmailPriorityCodeID
	LEFT JOIN LKP_SupEmailTypeCode
	ON LKP_SupEmailTypeCode.SupEmailTypeCodeID = SQ_ContractCustomerEmailAddress.SupEmailTypeCodeId
	LEFT JOIN LKP_contract_customer_dim
	ON LKP_contract_customer_dim.edw_contract_cust_ak_id = SQ_ContractCustomerEmailAddress.ContractCustomerAKID AND LKP_contract_customer_dim.eff_from_date <= SQ_ContractCustomerEmailAddress.ModifiedDate AND LKP_contract_customer_dim.eff_to_date > SQ_ContractCustomerEmailAddress.ModifiedDate
),
RTR_Insert_Update AS (
	SELECT
	ContractCustomerEmailAddressDimID AS lkp_ContractCustomerEmailAddressDimID,
	CurrentSnapshotFlag,
	AuditId,
	EffectiveDate,
	ExpirationDate,
	SourceSystemId,
	CreatedDate,
	ModifiedDate,
	o_contract_cust_dim_id AS contract_cust_dim_id,
	o_CustomerEmailAddress AS CustomerEmailAddress,
	o_EmailPriorityCode AS EmailPriorityCode,
	o_EmailPriorityDescription AS EmailPriorityDescription,
	o_SupEmailTypeCodeCode AS SupEmailTypeCodeCode,
	o_SupEmailTypeDescription AS SupEmailTypeDescription,
	o_cust_num AS cust_num,
	MasterFlag,
	o_ContractCustomerEmailAddressAKID AS ContractCustomerEmailAKID,
	o_ContractCustomerEmailAddressID AS ContractCustomerEmailAddressID
	FROM Exp_SetDefaultValue
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE isnull(lkp_ContractCustomerEmailAddressDimID)),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE not isnull(lkp_ContractCustomerEmailAddressDimID)),
UPD_ContractCustomerEmailAddressDim AS (
	SELECT
	lkp_ContractCustomerEmailAddressDimID AS lkp_ContractCustomerEmailAddressDimID4, 
	ModifiedDate AS ModifiedDate4, 
	contract_cust_dim_id AS contract_cust_dim_id3, 
	CustomerEmailAddress AS CustomerEmailAddress4, 
	EmailPriorityCode AS EmailPriorityCode3, 
	EmailPriorityDescription AS EmailPriorityDescription3, 
	SupEmailTypeCodeCode AS SupEmailTypeCodeCode3, 
	SupEmailTypeDescription AS SupEmailTypeDescription3, 
	cust_num AS cust_num3, 
	MasterFlag AS MasterFlag2, 
	ContractCustomerEmailAKID AS ContractCustomerEmailAKID3, 
	ContractCustomerEmailAddressID AS ContractCustomerEmailAddressID3
	FROM RTR_Insert_Update_UPDATE
),
TGT_UPD_ContractCustomerEmailAddressDim AS (
	MERGE INTO ContractCustomerEmailAddressDim AS T
	USING UPD_ContractCustomerEmailAddressDim AS S
	ON T.ContractCustomerEmailAddressDimID = S.lkp_ContractCustomerEmailAddressDimID4
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate4, T.ContractCustmerDimID = S.contract_cust_dim_id3, T.ContractCustomerEmailAddress = S.CustomerEmailAddress4, T.EmailPriorityCode = S.EmailPriorityCode3, T.EmailPriorityDescription = S.EmailPriorityDescription3, T.EmailTypeCode = S.SupEmailTypeCodeCode3, T.EmailTypeDescription = S.SupEmailTypeDescription3, T.CustomerNumber = S.cust_num3, T.MasterEmailFlag = S.MasterFlag2, T.EDWContractCustomerEmailAddressAKID = S.ContractCustomerEmailAKID3, T.EDWContractCustomerEmailAddressPKID = S.ContractCustomerEmailAddressID3
),
EXP_MasterFlag_new_customer AS (
	SELECT
	CurrentSnapshotFlag AS CurrentSnapshotFlag1,
	AuditId AS AuditId1,
	EffectiveDate AS EffectiveDate1,
	ExpirationDate AS ExpirationDate1,
	SourceSystemId AS SourceSystemId1,
	CreatedDate AS CreatedDate1,
	ModifiedDate AS ModifiedDate1,
	contract_cust_dim_id AS contract_cust_dim_id1,
	CustomerEmailAddress AS CustomerEmailAddress1,
	EmailPriorityCode AS EmailPriorityCode1,
	EmailPriorityDescription AS EmailPriorityDescription1,
	SupEmailTypeCodeCode AS SupEmailTypeCodeCode1,
	SupEmailTypeDescription AS SupEmailTypeDescription1,
	cust_num AS cust_num1,
	MasterFlag AS MasterEmailFlag,
	ContractCustomerEmailAKID AS ContractCustomerEmailAKID1,
	ContractCustomerEmailAddressID AS ContractCustomerEmailAddressID1
	FROM RTR_Insert_Update_INSERT
),
TGT_Insert_ContractCustomerEmailAddressDim AS (
	INSERT INTO ContractCustomerEmailAddressDim
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, ContractCustmerDimID, ContractCustomerEmailAddress, EmailPriorityCode, EmailPriorityDescription, EmailTypeCode, EmailTypeDescription, CustomerNumber, MasterEmailFlag, EDWContractCustomerEmailAddressAKID, EDWContractCustomerEmailAddressPKID)
	SELECT 
	CurrentSnapshotFlag1 AS CURRENTSNAPSHOTFLAG, 
	AuditId1 AS AUDITID, 
	EffectiveDate1 AS EFFECTIVEDATE, 
	ExpirationDate1 AS EXPIRATIONDATE, 
	SourceSystemId1 AS SOURCESYSTEMID, 
	CreatedDate1 AS CREATEDDATE, 
	ModifiedDate1 AS MODIFIEDDATE, 
	contract_cust_dim_id1 AS CONTRACTCUSTMERDIMID, 
	CustomerEmailAddress1 AS CONTRACTCUSTOMEREMAILADDRESS, 
	EmailPriorityCode1 AS EMAILPRIORITYCODE, 
	EmailPriorityDescription1 AS EMAILPRIORITYDESCRIPTION, 
	SupEmailTypeCodeCode1 AS EMAILTYPECODE, 
	SupEmailTypeDescription1 AS EMAILTYPEDESCRIPTION, 
	cust_num1 AS CUSTOMERNUMBER, 
	MASTEREMAILFLAG, 
	ContractCustomerEmailAKID1 AS EDWCONTRACTCUSTOMEREMAILADDRESSAKID, 
	ContractCustomerEmailAddressID1 AS EDWCONTRACTCUSTOMEREMAILADDRESSPKID
	FROM EXP_MasterFlag_new_customer
),
SQ_ContractCustomerEmailAddressDim AS (
	SELECT ContractCustomerEmailAddressDimID,CustomerNumber
	FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.ContractCustomerEmailAddressDim a
	WHERE EXISTS
		( SELECT 1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ContractCustomerEmailAddressDim b WHERE a.CustomerNumber=b.CustomerNumber and a.ModifiedDate >='@{pipeline().parameters.SELECTION_START_TS}' GROUP BY b.CustomerNumber HAVING count(1) > 1) 
	and CustomerNumber<>'N/A' 
	ORDER BY  CustomerNumber,ModifiedDate,EmailPriorityCode, EmailTypeCode
),
EXP_Set_MasterEmailFlag AS (
	SELECT
	ContractCustomerEmailAddressDimID,
	CustomerNumber,
	-- *INF*: IIF(CustomerNumber=v_Prev_CustomerNumber,0,1)
	IFF(CustomerNumber = v_Prev_CustomerNumber, 0, 1) AS v_MasterEmailFlag,
	CustomerNumber AS v_Prev_CustomerNumber,
	v_MasterEmailFlag AS MasterEmailFlag
	FROM SQ_ContractCustomerEmailAddressDim
),
UPD_MasterEmailFlag AS (
	SELECT
	ContractCustomerEmailAddressDimID, 
	MasterEmailFlag
	FROM EXP_Set_MasterEmailFlag
),
ContractCustomerEmailAddressDim_MasterEmailFlag AS (
	MERGE INTO ContractCustomerEmailAddressDim AS T
	USING UPD_MasterEmailFlag AS S
	ON T.ContractCustomerEmailAddressDimID = S.ContractCustomerEmailAddressDimID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.MasterEmailFlag = S.MasterEmailFlag
),