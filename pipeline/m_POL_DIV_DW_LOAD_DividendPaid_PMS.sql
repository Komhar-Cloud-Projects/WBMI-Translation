WITH
SQ_WCDividendPaidStage AS (
	SELECT
		WCDividendPaidStageId,
		WCDividendStageRecID,
		DividendPaidDate,
		DividendPaidAmt,
		AgencyCode,
		PolicySymbol,
		PolicyNumber,
		PolicyModule,
		ExtractDate,
		SourceSystemId
	FROM WCDividendPaidStage
),
EXP_GetValues AS (
	SELECT
	AgencyCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(SUBSTR(AgencyCode,1,2))))
	UDF_DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(SUBSTR(AgencyCode, 1, 2)))) AS o_StateCode,
	PolicySymbol,
	PolicyNumber,
	PolicyModule,
	PolicySymbol||PolicyNumber||PolicyModule AS O_POL_KEY,
	DividendPaidDate AS DividendTransactionEnteredDate,
	-- *INF*: LAST_DAY(DividendTransactionEnteredDate)
	LAST_DAY(DividendTransactionEnteredDate) AS DividendRunDate,
	DividendPaidAmt
	FROM SQ_WCDividendPaidStage
),
LKP_Policy_PolicyAKID AS (
	SELECT
	pol_ak_id,
	pol_key
	FROM (
		SELECT policy.pol_ak_id as pol_ak_id, ltrim(rtrim(policy.pol_key)) as pol_key FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id DESC) = 1
),
LKP_Dividend_Plan_Info AS (
	SELECT
	DividendPlan,
	DividendType,
	SupDividendTypeId,
	PolicyAKId
	FROM (
		SELECT 
			DividendPlan,
			DividendType,
			SupDividendTypeId,
			PolicyAKId
		FROM Dividend
		WHERE DividendPaidAmount = 0.0 AND Dividendplan <> 'No Dividend'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId ORDER BY DividendPlan DESC) = 1
),
LKP_sup_state AS (
	SELECT
	sup_state_id,
	state_abbrev
	FROM (
		SELECT 
			sup_state_id,
			state_abbrev
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_abbrev ORDER BY sup_state_id DESC) = 1
),
EXP_sup_state_id AS (
	SELECT
	LKP_Policy_PolicyAKID.pol_ak_id AS i_PolicyAKId,
	-- *INF*: IIF(ISNULL(i_PolicyAKId),-1,i_PolicyAKId)
	IFF(i_PolicyAKId IS NULL, - 1, i_PolicyAKId) AS o_PolicyAKId,
	0.0 AS DividendAmount,
	EXP_GetValues.DividendTransactionEnteredDate,
	EXP_GetValues.DividendRunDate,
	EXP_GetValues.o_StateCode AS StateCode,
	LKP_Dividend_Plan_Info.DividendPlan,
	-- *INF*: IIF(ISNULL(DividendPlan),'No Dividend',DividendPlan)
	-- 
	-- ---- Default value of Dividend Plan which has dividend paid amount is No Dividend
	IFF(DividendPlan IS NULL, 'No Dividend', DividendPlan) AS O_DividendPlan,
	LKP_Dividend_Plan_Info.DividendType,
	-- *INF*: IIF(ISNULL(DividendType),'No Dividend Plan',DividendType)
	-- 
	-- ---- Default values of Dividend Type which has dividend paid amount is No Dividend Plan
	IFF(DividendType IS NULL, 'No Dividend Plan', DividendType) AS O_DividendType,
	LKP_Dividend_Plan_Info.SupDividendTypeId,
	-- *INF*: IIF(ISNULL(SupDividendTypeId),-1,SupDividendTypeId)
	IFF(SupDividendTypeId IS NULL, - 1, SupDividendTypeId) AS O_SupDividendTypeId,
	LKP_sup_state.sup_state_id AS lkp_sup_state_id,
	-- *INF*: IIF(ISNULL(lkp_sup_state_id),-1,lkp_sup_state_id)
	IFF(lkp_sup_state_id IS NULL, - 1, lkp_sup_state_id) AS o_sup_state_id,
	EXP_GetValues.DividendPaidAmt
	FROM EXP_GetValues
	LEFT JOIN LKP_Dividend_Plan_Info
	ON LKP_Dividend_Plan_Info.PolicyAKId = LKP_Policy_PolicyAKID.pol_ak_id
	LEFT JOIN LKP_Policy_PolicyAKID
	ON LKP_Policy_PolicyAKID.pol_key = EXP_GetValues.O_POL_KEY
	LEFT JOIN LKP_sup_state
	ON LKP_sup_state.state_abbrev = EXP_GetValues.o_StateCode
),
LKP_Dividend AS (
	SELECT
	DividendId,
	DividendPayableAmount,
	DividendPlan,
	DividendType,
	SupStateId,
	SupDividendTypeId,
	DividendPaidAmount,
	PolicyAKId,
	DividendTransactionEnteredDate,
	StateCode
	FROM (
		SELECT 
			DividendId,
			DividendPayableAmount,
			DividendPlan,
			DividendType,
			SupStateId,
			SupDividendTypeId,
			DividendPaidAmount,
			PolicyAKId,
			DividendTransactionEnteredDate,
			StateCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Dividend
		WHERE DividendPlan='N/A' and DividendType='N/A'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId,DividendTransactionEnteredDate,StateCode ORDER BY DividendId DESC) = 1
),
EXP_MetaData AS (
	SELECT
	LKP_Dividend.DividendId AS lkp_DividendId,
	LKP_Dividend.DividendPayableAmount AS lkp_DividendPayableAmount,
	LKP_Dividend.DividendPlan AS lkp_DividendPlan,
	LKP_Dividend.DividendType AS lkp_DividendType,
	LKP_Dividend.SupStateId AS lkp_SupStateId,
	LKP_Dividend.SupDividendTypeId AS lkp_SupDividendTypeId,
	LKP_Dividend.DividendPaidAmount AS lkp_DividendPaidAmount,
	EXP_sup_state_id.o_PolicyAKId AS PolicyAKId,
	EXP_sup_state_id.DividendAmount AS DividendPayableAmount,
	EXP_sup_state_id.DividendTransactionEnteredDate,
	EXP_sup_state_id.DividendRunDate,
	EXP_sup_state_id.StateCode,
	EXP_sup_state_id.O_DividendPlan AS DividendPlan,
	EXP_sup_state_id.O_DividendType AS DividendType,
	EXP_sup_state_id.O_SupDividendTypeId AS SupDividendTypeId,
	EXP_sup_state_id.o_sup_state_id AS sup_state_id,
	lkp_DividendId AS o_DividendId,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_DividendId), 'NEW', 
	-- lkp_DividendPayableAmount <>DividendPayableAmount OR
	-- lkp_DividendPlan<>DividendPlan OR
	-- lkp_DividendType<>DividendType OR
	-- lkp_SupStateId<>sup_state_id OR
	-- lkp_SupDividendTypeId<>SupDividendTypeId OR
	-- lkp_DividendPaidAmount<>DividendPaidAmount,'UPDATE',
	-- 'NOCHANGE')
	DECODE(
	    TRUE,
	    lkp_DividendId IS NULL, 'NEW',
	    lkp_DividendPayableAmount <> DividendPayableAmount OR lkp_DividendPlan <> DividendPlan OR lkp_DividendType <> DividendType OR lkp_SupStateId <> sup_state_id OR lkp_SupDividendTypeId <> SupDividendTypeId OR lkp_DividendPaidAmount <> DividendPaidAmount, 'UPDATE',
	    'NOCHANGE'
	) AS o_ChangeFlag,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800-01-01 01:00:00', 'YYYY-MM-DD HH24:MI:SS')
	TO_TIMESTAMP('1800-01-01 01:00:00', 'YYYY-MM-DD HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
	TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	EXP_sup_state_id.DividendPaidAmt AS DividendPaidAmount
	FROM EXP_sup_state_id
	LEFT JOIN LKP_Dividend
	ON LKP_Dividend.PolicyAKId = EXP_sup_state_id.o_PolicyAKId AND LKP_Dividend.DividendTransactionEnteredDate = EXP_sup_state_id.DividendTransactionEnteredDate AND LKP_Dividend.StateCode = EXP_sup_state_id.StateCode
),
RTR_Target AS (
	SELECT
	o_ChangeFlag AS ChangeFlag,
	o_DividendId AS DividendId,
	PolicyAKId,
	DividendPayableAmount AS DividendAmount,
	DividendTransactionEnteredDate,
	DividendRunDate,
	StateCode,
	DividendPlan,
	DividendType,
	SupDividendTypeId,
	sup_state_id AS SupStateId,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditID AS AuditID,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	DividendPaidAmount
	FROM EXP_MetaData
),
RTR_Target_NEW AS (SELECT * FROM RTR_Target WHERE ChangeFlag='NEW'),
RTR_Target_UPDATE AS (SELECT * FROM RTR_Target WHERE ChangeFlag='UPDATE'),
SEQ_DividendAKId AS (
	CREATE SEQUENCE SEQ_DividendAKId
	START = 1
	INCREMENT = 1;
),
Dividend_INSERT AS (
	INSERT INTO Dividend
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, DividendAKId, PolicyAKId, DividendPayableAmount, DividendTransactionEnteredDate, DividendRunDate, StateCode, DividendPlan, DividendType, SupStateId, SupDividendTypeId, DividendPaidAmount)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SourceSystemID AS SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	SEQ_DividendAKId.NEXTVAL AS DIVIDENDAKID, 
	POLICYAKID, 
	DividendAmount AS DIVIDENDPAYABLEAMOUNT, 
	DIVIDENDTRANSACTIONENTEREDDATE, 
	DIVIDENDRUNDATE, 
	STATECODE, 
	DIVIDENDPLAN, 
	DIVIDENDTYPE, 
	SUPSTATEID, 
	SUPDIVIDENDTYPEID, 
	DIVIDENDPAIDAMOUNT
	FROM RTR_Target_NEW
),
UPD_UPDATE AS (
	SELECT
	DividendId, 
	PolicyAKId, 
	DividendAmount, 
	DividendTransactionEnteredDate, 
	DividendRunDate, 
	StateCode, 
	DividendPlan, 
	DividendType, 
	SupDividendTypeId, 
	SupStateId, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	DividendPaidAmount AS DividendPaidAmount3
	FROM RTR_Target_UPDATE
),
Dividend_UPDATE AS (
	MERGE INTO Dividend AS T
	USING UPD_UPDATE AS S
	ON T.DividendId = S.DividendId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditID = S.AuditID, T.ModifiedDate = S.ModifiedDate, T.DividendPayableAmount = S.DividendAmount, T.DividendPlan = S.DividendPlan, T.DividendType = S.DividendType, T.SupStateId = S.SupStateId, T.SupDividendTypeId = S.SupDividendTypeId, T.DividendPaidAmount = S.DividendPaidAmount3
),