WITH
SQ_Pif43IXUnmodStage AS (
	SELECT 
	Pif43IXUnmodYearTransaction, 
	Pif43IXUnmodMonthTransaction, 
	Pif43IXUnmodDayTransaction, 
	Pif43IXUnmodStage.Pif43IXUnmodWbmDividendPaid,
	policy.pol_ak_id, 
	SupDividendType.SupDividendTypeID, 
	SupDividendType.PMSStateCode, 
	SupDividendType.DividendType, 
	SupDividendType.DividendPlan
	FROM (select distinct
	PifSymbol 
	,PifPolicyNumber 
	,PifModule 
	,Pif43IXUnmodWCRatingState 
	,Pif43IXUnmodYearTransaction
	,Pif43IXUnmodMonthTransaction
	,Pif43IXUnmodDayTransaction
	,Pif43IXUnmodSplitRateSeq
	,Pif43IXUnmodWbmDividendPaid
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43IXUnmodStage pif43
	where Pif43IXUnmodSegmentLevelCode='I' 
	and Pif43IXUnmodSegmentPartCode='X' 
	and Pif43IXUnmodYearTransaction > = 2001
	and not exists (select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43IXUnmodStage 
	where pif43.PifSymbol=PifSymbol
	and pif43.PifPolicyNumber=PifPolicyNumber
	and pif43.PifModule=PifModule
	and Pif43IXUnmodSegmentLevelCode='I' 
	and Pif43IXUnmodSegmentPartCode='X' 
	and pif43.Pif43IXUnmodWCRatingState=Pif43IXUnmodWCRatingState
	and pif43.Pif43IXUnmodSplitRateSeq<Pif43IXUnmodSplitRateSeq)
	) as Pif43IXUnmodStage 
	join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy as policy
	on LTRIM(RTRIM(Pif43IXUnmodStage.PifSymbol))=policy.pol_sym 
	and LTRIM(RTRIM(Pif43IXUnmodStage.PifPolicyNumber))=policy.pol_num 
	and LTRIM(RTRIM(Pif43IXUnmodStage.PifModule))=policy.pol_mod 
	and policy.source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}' and policy.crrnt_snpsht_flag=1 
	join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SupDividendType as SupDividendType
	on SUBSTRING(LTRIM(RTRIM(Pif43IXUnmodStage.PifSymbol)),1,2)=SupDividendType.PMSPolicySymbol 
	and LTRIM(RTRIM(Pif43IXUnmodStage.Pif43IXUnmodWCRatingState))=SupDividendType.PMSStateCode 
	and policy.pol_eff_date between SupDividendType.EffectiveDate and SupDividendType.ExpirationDate
	and SupDividendType.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
),
EXP_GetValues AS (
	SELECT
	Pif43IXUnmodYearTransaction AS i_Pif43IXUnmodYearTransaction,
	Pif43IXUnmodMonthTransaction AS i_Pif43IXUnmodMonthTransaction,
	Pif43IXUnmodDayTransaction AS i_Pif43IXUnmodDayTransaction,
	Pif43IXUnmodWbmDividendPaid AS i_Pif43IXUnmodWbmDividendPaid,
	pol_ak_id AS i_pol_ak_id,
	SupDividendTypeID AS i_SupDividendTypeID,
	PMSStateCode AS i_PMSStateCode,
	DividendType AS i_DividendType,
	DividendPlan AS i_DividendPlan,
	i_pol_ak_id AS o_PolicyAKId,
	-- *INF*: IIF(ISNULL(i_Pif43IXUnmodWbmDividendPaid), 0.00,ROUND(i_Pif43IXUnmodWbmDividendPaid,2))
	IFF(i_Pif43IXUnmodWbmDividendPaid IS NULL,
		0.00,
		ROUND(i_Pif43IXUnmodWbmDividendPaid, 2
		)
	) AS o_DividendAmount,
	-- *INF*: TO_DATE(TO_CHAR(TO_INTEGER(i_Pif43IXUnmodYearTransaction))||LPAD(TO_CHAR(TO_INTEGER(i_Pif43IXUnmodMonthTransaction)),2,'0')||LPAD(TO_CHAR(TO_INTEGER(i_Pif43IXUnmodDayTransaction)),2,'0'),'YYYYMMDD' )
	TO_DATE(TO_CHAR(CAST(i_Pif43IXUnmodYearTransaction AS INTEGER)
		) || LPAD(TO_CHAR(CAST(i_Pif43IXUnmodMonthTransaction AS INTEGER)
			), 2, '0'
		) || LPAD(TO_CHAR(CAST(i_Pif43IXUnmodDayTransaction AS INTEGER)
			), 2, '0'
		), 'YYYYMMDD'
	) AS o_DividendTransactionEnteredDate,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(i_PMSStateCode)))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(i_PMSStateCode
			)
		)
	) AS o_StateCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(i_DividendPlan)))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(i_DividendPlan
			)
		)
	) AS o_DividendPlan,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(i_DividendType)))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(i_DividendType
			)
		)
	) AS o_DividendType,
	-- *INF*: IIF(ISNULL(i_SupDividendTypeID),-1,i_SupDividendTypeID)
	IFF(i_SupDividendTypeID IS NULL,
		- 1,
		i_SupDividendTypeID
	) AS o_SupDividendTypeId
	FROM SQ_Pif43IXUnmodStage
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
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_abbrev ORDER BY sup_state_id) = 1
),
EXP_sup_state_id AS (
	SELECT
	EXP_GetValues.o_PolicyAKId AS PolicyAKId,
	EXP_GetValues.o_DividendAmount AS DividendAmount,
	EXP_GetValues.o_DividendTransactionEnteredDate AS DividendTransactionEnteredDate,
	EXP_GetValues.o_StateCode AS StateCode,
	EXP_GetValues.o_DividendPlan AS DividendPlan,
	EXP_GetValues.o_DividendType AS DividendType,
	EXP_GetValues.o_SupDividendTypeId AS SupDividendTypeId,
	LKP_sup_state.sup_state_id AS lkp_sup_state_id,
	-- *INF*: IIF(ISNULL(lkp_sup_state_id),-1,lkp_sup_state_id)
	IFF(lkp_sup_state_id IS NULL,
		- 1,
		lkp_sup_state_id
	) AS o_sup_state_id
	FROM EXP_GetValues
	LEFT JOIN LKP_sup_state
	ON LKP_sup_state.state_abbrev = EXP_GetValues.o_StateCode
),
AGG_SUMAMT AS (
	SELECT
	PolicyAKId,
	DividendAmount,
	-- *INF*: ROUND(SUM(DividendAmount),2)
	ROUND(SUM(DividendAmount
		), 2
	) AS sum_DividendAmount,
	DividendTransactionEnteredDate,
	StateCode,
	DividendPlan,
	DividendType,
	SupDividendTypeId,
	o_sup_state_id AS sup_state_id
	FROM EXP_sup_state_id
	GROUP BY PolicyAKId, DividendTransactionEnteredDate, StateCode
),
EXP_TRAN AS (
	SELECT
	PolicyAKId,
	sum_DividendAmount AS DividendAmount,
	DividendTransactionEnteredDate,
	StateCode,
	DividendPlan,
	DividendType,
	SupDividendTypeId,
	sup_state_id
	FROM AGG_SUMAMT
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
		WHERE EXISTS ( SELECT 1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE Dividend.PolicyAKId=policy.pol_ak_id
		AND policy.crrnt_snpsht_flag=1
		AND policy.source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and exists (select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43IXUnmodStage IX
		where LTRIM(RTRIM(IX.PifSymbol))=policy.pol_sym 
		and LTRIM(RTRIM(IX.PifPolicyNumber))=policy.pol_num 
		and LTRIM(RTRIM(IX.PifModule))=policy.pol_mod))
		AND Dividend.CurrentSnapshotFlag = 1
		AND Dividend.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId,DividendTransactionEnteredDate,StateCode ORDER BY DividendId) = 1
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
	EXP_TRAN.PolicyAKId,
	EXP_TRAN.DividendAmount AS DividendPayableAmount,
	EXP_TRAN.DividendTransactionEnteredDate,
	-- *INF*: ADD_TO_DATE(TRUNC(ADD_TO_DATE(DividendTransactionEnteredDate,'MM',1), 'MM'),'DD',-1)
	DATEADD(DAY,- 1,CAST(TRUNC(DATEADD(MONTH,1,DividendTransactionEnteredDate), 'MONTH') AS TIMESTAMP_NTZ(0))) AS DividendRunDate,
	EXP_TRAN.StateCode,
	EXP_TRAN.DividendPlan,
	EXP_TRAN.DividendType,
	EXP_TRAN.SupDividendTypeId,
	EXP_TRAN.sup_state_id,
	lkp_DividendId AS o_DividendId,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_DividendId), 'NEW', 
	-- lkp_DividendPayableAmount <>DividendPayableAmount OR
	-- lkp_DividendPlan<>DividendPlan OR
	-- lkp_DividendType<>DividendType OR
	-- lkp_SupStateId<>sup_state_id OR
	-- lkp_SupDividendTypeId<>SupDividendTypeId OR
	-- lkp_DividendPaidAmount<>v_DividendPaidAmount,'UPDATE',
	-- 'NOCHANGE')
	DECODE(TRUE,
		lkp_DividendId IS NULL, 'NEW',
		lkp_DividendPayableAmount <> DividendPayableAmount 
		OR lkp_DividendPlan <> DividendPlan 
		OR lkp_DividendType <> DividendType 
		OR lkp_SupStateId <> sup_state_id 
		OR lkp_SupDividendTypeId <> SupDividendTypeId 
		OR lkp_DividendPaidAmount <> v_DividendPaidAmount, 'UPDATE',
		'NOCHANGE'
	) AS o_ChangeFlag,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800-01-01 01:00:00', 'YYYY-MM-DD HH24:MI:SS')
	TO_DATE('1800-01-01 01:00:00', 'YYYY-MM-DD HH24:MI:SS'
	) AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
	TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'
	) AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	0.0 AS v_DividendPaidAmount,
	v_DividendPaidAmount AS DividendPaidAmount
	FROM EXP_TRAN
	LEFT JOIN LKP_Dividend
	ON LKP_Dividend.PolicyAKId = EXP_TRAN.PolicyAKId AND LKP_Dividend.DividendTransactionEnteredDate = EXP_TRAN.DividendTransactionEnteredDate AND LKP_Dividend.StateCode = EXP_TRAN.StateCode
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