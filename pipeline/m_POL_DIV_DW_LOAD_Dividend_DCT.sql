WITH
SQ_WBWCDividendStage AS (
	select 
	 DividendPaid,
	pol_ak_id,
	TransactionDate,
	PrimaryLocationState,
	DividendType,
	DividendOptions,
	RatingPlan,
	PolicyNumber
	 from (
	
	SELECT  
	Rank() over(partition by pol.pol_ak_id,ISNULL(WBWCD.State,DCWCL.PrimaryLocationState) order by DCT.TransactionDate desc) rankperstate,
	case when WBWCL.DividendPaidDate is not null then 
	ISNULL(WBWCL.DividendPremium,0) else 0 end DividendPaid,
	pol.pol_ak_id, 
	DCT.TransactionDate, 
	ISNULL(WBWCD.State,DCWCL.PrimaryLocationState) as PrimaryLocationState, 
	WBWCD.DividendType, 
	WBWCD.DividendOptions, 
	DCWCL.RatingPlan, 
	DCP.PolicyNumber 
	FROM DCPolicyStaging DCP
	join WBPolicyStaging WBP on DCP.SessionId=WBP.SessionId and DCP.PolicyId=WBP.PolicyId 
	join DCTransactionStaging DCT on DCT.SessionId=DCP.SessionId  
	----and DCT.HistoryID=( select max(A.HistoryID) from DCTransactionStaging A where A.SessionId=DCT.SessionId) 
	and DCT.State='Committed'
	join DCLineStaging DCL on WBP.SessionID = DCL.SessionID and WBP.PolicyID = DCL.PolicyID
	join DCWCLineStaging DCWCL on DCL.SessionID = DCWCL.SessionID and DCL.LineID = DCWCL.LineID
	join WBWCLineStage WBWCL on WBWCL.SessionId = DCWCL.SessionId and WBWCL.WCLineId = DCWCL.WC_LineId
	join WBWCDividendStage WBWCD on WBWCL.SessionId=WBWCD.SessionId  and WBWCD.WCLineId = WBWCL.WCLineId
	inner hash join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol
	on DCP.PolicyNumber=pol.pol_num 
	and pol.pol_mod=WBP.PolicyVersionFormatted 
	and pol.source_sys_id='DCT'
	and pol.crrnt_snpsht_flag=1
	where not ((ISNULL(WBWCD.DividendType,'None')='None'
	 or ISNULL(DividendOptions,'0')='0') and ISNULL(WBWCL.DividendPremium,0)=0) 
	 ) a where rankperstate=1
),
FLT_Remove_Invalid_Policies AS (
	SELECT
	DividendPaid, 
	pol_ak_id, 
	TransactionDate, 
	PrimaryLocationState, 
	DividendType, 
	DividendOption, 
	RatingPlan, 
	PolicyNumber
	FROM SQ_WBWCDividendStage
	WHERE LENGTH(PolicyNumber)=7
),
EXP_GetValue AS (
	SELECT
	DividendPaid AS i_DividendPaid,
	pol_ak_id AS i_pol_ak_id,
	TransactionDate AS i_TransactionDate,
	PrimaryLocationState AS i_PrimaryLocationState,
	DividendType AS i_DividendType,
	DividendOption AS i_DividendOption,
	RatingPlan AS i_RatingPlan,
	-- *INF*: REPLACESTR(1,:UDF.DEFAULT_VALUE_FOR_STRINGS(IIF(LTRIM(RTRIM(i_DividendType))='None','N/A',LTRIM(RTRIM(i_DividendType)))),' ','')
	REGEXP_REPLACE(UDF_DEFAULT_VALUE_FOR_STRINGS(
	        IFF(
	            LTRIM(RTRIM(i_DividendType)) = 'None', 'N/A', LTRIM(RTRIM(i_DividendType))
	        )),' ','') AS v_DividendType,
	-- *INF*: REPLACESTR(1,:UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(i_DividendOption))),' ','')
	-- 
	-- 
	-- 
	-- --REPLACESTR(1,:UDF.DEFAULT_VALUE_FOR_STRINGS(IIF(LTRIM(RTRIM(i_DividendOption))='0','N/A',LTRIM(RTRIM(i_DividendOption)))),' ','')
	-- 
	-- 
	-- 
	REGEXP_REPLACE(UDF_DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(i_DividendOption))),' ','') AS v_DividendOption,
	i_pol_ak_id AS o_pol_ak_id,
	i_DividendPaid AS o_DividendPaid,
	-- *INF*: TO_DATE(TO_CHAR(
	-- IIF(ISNULL(i_TransactionDate),TO_DATE('1800-01-01 00:00:00.000','YYYY-MM-DD HH24:MI:SS.MS'),i_TransactionDate)
	-- ,'YYYYMMDD'),'YYYYMMDD')
	TO_TIMESTAMP(TO_CHAR(
	        IFF(
	            i_TransactionDate IS NULL,
	            TO_TIMESTAMP('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.MS'),
	            i_TransactionDate
	        ), 'YYYYMMDD'), 'YYYYMMDD') AS o_TransactionDate,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(i_PrimaryLocationState)))
	UDF_DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(i_PrimaryLocationState))) AS o_PrimaryLocationState,
	-- *INF*: v_DividendType
	-- 
	-- --IIF(v_DividendType!='FlatCapped',v_DividendType,'CappedFlat')
	v_DividendType AS o_DividendType,
	-- *INF*: DECODE(TRUE,
	-- v_DividendOption='0','N/A',
	-- IS_NUMBER(v_DividendOption),v_DividendOption||'%',
	-- REPLACESTR(0,v_DividendOption,'with','w/'))
	DECODE(
	    TRUE,
	    v_DividendOption = '0', 'N/A',
	    REGEXP_LIKE(v_DividendOption, '^[0-9]+$'), v_DividendOption || '%',
	    REGEXP_REPLACE(v_DividendOption,'with','w/','i')
	) AS o_DividendOption,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(i_RatingPlan)))
	UDF_DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(i_RatingPlan))) AS o_RatingPlan
	FROM FLT_Remove_Invalid_Policies
),
AGG_RemoveDuplicate AS (
	SELECT
	o_pol_ak_id AS pol_ak_id,
	o_DividendPaid AS DividendPaid,
	o_TransactionDate AS TransactionDate,
	o_PrimaryLocationState AS PrimaryLocationState,
	o_DividendType AS DividendType,
	o_DividendOption AS DividendOption,
	o_RatingPlan AS RatingPlan
	FROM EXP_GetValue
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id, TransactionDate, PrimaryLocationState, DividendType, DividendOption, RatingPlan ORDER BY NULL) = 1
),
LKP_SupDividendType AS (
	SELECT
	SupDividendTypeID,
	PMSStateCode,
	DividendType,
	DividendPlan,
	StandardDividendType,
	StandardDividendPlan
	FROM (
		SELECT a.SupDividendTypeID as SupDividendTypeID,
		replace(a.StandardDividendType,'?','') as StandardDividendType, 
		replace(replace(a.StandardDividendPlan,' ',''),'?','') as StandardDividendPlan,
		a.PMSStateCode as PMSStateCode, 
		replace(a.DividendType,' ','') as DividendType, 
		replace(replace(a.DividendPlan,' ',''),'?','') as DividendPlan
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDividendType a
		where a.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PMSStateCode,DividendType,DividendPlan ORDER BY SupDividendTypeID) = 1
),
EXP_PlanandType AS (
	SELECT
	AGG_RemoveDuplicate.pol_ak_id AS i_pol_ak_id,
	AGG_RemoveDuplicate.DividendPaid AS i_DividendPaid,
	AGG_RemoveDuplicate.TransactionDate AS i_TransactionDate,
	AGG_RemoveDuplicate.PrimaryLocationState AS i_PrimaryLocationState,
	LKP_SupDividendType.SupDividendTypeID AS lkp_SupDividendTypeID,
	LKP_SupDividendType.PMSStateCode AS lkp_PMSStateCode,
	LKP_SupDividendType.DividendType AS lkp_DividendType,
	LKP_SupDividendType.DividendPlan AS lkp_DividendPlan,
	LKP_SupDividendType.StandardDividendType AS lkp_StandardDividendType,
	LKP_SupDividendType.StandardDividendPlan AS lkp_StandardDividendPlan,
	AGG_RemoveDuplicate.RatingPlan AS i_RatingPlan,
	i_pol_ak_id AS o_PolicyAKId,
	-- *INF*: IIF(ISNULL(i_DividendPaid),0,i_DividendPaid)
	IFF(i_DividendPaid IS NULL, 0, i_DividendPaid) AS o_DividendAmount,
	-- *INF*: IIF(ISNULL(i_TransactionDate),TO_DATE('1800-01-01 00:00:00.000','YYYY-MM-DD HH24:MI:SS.MS'),i_TransactionDate)
	IFF(
	    i_TransactionDate IS NULL,
	    TO_TIMESTAMP('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.MS'),
	    i_TransactionDate
	) AS o_DividendTransactionEnteredDate,
	-- *INF*: IIF(ISNULL(i_PrimaryLocationState),'N/A',i_PrimaryLocationState)
	-- 
	-- --IIF(ISNULL(lkp_PMSStateCode),'N/A',lkp_PMSStateCode)
	IFF(i_PrimaryLocationState IS NULL, 'N/A', i_PrimaryLocationState) AS o_StateCode,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_DividendPlan) or lkp_DividendPlan='N/A','No Dividend',
	-- lkp_DividendType='FlatVariable', 'Flat '||lkp_StandardDividendPlan||' Variable',
	-- lkp_DividendType='Flat',lkp_StandardDividendPlan||' Flat',
	-- lkp_StandardDividendType||' '||lkp_StandardDividendPlan
	-- )
	-- 
	DECODE(
	    TRUE,
	    lkp_DividendPlan IS NULL or lkp_DividendPlan = 'N/A', 'No Dividend',
	    lkp_DividendType = 'FlatVariable', 'Flat ' || lkp_StandardDividendPlan || ' Variable',
	    lkp_DividendType = 'Flat', lkp_StandardDividendPlan || ' Flat',
	    lkp_StandardDividendType || ' ' || lkp_StandardDividendPlan
	) AS v_DividendPlan,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(lkp_DividendType),lkp_StandardDividendType,
	-- 'No Dividend')
	-- 
	DECODE(
	    TRUE,
	    lkp_DividendType IS NOT NULL, lkp_StandardDividendType,
	    'No Dividend'
	) AS v_DividendType,
	-- *INF*: IIF(ISNULL(v_DividendPlan),'N/A',v_DividendPlan)
	-- 
	IFF(v_DividendPlan IS NULL, 'N/A', v_DividendPlan) AS o_DividendPlan,
	-- *INF*: IIF(ISNULL(v_DividendType),'N/A',v_DividendType)
	-- 
	IFF(v_DividendType IS NULL, 'N/A', v_DividendType) AS o_DividendType,
	-- *INF*: IIF(ISNULL(lkp_SupDividendTypeID),-1,lkp_SupDividendTypeID)
	IFF(lkp_SupDividendTypeID IS NULL, - 1, lkp_SupDividendTypeID) AS o_SupDividendTypeId
	FROM AGG_RemoveDuplicate
	LEFT JOIN LKP_SupDividendType
	ON LKP_SupDividendType.PMSStateCode = AGG_RemoveDuplicate.PrimaryLocationState AND LKP_SupDividendType.DividendType = AGG_RemoveDuplicate.DividendType AND LKP_SupDividendType.DividendPlan = AGG_RemoveDuplicate.DividendOption
),
LKP_sup_state AS (
	SELECT
	sup_state_id,
	state_code
	FROM (
		SELECT 
			sup_state_id,
			state_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY sup_state_id) = 1
),
EXP_sup_state AS (
	SELECT
	EXP_PlanandType.o_PolicyAKId AS PolicyAKId,
	EXP_PlanandType.o_DividendAmount AS DividendAmount,
	EXP_PlanandType.o_DividendTransactionEnteredDate AS DividendTransactionEnteredDate,
	EXP_PlanandType.o_StateCode AS StateCode,
	EXP_PlanandType.o_DividendPlan AS DividendPlan,
	EXP_PlanandType.o_DividendType AS DividendType,
	EXP_PlanandType.o_SupDividendTypeId AS SupDividendTypeId,
	LKP_sup_state.sup_state_id AS lkp_sup_state_id,
	-- *INF*: IIF(ISNULL(lkp_sup_state_id),-1,lkp_sup_state_id)
	IFF(lkp_sup_state_id IS NULL, - 1, lkp_sup_state_id) AS o_sup_state_id
	FROM EXP_PlanandType
	LEFT JOIN LKP_sup_state
	ON LKP_sup_state.state_code = EXP_PlanandType.o_StateCode
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
			StateCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Dividend
		WHERE Dividend.CurrentSnapshotFlag = 1
		AND Dividend.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId,StateCode ORDER BY DividendId) = 1
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
	EXP_sup_state.PolicyAKId,
	EXP_sup_state.DividendAmount,
	EXP_sup_state.DividendTransactionEnteredDate,
	-- *INF*: ADD_TO_DATE(TRUNC(ADD_TO_DATE(DividendTransactionEnteredDate,'MM',1), 'MM'),'DD',-1)
	-- 
	DATEADD(DAY,- 1,CAST(TRUNC(DATEADD(MONTH,1,DividendTransactionEnteredDate), 'MONTH') AS TIMESTAMP_NTZ(0))) AS DividendRunDate,
	EXP_sup_state.StateCode,
	EXP_sup_state.DividendPlan,
	EXP_sup_state.DividendType,
	EXP_sup_state.SupDividendTypeId,
	EXP_sup_state.o_sup_state_id AS sup_state_id,
	lkp_DividendId AS o_DividendId,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_DividendId), 'NEW', 
	-- lkp_DividendPayableAmount <>DividendAmount OR
	-- lkp_DividendPlan<>DividendPlan OR
	-- lkp_DividendType<>DividendType OR
	-- lkp_SupStateId<>sup_state_id OR
	-- lkp_SupDividendTypeId<>SupDividendTypeId OR
	-- lkp_DividendPaidAmount<>DividendAmount,'UPDATE',
	-- 'NOCHANGE')
	DECODE(
	    TRUE,
	    lkp_DividendId IS NULL, 'NEW',
	    lkp_DividendPayableAmount <> DividendAmount OR lkp_DividendPlan <> DividendPlan OR lkp_DividendType <> DividendType OR lkp_SupStateId <> sup_state_id OR lkp_SupDividendTypeId <> SupDividendTypeId OR lkp_DividendPaidAmount <> DividendAmount, 'UPDATE',
	    'NOCHANGE'
	) AS o_ChangeFlag,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
	TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
	TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate
	FROM EXP_sup_state
	LEFT JOIN LKP_Dividend
	ON LKP_Dividend.PolicyAKId = EXP_sup_state.PolicyAKId AND LKP_Dividend.StateCode = EXP_sup_state.StateCode
),
RTR_Target AS (
	SELECT
	o_ChangeFlag AS ChangeFlag,
	o_DividendId AS DividendId,
	PolicyAKId,
	DividendAmount,
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
	o_ModifiedDate AS ModifiedDate
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
	DividendAmount AS DIVIDENDPAIDAMOUNT
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
	ModifiedDate
	FROM RTR_Target_UPDATE
),
Dividend_UPDATE AS (
	MERGE INTO Dividend AS T
	USING UPD_UPDATE AS S
	ON T.DividendId = S.DividendId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditID = S.AuditID, T.ModifiedDate = S.ModifiedDate, T.DividendPayableAmount = S.DividendAmount, T.DividendPlan = S.DividendPlan, T.DividendType = S.DividendType, T.SupStateId = S.SupStateId, T.SupDividendTypeId = S.SupDividendTypeId, T.DividendPaidAmount = S.DividendAmount
),