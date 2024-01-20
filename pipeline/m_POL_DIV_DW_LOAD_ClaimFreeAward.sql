WITH
SQ_PVACCTHistStage AS (
	SELECT
		PVACCTHistStageID,
		ExtractDate,
		SourceSyStemID,
		PvahPolicyNumber,
		PvahExpYYYY,
		PvahExpMM,
		PvahMod,
		PvahHHPolicyNumber,
		PvahHHExpYYMM,
		PvahSymbol,
		PvahRenewalCode,
		PvahTerm,
		PvahPremHome,
		PvahPremAuto,
		PvahPremUMB,
		PvahPremIM,
		PvahPremAutoBI,
		PvahPremAutoPD,
		PvahPremAutoColl,
		PvahPremAutoComp,
		PvahCashApplied1,
		PvahPaymntType1,
		PvahYYMDApplied1,
		PvahCashApplied2,
		PvahPaymntType2,
		PvahYYMDApplied2,
		PvahCashApplied3,
		PvahPaymntType3,
		PvahYYMDApplied3,
		PvahCashApplied4,
		PvahPaymntType4,
		PvahYYMDApplied4,
		PvahCashApplied5,
		PvahPaymntType5,
		PvahYYMDApplied5,
		PvahBCMSAcctNumber,
		PvahBCMSSUBAcctNum,
		PvahBCMSBillingEntity,
		PvahBCMSBillingType,
		PvahBCMSBillingClass,
		PvahSYS38MaintCD,
		PvahState,
		PvahAgency,
		PvahName,
		PvahPremDFire
	FROM PVACCTHistStage
	WHERE PVACCTHistStage.ExtractDate>='@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_GetValues AS (
	SELECT
	PvahPolicyNumber AS i_PvahPolicyNumber,
	PvahMod AS i_PvahMod,
	PvahSymbol AS i_PvahSymbol,
	PvahCashApplied1 AS i_PvahCashApplied1,
	PvahPaymntType1 AS i_PvahPaymntType1,
	PvahYYMDApplied1 AS i_PvahYYMDApplied1,
	PvahCashApplied2 AS i_PvahCashApplied2,
	PvahPaymntType2 AS i_PvahPaymntType2,
	PvahYYMDApplied2 AS i_PvahYYMDApplied2,
	PvahCashApplied3 AS i_PvahCashApplied3,
	PvahPaymntType3 AS i_PvahPaymntType3,
	PvahYYMDApplied3 AS i_PvahYYMDApplied3,
	PvahCashApplied4 AS i_PvahCashApplied4,
	PvahPaymntType4 AS i_PvahPaymntType4,
	PvahYYMDApplied4 AS i_PvahYYMDApplied4,
	PvahCashApplied5 AS i_PvahCashApplied5,
	PvahPaymntType5 AS i_PvahPaymntType5,
	PvahYYMDApplied5 AS i_PvahYYMDApplied5,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(i_PvahPolicyNumber)))
	UDF_DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(i_PvahPolicyNumber))) AS v_PvahPolicyNumber,
	-- *INF*: LPAD(:UDF.DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(i_PvahMod)),2,'0')
	LPAD(UDF_DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(i_PvahMod)), 2, '0') AS v_PvahMod,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(i_PvahSymbol)))
	UDF_DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(i_PvahSymbol))) AS v_PvahSymbol,
	v_PvahSymbol||v_PvahPolicyNumber||v_PvahMod AS o_pol_key,
	i_PvahCashApplied1 AS o_PvahCashApplied1,
	-- *INF*: LTRIM(RTRIM(i_PvahPaymntType1))
	LTRIM(RTRIM(i_PvahPaymntType1)) AS o_PvahPaymntType1,
	-- *INF*: TO_CHAR(i_PvahYYMDApplied1)
	TO_CHAR(i_PvahYYMDApplied1) AS o_PvahYYMDApplied1,
	i_PvahCashApplied2 AS o_PvahCashApplied2,
	-- *INF*: LTRIM(RTRIM(i_PvahPaymntType2))
	LTRIM(RTRIM(i_PvahPaymntType2)) AS o_PvahPaymntType2,
	-- *INF*: TO_CHAR(i_PvahYYMDApplied2)
	TO_CHAR(i_PvahYYMDApplied2) AS o_PvahYYMDApplied2,
	i_PvahCashApplied3 AS o_PvahCashApplied3,
	-- *INF*: LTRIM(RTRIM(i_PvahPaymntType3))
	LTRIM(RTRIM(i_PvahPaymntType3)) AS o_PvahPaymntType3,
	-- *INF*: TO_CHAR(i_PvahYYMDApplied3)
	TO_CHAR(i_PvahYYMDApplied3) AS o_PvahYYMDApplied3,
	i_PvahCashApplied4 AS o_PvahCashApplied4,
	-- *INF*: LTRIM(RTRIM(i_PvahPaymntType4))
	LTRIM(RTRIM(i_PvahPaymntType4)) AS o_PvahPaymntType4,
	-- *INF*: TO_CHAR(i_PvahYYMDApplied4)
	TO_CHAR(i_PvahYYMDApplied4) AS o_PvahYYMDApplied4,
	i_PvahCashApplied5 AS o_PvahCashApplied5,
	-- *INF*: LTRIM(RTRIM(i_PvahPaymntType5))
	LTRIM(RTRIM(i_PvahPaymntType5)) AS o_PvahPaymntType5,
	-- *INF*: TO_CHAR(i_PvahYYMDApplied5)
	TO_CHAR(i_PvahYYMDApplied5) AS o_PvahYYMDApplied5
	FROM SQ_PVACCTHistStage
),
LKP_policy AS (
	SELECT
	pol_ak_id,
	pol_key
	FROM (
		SELECT 
			pol_ak_id,
			pol_key
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE crrnt_snpsht_flag=1 and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and exists (
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PVACCTHistStage
		where LTRIM(RTRIM(PVACCTHistStage.PvahSymbol))=policy.pol_sym
		and LTRIM(RTRIM(PVACCTHistStage.PvahPolicyNumber))=policy.pol_num)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id) = 1
),
EXP_UpdateInvalid AS (
	SELECT
	LKP_policy.pol_ak_id AS i_pol_ak_id,
	EXP_GetValues.o_PvahCashApplied1 AS i_PvahCashApplied1,
	EXP_GetValues.o_PvahPaymntType1 AS i_PvahPaymntType1,
	EXP_GetValues.o_PvahYYMDApplied1 AS i_PvahYYMDApplied1,
	EXP_GetValues.o_PvahCashApplied2 AS i_PvahCashApplied2,
	EXP_GetValues.o_PvahPaymntType2 AS i_PvahPaymntType2,
	EXP_GetValues.o_PvahYYMDApplied2 AS i_PvahYYMDApplied2,
	EXP_GetValues.o_PvahCashApplied3 AS i_PvahCashApplied3,
	EXP_GetValues.o_PvahPaymntType3 AS i_PvahPaymntType3,
	EXP_GetValues.o_PvahYYMDApplied3 AS i_PvahYYMDApplied3,
	EXP_GetValues.o_PvahCashApplied4 AS i_PvahCashApplied4,
	EXP_GetValues.o_PvahPaymntType4 AS i_PvahPaymntType4,
	EXP_GetValues.o_PvahYYMDApplied4 AS i_PvahYYMDApplied4,
	EXP_GetValues.o_PvahCashApplied5 AS i_PvahCashApplied5,
	EXP_GetValues.o_PvahPaymntType5 AS i_PvahPaymntType5,
	EXP_GetValues.o_PvahYYMDApplied5 AS i_PvahYYMDApplied5,
	i_PvahCashApplied1 AS v_PvahCashApplied1,
	i_PvahPaymntType1 AS v_PvahPaymntType1,
	i_PvahYYMDApplied1 AS v_PvahYYMDApplied1,
	-- *INF*: IIF(v_PvahCashApplied1=0 and v_PvahYYMDApplied1='0' and v_PvahPaymntType1='',0,i_PvahCashApplied2)
	IFF(
	    v_PvahCashApplied1 = 0 and v_PvahYYMDApplied1 = '0' and v_PvahPaymntType1 = '', 0,
	    i_PvahCashApplied2
	) AS v_PvahCashApplied2,
	-- *INF*: IIF(v_PvahCashApplied1=0 and v_PvahYYMDApplied1='0' and v_PvahPaymntType1='','',i_PvahPaymntType2)
	IFF(
	    v_PvahCashApplied1 = 0 and v_PvahYYMDApplied1 = '0' and v_PvahPaymntType1 = '', '',
	    i_PvahPaymntType2
	) AS v_PvahPaymntType2,
	-- *INF*: IIF(v_PvahCashApplied1=0 and v_PvahYYMDApplied1='0' and v_PvahPaymntType1='','0',i_PvahYYMDApplied2)
	IFF(
	    v_PvahCashApplied1 = 0 and v_PvahYYMDApplied1 = '0' and v_PvahPaymntType1 = '', '0',
	    i_PvahYYMDApplied2
	) AS v_PvahYYMDApplied2,
	-- *INF*: IIF(v_PvahCashApplied2=0 and v_PvahYYMDApplied2='0' and RTRIM(v_PvahPaymntType2)='',0,i_PvahCashApplied3)
	IFF(
	    v_PvahCashApplied2 = 0 and v_PvahYYMDApplied2 = '0' and RTRIM(v_PvahPaymntType2) = '', 0,
	    i_PvahCashApplied3
	) AS v_PvahCashApplied3,
	-- *INF*: IIF(v_PvahCashApplied2=0 and v_PvahYYMDApplied2='0' and RTRIM(v_PvahPaymntType2)='','',i_PvahPaymntType3)
	IFF(
	    v_PvahCashApplied2 = 0 and v_PvahYYMDApplied2 = '0' and RTRIM(v_PvahPaymntType2) = '', '',
	    i_PvahPaymntType3
	) AS v_PvahPaymntType3,
	-- *INF*: IIF(v_PvahCashApplied2=0 and v_PvahYYMDApplied2='0' and RTRIM(v_PvahPaymntType2)='','0',i_PvahYYMDApplied3)
	IFF(
	    v_PvahCashApplied2 = 0 and v_PvahYYMDApplied2 = '0' and RTRIM(v_PvahPaymntType2) = '', '0',
	    i_PvahYYMDApplied3
	) AS v_PvahYYMDApplied3,
	-- *INF*: IIF(v_PvahCashApplied3=0 and v_PvahYYMDApplied3='0' and RTRIM(v_PvahPaymntType3)='',0,i_PvahCashApplied4)
	IFF(
	    v_PvahCashApplied3 = 0 and v_PvahYYMDApplied3 = '0' and RTRIM(v_PvahPaymntType3) = '', 0,
	    i_PvahCashApplied4
	) AS v_PvahCashApplied4,
	-- *INF*: IIF(v_PvahCashApplied3=0 and v_PvahYYMDApplied3='0' and RTRIM(v_PvahPaymntType3)='','',i_PvahPaymntType4)
	IFF(
	    v_PvahCashApplied3 = 0 and v_PvahYYMDApplied3 = '0' and RTRIM(v_PvahPaymntType3) = '', '',
	    i_PvahPaymntType4
	) AS v_PvahPaymntType4,
	-- *INF*: IIF(v_PvahCashApplied3=0 and v_PvahYYMDApplied3='0' and RTRIM(v_PvahPaymntType3)='','0',i_PvahYYMDApplied4)
	IFF(
	    v_PvahCashApplied3 = 0 and v_PvahYYMDApplied3 = '0' and RTRIM(v_PvahPaymntType3) = '', '0',
	    i_PvahYYMDApplied4
	) AS v_PvahYYMDApplied4,
	-- *INF*: IIF(v_PvahCashApplied4=0 and v_PvahYYMDApplied4='0' and RTRIM(v_PvahPaymntType4)='',0,i_PvahCashApplied5)
	IFF(
	    v_PvahCashApplied4 = 0 and v_PvahYYMDApplied4 = '0' and RTRIM(v_PvahPaymntType4) = '', 0,
	    i_PvahCashApplied5
	) AS v_PvahCashApplied5,
	-- *INF*: IIF(v_PvahCashApplied4=0 and v_PvahYYMDApplied4='0' and RTRIM(v_PvahPaymntType4)='','',i_PvahPaymntType5)
	IFF(
	    v_PvahCashApplied4 = 0 and v_PvahYYMDApplied4 = '0' and RTRIM(v_PvahPaymntType4) = '', '',
	    i_PvahPaymntType5
	) AS v_PvahPaymntType5,
	-- *INF*: IIF(v_PvahCashApplied4=0 and v_PvahYYMDApplied4='0' and RTRIM(v_PvahPaymntType4)='','0',i_PvahYYMDApplied5)
	IFF(
	    v_PvahCashApplied4 = 0 and v_PvahYYMDApplied4 = '0' and RTRIM(v_PvahPaymntType4) = '', '0',
	    i_PvahYYMDApplied5
	) AS v_PvahYYMDApplied5,
	-- *INF*: IIF(ISNULL(i_pol_ak_id),-1,i_pol_ak_id)
	IFF(i_pol_ak_id IS NULL, - 1, i_pol_ak_id) AS o_pol_ak_id,
	v_PvahCashApplied1 AS o_PvahCashApplied1,
	v_PvahPaymntType1 AS o_PvahPaymntType1,
	v_PvahYYMDApplied1 AS o_PvahYYMDApplied1,
	v_PvahCashApplied2 AS o_PvahCashApplied2,
	v_PvahPaymntType2 AS o_PvahPaymntType2,
	v_PvahYYMDApplied2 AS o_PvahYYMDApplied2,
	v_PvahCashApplied3 AS o_PvahCashApplied3,
	v_PvahPaymntType3 AS o_PvahPaymntType3,
	v_PvahYYMDApplied3 AS o_PvahYYMDApplied3,
	v_PvahCashApplied4 AS o_PvahCashApplied4,
	v_PvahPaymntType4 AS o_PvahPaymntType4,
	v_PvahYYMDApplied4 AS o_PvahYYMDApplied4,
	v_PvahCashApplied5 AS o_PvahCashApplied5,
	v_PvahPaymntType5 AS o_PvahPaymntType5,
	v_PvahYYMDApplied5 AS o_PvahYYMDApplied5
	FROM EXP_GetValues
	LEFT JOIN LKP_policy
	ON LKP_policy.pol_key = EXP_GetValues.o_pol_key
),
NRM_TRANS AS (
),
FIL_RemoveInvalid AS (
	SELECT
	pol_ak_id, 
	PvahCashApplied, 
	PvahPaymntType, 
	PvahYYMDApplied
	FROM NRM_TRANS
	WHERE PvahCashApplied>0 and PvahYYMDApplied>'0' and RTRIM(PvahPaymntType)>'' and IIF(IS_DATE(PvahYYMDApplied,'YYYYMMDD'),PvahYYMDApplied,'0')>='20010101'
and pol_ak_id<>-1
),
AGG_SUM AS (
	SELECT
	pol_ak_id,
	PvahCashApplied,
	-- *INF*: ROUND(SUM(PvahCashApplied),2)
	ROUND(SUM(PvahCashApplied), 2) AS sum_PvahCashApplied,
	PvahPaymntType,
	PvahYYMDApplied
	FROM FIL_RemoveInvalid
	GROUP BY pol_ak_id, PvahPaymntType, PvahYYMDApplied
),
EXP_ClaimFreeAwardType AS (
	SELECT
	pol_ak_id AS i_pol_ak_id,
	sum_PvahCashApplied AS i_PvahCashApplied,
	PvahPaymntType AS i_PvahPaymntType,
	PvahYYMDApplied AS i_PvahYYMDApplied,
	i_pol_ak_id AS o_PolicyAKId,
	i_PvahCashApplied AS o_ClaimFreeAwardAmount,
	-- *INF*: DECODE(
	-- i_PvahPaymntType,
	-- 'L','CLAIM',
	-- 'P','PREMIUM',
	-- 'C','CLOSOUT',
	-- 'M','MOVEPV',
	-- 'N','GETPV',
	-- 'Q','LIQ-CHK',
	-- 'W','LIQ-WOFF',
	-- 'K','CHECK',
	-- 'Z','MANUAL-DISB',
	-- i_PvahPaymntType
	-- )
	DECODE(
	    i_PvahPaymntType,
	    'L', 'CLAIM',
	    'P', 'PREMIUM',
	    'C', 'CLOSOUT',
	    'M', 'MOVEPV',
	    'N', 'GETPV',
	    'Q', 'LIQ-CHK',
	    'W', 'LIQ-WOFF',
	    'K', 'CHECK',
	    'Z', 'MANUAL-DISB',
	    i_PvahPaymntType
	) AS o_ClaimFreeAwardType,
	-- *INF*: TO_DATE( i_PvahYYMDApplied,'YYYYMMDD')
	TO_TIMESTAMP(i_PvahYYMDApplied, 'YYYYMMDD') AS o_ClaimFreeAwardTransactionEnteredDate
	FROM AGG_SUM
),
LKP_ClaimFreeAward AS (
	SELECT
	ClaimFreeAwardID,
	ClaimFreeAwardAKId,
	ClaimFreeAwardAmount,
	ClaimFreeAwardRunDate,
	PolicyAKId,
	ClaimFreeAwardType,
	ClaimFreeAwardTransactionEnteredDate
	FROM (
		SELECT 
			ClaimFreeAwardID,
			ClaimFreeAwardAKId,
			ClaimFreeAwardAmount,
			ClaimFreeAwardRunDate,
			PolicyAKId,
			ClaimFreeAwardType,
			ClaimFreeAwardTransactionEnteredDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ClaimFreeAward
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId,ClaimFreeAwardType,ClaimFreeAwardTransactionEnteredDate ORDER BY ClaimFreeAwardID) = 1
),
EXP_ChangeFlag AS (
	SELECT
	LKP_ClaimFreeAward.ClaimFreeAwardID AS lkp_ClaimFreeAwardID,
	LKP_ClaimFreeAward.ClaimFreeAwardAKId AS lkp_ClaimFreeAwardAKId,
	LKP_ClaimFreeAward.ClaimFreeAwardAmount AS lkp_ClaimFreeAwardAmount,
	LKP_ClaimFreeAward.ClaimFreeAwardRunDate AS lkp_ClaimFreeAwardRunDate,
	EXP_ClaimFreeAwardType.o_PolicyAKId AS PolicyAKId,
	EXP_ClaimFreeAwardType.o_ClaimFreeAwardAmount AS ClaimFreeAwardAmount,
	EXP_ClaimFreeAwardType.o_ClaimFreeAwardType AS ClaimFreeAwardType,
	EXP_ClaimFreeAwardType.o_ClaimFreeAwardTransactionEnteredDate AS ClaimFreeAwardTransactionEnteredDate,
	-- *INF*: LAST_DAY(ClaimFreeAwardTransactionEnteredDate)
	LAST_DAY(ClaimFreeAwardTransactionEnteredDate) AS v_ClaimFreeAwardRunDate,
	-- *INF*: IIF(ISNULL(lkp_ClaimFreeAwardAKId),'NEW',IIF(
	-- ClaimFreeAwardAmount<>lkp_ClaimFreeAwardAmount OR lkp_ClaimFreeAwardRunDate <> v_ClaimFreeAwardRunDate,'UPDATE','NOCHANGE'))
	IFF(
	    lkp_ClaimFreeAwardAKId IS NULL, 'NEW',
	    IFF(
	        ClaimFreeAwardAmount <> lkp_ClaimFreeAwardAmount
	        or lkp_ClaimFreeAwardRunDate <> v_ClaimFreeAwardRunDate,
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS o_Change_Flag,
	v_ClaimFreeAwardRunDate AS o_ClaimFreeAwardRunDate,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800-01-01 01:00:00', 'YYYY-MM-DD HH24:MI:SS')
	TO_TIMESTAMP('1800-01-01 01:00:00', 'YYYY-MM-DD HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
	TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate
	FROM EXP_ClaimFreeAwardType
	LEFT JOIN LKP_ClaimFreeAward
	ON LKP_ClaimFreeAward.PolicyAKId = EXP_ClaimFreeAwardType.o_PolicyAKId AND LKP_ClaimFreeAward.ClaimFreeAwardType = EXP_ClaimFreeAwardType.o_ClaimFreeAwardType AND LKP_ClaimFreeAward.ClaimFreeAwardTransactionEnteredDate = EXP_ClaimFreeAwardType.o_ClaimFreeAwardTransactionEnteredDate
),
RTR_Target AS (
	SELECT
	o_Change_Flag AS Change_Flag,
	lkp_ClaimFreeAwardID AS ClaimFreeAwardID,
	lkp_ClaimFreeAwardAKId AS ClaimFreeAwardAKId,
	PolicyAKId,
	ClaimFreeAwardAmount,
	ClaimFreeAwardType,
	ClaimFreeAwardTransactionEnteredDate,
	o_ClaimFreeAwardRunDate AS ClaimFreeAwardRunDate,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditID AS AuditID,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_SourceSystemId AS SourceSystemId,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate
	FROM EXP_ChangeFlag
),
RTR_Target_INSERT AS (SELECT * FROM RTR_Target WHERE Change_Flag='NEW'),
RTR_Target_UPDATE AS (SELECT * FROM RTR_Target WHERE Change_Flag='UPDATE'),
UPD_UPDATE AS (
	SELECT
	ClaimFreeAwardID, 
	ClaimFreeAwardAmount, 
	ClaimFreeAwardRunDate, 
	AuditID, 
	ModifiedDate
	FROM RTR_Target_UPDATE
),
ClaimFreeAward_UPDATE AS (
	MERGE INTO ClaimFreeAward AS T
	USING UPD_UPDATE AS S
	ON T.ClaimFreeAwardID = S.ClaimFreeAwardID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditID = S.AuditID, T.ModifiedDate = S.ModifiedDate, T.ClaimFreeAwardAmount = S.ClaimFreeAwardAmount, T.ClaimFreeAwardRunDate = S.ClaimFreeAwardRunDate
),
SEQ_ClaimFreeAwardAKId AS (
	CREATE SEQUENCE SEQ_ClaimFreeAwardAKId
	START = 1
	INCREMENT = 1;
),
ClaimFreeAward_INSERT AS (
	INSERT INTO ClaimFreeAward
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, ClaimFreeAwardAKId, PolicyAKId, ClaimFreeAwardAmount, ClaimFreeAwardType, ClaimFreeAwardTransactionEnteredDate, ClaimFreeAwardRunDate)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SourceSystemId AS SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	SEQ_ClaimFreeAwardAKId.NEXTVAL AS CLAIMFREEAWARDAKID, 
	POLICYAKID, 
	CLAIMFREEAWARDAMOUNT, 
	CLAIMFREEAWARDTYPE, 
	CLAIMFREEAWARDTRANSACTIONENTEREDDATE, 
	CLAIMFREEAWARDRUNDATE
	FROM RTR_Target_INSERT
),