WITH
LKP_Policy AS (
	SELECT
	pol_ak_id,
	pol_num,
	i_pol_num,
	pol_mod,
	i_pol_mod
	FROM (
		SELECT 
			pol_ak_id,
			pol_num,
			i_pol_num,
			pol_mod,
			i_pol_mod
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE source_sys_id  = 'DCT' and crrnt_snpsht_flag = '1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_num,pol_mod ORDER BY pol_ak_id) = 1
),
SQ_WBWCDCTDividendStage AS (
	Select 
	WBWCD.DividendType, 
	WBWCD.DividendOptions, 
	DCP.PolicyNumber, 
	DCT.HistoryID,
	DCS.Purpose, 
	DCT.Type,
	WBWCL.DividendPrior, 
	WBWCL.DividendChange, 
	WBWCL.DividendPremium, 
	WBWCD.DividendPaid, 
	wbwcl.DividendPaidDate,
	DCS.Createdatetime, 
	DCS.SessionID,
	max(DCS.SessionID) over(partition by DCT.HistoryID,DCS.Purpose, ISNULL(WBWCD.State,DCWCL.PrimaryLocationState),DCT.Type) MaxSessionId,
	WBWCD.State,
	DCWCL.PrimaryLocationState,
	wbp.PolicyVersionFormatted,
	WBWCD.ManualDividendCalculation,
	WBWCD.ExtractDate as TransactionPostedDate
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPolicyStaging DCP
	join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WBPolicyStaging WBP on DCP.SessionId=WBP.SessionId and DCP.PolicyId=WBP.PolicyId 
	join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTransactionStaging DCT on DCT.SessionId=DCP.SessionId  
	and DCT.State='Committed' and DCT.type like '%Dividend'
	join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLineStaging DCL on WBP.SessionID = DCL.SessionID and WBP.PolicyID = DCL.PolicyID and DCL.Type = 'WorkersCompensation'
	join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.DCWCLineStaging DCWCL on DCL.SessionID = DCWCL.SessionID and DCL.LineID = DCWCL.LineID
	join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WBWCLineStage WBWCL on WBWCL.SessionId = DCWCL.SessionId and WBWCL.WCLineId = DCWCL.WC_LineId 
	join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WBWCDividendStage WBWCD on WBWCL.SessionId=WBWCD.SessionId  and WBWCD.WCLineId = WBWCL.WCLineId
	join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.DCSessionStaging DCS on dcs.SessionId = DCT.SessionId
	@{pipeline().parameters.WHERE_CLAUSE}
	  order by  DCT.SessionId
),
FLT_Remove_Invalid_Policies AS (
	SELECT
	CreateDateTime AS TransactionDate, 
	DividendType, 
	DividendOption, 
	PolicyNumber, 
	HistoryID, 
	Purpose, 
	Type, 
	DividendPrior, 
	DividendChange, 
	DividendPremium, 
	DividendPaid, 
	DividendPaidDate, 
	SessionId, 
	MaxSessionId, 
	Dividend_state, 
	Line_state, 
	PolicyVersionFormatted, 
	ManualDividendCalculation, 
	TransactionPostedDate
	FROM SQ_WBWCDCTDividendStage
	WHERE LENGTH(PolicyNumber)=7  and SessionId = MaxSessionId
),
EXP_GetValue AS (
	SELECT
	TransactionDate AS i_TransactionDate,
	DividendType AS i_DividendType,
	DividendOption AS i_DividendOption,
	HistoryID AS i_HistoryID,
	Purpose AS i_Purpose,
	Type AS i_Type,
	DividendPrior AS i_DividendPrior,
	DividendChange AS i_DividendChange,
	DividendPremium AS i_DividendPremium,
	DividendPaid AS i_DividendPaid,
	Dividend_state AS i_Dividend_state,
	Line_state AS i_Line_state,
	PolicyNumber AS i_PolicyNumber,
	PolicyVersionFormatted AS i_PolicyVersionFormatted,
	ManualDividendCalculation AS i_ManualDividendCalculation,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(iif (not isnull(i_Dividend_state),i_Dividend_state,i_Line_state))))
	UDF_DEFAULT_VALUE_FOR_STRINGS(LTRIM(RTRIM(
	            IFF(
	                i_Dividend_state IS NOT NULL, i_Dividend_state, i_Line_state
	            )))) AS v_PrimaryLocationState,
	-- *INF*: :LKP.LKP_POLICY(i_PolicyNumber,i_PolicyVersionFormatted)
	LKP_POLICY_i_PolicyNumber_i_PolicyVersionFormatted.pol_ak_id AS v_pol_ak_id,
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
	-- *INF*: IIF(i_ManualDividendCalculation='T',1,0)
	IFF(i_ManualDividendCalculation = 'T', 1, 0) AS v_ManualDividendCalculation,
	-- *INF*: iif(not isnull(i_DividendChange),i_DividendChange, 0.00)
	IFF(i_DividendChange IS NOT NULL, i_DividendChange, 0.00) AS v_DividendChange,
	-- *INF*: iif(not isnull(i_DividendPremium),i_DividendPremium, 0.00)
	IFF(i_DividendPremium IS NOT NULL, i_DividendPremium, 0.00) AS v_DividendPremium,
	-- *INF*: iif(not isnull(i_DividendPaid),i_DividendPaid, 0.00)
	IFF(i_DividendPaid IS NOT NULL, i_DividendPaid, 0.00) AS v_DividendPaid,
	-- *INF*: decode(TRUE, i_Type = 'Dividend' and i_Purpose = 'Onset' ,v_DividendPremium,
	-- 	 i_Type = 'Dividend' and i_Purpose = 'Offset', v_DividendPremium * -1, i_Type = 'RevisedDividend' and i_Purpose = 'Onset', v_DividendChange, i_Type = 'RevisedDividend' and i_Purpose = 'Offset' , v_DividendChange * -1,
	--  i_Type = 'VoidDividend' , 0 )
	decode(
	    TRUE,
	    i_Type = 'Dividend' and i_Purpose = 'Onset', v_DividendPremium,
	    i_Type = 'Dividend' and i_Purpose = 'Offset', v_DividendPremium * - 1,
	    i_Type = 'RevisedDividend' and i_Purpose = 'Onset', v_DividendChange,
	    i_Type = 'RevisedDividend' and i_Purpose = 'Offset', v_DividendChange * - 1,
	    i_Type = 'VoidDividend', 0
	) AS v_DividendPaidAmount,
	DividendPaidDate AS i_DividendPaidDate,
	-- *INF*: iif(not isnull(v_pol_ak_id),v_pol_ak_id,-1)
	IFF(v_pol_ak_id IS NOT NULL, v_pol_ak_id, - 1) AS o_pol_ak_id,
	v_DividendPaidAmount AS o_DividendPaidAmount,
	-- *INF*: TO_DATE(TO_CHAR(
	-- IIF(ISNULL(i_TransactionDate),TO_DATE('1800-01-01 00:00:00.000','YYYY-MM-DD HH24:MI:SS.MS'),i_TransactionDate)
	-- ,'YYYYMMDD'),'YYYYMMDD')
	TO_TIMESTAMP(TO_CHAR(
	        IFF(
	            i_TransactionDate IS NULL,
	            TO_TIMESTAMP('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.MS'),
	            i_TransactionDate
	        ), 'YYYYMMDD'), 'YYYYMMDD') AS o_TransactionDate,
	v_PrimaryLocationState AS o_PrimaryLocationState,
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
	i_HistoryID AS o_HistoryID,
	i_Purpose AS o_Purpose,
	i_Type AS o_Type,
	i_DividendPrior AS o_DividendPrior,
	i_DividendChange AS o_DividendChange,
	i_DividendPremium AS o_DividendPremium,
	-- *INF*: IIF(ISNULL(i_DividendPaidDate),TO_DATE('1800-01-01 00:00:00.000','YYYY-MM-DD HH24:MI:SS.MS'),i_DividendPaidDate)
	IFF(
	    i_DividendPaidDate IS NULL,
	    TO_TIMESTAMP('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.MS'),
	    i_DividendPaidDate
	) AS o_DividendPaidDate,
	TransactionPostedDate,
	SessionId
	FROM FLT_Remove_Invalid_Policies
	LEFT JOIN LKP_POLICY LKP_POLICY_i_PolicyNumber_i_PolicyVersionFormatted
	ON LKP_POLICY_i_PolicyNumber_i_PolicyVersionFormatted.pol_num = i_PolicyNumber
	AND LKP_POLICY_i_PolicyNumber_i_PolicyVersionFormatted.pol_mod = i_PolicyVersionFormatted

),
mplt_Evaluate_DCTDividend AS (WITH
	INPUT_DCTDividend AS (
		
	),
	EXP_Passthrough AS (
		SELECT
		pol_ak_id,
		DividendPaidAmount,
		TransactionEnteredDate,
		TransactionPostedDate,
		PrimaryLocationState,
		DividendType,
		DividendOption,
		HistoryID,
		Purpose,
		Type,
		DividendPrior,
		DividendChange,
		DividendPremium,
		DividendPaidDate,
		SessionId,
		PolicyKey
		FROM INPUT_DCTDividend
	),
	AGG_RemoveDuplicate AS (
		SELECT
		pol_ak_id,
		DividendPaidAmount,
		TransactionEnteredDate,
		TransactionPostedDate,
		PrimaryLocationState,
		DividendType,
		DividendOption,
		HistoryID,
		Purpose,
		Type,
		DividendPrior,
		DividendChange,
		DividendPremium,
		DividendPaidDate,
		SessionId,
		PolicyKey
		FROM EXP_Passthrough
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id, TransactionEnteredDate, TransactionPostedDate, PrimaryLocationState, HistoryID, Purpose, Type, SessionId, PolicyKey ORDER BY NULL) = 1
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
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PMSStateCode,DividendType,DividendPlan ORDER BY SupDividendTypeID DESC) = 1
	),
	EXP_PlanandType AS (
		SELECT
		AGG_RemoveDuplicate.pol_ak_id AS i_pol_ak_id,
		AGG_RemoveDuplicate.DividendPaidAmount AS i_DividendPaidAmount,
		AGG_RemoveDuplicate.TransactionEnteredDate AS i_TransactionDate,
		AGG_RemoveDuplicate.TransactionPostedDate,
		AGG_RemoveDuplicate.PrimaryLocationState AS i_PrimaryLocationState,
		LKP_SupDividendType.SupDividendTypeID AS lkp_SupDividendTypeID,
		LKP_SupDividendType.PMSStateCode AS lkp_PMSStateCode,
		LKP_SupDividendType.DividendType AS lkp_DividendType,
		LKP_SupDividendType.DividendPlan AS lkp_DividendPlan,
		LKP_SupDividendType.StandardDividendType AS lkp_StandardDividendType,
		LKP_SupDividendType.StandardDividendPlan AS lkp_StandardDividendPlan,
		i_pol_ak_id AS o_PolicyAKId,
		-- *INF*: IIF(ISNULL(i_DividendPaidAmount),0,i_DividendPaidAmount)
		IFF(i_DividendPaidAmount IS NULL, 0, i_DividendPaidAmount) AS o_DividendPaidAmount,
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
		IFF(lkp_SupDividendTypeID IS NULL, - 1, lkp_SupDividendTypeID) AS o_SupDividendTypeId,
		AGG_RemoveDuplicate.HistoryID AS i_HistoryID,
		AGG_RemoveDuplicate.Purpose AS i_Purpose,
		AGG_RemoveDuplicate.Type AS i_Type,
		AGG_RemoveDuplicate.DividendPrior AS i_DividendPrior,
		AGG_RemoveDuplicate.DividendChange AS i_DividendChange,
		AGG_RemoveDuplicate.DividendPremium AS i_DividendPremium,
		AGG_RemoveDuplicate.DividendPaidDate AS i_DividendPaidDate,
		AGG_RemoveDuplicate.SessionId,
		AGG_RemoveDuplicate.PolicyKey
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
			s.sup_state_id as sup_state_id, 
			s.state_code as state_code FROM 
			@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state s
			WHERE s.crrnt_snpsht_flag = 1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY sup_state_id DESC) = 1
	),
	EXP_sup_state AS (
		SELECT
		EXP_PlanandType.o_PolicyAKId AS PolicyAKId,
		EXP_PlanandType.o_DividendPaidAmount AS DividendPaidAmount,
		EXP_PlanandType.o_DividendTransactionEnteredDate AS DividendTransactionEnteredDate,
		EXP_PlanandType.o_StateCode AS StateCode,
		EXP_PlanandType.o_DividendPlan AS DividendPlan,
		EXP_PlanandType.o_DividendType AS DividendType,
		EXP_PlanandType.o_SupDividendTypeId AS SupDividendTypeId,
		EXP_PlanandType.i_HistoryID AS HistoryID,
		EXP_PlanandType.i_Purpose AS Purpose,
		EXP_PlanandType.i_Type AS Type,
		EXP_PlanandType.i_DividendPrior AS DividendPrior,
		EXP_PlanandType.i_DividendChange AS DividendChange,
		EXP_PlanandType.i_DividendPremium AS DividendPremium,
		EXP_PlanandType.i_DividendPaidDate AS DividendPaidDate,
		LKP_sup_state.sup_state_id AS lkp_sup_state_id,
		-- *INF*: IIF(ISNULL(lkp_sup_state_id),-1,lkp_sup_state_id)
		IFF(lkp_sup_state_id IS NULL, - 1, lkp_sup_state_id) AS o_sup_state_id,
		EXP_PlanandType.SessionId,
		EXP_PlanandType.PolicyKey,
		EXP_PlanandType.TransactionPostedDate
		FROM EXP_PlanandType
		LEFT JOIN LKP_sup_state
		ON LKP_sup_state.state_code = EXP_PlanandType.o_StateCode
	),
	LKP_DCTDividend_find_prior AS (
		SELECT
		DCTDividendId,
		DCTDividendAKId,
		DividendTransactionEnteredDate,
		DividendPaidAmount,
		DCTDividendPriorAmount,
		DCTDividendChangeAmount,
		DCTDividendPremiumAmount,
		DCTDividendPaidDate,
		DCTDividendHistoryId,
		StateCode,
		DCTDividendPurpose
		FROM (
			SELECT 
			DCTDividend.DCTDividendId as DCTDividendId, 
			DCTDividend.DCTDividendAKId as DCTDividendAKId, 
			DCTDividend.DividendTransactionEnteredDate as DividendTransactionEnteredDate, DCTDividend.DividendTransactionPostedDate as DividendTransactionPostedDate, DCTDividend.DividendPaidAmount as DividendPaidAmount, 
			DCTDividend.DCTDividendPriorAmount as DCTDividendPriorAmount, DCTDividend.DCTDividendChangeAmount as DCTDividendChangeAmount, DCTDividend.DCTDividendPremiumAmount as DCTDividendPremiumAmount, DCTDividend.DCTDividendPaidDate as DCTDividendPaidDate, 
			DCTDividend.DCTDividendHistoryId as DCTDividendHistoryId, 
			DCTDividend.DCTDividendPurpose as DCTDividendPurpose, 
			DCTDividend.StateCode as StateCode 
			FROM DCTDividend
			order by DCTDividendHistoryId,DCTDividendPurpose, DCTDividendAKId --
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY DCTDividendHistoryId,StateCode,DCTDividendPurpose ORDER BY DCTDividendId DESC) = 1
	),
	EXP_Collect_Original_Build_Deprecated AS (
		SELECT
		EXP_sup_state.PolicyAKId,
		EXP_sup_state.DividendPaidAmount,
		EXP_sup_state.DividendTransactionEnteredDate,
		EXP_sup_state.StateCode,
		EXP_sup_state.DividendPlan,
		EXP_sup_state.DividendType,
		EXP_sup_state.SupDividendTypeId,
		EXP_sup_state.o_sup_state_id,
		EXP_sup_state.HistoryID,
		EXP_sup_state.Purpose,
		EXP_sup_state.Type,
		EXP_sup_state.DividendPrior,
		EXP_sup_state.DividendChange,
		EXP_sup_state.DividendPremium,
		EXP_sup_state.DividendPaidDate,
		EXP_sup_state.SessionId,
		EXP_sup_state.PolicyKey,
		LKP_DCTDividend_find_prior.DCTDividendId AS lkp_DCTDividendId,
		LKP_DCTDividend_find_prior.DCTDividendAKId AS lkp_DCTDividendAKId,
		lkp_DCTDividendAKId*-1 AS o_lkp_DCTDividendAKId,
		LKP_DCTDividend_find_prior.DividendTransactionEnteredDate AS lkp_DividendTransactionEnteredDate,
		LKP_DCTDividend_find_prior.DividendPaidAmount AS lkp_DividendPaidAmount,
		lkp_DividendPaidAmount *  - 1 AS o_lkp_DividendPaidAmount,
		-- *INF*: decode (TRUE,
		-- Purpose = 'Onset','Offset',
		-- 'Onset')
		decode(
		    TRUE,
		    Purpose = 'Onset', 'Offset',
		    'Onset'
		) AS DerivedPurpose,
		LKP_DCTDividend_find_prior.DCTDividendPriorAmount AS lkp_DCTDividendPriorAmount,
		LKP_DCTDividend_find_prior.DCTDividendChangeAmount AS lkp_DCTDividendChangeAmount,
		LKP_DCTDividend_find_prior.DCTDividendPremiumAmount AS lkp_DCTDividendPremiumAmount,
		LKP_DCTDividend_find_prior.DCTDividendPaidDate AS lkp_DCTDividendPaidDate,
		-- *INF*: DECODE (TRUE,
		-- NOT isnull(lkp_DCTDividendId)  AND Type = 'Dividend'  AND lkp_DCTDividendPremiumAmount != DividendPremium,'Y',
		-- NOT isnull(lkp_DCTDividendId)  AND Type = 'RevisedDividend'  AND lkp_DCTDividendChangeAmount != DividendChange,'Y',
		-- NOT isnull(lkp_DCTDividendId)  AND Type = 'Dividend'  AND lkp_DCTDividendPremiumAmount = DividendPremium,'S',
		-- NOT isnull(lkp_DCTDividendId)  AND Type = 'RevisedDividend'  AND lkp_DCTDividendChangeAmount = DividendChange,'S',
		-- NOT isnull(lkp_DCTDividendId)  AND Type = 'VoidDividend','S',
		-- 'N')
		DECODE(
		    TRUE,
		    lkp_DCTDividendId IS NULL AND Type = 'Dividend' AND lkp_DCTDividendPremiumAmount != DividendPreNOT mium, 'Y',
		    lkp_DCTDividendId IS NULL AND Type = 'RevisedDividend' AND lkp_DCTDividendChangeAmount != DividendChNOT ange, 'Y',
		    lkp_DCTDividendId IS NULL AND Type = 'Dividend' AND lkp_DCTDividendPremiumAmount = DividendPreNOT mium, 'S',
		    lkp_DCTDividendId IS NULL AND Type = 'RevisedDividend' AND lkp_DCTDividendChangeAmount = DividendChNOT ange, 'S',
		    lkp_DCTDividendId IS NULL AND Type = 'VoidDividNOT end', 'S',
		    'N'
		) AS Deprecate_Flag,
		EXP_sup_state.TransactionPostedDate
		FROM EXP_sup_state
		LEFT JOIN LKP_DCTDividend_find_prior
		ON LKP_DCTDividend_find_prior.DCTDividendHistoryId = EXP_sup_state.HistoryID AND LKP_DCTDividend_find_prior.StateCode = EXP_sup_state.StateCode AND LKP_DCTDividend_find_prior.DCTDividendPurpose = EXP_sup_state.Purpose
	),
	RTR_Insert_and_Deprecation AS (
		SELECT
		PolicyAKId,
		DividendPaidAmount,
		DividendTransactionEnteredDate,
		TransactionPostedDate AS DividendTransactionPostedDate,
		StateCode,
		DividendPlan,
		DividendType,
		SupDividendTypeId,
		o_sup_state_id,
		HistoryID,
		Purpose,
		Type,
		DividendPrior,
		DividendChange,
		DividendPremium,
		DividendPaidDate,
		SessionId,
		PolicyKey,
		o_lkp_DCTDividendAKId AS lkp_DCTDividendAKId,
		DerivedPurpose,
		lkp_DividendTransactionEnteredDate,
		o_lkp_DividendPaidAmount AS lkp_DividendPaidAmount,
		lkp_DCTDividendPriorAmount,
		lkp_DCTDividendChangeAmount,
		lkp_DCTDividendPremiumAmount,
		lkp_DCTDividendPaidDate,
		Deprecate_Flag
		FROM EXP_Collect_Original_Build_Deprecated
	),
	RTR_Insert_and_Deprecation_DEPRECATION AS (SELECT * FROM RTR_Insert_and_Deprecation WHERE Deprecate_Flag = 'Y'),
	RTR_Insert_and_Deprecation_SKIP AS (SELECT * FROM RTR_Insert_and_Deprecation WHERE Deprecate_Flag = 'S'),
	RTR_Insert_and_Deprecation_INSERT AS (SELECT * FROM RTR_Insert_and_Deprecation WHERE Deprecate_Flag  !=  'S'),
	Union_Insert_and_Deprecation AS (
		SELECT PolicyAKId, DividendPaidAmount, DividendTransactionEnteredDate, StateCode, DividendPlan, DividendType, SupDividendTypeId, o_sup_state_id AS sup_state_id, HistoryID, Purpose, Type, DividendPrior, DividendChange, DividendPremium, DividendPaidDate, SessionId, PolicyKey, DividendTransactionPostedDate
		FROM 
		UNION
		SELECT PolicyAKId, lkp_DividendPaidAmount AS DividendPaidAmount, lkp_DividendTransactionEnteredDate AS DividendTransactionEnteredDate, StateCode, DividendPlan, DividendType, SupDividendTypeId, o_sup_state_id AS sup_state_id, HistoryID, DerivedPurpose AS Purpose, Type, lkp_DCTDividendPriorAmount AS DividendPrior, lkp_DCTDividendChangeAmount AS DividendChange, lkp_DCTDividendPremiumAmount AS DividendPremium, lkp_DCTDividendPaidDate AS DividendPaidDate, lkp_DCTDividendAKId AS SessionId, PolicyKey, DividendTransactionPostedDate
		FROM 
	),
	EXP_PreTarget AS (
		SELECT
		PolicyAKId,
		DividendPaidAmount,
		DividendTransactionEnteredDate,
		DividendTransactionPostedDate,
		StateCode,
		DividendPlan,
		DividendType,
		SupDividendTypeId,
		sup_state_id,
		HistoryID,
		Purpose,
		Type,
		DividendPrior,
		DividendChange,
		DividendPremium,
		DividendPaidDate,
		SessionId,
		PolicyKey
		FROM Union_Insert_and_Deprecation
	),
	OUTPUT_DCTDividend AS (
		SELECT
		PolicyAKId, 
		DividendPaidAmount, 
		DividendTransactionEnteredDate, 
		DividendTransactionPostedDate, 
		StateCode, 
		DividendPlan, 
		DividendType, 
		SupDividendTypeId, 
		sup_state_id, 
		HistoryID, 
		Purpose, 
		Type, 
		DividendPrior, 
		DividendChange, 
		DividendPremium, 
		DividendPaidDate, 
		SessionId, 
		PolicyKey
		FROM EXP_PreTarget
	),
),
EXP_MetaData AS (
	SELECT
	PolicyAKId,
	DividendPaidAmount1 AS DividendPaidAmount,
	DividendTransactionEnteredDate,
	DividendTransactionPostedDate,
	-- *INF*: ADD_TO_DATE(TRUNC(ADD_TO_DATE(DividendTransactionPostedDate,'MM',1), 'MM'),'DD',-1)
	-- 
	DATEADD(DAY,- 1,CAST(TRUNC(DATEADD(MONTH,1,DividendTransactionPostedDate), 'MONTH') AS TIMESTAMP_NTZ(0))) AS DividendRunDate,
	StateCode,
	DividendPlan,
	DividendType1 AS DividendType,
	SupDividendTypeId,
	sup_state_id,
	HistoryID1 AS HistoryID,
	Purpose1 AS Purpose,
	Type1 AS Type,
	DividendPrior1 AS DividendPrior,
	DividendChange1 AS DividendChange,
	DividendPremium1 AS DividendPremium,
	DividendPaidDate1 AS DividendPaidDate,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	SessionId1 AS o_DCTDividendAKId,
	-- *INF*: TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
	TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
	TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate
	FROM mplt_Evaluate_DCTDividend
),
DCTDividend AS (
	INSERT INTO DCTDividend
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, DCTDividendAKId, PolicyAKId, DividendTransactionEnteredDate, DividendRunDate, StateCode, DividendPlan, DividendType, SupStateId, SupDividendTypeId, DividendPaidAmount, DCTDividendHistoryId, DCTDividendPurpose, DCTDividendTransactionType, DCTDividendPriorAmount, DCTDividendChangeAmount, DCTDividendPremiumAmount, DCTDividendPaidDate)
	SELECT 
	o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	o_AuditID AS AUDITID, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_DCTDividendAKId AS DCTDIVIDENDAKID, 
	POLICYAKID, 
	DIVIDENDTRANSACTIONENTEREDDATE, 
	DIVIDENDRUNDATE, 
	STATECODE, 
	DIVIDENDPLAN, 
	DIVIDENDTYPE, 
	sup_state_id AS SUPSTATEID, 
	SUPDIVIDENDTYPEID, 
	DIVIDENDPAIDAMOUNT, 
	HistoryID AS DCTDIVIDENDHISTORYID, 
	Purpose AS DCTDIVIDENDPURPOSE, 
	Type AS DCTDIVIDENDTRANSACTIONTYPE, 
	DividendPrior AS DCTDIVIDENDPRIORAMOUNT, 
	DividendChange AS DCTDIVIDENDCHANGEAMOUNT, 
	DividendPremium AS DCTDIVIDENDPREMIUMAMOUNT, 
	DividendPaidDate AS DCTDIVIDENDPAIDDATE
	FROM EXP_MetaData
),