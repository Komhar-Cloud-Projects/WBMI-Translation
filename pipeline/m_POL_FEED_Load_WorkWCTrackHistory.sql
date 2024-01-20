WITH
SQ_DC_WC_State_StateFlag AS (
	Select distinct D.PolKey,DT.HistoryID,St.State ListedState,
	case when St.Deleted=1 then '1' Else '0' End as DeletedStateFlag
	from DC_WC_State ST	
	inner  join DC_Transaction DT
	on ST.SessionId=DT.SessionId
	inner join WB_Policy P
	on P.SessionId=DT.SessionId
	inner JOIN
	(Select distinct WP.PolicyNumber+WP.PolicyVersionFormatted PolKey from WB_Policy WP
	inner join DC_Transaction T with(nolock)
	on WP.SessionId=T.SessionId
	inner join DC_Line DL with(nolock)
	on T.Sessionid=DL.Sessionid
	inner join DC_Session S
	on WP.SessionID=S.SessionID
	where S.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	and T.State='Committed'
	and DL.Type='WorkersCompensation'
	and S.Purpose='Onset'
	and T.State='Committed'
	@{pipeline().parameters.WHERE_CLAUSE_STATE}
	) D
	on D.PolKey=(P.PolicyNumber+P.PolicyVersionFormatted)
	where DT.State='Committed'
),
EXP_StateFlag AS (
	SELECT
	PolKey AS PolKey_StateFlag,
	HistoryID AS HistoryID_StateFlag,
	ListedState AS State_StateFlag,
	-- *INF*: LTRIM(RTRIM(State_StateFlag))
	LTRIM(RTRIM(State_StateFlag)) AS o_State_StateFlag,
	DeletedStateFlag AS Deleted_StateFlag
	FROM SQ_DC_WC_State_StateFlag
),
LKP_TrackHistory AS (
	SELECT
	HistoryID,
	PolicyKey
	FROM (
		Select distinct HistoryID as HistoryID, PolicyKey as PolicyKey  from WorkWCTrackHistory
		order by 2,1--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY HistoryID,PolicyKey ORDER BY HistoryID DESC) = 1
),
EXP_Filter AS (
	SELECT
	EXP_StateFlag.PolKey_StateFlag,
	EXP_StateFlag.HistoryID_StateFlag,
	EXP_StateFlag.o_State_StateFlag AS State_StateFlag,
	EXP_StateFlag.Deleted_StateFlag,
	LKP_TrackHistory.HistoryID AS HistoryID_LKP,
	LKP_TrackHistory.PolicyKey AS PolicyKey_LKP,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(HistoryID_LKP))),'NEW','EXISTS')
	IFF(LTRIM(RTRIM(HistoryID_LKP)) IS NULL, 'NEW', 'EXISTS') AS FilterFlag
	FROM EXP_StateFlag
	LEFT JOIN LKP_TrackHistory
	ON LKP_TrackHistory.HistoryID = EXP_StateFlag.HistoryID_StateFlag AND LKP_TrackHistory.PolicyKey = EXP_StateFlag.PolKey_StateFlag
),
FIL_NewTxns AS (
	SELECT
	PolKey_StateFlag, 
	HistoryID_StateFlag, 
	State_StateFlag, 
	Deleted_StateFlag, 
	FilterFlag
	FROM EXP_Filter
	WHERE LTRIM(RTRIM(FilterFlag))='EXISTS'
),
SRT_State AS (
	SELECT
	PolKey_StateFlag, 
	HistoryID_StateFlag, 
	State_StateFlag, 
	Deleted_StateFlag, 
	FilterFlag
	FROM FIL_NewTxns
	ORDER BY PolKey_StateFlag ASC, HistoryID_StateFlag ASC, State_StateFlag ASC, Deleted_StateFlag ASC
),
AGG_Latest_Txn AS (
	SELECT
	PolKey_StateFlag,
	HistoryID_StateFlag,
	State_StateFlag,
	Deleted_StateFlag
	FROM SRT_State
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolKey_StateFlag, State_StateFlag ORDER BY NULL) = 1
),
EXP_Latest_Txn AS (
	SELECT
	PolKey_StateFlag,
	HistoryID_StateFlag,
	State_StateFlag,
	Deleted_StateFlag
	FROM AGG_Latest_Txn
),
SQ_DC_Transaction_PrevTransType AS (
	select distinct A.Policykey,A.Historyid,A.Type,ParentHistoryid,DT.Type as PreviousTransactionType from(
	select Pol_key as Policykey,Historyid,Type,max(AHistoryID) over(partition by ID order by Historyid) ParentHistoryid
	from (
	select B.PolicyNumber+B.PolicyVersionFormatted Pol_key,
	DC.ID,
	Historyid,
	case when C.Type in ('New','Renew') then C.Historyid else 0 end AHistoryID,
	C.Type
	from DC_Policy A
	inner join WB_Policy B
	on A.PolicyId=B.PolicyId
	inner join DC_Transaction C
	on A.SessionId=C.SessionId
	inner join DC_Session D
	on C.SessionId=D.SessionId
	inner join DC_Line E
	on D.SessionId=E.SessionId
	inner join DC_Coverage DC
	on C.Sessionid=DC.Sessionid
	inner join (
	select distinct DC.ID CoverageGuid
	from DC_Transaction DT
	inner join DC_Session DS
	on DT.Sessionid=DS.Sessionid
	inner join DC_Line DL
	on DT.Sessionid=DL.Sessionid
	inner join DC_Coverage DC
	on DT.Sessionid=DC.Sessionid
	inner join WB_Policy WP
	on DT.SessionId=WP.SessionId
	where DL.Type='WorkersCompensation'
	and DS.Purpose='Onset'
	and DT.State='Committed'
	and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	@{pipeline().parameters.WHERE_CLAUSE_TRANSTYPE}
	) X
	on DC.ID=X.CoverageGuid
	where E.Type='WorkersCompensation'
	and D.Purpose='Onset'
	and C.State='Committed'
	and C.Type in ('New','Renew','Reissue','Rewrite','Restart')
	) A
	) A
	inner Join DC_Transaction DT 
	on A.ParentHistoryid=DT.HistoryID
	where A.ParentHistoryid<>A.HistoryID
	order by 1,2
),
EXP_PrevTransType AS (
	SELECT
	Policykey AS Policykey_PrevTransType,
	HistoryID AS HistoryID_PrevTransType,
	Type,
	ParentHistoryid,
	PreviousTransactionType
	FROM SQ_DC_Transaction_PrevTransType
),
SQ_WCPOLS_DG_ExclusionList AS (

-- TODO Manual --

),
EXP_ExclusionList AS (
	SELECT
	ExtractDate,
	-- *INF*: ADD_TO_DATE(TO_DATE(ExtractDate,'MM/DD/YYYY'),'HH',13)
	DATEADD(HOUR,13,TO_TIMESTAMP(ExtractDate, 'MM/DD/YYYY')) AS v_ExtractDate,
	v_ExtractDate AS o_ExtractDate,
	-- *INF*: TO_CHAR(v_ExtractDate,'YYYYMMDD')
	TO_CHAR(v_ExtractDate, 'YYYYMMDD') AS TransactionDate_Exclusion,
	Policykey,
	-- *INF*: LPAD(Policykey,9,'0')
	LPAD(Policykey, 9, '0') AS o_Policykey,
	TransactionType
	FROM SQ_WCPOLS_DG_ExclusionList
),
FIL_ExclusionList AS (
	SELECT
	o_ExtractDate AS ExtractDate, 
	TransactionDate_Exclusion, 
	o_Policykey AS Policykey, 
	TransactionType
	FROM EXP_ExclusionList
	WHERE TRUE

--ExtractDate>=@{pipeline().parameters.SELECTION_START_TS}
),
SQ_DC_Session AS (
	Select HistoryID,TransactionDate,Trans_TransactionType,Policy_TransactionType,Purpose,Sessionid,
	PolicyNumber,PolicyVersionFormatted,A.StateProv,PremiumBearingFlag,max(StateAddFlag) over(partition by A.HistoryID,A.PolicyNumber,A.PolicyVersionFormatted) StateAddFlag,NewPremium,Charge,PriorPremium,ReasonCode,InterstateRiskID, DeletedStateFlag, PolicyState
	from (
	Select distinct DT.HistoryID,DT.TransactionDate,
	DT.Type as Trans_TransactionType,
	WP.PolicyIssueCodeDesc as Policy_TransactionType,
	DS.Purpose,DS.Sessionid,DS.CreateDateTime,WP.PolicyNumber,WP.PolicyVersionFormatted,DLOC.StateProv,
	case when DT.Type='Endorse' and DT.Charge<>0.00 then '1' else '0' end PremiumBearingFlag,
	DT.NewPremium,DT.Charge,DT.PriorPremium,WR.Code ReasonCode,WWL.InterstateRiskID,
	Max(DT.Sessionid) over(Partition by DT.HistoryID,Purpose) Max_Sessionid,
	case when WWS.WCStateAddedThisTransaction=1 then '1' else '0' end as StateAddFlag,
	case when DWS.Deleted=1 then '1' Else '0' End as DeletedStateFlag, 
	DWS.[State] as PolicyState
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Reason WR with(nolock)
	on DT.Transactionid=WR.Transactionid
	and DT.Sessionid=WR.Sessionid
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
	on DT.Sessionid=DS.Sessionid
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
	on DT.Sessionid=DL.Sessionid
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy WP with(nolock)
	on DT.Sessionid=WP.Sessionid
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Location DLOC with(nolock)
	on WP.SessionId=DLOC.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_LocationAssociation DLOCA with(nolock)
	on DLOC.SessionId=DLOCA.SessionId
	and DLOC.LocationId=DLOCA.LocationId
	and DLOCA.LocationAssociationType in ('Location','WC_Location')
	left Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_WC_State WWS
	on DS.SessionId=WWS.SessionId
	left Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_WC_Line WWL with(nolock)
	on DS.SessionId=WWL.SessionId
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_WC_State DWS with (nolock)
	on DS.sessionid=DWS.sessionid --and DWS.Deleted=1 
	where DL.Type='WorkersCompensation'
	and DS.Purpose='Onset'
	and DT.State='Committed'
	and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	and DT.Type not in (@{pipeline().parameters.EXCLUDE_TRANSACTIONTYPES})
	@{pipeline().parameters.WHERE_CLAUSE}
	) A
	where Sessionid=Max_Sessionid
	order by PolicyNumber+PolicyVersionFormatted ASC,PolicyState ASC,A.HistoryID DESC
),
EXP_DataCollect AS (
	SELECT
	HistoryID,
	TransactionDate,
	-- *INF*: TO_CHAR(TransactionDate,'YYYYMMDD')
	TO_CHAR(TransactionDate, 'YYYYMMDD') AS o_TransactionDate,
	Trans_TransactionType,
	Policy_TransactionType,
	-- *INF*: DECODE(TRUE,
	-- IN(Trans_TransactionType,'Reissue','Rewrite') AND NOT ISNULL(Policy_TransactionType),Policy_TransactionType,
	-- Trans_TransactionType)
	DECODE(
	    TRUE,
	    Trans_TransactionType IN ('Reissue','Rewrite') AND Policy_TransactionType IS NOT NULL, Policy_TransactionType,
	    Trans_TransactionType
	) AS v_TransactionType,
	-- *INF*: IIF(LTRIM(RTRIM(v_TransactionType))='Renewal','Renew',v_TransactionType)
	IFF(LTRIM(RTRIM(v_TransactionType)) = 'Renewal', 'Renew', v_TransactionType) AS o_TransactionType,
	Purpose,
	SessionId,
	PolicyNumber,
	PolicyVersionFormatted,
	-- *INF*: ltrim(rtrim(PolicyNumber)) || Ltrim(Rtrim(PolicyVersionFormatted))
	ltrim(rtrim(PolicyNumber)) || Ltrim(Rtrim(PolicyVersionFormatted)) AS o_PolicyKey,
	StateProv,
	PremiumBearingFlag,
	StateAddFlag,
	NewPremium,
	Charge,
	PriorPremium,
	ReasonCode,
	InterstateRiskID,
	StateDeletedFlag,
	PolicyState,
	-- *INF*: LTRIM(RTRIM(PolicyState))
	LTRIM(RTRIM(PolicyState)) AS o_PolicyState
	FROM SQ_DC_Session
),
LKP_History_OSE AS (
	SELECT
	HistoryID
	FROM (
		Select Historyid as Historyid
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.History (nolock)
		where Change='OSEAdjustment'
		and Historyid not in (select HistoryID from (
		select distinct WP.PolicyNumber,WP.PolicyVersionFormatted,DT.HistoryID,DT.Type,H.Change
		,ROW_NUMBER()over(Partition by WP.PolicyNumber,WP.PolicyVersionFormatted order by DT.Historyid) Cancel_Rank
		from 
		(select distinct WP.PolicyNumber,WP.PolicyVersionFormatted,DT.HistoryID 
		from @{pipeline().parameters.DATABASE_NAME_IDO}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT
		inner join @{pipeline().parameters.DATABASE_NAME_IDO}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy WP
		on DT.Sessionid=WP.Sessionid
		inner join @{pipeline().parameters.DATABASE_NAME_IDO}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL
		on DT.SessionId=DL.SessionId
		where DT.Historyid in (
		select HistoryID
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.History
		where Change='OSEAdjustment'
		and Type='Cancel')
		and DL.Type='WorkersCompensation'
		and DT.State='Committed') A
		inner join @{pipeline().parameters.DATABASE_NAME_IDO}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy WP
		on A.PolicyNumber=WP.PolicyNumber
		and A.PolicyVersionFormatted=WP.PolicyVersionFormatted
		inner join @{pipeline().parameters.DATABASE_NAME_IDO}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT
		on WP.SessionId=DT.SessionId
		left Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.History H
		on DT.HistoryID=H.HistoryID
		and H.Change='OSEAdjustment'
		and H.Type='Cancel'
		where DT.Type='Cancel'
		) A
		where Cancel_rank=1
		and Change is not null)
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY HistoryID ORDER BY HistoryID) = 1
),
LKP_LatestSession AS (
	SELECT
	SessionId,
	Purpose,
	HistoryID
	FROM (
		Select distinct DT.HistoryID AS HistoryID,
		DS.Purpose AS Purpose,
		Max(DS.Sessionid) AS Sessionid
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
		on DT.Sessionid=DS.Sessionid
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
		on DT.Sessionid=DL.Sessionid
		where DL.Type='WorkersCompensation'
		and DS.Purpose='Onset'
		and DT.State='Committed'
		and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
		group by DT.HistoryID,DS.Purpose
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionId,Purpose,HistoryID ORDER BY SessionId) = 1
),
RTR_OSETransactions AS (
	SELECT
	LKP_History_OSE.HistoryID AS LKP_HistoryID,
	EXP_DataCollect.HistoryID,
	EXP_DataCollect.o_TransactionDate,
	EXP_DataCollect.o_TransactionType AS TransactionType,
	EXP_DataCollect.Purpose,
	EXP_DataCollect.SessionId,
	EXP_DataCollect.o_PolicyKey,
	EXP_DataCollect.StateProv,
	EXP_DataCollect.PremiumBearingFlag,
	EXP_DataCollect.StateCount,
	EXP_DataCollect.StateAddFlag,
	EXP_DataCollect.NewPremium,
	EXP_DataCollect.Charge,
	EXP_DataCollect.PriorPremium,
	EXP_DataCollect.ReasonCode,
	EXP_DataCollect.InterstateRiskID,
	LKP_LatestSession.SessionId AS LKP_SessionId,
	EXP_DataCollect.StateDeletedFlag,
	EXP_DataCollect.o_PolicyState AS PolicyState
	FROM EXP_DataCollect
	LEFT JOIN LKP_History_OSE
	ON LKP_History_OSE.HistoryID = EXP_DataCollect.HistoryID
	LEFT JOIN LKP_LatestSession
	ON LKP_LatestSession.SessionId = EXP_DataCollect.SessionId AND LKP_LatestSession.Purpose = EXP_DataCollect.Purpose AND LKP_LatestSession.HistoryID = EXP_DataCollect.HistoryID
),
RTR_OSETransactions_Non_Endorsement_Transactions AS (SELECT * FROM RTR_OSETransactions WHERE ISNULL(LKP_HistoryID) and (Not ISNULL(LKP_SessionId)) AND TransactionType<>'Endorse'),
RTR_OSETransactions_Endorsement_Transactions AS (SELECT * FROM RTR_OSETransactions WHERE ISNULL(LKP_HistoryID) and (Not ISNULL(LKP_SessionId)) AND TransactionType='Endorse'),
EXP_SRC_DataCollect_NonEndorse AS (
	SELECT
	'DCT' AS SourceSystemID,
	'WCPOLS02Record' AS TableName,
	'StateCode' AS ProcessName,
	HistoryID,
	o_TransactionDate AS TransactionDate,
	TransactionType,
	Purpose,
	SessionId,
	o_PolicyKey AS PolicyKey,
	StateProv,
	PremiumBearingFlag,
	StateCount,
	StateAddFlag,
	NewPremium,
	Charge,
	PriorPremium,
	ReasonCode,
	InterstateRiskID,
	StateDeletedFlag,
	PolicyState,
	-- *INF*: IIF(IN(TransactionType,'EndorsePremiumBearing','EndorseNonPremiumBearing','EndorseAddDeleteState'),'Endorse',
	-- TransactionType)
	IFF(
	    TransactionType IN ('EndorsePremiumBearing','EndorseNonPremiumBearing','EndorseAddDeleteState'),
	    'Endorse',
	    TransactionType
	) AS LKP_TransactionType,
	PolicyKey AS v_CurrentRecord,
	TransactionType AS v_CurrentEndorse,
	StateProv AS v_CurrentState,
	-- *INF*: DECODE(TRUE,
	-- v_CurrentRecord=v_PreviousRecord AND v_CurrentState=v_OldState,
	-- 'NOCHANGE',
	-- v_CurrentRecord=v_PreviousRecord AND v_CurrentState<>v_OldState,
	-- 'CHANGE',
	-- v_CurrentRecord<>v_PreviousRecord,'CHANGE'
	-- )
	DECODE(
	    TRUE,
	    v_CurrentRecord = v_PreviousRecord AND v_CurrentState = v_OldState, 'NOCHANGE',
	    v_CurrentRecord = v_PreviousRecord AND v_CurrentState <> v_OldState, 'CHANGE',
	    v_CurrentRecord <> v_PreviousRecord, 'CHANGE'
	) AS v_StateCounter,
	StateProv AS v_OldState,
	-- *INF*: DECODE(TRUE,
	-- v_CurrentRecord=v_PreviousRecord AND TransactionType='Endorse',
	-- v_EndorseCounter+1,
	-- v_CurrentRecord<>v_PreviousRecord AND TransactionType='Endorse',1,
	-- v_CurrentRecord=v_PreviousRecord AND TransactionType<> 'Endorse', v_EndorseCounter,
	-- 0)
	DECODE(
	    TRUE,
	    v_CurrentRecord = v_PreviousRecord AND TransactionType = 'Endorse', v_EndorseCounter + 1,
	    v_CurrentRecord <> v_PreviousRecord AND TransactionType = 'Endorse', 1,
	    v_CurrentRecord = v_PreviousRecord AND TransactionType <> 'Endorse', v_EndorseCounter,
	    0
	) AS v_EndorseCounter,
	TransactionType AS v_OldEndorse,
	-- *INF*: DECODE(TRUE,
	-- TransactionType<>'Endorse' ,1,
	-- v_EndorseCounter=1,1,
	-- 0)
	DECODE(
	    TRUE,
	    TransactionType <> 'Endorse', 1,
	    v_EndorseCounter = 1, 1,
	    0
	) AS v_RecordCount,
	PolicyKey AS v_PreviousRecord,
	v_RecordCount AS o_RecordCount
	FROM RTR_OSETransactions_Non_Endorsement_Transactions
),
EXP_SRC_DataCollect_Endorse AS (
	SELECT
	'DCT' AS SourceSystemID,
	'WCPOLS02Record' AS TableName,
	'StateCode' AS ProcessName,
	HistoryID,
	o_TransactionDate AS TransactionDate,
	TransactionType,
	Purpose,
	SessionId,
	o_PolicyKey AS PolicyKey,
	StateProv,
	PremiumBearingFlag,
	StateCount,
	StateAddFlag,
	NewPremium,
	Charge,
	PriorPremium,
	ReasonCode,
	InterstateRiskID,
	StateDeletedFlag,
	PolicyState,
	-- *INF*: IIF(IN(TransactionType,'EndorsePremiumBearing','EndorseNonPremiumBearing','EndorseAddDeleteState'),'Endorse',
	-- TransactionType)
	IFF(
	    TransactionType IN ('EndorsePremiumBearing','EndorseNonPremiumBearing','EndorseAddDeleteState'),
	    'Endorse',
	    TransactionType
	) AS LKP_TransactionType,
	PolicyKey||StateProv||TransactionDate AS v_CurrentRecord,
	-- *INF*: DECODE(TRUE,
	-- v_CurrentRecord=v_PreviousRecord ,
	-- --AND TransactionType='Endorse',
	-- v_EndorseCounter+1,
	-- v_CurrentRecord<>v_PreviousRecord ,
	-- --AND TransactionType='Endorse',
	-- 1,
	-- --v_CurrentRecord=v_PreviousRecord AND TransactionType<> 'Endorse', v_EndorseCounter,
	-- 0)
	DECODE(
	    TRUE,
	    v_CurrentRecord = v_PreviousRecord, v_EndorseCounter + 1,
	    v_CurrentRecord <> v_PreviousRecord, 1,
	    0
	) AS v_EndorseCounter,
	-- *INF*: DECODE(TRUE,
	-- v_EndorseCounter=1,1,
	-- 0)
	DECODE(
	    TRUE,
	    v_EndorseCounter = 1, 1,
	    0
	) AS v_RecordCount,
	PolicyKey||StateProv||TransactionDate AS v_PreviousRecord,
	-- *INF*: 1
	-- --v_RecordCount
	1 AS o_RecordCount
	FROM RTR_OSETransactions_Endorsement_Transactions
),
FIL_Latest_Endorsement AS (
	SELECT
	SourceSystemID, 
	TableName, 
	ProcessName, 
	HistoryID, 
	TransactionDate, 
	TransactionType, 
	Purpose, 
	SessionId, 
	PolicyKey, 
	StateProv, 
	PremiumBearingFlag, 
	StateCount, 
	StateAddFlag, 
	NewPremium, 
	Charge, 
	PriorPremium, 
	ReasonCode, 
	InterstateRiskID, 
	LKP_TransactionType, 
	o_RecordCount AS RecordCount, 
	StateDeletedFlag, 
	PolicyState
	FROM EXP_SRC_DataCollect_Endorse
	WHERE RecordCount=1
),
Union_AllTransactions AS (
	SELECT SourceSystemID, TableName, ProcessName, HistoryID, TransactionDate, TransactionType, Purpose, SessionId, PolicyKey, StateProv, PremiumBearingFlag, StateCount, StateAddFlag, NewPremium, Charge, PriorPremium, ReasonCode, InterstateRiskID, LKP_TransactionType, o_RecordCount, StateDeletedFlag, PolicyState
	FROM EXP_SRC_DataCollect_NonEndorse
	UNION
	SELECT SourceSystemID, TableName, ProcessName, HistoryID, TransactionDate, TransactionType, Purpose, SessionId, PolicyKey, StateProv, PremiumBearingFlag, StateCount, StateAddFlag, NewPremium, Charge, PriorPremium, ReasonCode, InterstateRiskID, LKP_TransactionType, RecordCount AS o_RecordCount, StateDeletedFlag, PolicyState
	FROM FIL_Latest_Endorsement
),
JNR_TrackHistory AS (SELECT
	FIL_ExclusionList.TransactionDate_Exclusion, 
	FIL_ExclusionList.Policykey AS Policykey_Exclusion, 
	FIL_ExclusionList.TransactionType AS TransactionType_Exclusion, 
	Union_AllTransactions.SourceSystemID, 
	Union_AllTransactions.TableName, 
	Union_AllTransactions.ProcessName, 
	Union_AllTransactions.HistoryID, 
	Union_AllTransactions.TransactionDate, 
	Union_AllTransactions.TransactionType, 
	Union_AllTransactions.Purpose, 
	Union_AllTransactions.SessionId, 
	Union_AllTransactions.PolicyKey, 
	Union_AllTransactions.StateProv, 
	Union_AllTransactions.PremiumBearingFlag, 
	Union_AllTransactions.StateCount, 
	Union_AllTransactions.StateAddFlag, 
	Union_AllTransactions.NewPremium, 
	Union_AllTransactions.Charge, 
	Union_AllTransactions.PriorPremium, 
	Union_AllTransactions.ReasonCode, 
	Union_AllTransactions.InterstateRiskID, 
	Union_AllTransactions.LKP_TransactionType, 
	Union_AllTransactions.o_RecordCount AS RecordCount, 
	Union_AllTransactions.StateDeletedFlag, 
	Union_AllTransactions.PolicyState
	FROM Union_AllTransactions
	LEFT OUTER JOIN FIL_ExclusionList
	ON FIL_ExclusionList.Policykey = Union_AllTransactions.PolicyKey AND FIL_ExclusionList.TransactionType = Union_AllTransactions.LKP_TransactionType AND FIL_ExclusionList.TransactionDate_Exclusion = Union_AllTransactions.TransactionDate
),
FIL_RemoveHistoryID AS (
	SELECT
	Policykey_Exclusion, 
	TransactionType_Exclusion, 
	SourceSystemID, 
	TableName, 
	ProcessName, 
	HistoryID, 
	TransactionDate, 
	TransactionType, 
	Purpose, 
	SessionId, 
	PolicyKey, 
	StateProv, 
	PremiumBearingFlag, 
	StateCount, 
	StateAddFlag, 
	NewPremium, 
	Charge, 
	PriorPremium, 
	ReasonCode, 
	InterstateRiskID, 
	StateDeletedFlag, 
	PolicyState
	FROM JNR_TrackHistory
	WHERE ISNULL (Policykey_Exclusion)
),
JNR_PrevTransType AS (SELECT
	FIL_RemoveHistoryID.HistoryID, 
	FIL_RemoveHistoryID.TransactionType, 
	FIL_RemoveHistoryID.Purpose, 
	FIL_RemoveHistoryID.SessionId, 
	FIL_RemoveHistoryID.PolicyKey, 
	FIL_RemoveHistoryID.StateProv, 
	FIL_RemoveHistoryID.PremiumBearingFlag, 
	FIL_RemoveHistoryID.StateCount, 
	FIL_RemoveHistoryID.StateAddFlag, 
	FIL_RemoveHistoryID.NewPremium, 
	FIL_RemoveHistoryID.Charge, 
	FIL_RemoveHistoryID.PriorPremium, 
	FIL_RemoveHistoryID.ReasonCode, 
	FIL_RemoveHistoryID.InterstateRiskID, 
	FIL_RemoveHistoryID.StateDeletedFlag, 
	FIL_RemoveHistoryID.PolicyState, 
	FIL_RemoveHistoryID.TransactionDate, 
	EXP_PrevTransType.Policykey_PrevTransType, 
	EXP_PrevTransType.HistoryID_PrevTransType, 
	EXP_PrevTransType.Type, 
	EXP_PrevTransType.ParentHistoryid, 
	EXP_PrevTransType.PreviousTransactionType
	FROM FIL_RemoveHistoryID
	LEFT OUTER JOIN EXP_PrevTransType
	ON EXP_PrevTransType.Policykey_PrevTransType = FIL_RemoveHistoryID.PolicyKey
),
JNR_StateLogic AS (SELECT
	EXP_Latest_Txn.PolKey_StateFlag, 
	EXP_Latest_Txn.HistoryID_StateFlag, 
	EXP_Latest_Txn.State_StateFlag, 
	EXP_Latest_Txn.Deleted_StateFlag, 
	JNR_PrevTransType.HistoryID, 
	JNR_PrevTransType.TransactionType, 
	JNR_PrevTransType.Purpose, 
	JNR_PrevTransType.SessionId, 
	JNR_PrevTransType.PolicyKey, 
	JNR_PrevTransType.StateProv, 
	JNR_PrevTransType.PremiumBearingFlag, 
	JNR_PrevTransType.StateCount, 
	JNR_PrevTransType.StateAddFlag, 
	JNR_PrevTransType.PreviousTransactionType, 
	JNR_PrevTransType.NewPremium, 
	JNR_PrevTransType.Charge, 
	JNR_PrevTransType.PriorPremium, 
	JNR_PrevTransType.ReasonCode, 
	JNR_PrevTransType.InterstateRiskID, 
	JNR_PrevTransType.StateDeletedFlag, 
	JNR_PrevTransType.PolicyState, 
	JNR_PrevTransType.Policykey_PrevTransType, 
	JNR_PrevTransType.HistoryID_PrevTransType, 
	JNR_PrevTransType.Type, 
	JNR_PrevTransType.ParentHistoryid, 
	JNR_PrevTransType.TransactionDate
	FROM JNR_PrevTransType
	LEFT OUTER JOIN EXP_Latest_Txn
	ON EXP_Latest_Txn.PolKey_StateFlag = JNR_PrevTransType.PolicyKey AND EXP_Latest_Txn.State_StateFlag = JNR_PrevTransType.PolicyState
),
EXP_GetFlags AS (
	SELECT
	-- *INF*: DECODE(TRUE,
	-- IN(PolicyState,'WI','MN','MI','NC','PA','DE','MA','NJ','CA') and (ISNULL(InterstateRiskID) or InterstateRiskID=0),'0',
	-- NOT IN(PolicyState,'WI','MN','MI','NC','PA','DE','MA','NJ','CA') AND v_RequiredState='0','0',
	-- '1')
	DECODE(
	    TRUE,
	    PolicyState IN ('WI','MN','MI','NC','PA','DE','MA','NJ','CA') and (InterstateRiskID IS NULL or InterstateRiskID = 0), '0',
	    NOT PolicyState IN ('WI','MN','MI','NC','PA','DE','MA','NJ','CA') AND v_RequiredState = '0', '0',
	    '1'
	) AS o_NCCIRequiredFlag,
	-- *INF*: IIF(PolicyState='WI' AND v_RequiredState='1','1','0')
	IFF(PolicyState = 'WI' AND v_RequiredState = '1', '1', '0') AS o_WIRequiredFlag,
	-- *INF*: IIF(PolicyState='MI' AND v_RequiredState='1','1','0')
	IFF(PolicyState = 'MI' AND v_RequiredState = '1', '1', '0') AS o_MIRequiredFlag,
	-- *INF*: IIF(PolicyState='MN' AND v_RequiredState='1','1','0')
	IFF(PolicyState = 'MN' AND v_RequiredState = '1', '1', '0') AS o_MNRequiredFlag,
	-- *INF*: IIF(PolicyState='NC' AND v_RequiredState='1','1','0')
	IFF(PolicyState = 'NC' AND v_RequiredState = '1', '1', '0') AS o_NCRequiredFlag,
	HistoryID,
	TransactionType,
	Purpose,
	SessionId,
	PolicyKey,
	StateProv,
	PremiumBearingFlag,
	StateCount,
	StateAddFlag,
	PreviousTransactionType,
	NewPremium,
	Charge,
	PriorPremium,
	ReasonCode,
	InterstateRiskID,
	StateDeletedFlag,
	PolicyState,
	Policykey_PrevTransType,
	HistoryID_PrevTransType,
	Type,
	ParentHistoryid,
	HistoryID_StateFlag,
	PolKey_StateFlag,
	State_StateFlag,
	Deleted_StateFlag,
	-- *INF*: DECODE(TRUE,
	-- IN(TransactionType,'Renew','New','Reissue','Rewrite') AND StateDeletedFlag='0', '1',
	-- IN(TransactionType,'Renew','New','Reissue','Rewrite') AND StateDeletedFlag='1', '0',
	-- NOT ISNULL (State_StateFlag) AND Deleted_StateFlag='0','1',
	-- NOT ISNULL(State_StateFlag) AND Deleted_StateFlag='1' AND StateDeletedFlag='1','0',
	-- ISNULL(State_StateFlag) AND StateDeletedFlag='0','1',
	-- ISNULL(State_StateFlag) AND StateDeletedFlag='1','1',
	-- '1'
	-- )
	DECODE(
	    TRUE,
	    TransactionType IN ('Renew','New','Reissue','Rewrite') AND StateDeletedFlag = '0', '1',
	    TransactionType IN ('Renew','New','Reissue','Rewrite') AND StateDeletedFlag = '1', '0',
	    State_StateFlag IS NULL AND Deleted_StateFlag =NOT  '0', '1',
	    State_StateFlag IS NULL AND Deleted_StateFlag = '1' AND StateDeletedFlag =NOT  '1', '0',
	    State_StateFlag IS NULL AND StateDeletedFlag = '0', '1',
	    State_StateFlag IS NULL AND StateDeletedFlag = '1', '1',
	    '1'
	) AS v_RequiredState,
	-- *INF*: DECODE(TRUE,
	-- IN(TransactionType,'Renew','New','Reissue','Rewrite') AND StateDeletedFlag='1', '0',
	-- --NOT ISNULL (State_AB) AND Deleted_AB='0','0',
	-- --NOT ISNULL(State_AB) AND Deleted_AB='1' AND StateDeletedFlag='1','0',
	-- NOT ISNULL(State_StateFlag) AND Deleted_StateFlag='0' AND StateDeletedFlag='1','1',
	-- NOT IN(TransactionType,'Renew','New','Reissue','Rewrite') AND ISNULL(State_StateFlag) AND StateDeletedFlag='1','1',
	-- '0')
	-- 
	DECODE(
	    TRUE,
	    TransactionType IN ('Renew','New','Reissue','Rewrite') AND StateDeletedFlag = '1', '0',
	    State_StateFlag IS NULL AND Deleted_StateFlag = '0' AND StateDeletedFlag =NOT  '1', '1',
	    NOT TransactionType IN ('Renew','New','Reissue','Rewrite') AND State_StateFlag IS NULL AND StateDeletedFlag = '1', '1',
	    '0'
	) AS v_DeletedStateFlag,
	-- *INF*: IIF(LTRIM(RTRIM(v_DeletedStateFlag))='1',LTRIM(RTRIM(PolicyState)))
	IFF(LTRIM(RTRIM(v_DeletedStateFlag)) = '1', LTRIM(RTRIM(PolicyState))) AS o_DeletedState,
	-- *INF*: LTRIM(RTRIM(v_DeletedStateFlag))
	LTRIM(RTRIM(v_DeletedStateFlag)) AS o_StateDeletedFlag,
	TransactionDate
	FROM JNR_StateLogic
),
AGG_RemoveDups AS (
	SELECT
	o_NCCIRequiredFlag AS i_NCCIRequiredFlag,
	o_WIRequiredFlag AS i_WIRequiredFlag,
	o_MIRequiredFlag AS i_MIRequiredFlag,
	o_MNRequiredFlag AS i_MNRequiredFlag,
	o_NCRequiredFlag AS i_NCRequiredFlag,
	-- *INF*: Max(i_NCCIRequiredFlag)
	Max(i_NCCIRequiredFlag) AS o_NCCIRequiredFlag,
	-- *INF*: Max(i_WIRequiredFlag)
	Max(i_WIRequiredFlag) AS o_WIRequiredFlag,
	-- *INF*: Max(i_MIRequiredFlag)
	Max(i_MIRequiredFlag) AS o_MIRequiredFlag,
	-- *INF*: Max(i_MNRequiredFlag)
	Max(i_MNRequiredFlag) AS o_MNRequiredFlag,
	-- *INF*: Max(i_NCRequiredFlag)
	Max(i_NCRequiredFlag) AS o_NCRequiredFlag,
	HistoryID,
	TransactionType,
	Purpose,
	SessionId,
	PolicyKey,
	PremiumBearingFlag,
	-- *INF*: max(PremiumBearingFlag)
	max(PremiumBearingFlag) AS o_PremiumBearingFlag,
	StateCount,
	StateAddFlag,
	-- *INF*: Max(StateAddFlag)
	Max(StateAddFlag) AS o_StateAddFlag,
	PreviousTransactionType,
	NewPremium,
	Charge,
	PriorPremium,
	ReasonCode,
	InterstateRiskID,
	o_StateDeletedFlag AS i_StateDeletedFlag,
	o_DeletedState AS i_DeletedState,
	-- *INF*: MAX(i_StateDeletedFlag)
	MAX(i_StateDeletedFlag) AS o_StateDeletedFlag,
	-- *INF*: MAX(i_DeletedState)
	MAX(i_DeletedState) AS o_DeletedState,
	TransactionDate
	FROM EXP_GetFlags
	GROUP BY HistoryID, TransactionType, Purpose, SessionId, PolicyKey
),
EXP_TGTDataCollect AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	CURRENT_TIMESTAMP AS ExtractDate,
	HistoryID,
	TransactionType,
	-- *INF*: IIF(TransactionType='Endorse',IIF(StateAddFlag='1'  or StateDeletedFlag='1','EndorseAddDeleteState',IIF(PremiumBearingFlag='1','EndorsePremiumBearing','EndorseNonPremiumBearing')),TransactionType)
	IFF(
	    TransactionType = 'Endorse',
	    IFF(
	        StateAddFlag = '1'
	    or StateDeletedFlag = '1', 'EndorseAddDeleteState',
	        IFF(
	            PremiumBearingFlag = '1', 'EndorsePremiumBearing',
	            'EndorseNonPremiumBearing'
	        )
	    ),
	    TransactionType
	) AS o_TransactionType,
	Purpose,
	SessionId,
	PolicyKey,
	o_NCCIRequiredFlag AS NCCIRequiredFlag,
	o_WIRequiredFlag AS WIRequiredFlag,
	o_MIRequiredFlag AS MIRequiredFlag,
	o_MNRequiredFlag AS MNRequiredFlag,
	o_PremiumBearingFlag AS PremiumBearingFlag,
	StateCount,
	o_StateAddFlag AS StateAddFlag,
	PreviousTransactionType,
	NewPremium,
	Charge,
	PriorPremium,
	ReasonCode,
	InterstateRiskID,
	o_StateDeletedFlag AS StateDeletedFlag,
	o_DeletedState AS DeletedState,
	TransactionDate,
	o_NCRequiredFlag AS NCRequiredFlag
	FROM AGG_RemoveDups
),
RTR_Split_For_DeleteState_Rules AS (
	SELECT
	AuditId,
	ExtractDate,
	HistoryID,
	o_TransactionType AS TransactionType,
	Purpose,
	SessionId,
	PolicyKey,
	NCCIRequiredFlag,
	WIRequiredFlag,
	MIRequiredFlag,
	MNRequiredFlag,
	PremiumBearingFlag,
	StateCount,
	StateAddFlag,
	PreviousTransactionType,
	NewPremium,
	Charge,
	PriorPremium,
	ReasonCode,
	StateDeletedFlag,
	DeletedState,
	TransactionDate,
	NCRequiredFlag
	FROM EXP_TGTDataCollect
),
RTR_Split_For_DeleteState_Rules_Normal AS (SELECT * FROM RTR_Split_For_DeleteState_Rules WHERE NOT(TransactionType='Endorse' AND IN(StateDeletedFlag,'T','1') AND IN(DeletedState,'MN','WI'))),
RTR_Split_For_DeleteState_Rules_WI_MN_DeleteState_via_Endorsment AS (SELECT * FROM RTR_Split_For_DeleteState_Rules WHERE (TransactionType='Endorse' AND IN(StateDeletedFlag,'T','1') AND IN(DeletedState,'MN','WI'))),
EXP_Apply_Endorse_DeleteState_Rules AS (
	SELECT
	AuditId,
	ExtractDate,
	HistoryID,
	TransactionType AS i_TransactionType,
	'Cancel' AS o_TransactionType,
	Purpose,
	SessionId,
	PolicyKey,
	NCCIRequiredFlag,
	WIRequiredFlag,
	MIRequiredFlag,
	MNRequiredFlag,
	PremiumBearingFlag,
	StateCount,
	StateAddFlag,
	PreviousTransactionType,
	NewPremium,
	Charge,
	PriorPremium,
	ReasonCode,
	StateDeletedFlag,
	DeletedState,
	TransactionDate AS TransactionDate3,
	NCRequiredFlag AS NCRequiredFlag3
	FROM RTR_Split_For_DeleteState_Rules_WI_MN_DeleteState_via_Endorsment
),
WorkWCTrackHistory_WI_MN_DeleteState AS (
	INSERT INTO WorkWCTrackHistory
	(Auditid, ExtractDate, HistoryID, TransactionType, Purpose, Sessionid, PolicyKey, NCCIRequiredFlag, WIRequiredFlag, MIRequiredFlag, MNRequiredFlag, PremiumBearingFlag, StateCount, StateAddFlag, PreviousPolicyTransactionType, NewPremium, Charge, PriorPremium, ReasonCode, StateDeletedFlag, DeletedState, TransactionDate, OriginalTransactionType, NCRequiredFlag)
	SELECT 
	AuditId AS AUDITID, 
	EXTRACTDATE, 
	HISTORYID, 
	o_TransactionType AS TRANSACTIONTYPE, 
	PURPOSE, 
	SessionId AS SESSIONID, 
	POLICYKEY, 
	NCCIREQUIREDFLAG, 
	WIREQUIREDFLAG, 
	MIREQUIREDFLAG, 
	MNREQUIREDFLAG, 
	PREMIUMBEARINGFLAG, 
	STATECOUNT, 
	STATEADDFLAG, 
	PreviousTransactionType AS PREVIOUSPOLICYTRANSACTIONTYPE, 
	NEWPREMIUM, 
	CHARGE, 
	PRIORPREMIUM, 
	REASONCODE, 
	STATEDELETEDFLAG, 
	DELETEDSTATE, 
	TransactionDate3 AS TRANSACTIONDATE, 
	o_TransactionType AS ORIGINALTRANSACTIONTYPE, 
	NCRequiredFlag3 AS NCREQUIREDFLAG
	FROM EXP_Apply_Endorse_DeleteState_Rules
),
WorkWCTrackHistory AS (
	TRUNCATE TABLE WorkWCTrackHistory;
	INSERT INTO WorkWCTrackHistory
	(Auditid, ExtractDate, HistoryID, TransactionType, Purpose, Sessionid, PolicyKey, NCCIRequiredFlag, WIRequiredFlag, MIRequiredFlag, MNRequiredFlag, PremiumBearingFlag, StateCount, StateAddFlag, PreviousPolicyTransactionType, NewPremium, Charge, PriorPremium, ReasonCode, StateDeletedFlag, DeletedState, TransactionDate, OriginalTransactionType, NCRequiredFlag)
	SELECT 
	AuditId AS AUDITID, 
	EXTRACTDATE, 
	HISTORYID, 
	TRANSACTIONTYPE, 
	PURPOSE, 
	SessionId AS SESSIONID, 
	POLICYKEY, 
	NCCIREQUIREDFLAG, 
	WIREQUIREDFLAG, 
	MIREQUIREDFLAG, 
	MNREQUIREDFLAG, 
	PREMIUMBEARINGFLAG, 
	STATECOUNT, 
	STATEADDFLAG, 
	PreviousTransactionType AS PREVIOUSPOLICYTRANSACTIONTYPE, 
	NEWPREMIUM, 
	CHARGE, 
	PRIORPREMIUM, 
	REASONCODE, 
	STATEDELETEDFLAG, 
	DELETEDSTATE, 
	TRANSACTIONDATE, 
	TransactionType AS ORIGINALTRANSACTIONTYPE, 
	NCREQUIREDFLAG
	FROM RTR_Split_For_DeleteState_Rules_Normal
),