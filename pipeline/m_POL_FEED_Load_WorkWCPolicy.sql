WITH
SQ_PriorPolicy AS (
	select Pol_key as Policykey,ID as Coverageguid,Historyid,Type,PriorPolicykey,Max_Historyid,Status,PolicyType,Min_Historyid,cnt
	from (
	select B.PolicyNumber+B.PolicyVersionFormatted Pol_key,
	DC.ID,
	C.HistoryID,
	C.Type,
	ISNULL(case when G.CarrierName='WestBend' then F.PolicySymbol else '' end,'')+ISNULL(case when G.CarrierName='WestBend' then G.PolicyNumber else '' end,'')+ISNULL(case when G.CarrierName='WestBend' then F.PolicyMod else '' end,'') PriorPolicyKey,
	max(C.Historyid) over(partition by B.PolicyNumber+B.PolicyVersionFormatted) Max_Historyid,
	A.Status,
	max(case when C.Type in ('Renew','Rewrite','New','Reissue') then C.Type else '' end) over (Partition by B.PolicyNumber+B.PolicyVersionFormatted) PolicyType,
	min(Historyid) over(partition by B.PolicyNumber+B.PolicyVersionFormatted,DC.ID) Min_Historyid,count(B.PolicyNumber+B.PolicyVersionFormatted) over(partition by DC.ID) cnt
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy B
	on A.PolicyId=B.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction C
	on A.SessionId=C.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session D
	on C.SessionId=D.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line E
	on D.SessionId=E.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage DC
	on C.Sessionid=DC.Sessionid
	inner join (
	select distinct DC.ID CoverageGuid
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS
	on DT.Sessionid=DS.Sessionid
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL
	on DT.Sessionid=DL.Sessionid
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage DC
	on DT.Sessionid=DC.Sessionid
	where DL.Type='WorkersCompensation'
	and DS.Purpose='Onset'
	and DT.State='Committed'
	and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	) X
	on DC.ID=X.CoverageGuid
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CL_PriorInsurance F
	on E.SessionId=F.SessionId
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_PriorInsurance G
	on A.PolicyId=G.PolicyId
	and A.SessionId=G.SessionId
	where E.Type='WorkersCompensation'
	and D.Purpose='Onset'
	and C.State='Committed'
	) A
	where A.Historyid=A.Max_Historyid
	order by 2,Min_Historyid,3
),
EXP_PriorPolicy AS (
	SELECT
	Policykey,
	CoverageGuid,
	HistoryID,
	Type,
	PriorPolicykey,
	Max_HistoryID,
	Status,
	PolicyType,
	Min_HistoryID,
	-- *INF*: IIF(CoverageGuid<>v_Prev_CoverageGuid,
	-- Decode(TRUE,
	-- PolicyType='New' and Status='Cancelled','',
	-- PolicyType='New',Policykey,
	-- PolicyType='Renew' and ISNULL(PriorPolicykey),'',
	-- PolicyType='Renew',PriorPolicykey,''),
	-- Decode(TRUE,
	-- PolicyType='Renew',v_PreviousPolicyKey,
	-- IN(PolicyType,'Reissue','Rewrite'),v_DerivedPolicyKey,''))
	IFF(
	    CoverageGuid <> v_Prev_CoverageGuid,
	    Decode(
	        TRUE,
	        PolicyType = 'New'
	    and Status = 'Cancelled', '',
	        PolicyType = 'New', Policykey,
	        PolicyType = 'Renew'
	    and PriorPolicykey IS NULL, '',
	        PolicyType = 'Renew', PriorPolicykey,
	        ''
	    ),
	    Decode(
	        TRUE,
	        PolicyType = 'Renew', v_PreviousPolicyKey,
	        PolicyType IN ('Reissue','Rewrite'), v_DerivedPolicyKey,
	        ''
	    )
	) AS v_DerivedPolicyKey,
	Policykey AS v_PreviousPolicyKey,
	CoverageGuid AS v_Prev_CoverageGuid,
	Status AS v_Prev_Status,
	PolicyType AS v_prev_PolicyType,
	-- *INF*: IIF(v_DerivedPolicyKey=Policykey,'',v_DerivedPolicyKey)
	IFF(v_DerivedPolicyKey = Policykey, '', v_DerivedPolicyKey) AS PreviousPolicyKey,
	Cnt
	FROM SQ_PriorPolicy
),
FIL_NoPriorPolicy AS (
	SELECT
	Policykey, 
	PreviousPolicyKey, 
	Cnt
	FROM EXP_PriorPolicy
	WHERE NOT ISNULL(PreviousPolicyKey) and PreviousPolicyKey<>''
),
SRT_PriorPolicy_DuplicateElimination AS (
	SELECT
	Policykey, 
	PreviousPolicyKey, 
	Cnt
	FROM FIL_NoPriorPolicy
	ORDER BY Policykey ASC, Cnt ASC
),
AGG_Select_Correct_PriorPolicykey AS (
	SELECT
	Policykey,
	PreviousPolicyKey,
	Cnt
	FROM SRT_PriorPolicy_DuplicateElimination
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Policykey ORDER BY NULL) = 1
),
SQ_DC_Policy AS (
	select 
	DT.HistoryID,
	DS.Purpose,
	DP.SessionId,
	DP.PolicyID,
	DP.Term PolicyTerm,
	DP.PolicyNumber+WP.PolicyVersionFormatted PolicyKey,
	DP.EffectiveDate PolicyEffectiveDate,
	DP.ExpirationDate PolicyExpirationDate,
	WP.IsRollover,
	ISNULL(DT.TransactionDate,DT.CreatedDate) as TransactionDate,
	DT.EffectiveDate TransactionEffectiveDate,
	DT.ExpirationDate TransactionExpirationDate,
	DT.Type TransactionType,
	DP.NAICSCode,
	WP.Division
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy DP with(nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy WP with(nolock)
	on DP.PolicyId=WP.PolicyId
	and DP.SessionId=WP.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
	on DP.SessionId=DL.SessionId
	and DP.PolicyId=DL.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
	on DL.SessionId=DS.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
	on DL.SessionId=DT.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Transaction WT with(nolock)
	on DT.TransactionId=WT.TransactionId
	and DT.SessionId=WT.SessionId
	where DL.Type='WorkersCompensation'
	and DS.Purpose='Onset'
	and DT.State='Committed'
	and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_SRCDataCollect AS (
	SELECT
	HistoryID,
	Purpose,
	SessionId,
	PolicyId,
	PolicyTerm,
	PolicyKey,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	IsRollover,
	TransactionDate,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	TransactionType,
	NAICSCode,
	Division
	FROM SQ_DC_Policy
),
JNR_PriorPolicy AS (SELECT
	EXP_SRCDataCollect.HistoryID, 
	EXP_SRCDataCollect.Purpose, 
	EXP_SRCDataCollect.SessionId, 
	EXP_SRCDataCollect.PolicyId, 
	EXP_SRCDataCollect.PolicyTerm, 
	EXP_SRCDataCollect.PolicyKey, 
	EXP_SRCDataCollect.PolicyEffectiveDate, 
	EXP_SRCDataCollect.PolicyExpirationDate, 
	EXP_SRCDataCollect.IsRollover, 
	EXP_SRCDataCollect.TransactionDate, 
	EXP_SRCDataCollect.TransactionEffectiveDate, 
	EXP_SRCDataCollect.TransactionExpirationDate, 
	EXP_SRCDataCollect.TransactionType, 
	EXP_SRCDataCollect.NAICSCode, 
	EXP_SRCDataCollect.Division, 
	AGG_Select_Correct_PriorPolicykey.Policykey AS I_Policykey, 
	AGG_Select_Correct_PriorPolicykey.PreviousPolicyKey
	FROM AGG_Select_Correct_PriorPolicykey
	RIGHT OUTER JOIN EXP_SRCDataCollect
	ON EXP_SRCDataCollect.PolicyKey = AGG_Select_Correct_PriorPolicykey.Policykey
),
EXP_PrevousNonCancelledPolicy AS (
	SELECT
	HistoryID,
	Purpose,
	SessionId,
	PolicyId,
	PolicyTerm,
	PolicyKey,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	IsRollover,
	TransactionDate,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	TransactionType,
	NAICSCode,
	Division,
	PreviousPolicyKey
	FROM JNR_PriorPolicy
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
LKP_WorkWCTrackHistory AS (
	SELECT
	WCTrackHistoryID,
	Auditid,
	HistoryID,
	Purpose
	FROM (
		SELECT 
		WorkWCTrackHistory.WCTrackHistoryID as WCTrackHistoryID, 
		WorkWCTrackHistory.Auditid as Auditid, 
		WorkWCTrackHistory.HistoryID as HistoryID, 
		WorkWCTrackHistory.Purpose as Purpose 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory
		order by WorkWCTrackHistory.HistoryID,WorkWCTrackHistory.Purpose,WorkWCTrackHistory.Auditid ASC
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY HistoryID,Purpose ORDER BY WCTrackHistoryID) = 1
),
EXP_RecordFlagging AS (
	SELECT
	LKP_WorkWCTrackHistory.WCTrackHistoryID AS lkp_WCTrackHistoryID,
	LKP_WorkWCTrackHistory.Auditid AS lkp_Auditid,
	CURRENT_TIMESTAMP AS ExtractDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Auditid,
	-- *INF*: IIF(lkp_Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AND (NOT ISNULL(lkp_SessionId)),'1','0')
	IFF(lkp_Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AND (lkp_SessionId IS NOT NULL), '1', '0') AS FilterFlag,
	EXP_PrevousNonCancelledPolicy.PolicyId,
	EXP_PrevousNonCancelledPolicy.PolicyTerm,
	EXP_PrevousNonCancelledPolicy.PolicyKey,
	EXP_PrevousNonCancelledPolicy.PolicyEffectiveDate,
	EXP_PrevousNonCancelledPolicy.PolicyExpirationDate,
	EXP_PrevousNonCancelledPolicy.IsRollover,
	EXP_PrevousNonCancelledPolicy.TransactionDate,
	EXP_PrevousNonCancelledPolicy.TransactionEffectiveDate,
	EXP_PrevousNonCancelledPolicy.TransactionExpirationDate,
	EXP_PrevousNonCancelledPolicy.TransactionType,
	EXP_PrevousNonCancelledPolicy.PreviousPolicyKey,
	EXP_PrevousNonCancelledPolicy.NAICSCode,
	EXP_PrevousNonCancelledPolicy.Division,
	LKP_LatestSession.SessionId AS lkp_SessionId
	FROM EXP_PrevousNonCancelledPolicy
	LEFT JOIN LKP_LatestSession
	ON LKP_LatestSession.SessionId = EXP_PrevousNonCancelledPolicy.SessionId AND LKP_LatestSession.Purpose = EXP_PrevousNonCancelledPolicy.Purpose AND LKP_LatestSession.HistoryID = EXP_PrevousNonCancelledPolicy.HistoryID
	LEFT JOIN LKP_WorkWCTrackHistory
	ON LKP_WorkWCTrackHistory.HistoryID = EXP_PrevousNonCancelledPolicy.HistoryID AND LKP_WorkWCTrackHistory.Purpose = EXP_PrevousNonCancelledPolicy.Purpose
),
FIL_ExcludeSubmittedRecords AS (
	SELECT
	lkp_WCTrackHistoryID AS WCTrackHistoryID, 
	ExtractDate, 
	Auditid, 
	FilterFlag, 
	PolicyId, 
	PolicyTerm, 
	PolicyKey, 
	PolicyEffectiveDate, 
	PolicyExpirationDate, 
	IsRollover, 
	TransactionDate, 
	TransactionEffectiveDate, 
	TransactionExpirationDate, 
	TransactionType, 
	PreviousPolicyKey, 
	NAICSCode, 
	Division
	FROM EXP_RecordFlagging
	WHERE FilterFlag='1'
),
WorkWCPolicy AS (
	TRUNCATE TABLE WorkWCPolicy;
	INSERT INTO WorkWCPolicy
	(Auditid, ExtractDate, WCTrackHistoryID, PolicyId, PolicyKey, PolicyTerm, PolicyEffectiveDate, PolicyExpirationDate, IsRollover, TransactionDate, TransactionEffectiveDate, TransactionExpirationDate, TransactionType, PreviousPolicyKey, NAICSCode, Division)
	SELECT 
	AUDITID, 
	EXTRACTDATE, 
	WCTRACKHISTORYID, 
	POLICYID, 
	POLICYKEY, 
	POLICYTERM, 
	POLICYEFFECTIVEDATE, 
	POLICYEXPIRATIONDATE, 
	ISROLLOVER, 
	TRANSACTIONDATE, 
	TRANSACTIONEFFECTIVEDATE, 
	TRANSACTIONEXPIRATIONDATE, 
	TRANSACTIONTYPE, 
	PREVIOUSPOLICYKEY, 
	NAICSCODE, 
	DIVISION
	FROM FIL_ExcludeSubmittedRecords
),