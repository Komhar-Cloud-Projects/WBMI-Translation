WITH
SQ_WB_CL_PrintDoc AS (
	select 
	DT.HistoryID,
	DS.Purpose,
	DS.SessionId,
	PJ.PolicyId,
	PD.Caption,
	PD.FormName,
	PD.[Order] FormOrder,
	PD.PrintDefault,
	PD.Selected,
	PD.OnPolicy,
	PD.[Add],
	PD.[Remove]
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CL_PrintDoc PD
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CL_PrintJob PJ
	on PD.WB_CL_PrintJobId=PJ.WB_CL_PrintJobId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
	on PJ.SessionId=DL.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
	on DL.SessionId=DS.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
	on DL.SessionId=DT.SessionId
	where DL.Type='WorkersCompensation'
	and DT.State='Committed'
	and DS.Purpose='Onset'
	and DT.Type<>'Endorse'
	and PD.[Order] is NOT NULL
	and (PD.OnPolicy=1 OR PD.[Add] = 1) AND (PD.Remove is null OR PD.Remove = 0) and PD.[Order] is NOT NULL
	and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_SRCDataCollect AS (
	SELECT
	HistoryID,
	Purpose,
	SessionId,
	PolicyId,
	Caption,
	FormName,
	FormOrder,
	PrintDefault,
	Selected,
	OnPolicy,
	Add,
	Remove
	FROM SQ_WB_CL_PrintDoc
),
SQ_WB_CL_PrintDoc_Endorse AS (
	select 
	DT.HistoryID,
	DS.Purpose,
	DS.SessionId,
	PJ.PolicyId,
	PD.Caption,
	PD.FormName,
	PD.[Order] FormOrder,
	PD.PrintDefault,
	PD.Selected,
	ISNULL(PD.OnPolicy,0) OnPolicy,
	ISNULL(PD.[Add],0) [Add],
	ISNULL(PD.[Remove],0) [Remove],
	PolKey
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CL_PrintDoc PD
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CL_PrintJob PJ
	on PD.WB_CL_PrintJobId=PJ.WB_CL_PrintJobId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
	on PJ.SessionId=DL.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
	on DL.SessionId=DS.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
	on DL.SessionId=DT.SessionId
	inner JOIN
	(Select PolKey,MaxHistoryID,Max(B.SessionID) MaxSessionID from
	(Select distinct WP.PolicyNumber+WP.PolicyVersionFormatted PolKey,(T.HistoryID) MaxHistoryID from WB_Policy WP
	inner join DC_Transaction T with(nolock)
	on WP.SessionId=T.SessionId
	inner join DC_Line DL with(nolock)
	on T.Sessionid=DL.Sessionid
	inner join DC_Session S
	on WP.SessionID=S.SessionID
	inner join @{pipeline().parameters.DATABASE_EXAMPLEDATA}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.History H with (NOLOCK)
	on H.HistoryID=T.HistoryID
	and S.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	and T.State='Committed'
	and DL.Type='WorkersCompensation'
	and S.Purpose='Onset'
	and T.State='Committed'
	and T.Type='Endorse'
	and H.Change<>'OSEAdjustment'
	--and H.DeprecatedBy IS NULL
	--group by WP.PolicyNumber+WP.PolicyVersionFormatted
	@{pipeline().parameters.WHERE_CLAUSE_ENDORSE}
	) D
	inner join DC_Transaction A
	on A.HistoryID=D.MaxHistoryID
	inner join DC_Session B
	on A.SessionId=B.SessionId
	where B.Purpose='Onset'
	group by PolKey,MaxHistoryID) C
	ON C.MaxHistoryID=DT.HistoryID and C.MaxSessionID=DT.SessionId
	where DL.Type='WorkersCompensation'
	and DT.State='Committed'
	and DS.Purpose='Onset'
	and PD.[Order] is NOT NULL
	and DT.Type='Endorse'
	and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	order by PolKey,FormName
),
EXPTRANS_ENDORSE AS (
	SELECT
	HistoryID,
	Purpose,
	SessionId,
	PolicyId,
	Caption,
	FormName,
	FormOrder,
	PrintDefault,
	Selected,
	OnPolicy,
	Add,
	Remove,
	PolicyKey
	FROM SQ_WB_CL_PrintDoc_Endorse
),
SQ_WB_CL_PrintDoc_EndorseMissingForms AS (
	select 
	DT.HistoryID,
	DS.Purpose,
	DS.SessionId,
	PJ.PolicyId,
	PD.Caption,
	PD.FormName,
	PD.[Order] FormOrder,
	PD.PrintDefault,
	PD.Selected,
	ISNULL(PD.OnPolicy,0) OnPolicy,
	ISNULL(PD.[Add],0) [Add],
	ISNULL(PD.[Remove],0) [Remove],
	PolKey
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CL_PrintDoc PD
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CL_PrintJob PJ
	on PD.WB_CL_PrintJobId=PJ.WB_CL_PrintJobId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
	on PJ.SessionId=DL.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
	on DL.SessionId=DS.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
	on DL.SessionId=DT.SessionId
	inner JOIN
	(Select TR.HistoryID CurrHistID,TR.SessionId CurrSessionID,PolKey,D.MaxHistoryID,RANK() OVER (PARTITION BY PolKey ORDER BY TR.HistoryID DESC,P.SessionId DESC) RANK from DC_Transaction TR with (NOLOCK) 
	inner join WB_Policy P with (NOLOCK)
	on P.SessionId=TR.SessionId
	inner join @{pipeline().parameters.DATABASE_EXAMPLEDATA}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.History HI with (NOLOCK)
	on HI.HistoryID=TR.HistoryID 
	and HI.DeprecatedBy IS NULL
	inner join DC_Session S with (NOLOCK)
	on S.SessionId=TR.SessionId
	inner join 
	(Select distinct WP.PolicyNumber+WP.PolicyVersionFormatted PolKey,Max(T.HistoryID) MaxHistoryID from WB_Policy WP
	inner join DC_Transaction T with(nolock)
	on WP.SessionId=T.SessionId
	inner join DC_Line DL with(nolock)
	on T.Sessionid=DL.Sessionid
	inner join DC_Session S
	on WP.SessionID=S.SessionID
	inner join @{pipeline().parameters.DATABASE_EXAMPLEDATA}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.History H with (NOLOCK)
	on H.HistoryID=T.HistoryID
	and S.CreateDateTime  between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	and T.State='Committed'
	and DL.Type='WorkersCompensation'
	and S.Purpose='Onset'
	and T.State='Committed'
	and T.Type='Endorse'
	and H.Change<>'OSEAdjustment'
	--and H.DeprecatedBy IS NULL
	group by WP.PolicyNumber+WP.PolicyVersionFormatted
	@{pipeline().parameters.WHERE_CLAUSE_FORMS}
	) D
	on D.PolKey=(P.PolicyNumber+P.PolicyVersionFormatted)
	where TR.State='Committed'
	and TR.HistoryID<>D.MaxHistoryID
	--and HI.DeprecatedBy IS NULL
	and S.Purpose='Onset' 
	) B
	ON B.CurrHistID=DT.HistoryID and B.CurrSessionID=DT.SessionId and B.RANK=1 
	where DL.Type='WorkersCompensation'
	and DT.State='Committed'
	and DS.Purpose='Onset'
	and PD.[Order] is NOT NULL
	and (PD.OnPolicy=1 OR PD.[Add] = 1) AND (PD.Remove is null OR PD.Remove = 0) and PD.[Order] is NOT NULL
	and B.RANK=1
	order by PolKey,FormName
),
EXPTRANS_FORMS AS (
	SELECT
	HistoryID,
	Purpose,
	SessionId,
	PolicyId,
	Caption,
	FormName,
	FormOrder,
	PrintDefault,
	Selected,
	OnPolicy,
	Add,
	Remove,
	PolicyKey
	FROM SQ_WB_CL_PrintDoc_EndorseMissingForms
),
JNR_ENDORSE AS (SELECT
	EXPTRANS_ENDORSE.HistoryID AS HistoryID_End, 
	EXPTRANS_ENDORSE.Purpose AS Purpose_End, 
	EXPTRANS_ENDORSE.SessionId AS SessionId_End, 
	EXPTRANS_ENDORSE.PolicyId AS PolicyId_End, 
	EXPTRANS_ENDORSE.Caption AS Caption_End, 
	EXPTRANS_ENDORSE.FormName AS FormName_End, 
	EXPTRANS_ENDORSE.FormOrder AS FormOrder_End, 
	EXPTRANS_ENDORSE.PrintDefault AS PrintDefault_End, 
	EXPTRANS_ENDORSE.Selected AS Selected_End, 
	EXPTRANS_ENDORSE.OnPolicy AS OnPolicy_End, 
	EXPTRANS_ENDORSE.Add AS Add_End, 
	EXPTRANS_ENDORSE.Remove AS Remove_End, 
	EXPTRANS_ENDORSE.PolicyKey AS PolicyKey_End, 
	EXPTRANS_FORMS.HistoryID AS HistoryID_Forms, 
	EXPTRANS_FORMS.Purpose AS Purpose_Forms, 
	EXPTRANS_FORMS.SessionId AS SessionId_Forms, 
	EXPTRANS_FORMS.PolicyId AS PolicyId_Forms, 
	EXPTRANS_FORMS.Caption AS Caption_Forms, 
	EXPTRANS_FORMS.FormName AS FormName_Forms, 
	EXPTRANS_FORMS.FormOrder AS FormOrder_Forms, 
	EXPTRANS_FORMS.PrintDefault AS PrintDefault_Forms, 
	EXPTRANS_FORMS.Selected AS Selected_Forms, 
	EXPTRANS_FORMS.OnPolicy AS OnPolicy_Forms, 
	EXPTRANS_FORMS.Add AS Add_Forms, 
	EXPTRANS_FORMS.Remove AS Remove_Forms, 
	EXPTRANS_FORMS.PolicyKey AS PolicyKey_Forms
	FROM EXPTRANS_ENDORSE
	LEFT OUTER JOIN EXPTRANS_FORMS
	ON EXPTRANS_FORMS.PolicyKey = EXPTRANS_ENDORSE.PolicyKey AND EXPTRANS_FORMS.FormName = EXPTRANS_ENDORSE.FormName
),
EXP_FormFlags AS (
	SELECT
	HistoryID_End,
	Purpose_End,
	SessionId_End,
	PolicyId_End,
	Caption_End,
	FormName_End,
	FormOrder_End,
	PrintDefault_End,
	Selected_End,
	OnPolicy_End,
	Add_End,
	Remove_End,
	HistoryID_Forms,
	Purpose_Forms,
	SessionId_Forms,
	PolicyId_Forms,
	Caption_Forms,
	FormName_Forms,
	FormOrder_Forms,
	PrintDefault_Forms,
	Selected_Forms,
	OnPolicy_Forms,
	Add_Forms,
	Remove_Forms,
	-- *INF*: DECODE(TRUE,
	-- Remove_End<>'1' AND (OnPolicy_End='1' OR OnPolicy_Forms='1'),'1',
	-- '0')
	-- 
	DECODE(
	    TRUE,
	    Remove_End <> '1' AND (OnPolicy_End = '1' OR OnPolicy_Forms = '1'), '1',
	    '0'
	) AS v_OnPolicy,
	-- *INF*: DECODE(TRUE,
	-- Remove_End<>'1' AND (Add_End='1' OR Add_Forms='1'),'1',
	-- '0')
	DECODE(
	    TRUE,
	    Remove_End <> '1' AND (Add_End = '1' OR Add_Forms = '1'), '1',
	    '0'
	) AS v_Add,
	-- *INF*: DECODE(TRUE,
	-- Remove_End='1','1',
	-- '0')
	DECODE(
	    TRUE,
	    Remove_End = '1', '1',
	    '0'
	) AS v_Remove,
	v_OnPolicy AS OnPolicy,
	v_Add AS Add,
	v_Remove AS Remove
	FROM JNR_ENDORSE
),
FIL_Forms AS (
	SELECT
	HistoryID_End AS HistoryID, 
	Purpose_End AS Purpose, 
	SessionId_End AS SessionId, 
	PolicyId_End AS PolicyId, 
	Caption_End AS Caption, 
	FormName_End AS FormName, 
	FormOrder_End AS FormOrder, 
	PrintDefault_End AS PrintDefault, 
	Selected_End AS Selected, 
	OnPolicy, 
	Add, 
	Remove
	FROM EXP_FormFlags
	WHERE (OnPolicy='1' OR Add = '1') AND Remove='0'
),
Union AS (
	SELECT HistoryID, Purpose, SessionId, PolicyId, Caption, FormName, FormOrder, PrintDefault, Selected, OnPolicy, Add, Remove
	FROM EXP_SRCDataCollect
	UNION
	SELECT HistoryID, Purpose, SessionId, PolicyId, Caption, FormName, FormOrder, PrintDefault, Selected, OnPolicy, Add, Remove
	FROM FIL_Forms
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
	Union.PolicyId,
	Union.Caption,
	Union.FormName,
	Union.FormOrder,
	Union.PrintDefault,
	Union.Selected,
	Union.OnPolicy,
	Union.Add,
	Union.Remove,
	LKP_LatestSession.SessionId AS lkp_SessionId
	FROM Union
	LEFT JOIN LKP_LatestSession
	ON LKP_LatestSession.SessionId = Union.SessionId AND LKP_LatestSession.Purpose = Union.Purpose AND LKP_LatestSession.HistoryID = Union.HistoryID
	LEFT JOIN LKP_WorkWCTrackHistory
	ON LKP_WorkWCTrackHistory.HistoryID = Union.HistoryID AND LKP_WorkWCTrackHistory.Purpose = Union.Purpose
),
FIL_ExcludeSubmittedRecords AS (
	SELECT
	lkp_WCTrackHistoryID AS WCTrackHistoryID, 
	ExtractDate, 
	Auditid, 
	FilterFlag, 
	PolicyId, 
	Caption, 
	FormName, 
	FormOrder, 
	PrintDefault, 
	Selected, 
	OnPolicy, 
	Add, 
	Remove
	FROM EXP_RecordFlagging
	WHERE FilterFlag='1'
),
WorkWCForms AS (
	TRUNCATE TABLE WorkWCForms;
	INSERT INTO WorkWCForms
	(Auditid, ExtractDate, WCTrackHistoryID, PolicyId, Caption, FormName, FormOrder, PrintDefault, Selected, OnPolicy, Add, Remove)
	SELECT 
	AUDITID, 
	EXTRACTDATE, 
	WCTRACKHISTORYID, 
	POLICYID, 
	CAPTION, 
	FORMNAME, 
	FORMORDER, 
	PRINTDEFAULT, 
	SELECTED, 
	ONPOLICY, 
	ADD, 
	REMOVE
	FROM FIL_ExcludeSubmittedRecords
),