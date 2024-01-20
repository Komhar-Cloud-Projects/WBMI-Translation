WITH
SQ_DC_Deductible AS (
	--DC_WC_StateTerm
	select 
	DT.HistoryID,
	DS.Purpose,
	DS.Sessionid,
	A.Coverageid, 
	A.Type CoverageType,
	B.Type DeductibleType,
	B.Value DeductibleValue
	from 
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy DP with(nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
	on DP.SessionId=DL.SessionId
	and DP.PolicyId=DL.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
	on DP.SessionId=DS.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
	on DS.SessionId=DT.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage A with(nolock)
	on DP.SessionId=A.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Deductible B with(nolock)
	on A.SessionId=B.SessionId
	and A.ObjectId=B.ObjectId
	and A.ObjectName=B.ObjectName
	where DL.Type='WorkersCompensation'
	and DS.Purpose='Onset'
	and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	and DT.State='Committed'
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION
	
	--DC_Coverage
	Select 
	DT.HistoryID,
	DS.Purpose,
	DS.Sessionid,
	C.Coverageid, 
	C.Type CoverageType,
	DLI.Type DeductibleType,
	DLI.Value DeductibleValue
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy DP with(nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
	on DP.SessionId=DL.SessionId
	and DP.PolicyId=DL.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
	on DP.SessionId=DS.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
	on DS.SessionId=DT.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage C with(nolock)
	on DP.SessionId=C.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Deductible DLI with(nolock)
	on C.SessionId=DLI.SessionId
	and C.CoverageId=DLI.ObjectId
	and DLI.ObjectName='DC_Coverage'
	where DL.Type='WorkersCompensation'
	and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	and DT.State='Committed'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_SRCDataCollect AS (
	SELECT
	HistoryID,
	Purpose,
	SessionId,
	CoverageId,
	CoverageType,
	DeductibleType,
	DeductibleValue
	FROM SQ_DC_Deductible
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
	EXP_SRCDataCollect.CoverageId,
	EXP_SRCDataCollect.CoverageType,
	EXP_SRCDataCollect.DeductibleType,
	EXP_SRCDataCollect.DeductibleValue,
	LKP_LatestSession.SessionId AS lkp_SessionId
	FROM EXP_SRCDataCollect
	LEFT JOIN LKP_LatestSession
	ON LKP_LatestSession.SessionId = EXP_SRCDataCollect.SessionId AND LKP_LatestSession.Purpose = EXP_SRCDataCollect.Purpose AND LKP_LatestSession.HistoryID = EXP_SRCDataCollect.HistoryID
	LEFT JOIN LKP_WorkWCTrackHistory
	ON LKP_WorkWCTrackHistory.HistoryID = EXP_SRCDataCollect.HistoryID AND LKP_WorkWCTrackHistory.Purpose = EXP_SRCDataCollect.Purpose
),
FIL_ExcludeSubmittedRecords AS (
	SELECT
	lkp_WCTrackHistoryID AS WCTrackHistoryID, 
	ExtractDate, 
	Auditid, 
	FilterFlag, 
	CoverageId, 
	CoverageType, 
	DeductibleType, 
	DeductibleValue
	FROM EXP_RecordFlagging
	WHERE FilterFlag='1'
),
WorkWCDeductible AS (
	TRUNCATE TABLE WorkWCDeductible;
	INSERT INTO WorkWCDeductible
	(Auditid, ExtractDate, WCTrackHistoryID, CoverageId, CoverageType, DeductibleType, DeductibleValue)
	SELECT 
	AUDITID, 
	EXTRACTDATE, 
	WCTRACKHISTORYID, 
	COVERAGEID, 
	COVERAGETYPE, 
	DEDUCTIBLETYPE, 
	DEDUCTIBLEVALUE
	FROM FIL_ExcludeSubmittedRecords
),