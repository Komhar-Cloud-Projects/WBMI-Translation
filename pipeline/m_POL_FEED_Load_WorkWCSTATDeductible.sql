WITH
SQ_STATDeductible AS (
	Select 
	distinct DT.HistoryID,
	DS.Purpose,
	DS.SessionId,
	C.CoverageId,
	DWS.State,
	C.Type CoverageType,
	D.Type DeductibleType,
	D.Value DeductibleValue,
	S.Type StatCodeType,
	S.Value StatCodeValue
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
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Deductible D with(nolock)
	on C.SessionId=D.SessionId
	and C.CoverageId=D.ObjectId
	and D.ObjectName='DC_Coverage'
	inner join DC_WC_StateTerm St
	on ST.SessionId=C.SessionId and C.ObjectID=St.WC_StateTermId and C.ObjectName='DC_WC_StateTerm'
	inner join DC_StatCode S
	on S.SessionId=C.SessionId and S.ObjectName='DC_Coverage' and S.ObjectId=C.CoverageId
	inner join DC_WC_State DWS
	on DWS.SessionId=S.SessionId and DWS.WC_StateId=St.WC_StateId
	and DL.Type='WorkersCompensation'
	and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	and DT.State='Committed'
	@{pipeline().parameters.WHERE_CLAUSE}
	order by 1
),
EXP_SRC_DataCollect AS (
	SELECT
	HistoryID,
	Purpose,
	SessionId,
	CoverageId,
	State,
	CoverageType,
	DeductibleType,
	DeductibleValue,
	StatCodeType,
	StatCodeValue
	FROM SQ_STATDeductible
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
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkWCTrackHistory
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
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: IIF(lkp_Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AND (NOT ISNULL(lkp_SessionId)),'1','0')
	IFF(lkp_Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AND (lkp_SessionId IS NOT NULL), '1', '0') AS FilterFlag,
	EXP_SRC_DataCollect.CoverageId,
	EXP_SRC_DataCollect.State,
	EXP_SRC_DataCollect.CoverageType,
	EXP_SRC_DataCollect.DeductibleType,
	EXP_SRC_DataCollect.DeductibleValue,
	EXP_SRC_DataCollect.StatCodeType,
	EXP_SRC_DataCollect.StatCodeValue,
	LKP_LatestSession.SessionId AS lkp_SessionId
	FROM EXP_SRC_DataCollect
	LEFT JOIN LKP_LatestSession
	ON LKP_LatestSession.SessionId = EXP_SRC_DataCollect.SessionId AND LKP_LatestSession.Purpose = EXP_SRC_DataCollect.Purpose AND LKP_LatestSession.HistoryID = EXP_SRC_DataCollect.HistoryID
	LEFT JOIN LKP_WorkWCTrackHistory
	ON LKP_WorkWCTrackHistory.HistoryID = EXP_SRC_DataCollect.HistoryID AND LKP_WorkWCTrackHistory.Purpose = EXP_SRC_DataCollect.Purpose
),
FIL_ExcludeSubmittedRecords AS (
	SELECT
	lkp_WCTrackHistoryID AS WCTrackHistoryID, 
	ExtractDate, 
	AuditID, 
	FilterFlag, 
	CoverageId, 
	State, 
	CoverageType, 
	DeductibleType, 
	DeductibleValue, 
	StatCodeType, 
	StatCodeValue
	FROM EXP_RecordFlagging
	WHERE FilterFlag='1'
),
WorkWCSTATDeductible AS (
	TRUNCATE TABLE WorkWCSTATDeductible;
	INSERT INTO WorkWCSTATDeductible
	(Auditid, ExtractDate, WCTrackHistoryID, CoverageId, State, CoverageType, DeductibleType, DeductibleValue, StatCodeType, StatCodeValue)
	SELECT 
	AuditID AS AUDITID, 
	EXTRACTDATE, 
	WCTRACKHISTORYID, 
	COVERAGEID, 
	STATE, 
	COVERAGETYPE, 
	DEDUCTIBLETYPE, 
	DEDUCTIBLEVALUE, 
	STATCODETYPE, 
	STATCODEVALUE
	FROM FIL_ExcludeSubmittedRecords
),