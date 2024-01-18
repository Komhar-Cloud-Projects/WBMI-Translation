WITH
SQ_DC_WC_Risk AS (
	select 
	DT.HistoryID,
	DS.Purpose,
	DS.SessionId,
	DWR.WC_RiskId,
	DWR.Exposure,
	DWR.NCCIDescription,
	DWR.ExposureBasis,
	DWR.ExposureEstimated,
	DWR.ExposureAudited,
	DWR.Description,
	DWR.WC_LocationId
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy DP with(nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
	on DP.SessionId=DL.SessionId
	and DP.PolicyId=DL.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
	on DP.SessionId=DS.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
	on DS.SessionId=DT.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_WC_Risk DWR with(nolock)
	on DL.LineId=DWR.LineId
	and DL.SessionId=DWR.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_WC_Risk WWR with(nolock)
	on DWR.WC_RiskId=WWR.WC_RiskId
	and DWR.SessionId=WWR.SessionId
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_WC_RiskCoverageTerm DWRCT with(nolock)
	on DWR.WC_RiskId=DWRCT.WC_RiskId
	and DWR.SessionId=DWRCT.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_WC_RiskDefault DWRD with(nolock)
	on DWR.WC_RiskId=DWRD.WC_RiskId
	and DWR.SessionId=DWRD.SessionId
	where DL.Type='WorkersCompensation'
	and DS.Purpose='Onset'
	and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	and DT.State='Committed'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_SRCDataCollect AS (
	SELECT
	HistoryID,
	Purpose,
	SessionId,
	WC_RiskId,
	Exposure,
	NCCIDescription,
	ExposureBasis,
	ExposureEstimated,
	ExposureAudited,
	Description,
	WC_LocationId
	FROM SQ_DC_WC_Risk
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
	-- *INF*: IIF(lkp_Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} and (NOT ISNULL(lkp_SessionId)),'1','0')
	IFF(lkp_Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} and (lkp_SessionId IS NOT NULL), '1', '0') AS FilterFlag,
	EXP_SRCDataCollect.WC_RiskId,
	EXP_SRCDataCollect.Exposure,
	EXP_SRCDataCollect.NCCIDescription,
	EXP_SRCDataCollect.ExposureBasis,
	EXP_SRCDataCollect.ExposureEstimated,
	EXP_SRCDataCollect.ExposureAudited,
	EXP_SRCDataCollect.Description,
	EXP_SRCDataCollect.WC_LocationId,
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
	WC_RiskId, 
	Exposure, 
	NCCIDescription, 
	ExposureBasis, 
	ExposureEstimated, 
	ExposureAudited, 
	Description, 
	WC_LocationId
	FROM EXP_RecordFlagging
	WHERE FilterFlag='1'
),
WorkWCRisk AS (
	TRUNCATE TABLE WorkWCRisk;
	INSERT INTO WorkWCRisk
	(Auditid, ExtractDate, WCTrackHistoryID, WC_RiskId, Exposure, ExposureAudited, ExposureBasis, ExposureEstimated, Description, WC_LocationId, NCCIDescription)
	SELECT 
	AUDITID, 
	EXTRACTDATE, 
	WCTRACKHISTORYID, 
	WC_RISKID, 
	EXPOSURE, 
	EXPOSUREAUDITED, 
	EXPOSUREBASIS, 
	EXPOSUREESTIMATED, 
	DESCRIPTION, 
	WC_LOCATIONID, 
	NCCIDESCRIPTION
	FROM FIL_ExcludeSubmittedRecords
),