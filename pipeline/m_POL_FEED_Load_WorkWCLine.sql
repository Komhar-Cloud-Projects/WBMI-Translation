WITH
SQ_DC_Line AS (
	Select 
	DT.Historyid,
	DS.Purpose,
	DL.SessionId,
	DL.LineId,
	DL.Type LineType,
	DL.AuditPeriod,
	DWL.PrimaryLocationState,
	WWL.InterstateRiskID,
	WL.RatingPlan,
	WWL.MinimumPremiumMaximum,
	DWL.MinimumPremiumMaximumState,
	DWL.InstallmentType,
	DWL.AnniversaryRatingDate,
	WWL.DepositPremium,
	DWL.AnniversaryRating,
	WWL.OtherStatesInsuranceConditional
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
	Inner Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Line WL with(nolock)
	on DL.SessionId=WL.SessionId
	and DL.LineId=WL.LineId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
	on DL.SessionId=DT.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
	on DL.SessionId=DS.SessionId
	left Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_WC_Line DWL with(nolock)
	on Dl.SessionId=DWL.SessionId
	and DL.LineId=DWL.LineId
	left Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_WC_Line WWL with(nolock)
	on Dl.SessionId=WWL.SessionId
	and DWL.WC_LineId=WWL.WC_LineId
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
	LineType,
	AuditPeriod,
	PrimaryLocationState,
	InterstateRiskID,
	RatingPlan,
	LineId,
	MinimumPremiumMaximum,
	MinimumPremiumMaximumState,
	InstallmentType,
	AnniversaryRatingDate,
	DepositPremium,
	AnniversaryRating,
	OtherStatesInsuranceConditional
	FROM SQ_DC_Line
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
	EXP_SRCDataCollect.LineType,
	EXP_SRCDataCollect.AuditPeriod,
	EXP_SRCDataCollect.PrimaryLocationState,
	EXP_SRCDataCollect.InterstateRiskID,
	EXP_SRCDataCollect.RatingPlan,
	-- *INF*: IIF(lkp_Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AND (NOT ISNULL(lkp_SessionId)),'1','0')
	IFF(lkp_Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AND (lkp_SessionId IS NOT NULL), '1', '0') AS FilterFlag,
	EXP_SRCDataCollect.LineId,
	EXP_SRCDataCollect.MinimumPremiumMaximum,
	EXP_SRCDataCollect.MinimumPremiumMaximumState,
	EXP_SRCDataCollect.InstallmentType,
	EXP_SRCDataCollect.AnniversaryRatingDate,
	EXP_SRCDataCollect.DepositPremium,
	EXP_SRCDataCollect.AnniversaryRating,
	LKP_LatestSession.SessionId AS lkp_SessionId,
	EXP_SRCDataCollect.OtherStatesInsuranceConditional
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
	LineType, 
	AuditPeriod, 
	PrimaryLocationState, 
	InterstateRiskID, 
	RatingPlan, 
	FilterFlag, 
	LineId, 
	MinimumPremiumMaximum, 
	MinimumPremiumMaximumState, 
	InstallmentType, 
	AnniversaryRatingDate, 
	DepositPremium, 
	AnniversaryRating, 
	OtherStatesInsuranceConditional
	FROM EXP_RecordFlagging
	WHERE FilterFlag='1'
),
WorkWCLine AS (
	TRUNCATE TABLE WorkWCLine;
	INSERT INTO WorkWCLine
	(Auditid, ExtractDate, WCTrackHistoryID, LineId, LineType, AuditPeriod, RatingPlan, PrimaryLocationState, InterstateRiskID, MinimumPremiumMaximum, MinimumPremiumMaximumState, InstallmentType, AnniversaryRatingDate, DepositPremium, AnniversaryRating, OtherStatesInsuranceConditional)
	SELECT 
	AUDITID, 
	EXTRACTDATE, 
	WCTRACKHISTORYID, 
	LINEID, 
	LINETYPE, 
	AUDITPERIOD, 
	RATINGPLAN, 
	PRIMARYLOCATIONSTATE, 
	INTERSTATERISKID, 
	MINIMUMPREMIUMMAXIMUM, 
	MINIMUMPREMIUMMAXIMUMSTATE, 
	INSTALLMENTTYPE, 
	ANNIVERSARYRATINGDATE, 
	DEPOSITPREMIUM, 
	ANNIVERSARYRATING, 
	OTHERSTATESINSURANCECONDITIONAL
	FROM FIL_ExcludeSubmittedRecords
),