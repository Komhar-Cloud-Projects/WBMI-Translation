WITH
SQ_DC_StatCode AS (
	Select DT.HistoryID,DT.SessionId,DS.Purpose,PC.ObjectId,PC.CoverageId,PC.Type CoverageType,DCC.Type,DCC.Value,'ClassCode' Attribute
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy DP with(nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
	on DP.SessionId=DL.SessionId
	and DP.PolicyId=DL.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
	on DP.SessionId=DS.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
	on DS.SessionId=DT.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage PC with(nolock)
	on DP.SessionId=PC.SessionId
	inner Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_ClassCode DCC with(nolock)
	on PC.ObjectId=DCC.ObjectId
	and PC.SessionId=DCC.SessionId
	and PC.Type='ManualPremium'
	Left Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_StatCode DSC with(nolock)
	on PC.ObjectId=DSC.ObjectId
	and PC.SessionId=DSC.SessionId
	and DSC.Type not in ('ExperienceModification','ExpenseConstant','PremiumDiscount')
	where DL.Type='WorkersCompensation'
	and DS.Purpose='Onset'
	and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	and DT.State='Committed'
	and DCC.Value is not null and DCC.Value<>'0000' and ( len(DCC.Value)=4 or len(DCC.Value)=3)
	
	@{pipeline().parameters.WHERE_CLAUSE}
	
	Union ALL
	
	Select DT.HistoryID,DT.SessionId,DS.Purpose,PC.ObjectId,PC.CoverageId,PC.Type,DSC.Type,DSC.Value,'StatCode'
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy DP with(nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
	on DP.SessionId=DL.SessionId
	and DP.PolicyId=DL.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
	on DP.SessionId=DS.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
	on DS.SessionId=DT.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage PC with(nolock)
	on DP.SessionId=PC.SessionId
	inner Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_StatCode DSC with(nolock)
	on PC.Coverageid=DSC.ObjectId
	and PC.SessionId=DSC.SessionId
	and DSC.Type not in ('ExperienceModification','ExpenseConstant','PremiumDiscount')
	where DL.Type='WorkersCompensation'
	and DS.Purpose='Onset'
	and DT.State='Committed'
	and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	and DSC.Value is not null and DSC.Value<>'0000' and len(DSC.Value)=4
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_SRC_DataCollect AS (
	SELECT
	HistoryID,
	SessionId,
	Purpose,
	ObjectId,
	CoverageId,
	CoverageType,
	Type,
	Value,
	Attribute
	FROM SQ_DC_StatCode
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
	EXP_SRC_DataCollect.ObjectId,
	EXP_SRC_DataCollect.CoverageId,
	EXP_SRC_DataCollect.CoverageType,
	EXP_SRC_DataCollect.Type,
	EXP_SRC_DataCollect.Value AS i_Value,
	-- *INF*: iif(Attribute='ClassCode',
	-- lpad(i_Value,4,'0')
	-- ,i_Value)
	IFF(Attribute = 'ClassCode', lpad(i_Value, 4, '0'), i_Value) AS Value,
	EXP_SRC_DataCollect.Attribute,
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
	Auditid, 
	FilterFlag, 
	ObjectId, 
	CoverageId, 
	CoverageType, 
	Type, 
	Value, 
	Attribute
	FROM EXP_RecordFlagging
	WHERE FilterFlag='1'
),
WorkWCCoverageDetails AS (
	TRUNCATE TABLE WorkWCCoverageDetails;
	INSERT INTO WorkWCCoverageDetails
	(Auditid, ExtractDate, WCTrackHistoryID, ObjectId, CoverageId, CoverageType, Type, Value, Attribute)
	SELECT 
	AUDITID, 
	EXTRACTDATE, 
	WCTRACKHISTORYID, 
	OBJECTID, 
	COVERAGEID, 
	COVERAGETYPE, 
	TYPE, 
	VALUE, 
	ATTRIBUTE
	FROM FIL_ExcludeSubmittedRecords
),