WITH
SQ_WB_IM_Line AS (
	WITH cte_WBIMLine(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.IM_LineId, 
	X.WB_IM_LineId, 
	X.SessionId, 
	X.CurrentEquipementNumber, 
	X.ReportingTotalPremiumAdjustment, 
	X.ReportingPremiumResult, 
	X.ReportingReporterMinimumPremiumApplies, 
	X.QuotedScheduleMod, 
	X.ScheduleModCaption, 
	X.TotalContractorsEquipmentCatastropheLimit, 
	X.TotalContractorsEquipmentMiscEquipmentLimit, 
	X.TotalContractorsEquipmentEmployeeToolsLimit, 
	X.TotalContractorsEquipmentCatastropheLimitValue, 
	X.AssociationFactor 
	FROM
	WB_IM_Line X
	inner join
	cte_WBIMLine Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	IM_LineId,
	WB_IM_LineId,
	SessionId,
	CurrentEquipementNumber,
	ReportingTotalPremiumAdjustment,
	ReportingPremiumResult,
	ReportingReporterMinimumPremiumApplies AS i_ReportingReporterMinimumPremiumApplies,
	-- *INF*: IIF(i_ReportingReporterMinimumPremiumApplies='T','1','0')
	IFF(i_ReportingReporterMinimumPremiumApplies = 'T', '1', '0') AS o_ReportingReporterMinimumPremiumApplies,
	QuotedScheduleMod,
	ScheduleModCaption,
	TotalContractorsEquipmentCatastropheLimit,
	TotalContractorsEquipmentMiscEquipmentLimit,
	TotalContractorsEquipmentEmployeeToolsLimit,
	TotalContractorsEquipmentCatastropheLimitValue,
	AssociationFactor
	FROM SQ_WB_IM_Line
),
WBIMLineStage AS (
	TRUNCATE TABLE WBIMLineStage;
	INSERT INTO WBIMLineStage
	(ExtractDate, SourceSystemId, IM_LineId, WB_IM_LineId, SessionId, CurrentEquipementNumber, ReportingTotalPremiumAdjustment, ReportingPremiumResult, ReportingReporterMinimumPremiumApplies, QuotedScheduleMod, ScheduleModCaption, TotalContractorsEquipmentCatastropheLimit, TotalContractorsEquipmentMiscEquipmentLimit, TotalContractorsEquipmentEmployeeToolsLimit, TotalContractorsEquipmentCatastropheLimitValue, AssociationFactor)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	IM_LINEID, 
	WB_IM_LINEID, 
	SESSIONID, 
	CURRENTEQUIPEMENTNUMBER, 
	REPORTINGTOTALPREMIUMADJUSTMENT, 
	REPORTINGPREMIUMRESULT, 
	o_ReportingReporterMinimumPremiumApplies AS REPORTINGREPORTERMINIMUMPREMIUMAPPLIES, 
	QUOTEDSCHEDULEMOD, 
	SCHEDULEMODCAPTION, 
	TOTALCONTRACTORSEQUIPMENTCATASTROPHELIMIT, 
	TOTALCONTRACTORSEQUIPMENTMISCEQUIPMENTLIMIT, 
	TOTALCONTRACTORSEQUIPMENTEMPLOYEETOOLSLIMIT, 
	TOTALCONTRACTORSEQUIPMENTCATASTROPHELIMITVALUE, 
	ASSOCIATIONFACTOR
	FROM EXP_Metadata
),