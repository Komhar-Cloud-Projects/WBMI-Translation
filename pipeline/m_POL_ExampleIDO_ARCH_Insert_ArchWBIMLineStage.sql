WITH
SQ_WBIMLineStage AS (
	SELECT
		WBIMLineStageId,
		ExtractDate,
		SourceSystemId,
		IM_LineId,
		WB_IM_LineId,
		SessionId,
		CurrentEquipementNumber,
		ReportingTotalPremiumAdjustment,
		ReportingPremiumResult,
		ReportingReporterMinimumPremiumApplies,
		QuotedScheduleMod,
		ScheduleModCaption,
		TotalContractorsEquipmentCatastropheLimit,
		TotalContractorsEquipmentMiscEquipmentLimit,
		TotalContractorsEquipmentEmployeeToolsLimit,
		TotalContractorsEquipmentCatastropheLimitValue,
		AssociationFactor
	FROM WBIMLineStage
),
EXPTRANS AS (
	SELECT
	WBIMLineStageId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
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
	FROM SQ_WBIMLineStage
),
ArchWBIMLineStage AS (
	INSERT INTO ArchWBIMLineStage
	(ExtractDate, SourceSystemId, AuditId, WBIMLineStageId, IM_LineId, WB_IM_LineId, SessionId, CurrentEquipementNumber, ReportingTotalPremiumAdjustment, ReportingPremiumResult, ReportingReporterMinimumPremiumApplies, QuotedScheduleMod, ScheduleModCaption, TotalContractorsEquipmentCatastropheLimit, TotalContractorsEquipmentMiscEquipmentLimit, TotalContractorsEquipmentEmployeeToolsLimit, TotalContractorsEquipmentCatastropheLimitValue, AssociationFactor)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBIMLINESTAGEID, 
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
	FROM EXPTRANS
),