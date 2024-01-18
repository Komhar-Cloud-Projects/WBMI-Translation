WITH
SQ_DCBPLineStage AS (
	SELECT
		DCBPLineStageId,
		ExtractDate,
		SourceSystemId,
		LineId,
		BP_LineId,
		SessionId,
		Id,
		BusinessIncomeAndExtraExpense,
		CommissionPercentage,
		Description,
		DescriptionOverride,
		DesignatedLocation,
		Earthquake,
		EarthquakeAggregateLimit,
		FloodCoverage,
		FungiCoverage,
		FungiLiability,
		IsFinalReport,
		IsReportable,
		PropertyDamageDeductibleEndorsement
	FROM DCBPLineStage
),
EXP_DCBPLineStage AS (
	SELECT
	DCBPLineStageId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	LineId,
	BP_LineId,
	SessionId,
	Id,
	BusinessIncomeAndExtraExpense AS i_BusinessIncomeAndExtraExpense,
	-- *INF*: IIF(i_BusinessIncomeAndExtraExpense='T','1','0')
	IFF(i_BusinessIncomeAndExtraExpense = 'T', '1', '0') AS o_BusinessIncomeAndExtraExpense,
	CommissionPercentage,
	Description,
	DescriptionOverride,
	DesignatedLocation AS i_DesignatedLocation,
	-- *INF*: IIF(i_DesignatedLocation='T','1','0')
	IFF(i_DesignatedLocation = 'T', '1', '0') AS o_DesignatedLocation,
	Earthquake AS i_Earthquake,
	-- *INF*: IIF(i_Earthquake='T','1','0')
	IFF(i_Earthquake = 'T', '1', '0') AS o_Earthquake,
	EarthquakeAggregateLimit AS i_EarthquakeAggregateLimit,
	-- *INF*: IIF(i_EarthquakeAggregateLimit='T','1','0')
	IFF(i_EarthquakeAggregateLimit = 'T', '1', '0') AS o_EarthquakeAggregateLimit,
	FloodCoverage AS i_FloodCoverage,
	-- *INF*: IIF(i_FloodCoverage='T','1','0')
	IFF(i_FloodCoverage = 'T', '1', '0') AS o_FloodCoverage,
	FungiCoverage AS i_FungiCoverage,
	-- *INF*: IIF(i_FungiCoverage='T','1','0')
	IFF(i_FungiCoverage = 'T', '1', '0') AS o_FungiCoverage,
	FungiLiability,
	IsFinalReport AS i_IsFinalReport,
	-- *INF*: IIF(i_IsFinalReport='T','1','0')
	IFF(i_IsFinalReport = 'T', '1', '0') AS o_IsFinalReport,
	IsReportable AS i_IsReportable,
	-- *INF*: IIF(i_IsReportable='T','1','0')
	IFF(i_IsReportable = 'T', '1', '0') AS o_IsReportable,
	PropertyDamageDeductibleEndorsement
	FROM SQ_DCBPLineStage
),
ArchDCBPLineStage AS (
	INSERT INTO ArchDCBPLineStage
	(ExtractDate, SourceSystemId, AuditId, DCBPLineStageId, LineId, BP_LineId, SessionId, Id, BusinessIncomeAndExtraExpense, CommissionPercentage, Description, DescriptionOverride, DesignatedLocation, Earthquake, EarthquakeAggregateLimit, FloodCoverage, FungiCoverage, FungiLiability, IsFinalReport, IsReportable, PropertyDamageDeductibleEndorsement)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCBPLINESTAGEID, 
	LINEID, 
	BP_LINEID, 
	SESSIONID, 
	ID, 
	o_BusinessIncomeAndExtraExpense AS BUSINESSINCOMEANDEXTRAEXPENSE, 
	COMMISSIONPERCENTAGE, 
	DESCRIPTION, 
	DESCRIPTIONOVERRIDE, 
	o_DesignatedLocation AS DESIGNATEDLOCATION, 
	o_Earthquake AS EARTHQUAKE, 
	o_EarthquakeAggregateLimit AS EARTHQUAKEAGGREGATELIMIT, 
	o_FloodCoverage AS FLOODCOVERAGE, 
	o_FungiCoverage AS FUNGICOVERAGE, 
	FUNGILIABILITY, 
	o_IsFinalReport AS ISFINALREPORT, 
	o_IsReportable AS ISREPORTABLE, 
	PROPERTYDAMAGEDEDUCTIBLEENDORSEMENT
	FROM EXP_DCBPLineStage
),