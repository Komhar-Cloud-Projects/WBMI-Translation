WITH
SQ_DC_BP_Line AS (
	WITH cte_DCBPLine(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.BP_LineId, 
	X.SessionId, 
	X.Id, 
	X.BusinessIncomeAndExtraExpense, 
	X.CommissionPercentage, 
	X.Description, 
	X.DescriptionOverride, 
	X.DesignatedLocation, 
	X.Earthquake, 
	X.EarthquakeAggregateLimit, 
	X.FloodCoverage, 
	X.FungiCoverage, 
	X.FungiLiability, 
	X.IsFinalReport, 
	X.IsReportable, 
	X.PropertyDamageDeductibleEndorsement 
	FROM
	DC_BP_Line X
	inner join
	cte_DCBPLine Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_DCBPLineStage AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
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
	FROM SQ_DC_BP_Line
),
DCBPLineStage AS (
	TRUNCATE TABLE DCBPLineStage;
	INSERT INTO DCBPLineStage
	(ExtractDate, SourceSystemId, LineId, BP_LineId, SessionId, Id, BusinessIncomeAndExtraExpense, CommissionPercentage, Description, DescriptionOverride, DesignatedLocation, Earthquake, EarthquakeAggregateLimit, FloodCoverage, FungiCoverage, FungiLiability, IsFinalReport, IsReportable, PropertyDamageDeductibleEndorsement)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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