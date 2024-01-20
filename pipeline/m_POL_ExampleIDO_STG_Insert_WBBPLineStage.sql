WITH
SQ_WB_BP_Line AS (
	WITH cte_WBBPLine(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.BP_LineId, 
	X.WB_BP_LineId, 
	X.SessionId, 
	X.PlusPak, 
	X.IncludeEarthquake, 
	X.Description, 
	X.Value, 
	X.Age, 
	X.MaintenanceAgreement, 
	X.Warranty, 
	X.AssociationFactorLiability, 
	X.AssociationFactorProperty, 
	X.Override, 
	X.LineAdditionalOptionalCoveragesPremium, 
	X.PlanId, 
	X.Graduated, 
	X.GraduatedRateValue, 
	X.Maximum, 
	X.RateType, 
	X.RateValue 
	FROM
	WB_BP_Line X
	inner join
	cte_WBBPLine Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	BP_LineId,
	WB_BP_LineId,
	SessionId,
	PlusPak AS i_PlusPak,
	-- *INF*: IIF(i_PlusPak='T','1','0')
	IFF(i_PlusPak = 'T', '1', '0') AS o_PlusPak,
	IncludeEarthquake AS i_IncludeEarthquake,
	-- *INF*: IIF(i_IncludeEarthquake='T','1','0')
	IFF(i_IncludeEarthquake = 'T', '1', '0') AS o_IncludeEarthquake,
	Description,
	Value,
	Age,
	MaintenanceAgreement,
	Warranty,
	AssociationFactorLiability,
	AssociationFactorProperty,
	Override AS i_Override,
	-- *INF*: IIF(i_Override='T','1','0')
	IFF(i_Override = 'T', '1', '0') AS o_Override,
	LineAdditionalOptionalCoveragesPremium,
	PlanId,
	Graduated AS i_Graduated,
	-- *INF*: IIF(i_Graduated='T','1','0')
	IFF(i_Graduated = 'T', '1', '0') AS o_Graduated,
	GraduatedRateValue,
	Maximum,
	RateType,
	RateValue
	FROM SQ_WB_BP_Line
),
WBBPLineStage AS (
	TRUNCATE TABLE WBBPLineStage;
	INSERT INTO WBBPLineStage
	(ExtractDate, SourceSystemId, BP_LineId, WB_BP_LineId, SessionId, PlusPak, IncludeEarthquake, Description, Value, Age, MaintenanceAgreement, Warranty, AssociationFactorLiability, AssociationFactorProperty, Override, LineAdditionalOptionalCoveragesPremium, PlanId, Graduated, GraduatedRateValue, Maximum, RateType, RateValue)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	BP_LINEID, 
	WB_BP_LINEID, 
	SESSIONID, 
	o_PlusPak AS PLUSPAK, 
	o_IncludeEarthquake AS INCLUDEEARTHQUAKE, 
	DESCRIPTION, 
	VALUE, 
	AGE, 
	MAINTENANCEAGREEMENT, 
	WARRANTY, 
	ASSOCIATIONFACTORLIABILITY, 
	ASSOCIATIONFACTORPROPERTY, 
	o_Override AS OVERRIDE, 
	LINEADDITIONALOPTIONALCOVERAGESPREMIUM, 
	PLANID, 
	o_Graduated AS GRADUATED, 
	GRADUATEDRATEVALUE, 
	MAXIMUM, 
	RATETYPE, 
	RATEVALUE
	FROM EXP_Metadata
),