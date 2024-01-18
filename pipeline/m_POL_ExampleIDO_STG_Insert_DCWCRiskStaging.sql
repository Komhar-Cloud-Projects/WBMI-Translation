WITH
SQ_DC_WC_Risk AS (
	WITH cte_DCWCRisk(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.WC_LocationId, 
	X.WC_RiskId, 
	X.SessionId, 
	X.Id, 
	X.TermType, 
	X.Description, 
	X.EffectiveDate, 
	X.Exposure, 
	X.ExposureAudited, 
	X.ExposureBasis, 
	X.ExposureEstimated, 
	X.FirePopulation, 
	X.NumberOfActiveVolunteers, 
	X.NumberOfSalariedFiremen, 
	X.RiskAircraftIndicator, 
	X.TermExposureBasis, 
	X.NCCIDescription, 
	X.WC_LocationXmlId 
	FROM
	DC_WC_Risk X
	inner join
	cte_DCWCRisk Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	WC_LocationId,
	WC_RiskId,
	SessionId,
	Id,
	TermType,
	Description,
	EffectiveDate,
	Exposure,
	ExposureAudited,
	ExposureBasis,
	ExposureEstimated,
	FirePopulation,
	NumberOfActiveVolunteers,
	NumberOfSalariedFiremen,
	RiskAircraftIndicator AS i_RiskAircraftIndicator,
	-- *INF*: DECODE(i_RiskAircraftIndicator,'T',1,'F',0,NULL)
	DECODE(
	    i_RiskAircraftIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_RiskAircraftIndicator,
	TermExposureBasis,
	NCCIDescription,
	WC_LocationXmlId,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_WC_Risk
),
DCWCRiskStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCWCRiskStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCWCRiskStaging
	(LineId, WC_RiskId, SessionId, WC_LocationId, Id, TermType, Description, EffectiveDate, Exposure, ExposureAudited, ExposureBasis, ExposureEstimated, FirePopulation, NumberOfActiveVolunteers, NumberOfSalariedFiremen, RiskAircraftIndicator, TermExposureBasis, NCCIDescription, WC_LocationXmlId, ExtractDate, SourceSystemId)
	SELECT 
	LINEID, 
	WC_RISKID, 
	SESSIONID, 
	WC_LOCATIONID, 
	ID, 
	TERMTYPE, 
	DESCRIPTION, 
	EFFECTIVEDATE, 
	EXPOSURE, 
	EXPOSUREAUDITED, 
	EXPOSUREBASIS, 
	EXPOSUREESTIMATED, 
	FIREPOPULATION, 
	NUMBEROFACTIVEVOLUNTEERS, 
	NUMBEROFSALARIEDFIREMEN, 
	o_RiskAircraftIndicator AS RISKAIRCRAFTINDICATOR, 
	TERMEXPOSUREBASIS, 
	NCCIDESCRIPTION, 
	WC_LOCATIONXMLID, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),