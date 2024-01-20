WITH
SQ_DCWCRiskStaging AS (
	SELECT
		LineId,
		WC_RiskId,
		SessionId,
		WC_LocationId,
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
		RiskAircraftIndicator,
		TermExposureBasis,
		NCCIDescription,
		WC_LocationXmlId,
		ExtractDate,
		SourceSystemId
	FROM DCWCRiskStaging
),
EXP_Metadata AS (
	SELECT
	LineId,
	WC_RiskId,
	SessionId,
	WC_LocationId,
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
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCWCRiskStaging
),
archDCWCRiskStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCWCRiskStaging
	(LineId, WC_LocationId, WC_RiskId, SessionId, Id, TermType, Description, EffectiveDate, Exposure, ExposureAudited, ExposureBasis, ExposureEstimated, FirePopulation, NumberOfActiveVolunteers, NumberOfSalariedFiremen, RiskAircraftIndicator, TermExposureBasis, NCCIDescription, WC_LocationXmlId, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	LINEID, 
	WC_LOCATIONID, 
	WC_RISKID, 
	SESSIONID, 
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
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),