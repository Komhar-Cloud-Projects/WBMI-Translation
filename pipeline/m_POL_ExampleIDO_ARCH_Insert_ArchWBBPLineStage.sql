WITH
SQ_WBBPLineStage AS (
	SELECT
		WBBPLineStageId,
		ExtractDate,
		SourceSystemId,
		BP_LineId,
		WB_BP_LineId,
		SessionId,
		PlusPak,
		IncludeEarthquake,
		Description,
		Value,
		Age,
		MaintenanceAgreement,
		Warranty,
		AssociationFactorLiability,
		AssociationFactorProperty,
		Override,
		LineAdditionalOptionalCoveragesPremium,
		PlanId,
		Graduated,
		GraduatedRateValue,
		Maximum,
		RateType,
		RateValue
	FROM WBBPLineStage
),
EXPTRANS AS (
	SELECT
	WBBPLineStageId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
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
	FROM SQ_WBBPLineStage
),
ArchWBBPLineStage AS (
	INSERT INTO ArchWBBPLineStage
	(ExtractDate, SourceSystemId, AuditId, WBBPLineStageId, BP_LineId, WB_BP_LineId, SessionId, PlusPak, IncludeEarthquake, Description, Value, Age, MaintenanceAgreement, Warranty, AssociationFactorLiability, AssociationFactorProperty, Override, LineAdditionalOptionalCoveragesPremium, PlanId, Graduated, GraduatedRateValue, Maximum, RateType, RateValue)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBBPLINESTAGEID, 
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
	FROM EXPTRANS
),