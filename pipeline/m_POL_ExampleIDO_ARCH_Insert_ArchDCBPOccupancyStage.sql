WITH
SQ_DCBPOccupancyStage AS (
	SELECT
		DCBPOccupancyStageId,
		BPRiskId,
		BPOccupancyId,
		SessionId,
		Id,
		AssociationUnitOwners,
		BOPNewEQ,
		BOPNewEQOverride,
		BOPNewEQSL,
		BOPNewEQSLOverride,
		BOPNewLiabClassGroup,
		BOPNewLiabClassGroupOverride,
		BOPNewLiabExpBase,
		BOPNewLiabExpBaseOverride,
		BOPNewNAICS,
		BOPNewNAICSOverride,
		BOPNewPropRateNo,
		BOPNewPropRateNoOverride,
		BOPNewSIC,
		BOPNewSICOverride,
		BOPPMA,
		BOPPMAOverride,
		BOPRateGroup,
		BOPRateGroupOverride,
		BOPRateNumber,
		BOPRateNumberOverride,
		BOPSquareFootage,
		BuildingPropertyOwnership,
		CSP,
		CSPOverride,
		Description,
		DescriptionBOP,
		Eligible,
		FloorAreaComputation,
		OccupancyType,
		OccupancyTypeMonoline,
		OccupancyTypeOverride,
		RateGroup,
		RateGroupOverride,
		RateNumberRelativity,
		ExtractDate,
		SourceSystemId
	FROM DCBPOccupancyStage
),
EXP_Metadata AS (
	SELECT
	DCBPOccupancyStageId,
	BPRiskId,
	BPOccupancyId,
	SessionId,
	Id,
	AssociationUnitOwners,
	BOPNewEQ,
	BOPNewEQOverride,
	BOPNewEQSL,
	BOPNewEQSLOverride,
	BOPNewLiabClassGroup,
	BOPNewLiabClassGroupOverride,
	BOPNewLiabExpBase,
	BOPNewLiabExpBaseOverride,
	BOPNewNAICS,
	BOPNewNAICSOverride,
	BOPNewPropRateNo,
	BOPNewPropRateNoOverride,
	BOPNewSIC,
	BOPNewSICOverride,
	BOPPMA,
	BOPPMAOverride,
	BOPRateGroup,
	BOPRateGroupOverride,
	BOPRateNumber,
	BOPRateNumberOverride,
	BOPSquareFootage,
	BuildingPropertyOwnership,
	CSP,
	CSPOverride,
	Description,
	DescriptionBOP,
	Eligible,
	FloorAreaComputation,
	OccupancyType,
	OccupancyTypeMonoline,
	OccupancyTypeOverride,
	RateGroup,
	RateGroupOverride,
	RateNumberRelativity,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCBPOccupancyStage
),
ArchDCBPOccupancyStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCBPOccupancyStage
	(DCBPOccupancyStageId, BPRiskId, BPOccupancyId, SessionId, Id, AssociationUnitOwners, BOPNewEQ, BOPNewEQOverride, BOPNewEQSL, BOPNewEQSLOverride, BOPNewLiabClassGroup, BOPNewLiabClassGroupOverride, BOPNewLiabExpBase, BOPNewLiabExpBaseOverride, BOPNewNAICS, BOPNewNAICSOverride, BOPNewPropRateNo, BOPNewPropRateNoOverride, BOPNewSIC, BOPNewSICOverride, BOPPMA, BOPPMAOverride, BOPRateGroup, BOPRateGroupOverride, BOPRateNumber, BOPRateNumberOverride, BOPSquareFootage, BuildingPropertyOwnership, CSP, CSPOverride, Description, DescriptionBOP, Eligible, FloorAreaComputation, OccupancyType, OccupancyTypeMonoline, OccupancyTypeOverride, RateGroup, RateGroupOverride, RateNumberRelativity, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	DCBPOCCUPANCYSTAGEID, 
	BPRISKID, 
	BPOCCUPANCYID, 
	SESSIONID, 
	ID, 
	ASSOCIATIONUNITOWNERS, 
	BOPNEWEQ, 
	BOPNEWEQOVERRIDE, 
	BOPNEWEQSL, 
	BOPNEWEQSLOVERRIDE, 
	BOPNEWLIABCLASSGROUP, 
	BOPNEWLIABCLASSGROUPOVERRIDE, 
	BOPNEWLIABEXPBASE, 
	BOPNEWLIABEXPBASEOVERRIDE, 
	BOPNEWNAICS, 
	BOPNEWNAICSOVERRIDE, 
	BOPNEWPROPRATENO, 
	BOPNEWPROPRATENOOVERRIDE, 
	BOPNEWSIC, 
	BOPNEWSICOVERRIDE, 
	BOPPMA, 
	BOPPMAOVERRIDE, 
	BOPRATEGROUP, 
	BOPRATEGROUPOVERRIDE, 
	BOPRATENUMBER, 
	BOPRATENUMBEROVERRIDE, 
	BOPSQUAREFOOTAGE, 
	BUILDINGPROPERTYOWNERSHIP, 
	CSP, 
	CSPOVERRIDE, 
	DESCRIPTION, 
	DESCRIPTIONBOP, 
	ELIGIBLE, 
	FLOORAREACOMPUTATION, 
	OCCUPANCYTYPE, 
	OCCUPANCYTYPEMONOLINE, 
	OCCUPANCYTYPEOVERRIDE, 
	RATEGROUP, 
	RATEGROUPOVERRIDE, 
	RATENUMBERRELATIVITY, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),