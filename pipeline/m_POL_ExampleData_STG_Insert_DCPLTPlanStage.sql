WITH
SQ_DC_PLT_Plan AS (
	SELECT DC_PLT_Plan.PlanId, DC_PLT_Plan.AgencyId, DC_PLT_Plan.LineOfBusinessCode, DC_PLT_Plan.MasterCompanyCode, 
	DC_PLT_Plan.PlanActivationDate, DC_PLT_Plan.PlanExpirationDate, DC_PLT_Plan.PolicyInceptionDate, 
	DC_PLT_Plan.ProductCode, DC_PLT_Plan.StateCode, DC_PLT_Plan.UserKey1, DC_PLT_Plan.UserKey2, DC_PLT_Plan.UserKey3, 
	DC_PLT_Plan.UserKey4, DC_PLT_Plan.UserKey5, DC_PLT_Plan.PlanClassCode, 
	DC_PLT_Plan.PlanTypeCode, null as PlanData, DC_PLT_Plan.LastUpdatedTimestamp, DC_PLT_Plan.LastUpdatedUserId 
	FROM DC_PLT_Plan with(nolock)
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	PlanId,
	AgencyId,
	LineOfBusinessCode,
	MasterCompanyCode,
	PlanActivationDate,
	PlanExpirationDate,
	PolicyInceptionDate,
	ProductCode,
	StateCode,
	UserKey1,
	UserKey2,
	UserKey3,
	UserKey4,
	UserKey5,
	PlanClassCode,
	PlanTypeCode,
	PlanData,
	LastUpdatedTimestamp,
	LastUpdatedUserId
	FROM SQ_DC_PLT_Plan
),
DCPLTPlanStage AS (
	TRUNCATE TABLE DCPLTPlanStage;
	INSERT INTO DCPLTPlanStage
	(ExtractDate, SourceSystemId, PlanId, AgencyId, LineOfBusinessCode, MasterCompanyCode, PlanActivationDate, PlanExpirationDate, PolicyInceptionDate, ProductCode, StateCode, UserKey1, UserKey2, UserKey3, UserKey4, UserKey5, PlanClassCode, PlanTypeCode, PlanData, LastUpdatedTimestamp, LastUpdatedUserId)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	PLANID, 
	AGENCYID, 
	LINEOFBUSINESSCODE, 
	MASTERCOMPANYCODE, 
	PLANACTIVATIONDATE, 
	PLANEXPIRATIONDATE, 
	POLICYINCEPTIONDATE, 
	PRODUCTCODE, 
	STATECODE, 
	USERKEY1, 
	USERKEY2, 
	USERKEY3, 
	USERKEY4, 
	USERKEY5, 
	PLANCLASSCODE, 
	PLANTYPECODE, 
	PLANDATA, 
	LASTUPDATEDTIMESTAMP, 
	LASTUPDATEDUSERID
	FROM EXP_Metadata
),