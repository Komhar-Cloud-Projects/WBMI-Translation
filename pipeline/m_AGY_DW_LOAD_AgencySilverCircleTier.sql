WITH
SQ_AgencySilverCircleTierStaging AS (
	SELECT
		AgencySilverCircleTierStageId,
		AgencyCode,
		SilverCircleYear,
		SilverCircleLevelDescription,
		HashKey,
		ModifiedUserId,
		ModifiedDate,
		ExtractDate,
		SourceSystemId
	FROM AgencySilverCircleTierStaging
),
EXP_Input AS (
	SELECT
	AgencyCode,
	SilverCircleYear,
	SilverCircleLevelDescription AS i_SilverCircleLevelDescription,
	-- *INF*: IIF(UPPER(i_SilverCircleLevelDescription)='NONE','Emerging',i_SilverCircleLevelDescription)
	IFF(
	    UPPER(i_SilverCircleLevelDescription) = 'NONE', 'Emerging', i_SilverCircleLevelDescription
	) AS o_SilverCircleLevelDescription,
	HashKey,
	ModifiedUserId,
	ModifiedDate,
	CURRENT_TIMESTAMP AS CreatedDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_AgencySilverCircleTierStaging
),
AgencySilverCircleTier AS (
	TRUNCATE TABLE AgencySilverCircleTier;
	INSERT INTO AgencySilverCircleTier
	(AgencyCode, SilverCircleYear, SilverCircleLevelDescription, HashKey, ModifiedUserId, ModifiedDate, CreatedDate, SourceSystemId, AuditId)
	SELECT 
	AGENCYCODE, 
	SILVERCIRCLEYEAR, 
	o_SilverCircleLevelDescription AS SILVERCIRCLELEVELDESCRIPTION, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	CREATEDDATE, 
	SOURCESYSTEMID, 
	AUDITID
	FROM EXP_Input
),
SQ_AgencyDim AS (
	With CTE as(
	SELECT DISTINCT
	A.AgencyCode as AgencyCode,
	A.SilverCircleYear as SilverCircleYear 
	FROM 
	RPT_EDM.dbo.AgencySilverCircleTier A
	)
	
	
	SELECT DISTINCT
	A.AgencyCode as AgencyCode,
	A.CalendarYear as CalendarYear from 
	(
	SELECT DISTINCT
	AgencyCode as AgencyCode,
	CalendarYear as CalendarYear
	FROM 
	@{pipeline().parameters.DM_DATABASE_NAME}.dbo.calendar_dim, 
	@{pipeline().parameters.DM_DATABASE_NAME}.v3.AgencyDim
	WHERE
	CalendarYear between('2010') and year(getdate()) and CurrentSnapshotFlag=1) A
	LEFT JOIN CTE 
		on CTE.AgencyCode=A.AgencyCode and CTE.SilverCircleYear=A.CalendarYear
	WHERE  CTE.AgencyCode is null
	ORDER BY 1,2
),
EXP_SetDefaultValues AS (
	SELECT
	AgencyCode,
	CalendarYear AS Year,
	'N/A' AS DefaultNA,
	CURRENT_TIMESTAMP AS DefautDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_AgencyDim
),
AgencySilverCircleTier_NonSCA AS (
	INSERT INTO AgencySilverCircleTier
	(AgencyCode, SilverCircleYear, SilverCircleLevelDescription, HashKey, ModifiedUserId, ModifiedDate, CreatedDate, SourceSystemId, AuditId)
	SELECT 
	AGENCYCODE, 
	Year AS SILVERCIRCLEYEAR, 
	DefaultNA AS SILVERCIRCLELEVELDESCRIPTION, 
	DefaultNA AS HASHKEY, 
	DefaultNA AS MODIFIEDUSERID, 
	DefautDate AS MODIFIEDDATE, 
	DefautDate AS CREATEDDATE, 
	DefaultNA AS SOURCESYSTEMID, 
	AUDITID
	FROM EXP_SetDefaultValues
),