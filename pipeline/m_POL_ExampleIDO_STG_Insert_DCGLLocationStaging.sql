WITH
SQ_DC_GL_Location AS (
	WITH cte_DCGLLocation(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.GL_LocationId, 
	X.SessionId, 
	X.Id, 
	X.ExcludeCoverageCMedicalPayments, 
	X.TerrorismTerritory, 
	X.Territory,
	X.Description,
	X.[Number]   
	FROM
	DC_GL_Location X
	inner join
	cte_DCGLLocation Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	GL_LocationId,
	SessionId,
	Id,
	ExcludeCoverageCMedicalPayments,
	TerrorismTerritory,
	Territory,
	-- *INF*: DECODE(ExcludeCoverageCMedicalPayments, 'T', 1, 'F', 0 , NULL)
	DECODE(
	    ExcludeCoverageCMedicalPayments,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ExcludeCoverageCMedicalPayments,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	Description,
	Number
	FROM SQ_DC_GL_Location
),
DCGLLocationStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCGLLocationStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCGLLocationStaging
	(GL_LocationId, SessionId, Id, ExcludeCoverageCMedicalPayments, TerrorismTerritory, Territory, ExtractDate, SourceSystemId, Description, Number)
	SELECT 
	GL_LOCATIONID, 
	SESSIONID, 
	ID, 
	o_ExcludeCoverageCMedicalPayments AS EXCLUDECOVERAGECMEDICALPAYMENTS, 
	TERRORISMTERRITORY, 
	TERRITORY, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	DESCRIPTION, 
	NUMBER
	FROM EXP_Metadata
),