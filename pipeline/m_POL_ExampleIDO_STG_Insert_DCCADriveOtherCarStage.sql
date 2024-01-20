WITH
SQ_DC_CA_DriveOtherCar AS (
	WITH cte_DCCADriveOtherCar(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CA_StateId, 
	X.CA_DriveOtherCarId, 
	X.SessionId, X.Id, 
	X.CertificateOfInsurance, 
	X.NumberOfEmployeesEstimate, 
	X.RiskDOCStacked, 
	X.RiskDOCUIMStacked, 
	X.Territory 
	FROM
	DC_CA_DriveOtherCar X
	inner join
	cte_DCCADriveOtherCar Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CA_StateId,
	CA_DriveOtherCarId,
	SessionId,
	Id,
	CertificateOfInsurance,
	NumberOfEmployeesEstimate,
	RiskDOCStacked AS i_RiskDOCStacked,
	RiskDOCUIMStacked AS i_RiskDOCUIMStacked,
	-- *INF*: DECODE(i_RiskDOCStacked, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_RiskDOCStacked,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_RiskDOCStacked,
	-- *INF*: DECODE(i_RiskDOCUIMStacked, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_RiskDOCUIMStacked,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_RiskDOCUIMStacked,
	Territory,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CA_DriveOtherCar
),
DCCADriveOtherCarStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCADriveOtherCarStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCADriveOtherCarStage
	(ExtractDate, SourceSystemId, CA_StateId, CA_DriveOtherCarId, SessionId, Id, CertificateOfInsurance, NumberOfEmployeesEstimate, RiskDOCStacked, RiskDOCUIMStacked, Territory)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CA_STATEID, 
	CA_DRIVEOTHERCARID, 
	SESSIONID, 
	ID, 
	CERTIFICATEOFINSURANCE, 
	NUMBEROFEMPLOYEESESTIMATE, 
	o_RiskDOCStacked AS RISKDOCSTACKED, 
	o_RiskDOCUIMStacked AS RISKDOCUIMSTACKED, 
	TERRITORY
	FROM EXP_Metadata
),