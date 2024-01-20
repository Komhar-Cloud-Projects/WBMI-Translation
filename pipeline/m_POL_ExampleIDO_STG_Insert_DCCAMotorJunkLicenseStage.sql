WITH
SQ_DC_CA_MotorJunkLicense AS (
	WITH cte_DCCAMotorJunkLicense(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CA_StateId, 
	X.CA_MotorJunkLicenseId, 
	X.SessionId, 
	X.Id, 
	X.CertificateOfInsurance, 
	X.Territory 
	FROM
	DC_CA_MotorJunkLicense X
	inner join
	cte_DCCAMotorJunkLicense Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CA_StateId,
	CA_MotorJunkLicenseId,
	SessionId,
	Id,
	CertificateOfInsurance,
	Territory,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CA_MotorJunkLicense
),
DCCAMotorJunkLicenseStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCAMotorJunkLicenseStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCAMotorJunkLicenseStage
	(ExtractDate, SourceSystemId, CA_StateId, CA_MotorJunkLicenseId, SessionId, Id, CertificateOfInsurance, Territory)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CA_STATEID, 
	CA_MOTORJUNKLICENSEID, 
	SESSIONID, 
	ID, 
	CERTIFICATEOFINSURANCE, 
	TERRITORY
	FROM EXP_Metadata
),