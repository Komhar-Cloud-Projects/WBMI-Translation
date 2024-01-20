WITH
SQ_DC_CA_HiredAndBorrow AS (
	WITH cte_DCCAHiredAndBorrow(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT X.LineId, 
	X.CA_StateId, 
	X.CA_HiredAndBorrowId, 
	X.SessionId, 
	X.Id, 
	X.Auditable, 
	X.ExtendedAddlInsured, 
	X.HoldHarmlessAgreement, 
	X.InsuranceRequired, 
	X.PermittedCarrier, 
	X.StatedAmountAudit, 
	X.StatedAmountEstimate, 
	X.Type 
	FROM
	DC_CA_HiredAndBorrow X
	inner join
	cte_DCCAHiredAndBorrow Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	CA_StateId,
	CA_HiredAndBorrowId,
	SessionId,
	Id,
	Auditable AS i_Auditable,
	ExtendedAddlInsured AS i_ExtendedAddlInsured,
	HoldHarmlessAgreement AS i_HoldHarmlessAgreement,
	InsuranceRequired AS i_InsuranceRequired,
	PermittedCarrier AS i_PermittedCarrier,
	-- *INF*: DECODE(i_Auditable, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_Auditable,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_Auditable,
	-- *INF*: DECODE(i_ExtendedAddlInsured, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ExtendedAddlInsured,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ExtendedAddlInsured,
	-- *INF*: DECODE(i_HoldHarmlessAgreement, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_HoldHarmlessAgreement,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_HoldHarmlessAgreement,
	-- *INF*: DECODE(i_InsuranceRequired, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_InsuranceRequired,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_InsuranceRequired,
	-- *INF*: DECODE(i_PermittedCarrier, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_PermittedCarrier,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_PermittedCarrier,
	StatedAmountAudit,
	StatedAmountEstimate,
	Type,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CA_HiredAndBorrow
),
DCCAHiredAndBorrowStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCAHiredAndBorrowStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCAHiredAndBorrowStage
	(ExtractDate, SourceSystemId, LineId, CA_StateId, CA_HiredAndBorrowId, SessionId, Id, Auditable, ExtendedAddlInsured, HoldHarmlessAgreement, InsuranceRequired, PermittedCarrier, StatedAmountAudit, StatedAmountEstimate, Type)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LINEID, 
	CA_STATEID, 
	CA_HIREDANDBORROWID, 
	SESSIONID, 
	ID, 
	o_Auditable AS AUDITABLE, 
	o_ExtendedAddlInsured AS EXTENDEDADDLINSURED, 
	o_HoldHarmlessAgreement AS HOLDHARMLESSAGREEMENT, 
	o_InsuranceRequired AS INSURANCEREQUIRED, 
	o_PermittedCarrier AS PERMITTEDCARRIER, 
	STATEDAMOUNTAUDIT, 
	STATEDAMOUNTESTIMATE, 
	TYPE
	FROM EXP_Metadata
),