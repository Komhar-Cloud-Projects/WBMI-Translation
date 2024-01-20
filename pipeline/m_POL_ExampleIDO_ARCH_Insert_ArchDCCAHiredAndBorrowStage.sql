WITH
SQ_DCCAHiredAndBorrowStage AS (
	SELECT
		DCCAHiredAndBorrowStageId,
		ExtractDate,
		SourceSystemId,
		LineId,
		CA_StateId,
		CA_HiredAndBorrowId,
		SessionId,
		Id,
		Auditable,
		ExtendedAddlInsured,
		HoldHarmlessAgreement,
		InsuranceRequired,
		PermittedCarrier,
		StatedAmountAudit,
		StatedAmountEstimate,
		Type
	FROM DCCAHiredAndBorrowStage
),
EXP_Metadata AS (
	SELECT
	DCCAHiredAndBorrowStageId,
	ExtractDate,
	SourceSystemId,
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
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCAHiredAndBorrowStage
),
ArchDCCAHiredAndBorrowStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCAHiredAndBorrowStage
	(ExtractDate, SourceSystemId, AuditId, DCCAHiredAndBorrowStageId, LineId, CA_StateId, CA_HiredAndBorrowId, SessionId, Id, Auditable, ExtendedAddlInsured, HoldHarmlessAgreement, InsuranceRequired, PermittedCarrier, StatedAmountAudit, StatedAmountEstimate, Type)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCCAHIREDANDBORROWSTAGEID, 
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