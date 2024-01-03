WITH
SQ_WB_EDWInceptionToDate1 AS (
	SELECT
		PolicyNumber,
		PolicyVersion,
		EffectiveDate,
		ExpirationDate,
		IDOWrittenChange,
		IDOTaxesChange,
		DCTWrittenChange,
		DCTTaxesChange,
		DCTIDOWrittenChangeOOBAmount,
		DCTIDOTaxesChangeOOBAmount,
		ModifiedDate,
		WBEDWInceptionToDateId
	FROM WB_EDWInceptionToDate
),
EXP_Default_Value AS (
	SELECT
	@{pipeline().parameters.AUDITID} AS o_AuditId,
	@{pipeline().parameters.SOURCESYSTEMID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	PolicyNumber AS i_PolicyNumber,
	PolicyVersion AS i_PolicyVersion,
	EffectiveDate,
	ExpirationDate,
	IDOWrittenChange AS i_IDOWrittenChange,
	IDOTaxesChange AS i_IDOTaxesChange,
	DCTWrittenChange AS i_DCTWrittenChange,
	DCTTaxesChange AS i_DCTTaxesChange,
	DCTIDOWrittenChangeOOBAmount AS i_DCTIDOWrittenChangeOOBAmount,
	DCTIDOTaxesChangeOOBAmount AS i_DCTIDOTaxesChangeOOBAmount,
	ModifiedDate,
	WBEDWInceptionToDateId AS i_WBEDWInceptionToDateId,
	-- *INF*: IIF(NOT ISNULL(i_PolicyNumber), i_PolicyNumber, '0')
	IFF(NOT i_PolicyNumber IS NULL, i_PolicyNumber, '0') AS o_PolicyNumber,
	-- *INF*: IIF(NOT ISNULL(i_PolicyVersion), i_PolicyVersion, '0')
	IFF(NOT i_PolicyVersion IS NULL, i_PolicyVersion, '0') AS o_PolicyVersion,
	-- *INF*: IIF(NOT ISNULL(i_IDOWrittenChange), i_IDOWrittenChange, 0)
	IFF(NOT i_IDOWrittenChange IS NULL, i_IDOWrittenChange, 0) AS o_IDOWrittenChange,
	-- *INF*: IIF(NOT ISNULL(i_IDOTaxesChange), i_IDOTaxesChange, 0)
	IFF(NOT i_IDOTaxesChange IS NULL, i_IDOTaxesChange, 0) AS o_IDOTaxesChange,
	-- *INF*: IIF(NOT ISNULL(i_DCTWrittenChange), i_DCTWrittenChange, 0)
	IFF(NOT i_DCTWrittenChange IS NULL, i_DCTWrittenChange, 0) AS o_DCTWrittenChange,
	-- *INF*: IIF(NOT ISNULL(i_DCTTaxesChange), i_DCTTaxesChange, 0)
	IFF(NOT i_DCTTaxesChange IS NULL, i_DCTTaxesChange, 0) AS o_DCTTaxesChange,
	-- *INF*: IIF(NOT ISNULL(i_DCTIDOWrittenChangeOOBAmount), i_DCTIDOWrittenChangeOOBAmount, 0)
	IFF(NOT i_DCTIDOWrittenChangeOOBAmount IS NULL, i_DCTIDOWrittenChangeOOBAmount, 0) AS o_DCTIDOWrittenChangeOOBAmount,
	-- *INF*: IIF(NOT ISNULL(i_DCTIDOTaxesChangeOOBAmount), i_DCTIDOTaxesChangeOOBAmount, 0)
	IFF(NOT i_DCTIDOTaxesChangeOOBAmount IS NULL, i_DCTIDOTaxesChangeOOBAmount, 0) AS o_DCTIDOTaxesChangeOOBAmount,
	-- *INF*: IIF(NOT ISNULL(i_WBEDWInceptionToDateId), i_WBEDWInceptionToDateId, 0)
	IFF(NOT i_WBEDWInceptionToDateId IS NULL, i_WBEDWInceptionToDateId, 0) AS o_WBEDWInceptionToDateId
	FROM SQ_WB_EDWInceptionToDate1
),
WBEDWInceptionToDate AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBEDWInceptionToDate;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBEDWInceptionToDate
	(AuditId, SourceSystemId, CreatedDate, ModifiedDate, PolicyNumber, PolicyVersion, EffectiveDate, ExpirationDate, IDOWrittenChange, IDOTaxesChange, DCTWrittenChange, DCTTaxesChange, DCTIDOWrittenChangeOOBAmount, DCTIDOTaxesChangeOOBAmount, SourceModifiedDate, SourceWBEDWInceptionToDateId)
	SELECT 
	o_AuditId AS AUDITID, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_PolicyNumber AS POLICYNUMBER, 
	o_PolicyVersion AS POLICYVERSION, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	o_IDOWrittenChange AS IDOWRITTENCHANGE, 
	o_IDOTaxesChange AS IDOTAXESCHANGE, 
	o_DCTWrittenChange AS DCTWRITTENCHANGE, 
	o_DCTTaxesChange AS DCTTAXESCHANGE, 
	o_DCTIDOWrittenChangeOOBAmount AS DCTIDOWRITTENCHANGEOOBAMOUNT, 
	o_DCTIDOTaxesChangeOOBAmount AS DCTIDOTAXESCHANGEOOBAMOUNT, 
	ModifiedDate AS SOURCEMODIFIEDDATE, 
	o_WBEDWInceptionToDateId AS SOURCEWBEDWINCEPTIONTODATEID
	FROM EXP_Default_Value
),