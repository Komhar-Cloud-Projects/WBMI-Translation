WITH
SQ_WCDividendPaidStage AS (
	SELECT
		WCDividendPaidStageId,
		WCDividendStageRecID,
		DividendPaidDate,
		DividendPaidAmt,
		AgencyCode,
		PolicySymbol,
		PolicyNumber,
		PolicyModule,
		ExtractDate,
		SourceSystemId
	FROM WCDividendPaidStage
),
EXP_DataCollect AS (
	SELECT
	WCDividendPaidStageId,
	WCDividendStageRecID,
	DividendPaidDate,
	DividendPaidAmt,
	AgencyCode,
	PolicySymbol,
	PolicyNumber,
	PolicyModule,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_WCDividendPaidStage
),
ArchWCDividendPaidStage AS (
	INSERT INTO ArchWCDividendPaidStage
	(WCDividendPaidStageId, WCDividendStageRecID, DividendPaidDate, DividendPaidAmt, AgencyCode, PolicySymbol, PolicyNumber, PolicyModule, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	WCDIVIDENDPAIDSTAGEID, 
	WCDIVIDENDSTAGERECID, 
	DIVIDENDPAIDDATE, 
	DIVIDENDPAIDAMT, 
	AGENCYCODE, 
	POLICYSYMBOL, 
	POLICYNUMBER, 
	POLICYMODULE, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID
	FROM EXP_DataCollect
),