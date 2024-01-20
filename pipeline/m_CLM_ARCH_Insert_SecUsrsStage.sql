WITH
SQ_SecUsrsStage AS (
	SELECT
		SecUsrsStageId,
		SecUsrId,
		SecUsrTypeCd,
		SecLstLogonDt,
		SecLstLogonTm,
		SecUsrEffDt,
		SecUsrExpDt,
		SecUsrCltId,
		ExtractDate,
		SourceSystemId
	FROM SecUsrsStage
),
EXPTRANS AS (
	SELECT
	SecUsrsStageId,
	SecUsrId,
	SecUsrTypeCd,
	SecLstLogonDt,
	SecLstLogonTm,
	SecUsrEffDt,
	SecUsrExpDt,
	SecUsrCltId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_SecUsrsStage
),
ArchSecUsrsStage AS (
	INSERT INTO ArchSecUsrsStage
	(SecUsrsStageId, SecUsrId, SecUsrTypeCd, SecLstLogonDt, SecLstLogonTm, SecUsrEffDt, SecUsrExpDt, SecUsrCltId, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	SECUSRSSTAGEID, 
	SECUSRID, 
	SECUSRTYPECD, 
	SECLSTLOGONDT, 
	SECLSTLOGONTM, 
	SECUSREFFDT, 
	SECUSREXPDT, 
	SECUSRCLTID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID
	FROM EXPTRANS
),