WITH
SQ_WBCACoveragePIPStage AS (
	SELECT
		WBCACoveragePIPStageId,
		ExtractDate,
		SourceSystemId,
		CACoveragePIPId,
		SessionId,
		RideSharingArrangement,
		RideSharingUsage,
		MedicalExpenses,
		WBCACoveragePIPId
	FROM WBCACoveragePIPStage
),
EXPTRANS AS (
	SELECT
	WBCACoveragePIPStageId,
	CACoveragePIPId,
	WBCACoveragePIPId,
	SessionId,
	RideSharingArrangement,
	RideSharingUsage,
	MedicalExpenses,
	ExtractDate,
	SourceSystemId
	FROM SQ_WBCACoveragePIPStage
),
ArchWBCACoveragePIPStage AS (
	INSERT INTO ArchWBCACoveragePIPStage
	(WBCACoveragePIPStageId, ExtractDate, SourceSystemId, CACoveragePIPId, WBCACoveragePIPId, SessionId, RideSharingArrangement, RideSharingUsage, MedicalExpenses)
	SELECT 
	WBCACOVERAGEPIPSTAGEID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	CACOVERAGEPIPID, 
	WBCACOVERAGEPIPID, 
	SESSIONID, 
	RIDESHARINGARRANGEMENT, 
	RIDESHARINGUSAGE, 
	MEDICALEXPENSES
	FROM EXPTRANS
),