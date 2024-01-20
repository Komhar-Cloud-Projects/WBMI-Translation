WITH
SQ_ISOFireProtectStage AS (
	SELECT
		ISOFireProtectStageID,
		ExtractDate,
		SourceSyStemID,
		StateCode,
		City,
		County,
		ISOEffDate,
		ISOExpDate,
		DeleteFlag,
		ModifiedBy,
		TaxLoc,
		MineSubsidFlag,
		HydrantExclFlag,
		ProtectClass,
		AltProtectClass,
		BcegsEffYear,
		CommrclInsType,
		PersInsType,
		Footnote
	FROM ISOFireProtectStage
),
EXP_Metadata AS (
	SELECT
	ISOFireProtectStageID,
	ExtractDate,
	SourceSyStemID,
	StateCode,
	City,
	County,
	ISOEffDate,
	ISOExpDate,
	DeleteFlag,
	ModifiedBy,
	TaxLoc,
	MineSubsidFlag,
	HydrantExclFlag,
	ProtectClass,
	AltProtectClass,
	BcegsEffYear,
	CommrclInsType,
	PersInsType,
	Footnote,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_ISOFireProtectStage
),
ArchISOFireProtectStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchISOFireProtectStage
	(ISOFireProtectStageID, ExtractDate, SourceSyStemID, AuditID, StateCode, City, County, ISOEffDate, ISOExpDate, DeleteFlag, ModifiedBy, TaxLoc, MineSubsidFlag, HydrantExclFlag, ProtectClass, AltProtectClass, BcegsEffYear, CommrclInsType, PersInsType, Footnote)
	SELECT 
	ISOFIREPROTECTSTAGEID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	STATECODE, 
	CITY, 
	COUNTY, 
	ISOEFFDATE, 
	ISOEXPDATE, 
	DELETEFLAG, 
	MODIFIEDBY, 
	TAXLOC, 
	MINESUBSIDFLAG, 
	HYDRANTEXCLFLAG, 
	PROTECTCLASS, 
	ALTPROTECTCLASS, 
	BCEGSEFFYEAR, 
	COMMRCLINSTYPE, 
	PERSINSTYPE, 
	FOOTNOTE
	FROM EXP_Metadata
),