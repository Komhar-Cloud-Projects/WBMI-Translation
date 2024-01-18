WITH
SQ_DCIMRiskInlandMarineStage AS (
	SELECT
		DCIMRiskInlandMarineStageId,
		IMRiskId,
		IMRiskInlandMarineId,
		SessionId,
		Id,
		Type,
		BG1PersonalPropertyBaseLossCostOverride,
		BG1PlusBG2PersonalPropertyRateOverride,
		BG2PersonalPropertyBaseLossCostOverride,
		CompanyFactor,
		CompanyRate,
		ExcessRate,
		PremiumBase,
		TentativeRate,
		NamedStormMaximum,
		NamedStormMinimum,
		NamedStormPercentage,
		ReceptacleTypeA,
		ReceptacleTypeB,
		ReceptacleTypeC,
		ReceptacleTypeD,
		ReceptacleTypeE,
		ReceptacleTypeF,
		ReceptacleTypeG,
		ExtractDate,
		SourceSystemId
	FROM DCIMRiskInlandMarineStage
),
EXP_Metadata AS (
	SELECT
	DCIMRiskInlandMarineStageId,
	IMRiskId,
	IMRiskInlandMarineId,
	SessionId,
	Id,
	Type,
	BG1PersonalPropertyBaseLossCostOverride,
	BG1PlusBG2PersonalPropertyRateOverride,
	BG2PersonalPropertyBaseLossCostOverride,
	CompanyFactor,
	CompanyRate,
	ExcessRate AS i_ExcessRate,
	-- *INF*: DECODE(i_ExcessRate, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ExcessRate,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ExcessRate,
	PremiumBase,
	TentativeRate AS i_TentativeRate,
	-- *INF*: DECODE(i_TentativeRate, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TentativeRate,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TentativeRate,
	NamedStormMaximum,
	NamedStormMinimum,
	NamedStormPercentage,
	ReceptacleTypeA AS i_ReceptacleTypeA,
	-- *INF*: DECODE(i_ReceptacleTypeA, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ReceptacleTypeA,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ReceptacleTypeA,
	ReceptacleTypeB AS i_ReceptacleTypeB,
	-- *INF*: DECODE(i_ReceptacleTypeB, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ReceptacleTypeB,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ReceptacleTypeB,
	ReceptacleTypeC AS i_ReceptacleTypeC,
	-- *INF*: DECODE(i_ReceptacleTypeC, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ReceptacleTypeC,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ReceptacleTypeC,
	ReceptacleTypeD AS i_ReceptacleTypeD,
	-- *INF*: DECODE(i_ReceptacleTypeD, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ReceptacleTypeD,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ReceptacleTypeD,
	ReceptacleTypeE AS i_ReceptacleTypeE,
	-- *INF*: DECODE(i_ReceptacleTypeE, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ReceptacleTypeE,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ReceptacleTypeE,
	ReceptacleTypeF AS i_ReceptacleTypeF,
	-- *INF*: DECODE(i_ReceptacleTypeF, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ReceptacleTypeF,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ReceptacleTypeF,
	ReceptacleTypeG AS i_ReceptacleTypeG,
	-- *INF*: DECODE(i_ReceptacleTypeG, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ReceptacleTypeG,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ReceptacleTypeG,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCIMRiskInlandMarineStage
),
ArchDCIMRiskInlandMarineStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCIMRiskInlandMarineStage
	(DCIMRiskInlandMarineStageId, IMRiskId, IMRiskInlandMarineId, SessionId, Id, Type, BG1PersonalPropertyBaseLossCostOverride, BG1PlusBG2PersonalPropertyRateOverride, BG2PersonalPropertyBaseLossCostOverride, CompanyFactor, CompanyRate, ExcessRate, PremiumBase, TentativeRate, NamedStormMaximum, NamedStormMinimum, NamedStormPercentage, ReceptacleTypeA, ReceptacleTypeB, ReceptacleTypeC, ReceptacleTypeD, ReceptacleTypeE, ReceptacleTypeF, ReceptacleTypeG, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	DCIMRISKINLANDMARINESTAGEID, 
	IMRISKID, 
	IMRISKINLANDMARINEID, 
	SESSIONID, 
	ID, 
	TYPE, 
	BG1PERSONALPROPERTYBASELOSSCOSTOVERRIDE, 
	BG1PLUSBG2PERSONALPROPERTYRATEOVERRIDE, 
	BG2PERSONALPROPERTYBASELOSSCOSTOVERRIDE, 
	COMPANYFACTOR, 
	COMPANYRATE, 
	o_ExcessRate AS EXCESSRATE, 
	PREMIUMBASE, 
	o_TentativeRate AS TENTATIVERATE, 
	NAMEDSTORMMAXIMUM, 
	NAMEDSTORMMINIMUM, 
	NAMEDSTORMPERCENTAGE, 
	o_ReceptacleTypeA AS RECEPTACLETYPEA, 
	o_ReceptacleTypeB AS RECEPTACLETYPEB, 
	o_ReceptacleTypeC AS RECEPTACLETYPEC, 
	o_ReceptacleTypeD AS RECEPTACLETYPED, 
	o_ReceptacleTypeE AS RECEPTACLETYPEE, 
	o_ReceptacleTypeF AS RECEPTACLETYPEF, 
	o_ReceptacleTypeG AS RECEPTACLETYPEG, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),