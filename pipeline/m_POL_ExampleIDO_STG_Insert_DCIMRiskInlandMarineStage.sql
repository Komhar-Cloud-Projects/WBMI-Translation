WITH
SQ_DC_IM_RiskInlandMarine AS (
	WITH cte_DCIMRiskInlandMarine(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.IM_RiskId, 
	X.IM_RiskInlandMarineId, 
	X.SessionId, 
	X.Id, 
	X.Type, 
	X.BG1PersonalPropertyBaseLossCostOverride, 
	X.BG1PlusBG2PersonalPropertyRateOverride, 
	X.BG2PersonalPropertyBaseLossCostOverride, 
	X.CompanyFactor, 
	X.CompanyRate, 
	X.ExcessRate, 
	X.PremiumBase, 
	X.TentativeRate, 
	X.NamedStormMaximum, 
	X.NamedStormMinimum, 
	X.NamedStormPercentage, 
	X.ReceptacleTypeA, 
	X.ReceptacleTypeB, 
	X.ReceptacleTypeC, 
	X.ReceptacleTypeD, 
	X.ReceptacleTypeE, 
	X.ReceptacleTypeF, 
	X.ReceptacleTypeG 
	FROM
	DC_IM_RiskInlandMarine X
	inner join
	cte_DCIMRiskInlandMarine Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	IM_RiskId,
	IM_RiskInlandMarineId,
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
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_IM_RiskInlandMarine
),
DCIMRiskInlandMarineStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCIMRiskInlandMarineStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCIMRiskInlandMarineStage
	(IMRiskId, IMRiskInlandMarineId, SessionId, Id, Type, BG1PersonalPropertyBaseLossCostOverride, BG1PlusBG2PersonalPropertyRateOverride, BG2PersonalPropertyBaseLossCostOverride, CompanyFactor, CompanyRate, ExcessRate, PremiumBase, TentativeRate, NamedStormMaximum, NamedStormMinimum, NamedStormPercentage, ReceptacleTypeA, ReceptacleTypeB, ReceptacleTypeC, ReceptacleTypeD, ReceptacleTypeE, ReceptacleTypeF, ReceptacleTypeG, ExtractDate, SourceSystemId)
	SELECT 
	IM_RiskId AS IMRISKID, 
	IM_RiskInlandMarineId AS IMRISKINLANDMARINEID, 
	SESSIONID, 
	ID, 
	TYPE, 
	BG1PERSONALPROPERTYBASELOSSCOSTOVERRIDE, 
	BG1PLUSBG2PERSONALPROPERTYRATEOVERRIDE, 
	BG2PERSONALPROPERTYBASELOSSCOSTOVERRIDE, 
	COMPANYFACTOR, 
	COMPANYRATE, 
	EXCESSRATE, 
	PREMIUMBASE, 
	TENTATIVERATE, 
	NAMEDSTORMMAXIMUM, 
	NAMEDSTORMMINIMUM, 
	NAMEDSTORMPERCENTAGE, 
	RECEPTACLETYPEA, 
	RECEPTACLETYPEB, 
	RECEPTACLETYPEC, 
	RECEPTACLETYPED, 
	RECEPTACLETYPEE, 
	RECEPTACLETYPEF, 
	RECEPTACLETYPEG, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),