WITH
SQ_DC_CA_NonOwned AS (
	WITH cte_DCCANonOwned(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.CA_NonOwnedId, 
	X.SessionId, 
	X.Id, 
	X.Auditable, 
	X.ExtendedEmployeeCov, 
	X.ExtendedVolunteerCov, 
	X.GarageServiceOperations, 
	X.MoreThan50PercentBusinessUse, 
	X.MoreThan50PercentBusinessUseAudit, 
	X.MoreThan50PercentBusinessUseEstimate, 
	X.NumberOfEmployees, 
	X.NumberOfEmployeesAudit, 
	X.NumberOfEmployeesEstimate, 
	X.NumberOfInstructors, 
	X.NumberOfInstructorsAudit, 
	X.NumberOfInstructorsEstimate, 
	X.NumberOfPartners, 
	X.NumberOfPartnersAudit, 
	X.NumberOfPartnersEstimate, 
	X.NumberOfVolunteers, 
	X.NumberOfVolunteersAudit, 
	X.NumberOfVolunteersEstimate, 
	X.PartnersRateTerritory, 
	X.SocialServiceAgency, 
	X.SocialServiceAgencyRisks, 
	X.SocialServiceAgencyRisksARate 
	FROM
	DC_CA_NonOwned X
	inner join
	cte_DCCANonOwned Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	CA_NonOwnedId,
	SessionId,
	Id,
	Auditable,
	ExtendedEmployeeCov AS i_ExtendedEmployeeCov,
	ExtendedVolunteerCov AS i_ExtendedVolunteerCov,
	GarageServiceOperations AS i_GarageServiceOperations,
	MoreThan50PercentBusinessUse AS i_MoreThan50PercentBusinessUse,
	MoreThan50PercentBusinessUseAudit AS i_MoreThan50PercentBusinessUseAudit,
	MoreThan50PercentBusinessUseEstimate AS i_MoreThan50PercentBusinessUseEstimate,
	-- *INF*: DECODE(i_ExtendedEmployeeCov, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ExtendedEmployeeCov,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ExtendedEmployeeCov,
	-- *INF*: DECODE(i_ExtendedVolunteerCov, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ExtendedVolunteerCov,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ExtendedVolunteerCov,
	-- *INF*: DECODE(i_GarageServiceOperations, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_GarageServiceOperations,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_GarageServiceOperations,
	-- *INF*: DECODE(i_MoreThan50PercentBusinessUse, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_MoreThan50PercentBusinessUse,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_MoreThan50PercentBusinessUse,
	-- *INF*: DECODE(i_MoreThan50PercentBusinessUseAudit, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_MoreThan50PercentBusinessUseAudit,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_MoreThan50PercentBusinessUseAudit,
	-- *INF*: DECODE(i_MoreThan50PercentBusinessUseEstimate, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_MoreThan50PercentBusinessUseEstimate,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_MoreThan50PercentBusinessUseEstimate,
	NumberOfEmployees,
	NumberOfEmployeesAudit,
	NumberOfEmployeesEstimate,
	NumberOfInstructors,
	NumberOfInstructorsAudit,
	NumberOfInstructorsEstimate,
	NumberOfPartners,
	NumberOfPartnersAudit,
	NumberOfPartnersEstimate,
	NumberOfVolunteers,
	NumberOfVolunteersAudit,
	NumberOfVolunteersEstimate,
	PartnersRateTerritory,
	SocialServiceAgency AS i_SocialServiceAgency,
	SocialServiceAgencyRisks AS i_SocialServiceAgencyRisks,
	-- *INF*: DECODE(i_SocialServiceAgency, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_SocialServiceAgency,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_SocialServiceAgency,
	-- *INF*: DECODE(i_SocialServiceAgencyRisks, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_SocialServiceAgencyRisks,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_SocialServiceAgencyRisks,
	SocialServiceAgencyRisksARate,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CA_NonOwned
),
DCCANonOwnedStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCANonOwnedStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCANonOwnedStage
	(ExtractDate, SourceSystemId, LineId, CA_NonOwnedId, SessionId, Id, Auditable, ExtendedEmployeeCov, ExtendedVolunteerCov, GarageServiceOperations, MoreThan50PercentBusinessUse, MoreThan50PercentBusinessUseAudit, MoreThan50PercentBusinessUseEstimate, NumberOfEmployees, NumberOfEmployeesAudit, NumberOfEmployeesEstimate, NumberOfInstructors, NumberOfInstructorsAudit, NumberOfInstructorsEstimate, NumberOfPartners, NumberOfPartnersAudit, NumberOfPartnersEstimate, NumberOfVolunteers, NumberOfVolunteersAudit, NumberOfVolunteersEstimate, PartnersRateTerritory, SocialServiceAgency, SocialServiceAgencyRisks, SocialServiceAgencyRisksARate)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LINEID, 
	CA_NONOWNEDID, 
	SESSIONID, 
	ID, 
	AUDITABLE, 
	o_ExtendedEmployeeCov AS EXTENDEDEMPLOYEECOV, 
	o_ExtendedVolunteerCov AS EXTENDEDVOLUNTEERCOV, 
	o_GarageServiceOperations AS GARAGESERVICEOPERATIONS, 
	o_MoreThan50PercentBusinessUse AS MORETHAN50PERCENTBUSINESSUSE, 
	o_MoreThan50PercentBusinessUseAudit AS MORETHAN50PERCENTBUSINESSUSEAUDIT, 
	o_MoreThan50PercentBusinessUseEstimate AS MORETHAN50PERCENTBUSINESSUSEESTIMATE, 
	NUMBEROFEMPLOYEES, 
	NUMBEROFEMPLOYEESAUDIT, 
	NUMBEROFEMPLOYEESESTIMATE, 
	NUMBEROFINSTRUCTORS, 
	NUMBEROFINSTRUCTORSAUDIT, 
	NUMBEROFINSTRUCTORSESTIMATE, 
	NUMBEROFPARTNERS, 
	NUMBEROFPARTNERSAUDIT, 
	NUMBEROFPARTNERSESTIMATE, 
	NUMBEROFVOLUNTEERS, 
	NUMBEROFVOLUNTEERSAUDIT, 
	NUMBEROFVOLUNTEERSESTIMATE, 
	PARTNERSRATETERRITORY, 
	o_SocialServiceAgency AS SOCIALSERVICEAGENCY, 
	o_SocialServiceAgencyRisks AS SOCIALSERVICEAGENCYRISKS, 
	SOCIALSERVICEAGENCYRISKSARATE
	FROM EXP_Metadata
),