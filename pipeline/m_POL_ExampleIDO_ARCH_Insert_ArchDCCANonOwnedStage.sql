WITH
SQ_DCCANonOwnedStage AS (
	SELECT
		DCCANonOwnedStageId,
		ExtractDate,
		SourceSystemId,
		LineId,
		CA_NonOwnedId,
		SessionId,
		Id,
		Auditable,
		ExtendedEmployeeCov,
		ExtendedVolunteerCov,
		GarageServiceOperations,
		MoreThan50PercentBusinessUse,
		MoreThan50PercentBusinessUseAudit,
		MoreThan50PercentBusinessUseEstimate,
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
		SocialServiceAgency,
		SocialServiceAgencyRisks,
		SocialServiceAgencyRisksARate
	FROM DCCANonOwnedStage
),
EXP_Metadata AS (
	SELECT
	DCCANonOwnedStageId,
	ExtractDate,
	SourceSystemId,
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
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCANonOwnedStage
),
ArchDCCANonOwnedStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCANonOwnedStage
	(ExtractDate, SourceSystemId, AuditId, DCCANonOwnedStageId, LineId, CA_NonOwnedId, SessionId, Id, Auditable, ExtendedEmployeeCov, ExtendedVolunteerCov, GarageServiceOperations, MoreThan50PercentBusinessUse, MoreThan50PercentBusinessUseAudit, MoreThan50PercentBusinessUseEstimate, NumberOfEmployees, NumberOfEmployeesAudit, NumberOfEmployeesEstimate, NumberOfInstructors, NumberOfInstructorsAudit, NumberOfInstructorsEstimate, NumberOfPartners, NumberOfPartnersAudit, NumberOfPartnersEstimate, NumberOfVolunteers, NumberOfVolunteersAudit, NumberOfVolunteersEstimate, PartnersRateTerritory, SocialServiceAgency, SocialServiceAgencyRisks, SocialServiceAgencyRisksARate)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCCANONOWNEDSTAGEID, 
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