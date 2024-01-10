WITH
SQ_Shortcut_to_AgencyRelationshipDim AS (
	with ##AgencyRelationship as (
	select EDWAgencyAKId, EDWLegalPrimaryAgencyAKId, AgencyRelationshipEffectiveDate, AgencyRelationshipExpirationDate, 1  as LoadOrder from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyRelationshipDim where CurrentSnapshotFlag=1
	)
	-- Get all records from AgencyrelationshipDim
	select EDWAgencyAKId, EDWLegalPrimaryAgencyAKId, AgencyRelationshipEffectiveDate, AgencyRelationshipExpirationDate,LoadOrder  from 
	 ##AgencyRelationship
	union all
	-- Identify gaps in timeperiod between relationship and create records for those time period
	select EDWAgencyAKId,EDWAgencyAKId,AgencyRelationshipExpirationDate,New_AgencyRelationshipEffectiveDate,2 as LoadOrder 
	from (
	select EDWAgencyAKId,EDWLegalPrimaryAgencyAKId,AgencyRelationshipEffectiveDate,AgencyRelationshipExpirationDate,
	lead(AgencyRelationshipEffectiveDate) over(Partition By EDWAgencyAKId order by AgencyRelationshipEffectiveDate, AgencyRelationshipExpirationDate) New_AgencyRelationshipEffectiveDate 
	from ##AgencyRelationship
	where AgencyRelationshipEffectiveDate<=AgencyRelationshipExpirationDate
	) A
	where A.AgencyRelationshipExpirationDate<>A.New_AgencyRelationshipEffectiveDate
	
	union all
	-- identify initial start date of a relationshipd and create record from 1800-01-01
	select EDWAgencyAKId,EDWAgencyAKId,'1800-01-01',AgencyRelationshipEffectiveDate,3 as LoadOrder from (
	select EDWAgencyAKId,EDWLegalPrimaryAgencyAKId,AgencyRelationshipEffectiveDate,AgencyRelationshipExpirationDate,Lag(AgencyRelationshipEffectiveDate) over(Partition By EDWAgencyAKId order by AgencyRelationshipEffectiveDate, AgencyRelationshipExpirationDate) New_AgencyRelationshipEffectiveDate from ##AgencyRelationship
	where AgencyRelationshipEffectiveDate<=AgencyRelationshipExpirationDate
	) A
	where New_AgencyRelationshipEffectiveDate is Null
	and ((AgencyRelationshipEffectiveDate<>'1800-01-01' and AgencyRelationShipExpirationDate<>'1800-01-01')
	or AgencyRelationShipEffectiveDate<>'1800-01-01')
	
	union all
	-- identify current date and create records from last expiration date till current date
	select EDWAgencyAKId,EDWAgencyAKId,AgencyRelationshipExpirationDate,'2999-12-31',4 as LoadOrder from 
	(select EDWAgencyAKId,EDWLegalPrimaryAgencyAKId,AgencyRelationshipEffectiveDate,AgencyRelationshipExpirationDate,Lead(AgencyRelationshipEffectiveDate) over(Partition By EDWAgencyAKId order by AgencyRelationshipEffectiveDate, AgencyRelationshipExpirationDate) New_AgencyRelationshipEffectiveDate from ##AgencyRelationship
	where AgencyRelationshipEffectiveDate<=AgencyRelationshipExpirationDate
	) A
	where New_AgencyRelationshipEffectiveDate is null
	and AgencyRelationshipExpirationDate<Getdate()
	--and AgencyRelationshipExpirationDate <> '1800-01-01' 
	
	union all
	-- identify agencies missing a relationship currently and create records for missing agencies
	select distinct A.EDWAgencyAKId,A.EDWAgencyAKId,'1800-01-01','2999-12-31',5 as LoadOrder from v3.AgencyDim A
	left outer join AgencyRelationshipDim B
	on A.EDWAgencyAKId=B.EDWAgencyAKId
	where B.EDWAgencyAKId is null
),
EXP_Standalone AS (
	SELECT
	EDWAgencyAKId,
	EDWLegalPrimaryAgencyAKId,
	AgencyRelationshipEffectiveDate,
	AgencyRelationshipExpirationDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	sysdate AS o_CreateDate,
	sysdate AS o_ModifiedDate
	FROM SQ_Shortcut_to_AgencyRelationshipDim
),
AgencyRelationshipCurrent AS (
	TRUNCATE TABLE Shortcut_to_AgencyRelationshipCurrent;
	INSERT INTO Shortcut_to_AgencyRelationshipCurrent
	(AuditId, CreatedDate, ModifiedDate, EDWAgencyAKID, EDWLegalPrimaryAgencyAKId, AgencyRelationshipEffectiveDate, AgencyRelationshipExpirationDate)
	SELECT 
	o_AuditID AS AUDITID, 
	o_CreateDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	EDWAgencyAKId AS EDWAGENCYAKID, 
	EDWLEGALPRIMARYAGENCYAKID, 
	AGENCYRELATIONSHIPEFFECTIVEDATE, 
	AGENCYRELATIONSHIPEXPIRATIONDATE
	FROM EXP_Standalone
),