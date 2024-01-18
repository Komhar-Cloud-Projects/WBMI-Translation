WITH
SQ_WorkCompClass AS (

-- TODO Manual --

),
EXP_Detect_Changes AS (
	SELECT
	Line_of_Business_Abbreviation AS LineOfBusinessAbbreviation,
	Rating_State_Code AS RatingStateCode,
	Class_Effective_Date AS ClassEffectiveDate,
	Class_Expiration_Date AS ClassExpirationDate,
	Class_Code AS ClassCode,
	Class_Description AS ClassDescription,
	Class_Code_Originating_Organization AS ClassCodeOriginatingOrganization,
	Ratable_Class_Indicator AS RatableClassIndicator,
	Subject_To_Experience_Modification_Class_Indicator AS SubjectToExperienceModificationClassIndicator,
	Experience_Modification_Class_Indicator AS ExperienceModificationClassIndicator,
	Scheduled_Modification_Class_Indicator AS ScheduledModificationClassIndicator,
	Surcharge_Class_Indicator AS SurchargeClassIndicator,
	Other_Modification_Class_Indicator AS OtherModificationClassIndicator,
	Hazard_Group_Code AS HazardGroupCode,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate,
	LineOfBusinessAbbreviation AS i_LineOfBusinessAbbreviation,
	RatingStateCode AS i_RatingStateCode,
	-- *INF*: TO_DATE(ClassEffectiveDate,'YYYY-MM-DD HH24:MI:SS.MS')
	TO_TIMESTAMP(ClassEffectiveDate, 'YYYY-MM-DD HH24:MI:SS.MS') AS i_ClassEffectiveDate,
	-- *INF*: TO_DATE(ClassExpirationDate,'YYYY-MM-DD HH24:MI:SS.MS')
	TO_TIMESTAMP(ClassExpirationDate, 'YYYY-MM-DD HH24:MI:SS.MS') AS i_ClassExpirationDate,
	ClassCode AS i_ClassCode,
	ClassDescription AS i_ClassDescription,
	ClassCodeOriginatingOrganization AS i_ClassCodeOriginatingOrganization,
	RatableClassIndicator AS i_RatableClassIndicator,
	SubjectToExperienceModificationClassIndicator AS i_SubjectToExperienceModificationClassIndicator,
	ExperienceModificationClassIndicator AS i_ExperienceModificationClassIndicator,
	ScheduledModificationClassIndicator AS i_ScheduledModificationClassIndicator,
	SurchargeClassIndicator AS i_SurchargeClassIndicator,
	OtherModificationClassIndicator AS i_OtherModificationClassIndicator,
	HazardGroupCode AS i_HazardGroupCode,
	Merit_Rating_Class_Indicator
	FROM SQ_WorkCompClass
),
SupClassificationWorkersCompensation_IR AS (
	TRUNCATE TABLE SupClassificationWorkersCompensation;
	INSERT INTO SupClassificationWorkersCompensation
	(AuditId, CreatedDate, ModifiedDate, LineOfBusinessAbbreviation, RatingStateCode, EffectiveDate, ExpirationDate, ClassCode, ClassDescription, OriginatingOrganizationCode, RatableClassIndicator, SubjectToExperienceModificationClassIndicator, ExperienceModificationClassIndicator, ScheduledModificationClassIndicator, SurchargeClassIndicator, OtherModificationClassIndicator, HazardGroupCode, MeritRatingClassIndicator)
	SELECT 
	o_AuditId AS AUDITID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	i_LineOfBusinessAbbreviation AS LINEOFBUSINESSABBREVIATION, 
	i_RatingStateCode AS RATINGSTATECODE, 
	i_ClassEffectiveDate AS EFFECTIVEDATE, 
	i_ClassExpirationDate AS EXPIRATIONDATE, 
	i_ClassCode AS CLASSCODE, 
	i_ClassDescription AS CLASSDESCRIPTION, 
	i_ClassCodeOriginatingOrganization AS ORIGINATINGORGANIZATIONCODE, 
	i_RatableClassIndicator AS RATABLECLASSINDICATOR, 
	i_SubjectToExperienceModificationClassIndicator AS SUBJECTTOEXPERIENCEMODIFICATIONCLASSINDICATOR, 
	i_ExperienceModificationClassIndicator AS EXPERIENCEMODIFICATIONCLASSINDICATOR, 
	i_ScheduledModificationClassIndicator AS SCHEDULEDMODIFICATIONCLASSINDICATOR, 
	i_SurchargeClassIndicator AS SURCHARGECLASSINDICATOR, 
	i_OtherModificationClassIndicator AS OTHERMODIFICATIONCLASSINDICATOR, 
	i_HazardGroupCode AS HAZARDGROUPCODE, 
	Merit_Rating_Class_Indicator AS MERITRATINGCLASSINDICATOR
	FROM EXP_Detect_Changes
),