WITH
LKP_SupClassificationWorkersCompensation_CSFlag AS (
	SELECT
	SupClassificationWorkersCompensationId,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode,
	RatableClassIndicator,
	SubjectToExperienceModificationClassIndicator,
	ExperienceModificationClassIndicator,
	ScheduledModificationClassIndicator,
	SurchargeClassIndicator,
	OtherModificationClassIndicator,
	EffectiveDate,
	HazardGroupCode,
	MeritRatingClassIndicator
	FROM (
		SELECT 
			SupClassificationWorkersCompensationId,
			RatingStateCode,
			ClassCode,
			ClassDescription,
			OriginatingOrganizationCode,
			RatableClassIndicator,
			SubjectToExperienceModificationClassIndicator,
			ExperienceModificationClassIndicator,
			ScheduledModificationClassIndicator,
			SurchargeClassIndicator,
			OtherModificationClassIndicator,
			EffectiveDate,
			HazardGroupCode,
			MeritRatingClassIndicator
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationWorkersCompensation
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,ClassCode,ClassDescription,OriginatingOrganizationCode,RatableClassIndicator,SubjectToExperienceModificationClassIndicator,ExperienceModificationClassIndicator,ScheduledModificationClassIndicator,SurchargeClassIndicator,OtherModificationClassIndicator,EffectiveDate,HazardGroupCode,MeritRatingClassIndicator ORDER BY SupClassificationWorkersCompensationId) = 1
),
SQ_IR_SupClassificationWorkersCompensation AS (
	SELECT
		SupClassificationWorkersCompensationId,
		AuditId,
		CreatedDate,
		ModifiedDate,
		LineOfBusinessAbbreviation,
		RatingStateCode,
		EffectiveDate,
		ExpirationDate,
		ClassCode,
		ClassDescription,
		OriginatingOrganizationCode,
		RatableClassIndicator,
		SubjectToExperienceModificationClassIndicator,
		ExperienceModificationClassIndicator,
		ScheduledModificationClassIndicator,
		SurchargeClassIndicator,
		OtherModificationClassIndicator,
		HazardGroupCode,
		MeritRatingClassIndicator
	FROM SupClassificationWorkersCompensation
),
LKP_SupClassificationWorkersCompensation AS (
	SELECT
	SupClassificationWorkersCompensationId,
	CurrentSnapshotFlag,
	AuditId,
	EffectiveDate,
	ExpirationDate,
	SourceSystemId,
	CreatedDate,
	ModifiedDate,
	LineOfBusinessAbbreviation,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode,
	RatableClassIndicator,
	SubjectToExperienceModificationClassIndicator,
	ExperienceModificationClassIndicator,
	ScheduledModificationClassIndicator,
	SurchargeClassIndicator,
	OtherModificationClassIndicator,
	HazardGroupCode,
	MeritRatingClassIndicator
	FROM (
		SELECT 
			SupClassificationWorkersCompensationId,
			CurrentSnapshotFlag,
			AuditId,
			EffectiveDate,
			ExpirationDate,
			SourceSystemId,
			CreatedDate,
			ModifiedDate,
			LineOfBusinessAbbreviation,
			RatingStateCode,
			ClassCode,
			ClassDescription,
			OriginatingOrganizationCode,
			RatableClassIndicator,
			SubjectToExperienceModificationClassIndicator,
			ExperienceModificationClassIndicator,
			ScheduledModificationClassIndicator,
			SurchargeClassIndicator,
			OtherModificationClassIndicator,
			HazardGroupCode,
			MeritRatingClassIndicator
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationWorkersCompensation
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,OriginatingOrganizationCode,ClassCode ORDER BY SupClassificationWorkersCompensationId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	SQ_IR_SupClassificationWorkersCompensation.LineOfBusinessAbbreviation,
	SQ_IR_SupClassificationWorkersCompensation.RatingStateCode,
	SQ_IR_SupClassificationWorkersCompensation.EffectiveDate,
	SQ_IR_SupClassificationWorkersCompensation.ExpirationDate,
	SQ_IR_SupClassificationWorkersCompensation.ClassCode,
	SQ_IR_SupClassificationWorkersCompensation.ClassDescription,
	SQ_IR_SupClassificationWorkersCompensation.OriginatingOrganizationCode,
	SQ_IR_SupClassificationWorkersCompensation.RatableClassIndicator,
	SQ_IR_SupClassificationWorkersCompensation.SubjectToExperienceModificationClassIndicator,
	SQ_IR_SupClassificationWorkersCompensation.ExperienceModificationClassIndicator,
	SQ_IR_SupClassificationWorkersCompensation.ScheduledModificationClassIndicator,
	SQ_IR_SupClassificationWorkersCompensation.SurchargeClassIndicator,
	SQ_IR_SupClassificationWorkersCompensation.OtherModificationClassIndicator,
	SQ_IR_SupClassificationWorkersCompensation.HazardGroupCode,
	SQ_IR_SupClassificationWorkersCompensation.MeritRatingClassIndicator,
	LKP_SupClassificationWorkersCompensation.SupClassificationWorkersCompensationId AS lkp_SupClassificationWorkersCompensationId,
	LKP_SupClassificationWorkersCompensation.EffectiveDate AS lkp_EffectiveDate,
	LKP_SupClassificationWorkersCompensation.ExpirationDate AS lkp_ExpirationDate,
	LKP_SupClassificationWorkersCompensation.LineOfBusinessAbbreviation AS lkp_LineOfBusinessAbbreviation,
	LKP_SupClassificationWorkersCompensation.RatingStateCode AS lkp_RatingStateCode,
	LKP_SupClassificationWorkersCompensation.ClassCode AS lkp_ClassCode,
	LKP_SupClassificationWorkersCompensation.ClassDescription AS lkp_ClassDescription,
	LKP_SupClassificationWorkersCompensation.OriginatingOrganizationCode AS lkp_OriginatingOrganizationCode,
	LKP_SupClassificationWorkersCompensation.RatableClassIndicator AS lkp_RatableClassIndicator,
	LKP_SupClassificationWorkersCompensation.SubjectToExperienceModificationClassIndicator AS lkp_SubjectToExperienceModificationClassIndicator,
	LKP_SupClassificationWorkersCompensation.ExperienceModificationClassIndicator AS lkp_ExperienceModificationClassIndicator,
	LKP_SupClassificationWorkersCompensation.ScheduledModificationClassIndicator AS lkp_ScheduledModificationClassIndicator,
	LKP_SupClassificationWorkersCompensation.SurchargeClassIndicator AS lkp_SurchargeClassIndicator,
	LKP_SupClassificationWorkersCompensation.OtherModificationClassIndicator AS lkp_OtherModificationClassIndicator,
	LKP_SupClassificationWorkersCompensation.HazardGroupCode AS lkp_HazardGroupCode,
	LKP_SupClassificationWorkersCompensation.MeritRatingClassIndicator AS lkp_MeritRatingClassIndicator,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(:LKP.LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_CSFLAG(RatingStateCode
	-- ,ClassCode
	-- ,ClassDescription
	-- ,OriginatingOrganizationCode
	-- ,RatableClassIndicator
	-- ,SubjectToExperienceModificationClassIndicator
	-- ,ExperienceModificationClassIndicator
	-- ,ScheduledModificationClassIndicator
	-- ,SurchargeClassIndicator
	-- ,OtherModificationClassIndicator
	-- ,EffectiveDate
	-- ,HazardGroupCode
	-- ,MeritRatingClassIndicator)),
	-- 'NOCHANGE',
	-- 'INSERT')						
	DECODE(TRUE,
		NOT LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_CSFLAG_RatingStateCode_ClassCode_ClassDescription_OriginatingOrganizationCode_RatableClassIndicator_SubjectToExperienceModificationClassIndicator_ExperienceModificationClassIndicator_ScheduledModificationClassIndicator_SurchargeClassIndicator_OtherModificationClassIndicator_EffectiveDate_HazardGroupCode_MeritRatingClassIndicator.SupClassificationWorkersCompensationId IS NULL, 'NOCHANGE',
		'INSERT') AS v_RecordPopulated,
	-- *INF*: DECODE(TRUE,
	-- ExpirationDate   <=  lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
	-- ISNULL(lkp_SupClassificationWorkersCompensationId) 
	-- OR (  RatingStateCode = lkp_RatingStateCode 	
	-- AND  ClassCode = lkp_ClassCode 
	-- AND  OriginatingOrganizationCode = lkp_OriginatingOrganizationCode 
	-- 				 AND  (ClassDescription <>lkp_ClassDescription 
	-- 								  OR EffectiveDate <> lkp_EffectiveDate
	--                                                      OR ExpirationDate <> lkp_ExpirationDate
	-- 								  OR RatableClassIndicator <> lkp_RatableClassIndicator 
	--                                                      OR  SubjectToExperienceModificationClassIndicator  <> lkp_SubjectToExperienceModificationClassIndicator
	--                                                      OR  ExperienceModificationClassIndicator <>  lkp_ExperienceModificationClassIndicator
	--                                                      OR  ScheduledModificationClassIndicator  <>  lkp_ScheduledModificationClassIndicator
	--                                                      OR  SurchargeClassIndicator <>  lkp_SurchargeClassIndicator
	--                                                      OR  OtherModificationClassIndicator <> lkp_OtherModificationClassIndicator
	--                                                      OR  HazardGroupCode <> lkp_HazardGroupCode
	--                                                      OR MeritRatingClassIndicator <> lkp_MeritRatingClassIndicator
	-- 		  )	   		
	-- ),'INSERT',
	-- RatingStateCode <>lkp_RatingStateCode OR
	-- ClassCode <>lkp_ClassCode  OR
	-- OriginatingOrganizationCode <> lkp_OriginatingOrganizationCode,
	-- 'UPDATE',
	-- 'NOCHANGE'
	-- )
	DECODE(TRUE,
		ExpirationDate <= lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
		lkp_SupClassificationWorkersCompensationId IS NULL OR ( RatingStateCode = lkp_RatingStateCode AND ClassCode = lkp_ClassCode AND OriginatingOrganizationCode = lkp_OriginatingOrganizationCode AND ( ClassDescription <> lkp_ClassDescription OR EffectiveDate <> lkp_EffectiveDate OR ExpirationDate <> lkp_ExpirationDate OR RatableClassIndicator <> lkp_RatableClassIndicator OR SubjectToExperienceModificationClassIndicator <> lkp_SubjectToExperienceModificationClassIndicator OR ExperienceModificationClassIndicator <> lkp_ExperienceModificationClassIndicator OR ScheduledModificationClassIndicator <> lkp_ScheduledModificationClassIndicator OR SurchargeClassIndicator <> lkp_SurchargeClassIndicator OR OtherModificationClassIndicator <> lkp_OtherModificationClassIndicator OR HazardGroupCode <> lkp_HazardGroupCode OR MeritRatingClassIndicator <> lkp_MeritRatingClassIndicator ) ), 'INSERT',
		RatingStateCode <> lkp_RatingStateCode OR ClassCode <> lkp_ClassCode OR OriginatingOrganizationCode <> lkp_OriginatingOrganizationCode, 'UPDATE',
		'NOCHANGE') AS v_ChangeFlag,
	-- *INF*: @{pipeline().parameters.SOURCE_SYSTEM_ID}
	-- --'N/A'
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	1 AS o_CurrentSnapshotFlag,
	v_ChangeFlag AS o_ChangeFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate,
	LineOfBusinessAbbreviation AS o_LineOfBusinessAbbreviation,
	RatingStateCode AS o_RatingStateCode,
	EffectiveDate AS o_ClassEffectiveDate,
	ExpirationDate AS o_ClassExpirationDate,
	ClassCode AS o_ClassCode,
	ClassDescription AS o_ClassDescription,
	OriginatingOrganizationCode AS o_ClassCodeOriginatingOrganization,
	RatableClassIndicator AS o_RatableClassIndicator,
	SubjectToExperienceModificationClassIndicator AS o_SubjectToExperienceModificationClassIndicator,
	ExperienceModificationClassIndicator AS o_ExperienceModificationClassIndicator,
	ScheduledModificationClassIndicator AS o_ScheduledModificationClassIndicator,
	SurchargeClassIndicator AS o_SurchargeClassIndicator,
	OtherModificationClassIndicator AS o_OtherModificationClassIndicator,
	HazardGroupCode AS o_HazardGroupCode
	FROM SQ_IR_SupClassificationWorkersCompensation
	LEFT JOIN LKP_SupClassificationWorkersCompensation
	ON LKP_SupClassificationWorkersCompensation.RatingStateCode = SQ_IR_SupClassificationWorkersCompensation.RatingStateCode AND LKP_SupClassificationWorkersCompensation.OriginatingOrganizationCode = SQ_IR_SupClassificationWorkersCompensation.OriginatingOrganizationCode AND LKP_SupClassificationWorkersCompensation.ClassCode = SQ_IR_SupClassificationWorkersCompensation.ClassCode
	LEFT JOIN LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_CSFLAG LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_CSFLAG_RatingStateCode_ClassCode_ClassDescription_OriginatingOrganizationCode_RatableClassIndicator_SubjectToExperienceModificationClassIndicator_ExperienceModificationClassIndicator_ScheduledModificationClassIndicator_SurchargeClassIndicator_OtherModificationClassIndicator_EffectiveDate_HazardGroupCode_MeritRatingClassIndicator
	ON LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_CSFLAG_RatingStateCode_ClassCode_ClassDescription_OriginatingOrganizationCode_RatableClassIndicator_SubjectToExperienceModificationClassIndicator_ExperienceModificationClassIndicator_ScheduledModificationClassIndicator_SurchargeClassIndicator_OtherModificationClassIndicator_EffectiveDate_HazardGroupCode_MeritRatingClassIndicator.RatingStateCode = RatingStateCode
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_CSFLAG_RatingStateCode_ClassCode_ClassDescription_OriginatingOrganizationCode_RatableClassIndicator_SubjectToExperienceModificationClassIndicator_ExperienceModificationClassIndicator_ScheduledModificationClassIndicator_SurchargeClassIndicator_OtherModificationClassIndicator_EffectiveDate_HazardGroupCode_MeritRatingClassIndicator.ClassCode = ClassCode
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_CSFLAG_RatingStateCode_ClassCode_ClassDescription_OriginatingOrganizationCode_RatableClassIndicator_SubjectToExperienceModificationClassIndicator_ExperienceModificationClassIndicator_ScheduledModificationClassIndicator_SurchargeClassIndicator_OtherModificationClassIndicator_EffectiveDate_HazardGroupCode_MeritRatingClassIndicator.ClassDescription = ClassDescription
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_CSFLAG_RatingStateCode_ClassCode_ClassDescription_OriginatingOrganizationCode_RatableClassIndicator_SubjectToExperienceModificationClassIndicator_ExperienceModificationClassIndicator_ScheduledModificationClassIndicator_SurchargeClassIndicator_OtherModificationClassIndicator_EffectiveDate_HazardGroupCode_MeritRatingClassIndicator.OriginatingOrganizationCode = OriginatingOrganizationCode
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_CSFLAG_RatingStateCode_ClassCode_ClassDescription_OriginatingOrganizationCode_RatableClassIndicator_SubjectToExperienceModificationClassIndicator_ExperienceModificationClassIndicator_ScheduledModificationClassIndicator_SurchargeClassIndicator_OtherModificationClassIndicator_EffectiveDate_HazardGroupCode_MeritRatingClassIndicator.RatableClassIndicator = RatableClassIndicator
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_CSFLAG_RatingStateCode_ClassCode_ClassDescription_OriginatingOrganizationCode_RatableClassIndicator_SubjectToExperienceModificationClassIndicator_ExperienceModificationClassIndicator_ScheduledModificationClassIndicator_SurchargeClassIndicator_OtherModificationClassIndicator_EffectiveDate_HazardGroupCode_MeritRatingClassIndicator.SubjectToExperienceModificationClassIndicator = SubjectToExperienceModificationClassIndicator
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_CSFLAG_RatingStateCode_ClassCode_ClassDescription_OriginatingOrganizationCode_RatableClassIndicator_SubjectToExperienceModificationClassIndicator_ExperienceModificationClassIndicator_ScheduledModificationClassIndicator_SurchargeClassIndicator_OtherModificationClassIndicator_EffectiveDate_HazardGroupCode_MeritRatingClassIndicator.ExperienceModificationClassIndicator = ExperienceModificationClassIndicator
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_CSFLAG_RatingStateCode_ClassCode_ClassDescription_OriginatingOrganizationCode_RatableClassIndicator_SubjectToExperienceModificationClassIndicator_ExperienceModificationClassIndicator_ScheduledModificationClassIndicator_SurchargeClassIndicator_OtherModificationClassIndicator_EffectiveDate_HazardGroupCode_MeritRatingClassIndicator.ScheduledModificationClassIndicator = ScheduledModificationClassIndicator
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_CSFLAG_RatingStateCode_ClassCode_ClassDescription_OriginatingOrganizationCode_RatableClassIndicator_SubjectToExperienceModificationClassIndicator_ExperienceModificationClassIndicator_ScheduledModificationClassIndicator_SurchargeClassIndicator_OtherModificationClassIndicator_EffectiveDate_HazardGroupCode_MeritRatingClassIndicator.SurchargeClassIndicator = SurchargeClassIndicator
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_CSFLAG_RatingStateCode_ClassCode_ClassDescription_OriginatingOrganizationCode_RatableClassIndicator_SubjectToExperienceModificationClassIndicator_ExperienceModificationClassIndicator_ScheduledModificationClassIndicator_SurchargeClassIndicator_OtherModificationClassIndicator_EffectiveDate_HazardGroupCode_MeritRatingClassIndicator.OtherModificationClassIndicator = OtherModificationClassIndicator
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_CSFLAG_RatingStateCode_ClassCode_ClassDescription_OriginatingOrganizationCode_RatableClassIndicator_SubjectToExperienceModificationClassIndicator_ExperienceModificationClassIndicator_ScheduledModificationClassIndicator_SurchargeClassIndicator_OtherModificationClassIndicator_EffectiveDate_HazardGroupCode_MeritRatingClassIndicator.EffectiveDate = EffectiveDate
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_CSFLAG_RatingStateCode_ClassCode_ClassDescription_OriginatingOrganizationCode_RatableClassIndicator_SubjectToExperienceModificationClassIndicator_ExperienceModificationClassIndicator_ScheduledModificationClassIndicator_SurchargeClassIndicator_OtherModificationClassIndicator_EffectiveDate_HazardGroupCode_MeritRatingClassIndicator.HazardGroupCode = HazardGroupCode
	AND LKP_SUPCLASSIFICATIONWORKERSCOMPENSATION_CSFLAG_RatingStateCode_ClassCode_ClassDescription_OriginatingOrganizationCode_RatableClassIndicator_SubjectToExperienceModificationClassIndicator_ExperienceModificationClassIndicator_ScheduledModificationClassIndicator_SurchargeClassIndicator_OtherModificationClassIndicator_EffectiveDate_HazardGroupCode_MeritRatingClassIndicator.MeritRatingClassIndicator = MeritRatingClassIndicator

),
RTR_Insert_Update AS (
	SELECT
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_SourceSystemId AS SourceSystemId,
	o_ChangeFlag AS ChangeFlag,
	o_AuditId AS AuditId,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_LineOfBusinessAbbreviation AS LineOfBusinessAbbreviation,
	o_RatingStateCode AS RatingStateCode,
	o_ClassEffectiveDate AS ClassEffectiveDate,
	o_ClassExpirationDate AS ClassExpirationDate,
	o_ClassCode AS ClassCode,
	o_ClassDescription AS ClassDescription,
	o_ClassCodeOriginatingOrganization AS ClassCodeOriginatingOrganization,
	o_RatableClassIndicator AS RatableClassIndicator,
	o_SubjectToExperienceModificationClassIndicator AS SubjectToExperienceModificationClassIndicator,
	o_ExperienceModificationClassIndicator AS ExperienceModificationClassIndicator,
	o_ScheduledModificationClassIndicator AS ScheduledModificationClassIndicator,
	o_SurchargeClassIndicator AS SurchargeClassIndicator,
	o_OtherModificationClassIndicator AS OtherModificationClassIndicator,
	o_HazardGroupCode AS HazardGroupCode,
	MeritRatingClassIndicator
	FROM EXP_Detect_Changes
),
RTR_Insert_Update_INSERT_OR_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='INSERT'
OR ChangeFlag='UPDATE'),
SupClassificationWorkersCompensation_IL AS (
	INSERT INTO SupClassificationWorkersCompensation
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, LineOfBusinessAbbreviation, RatingStateCode, ClassCode, ClassDescription, OriginatingOrganizationCode, RatableClassIndicator, SubjectToExperienceModificationClassIndicator, ExperienceModificationClassIndicator, ScheduledModificationClassIndicator, SurchargeClassIndicator, OtherModificationClassIndicator, HazardGroupCode, MeritRatingClassIndicator)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	ClassEffectiveDate AS EFFECTIVEDATE, 
	ClassExpirationDate AS EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	LINEOFBUSINESSABBREVIATION, 
	RATINGSTATECODE, 
	CLASSCODE, 
	CLASSDESCRIPTION, 
	ClassCodeOriginatingOrganization AS ORIGINATINGORGANIZATIONCODE, 
	RATABLECLASSINDICATOR, 
	SUBJECTTOEXPERIENCEMODIFICATIONCLASSINDICATOR, 
	EXPERIENCEMODIFICATIONCLASSINDICATOR, 
	SCHEDULEDMODIFICATIONCLASSINDICATOR, 
	SURCHARGECLASSINDICATOR, 
	OTHERMODIFICATIONCLASSINDICATOR, 
	HAZARDGROUPCODE, 
	MERITRATINGCLASSINDICATOR
	FROM RTR_Insert_Update_INSERT_OR_UPDATE
),
SQ_SupClassificationWorkersCompensation AS (
	SELECT SupClassificationWorkersCompensation.SupClassificationWorkersCompensationId
	, SupClassificationWorkersCompensation.EffectiveDate
	, SupClassificationWorkersCompensation.ExpirationDate
	, SupClassificationWorkersCompensation.RatingStateCode
	, SupClassificationWorkersCompensation.ClassCode
	, SupClassificationWorkersCompensation.ClassDescription
	, SupClassificationWorkersCompensation.OriginatingOrganizationCode
	, SupClassificationWorkersCompensation.RatableClassIndicator
	, SupClassificationWorkersCompensation.SubjectToExperienceModificationClassIndicator
	, SupClassificationWorkersCompensation.ExperienceModificationClassIndicator
	, SupClassificationWorkersCompensation.ScheduledModificationClassIndicator 
	, SupClassificationWorkersCompensation.SurchargeClassIndicator 
	, SupClassificationWorkersCompensation.OtherModificationClassIndicator 
	, SupClassificationWorkersCompensation.HazardGroupCode 
	,SupClassificationWorkersCompensation.MeritRatingClassIndicator
	FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationWorkersCompensation
	where CurrentSnapshotFlag = 1 
	ORDER BY SupClassificationWorkersCompensation.ClassCode  ,
	SupClassificationWorkersCompensation.RatingStateCode, 
	SupClassificationWorkersCompensation.OriginatingOrganizationCode, 
	SupClassificationWorkersCompensation.EffectiveDate DESC,
	SupClassificationWorkersCompensation.CreatedDate DESC
),
EXP_Lag_Eff_dates AS (
	SELECT
	SupClassificationWorkersCompensationId,
	EffectiveDate,
	ExpirationDate,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode,
	RatableClassIndicator,
	SubjectToExperienceModificationClassIndicator,
	ExperienceModificationClassIndicator,
	ScheduledModificationClassIndicator,
	SurchargeClassIndicator,
	OtherModificationClassIndicator,
	HazardGroupCode,
	MeritRatingClassIndicator,
	-- *INF*: DECODE(TRUE,RatingStateCode = v_PREV_ROW_RatingStateCode
	-- 		 AND ClassCode = v_PREV_ROW_ClassCode
	--              AND OriginatingOrganizationCode = v_PREV_ROW_OriginatingOrganizationCode
	-- 		AND (	ClassDescription <> v_PREV_ROW_ClassDescription
	-- 			--OR 	EffectiveDate <>  v_PREV_ROW_EffectiveDate   
	-- 			OR  ADD_TO_DATE(ExpirationDate,'SS',+1) <>v_PREV_ROW_EffectiveDate   
	-- 	             OR RatableClassIndicator <> v_PREV_ROW_RatableClassIndicator
	--                    OR SubjectToExperienceModificationClassIndicator <> v_PREV_ROW_SubjectToExperienceModificationClassIndicator
	--                    OR ExperienceModificationClassIndicator <> v_PREV_ROW_ExperienceModificationClassIndicator
	--                    OR ScheduledModificationClassIndicator <> v_PREV_ROW_ScheduledModificationClassIndicator
	--                    OR SurchargeClassIndicator <> v_PREV_ROW_SurchargeClassIndicator
	--                    OR OtherModificationClassIndicator <> v_PREV_ROW_OtherModificationClassIndicator
	--                    OR HazardGroupCode <> v_PREV_ROW_HazardGroupCode
	-- 		      OR MeritRatingClassIndicator != v_PREV_ROW_MeritRatingClassIndicator
	-- 	             )
	-- 		,'0','1')
	DECODE(TRUE,
		RatingStateCode = v_PREV_ROW_RatingStateCode AND ClassCode = v_PREV_ROW_ClassCode AND OriginatingOrganizationCode = v_PREV_ROW_OriginatingOrganizationCode AND ( ClassDescription <> v_PREV_ROW_ClassDescription OR ADD_TO_DATE(ExpirationDate, 'SS', + 1) <> v_PREV_ROW_EffectiveDate OR RatableClassIndicator <> v_PREV_ROW_RatableClassIndicator OR SubjectToExperienceModificationClassIndicator <> v_PREV_ROW_SubjectToExperienceModificationClassIndicator OR ExperienceModificationClassIndicator <> v_PREV_ROW_ExperienceModificationClassIndicator OR ScheduledModificationClassIndicator <> v_PREV_ROW_ScheduledModificationClassIndicator OR SurchargeClassIndicator <> v_PREV_ROW_SurchargeClassIndicator OR OtherModificationClassIndicator <> v_PREV_ROW_OtherModificationClassIndicator OR HazardGroupCode <> v_PREV_ROW_HazardGroupCode OR MeritRatingClassIndicator != v_PREV_ROW_MeritRatingClassIndicator ), '0',
		'1') AS v_CurrentSnapshotFlag,
	-- *INF*: --ADD_TO_DATE( 
	-- ADD_TO_DATE(   --v_PREV_ROW_EffectiveDate
	-- 	IIF(v_PREV_ROW_EffectiveDate =  TO_DATE('1800-01-01 00:00:00' , 'YYYY-MM-DD HH24:MI:SS' ) , sysdate ,v_PREV_ROW_EffectiveDate )
	-- 
	-- ,'SS',-1)
	-- -- , 'HH' , -1)
	ADD_TO_DATE(IFF(v_PREV_ROW_EffectiveDate = TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), sysdate, v_PREV_ROW_EffectiveDate), 'SS', - 1) AS v_ClassExpirationDate,
	v_CurrentSnapshotFlag AS o_CurrentSnapshotFlag,
	v_ClassExpirationDate AS ClassExpirationDate,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	ExpirationDate AS v_PREV_ROW_ExpirationDate,
	RatingStateCode AS v_PREV_ROW_RatingStateCode,
	ClassCode AS v_PREV_ROW_ClassCode,
	ClassDescription AS v_PREV_ROW_ClassDescription,
	OriginatingOrganizationCode AS v_PREV_ROW_OriginatingOrganizationCode,
	RatableClassIndicator AS v_PREV_ROW_RatableClassIndicator,
	SubjectToExperienceModificationClassIndicator AS v_PREV_ROW_SubjectToExperienceModificationClassIndicator,
	ExperienceModificationClassIndicator AS v_PREV_ROW_ExperienceModificationClassIndicator,
	ScheduledModificationClassIndicator AS v_PREV_ROW_ScheduledModificationClassIndicator,
	SurchargeClassIndicator AS v_PREV_ROW_SurchargeClassIndicator,
	OtherModificationClassIndicator AS v_PREV_ROW_OtherModificationClassIndicator,
	HazardGroupCode AS v_PREV_ROW_HazardGroupCode,
	MeritRatingClassIndicator AS v_PREV_ROW_MeritRatingClassIndicator,
	sysdate AS ModifiedDate
	FROM SQ_SupClassificationWorkersCompensation
),
FIL_FirstRowInAKGroup AS (
	SELECT
	SupClassificationWorkersCompensationId AS SupClassificationCommercialAutoID, 
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
	ClassExpirationDate, 
	ModifiedDate
	FROM EXP_Lag_Eff_dates
	WHERE CurrentSnapshotFlag = '0'
),
UPD_SupISOGLClassGroup AS (
	SELECT
	SupClassificationCommercialAutoID, 
	CurrentSnapshotFlag, 
	ClassExpirationDate, 
	ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
SupClassificationWorkersCompensation_CheckExpDate AS (
	MERGE INTO SupClassificationWorkersCompensation AS T
	USING UPD_SupISOGLClassGroup AS S
	ON T.SupClassificationWorkersCompensationId = S.SupClassificationCommercialAutoID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ClassExpirationDate, T.ModifiedDate = S.ModifiedDate
),