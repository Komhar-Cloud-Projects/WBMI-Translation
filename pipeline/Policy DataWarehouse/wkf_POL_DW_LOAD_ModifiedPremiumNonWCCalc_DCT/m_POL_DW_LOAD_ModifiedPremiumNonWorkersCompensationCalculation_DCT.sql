WITH
SQ_PremiumMasterCalculation AS (
	select
	PMC.PremiumMasterCalculationID,
	PMC.PremiumMasterRunDate,
	PMC.PremiumMasterPremium,
	PTRM.OtherModifiedFactor,
	PTRM.ScheduleModifiedFactor,
	PTRM.ExperienceModifiedFactor,
	PTRM.TransitionFactor
	from
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation PMC inner Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransactionRatingModifier PTRM
	on PMC.PremiumTransactionAKID = PTRM.PremiumTransactionAKID
	AND PMC.CurrentSnapshotFlag = 1
	AND PMC.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	AND PMC.PremiumMasterPremiumType = 'D'
	AND PMC.PremiumMasterPremium <> 0
	AND PMC.PremiumMasterReasonAmendedCode NOT IN ('CWO', 'Claw Back')
	AND convert(VARCHAR(6), PMC.PremiumMasterRunDate, 112) = CONVERT(VARCHAR(6), cast('@{pipeline().parameters.SELECTION_START_TS}' AS DATE), 112)
	inner Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC 
	on PMC.RatingCoverageAKID = RC.RatingCoverageAKID and PMC.RatingCoverageEffectiveDate = RC.EffectiveDate 
	inner Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product PRD 
	on RC.ProductAKId = PRD.ProductAKId AND PRD.CurrentSnapshotFlag = 1 AND PRD.ProductAbbreviation <> 'WC'
),
EXP_Extract_IL AS (
	SELECT
	PremiumMasterCalculationID,
	PremiumMasterRunDate,
	PremiumMasterPremium,
	OtherModifiedFactor,
	ScheduleModifiedFactor,
	ExperienceModifiedFactor,
	TransitionFactor
	FROM SQ_PremiumMasterCalculation
),
mplt_Compute_DCT_NonWC_ModifiedPremium AS (WITH
	INPUT AS (
		
	),
	EXP_Compute_ModifierAmounts AS (
		SELECT
		i_PremiumMasterPremium,
		i_OtherModifiedFactor,
		i_ScheduleModifiedFactor,
		i_ExperienceModifiedFactor,
		i_TransitionFactor,
		i_DirectWrittenPremium AS DirectWrittenPremium,
		-- *INF*: TO_DECIMAL((DirectWrittenPremium/i_TransitionFactor),8)
		TO_DECIMAL(( DirectWrittenPremium / i_TransitionFactor ), 8) AS v_OtherModifiedPremium_div,
		-- *INF*: TO_DECIMAL((v_OtherModifiedPremium_div/ i_OtherModifiedFactor),8)
		TO_DECIMAL(( v_OtherModifiedPremium_div / i_OtherModifiedFactor ), 8) AS v_ScheduleModifiedPremium_div,
		-- *INF*: TO_DECIMAL((v_ScheduleModifiedPremium_div/i_ScheduleModifiedFactor),8)
		TO_DECIMAL(( v_ScheduleModifiedPremium_div / i_ScheduleModifiedFactor ), 8) AS v_ExperienceModifiedPremium_div,
		-- *INF*: TO_DECIMAL((v_ExperienceModifiedPremium_div/i_ExperienceModifiedFactor),8)
		TO_DECIMAL(( v_ExperienceModifiedPremium_div / i_ExperienceModifiedFactor ), 8) AS v_SubjectDirectWrittenPremium_div,
		-- *INF*: ROUND(v_OtherModifiedPremium_div,4)
		ROUND(v_OtherModifiedPremium_div, 4) AS v_OtherModifiedPremium,
		-- *INF*: ROUND(v_ScheduleModifiedPremium_div,4)
		ROUND(v_ScheduleModifiedPremium_div, 4) AS v_ScheduleModifiedPremium,
		-- *INF*: ROUND(v_ExperienceModifiedPremium_div,4)
		ROUND(v_ExperienceModifiedPremium_div, 4) AS v_ExperienceModifiedPremium,
		-- *INF*: ROUND(v_SubjectDirectWrittenPremium_div,4)
		ROUND(v_SubjectDirectWrittenPremium_div, 4) AS v_SubjectDirectWrittenPremium,
		v_OtherModifiedPremium AS o_OtherModifiedPremium,
		v_ScheduleModifiedPremium AS o_ScheduleModifiedPremium,
		v_ExperienceModifiedPremium AS o_ExperienceModifiedPremium,
		v_SubjectDirectWrittenPremium AS o_SubjectDirectWrittenPremium
		FROM INPUT
	),
	OUTPUT AS (
		SELECT
		DirectWrittenPremium AS o_DirectWrittenPremium, 
		o_OtherModifiedPremium, 
		o_ScheduleModifiedPremium, 
		o_ExperienceModifiedPremium, 
		o_SubjectDirectWrittenPremium
		FROM EXP_Compute_ModifierAmounts
	),
),
EXP_AuditColumns AS (
	SELECT
	EXP_Extract_IL.PremiumMasterCalculationID,
	EXP_Extract_IL.PremiumMasterRunDate,
	mplt_Compute_DCT_NonWC_ModifiedPremium.o_DirectWrittenPremium,
	mplt_Compute_DCT_NonWC_ModifiedPremium.o_OtherModifiedPremium,
	mplt_Compute_DCT_NonWC_ModifiedPremium.o_ScheduleModifiedPremium,
	mplt_Compute_DCT_NonWC_ModifiedPremium.o_ExperienceModifiedPremium,
	mplt_Compute_DCT_NonWC_ModifiedPremium.o_SubjectDirectWrittenPremium,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	0 AS GeneratedRecordIndicator
	FROM EXP_Extract_IL
	 -- Manually join with mplt_Compute_DCT_NonWC_ModifiedPremium
),
LKP_ModifiedPremiumNonWorkersCompensationCalculation AS (
	SELECT
	ModifiedPremiumNonWorkersCompensationCalculationId,
	PremiumMasterCalculationId
	FROM (
		SELECT 
			ModifiedPremiumNonWorkersCompensationCalculationId,
			PremiumMasterCalculationId
		FROM ModifiedPremiumNonWorkersCompensationCalculation
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumMasterCalculationId ORDER BY ModifiedPremiumNonWorkersCompensationCalculationId) = 1
),
FTR_ModifiedPremiumNonWorkersCompensationCalculation_Insert AS (
	SELECT
	LKP_ModifiedPremiumNonWorkersCompensationCalculation.ModifiedPremiumNonWorkersCompensationCalculationId AS lkp_ModifiedPremiumNonWorkersCompensationCalculationId, 
	EXP_AuditColumns.PremiumMasterCalculationID, 
	EXP_AuditColumns.PremiumMasterRunDate, 
	EXP_AuditColumns.o_DirectWrittenPremium, 
	EXP_AuditColumns.o_OtherModifiedPremium, 
	EXP_AuditColumns.o_ScheduleModifiedPremium, 
	EXP_AuditColumns.o_ExperienceModifiedPremium, 
	EXP_AuditColumns.o_SubjectDirectWrittenPremium, 
	EXP_AuditColumns.AuditId, 
	EXP_AuditColumns.SourceSystemID, 
	EXP_AuditColumns.CreatedDate, 
	EXP_AuditColumns.ModifiedDate, 
	EXP_AuditColumns.GeneratedRecordIndicator
	FROM EXP_AuditColumns
	LEFT JOIN LKP_ModifiedPremiumNonWorkersCompensationCalculation
	ON LKP_ModifiedPremiumNonWorkersCompensationCalculation.PremiumMasterCalculationId = EXP_AuditColumns.PremiumMasterCalculationID
	WHERE isnull(lkp_ModifiedPremiumNonWorkersCompensationCalculationId)
),
ModifiedPremiumNonWorkersCompensationCalculation AS (
	INSERT INTO ModifiedPremiumNonWorkersCompensationCalculation
	(AuditId, SourceSystemID, CreatedDate, ModifiedDate, PremiumMasterCalculationId, RunDate, GeneratedRecordIndicator, DirectWrittenPremium, OtherModifiedPremium, ScheduleModifiedPremium, ExperienceModifiedPremium, SubjectWrittenPremium)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	PremiumMasterCalculationID AS PREMIUMMASTERCALCULATIONID, 
	PremiumMasterRunDate AS RUNDATE, 
	GENERATEDRECORDINDICATOR, 
	o_DirectWrittenPremium AS DIRECTWRITTENPREMIUM, 
	o_OtherModifiedPremium AS OTHERMODIFIEDPREMIUM, 
	o_ScheduleModifiedPremium AS SCHEDULEMODIFIEDPREMIUM, 
	o_ExperienceModifiedPremium AS EXPERIENCEMODIFIEDPREMIUM, 
	o_SubjectDirectWrittenPremium AS SUBJECTWRITTENPREMIUM
	FROM FTR_ModifiedPremiumNonWorkersCompensationCalculation_Insert
),