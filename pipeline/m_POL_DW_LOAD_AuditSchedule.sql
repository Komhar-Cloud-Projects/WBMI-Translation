WITH
SQ_pif_43ld_stage AS (
	SELECT
		pif_symbol,
		pif_policy_number,
		pif_module,
		pmd4d_insurance_line,
		pmd4d_status_1,
		pmd4d_effective_date_1,
		pmd4d_expiration_date_1,
		pmd4d_status_2,
		pmd4d_effective_date_2,
		pmd4d_expiration_date_2,
		pmd4d_status_3,
		pmd4d_effective_date_3,
		pmd4d_expiration_date_3,
		pmd4d_status_4,
		pmd4d_effective_date_4,
		pmd4d_expiration_date_4
	FROM pif_43ld_stage
	WHERE pif_43ld_stage.pmd4d_effective_date_1<>0 and pif_43ld_stage.pmd4d_effective_date_1 is not null
),
RTR_AuditSchedule AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	pmd4d_insurance_line,
	pmd4d_status_1,
	pmd4d_effective_date_1,
	pmd4d_expiration_date_1,
	pmd4d_status_2,
	pmd4d_effective_date_2,
	pmd4d_expiration_date_2,
	pmd4d_status_3,
	pmd4d_effective_date_3,
	pmd4d_expiration_date_3,
	pmd4d_status_4,
	pmd4d_effective_date_4,
	pmd4d_expiration_date_4
	FROM SQ_pif_43ld_stage
),
RTR_AuditSchedule_Effective1 AS (SELECT * FROM RTR_AuditSchedule WHERE pmd4d_effective_date_1>0 and NOT ISNULL(pmd4d_effective_date_1)),
RTR_AuditSchedule_Effective2 AS (SELECT * FROM RTR_AuditSchedule WHERE pmd4d_effective_date_2>0 and NOT ISNULL(pmd4d_effective_date_2)),
RTR_AuditSchedule_Effective3 AS (SELECT * FROM RTR_AuditSchedule WHERE pmd4d_effective_date_3>0 and NOT ISNULL(pmd4d_effective_date_3)),
RTR_AuditSchedule_Effective4 AS (SELECT * FROM RTR_AuditSchedule WHERE pmd4d_effective_date_4>0 and NOT ISNULL(pmd4d_effective_date_4)),
Union_AuditSchedule AS (
	SELECT pif_symbol, pif_policy_number, pif_module, pmd4d_insurance_line, pmd4d_status_ AS pmd4d_status, pmd4d_effective_date_ AS pmd4d_effective_date, pmd4d_expiration_date_ AS pmd4d_expiration_date
	FROM 
	UNION
	SELECT pif_symbol, pif_policy_number, pif_module, pmd4d_insurance_line, pmd4d_status_2 AS pmd4d_status, pmd4d_effective_date_2 AS pmd4d_effective_date, pmd4d_expiration_date_2 AS pmd4d_expiration_date
	FROM 
	UNION
	SELECT pif_symbol, pif_policy_number, pif_module, pmd4d_insurance_line, pmd4d_status_3 AS pmd4d_status, pmd4d_effective_date_3 AS pmd4d_effective_date, pmd4d_expiration_date_3 AS pmd4d_expiration_date
	FROM 
	UNION
	SELECT pif_symbol, pif_policy_number, pif_module, pmd4d_insurance_line, pmd4d_status_4 AS pmd4d_status, pmd4d_effective_date_4 AS pmd4d_effective_date, pmd4d_expiration_date_4 AS pmd4d_expiration_date
	FROM 
),
EXP_Status_Decode AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	pmd4d_insurance_line,
	pmd4d_status,
	pmd4d_effective_date,
	pmd4d_expiration_date,
	-- *INF*: DECODE(pmd4d_status, 'A',1,'C',2,'E',3,'V',4,'B',5,'O',6,'X',7,'R',8,9)
	DECODE(pmd4d_status,
		'A', 1,
		'C', 2,
		'E', 3,
		'V', 4,
		'B', 5,
		'O', 6,
		'X', 7,
		'R', 8,
		9) AS v_Status_Rank,
	v_Status_Rank AS o_Status_Rank
	FROM Union_AuditSchedule
),
SRT_AuditSchedule AS (
	SELECT
	pif_symbol, 
	pif_policy_number, 
	pif_module, 
	pmd4d_insurance_line, 
	pmd4d_effective_date, 
	pmd4d_expiration_date, 
	o_Status_Rank, 
	pmd4d_status
	FROM EXP_Status_Decode
	ORDER BY pif_symbol ASC, pif_policy_number ASC, pif_module ASC, pmd4d_insurance_line ASC, pmd4d_effective_date ASC, pmd4d_expiration_date ASC, o_Status_Rank DESC
),
AGG_Duplicate AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	pmd4d_insurance_line,
	pmd4d_effective_date,
	pmd4d_expiration_date,
	o_Status_Rank,
	pmd4d_status
	FROM SRT_AuditSchedule
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol, pif_policy_number, pif_module, pmd4d_insurance_line, pmd4d_effective_date, pmd4d_expiration_date ORDER BY NULL) = 1
),
EXP_MetaValues AS (
	SELECT
	pif_symbol AS i_pif_symbol,
	pif_policy_number AS i_pif_policy_number,
	pif_module AS i_pif_module,
	pmd4d_insurance_line AS i_pmd4d_insurance_line,
	pmd4d_status AS i_pmd4d_status,
	pmd4d_effective_date AS i_pmd4d_effective_date,
	pmd4d_expiration_date AS i_pmd4d_expiration_date,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	-- *INF*: TO_DATE('18000101','YYYYMMDD')
	TO_DATE('18000101', 'YYYYMMDD') AS o_EffectiveDate,
	-- *INF*: TO_DATE('21001231','YYYYMMDD')
	TO_DATE('21001231', 'YYYYMMDD') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_pif_symbol || i_pif_policy_number || i_pif_module AS o_PolicyKey,
	i_pmd4d_insurance_line AS o_InsuranceLine,
	-- *INF*: DECODE(TRUE,
	-- i_pmd4d_status='N','Unassigned',
	-- i_pmd4d_status='C','Completed',
	-- i_pmd4d_status='A','Amended',
	-- i_pmd4d_status='R','Requested',
	-- i_pmd4d_status='B','Bypassed',
	-- i_pmd4d_status='E','Estimated',
	-- i_pmd4d_status='V','Reversed',
	-- i_pmd4d_status='O','Overdue',
	-- i_pmd4d_status='X','Coverage Cancelled',
	-- i_pmd4d_status='U','Unscheduled Request',
	-- i_pmd4d_status='K','Unscheduled Complete',
	-- i_pmd4d_status='P','Unscheduled Overdue',
	-- i_pmd4d_status='D','Deleted'
	-- )
	DECODE(TRUE,
		i_pmd4d_status = 'N', 'Unassigned',
		i_pmd4d_status = 'C', 'Completed',
		i_pmd4d_status = 'A', 'Amended',
		i_pmd4d_status = 'R', 'Requested',
		i_pmd4d_status = 'B', 'Bypassed',
		i_pmd4d_status = 'E', 'Estimated',
		i_pmd4d_status = 'V', 'Reversed',
		i_pmd4d_status = 'O', 'Overdue',
		i_pmd4d_status = 'X', 'Coverage Cancelled',
		i_pmd4d_status = 'U', 'Unscheduled Request',
		i_pmd4d_status = 'K', 'Unscheduled Complete',
		i_pmd4d_status = 'P', 'Unscheduled Overdue',
		i_pmd4d_status = 'D', 'Deleted') AS o_AuditStatus,
	-- *INF*: TO_DATE(TO_CHAR(i_pmd4d_effective_date),'yyyymmdd')
	TO_DATE(TO_CHAR(i_pmd4d_effective_date), 'yyyymmdd') AS o_AuditEffectiveDate,
	-- *INF*: IIF(ISNULL(i_pmd4d_expiration_date) OR i_pmd4d_expiration_date=0,21001231,i_pmd4d_expiration_date)
	IFF(i_pmd4d_expiration_date IS NULL OR i_pmd4d_expiration_date = 0, 21001231, i_pmd4d_expiration_date) AS v_pmd4d_expiration_date,
	-- *INF*: TO_DATE(TO_CHAR(i_pmd4d_expiration_date),'yyyymmdd')
	TO_DATE(TO_CHAR(i_pmd4d_expiration_date), 'yyyymmdd') AS o_AuditExpirationDate
	FROM AGG_Duplicate
),
LKP_AuditSchedule AS (
	SELECT
	AuditScheduleId,
	PolicyKey,
	InsuranceLine,
	AuditEffectiveDate,
	AuditExpirationDate
	FROM (
		SELECT 
			AuditScheduleId,
			PolicyKey,
			InsuranceLine,
			AuditEffectiveDate,
			AuditExpirationDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.AuditSchedule
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,InsuranceLine,AuditEffectiveDate,AuditExpirationDate ORDER BY AuditScheduleId) = 1
),
FIL_AuditSchedule AS (
	SELECT
	LKP_AuditSchedule.AuditScheduleId AS lkp_AuditScheduleId, 
	EXP_MetaValues.o_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
	EXP_MetaValues.o_AuditId AS AuditId, 
	EXP_MetaValues.o_EffectiveDate AS EffectiveDate, 
	EXP_MetaValues.o_ExpirationDate AS ExpirationDate, 
	EXP_MetaValues.o_SourceSystemId AS SourceSystemId, 
	EXP_MetaValues.o_CreatedDate AS CreatedDate, 
	EXP_MetaValues.o_ModifiedDate AS ModifiedDate, 
	EXP_MetaValues.o_PolicyKey AS PolicyKey, 
	EXP_MetaValues.o_InsuranceLine AS InsuranceLine, 
	EXP_MetaValues.o_AuditStatus AS AuditStatus, 
	EXP_MetaValues.o_AuditEffectiveDate AS AuditEffectiveDate, 
	EXP_MetaValues.o_AuditExpirationDate AS AuditExpirationDate
	FROM EXP_MetaValues
	LEFT JOIN LKP_AuditSchedule
	ON LKP_AuditSchedule.PolicyKey = EXP_MetaValues.o_PolicyKey AND LKP_AuditSchedule.InsuranceLine = EXP_MetaValues.o_InsuranceLine AND LKP_AuditSchedule.AuditEffectiveDate = EXP_MetaValues.o_AuditEffectiveDate AND LKP_AuditSchedule.AuditExpirationDate = EXP_MetaValues.o_AuditExpirationDate
	WHERE ISNULL(lkp_AuditScheduleId)
),
SEQ_AuditSchedule AS (
	CREATE SEQUENCE SEQ_AuditSchedule
	START = 0
	INCREMENT = 1;
),
AuditSchedule AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AuditSchedule
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, AuditScheduleAKId, PolicyKey, InsuranceLine, AuditStatus, AuditEffectiveDate, AuditExpirationDate)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	SEQ_AuditSchedule.NEXTVAL AS AUDITSCHEDULEAKID, 
	POLICYKEY, 
	INSURANCELINE, 
	AUDITSTATUS, 
	AUDITEFFECTIVEDATE, 
	AUDITEXPIRATIONDATE
	FROM FIL_AuditSchedule
),