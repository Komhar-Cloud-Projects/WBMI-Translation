WITH
LKP_claimant_dim AS (
	SELECT
	claimant_dim_id,
	edw_claim_party_occurrence_ak_id
	FROM (
		SELECT 
		C.claimant_dim_id as claimant_dim_id, C.edw_claim_party_occurrence_ak_id as edw_claim_party_occurrence_ak_id 
		
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_dim C
		
		where
		C.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_party_occurrence_ak_id ORDER BY claimant_dim_id) = 1
),
LKP_claim_party_occurrence AS (
	SELECT
	claim_party_occurrence_id,
	claim_party_occurrence_ak_id
	FROM (
		SELECT 
		CPO.claim_party_occurrence_id as claim_party_occurrence_id, CPO.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id 
		
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO
		
		where
		CPO.crrnt_snpsht_flag = 1
		AND
		CPO.claim_party_role_code in ('CMT','CLMT')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id ORDER BY claim_party_occurrence_id) = 1
),
LKP_ClaimantMedicalCodeDim AS (
	SELECT
	ClaimantMedicalCodeDimId,
	EdwClaimPartyOccurrenceAkId,
	EdwSupMedicalDiagnosisCodePkId,
	EdwSupMedicalCauseCodePkId,
	EdwSupSurgeryTypePkId,
	MedicalCode,
	MedicalCodeType
	FROM (
		SELECT 
		C.ClaimantMedicalCodeDimId as ClaimantMedicalCodeDimId, 
		C.EdwClaimPartyOccurrenceAkId as EdwClaimPartyOccurrenceAkId, 
		C.EdwSupMedicalDiagnosisCodePkId as EdwSupMedicalDiagnosisCodePkId, 
		C.EdwSupMedicalCauseCodePkId as EdwSupMedicalCauseCodePkId, 
		C.EdwSupSurgeryTypePkId as EdwSupSurgeryTypePkId, 
		C.MedicalCode as MedicalCode, 
		C.MedicalCodeDescription as MedicalCodeDescription, 
		C.MedicalCodeType as MedicalCodeType 
		
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.ClaimantMedicalCodeDim C
		
		where
		C.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EdwClaimPartyOccurrenceAkId,EdwSupMedicalDiagnosisCodePkId,EdwSupMedicalCauseCodePkId,EdwSupSurgeryTypePkId,MedicalCode,MedicalCodeType ORDER BY ClaimantMedicalCodeDimId) = 1
),
SQ_claim_medical_Cause AS (
	SELECT 
	CM.claim_med_id, 
	CM.eff_from_date, 
	CM.claim_med_ak_id, 
	CM.claim_party_occurrence_ak_id, 
	CM.patient_cause_code 
	
	
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_medical CM
	
	where
	CM.crrnt_snpsht_flag = 1
	AND
	CM.patient_cause_code <> 'N/A'
	AND
	CM.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	
	order by
	CM.claim_med_ak_id
),
EXP_Src_Value_CAUSE AS (
	SELECT
	claim_med_id,
	EffectiveDate,
	claim_med_ak_id,
	claim_party_occurrence_ak_id,
	patient_cause_code AS IN_patient_cause_code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(IN_patient_cause_code),'N/A',
	-- IS_SPACES(IN_patient_cause_code),'N/A',
	-- LENGTH(IN_patient_cause_code)=0,'N/A',
	-- LTRIM(RTRIM(IN_patient_cause_code)))
	DECODE(TRUE,
		IN_patient_cause_code IS NULL, 'N/A',
		IS_SPACES(IN_patient_cause_code), 'N/A',
		LENGTH(IN_patient_cause_code) = 0, 'N/A',
		LTRIM(RTRIM(IN_patient_cause_code))) AS patient_cause_code
	FROM SQ_claim_medical_Cause
),
LKP_SupMedicalCauseCode_Src AS (
	SELECT
	SupMedicalCauseCodeId,
	ShortDescription,
	MedicalCauseCode
	FROM (
		SELECT 
		SC.SupMedicalCauseCodeId as SupMedicalCauseCodeId, 
		ltrim(rtrim(SC.ShortDescription)) as ShortDescription, 
		ltrim(rtrim(SC.MedicalCauseCode)) as MedicalCauseCode 
		
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.SupMedicalCauseCode SC
		
		where
		SC.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MedicalCauseCode ORDER BY SupMedicalCauseCodeId) = 1
),
EXP_Defualt_Values_CAUSE AS (
	SELECT
	EXP_Src_Value_CAUSE.claim_party_occurrence_ak_id AS IN_claim_party_occurrence_ak_id,
	-- *INF*: iif(isnull(IN_claim_party_occurrence_ak_id),-1,IN_claim_party_occurrence_ak_id)
	IFF(IN_claim_party_occurrence_ak_id IS NULL, - 1, IN_claim_party_occurrence_ak_id) AS v_claim_party_occurrence_ak_id,
	v_claim_party_occurrence_ak_id AS EdwClaimPartyOccurrenceAkId,
	-- *INF*: iif(isnull(:LKP.LKP_CLAIM_PARTY_OCCURRENCE(IN_claim_party_occurrence_ak_id)),
	-- -1,
	-- :LKP.LKP_CLAIM_PARTY_OCCURRENCE(IN_claim_party_occurrence_ak_id))
	IFF(LKP_CLAIM_PARTY_OCCURRENCE_IN_claim_party_occurrence_ak_id.claim_party_occurrence_id IS NULL, - 1, LKP_CLAIM_PARTY_OCCURRENCE_IN_claim_party_occurrence_ak_id.claim_party_occurrence_id) AS v_claim_party_occurrence_id,
	v_claim_party_occurrence_id AS EdwClaimPartyOccurrencePkId,
	-- *INF*: iif(isnull(:LKP.LKP_CLAIMANT_DIM(IN_claim_party_occurrence_ak_id))
	-- ,-1,
	-- :LKP.LKP_CLAIMANT_DIM(IN_claim_party_occurrence_ak_id))
	IFF(LKP_CLAIMANT_DIM_IN_claim_party_occurrence_ak_id.claimant_dim_id IS NULL, - 1, LKP_CLAIMANT_DIM_IN_claim_party_occurrence_ak_id.claimant_dim_id) AS v_claimant_dim_id,
	v_claimant_dim_id AS claimant_dim_id,
	-1 AS v_SupMedicalDiagnosisCodeId,
	v_SupMedicalDiagnosisCodeId AS EdwSupMedicalDiagnosisCodePkId,
	LKP_SupMedicalCauseCode_Src.SupMedicalCauseCodeId AS IN_SupMedicalCauseCodeId,
	-- *INF*: iif(isnull(IN_SupMedicalCauseCodeId),-1,IN_SupMedicalCauseCodeId)
	IFF(IN_SupMedicalCauseCodeId IS NULL, - 1, IN_SupMedicalCauseCodeId) AS v_SupMedicalCauseCodeId,
	v_SupMedicalCauseCodeId AS EdwSupMedicalCauseCodePkId,
	-1 AS v_SupMedicalSurgeryCodeId,
	v_SupMedicalSurgeryCodeId AS EdwSupSurgeryTypePkId,
	'CAUSE' AS v_MedicalCauseType,
	v_MedicalCauseType AS MedicalCodeType,
	EXP_Src_Value_CAUSE.patient_cause_code AS IN_MedicalCauseCode,
	-- *INF*: iif(isnull(IN_MedicalCauseCode),'N/A',IN_MedicalCauseCode)
	IFF(IN_MedicalCauseCode IS NULL, 'N/A', IN_MedicalCauseCode) AS v_MedicalCauseCode,
	v_MedicalCauseCode AS MedicalCode,
	LKP_SupMedicalCauseCode_Src.ShortDescription AS IN_MedicalCauseDescription,
	-- *INF*: iif(isnull(IN_MedicalCauseDescription),'N/A',IN_MedicalCauseDescription)
	IFF(IN_MedicalCauseDescription IS NULL, 'N/A', IN_MedicalCauseDescription) AS v_MedicalCauseDescription,
	v_MedicalCauseDescription AS MedicalCodeDescription,
	EXP_Src_Value_CAUSE.EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	sysdate AS CreatedDate,
	sysdate AS ModifiedDate,
	-- *INF*: :LKP.LKP_CLAIMANTMEDICALCODEDIM(v_claim_party_occurrence_ak_id, v_SupMedicalDiagnosisCodeId, v_SupMedicalCauseCodeId, v_SupMedicalSurgeryCodeId,v_MedicalCauseCode,v_MedicalCauseType)
	LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalCauseCode_v_MedicalCauseType.ClaimantMedicalCodeDimId AS v_ClaimantMedicalDimId,
	v_ClaimantMedicalDimId AS ClaimantMedicalCodeDimId
	FROM EXP_Src_Value_CAUSE
	LEFT JOIN LKP_SupMedicalCauseCode_Src
	ON LKP_SupMedicalCauseCode_Src.MedicalCauseCode = EXP_Src_Value_CAUSE.patient_cause_code
	LEFT JOIN LKP_CLAIM_PARTY_OCCURRENCE LKP_CLAIM_PARTY_OCCURRENCE_IN_claim_party_occurrence_ak_id
	ON LKP_CLAIM_PARTY_OCCURRENCE_IN_claim_party_occurrence_ak_id.claim_party_occurrence_ak_id = IN_claim_party_occurrence_ak_id

	LEFT JOIN LKP_CLAIMANT_DIM LKP_CLAIMANT_DIM_IN_claim_party_occurrence_ak_id
	ON LKP_CLAIMANT_DIM_IN_claim_party_occurrence_ak_id.edw_claim_party_occurrence_ak_id = IN_claim_party_occurrence_ak_id

	LEFT JOIN LKP_CLAIMANTMEDICALCODEDIM LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalCauseCode_v_MedicalCauseType
	ON LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalCauseCode_v_MedicalCauseType.EdwClaimPartyOccurrenceAkId = v_claim_party_occurrence_ak_id
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalCauseCode_v_MedicalCauseType.EdwSupMedicalDiagnosisCodePkId = v_SupMedicalDiagnosisCodeId
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalCauseCode_v_MedicalCauseType.EdwSupMedicalCauseCodePkId = v_SupMedicalCauseCodeId
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalCauseCode_v_MedicalCauseType.EdwSupSurgeryTypePkId = v_SupMedicalSurgeryCodeId
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalCauseCode_v_MedicalCauseType.MedicalCode = v_MedicalCauseCode
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalCauseCode_v_MedicalCauseType.MedicalCodeType = v_MedicalCauseType

),
RTR_Cause AS (
	SELECT
	ClaimantMedicalCodeDimId,
	EdwClaimPartyOccurrencePkId,
	EdwClaimPartyOccurrenceAkId,
	claimant_dim_id,
	EdwSupMedicalDiagnosisCodePkId,
	EdwSupMedicalCauseCodePkId,
	EdwSupSurgeryTypePkId,
	MedicalCodeType,
	MedicalCode,
	MedicalCodeDescription,
	EffectiveDate,
	ExpirationDate,
	CurrentSnapshotFlag,
	AuditID,
	CreatedDate,
	ModifiedDate
	FROM EXP_Defualt_Values_CAUSE
),
RTR_Cause_Insert_Cause AS (SELECT * FROM RTR_Cause WHERE isnull(ClaimantMedicalCodeDimId)),
RTR_Cause_DEFAULT1 AS (SELECT * FROM RTR_Cause WHERE NOT ( (isnull(ClaimantMedicalCodeDimId)) )),
UPD_Update_CAUSE AS (
	SELECT
	ClaimantMedicalCodeDimId, 
	EdwClaimPartyOccurrencePkId, 
	EdwClaimPartyOccurrenceAkId, 
	claimant_dim_id, 
	EdwSupMedicalDiagnosisCodePkId, 
	EdwSupMedicalCauseCodePkId, 
	EdwSupSurgeryTypePkId, 
	MedicalCodeType, 
	MedicalCode, 
	MedicalCodeDescription, 
	EffectiveDate, 
	ExpirationDate, 
	CurrentSnapshotFlag, 
	AuditID, 
	CreatedDate, 
	ModifiedDate
	FROM RTR_Cause_DEFAULT1
),
ClaimantMedicalCodeDim_Update_CAUSE AS (
	MERGE INTO ClaimantMedicalCodeDim AS T
	USING UPD_Update_CAUSE AS S
	ON T.ClaimantMedicalCodeDimId = S.ClaimantMedicalCodeDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.AuditID = S.AuditID, T.EffectiveDate = S.EffectiveDate, T.ExpirationDate = S.ExpirationDate, T.CreatedDate = S.CreatedDate, T.ModifiedDate = S.ModifiedDate, T.EdwClaimPartyOccurrencePkId = S.EdwClaimPartyOccurrencePkId, T.EdwClaimPartyOccurrenceAkId = S.EdwClaimPartyOccurrenceAkId, T.claimant_dim_id = S.claimant_dim_id, T.EdwSupMedicalDiagnosisCodePkId = S.EdwSupMedicalDiagnosisCodePkId, T.EdwSupMedicalCauseCodePkId = S.EdwSupMedicalCauseCodePkId, T.EdwSupSurgeryTypePkId = S.EdwSupSurgeryTypePkId, T.MedicalCodeType = S.MedicalCodeType, T.MedicalCode = S.MedicalCode, T.MedicalCodeDescription = S.MedicalCodeDescription
),
UPD_Insert_CAUSE AS (
	SELECT
	EdwClaimPartyOccurrencePkId, 
	EdwClaimPartyOccurrenceAkId, 
	claimant_dim_id, 
	EdwSupMedicalDiagnosisCodePkId, 
	EdwSupMedicalCauseCodePkId, 
	EdwSupSurgeryTypePkId, 
	MedicalCodeType, 
	MedicalCode, 
	MedicalCodeDescription, 
	EffectiveDate, 
	ExpirationDate, 
	CurrentSnapshotFlag, 
	AuditID, 
	CreatedDate, 
	ModifiedDate
	FROM RTR_Cause_Insert_Cause
),
ClaimantMedicalCodeDim_Insert_CAUSE AS (
	INSERT INTO ClaimantMedicalCodeDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, EdwClaimPartyOccurrencePkId, EdwClaimPartyOccurrenceAkId, claimant_dim_id, EdwSupMedicalDiagnosisCodePkId, EdwSupMedicalCauseCodePkId, EdwSupSurgeryTypePkId, MedicalCodeType, MedicalCode, MedicalCodeDescription)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	EDWCLAIMPARTYOCCURRENCEPKID, 
	EDWCLAIMPARTYOCCURRENCEAKID, 
	CLAIMANT_DIM_ID, 
	EDWSUPMEDICALDIAGNOSISCODEPKID, 
	EDWSUPMEDICALCAUSECODEPKID, 
	EDWSUPSURGERYTYPEPKID, 
	MEDICALCODETYPE, 
	MEDICALCODE, 
	MEDICALCODEDESCRIPTION
	FROM UPD_Insert_CAUSE
),
SQ_Claim_medical_DIAG AS (
	SELECT
	CM.claim_med_id, 
	CM.eff_from_date,
	CM.claim_med_ak_id, 
	CM.claim_party_occurrence_ak_id,
	CM.patient_diag_code
	
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_medical CM 
	
	where
	CM.crrnt_snpsht_flag = 1
	AND
	CM.patient_diag_code <> 'N/A'
	AND
	CM.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	
	order by 
	CM.claim_med_ak_id
),
EXP_Src_Value_DIAG AS (
	SELECT
	claim_med_id,
	EffectiveDate,
	claim_med_ak_id,
	claim_party_occurrence_ak_id,
	patient_diag_code AS IN_patient_diag_code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(IN_patient_diag_code),'N/A',
	-- IS_SPACES(IN_patient_diag_code),'N/A',
	-- LENGTH(IN_patient_diag_code)=0,'N/A',
	-- LTRIM(RTRIM(IN_patient_diag_code)))
	DECODE(TRUE,
		IN_patient_diag_code IS NULL, 'N/A',
		IS_SPACES(IN_patient_diag_code), 'N/A',
		LENGTH(IN_patient_diag_code) = 0, 'N/A',
		LTRIM(RTRIM(IN_patient_diag_code))) AS patient_diag_code
	FROM SQ_Claim_medical_DIAG
),
LKP_SupMedicalDiagnosisCode_Src AS (
	SELECT
	SupMedicalDiagnosisCodeId,
	ShortDescription,
	MedicalDiagnosisCode
	FROM (
		SELECT 
		SD.SupMedicalDiagnosisCodeId as SupMedicalDiagnosisCodeId, 
		ltrim(rtrim(SD.ShortDescription)) as ShortDescription, 
		ltrim(rtrim(SD.MedicalDiagnosisCode)) as MedicalDiagnosisCode 
		
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.SupMedicalDiagnosisCode SD
		
		where
		SD.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MedicalDiagnosisCode ORDER BY SupMedicalDiagnosisCodeId) = 1
),
EXP_Defualt_Values_DIAG AS (
	SELECT
	EXP_Src_Value_DIAG.claim_party_occurrence_ak_id AS IN_claim_party_occurrence_ak_id,
	-- *INF*: iif(isnull(IN_claim_party_occurrence_ak_id),-1,IN_claim_party_occurrence_ak_id)
	IFF(IN_claim_party_occurrence_ak_id IS NULL, - 1, IN_claim_party_occurrence_ak_id) AS v_claim_party_occurrence_ak_id,
	v_claim_party_occurrence_ak_id AS EdwClaimPartyOccurrenceAkId,
	-- *INF*: iif(isnull(:LKP.LKP_CLAIM_PARTY_OCCURRENCE(IN_claim_party_occurrence_ak_id)),
	-- -1,
	-- :LKP.LKP_CLAIM_PARTY_OCCURRENCE(IN_claim_party_occurrence_ak_id))
	IFF(LKP_CLAIM_PARTY_OCCURRENCE_IN_claim_party_occurrence_ak_id.claim_party_occurrence_id IS NULL, - 1, LKP_CLAIM_PARTY_OCCURRENCE_IN_claim_party_occurrence_ak_id.claim_party_occurrence_id) AS v_claim_party_occurrence_id,
	v_claim_party_occurrence_id AS EdwClaimPartyOccurrencePkId,
	-- *INF*: iif(isnull(:LKP.LKP_CLAIMANT_DIM(IN_claim_party_occurrence_ak_id))
	-- ,-1,
	-- :LKP.LKP_CLAIMANT_DIM(IN_claim_party_occurrence_ak_id))
	IFF(LKP_CLAIMANT_DIM_IN_claim_party_occurrence_ak_id.claimant_dim_id IS NULL, - 1, LKP_CLAIMANT_DIM_IN_claim_party_occurrence_ak_id.claimant_dim_id) AS v_claimant_dim_id,
	v_claimant_dim_id AS claimant_dim_id,
	LKP_SupMedicalDiagnosisCode_Src.SupMedicalDiagnosisCodeId AS IN_SupMedicalDiagnosisCodeId,
	-- *INF*: iif(isnull(IN_SupMedicalDiagnosisCodeId),-1,IN_SupMedicalDiagnosisCodeId)
	IFF(IN_SupMedicalDiagnosisCodeId IS NULL, - 1, IN_SupMedicalDiagnosisCodeId) AS v_SupMedicalDiagnosisCodeId,
	v_SupMedicalDiagnosisCodeId AS EdwSupMedicalDiagnosisCodePkId,
	-1 AS v_SupMedicalCauseCodeId,
	v_SupMedicalCauseCodeId AS EdwSupMedicalCauseCodePkId,
	-1 AS v_SupMedicalSurgeryCodeId,
	v_SupMedicalSurgeryCodeId AS EdwSupSurgeryTypePkId,
	'DIAG' AS v_MedicalDiagnosisType,
	v_MedicalDiagnosisType AS MedicalCodeType,
	EXP_Src_Value_DIAG.patient_diag_code AS IN_MedicalDiagnosisCode,
	-- *INF*: iif(isnull(IN_MedicalDiagnosisCode),'N/A',IN_MedicalDiagnosisCode)
	IFF(IN_MedicalDiagnosisCode IS NULL, 'N/A', IN_MedicalDiagnosisCode) AS v_MedicalDiagnosisCode,
	v_MedicalDiagnosisCode AS MedicalCode,
	LKP_SupMedicalDiagnosisCode_Src.ShortDescription AS IN_MedicalDiagnosisDescription,
	-- *INF*: iif(isnull(IN_MedicalDiagnosisDescription),'N/A',IN_MedicalDiagnosisDescription)
	IFF(IN_MedicalDiagnosisDescription IS NULL, 'N/A', IN_MedicalDiagnosisDescription) AS v_MedicalDiagnosisDescription,
	v_MedicalDiagnosisDescription AS MedicalCodeDescription,
	EXP_Src_Value_DIAG.EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	sysdate AS CreatedDate,
	sysdate AS ModifiedDate,
	-- *INF*: :LKP.LKP_CLAIMANTMEDICALCODEDIM(v_claim_party_occurrence_ak_id,v_SupMedicalDiagnosisCodeId,v_SupMedicalCauseCodeId,v_SupMedicalSurgeryCodeId,v_MedicalDiagnosisCode,v_MedicalDiagnosisType)
	LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalDiagnosisCode_v_MedicalDiagnosisType.ClaimantMedicalCodeDimId AS v_ClaimantMedicalDimId,
	v_ClaimantMedicalDimId AS ClaimantMedicalCodeDimId
	FROM EXP_Src_Value_DIAG
	LEFT JOIN LKP_SupMedicalDiagnosisCode_Src
	ON LKP_SupMedicalDiagnosisCode_Src.MedicalDiagnosisCode = EXP_Src_Value_DIAG.patient_diag_code
	LEFT JOIN LKP_CLAIM_PARTY_OCCURRENCE LKP_CLAIM_PARTY_OCCURRENCE_IN_claim_party_occurrence_ak_id
	ON LKP_CLAIM_PARTY_OCCURRENCE_IN_claim_party_occurrence_ak_id.claim_party_occurrence_ak_id = IN_claim_party_occurrence_ak_id

	LEFT JOIN LKP_CLAIMANT_DIM LKP_CLAIMANT_DIM_IN_claim_party_occurrence_ak_id
	ON LKP_CLAIMANT_DIM_IN_claim_party_occurrence_ak_id.edw_claim_party_occurrence_ak_id = IN_claim_party_occurrence_ak_id

	LEFT JOIN LKP_CLAIMANTMEDICALCODEDIM LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalDiagnosisCode_v_MedicalDiagnosisType
	ON LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalDiagnosisCode_v_MedicalDiagnosisType.EdwClaimPartyOccurrenceAkId = v_claim_party_occurrence_ak_id
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalDiagnosisCode_v_MedicalDiagnosisType.EdwSupMedicalDiagnosisCodePkId = v_SupMedicalDiagnosisCodeId
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalDiagnosisCode_v_MedicalDiagnosisType.EdwSupMedicalCauseCodePkId = v_SupMedicalCauseCodeId
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalDiagnosisCode_v_MedicalDiagnosisType.EdwSupSurgeryTypePkId = v_SupMedicalSurgeryCodeId
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalDiagnosisCode_v_MedicalDiagnosisType.MedicalCode = v_MedicalDiagnosisCode
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalDiagnosisCode_v_MedicalDiagnosisType.MedicalCodeType = v_MedicalDiagnosisType

),
RTR_Diag AS (
	SELECT
	ClaimantMedicalCodeDimId,
	EdwClaimPartyOccurrencePkId,
	EdwClaimPartyOccurrenceAkId,
	claimant_dim_id,
	EdwSupMedicalDiagnosisCodePkId,
	EdwSupMedicalCauseCodePkId,
	EdwSupSurgeryTypePkId,
	MedicalCodeType,
	MedicalCode,
	MedicalCodeDescription,
	EffectiveDate,
	ExpirationDate,
	CurrentSnapshotFlag,
	AuditID,
	CreatedDate,
	ModifiedDate
	FROM EXP_Defualt_Values_DIAG
),
RTR_Diag_Insert_Diag AS (SELECT * FROM RTR_Diag WHERE isnull(ClaimantMedicalCodeDimId)),
RTR_Diag_DEFAULT1 AS (SELECT * FROM RTR_Diag WHERE NOT ( (isnull(ClaimantMedicalCodeDimId)) )),
UPD_update_DIAG AS (
	SELECT
	ClaimantMedicalCodeDimId, 
	EdwClaimPartyOccurrencePkId, 
	EdwClaimPartyOccurrenceAkId, 
	claimant_dim_id, 
	EdwSupMedicalDiagnosisCodePkId, 
	EdwSupMedicalCauseCodePkId, 
	EdwSupSurgeryTypePkId, 
	MedicalCodeType, 
	MedicalCode, 
	MedicalCodeDescription, 
	EffectiveDate, 
	ExpirationDate, 
	CurrentSnapshotFlag, 
	AuditID, 
	CreatedDate, 
	ModifiedDate
	FROM RTR_Diag_DEFAULT1
),
ClaimantMedicalCodeDim_Update_DIAG AS (
	MERGE INTO ClaimantMedicalCodeDim AS T
	USING UPD_update_DIAG AS S
	ON T.ClaimantMedicalCodeDimId = S.ClaimantMedicalCodeDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.AuditID = S.AuditID, T.EffectiveDate = S.EffectiveDate, T.ExpirationDate = S.ExpirationDate, T.CreatedDate = S.CreatedDate, T.ModifiedDate = S.ModifiedDate, T.EdwClaimPartyOccurrencePkId = S.EdwClaimPartyOccurrencePkId, T.EdwClaimPartyOccurrenceAkId = S.EdwClaimPartyOccurrenceAkId, T.claimant_dim_id = S.claimant_dim_id, T.EdwSupMedicalDiagnosisCodePkId = S.EdwSupMedicalDiagnosisCodePkId, T.EdwSupMedicalCauseCodePkId = S.EdwSupMedicalCauseCodePkId, T.EdwSupSurgeryTypePkId = S.EdwSupSurgeryTypePkId, T.MedicalCodeType = S.MedicalCodeType, T.MedicalCode = S.MedicalCode, T.MedicalCodeDescription = S.MedicalCodeDescription
),
UPD_Insert_DIAG AS (
	SELECT
	EdwClaimPartyOccurrencePkId, 
	EdwClaimPartyOccurrenceAkId, 
	claimant_dim_id, 
	EdwSupMedicalDiagnosisCodePkId, 
	EdwSupMedicalCauseCodePkId, 
	EdwSupSurgeryTypePkId, 
	MedicalCodeType, 
	MedicalCode, 
	MedicalCodeDescription, 
	EffectiveDate, 
	ExpirationDate, 
	CurrentSnapshotFlag, 
	AuditID, 
	CreatedDate, 
	ModifiedDate
	FROM RTR_Diag_Insert_Diag
),
ClaimantMedicalCodeDim_Insert_DIAG AS (
	INSERT INTO ClaimantMedicalCodeDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, EdwClaimPartyOccurrencePkId, EdwClaimPartyOccurrenceAkId, claimant_dim_id, EdwSupMedicalDiagnosisCodePkId, EdwSupMedicalCauseCodePkId, EdwSupSurgeryTypePkId, MedicalCodeType, MedicalCode, MedicalCodeDescription)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	EDWCLAIMPARTYOCCURRENCEPKID, 
	EDWCLAIMPARTYOCCURRENCEAKID, 
	CLAIMANT_DIM_ID, 
	EDWSUPMEDICALDIAGNOSISCODEPKID, 
	EDWSUPMEDICALCAUSECODEPKID, 
	EDWSUPSURGERYTYPEPKID, 
	MEDICALCODETYPE, 
	MEDICALCODE, 
	MEDICALCODEDESCRIPTION
	FROM UPD_Insert_DIAG
),
SQ_claim_medical_patient_diagnosis_additional AS (
	SELECT 
	CMDA.claim_med_patient_diag_add_id, 
	CMDA.eff_from_date, 
	CMDA.claim_med_patient_diag_add_ak_id, 
	CMDA.claim_med_ak_id, 
	CMDA.patient_add_code, 
	CMDA.patient_diag_code 
	
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_medical_patient_diagnosis_additional CMDA
	 
	 where
	 CMDA.crrnt_snpsht_flag = 1
	 AND
	 CMDA.patient_diag_code <> 'N/A'
),
EXP_Src_Values_DIAG_ADD AS (
	SELECT
	claim_med_patient_diag_add_id,
	EffectiveDate,
	claim_med_patient_diag_add_ak_id,
	claim_med_ak_id,
	patient_add_code,
	patient_diag_code AS IN_patient_diag_code,
	-- *INF*: iif(isnull(IN_patient_diag_code), 'N/A',IN_patient_diag_code)
	IFF(IN_patient_diag_code IS NULL, 'N/A', IN_patient_diag_code) AS patient_diag_code
	FROM SQ_claim_medical_patient_diagnosis_additional
),
LKP_SupMedicalDiagnosisCode_Add AS (
	SELECT
	SupMedicalDiagnosisCodeId,
	ShortDescription,
	MedicalDiagnosisCode
	FROM (
		SELECT 
		SD.SupMedicalDiagnosisCodeId as SupMedicalDiagnosisCodeId, 
		ltrim(rtrim(SD.ShortDescription)) as ShortDescription, 
		ltrim(rtrim(SD.MedicalDiagnosisCode)) as MedicalDiagnosisCode 
		
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.SupMedicalDiagnosisCode SD
		
		where
		SD.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MedicalDiagnosisCode ORDER BY SupMedicalDiagnosisCodeId) = 1
),
LKP_claim_medical_ADD AS (
	SELECT
	claim_party_occurrence_ak_id,
	claim_med_ak_id
	FROM (
		SELECT 
		CM.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
		CM.claim_med_ak_id as claim_med_ak_id 
		
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_medical CM
		
		where
		CM.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_med_ak_id ORDER BY claim_party_occurrence_ak_id) = 1
),
EXP_Defualt_Values_DIAG_ADD AS (
	SELECT
	LKP_claim_medical_ADD.claim_party_occurrence_ak_id AS IN_claim_party_occurrence_ak_id,
	-- *INF*: iif(isnull(IN_claim_party_occurrence_ak_id),-1,IN_claim_party_occurrence_ak_id)
	IFF(IN_claim_party_occurrence_ak_id IS NULL, - 1, IN_claim_party_occurrence_ak_id) AS v_claim_party_occurrence_ak_id,
	v_claim_party_occurrence_ak_id AS EdwClaimPartyOccurrenceAkId,
	-- *INF*: iif(isnull(:LKP.LKP_CLAIM_PARTY_OCCURRENCE(IN_claim_party_occurrence_ak_id)),
	-- -1,
	-- :LKP.LKP_CLAIM_PARTY_OCCURRENCE(IN_claim_party_occurrence_ak_id))
	IFF(LKP_CLAIM_PARTY_OCCURRENCE_IN_claim_party_occurrence_ak_id.claim_party_occurrence_id IS NULL, - 1, LKP_CLAIM_PARTY_OCCURRENCE_IN_claim_party_occurrence_ak_id.claim_party_occurrence_id) AS v_claim_party_occurrence_id,
	v_claim_party_occurrence_id AS EdwClaimPartyOccurrencePkId,
	-- *INF*: iif(isnull(:LKP.LKP_CLAIMANT_DIM(IN_claim_party_occurrence_ak_id))
	-- ,-1,
	-- :LKP.LKP_CLAIMANT_DIM(IN_claim_party_occurrence_ak_id))
	IFF(LKP_CLAIMANT_DIM_IN_claim_party_occurrence_ak_id.claimant_dim_id IS NULL, - 1, LKP_CLAIMANT_DIM_IN_claim_party_occurrence_ak_id.claimant_dim_id) AS v_claimant_dim_id,
	v_claimant_dim_id AS claimant_dim_id,
	LKP_SupMedicalDiagnosisCode_Add.SupMedicalDiagnosisCodeId AS IN_SupMedicalDiagnosisCodeId_ADD,
	-- *INF*: iif(isnull(IN_SupMedicalDiagnosisCodeId_ADD),-1,IN_SupMedicalDiagnosisCodeId_ADD)
	IFF(IN_SupMedicalDiagnosisCodeId_ADD IS NULL, - 1, IN_SupMedicalDiagnosisCodeId_ADD) AS v_SupMedicalDiagnosisCodeId_ADD,
	v_SupMedicalDiagnosisCodeId_ADD AS EdwSupMedicalDiagnosisCodePkId,
	-1 AS v_SupMedicalCauseCodeId,
	v_SupMedicalCauseCodeId AS EdwSupMedicalCauseCodePkId,
	-1 AS v_SupMedicalSurgeryCodeId,
	v_SupMedicalSurgeryCodeId AS EdwSupSurgeryTypePkId,
	'DIAG' AS v_MedicalDiagnosisType_ADD,
	v_MedicalDiagnosisType_ADD AS MedicalCodeType,
	EXP_Src_Values_DIAG_ADD.patient_diag_code AS IN_MedicalDiagnosisCode_ADD,
	-- *INF*: iif(isnull(IN_MedicalDiagnosisCode_ADD),'N/A',IN_MedicalDiagnosisCode_ADD)
	IFF(IN_MedicalDiagnosisCode_ADD IS NULL, 'N/A', IN_MedicalDiagnosisCode_ADD) AS v_MedicalDiagnosisCode_ADD,
	v_MedicalDiagnosisCode_ADD AS MedicalCode,
	LKP_SupMedicalDiagnosisCode_Add.ShortDescription AS IN_MedicalDiagnosisDescription_ADD,
	-- *INF*: iif(isnull(IN_MedicalDiagnosisDescription_ADD),'N/A',IN_MedicalDiagnosisDescription_ADD)
	IFF(IN_MedicalDiagnosisDescription_ADD IS NULL, 'N/A', IN_MedicalDiagnosisDescription_ADD) AS v_MedicalDiagnosisDescription_ADD,
	v_MedicalDiagnosisDescription_ADD AS MedicalCodeDescription,
	EXP_Src_Values_DIAG_ADD.EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	sysdate AS CreatedDate,
	sysdate AS ModifiedDate,
	-- *INF*: :LKP.LKP_CLAIMANTMEDICALCODEDIM(v_claim_party_occurrence_ak_id, v_SupMedicalDiagnosisCodeId_ADD, v_SupMedicalCauseCodeId, v_SupMedicalSurgeryCodeId,v_MedicalDiagnosisCode_ADD,v_MedicalDiagnosisType_ADD)
	-- 
	LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_ADD_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalDiagnosisCode_ADD_v_MedicalDiagnosisType_ADD.ClaimantMedicalCodeDimId AS v_ClaimantMedicalCodeDimId,
	v_ClaimantMedicalCodeDimId AS ClaimantMedicalCodeDimId
	FROM EXP_Src_Values_DIAG_ADD
	LEFT JOIN LKP_SupMedicalDiagnosisCode_Add
	ON LKP_SupMedicalDiagnosisCode_Add.MedicalDiagnosisCode = EXP_Src_Values_DIAG_ADD.patient_diag_code
	LEFT JOIN LKP_claim_medical_ADD
	ON LKP_claim_medical_ADD.claim_med_ak_id = EXP_Src_Values_DIAG_ADD.claim_med_ak_id
	LEFT JOIN LKP_CLAIM_PARTY_OCCURRENCE LKP_CLAIM_PARTY_OCCURRENCE_IN_claim_party_occurrence_ak_id
	ON LKP_CLAIM_PARTY_OCCURRENCE_IN_claim_party_occurrence_ak_id.claim_party_occurrence_ak_id = IN_claim_party_occurrence_ak_id

	LEFT JOIN LKP_CLAIMANT_DIM LKP_CLAIMANT_DIM_IN_claim_party_occurrence_ak_id
	ON LKP_CLAIMANT_DIM_IN_claim_party_occurrence_ak_id.edw_claim_party_occurrence_ak_id = IN_claim_party_occurrence_ak_id

	LEFT JOIN LKP_CLAIMANTMEDICALCODEDIM LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_ADD_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalDiagnosisCode_ADD_v_MedicalDiagnosisType_ADD
	ON LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_ADD_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalDiagnosisCode_ADD_v_MedicalDiagnosisType_ADD.EdwClaimPartyOccurrenceAkId = v_claim_party_occurrence_ak_id
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_ADD_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalDiagnosisCode_ADD_v_MedicalDiagnosisType_ADD.EdwSupMedicalDiagnosisCodePkId = v_SupMedicalDiagnosisCodeId_ADD
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_ADD_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalDiagnosisCode_ADD_v_MedicalDiagnosisType_ADD.EdwSupMedicalCauseCodePkId = v_SupMedicalCauseCodeId
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_ADD_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalDiagnosisCode_ADD_v_MedicalDiagnosisType_ADD.EdwSupSurgeryTypePkId = v_SupMedicalSurgeryCodeId
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_ADD_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalDiagnosisCode_ADD_v_MedicalDiagnosisType_ADD.MedicalCode = v_MedicalDiagnosisCode_ADD
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_id_v_SupMedicalDiagnosisCodeId_ADD_v_SupMedicalCauseCodeId_v_SupMedicalSurgeryCodeId_v_MedicalDiagnosisCode_ADD_v_MedicalDiagnosisType_ADD.MedicalCodeType = v_MedicalDiagnosisType_ADD

),
RTR_Diag_ADD AS (
	SELECT
	ClaimantMedicalCodeDimId,
	EdwClaimPartyOccurrencePkId,
	EdwClaimPartyOccurrenceAkId,
	claimant_dim_id,
	EdwSupMedicalDiagnosisCodePkId,
	EdwSupMedicalCauseCodePkId,
	EdwSupSurgeryTypePkId,
	MedicalCodeType,
	MedicalCode,
	MedicalCodeDescription,
	EffectiveDate,
	ExpirationDate,
	CurrentSnapshotFlag,
	AuditID,
	CreatedDate,
	ModifiedDate
	FROM EXP_Defualt_Values_DIAG_ADD
),
RTR_Diag_ADD_Insert_Diag_ADD AS (SELECT * FROM RTR_Diag_ADD WHERE isnull(ClaimantMedicalCodeDimId)),
RTR_Diag_ADD_DEFAULT1 AS (SELECT * FROM RTR_Diag_ADD WHERE NOT ( (isnull(ClaimantMedicalCodeDimId)) )),
UPD_Update_DIAG_ADD AS (
	SELECT
	ClaimantMedicalCodeDimId, 
	EdwClaimPartyOccurrencePkId, 
	EdwClaimPartyOccurrenceAkId, 
	claimant_dim_id, 
	EdwSupMedicalDiagnosisCodePkId, 
	EdwSupMedicalCauseCodePkId, 
	EdwSupSurgeryTypePkId, 
	MedicalCodeType, 
	MedicalCode, 
	MedicalCodeDescription, 
	EffectiveDate, 
	ExpirationDate, 
	CurrentSnapshotFlag, 
	AuditID, 
	CreatedDate, 
	ModifiedDate
	FROM RTR_Diag_ADD_DEFAULT1
),
ClaimantMedicalCodeDim_DIAG_ADD_Update AS (
	MERGE INTO ClaimantMedicalCodeDim AS T
	USING UPD_Update_DIAG_ADD AS S
	ON T.ClaimantMedicalCodeDimId = S.ClaimantMedicalCodeDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.AuditID = S.AuditID, T.EffectiveDate = S.EffectiveDate, T.ExpirationDate = S.ExpirationDate, T.CreatedDate = S.CreatedDate, T.ModifiedDate = S.ModifiedDate, T.EdwClaimPartyOccurrencePkId = S.EdwClaimPartyOccurrencePkId, T.EdwClaimPartyOccurrenceAkId = S.EdwClaimPartyOccurrenceAkId, T.claimant_dim_id = S.claimant_dim_id, T.EdwSupMedicalDiagnosisCodePkId = S.EdwSupMedicalDiagnosisCodePkId, T.EdwSupMedicalCauseCodePkId = S.EdwSupMedicalCauseCodePkId, T.EdwSupSurgeryTypePkId = S.EdwSupSurgeryTypePkId, T.MedicalCodeType = S.MedicalCodeType, T.MedicalCode = S.MedicalCode, T.MedicalCodeDescription = S.MedicalCodeDescription
),
UPD_Insert_DIAG_ADD AS (
	SELECT
	EdwClaimPartyOccurrencePkId, 
	EdwClaimPartyOccurrenceAkId, 
	claimant_dim_id, 
	EdwSupMedicalDiagnosisCodePkId, 
	EdwSupMedicalCauseCodePkId, 
	EdwSupSurgeryTypePkId, 
	MedicalCodeType, 
	MedicalCode, 
	MedicalCodeDescription, 
	EffectiveDate, 
	ExpirationDate, 
	CurrentSnapshotFlag, 
	AuditID, 
	CreatedDate, 
	ModifiedDate
	FROM RTR_Diag_ADD_Insert_Diag_ADD
),
ClaimantMedicalCodeDim_DIAG_ADD_Insert AS (
	INSERT INTO ClaimantMedicalCodeDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, EdwClaimPartyOccurrencePkId, EdwClaimPartyOccurrenceAkId, claimant_dim_id, EdwSupMedicalDiagnosisCodePkId, EdwSupMedicalCauseCodePkId, EdwSupSurgeryTypePkId, MedicalCodeType, MedicalCode, MedicalCodeDescription)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	EDWCLAIMPARTYOCCURRENCEPKID, 
	EDWCLAIMPARTYOCCURRENCEAKID, 
	CLAIMANT_DIM_ID, 
	EDWSUPMEDICALDIAGNOSISCODEPKID, 
	EDWSUPMEDICALCAUSECODEPKID, 
	EDWSUPSURGERYTYPEPKID, 
	MEDICALCODETYPE, 
	MEDICALCODE, 
	MEDICALCODEDESCRIPTION
	FROM UPD_Insert_DIAG_ADD
),
SQ_ClaimantSurgeryDetail AS (
	SELECT 
	C.ClaimantSurgeryDetailId, 
	C.EffectiveDate, 
	C.claim_party_occurrence_ak_Id, 
	C.SupSurgeryTypeId, 
	C.clmt_surgery_detail_id 
	
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.ClaimantSurgeryDetail C
	
	where
	C.CurrentSnapshotFlag = 1
	AND
	C.CreatedDate >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_Src_Values_SURGERY AS (
	SELECT
	ClaimantSurgeryDetailId,
	EffectiveDate,
	claim_party_occurrence_ak_Id,
	SupSurgeryTypeId,
	clmt_surgery_detail_id
	FROM SQ_ClaimantSurgeryDetail
),
LKP_SupSurgeryType AS (
	SELECT
	SupSurgeryTypeId,
	SurgeryTypeCode,
	SurgeryTypeDescription
	FROM (
		SELECT 
		S.SurgeryTypeCode as SurgeryTypeCode, 
		S.SurgeryTypeDescription as SurgeryTypeDescription, 
		S.SupSurgeryTypeId as SupSurgeryTypeId 
		
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.SupSurgeryType S
		
		where
		S.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SupSurgeryTypeId ORDER BY SupSurgeryTypeId) = 1
),
EXP_Lkp_Extract_SURGERY AS (
	SELECT
	EXP_Src_Values_SURGERY.claim_party_occurrence_ak_Id AS IN_claim_party_occurrence_ak_Id,
	-- *INF*: iif(isnull(IN_claim_party_occurrence_ak_Id),-1,IN_claim_party_occurrence_ak_Id)
	IFF(IN_claim_party_occurrence_ak_Id IS NULL, - 1, IN_claim_party_occurrence_ak_Id) AS v_claim_party_occurrence_ak_Id,
	v_claim_party_occurrence_ak_Id AS EdwClaimPartyOccurrenceAkId,
	-- *INF*: iif(isnull(:LKP.LKP_CLAIM_PARTY_OCCURRENCE(IN_claim_party_occurrence_ak_Id)),
	-- -1,
	-- :LKP.LKP_CLAIM_PARTY_OCCURRENCE(IN_claim_party_occurrence_ak_Id))
	IFF(LKP_CLAIM_PARTY_OCCURRENCE_IN_claim_party_occurrence_ak_Id.claim_party_occurrence_id IS NULL, - 1, LKP_CLAIM_PARTY_OCCURRENCE_IN_claim_party_occurrence_ak_Id.claim_party_occurrence_id) AS v_claim_party_occurrence_id,
	v_claim_party_occurrence_id AS EdwClaimPartyOccurrencePkId,
	-- *INF*: iif(isnull(:LKP.LKP_CLAIMANT_DIM(IN_claim_party_occurrence_ak_Id))
	-- ,-1,
	-- :LKP.LKP_CLAIMANT_DIM(IN_claim_party_occurrence_ak_Id))
	IFF(LKP_CLAIMANT_DIM_IN_claim_party_occurrence_ak_Id.claimant_dim_id IS NULL, - 1, LKP_CLAIMANT_DIM_IN_claim_party_occurrence_ak_Id.claimant_dim_id) AS v_claimant_dim_id,
	v_claimant_dim_id AS claimant_dim_id,
	-1 AS v_SupMedicalDiagnosisCodeId,
	v_SupMedicalDiagnosisCodeId AS EdwSupMedicalDiagnosisCodePkId,
	-1 AS v_SupMedicalCauseCodeId,
	v_SupMedicalCauseCodeId AS EdwSupMedicalCauseCodePkId,
	LKP_SupSurgeryType.SupSurgeryTypeId AS IN_SupSurgeryTypeId,
	-- *INF*: iif(isnull(IN_SupSurgeryTypeId), -1, IN_SupSurgeryTypeId)
	IFF(IN_SupSurgeryTypeId IS NULL, - 1, IN_SupSurgeryTypeId) AS v_SupSurgeryTypeId,
	v_SupSurgeryTypeId AS EdwSupSurgeryTypePkId,
	LKP_SupSurgeryType.SurgeryTypeCode AS IN_SurgeryTypeCode,
	-- *INF*: iif(isnull(ltrim(rtrim(IN_SurgeryTypeCode))),'N/A',IN_SurgeryTypeCode)
	IFF(ltrim(rtrim(IN_SurgeryTypeCode)) IS NULL, 'N/A', IN_SurgeryTypeCode) AS v_SurgeryTypeCode,
	v_SurgeryTypeCode AS MedicalCode,
	LKP_SupSurgeryType.SurgeryTypeDescription AS IN_SurgeryTypeDescription,
	-- *INF*: iif(isnull(Ltrim(Rtrim(IN_SurgeryTypeDescription))),'N/A',IN_SurgeryTypeDescription)
	IFF(Ltrim(Rtrim(IN_SurgeryTypeDescription)) IS NULL, 'N/A', IN_SurgeryTypeDescription) AS v_SurgeryDescription,
	v_SurgeryDescription AS MedicalCodeDescription,
	'SURGERY' AS v_SurgeryType,
	v_SurgeryType AS MedicalCodeType,
	EXP_Src_Values_SURGERY.EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	sysdate AS CreatedDate,
	sysdate AS ModifiedDate,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	-- *INF*: :LKP.LKP_CLAIMANTMEDICALCODEDIM(v_claim_party_occurrence_ak_Id,v_SupMedicalDiagnosisCodeId,v_SupMedicalCauseCodeId,v_SupSurgeryTypeId,v_SurgeryTypeCode,v_SurgeryType)
	-- 
	LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_Id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupSurgeryTypeId_v_SurgeryTypeCode_v_SurgeryType.ClaimantMedicalCodeDimId AS v_ClaimantMedicalCodeDimId,
	v_ClaimantMedicalCodeDimId AS ClaimantMedicalCodeDimId
	FROM EXP_Src_Values_SURGERY
	LEFT JOIN LKP_SupSurgeryType
	ON LKP_SupSurgeryType.SupSurgeryTypeId = EXP_Src_Values_SURGERY.SupSurgeryTypeId
	LEFT JOIN LKP_CLAIM_PARTY_OCCURRENCE LKP_CLAIM_PARTY_OCCURRENCE_IN_claim_party_occurrence_ak_Id
	ON LKP_CLAIM_PARTY_OCCURRENCE_IN_claim_party_occurrence_ak_Id.claim_party_occurrence_ak_id = IN_claim_party_occurrence_ak_Id

	LEFT JOIN LKP_CLAIMANT_DIM LKP_CLAIMANT_DIM_IN_claim_party_occurrence_ak_Id
	ON LKP_CLAIMANT_DIM_IN_claim_party_occurrence_ak_Id.edw_claim_party_occurrence_ak_id = IN_claim_party_occurrence_ak_Id

	LEFT JOIN LKP_CLAIMANTMEDICALCODEDIM LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_Id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupSurgeryTypeId_v_SurgeryTypeCode_v_SurgeryType
	ON LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_Id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupSurgeryTypeId_v_SurgeryTypeCode_v_SurgeryType.EdwClaimPartyOccurrenceAkId = v_claim_party_occurrence_ak_Id
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_Id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupSurgeryTypeId_v_SurgeryTypeCode_v_SurgeryType.EdwSupMedicalDiagnosisCodePkId = v_SupMedicalDiagnosisCodeId
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_Id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupSurgeryTypeId_v_SurgeryTypeCode_v_SurgeryType.EdwSupMedicalCauseCodePkId = v_SupMedicalCauseCodeId
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_Id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupSurgeryTypeId_v_SurgeryTypeCode_v_SurgeryType.EdwSupSurgeryTypePkId = v_SupSurgeryTypeId
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_Id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupSurgeryTypeId_v_SurgeryTypeCode_v_SurgeryType.MedicalCode = v_SurgeryTypeCode
	AND LKP_CLAIMANTMEDICALCODEDIM_v_claim_party_occurrence_ak_Id_v_SupMedicalDiagnosisCodeId_v_SupMedicalCauseCodeId_v_SupSurgeryTypeId_v_SurgeryTypeCode_v_SurgeryType.MedicalCodeType = v_SurgeryType

),
RTR_Surgery AS (
	SELECT
	ClaimantMedicalCodeDimId,
	CurrentSnapshotFlag,
	AuditId AS AuditID,
	EffectiveDate,
	ExpirationDate,
	CreatedDate,
	ModifiedDate,
	EdwClaimPartyOccurrencePkId,
	EdwClaimPartyOccurrenceAkId,
	claimant_dim_id,
	EdwSupMedicalDiagnosisCodePkId,
	EdwSupMedicalCauseCodePkId,
	EdwSupSurgeryTypePkId,
	MedicalCodeType,
	MedicalCode,
	MedicalCodeDescription
	FROM EXP_Lkp_Extract_SURGERY
),
RTR_Surgery_Insert_Surgery AS (SELECT * FROM RTR_Surgery WHERE isnull(ClaimantMedicalCodeDimId)),
RTR_Surgery_DEFAULT1 AS (SELECT * FROM RTR_Surgery WHERE NOT ( (isnull(ClaimantMedicalCodeDimId)) )),
UPD_Update_SURGERY AS (
	SELECT
	ClaimantMedicalCodeDimId, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModifiedDate, 
	EdwClaimPartyOccurrencePkId, 
	EdwClaimPartyOccurrenceAkId, 
	claimant_dim_id, 
	EdwSupMedicalDiagnosisCodePkId, 
	EdwSupMedicalCauseCodePkId, 
	EdwSupSurgeryTypePkId, 
	MedicalCodeType, 
	MedicalCode, 
	MedicalCodeDescription
	FROM RTR_Surgery_DEFAULT1
),
ClaimantMedicalCodeDim_Update_SURGERY AS (
	MERGE INTO ClaimantMedicalCodeDim AS T
	USING UPD_Update_SURGERY AS S
	ON T.ClaimantMedicalCodeDimId = S.ClaimantMedicalCodeDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.AuditID = S.AuditID, T.EffectiveDate = S.EffectiveDate, T.ExpirationDate = S.ExpirationDate, T.CreatedDate = S.CreatedDate, T.ModifiedDate = S.ModifiedDate, T.EdwClaimPartyOccurrencePkId = S.EdwClaimPartyOccurrencePkId, T.EdwClaimPartyOccurrenceAkId = S.EdwClaimPartyOccurrenceAkId, T.claimant_dim_id = S.claimant_dim_id, T.EdwSupMedicalDiagnosisCodePkId = S.EdwSupMedicalDiagnosisCodePkId, T.EdwSupMedicalCauseCodePkId = S.EdwSupMedicalCauseCodePkId, T.EdwSupSurgeryTypePkId = S.EdwSupSurgeryTypePkId, T.MedicalCodeType = S.MedicalCodeType, T.MedicalCode = S.MedicalCode, T.MedicalCodeDescription = S.MedicalCodeDescription
),
UPD_Insert_SURGERY AS (
	SELECT
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModifiedDate, 
	EdwClaimPartyOccurrencePkId, 
	EdwClaimPartyOccurrenceAkId, 
	claimant_dim_id, 
	EdwSupMedicalDiagnosisCodePkId, 
	EdwSupMedicalCauseCodePkId, 
	EdwSupSurgeryTypePkId, 
	MedicalCodeType, 
	MedicalCode, 
	MedicalCodeDescription
	FROM RTR_Surgery_Insert_Surgery
),
ClaimantMedicalCodeDim_Insert_SURGERY AS (
	INSERT INTO ClaimantMedicalCodeDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, EdwClaimPartyOccurrencePkId, EdwClaimPartyOccurrenceAkId, claimant_dim_id, EdwSupMedicalDiagnosisCodePkId, EdwSupMedicalCauseCodePkId, EdwSupSurgeryTypePkId, MedicalCodeType, MedicalCode, MedicalCodeDescription)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	EDWCLAIMPARTYOCCURRENCEPKID, 
	EDWCLAIMPARTYOCCURRENCEAKID, 
	CLAIMANT_DIM_ID, 
	EDWSUPMEDICALDIAGNOSISCODEPKID, 
	EDWSUPMEDICALCAUSECODEPKID, 
	EDWSUPSURGERYTYPEPKID, 
	MEDICALCODETYPE, 
	MEDICALCODE, 
	MEDICALCODEDESCRIPTION
	FROM UPD_Insert_SURGERY
),
SQ_ClaimantMedicalCodeDim AS (
	SELECT 
	A.ClaimantMedicalCodeDimId, 
	A.EffectiveDate, 
	A.ExpirationDate, 
	A.EdwClaimPartyOccurrenceAkId 
	
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.ClaimantMedicalCodeDim A
	
	where Exists 
	    (
	Select 1 
	from
	@{pipeline().parameters.TARGET_TABLE_OWNER}.ClaimantMedicalCodeDim B
	
	where
	A.EdwClaimPartyOccurrenceAkId = B.EdwClaimPartyOccurrenceAkId 
	AND
	A.MedicalCode = B.MedicalCode
	AND
	A.MedicalCodeType = B.MedicalCodeType
	AND
	B.CurrentSnapshotFlag = 1
	
	Group by
	B.EdwClaimPartyOccurrenceAkId,
	B.MedicalCode,
	B.MedicalCodeType
	
	Having
	Count(*) > 1
	   )
	
	Order by
	A.EdwClaimPartyOccurrenceAkId,
	A.EffectiveDate DESC
),
EXP_Lag_ExpirationDate AS (
	SELECT
	ClaimantMedicalCodeDimId,
	EffectiveDate,
	ExpirationDate AS Orig_ExpirationDate,
	-- *INF*: DECODE(TRUE,
	-- 	EdwClaimPartyOccurrenceAkId= v_PREV_ROW_EdwClaimPartyOccurrenceAkId, ADD_TO_DATE(v_PREV_ROW_EffectiveDate,'SS',-1),
	-- 	Orig_ExpirationDate)
	DECODE(TRUE,
		EdwClaimPartyOccurrenceAkId = v_PREV_ROW_EdwClaimPartyOccurrenceAkId, ADD_TO_DATE(v_PREV_ROW_EffectiveDate, 'SS', - 1),
		Orig_ExpirationDate) AS v_ExpirationDate,
	v_ExpirationDate AS ExpirationDate,
	EdwClaimPartyOccurrenceAkId,
	EdwClaimPartyOccurrenceAkId AS v_PREV_ROW_EdwClaimPartyOccurrenceAkId,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	0 AS CurrentSnapshotFlag,
	sysdate AS ModifiedDate
	FROM SQ_ClaimantMedicalCodeDim
),
FIL_ExpirationDate AS (
	SELECT
	ClaimantMedicalCodeDimId, 
	ExpirationDate, 
	CurrentSnapshotFlag, 
	ModifiedDate, 
	Orig_ExpirationDate
	FROM EXP_Lag_ExpirationDate
	WHERE ExpirationDate <> Orig_ExpirationDate
),
UPD_ExpirationDate AS (
	SELECT
	ClaimantMedicalCodeDimId, 
	ExpirationDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM FIL_ExpirationDate
),
ClaimantMedicalCodeDim_Expire AS (
	MERGE INTO ClaimantMedicalCodeDim AS T
	USING UPD_ExpirationDate AS S
	ON T.ClaimantMedicalCodeDimId = S.ClaimantMedicalCodeDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),