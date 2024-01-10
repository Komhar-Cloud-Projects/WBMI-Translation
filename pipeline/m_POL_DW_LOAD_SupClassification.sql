WITH
SQ_CSV_SupClassification AS (

-- TODO Manual --

),
AGG_RemoveDuplicate AS (
	SELECT
	LineOfBusinessAbbreviation,
	StateCode,
	OriginatingOrganizationCode,
	ClassCode,
	ClassDescription,
	ClassGroupDescription,
	RatingBasis
	FROM SQ_CSV_SupClassification
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LineOfBusinessAbbreviation, StateCode, OriginatingOrganizationCode, ClassCode, ClassDescription, ClassGroupDescription, RatingBasis ORDER BY NULL) = 1
),
EXP_DefaultValues AS (
	SELECT
	LineOfBusinessAbbreviation AS i_LineOfBusinessAbbreviation,
	StateCode AS i_StateCode,
	OriginatingOrganizationCode AS i_OriginatingOrganizationCode,
	ClassCode AS i_ClassCode,
	ClassDescription AS i_ClassDescription,
	ClassGroupDescription AS i_ClassGroupDescription,
	RatingBasis AS i_RatingBasis,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_LineOfBusinessAbbreviation)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_LineOfBusinessAbbreviation
	) AS o_LineOfBusinessAbbreviation,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_StateCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_StateCode
	) AS o_StateCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_OriginatingOrganizationCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_OriginatingOrganizationCode
	) AS o_OriginatingOrganizationCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClassCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClassCode
	) AS o_ClassCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClassDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClassDescription
	) AS o_ClassDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_RatingBasis)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_RatingBasis
	) AS o_RatingBasis,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClassGroupDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_ClassGroupDescription
	) AS o_ClassGroupDescription
	FROM AGG_RemoveDuplicate
),
LKP_SupClassification AS (
	SELECT
	SupClassificationId,
	OriginatingOrganizationCode,
	ClassDescription,
	RatingBasis,
	ClassGroupDescription,
	ClassCode,
	StateAbbreviation,
	LineOfBusinessAbbreviation
	FROM (
		SELECT 
			SupClassificationId,
			OriginatingOrganizationCode,
			ClassDescription,
			RatingBasis,
			ClassGroupDescription,
			ClassCode,
			StateAbbreviation,
			LineOfBusinessAbbreviation
		FROM SupClassification
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode,StateAbbreviation,LineOfBusinessAbbreviation ORDER BY SupClassificationId) = 1
),
EXP_UpdateOrInsert AS (
	SELECT
	LKP_SupClassification.SupClassificationId AS lkp_SupClassificationId,
	LKP_SupClassification.OriginatingOrganizationCode AS lkp_OriginatingOrganizationCode,
	LKP_SupClassification.ClassDescription AS lkp_ClassDescription,
	LKP_SupClassification.RatingBasis AS lkp_RatingBasis,
	LKP_SupClassification.ClassGroupDescription AS lkp_ClassGroupDescription,
	EXP_DefaultValues.o_LineOfBusinessAbbreviation AS LineOfBusinessAbbreviation,
	EXP_DefaultValues.o_StateCode AS StateCode,
	EXP_DefaultValues.o_OriginatingOrganizationCode AS OriginatingOrganizationCode,
	EXP_DefaultValues.o_ClassCode AS ClassCode,
	EXP_DefaultValues.o_ClassDescription AS ClassDescription,
	EXP_DefaultValues.o_RatingBasis AS RatingBasis,
	EXP_DefaultValues.o_ClassGroupDescription AS ClassGroupDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(lkp_RatingBasis)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(lkp_RatingBasis
	) AS v_lkp_RatingBasis,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(lkp_ClassGroupDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(lkp_ClassGroupDescription
	) AS v_lkp_ClassGroupDescription,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_SupClassificationId),'INSERT',
	-- lkp_OriginatingOrganizationCode != OriginatingOrganizationCode 
	-- OR lkp_ClassDescription != ClassDescription 
	-- OR v_lkp_RatingBasis != RatingBasis 
	-- OR v_lkp_ClassGroupDescription != ClassGroupDescription,'UPDATE',
	-- 'IGNORE'
	-- )
	DECODE(TRUE,
		lkp_SupClassificationId IS NULL, 'INSERT',
		lkp_OriginatingOrganizationCode != OriginatingOrganizationCode 
		OR lkp_ClassDescription != ClassDescription 
		OR v_lkp_RatingBasis != RatingBasis 
		OR v_lkp_ClassGroupDescription != ClassGroupDescription, 'UPDATE',
		'IGNORE'
	) AS o_ChangeFlag
	FROM EXP_DefaultValues
	LEFT JOIN LKP_SupClassification
	ON LKP_SupClassification.ClassCode = EXP_DefaultValues.o_ClassCode AND LKP_SupClassification.StateAbbreviation = EXP_DefaultValues.o_StateCode AND LKP_SupClassification.LineOfBusinessAbbreviation = EXP_DefaultValues.o_LineOfBusinessAbbreviation
),
RTR_InsertOrUpdate AS (
	SELECT
	lkp_SupClassificationId,
	LineOfBusinessAbbreviation,
	StateCode,
	OriginatingOrganizationCode,
	ClassCode,
	ClassDescription,
	RatingBasis,
	ClassGroupDescription,
	o_AuditId AS AuditId,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_ChangeFlag AS ChangeFlag
	FROM EXP_UpdateOrInsert
),
RTR_InsertOrUpdate_INSERT AS (SELECT * FROM RTR_InsertOrUpdate WHERE ChangeFlag='INSERT'),
RTR_InsertOrUpdate_UPDATE AS (SELECT * FROM RTR_InsertOrUpdate WHERE ChangeFlag='UPDATE'),
TGT_SupClassification_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassification
	(AuditId, CreatedDate, ModifiedDate, LineOfBusinessAbbreviation, StateAbbreviation, OriginatingOrganizationCode, ClassCode, ClassDescription, RatingBasis, ClassGroupDescription)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	LINEOFBUSINESSABBREVIATION, 
	StateCode AS STATEABBREVIATION, 
	ORIGINATINGORGANIZATIONCODE, 
	CLASSCODE, 
	CLASSDESCRIPTION, 
	RATINGBASIS, 
	CLASSGROUPDESCRIPTION
	FROM RTR_InsertOrUpdate_INSERT
),
UPD_SupClassification AS (
	SELECT
	lkp_SupClassificationId AS SupClassificationId, 
	LineOfBusinessAbbreviation, 
	StateCode, 
	OriginatingOrganizationCode, 
	ClassCode, 
	ClassDescription, 
	RatingBasis, 
	ClassGroupDescription, 
	AuditId, 
	CreatedDate, 
	ModifiedDate
	FROM RTR_InsertOrUpdate_UPDATE
),
TGT_SupClassification_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassification AS T
	USING UPD_SupClassification AS S
	ON T.SupClassificationId = S.SupClassificationId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.LineOfBusinessAbbreviation = S.LineOfBusinessAbbreviation, T.StateAbbreviation = S.StateCode, T.OriginatingOrganizationCode = S.OriginatingOrganizationCode, T.ClassCode = S.ClassCode, T.ClassDescription = S.ClassDescription, T.RatingBasis = S.RatingBasis, T.ClassGroupDescription = S.ClassGroupDescription
),