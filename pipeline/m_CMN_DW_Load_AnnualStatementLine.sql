WITH
SQ_AnnualStatementLine AS (
	SELECT
		AnnualStatementLineId,
		EffectiveDate,
		ExpirationDate,
		SchedulePNumber,
		SchedulePName,
		AnnualStatementLineNumber,
		AnnualStatementLineNumberDescription,
		AnnualStatementLineCode,
		AnnualStatementLineCodeDescription,
		SubAnnualStatementLineNumber,
		SubAnnualStatementLineNumberDescription,
		SubAnnualStatementLineCode,
		SubAnnualStatementLineCodeDescription,
		SubNonAnnualStatementLineCode,
		SubNonAnnualStatementLineCodeDescription
	FROM AnnualStatementLine
),
EXP_Trim_Values AS (
	SELECT
	AnnualStatementLineId,
	EffectiveDate,
	ExpirationDate,
	SchedulePNumber,
	SchedulePName,
	AnnualStatementLineNumber,
	AnnualStatementLineNumberDescription,
	AnnualStatementLineCode,
	AnnualStatementLineCodeDescription,
	SubAnnualStatementLineNumber,
	SubAnnualStatementLineNumberDescription,
	SubAnnualStatementLineCode,
	SubAnnualStatementLineCodeDescription,
	SubNonAnnualStatementLineCode,
	SubNonAnnualStatementLineCodeDescription,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	CURRENT_TIMESTAMP AS o_CreatedDate,
	CURRENT_TIMESTAMP AS o_ModifiedDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_AnnualStatementLine
),
LKP_AnnualStatementLine AS (
	SELECT
	AnnualStatementLineId
	FROM (
		SELECT 
			AnnualStatementLineId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.AnnualStatementLine
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AnnualStatementLineId ORDER BY AnnualStatementLineId) = 1
),
RTR_Insert_Update AS (
	SELECT
	LKP_AnnualStatementLine.AnnualStatementLineId AS lkp_AnnualStatementLineId,
	EXP_Trim_Values.AnnualStatementLineId,
	EXP_Trim_Values.EffectiveDate AS EffectiveFromDate,
	EXP_Trim_Values.ExpirationDate AS EffectiveToDate,
	EXP_Trim_Values.SchedulePNumber,
	EXP_Trim_Values.SchedulePName,
	EXP_Trim_Values.AnnualStatementLineNumber,
	EXP_Trim_Values.AnnualStatementLineNumberDescription,
	EXP_Trim_Values.AnnualStatementLineCode,
	EXP_Trim_Values.AnnualStatementLineCodeDescription,
	EXP_Trim_Values.SubAnnualStatementLineNumber,
	EXP_Trim_Values.SubAnnualStatementLineNumberDescription,
	EXP_Trim_Values.SubAnnualStatementLineCode,
	EXP_Trim_Values.SubAnnualStatementLineCodeDescription,
	EXP_Trim_Values.SubNonAnnualStatementLineCode,
	EXP_Trim_Values.SubNonAnnualStatementLineCodeDescription,
	EXP_Trim_Values.o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	EXP_Trim_Values.o_AuditId AS AuditId,
	EXP_Trim_Values.o_CreatedDate AS CreatedDate,
	EXP_Trim_Values.o_ModifiedDate AS ModifiedDate,
	EXP_Trim_Values.o_SourceSystemId AS SourceSystemId
	FROM EXP_Trim_Values
	LEFT JOIN LKP_AnnualStatementLine
	ON LKP_AnnualStatementLine.AnnualStatementLineId = EXP_Trim_Values.AnnualStatementLineId
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE ISNULL(lkp_AnnualStatementLineId)),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE NOT ISNULL(lkp_AnnualStatementLineId)),
TGT_AnnualStatementLine_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AnnualStatementLine
	(AnnualStatementLineId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, SchedulePNumber, SchedulePName, AnnualStatementLineNumber, AnnualStatementLineNumberDescription, AnnualStatementLineCode, AnnualStatementLineCodeDescription, SubAnnualStatementLineNumber, SubAnnualStatementLineNumberDescription, SubAnnualStatementLineCode, SubAnnualStatementLineCodeDescription, SubNonAnnualStatementLineCode, SubNonAnnualStatementLineCodeDescription)
	SELECT 
	ANNUALSTATEMENTLINEID, 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EffectiveFromDate AS EFFECTIVEDATE, 
	EffectiveToDate AS EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	SCHEDULEPNUMBER, 
	SCHEDULEPNAME, 
	ANNUALSTATEMENTLINENUMBER, 
	ANNUALSTATEMENTLINENUMBERDESCRIPTION, 
	ANNUALSTATEMENTLINECODE, 
	ANNUALSTATEMENTLINECODEDESCRIPTION, 
	SUBANNUALSTATEMENTLINENUMBER, 
	SUBANNUALSTATEMENTLINENUMBERDESCRIPTION, 
	SUBANNUALSTATEMENTLINECODE, 
	SUBANNUALSTATEMENTLINECODEDESCRIPTION, 
	SUBNONANNUALSTATEMENTLINECODE, 
	SUBNONANNUALSTATEMENTLINECODEDESCRIPTION
	FROM RTR_Insert_Update_INSERT
),
UPD_AnnualStatementLine AS (
	SELECT
	AnnualStatementLineId, 
	AuditId, 
	EffectiveFromDate, 
	EffectiveToDate, 
	ModifiedDate, 
	SchedulePNumber, 
	SchedulePName, 
	AnnualStatementLineNumber, 
	AnnualStatementLineNumberDescription, 
	AnnualStatementLineCode, 
	AnnualStatementLineCodeDescription, 
	SubAnnualStatementLineNumber, 
	SubAnnualStatementLineNumberDescription, 
	SubAnnualStatementLineCode, 
	SubAnnualStatementLineCodeDescription, 
	SubNonAnnualStatementLineCode, 
	SubNonAnnualStatementLineCodeDescription, 
	SourceSystemId
	FROM RTR_Insert_Update_UPDATE
),
TGT_AnnualStatementLine_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AnnualStatementLine AS T
	USING UPD_AnnualStatementLine AS S
	ON T.AnnualStatementLineId = S.AnnualStatementLineId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId, T.EffectiveDate = S.EffectiveFromDate, T.ExpirationDate = S.EffectiveToDate, T.SourceSystemId = S.SourceSystemId, T.ModifiedDate = S.ModifiedDate, T.SchedulePNumber = S.SchedulePNumber, T.SchedulePName = S.SchedulePName, T.AnnualStatementLineNumber = S.AnnualStatementLineNumber, T.AnnualStatementLineNumberDescription = S.AnnualStatementLineNumberDescription, T.AnnualStatementLineCode = S.AnnualStatementLineCode, T.AnnualStatementLineCodeDescription = S.AnnualStatementLineCodeDescription, T.SubAnnualStatementLineNumber = S.SubAnnualStatementLineNumber, T.SubAnnualStatementLineNumberDescription = S.SubAnnualStatementLineNumberDescription, T.SubAnnualStatementLineCode = S.SubAnnualStatementLineCode, T.SubAnnualStatementLineCodeDescription = S.SubAnnualStatementLineCodeDescription, T.SubNonAnnualStatementLineCode = S.SubNonAnnualStatementLineCode, T.SubNonAnnualStatementLineCodeDescription = S.SubNonAnnualStatementLineCodeDescription
),