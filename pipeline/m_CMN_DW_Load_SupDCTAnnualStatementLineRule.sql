WITH
SQ_SupDCTAnnualStatementLineRules AS (
	SELECT
		SupDctAnnualStatementLineRulesId,
		ModifiedUserId,
		ModifiedDate,
		SystemCoverageId,
		AnnualStatementLineId
	FROM SupDCTAnnualStatementLineRules
),
EXP_Set_Metadata AS (
	SELECT
	SupDctAnnualStatementLineRulesId,
	SystemCoverageId,
	AnnualStatementLineId,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	-- *INF*: TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
	TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
	TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CURRENT_TIMESTAMP AS o_CreatedDate,
	CURRENT_TIMESTAMP AS o_ModifiedDate
	FROM SQ_SupDCTAnnualStatementLineRules
),
LKP_SupDCTAnnualStatementLineRule AS (
	SELECT
	SupDCTAnnualStatementLineRuleId
	FROM (
		SELECT 
			SupDCTAnnualStatementLineRuleId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDCTAnnualStatementLineRule
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SupDCTAnnualStatementLineRuleId ORDER BY SupDCTAnnualStatementLineRuleId) = 1
),
RTR_Insert_Update AS (
	SELECT
	LKP_SupDCTAnnualStatementLineRule.SupDCTAnnualStatementLineRuleId AS lkp_SupDCTAnnualStatementLineRuleId,
	EXP_Set_Metadata.SupDctAnnualStatementLineRulesId,
	EXP_Set_Metadata.o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	EXP_Set_Metadata.o_AuditId AS AuditId,
	EXP_Set_Metadata.o_EffectiveDate AS EffectiveDate,
	EXP_Set_Metadata.o_ExpirationDate AS ExpirationDate,
	EXP_Set_Metadata.o_SourceSystemId AS SourceSystemId,
	EXP_Set_Metadata.o_CreatedDate AS CreatedDate,
	EXP_Set_Metadata.o_ModifiedDate AS ModifiedDate,
	EXP_Set_Metadata.SystemCoverageId,
	EXP_Set_Metadata.AnnualStatementLineId
	FROM EXP_Set_Metadata
	LEFT JOIN LKP_SupDCTAnnualStatementLineRule
	ON LKP_SupDCTAnnualStatementLineRule.SupDCTAnnualStatementLineRuleId = EXP_Set_Metadata.SupDctAnnualStatementLineRulesId
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE ISNULL(lkp_SupDCTAnnualStatementLineRuleId)),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE NOT ISNULL(lkp_SupDCTAnnualStatementLineRuleId)),
SupDCTAnnualStatementLineRule_INSERT AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDCTAnnualStatementLineRule;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDCTAnnualStatementLineRule
	(SupDCTAnnualStatementLineRuleId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, SystemCoverageId, AnnualStatementLineId)
	SELECT 
	SupDctAnnualStatementLineRulesId AS SUPDCTANNUALSTATEMENTLINERULEID, 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	SYSTEMCOVERAGEID, 
	ANNUALSTATEMENTLINEID
	FROM RTR_Insert_Update_INSERT
),
UPD_SupDCTAnnualStatementLineRule AS (
	SELECT
	SupDctAnnualStatementLineRulesId AS SupDCTAnnualStatementLineRuleId, 
	AuditId, 
	ModifiedDate, 
	SystemCoverageId, 
	AnnualStatementLineId
	FROM RTR_Insert_Update_UPDATE
),
SupDCTAnnualStatementLineRule_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDCTAnnualStatementLineRule AS T
	USING UPD_SupDCTAnnualStatementLineRule AS S
	ON T.SupDCTAnnualStatementLineRuleId = S.SupDCTAnnualStatementLineRuleId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId, T.ModifiedDate = S.ModifiedDate, T.SystemCoverageId = S.SystemCoverageId, T.AnnualStatementLineId = S.AnnualStatementLineId
),