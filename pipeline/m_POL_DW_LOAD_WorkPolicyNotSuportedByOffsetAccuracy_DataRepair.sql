WITH
SQ_policy AS (
	select distinct trk.PolicyKey as PolicyKey
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPremiumTransactionTracking trk
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
	on trk.PremiumTransactionID = PT.PremiumTransactionID
	 AND pt.NegateRestateCode <> 'N/A'
	@{pipeline().parameters.INCREMENTALCOVERAGEFILTER}
),
EXP_Pass_Value AS (
	SELECT
	PolicyKey
	FROM SQ_policy
),
mplt_Exclusivereason_WorkPolicyNotSuportedByOffsetAccuracy AS (WITH
	Input AS (
		
	),
	Lkp_WorkPolicyNotSuportedByOffsetAccuracy AS (
		SELECT
		WorkPolicyNotSuportedByOffsetAccuracyId,
		PolicyKey,
		ExclusionReason,
		i_Policykey
		FROM (
			SELECT 
				WorkPolicyNotSuportedByOffsetAccuracyId,
				PolicyKey,
				ExclusionReason,
				i_Policykey
			FROM WorkPolicyNotSuportedByOffsetAccuracy
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY WorkPolicyNotSuportedByOffsetAccuracyId) = 1
	),
	EXP_ExclusionReason AS (
		SELECT
		WorkPolicyNotSuportedByOffsetAccuracyId,
		ExclusionReason AS lkp_ExclusionReason,
		-- *INF*: DECODE(TRUE,
		-- lkp_ExclusionReason = 'Data Repair', lkp_ExclusionReason,
		-- ISNULL(lkp_ExclusionReason) OR lkp_ExclusionReason = '','Data Repair',
		-- NOT ISNULL(lkp_ExclusionReason) AND INSTR(lkp_ExclusionReason,'Data Repair') = 0,
		-- CONCAT(lkp_ExclusionReason,', Data Repair'),
		-- lkp_ExclusionReason)
		DECODE(TRUE,
		lkp_ExclusionReason = 'Data Repair', lkp_ExclusionReason,
		lkp_ExclusionReason IS NULL OR lkp_ExclusionReason = '', 'Data Repair',
		NOT lkp_ExclusionReason IS NULL AND INSTR(lkp_ExclusionReason, 'Data Repair') = 0, CONCAT(lkp_ExclusionReason, ', Data Repair'),
		lkp_ExclusionReason) AS v_DataRepair,
		-- *INF*: DECODE(TRUE,
		-- lkp_ExclusionReason = 'Missing/blank WorkDCTPremiumTransactionTracking', lkp_ExclusionReason,
		-- ISNULL(lkp_ExclusionReason) OR lkp_ExclusionReason = '','Missing/blank WorkDCTPremiumTransactionTracking',
		-- NOT ISNULL(lkp_ExclusionReason) AND INSTR(lkp_ExclusionReason,'Missing/blank WorkDCTPremiumTransactionTracking') = 0,
		-- CONCAT(lkp_ExclusionReason,', Missing/blank WorkDCTPremiumTransactionTracking'),
		-- lkp_ExclusionReason)
		DECODE(TRUE,
		lkp_ExclusionReason = 'Missing/blank WorkDCTPremiumTransactionTracking', lkp_ExclusionReason,
		lkp_ExclusionReason IS NULL OR lkp_ExclusionReason = '', 'Missing/blank WorkDCTPremiumTransactionTracking',
		NOT lkp_ExclusionReason IS NULL AND INSTR(lkp_ExclusionReason, 'Missing/blank WorkDCTPremiumTransactionTracking') = 0, CONCAT(lkp_ExclusionReason, ', Missing/blank WorkDCTPremiumTransactionTracking'),
		lkp_ExclusionReason) AS v_MissingTre,
		-- *INF*: DECODE(TRUE,
		-- lkp_ExclusionReason = 'CoverageId changed on PT', lkp_ExclusionReason,
		-- ISNULL(lkp_ExclusionReason) OR lkp_ExclusionReason = '','CoverageId changed on PT',
		-- NOT ISNULL(lkp_ExclusionReason) AND INSTR(lkp_ExclusionReason,'CoverageId changed on PT') = 0,
		-- CONCAT(lkp_ExclusionReason,', CoverageId changed on PT'),
		-- lkp_ExclusionReason)
		DECODE(TRUE,
		lkp_ExclusionReason = 'CoverageId changed on PT', lkp_ExclusionReason,
		lkp_ExclusionReason IS NULL OR lkp_ExclusionReason = '', 'CoverageId changed on PT',
		NOT lkp_ExclusionReason IS NULL AND INSTR(lkp_ExclusionReason, 'CoverageId changed on PT') = 0, CONCAT(lkp_ExclusionReason, ', CoverageId changed on PT'),
		lkp_ExclusionReason) AS v_coidchange,
		v_DataRepair AS o_DataRepair,
		v_MissingTre AS o_MissingTre,
		v_coidchange AS o_covidchange
		FROM Lkp_WorkPolicyNotSuportedByOffsetAccuracy
	),
	Output AS (
		SELECT
		WorkPolicyNotSuportedByOffsetAccuracyId, 
		o_DataRepair AS ExclusionReason_DataRepair, 
		o_MissingTre AS ExclusionReason_MissingTre, 
		o_covidchange AS ExclusionReason_covidchange
		FROM EXP_ExclusionReason
	),
),
EXP_Get_Values AS (
	SELECT
	EXP_Pass_Value.PolicyKey AS i_PolicyKey,
	mplt_Exclusivereason_WorkPolicyNotSuportedByOffsetAccuracy.WorkPolicyNotSuportedByOffsetAccuracyId AS i_WorkPolicyNotSuportedByOffsetAccuracyId,
	mplt_Exclusivereason_WorkPolicyNotSuportedByOffsetAccuracy.ExclusionReason_DataRepair AS i_ExclusionReason,
	-- *INF*: DECODE(TRUE,
	-- i_ExclusionReason = 'Data Repair', i_ExclusionReason,
	-- ISNULL(i_ExclusionReason) OR i_ExclusionReason = '','Data Repair',
	-- NOT ISNULL(i_ExclusionReason) AND INSTR(i_ExclusionReason,'Data Repair') = 0,
	-- CONCAT(i_ExclusionReason,', Data Repair'),
	-- i_ExclusionReason)
	DECODE(TRUE,
	i_ExclusionReason = 'Data Repair', i_ExclusionReason,
	i_ExclusionReason IS NULL OR i_ExclusionReason = '', 'Data Repair',
	NOT i_ExclusionReason IS NULL AND INSTR(i_ExclusionReason, 'Data Repair') = 0, CONCAT(i_ExclusionReason, ', Data Repair'),
	i_ExclusionReason) AS v_ExclusionReason,
	-- *INF*: IIF(ISNULL(i_WorkPolicyNotSuportedByOffsetAccuracyId),'INSERT','UPDATE')
	IFF(i_WorkPolicyNotSuportedByOffsetAccuracyId IS NULL, 'INSERT', 'UPDATE') AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag,
	i_WorkPolicyNotSuportedByOffsetAccuracyId AS o_WorkPolicyNotSuportedByOffsetAccuracyId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	'DCT' AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_PolicyKey AS o_PolicyKey,
	i_ExclusionReason AS o_ExclusionReason
	FROM EXP_Pass_Value
	 -- Manually join with mplt_Exclusivereason_WorkPolicyNotSuportedByOffsetAccuracy
),
RTR_Insert_Update AS (
	SELECT
	o_ChangeFlag AS ChangeFlag,
	o_WorkPolicyNotSuportedByOffsetAccuracyId AS WorkPolicyNotSuportedByOffsetAccuracyId,
	o_AuditID AS AuditID,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_PolicyKey AS PolicyKey,
	o_ExclusionReason AS ExclusionReason
	FROM EXP_Get_Values
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag = 'INSERT'),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag = 'UPDATE'),
WorkPolicyNotSuportedByOffsetAccuracy_Insert AS (
	INSERT INTO WorkPolicyNotSuportedByOffsetAccuracy
	(AuditID, SourceSystemID, CreatedDate, ModifiedDate, PolicyKey, ExclusionReason)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	POLICYKEY, 
	EXCLUSIONREASON
	FROM RTR_Insert_Update_INSERT
),
UPD_WorkPolicyNotSuportedByOffsetAccuracy AS (
	SELECT
	WorkPolicyNotSuportedByOffsetAccuracyId AS WorkPolicyNotSuportedByOffsetAccuracyId3, 
	ModifiedDate AS ModifiedDate3, 
	ExclusionReason AS ExclusionReason3
	FROM RTR_Insert_Update_UPDATE
),
WorkPolicyNotSuportedByOffsetAccuracy_Update AS (
	MERGE INTO WorkPolicyNotSuportedByOffsetAccuracy AS T
	USING UPD_WorkPolicyNotSuportedByOffsetAccuracy AS S
	ON T.WorkPolicyNotSuportedByOffsetAccuracyId = S.WorkPolicyNotSuportedByOffsetAccuracyId3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate3, T.ExclusionReason = S.ExclusionReason3
),