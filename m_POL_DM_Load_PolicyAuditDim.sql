WITH
SQ_PolicyAudit AS (
	SELECT
		CurrentSnapshotFlag,
		EffectiveDate,
		ExpirationDate,
		PolicyAuditAKId,
		AssignedAuditor,
		AuditFrequency,
		AuditType,
		AuditContactCity,
		AuditContactStateAbbreviation,
		PermanentOverrideFlag,
		PolicyPeriodOverrideFlag,
		FrontingPolicyFlag,
		AuditCloseOutFlag,
		AuditStatus,
		AuditableFlag,
		AssignedAuditorOveride AS AssignedAuditorOverideFlag,
		AuditTypeOverride AS AuditTypeOverrideFlag,
		AuditablePremium,
		IsAuditableFlag,
		NoncomplianceofWCPoolAudit
	FROM PolicyAudit
	WHERE PolicyAudit.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_PassThrough AS (
	SELECT
	CurrentSnapshotFlag AS i_CurrentSnapshotFlag,
	-- *INF*: DECODE(i_CurrentSnapshotFlag,'T','1','F','0','0')
	DECODE(i_CurrentSnapshotFlag,
	'T', '1',
	'F', '0',
	'0') AS o_CurrentSnapshotFlag,
	EffectiveDate,
	ExpirationDate,
	PolicyAuditAKId,
	AssignedAuditor,
	AuditFrequency,
	AuditType,
	AuditContactCity,
	AuditContactStateAbbreviation,
	PermanentOverrideFlag AS i_PermanentOverrideFlag,
	-- *INF*: DECODE(i_PermanentOverrideFlag,'T','1','F','0','0')
	DECODE(i_PermanentOverrideFlag,
	'T', '1',
	'F', '0',
	'0') AS o_PermanentOverrideFlag,
	PolicyPeriodOverrideFlag AS i_PolicyPeriodOverrideFlag,
	-- *INF*: DECODE(i_PolicyPeriodOverrideFlag,'T','1','F','0','0')
	DECODE(i_PolicyPeriodOverrideFlag,
	'T', '1',
	'F', '0',
	'0') AS o_PolicyPeriodOverrideFlag,
	FrontingPolicyFlag AS i_FrontingPolicyFlag,
	-- *INF*: DECODE(i_FrontingPolicyFlag,'T','1','F','0','0')
	DECODE(i_FrontingPolicyFlag,
	'T', '1',
	'F', '0',
	'0') AS o_FrontingPolicyFlag,
	AuditCloseOutFlag AS i_AuditCloseOutFlag,
	-- *INF*: DECODE(i_AuditCloseOutFlag,'T','1','F','0','0')
	DECODE(i_AuditCloseOutFlag,
	'T', '1',
	'F', '0',
	'0') AS o_AuditCloseOutFlag,
	AuditStatus,
	AuditableFlag AS i_AuditableFlag,
	-- *INF*: DECODE(i_AuditableFlag,'T','1','F','0','0')
	DECODE(i_AuditableFlag,
	'T', '1',
	'F', '0',
	'0') AS o_AuditableFlag,
	AssignedAuditorOverideFlag AS i_AssignedAuditorOverideFlag,
	-- *INF*: i_AssignedAuditorOverideFlag
	-- --DECODE(i_AssignedAuditorOverideFlag,'T','1','F','0','0')
	i_AssignedAuditorOverideFlag AS o_AssignedAuditorOverideFlag,
	AuditTypeOverrideFlag AS i_AuditTypeOverrideFlag,
	-- *INF*: i_AuditTypeOverrideFlag
	-- --DECODE(i_AuditTypeOverrideFlag,'T','1','F','0','0')
	i_AuditTypeOverrideFlag AS o_AuditTypeOverrideFlag,
	AuditablePremium,
	IsAuditableFlag AS i_IsAuditableFlag,
	-- *INF*: DECODE(i_IsAuditableFlag,'T','1','F','0','0')
	DECODE(i_IsAuditableFlag,
	'T', '1',
	'F', '0',
	'0') AS o_IsAuditableFlag,
	NoncomplianceofWCPoolAudit AS i_NoncomplianceofWCPoolAudit,
	-- *INF*: DECODE(i_NoncomplianceofWCPoolAudit,'T','1','F','0','0')
	DECODE(i_NoncomplianceofWCPoolAudit,
	'T', '1',
	'F', '0',
	'0') AS o_NoncomplianceofWCPoolAudit
	FROM SQ_PolicyAudit
),
LKP_PolicyAuditDim AS (
	SELECT
	PolicyAuditDimId,
	EDWPolicyAuditAKId,
	EffectiveDate
	FROM (
		SELECT 
			PolicyAuditDimId,
			EDWPolicyAuditAKId,
			EffectiveDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyAuditDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPolicyAuditAKId,EffectiveDate ORDER BY PolicyAuditDimId) = 1
),
LKP_sup_state AS (
	SELECT
	state_descript,
	state_code
	FROM (
		SELECT 
			state_descript,
			state_code
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_state
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY state_descript) = 1
),
EXPTRANS AS (
	SELECT
	LKP_PolicyAuditDim.PolicyAuditDimId,
	-- *INF*: IIF(ISNULL(PolicyAuditDimId),1,0)
	IFF(PolicyAuditDimId IS NULL, 1, 0) AS v_ChangeFlag,
	EXP_PassThrough.o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	EXP_PassThrough.EffectiveDate,
	EXP_PassThrough.ExpirationDate,
	EXP_PassThrough.PolicyAuditAKId AS EDWPolicyAuditAKId,
	EXP_PassThrough.AssignedAuditor,
	EXP_PassThrough.AuditFrequency,
	EXP_PassThrough.AuditType,
	EXP_PassThrough.AuditContactCity,
	EXP_PassThrough.AuditContactStateAbbreviation,
	LKP_sup_state.state_descript AS i_AuditContactStateDescription,
	-- *INF*: IIF(NOT ISNULL(i_AuditContactStateDescription),i_AuditContactStateDescription,'N/A')
	IFF(NOT i_AuditContactStateDescription IS NULL, i_AuditContactStateDescription, 'N/A') AS o_AuditContactStateDescription,
	EXP_PassThrough.o_PermanentOverrideFlag AS PermanentOverrideFlag,
	EXP_PassThrough.o_PolicyPeriodOverrideFlag AS PolicyPeriodOverrideFlag,
	EXP_PassThrough.o_FrontingPolicyFlag AS FrontingPolicyFlag,
	EXP_PassThrough.o_AuditCloseOutFlag AS AuditCloseOutFlag,
	EXP_PassThrough.AuditStatus,
	EXP_PassThrough.o_AuditableFlag AS AuditableFlag,
	EXP_PassThrough.o_AssignedAuditorOverideFlag AS AssignedAuditorOverideFlag,
	EXP_PassThrough.o_AuditTypeOverrideFlag AS AuditTypeOverrideFlag,
	EXP_PassThrough.AuditablePremium,
	EXP_PassThrough.o_IsAuditableFlag AS IsAuditableFlag,
	EXP_PassThrough.o_NoncomplianceofWCPoolAudit,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	v_ChangeFlag AS o_ChangeFlag
	FROM EXP_PassThrough
	LEFT JOIN LKP_PolicyAuditDim
	ON LKP_PolicyAuditDim.EDWPolicyAuditAKId = EXP_PassThrough.PolicyAuditAKId AND LKP_PolicyAuditDim.EffectiveDate = EXP_PassThrough.EffectiveDate
	LEFT JOIN LKP_sup_state
	ON LKP_sup_state.state_code = EXP_PassThrough.AuditContactStateAbbreviation
),
RTR_Insert AS (
	SELECT
	PolicyAuditDimId,
	CurrentSnapshotFlag,
	o_AuditId AS AuditID,
	EffectiveDate,
	ExpirationDate,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	EDWPolicyAuditAKId,
	AssignedAuditor,
	AuditFrequency,
	AuditType,
	AuditContactCity,
	AuditContactStateAbbreviation,
	o_AuditContactStateDescription AS AuditContactStateDescription,
	PermanentOverrideFlag,
	PolicyPeriodOverrideFlag,
	FrontingPolicyFlag,
	AuditCloseOutFlag,
	AuditStatus,
	AuditableFlag,
	AssignedAuditorOverideFlag,
	AuditTypeOverrideFlag,
	AuditablePremium,
	IsAuditableFlag,
	o_ChangeFlag AS ChangeFlag,
	o_NoncomplianceofWCPoolAudit
	FROM EXPTRANS
),
RTR_Insert_INSERT AS (SELECT * FROM RTR_Insert WHERE ChangeFlag=1),
RTR_Insert_UPDATE AS (SELECT * FROM RTR_Insert WHERE ChangeFlag=0),
UPDTRANS AS (
	SELECT
	PolicyAuditDimId, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModifiedDate, 
	EDWPolicyAuditAKId, 
	AssignedAuditor, 
	AuditFrequency, 
	AuditType, 
	AuditContactCity, 
	AuditContactStateAbbreviation, 
	AuditContactStateDescription, 
	PermanentOverrideFlag, 
	PolicyPeriodOverrideFlag, 
	FrontingPolicyFlag, 
	AuditCloseOutFlag, 
	AuditStatus, 
	AuditableFlag, 
	AssignedAuditorOverideFlag, 
	AuditTypeOverrideFlag, 
	AuditablePremium, 
	IsAuditableFlag, 
	o_NoncomplianceofWCPoolAudit
	FROM RTR_Insert_UPDATE
),
TGT_PolicyAuditDim_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyAuditDim AS T
	USING UPDTRANS AS S
	ON T.PolicyAuditDimId = S.PolicyAuditDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.AuditID = S.AuditID, T.EffectiveDate = S.EffectiveDate, T.ExpirationDate = S.ExpirationDate, T.CreatedDate = S.CreatedDate, T.ModifiedDate = S.ModifiedDate, T.EDWPolicyAuditAKId = S.EDWPolicyAuditAKId, T.AssignedAuditor = S.AssignedAuditor, T.AuditFrequency = S.AuditFrequency, T.AuditType = S.AuditType, T.AuditContactCity = S.AuditContactCity, T.AuditContactStateAbbreviation = S.AuditContactStateAbbreviation, T.AuditContactStateDescription = S.AuditContactStateDescription, T.PermanentOverrideFlag = S.PermanentOverrideFlag, T.PolicyPeriodOverrideFlag = S.PolicyPeriodOverrideFlag, T.FrontingPolicyFlag = S.FrontingPolicyFlag, T.AuditCloseOutFlag = S.AuditCloseOutFlag, T.AuditStatus = S.AuditStatus, T.AuditableFlag = S.AuditableFlag, T.AssignedAuditorOveride = S.AssignedAuditorOverideFlag, T.AuditTypeOverride = S.AuditTypeOverrideFlag, T.AuditablePremium = S.AuditablePremium, T.IsAuditableFlag = S.IsAuditableFlag, T.NoncomplianceofWCPoolAudit = S.o_NoncomplianceofWCPoolAudit
),
TGT_PolicyAuditDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyAuditDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, EDWPolicyAuditAKId, AssignedAuditor, AuditFrequency, AuditType, AuditContactCity, AuditContactStateAbbreviation, AuditContactStateDescription, PermanentOverrideFlag, PolicyPeriodOverrideFlag, FrontingPolicyFlag, AuditCloseOutFlag, AuditStatus, AuditableFlag, AssignedAuditorOveride, AuditTypeOverride, AuditablePremium, IsAuditableFlag, NoncomplianceofWCPoolAudit)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	EDWPOLICYAUDITAKID, 
	ASSIGNEDAUDITOR, 
	AUDITFREQUENCY, 
	AUDITTYPE, 
	AUDITCONTACTCITY, 
	AUDITCONTACTSTATEABBREVIATION, 
	AUDITCONTACTSTATEDESCRIPTION, 
	PERMANENTOVERRIDEFLAG, 
	POLICYPERIODOVERRIDEFLAG, 
	FRONTINGPOLICYFLAG, 
	AUDITCLOSEOUTFLAG, 
	AUDITSTATUS, 
	AUDITABLEFLAG, 
	AssignedAuditorOverideFlag AS ASSIGNEDAUDITOROVERIDE, 
	AuditTypeOverrideFlag AS AUDITTYPEOVERRIDE, 
	AUDITABLEPREMIUM, 
	ISAUDITABLEFLAG, 
	o_NoncomplianceofWCPoolAudit AS NONCOMPLIANCEOFWCPOOLAUDIT
	FROM RTR_Insert_INSERT
),