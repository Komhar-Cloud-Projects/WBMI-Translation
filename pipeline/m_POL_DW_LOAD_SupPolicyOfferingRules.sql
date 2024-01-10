WITH
SQ_SupPolicyOfferingRules AS (
	SELECT
		SupPolicyOfferingRulesId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		SupPolicyOfferingRuleAKId,
		SourceCode,
		SequenceNumber,
		PolicyOfferingCode,
		PolicySymbol,
		InsuranceLine,
		MajorPerilCode,
		SublineCode,
		RiskUnitGroup,
		ClassCode
	FROM SupPolicyOfferingRules
),
EXP_DateValues AS (
	SELECT
	ModifiedDate AS i_ModifiedDate,
	EffectiveDate AS i_EffectiveDate,
	ExpirationDate AS i_ExpirationDate,
	-- *INF*: IIF(ISNULL(i_EffectiveDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_EffectiveDate)
	IFF(i_EffectiveDate IS NULL,
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		),
		i_EffectiveDate
	) AS o_EffectiveDate,
	-- *INF*: IIF(ISNULL(i_ExpirationDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_ExpirationDate)
	IFF(i_ExpirationDate IS NULL,
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		),
		i_ExpirationDate
	) AS o_ExpirationDate,
	-- *INF*: IIF(ISNULL(i_ModifiedDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_ModifiedDate)
	IFF(i_ModifiedDate IS NULL,
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		),
		i_ModifiedDate
	) AS o_ModifiedDate
	FROM SQ_SupPolicyOfferingRules
),
EXP_NumericValues AS (
	SELECT
	SupPolicyOfferingRulesId AS i_SupPolicyOfferingRulesId,
	SupPolicyOfferingRuleAKId AS i_SupPolicyOfferingRuleAKId,
	SequenceNumber AS i_SequenceNumber,
	-- *INF*: IIF(ISNULL(i_SupPolicyOfferingRulesId),-1,i_SupPolicyOfferingRulesId)
	IFF(i_SupPolicyOfferingRulesId IS NULL,
		- 1,
		i_SupPolicyOfferingRulesId
	) AS o_SupPolicyOfferingRulesId,
	-- *INF*: IIF(ISNULL(i_SupPolicyOfferingRuleAKId),-1,i_SupPolicyOfferingRuleAKId)
	IFF(i_SupPolicyOfferingRuleAKId IS NULL,
		- 1,
		i_SupPolicyOfferingRuleAKId
	) AS o_SupPolicyOfferingRuleAKId,
	-- *INF*: IIF(ISNULL(i_SequenceNumber),-1,i_SequenceNumber)
	IFF(i_SequenceNumber IS NULL,
		- 1,
		i_SequenceNumber
	) AS o_SequenceNumber
	FROM SQ_SupPolicyOfferingRules
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	SourceCode AS i_SourceCode,
	PolicyOfferingCode AS i_PolicyOfferingCode,
	PolicySymbol AS i_PolicySymbol,
	InsuranceLine AS i_InsuranceLine,
	MajorPerilCode AS i_MajorPerilCode,
	SublineCode AS i_SublineCode,
	RiskUnitGroup AS i_RiskUnitGroup,
	ClassCode AS i_ClassCode,
	-- *INF*: IIF(i_ExpirationDate>=TO_DATE('21001231','YYYYMMDD'),1,0)
	IFF(i_ExpirationDate >= TO_DATE('21001231', 'YYYYMMDD'
		),
		1,
		0
	) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_SourceCode) OR LENGTH(i_SourceCode)=0 OR IS_SPACES(i_SourceCode),'N/A',LTRIM(RTRIM(i_SourceCode)))
	IFF(i_SourceCode IS NULL 
		OR LENGTH(i_SourceCode
		) = 0 
		OR LENGTH(i_SourceCode)>0 AND TRIM(i_SourceCode)='',
		'N/A',
		LTRIM(RTRIM(i_SourceCode
			)
		)
	) AS o_SourceCode,
	-- *INF*: IIF(ISNULL(i_PolicyOfferingCode) OR LENGTH(i_PolicyOfferingCode)=0 OR IS_SPACES(i_PolicyOfferingCode),'N/A',LTRIM(RTRIM(i_PolicyOfferingCode)))
	IFF(i_PolicyOfferingCode IS NULL 
		OR LENGTH(i_PolicyOfferingCode
		) = 0 
		OR LENGTH(i_PolicyOfferingCode)>0 AND TRIM(i_PolicyOfferingCode)='',
		'N/A',
		LTRIM(RTRIM(i_PolicyOfferingCode
			)
		)
	) AS o_PolicyOfferingCode,
	-- *INF*: IIF(ISNULL(i_PolicySymbol) OR LENGTH(i_PolicySymbol)=0 OR IS_SPACES(i_PolicySymbol),'N/A',LTRIM(RTRIM(i_PolicySymbol)))
	IFF(i_PolicySymbol IS NULL 
		OR LENGTH(i_PolicySymbol
		) = 0 
		OR LENGTH(i_PolicySymbol)>0 AND TRIM(i_PolicySymbol)='',
		'N/A',
		LTRIM(RTRIM(i_PolicySymbol
			)
		)
	) AS o_PolicySymbol,
	-- *INF*: IIF(ISNULL(i_InsuranceLine) OR LENGTH(i_InsuranceLine)=0 OR IS_SPACES(i_InsuranceLine),'N/A',LTRIM(RTRIM(i_InsuranceLine)))
	IFF(i_InsuranceLine IS NULL 
		OR LENGTH(i_InsuranceLine
		) = 0 
		OR LENGTH(i_InsuranceLine)>0 AND TRIM(i_InsuranceLine)='',
		'N/A',
		LTRIM(RTRIM(i_InsuranceLine
			)
		)
	) AS o_InsuranceLine,
	-- *INF*: IIF(ISNULL(i_MajorPerilCode) OR LENGTH(i_MajorPerilCode)=0 OR IS_SPACES(i_MajorPerilCode),'N/A',LTRIM(RTRIM(i_MajorPerilCode)))
	IFF(i_MajorPerilCode IS NULL 
		OR LENGTH(i_MajorPerilCode
		) = 0 
		OR LENGTH(i_MajorPerilCode)>0 AND TRIM(i_MajorPerilCode)='',
		'N/A',
		LTRIM(RTRIM(i_MajorPerilCode
			)
		)
	) AS o_MajorPerilCode,
	-- *INF*: IIF(ISNULL(i_SublineCode) OR LENGTH(i_SublineCode)=0 OR IS_SPACES(i_SublineCode),'N/A',LTRIM(RTRIM(i_SublineCode)))
	IFF(i_SublineCode IS NULL 
		OR LENGTH(i_SublineCode
		) = 0 
		OR LENGTH(i_SublineCode)>0 AND TRIM(i_SublineCode)='',
		'N/A',
		LTRIM(RTRIM(i_SublineCode
			)
		)
	) AS o_SublineCode,
	-- *INF*: IIF(ISNULL(i_RiskUnitGroup) OR LENGTH(i_RiskUnitGroup)=0 OR IS_SPACES(i_RiskUnitGroup),'N/A',LTRIM(RTRIM(i_RiskUnitGroup)))
	IFF(i_RiskUnitGroup IS NULL 
		OR LENGTH(i_RiskUnitGroup
		) = 0 
		OR LENGTH(i_RiskUnitGroup)>0 AND TRIM(i_RiskUnitGroup)='',
		'N/A',
		LTRIM(RTRIM(i_RiskUnitGroup
			)
		)
	) AS o_RiskUnitGroup,
	-- *INF*: IIF(ISNULL(i_ClassCode) OR LENGTH(i_ClassCode)=0 OR IS_SPACES(i_ClassCode),'N/A',LTRIM(RTRIM(i_ClassCode)))
	IFF(i_ClassCode IS NULL 
		OR LENGTH(i_ClassCode
		) = 0 
		OR LENGTH(i_ClassCode)>0 AND TRIM(i_ClassCode)='',
		'N/A',
		LTRIM(RTRIM(i_ClassCode
			)
		)
	) AS o_ClassCode
	FROM SQ_SupPolicyOfferingRules
),
TGT_SupPolicyOfferingRules_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupPolicyOfferingRules AS T
	USING EXP_StringValues AS S
	ON T.SupPolicyOfferingRulesId = S.o_SupPolicyOfferingRulesId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.SupPolicyOfferingRulesAKId = S.o_SupPolicyOfferingRuleAKId, T.SourceCode = S.o_SourceCode, T.SequenceNumber = S.o_SequenceNumber, T.PolicyOfferingCode = S.o_PolicyOfferingCode, T.PolicySymbol = S.o_PolicySymbol, T.InsuranceLine = S.o_InsuranceLine, T.MajorPerilCode = S.o_MajorPerilCode, T.SublineCode = S.o_SublineCode, T.RiskUnitGroup = S.o_RiskUnitGroup, T.ClassCode = S.o_ClassCode
	WHEN NOT MATCHED THEN
	INSERT (SupPolicyOfferingRulesId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, SupPolicyOfferingRulesAKId, SourceCode, SequenceNumber, PolicyOfferingCode, PolicySymbol, InsuranceLine, MajorPerilCode, SublineCode, RiskUnitGroup, ClassCode)
	VALUES (
	EXP_NumericValues.o_SupPolicyOfferingRulesId AS SUPPOLICYOFFERINGRULESID, 
	EXP_StringValues.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_SupPolicyOfferingRuleAKId AS SUPPOLICYOFFERINGRULESAKID, 
	EXP_StringValues.o_SourceCode AS SOURCECODE, 
	EXP_NumericValues.o_SequenceNumber AS SEQUENCENUMBER, 
	EXP_StringValues.o_PolicyOfferingCode AS POLICYOFFERINGCODE, 
	EXP_StringValues.o_PolicySymbol AS POLICYSYMBOL, 
	EXP_StringValues.o_InsuranceLine AS INSURANCELINE, 
	EXP_StringValues.o_MajorPerilCode AS MAJORPERILCODE, 
	EXP_StringValues.o_SublineCode AS SUBLINECODE, 
	EXP_StringValues.o_RiskUnitGroup AS RISKUNITGROUP, 
	EXP_StringValues.o_ClassCode AS CLASSCODE)
),