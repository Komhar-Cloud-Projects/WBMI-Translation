WITH
SQ_SupLineOfBusinessRules AS (
	SELECT
		SupLineOfBusinessRulesId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		SupLineOfBusinessRulesAKId,
		SourceCode,
		SequenceNumber,
		LineOfBusinessCode,
		PolicySymbol,
		ClassOfBusiness,
		InsuranceLine,
		TypeBureauCode,
		MajorPerilCode,
		ClassCode,
		RiskUnitGroup,
		BureauSpecialUseCode,
		LocationUnitNumber,
		BureauCode2
	FROM SupLineOfBusinessRules
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
	FROM SQ_SupLineOfBusinessRules
),
EXP_NumericValues AS (
	SELECT
	SupLineOfBusinessRulesId AS i_SupLineOfBusinessRulesId,
	SupLineOfBusinessRulesAKId AS i_SupLineOfBusinessRulesAKId,
	SequenceNumber AS i_SequenceNumber,
	-- *INF*: IIF(ISNULL(i_SupLineOfBusinessRulesId),-1,i_SupLineOfBusinessRulesId)
	IFF(i_SupLineOfBusinessRulesId IS NULL,
		- 1,
		i_SupLineOfBusinessRulesId
	) AS o_SupLineOfBusinessRulesId,
	-- *INF*: IIF(ISNULL(i_SupLineOfBusinessRulesAKId),-1,i_SupLineOfBusinessRulesAKId)
	IFF(i_SupLineOfBusinessRulesAKId IS NULL,
		- 1,
		i_SupLineOfBusinessRulesAKId
	) AS o_SupLineOfBusinessRulesAKId,
	-- *INF*: IIF(ISNULL(i_SequenceNumber),-1,i_SequenceNumber)
	IFF(i_SequenceNumber IS NULL,
		- 1,
		i_SequenceNumber
	) AS o_SequenceNumber
	FROM SQ_SupLineOfBusinessRules
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	SourceCode AS i_SourceCode,
	LineOfBusinessCode AS i_LineOfBusinessCode,
	PolicySymbol AS i_PolicySymbol,
	ClassOfBusiness AS i_ClassOfBusiness,
	InsuranceLine AS i_InsuranceLine,
	TypeBureauCode AS i_TypeBureauCode,
	MajorPerilCode AS i_MajorPerilCode,
	ClassCode AS i_ClassCode,
	RiskUnitGroup AS i_RiskUnitGroup,
	BureauSpecialUseCode AS i_BureauSpecialUseCode,
	LocationUnitNumber AS i_LocationUnitNumber,
	BureauCode2 AS i_BureauCode2,
	-- *INF*: IIF(TRUNC(i_ExpirationDate)=TO_DATE('2100-12-31','YYYY-MM-DD'),1,0)
	IFF(TRUNC(i_ExpirationDate) = TO_DATE('2100-12-31', 'YYYY-MM-DD'
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
	-- *INF*: IIF(ISNULL(i_LineOfBusinessCode) OR LENGTH(i_LineOfBusinessCode)=0 OR IS_SPACES(i_LineOfBusinessCode),'N/A',LTRIM(RTRIM(i_LineOfBusinessCode)))
	IFF(i_LineOfBusinessCode IS NULL 
		OR LENGTH(i_LineOfBusinessCode
		) = 0 
		OR LENGTH(i_LineOfBusinessCode)>0 AND TRIM(i_LineOfBusinessCode)='',
		'N/A',
		LTRIM(RTRIM(i_LineOfBusinessCode
			)
		)
	) AS o_LineOfBusinessCode,
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
	-- *INF*: IIF(ISNULL(i_ClassOfBusiness) OR LENGTH(i_ClassOfBusiness)=0 OR IS_SPACES(i_ClassOfBusiness),'N/A',LTRIM(RTRIM(i_ClassOfBusiness)))
	IFF(i_ClassOfBusiness IS NULL 
		OR LENGTH(i_ClassOfBusiness
		) = 0 
		OR LENGTH(i_ClassOfBusiness)>0 AND TRIM(i_ClassOfBusiness)='',
		'N/A',
		LTRIM(RTRIM(i_ClassOfBusiness
			)
		)
	) AS o_ClassOfBusiness,
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
	-- *INF*: IIF(ISNULL(i_TypeBureauCode) OR LENGTH(i_TypeBureauCode)=0 OR IS_SPACES(i_TypeBureauCode),'N/A',LTRIM(RTRIM(i_TypeBureauCode)))
	IFF(i_TypeBureauCode IS NULL 
		OR LENGTH(i_TypeBureauCode
		) = 0 
		OR LENGTH(i_TypeBureauCode)>0 AND TRIM(i_TypeBureauCode)='',
		'N/A',
		LTRIM(RTRIM(i_TypeBureauCode
			)
		)
	) AS o_TypeBureauCode,
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
	-- *INF*: IIF(ISNULL(i_ClassCode) OR LENGTH(i_ClassCode)=0 OR IS_SPACES(i_ClassCode),'N/A',LTRIM(RTRIM(i_ClassCode)))
	IFF(i_ClassCode IS NULL 
		OR LENGTH(i_ClassCode
		) = 0 
		OR LENGTH(i_ClassCode)>0 AND TRIM(i_ClassCode)='',
		'N/A',
		LTRIM(RTRIM(i_ClassCode
			)
		)
	) AS o_ClassCode,
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
	-- *INF*: IIF(ISNULL(i_BureauSpecialUseCode) OR LENGTH(i_BureauSpecialUseCode)=0 OR IS_SPACES(i_BureauSpecialUseCode),'N/A',LTRIM(RTRIM(i_BureauSpecialUseCode)))
	IFF(i_BureauSpecialUseCode IS NULL 
		OR LENGTH(i_BureauSpecialUseCode
		) = 0 
		OR LENGTH(i_BureauSpecialUseCode)>0 AND TRIM(i_BureauSpecialUseCode)='',
		'N/A',
		LTRIM(RTRIM(i_BureauSpecialUseCode
			)
		)
	) AS o_BureauSpecialUseCode,
	-- *INF*: IIF(ISNULL(i_LocationUnitNumber) OR LENGTH(i_LocationUnitNumber)=0 OR IS_SPACES(i_LocationUnitNumber),'N/A',LTRIM(RTRIM(i_LocationUnitNumber)))
	IFF(i_LocationUnitNumber IS NULL 
		OR LENGTH(i_LocationUnitNumber
		) = 0 
		OR LENGTH(i_LocationUnitNumber)>0 AND TRIM(i_LocationUnitNumber)='',
		'N/A',
		LTRIM(RTRIM(i_LocationUnitNumber
			)
		)
	) AS o_LocationUnitNumber,
	-- *INF*: IIF(ISNULL(i_BureauCode2) OR LENGTH(i_BureauCode2)=0 OR IS_SPACES(i_BureauCode2),'N/A',LTRIM(RTRIM(i_BureauCode2)))
	IFF(i_BureauCode2 IS NULL 
		OR LENGTH(i_BureauCode2
		) = 0 
		OR LENGTH(i_BureauCode2)>0 AND TRIM(i_BureauCode2)='',
		'N/A',
		LTRIM(RTRIM(i_BureauCode2
			)
		)
	) AS o_BureauCode2
	FROM SQ_SupLineOfBusinessRules
),
TGT_SupInsuranceReferenceLineOfBusinessRules_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupInsuranceReferenceLineOfBusinessRules AS T
	USING EXP_StringValues AS S
	ON T.SupInsuranceReferenceLineOfBusinessRulesId = S.o_SupLineOfBusinessRulesId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.SupInsuranceReferenceLineOfBusinessRulesAKId = S.o_SupLineOfBusinessRulesAKId, T.SourceCode = S.o_SourceCode, T.SequenceNumber = S.o_SequenceNumber, T.InsuranceReferenceLineOfBusinessCode = S.o_LineOfBusinessCode, T.PolicySymbol = S.o_PolicySymbol, T.ClassOfBusiness = S.o_ClassOfBusiness, T.InsuranceLine = S.o_InsuranceLine, T.TypeBureauCode = S.o_TypeBureauCode, T.MajorPerilCode = S.o_MajorPerilCode, T.ClassCode = S.o_ClassCode, T.RiskUnitGroup = S.o_RiskUnitGroup, T.BureauSpecialUseCode = S.o_BureauSpecialUseCode, T.LocationUnitNumber = S.o_LocationUnitNumber, T.BureauCode2 = S.o_BureauCode2
	WHEN NOT MATCHED THEN
	INSERT (SupInsuranceReferenceLineOfBusinessRulesId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, SupInsuranceReferenceLineOfBusinessRulesAKId, SourceCode, SequenceNumber, InsuranceReferenceLineOfBusinessCode, PolicySymbol, ClassOfBusiness, InsuranceLine, TypeBureauCode, MajorPerilCode, ClassCode, RiskUnitGroup, BureauSpecialUseCode, LocationUnitNumber, BureauCode2)
	VALUES (
	EXP_NumericValues.o_SupLineOfBusinessRulesId AS SUPINSURANCEREFERENCELINEOFBUSINESSRULESID, 
	EXP_StringValues.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_SupLineOfBusinessRulesAKId AS SUPINSURANCEREFERENCELINEOFBUSINESSRULESAKID, 
	EXP_StringValues.o_SourceCode AS SOURCECODE, 
	EXP_NumericValues.o_SequenceNumber AS SEQUENCENUMBER, 
	EXP_StringValues.o_LineOfBusinessCode AS INSURANCEREFERENCELINEOFBUSINESSCODE, 
	EXP_StringValues.o_PolicySymbol AS POLICYSYMBOL, 
	EXP_StringValues.o_ClassOfBusiness AS CLASSOFBUSINESS, 
	EXP_StringValues.o_InsuranceLine AS INSURANCELINE, 
	EXP_StringValues.o_TypeBureauCode AS TYPEBUREAUCODE, 
	EXP_StringValues.o_MajorPerilCode AS MAJORPERILCODE, 
	EXP_StringValues.o_ClassCode AS CLASSCODE, 
	EXP_StringValues.o_RiskUnitGroup AS RISKUNITGROUP, 
	EXP_StringValues.o_BureauSpecialUseCode AS BUREAUSPECIALUSECODE, 
	EXP_StringValues.o_LocationUnitNumber AS LOCATIONUNITNUMBER, 
	EXP_StringValues.o_BureauCode2 AS BUREAUCODE2)
),