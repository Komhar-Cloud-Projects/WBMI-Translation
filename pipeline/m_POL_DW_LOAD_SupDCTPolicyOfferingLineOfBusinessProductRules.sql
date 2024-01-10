WITH
SQ_SupDCTPolicyOfferingLineOfBusinessProductRules AS (
	SELECT
		SupDCTPolicyOfferingLineOfBusinessProductRulesId,
		ModifiedUserId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		SupDCTPolicyOfferingLineOfBusinessProductRulesAKId,
		DCTPolicyDivision,
		DCTProductCode,
		DCTProductType,
		DCTLineOfBusinessCode,
		PolicyOfferingCode,
		LineOfBusinessCode,
		ProductCode,
		DCTCoverageType
	FROM SupDCTPolicyOfferingLineOfBusinessProductRules1
),
EXP_DateValues AS (
	SELECT
	ModifiedDate AS i_ModifiedDate,
	EffectiveDate AS i_EffectiveDate,
	ExpirationDate AS i_ExpirationDate,
	-- *INF*: IIF(ISNULL(i_ModifiedDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_ModifiedDate)
	IFF(i_ModifiedDate IS NULL,
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		),
		i_ModifiedDate
	) AS o_ModifiedDate,
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
	) AS o_ExpirationDate
	FROM SQ_SupDCTPolicyOfferingLineOfBusinessProductRules
),
EXP_NumericValues AS (
	SELECT
	SupDCTPolicyOfferingLineOfBusinessProductRulesId AS i_SupDCTPolicyOfferingLineOfBusinessProductRulesId,
	SupDCTPolicyOfferingLineOfBusinessProductRulesAKId AS i_SupDCTPolicyOfferingLineOfBusinessProductRulesAKId,
	-- *INF*: IIF(ISNULL(i_SupDCTPolicyOfferingLineOfBusinessProductRulesId),-1,i_SupDCTPolicyOfferingLineOfBusinessProductRulesId)
	IFF(i_SupDCTPolicyOfferingLineOfBusinessProductRulesId IS NULL,
		- 1,
		i_SupDCTPolicyOfferingLineOfBusinessProductRulesId
	) AS o_SupDCTPolicyOfferingLineOfBusinessProductRulesId,
	-- *INF*: IIF(ISNULL(i_SupDCTPolicyOfferingLineOfBusinessProductRulesAKId),-1,i_SupDCTPolicyOfferingLineOfBusinessProductRulesAKId)
	IFF(i_SupDCTPolicyOfferingLineOfBusinessProductRulesAKId IS NULL,
		- 1,
		i_SupDCTPolicyOfferingLineOfBusinessProductRulesAKId
	) AS o_SupDCTPolicyOfferingLineOfBusinessProductRulesAKId
	FROM SQ_SupDCTPolicyOfferingLineOfBusinessProductRules
),
EXP_StringValues AS (
	SELECT
	ModifiedUserId AS i_ModifiedUserId,
	DCTPolicyDivision AS i_DCTPolicyDivision,
	DCTProductCode AS i_DCTProductCode,
	DCTProductType AS i_DCTProductType,
	DCTLineOfBusinessCode AS i_DCTLineOfBusinessCode,
	PolicyOfferingCode AS i_PolicyOfferingCode,
	LineOfBusinessCode AS i_LineOfBusinessCode,
	ProductCode AS i_ProductCode,
	ExpirationDate AS i_ExpirationDate,
	DCTCoverageType AS i_DCTCoverageType,
	-- *INF*: IIF(TRUNC(i_ExpirationDate)=TO_DATE('2100-12-31','YYYY-MM-DD'),1,0)
	IFF(TRUNC(i_ExpirationDate) = TO_DATE('2100-12-31', 'YYYY-MM-DD'
		),
		1,
		0
	) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_ModifiedUserId) OR LENGTH(i_ModifiedUserId)=0 OR IS_SPACES(i_ModifiedUserId),'N/A',LTRIM(RTRIM(i_ModifiedUserId)))
	IFF(i_ModifiedUserId IS NULL 
		OR LENGTH(i_ModifiedUserId
		) = 0 
		OR LENGTH(i_ModifiedUserId)>0 AND TRIM(i_ModifiedUserId)='',
		'N/A',
		LTRIM(RTRIM(i_ModifiedUserId
			)
		)
	) AS o_ModifiedUserId,
	-- *INF*: IIF(ISNULL(i_DCTPolicyDivision) OR LENGTH(i_DCTPolicyDivision)=0 OR IS_SPACES(i_DCTPolicyDivision),'N/A',LTRIM(RTRIM(i_DCTPolicyDivision)))
	IFF(i_DCTPolicyDivision IS NULL 
		OR LENGTH(i_DCTPolicyDivision
		) = 0 
		OR LENGTH(i_DCTPolicyDivision)>0 AND TRIM(i_DCTPolicyDivision)='',
		'N/A',
		LTRIM(RTRIM(i_DCTPolicyDivision
			)
		)
	) AS o_DCTPolicyDivision,
	-- *INF*: IIF(ISNULL(i_DCTProductCode) OR LENGTH(i_DCTProductCode)=0 OR IS_SPACES(i_DCTProductCode),'N/A',LTRIM(RTRIM(i_DCTProductCode)))
	IFF(i_DCTProductCode IS NULL 
		OR LENGTH(i_DCTProductCode
		) = 0 
		OR LENGTH(i_DCTProductCode)>0 AND TRIM(i_DCTProductCode)='',
		'N/A',
		LTRIM(RTRIM(i_DCTProductCode
			)
		)
	) AS o_DCTProductCode,
	-- *INF*: IIF(ISNULL(i_DCTProductType) OR LENGTH(i_DCTProductType)=0 OR IS_SPACES(i_DCTProductType),'N/A',LTRIM(RTRIM(i_DCTProductType)))
	IFF(i_DCTProductType IS NULL 
		OR LENGTH(i_DCTProductType
		) = 0 
		OR LENGTH(i_DCTProductType)>0 AND TRIM(i_DCTProductType)='',
		'N/A',
		LTRIM(RTRIM(i_DCTProductType
			)
		)
	) AS o_DCTProductType,
	-- *INF*: IIF(ISNULL(i_DCTLineOfBusinessCode) OR LENGTH(i_DCTLineOfBusinessCode)=0 OR IS_SPACES(i_DCTLineOfBusinessCode),'N/A',LTRIM(RTRIM(i_DCTLineOfBusinessCode)))
	IFF(i_DCTLineOfBusinessCode IS NULL 
		OR LENGTH(i_DCTLineOfBusinessCode
		) = 0 
		OR LENGTH(i_DCTLineOfBusinessCode)>0 AND TRIM(i_DCTLineOfBusinessCode)='',
		'N/A',
		LTRIM(RTRIM(i_DCTLineOfBusinessCode
			)
		)
	) AS o_DCTLineOfBusinessCode,
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
	-- *INF*: IIF(ISNULL(i_ProductCode) OR LENGTH(i_ProductCode)=0 OR IS_SPACES(i_ProductCode),'N/A',LTRIM(RTRIM(i_ProductCode)))
	IFF(i_ProductCode IS NULL 
		OR LENGTH(i_ProductCode
		) = 0 
		OR LENGTH(i_ProductCode)>0 AND TRIM(i_ProductCode)='',
		'N/A',
		LTRIM(RTRIM(i_ProductCode
			)
		)
	) AS o_ProductCode,
	-- *INF*: IIF(ISNULL(i_DCTCoverageType) OR LENGTH(i_DCTCoverageType)=0 OR IS_SPACES(i_DCTCoverageType),'N/A',LTRIM(RTRIM(i_DCTCoverageType)))
	IFF(i_DCTCoverageType IS NULL 
		OR LENGTH(i_DCTCoverageType
		) = 0 
		OR LENGTH(i_DCTCoverageType)>0 AND TRIM(i_DCTCoverageType)='',
		'N/A',
		LTRIM(RTRIM(i_DCTCoverageType
			)
		)
	) AS o_DCTCoverageType
	FROM SQ_SupDCTPolicyOfferingLineOfBusinessProductRules
),
TGT_SupDCTPolicyOfferingLineOfBusinessProductRules_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDCTPolicyOfferingLineOfBusinessProductRules AS T
	USING EXP_DateValues AS S
	ON T.SupDCTPolicyOfferingLineOfBusinessProductRulesId = S.o_SupDCTPolicyOfferingLineOfBusinessProductRulesId
	WHEN MATCHED THEN
	UPDATE SET T.ModifiedUserId = S.o_ModifiedUserId, T.ModifiedDate = S.o_ModifiedDate, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SupDCTPolicyOfferingLineOfBusinessProductRulesAKId = S.o_SupDCTPolicyOfferingLineOfBusinessProductRulesAKId, T.DCTPolicyDivision = S.o_DCTPolicyDivision, T.DCTProductCode = S.o_DCTProductCode, T.DCTProductType = S.o_DCTProductType, T.DCTLineOfBusinessCode = S.o_DCTLineOfBusinessCode, T.PolicyOfferingCode = S.o_PolicyOfferingCode, T.InsuranceReferenceLineOfBusinessCode = S.o_LineOfBusinessCode, T.ProductCode = S.o_ProductCode, T.DCTCoverageType = S.o_DCTCoverageType
	WHEN NOT MATCHED THEN
	INSERT (SupDCTPolicyOfferingLineOfBusinessProductRulesId, ModifiedUserId, ModifiedDate, EffectiveDate, ExpirationDate, SupDCTPolicyOfferingLineOfBusinessProductRulesAKId, DCTPolicyDivision, DCTProductCode, DCTProductType, DCTLineOfBusinessCode, PolicyOfferingCode, InsuranceReferenceLineOfBusinessCode, ProductCode, DCTCoverageType)
	VALUES (
	EXP_NumericValues.o_SupDCTPolicyOfferingLineOfBusinessProductRulesId AS SUPDCTPOLICYOFFERINGLINEOFBUSINESSPRODUCTRULESID, 
	EXP_StringValues.o_ModifiedUserId AS MODIFIEDUSERID, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_NumericValues.o_SupDCTPolicyOfferingLineOfBusinessProductRulesAKId AS SUPDCTPOLICYOFFERINGLINEOFBUSINESSPRODUCTRULESAKID, 
	EXP_StringValues.o_DCTPolicyDivision AS DCTPOLICYDIVISION, 
	EXP_StringValues.o_DCTProductCode AS DCTPRODUCTCODE, 
	EXP_StringValues.o_DCTProductType AS DCTPRODUCTTYPE, 
	EXP_StringValues.o_DCTLineOfBusinessCode AS DCTLINEOFBUSINESSCODE, 
	EXP_StringValues.o_PolicyOfferingCode AS POLICYOFFERINGCODE, 
	EXP_StringValues.o_LineOfBusinessCode AS INSURANCEREFERENCELINEOFBUSINESSCODE, 
	EXP_StringValues.o_ProductCode AS PRODUCTCODE, 
	EXP_StringValues.o_DCTCoverageType AS DCTCOVERAGETYPE)
),