WITH
SQ_DCTWorkTables AS (
	SELECT
		WorkDCTCoverageTransaction.CoverageGUID,
		WorkDCTPolicy.PolicyGUId,
		WorkDCTPolicy.QuoteActionTimeStamp,
		WorkDCTPolicy.SessionId,
		WorkDCTPolicy.Division,
		WorkDCTPolicy.WBProduct,
		WorkDCTPolicy.WBProductType,
		WorkDCTInsuranceLine.LineType,
		WorkDCTInsuranceLine.RiskGrade,
		WorkDCTCoverageTransaction.Written,
		WorkDCTTransactionInsuranceLineLocationBridge.LineId
	FROM WorkDCTPolicy
	INNER JOIN WorkDCTTransactionInsuranceLineLocationBridge
	INNER JOIN WorkDCTCoverageTransaction
	INNER JOIN WorkDCTInsuranceLine
	ON WorkDCTPolicy.PolicyId=WorkDCTInsuranceLine.PolicyId
	and
	WorkDCTPolicy.QuoteActionTimeStamp is not null
	and
	WorkDCTInsuranceLine.LineId=WorkDCTTransactionInsuranceLineLocationBridge.LineId
	and
	WorkDCTTransactionInsuranceLineLocationBridge.CoverageId=WorkDCTCoverageTransaction.CoverageId
	and
	WorkDCTCoverageTransaction.CoverageGUId is not null
	@{pipeline().parameters.WHERE_CLAUSE}
),
AGG_RemoveDuplicates AS (
	SELECT
	CoverageGUID AS i_CoverageGUID,
	PolicyGUId AS QuoteKey,
	QuoteActionTimeStamp AS StatusDate,
	SessionId AS i_SessionId,
	Division,
	WBProduct,
	WBProductType,
	LineType AS LType,
	RiskGrade,
	Written
	FROM SQ_DCTWorkTables
	QUALIFY ROW_NUMBER() OVER (PARTITION BY i_CoverageGUID, QuoteKey, StatusDate ORDER BY NULL) = 1
),
EXP_GetValues AS (
	SELECT
	QuoteKey AS i_QuoteKey,
	StatusDate AS i_StatusDate,
	Division AS i_Division,
	WBProduct AS i_WBProduct,
	WBProductType AS i_WBProductType,
	LType AS i_LType,
	RiskGrade AS i_RiskGrade,
	Written AS i_Written,
	-- *INF*: LTRIM(RTRIM(i_QuoteKey))
	LTRIM(RTRIM(i_QuoteKey
		)
	) AS o_QuoteKey,
	i_StatusDate AS o_StatusDate,
	-- *INF*: IIF(ISNULL(i_Division) OR IS_SPACES(i_Division) OR LENGTH(i_Division)=0, 'N/A', LTRIM(RTRIM(i_Division)))
	IFF(i_Division IS NULL 
		OR LENGTH(i_Division)>0 AND TRIM(i_Division)='' 
		OR LENGTH(i_Division
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_Division
			)
		)
	) AS o_Division,
	-- *INF*: IIF(ISNULL(i_WBProduct) OR IS_SPACES(i_WBProduct) OR LENGTH(i_WBProduct)=0, 'N/A', LTRIM(RTRIM(i_WBProduct)))
	IFF(i_WBProduct IS NULL 
		OR LENGTH(i_WBProduct)>0 AND TRIM(i_WBProduct)='' 
		OR LENGTH(i_WBProduct
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_WBProduct
			)
		)
	) AS o_WBProduct,
	-- *INF*: IIF(ISNULL(i_WBProductType) OR IS_SPACES(i_WBProductType) OR LENGTH(i_WBProductType)=0, 'N/A', LTRIM(RTRIM(i_WBProductType)))
	IFF(i_WBProductType IS NULL 
		OR LENGTH(i_WBProductType)>0 AND TRIM(i_WBProductType)='' 
		OR LENGTH(i_WBProductType
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_WBProductType
			)
		)
	) AS o_WBProductType,
	-- *INF*: IIF(ISNULL(i_LType) OR IS_SPACES(i_LType) OR LENGTH(i_LType)=0, 'N/A', LTRIM(RTRIM(i_LType)))
	IFF(i_LType IS NULL 
		OR LENGTH(i_LType)>0 AND TRIM(i_LType)='' 
		OR LENGTH(i_LType
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_LType
			)
		)
	) AS o_LType,
	-- *INF*: IIF(ISNULL(i_Written),0,i_Written)
	IFF(i_Written IS NULL,
		0,
		i_Written
	) AS o_WrittenPremium,
	-- *INF*: IIF(ISNULL(i_RiskGrade) OR IS_SPACES(i_RiskGrade) OR LENGTH(i_RiskGrade)=0 OR IS_NUMBER(i_RiskGrade)=0, 'N/A', LTRIM(RTRIM(i_RiskGrade)))
	-- 
	IFF(i_RiskGrade IS NULL 
		OR LENGTH(i_RiskGrade)>0 AND TRIM(i_RiskGrade)='' 
		OR LENGTH(i_RiskGrade
		) = 0 
		OR REGEXP_LIKE(i_RiskGrade, '^[0-9]+$') = 0,
		'N/A',
		LTRIM(RTRIM(i_RiskGrade
			)
		)
	) AS o_RiskGrade
	FROM AGG_RemoveDuplicates
),
LKP_SupDCTPolicyOfferingLineOfBusinessProductRules AS (
	SELECT
	InsuranceReferenceLineOfBusinessCode,
	ProductCode,
	DCTPolicyDivision,
	DCTProductCode,
	DCTProductType,
	DCTLineOfBusinessCode
	FROM (
		SELECT 
			InsuranceReferenceLineOfBusinessCode,
			ProductCode,
			DCTPolicyDivision,
			DCTProductCode,
			DCTProductType,
			DCTLineOfBusinessCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDCTPolicyOfferingLineOfBusinessProductRules
		WHERE Getdate() between EffectiveDate and ExpirationDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY DCTPolicyDivision,DCTProductCode,DCTProductType,DCTLineOfBusinessCode ORDER BY InsuranceReferenceLineOfBusinessCode) = 1
),
LKP_InsuranceReferenceLineOfBusiness AS (
	SELECT
	InsuranceReferenceLineOfBusinessAKId,
	InsuranceReferenceLineOfBusinessCode
	FROM (
		SELECT 
			InsuranceReferenceLineOfBusinessAKId,
			InsuranceReferenceLineOfBusinessCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceLineOfBusiness
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceReferenceLineOfBusinessCode ORDER BY InsuranceReferenceLineOfBusinessAKId) = 1
),
LKP_Product AS (
	SELECT
	ProductAKId,
	ProductCode
	FROM (
		SELECT 
			ProductAKId,
			ProductCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Product
		WHERE CurrentSnapshotFlag=1 and getdate() between EffectiveDate and ExpirationDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProductCode ORDER BY ProductAKId) = 1
),
LKP_Quote AS (
	SELECT
	QuoteAKId,
	StatusDate,
	QuoteKey
	FROM (
		Select 
		b.QuoteAKId as QuoteAKId,
		b.QuoteKey as QuoteKey,
		b.StatusDate as StatusDate
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.Quote b
		where b.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and exists (select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy w
		where w.PolicyGUId=b.QuoteKey)
		order by b.StatusDate,b.QuoteKey
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StatusDate,QuoteKey ORDER BY QuoteAKId) = 1
),
EXP_NewFlag AS (
	SELECT
	LKP_Quote.QuoteAKId AS i_QuoteAKId,
	LKP_Product.ProductAKId AS i_ProductAKId,
	LKP_InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessAKId AS i_InsuranceReferenceLineOfBusinessAKId,
	EXP_GetValues.o_StatusDate AS StatusDate,
	EXP_GetValues.o_WrittenPremium AS WrittenPremium,
	EXP_GetValues.o_RiskGrade AS RiskGrade,
	-- *INF*: IIF(ISNULL(i_ProductAKId),-1,i_ProductAKId)
	IFF(i_ProductAKId IS NULL,
		- 1,
		i_ProductAKId
	) AS v_ProductAKId,
	-- *INF*: IIF(ISNULL(i_InsuranceReferenceLineOfBusinessAKId),-1,i_InsuranceReferenceLineOfBusinessAKId)
	IFF(i_InsuranceReferenceLineOfBusinessAKId IS NULL,
		- 1,
		i_InsuranceReferenceLineOfBusinessAKId
	) AS v_InsuranceReferenceLineOfBusinessAKId,
	-- *INF*: IIF(ISNULL(i_QuoteAKId),-1,i_QuoteAKId)
	IFF(i_QuoteAKId IS NULL,
		- 1,
		i_QuoteAKId
	) AS o_QuoteAKId,
	v_ProductAKId AS o_ProductAKId,
	v_InsuranceReferenceLineOfBusinessAKId AS o_InsuranceReferenceLineOfBusinessAKId
	FROM EXP_GetValues
	LEFT JOIN LKP_InsuranceReferenceLineOfBusiness
	ON LKP_InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessCode = LKP_SupDCTPolicyOfferingLineOfBusinessProductRules.InsuranceReferenceLineOfBusinessCode
	LEFT JOIN LKP_Product
	ON LKP_Product.ProductCode = LKP_SupDCTPolicyOfferingLineOfBusinessProductRules.ProductCode
	LEFT JOIN LKP_Quote
	ON LKP_Quote.StatusDate = EXP_GetValues.o_StatusDate AND LKP_Quote.QuoteKey = EXP_GetValues.o_QuoteKey
),
AGG_SumPremium AS (
	SELECT
	WrittenPremium AS i_WrittenPremium,
	-- *INF*: SUM(i_WrittenPremium)
	SUM(i_WrittenPremium
	) AS WrittenPremium,
	o_QuoteAKId AS QuoteAKId,
	StatusDate,
	o_ProductAKId AS ProductAKId,
	o_InsuranceReferenceLineOfBusinessAKId AS InsuranceReferenceLineOfBusinessAKId,
	RiskGrade
	FROM EXP_NewFlag
	GROUP BY QuoteAKId, StatusDate, ProductAKId, InsuranceReferenceLineOfBusinessAKId, RiskGrade
),
LKP_QuoteTransaction AS (
	SELECT
	QuoteTransactionID,
	StatusDate,
	QuoteAKId,
	ProductAKId,
	InsuranceReferenceLineOfBusinessAKId,
	RiskGradeCode
	FROM (
		Select  
		a.QuoteTransactionID as QuoteTransactionID,
		a.QuoteAKId as QuoteAKId,
		a.StatusDate as StatusDate,
		a.ProductAKId as ProductAKId,
		a.InsuranceReferenceLineOfBusinessAKId as InsuranceReferenceLineOfBusinessAKId,
		a.RiskGradeCode as RiskGradeCode
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.QuoteTransaction a
		join
		@{pipeline().parameters.TARGET_TABLE_OWNER}.Quote c
		on
		c.QuoteAKId=a.QuoteAKId
		and c.StatusDate=a.StatusDate
		where a.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and exists (
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy w
		where w.PolicyGUId=c.QuoteKey)
		order by a.StatusDate,a.QuoteAKId,a.ProductAKId,a.InsuranceReferenceLineOfBusinessAKId,a.RiskGradeCode
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StatusDate,QuoteAKId,ProductAKId,InsuranceReferenceLineOfBusinessAKId,RiskGradeCode ORDER BY QuoteTransactionID) = 1
),
FIL_NewFlag AS (
	SELECT
	LKP_QuoteTransaction.QuoteTransactionID, 
	AGG_SumPremium.WrittenPremium, 
	AGG_SumPremium.RiskGrade, 
	AGG_SumPremium.QuoteAKId, 
	AGG_SumPremium.StatusDate, 
	AGG_SumPremium.ProductAKId, 
	AGG_SumPremium.InsuranceReferenceLineOfBusinessAKId
	FROM AGG_SumPremium
	LEFT JOIN LKP_QuoteTransaction
	ON LKP_QuoteTransaction.StatusDate = AGG_SumPremium.StatusDate AND LKP_QuoteTransaction.QuoteAKId = AGG_SumPremium.QuoteAKId AND LKP_QuoteTransaction.ProductAKId = AGG_SumPremium.ProductAKId AND LKP_QuoteTransaction.InsuranceReferenceLineOfBusinessAKId = AGG_SumPremium.InsuranceReferenceLineOfBusinessAKId AND LKP_QuoteTransaction.RiskGradeCode = AGG_SumPremium.RiskGrade
	WHERE ISNULL(QuoteTransactionID)
),
LKP_QuoteTransactionAKId AS (
	SELECT
	QuoteTransactionAKID,
	QuoteAKId,
	ProductAKId,
	InsuranceReferenceLineOfBusinessAKId
	FROM (
		Select  
		a.QuoteTransactionAKID as QuoteTransactionAKID,
		a.QuoteAKId as QuoteAKId,
		a.ProductAKId as ProductAKId,
		a.InsuranceReferenceLineOfBusinessAKId as InsuranceReferenceLineOfBusinessAKId
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.QuoteTransaction a
		join
		@{pipeline().parameters.TARGET_TABLE_OWNER}.Quote c
		on
		c.QuoteAKId=a.QuoteAKId
		and c.CurrentSnapshotFlag=1
		where a.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and exists (
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy w
		where w.PolicyGUId=c.QuoteKey)
		order by a.QuoteAKId,a.StatusDate,a.ProductAKId,a.InsuranceReferenceLineOfBusinessAKId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY QuoteAKId,ProductAKId,InsuranceReferenceLineOfBusinessAKId ORDER BY QuoteTransactionAKID) = 1
),
SEQ_QuoteTransactionAKId AS (
	CREATE SEQUENCE SEQ_QuoteTransactionAKId
	START = 1
	INCREMENT = 1;
),
EXP_GetSupportIds AS (
	SELECT
	SEQ_QuoteTransactionAKId.NEXTVAL AS i_NEXTVAL,
	LKP_QuoteTransactionAKId.QuoteTransactionAKID AS i_QuoteTransactionAKID,
	FIL_NewFlag.WrittenPremium AS i_WrittenPremium,
	FIL_NewFlag.RiskGrade AS i_RiskGrade,
	FIL_NewFlag.QuoteAKId AS i_QuoteAKId,
	FIL_NewFlag.StatusDate AS i_StatusDate,
	FIL_NewFlag.ProductAKId AS i_ProductAKId,
	FIL_NewFlag.InsuranceReferenceLineOfBusinessAKId AS i_InsuranceReferenceLineOfBusinessAKId,
	-- *INF*: IIF(i_QuoteAKId=v_prev_QuoteAKId,v_NEXTVAL,i_NEXTVAL)
	IFF(i_QuoteAKId = v_prev_QuoteAKId,
		v_NEXTVAL,
		i_NEXTVAL
	) AS v_NEXTVAL,
	i_QuoteAKId AS v_prev_QuoteAKId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: IIF(ISNULL(i_QuoteTransactionAKID),v_NEXTVAL,i_QuoteTransactionAKID)
	IFF(i_QuoteTransactionAKID IS NULL,
		v_NEXTVAL,
		i_QuoteTransactionAKID
	) AS o_QuoteTransactionAKID,
	i_QuoteAKId AS o_QuoteAKId,
	i_StatusDate AS o_StatusDate,
	i_WrittenPremium AS o_WrittenPremium,
	i_ProductAKId AS o_ProductAKId,
	i_InsuranceReferenceLineOfBusinessAKId AS o_InsuranceReferenceLineOfBusinessAKId,
	i_RiskGrade AS o_RiskGrade
	FROM FIL_NewFlag
	LEFT JOIN LKP_QuoteTransactionAKId
	ON LKP_QuoteTransactionAKId.QuoteAKId = FIL_NewFlag.QuoteAKId AND LKP_QuoteTransactionAKId.ProductAKId = FIL_NewFlag.ProductAKId AND LKP_QuoteTransactionAKId.InsuranceReferenceLineOfBusinessAKId = FIL_NewFlag.InsuranceReferenceLineOfBusinessAKId
),
QuoteTransaction AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.QuoteTransaction
	(AuditID, SourceSystemID, CreatedDate, ModifiedDate, QuoteTransactionAKID, QuoteAKID, StatusDate, WrittenPremium, ProductAKId, InsuranceReferenceLineOfBusinessAKId, RiskGradeCode)
	SELECT 
	o_AuditID AS AUDITID, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_QuoteTransactionAKID AS QUOTETRANSACTIONAKID, 
	o_QuoteAKId AS QUOTEAKID, 
	o_StatusDate AS STATUSDATE, 
	o_WrittenPremium AS WRITTENPREMIUM, 
	o_ProductAKId AS PRODUCTAKID, 
	o_InsuranceReferenceLineOfBusinessAKId AS INSURANCEREFERENCELINEOFBUSINESSAKID, 
	o_RiskGrade AS RISKGRADECODE
	FROM EXP_GetSupportIds
),