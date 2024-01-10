WITH
SQ_GenLiabClass AS (

-- TODO Manual --

),
EXP_DefaultValues AS (
	SELECT
	LineOfBusinessAbbreviation AS i_LineofBusinessAbbreviation,
	RatingStateCode AS i_RatingStateCode,
	EffectiveDate AS i_ClassEffectiveDate,
	ExpirationDate AS i_ClassExpirationDate,
	ClassCode AS i_ClassCode,
	ClassDescription AS i_ClassDescription,
	OriginatingOrganizationCode AS i_ClassCodeOriginatingOrganization,
	SublineCode_334 AS i_ISOGLClassGroupPremOp_Subline334,
	SublineCode_336 AS i_ISOGLClassGroupProdCO_Subline336,
	ISOGeneralLiabilityClassSummary_334 AS i_ISOGLClassSummary_334,
	ISOGeneralLiabilityClassSummary_336 AS i_ISOGLClassSummary_336,
	RatingBasis AS i_RatingBasis,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(i_LineofBusinessAbbreviation))) OR IS_SPACES(LTRIM(RTRIM(i_LineofBusinessAbbreviation))) OR LENGTH(LTRIM(RTRIM(i_LineofBusinessAbbreviation)))=0,'N/A',LTRIM(RTRIM(i_LineofBusinessAbbreviation)))
	IFF(LTRIM(RTRIM(i_LineofBusinessAbbreviation)) IS NULL OR IS_SPACES(LTRIM(RTRIM(i_LineofBusinessAbbreviation))) OR LENGTH(LTRIM(RTRIM(i_LineofBusinessAbbreviation))) = 0, 'N/A', LTRIM(RTRIM(i_LineofBusinessAbbreviation))) AS o_LineofBusinessAbbreviation,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(i_RatingStateCode))) OR IS_SPACES(LTRIM(RTRIM(i_RatingStateCode))) OR LENGTH(LTRIM(RTRIM(i_RatingStateCode)))=0,'N/A',LTRIM(RTRIM(i_RatingStateCode)))
	IFF(LTRIM(RTRIM(i_RatingStateCode)) IS NULL OR IS_SPACES(LTRIM(RTRIM(i_RatingStateCode))) OR LENGTH(LTRIM(RTRIM(i_RatingStateCode))) = 0, 'N/A', LTRIM(RTRIM(i_RatingStateCode))) AS o_RatingStateCode,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(i_ClassEffectiveDate))) 
	-- OR IS_SPACES(LTRIM(RTRIM(i_ClassEffectiveDate))) 
	-- OR LENGTH(LTRIM(RTRIM(i_ClassEffectiveDate)))=0
	-- OR LTRIM(RTRIM(i_ClassEffectiveDate))='1900-01-01 01:00:00','1800-01-01 01:00:00',LTRIM(RTRIM(i_ClassEffectiveDate)))
	-- 
	-- 
	-- 
	-- --IIF(ISNULL(i_ClassEffectiveDate) OR i_ClassEffectiveDate=TO_DATE('01/01/1900 00:00:00','MM/DD/YYYY HH24:MI:SS'),'01/01/1800 01:00:00', TO_CHAR(i_ClassEffectiveDate,'MM/DD/YYYY HH24:MI:SS'))
	IFF(LTRIM(RTRIM(i_ClassEffectiveDate)) IS NULL OR IS_SPACES(LTRIM(RTRIM(i_ClassEffectiveDate))) OR LENGTH(LTRIM(RTRIM(i_ClassEffectiveDate))) = 0 OR LTRIM(RTRIM(i_ClassEffectiveDate)) = '1900-01-01 01:00:00', '1800-01-01 01:00:00', LTRIM(RTRIM(i_ClassEffectiveDate))) AS o_ClassEffectiveDate,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(i_ClassExpirationDate))) 
	-- OR IS_SPACES(LTRIM(RTRIM(i_ClassExpirationDate)))
	-- OR LENGTH(LTRIM(RTRIM(i_ClassExpirationDate)))=0
	-- OR LTRIM(RTRIM(i_ClassExpirationDate))='2999-01-01 01:00:00','2100-12-31 23:59:59',LTRIM(RTRIM(i_ClassExpirationDate)))
	-- 
	-- 
	-- 
	-- --IIF(ISNULL(i_ClassExpirationDate) OR i_ClassExpirationDate=TO_DATE('01/01/2999 00:00:00','MM/DD/YYYY HH24:MI:SS'),'12/31/2100 23:59:59', TO_CHAR(i_ClassExpirationDate,'MM/DD/YYYY HH24:MI:SS'))
	IFF(LTRIM(RTRIM(i_ClassExpirationDate)) IS NULL OR IS_SPACES(LTRIM(RTRIM(i_ClassExpirationDate))) OR LENGTH(LTRIM(RTRIM(i_ClassExpirationDate))) = 0 OR LTRIM(RTRIM(i_ClassExpirationDate)) = '2999-01-01 01:00:00', '2100-12-31 23:59:59', LTRIM(RTRIM(i_ClassExpirationDate))) AS o_ClassExpirationDate,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(i_ClassCode))) OR IS_SPACES(LTRIM(RTRIM(i_ClassCode))) OR LENGTH(LTRIM(RTRIM(i_ClassCode)))=0,'N/A',LTRIM(RTRIM(i_ClassCode)))
	IFF(LTRIM(RTRIM(i_ClassCode)) IS NULL OR IS_SPACES(LTRIM(RTRIM(i_ClassCode))) OR LENGTH(LTRIM(RTRIM(i_ClassCode))) = 0, 'N/A', LTRIM(RTRIM(i_ClassCode))) AS o_ClassCode,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(i_ClassDescription))) OR IS_SPACES(LTRIM(RTRIM(i_ClassDescription))) OR LENGTH(LTRIM(RTRIM(i_ClassDescription)))=0,'N/A',LTRIM(RTRIM(i_ClassDescription)))
	IFF(LTRIM(RTRIM(i_ClassDescription)) IS NULL OR IS_SPACES(LTRIM(RTRIM(i_ClassDescription))) OR LENGTH(LTRIM(RTRIM(i_ClassDescription))) = 0, 'N/A', LTRIM(RTRIM(i_ClassDescription))) AS o_ClassDescription,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(i_ClassCodeOriginatingOrganization))) OR IS_SPACES(LTRIM(RTRIM(i_ClassCodeOriginatingOrganization))) OR LENGTH(LTRIM(RTRIM(i_ClassCodeOriginatingOrganization)))=0,'N/A',LTRIM(RTRIM(i_ClassCodeOriginatingOrganization)))
	IFF(LTRIM(RTRIM(i_ClassCodeOriginatingOrganization)) IS NULL OR IS_SPACES(LTRIM(RTRIM(i_ClassCodeOriginatingOrganization))) OR LENGTH(LTRIM(RTRIM(i_ClassCodeOriginatingOrganization))) = 0, 'N/A', LTRIM(RTRIM(i_ClassCodeOriginatingOrganization))) AS o_ClassCodeOriginatingOrganization,
	'334' AS o_Subline_334,
	'336' AS o_Subline_336,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(i_ISOGLClassGroupPremOp_Subline334))) OR IS_SPACES(LTRIM(RTRIM(i_ISOGLClassGroupPremOp_Subline334))) OR LENGTH(LTRIM(RTRIM(i_ISOGLClassGroupPremOp_Subline334)))=0,'N/A',LTRIM(RTRIM(i_ISOGLClassGroupPremOp_Subline334)))
	IFF(LTRIM(RTRIM(i_ISOGLClassGroupPremOp_Subline334)) IS NULL OR IS_SPACES(LTRIM(RTRIM(i_ISOGLClassGroupPremOp_Subline334))) OR LENGTH(LTRIM(RTRIM(i_ISOGLClassGroupPremOp_Subline334))) = 0, 'N/A', LTRIM(RTRIM(i_ISOGLClassGroupPremOp_Subline334))) AS o_ISOGLClassGroupPremOp_Subline334,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(i_ISOGLClassGroupProdCO_Subline336))) OR IS_SPACES(LTRIM(RTRIM(i_ISOGLClassGroupProdCO_Subline336))) OR LENGTH(LTRIM(RTRIM(i_ISOGLClassGroupProdCO_Subline336)))=0,'N/A',LTRIM(RTRIM(i_ISOGLClassGroupProdCO_Subline336)))
	IFF(LTRIM(RTRIM(i_ISOGLClassGroupProdCO_Subline336)) IS NULL OR IS_SPACES(LTRIM(RTRIM(i_ISOGLClassGroupProdCO_Subline336))) OR LENGTH(LTRIM(RTRIM(i_ISOGLClassGroupProdCO_Subline336))) = 0, 'N/A', LTRIM(RTRIM(i_ISOGLClassGroupProdCO_Subline336))) AS o_ISOGLClassGroupProdCO_Subline336,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(i_ISOGLClassSummary_334))) OR IS_SPACES(LTRIM(RTRIM(i_ISOGLClassSummary_334))) OR LENGTH(LTRIM(RTRIM(i_ISOGLClassSummary_334)))=0,'N/A',LTRIM(RTRIM(i_ISOGLClassSummary_334)))
	IFF(LTRIM(RTRIM(i_ISOGLClassSummary_334)) IS NULL OR IS_SPACES(LTRIM(RTRIM(i_ISOGLClassSummary_334))) OR LENGTH(LTRIM(RTRIM(i_ISOGLClassSummary_334))) = 0, 'N/A', LTRIM(RTRIM(i_ISOGLClassSummary_334))) AS o_ISOGLClassSummary_334,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(i_ISOGLClassSummary_336))) OR IS_SPACES(LTRIM(RTRIM(i_ISOGLClassSummary_336))) OR LENGTH(LTRIM(RTRIM(i_ISOGLClassSummary_336)))=0,'N/A',LTRIM(RTRIM(i_ISOGLClassSummary_336)))
	IFF(LTRIM(RTRIM(i_ISOGLClassSummary_336)) IS NULL OR IS_SPACES(LTRIM(RTRIM(i_ISOGLClassSummary_336))) OR LENGTH(LTRIM(RTRIM(i_ISOGLClassSummary_336))) = 0, 'N/A', LTRIM(RTRIM(i_ISOGLClassSummary_336))) AS o_ISOGLClassSummary_336,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(i_RatingBasis))) OR IS_SPACES(LTRIM(RTRIM(i_RatingBasis))) OR LENGTH(LTRIM(RTRIM(i_RatingBasis)))=0,'N/A',LTRIM(RTRIM(i_RatingBasis)))
	IFF(LTRIM(RTRIM(i_RatingBasis)) IS NULL OR IS_SPACES(LTRIM(RTRIM(i_RatingBasis))) OR LENGTH(LTRIM(RTRIM(i_RatingBasis))) = 0, 'N/A', LTRIM(RTRIM(i_RatingBasis))) AS o_RatingBasis
	FROM SQ_GenLiabClass
),
NRM_ClassGroup_ClassSummary AS (
),
EXP_CalculateData AS (
	SELECT
	LineofBusinessAbbreviation,
	RatingStateCode,
	ClassEffectiveDate,
	ClassExpirationDate,
	ClassCode,
	ClassDescription,
	ClassCodeOriginatingOrganization,
	Subline,
	ISOGLClassGroup,
	ISOGLClassSummary,
	RatingBasis,
	LineofBusinessAbbreviation AS o_LineofBusinessAbbreviation,
	-- *INF*: TO_DATE(SUBSTR(ClassEffectiveDate,1,19),'YYYY-MM-DD HH24:MI:SS')
	TO_DATE(SUBSTR(ClassEffectiveDate, 1, 19), 'YYYY-MM-DD HH24:MI:SS') AS o_ClassEffectiveDate,
	-- *INF*: TO_DATE(SUBSTR(ClassExpirationDate,1,19),'YYYY-MM-DD HH24:MI:SS')
	TO_DATE(SUBSTR(ClassExpirationDate, 1, 19), 'YYYY-MM-DD HH24:MI:SS') AS o_ClassExpirationDate,
	-- *INF*: LTRIM(RTRIM(RatingStateCode))
	LTRIM(RTRIM(RatingStateCode)) AS o_RatingStateCode,
	Subline AS o_Subline,
	ClassCode AS o_ClassCode,
	-- *INF*: LTRIM(RTRIM(REPLACESTR(1,ClassDescription,'"','')))
	LTRIM(RTRIM(REPLACESTR(1, ClassDescription, '"', ''))) AS o_ClassDescription,
	-- *INF*: LTRIM(RTRIM(ClassCodeOriginatingOrganization))
	LTRIM(RTRIM(ClassCodeOriginatingOrganization)) AS o_ClassCodeOriginatingOrganization,
	-- *INF*: LTRIM(RTRIM(IIF(ISOGLClassGroup!='N/A',LPAD(ISOGLClassGroup,2,'0'),ISOGLClassGroup)))
	LTRIM(RTRIM(IFF(ISOGLClassGroup != 'N/A', LPAD(ISOGLClassGroup, 2, '0'), ISOGLClassGroup))) AS o_ISOGLClassGroup,
	-- *INF*: LTRIM(RTRIM(REPLACESTR(1,ISOGLClassSummary,'"','')))
	LTRIM(RTRIM(REPLACESTR(1, ISOGLClassSummary, '"', ''))) AS o_ISOGLClassSummary,
	-- *INF*: LTRIM(RTRIM(RatingBasis))
	LTRIM(RTRIM(RatingBasis)) AS o_RatingBasis
	FROM NRM_ClassGroup_ClassSummary
),
FIL_ValidClassGroup AS (
	SELECT
	o_LineofBusinessAbbreviation AS LineofBusinessAbbreviation, 
	o_ClassEffectiveDate AS ClassEffectiveDate, 
	o_ClassExpirationDate AS ClassExpirationDate, 
	o_RatingStateCode AS RatingStateCode, 
	o_Subline AS Subline, 
	o_ClassCode AS ClassCode, 
	o_ClassDescription AS ClassDescription, 
	o_ClassCodeOriginatingOrganization AS ClassCodeOriginatingOrganization, 
	o_ISOGLClassGroup AS ISOGLClassGroup, 
	o_ISOGLClassSummary AS ISOGLClassSummary, 
	o_RatingBasis AS RatingBasis
	FROM EXP_CalculateData
	WHERE 1=1

--NOT (ISOGLClassGroup='N/A' and ISOGLClassSummary='N/A')
),
AGG_RemoveDuplicate AS (
	SELECT
	LineofBusinessAbbreviation,
	ClassEffectiveDate,
	ClassExpirationDate,
	RatingStateCode,
	Subline,
	ClassCode,
	ClassDescription,
	ClassCodeOriginatingOrganization,
	ISOGLClassGroup,
	ISOGLClassSummary,
	RatingBasis
	FROM FIL_ValidClassGroup
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LineofBusinessAbbreviation, ClassEffectiveDate, ClassExpirationDate, RatingStateCode, Subline, ClassCode, ClassDescription, ClassCodeOriginatingOrganization, ISOGLClassGroup, ISOGLClassSummary, RatingBasis ORDER BY NULL) = 1
),
EXP_UpdateOrInsert AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	LineofBusinessAbbreviation,
	RatingStateCode,
	ClassEffectiveDate AS EffectiveDate,
	ClassExpirationDate AS ExpirationDate,
	ClassCode,
	ClassDescription,
	ClassCodeOriginatingOrganization,
	Subline,
	ISOGLClassSummary,
	ISOGLClassGroup,
	RatingBasis
	FROM AGG_RemoveDuplicate
),
SupClassificationGeneralLiability AS (
	TRUNCATE TABLE SupClassificationGeneralLiability;
	INSERT INTO SupClassificationGeneralLiability
	(AuditId, CreatedDate, ModifiedDate, LineOfBusinessAbbreviation, RatingStateCode, EffectiveDate, ExpirationDate, ClassCode, ClassDescription, OriginatingOrganizationCode, SublineCode, ISOGeneralLiabilityClassSummary, ISOGeneralLiabilityClassGroupCode, RatingBasis)
	SELECT 
	o_AuditId AS AUDITID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	LineofBusinessAbbreviation AS LINEOFBUSINESSABBREVIATION, 
	RATINGSTATECODE, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CLASSCODE, 
	CLASSDESCRIPTION, 
	ClassCodeOriginatingOrganization AS ORIGINATINGORGANIZATIONCODE, 
	Subline AS SUBLINECODE, 
	ISOGLClassSummary AS ISOGENERALLIABILITYCLASSSUMMARY, 
	ISOGLClassGroup AS ISOGENERALLIABILITYCLASSGROUPCODE, 
	RATINGBASIS
	FROM EXP_UpdateOrInsert
),