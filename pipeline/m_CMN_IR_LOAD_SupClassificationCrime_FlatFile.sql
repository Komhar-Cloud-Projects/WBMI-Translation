WITH
SQ_CrimeClass AS (

-- TODO Manual --

),
EXP_CalculateData AS (
	SELECT
	LineOfBusinessAbbreviation AS i_LineofBusinessAbbreviation,
	RatingStateCode AS i_RatingStateCode,
	EffectiveDate AS i_Effective_Date,
	ExpirationDate AS i_Expiration_Date,
	ClassCode AS i_ClassCode,
	ClassDescription AS i_ClassDescription,
	OriginatingOrganizationCode AS i_ClassCodeOriginatingOrganization,
	IndustryGroup AS i_CrimeIndustryGroup,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate,
	-- *INF*: LTRIM(RTRIM(i_LineofBusinessAbbreviation))
	LTRIM(RTRIM(i_LineofBusinessAbbreviation
		)
	) AS o_LineOfBusinessAbbreviation,
	-- *INF*: LTRIM(RTRIM(i_RatingStateCode))
	LTRIM(RTRIM(i_RatingStateCode
		)
	) AS o_RatingStateCode,
	-- *INF*: TO_DATE(substr(i_Effective_Date,1,19),'YYYY-MM-DD HH24:MI:SS')
	-- --TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
	TO_DATE(substr(i_Effective_Date, 1, 19
		), 'YYYY-MM-DD HH24:MI:SS'
	) AS o_EffectiveDate,
	-- *INF*: TO_DATE(substr(i_Expiration_Date,1,19),'YYYY-MM-DD HH24:MI:SS')
	-- --TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
	TO_DATE(substr(i_Expiration_Date, 1, 19
		), 'YYYY-MM-DD HH24:MI:SS'
	) AS o_ExpirationDate,
	-- *INF*: LTRIM(RTRIM(i_ClassCode))
	LTRIM(RTRIM(i_ClassCode
		)
	) AS o_ClassCode,
	-- *INF*: LTRIM(RTRIM(i_ClassDescription))
	LTRIM(RTRIM(i_ClassDescription
		)
	) AS o_ClassDescription,
	-- *INF*: LTRIM(RTRIM(i_ClassCodeOriginatingOrganization))
	LTRIM(RTRIM(i_ClassCodeOriginatingOrganization
		)
	) AS o_OriginatingOrganizationCode,
	-- *INF*: LTRIM(RTRIM(i_CrimeIndustryGroup))
	LTRIM(RTRIM(i_CrimeIndustryGroup
		)
	) AS o_IndustryGroup
	FROM SQ_CrimeClass
),
SupClassificationCrime_IR AS (
	INSERT INTO SupClassificationCrime
	(AuditId, CreatedDate, ModifiedDate, LineOfBusinessAbbreviation, RatingStateCode, EffectiveDate, ExpirationDate, ClassCode, ClassDescription, OriginatingOrganizationCode, IndustryGroup)
	SELECT 
	o_AuditId AS AUDITID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_LineOfBusinessAbbreviation AS LINEOFBUSINESSABBREVIATION, 
	o_RatingStateCode AS RATINGSTATECODE, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_ClassCode AS CLASSCODE, 
	o_ClassDescription AS CLASSDESCRIPTION, 
	o_OriginatingOrganizationCode AS ORIGINATINGORGANIZATIONCODE, 
	o_IndustryGroup AS INDUSTRYGROUP
	FROM EXP_CalculateData
),