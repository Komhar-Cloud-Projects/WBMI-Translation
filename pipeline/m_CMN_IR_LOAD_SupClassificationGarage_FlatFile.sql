WITH
SQ_GarageClass AS (

-- TODO Manual --

),
EXP_MetaData AS (
	SELECT
	LineOfBusinessAbbreviation AS LineofBusinessAbbreviation,
	RatingStateCode,
	EffectiveDate AS ClassEffectiveDate,
	ExpirationDate AS ClassExpirationDate,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode AS ClassCodeOriginatingOrganization,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_Auditid,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate,
	-- *INF*: LTRIM(RTRIM(LineofBusinessAbbreviation))
	LTRIM(RTRIM(LineofBusinessAbbreviation)) AS o_LineofBusinessAbbreviation,
	-- *INF*: LTRIM(RTRIM(RatingStateCode))
	LTRIM(RTRIM(RatingStateCode)) AS o_RatingStateCode,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(ClassEffectiveDate))) 
	-- OR IS_SPACES(LTRIM(RTRIM(ClassEffectiveDate))) 
	-- OR LENGTH(LTRIM(RTRIM(ClassEffectiveDate)))=0
	-- OR LTRIM(RTRIM(SUBSTR(ClassEffectiveDate,1,10)))='1900-01-01', TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),
	-- TO_DATE( SUBSTR( ClassEffectiveDate ,1,19 )  ,'YYYY-MM-DD HH24:MI:SS'))
	IFF(
	    LTRIM(RTRIM(ClassEffectiveDate)) IS NULL
	    or LENGTH(LTRIM(RTRIM(ClassEffectiveDate)))>0
	    and TRIM(LTRIM(RTRIM(ClassEffectiveDate)))=''
	    or LENGTH(LTRIM(RTRIM(ClassEffectiveDate))) = 0
	    or LTRIM(RTRIM(SUBSTR(ClassEffectiveDate, 1, 10))) = '1900-01-01',
	    TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
	    TO_TIMESTAMP(SUBSTR(ClassEffectiveDate, 1, 19), 'YYYY-MM-DD HH24:MI:SS')
	) AS o_ClassEffectiveDate,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(ClassExpirationDate))) 
	-- OR IS_SPACES(LTRIM(RTRIM(ClassExpirationDate)))
	-- OR LENGTH(LTRIM(RTRIM(ClassExpirationDate)))=0
	-- OR LTRIM(RTRIM(SUBSTR(ClassExpirationDate,1,10)))='2999-01-01', TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),
	-- TO_DATE(SUBSTR(ClassExpirationDate , 1,19  ),'YYYY-MM-DD HH24:MI:SS'))
	-- 
	IFF(
	    LTRIM(RTRIM(ClassExpirationDate)) IS NULL
	    or LENGTH(LTRIM(RTRIM(ClassExpirationDate)))>0
	    and TRIM(LTRIM(RTRIM(ClassExpirationDate)))=''
	    or LENGTH(LTRIM(RTRIM(ClassExpirationDate))) = 0
	    or LTRIM(RTRIM(SUBSTR(ClassExpirationDate, 1, 10))) = '2999-01-01',
	    TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
	    TO_TIMESTAMP(SUBSTR(ClassExpirationDate, 1, 19), 'YYYY-MM-DD HH24:MI:SS')
	) AS o_ClassExpirationDate,
	-- *INF*: LTRIM(RTRIM(ClassCode))
	LTRIM(RTRIM(ClassCode)) AS o_ClassCode,
	-- *INF*: LTRIM(RTRIM(ClassDescription))
	LTRIM(RTRIM(ClassDescription)) AS o_ClassDescription,
	-- *INF*: LTRIM(RTRIM(ClassCodeOriginatingOrganization))
	LTRIM(RTRIM(ClassCodeOriginatingOrganization)) AS o_ClassCodeOriginatingOrganization
	FROM SQ_GarageClass
),
SupClassificationGarage_IR AS (
	TRUNCATE TABLE SupClassificationGarage;
	INSERT INTO SupClassificationGarage
	(AuditId, CreatedDate, ModifiedDate, LineOfBusinessAbbreviation, RatingStateCode, EffectiveDate, ExpirationDate, ClassCode, ClassDescription, OriginatingOrganizationCode)
	SELECT 
	o_Auditid AS AUDITID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_LineofBusinessAbbreviation AS LINEOFBUSINESSABBREVIATION, 
	o_RatingStateCode AS RATINGSTATECODE, 
	o_ClassEffectiveDate AS EFFECTIVEDATE, 
	o_ClassExpirationDate AS EXPIRATIONDATE, 
	o_ClassCode AS CLASSCODE, 
	o_ClassDescription AS CLASSDESCRIPTION, 
	o_ClassCodeOriginatingOrganization AS ORIGINATINGORGANIZATIONCODE
	FROM EXP_MetaData
),