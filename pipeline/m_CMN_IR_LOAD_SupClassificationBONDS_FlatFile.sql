WITH
SQ_BondsClass AS (

-- TODO Manual --

),
EXP_Detect_Changes AS (
	SELECT
	LineOfBusinessAbbreviation AS i_LineOfBusinessAbbreviation,
	RatingStateCode AS i_RatingStateCode,
	EffectiveDate AS i_EffectiveDate,
	ExpirationDate AS i_ExpirationDate,
	ClassCode AS i_ClassCode,
	ClassDescription AS i_ClassDescription,
	OriginatingOrganizationCode AS i_OriginatingOrganizationCode,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: LTRIM(RTRIM(i_LineOfBusinessAbbreviation))
	LTRIM(RTRIM(i_LineOfBusinessAbbreviation)) AS o_LineOfBusinessAbbreviation,
	-- *INF*: LTRIM(RTRIM(i_RatingStateCode))
	LTRIM(RTRIM(i_RatingStateCode)) AS o_RatingStateCode,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(i_EffectiveDate))) 
	-- OR IS_SPACES(LTRIM(RTRIM(i_EffectiveDate))) 
	-- OR LENGTH(LTRIM(RTRIM(i_EffectiveDate)))=0
	-- OR LTRIM(RTRIM(SUBSTR( i_EffectiveDate,1,10)))='1900-01-01',
	-- TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),
	-- TO_DATE(SUBSTR( i_EffectiveDate,1,19 )  ,'YYYY-MM-DD HH24:MI:SS'))
	IFF(LTRIM(RTRIM(i_EffectiveDate)) IS NULL OR IS_SPACES(LTRIM(RTRIM(i_EffectiveDate))) OR LENGTH(LTRIM(RTRIM(i_EffectiveDate))) = 0 OR LTRIM(RTRIM(SUBSTR(i_EffectiveDate, 1, 10))) = '1900-01-01', TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), TO_DATE(SUBSTR(i_EffectiveDate, 1, 19), 'YYYY-MM-DD HH24:MI:SS')) AS o_EffectiveDate,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(i_ExpirationDate))) 
	-- OR IS_SPACES(LTRIM(RTRIM(i_ExpirationDate)))
	-- OR LENGTH(LTRIM(RTRIM(i_ExpirationDate)))=0
	-- OR LTRIM(RTRIM(SUBSTR(i_ExpirationDate,1,10)))='2999-01-01',
	-- TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),
	-- TO_DATE(SUBSTR(i_ExpirationDate,1,19 )  ,'YYYY-MM-DD HH24:MI:SS'))
	IFF(LTRIM(RTRIM(i_ExpirationDate)) IS NULL OR IS_SPACES(LTRIM(RTRIM(i_ExpirationDate))) OR LENGTH(LTRIM(RTRIM(i_ExpirationDate))) = 0 OR LTRIM(RTRIM(SUBSTR(i_ExpirationDate, 1, 10))) = '2999-01-01', TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), TO_DATE(SUBSTR(i_ExpirationDate, 1, 19), 'YYYY-MM-DD HH24:MI:SS')) AS o_ExpirationDate,
	-- *INF*: LTRIM(RTRIM(i_ClassCode))
	LTRIM(RTRIM(i_ClassCode)) AS o_ClassCode,
	-- *INF*: LTRIM(RTRIM(i_ClassDescription))
	LTRIM(RTRIM(i_ClassDescription)) AS o_ClassDescription,
	-- *INF*: LTRIM(RTRIM(i_OriginatingOrganizationCode))
	LTRIM(RTRIM(i_OriginatingOrganizationCode)) AS o_OriginatingOrganizationCode
	FROM SQ_BondsClass
),
SupClassificationBonds_IR AS (
	TRUNCATE TABLE SupClassificationBonds;
	INSERT INTO SupClassificationBonds
	(AuditId, CreatedDate, ModifiedDate, LineOfBusinessAbbreviation, RatingStateCode, EffectiveDate, ExpirationDate, ClassCode, ClassDescription, OriginatingOrganizationCode)
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
	o_OriginatingOrganizationCode AS ORIGINATINGORGANIZATIONCODE
	FROM EXP_Detect_Changes
),