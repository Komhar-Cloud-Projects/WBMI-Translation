WITH
SQ_DirectorsOfficersClass AS (

-- TODO Manual --

),
EXP_MetaData AS (
	SELECT
	LineOfBusinessAbbreviation AS i_LineOfBusinessAbbreviation,
	RatingStateCode AS i_RatingStateCode,
	EffectiveDate AS i_ClassEffectiveDate,
	ExpirationDate AS i_ClassExpirationDate,
	ClassCode AS i_ClassCode,
	ClassDescription AS i_ClassDescription,
	OriginatingOrganizationCode AS i_ClassCodeOriginatingOrganization,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate,
	-- *INF*: LTRIM(RTRIM(i_LineOfBusinessAbbreviation))
	LTRIM(RTRIM(i_LineOfBusinessAbbreviation
		)
	) AS o_LineOfBusinessAbbreviation,
	-- *INF*: LTRIM(RTRIM(i_RatingStateCode))
	LTRIM(RTRIM(i_RatingStateCode
		)
	) AS o_RatingStateCode,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(i_ClassEffectiveDate))) 
	-- OR IS_SPACES(LTRIM(RTRIM(i_ClassEffectiveDate))) 
	-- OR LENGTH(LTRIM(RTRIM(i_ClassEffectiveDate)))=0
	-- OR LTRIM(RTRIM(SUBSTR( i_ClassEffectiveDate,1,10)))='1900-01-01',
	-- TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),
	-- TO_DATE(  SUBSTR( i_ClassEffectiveDate ,1,19 )  ,'YYYY-MM-DD HH24:MI:SS'))
	IFF(LTRIM(RTRIM(i_ClassEffectiveDate
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(i_ClassEffectiveDate
			)
		))>0 AND TRIM(LTRIM(RTRIM(i_ClassEffectiveDate
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(i_ClassEffectiveDate
				)
			)
		) = 0 
		OR LTRIM(RTRIM(SUBSTR(i_ClassEffectiveDate, 1, 10
				)
			)
		) = '1900-01-01',
		TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'
		),
		TO_DATE(SUBSTR(i_ClassEffectiveDate, 1, 19
			), 'YYYY-MM-DD HH24:MI:SS'
		)
	) AS o_ClassEffectiveDate,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(i_ClassExpirationDate))) 
	-- OR IS_SPACES(LTRIM(RTRIM(i_ClassExpirationDate)))
	-- OR LENGTH(LTRIM(RTRIM(i_ClassExpirationDate)))=0
	-- OR LTRIM(RTRIM(SUBSTR(i_ClassExpirationDate,1,10)))='2999-01-01',
	-- TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),
	-- TO_DATE(SUBSTR(i_ClassExpirationDate , 1,19  ),'YYYY-MM-DD HH24:MI:SS'))
	IFF(LTRIM(RTRIM(i_ClassExpirationDate
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(i_ClassExpirationDate
			)
		))>0 AND TRIM(LTRIM(RTRIM(i_ClassExpirationDate
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(i_ClassExpirationDate
				)
			)
		) = 0 
		OR LTRIM(RTRIM(SUBSTR(i_ClassExpirationDate, 1, 10
				)
			)
		) = '2999-01-01',
		TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'
		),
		TO_DATE(SUBSTR(i_ClassExpirationDate, 1, 19
			), 'YYYY-MM-DD HH24:MI:SS'
		)
	) AS o_ClassExpirationDate,
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
	) AS o_ClassCodeOriginatingOrganization
	FROM SQ_DirectorsOfficersClass
),
SupClassificationDirectorsOfficers_IR AS (
	TRUNCATE TABLE SupClassificationDirectorsOfficers;
	INSERT INTO SupClassificationDirectorsOfficers
	(AuditId, CreatedDate, ModifiedDate, LineOfBusinessAbbreviation, RatingStateCode, EffectiveDate, ExpirationDate, ClassCode, ClassDescription, OriginatingOrganizationCode)
	SELECT 
	o_AuditId AS AUDITID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_LineOfBusinessAbbreviation AS LINEOFBUSINESSABBREVIATION, 
	o_RatingStateCode AS RATINGSTATECODE, 
	o_ClassEffectiveDate AS EFFECTIVEDATE, 
	o_ClassExpirationDate AS EXPIRATIONDATE, 
	o_ClassCode AS CLASSCODE, 
	o_ClassDescription AS CLASSDESCRIPTION, 
	o_ClassCodeOriginatingOrganization AS ORIGINATINGORGANIZATIONCODE
	FROM EXP_MetaData
),