WITH
SQ_CommPropClass AS (

-- TODO Manual --

),
EXP_DefaultValues AS (
	SELECT
	Line_of_Business_Abbreviation AS LineofBusinessAbbreviation,
	Rating_State_Code AS RatingStateCode,
	Class_Effective_Date AS ClassEffectiveDate,
	Class_Expiration_Date AS ClassExpirationDate,
	Class_Code AS ClassCode,
	Class_Description AS ClassDescription,
	Class_Code_Originating_Organization AS ClassCodeOriginatingOrganization,
	ISO_CP_Rating_Group AS ISOCPRatingGroup,
	Property_Special_Class AS PropertySpecialClass,
	Notes,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(LineofBusinessAbbreviation))) OR IS_SPACES(LTRIM(RTRIM(LineofBusinessAbbreviation))) OR LENGTH(LTRIM(RTRIM(LineofBusinessAbbreviation)))=0,'N/A',LTRIM(RTRIM(LineofBusinessAbbreviation)))
	IFF(
	    LTRIM(RTRIM(LineofBusinessAbbreviation)) IS NULL
	    or LENGTH(LTRIM(RTRIM(LineofBusinessAbbreviation)))>0
	    and TRIM(LTRIM(RTRIM(LineofBusinessAbbreviation)))=''
	    or LENGTH(LTRIM(RTRIM(LineofBusinessAbbreviation))) = 0,
	    'N/A',
	    LTRIM(RTRIM(LineofBusinessAbbreviation))
	) AS o_LineofBusinessAbbreviation,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(RatingStateCode))) OR IS_SPACES(LTRIM(RTRIM(RatingStateCode))) OR LENGTH(LTRIM(RTRIM(RatingStateCode)))=0,'N/A',LTRIM(RTRIM(RatingStateCode)))
	IFF(
	    LTRIM(RTRIM(RatingStateCode)) IS NULL
	    or LENGTH(LTRIM(RTRIM(RatingStateCode)))>0
	    and TRIM(LTRIM(RTRIM(RatingStateCode)))=''
	    or LENGTH(LTRIM(RTRIM(RatingStateCode))) = 0,
	    'N/A',
	    LTRIM(RTRIM(RatingStateCode))
	) AS o_RatingStateCode,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(ClassEffectiveDate))) 
	-- OR IS_SPACES(LTRIM(RTRIM(ClassEffectiveDate))) 
	-- OR LENGTH(LTRIM(RTRIM(ClassEffectiveDate)))=0
	-- OR LTRIM(RTRIM(ClassEffectiveDate))='1900-01-01', TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),TO_DATE(LTRIM(RTRIM(ClassEffectiveDate)), 'YYYY-MM-DD'))
	-- 
	-- 
	-- 
	-- --IIF(ISNULL(i_ClassEffectiveDate) OR i_ClassEffectiveDate=TO_DATE('01/01/1900 00:00:00','MM/DD/YYYY HH24:MI:SS'),'01/01/1800 01:00:00', TO_CHAR(i_ClassEffectiveDate,'MM/DD/YYYY HH24:MI:SS'))
	IFF(
	    LTRIM(RTRIM(ClassEffectiveDate)) IS NULL
	    or LENGTH(LTRIM(RTRIM(ClassEffectiveDate)))>0
	    and TRIM(LTRIM(RTRIM(ClassEffectiveDate)))=''
	    or LENGTH(LTRIM(RTRIM(ClassEffectiveDate))) = 0
	    or LTRIM(RTRIM(ClassEffectiveDate)) = '1900-01-01',
	    TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    TO_TIMESTAMP(LTRIM(RTRIM(ClassEffectiveDate)), 'YYYY-MM-DD')
	) AS o_ClassEffectiveDate,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(ClassExpirationDate))) 
	-- OR IS_SPACES(LTRIM(RTRIM(ClassExpirationDate)))
	-- OR LENGTH(LTRIM(RTRIM(ClassExpirationDate)))=0
	-- OR LTRIM(RTRIM(ClassExpirationDate))='2999-01-01', TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS'),TO_DATE(LTRIM(RTRIM(ClassExpirationDate)), 'YYYY-MM-DD'))
	-- 
	-- 
	-- 
	-- --IIF(ISNULL(i_ClassExpirationDate) OR i_ClassExpirationDate=TO_DATE('01/01/2999 00:00:00','MM/DD/YYYY HH24:MI:SS'),'12/31/2100 23:59:59', TO_CHAR(i_ClassExpirationDate,'MM/DD/YYYY HH24:MI:SS'))
	IFF(
	    LTRIM(RTRIM(ClassExpirationDate)) IS NULL
	    or LENGTH(LTRIM(RTRIM(ClassExpirationDate)))>0
	    and TRIM(LTRIM(RTRIM(ClassExpirationDate)))=''
	    or LENGTH(LTRIM(RTRIM(ClassExpirationDate))) = 0
	    or LTRIM(RTRIM(ClassExpirationDate)) = '2999-01-01',
	    TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'),
	    TO_TIMESTAMP(LTRIM(RTRIM(ClassExpirationDate)), 'YYYY-MM-DD')
	) AS o_ClassExpirationDate,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(ClassCode))) OR IS_SPACES(LTRIM(RTRIM(ClassCode))) OR LENGTH(LTRIM(RTRIM(ClassCode)))=0,'N/A',LTRIM(RTRIM(ClassCode)))
	IFF(
	    LTRIM(RTRIM(ClassCode)) IS NULL
	    or LENGTH(LTRIM(RTRIM(ClassCode)))>0
	    and TRIM(LTRIM(RTRIM(ClassCode)))=''
	    or LENGTH(LTRIM(RTRIM(ClassCode))) = 0,
	    'N/A',
	    LTRIM(RTRIM(ClassCode))
	) AS o_ClassCode,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(ClassDescription))) OR IS_SPACES(LTRIM(RTRIM(ClassDescription))) OR LENGTH(LTRIM(RTRIM(ClassDescription)))=0,'N/A',LTRIM(RTRIM(ClassDescription)))
	IFF(
	    LTRIM(RTRIM(ClassDescription)) IS NULL
	    or LENGTH(LTRIM(RTRIM(ClassDescription)))>0
	    and TRIM(LTRIM(RTRIM(ClassDescription)))=''
	    or LENGTH(LTRIM(RTRIM(ClassDescription))) = 0,
	    'N/A',
	    LTRIM(RTRIM(ClassDescription))
	) AS o_ClassDescription,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(ClassCodeOriginatingOrganization))) OR IS_SPACES(LTRIM(RTRIM(ClassCodeOriginatingOrganization))) OR LENGTH(LTRIM(RTRIM(ClassCodeOriginatingOrganization)))=0,'N/A',LTRIM(RTRIM(ClassCodeOriginatingOrganization)))
	IFF(
	    LTRIM(RTRIM(ClassCodeOriginatingOrganization)) IS NULL
	    or LENGTH(LTRIM(RTRIM(ClassCodeOriginatingOrganization)))>0
	    and TRIM(LTRIM(RTRIM(ClassCodeOriginatingOrganization)))=''
	    or LENGTH(LTRIM(RTRIM(ClassCodeOriginatingOrganization))) = 0,
	    'N/A',
	    LTRIM(RTRIM(ClassCodeOriginatingOrganization))
	) AS o_ClassCodeOriginatingOrganization,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(ISOCPRatingGroup))) OR IS_SPACES(LTRIM(RTRIM(ISOCPRatingGroup))) OR LENGTH(LTRIM(RTRIM(ISOCPRatingGroup)))=0,'N/A',LTRIM(RTRIM(ISOCPRatingGroup)))
	IFF(
	    LTRIM(RTRIM(ISOCPRatingGroup)) IS NULL
	    or LENGTH(LTRIM(RTRIM(ISOCPRatingGroup)))>0
	    and TRIM(LTRIM(RTRIM(ISOCPRatingGroup)))=''
	    or LENGTH(LTRIM(RTRIM(ISOCPRatingGroup))) = 0,
	    'N/A',
	    LTRIM(RTRIM(ISOCPRatingGroup))
	) AS o_ISOCPRatingGroup,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(PropertySpecialClass))) OR IS_SPACES(LTRIM(RTRIM(PropertySpecialClass))) OR LENGTH(LTRIM(RTRIM(PropertySpecialClass)))=0,'N/A',LTRIM(RTRIM(PropertySpecialClass)))
	IFF(
	    LTRIM(RTRIM(PropertySpecialClass)) IS NULL
	    or LENGTH(LTRIM(RTRIM(PropertySpecialClass)))>0
	    and TRIM(LTRIM(RTRIM(PropertySpecialClass)))=''
	    or LENGTH(LTRIM(RTRIM(PropertySpecialClass))) = 0,
	    'N/A',
	    LTRIM(RTRIM(PropertySpecialClass))
	) AS o_PropertySpecialClass,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(Notes))) OR IS_SPACES(LTRIM(RTRIM(Notes))) OR LENGTH(LTRIM(RTRIM(Notes)))=0,'N/A',LTRIM(RTRIM(Notes)))
	IFF(
	    LTRIM(RTRIM(Notes)) IS NULL
	    or LENGTH(LTRIM(RTRIM(Notes)))>0
	    and TRIM(LTRIM(RTRIM(Notes)))=''
	    or LENGTH(LTRIM(RTRIM(Notes))) = 0,
	    'N/A',
	    LTRIM(RTRIM(Notes))
	) AS o_Notes
	FROM SQ_CommPropClass
),
EXP_CalculateData AS (
	SELECT
	o_LineofBusinessAbbreviation AS i_LineofBusinessAbbreviation,
	o_RatingStateCode AS i_RatingStateCode,
	o_ClassEffectiveDate AS i_ClassEffectiveDate,
	o_ClassExpirationDate AS i_ClassExpirationDate,
	o_ClassCode AS i_ClassCode,
	o_ClassDescription AS i_ClassDescription,
	o_ClassCodeOriginatingOrganization AS i_ClassCodeOriginatingOrganization,
	o_ISOCPRatingGroup AS i_ISOCPRatingGroup,
	o_PropertySpecialClass AS i_PropertySpecialClass,
	o_Notes AS i_Notes,
	-- *INF*: LTRIM(RTRIM(i_LineofBusinessAbbreviation))
	LTRIM(RTRIM(i_LineofBusinessAbbreviation)) AS o_LineofBusinessAbbreviation,
	i_ClassEffectiveDate AS o_ClassEffectiveDate,
	i_ClassExpirationDate AS o_ClassExpirationDate,
	i_RatingStateCode AS o_RatingStateCode,
	-- *INF*: i_ClassCode
	-- 
	-- 
	-- 
	-- --LPAD(i_ClassCode,6,'0')
	i_ClassCode AS o_ClassCode,
	-- *INF*: LTRIM(RTRIM(REPLACESTR(1,i_ClassDescription,'"','')))
	LTRIM(RTRIM(REGEXP_REPLACE(i_ClassDescription,'"',''))) AS o_ClassDescription,
	-- *INF*: LTRIM(RTRIM(i_ClassCodeOriginatingOrganization))
	LTRIM(RTRIM(i_ClassCodeOriginatingOrganization)) AS o_ClassCodeOriginatingOrganization,
	-- *INF*: LTRIM(RTRIM(IIF(i_ISOCPRatingGroup!='N/A',LPAD(i_ISOCPRatingGroup,2,'0'),i_ISOCPRatingGroup)))
	LTRIM(RTRIM(
	        IFF(
	            i_ISOCPRatingGroup != 'N/A', LPAD(i_ISOCPRatingGroup, 2, '0'),
	            i_ISOCPRatingGroup
	        ))) AS o_ISOCPRatingGroup,
	-- *INF*: LTRIM(RTRIM(IIF(i_PropertySpecialClass!='N/A',LPAD(i_PropertySpecialClass,2,'0'),i_PropertySpecialClass)))
	LTRIM(RTRIM(
	        IFF(
	            i_PropertySpecialClass != 'N/A', LPAD(i_PropertySpecialClass, 2, '0'),
	            i_PropertySpecialClass
	        ))) AS o_PropertySpecialClass
	FROM EXP_DefaultValues
),
EXP_UpdateOrInsert AS (
	SELECT
	o_ClassEffectiveDate AS ClassEffectiveDate,
	o_ClassExpirationDate AS ClassExpirationDate,
	o_LineofBusinessAbbreviation AS LineofBusinessAbbreviation,
	o_RatingStateCode AS RatingStateCode,
	o_ClassCode AS ClassCode,
	o_ClassDescription AS ClassDescription,
	o_ClassCodeOriginatingOrganization AS ClassCodeOriginatingOrganization,
	o_ISOCPRatingGroup AS ISOCPRatingGroup,
	o_PropertySpecialClass AS PropertySpecialClass,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate
	FROM EXP_CalculateData
),
SupClassificationCommercialProperty_IR AS (
	TRUNCATE TABLE SupClassificationCommercialProperty;
	INSERT INTO SupClassificationCommercialProperty
	(AuditId, CreatedDate, ModifiedDate, LineOfBusinessAbbreviation, RatingStateCode, EffectiveDate, ExpirationDate, ClassCode, ClassDescription, OriginatingOrganizationCode, ISOCPRatingGroup, CommercialPropertySpecialClass)
	SELECT 
	o_AuditId AS AUDITID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	LineofBusinessAbbreviation AS LINEOFBUSINESSABBREVIATION, 
	RATINGSTATECODE, 
	ClassEffectiveDate AS EFFECTIVEDATE, 
	ClassExpirationDate AS EXPIRATIONDATE, 
	CLASSCODE, 
	CLASSDESCRIPTION, 
	ClassCodeOriginatingOrganization AS ORIGINATINGORGANIZATIONCODE, 
	ISOCPRATINGGROUP, 
	PropertySpecialClass AS COMMERCIALPROPERTYSPECIALCLASS
	FROM EXP_UpdateOrInsert
),