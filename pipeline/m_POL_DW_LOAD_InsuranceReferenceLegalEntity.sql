WITH
SQ_LegalEntity AS (
	SELECT
		LegalEntityId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		LegalEntityAKId,
		LegalEntityCode,
		LegalEntityDescription,
		LegalEntityAbbreviation
	FROM LegalEntity
),
EXP_DateValues AS (
	SELECT
	ModifiedDate AS i_ModifiedDate,
	EffectiveDate AS i_EffectiveDate,
	ExpirationDate AS i_ExpirationDate,
	-- *INF*: IIF(ISNULL(i_EffectiveDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_EffectiveDate)
	IFF(
	    i_EffectiveDate IS NULL, TO_TIMESTAMP('21001231235959', 'YYYYMMDDHH24MISS'), i_EffectiveDate
	) AS o_EffectiveDate,
	-- *INF*: IIF(ISNULL(i_ExpirationDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_ExpirationDate)
	IFF(
	    i_ExpirationDate IS NULL, TO_TIMESTAMP('21001231235959', 'YYYYMMDDHH24MISS'),
	    i_ExpirationDate
	) AS o_ExpirationDate,
	-- *INF*: IIF(ISNULL(i_ModifiedDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_ModifiedDate)
	IFF(
	    i_ModifiedDate IS NULL, TO_TIMESTAMP('21001231235959', 'YYYYMMDDHH24MISS'), i_ModifiedDate
	) AS o_ModifiedDate
	FROM SQ_LegalEntity
),
EXP_NumericValues AS (
	SELECT
	LegalEntityId AS i_LegalEntityId,
	LegalEntityAKId AS i_LegalEntityAKId,
	-- *INF*: IIF(ISNULL(i_LegalEntityId),-1,i_LegalEntityId)
	IFF(i_LegalEntityId IS NULL, - 1, i_LegalEntityId) AS o_LegalEntityId,
	-- *INF*: IIF(ISNULL(i_LegalEntityAKId),-1,i_LegalEntityAKId)
	IFF(i_LegalEntityAKId IS NULL, - 1, i_LegalEntityAKId) AS o_LegalEntityAKId
	FROM SQ_LegalEntity
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	LegalEntityCode AS i_LegalEntityCode,
	LegalEntityDescription AS i_LegalEntityDescription,
	LegalEntityAbbreviation AS i_LegalEntityAbbreviation,
	-- *INF*: IIF(TRUNC(i_ExpirationDate)=TO_DATE('2100-12-31','YYYY-MM-DD'),1,0)
	IFF(TRUNC(i_ExpirationDate) = TO_TIMESTAMP('2100-12-31', 'YYYY-MM-DD'), 1, 0) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_LegalEntityCode) OR LENGTH(i_LegalEntityCode)=0 OR IS_SPACES(i_LegalEntityCode),'N/A',LTRIM(RTRIM(i_LegalEntityCode)))
	IFF(
	    i_LegalEntityCode IS NULL
	    or LENGTH(i_LegalEntityCode) = 0
	    or LENGTH(i_LegalEntityCode)>0
	    and TRIM(i_LegalEntityCode)='',
	    'N/A',
	    LTRIM(RTRIM(i_LegalEntityCode))
	) AS o_LegalEntityCode,
	-- *INF*: IIF(ISNULL(i_LegalEntityDescription) OR LENGTH(i_LegalEntityDescription)=0 OR IS_SPACES(i_LegalEntityDescription),'N/A',LTRIM(RTRIM(i_LegalEntityDescription)))
	IFF(
	    i_LegalEntityDescription IS NULL
	    or LENGTH(i_LegalEntityDescription) = 0
	    or LENGTH(i_LegalEntityDescription)>0
	    and TRIM(i_LegalEntityDescription)='',
	    'N/A',
	    LTRIM(RTRIM(i_LegalEntityDescription))
	) AS o_LegalEntityDescription,
	-- *INF*: IIF(ISNULL(i_LegalEntityAbbreviation) OR LENGTH(i_LegalEntityAbbreviation)=0 OR IS_SPACES(i_LegalEntityAbbreviation),'N/A',LTRIM(RTRIM(i_LegalEntityAbbreviation)))
	IFF(
	    i_LegalEntityAbbreviation IS NULL
	    or LENGTH(i_LegalEntityAbbreviation) = 0
	    or LENGTH(i_LegalEntityAbbreviation)>0
	    and TRIM(i_LegalEntityAbbreviation)='',
	    'N/A',
	    LTRIM(RTRIM(i_LegalEntityAbbreviation))
	) AS o_LegalEntityAbbreviation
	FROM SQ_LegalEntity
),
TGT_InsuranceReferenceLegalEntity_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceLegalEntity AS T
	USING EXP_StringValues AS S
	ON T.InsuranceReferenceLegalEntityId = S.o_LegalEntityId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.InsuranceReferenceLegalEntityAKId = S.o_LegalEntityAKId, T.InsuranceReferenceLegalEntityCode = S.o_LegalEntityCode, T.InsuranceReferenceLegalEntityDescription = S.o_LegalEntityDescription, T.InsuranceReferenceLegalEntityAbbreviation = S.o_LegalEntityAbbreviation
	WHEN NOT MATCHED THEN
	INSERT (InsuranceReferenceLegalEntityId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, InsuranceReferenceLegalEntityAKId, InsuranceReferenceLegalEntityCode, InsuranceReferenceLegalEntityDescription, InsuranceReferenceLegalEntityAbbreviation)
	VALUES (
	EXP_NumericValues.o_LegalEntityId AS INSURANCEREFERENCELEGALENTITYID, 
	EXP_StringValues.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_LegalEntityAKId AS INSURANCEREFERENCELEGALENTITYAKID, 
	EXP_StringValues.o_LegalEntityCode AS INSURANCEREFERENCELEGALENTITYCODE, 
	EXP_StringValues.o_LegalEntityDescription AS INSURANCEREFERENCELEGALENTITYDESCRIPTION, 
	EXP_StringValues.o_LegalEntityAbbreviation AS INSURANCEREFERENCELEGALENTITYABBREVIATION)
),