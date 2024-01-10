WITH
SQ_InsuranceSegment AS (
	SELECT
		InsuranceSegmentId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		InsuranceSegmentAKId,
		InsuranceSegmentCode,
		InsuranceSegmentAbbreviation,
		InsuranceSegmentDescription
	FROM InsuranceSegment
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
	FROM SQ_InsuranceSegment
),
EXP_NumericValues AS (
	SELECT
	InsuranceSegmentId AS i_InsuranceSegmentId,
	InsuranceSegmentAKId AS i_InsuranceSegmentAKId,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentId),-1,i_InsuranceSegmentId)
	IFF(i_InsuranceSegmentId IS NULL,
		- 1,
		i_InsuranceSegmentId
	) AS o_InsuranceSegmentId,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentAKId),-1,i_InsuranceSegmentAKId)
	IFF(i_InsuranceSegmentAKId IS NULL,
		- 1,
		i_InsuranceSegmentAKId
	) AS o_InsuranceSegmentAKId
	FROM SQ_InsuranceSegment
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	InsuranceSegmentCode AS i_InsuranceSegmentCode,
	InsuranceSegmentAbbreviation AS i_InsuranceSegmentAbbreviation,
	InsuranceSegmentDescription AS i_InsuranceSegmentDescription,
	-- *INF*: IIF(TRUNC(i_ExpirationDate)=TO_DATE('2100-12-31','YYYY-MM-DD'),1,0)
	IFF(TRUNC(i_ExpirationDate) = TO_DATE('2100-12-31', 'YYYY-MM-DD'
		),
		1,
		0
	) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentCode) OR LENGTH(i_InsuranceSegmentCode)=0 OR IS_SPACES(i_InsuranceSegmentCode),'N/A',LTRIM(RTRIM(i_InsuranceSegmentCode)))
	IFF(i_InsuranceSegmentCode IS NULL 
		OR LENGTH(i_InsuranceSegmentCode
		) = 0 
		OR LENGTH(i_InsuranceSegmentCode)>0 AND TRIM(i_InsuranceSegmentCode)='',
		'N/A',
		LTRIM(RTRIM(i_InsuranceSegmentCode
			)
		)
	) AS o_InsuranceSegmentCode,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentAbbreviation) OR LENGTH(i_InsuranceSegmentAbbreviation)=0 OR IS_SPACES(i_InsuranceSegmentAbbreviation),'N/A',LTRIM(RTRIM(i_InsuranceSegmentAbbreviation)))
	IFF(i_InsuranceSegmentAbbreviation IS NULL 
		OR LENGTH(i_InsuranceSegmentAbbreviation
		) = 0 
		OR LENGTH(i_InsuranceSegmentAbbreviation)>0 AND TRIM(i_InsuranceSegmentAbbreviation)='',
		'N/A',
		LTRIM(RTRIM(i_InsuranceSegmentAbbreviation
			)
		)
	) AS o_InsuranceSegmentAbbreviation,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentDescription) OR LENGTH(i_InsuranceSegmentDescription)=0 OR IS_SPACES(i_InsuranceSegmentDescription),'N/A',LTRIM(RTRIM(i_InsuranceSegmentDescription)))
	IFF(i_InsuranceSegmentDescription IS NULL 
		OR LENGTH(i_InsuranceSegmentDescription
		) = 0 
		OR LENGTH(i_InsuranceSegmentDescription)>0 AND TRIM(i_InsuranceSegmentDescription)='',
		'N/A',
		LTRIM(RTRIM(i_InsuranceSegmentDescription
			)
		)
	) AS o_InsuranceSegmentDescription
	FROM SQ_InsuranceSegment
),
TGT_InsuranceSegment_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceSegment AS T
	USING EXP_StringValues AS S
	ON T.InsuranceSegmentId = S.o_InsuranceSegmentId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.InsuranceSegmentAKId = S.o_InsuranceSegmentAKId, T.InsuranceSegmentCode = S.o_InsuranceSegmentCode, T.InsuranceSegmentAbbreviation = S.o_InsuranceSegmentAbbreviation, T.InsuranceSegmentDescription = S.o_InsuranceSegmentDescription
	WHEN NOT MATCHED THEN
	INSERT (InsuranceSegmentId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, InsuranceSegmentAKId, InsuranceSegmentCode, InsuranceSegmentAbbreviation, InsuranceSegmentDescription)
	VALUES (
	EXP_NumericValues.o_InsuranceSegmentId AS INSURANCESEGMENTID, 
	EXP_StringValues.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_InsuranceSegmentAKId AS INSURANCESEGMENTAKID, 
	EXP_StringValues.o_InsuranceSegmentCode AS INSURANCESEGMENTCODE, 
	EXP_StringValues.o_InsuranceSegmentAbbreviation AS INSURANCESEGMENTABBREVIATION, 
	EXP_StringValues.o_InsuranceSegmentDescription AS INSURANCESEGMENTDESCRIPTION)
),