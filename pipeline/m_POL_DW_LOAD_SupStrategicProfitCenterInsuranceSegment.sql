WITH
SQ_SupStrategicProfitCenterInsuranceSegment AS (
	SELECT
		SupStrategicProfitCenterInsuranceSegmentId,
		ModifiedUserId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		SupStrategicProfitCenterInsuranceSegmentAKId,
		SourceCode,
		PolicyNumber1,
		PolicySymbol1,
		Division,
		StrategicProfitCenterCode,
		InsuranceSegmentCode
	FROM SupStrategicProfitCenterInsuranceSegment
),
EXP_DefaultValues AS (
	SELECT
	SupStrategicProfitCenterInsuranceSegmentId AS i_SupStrategicProfitCenterInsuranceSegmentId,
	ModifiedUserId AS i_ModifiedUserId,
	ModifiedDate AS i_ModifiedDate,
	EffectiveDate AS i_EffectiveDate,
	ExpirationDate AS i_ExpirationDate,
	SupStrategicProfitCenterInsuranceSegmentAKId AS i_SupStrategicProfitCenterInsuranceSegmentAKId,
	SourceCode AS i_SourceCode,
	PolicyNumber1 AS i_PolicyNumber1,
	PolicySymbol1 AS i_PolicySymbol1,
	Division AS i_Division,
	StrategicProfitCenterCode AS i_StrategicProfitCenterCode,
	InsuranceSegmentCode AS i_InsuranceSegmentCode,
	-- *INF*: IIF(ISNULL(i_SupStrategicProfitCenterInsuranceSegmentId),-1,i_SupStrategicProfitCenterInsuranceSegmentId)
	IFF(i_SupStrategicProfitCenterInsuranceSegmentId IS NULL,
		- 1,
		i_SupStrategicProfitCenterInsuranceSegmentId
	) AS o_SupStrategicProfitCenterInsuranceSegmentId,
	-- *INF*: IIF(i_ExpirationDate>=TO_DATE('21001231','YYYYMMDD'),1,0)
	IFF(i_ExpirationDate >= TO_DATE('21001231', 'YYYYMMDD'
		),
		1,
		0
	) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
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
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_ModifiedDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_ModifiedDate)
	IFF(i_ModifiedDate IS NULL,
		TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'
		),
		i_ModifiedDate
	) AS o_ModifiedDate,
	-- *INF*: IIF(ISNULL(i_SupStrategicProfitCenterInsuranceSegmentAKId),-1,i_SupStrategicProfitCenterInsuranceSegmentAKId)
	IFF(i_SupStrategicProfitCenterInsuranceSegmentAKId IS NULL,
		- 1,
		i_SupStrategicProfitCenterInsuranceSegmentAKId
	) AS o_SupStrategicProfitCenterInsuranceSegmentAKId,
	-- *INF*: IIF(ISNULL(i_SourceCode) OR LENGTH(i_SourceCode)=0 OR IS_SPACES(i_SourceCode),'N/A',LTRIM(RTRIM(i_SourceCode)))
	IFF(i_SourceCode IS NULL 
		OR LENGTH(i_SourceCode
		) = 0 
		OR LENGTH(i_SourceCode)>0 AND TRIM(i_SourceCode)='',
		'N/A',
		LTRIM(RTRIM(i_SourceCode
			)
		)
	) AS o_SourceCode,
	-- *INF*: IIF(ISNULL(i_PolicyNumber1) OR LENGTH(i_PolicyNumber1)=0 OR IS_SPACES(i_PolicyNumber1),'N/A',LTRIM(RTRIM(i_PolicyNumber1)))
	IFF(i_PolicyNumber1 IS NULL 
		OR LENGTH(i_PolicyNumber1
		) = 0 
		OR LENGTH(i_PolicyNumber1)>0 AND TRIM(i_PolicyNumber1)='',
		'N/A',
		LTRIM(RTRIM(i_PolicyNumber1
			)
		)
	) AS o_PolicyNumber1,
	-- *INF*: IIF(ISNULL(i_PolicySymbol1) OR LENGTH(i_PolicySymbol1)=0 OR IS_SPACES(i_PolicySymbol1),'N/A',LTRIM(RTRIM(i_PolicySymbol1)))
	IFF(i_PolicySymbol1 IS NULL 
		OR LENGTH(i_PolicySymbol1
		) = 0 
		OR LENGTH(i_PolicySymbol1)>0 AND TRIM(i_PolicySymbol1)='',
		'N/A',
		LTRIM(RTRIM(i_PolicySymbol1
			)
		)
	) AS o_PolicySymbol1,
	-- *INF*: IIF(ISNULL(i_Division) OR LENGTH(i_Division)=0 OR IS_SPACES(i_Division),'N/A',LTRIM(RTRIM(i_Division)))
	IFF(i_Division IS NULL 
		OR LENGTH(i_Division
		) = 0 
		OR LENGTH(i_Division)>0 AND TRIM(i_Division)='',
		'N/A',
		LTRIM(RTRIM(i_Division
			)
		)
	) AS o_Division,
	-- *INF*: IIF(ISNULL(i_StrategicProfitCenterCode) OR LENGTH(i_StrategicProfitCenterCode)=0 OR IS_SPACES(i_StrategicProfitCenterCode),'N/A',LTRIM(RTRIM(i_StrategicProfitCenterCode)))
	IFF(i_StrategicProfitCenterCode IS NULL 
		OR LENGTH(i_StrategicProfitCenterCode
		) = 0 
		OR LENGTH(i_StrategicProfitCenterCode)>0 AND TRIM(i_StrategicProfitCenterCode)='',
		'N/A',
		LTRIM(RTRIM(i_StrategicProfitCenterCode
			)
		)
	) AS o_StrategicProfitCenterCode,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentCode) OR LENGTH(i_InsuranceSegmentCode)=0 OR IS_SPACES(i_InsuranceSegmentCode),'N/A',LTRIM(RTRIM(i_InsuranceSegmentCode)))
	IFF(i_InsuranceSegmentCode IS NULL 
		OR LENGTH(i_InsuranceSegmentCode
		) = 0 
		OR LENGTH(i_InsuranceSegmentCode)>0 AND TRIM(i_InsuranceSegmentCode)='',
		'N/A',
		LTRIM(RTRIM(i_InsuranceSegmentCode
			)
		)
	) AS o_InsuranceSegmentCode
	FROM SQ_SupStrategicProfitCenterInsuranceSegment
),
TGT_SupStrategicProfitCenterInsuranceSegment_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupStrategicProfitCenterInsuranceSegment AS T
	USING EXP_DefaultValues AS S
	ON T.SupStrategicProfitCenterInsuranceSegmentId = S.o_SupStrategicProfitCenterInsuranceSegmentId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.SupStrategicProfitCenterInsuranceSegmentAKId = S.o_SupStrategicProfitCenterInsuranceSegmentAKId, T.SourceCode = S.o_SourceCode, T.PolicyNumber1 = S.o_PolicyNumber1, T.PolicySymbol1 = S.o_PolicySymbol1, T.Division = S.o_Division, T.StrategicProfitCenterCode = S.o_StrategicProfitCenterCode, T.InsuranceSegmentCode = S.o_InsuranceSegmentCode
	WHEN NOT MATCHED THEN
	INSERT (SupStrategicProfitCenterInsuranceSegmentId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, SupStrategicProfitCenterInsuranceSegmentAKId, SourceCode, PolicyNumber1, PolicySymbol1, Division, StrategicProfitCenterCode, InsuranceSegmentCode)
	VALUES (
	o_SupStrategicProfitCenterInsuranceSegmentId AS SUPSTRATEGICPROFITCENTERINSURANCESEGMENTID, 
	o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	o_AuditId AS AUDITID, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_SupStrategicProfitCenterInsuranceSegmentAKId AS SUPSTRATEGICPROFITCENTERINSURANCESEGMENTAKID, 
	o_SourceCode AS SOURCECODE, 
	o_PolicyNumber1 AS POLICYNUMBER1, 
	o_PolicySymbol1 AS POLICYSYMBOL1, 
	o_Division AS DIVISION, 
	o_StrategicProfitCenterCode AS STRATEGICPROFITCENTERCODE, 
	o_InsuranceSegmentCode AS INSURANCESEGMENTCODE)
),