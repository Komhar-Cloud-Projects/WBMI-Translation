WITH
SQ_StrategicProfitCenter AS (
	SELECT
		StrategicProfitCenterId,
		ModifiedDate,
		EffectiveDate,
		ExpirationDate,
		StrategicProfitCenterAKId,
		EnterpriseGroupId,
		LegalEntityId,
		StrategicProfitCenterCode,
		StrategicProfitCenterAbbreviation,
		StrategicProfitCenterDescription
	FROM StrategicProfitCenter
),
EXP_DateValues AS (
	SELECT
	ModifiedDate AS i_ModifiedDate,
	EffectiveDate AS i_EffectiveDate,
	ExpirationDate AS i_ExpirationDate,
	-- *INF*: IIF(ISNULL(i_EffectiveDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_EffectiveDate)
	IFF(i_EffectiveDate IS NULL, TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'), i_EffectiveDate) AS o_EffectiveDate,
	-- *INF*: IIF(ISNULL(i_ExpirationDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_ExpirationDate)
	IFF(i_ExpirationDate IS NULL, TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'), i_ExpirationDate) AS o_ExpirationDate,
	-- *INF*: IIF(ISNULL(i_ModifiedDate),TO_DATE('21001231235959','YYYYMMDDHH24MISS'),i_ModifiedDate)
	IFF(i_ModifiedDate IS NULL, TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'), i_ModifiedDate) AS o_ModifiedDate
	FROM SQ_StrategicProfitCenter
),
EXP_NumericValues AS (
	SELECT
	StrategicProfitCenterId AS i_StrategicProfitCenterId,
	StrategicProfitCenterAKId AS i_StrategicProfitCenterAKId,
	EnterpriseGroupId AS i_EnterpriseGroupId,
	LegalEntityId AS i_LegalEntityId,
	-- *INF*: IIF(ISNULL(i_StrategicProfitCenterId),-1,i_StrategicProfitCenterId)
	IFF(i_StrategicProfitCenterId IS NULL, - 1, i_StrategicProfitCenterId) AS o_StrategicProfitCenterId,
	-- *INF*: IIF(ISNULL(i_StrategicProfitCenterAKId),-1,i_StrategicProfitCenterAKId)
	IFF(i_StrategicProfitCenterAKId IS NULL, - 1, i_StrategicProfitCenterAKId) AS o_StrategicProfitCenterAKId,
	-- *INF*: IIF(ISNULL(i_EnterpriseGroupId),-1,i_EnterpriseGroupId)
	IFF(i_EnterpriseGroupId IS NULL, - 1, i_EnterpriseGroupId) AS o_EnterpriseGroupId,
	-- *INF*: IIF(ISNULL(i_LegalEntityId),-1,i_LegalEntityId)
	IFF(i_LegalEntityId IS NULL, - 1, i_LegalEntityId) AS o_LegalEntityId
	FROM SQ_StrategicProfitCenter
),
EXP_StringValues AS (
	SELECT
	ExpirationDate AS i_ExpirationDate,
	StrategicProfitCenterCode AS i_StrategicProfitCenterCode,
	StrategicProfitCenterAbbreviation AS i_StrategicProfitCenterAbbreviation,
	StrategicProfitCenterDescription AS i_StrategicProfitCenterDescription,
	-- *INF*: IIF(TRUNC(i_ExpirationDate)=TO_DATE('2100-12-31','YYYY-MM-DD'),1,0)
	IFF(TRUNC(i_ExpirationDate) = TO_DATE('2100-12-31', 'YYYY-MM-DD'), 1, 0) AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	-- *INF*: IIF(ISNULL(i_StrategicProfitCenterCode) OR LENGTH(i_StrategicProfitCenterCode)=0 OR IS_SPACES(i_StrategicProfitCenterCode),'N/A',LTRIM(RTRIM(i_StrategicProfitCenterCode)))
	IFF(i_StrategicProfitCenterCode IS NULL OR LENGTH(i_StrategicProfitCenterCode) = 0 OR IS_SPACES(i_StrategicProfitCenterCode), 'N/A', LTRIM(RTRIM(i_StrategicProfitCenterCode))) AS o_StrategicProfitCenterCode,
	-- *INF*: IIF(ISNULL(i_StrategicProfitCenterAbbreviation) OR LENGTH(i_StrategicProfitCenterAbbreviation)=0 OR IS_SPACES(i_StrategicProfitCenterAbbreviation),'N/A',LTRIM(RTRIM(i_StrategicProfitCenterAbbreviation)))
	IFF(i_StrategicProfitCenterAbbreviation IS NULL OR LENGTH(i_StrategicProfitCenterAbbreviation) = 0 OR IS_SPACES(i_StrategicProfitCenterAbbreviation), 'N/A', LTRIM(RTRIM(i_StrategicProfitCenterAbbreviation))) AS o_StrategicProfitCenterAbbreviation,
	-- *INF*: IIF(ISNULL(i_StrategicProfitCenterDescription) OR LENGTH(i_StrategicProfitCenterDescription)=0 OR IS_SPACES(i_StrategicProfitCenterDescription),'N/A',LTRIM(RTRIM(i_StrategicProfitCenterDescription)))
	IFF(i_StrategicProfitCenterDescription IS NULL OR LENGTH(i_StrategicProfitCenterDescription) = 0 OR IS_SPACES(i_StrategicProfitCenterDescription), 'N/A', LTRIM(RTRIM(i_StrategicProfitCenterDescription))) AS o_StrategicProfitCenterDescription
	FROM SQ_StrategicProfitCenter
),
TGT_StrategicProfitCenter_UpdateElseInsert AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.StrategicProfitCenter AS T
	USING EXP_StringValues AS S
	ON T.StrategicProfitCenterId = S.o_StrategicProfitCenterId
	WHEN MATCHED THEN
	UPDATE SET T.CurrentSnapshotFlag = S.o_CurrentSnapshotFlag, T.AuditId = S.o_AuditId, T.EffectiveDate = S.o_EffectiveDate, T.ExpirationDate = S.o_ExpirationDate, T.SourceSystemId = S.o_SourceSystemId, T.CreatedDate = S.o_CreatedDate, T.ModifiedDate = S.o_ModifiedDate, T.StrategicProfitCenterAKId = S.o_StrategicProfitCenterAKId, T.EnterpriseGroupId = S.o_EnterpriseGroupId, T.InsuranceReferenceLegalEntityId = S.o_LegalEntityId, T.StrategicProfitCenterCode = S.o_StrategicProfitCenterCode, T.StrategicProfitCenterAbbreviation = S.o_StrategicProfitCenterAbbreviation, T.StrategicProfitCenterDescription = S.o_StrategicProfitCenterDescription
	WHEN NOT MATCHED THEN
	INSERT (StrategicProfitCenterId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, StrategicProfitCenterAKId, EnterpriseGroupId, InsuranceReferenceLegalEntityId, StrategicProfitCenterCode, StrategicProfitCenterAbbreviation, StrategicProfitCenterDescription)
	VALUES (
	EXP_NumericValues.o_StrategicProfitCenterId AS STRATEGICPROFITCENTERID, 
	EXP_StringValues.o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	EXP_StringValues.o_AuditId AS AUDITID, 
	EXP_DateValues.o_EffectiveDate AS EFFECTIVEDATE, 
	EXP_DateValues.o_ExpirationDate AS EXPIRATIONDATE, 
	EXP_StringValues.o_SourceSystemId AS SOURCESYSTEMID, 
	EXP_StringValues.o_CreatedDate AS CREATEDDATE, 
	EXP_DateValues.o_ModifiedDate AS MODIFIEDDATE, 
	EXP_NumericValues.o_StrategicProfitCenterAKId AS STRATEGICPROFITCENTERAKID, 
	EXP_NumericValues.o_EnterpriseGroupId AS ENTERPRISEGROUPID, 
	EXP_NumericValues.o_LegalEntityId AS INSURANCEREFERENCELEGALENTITYID, 
	EXP_StringValues.o_StrategicProfitCenterCode AS STRATEGICPROFITCENTERCODE, 
	EXP_StringValues.o_StrategicProfitCenterAbbreviation AS STRATEGICPROFITCENTERABBREVIATION, 
	EXP_StringValues.o_StrategicProfitCenterDescription AS STRATEGICPROFITCENTERDESCRIPTION)
),