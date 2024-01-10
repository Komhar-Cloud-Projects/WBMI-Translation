WITH
SQ_CoverageSummary AS (
	SELECT
		CoverageSummaryId,
		CoverageSummaryCode,
		CoverageSummaryDescription
	FROM CoverageSummary
	WHERE CoverageSummaryCode IS NOT NULL
),
EXP_Values AS (
	SELECT
	CoverageSummaryId AS i_CoverageSummaryId,
	CoverageSummaryCode AS i_CoverageSummaryCode,
	CoverageSummaryDescription AS i_CoverageSummaryDescription,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: IIF(ISNULL(i_CoverageSummaryId) OR LENGTH(i_CoverageSummaryId)=0, Error('Coverage Summary is missing an ID'), i_CoverageSummaryId)
	IFF(i_CoverageSummaryId IS NULL OR LENGTH(i_CoverageSummaryId) = 0, Error('Coverage Summary is missing an ID'), i_CoverageSummaryId) AS o_CoverageSummaryId,
	-- *INF*: IIF(ISNULL(i_CoverageSummaryCode) OR LENGTH(i_CoverageSummaryCode)=0 OR IS_SPACES(i_CoverageSummaryCode), 'N/A', LTRIM(RTRIM(i_CoverageSummaryCode)))
	IFF(i_CoverageSummaryCode IS NULL OR LENGTH(i_CoverageSummaryCode) = 0 OR IS_SPACES(i_CoverageSummaryCode), 'N/A', LTRIM(RTRIM(i_CoverageSummaryCode))) AS o_CoverageSummaryCode,
	-- *INF*: IIF(ISNULL(i_CoverageSummaryDescription) OR LENGTH(i_CoverageSummaryDescription)=0 OR IS_SPACES(i_CoverageSummaryDescription), 'N/A', LTRIM(RTRIM(i_CoverageSummaryDescription)))
	IFF(i_CoverageSummaryDescription IS NULL OR LENGTH(i_CoverageSummaryDescription) = 0 OR IS_SPACES(i_CoverageSummaryDescription), 'N/A', LTRIM(RTRIM(i_CoverageSummaryDescription))) AS o_CoverageSummaryDescription
	FROM SQ_CoverageSummary
),
LKP_CoverageSummary AS (
	SELECT
	CoverageSummaryId,
	CoverageSummaryCode,
	CoverageSummaryDescription
	FROM (
		SELECT 
			CoverageSummaryId,
			CoverageSummaryCode,
			CoverageSummaryDescription
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageSummary
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageSummaryId ORDER BY CoverageSummaryId) = 1
),
EXP_ChangeFlag AS (
	SELECT
	LKP_CoverageSummary.CoverageSummaryId AS lkp_CoverageSummaryId,
	LKP_CoverageSummary.CoverageSummaryCode AS lkp_CoverageSummaryCode,
	LKP_CoverageSummary.CoverageSummaryDescription AS lkp_CoverageSummaryDescription,
	EXP_Values.o_CoverageSummaryId AS CoverageSummaryId,
	EXP_Values.o_CoverageSummaryCode AS CoverageSummaryCode,
	EXP_Values.o_CoverageSummaryDescription AS CoverageSummaryDescription,
	EXP_Values.o_CreatedDate AS CreatedDate,
	EXP_Values.o_ModifiedDate AS ModifiedDate,
	-- *INF*: DECODE(TRUE, 
	-- ISNULL(lkp_CoverageSummaryId), 1,
	-- lkp_CoverageSummaryDescription<>CoverageSummaryDescription, 2, 
	-- lkp_CoverageSummaryCode <> CoverageSummaryCode, 2,
	-- 0)
	DECODE(TRUE,
		lkp_CoverageSummaryId IS NULL, 1,
		lkp_CoverageSummaryDescription <> CoverageSummaryDescription, 2,
		lkp_CoverageSummaryCode <> CoverageSummaryCode, 2,
		0) AS o_ChangeFlag
	FROM EXP_Values
	LEFT JOIN LKP_CoverageSummary
	ON LKP_CoverageSummary.CoverageSummaryId = EXP_Values.o_CoverageSummaryId
),
RTR_Insert_Update_Groups AS (
	SELECT
	CoverageSummaryId,
	CreatedDate,
	ModifiedDate,
	CoverageSummaryCode,
	CoverageSummaryDescription,
	o_ChangeFlag AS ChangeFlag
	FROM EXP_ChangeFlag
),
RTR_Insert_Update_Groups_INSERT AS (SELECT * FROM RTR_Insert_Update_Groups WHERE ChangeFlag=1),
RTR_Insert_Update_Groups_UPDATE AS (SELECT * FROM RTR_Insert_Update_Groups WHERE ChangeFlag=2),
CoverageSummary_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageSummary
	(CoverageSummaryId, CreatedDate, ModifiedDate, CoverageSummaryCode, CoverageSummaryDescription)
	SELECT 
	COVERAGESUMMARYID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	COVERAGESUMMARYCODE, 
	COVERAGESUMMARYDESCRIPTION
	FROM RTR_Insert_Update_Groups_INSERT
),
UPD_CoverageSummary AS (
	SELECT
	CoverageSummaryId, 
	ModifiedDate, 
	CoverageSummaryCode, 
	CoverageSummaryDescription
	FROM RTR_Insert_Update_Groups_UPDATE
),
CoverageSummary_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageSummary AS T
	USING UPD_CoverageSummary AS S
	ON T.CoverageSummaryId = S.CoverageSummaryId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.CoverageSummaryCode = S.CoverageSummaryCode, T.CoverageSummaryDescription = S.CoverageSummaryDescription
),