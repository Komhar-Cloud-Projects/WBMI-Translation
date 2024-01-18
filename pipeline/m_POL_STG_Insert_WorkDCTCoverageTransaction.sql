WITH
SQ_WorkDCTCoverageTransaction AS (
	WITH PCoverage
	AS (
		SELECT A.SessionId,
			A.ObjectId AS ParentCoverageObjectId,
			A.ObjectName AS ParentCoverageObjectName,
			A.CoverageId,
			A.Id AS CoverageGUID,
			A.Type AS CoverageType,
			A.Change,
			A.Premium AS Premium,
			A.Type AS ParentCoverageType,
			CASE 
				WHEN A.Deleted = 1
					THEN '1'
				WHEN W.Indicator = 1
					THEN '0'
				WHEN W.IndicatorbValue = 1
					THEN '0'
				ELSE '1'
				END AS CoverageDeleteFlag,
			A.Written,
			A.Prior,
			A.BaseRate,
			Modifier.Value IncreasedLimitFactor
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCoverageStaging A
		INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCoverageStage W
			ON W.CoverageId = A.CoverageId
	LEFT JOIN (SELECT DISTINCT D.ObjectID,D.value FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCModifierStaging D
	             WHERE  D.ObjectName = 'DC_Coverage'
	                    AND D.Type = 'IncreasedLimitFactor') Modifier ON Modifier.ObjectId = A.CoverageId
	       WHERE A.ObjectName <> 'DC_Coverage'
	             -- AND A.Type <> 'IncreaseInBuildingExpenses'
	       )	
	SELECT *,
		'N/A' SubCoverageType
	FROM PCoverage
	
	UNION ALL
	
	SELECT B.SessionId AS SessionId,
		B.ParentCoverageObjectId,
		B.ParentCoverageObjectName,
		A.CoverageId,
		A.Id AS CoverageGUID,
		B.ParentCoverageType + '/' + A.Type AS CoverageType,
		A.Change,
		A.Premium AS Premium,
		B.ParentCoverageType AS Ptype,
		CASE 
			WHEN B.CoverageDeleteFlag = 1
				THEN '1'
			WHEN A.Deleted = 1
				THEN '1'
			WHEN W.Indicator = 1
				THEN '0'
			WHEN W.IndicatorbValue = 1
				THEN '0'
			ELSE '1'
			END AS CoverageDeleteFlag,
		A.Written,
		A.Prior,
		A.BaseRate,
		Modifier.Value IncreasedLimitFactor,
		A.Type AS SubCoverageType
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCoverageStaging A
	INNER JOIN PCoverage B
		ON A.ObjectId = B.CoverageId
			AND A.ObjectName = 'DC_Coverage'
			AND B.ParentCoverageObjectName <> 'DC_Coverage'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCoverageStage W
		ON W.CoverageId = A.CoverageId
	LEFT OUTER JOIN (SELECT DISTINCT D.ObjectID,D.value FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCModifierStaging D
	             WHERE  D.ObjectName = 'DC_Coverage'
	                    AND D.Type = 'IncreasedLimitFactor' AND D.Scope IS NULL) Modifier ON Modifier.ObjectId = A.CoverageId
),
EXP_Default AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SessionId,
	ParentCoverageObjectId,
	ParentCoverageObjectName,
	CoverageId,
	CoverageGUID,
	CoverageType,
	Change,
	Premium,
	ParentCoverageType,
	CoverageDeleteFlag,
	Written,
	Prior,
	BaseRate,
	IncreasedLimitFactor,
	SubCoverageType
	FROM SQ_WorkDCTCoverageTransaction
),
WorkDCTCoverageTransaction AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkDCTCoverageTransaction;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkDCTCoverageTransaction
	(ExtractDate, SourceSystemId, SessionId, ParentCoverageObjectId, ParentCoverageObjectName, CoverageId, CoverageGUID, CoverageType, Change, Premium, ParentCoverageType, CoverageDeleteFlag, Written, Prior, BaseRate, IncreasedLimitFactor, SubCoverageType)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	SESSIONID, 
	PARENTCOVERAGEOBJECTID, 
	PARENTCOVERAGEOBJECTNAME, 
	COVERAGEID, 
	COVERAGEGUID, 
	COVERAGETYPE, 
	CHANGE, 
	PREMIUM, 
	PARENTCOVERAGETYPE, 
	COVERAGEDELETEFLAG, 
	WRITTEN, 
	PRIOR, 
	BASERATE, 
	INCREASEDLIMITFACTOR, 
	SUBCOVERAGETYPE
	FROM EXP_Default
),