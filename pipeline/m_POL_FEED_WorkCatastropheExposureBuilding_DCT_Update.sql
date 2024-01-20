WITH
SQ_CatastropheExposureBuilding_Prototype AS (
	SELECT distinct W.WorkCatastropheExposureBuildingId, S.PolicyBlanketDeductible
	FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkCatastropheExposureBuilding W
	inner join (select PolicyKey, BusinessType FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkCatastropheExposureTransaction WHERE CoverageGroupDescription like '%blanket%' ) WCET on W.PolicyKey=WCET.PolicyKey and w.BusinessType=WCET.BusinessType
	INNER JOIN (SELECT PolicyKey,BusinessType, MAX(LocationDeductible)    as PolicyBlanketDeductible FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkCatastropheExposureBuilding  WHERE AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AND  BusinessType in ('Commercial Property','SMARTbusiness') GROUP BY  PolicyKey,BusinessType ) S
	ON W.PolicyKey=S.PolicyKey and w.BusinessType=S.BusinessType
),
EXP_PBD AS (
	SELECT
	WorkCatastropheExposureBuildingId,
	PolicyBlanketDeductible,
	SYSDATE AS o_ModifiedDate
	FROM SQ_CatastropheExposureBuilding_Prototype
),
UPD_Update AS (
	SELECT
	WorkCatastropheExposureBuildingId, 
	PolicyBlanketDeductible, 
	o_ModifiedDate AS ModifiedDate
	FROM EXP_PBD
),
WorkCatastropheExposureBuilding AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkCatastropheExposureBuilding AS T
	USING UPD_Update AS S
	ON T.WorkCatastropheExposureBuildingId = S.WorkCatastropheExposureBuildingId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.PolicyBlanketDeductible = S.PolicyBlanketDeductible
),