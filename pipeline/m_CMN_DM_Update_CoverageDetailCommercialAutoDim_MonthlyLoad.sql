WITH
SQ_CoverageDetailCommercialAuto AS (
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	select CDD.CoverageDetailDimId AS CoverageDetailDimId,
	CDCA.RadiusOfOperation,
	CDCA.VehicleTypeSize,
	CDCA.BusinessUseClass,
	CDCA.SecondaryClass,
	CDCA.FleetType,
	CDCA.SecondaryClassGroup
	from  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CDCA
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
	ON CDCA.PremiumTransactionID=CDD.EDWPremiumTransactionPKId
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAutoDim CAD
	on CDD.CoverageDetailDimId=CAD.CoverageDetailDimId
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=0
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	select CDD.CoverageDetailDimId AS CoverageDetailDimId,
	CDCA.RadiusOfOperation,
	CDCA.VehicleTypeSize,
	CDCA.BusinessUseClass,
	CDCA.SecondaryClass,
	CDCA.FleetType,
	CDCA.SecondaryClassGroup
	from  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CDCA
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
	ON CDCA.PremiumTransactionID=CDD.EDWPremiumTransactionPKId
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAutoDim CAD
	on CDD.CoverageDetailDimId=CAD.CoverageDetailDimId
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=1 
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	select CDD.CoverageDetailDimId AS CoverageDetailDimId,
	CDCA.RadiusOfOperation,
	CDCA.VehicleTypeSize,
	CDCA.BusinessUseClass,
	CDCA.SecondaryClass,
	CDCA.FleetType,
	CDCA.SecondaryClassGroup
	from  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CDCA
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
	ON CDCA.PremiumTransactionID=CDD.EDWPremiumTransactionPKId
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAutoDim CAD
	on CDD.CoverageDetailDimId=CAD.CoverageDetailDimId 
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=2 
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	select CDD.CoverageDetailDimId AS CoverageDetailDimId,
	CDCA.RadiusOfOperation,
	CDCA.VehicleTypeSize,
	CDCA.BusinessUseClass,
	CDCA.SecondaryClass,
	CDCA.FleetType,
	CDCA.SecondaryClassGroup
	from  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialAuto CDCA
	INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
	ON CDCA.PremiumTransactionID=CDD.EDWPremiumTransactionPKId
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAutoDim CAD
	on CDD.CoverageDetailDimId=CAD.CoverageDetailDimId 
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=3 
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_metadata AS (
	SELECT
	CoverageDetailDimId AS i_CoverageDetailDimId,
	RadiusOfOperation AS i_RadiusOfOperation,
	VehicleTypeSize AS i_VehicleTypeSize,
	BusinessUseClass AS i_BusinessUseClass,
	SecondaryClass AS i_SecondaryClass,
	FleetType AS i_FleetType,
	SecondaryClassGroup AS i_SecondaryClassGroup,
	i_CoverageDetailDimId AS o_CoverageDetailDimId,
	-- *INF*: IIF(ISNULL(i_RadiusOfOperation),'N/A',i_RadiusOfOperation)
	IFF(i_RadiusOfOperation IS NULL, 'N/A', i_RadiusOfOperation) AS o_RadiusOfOperation,
	-- *INF*: IIF(ISNULL(i_VehicleTypeSize),'N/A',i_VehicleTypeSize)
	IFF(i_VehicleTypeSize IS NULL, 'N/A', i_VehicleTypeSize) AS o_VehicleTypeSize,
	-- *INF*: IIF(ISNULL(i_BusinessUseClass),'N/A',i_BusinessUseClass)
	IFF(i_BusinessUseClass IS NULL, 'N/A', i_BusinessUseClass) AS o_BusinessUseClass,
	-- *INF*: IIF(ISNULL(i_SecondaryClass),'N/A',i_SecondaryClass)
	IFF(i_SecondaryClass IS NULL, 'N/A', i_SecondaryClass) AS o_SecondaryClass,
	-- *INF*: IIF(ISNULL(i_FleetType),'N/A',i_FleetType)
	IFF(i_FleetType IS NULL, 'N/A', i_FleetType) AS o_FleetType,
	-- *INF*: IIF(ISNULL(i_SecondaryClassGroup),'N/A',i_SecondaryClassGroup)
	IFF(i_SecondaryClassGroup IS NULL, 'N/A', i_SecondaryClassGroup) AS o_SecondaryClassGroup
	FROM SQ_CoverageDetailCommercialAuto
),
UPD_ADDEDCOLUMNS AS (
	SELECT
	o_CoverageDetailDimId AS CoverageDetailDimId, 
	o_RadiusOfOperation AS RadiusOfOperation, 
	o_VehicleTypeSize AS VehicleTypeSize, 
	o_BusinessUseClass AS BusinessUseClass, 
	o_SecondaryClass AS SecondaryClass, 
	o_FleetType AS FleetType, 
	o_SecondaryClassGroup AS SecondaryClassGroup
	FROM EXP_metadata
),
CoverageDetailCommercialAutoDim AS (
	MERGE INTO CoverageDetailCommercialAutoDim AS T
	USING UPD_ADDEDCOLUMNS AS S
	ON 
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CoverageDetailDimId = S.CoverageDetailDimId, T.RadiusOfOperation = S.RadiusOfOperation, T.VehicleTypeSize = S.VehicleTypeSize, T.BusinessUseClass = S.BusinessUseClass, T.SecondaryClass = S.SecondaryClass, T.FleetType = S.FleetType, T.SecondaryClassGroup = S.SecondaryClassGroup
),