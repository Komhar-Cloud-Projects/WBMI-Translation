WITH
LKP_CoverageLimit AS (
	SELECT
	CoverageLimitType,
	PremiumTransactionAKID
	FROM (
		SELECT
		CoverageLimit.CoverageLimitType AS CoverageLimitType,
		CoverageLimitBridge.PremiumTransactionAKID AS PremiumTransactionAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageLimit
		INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageLimitBridge
		ON CoverageLimit.CoverageLimitID=CoverageLimitBridge.CoverageLimitID
		WHERE CoverageLimit.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and CoverageLimit.CoverageLimitType IN ('PersonalInjuryProtectionBasicLimit','PersonalInjuryProtectionExcessLimit')
		ORDER BY PremiumTransactionAKID--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKID ORDER BY CoverageLimitType) = 1
),
LKP_FiveColumns AS (
	SELECT
	lkp_result,
	ClassCode,
	RatingStateCode
	FROM (
		SELECT ClassCode as ClassCode,
		RatingStateCode as RatingStateCode,
		VehicleTypeSize+'@1'
		       +BusinessUseClass+'@2'
			   +SecondaryClass+'@3'
			   +FleetType+'@4'
			   +SecondaryClassGroup+'@5'
		         +RadiusOfOperation+'@6'
		      as lkp_result
		  FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationCommercialAuto
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode,RatingStateCode ORDER BY lkp_result) = 1
),
SQ_PMS AS (
	SELECT b.pif_4514_stage_id
	      ,b.sar_major_peril
	      ,b.sar_code_4
		,a.VehicleClassCode
		,ISNULL(a.VehicleWeight,0) AS VehicleWeight
		,ISNULL(a.VehicleWeight2,0) AS VehicleWeight2
		,a.YearMake
		,a.StatedAmount
	      ,a.CostNew
	      ,b.sar_state
	,a.LastChangeDateCentury
	,a.LastChangeDateYear
	,a.LastChangeDateMonth
	,a.LastChangeDateDay
	,a.VehicleDeletedIndicator
	,LTRIM(RTRIM(sar_class_1_4)) + LTRIM(RTRIM(sar_class_5_6)) as ClassCode
	,LTRIM(sar_exp_month)+'/'+LTRIM(sar_exp_day)+'/'+LTRIM(sar_exp_year) as PremiumTransactionExpirationDate,
	a.CostNew7
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif350Stage a
	RIGHT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_4514_stage b ON a.PifSymbol = b.pif_symbol
		AND a.PifPolicyNumber = b.pif_policy_number
		AND a.PifModule = b.pif_module
		AND a.STATE = b.sar_state
		AND a.territory = b.sar_loc_prov_territory
		AND a.vehicleclasscode = ltrim(rtrim(b.sar_class_1_4 + b.sar_class_5_6))
		AND a.zippostalcode = b.sar_zip_postal_code
	AND RIGHT('00'+Convert(varchar,a.UnitNum),3)=b.sar_unit
	WHERE b.sar_type_bureau IN ('AL','AN','AP') AND b.sar_insurance_line IN ('CA','','GA')
),
EXP_MetaData AS (
	SELECT
	pif_4514_stage_id AS i_pif_4514_stage_id,
	sar_major_peril AS i_sar_major_peril,
	sar_code_4 AS i_sar_code_4,
	VehicleClassCode AS i_VehicleClassCode,
	VehicleWeight AS i_VehicleWeight,
	VehicleWeight2 AS i_VehicleWeight2,
	YearMake AS i_YearMake,
	StatedAmount AS i_StatedAmount,
	CosetNew AS i_CostNew,
	CostNew7 AS i_CostNew7,
	LastChangeDateCentury AS i_LastChangeDateCentury,
	LastChangeDateYear AS i_LastChangeDateYear,
	LastChangeDateMonth AS i_LastChangeDateMonth,
	LastchangeDateDay AS i_LastchangeDateDay,
	VehicleDeletedIndicator AS i_VehicleDeletedIndicator,
	sar_state AS i_sar_state,
	ClassCode AS i_ClassCode,
	PremiumTransactionExpirationDate AS i_PremiumTransactionExpirationDate,
	-- *INF*: TO_DATE(i_PremiumTransactionExpirationDate,'MM/DD/YYYY')
	TO_DATE(i_PremiumTransactionExpirationDate, 'MM/DD/YYYY') AS v_PTExpDate,
	-- *INF*: DECODE(true,
	-- NOT ISNULL(:LKP.LKP_FiveColumns(i_ClassCode,i_sar_state)),:LKP.LKP_FiveColumns(i_ClassCode,i_sar_state),
	-- NOT ISNULL(:LKP.LKP_FiveColumns(i_ClassCode,'99')),:LKP.LKP_FiveColumns(i_ClassCode,'99'),
	-- 'N/A')
	DECODE(true,
		NOT LKP_FIVECOLUMNS_i_ClassCode_i_sar_state.lkp_result IS NULL, LKP_FIVECOLUMNS_i_ClassCode_i_sar_state.lkp_result,
		NOT LKP_FIVECOLUMNS_i_ClassCode_99.lkp_result IS NULL, LKP_FIVECOLUMNS_i_ClassCode_99.lkp_result,
		'N/A') AS v_lkp_result,
	i_pif_4514_stage_id AS o_pif_4514_stage_id,
	-- *INF*: RTRIM(LTRIM(i_VehicleClassCode))
	RTRIM(LTRIM(i_VehicleClassCode)) AS o_VehicleClassCode,
	i_VehicleWeight AS o_VehicleWeight,
	i_VehicleWeight2 AS o_VehicleWeight2,
	-- *INF*: IIF(ISNULL(i_CostNew7),i_CostNew,i_CostNew7)
	IFF(i_CostNew7 IS NULL, i_CostNew, i_CostNew7) AS v_CostNew,
	-- *INF*: --- Logic applied to use CostNew7 field if it is not Null else use CostNew field
	-- IIF(ISNULL(v_CostNew),0,v_CostNew)
	IFF(v_CostNew IS NULL, 0, v_CostNew) AS o_CostNew,
	-- *INF*: IIF(IS_DATE(i_LastChangeDateMonth||'/'||i_LastchangeDateDay||'/'||i_LastChangeDateCentury||i_LastChangeDateYear,'MM/DD/YYYY'),
	-- TO_DATE(i_LastChangeDateMonth||'/'||i_LastchangeDateDay||'/'||i_LastChangeDateCentury||i_LastChangeDateYear,'MM/DD/YYYY'),
	-- TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'))
	IFF(IS_DATE(i_LastChangeDateMonth || '/' || i_LastchangeDateDay || '/' || i_LastChangeDateCentury || i_LastChangeDateYear, 'MM/DD/YYYY'), TO_DATE(i_LastChangeDateMonth || '/' || i_LastchangeDateDay || '/' || i_LastChangeDateCentury || i_LastChangeDateYear, 'MM/DD/YYYY'), TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')) AS v_VehicleDeleteDate,
	-- *INF*: IIF(i_VehicleDeletedIndicator='D',v_VehicleDeleteDate,TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'))
	IFF(i_VehicleDeletedIndicator = 'D', v_VehicleDeleteDate, TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')) AS o_VehicleDeleteDate,
	-- *INF*: LTRIM(RTRIM(i_sar_major_peril))
	LTRIM(RTRIM(i_sar_major_peril)) AS o_sar_major_peril,
	-- *INF*: SUBSTR(LTRIM(RTRIM(i_sar_code_4)),1,1)
	SUBSTR(LTRIM(RTRIM(i_sar_code_4)), 1, 1) AS o_sar_code_4_1,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_sar_state)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_sar_state) AS o_sar_state,
	-- *INF*: SUBSTR(v_lkp_result,instr(v_lkp_result,'@5')+2,instr(v_lkp_result,'@6')-instr(v_lkp_result,'@5')-2)
	SUBSTR(v_lkp_result, instr(v_lkp_result, '@5') + 2, instr(v_lkp_result, '@6') - instr(v_lkp_result, '@5') - 2) AS o_RadiusOfOperation,
	-- *INF*: SUBSTR(v_lkp_result,1,instr(v_lkp_result,'@1')-1)
	SUBSTR(v_lkp_result, 1, instr(v_lkp_result, '@1') - 1) AS o_VehicleTypeSize,
	-- *INF*: SUBSTR(v_lkp_result,instr(v_lkp_result,'@1')+2,instr(v_lkp_result,'@2')-instr(v_lkp_result,'@1')-2)
	SUBSTR(v_lkp_result, instr(v_lkp_result, '@1') + 2, instr(v_lkp_result, '@2') - instr(v_lkp_result, '@1') - 2) AS o_BusinessUseClass,
	-- *INF*: SUBSTR(v_lkp_result,instr(v_lkp_result,'@2')+2,instr(v_lkp_result,'@3')-instr(v_lkp_result,'@2')-2)
	SUBSTR(v_lkp_result, instr(v_lkp_result, '@2') + 2, instr(v_lkp_result, '@3') - instr(v_lkp_result, '@2') - 2) AS o_SecondaryClass,
	-- *INF*: SUBSTR(v_lkp_result,instr(v_lkp_result,'@3')+2,instr(v_lkp_result,'@4')-instr(v_lkp_result,'@3')-2)
	SUBSTR(v_lkp_result, instr(v_lkp_result, '@3') + 2, instr(v_lkp_result, '@4') - instr(v_lkp_result, '@3') - 2) AS o_FleetType,
	-- *INF*: SUBSTR(v_lkp_result,instr(v_lkp_result,'@4')+2,instr(v_lkp_result,'@5')-instr(v_lkp_result,'@4')-2)
	SUBSTR(v_lkp_result, instr(v_lkp_result, '@4') + 2, instr(v_lkp_result, '@5') - instr(v_lkp_result, '@4') - 2) AS o_SecondaryClassGroup
	FROM SQ_PMS
	LEFT JOIN LKP_FIVECOLUMNS LKP_FIVECOLUMNS_i_ClassCode_i_sar_state
	ON LKP_FIVECOLUMNS_i_ClassCode_i_sar_state.ClassCode = i_ClassCode
	AND LKP_FIVECOLUMNS_i_ClassCode_i_sar_state.RatingStateCode = i_sar_state

	LEFT JOIN LKP_FIVECOLUMNS LKP_FIVECOLUMNS_i_ClassCode_99
	ON LKP_FIVECOLUMNS_i_ClassCode_99.ClassCode = i_ClassCode
	AND LKP_FIVECOLUMNS_i_ClassCode_99.RatingStateCode = '99'

),
LKP_WorkPremiumTransacion_StatisticalCoverageHashKey AS (
	SELECT
	StatisticalCoverageHashKey,
	PremiumTransactionStageId
	FROM (
		select MAX(t1.StatisticalCoverageHashKey) AS StatisticalCoverageHashKey,PremiumTransactionStageId AS PremiumTransactionStageId from (select SC.StatisticalCoverageHashKey AS StatisticalCoverageHashKey,WPT.PremiumTransactionStageId AS PremiumTransactionStageId from @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT ON SC.StatisticalCoverageAKID=PT.StatisticalCoverageAKID AND SC.CurrentSnapshotFlag=1 AND PT.CurrentSnapshotFlag=1 AND SC.SourceSystemID='PMS'
		AND PT.SourceSystemID='PMS' INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransaction WPT ON PT.PremiumTransactionAKID=WPT.PremiumTransactionAKID          AND WPT.SourceSystemID='PMS')t1 group by t1.PremiumTransactionStageId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionStageId ORDER BY StatisticalCoverageHashKey) = 1
),
LKP_WorkPremiumTransaction_PremiumTransactionID AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionAKID,
	PremiumTransactionStageId
	FROM (
		select 
		MAX(t1.PremiumTransactionID) AS PremiumTransactionID,
		MAX(t1.PremiumTransactionAKID) AS PremiumTransactionAKID,
		PremiumTransactionStageId AS PremiumTransactionStageId
		from (
		select PT.PremiumTransactionID AS PremiumTransactionID,PT.PremiumTransactionAKID,WPT.PremiumTransactionStageId AS PremiumTransactionStageId
		  from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT
		  INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkPremiumTransaction WPT
		  ON PT.PremiumTransactionAKID = WPT.PremiumTransactionAKId AND PT.CurrentSnapshotFlag=1 AND PT.SourceSystemID='PMS' AND WPT.SourceSystemID='PMS')t1
		  group by t1.PremiumTransactionStageId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionStageId ORDER BY PremiumTransactionID) = 1
),
FIL_NULL_PremiumTransactionID AS (
	SELECT
	LKP_WorkPremiumTransaction_PremiumTransactionID.PremiumTransactionID, 
	LKP_WorkPremiumTransacion_StatisticalCoverageHashKey.StatisticalCoverageHashKey, 
	EXP_MetaData.o_VehicleClassCode AS VehicleClassCode, 
	EXP_MetaData.o_VehicleWeight AS VehicleWeight, 
	EXP_MetaData.o_VehicleWeight2 AS VehicleWeight2, 
	EXP_MetaData.o_CostNew AS CostNew, 
	EXP_MetaData.o_VehicleDeleteDate AS VehicleDeleteDate, 
	LKP_WorkPremiumTransaction_PremiumTransactionID.PremiumTransactionAKID, 
	EXP_MetaData.o_sar_major_peril AS sar_major_peril, 
	EXP_MetaData.o_sar_code_4_1 AS sar_code_4_1, 
	EXP_MetaData.o_sar_state AS sar_state, 
	EXP_MetaData.o_RadiusOfOperation AS RadiusOfOperation, 
	EXP_MetaData.o_VehicleTypeSize AS VehicleTypeSize, 
	EXP_MetaData.o_BusinessUseClass AS BusinessUseClass, 
	EXP_MetaData.o_SecondaryClass AS SecondaryClass, 
	EXP_MetaData.o_FleetType AS FleetType, 
	EXP_MetaData.o_SecondaryClassGroup AS SecondaryClassGroup
	FROM EXP_MetaData
	LEFT JOIN LKP_WorkPremiumTransacion_StatisticalCoverageHashKey
	ON LKP_WorkPremiumTransacion_StatisticalCoverageHashKey.PremiumTransactionStageId = EXP_MetaData.o_pif_4514_stage_id
	LEFT JOIN LKP_WorkPremiumTransaction_PremiumTransactionID
	ON LKP_WorkPremiumTransaction_PremiumTransactionID.PremiumTransactionStageId = EXP_MetaData.o_pif_4514_stage_id
	WHERE NOT ISNULL(PremiumTransactionID)
),
EXP_CoverageDetailCommercialAuto AS (
	SELECT
	PremiumTransactionID AS i_PremiumTransactionID,
	StatisticalCoverageHashKey AS i_StatisticalCoverageHashKey,
	VehicleClassCode AS i_VehicleClassCode,
	VehicleWeight AS i_VehicleWeight,
	VehicleWeight2 AS i_VehicleWeight2,
	PremiumTransactionAKID AS i_PremiumTransactionAKID,
	sar_major_peril AS i_sar_major_peril,
	sar_code_4_1 AS i_sar_code_4_1,
	sar_state AS i_sar_state,
	VehicleTypeSize AS i_CommercialAutoVehicleTypeSize,
	BusinessUseClass AS i_CommercialAutoBusinessUseClass,
	SecondaryClass AS i_SecondaryClass,
	FleetType AS i_FleetType,
	SecondaryClassGroup AS i_SecondaryClassGroup,
	YearMake,
	StatedAmount,
	-- *INF*: TO_DECIMAL(TO_CHAR(i_VehicleWeight) || TO_CHAR(i_VehicleWeight2))
	TO_DECIMAL(TO_CHAR(i_VehicleWeight) || TO_CHAR(i_VehicleWeight2)) AS v_VehicleWeight,
	-- *INF*: SUBSTR(i_VehicleClassCode,1,3)
	SUBSTR(i_VehicleClassCode, 1, 3) AS v_VehicleClassCode_1_3,
	-- *INF*: SUBSTR(i_VehicleClassCode,1,4)
	SUBSTR(i_VehicleClassCode, 1, 4) AS v_VehicleClassCode_1_4,
	-- *INF*: SUBSTR(i_VehicleClassCode,4,2)
	SUBSTR(i_VehicleClassCode, 4, 2) AS v_VehicleClassCode_4_5,
	-- *INF*: SUBSTR(i_VehicleClassCode,3,1)
	SUBSTR(i_VehicleClassCode, 3, 1) AS v_VehicleClassCode_3_1,
	-- *INF*: :LKP.LKP_COVERAGELIMIT(i_PremiumTransactionAKID)
	LKP_COVERAGELIMIT_i_PremiumTransactionAKID.CoverageLimitType AS v_CoverageLimitType,
	CostNew,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	-- *INF*: IIF(ISNULL(i_StatisticalCoverageHashKey),'N/A',i_StatisticalCoverageHashKey)
	IFF(i_StatisticalCoverageHashKey IS NULL, 'N/A', i_StatisticalCoverageHashKey) AS o_StatisticalCoverageHashKey,
	-- *INF*: DECODE(TRUE,
	-- v_VehicleClassCode_1_3  >=  '011' AND v_VehicleClassCode_1_3 <=  '236' AND v_VehicleWeight <= 10000,'Light Trucks',
	-- v_VehicleClassCode_1_3  >=  '011' AND v_VehicleClassCode_1_3 <=  '236' AND v_VehicleWeight >= 10001 AND v_VehicleWeight <= 20000,'Medium Trucks',
	-- v_VehicleClassCode_1_3 >= '311' AND v_VehicleClassCode_1_3 <= '366','Heavy Trucks',
	-- v_VehicleClassCode_1_3 >= '401' AND v_VehicleClassCode_1_3 <= '506' AND v_VehicleWeight > 45000,'Extra Heavy Trucks',
	-- v_VehicleClassCode_1_3 >= '401' AND v_VehicleClassCode_1_3 <= '506' AND v_VehicleWeight < 45000,'Extra Heavy Truck Tractors',
	-- v_VehicleClassCode_1_3 >= '671' AND v_VehicleClassCode_1_3 <= '676','Semitrailers',
	-- v_VehicleClassCode_1_3 >= '681' AND v_VehicleClassCode_1_3 <= '686','Trailers',
	-- v_VehicleClassCode_1_3 >= '691' AND v_VehicleClassCode_1_3 <= '696','ServiceUtilityTrailers',
	-- v_VehicleClassCode_1_3='707','GarageKeepers',
	-- 
	-- in(v_VehicleClassCode_1_4, '6601','6602','6603','6604','6610','6611','6614','6619','6620', '6670','6671','7040','7059','7060','7070','7072','7219','7480','7721' ,'7800','7802','7804','7852','7909','7912','7919','7923','7929','7960','7961','7962','7963','7964','7970','7971','7985','7986','7993','7996','8001','8002','9020','9450','9461','9462','9463','9625','9771','9999','9920','73998'),'Special Class',
	-- 
	-- in(v_VehicleClassCode_1_4, '7399', '7908', '7911', '7915', '7922','7926','7927'),'PrivatePassenger',
	-- 
	-- v_VehicleClassCode_1_4 = '7398','Fleet',
	-- v_VehicleClassCode_1_4 = '7391','All Other NonFleet',
	-- v_VehicleClassCode_1_4 = '7381','NO <5 Yrs-Not Work',
	-- v_VehicleClassCode_1_4 = '7382','NO <5 Yrs-Less 15',
	-- v_VehicleClassCode_1_4 = '7383','NO <5 Yrs-More 15',
	-- v_VehicleClassCode_1_4 = '7386','Not Owner-Not Work',
	-- v_VehicleClassCode_1_4 = '7387','Not Owner-Less 15',
	-- v_VehicleClassCode_1_4 = '7388','Not Owner-More 15',
	-- v_VehicleClassCode_1_4 = '7392','Owner-Not Work',
	-- v_VehicleClassCode_1_4 = '7393','Owner-Less 15',
	-- v_VehicleClassCode_1_4 = '7394','Owner-More 15',
	-- 'N/A'
	-- )
	DECODE(TRUE,
		v_VehicleClassCode_1_3 >= '011' AND v_VehicleClassCode_1_3 <= '236' AND v_VehicleWeight <= 10000, 'Light Trucks',
		v_VehicleClassCode_1_3 >= '011' AND v_VehicleClassCode_1_3 <= '236' AND v_VehicleWeight >= 10001 AND v_VehicleWeight <= 20000, 'Medium Trucks',
		v_VehicleClassCode_1_3 >= '311' AND v_VehicleClassCode_1_3 <= '366', 'Heavy Trucks',
		v_VehicleClassCode_1_3 >= '401' AND v_VehicleClassCode_1_3 <= '506' AND v_VehicleWeight > 45000, 'Extra Heavy Trucks',
		v_VehicleClassCode_1_3 >= '401' AND v_VehicleClassCode_1_3 <= '506' AND v_VehicleWeight < 45000, 'Extra Heavy Truck Tractors',
		v_VehicleClassCode_1_3 >= '671' AND v_VehicleClassCode_1_3 <= '676', 'Semitrailers',
		v_VehicleClassCode_1_3 >= '681' AND v_VehicleClassCode_1_3 <= '686', 'Trailers',
		v_VehicleClassCode_1_3 >= '691' AND v_VehicleClassCode_1_3 <= '696', 'ServiceUtilityTrailers',
		v_VehicleClassCode_1_3 = '707', 'GarageKeepers',
		in(v_VehicleClassCode_1_4, '6601', '6602', '6603', '6604', '6610', '6611', '6614', '6619', '6620', '6670', '6671', '7040', '7059', '7060', '7070', '7072', '7219', '7480', '7721', '7800', '7802', '7804', '7852', '7909', '7912', '7919', '7923', '7929', '7960', '7961', '7962', '7963', '7964', '7970', '7971', '7985', '7986', '7993', '7996', '8001', '8002', '9020', '9450', '9461', '9462', '9463', '9625', '9771', '9999', '9920', '73998'), 'Special Class',
		in(v_VehicleClassCode_1_4, '7399', '7908', '7911', '7915', '7922', '7926', '7927'), 'PrivatePassenger',
		v_VehicleClassCode_1_4 = '7398', 'Fleet',
		v_VehicleClassCode_1_4 = '7391', 'All Other NonFleet',
		v_VehicleClassCode_1_4 = '7381', 'NO <5 Yrs-Not Work',
		v_VehicleClassCode_1_4 = '7382', 'NO <5 Yrs-Less 15',
		v_VehicleClassCode_1_4 = '7383', 'NO <5 Yrs-More 15',
		v_VehicleClassCode_1_4 = '7386', 'Not Owner-Not Work',
		v_VehicleClassCode_1_4 = '7387', 'Not Owner-Less 15',
		v_VehicleClassCode_1_4 = '7388', 'Not Owner-More 15',
		v_VehicleClassCode_1_4 = '7392', 'Owner-Not Work',
		v_VehicleClassCode_1_4 = '7393', 'Owner-Less 15',
		v_VehicleClassCode_1_4 = '7394', 'Owner-More 15',
		'N/A') AS o_VehicleType,
	-- *INF*: IIF(LENGTH(i_RadiusOfOperation)>0,i_RadiusOfOperation,'N/A')
	IFF(LENGTH(i_RadiusOfOperation) > 0, i_RadiusOfOperation, 'N/A') AS o_RadiusOfOperation,
	-- *INF*: DECODE(TRUE,
	-- IN(v_VehicleClassCode_4_5,'02','03','21','22','23','24','25','26','27','28','29'),'Truckers',
	-- IN(v_VehicleClassCode_4_5,'31','32','33','34','35','36','37','38','39'),'Food Delivery',
	-- IN(v_VehicleClassCode_4_5,'41','42','43','44','45','46','47','48','49'),'Special Delivery',
	-- IN(v_VehicleClassCode_4_5,'51','52','53','54','55','56','57','58','59'),'Waste',
	-- IN(v_VehicleClassCode_4_5,'61','62','63','64','65','66','67','68','69'),'Farmers',
	-- IN(v_VehicleClassCode_4_5,'71','72','73','74','75','76','77','78','79'),'Dump/Transit',
	-- IN(v_VehicleClassCode_4_5,'81','82','83','84','85','86','87','88','89'),'Contractors',
	-- IN(v_VehicleClassCode_4_5,'93','94','95','96','97'),'Van/Bus',
	-- v_VehicleClassCode_4_5 = '91','Logging',
	-- IN(v_VehicleClassCode_4_5,'92','98','99'),'Other',
	-- 'N/A'
	-- )
	DECODE(TRUE,
		IN(v_VehicleClassCode_4_5, '02', '03', '21', '22', '23', '24', '25', '26', '27', '28', '29'), 'Truckers',
		IN(v_VehicleClassCode_4_5, '31', '32', '33', '34', '35', '36', '37', '38', '39'), 'Food Delivery',
		IN(v_VehicleClassCode_4_5, '41', '42', '43', '44', '45', '46', '47', '48', '49'), 'Special Delivery',
		IN(v_VehicleClassCode_4_5, '51', '52', '53', '54', '55', '56', '57', '58', '59'), 'Waste',
		IN(v_VehicleClassCode_4_5, '61', '62', '63', '64', '65', '66', '67', '68', '69'), 'Farmers',
		IN(v_VehicleClassCode_4_5, '71', '72', '73', '74', '75', '76', '77', '78', '79'), 'Dump/Transit',
		IN(v_VehicleClassCode_4_5, '81', '82', '83', '84', '85', '86', '87', '88', '89'), 'Contractors',
		IN(v_VehicleClassCode_4_5, '93', '94', '95', '96', '97'), 'Van/Bus',
		v_VehicleClassCode_4_5 = '91', 'Logging',
		IN(v_VehicleClassCode_4_5, '92', '98', '99'), 'Other',
		'N/A') AS o_SecondaryVehicleType,
	-- *INF*: IIF(IN(v_VehicleClassCode_4_5,'71','72','73','74','75','76','77','78','79'),'1','0')
	IFF(IN(v_VehicleClassCode_4_5, '71', '72', '73', '74', '75', '76', '77', '78', '79'), '1', '0') AS o_UsedInDumpingIndicator,
	VehicleDeleteDate,
	-- *INF*: DECODE(TRUE,
	-- i_sar_state='16' AND i_sar_major_peril='130' AND v_CoverageLimitType='PersonalInjuryProtectionBasicLimit' AND i_sar_code_4_1='1','671',
	-- i_sar_state='16' AND i_sar_major_peril='130' AND v_CoverageLimitType='PersonalInjuryProtectionExcessLimit' AND i_sar_code_4_1='1','672',
	-- i_sar_state='16' AND i_sar_major_peril='130' AND v_CoverageLimitType='PersonalInjuryProtectionBasicLimit' AND i_sar_code_4_1='0','681',
	-- i_sar_state='16' AND i_sar_major_peril='130' AND v_CoverageLimitType='PersonalInjuryProtectionExcessLimit' AND i_sar_code_4_1='0','682',
	-- i_sar_state='15' AND i_sar_major_peril='130' AND v_CoverageLimitType='PersonalInjuryProtectionBasicLimit' AND i_sar_code_4_1='0','681',
	-- i_sar_state='15' AND i_sar_major_peril='130' AND v_CoverageLimitType='PersonalInjuryProtectionExcessLimit' AND i_sar_code_4_1='0','682',
	-- i_sar_state='22' AND i_sar_major_peril='130' AND v_CoverageLimitType='PersonalInjuryProtectionBasicLimit' AND IN(i_sar_code_4_1,'1','3'),'671',
	-- i_sar_state='22' AND i_sar_major_peril='130' AND v_CoverageLimitType='PersonalInjuryProtectionBasicLimit' AND i_sar_code_4_1='0','681',
	-- i_sar_state='22' AND i_sar_major_peril='130' AND v_CoverageLimitType='PersonalInjuryProtectionExcessLimit' AND i_sar_code_4_1='0','695',
	-- i_sar_state='21' AND i_sar_major_peril='130' AND IN(i_sar_code_4_1,'1','3'),'671',
	-- i_sar_state='21' AND i_sar_major_peril='130' AND i_sar_code_4_1='0','681',
	-- 'N/A')
	DECODE(TRUE,
		i_sar_state = '16' AND i_sar_major_peril = '130' AND v_CoverageLimitType = 'PersonalInjuryProtectionBasicLimit' AND i_sar_code_4_1 = '1', '671',
		i_sar_state = '16' AND i_sar_major_peril = '130' AND v_CoverageLimitType = 'PersonalInjuryProtectionExcessLimit' AND i_sar_code_4_1 = '1', '672',
		i_sar_state = '16' AND i_sar_major_peril = '130' AND v_CoverageLimitType = 'PersonalInjuryProtectionBasicLimit' AND i_sar_code_4_1 = '0', '681',
		i_sar_state = '16' AND i_sar_major_peril = '130' AND v_CoverageLimitType = 'PersonalInjuryProtectionExcessLimit' AND i_sar_code_4_1 = '0', '682',
		i_sar_state = '15' AND i_sar_major_peril = '130' AND v_CoverageLimitType = 'PersonalInjuryProtectionBasicLimit' AND i_sar_code_4_1 = '0', '681',
		i_sar_state = '15' AND i_sar_major_peril = '130' AND v_CoverageLimitType = 'PersonalInjuryProtectionExcessLimit' AND i_sar_code_4_1 = '0', '682',
		i_sar_state = '22' AND i_sar_major_peril = '130' AND v_CoverageLimitType = 'PersonalInjuryProtectionBasicLimit' AND IN(i_sar_code_4_1, '1', '3'), '671',
		i_sar_state = '22' AND i_sar_major_peril = '130' AND v_CoverageLimitType = 'PersonalInjuryProtectionBasicLimit' AND i_sar_code_4_1 = '0', '681',
		i_sar_state = '22' AND i_sar_major_peril = '130' AND v_CoverageLimitType = 'PersonalInjuryProtectionExcessLimit' AND i_sar_code_4_1 = '0', '695',
		i_sar_state = '21' AND i_sar_major_peril = '130' AND IN(i_sar_code_4_1, '1', '3'), '671',
		i_sar_state = '21' AND i_sar_major_peril = '130' AND i_sar_code_4_1 = '0', '681',
		'N/A') AS o_PIPBureaucoverageCode,
	-- *INF*: IIF(LENGTH(i_CommercialAutoVehicleTypeSize)>0,i_CommercialAutoVehicleTypeSize,'N/A')
	IFF(LENGTH(i_CommercialAutoVehicleTypeSize) > 0, i_CommercialAutoVehicleTypeSize, 'N/A') AS o_CommercialAutoVehicleTypeSize,
	-- *INF*: IIF(LENGTH(i_CommercialAutoBusinessUseClass)>0,i_CommercialAutoBusinessUseClass,'N/A')
	IFF(LENGTH(i_CommercialAutoBusinessUseClass) > 0, i_CommercialAutoBusinessUseClass, 'N/A') AS o_CommercialAutoBusinessUseClass,
	-- *INF*: IIF(LENGTH(i_SecondaryClass)>0,i_SecondaryClass,'N/A')
	IFF(LENGTH(i_SecondaryClass) > 0, i_SecondaryClass, 'N/A') AS o_SecondaryClass,
	-- *INF*: IIF(LENGTH(i_FleetType)>0,i_FleetType,'N/A')
	IFF(LENGTH(i_FleetType) > 0, i_FleetType, 'N/A') AS o_FleetType,
	-- *INF*: IIF(LENGTH(i_SecondaryClassGroup)>0,i_SecondaryClassGroup,'N/A')
	-- 
	IFF(LENGTH(i_SecondaryClassGroup) > 0, i_SecondaryClassGroup, 'N/A') AS o_SecondaryClassGroup
	FROM FIL_NULL_PremiumTransactionID
	LEFT JOIN LKP_COVERAGELIMIT LKP_COVERAGELIMIT_i_PremiumTransactionAKID
	ON LKP_COVERAGELIMIT_i_PremiumTransactionAKID.PremiumTransactionAKID = i_PremiumTransactionAKID

),
LKP_CoverageDetailCommercialAuto AS (
	SELECT
	PremiumTransactionID,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	CoverageGuid,
	VehicleGroupCode,
	RadiusOfOperation,
	SecondaryVehicleType,
	UsedInDumpingIndicator,
	VehicleYear,
	StatedAmount,
	CostNew,
	VehicleDeleteDate,
	VIN,
	VehicleMake,
	VehicleModel,
	VehicleNumber,
	CompositeRatedFlag,
	TerminalZoneCode,
	VehicleType,
	PIPBureauCoverageCode,
	RetroactiveDate,
	VehicleTypeSize,
	BusinessUseClass,
	SecondaryClass,
	FleetType,
	SecondaryClassGroup,
	IncludeUIM,
	CoordinationOfBenefits,
	MedicalExpensesOption,
	CoveredByWorkersCompensationFlag
	FROM (
		SELECT 
			PremiumTransactionID,
			CurrentSnapshotFlag,
			AuditID,
			EffectiveDate,
			ExpirationDate,
			SourceSystemID,
			CreatedDate,
			ModifiedDate,
			CoverageGuid,
			VehicleGroupCode,
			RadiusOfOperation,
			SecondaryVehicleType,
			UsedInDumpingIndicator,
			VehicleYear,
			StatedAmount,
			CostNew,
			VehicleDeleteDate,
			VIN,
			VehicleMake,
			VehicleModel,
			VehicleNumber,
			CompositeRatedFlag,
			TerminalZoneCode,
			VehicleType,
			PIPBureauCoverageCode,
			RetroactiveDate,
			VehicleTypeSize,
			BusinessUseClass,
			SecondaryClass,
			FleetType,
			SecondaryClassGroup,
			IncludeUIM,
			CoordinationOfBenefits,
			MedicalExpensesOption,
			CoveredByWorkersCompensationFlag
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialAuto
		WHERE SourceSystemID='PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_CoverageDetailCommercialAuto.PremiumTransactionID AS lkp_PremiumTransactionID,
	LKP_CoverageDetailCommercialAuto.VehicleGroupCode AS lkp_VehicleType,
	LKP_CoverageDetailCommercialAuto.RadiusOfOperation AS lkp_RadiusOfOperation,
	LKP_CoverageDetailCommercialAuto.SecondaryVehicleType AS lkp_SecondaryVehicleType,
	LKP_CoverageDetailCommercialAuto.UsedInDumpingIndicator AS lkp_UsedInDumpingIndicator,
	LKP_CoverageDetailCommercialAuto.VehicleYear AS lkp_VehicleYear,
	LKP_CoverageDetailCommercialAuto.StatedAmount AS lkp_StatedAmount,
	LKP_CoverageDetailCommercialAuto.CostNew AS lkp_CostNew,
	LKP_CoverageDetailCommercialAuto.VehicleDeleteDate AS lkp_VehicleDeleteDate,
	LKP_CoverageDetailCommercialAuto.VIN AS lkp_VIN,
	-- *INF*: IIF(ISNULL(lkp_VIN), 'TBD', lkp_VIN)
	IFF(lkp_VIN IS NULL, 'TBD', lkp_VIN) AS v_lkp_VIN,
	LKP_CoverageDetailCommercialAuto.VehicleMake AS lkp_VehicleMake,
	-- *INF*: IIF(ISNULL(lkp_VehicleMake), 'TBD', lkp_VehicleMake)
	IFF(lkp_VehicleMake IS NULL, 'TBD', lkp_VehicleMake) AS v_lkp_VehicleMake,
	LKP_CoverageDetailCommercialAuto.VehicleModel AS lkp_VehicleModel,
	-- *INF*: IIF(ISNULL(lkp_VehicleModel), 'TBD', lkp_VehicleModel)
	IFF(lkp_VehicleModel IS NULL, 'TBD', lkp_VehicleModel) AS v_lkp_VehicleModel,
	LKP_CoverageDetailCommercialAuto.VehicleNumber AS lkp_VehicleNumber,
	-- *INF*: IIF(ISNULL(lkp_VehicleNumber), -1, lkp_VehicleNumber)
	IFF(lkp_VehicleNumber IS NULL, - 1, lkp_VehicleNumber) AS v_lkp_VehicleNumber,
	LKP_CoverageDetailCommercialAuto.PIPBureauCoverageCode AS lkp_PIPBureaucoverageCode,
	LKP_CoverageDetailCommercialAuto.VehicleTypeSize AS lkp_CommercialAutoVehicleTypeSize,
	LKP_CoverageDetailCommercialAuto.BusinessUseClass AS lkp_CommercialAutoBusinessUseClass,
	LKP_CoverageDetailCommercialAuto.SecondaryClass AS lkp_SecondaryClass,
	LKP_CoverageDetailCommercialAuto.FleetType AS lkp_FleetType,
	LKP_CoverageDetailCommercialAuto.SecondaryClassGroup AS lkp_SecondaryClassGroup,
	LKP_CoverageDetailCommercialAuto.IncludeUIM AS lkp_IncludeUIM,
	LKP_CoverageDetailCommercialAuto.CoordinationOfBenefits AS lkp_CoordinationOfBenefits,
	LKP_CoverageDetailCommercialAuto.MedicalExpensesOption AS lkp_MedicalExpensesOption,
	LKP_CoverageDetailCommercialAuto.CoveredByWorkersCompensationFlag AS lkp_CoveredByWorkersCompensationFlag,
	-- *INF*: IIF(ISNULL(lkp_PIPBureaucoverageCode),'N/A',lkp_PIPBureaucoverageCode)
	IFF(lkp_PIPBureaucoverageCode IS NULL, 'N/A', lkp_PIPBureaucoverageCode) AS v_lkp_PIPBureaucoverageCode,
	EXP_CoverageDetailCommercialAuto.CostNew AS i_CostNew,
	EXP_CoverageDetailCommercialAuto.o_PremiumTransactionID AS i_PremiumTransactionID,
	EXP_CoverageDetailCommercialAuto.o_StatisticalCoverageHashKey AS i_StatisticalCoverageHashKey,
	EXP_CoverageDetailCommercialAuto.o_VehicleType AS i_VehicleType,
	EXP_CoverageDetailCommercialAuto.o_RadiusOfOperation AS i_RadiusOfOperation,
	EXP_CoverageDetailCommercialAuto.o_SecondaryVehicleType AS i_SecondaryVehicleType,
	EXP_CoverageDetailCommercialAuto.o_UsedInDumpingIndicator AS i_UsedInDumpingIndicator,
	EXP_CoverageDetailCommercialAuto.YearMake AS i_YearMake,
	EXP_CoverageDetailCommercialAuto.StatedAmount AS i_StatedAmount,
	EXP_CoverageDetailCommercialAuto.VehicleDeleteDate AS i_VehicleDeleteDate,
	EXP_CoverageDetailCommercialAuto.o_PIPBureaucoverageCode AS i_PIPBureaucoverageCode,
	EXP_CoverageDetailCommercialAuto.o_CommercialAutoVehicleTypeSize AS i_CommercialAutoVehicleTypeSize,
	EXP_CoverageDetailCommercialAuto.o_CommercialAutoBusinessUseClass AS i_CommercialAutoBusinessUseClass,
	EXP_CoverageDetailCommercialAuto.o_SecondaryClass AS i_SecondaryClass,
	EXP_CoverageDetailCommercialAuto.o_FleetType AS i_FleetType,
	EXP_CoverageDetailCommercialAuto.o_SecondaryClassGroup AS i_SecondaryClassGroup,
	-- *INF*: RTRIM(LTRIM(lkp_VehicleType))
	RTRIM(LTRIM(lkp_VehicleType)) AS v_lkp_VehicleType,
	-- *INF*: RTRIM(LTRIM(lkp_RadiusOfOperation))
	RTRIM(LTRIM(lkp_RadiusOfOperation)) AS v_lkp_RadiusOfOperation,
	-- *INF*: RTRIM(LTRIM(lkp_SecondaryVehicleType))
	RTRIM(LTRIM(lkp_SecondaryVehicleType)) AS v_lkp_SecondaryVehicleType,
	-- *INF*: IIF(RTRIM(LTRIM(lkp_UsedInDumpingIndicator))='T','1','0')
	IFF(RTRIM(LTRIM(lkp_UsedInDumpingIndicator)) = 'T', '1', '0') AS v_lkp_UsedInDumpingIndicator,
	-- *INF*: SUBSTR('0000' || IIF(ISNULL(i_YearMake), '0000', TO_CHAR(i_YearMake)), -4, 4)
	SUBSTR('0000' || IFF(i_YearMake IS NULL, '0000', TO_CHAR(i_YearMake)), - 4, 4) AS v_YearMake,
	-- *INF*: IIF(ISNULL(i_StatedAmount), '0', TO_CHAR(i_StatedAmount))
	IFF(i_StatedAmount IS NULL, '0', TO_CHAR(i_StatedAmount)) AS v_StatedAmount,
	'N/A' AS v_VIN,
	v_VIN AS o_VIN,
	'N/A' AS v_VehicleMake,
	v_VehicleMake AS o_VehicleMake,
	'N/A' AS v_VehicleModel,
	v_VehicleModel AS o_VehicleModel,
	0 AS v_VehicleNumber,
	v_VehicleNumber AS o_VehicleNumber,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.US')
	TO_DATE('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.US') AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.US')
	TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.US') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_StatisticalCoverageHashKey AS o_StatisticalCoverageHashKey,
	i_VehicleType AS o_VehicleGroupCode,
	i_RadiusOfOperation AS o_RadiusOfOperation,
	i_SecondaryVehicleType AS o_SecondaryVehicleType,
	i_UsedInDumpingIndicator AS o_UsedInDumpingIndicator,
	v_YearMake AS o_YearMake,
	v_StatedAmount AS o_StatedAmount,
	i_CostNew AS o_CostNew,
	i_VehicleDeleteDate AS o_VehicleDeleteDate,
	0 AS o_CompositeRatedFlag,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_PremiumTransactionID),
	-- 'NEW',
	-- v_lkp_VehicleType != i_VehicleType OR v_lkp_RadiusOfOperation !=i_RadiusOfOperation OR v_lkp_SecondaryVehicleType != i_SecondaryVehicleType OR v_lkp_UsedInDumpingIndicator  != i_UsedInDumpingIndicator OR v_YearMake != lkp_VehicleYear OR v_StatedAmount != lkp_StatedAmount 
	-- OR lkp_CostNew != i_CostNew
	-- OR lkp_VehicleDeleteDate != i_VehicleDeleteDate
	-- OR v_lkp_VIN != v_VIN
	-- OR v_lkp_VehicleMake != v_VehicleMake
	-- OR v_lkp_VehicleModel != v_VehicleModel
	-- OR v_lkp_VehicleNumber != v_VehicleNumber
	-- OR v_lkp_PIPBureaucoverageCode<>i_PIPBureaucoverageCode
	-- OR lkp_CommercialAutoVehicleTypeSize<>i_CommercialAutoVehicleTypeSize
	-- OR lkp_CommercialAutoBusinessUseClass<>i_CommercialAutoBusinessUseClass
	-- OR lkp_SecondaryClass<>i_SecondaryClass
	-- OR lkp_FleetType<>i_FleetType
	-- OR lkp_SecondaryClassGroup<>i_SecondaryClassGroup
	-- OR lkp_IncludeUIM<>v_IncludeUIM,
	-- 'UPDATE',
	-- 'NOCHANGE')
	DECODE(TRUE,
		lkp_PremiumTransactionID IS NULL, 'NEW',
		v_lkp_VehicleType != i_VehicleType OR v_lkp_RadiusOfOperation != i_RadiusOfOperation OR v_lkp_SecondaryVehicleType != i_SecondaryVehicleType OR v_lkp_UsedInDumpingIndicator != i_UsedInDumpingIndicator OR v_YearMake != lkp_VehicleYear OR v_StatedAmount != lkp_StatedAmount OR lkp_CostNew != i_CostNew OR lkp_VehicleDeleteDate != i_VehicleDeleteDate OR v_lkp_VIN != v_VIN OR v_lkp_VehicleMake != v_VehicleMake OR v_lkp_VehicleModel != v_VehicleModel OR v_lkp_VehicleNumber != v_VehicleNumber OR v_lkp_PIPBureaucoverageCode <> i_PIPBureaucoverageCode OR lkp_CommercialAutoVehicleTypeSize <> i_CommercialAutoVehicleTypeSize OR lkp_CommercialAutoBusinessUseClass <> i_CommercialAutoBusinessUseClass OR lkp_SecondaryClass <> i_SecondaryClass OR lkp_FleetType <> i_FleetType OR lkp_SecondaryClassGroup <> i_SecondaryClassGroup OR lkp_IncludeUIM <> v_IncludeUIM, 'UPDATE',
		'NOCHANGE') AS o_ChangeFlag,
	'N/A' AS o_TerminalZoneCode,
	'N/A' AS o_VehicleType,
	i_PIPBureaucoverageCode AS o_PIPBureaucoverageCode,
	i_CommercialAutoVehicleTypeSize AS o_CommercialAutoVehicleTypeSize,
	i_CommercialAutoBusinessUseClass AS o_CommercialAutoBusinessUseClass,
	i_SecondaryClass AS o_SecondaryClass,
	i_FleetType AS o_FleetType,
	i_SecondaryClassGroup AS o_SecondaryClassGroup,
	'N/A' AS v_IncludeUIM,
	v_IncludeUIM AS o_IncludeUIM,
	-- *INF*: TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
	TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AS RetroactiveDate,
	'N/A' AS o_CoordinationOfBenefits,
	'N/A' AS o_MedicalExpensesOption,
	0 AS o_CoveredByWorkersCompensationFlag,
	'N/A' AS o_SubjectToNoFault,
	-1 AS o_AdditionalLimitKS,
	-1 AS o_AdditionalLimitKY,
	-1 AS o_AdditionalLimitMN,
	'N/A' AS o_RatingZoneCode,
	0 AS o_ReplacementCost,
	0 AS o_FullGlassIndicator,
	0 AS o_HistoricVehicleIndicator
	FROM EXP_CoverageDetailCommercialAuto
	LEFT JOIN LKP_CoverageDetailCommercialAuto
	ON LKP_CoverageDetailCommercialAuto.PremiumTransactionID = EXP_CoverageDetailCommercialAuto.o_PremiumTransactionID
),
RTR_INSERT_UPDATE AS (
	SELECT
	o_PremiumTransactionID AS PremiumTransactionID,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditID AS AuditID,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_StatisticalCoverageHashKey AS StatisticalCoverageHashKey,
	o_VehicleGroupCode AS VehicleGroupCode,
	o_RadiusOfOperation AS RadiusOfOperation,
	o_SecondaryVehicleType AS SecondaryVehicleType,
	o_UsedInDumpingIndicator AS UsedInDumpingIndicator,
	o_YearMake AS YearMake,
	o_StatedAmount AS StatedAmount,
	o_CostNew AS CostNew,
	o_VehicleDeleteDate AS VehicleDeleteDate,
	o_ChangeFlag AS ChangeFlag,
	o_VIN,
	o_VehicleMake,
	o_VehicleModel,
	o_VehicleNumber,
	o_CompositeRatedFlag,
	o_TerminalZoneCode,
	o_VehicleType AS VehicleType,
	o_PIPBureaucoverageCode AS PIPBureaucoverageCode,
	o_CommercialAutoVehicleTypeSize AS CommercialAutoVehicleTypeSize,
	o_CommercialAutoBusinessUseClass AS CommercialAutoBusinessUseClass,
	o_SecondaryClass AS SecondaryClass,
	o_FleetType AS FleetType,
	o_SecondaryClassGroup AS SecondaryClassGroup,
	o_IncludeUIM AS IncludeUIM,
	RetroactiveDate,
	o_CoordinationOfBenefits,
	o_MedicalExpensesOption,
	o_CoveredByWorkersCompensationFlag,
	o_SubjectToNoFault,
	o_AdditionalLimitKS,
	o_AdditionalLimitKY,
	o_AdditionalLimitMN,
	o_RatingZoneCode,
	o_ReplacementCost,
	o_FullGlassIndicator,
	o_HistoricVehicleIndicator
	FROM EXP_DetectChanges
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE ChangeFlag='NEW'),
RTR_INSERT_UPDATE_UPDATE AS (SELECT * FROM RTR_INSERT_UPDATE WHERE ChangeFlag='UPDATE'),
UPD_Exists AS (
	SELECT
	PremiumTransactionID, 
	ModifiedDate, 
	StatisticalCoverageHashKey, 
	VehicleGroupCode, 
	RadiusOfOperation, 
	SecondaryVehicleType, 
	UsedInDumpingIndicator, 
	YearMake, 
	StatedAmount, 
	CostNew, 
	VehicleDeleteDate, 
	o_VIN AS VIN, 
	o_VehicleMake AS VehicleMake, 
	o_VehicleModel AS VehicleModel, 
	o_VehicleNumber AS VehicleNumber, 
	o_CompositeRatedFlag AS CompositeRatedFlag, 
	o_TerminalZoneCode AS TerminalZoneCode, 
	VehicleType, 
	PIPBureaucoverageCode, 
	CommercialAutoVehicleTypeSize, 
	CommercialAutoBusinessUseClass, 
	SecondaryClass, 
	FleetType, 
	SecondaryClassGroup, 
	RetroactiveDate AS RetroactiveDate1, 
	IncludeUIM AS IncludeUIM3, 
	o_CoordinationOfBenefits AS o_CoordinationOfBenefits3, 
	o_MedicalExpensesOption AS o_MedicalExpensesOption3, 
	o_CoveredByWorkersCompensationFlag AS o_CoveredByWorkersCompensationFlag3, 
	o_SubjectToNoFault AS o_SubjectToNoFault3, 
	o_AdditionalLimitKS AS o_AdditionalLimitKS3, 
	o_AdditionalLimitKY AS o_AdditionalLimitKY3, 
	o_AdditionalLimitMN AS o_AdditionalLimitMN3, 
	o_RatingZoneCode AS o_RatingZoneCode3, 
	o_ReplacementCost AS o_ReplacementCost3, 
	o_FullGlassIndicator AS o_FullGlassIndicator3, 
	o_HistoricVehicleIndicator AS o_HistoricVehicleIndicator3
	FROM RTR_INSERT_UPDATE_UPDATE
),
CoverageDetailCommercialAuto_UPDATE AS (
	MERGE INTO CoverageDetailCommercialAuto AS T
	USING UPD_Exists AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.CoverageGuid = S.StatisticalCoverageHashKey, T.VehicleGroupCode = S.VehicleGroupCode, T.RadiusOfOperation = S.RadiusOfOperation, T.SecondaryVehicleType = S.SecondaryVehicleType, T.UsedInDumpingIndicator = S.UsedInDumpingIndicator, T.VehicleYear = S.YearMake, T.StatedAmount = S.StatedAmount, T.CostNew = S.CostNew, T.VehicleDeleteDate = S.VehicleDeleteDate, T.VIN = S.VIN, T.VehicleMake = S.VehicleMake, T.VehicleModel = S.VehicleModel, T.VehicleNumber = S.VehicleNumber, T.CompositeRatedFlag = S.CompositeRatedFlag, T.TerminalZoneCode = S.TerminalZoneCode, T.VehicleType = S.VehicleType, T.PIPBureauCoverageCode = S.PIPBureaucoverageCode, T.RetroactiveDate = S.RetroactiveDate1, T.VehicleTypeSize = S.CommercialAutoVehicleTypeSize, T.BusinessUseClass = S.CommercialAutoBusinessUseClass, T.SecondaryClass = S.SecondaryClass, T.FleetType = S.FleetType, T.SecondaryClassGroup = S.SecondaryClassGroup, T.IncludeUIM = S.IncludeUIM3, T.CoordinationOfBenefits = S.o_CoordinationOfBenefits3, T.MedicalExpensesOption = S.o_MedicalExpensesOption3, T.CoveredByWorkersCompensationFlag = S.o_CoveredByWorkersCompensationFlag3, T.SubjectToNoFault = S.o_SubjectToNoFault3, T.AdditionalLimitKS = S.o_AdditionalLimitKS3, T.AdditionalLimitKY = S.o_AdditionalLimitKY3, T.AdditionalLimitMN = S.o_AdditionalLimitMN3, T.RatingZoneCode = S.o_RatingZoneCode3, T.ReplacementCost = S.o_ReplacementCost3, T.FullGlassIndicator = S.o_FullGlassIndicator3, T.HistoricVehicleIndicator = S.o_HistoricVehicleIndicator3
),
CoverageDetailCommercialAuto_INSERT AS (
	INSERT INTO CoverageDetailCommercialAuto
	(PremiumTransactionID, CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CoverageGuid, VehicleGroupCode, RadiusOfOperation, SecondaryVehicleType, UsedInDumpingIndicator, VehicleYear, StatedAmount, CostNew, VehicleDeleteDate, VIN, VehicleMake, VehicleModel, VehicleNumber, CompositeRatedFlag, TerminalZoneCode, VehicleType, PIPBureauCoverageCode, RetroactiveDate, VehicleTypeSize, BusinessUseClass, SecondaryClass, FleetType, SecondaryClassGroup, IncludeUIM, CoordinationOfBenefits, MedicalExpensesOption, CoveredByWorkersCompensationFlag, SubjectToNoFault, AdditionalLimitKS, AdditionalLimitKY, AdditionalLimitMN, RatingZoneCode, ReplacementCost, FullGlassIndicator, HistoricVehicleIndicator)
	SELECT 
	PREMIUMTRANSACTIONID, 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	StatisticalCoverageHashKey AS COVERAGEGUID, 
	VEHICLEGROUPCODE, 
	RADIUSOFOPERATION, 
	SECONDARYVEHICLETYPE, 
	USEDINDUMPINGINDICATOR, 
	YearMake AS VEHICLEYEAR, 
	STATEDAMOUNT, 
	COSTNEW, 
	VEHICLEDELETEDATE, 
	o_VIN AS VIN, 
	o_VehicleMake AS VEHICLEMAKE, 
	o_VehicleModel AS VEHICLEMODEL, 
	o_VehicleNumber AS VEHICLENUMBER, 
	o_CompositeRatedFlag AS COMPOSITERATEDFLAG, 
	o_TerminalZoneCode AS TERMINALZONECODE, 
	VEHICLETYPE, 
	PIPBureaucoverageCode AS PIPBUREAUCOVERAGECODE, 
	RETROACTIVEDATE, 
	CommercialAutoVehicleTypeSize AS VEHICLETYPESIZE, 
	CommercialAutoBusinessUseClass AS BUSINESSUSECLASS, 
	SECONDARYCLASS, 
	FLEETTYPE, 
	SECONDARYCLASSGROUP, 
	INCLUDEUIM, 
	o_CoordinationOfBenefits AS COORDINATIONOFBENEFITS, 
	o_MedicalExpensesOption AS MEDICALEXPENSESOPTION, 
	o_CoveredByWorkersCompensationFlag AS COVEREDBYWORKERSCOMPENSATIONFLAG, 
	o_SubjectToNoFault AS SUBJECTTONOFAULT, 
	o_AdditionalLimitKS AS ADDITIONALLIMITKS, 
	o_AdditionalLimitKY AS ADDITIONALLIMITKY, 
	o_AdditionalLimitMN AS ADDITIONALLIMITMN, 
	o_RatingZoneCode AS RATINGZONECODE, 
	o_ReplacementCost AS REPLACEMENTCOST, 
	o_FullGlassIndicator AS FULLGLASSINDICATOR, 
	o_HistoricVehicleIndicator AS HISTORICVEHICLEINDICATOR
	FROM RTR_INSERT_UPDATE_INSERT
),