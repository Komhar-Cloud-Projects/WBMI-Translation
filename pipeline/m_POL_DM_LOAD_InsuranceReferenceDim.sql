WITH
SQ_Association AS (
	WITH T as (
	SELECT C.EnterpriseGroupId,
	D.InsuranceReferenceLegalEntityId,
	B.StrategicProfitCenterId,
	A.InsuranceSegmentId,
	A.PolicyOfferingId,
	A.ProductId,
	InsuranceReferenceLineOfBusinessId,
	RP.RatingPlanId
	FROM DBO.InsuranceReferenceBridge A
	LEFT JOIN DBO.StrategicProfitCenter B
	ON A.StrategicProfitCenterId=B.StrategicProfitCenterId
	LEFT JOIN DBO.EnterpriseGroup C
	ON B.EnterpriseGroupId=C.EnterpriseGroupId
	LEFT JOIN DBO.InsuranceReferenceLegalEntity D
	ON D.InsuranceReferenceLegalEntityId=B.InsuranceReferenceLegalEntityId
	LEFT JOIN dbo.RatingPlanProduct RP
	on A.ProductId=RP.ProductId
	),
	t_id as (
	SELECT DISTINCT EnterpriseGroupId,
	NULL as InsuranceReferenceLegalEntityId,
	NULL as StrategicProfitCenterId,
	NULL as InsuranceSegmentId,
	NULL as PolicyOfferingId,
	NULL as ProductId,
	NULL as InsuranceReferenceLineOfBusinessId,
	NULL as RatingPlanId
	FROM T
	UNION ALL
	SELECT DISTINCT EnterpriseGroupId,
	InsuranceReferenceLegalEntityId,
	NULL as StrategicProfitCenterId,
	NULL as InsuranceSegmentId,
	NULL as PolicyOfferingId,
	NULL as ProductId,
	NULL as InsuranceReferenceLineOfBusinessId,
	NULL as RatingPlanId
	FROM T
	UNION ALL
	SELECT DISTINCT EnterpriseGroupId,
	InsuranceReferenceLegalEntityId,
	StrategicProfitCenterId,
	NULL as InsuranceSegmentId,
	NULL as PolicyOfferingId,
	NULL as ProductId,
	NULL as InsuranceReferenceLineOfBusinessId,
	NULL as RatingPlanId
	FROM T
	UNION ALL
	SELECT DISTINCT EnterpriseGroupId,
	InsuranceReferenceLegalEntityId,
	5 as StrategicProfitCenterId,
	NULL as InsuranceSegmentId,
	NULL as PolicyOfferingId,
	NULL as ProductId,
	NULL as InsuranceReferenceLineOfBusinessId,
	NULL as RatingPlanId
	FROM T
	UNION ALL
	SELECT DISTINCT EnterpriseGroupId,
	InsuranceReferenceLegalEntityId,
	StrategicProfitCenterId,
	InsuranceSegmentId,
	NULL as PolicyOfferingId,
	NULL as ProductId,
	NULL as InsuranceReferenceLineOfBusinessId,
	NULL as RatingPlanId
	FROM T
	UNION ALL
	SELECT DISTINCT EnterpriseGroupId,
	InsuranceReferenceLegalEntityId,
	StrategicProfitCenterId,
	InsuranceSegmentId,
	PolicyOfferingId,
	NULL as ProductId,
	NULL as InsuranceReferenceLineOfBusinessId,
	NULL as RatingPlanId
	FROM T
	UNION ALL
	SELECT DISTINCT EnterpriseGroupId,
	InsuranceReferenceLegalEntityId,
	StrategicProfitCenterId,
	InsuranceSegmentId,
	26 as PolicyOfferingId,
	NULL as ProductId,
	NULL as InsuranceReferenceLineOfBusinessId,
	NULL as RatingPlanId
	FROM T
	UNION ALL
	SELECT DISTINCT EnterpriseGroupId,
	InsuranceReferenceLegalEntityId,
	StrategicProfitCenterId,
	InsuranceSegmentId,
	PolicyOfferingId,
	NULL as ProductId,
	NULL as InsuranceReferenceLineOfBusinessId,
	RatingPlanId
	FROM T
	UNION ALL
	SELECT DISTINCT EnterpriseGroupId,
	InsuranceReferenceLegalEntityId,
	StrategicProfitCenterId,
	InsuranceSegmentId,
	PolicyOfferingId,
	ProductId,
	NULL as InsuranceReferenceLineOfBusinessId,
	RatingPlanId
	FROM T
	UNION ALL
	SELECT DISTINCT EnterpriseGroupId,
	InsuranceReferenceLegalEntityId,
	StrategicProfitCenterId,
	InsuranceSegmentId,
	PolicyOfferingId,
	34 as ProductId,
	NULL as InsuranceReferenceLineOfBusinessId,
	RatingPlanId
	FROM T
	UNION ALL
	SELECT DISTINCT EnterpriseGroupId,
	InsuranceReferenceLegalEntityId,
	StrategicProfitCenterId,
	InsuranceSegmentId,
	PolicyOfferingId,
	34 as ProductId,
	33 as InsuranceReferenceLineOfBusinessId,
	RatingPlanId
	FROM T
	UNION ALL
	SELECT DISTINCT EnterpriseGroupId,
	InsuranceReferenceLegalEntityId,
	StrategicProfitCenterId,
	InsuranceSegmentId,
	PolicyOfferingId,
	ProductId,
	InsuranceReferenceLineOfBusinessId,
	RatingPlanId
	FROM T
	UNION ALL
	SELECT DISTINCT EnterpriseGroupId,
	InsuranceReferenceLegalEntityId,
	StrategicProfitCenterId,
	InsuranceSegmentId,
	PolicyOfferingId,
	ProductId,
	33 as InsuranceReferenceLineOfBusinessId,
	RatingPlanId
	FROM T
	UNION ALL
	SELECT DISTINCT EnterpriseGroupId,
	InsuranceReferenceLegalEntityId,
	StrategicProfitCenterId,
	InsuranceSegmentId,
	PolicyOfferingId,
	ProductId,
	InsuranceReferenceLineOfBusinessId,
	2 as RatingPlanId
	FROM T
	where PolicyOfferingId=1 and ProductId=34
	UNION ALL
	SELECT DISTINCT EnterpriseGroupId,
	InsuranceReferenceLegalEntityId,
	StrategicProfitCenterId,
	InsuranceSegmentId,
	PolicyOfferingId,
	ProductId,
	InsuranceReferenceLineOfBusinessId,
	3 as RatingPlanId
	FROM T
	where PolicyOfferingId=1 and ProductId=34
	UNION ALL
	SELECT DISTINCT EnterpriseGroupId,
	InsuranceReferenceLegalEntityId,
	StrategicProfitCenterId,
	InsuranceSegmentId,
	PolicyOfferingId,
	ProductId,
	InsuranceReferenceLineOfBusinessId,
	5 as RatingPlanId
	FROM T
	where PolicyOfferingId=1 and ProductId=34)
	select distinct ISNULL(EnterpriseGroupCode,'N/A'),
	ISNULL(EnterpriseGroupDescription,'N/A'),
	ISNULL(EnterpriseGroupAbbreviation,'N/A'),
	ISNULL(InsuranceReferenceLegalEntityCode,'N/A'),
	ISNULL(InsuranceReferenceLegalEntityDescription,'N/A'),
	ISNULL(InsuranceReferenceLegalEntityAbbreviation,'N/A'),
	ISNULL(StrategicProfitCenterCode,'N/A'),
	ISNULL(StrategicProfitCenterAbbreviation,'N/A'),
	ISNULL(StrategicProfitCenterDescription,'N/A'),
	ISNULL(InsuranceSegmentCode,'N/A'),
	ISNULL(InsuranceSegmentAbbreviation,'N/A'),
	ISNULL(InsuranceSegmentDescription,'N/A'),
	ISNULL(PolicyOfferingCode,'N/A'),
	ISNULL(PolicyOfferingAbbreviation,'N/A'),
	ISNULL(PolicyOfferingDescription,'N/A'),
	ISNULL(ProductCode,'N/A'),
	ISNULL(ProductAbbreviation,'N/A'),
	ISNULL(ProductDescription,'N/A'),
	ISNULL(InsuranceReferenceLineOfBusinessCode,'N/A'),
	ISNULL(InsuranceReferenceLineOfBusinessAbbreviation,'N/A'),
	ISNULL(InsuranceReferenceLineOfBusinessDescription,'N/A'),
	ISNULL(RP.RatingPlanCode, 'N/A'),
	ISNULL(RP.RatingPlanAbbreviation, 'N/A'),
	ISNULL(RP.RatingPlanDescription, 'N/A')
	from t_id 
	LEFT JOIN DBO.EnterpriseGroup EG
	ON t_id.EnterpriseGroupId=EG.EnterpriseGroupId
	LEFT JOIN DBO.InsuranceReferenceLegalEntity IRLE
	ON t_id.InsuranceReferenceLegalEntityId=IRLE.InsuranceReferenceLegalEntityId
	LEFT JOIN DBO.StrategicProfitCenter SP
	ON t_id.StrategicProfitCenterId=SP.StrategicProfitCenterId
	LEFT JOIN DBO.InsuranceSegment IST
	ON t_id.InsuranceSegmentId=IST.InsuranceSegmentId
	LEFT JOIN DBO.PolicyOffering PO
	ON t_id.PolicyOfferingId=PO.PolicyOfferingId
	LEFT JOIN DBO.Product P
	ON t_id.ProductId=P.ProductId
	LEFT JOIN DBO.InsuranceReferenceLineOfBusiness IRLB
	ON t_id.InsuranceReferenceLineOfBusinessId=IRLB.InsuranceReferenceLineOfBusinessId
	LEFT JOIN dbo.RatingPlan RP
	on t_id.RatingPlanId=RP.RatingPlanId
),
LKP_InsuranceReferenceDim AS (
	SELECT
	InsuranceReferenceDimID,
	EnterpriseGroupDescription,
	InsuranceReferenceLegalEntityDescription,
	StrategicProfitCenterAbbreviation,
	StrategicProfitCenterDescription,
	InsuranceSegmentDescription,
	PolicyOfferingAbbreviation,
	PolicyOfferingDescription,
	ProductAbbreviation,
	ProductDescription,
	InsuranceReferenceLineOfBusinessAbbreviation,
	InsuranceReferenceLineOfBusinessDescription,
	InsuranceReferenceCoverageTypeCode,
	InsuranceReferenceCoverageTypeDescription,
	EnterpriseGroupAbbreviation,
	InsuranceReferenceLegalEntityAbbreviation,
	InsuranceSegmentAbbreviation,
	RatingPlanAbbreviation,
	RatingPlanDescription,
	AccountingProductCode,
	AccountingProductAbbreviation,
	AccountingProductDescription,
	EnterpriseGroupCode,
	InsuranceReferenceLegalEntityCode,
	StrategicProfitCenterCode,
	InsuranceSegmentCode,
	PolicyOfferingCode,
	ProductCode,
	InsuranceReferenceLineOfBusinessCode,
	RatingPlanCode
	FROM (
		SELECT 
			InsuranceReferenceDimID,
			EnterpriseGroupDescription,
			InsuranceReferenceLegalEntityDescription,
			StrategicProfitCenterAbbreviation,
			StrategicProfitCenterDescription,
			InsuranceSegmentDescription,
			PolicyOfferingAbbreviation,
			PolicyOfferingDescription,
			ProductAbbreviation,
			ProductDescription,
			InsuranceReferenceLineOfBusinessAbbreviation,
			InsuranceReferenceLineOfBusinessDescription,
			InsuranceReferenceCoverageTypeCode,
			InsuranceReferenceCoverageTypeDescription,
			EnterpriseGroupAbbreviation,
			InsuranceReferenceLegalEntityAbbreviation,
			InsuranceSegmentAbbreviation,
			RatingPlanAbbreviation,
			RatingPlanDescription,
			AccountingProductCode,
			AccountingProductAbbreviation,
			AccountingProductDescription,
			EnterpriseGroupCode,
			InsuranceReferenceLegalEntityCode,
			StrategicProfitCenterCode,
			InsuranceSegmentCode,
			PolicyOfferingCode,
			ProductCode,
			InsuranceReferenceLineOfBusinessCode,
			RatingPlanCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EnterpriseGroupCode,InsuranceReferenceLegalEntityCode,StrategicProfitCenterCode,InsuranceSegmentCode,PolicyOfferingCode,ProductCode,InsuranceReferenceLineOfBusinessCode,RatingPlanCode ORDER BY InsuranceReferenceDimID) = 1
),
EXP_inputToLookup AS (
	SELECT
	StrategicProfitCenterCode,
	InsuranceSegmentCode,
	PolicyOfferingCode,
	InsuranceReferenceLineOfBusinessCode,
	RatingPlanCode,
	ProductCode
	FROM SQ_Association
),
LKP_SupAccountingProduct AS (
	SELECT
	AccountingProductCode,
	AccountingProductAbbreviation,
	AccountingProductDescription,
	StrategicProfitCenterCode,
	InsuranceSegmentCode,
	PolicyOfferingCode,
	LineOfBusinessCode,
	RatingPlanCode,
	ProductCode
	FROM (
		SELECT distinct
		AP.AccountingProductCode as AccountingProductCode, 
		AP.AccountingProductAbbreviation as AccountingProductAbbreviation, 
		AP.AccountingProductDescription as AccountingProductDescription,
		SupAP.StrategicProfitCenterCode as StrategicProfitCenterCode,
		SupAP.InsuranceSegmentCode as InsuranceSegmentCode,
		SupAP.PolicyOfferingCode as PolicyOfferingCode,
		SupAP.LineOfBusinessCode as LineOfBusinessCode,
		SupAP.RatingPlanCode as RatingPlanCode,
		SupAP.ProductCode as ProductCode
		FROM
		AccountingProduct AP 
		INNER JOIN SupAccountingProductRules SupAP ON 
		AP.AccountingProductCode=SupAP.AccountingProductCode
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterCode,InsuranceSegmentCode,PolicyOfferingCode,LineOfBusinessCode,RatingPlanCode,ProductCode ORDER BY AccountingProductCode) = 1
),
EXP_Detect_Changes_Add_MetaData AS (
	SELECT
	LKP_InsuranceReferenceDim.InsuranceReferenceDimID AS lkp_InsuranceReferenceDimID,
	LKP_InsuranceReferenceDim.EnterpriseGroupDescription AS lkp_EnterpriseGroupDescription,
	LKP_InsuranceReferenceDim.InsuranceReferenceLegalEntityDescription AS lkp_InsuranceReferenceLegalEntityDescription,
	LKP_InsuranceReferenceDim.StrategicProfitCenterAbbreviation AS lkp_StrategicProfitCenterAbbreviation,
	LKP_InsuranceReferenceDim.StrategicProfitCenterDescription AS lkp_StrategicProfitCenterDescription,
	LKP_InsuranceReferenceDim.InsuranceSegmentDescription AS lkp_InsuranceSegmentDescription,
	LKP_InsuranceReferenceDim.PolicyOfferingAbbreviation AS lkp_PolicyOfferingAbbreviation,
	LKP_InsuranceReferenceDim.PolicyOfferingDescription AS lkp_PolicyOfferingDescription,
	LKP_InsuranceReferenceDim.ProductAbbreviation AS lkp_ProductAbbreviation,
	LKP_InsuranceReferenceDim.ProductDescription AS lkp_ProductDescription,
	LKP_InsuranceReferenceDim.InsuranceReferenceLineOfBusinessAbbreviation AS lkp_InsuranceReferenceLineOfBusinessAbbreviation,
	LKP_InsuranceReferenceDim.InsuranceReferenceLineOfBusinessDescription AS lkp_InsuranceReferenceLineOfBusinessDescription,
	LKP_InsuranceReferenceDim.InsuranceReferenceCoverageTypeCode AS lkp_InsuranceReferenceCoverageTypeCode,
	LKP_InsuranceReferenceDim.InsuranceReferenceCoverageTypeDescription AS lkp_InsuranceReferenceCoverageTypeDescription,
	LKP_InsuranceReferenceDim.EnterpriseGroupAbbreviation AS lkp_EnterpriseGroupAbbreviation,
	LKP_InsuranceReferenceDim.InsuranceReferenceLegalEntityAbbreviation AS lkp_InsuranceReferenceLegalEntityAbbreviation,
	LKP_InsuranceReferenceDim.InsuranceSegmentAbbreviation AS lkp_InsuranceSegmentAbbreviation,
	LKP_InsuranceReferenceDim.RatingPlanAbbreviation AS lkp_RatingPlanAbbreviation,
	LKP_InsuranceReferenceDim.RatingPlanDescription AS lkp_RatingPlanDescription,
	LKP_InsuranceReferenceDim.AccountingProductCode AS lkp_AccountingProductCode,
	LKP_InsuranceReferenceDim.AccountingProductAbbreviation AS lkp_AccountingProductAbbreviation,
	LKP_InsuranceReferenceDim.AccountingProductDescription AS lkp_AccountingProductDescription,
	LKP_SupAccountingProduct.AccountingProductCode AS lkp_SupAP_AccountingProductCode,
	LKP_SupAccountingProduct.AccountingProductAbbreviation AS lkp_SupAP_AccountingProductAbbreviation,
	LKP_SupAccountingProduct.AccountingProductDescription AS lkp_SupAP_AccountingProductDescription,
	SQ_Association.EnterpriseGroupCode AS i_EnterpriseGroupCode,
	SQ_Association.EnterpriseGroupDescription AS i_EnterpriseGroupDescription,
	SQ_Association.EnterpriseGroupAbbreviation AS i_EnterpriseGroupAbbreviation,
	SQ_Association.InsuranceReferenceLegalEntityCode AS i_InsuranceReferenceLegalEntityCode,
	SQ_Association.InsuranceReferenceLegalEntityDescription AS i_InsuranceReferenceLegalEntityDescription,
	SQ_Association.InsuranceReferenceLegalEntityAbbreviation AS i_InsuranceReferenceLegalEntityAbbreviation,
	SQ_Association.StrategicProfitCenterCode AS i_StrategicProfitCenterCode,
	SQ_Association.StrategicProfitCenterAbbreviation AS i_StrategicProfitCenterAbbreviation,
	SQ_Association.StrategicProfitCenterDescription AS i_StrategicProfitCenterDescription,
	SQ_Association.InsuranceSegmentCode AS i_InsuranceSegmentCode,
	SQ_Association.InsuranceSegmentAbbreviation AS i_InsuranceSegmentAbbreviation,
	SQ_Association.InsuranceSegmentDescription AS i_InsuranceSegmentDescription,
	SQ_Association.PolicyOfferingCode AS i_PolicyOfferingCode,
	SQ_Association.PolicyOfferingAbbreviation AS i_PolicyOfferingAbbreviation,
	SQ_Association.PolicyOfferingDescription AS i_PolicyOfferingDescription,
	SQ_Association.ProductCode AS i_ProductCode,
	SQ_Association.ProductAbbreviation AS i_ProductAbbreviation,
	SQ_Association.ProductDescription AS i_ProductDescription,
	SQ_Association.InsuranceReferenceLineOfBusinessCode AS i_InsuranceReferenceLineOfBusinessCode,
	SQ_Association.InsuranceReferenceLineOfBusinessAbbreviation AS i_InsuranceReferenceLineOfBusinessAbbreviation,
	SQ_Association.InsuranceReferenceLineOfBusinessDescription AS i_InsuranceReferenceLineOfBusinessDescription,
	SQ_Association.RatingPlanCode AS i_RatingPlanCode,
	SQ_Association.RatingPlanAbbreviation AS i_RatingPlanAbbreviation,
	SQ_Association.RatingPlanDescription AS i_RatingPlanDescription,
	-- *INF*: IIF(ISNULL(lkp_SupAP_AccountingProductCode),'N/A',lkp_SupAP_AccountingProductCode)
	-- --Decode(TRUE, 
	-- --IN(i_ProductCode,'200','340','341') AND i_InsuranceSegmentCode='3','700',
	-- --i_ProductCode='200','200',
	-- --IN(i_ProductCode,'300','310','320','321','360','365','370','380','381'),'300',
	--  --IN(i_ProductCode,'311','400','420','440') AND NOT IN(i_InsuranceReferenceLineOfBusinessCode,'530','590'),'320',
	-- --i_ProductCode='330','330',
	-- --(IN(i_ProductCode,'500','520') AND NOT IN(i_InsuranceReferenceLineOfBusinessCode,'530','590','505','506','507')) OR i_ProductCode  ='510' and IN( i_PolicyOfferingCode,'500','600') AND IN(i_StrategicProfitCenterCode,'2','3'),'500',
	-- --IN(i_ProductCode,'510','620','630','640','650','660') AND i_StrategicProfitCenterCode='3','620',
	-- --i_ProductCode='100' AND  i_InsuranceSegmentAbbreviation='Pool','130',
	-- --i_ProductCode='100' AND i_InsuranceSegmentAbbreviation <>'Pool' AND IN(i_RatingPlanCode, '2', '5'),
	-- --'120',
	-- --i_ProductCode='100' AND i_InsuranceSegmentAbbreviation <>'Pool','100',
	-- --i_PolicyOfferingCode='800' AND i_InsuranceReferenceLineOfBusinessCode <>'812','800',
	-- --i_PolicyOfferingCode='801' AND i_InsuranceReferenceLineOfBusinessCode <>'812','801',
	-- --i_PolicyOfferingCode='810' AND i_InsuranceReferenceLineOfBusinessCode<>'812','810',
	-- --IN(i_InsuranceReferenceLineOfBusinessCode,'506','505','507','530','590','812'),'700',
	-- --i_ProductCode='312' AND i_StrategicProfitCenterAbbreviation <>'NSI' ,'320',
	-- --i_ProductCode='312' AND i_StrategicProfitCenterAbbreviation='NSI','310',
	-- --i_ProductCode='340','340',
	-- --i_ProductCode='341','341',
	-- --i_ProductCode='410','410',
	-- --i_ProductCode='430','430',
	-- --i_ProductCode='450','450',
	-- --i_ProductCode='550','550',
	-- --i_ProductCode='553','550',
	-- --i_ProductCode='610','610',
	-- --IN(i_ProductCode,'390','900'),'900','N/A')
	IFF(lkp_SupAP_AccountingProductCode IS NULL, 'N/A', lkp_SupAP_AccountingProductCode) AS v_AccountingProductCode,
	-- *INF*: IIF(ISNULL(lkp_SupAP_AccountingProductAbbreviation),'N/A',lkp_SupAP_AccountingProductAbbreviation)
	-- 
	-- --Decode(TRUE, 
	-- --IN(i_ProductCode,'200','340','341') AND i_InsuranceSegmentCode='3','Fully Ceded',
	-- --i_ProductCode='200','CL Auto',
	-- --IN(i_ProductCode,'300','310','320','321','360','365','370','380','381'),'GL',
	--  --IN(i_ProductCode,'311','400','420','440') AND NOT IN(i_InsuranceReferenceLineOfBusinessCode,'530','590'),'Disc',
	-- --i_ProductCode='330','EPLI Monoline',
	-- --(IN(i_ProductCode,'500','520') AND NOT IN(i_InsuranceReferenceLineOfBusinessCode,'530','590','505','506','507')) OR i_ProductCode  ='510' and IN( i_PolicyOfferingCode,'500','600') AND IN(i_StrategicProfitCenterCode,'2','3'),'CL Prop',
	-- --IN(i_ProductCode,'510','620','630','640','650','660') AND i_StrategicProfitCenterCode='3','Bonds-Non Contract',
	-- --i_ProductCode='100' AND  i_InsuranceSegmentAbbreviation='Pool','Pool',
	-- --i_ProductCode='100' AND i_InsuranceSegmentAbbreviation <>'Pool' AND IN(i_RatingPlanCode, '2', '5'),
	-- --'Work Comp - Retro',
	-- --i_ProductCode='100' AND i_InsuranceSegmentAbbreviation <>'Pool','Work Comp - Guaranteed Cost',
	-- --i_PolicyOfferingCode='800' AND i_InsuranceReferenceLineOfBusinessCode <>'812','H&H',
	-- --i_PolicyOfferingCode='801' AND i_InsuranceReferenceLineOfBusinessCode <>'812','Choice',
	-- --i_PolicyOfferingCode='810' AND i_InsuranceReferenceLineOfBusinessCode<>'812','Mono PL Prod',
	-- --IN(i_InsuranceReferenceLineOfBusinessCode,'506','505','507','530','590','812'),'Fully Ceded',
	-- --i_ProductCode='312' AND i_StrategicProfitCenterAbbreviation <>'NSI' ,'Disc',
	-- --i_ProductCode='312' AND i_StrategicProfitCenterAbbreviation='NSI','NFP D&O',
	-- --i_ProductCode='340','Garage Liab',
	-- --i_ProductCode='341','Auto Dlrs',
	-- --i_ProductCode='410','CBOP',
	-- --i_ProductCode='430','SBOP',
	-- --i_ProductCode='450','SMART',
	-- --i_ProductCode='550','CL IM',
	-- --i_ProductCode='553','CL IM',
	-- --i_ProductCode='610','Bonds-Contract',
	-- --IN(i_ProductCode,'390','900'),'CL Umb','N/A')
	IFF(
	    lkp_SupAP_AccountingProductAbbreviation IS NULL, 'N/A',
	    lkp_SupAP_AccountingProductAbbreviation
	) AS v_AccountingProductAbbreviation,
	-- *INF*: IIF(ISNULL(lkp_SupAP_AccountingProductDescription),'N/A',lkp_SupAP_AccountingProductDescription)
	-- 
	-- --Decode(TRUE, 
	-- --IN(i_ProductCode,'200','340','341') AND i_InsuranceSegmentCode='3','Fully Ceded',
	-- --i_ProductCode='200','Commercial Auto',
	-- --IN(i_ProductCode,'300','310','320','321','360','365','370','380','381'),'General Liability',
	-- -- IN(i_ProductCode,'311','400','420','440') AND NOT IN(i_InsuranceReferenceLineOfBusinessCode,'530','590'),'Discontinued Products',
	-- --i_ProductCode='330','Employment Practices Liability Insurance',
	-- --(IN(i_ProductCode,'500','520') AND NOT IN(i_InsuranceReferenceLineOfBusinessCode,'530','590','505','506','507')) OR i_ProductCode  ='510' and IN( -i_PolicyOfferingCode,'500','600') AND IN(i_StrategicProfitCenterCode,'2','3'),'Commercial Property',
	-- --IN(i_ProductCode,'510','620','630','640','650','660') AND i_StrategicProfitCenterCode='3','Bonds-Non Contract',
	-- --i_ProductCode='100' AND  i_InsuranceSegmentAbbreviation='Pool','Pool',
	-- --i_ProductCode='100' AND i_InsuranceSegmentAbbreviation <>'Pool' AND IN(i_RatingPlanCode, '2', '5'),
	-- --'Work Comp - Retro',
	-- --i_ProductCode='100' AND i_InsuranceSegmentAbbreviation <>'Pool','Work Comp - Guaranteed Cost',
	-- --i_PolicyOfferingCode='800' AND i_InsuranceReferenceLineOfBusinessCode <>'812','Home & Highway',
	-- --i_PolicyOfferingCode='801' AND i_InsuranceReferenceLineOfBusinessCode <>'812','West Bend Choice',
	-- --i_PolicyOfferingCode='810' AND i_InsuranceReferenceLineOfBusinessCode<>'812','Mono PL Products',
	-- --IN(i_InsuranceReferenceLineOfBusinessCode,'506','505','507','530','590','812'),'Fully Ceded',
	-- --i_ProductCode='312' AND i_StrategicProfitCenterAbbreviation <>'NSI' ,'Discontinued Products',
	-- --i_ProductCode='312' AND i_StrategicProfitCenterAbbreviation='NSI','Not-for-Profit D&O',
	-- --i_ProductCode='340','Garage Liability',
	-- --i_ProductCode='341','Auto Dealers',
	-- --i_ProductCode='410','CBOP',
	-- --i_ProductCode='430','SBOP',
	-- --i_ProductCode='450','SMARTbusiness',
	-- --i_ProductCode='550','Commercial Inland Marine',
	-- --i_ProductCode='553','Commercial Inland Marine',
	-- --i_ProductCode='610','Bonds-Contract',
	-- --IN(i_ProductCode,'390','900'),'Commercial Umbrella','N/A')
	IFF(
	    lkp_SupAP_AccountingProductDescription IS NULL, 'N/A',
	    lkp_SupAP_AccountingProductDescription
	) AS v_AccountingProductDescription,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_InsuranceReferenceDimID), 'Insert',
	-- lkp_EnterpriseGroupDescription<>i_EnterpriseGroupDescription
	-- OR lkp_InsuranceReferenceLegalEntityDescription<>i_InsuranceReferenceLegalEntityDescription
	-- OR lkp_StrategicProfitCenterAbbreviation<>i_StrategicProfitCenterAbbreviation
	-- OR lkp_StrategicProfitCenterDescription<>i_StrategicProfitCenterDescription
	-- OR lkp_InsuranceSegmentDescription<>i_InsuranceSegmentDescription
	-- OR lkp_PolicyOfferingAbbreviation<>i_PolicyOfferingAbbreviation
	-- OR lkp_PolicyOfferingDescription<>i_PolicyOfferingDescription
	-- OR lkp_ProductAbbreviation<>i_ProductAbbreviation
	-- OR lkp_ProductDescription<>i_ProductDescription
	-- OR lkp_InsuranceReferenceLineOfBusinessAbbreviation<>i_InsuranceReferenceLineOfBusinessAbbreviation
	-- OR lkp_InsuranceReferenceLineOfBusinessDescription<>i_InsuranceReferenceLineOfBusinessDescription
	-- OR lkp_EnterpriseGroupAbbreviation<>i_EnterpriseGroupAbbreviation
	-- OR lkp_InsuranceReferenceLegalEntityAbbreviation<>i_InsuranceReferenceLegalEntityAbbreviation
	-- OR lkp_InsuranceSegmentAbbreviation<>i_InsuranceSegmentAbbreviation OR lkp_RatingPlanAbbreviation<>i_RatingPlanAbbreviation OR lkp_RatingPlanDescription<>i_RatingPlanDescription OR lkp_AccountingProductCode<>v_AccountingProductCode OR lkp_AccountingProductAbbreviation<>v_AccountingProductAbbreviation OR lkp_AccountingProductDescription<>v_AccountingProductDescription, 'Update',
	--  'NoChange')
	DECODE(
	    TRUE,
	    lkp_InsuranceReferenceDimID IS NULL, 'Insert',
	    lkp_EnterpriseGroupDescription <> i_EnterpriseGroupDescription OR lkp_InsuranceReferenceLegalEntityDescription <> i_InsuranceReferenceLegalEntityDescription OR lkp_StrategicProfitCenterAbbreviation <> i_StrategicProfitCenterAbbreviation OR lkp_StrategicProfitCenterDescription <> i_StrategicProfitCenterDescription OR lkp_InsuranceSegmentDescription <> i_InsuranceSegmentDescription OR lkp_PolicyOfferingAbbreviation <> i_PolicyOfferingAbbreviation OR lkp_PolicyOfferingDescription <> i_PolicyOfferingDescription OR lkp_ProductAbbreviation <> i_ProductAbbreviation OR lkp_ProductDescription <> i_ProductDescription OR lkp_InsuranceReferenceLineOfBusinessAbbreviation <> i_InsuranceReferenceLineOfBusinessAbbreviation OR lkp_InsuranceReferenceLineOfBusinessDescription <> i_InsuranceReferenceLineOfBusinessDescription OR lkp_EnterpriseGroupAbbreviation <> i_EnterpriseGroupAbbreviation OR lkp_InsuranceReferenceLegalEntityAbbreviation <> i_InsuranceReferenceLegalEntityAbbreviation OR lkp_InsuranceSegmentAbbreviation <> i_InsuranceSegmentAbbreviation OR lkp_RatingPlanAbbreviation <> i_RatingPlanAbbreviation OR lkp_RatingPlanDescription <> i_RatingPlanDescription OR lkp_AccountingProductCode <> v_AccountingProductCode OR lkp_AccountingProductAbbreviation <> v_AccountingProductAbbreviation OR lkp_AccountingProductDescription <> v_AccountingProductDescription, 'Update',
	    'NoChange'
	) AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AudutId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: IIF(ISNULL(i_EnterpriseGroupCode),'N/A',i_EnterpriseGroupCode)
	IFF(i_EnterpriseGroupCode IS NULL, 'N/A', i_EnterpriseGroupCode) AS o_EnterpriseGroupCode,
	-- *INF*: IIF(ISNULL(i_EnterpriseGroupDescription),'N/A',i_EnterpriseGroupDescription)
	-- 
	IFF(i_EnterpriseGroupDescription IS NULL, 'N/A', i_EnterpriseGroupDescription) AS o_EnterpriseGroupDescription,
	-- *INF*: IIF(ISNULL(i_InsuranceReferenceLegalEntityCode),'N/A',i_InsuranceReferenceLegalEntityCode)
	IFF(i_InsuranceReferenceLegalEntityCode IS NULL, 'N/A', i_InsuranceReferenceLegalEntityCode) AS o_InsuranceReferenceLegalEntityCode,
	-- *INF*: IIF(ISNULL(i_InsuranceReferenceLegalEntityDescription),'N/A',i_InsuranceReferenceLegalEntityDescription)
	IFF(
	    i_InsuranceReferenceLegalEntityDescription IS NULL, 'N/A',
	    i_InsuranceReferenceLegalEntityDescription
	) AS o_InsuranceReferenceLegalEntityDescription,
	-- *INF*: IIF(ISNULL(i_StrategicProfitCenterCode),'N/A',i_StrategicProfitCenterCode)
	IFF(i_StrategicProfitCenterCode IS NULL, 'N/A', i_StrategicProfitCenterCode) AS o_StrategicProfitCenterCode,
	-- *INF*: IIF(ISNULL(i_StrategicProfitCenterAbbreviation),'N/A',i_StrategicProfitCenterAbbreviation)
	IFF(i_StrategicProfitCenterAbbreviation IS NULL, 'N/A', i_StrategicProfitCenterAbbreviation) AS o_StrategicProfitCenterAbbreviation,
	-- *INF*: IIF(ISNULL(i_StrategicProfitCenterDescription),'N/A',i_StrategicProfitCenterDescription)
	IFF(i_StrategicProfitCenterDescription IS NULL, 'N/A', i_StrategicProfitCenterDescription) AS o_StrategicProfitCenterDescription,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentCode),'N/A',i_InsuranceSegmentCode)
	IFF(i_InsuranceSegmentCode IS NULL, 'N/A', i_InsuranceSegmentCode) AS o_InsuranceSegmentCode,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentDescription),'N/A',i_InsuranceSegmentDescription)
	IFF(i_InsuranceSegmentDescription IS NULL, 'N/A', i_InsuranceSegmentDescription) AS o_InsuranceSegmentDescription,
	-- *INF*: IIF(ISNULL(i_PolicyOfferingCode),'N/A',i_PolicyOfferingCode)
	IFF(i_PolicyOfferingCode IS NULL, 'N/A', i_PolicyOfferingCode) AS o_PolicyOfferingCode,
	-- *INF*: IIF(ISNULL(i_PolicyOfferingAbbreviation),'N/A',i_PolicyOfferingAbbreviation)
	IFF(i_PolicyOfferingAbbreviation IS NULL, 'N/A', i_PolicyOfferingAbbreviation) AS o_PolicyOfferingAbbreviation,
	-- *INF*: IIF(ISNULL(i_PolicyOfferingDescription),'N/A',i_PolicyOfferingDescription)
	IFF(i_PolicyOfferingDescription IS NULL, 'N/A', i_PolicyOfferingDescription) AS o_PolicyOfferingDescription,
	-- *INF*: IIF(ISNULL(i_ProductCode),'N/A',i_ProductCode)
	IFF(i_ProductCode IS NULL, 'N/A', i_ProductCode) AS o_ProductCode,
	-- *INF*: IIF(ISNULL(i_ProductAbbreviation),'N/A',i_ProductAbbreviation)
	IFF(i_ProductAbbreviation IS NULL, 'N/A', i_ProductAbbreviation) AS o_ProductAbbreviation,
	-- *INF*: IIF(ISNULL(i_ProductDescription),'N/A',i_ProductDescription)
	IFF(i_ProductDescription IS NULL, 'N/A', i_ProductDescription) AS o_ProductDescription,
	-- *INF*: IIF(ISNULL(i_InsuranceReferenceLineOfBusinessCode),'N/A',i_InsuranceReferenceLineOfBusinessCode)
	IFF(
	    i_InsuranceReferenceLineOfBusinessCode IS NULL, 'N/A',
	    i_InsuranceReferenceLineOfBusinessCode
	) AS o_InsuranceReferenceLineOfBusinessCode,
	-- *INF*: IIF(ISNULL(i_InsuranceReferenceLineOfBusinessAbbreviation),'N/A',i_InsuranceReferenceLineOfBusinessAbbreviation)
	IFF(
	    i_InsuranceReferenceLineOfBusinessAbbreviation IS NULL, 'N/A',
	    i_InsuranceReferenceLineOfBusinessAbbreviation
	) AS o_InsuranceReferenceLineOfBusinessAbbreviation,
	-- *INF*: IIF(ISNULL(i_InsuranceReferenceLineOfBusinessDescription),'N/A',i_InsuranceReferenceLineOfBusinessDescription)
	IFF(
	    i_InsuranceReferenceLineOfBusinessDescription IS NULL, 'N/A',
	    i_InsuranceReferenceLineOfBusinessDescription
	) AS o_InsuranceReferenceLineOfBusinessDescription,
	'N/A' AS o_InsuranceReferenceCoverageTypeCode,
	'N/A' AS o_InsuranceReferenceCoverageTypeDescription,
	-- *INF*: IIF(ISNULL(i_EnterpriseGroupAbbreviation),'N/A',i_EnterpriseGroupAbbreviation)
	IFF(i_EnterpriseGroupAbbreviation IS NULL, 'N/A', i_EnterpriseGroupAbbreviation) AS o_EnterpriseGroupAbbreviation,
	-- *INF*: IIF(ISNULL(i_InsuranceReferenceLegalEntityAbbreviation),'N/A',i_InsuranceReferenceLegalEntityAbbreviation)
	IFF(
	    i_InsuranceReferenceLegalEntityAbbreviation IS NULL, 'N/A',
	    i_InsuranceReferenceLegalEntityAbbreviation
	) AS o_InsuranceReferenceLegalEntityAbbreviation,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentAbbreviation),'N/A',i_InsuranceSegmentAbbreviation)
	IFF(i_InsuranceSegmentAbbreviation IS NULL, 'N/A', i_InsuranceSegmentAbbreviation) AS o_InsuranceSegmentAbbreviation,
	v_AccountingProductCode AS o_AccountingProductCode,
	v_AccountingProductAbbreviation AS o_AccountingProductAbbreviation,
	v_AccountingProductDescription AS o_AccountingProductDescription,
	i_RatingPlanCode AS o_RatingPlanCode,
	i_RatingPlanAbbreviation AS o_RatingPlanAbbreviation,
	i_RatingPlanDescription AS o_RatingPlanDescription
	FROM SQ_Association
	LEFT JOIN LKP_InsuranceReferenceDim
	ON LKP_InsuranceReferenceDim.EnterpriseGroupCode = SQ_Association.EnterpriseGroupCode AND LKP_InsuranceReferenceDim.InsuranceReferenceLegalEntityCode = SQ_Association.InsuranceReferenceLegalEntityCode AND LKP_InsuranceReferenceDim.StrategicProfitCenterCode = SQ_Association.StrategicProfitCenterCode AND LKP_InsuranceReferenceDim.InsuranceSegmentCode = SQ_Association.InsuranceSegmentCode AND LKP_InsuranceReferenceDim.PolicyOfferingCode = SQ_Association.PolicyOfferingCode AND LKP_InsuranceReferenceDim.ProductCode = SQ_Association.ProductCode AND LKP_InsuranceReferenceDim.InsuranceReferenceLineOfBusinessCode = SQ_Association.InsuranceReferenceLineOfBusinessCode AND LKP_InsuranceReferenceDim.RatingPlanCode = SQ_Association.RatingPlanCode
	LEFT JOIN LKP_SupAccountingProduct
	ON LKP_SupAccountingProduct.StrategicProfitCenterCode = EXP_inputToLookup.StrategicProfitCenterCode AND LKP_SupAccountingProduct.InsuranceSegmentCode = EXP_inputToLookup.InsuranceSegmentCode AND LKP_SupAccountingProduct.PolicyOfferingCode = EXP_inputToLookup.PolicyOfferingCode AND LKP_SupAccountingProduct.LineOfBusinessCode = EXP_inputToLookup.InsuranceReferenceLineOfBusinessCode AND LKP_SupAccountingProduct.RatingPlanCode = EXP_inputToLookup.RatingPlanCode AND LKP_SupAccountingProduct.ProductCode = EXP_inputToLookup.ProductCode
),
RTR_InsertAndUpdate AS (
	SELECT
	lkp_InsuranceReferenceDimID,
	o_ChangeFlag AS ChangeFlag,
	o_AudutId AS AudutId,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_EnterpriseGroupCode AS EnterpriseGroupCode,
	o_EnterpriseGroupDescription AS EnterpriseGroupDescription,
	o_InsuranceReferenceLegalEntityCode AS InsuranceReferenceLegalEntityCode,
	o_InsuranceReferenceLegalEntityDescription AS InsuranceReferenceLegalEntityDescription,
	o_StrategicProfitCenterCode AS StrategicProfitCenterCode,
	o_StrategicProfitCenterAbbreviation AS StrategicProfitCenterAbbreviation,
	o_StrategicProfitCenterDescription AS StrategicProfitCenterDescription,
	o_InsuranceSegmentCode AS InsuranceSegmentCode,
	o_InsuranceSegmentDescription AS InsuranceSegmentDescription,
	o_PolicyOfferingCode AS PolicyOfferingCode,
	o_PolicyOfferingAbbreviation AS PolicyOfferingAbbreviation,
	o_PolicyOfferingDescription AS PolicyOfferingDescription,
	o_ProductCode AS ProductCode,
	o_ProductAbbreviation AS ProductAbbreviation,
	o_ProductDescription AS ProductDescription,
	o_InsuranceReferenceLineOfBusinessCode AS InsuranceReferenceLineOfBusinessCode,
	o_InsuranceReferenceLineOfBusinessAbbreviation AS InsuranceReferenceLineOfBusinessAbbreviation,
	o_InsuranceReferenceLineOfBusinessDescription AS InsuranceReferenceLineOfBusinessDescription,
	o_InsuranceReferenceCoverageTypeCode AS InsuranceReferenceCoverageTypeCode,
	o_InsuranceReferenceCoverageTypeDescription AS InsuranceReferenceCoverageTypeDescription,
	o_EnterpriseGroupAbbreviation AS EnterpriseGroupAbbreviation,
	o_InsuranceReferenceLegalEntityAbbreviation AS InsuranceReferenceLegalEntityAbbreviation,
	o_InsuranceSegmentAbbreviation AS InsuranceSegmentAbbreviation,
	o_AccountingProductCode AS AccountingProductCode,
	o_AccountingProductAbbreviation AS AccountingProductAbbreviation,
	o_AccountingProductDescription AS AccountingProductDescription,
	o_RatingPlanCode AS RatingPlanCode,
	o_RatingPlanAbbreviation AS RatingPlanAbbreviation,
	o_RatingPlanDescription AS RatingPlanDescription
	FROM EXP_Detect_Changes_Add_MetaData
),
RTR_InsertAndUpdate_Insert AS (SELECT * FROM RTR_InsertAndUpdate WHERE ChangeFlag='Insert' AND IIF(InsuranceSegmentAbbreviation='Pool' AND ProductAbbreviation='WC' AND RatingPlanAbbreviation<>'Guar Cost', FALSE, TRUE)),
RTR_InsertAndUpdate_Update AS (SELECT * FROM RTR_InsertAndUpdate WHERE ChangeFlag='Update'),
UPD_Insert AS (
	SELECT
	AudutId, 
	CreatedDate, 
	ModifiedDate, 
	EnterpriseGroupCode, 
	EnterpriseGroupDescription, 
	InsuranceReferenceLegalEntityCode, 
	InsuranceReferenceLegalEntityDescription, 
	StrategicProfitCenterCode, 
	StrategicProfitCenterAbbreviation, 
	StrategicProfitCenterDescription, 
	InsuranceSegmentCode, 
	InsuranceSegmentDescription, 
	PolicyOfferingCode, 
	PolicyOfferingAbbreviation, 
	PolicyOfferingDescription, 
	ProductCode, 
	ProductAbbreviation, 
	ProductDescription, 
	InsuranceReferenceLineOfBusinessCode, 
	InsuranceReferenceLineOfBusinessAbbreviation, 
	InsuranceReferenceLineOfBusinessDescription, 
	InsuranceReferenceCoverageTypeCode, 
	InsuranceReferenceCoverageTypeDescription, 
	EnterpriseGroupAbbreviation, 
	InsuranceReferenceLegalEntityAbbreviation, 
	InsuranceSegmentAbbreviation, 
	AccountingProductCode AS AccountingProductCode1, 
	AccountingProductAbbreviation AS AccountingProductAbbreviation1, 
	AccountingProductDescription AS AccountingProductDescription1, 
	RatingPlanCode AS RatingPlanCode1, 
	RatingPlanAbbreviation AS RatingPlanAbbreviation1, 
	RatingPlanDescription AS RatingPlanDescription1
	FROM RTR_InsertAndUpdate_Insert
),
TGT_InsuranceReferenceDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceDim
	(AuditId, CreatedDate, ModifiedDate, EnterpriseGroupCode, EnterpriseGroupDescription, InsuranceReferenceLegalEntityCode, InsuranceReferenceLegalEntityDescription, StrategicProfitCenterCode, StrategicProfitCenterAbbreviation, StrategicProfitCenterDescription, InsuranceSegmentCode, InsuranceSegmentDescription, PolicyOfferingCode, PolicyOfferingAbbreviation, PolicyOfferingDescription, ProductCode, ProductAbbreviation, ProductDescription, InsuranceReferenceLineOfBusinessCode, InsuranceReferenceLineOfBusinessAbbreviation, InsuranceReferenceLineOfBusinessDescription, InsuranceReferenceCoverageTypeCode, InsuranceReferenceCoverageTypeDescription, EnterpriseGroupAbbreviation, InsuranceReferenceLegalEntityAbbreviation, InsuranceSegmentAbbreviation, AccountingProductCode, AccountingProductAbbreviation, AccountingProductDescription, RatingPlanCode, RatingPlanAbbreviation, RatingPlanDescription)
	SELECT 
	AudutId AS AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	ENTERPRISEGROUPCODE, 
	ENTERPRISEGROUPDESCRIPTION, 
	INSURANCEREFERENCELEGALENTITYCODE, 
	INSURANCEREFERENCELEGALENTITYDESCRIPTION, 
	STRATEGICPROFITCENTERCODE, 
	STRATEGICPROFITCENTERABBREVIATION, 
	STRATEGICPROFITCENTERDESCRIPTION, 
	INSURANCESEGMENTCODE, 
	INSURANCESEGMENTDESCRIPTION, 
	POLICYOFFERINGCODE, 
	POLICYOFFERINGABBREVIATION, 
	POLICYOFFERINGDESCRIPTION, 
	PRODUCTCODE, 
	PRODUCTABBREVIATION, 
	PRODUCTDESCRIPTION, 
	INSURANCEREFERENCELINEOFBUSINESSCODE, 
	INSURANCEREFERENCELINEOFBUSINESSABBREVIATION, 
	INSURANCEREFERENCELINEOFBUSINESSDESCRIPTION, 
	INSURANCEREFERENCECOVERAGETYPECODE, 
	INSURANCEREFERENCECOVERAGETYPEDESCRIPTION, 
	ENTERPRISEGROUPABBREVIATION, 
	INSURANCEREFERENCELEGALENTITYABBREVIATION, 
	INSURANCESEGMENTABBREVIATION, 
	AccountingProductCode1 AS ACCOUNTINGPRODUCTCODE, 
	AccountingProductAbbreviation1 AS ACCOUNTINGPRODUCTABBREVIATION, 
	AccountingProductDescription1 AS ACCOUNTINGPRODUCTDESCRIPTION, 
	RatingPlanCode1 AS RATINGPLANCODE, 
	RatingPlanAbbreviation1 AS RATINGPLANABBREVIATION, 
	RatingPlanDescription1 AS RATINGPLANDESCRIPTION
	FROM UPD_Insert
),
UPD_Update AS (
	SELECT
	lkp_InsuranceReferenceDimID, 
	AudutId, 
	ModifiedDate, 
	EnterpriseGroupDescription, 
	InsuranceReferenceLegalEntityDescription, 
	StrategicProfitCenterAbbreviation, 
	StrategicProfitCenterDescription, 
	InsuranceSegmentDescription, 
	PolicyOfferingAbbreviation, 
	PolicyOfferingDescription, 
	ProductAbbreviation, 
	ProductDescription, 
	InsuranceReferenceLineOfBusinessAbbreviation, 
	InsuranceReferenceLineOfBusinessDescription, 
	InsuranceReferenceCoverageTypeCode, 
	InsuranceReferenceCoverageTypeDescription, 
	EnterpriseGroupAbbreviation, 
	InsuranceReferenceLegalEntityAbbreviation, 
	InsuranceSegmentAbbreviation, 
	AccountingProductCode AS AccountingProductCode3, 
	AccountingProductAbbreviation AS AccountingProductAbbreviation3, 
	AccountingProductDescription AS AccountingProductDescription3, 
	RatingPlanAbbreviation AS RatingPlanAbbreviation3, 
	RatingPlanDescription AS RatingPlanDescription3
	FROM RTR_InsertAndUpdate_Update
),
TGT_InsuranceReferenceDim_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceDim AS T
	USING UPD_Update AS S
	ON T.InsuranceReferenceDimId = S.lkp_InsuranceReferenceDimID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AudutId, T.ModifiedDate = S.ModifiedDate, T.EnterpriseGroupDescription = S.EnterpriseGroupDescription, T.InsuranceReferenceLegalEntityDescription = S.InsuranceReferenceLegalEntityDescription, T.StrategicProfitCenterAbbreviation = S.StrategicProfitCenterAbbreviation, T.StrategicProfitCenterDescription = S.StrategicProfitCenterDescription, T.InsuranceSegmentDescription = S.InsuranceSegmentDescription, T.PolicyOfferingAbbreviation = S.PolicyOfferingAbbreviation, T.PolicyOfferingDescription = S.PolicyOfferingDescription, T.ProductAbbreviation = S.ProductAbbreviation, T.ProductDescription = S.ProductDescription, T.InsuranceReferenceLineOfBusinessAbbreviation = S.InsuranceReferenceLineOfBusinessAbbreviation, T.InsuranceReferenceLineOfBusinessDescription = S.InsuranceReferenceLineOfBusinessDescription, T.InsuranceReferenceCoverageTypeCode = S.InsuranceReferenceCoverageTypeCode, T.InsuranceReferenceCoverageTypeDescription = S.InsuranceReferenceCoverageTypeDescription, T.EnterpriseGroupAbbreviation = S.EnterpriseGroupAbbreviation, T.InsuranceReferenceLegalEntityAbbreviation = S.InsuranceReferenceLegalEntityAbbreviation, T.InsuranceSegmentAbbreviation = S.InsuranceSegmentAbbreviation, T.AccountingProductCode = S.AccountingProductCode3, T.AccountingProductAbbreviation = S.AccountingProductAbbreviation3, T.AccountingProductDescription = S.AccountingProductDescription3, T.RatingPlanAbbreviation = S.RatingPlanAbbreviation3, T.RatingPlanDescription = S.RatingPlanDescription3
),