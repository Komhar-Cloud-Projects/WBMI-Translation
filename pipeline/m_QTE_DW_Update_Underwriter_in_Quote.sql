WITH
LKP_UnderwriterProductRelationship_UnderwritingAssociateAKID_ByAgySPCPOProg AS (
	SELECT
	AssociateCode,
	StrategicProfitCenterCode,
	AgencyCode,
	PolicyAmountMinimum,
	PolicyAmountMaximum,
	PolicyOfferingCode,
	ProgramCode
	FROM (
		SELECT DISTINCT 
		UPR.StrategicProfitCenterCode as StrategicProfitCenterCode, 
		Agency.AgencyCode as AgencyCode,
		UPR.PolicyAmountMinimum as PolicyAmountMinimum, 
		UPR.PolicyAmountMaximum as PolicyAmountMaximum, 
		UPR.PolicyOfferingCode as PolicyOfferingCode,
		UPR.ProgramCode as ProgramCode,
		MAX(UA.RoleSpecificUserCode) as AssociateCode
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwriterProductRelationship UPR
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwriterAgencyRelationship UAR
		on UPR.AssociateId=UAR.AssociateId
		and UAR.StrategicProfitCenterCode=UPR.StrategicProfitCenterCode
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Associate UA
		on UAR.AssociateId=UA.AssociateID
		AND UAR.StrategicProfitCenterCode=UA.StrategicProfitCenterCode
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.VWAgency Agency
		on UAR.AgencyId=Agency.AgencyID
		Where UA.associaterole = 'UNDERWRITER'
		group by 
		UPR.StrategicProfitCenterCode,
		Agency.AgencyCode,
		UPR.PolicyAmountMinimum, 
		UPR.PolicyAmountMaximum,
		UPR.PolicyOfferingCode,
		UPR.ProgramCode
		having count(distinct UA.RoleSpecificUserCode) = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterCode,AgencyCode,PolicyAmountMinimum,PolicyAmountMaximum,PolicyOfferingCode,ProgramCode ORDER BY AssociateCode DESC) = 1
),
LKP_UnderwriterProductRelationship_UnderwritingAssociateAKID_byAgySPC AS (
	SELECT
	AssociateCode,
	StrategicProfitCenterCode,
	AgencyCode,
	PolicyAmountMinimum,
	PolicyAmountMaximum
	FROM (
		SELECT DISTINCT 
		UPR.StrategicProfitCenterCode as StrategicProfitCenterCode, 
		Agency.AgencyCode as AgencyCode,
		UPR.PolicyAmountMinimum as PolicyAmountMinimum, 
		UPR.PolicyAmountMaximum as PolicyAmountMaximum, 
		MAX(UA.RoleSpecificUserCode) as AssociateCode
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwriterProductRelationship UPR
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwriterAgencyRelationship UAR
		on UPR.AssociateId=UAR.AssociateId
		and UAR.StrategicProfitCenterCode=UPR.StrategicProfitCenterCode
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Associate UA
		on UAR.AssociateId=UA.AssociateID
		AND UAR.StrategicProfitCenterCode=UA.StrategicProfitCenterCode
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.VWAgency Agency
		on UAR.AgencyId=Agency.AgencyID
		Where UA.associaterole = 'UNDERWRITER'
		group by 
		UPR.StrategicProfitCenterCode, 
		Agency.AgencyCode,
		UPR.PolicyAmountMinimum, 
		UPR.PolicyAmountMaximum
		having count(distinct UA.RoleSpecificUserCode) = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterCode,AgencyCode,PolicyAmountMinimum,PolicyAmountMaximum ORDER BY AssociateCode DESC) = 1
),
LKP_UnderwriterProductRelationship_UnderwritingAssociateAKID_byAgency AS (
	SELECT
	AssociateCode,
	StrategicProfitCenterCode,
	AgencyCode
	FROM (
		SELECT DISTINCT MAX(UA.RoleSpecificUserCode) AS AssociateCode,
		UAR.StrategicProfitCenterCode as StrategicProfitCenterCode,
		Agency.AgencyCode as AgencyCode
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwriterAgencyRelationship UAR
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Associate UA
		on UAR.AssociateId=UA.AssociateID
		AND UAR.StrategicProfitCenterCode=UA.StrategicProfitCenterCode
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.VWAgency Agency
		on UAR.AgencyId=Agency.AgencyID
		WHERE UA.associaterole = 'UNDERWRITER'
		group by 
		UAR.StrategicProfitCenterCode,
		Agency.AgencyCode
		having count(DISTINCT UA.RoleSpecificUserCode) = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterCode,AgencyCode ORDER BY AssociateCode DESC) = 1
),
LKP_UnderwriterProductRelationship_UnderwritingAssociateAKID_byAgySPCPO AS (
	SELECT
	AssociateCode,
	StrategicProfitCenterCode,
	AgencyCode,
	PolicyAmountMinimum,
	PolicyAmountMaximum,
	PolicyOfferingCode
	FROM (
		SELECT DISTINCT 
		UPR.StrategicProfitCenterCode as StrategicProfitCenterCode, 
		Agency.AgencyCode as AgencyCode,
		UPR.PolicyAmountMinimum as PolicyAmountMinimum, 
		UPR.PolicyAmountMaximum as PolicyAmountMaximum, 
		UPR.PolicyOfferingCode as PolicyOfferingCode,
		MAX(UA.RoleSpecificUserCode) as AssociateCode
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwriterProductRelationship UPR
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwriterAgencyRelationship UAR
		on UPR.AssociateId=UAR.AssociateId
		and UAR.StrategicProfitCenterCode=UPR.StrategicProfitCenterCode
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Associate UA
		on UAR.AssociateId=UA.AssociateID
		AND UAR.StrategicProfitCenterCode=UA.StrategicProfitCenterCode
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.VWAgency Agency
		on UAR.AgencyId=Agency.AgencyID
		Where UA.associaterole = 'UNDERWRITER'
		group by 
		UPR.StrategicProfitCenterCode, 
		Agency.AgencyCode,
		UPR.PolicyAmountMinimum, 
		UPR.PolicyAmountMaximum,
		UPR.PolicyOfferingCode
		having count(distinct UA.RoleSpecificUserCode) = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterCode,AgencyCode,PolicyAmountMinimum,PolicyAmountMaximum,PolicyOfferingCode ORDER BY AssociateCode DESC) = 1
),
LKP_BondProducts AS (
	SELECT
	ProductCode,
	QuoteId
	FROM (
		select Quote.QuoteId as QuoteId,
		Product.ProductCode as ProductCode
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.QuoteTransaction
		on Quote.QuoteAKId=QuoteTransaction.QuoteAKID
		and Quote.CurrentSnapshotFlag=1
		and Quote.StatusDate=QuoteTransaction.StatusDate
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product
		on QuoteTransaction.ProductAKId=Product.ProductAKId
		and Product.CurrentSnapshotFlag=1
		where Product.ProductCode in('610','620','630','640','650','660')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY QuoteId ORDER BY ProductCode DESC) = 1
),
LKP_UnderwriterProductRelationship_UnderwritingAssociateAKID_ByAgySPCPOProgBond AS (
	SELECT
	AssociateCode,
	StrategicProfitCenterCode,
	AgencyCode,
	PolicyAmountMinimum,
	PolicyAmountMaximum,
	PolicyOfferingCode,
	ProgramCode,
	BondCategory
	FROM (
		SELECT DISTINCT 
		UPR.StrategicProfitCenterCode as StrategicProfitCenterCode, 
		Agency.AgencyCode as AgencyCode,
		UPR.PolicyAmountMinimum as PolicyAmountMinimum, 
		UPR.PolicyAmountMaximum as PolicyAmountMaximum, 
		UPR.PolicyOfferingCode as PolicyOfferingCode,
		UPR.ProgramCode as ProgramCode,
		UPR.BondCategory as BondCategory,
		MAX(UA.RoleSpecificUserCode) as AssociateCode
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwriterProductRelationship UPR
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwriterAgencyRelationship UAR
		on UPR.AssociateId=UAR.AssociateId
		and UAR.StrategicProfitCenterCode=UPR.StrategicProfitCenterCode
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Associate UA
		on UAR.AssociateId=UA.AssociateID
		AND UAR.StrategicProfitCenterCode=UA.StrategicProfitCenterCode
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.VWAgency Agency
		on UAR.AgencyId=Agency.AgencyID
		Where UA.associaterole = 'UNDERWRITER'
		group by 
		UPR.StrategicProfitCenterCode,
		Agency.AgencyCode,
		UPR.PolicyAmountMinimum, 
		UPR.PolicyAmountMaximum,
		UPR.PolicyOfferingCode,
		UPR.ProgramCode,
		UPR.BondCategory
		having count(distinct UA.RoleSpecificUserCode) = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterCode,AgencyCode,PolicyAmountMinimum,PolicyAmountMaximum,PolicyOfferingCode,ProgramCode,BondCategory ORDER BY AssociateCode DESC) = 1
),
LKP_UnderwriterWorkCompPool AS (
	SELECT
	AssociateCode,
	Dummy
	FROM (
		select distinct 1 as Dummy,
		a.RoleSpecificUserCode as AssociateCode
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.associate a
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwriterProductRelationship b on a.AssociateID = b.AssociateId
		where a.associaterole = 'UNDERWRITER'
		and b.InsuranceSegmentCode = '3'
		group by a.RoleSpecificUserCode
		having count(DISTINCT a.RoleSpecificUserCode) = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Dummy ORDER BY AssociateCode DESC) = 1
),
SQ_Quote AS (
	select Q.QuoteId,
	Q.AgencyAKId,
	Q.StrategicProfitCenterAKId,
	Q.PolicyOfferingAKId,
	Q.ProgramAKId,
	ISNULL(SUM(QC.WrittenPremium),0) AS WrittenPremium
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote Q
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.QuoteTransaction QC
	on Q.QuoteAKId=QC.QuoteAKID
	and Q.StatusDate=QC.StatusDate
	where Q.underwritingassociateakid=-1
	GROUP BY Q.QuoteId,
	Q.AgencyAKId,Q.StrategicProfitCenterAKId,Q.ProgramAKId,Q.StrategicProfitCenterAKId,Q.PolicyOfferingAKId 
	ORDER BY Q.QuoteId
),
LKP_Agency_V2 AS (
	SELECT
	AgencyCode,
	TerminatedDate,
	AgencyAKID
	FROM (
		SELECT 
			AgencyCode,
			TerminatedDate,
			AgencyAKID
		FROM V2.Agency
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID ORDER BY AgencyCode) = 1
),
LKP_InsuranceSegment AS (
	SELECT
	InsuranceSegmentCode,
	InsuranceSegmentAKId
	FROM (
		SELECT 
			InsuranceSegmentCode,
			InsuranceSegmentAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceSegmentAKId ORDER BY InsuranceSegmentCode) = 1
),
LKP_PolicyOffering AS (
	SELECT
	PolicyOfferingCode,
	PolicyOfferingAKId
	FROM (
		SELECT 
			PolicyOfferingCode,
			PolicyOfferingAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyOfferingAKId ORDER BY PolicyOfferingCode) = 1
),
LKP_Program AS (
	SELECT
	ProgramCode,
	ProgramAKId
	FROM (
		SELECT 
			ProgramCode,
			ProgramAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Program
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProgramAKId ORDER BY ProgramCode) = 1
),
LKP_StrategicProfitCenter AS (
	SELECT
	StrategicProfitCenterCode,
	StrategicProfitCenterAKId
	FROM (
		SELECT 
			StrategicProfitCenterCode,
			StrategicProfitCenterAKId
		FROM StrategicProfitCenter
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterAKId ORDER BY StrategicProfitCenterCode) = 1
),
EXP_Quote_ProductRelationship AS (
	SELECT
	LKP_StrategicProfitCenter.StrategicProfitCenterCode AS i_StrategicProfitCenterCode,
	LKP_Agency_V2.AgencyCode AS i_AgencyCode,
	SQ_Quote.QuoteId AS i_QuoteId,
	SQ_Quote.WrittenPremium AS i_WrittenPremium,
	-- *INF*: IIF(i_StrategicProfitCenterCode='5',i_WrittenPremium,0)
	IFF(i_StrategicProfitCenterCode = '5', i_WrittenPremium, 0) AS v_WrittenPremium,
	LKP_PolicyOffering.PolicyOfferingCode AS i_PolicyOfferingCode,
	LKP_Program.ProgramCode AS i_ProgramCode,
	LKP_Agency_V2.TerminatedDate AS i_TerminatedDate,
	LKP_InsuranceSegment.InsuranceSegmentCode AS i_InsuranceSegmentCode,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(:LKP.LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGENCY(i_StrategicProfitCenterCode,i_AgencyCode)),NULL,:LKP.LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGENCY(i_StrategicProfitCenterCode,i_AgencyCode))
	DECODE(
	    TRUE,
	    LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGENCY_i_StrategicProfitCenterCode_i_AgencyCode.AssociateCode IS NULL, NULL,
	    LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGENCY_i_StrategicProfitCenterCode_i_AgencyCode.AssociateCode
	) AS v_UnderwritingAssociateAKID_Agy,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(v_UnderwritingAssociateAKID_Agy), :LKP.LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPC(i_StrategicProfitCenterCode,i_AgencyCode,v_WrittenPremium),v_UnderwritingAssociateAKID_Agy)
	DECODE(
	    TRUE,
	    v_UnderwritingAssociateAKID_Agy IS NULL, LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPC_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium.AssociateCode,
	    v_UnderwritingAssociateAKID_Agy
	) AS v_UnderwritingAssociateAKID_AgySPC,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(v_UnderwritingAssociateAKID_AgySPC), :LKP.LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPO(i_StrategicProfitCenterCode,i_AgencyCode,v_WrittenPremium,i_PolicyOfferingCode),v_UnderwritingAssociateAKID_AgySPC)
	DECODE(
	    TRUE,
	    v_UnderwritingAssociateAKID_AgySPC IS NULL, LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPO_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode.AssociateCode,
	    v_UnderwritingAssociateAKID_AgySPC
	) AS v_UnderwritingAssociateAKID_AgySPCPO,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(v_UnderwritingAssociateAKID_AgySPCPO),
	-- :LKP.LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROG(i_StrategicProfitCenterCode,i_AgencyCode,v_WrittenPremium,i_PolicyOfferingCode, i_ProgramCode),v_UnderwritingAssociateAKID_AgySPCPO)
	DECODE(
	    TRUE,
	    v_UnderwritingAssociateAKID_AgySPCPO IS NULL, LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROG_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode.AssociateCode,
	    v_UnderwritingAssociateAKID_AgySPCPO
	) AS v_UnderwritingAssociateAKID_AgySPCPOProg,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(v_UnderwritingAssociateAKID_AgySPCPOProg),
	-- 
	-- DECODE(TRUE,
	-- 
	-- :LKP.LKP_BONDPRODUCTS(i_QuoteId) = '610',
	-- :LKP.LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND(i_StrategicProfitCenterCode,i_AgencyCode,v_WrittenPremium,i_PolicyOfferingCode, i_ProgramCode,'Contract'),
	-- 
	-- IN(:LKP.LKP_BONDPRODUCTS(i_QuoteId),'620','630','640','650','660'),
	-- :LKP.LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND(i_StrategicProfitCenterCode,i_AgencyCode,v_WrittenPremium,i_PolicyOfferingCode, i_ProgramCode,'NonContract')
	-- 
	-- 
	-- ),v_UnderwritingAssociateAKID_AgySPCPOProg)
	DECODE(
	    TRUE,
	    v_UnderwritingAssociateAKID_AgySPCPOProg IS NULL, DECODE(
	        TRUE,
	        LKP_BONDPRODUCTS_i_QuoteId.ProductCode = '610', LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode_Contract.AssociateCode,
	        LKP_BONDPRODUCTS_i_QuoteId.ProductCode IN ('620','630','640','650','660'), LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode_NonContract.AssociateCode
	    ),
	    v_UnderwritingAssociateAKID_AgySPCPOProg
	) AS v_UnderwritingAssociateAKID_AgySPCPOProgBond,
	-- *INF*: IIF(ISNULL(v_UnderwritingAssociateAKID_AgySPCPOProgBond),'N/A', v_UnderwritingAssociateAKID_AgySPCPOProgBond)
	IFF(
	    v_UnderwritingAssociateAKID_AgySPCPOProgBond IS NULL, 'N/A',
	    v_UnderwritingAssociateAKID_AgySPCPOProgBond
	) AS v_UnderwritingAssociateCode,
	i_QuoteId AS o_QuoteId,
	-- *INF*: DECODE(TRUE,
	-- --Look for work comp pool underwriter
	-- v_UnderwritingAssociateCode='N/A' AND i_InsuranceSegmentCode = '3',
	-- IIF(ISNULL(:LKP.LKP_UNDERWRITERWORKCOMPPOOL(1)),'N/A',:LKP.LKP_UNDERWRITERWORKCOMPPOOL(1)),
	-- 
	-- --Other checks
	-- v_UnderwritingAssociateCode='N/A' AND i_TerminatedDate<>TO_DATE('12/31/2100','MM/DD/YYYY'),'0998',
	-- v_UnderwritingAssociateCode='N/A' AND IN  (i_AgencyCode,'16998','14998','21999','34999','16999','34998','12999','22999','98999','26999','55555','13999','24999','15999','48001','48966','14967','15966'),'0999',
	-- v_UnderwritingAssociateCode='N/A' AND i_PolicyOfferingCode='000','0999',
	-- LTRIM(RTRIM(v_UnderwritingAssociateCode)))
	DECODE(
	    TRUE,
	    v_UnderwritingAssociateCode = 'N/A' AND i_InsuranceSegmentCode = '3', IFF(
	        LKP_UNDERWRITERWORKCOMPPOOL_1.AssociateCode IS NULL, 'N/A',
	        LKP_UNDERWRITERWORKCOMPPOOL_1.AssociateCode
	    ),
	    v_UnderwritingAssociateCode = 'N/A' AND i_TerminatedDate <> TO_TIMESTAMP('12/31/2100', 'MM/DD/YYYY'), '0998',
	    v_UnderwritingAssociateCode = 'N/A' AND i_AgencyCode IN ('16998','14998','21999','34999','16999','34998','12999','22999','98999','26999','55555','13999','24999','15999','48001','48966','14967','15966'), '0999',
	    v_UnderwritingAssociateCode = 'N/A' AND i_PolicyOfferingCode = '000', '0999',
	    LTRIM(RTRIM(v_UnderwritingAssociateCode))
	) AS o_UnderwritingAssociateCode,
	sysdate AS o_ModifiedDate
	FROM SQ_Quote
	LEFT JOIN LKP_Agency_V2
	ON LKP_Agency_V2.AgencyAKID = SQ_Quote.AgencyAKId
	LEFT JOIN LKP_InsuranceSegment
	ON LKP_InsuranceSegment.InsuranceSegmentAKId = SQ_Quote.InsuranceSegmentAKId
	LEFT JOIN LKP_PolicyOffering
	ON LKP_PolicyOffering.PolicyOfferingAKId = SQ_Quote.PolicyOfferingAKId
	LEFT JOIN LKP_Program
	ON LKP_Program.ProgramAKId = SQ_Quote.ProgramAKId
	LEFT JOIN LKP_StrategicProfitCenter
	ON LKP_StrategicProfitCenter.StrategicProfitCenterAKId = SQ_Quote.StrategicProfitCenterAKId
	LEFT JOIN LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGENCY LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGENCY_i_StrategicProfitCenterCode_i_AgencyCode
	ON LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGENCY_i_StrategicProfitCenterCode_i_AgencyCode.StrategicProfitCenterCode = i_StrategicProfitCenterCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGENCY_i_StrategicProfitCenterCode_i_AgencyCode.AgencyCode = i_AgencyCode

	LEFT JOIN LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPC LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPC_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium
	ON LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPC_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium.StrategicProfitCenterCode = i_StrategicProfitCenterCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPC_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium.AgencyCode = i_AgencyCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPC_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium.PolicyAmountMinimum = v_WrittenPremium

	LEFT JOIN LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPO LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPO_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode
	ON LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPO_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode.StrategicProfitCenterCode = i_StrategicProfitCenterCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPO_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode.AgencyCode = i_AgencyCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPO_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode.PolicyAmountMinimum = v_WrittenPremium
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPO_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode.PolicyAmountMaximum = i_PolicyOfferingCode

	LEFT JOIN LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROG LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROG_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode
	ON LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROG_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode.StrategicProfitCenterCode = i_StrategicProfitCenterCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROG_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode.AgencyCode = i_AgencyCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROG_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode.PolicyAmountMinimum = v_WrittenPremium
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROG_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode.PolicyAmountMaximum = i_PolicyOfferingCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROG_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode.PolicyOfferingCode = i_ProgramCode

	LEFT JOIN LKP_BONDPRODUCTS LKP_BONDPRODUCTS_i_QuoteId
	ON LKP_BONDPRODUCTS_i_QuoteId.QuoteId = i_QuoteId

	LEFT JOIN LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode_Contract
	ON LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode_Contract.StrategicProfitCenterCode = i_StrategicProfitCenterCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode_Contract.AgencyCode = i_AgencyCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode_Contract.PolicyAmountMinimum = v_WrittenPremium
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode_Contract.PolicyAmountMaximum = i_PolicyOfferingCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode_Contract.PolicyOfferingCode = i_ProgramCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode_Contract.ProgramCode = 'Contract'

	LEFT JOIN LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode_NonContract
	ON LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode_NonContract.StrategicProfitCenterCode = i_StrategicProfitCenterCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode_NonContract.AgencyCode = i_AgencyCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode_NonContract.PolicyAmountMinimum = v_WrittenPremium
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode_NonContract.PolicyAmountMaximum = i_PolicyOfferingCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode_NonContract.PolicyOfferingCode = i_ProgramCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_v_WrittenPremium_i_PolicyOfferingCode_i_ProgramCode_NonContract.ProgramCode = 'NonContract'

	LEFT JOIN LKP_UNDERWRITERWORKCOMPPOOL LKP_UNDERWRITERWORKCOMPPOOL_1
	ON LKP_UNDERWRITERWORKCOMPPOOL_1.Dummy = 1

),
LKP_Quote AS (
	SELECT
	UnderwritingAssociateAKId,
	QuoteId
	FROM (
		SELECT 
			UnderwritingAssociateAKId,
			QuoteId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Quote
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY QuoteId ORDER BY UnderwritingAssociateAKId) = 1
),
LKP_UnderwriterAssociate AS (
	SELECT
	UnderwritingAssociateAKID,
	UnderwriterCode
	FROM (
		SELECT 
			UnderwritingAssociateAKID,
			UnderwriterCode
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwritingAssociate
		WHERE CurrentSnapshotFlag=1 and AssociateRole in ('UNDERWRITER','CUSTOM UNDERWRITER')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY UnderwriterCode ORDER BY UnderwritingAssociateAKID DESC) = 1
),
FIL_Change AS (
	SELECT
	EXP_Quote_ProductRelationship.o_QuoteId AS QuoteId, 
	LKP_Quote.UnderwritingAssociateAKId, 
	LKP_UnderwriterAssociate.UnderwritingAssociateAKID AS UnderwritingAssociateAKID_new, 
	EXP_Quote_ProductRelationship.o_ModifiedDate AS ModifiedDate
	FROM EXP_Quote_ProductRelationship
	LEFT JOIN LKP_Quote
	ON LKP_Quote.QuoteId = EXP_Quote_ProductRelationship.o_QuoteId
	LEFT JOIN LKP_UnderwriterAssociate
	ON LKP_UnderwriterAssociate.UnderwriterCode = EXP_Quote_ProductRelationship.o_UnderwritingAssociateCode
	WHERE UnderwritingAssociateAKId<>UnderwritingAssociateAKID_new
),
TGT_Quote_Update AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.Quote
	(QuoteId, ModifiedDate, UnderwritingAssociateAKId)
	SELECT 
	QUOTEID, 
	MODIFIEDDATE, 
	UnderwritingAssociateAKID_new AS UNDERWRITINGASSOCIATEAKID
	FROM FIL_Change
),