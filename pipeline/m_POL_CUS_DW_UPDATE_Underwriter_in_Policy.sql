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
LKP_PremiumSum AS (
	SELECT
	PremiumTransactionAmount,
	pol_id
	FROM (
		SELECT P.pol_id as pol_id,
		SUM(PT.premiumtransactionamount) as PremiumTransactionAmount
		FROM V2.policy P
		JOIN dbo.StrategicProfitCenter SPC
		ON P.StrategicProfitCenterAKId = SPC.StrategicProfitCenterAKId and SPC.StrategicProfitCenterDescription = 'Argent'
		JOIN DBO.PolicyCoverage PC
		ON PC.PolicyAKID=P.pol_ak_id
		AND PC.CurrentSnapshotFlag=1
		JOIN DBO.StatisticalCoverage SC
		ON SC.PolicyCoverageAKID=PC.PolicyCoverageAKID
		AND SC.CurrentSnapshotFlag=1
		JOIN DBO.PremiumTransaction PT
		ON PT.StatisticalCoverageAKID=SC.StatisticalCoverageAKID
		AND PT.CurrentSnapshotFlag=1
		WHERE PT.PremiumType = 'D'
		GROUP BY P.pol_id
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_id ORDER BY PremiumTransactionAmount DESC) = 1
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
LKP_BondProducts AS (
	SELECT
	ProductCode,
	pol_id
	FROM (
		select distinct p.pol_id as pol_id,
		prod.ProductCode as ProductCode
		from v2.policy p
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage pc on p.pol_ak_id = pc.PolicyAKID
				and pc.CurrentSnapshotFlag = 1
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage sc on pc.PolicyCoverageAKID = sc.PolicyCoverageAKID
				and sc.CurrentSnapshotFlag = 1
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product prod on sc.ProductAKId = prod.ProductAKId
				and prod.CurrentSnapshotFlag = 1
		where p.source_sys_id = 'PMS'
		 and p.crrnt_snpsht_flag = 1
		 and prod.ProductCode in('610','620','630','640','650','660')
		union
		select distinct p.pol_id, prod.ProductCode
		from v2.policy p
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage pc on p.pol_ak_id = pc.PolicyAKID
				and pc.CurrentSnapshotFlag = 1
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage rc on pc.PolicyCoverageAKID = rc.PolicyCoverageAKID
				and rc.CurrentSnapshotFlag = 1
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product prod on rc.ProductAKId = prod.ProductAKId
				and prod.CurrentSnapshotFlag = 1
		where p.source_sys_id = 'DCT'
		 and p.crrnt_snpsht_flag = 1
		 and prod.ProductCode in('610','620','630','640','650','660')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_id ORDER BY ProductCode DESC) = 1
),
LKP_SBAReinsurance AS (
	SELECT
	ReturnIndicator,
	pol_id,
	reins_co_num
	FROM (
		select b.pol_id as pol_id,
		a.reins_co_num as reins_co_num,
		'Y' as ReturnIndicator
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.reinsurance_coverage a
		join @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy b 
		on a.pol_ak_id = b.pol_ak_id
		where a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_id,reins_co_num ORDER BY ReturnIndicator DESC) = 1
),
SQ_policy AS (
	SELECT P.pol_id,
	P.agencyakid,
	P.strategicprofitcenterakid,
	P.PolicyOfferingAKID,
	P.ProgramAKID,
	P.InsuranceSegmentAKId,
	P.wbconnect_upload_code
	FROM V2.policy P
	WHERE P.crrnt_snpsht_flag=1
),
EXP_Policy_AgencyRelationship AS (
	SELECT
	pol_id AS i_pol_id,
	AgencyAKId AS i_AgencyAKId,
	StrategicProfitCenterAKId AS i_StrategicProfitCenterAKId,
	-- *INF*: IIF(i_StrategicProfitCenterAKId = 4,:LKP.LKP_PREMIUMSUM(i_pol_id),0)
	IFF(i_StrategicProfitCenterAKId = 4, LKP_PREMIUMSUM_i_pol_id.PremiumTransactionAmount, 0) AS v_PremiumTransactionAmount,
	PolicyOfferingAKID AS i_PolicyOfferingAKID,
	ProgramAKID AS i_ProgramAKID,
	InsuranceSegmentAKId AS i_InsuranceSegmentAKId,
	wbconnect_upload_code AS i_wbconnect_upload_code,
	i_pol_id AS o_pol_id,
	i_StrategicProfitCenterAKId AS o_StrategicProfitCenterAKId,
	i_AgencyAKId AS o_AgencyAKId,
	-- *INF*: IIF(v_PremiumTransactionAmount < 0,0,v_PremiumTransactionAmount)
	IFF(v_PremiumTransactionAmount < 0, 0, v_PremiumTransactionAmount) AS o_PremiumTransactionAmount,
	i_PolicyOfferingAKID AS o_PolicyOfferingAKID,
	i_ProgramAKID AS o_ProgramAKID,
	i_InsuranceSegmentAKId AS o_InsuranceSegmentAKId,
	i_wbconnect_upload_code AS o_wbconnect_upload_code
	FROM SQ_policy
	LEFT JOIN LKP_PREMIUMSUM LKP_PREMIUMSUM_i_pol_id
	ON LKP_PREMIUMSUM_i_pol_id.pol_id = i_pol_id

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
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency
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
EXP_Policy_ProductRelationship AS (
	SELECT
	LKP_StrategicProfitCenter.StrategicProfitCenterCode AS i_StrategicProfitCenterCode,
	LKP_Agency_V2.AgencyCode AS i_AgencyCode,
	EXP_Policy_AgencyRelationship.o_pol_id AS i_pol_id,
	EXP_Policy_AgencyRelationship.o_PremiumTransactionAmount AS i_PremiumTransactionAmount,
	LKP_PolicyOffering.PolicyOfferingCode AS i_PolicyOfferingCode,
	LKP_Program.ProgramCode AS i_ProgramCode,
	LKP_Agency_V2.TerminatedDate AS i_TerminatedDate,
	LKP_InsuranceSegment.InsuranceSegmentCode AS i_InsuranceSegmentCode,
	EXP_Policy_AgencyRelationship.o_wbconnect_upload_code AS i_wbconnect_upload_code,
	-- *INF*: DECODE(TRUE,
	-- 
	-- --Look for work comp pool underwriter
	-- i_InsuranceSegmentCode = '3',
	-- :LKP.LKP_UNDERWRITERWORKCOMPPOOL(1),
	-- 
	-- NULL
	-- 
	-- )
	DECODE(TRUE,
	i_InsuranceSegmentCode = '3', LKP_UNDERWRITERWORKCOMPPOOL_1.AssociateCode,
	NULL) AS v_UnderwritingAssociateAKID_WorkComp,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(v_UnderwritingAssociateAKID_WorkComp), :LKP.LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGENCY(i_StrategicProfitCenterCode,i_AgencyCode),v_UnderwritingAssociateAKID_WorkComp)
	DECODE(TRUE,
	v_UnderwritingAssociateAKID_WorkComp IS NULL, LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGENCY_i_StrategicProfitCenterCode_i_AgencyCode.AssociateCode,
	v_UnderwritingAssociateAKID_WorkComp) AS v_UnderwritingAssociateAKID_Agy,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(v_UnderwritingAssociateAKID_Agy), :LKP.LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPC(i_StrategicProfitCenterCode,i_AgencyCode,i_PremiumTransactionAmount),v_UnderwritingAssociateAKID_Agy)
	DECODE(TRUE,
	v_UnderwritingAssociateAKID_Agy IS NULL, LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPC_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount.AssociateCode,
	v_UnderwritingAssociateAKID_Agy) AS v_UnderwritingAssociateAKID_AgySPC,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(v_UnderwritingAssociateAKID_AgySPC), :LKP.LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPO(i_StrategicProfitCenterCode,i_AgencyCode,i_PremiumTransactionAmount,i_PolicyOfferingCode),v_UnderwritingAssociateAKID_AgySPC)
	DECODE(TRUE,
	v_UnderwritingAssociateAKID_AgySPC IS NULL, LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPO_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode.AssociateCode,
	v_UnderwritingAssociateAKID_AgySPC) AS v_UnderwritingAssociateAKID_AgySPCPO,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(v_UnderwritingAssociateAKID_AgySPCPO),
	-- :LKP.LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROG(i_StrategicProfitCenterCode,i_AgencyCode,i_PremiumTransactionAmount,i_PolicyOfferingCode, i_ProgramCode),v_UnderwritingAssociateAKID_AgySPCPO)
	DECODE(TRUE,
	v_UnderwritingAssociateAKID_AgySPCPO IS NULL, LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROG_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode.AssociateCode,
	v_UnderwritingAssociateAKID_AgySPCPO) AS v_UnderwritingAssociateAKID_AgySPCPOProg,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(v_UnderwritingAssociateAKID_AgySPCPOProg),
	-- 
	-- DECODE(TRUE,
	-- i_wbconnect_upload_code = 'B', :LKP.LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND(i_StrategicProfitCenterCode,i_AgencyCode,i_PremiumTransactionAmount,i_PolicyOfferingCode, i_ProgramCode,'Rapid'),
	-- 
	-- IN(:LKP.LKP_BONDPRODUCTS(i_pol_id),'610','620','630','640','650','660')
	-- 	and :LKP.LKP_SBAREINSURANCE(i_pol_id,'0125') = 'Y',
	-- :LKP.LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND(i_StrategicProfitCenterCode,i_AgencyCode,i_PremiumTransactionAmount,i_PolicyOfferingCode, i_ProgramCode,'SBA'),
	-- 
	-- :LKP.LKP_BONDPRODUCTS(i_pol_id) = '610',
	-- :LKP.LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND(i_StrategicProfitCenterCode,i_AgencyCode,i_PremiumTransactionAmount,i_PolicyOfferingCode, i_ProgramCode,'Contract'),
	-- 
	-- IN(:LKP.LKP_BONDPRODUCTS(i_pol_id),'620','630','640','650','660'),
	-- :LKP.LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND(i_StrategicProfitCenterCode,i_AgencyCode,i_PremiumTransactionAmount,i_PolicyOfferingCode, i_ProgramCode,'Non-Contract')
	-- 
	-- 
	-- ),v_UnderwritingAssociateAKID_AgySPCPOProg)
	DECODE(TRUE,
	v_UnderwritingAssociateAKID_AgySPCPOProg IS NULL, DECODE(TRUE,
	i_wbconnect_upload_code = 'B', LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Rapid.AssociateCode,
	IN(LKP_BONDPRODUCTS_i_pol_id.ProductCode, '610', '620', '630', '640', '650', '660') AND LKP_SBAREINSURANCE_i_pol_id_0125.ReturnIndicator = 'Y', LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_SBA.AssociateCode,
	LKP_BONDPRODUCTS_i_pol_id.ProductCode = '610', LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Contract.AssociateCode,
	IN(LKP_BONDPRODUCTS_i_pol_id.ProductCode, '620', '630', '640', '650', '660'), LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Non_Contract.AssociateCode),
	v_UnderwritingAssociateAKID_AgySPCPOProg) AS v_UnderwritingAssociateAKID_AgySPCPOProgBond,
	-- *INF*: IIF(ISNULL(v_UnderwritingAssociateAKID_AgySPCPOProgBond),'N/A', v_UnderwritingAssociateAKID_AgySPCPOProgBond)
	IFF(v_UnderwritingAssociateAKID_AgySPCPOProgBond IS NULL, 'N/A', v_UnderwritingAssociateAKID_AgySPCPOProgBond) AS v_UnderwritingAssociateCode,
	i_pol_id AS o_pol_id,
	-- *INF*: DECODE(TRUE,
	-- 
	-- --other checks
	-- v_UnderwritingAssociateCode='N/A' AND i_TerminatedDate<>TO_DATE('12/31/2100','MM/DD/YYYY'),'0998',
	-- v_UnderwritingAssociateCode='N/A' AND IN  (i_AgencyCode,'16998','14998','21999','34999','16999','34998','12999','22999','98999','26999','55555','13999','24999','15999','48001','48966','14967','15966'),'0999',
	-- v_UnderwritingAssociateCode='N/A' AND i_PolicyOfferingCode='000','0999',
	-- LTRIM(RTRIM(v_UnderwritingAssociateCode)))
	DECODE(TRUE,
	v_UnderwritingAssociateCode = 'N/A' AND i_TerminatedDate <> TO_DATE('12/31/2100', 'MM/DD/YYYY'), '0998',
	v_UnderwritingAssociateCode = 'N/A' AND IN(i_AgencyCode, '16998', '14998', '21999', '34999', '16999', '34998', '12999', '22999', '98999', '26999', '55555', '13999', '24999', '15999', '48001', '48966', '14967', '15966'), '0999',
	v_UnderwritingAssociateCode = 'N/A' AND i_PolicyOfferingCode = '000', '0999',
	LTRIM(RTRIM(v_UnderwritingAssociateCode))) AS o_UnderwritingAssociateCode,
	SYSDATE AS o_modified_date
	FROM EXP_Policy_AgencyRelationship
	LEFT JOIN LKP_Agency_V2
	ON LKP_Agency_V2.AgencyAKID = EXP_Policy_AgencyRelationship.o_AgencyAKId
	LEFT JOIN LKP_InsuranceSegment
	ON LKP_InsuranceSegment.InsuranceSegmentAKId = EXP_Policy_AgencyRelationship.o_InsuranceSegmentAKId
	LEFT JOIN LKP_PolicyOffering
	ON LKP_PolicyOffering.PolicyOfferingAKId = EXP_Policy_AgencyRelationship.o_PolicyOfferingAKID
	LEFT JOIN LKP_Program
	ON LKP_Program.ProgramAKId = EXP_Policy_AgencyRelationship.o_ProgramAKID
	LEFT JOIN LKP_StrategicProfitCenter
	ON LKP_StrategicProfitCenter.StrategicProfitCenterAKId = EXP_Policy_AgencyRelationship.o_StrategicProfitCenterAKId
	LEFT JOIN LKP_UNDERWRITERWORKCOMPPOOL LKP_UNDERWRITERWORKCOMPPOOL_1
	ON LKP_UNDERWRITERWORKCOMPPOOL_1.Dummy = 1

	LEFT JOIN LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGENCY LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGENCY_i_StrategicProfitCenterCode_i_AgencyCode
	ON LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGENCY_i_StrategicProfitCenterCode_i_AgencyCode.StrategicProfitCenterCode = i_StrategicProfitCenterCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGENCY_i_StrategicProfitCenterCode_i_AgencyCode.AgencyCode = i_AgencyCode

	LEFT JOIN LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPC LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPC_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount
	ON LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPC_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount.StrategicProfitCenterCode = i_StrategicProfitCenterCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPC_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount.AgencyCode = i_AgencyCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPC_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount.PolicyAmountMinimum = i_PremiumTransactionAmount

	LEFT JOIN LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPO LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPO_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode
	ON LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPO_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode.StrategicProfitCenterCode = i_StrategicProfitCenterCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPO_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode.AgencyCode = i_AgencyCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPO_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode.PolicyAmountMinimum = i_PremiumTransactionAmount
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPO_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode.PolicyAmountMaximum = i_PolicyOfferingCode

	LEFT JOIN LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROG LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROG_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode
	ON LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROG_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode.StrategicProfitCenterCode = i_StrategicProfitCenterCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROG_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode.AgencyCode = i_AgencyCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROG_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode.PolicyAmountMinimum = i_PremiumTransactionAmount
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROG_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode.PolicyAmountMaximum = i_PolicyOfferingCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROG_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode.PolicyOfferingCode = i_ProgramCode

	LEFT JOIN LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Rapid
	ON LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Rapid.StrategicProfitCenterCode = i_StrategicProfitCenterCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Rapid.AgencyCode = i_AgencyCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Rapid.PolicyAmountMinimum = i_PremiumTransactionAmount
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Rapid.PolicyAmountMaximum = i_PolicyOfferingCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Rapid.PolicyOfferingCode = i_ProgramCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Rapid.ProgramCode = 'Rapid'

	LEFT JOIN LKP_BONDPRODUCTS LKP_BONDPRODUCTS_i_pol_id
	ON LKP_BONDPRODUCTS_i_pol_id.pol_id = i_pol_id

	LEFT JOIN LKP_SBAREINSURANCE LKP_SBAREINSURANCE_i_pol_id_0125
	ON LKP_SBAREINSURANCE_i_pol_id_0125.pol_id = i_pol_id
	AND LKP_SBAREINSURANCE_i_pol_id_0125.reins_co_num = '0125'

	LEFT JOIN LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_SBA
	ON LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_SBA.StrategicProfitCenterCode = i_StrategicProfitCenterCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_SBA.AgencyCode = i_AgencyCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_SBA.PolicyAmountMinimum = i_PremiumTransactionAmount
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_SBA.PolicyAmountMaximum = i_PolicyOfferingCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_SBA.PolicyOfferingCode = i_ProgramCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_SBA.ProgramCode = 'SBA'

	LEFT JOIN LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Contract
	ON LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Contract.StrategicProfitCenterCode = i_StrategicProfitCenterCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Contract.AgencyCode = i_AgencyCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Contract.PolicyAmountMinimum = i_PremiumTransactionAmount
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Contract.PolicyAmountMaximum = i_PolicyOfferingCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Contract.PolicyOfferingCode = i_ProgramCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Contract.ProgramCode = 'Contract'

	LEFT JOIN LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Non_Contract
	ON LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Non_Contract.StrategicProfitCenterCode = i_StrategicProfitCenterCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Non_Contract.AgencyCode = i_AgencyCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Non_Contract.PolicyAmountMinimum = i_PremiumTransactionAmount
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Non_Contract.PolicyAmountMaximum = i_PolicyOfferingCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Non_Contract.PolicyOfferingCode = i_ProgramCode
	AND LKP_UNDERWRITERPRODUCTRELATIONSHIP_UNDERWRITINGASSOCIATEAKID_BYAGYSPCPOPROGBOND_i_StrategicProfitCenterCode_i_AgencyCode_i_PremiumTransactionAmount_i_PolicyOfferingCode_i_ProgramCode_Non_Contract.ProgramCode = 'Non-Contract'

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
EXP_MetaValues AS (
	SELECT
	EXP_Policy_ProductRelationship.o_pol_id AS pol_id,
	LKP_UnderwriterAssociate.UnderwritingAssociateAKID,
	EXP_Policy_ProductRelationship.o_modified_date AS modified_date
	FROM EXP_Policy_ProductRelationship
	LEFT JOIN LKP_UnderwriterAssociate
	ON LKP_UnderwriterAssociate.UnderwriterCode = EXP_Policy_ProductRelationship.o_UnderwritingAssociateCode
),
LKP_Tgt_Policy AS (
	SELECT
	UnderwritingAssociateAKId,
	o_pol_id,
	pol_id
	FROM (
		SELECT policy.UnderwritingAssociateAKId as UnderwritingAssociateAKId, policy.pol_id as pol_id 
		FROM v2.policy
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_id ORDER BY UnderwritingAssociateAKId) = 1
),
FIL_Valid_UnderwritingAssociateAKId AS (
	SELECT
	EXP_MetaValues.pol_id, 
	EXP_MetaValues.UnderwritingAssociateAKID AS UnderwritingAssociateAKId, 
	LKP_Tgt_Policy.UnderwritingAssociateAKId AS UnderwritingAssociateAKId_old, 
	EXP_MetaValues.modified_date
	FROM EXP_MetaValues
	LEFT JOIN LKP_Tgt_Policy
	ON LKP_Tgt_Policy.pol_id = EXP_MetaValues.pol_id
	WHERE UnderwritingAssociateAKId<>UnderwritingAssociateAKId_old
),
UPD_UnderwritingAssociateAKId AS (
	SELECT
	pol_id, 
	modified_date, 
	UnderwritingAssociateAKId
	FROM FIL_Valid_UnderwritingAssociateAKId
),
TGT_policy_Update AS (

	------------ PRE SQL ----------
	exec [spSetIndexStatus] @Enable = 0, @Schema = 'V2', @TableName = 'POLICY', @IndexWildcard = 'Ak3Policy'
	-------------------------------


	MERGE INTO @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy AS T
	USING UPD_UnderwritingAssociateAKId AS S
	ON T.pol_id = S.pol_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.modified_date = S.modified_date, T.UnderwritingAssociateAKId = S.UnderwritingAssociateAKId

	------------ POST SQL ----------
	exec [spSetIndexStatus] @Enable = 1, @Schema = 'V2', @TableName = 'POLICY', @IndexWildcard = 'Ak3Policy'
	-------------------------------


),