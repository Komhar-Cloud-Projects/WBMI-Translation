WITH
SQ_ArchWorkPremiumTransaction AS (
	SELECT distinct a.PremiumTransactionAKId, b.CoverageId
	FROM ArchWorkPremiumTransaction a
	INNER JOIN WC_Stage..ArchDCCoverageStaging b
	ON a.PremiumTransactionStageId=b.CoverageId
	and a.SourceSystemID='DCT' and b.ObjectName<>'DC_Line'
),
WorkPremiumTransactionHsbEquipBreakdown AS (
	TRUNCATE TABLE WorkPremiumTransactionHsbEquipBreakdown;
	INSERT INTO WorkPremiumTransactionHsbEquipBreakdown
	(PremiumTransactionAKId, CoverageId)
	SELECT 
	PREMIUMTRANSACTIONAKID, 
	COVERAGEID
	FROM SQ_ArchWorkPremiumTransaction
),