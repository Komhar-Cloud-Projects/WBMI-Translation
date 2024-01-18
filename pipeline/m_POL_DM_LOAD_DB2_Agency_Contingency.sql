WITH
SQ_WorkAgencyContingencyYearly AS (
	Declare @Date1 date
	
	set @date1=CAST(CAST(YEAR(GETDATE())+@{pipeline().parameters.NUM_OF_YEAR} as varchar)+'/12/31 00:00:00' as date)
	
	SELECT 
	WACY.StateCode, 
	WACY.AgencyNumber, 
	WACY.ContingencyYear, 
	WACY.GuaranteedFlag, 
	WACY.AgencyCode, 
	SUM(WACY.GuaranteedContingencyAmount), 
	SUM(WACY.ContingencyAmount), 
	SUM(WACY.YTDDirectWrittenPremium), 
	SUM(WACY.YTDNetEarnedPremium), 
	SUM(WACY.YTDNetIncurredLoss) 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkAgencyContingencyYearly WACY
	WHERE 
	WACY.CreatedDate = @date1 and len(WACY.AgencyCode)>=5
	Group by WACY.StateCode, 
	WACY.AgencyNumber, 
	WACY.ContingencyYear, 
	WACY.GuaranteedFlag, 
	WACY.AgencyCode
),
EXP_Passthrough AS (
	SELECT
	StateCode AS STATE_CODE,
	AgencyNumber AS AGENCY_NUM,
	ContingencyYear AS CONTINGENCY_YEAR,
	GuaranteedFlag AS GUARANTEED,
	AgencyCode AS AGENCY_CODE,
	GuaranteedContingencyAmount AS GUAR_CNTGNCY_AMT,
	ContingencyAmount AS CONTINGENCY_AMT,
	YTDDirectWrittenPremium AS PREM_WRITTEN,
	YTDNetEarnedPremium AS NET_PREM_EARNED,
	YTDNetIncurredLoss AS NET_LOSS_INCURRED
	FROM SQ_WorkAgencyContingencyYearly
),
AGENCY_CONTINGENCY AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AGENCY_CONTINGENCY
	(STATE_CODE, AGENCY_NUM, CONTINGENCY_YEAR, GUARANTEED, AGENCY_CODE, GUAR_CNTGNCY_AMT, CONTINGENCY_AMT, PREM_WRITTEN, NET_PREM_EARNED, NET_LOSS_INCURRED)
	SELECT 
	STATE_CODE, 
	AGENCY_NUM, 
	CONTINGENCY_YEAR, 
	GUARANTEED, 
	AGENCY_CODE, 
	GUAR_CNTGNCY_AMT, 
	CONTINGENCY_AMT, 
	PREM_WRITTEN, 
	NET_PREM_EARNED, 
	NET_LOSS_INCURRED
	FROM EXP_Passthrough
),