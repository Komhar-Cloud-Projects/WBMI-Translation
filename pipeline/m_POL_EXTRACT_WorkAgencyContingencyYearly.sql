WITH
SQ_AgencyProfitSharingYTDFact AS (
	Declare @Date1 date
	
	set @date1=CAST(CAST(YEAR(GETDATE())+@{pipeline().parameters.NUM_OF_YEAR} as varchar)+'/12/31 00:00:00' as date)
	
	select SUBSTRING((CASE   WHEN ard.legalprimaryagencycode   is null THEN a.agencycode ELSE ard.legalprimaryagencycode END),1,2) as StateCode,
	SUBSTRING((CASE   WHEN ard.legalprimaryagencycode   is null THEN a.agencycode ELSE ard.legalprimaryagencycode END),3,3) as AgencyNumber,
	C.clndr_yr as ContingencyYear,
	case when AG.ProfitSharingGuaranteeFlag=1 then 'Y' else 'N' end as ContingencyFlag,
	(CASE   WHEN ard.legalprimaryagencycode   is null THEN a.agencycode        ELSE ard.legalprimaryagencycode END) as AgencyCode,
	sum(APS.ProfitSharingPaymentAmount) as ContingencyAmount, 
	sum(APS.ProfitSharingEligibleDirectWrittenPremium) as YTDDirectWrittenPremium,
	sum(APS.NetDirectEarnedPremium) as YTDNetEarnedPremium,
	sum(APS.NetDirectIncurredLoss) as YTDNetLossIncurred,
	@date1 CreatedDate
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyProfitSharingYTDFact APS
	
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim C
	on APS.RunDateId=C.clndr_id
	
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim A
	on APS.AgencyDimId=A.AgencyDimID
	
	left join 
	 (select * from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Agencyrelationshipdim where currentsnapshotflag = 1) ard
	on (A.edwagencyakid=ard.edwagencyakid 
	 and c.clndr_date between ard.agencyrelationshipeffectivedate and ard.agencyrelationshipexpirationdate
	 )
	
	
	
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim LA
	on (case when ard.LegalPrimaryAgencyCode is null then a.agencycode else ard.LegalPrimaryAgencyCode end)=LA.AgencyCode
	
	and LA.CurrentSnapshotFlag=1
	JOIN @{pipeline().parameters.DATABASE_NAME_IL}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency AG
	on LA.EDWAgencyAKID=AG.AgencyAKID
	and AG.CurrentSnapshotFlag=1
	
	
	
	where GroupExperienceIndicator='GROUP'
	and C.clndr_date=@date1 and  SUBSTRING(case when ard.LegalPrimaryAgencyCode is null then a.agencycode else ard.LegalPrimaryAgencyCode end ,1,1) <> '7'
	and C.clndr_date=@date1
	group by 
	C.clndr_yr,
	AG.ProfitSharingGuaranteeFlag,
	case when ard.LegalPrimaryAgencyCode is null then a.agencycode else ard.LegalPrimaryAgencyCode end
	
	/*Left join with "agencyrelationshipdim" in order to get legalprimaryagencycode values from "AgencyRelationshipDim" table*/
),
EXP_SrcDataCollect AS (
	SELECT
	StateCode,
	AgencyNumber,
	ContingencyYear,
	GuaranteedFlag,
	AgencyCode,
	ContingencyAmount,
	YTDDirectWrittenPremium,
	YTDNetEarnedPremium,
	YTDNetLossIncurred,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	CreatedDate
	FROM SQ_AgencyProfitSharingYTDFact
),
SQ_AgencyProfitSharingYTDFact1 AS (
	Declare @Date1 date
	
	set @date1=CAST(CAST(YEAR(GETDATE())+@{pipeline().parameters.NUM_OF_YEAR} as varchar)+'/09/30 00:00:00' as date)
	
	select  (CASE 
				WHEN ard.LegalPrimaryAgencyCode IS NULL
					THEN a.agencycode
				ELSE ard.LegalPrimaryAgencyCode
				END
			) as AgencyCode,
	sum(APS.ProfitSharingPaymentAmount) as ProfitSharingPaymentAmount
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.AgencyProfitSharingYTDFact APS
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V3}.AgencyDim A
	on APS.AgencyDimId=A.AgencyDimID
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim C
	on APS.RunDateId=C.clndr_id
	
			left join (select * from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Agencyrelationshipdim where currentsnapshotflag = 1) ard
	on (a.edwagencyakid=ard.edwagencyakid and C.clndr_date between ard.agencyrelationshipeffectivedate and ard.agencyrelationshipexpirationdate)
	
	where GroupExperienceIndicator='GROUP'
	and C.clndr_date=@date1 and SUBSTRING(case when ard.LegalPrimaryAgencyCode is null then a.agencycode else ard.LegalPrimaryAgencyCode end,1,1) <> '7'
	 group by 
	(CASE 
				WHEN ard.LegalPrimaryAgencyCode IS NULL
					THEN a.agencycode
				ELSE ard.LegalPrimaryAgencyCode
				END
			)
),
JNR_GuaranteedAmount AS (SELECT
	EXP_SrcDataCollect.StateCode, 
	EXP_SrcDataCollect.AgencyNumber, 
	EXP_SrcDataCollect.ContingencyYear, 
	EXP_SrcDataCollect.GuaranteedFlag, 
	EXP_SrcDataCollect.AgencyCode, 
	EXP_SrcDataCollect.ContingencyAmount, 
	EXP_SrcDataCollect.YTDDirectWrittenPremium, 
	EXP_SrcDataCollect.YTDNetEarnedPremium, 
	EXP_SrcDataCollect.YTDNetLossIncurred, 
	EXP_SrcDataCollect.AuditID, 
	EXP_SrcDataCollect.CreatedDate, 
	SQ_AgencyProfitSharingYTDFact1.AgencyCode AS AgencyCode1, 
	SQ_AgencyProfitSharingYTDFact1.ProfitSharingPaymentAmount
	FROM SQ_AgencyProfitSharingYTDFact1
	RIGHT OUTER JOIN EXP_SrcDataCollect
	ON EXP_SrcDataCollect.AgencyCode = SQ_AgencyProfitSharingYTDFact1.AgencyCode
),
EXP_TgtDataCollect AS (
	SELECT
	AuditID,
	CreatedDate,
	StateCode,
	AgencyNumber,
	ContingencyYear,
	GuaranteedFlag,
	AgencyCode,
	ProfitSharingPaymentAmount AS GuaranteedContingencyAmount,
	-- *INF*: iif(ISNULL(GuaranteedContingencyAmount),0,GuaranteedContingencyAmount)
	IFF(GuaranteedContingencyAmount IS NULL, 0, GuaranteedContingencyAmount) AS o_GuaranteedContingencyAmount,
	ContingencyAmount,
	YTDDirectWrittenPremium,
	YTDNetEarnedPremium,
	YTDNetLossIncurred
	FROM JNR_GuaranteedAmount
),
WorkAgencyContingencyYearly AS (
	INSERT INTO WorkAgencyContingencyYearly
	(AuditId, CreatedDate, StateCode, AgencyNumber, ContingencyYear, GuaranteedFlag, AgencyCode, GuaranteedContingencyAmount, ContingencyAmount, YTDDirectWrittenPremium, YTDNetEarnedPremium, YTDNetIncurredLoss)
	SELECT 
	AuditID AS AUDITID, 
	CREATEDDATE, 
	STATECODE, 
	AGENCYNUMBER, 
	CONTINGENCYYEAR, 
	GUARANTEEDFLAG, 
	AGENCYCODE, 
	o_GuaranteedContingencyAmount AS GUARANTEEDCONTINGENCYAMOUNT, 
	CONTINGENCYAMOUNT, 
	YTDDIRECTWRITTENPREMIUM, 
	YTDNETEARNEDPREMIUM, 
	YTDNetLossIncurred AS YTDNETINCURREDLOSS
	FROM EXP_TgtDataCollect
),