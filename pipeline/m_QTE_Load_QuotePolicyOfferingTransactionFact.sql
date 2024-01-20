WITH
LKP_calendar_dim_clndr_yr AS (
	SELECT
	clndr_yr,
	clndr_date
	FROM (
		SELECT 
			clndr_yr,
			clndr_date
		FROM calendar_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_date ORDER BY clndr_yr) = 1
),
LKP_calendar_dim_clndr_id AS (
	SELECT
	clndr_id,
	clndr_date
	FROM (
		SELECT 
			clndr_id,
			clndr_date
		FROM calendar_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_date ORDER BY clndr_id) = 1
),
SQ_Quote AS (
	SELECT distinct
	 Quote.QuoteId , Quote.QuoteAKId , Quote.QuoteKey , Quote.AgencyAKId , Quote.QuoteNumber , Quote.BusinessClassCode , Quote.InternalExternalIndicator , Quote.RiskState , Quote.QuoteEffectiveDate
	 , Quote.ProgramAKId , Quote.CurrentSnapshotFlag , Quote.QuoteStatusCode , Quote.UnderwritingAssociateAKId , Quote.AgencyEmployeeAKId , Quote.StatusDate
	 , Quote.QuoteCreatedDate , Quote.EstimatedQuotePremium , EnterpriseGroup.EnterpriseGroupCode , InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityCode ,
	 StrategicProfitCenter.StrategicProfitCenterCode , InsuranceSegment.InsuranceSegmentCode , PolicyOffering.PolicyOfferingCode ,
	 Max(QuotePolicyOfferingTransaction.QuotePolicyOfferingTransactionId ) as QuotePolicyOfferingTransactionId
	 , Max(QuotePolicyOfferingTransaction.QuotePolicyOfferingTransactionAKID ) as QuotePolicyOfferingTransactionAKID 
	 , max(QuotePolicyOfferingTransaction.QuoteID ) as QuoteID 
	 ,max( QuotePolicyOfferingTransaction.QuoteAKID ) as QuoteAKID 
	 ,QuotePolicyOfferingTransaction.QuotePremium
	FROM
	Quote
	LEFT OUTER JOIN QuotePolicyOfferingTransaction
	on Quote.QuoteAKId =QuotePolicyOfferingTransaction.QuoteAKID and quote.currentsnapshotflag=1
	-- and quote.quoteid=QuotePolicyOfferingTransaction.quoteid ---commenting this join out as it wasn't allowing the proper records to come into the fact table because the premium isn't always on the first record
	left outer join PolicyOffering
	on PolicyOffering.PolicyOfferingAKId=Quote.PolicyOfferingAKId and PolicyOffering.CurrentSnapshotFlag=1
	left outer join StrategicProfitCenter
	on StrategicProfitCenter.StrategicProfitCenterAKId=Quote.StrategicProfitCenterAKId and StrategicProfitCenter.CurrentSnapshotFlag=1
	left outer join InsuranceSegment
	on InsuranceSegment.InsuranceSegmentAKId=Quote.InsuranceSegmentAKId and InsuranceSegment.CurrentSnapshotFlag=1
	left outer join EnterpriseGroup
	on EnterpriseGroup.EnterpriseGroupId=StrategicProfitCenter.EnterpriseGroupId
	left outer join InsuranceReferenceLegalEntity
	on InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityId=StrategicProfitCenter.InsuranceReferenceLegalEntityId
	WHERE Quote.CurrentSnapshotFlag = 1
	
	group by
	 Quote.QuoteId , Quote.QuoteAKId , Quote.QuoteKey , Quote.AgencyAKId , Quote.QuoteNumber , Quote.BusinessClassCode , Quote.InternalExternalIndicator , Quote.RiskState , Quote.QuoteEffectiveDate
	 , Quote.ProgramAKId , Quote.CurrentSnapshotFlag , Quote.QuoteStatusCode , Quote.UnderwritingAssociateAKId , Quote.AgencyEmployeeAKId , Quote.StatusDate
	 , Quote.QuoteCreatedDate , Quote.EstimatedQuotePremium , EnterpriseGroup.EnterpriseGroupCode , InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityCode ,
	 StrategicProfitCenter.StrategicProfitCenterCode , InsuranceSegment.InsuranceSegmentCode , PolicyOffering.PolicyOfferingCode ,
	QuotePolicyOfferingTransaction.QuotePremium
),
LKP_AgencyDim AS (
	SELECT
	AgencyDimID,
	EDWAgencyAKID
	FROM (
		SELECT 
			AgencyDimID,
			EDWAgencyAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V3}.AgencyDim
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyAKID ORDER BY AgencyDimID) = 1
),
LKP_AgencyEmployeeDim AS (
	SELECT
	AgencyEmployeeDimID,
	EDWAgencyEmployeeAKID
	FROM (
		SELECT 
			AgencyEmployeeDimID,
			EDWAgencyEmployeeAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyEmployeeDim
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyEmployeeAKID ORDER BY AgencyEmployeeDimID) = 1
),
LKP_BusinessClassDim AS (
	SELECT
	i_BusinessClassCode,
	BusinessClassDimId,
	BusinessClassCode
	FROM (
		SELECT 
			i_BusinessClassCode,
			BusinessClassDimId,
			BusinessClassCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.BusinessClassDim
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY BusinessClassCode ORDER BY i_BusinessClassCode) = 1
),
LKP_ClosedQuoteDim AS (
	SELECT
	ClosedQuoteDimId,
	EDWQuotePKId
	FROM (
		SELECT 
			ClosedQuoteDimId,
			EDWQuotePKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ClosedQuoteDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWQuotePKId ORDER BY ClosedQuoteDimId) = 1
),
LKP_DeclinedQuoteDim AS (
	SELECT
	DeclinedQuoteDimId,
	EDWQuotePKId
	FROM (
		SELECT 
			DeclinedQuoteDimId,
			EDWQuotePKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.DeclinedQuoteDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWQuotePKId ORDER BY DeclinedQuoteDimId) = 1
),
EXP_Default_ProductCode_LineOfBusinessCode AS (
	SELECT
	-- *INF*: '000'
	-- --IIF(ISNULL(i_ProductCode), '000', i_ProductCode)
	'000' AS o_ProductCode,
	-- *INF*: '000'
	-- --IIF(ISNULL(i_InsuranceReferenceLineOfBusinessCode), '000', i_InsuranceReferenceLineOfBusinessCode)
	'000' AS o_InsuranceReferenceLineOfBusinessCode,
	'1' AS o_RatingPlanCode,
	QuoteId
	FROM SQ_Quote
),
LKP_InsuranceReferenceDim AS (
	SELECT
	InsuranceReferenceDimId,
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
			InsuranceReferenceDimId,
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
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EnterpriseGroupCode,InsuranceReferenceLegalEntityCode,StrategicProfitCenterCode,InsuranceSegmentCode,PolicyOfferingCode,ProductCode,InsuranceReferenceLineOfBusinessCode,RatingPlanCode ORDER BY InsuranceReferenceDimId) = 1
),
LKP_PartyRoleInAgreement AS (
	SELECT
	PartyAkId,
	QuoteAkId
	FROM (
		SELECT 
			PartyAkId,
			QuoteAkId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.PartyRoleInAgreement
		WHERE CurrentSnapshotFlag = 1 and partyroleinagreementtypecode='Prospect'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY QuoteAkId ORDER BY PartyAkId) = 1
),
LKP_PartyDim AS (
	SELECT
	PartyDimID,
	PartyNumber,
	i_PartyAkId,
	EDWPartyAKID
	FROM (
		SELECT 
			PartyDimID,
			PartyNumber,
			i_PartyAkId,
			EDWPartyAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PartyDim
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPartyAKID ORDER BY PartyDimID) = 1
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
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProgramAKId ORDER BY ProgramCode) = 1
),
LKP_ProgramDim AS (
	SELECT
	i_ProgramCode,
	ProgramDimId,
	ProgramCode
	FROM (
		SELECT 
			i_ProgramCode,
			ProgramDimId,
			ProgramCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ProgramDim
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProgramCode ORDER BY i_ProgramCode) = 1
),
LKP_QuoteDim AS (
	SELECT
	QuoteDimId,
	ReleasedQuoteFlag,
	EDWQuoteAKId
	FROM (
		SELECT QuoteDimId as QuoteDimId,
		CONVERT(VARCHAR(1),ReleasedQuoteFlag) as ReleasedQuoteFlag,
		EDWQuoteAKId as EDWQuoteAKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.QuoteDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWQuoteAKId ORDER BY QuoteDimId) = 1
),
LKP_QuoteStatusDim AS (
	SELECT
	QuoteStatusDimId,
	QuoteStatusCode
	FROM (
		SELECT 
			QuoteStatusDimId,
			QuoteStatusCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.QuoteStatusDim
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY QuoteStatusCode ORDER BY QuoteStatusDimId) = 1
),
LKP_StateDim AS (
	SELECT
	StateDimID,
	StateAbbreviation
	FROM (
		SELECT 
			StateDimID,
			StateAbbreviation
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.StateDim
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StateAbbreviation ORDER BY StateDimID) = 1
),
LKP_UnderwritingDivisionDim AS (
	SELECT
	UnderwritingDivisionDimID,
	EDWUnderwritingAssociateAKID
	FROM (
		SELECT 
			UnderwritingDivisionDimID,
			EDWUnderwritingAssociateAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.UnderwritingDivisionDim
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWUnderwritingAssociateAKID ORDER BY UnderwritingDivisionDimID) = 1
),
EXP_GatherAllPorts AS (
	SELECT
	LKP_QuoteStatusDim.QuoteStatusDimId AS lkp_QuoteStatusDimId,
	LKP_StateDim.StateDimID AS lkp_StateDimID,
	LKP_PartyDim.PartyDimID AS lkp_PartyDimID,
	LKP_PartyDim.PartyNumber AS lkp_PartyNumber,
	LKP_AgencyDim.AgencyDimID AS lkp_AgencyDimID,
	LKP_AgencyEmployeeDim.AgencyEmployeeDimID AS lkp_AgencyEmployeeDimID,
	LKP_ProgramDim.ProgramDimId AS lkp_ProgramDimId,
	LKP_DeclinedQuoteDim.DeclinedQuoteDimId AS lkp_DeclinedQuoteDimId,
	LKP_ClosedQuoteDim.ClosedQuoteDimId AS lkp_ClosedQuoteDimId,
	LKP_BusinessClassDim.BusinessClassDimId AS lkp_BusinessClassDimId,
	LKP_InsuranceReferenceDim.InsuranceReferenceDimId AS lkp_InsuranceReferenceDimId,
	LKP_UnderwritingDivisionDim.UnderwritingDivisionDimID AS lkp_UnderwritingDivisionDimID,
	LKP_QuoteDim.QuoteDimId AS lkp_QuoteDimId,
	LKP_QuoteDim.ReleasedQuoteFlag AS lkp_ReleasedQuoteFlag,
	SQ_Quote.QuoteKey,
	SQ_Quote.QuoteNumber AS i_QuoteNumber,
	SQ_Quote.CurrentSnapshotFlag AS i_CurrentSnapshotFlag,
	SQ_Quote.QuoteStatusCode AS i_QuoteStatusCode,
	SQ_Quote.InternalExternalIndicator AS i_InternalExternalIndicator,
	SQ_Quote.QuoteEffectiveDate AS i_QuoteEffectiveDate,
	SQ_Quote.StatusDate,
	SQ_Quote.QuoteCreatedDate AS i_StatusCreatedDate,
	SQ_Quote.EstimatedQuotePremium AS i_EstimatedQuotePremium,
	SQ_Quote.QuoteId,
	-- *INF*: IIF(i_CurrentSnapshotFlag='T','1','0')
	IFF(i_CurrentSnapshotFlag = 'T', '1', '0') AS CurrentSnapshotFlag,
	-- *INF*: IIF(ISNULL(:LKP.LKP_CALENDAR_DIM_CLNDR_ID(TO_DATE(TO_CHAR(StatusDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))), -1, :LKP.LKP_CALENDAR_DIM_CLNDR_ID(TO_DATE(TO_CHAR(StatusDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')))
	IFF(
	    LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_StatusDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id IS NULL,
	    - 1,
	    LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_StatusDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id
	) AS QuoteStatusDateID,
	-- *INF*: IIF( ISNULL(:LKP.LKP_CALENDAR_DIM_CLNDR_ID(TO_DATE(TO_CHAR(i_StatusCreatedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))), -1, :LKP.LKP_CALENDAR_DIM_CLNDR_ID(TO_DATE(TO_CHAR(i_StatusCreatedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')))
	IFF(
	    LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_i_StatusCreatedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id IS NULL,
	    - 1,
	    LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_i_StatusCreatedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id
	) AS QuoteCreatedDateID,
	-- *INF*: IIF( ISNULL(:LKP.LKP_CALENDAR_DIM_CLNDR_ID(TO_DATE(TO_CHAR(i_QuoteEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))), -1, :LKP.LKP_CALENDAR_DIM_CLNDR_ID(TO_DATE(TO_CHAR(i_QuoteEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')))
	IFF(
	    LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_i_QuoteEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id IS NULL,
	    - 1,
	    LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_i_QuoteEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id
	) AS QuoteEffectiveDateID,
	-- *INF*: IIF( ISNULL(lkp_QuoteStatusDimId), -1, lkp_QuoteStatusDimId)
	IFF(lkp_QuoteStatusDimId IS NULL, - 1, lkp_QuoteStatusDimId) AS QuoteStatusDimID,
	-- *INF*: IIF( ISNULL(lkp_StateDimID), -1, lkp_StateDimID)
	IFF(lkp_StateDimID IS NULL, - 1, lkp_StateDimID) AS RiskStateDimID,
	-- *INF*: IIF( ISNULL(lkp_PartyDimID), -1, lkp_PartyDimID)
	IFF(lkp_PartyDimID IS NULL, - 1, lkp_PartyDimID) AS PartyDimID,
	-- *INF*: IIF( ISNULL(lkp_AgencyDimID), -1, lkp_AgencyDimID)
	IFF(lkp_AgencyDimID IS NULL, - 1, lkp_AgencyDimID) AS AgencyDimID,
	-- *INF*: IIF( ISNULL(lkp_AgencyEmployeeDimID), -1, lkp_AgencyEmployeeDimID)
	IFF(lkp_AgencyEmployeeDimID IS NULL, - 1, lkp_AgencyEmployeeDimID) AS AgencyEmployeeDimID,
	-- *INF*: IIF(ISNULL(lkp_UnderwritingDivisionDimID), -1,lkp_UnderwritingDivisionDimID)
	IFF(lkp_UnderwritingDivisionDimID IS NULL, - 1, lkp_UnderwritingDivisionDimID) AS UnderwritingDivisionDimID,
	-- *INF*: IIF( ISNULL(lkp_ProgramDimId), -1 , lkp_ProgramDimId)
	IFF(lkp_ProgramDimId IS NULL, - 1, lkp_ProgramDimId) AS ProgramDimId,
	-- *INF*: IIF( ISNULL(lkp_DeclinedQuoteDimId), -1, lkp_DeclinedQuoteDimId)
	IFF(lkp_DeclinedQuoteDimId IS NULL, - 1, lkp_DeclinedQuoteDimId) AS DeclinedQuoteDimId,
	-- *INF*: IIF( ISNULL(lkp_BusinessClassDimId), -1, lkp_BusinessClassDimId)
	IFF(lkp_BusinessClassDimId IS NULL, - 1, lkp_BusinessClassDimId) AS BusinessClassDimId,
	-- *INF*: IIF( ISNULL(lkp_InsuranceReferenceDimId), -1, lkp_InsuranceReferenceDimId)
	IFF(lkp_InsuranceReferenceDimId IS NULL, - 1, lkp_InsuranceReferenceDimId) AS InsuranceReferenceDimId,
	-- *INF*: IIF(i_InternalExternalIndicator =  'External', 1, 0)
	IFF(i_InternalExternalIndicator = 'External', 1, 0) AS ExternalQuoteIndicator,
	-- *INF*: IIF(NOT ISNULL(lkp_QuoteDimId),lkp_QuoteDimId)
	IFF(lkp_QuoteDimId IS NOT NULL, lkp_QuoteDimId) AS QuoteDimId,
	i_QuoteNumber AS QuoteCounter,
	-- *INF*: IIF(NOT ISNULL(lkp_PartyNumber),lkp_PartyNumber,'N/A')
	IFF(lkp_PartyNumber IS NOT NULL, lkp_PartyNumber, 'N/A') AS CustomerCounter,
	-- *INF*: IIF( ISNULL(i_EstimatedQuotePremium), 0, i_EstimatedQuotePremium)
	IFF(i_EstimatedQuotePremium IS NULL, 0, i_EstimatedQuotePremium) AS EstimatedQuotePremium,
	-- *INF*: IIF( ISNULL(lkp_ClosedQuoteDimId), -1, lkp_ClosedQuoteDimId)
	IFF(lkp_ClosedQuoteDimId IS NULL, - 1, lkp_ClosedQuoteDimId) AS ClosedQuoteDimId,
	SQ_Quote.QuotePolicyOfferingTransactionId AS i_QuotePolicyOfferingTransactionId,
	-- *INF*: IIF( ISNULL(i_QuotePolicyOfferingTransactionId), -1, i_QuotePolicyOfferingTransactionId)
	IFF(i_QuotePolicyOfferingTransactionId IS NULL, - 1, i_QuotePolicyOfferingTransactionId) AS QuotePolicyOfferingTransactionId,
	SQ_Quote.QuotePolicyOfferingTransactionAKID,
	SQ_Quote.QuoteID1,
	SQ_Quote.QuoteAKID1,
	SQ_Quote.QuotePremium AS i_QuotePremium,
	-- *INF*: iif(isnull(i_QuotePremium) ,0,i_QuotePremium)
	IFF(i_QuotePremium IS NULL, 0, i_QuotePremium) AS QuotePremium
	FROM SQ_Quote
	LEFT JOIN LKP_AgencyDim
	ON LKP_AgencyDim.EDWAgencyAKID = SQ_Quote.AgencyAKId
	LEFT JOIN LKP_AgencyEmployeeDim
	ON LKP_AgencyEmployeeDim.EDWAgencyEmployeeAKID = SQ_Quote.AgencyEmployeeAKId
	LEFT JOIN LKP_BusinessClassDim
	ON LKP_BusinessClassDim.BusinessClassCode = SQ_Quote.BusinessClassCode
	LEFT JOIN LKP_ClosedQuoteDim
	ON LKP_ClosedQuoteDim.EDWQuotePKId = SQ_Quote.QuoteId
	LEFT JOIN LKP_DeclinedQuoteDim
	ON LKP_DeclinedQuoteDim.EDWQuotePKId = SQ_Quote.QuoteId
	LEFT JOIN LKP_InsuranceReferenceDim
	ON LKP_InsuranceReferenceDim.EnterpriseGroupCode = SQ_Quote.EnterpriseGroupCode AND LKP_InsuranceReferenceDim.InsuranceReferenceLegalEntityCode = SQ_Quote.InsuranceReferenceLegalEntityCode AND LKP_InsuranceReferenceDim.StrategicProfitCenterCode = SQ_Quote.StrategicProfitCenterCode AND LKP_InsuranceReferenceDim.InsuranceSegmentCode = SQ_Quote.InsuranceSegmentCode AND LKP_InsuranceReferenceDim.PolicyOfferingCode = SQ_Quote.PolicyOfferingCode AND LKP_InsuranceReferenceDim.ProductCode = EXP_Default_ProductCode_LineOfBusinessCode.o_ProductCode AND LKP_InsuranceReferenceDim.InsuranceReferenceLineOfBusinessCode = EXP_Default_ProductCode_LineOfBusinessCode.o_InsuranceReferenceLineOfBusinessCode AND LKP_InsuranceReferenceDim.RatingPlanCode = EXP_Default_ProductCode_LineOfBusinessCode.o_RatingPlanCode
	LEFT JOIN LKP_PartyDim
	ON LKP_PartyDim.EDWPartyAKID = LKP_PartyRoleInAgreement.PartyAkId
	LEFT JOIN LKP_ProgramDim
	ON LKP_ProgramDim.ProgramCode = LKP_Program.ProgramCode
	LEFT JOIN LKP_QuoteDim
	ON LKP_QuoteDim.EDWQuoteAKId = SQ_Quote.QuoteAKId
	LEFT JOIN LKP_QuoteStatusDim
	ON LKP_QuoteStatusDim.QuoteStatusCode = SQ_Quote.QuoteStatusCode
	LEFT JOIN LKP_StateDim
	ON LKP_StateDim.StateAbbreviation = SQ_Quote.RiskState
	LEFT JOIN LKP_UnderwritingDivisionDim
	ON LKP_UnderwritingDivisionDim.EDWUnderwritingAssociateAKID = SQ_Quote.UnderwritingAssociateAKId
	LEFT JOIN LKP_CALENDAR_DIM_CLNDR_ID LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_StatusDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_StatusDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(TO_CHAR(StatusDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM_CLNDR_ID LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_i_StatusCreatedDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_i_StatusCreatedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(TO_CHAR(i_StatusCreatedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM_CLNDR_ID LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_i_QuoteEffectiveDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_i_QuoteEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(TO_CHAR(i_QuoteEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

),
LKP_FirstBound AS (
	SELECT
	QuoteKey,
	StatusDate
	FROM (
		Select 
		b.QuoteKey as QuoteKey,
		b.StatusDate as StatusDate
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.Quote b
		where b.QuoteStatusCode='Bound'
		and exists (
		select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote q
		where q.QuoteKey=b.QuoteKey
		and q.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}')
		and not exists (
		select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote c
		where c.QuoteKey=b.QuoteKey
		and c.StatusDate>b.StatusDate
		and c.QuoteStatusCode='Bound')
		order by b.StatusDate,b.QuoteKey
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StatusDate,QuoteKey ORDER BY QuoteKey) = 1
),
LKP_FirstReleaseQuote AS (
	SELECT
	QuoteKey,
	StatusDate
	FROM (
		Select 
		b.QuoteKey as QuoteKey,
		b.StatusDate as StatusDate
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.Quote b
		where b.QuoteStatusCode='Released Quote'
		and exists (
		select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote q
		where q.QuoteKey=b.QuoteKey
		and q.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}')
		and not exists (
		select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote c
		where c.QuoteKey=b.QuoteKey
		and c.StatusDate>b.StatusDate
		and c.QuoteStatusCode='Released Quote')
		order by b.StatusDate,b.QuoteKey
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StatusDate,QuoteKey ORDER BY QuoteKey) = 1
),
LKP_QuotePolicyOfferingTransactionFact AS (
	SELECT
	QuotePolicyOfferingTransactionfactId,
	EDWQuotePKId,
	EDWQuotePolicyOfferingTransactionPKId
	FROM (
		SELECT 
			QuotePolicyOfferingTransactionfactId,
			EDWQuotePKId,
			EDWQuotePolicyOfferingTransactionPKId
		FROM QuotePolicyOfferingTransactionFact
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWQuotePKId,EDWQuotePolicyOfferingTransactionPKId ORDER BY QuotePolicyOfferingTransactionfactId) = 1
),
EXP_InsertUpdate AS (
	SELECT
	LKP_FirstBound.QuoteKey AS lkp_FirstBound,
	LKP_FirstReleaseQuote.QuoteKey AS lkp_FirstReleasedQuote,
	-- *INF*: iif(ISNULL(lkp_QuotePolicyOfferingTransactionfactId),'Insert',
	-- 'Ignore')
	-- 
	-- --DECODE(TRUE,
	-- --lkp_CurrentSnapshotFlag='1' AND CurrentSnapshotFlag='0' AND ISNULL(lkp_FirstBound) AND ISNULL(lkp_FirstReleasedQuote),'Delete',
	-- --CurrentSnapshotFlag='0' AND ISNULL(lkp_FirstBound) AND ISNULL(lkp_FirstReleasedQuote),'Ignore',
	-- --ISNULL(lkp_QuoteTransactionFactId),'Insert',
	-- --'Ignore')
	IFF(lkp_QuotePolicyOfferingTransactionfactId IS NULL, 'Insert', 'Ignore') AS InsertUpdate,
	EXP_GatherAllPorts.QuoteId AS EDWQuotePKId,
	EXP_GatherAllPorts.CurrentSnapshotFlag,
	-- *INF*: IIF(ISNULL(lkp_FirstBound),'0','1')
	IFF(lkp_FirstBound IS NULL, '0', '1') AS FirstBoundFlag,
	EXP_GatherAllPorts.QuoteStatusDateID,
	EXP_GatherAllPorts.QuoteCreatedDateID,
	EXP_GatherAllPorts.QuoteEffectiveDateID,
	EXP_GatherAllPorts.QuoteStatusDimID,
	EXP_GatherAllPorts.RiskStateDimID,
	EXP_GatherAllPorts.PartyDimID,
	EXP_GatherAllPorts.AgencyDimID,
	EXP_GatherAllPorts.AgencyEmployeeDimID,
	EXP_GatherAllPorts.UnderwritingDivisionDimID,
	EXP_GatherAllPorts.ProgramDimId,
	EXP_GatherAllPorts.DeclinedQuoteDimId,
	EXP_GatherAllPorts.BusinessClassDimId,
	EXP_GatherAllPorts.InsuranceReferenceDimId,
	EXP_GatherAllPorts.ExternalQuoteIndicator,
	EXP_GatherAllPorts.QuoteDimId,
	EXP_GatherAllPorts.QuoteCounter,
	EXP_GatherAllPorts.CustomerCounter,
	EXP_GatherAllPorts.EstimatedQuotePremium,
	EXP_GatherAllPorts.ClosedQuoteDimId,
	-- *INF*: IIF(ISNULL(lkp_FirstReleasedQuote),'0','1')
	IFF(lkp_FirstReleasedQuote IS NULL, '0', '1') AS FirstReleasedQuoteFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	LKP_QuotePolicyOfferingTransactionFact.QuotePolicyOfferingTransactionfactId AS lkp_QuotePolicyOfferingTransactionfactId,
	EXP_GatherAllPorts.QuotePremium,
	EXP_GatherAllPorts.QuotePolicyOfferingTransactionId
	FROM EXP_GatherAllPorts
	LEFT JOIN LKP_FirstBound
	ON LKP_FirstBound.StatusDate = EXP_GatherAllPorts.StatusDate AND LKP_FirstBound.QuoteKey = EXP_GatherAllPorts.QuoteKey
	LEFT JOIN LKP_FirstReleaseQuote
	ON LKP_FirstReleaseQuote.StatusDate = EXP_GatherAllPorts.StatusDate AND LKP_FirstReleaseQuote.QuoteKey = EXP_GatherAllPorts.QuoteKey
	LEFT JOIN LKP_QuotePolicyOfferingTransactionFact
	ON LKP_QuotePolicyOfferingTransactionFact.EDWQuotePKId = EXP_GatherAllPorts.QuoteId AND LKP_QuotePolicyOfferingTransactionFact.EDWQuotePolicyOfferingTransactionPKId = EXP_GatherAllPorts.QuotePolicyOfferingTransactionId
),
FLTR_INSERT AS (
	SELECT
	InsertUpdate, 
	EDWQuotePKId, 
	CurrentSnapshotFlag, 
	FirstBoundFlag, 
	QuoteStatusDateID, 
	QuoteCreatedDateID, 
	QuoteEffectiveDateID, 
	QuoteStatusDimID, 
	RiskStateDimID, 
	PartyDimID, 
	AgencyDimID, 
	AgencyEmployeeDimID, 
	UnderwritingDivisionDimID, 
	ProgramDimId, 
	DeclinedQuoteDimId, 
	BusinessClassDimId, 
	InsuranceReferenceDimId, 
	ExternalQuoteIndicator, 
	QuoteDimId, 
	QuoteCounter, 
	CustomerCounter, 
	EstimatedQuotePremium, 
	ClosedQuoteDimId, 
	FirstReleasedQuoteFlag, 
	o_AuditId, 
	lkp_QuotePolicyOfferingTransactionfactId, 
	QuotePremium, 
	QuotePolicyOfferingTransactionId
	FROM EXP_InsertUpdate
	WHERE InsertUpdate='Insert'
),
QuotePolicyOfferingTransactionFact AS (
	TRUNCATE TABLE QuotePolicyOfferingTransactionFact;
	INSERT INTO QuotePolicyOfferingTransactionFact
	(EDWQuotePKId, EDWQuotePolicyOfferingTransactionPKId, FirstBoundFlag, QuoteStatusDateId, QuoteCreatedDateId, QuoteEffectiveDateId, QuoteStatusDimId, RiskStateDimId, PartyDimId, AgencyDimId, AgencyEmployeeDimId, UnderwritingDivisionDimId, ProgramDimId, DeclinedQuoteDimId, BusinessClassDimId, InsuranceReferenceDimId, ExternalQuoteIndicator, QuotePremium, AuditId, QuoteDimId, QuoteCounter, CustomerCounter, FirstReleasedQuoteFlag, EstimatedQuotePremium, ClosedQuoteDimId)
	SELECT 
	EDWQUOTEPKID, 
	QuotePolicyOfferingTransactionId AS EDWQUOTEPOLICYOFFERINGTRANSACTIONPKID, 
	FIRSTBOUNDFLAG, 
	QuoteStatusDateID AS QUOTESTATUSDATEID, 
	QuoteCreatedDateID AS QUOTECREATEDDATEID, 
	QuoteEffectiveDateID AS QUOTEEFFECTIVEDATEID, 
	QuoteStatusDimID AS QUOTESTATUSDIMID, 
	RiskStateDimID AS RISKSTATEDIMID, 
	PartyDimID AS PARTYDIMID, 
	AgencyDimID AS AGENCYDIMID, 
	AgencyEmployeeDimID AS AGENCYEMPLOYEEDIMID, 
	UnderwritingDivisionDimID AS UNDERWRITINGDIVISIONDIMID, 
	PROGRAMDIMID, 
	DECLINEDQUOTEDIMID, 
	BUSINESSCLASSDIMID, 
	INSURANCEREFERENCEDIMID, 
	EXTERNALQUOTEINDICATOR, 
	QUOTEPREMIUM, 
	o_AuditId AS AUDITID, 
	QUOTEDIMID, 
	QUOTECOUNTER, 
	CUSTOMERCOUNTER, 
	FIRSTRELEASEDQUOTEFLAG, 
	ESTIMATEDQUOTEPREMIUM, 
	CLOSEDQUOTEDIMID
	FROM FLTR_INSERT
),