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
	SELECT
		Quote.QuoteId,
		QuoteTransaction.QuoteTransactionId,
		Quote.QuoteAKId,
		Quote.QuoteKey,
		Quote.AgencyAKId,
		Quote.QuoteNumber,
		Quote.BusinessClassCode,
		Quote.InternalExternalIndicator,
		Quote.RiskState,
		Quote.QuoteEffectiveDate,
		Quote.ProgramAKId,
		QuoteTransaction.WrittenPremium,
		Quote.CurrentSnapshotFlag,
		Quote.QuoteStatusCode,
		Quote.UnderwritingAssociateAKId,
		Quote.AgencyEmployeeAKId,
		Quote.StatusDate,
		Quote.QuoteCreatedDate,
		Quote.EstimatedQuotePremium,
		EnterpriseGroup.EnterpriseGroupCode,
		InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityCode,
		StrategicProfitCenter.StrategicProfitCenterCode,
		InsuranceSegment.InsuranceSegmentCode,
		PolicyOffering.PolicyOfferingCode,
		Product.ProductCode,
		InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessCode,
		QuoteTransaction.RiskGradeCode
	FROM Quote
	INNER JOIN QuoteTransaction
	INNER JOIN InsuranceReferenceLegalEntity
	INNER JOIN InsuranceSegment
	INNER JOIN PolicyOffering
	INNER JOIN Product
	INNER JOIN StrategicProfitCenter
	INNER JOIN EnterpriseGroup
	INNER JOIN InsuranceReferenceLineOfBusiness
	ON {
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote
	left outer join @{pipeline().parameters.SOURCE_TABLE_OWNER}.QuoteTransaction 
	on Quote.QuoteAKId = QuoteTransaction.QuoteAKId
	and Quote.StatusDate=QuoteTransaction.StatusDate
	left outer join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering
	on PolicyOffering.PolicyOfferingAKId=Quote.PolicyOfferingAKId and PolicyOffering.CurrentSnapshotFlag=1
	left outer join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter
	on StrategicProfitCenter.StrategicProfitCenterAKId=Quote.StrategicProfitCenterAKId  and StrategicProfitCenter.CurrentSnapshotFlag=1
	left outer join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment
	on InsuranceSegment.InsuranceSegmentAKId=Quote.InsuranceSegmentAKId and InsuranceSegment.CurrentSnapshotFlag=1
	left outer join @{pipeline().parameters.SOURCE_TABLE_OWNER}.EnterpriseGroup
	on EnterpriseGroup.EnterpriseGroupId=StrategicProfitCenter.EnterpriseGroupId
	left outer join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLegalEntity
	on InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityId=StrategicProfitCenter.InsuranceReferenceLegalEntityId
	left outer join @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness
	on InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessAKId=QuoteTransaction.InsuranceReferenceLineOfBusinessAKId and InsuranceReferenceLineOfBusiness.CurrentSnapshotFlag=1
	left outer join @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product
	on Product.ProductAKId=QuoteTransaction.ProductAKId and Product.CurrentSnapshotFlag=1
	}
	WHERE Quote.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}' @{pipeline().parameters.WHERE_CLAUSE}
),
LKP_AgencyDim AS (
	SELECT
	i_AgencyAKId,
	AgencyDimID,
	AgencyCode,
	EDWAgencyAKID
	FROM (
		SELECT 
			i_AgencyAKId,
			AgencyDimID,
			AgencyCode,
			EDWAgencyAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V3}.AgencyDim
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyAKID ORDER BY i_AgencyAKId) = 1
),
LKP_AgencyEmployeeDim AS (
	SELECT
	i_AgencyEmployeeAKID,
	AgencyEmployeeDimID,
	EDWAgencyEmployeeAKID
	FROM (
		SELECT 
			i_AgencyEmployeeAKID,
			AgencyEmployeeDimID,
			EDWAgencyEmployeeAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyEmployeeDim
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyEmployeeAKID ORDER BY i_AgencyEmployeeAKID) = 1
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
	ProductCode AS i_ProductCode,
	InsuranceReferenceLineOfBusinessCode AS i_InsuranceReferenceLineOfBusinessCode,
	-- *INF*: IIF(ISNULL(i_ProductCode), '000', i_ProductCode)
	IFF(i_ProductCode IS NULL, '000', i_ProductCode) AS o_ProductCode,
	-- *INF*: IIF(ISNULL(i_InsuranceReferenceLineOfBusinessCode), '000', i_InsuranceReferenceLineOfBusinessCode)
	IFF(i_InsuranceReferenceLineOfBusinessCode IS NULL, '000', i_InsuranceReferenceLineOfBusinessCode) AS o_InsuranceReferenceLineOfBusinessCode,
	'1' AS o_RatingPlanCode
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
	i_ProgramAKId,
	ProgramAKId,
	ProgramCode
	FROM (
		SELECT 
			i_ProgramAKId,
			ProgramAKId,
			ProgramCode
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Program
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProgramAKId ORDER BY i_ProgramAKId) = 1
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
		WHERE EXISTS (
		SELECT 1 FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote q
		where q.QuoteAKId=QuoteDim.EDWQuoteAKId
		and q.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}' )
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
	SQ_Quote.QuoteTransactionId AS i_QuoteTransactionId,
	SQ_Quote.QuoteKey,
	SQ_Quote.QuoteNumber AS i_QuoteNumber,
	SQ_Quote.CurrentSnapshotFlag AS i_CurrentSnapshotFlag,
	SQ_Quote.QuoteStatusCode AS i_QuoteStatusCode,
	SQ_Quote.InternalExternalIndicator AS i_InternalExternalIndicator,
	SQ_Quote.QuoteEffectiveDate AS i_QuoteEffectiveDate,
	SQ_Quote.WrittenPremium AS i_WrittenPremium,
	SQ_Quote.StatusDate,
	SQ_Quote.QuoteCreatedDate AS i_StatusCreatedDate,
	SQ_Quote.EstimatedQuotePremium AS i_EstimatedQuotePremium,
	SQ_Quote.RiskGradeCode AS i_RiskGrade,
	SQ_Quote.QuoteId,
	-- *INF*: DECODE(TRUE,
	-- i_QuoteStatusCode='Initialized Quote',0,
	-- --IN(i_QuoteStatusCode,'Declined','Closed')=1 AND lkp_ReleasedQuoteFlag='0',0,
	-- i_WrittenPremium)
	DECODE(TRUE,
		i_QuoteStatusCode = 'Initialized Quote', 0,
		i_WrittenPremium) AS v_WrittenPremium,
	-- *INF*: IIF(NOT ISNULL(i_QuoteTransactionId),i_QuoteTransactionId,-1)
	IFF(NOT i_QuoteTransactionId IS NULL, i_QuoteTransactionId, - 1) AS QuoteTransactionId,
	-- *INF*: IIF(i_CurrentSnapshotFlag='T','1','0')
	IFF(i_CurrentSnapshotFlag = 'T', '1', '0') AS CurrentSnapshotFlag,
	-- *INF*: IIF(ISNULL(:LKP.LKP_CALENDAR_DIM_CLNDR_ID(TO_DATE(TO_CHAR(StatusDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))), -1, :LKP.LKP_CALENDAR_DIM_CLNDR_ID(TO_DATE(TO_CHAR(StatusDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')))
	IFF(LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_StatusDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id IS NULL, - 1, LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_StatusDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id) AS QuoteStatusDateID,
	-- *INF*: IIF( ISNULL(:LKP.LKP_CALENDAR_DIM_CLNDR_ID(TO_DATE(TO_CHAR(i_StatusCreatedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))), -1, :LKP.LKP_CALENDAR_DIM_CLNDR_ID(TO_DATE(TO_CHAR(i_StatusCreatedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')))
	IFF(LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_i_StatusCreatedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id IS NULL, - 1, LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_i_StatusCreatedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id) AS QuoteCreatedDateID,
	-- *INF*: IIF( ISNULL(:LKP.LKP_CALENDAR_DIM_CLNDR_ID(TO_DATE(TO_CHAR(i_QuoteEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))), -1, :LKP.LKP_CALENDAR_DIM_CLNDR_ID(TO_DATE(TO_CHAR(i_QuoteEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')))
	IFF(LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_i_QuoteEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id IS NULL, - 1, LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_i_QuoteEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id) AS QuoteEffectiveDateID,
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
	-- *INF*: IIF( ISNULL(v_WrittenPremium), 0, v_WrittenPremium)
	IFF(v_WrittenPremium IS NULL, 0, v_WrittenPremium) AS WrittenPremium,
	-- *INF*: IIF(ISNULL(i_RiskGrade), 'N/A', i_RiskGrade)
	IFF(i_RiskGrade IS NULL, 'N/A', i_RiskGrade) AS RiskGrade,
	-- *INF*: IIF(NOT ISNULL(lkp_QuoteDimId),lkp_QuoteDimId)
	IFF(NOT lkp_QuoteDimId IS NULL, lkp_QuoteDimId) AS QuoteDimId,
	i_QuoteNumber AS QuoteCounter,
	-- *INF*: IIF(NOT ISNULL(lkp_PartyNumber),lkp_PartyNumber,'N/A')
	IFF(NOT lkp_PartyNumber IS NULL, lkp_PartyNumber, 'N/A') AS CustomerCounter,
	-- *INF*: IIF( ISNULL(i_EstimatedQuotePremium), 0, i_EstimatedQuotePremium)
	IFF(i_EstimatedQuotePremium IS NULL, 0, i_EstimatedQuotePremium) AS EstimatedQuotePremium,
	-- *INF*: IIF( ISNULL(lkp_ClosedQuoteDimId), -1, lkp_ClosedQuoteDimId)
	IFF(lkp_ClosedQuoteDimId IS NULL, - 1, lkp_ClosedQuoteDimId) AS ClosedQuoteDimId
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
	ON LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_StatusDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_DATE(TO_CHAR(StatusDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM_CLNDR_ID LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_i_StatusCreatedDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_i_StatusCreatedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_DATE(TO_CHAR(i_StatusCreatedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDAR_DIM_CLNDR_ID LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_i_QuoteEffectiveDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDAR_DIM_CLNDR_ID_TO_DATE_TO_CHAR_i_QuoteEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_DATE(TO_CHAR(i_QuoteEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

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
LKP_QuoteTransactionFact AS (
	SELECT
	QuoteTransactionFactId,
	CurrentSnapshotFlag,
	EDWQuotePKId,
	EDWQuoteTransactionPKId
	FROM (
		SELECT a.QuoteTransactionFactId as QuoteTransactionFactId,
		convert(varchar(1),a.CurrentSnapshotFlag) as CurrentSnapshotFlag,
		a.EDWQuotePKId as EDWQuotePKId,
		a.EDWQuoteTransactionPKId as EDWQuoteTransactionPKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.QuoteTransactionFact a
		where exists (
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote c
		where c.QuoteNumber=a.QuoteCounter
		and c.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}')
		order by a.EDWQuotePKId,a.EDWQuoteTransactionPKId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWQuotePKId,EDWQuoteTransactionPKId ORDER BY QuoteTransactionFactId) = 1
),
EXP_InsertUpdate AS (
	SELECT
	LKP_QuoteTransactionFact.QuoteTransactionFactId AS lkp_QuoteTransactionFactId,
	LKP_QuoteTransactionFact.CurrentSnapshotFlag AS lkp_CurrentSnapshotFlag,
	LKP_FirstBound.QuoteKey AS lkp_FirstBound,
	LKP_FirstReleaseQuote.QuoteKey AS lkp_FirstReleasedQuote,
	-- *INF*: DECODE(TRUE,
	-- lkp_CurrentSnapshotFlag='1' AND CurrentSnapshotFlag='0' AND ISNULL(lkp_FirstBound) AND ISNULL(lkp_FirstReleasedQuote),'Delete',
	-- CurrentSnapshotFlag='0' AND ISNULL(lkp_FirstBound) AND ISNULL(lkp_FirstReleasedQuote),'Ignore',
	-- ISNULL(lkp_QuoteTransactionFactId),'Insert',
	-- 'Ignore')
	DECODE(TRUE,
		lkp_CurrentSnapshotFlag = '1' AND CurrentSnapshotFlag = '0' AND lkp_FirstBound IS NULL AND lkp_FirstReleasedQuote IS NULL, 'Delete',
		CurrentSnapshotFlag = '0' AND lkp_FirstBound IS NULL AND lkp_FirstReleasedQuote IS NULL, 'Ignore',
		lkp_QuoteTransactionFactId IS NULL, 'Insert',
		'Ignore') AS InsertUpdate,
	lkp_QuoteTransactionFactId AS QuoteTransactionFactId,
	EXP_GatherAllPorts.QuoteId AS EDWQuotePKId,
	EXP_GatherAllPorts.QuoteTransactionId AS EDWQuoteTransactionPKId,
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
	EXP_GatherAllPorts.WrittenPremium AS QuoteTransactionPremium,
	EXP_GatherAllPorts.RiskGrade,
	EXP_GatherAllPorts.QuoteDimId,
	EXP_GatherAllPorts.QuoteCounter,
	EXP_GatherAllPorts.CustomerCounter,
	EXP_GatherAllPorts.EstimatedQuotePremium,
	EXP_GatherAllPorts.ClosedQuoteDimId,
	-- *INF*: IIF(ISNULL(lkp_FirstReleasedQuote),'0','1')
	IFF(lkp_FirstReleasedQuote IS NULL, '0', '1') AS FirstReleasedQuoteFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM EXP_GatherAllPorts
	LEFT JOIN LKP_FirstBound
	ON LKP_FirstBound.StatusDate = EXP_GatherAllPorts.StatusDate AND LKP_FirstBound.QuoteKey = EXP_GatherAllPorts.QuoteKey
	LEFT JOIN LKP_FirstReleaseQuote
	ON LKP_FirstReleaseQuote.StatusDate = EXP_GatherAllPorts.StatusDate AND LKP_FirstReleaseQuote.QuoteKey = EXP_GatherAllPorts.QuoteKey
	LEFT JOIN LKP_QuoteTransactionFact
	ON LKP_QuoteTransactionFact.EDWQuotePKId = EXP_GatherAllPorts.QuoteId AND LKP_QuoteTransactionFact.EDWQuoteTransactionPKId = EXP_GatherAllPorts.QuoteTransactionId
),
RTR_InsertUPdate AS (
	SELECT
	InsertUpdate,
	QuoteTransactionFactId,
	EDWQuotePKId,
	EDWQuoteTransactionPKId,
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
	QuoteTransactionPremium,
	RiskGrade,
	QuoteDimId,
	o_AuditId AS AuditId,
	QuoteCounter,
	CustomerCounter,
	FirstReleasedQuoteFlag,
	EstimatedQuotePremium,
	ClosedQuoteDimId
	FROM EXP_InsertUpdate
),
RTR_InsertUPdate_Insert AS (SELECT * FROM RTR_InsertUPdate WHERE InsertUpdate = 'Insert'),
RTR_InsertUPdate_Delete AS (SELECT * FROM RTR_InsertUPdate WHERE InsertUpdate = 'Delete'),
UPD_QuoteTransactionFact_Delete AS (
	SELECT
	QuoteTransactionFactId
	FROM RTR_InsertUPdate_Delete
),
TGT_QuoteTransactionFact_Delete AS (
	DELETE FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.QuoteTransactionFact
	WHERE (QuoteTransactionFactId) IN (SELECT  QUOTETRANSACTIONFACTID FROM UPD_QuoteTransactionFact_Delete)
),
UPD_QuoteTransactionFact_Insert AS (
	SELECT
	EDWQuotePKId, 
	EDWQuoteTransactionPKId, 
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
	QuoteTransactionPremium, 
	AuditId, 
	RiskGrade, 
	QuoteDimId, 
	QuoteCounter, 
	CustomerCounter, 
	FirstReleasedQuoteFlag, 
	EstimatedQuotePremium, 
	ClosedQuoteDimId
	FROM RTR_InsertUPdate_Insert
),
TGT_QuoteTransactionFact_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.QuoteTransactionFact
	(EDWQuotePKId, EDWQuoteTransactionPKId, CurrentSnapshotFlag, FirstBoundFlag, QuoteStatusDateId, QuoteCreatedDateId, QuoteEffectiveDateId, QuoteStatusDimId, RiskStateDimId, PartyDimId, AgencyDimId, AgencyEmployeeDimId, UnderwritingDivisionDimId, ProgramDimId, DeclinedQuoteDimId, BusinessClassDimId, InsuranceReferenceDimId, ExternalQuoteIndicator, QuoteTransactionPremium, AuditId, RiskGradeCode, QuoteDimId, QuoteCounter, CustomerCounter, FirstReleasedQuoteFlag, EstimatedQuotePremium, ClosedQuoteDimId)
	SELECT 
	EDWQUOTEPKID, 
	EDWQUOTETRANSACTIONPKID, 
	CURRENTSNAPSHOTFLAG, 
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
	QUOTETRANSACTIONPREMIUM, 
	AUDITID, 
	RiskGrade AS RISKGRADECODE, 
	QUOTEDIMID, 
	QUOTECOUNTER, 
	CUSTOMERCOUNTER, 
	FIRSTRELEASEDQUOTEFLAG, 
	ESTIMATEDQUOTEPREMIUM, 
	CLOSEDQUOTEDIMID
	FROM UPD_QuoteTransactionFact_Insert

	------------ POST SQL ----------
	Update a set a.CurrentSnapshotFlag=b.CurrentSnapshotFlag
	from @{pipeline().parameters.TARGET_TABLE_OWNER}.QuoteTransactionFact a
	join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote b
	on a.EDWQuotePKId=b.QuoteId
	and a.CurrentSnapshotFlag<>b.CurrentSnapshotFlag
	-------------------------------


),