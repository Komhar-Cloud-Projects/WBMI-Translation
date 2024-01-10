WITH
SQ_DCLimitStaging AS (
	WITH PCoverage as (
	select A.SessionId, A.ObjectId AS ObjectId, A.ObjectName AS ObjectName, A.coverageID as PCoverageid,A.CoverageId, A.Id AS CoverageGUID, A.Type as CoverageType, 
	CASE WHEN A.ObjectName = 'DC_Line' THEN L.Type 
	ELSE 
	CASE substring(A.ObjectName,4,3) 
	when 'CF_' then 'Property'
	when 'GL_' then 'GeneralLiability'
	when 'WC_' then 'WorkersCompensation'
	when 'BP_' then 'BusinessOwners'
	when 'CR_' then 'Crime'
	when 'IM_' then 'InlandMarine'
	when 'EXL' then 'ExcessLiability'
	when 'CU_' then 'CommercialUmbrella'
	when 'CA_' then 'CommercialAuto'
	when 'CDO' then 'DirectorsAndOffsCondos'
	when 'EPL' then 'EmploymentPracticesLiab'
	when 'HIO' then 'HoleInOne'
	when 'EC_' then 'EventCancellation'
	ELSE 'N/A' 
	END END AS InsuranceLine 
	from DCCoverageStaging A
	left join DCLineStaging L on L.sessionid = A.sessionid and A.objectid = L.lineID
	where A.ObjectName <> 'DC_Coverage'
	
	union all
	
	select B.SessionId, B.ObjectId AS ObjectId, B.ObjectName AS ObjectName,B.coverageID as PCoverageid, A.CoverageId, A.Id AS CoverageGUID,A.Type as CoverageType, 
	CASE WHEN B.ObjectName = 'DC_Line' THEN L.Type
	ELSE
	case substring(B.ObjectName,4,3) 
	when 'CF_' then 'Property'
	when 'GL_' then 'GeneralLiability'
	when 'WC_' then 'WorkersCompensation'
	when 'BP_' then 'BusinessOwners'
	when 'CR_' then 'Crime'
	when 'IM_' then 'InlandMarine'
	when 'EXL' then 'ExcessLiability'
	when 'CU_' then 'CommercialUmbrella'
	when 'CA_' then 'CommercialAuto'
	when 'CDO' then 'DirectorsAndOffsCondos'
	when 'EPL' then 'EmploymentPracticesLiab'
	when 'HIO' then 'HoleInOne'
	when 'EC_' then 'EventCancellation'
	else 'N/A' 
	END END AS  InsuranceLine
	from DCCoverageStaging A
	inner join DCCoverageStaging B on A.SessionId=B.SessionId  and A.ObjectId=B.CoverageId and A.ObjectName='DC_Coverage'
	left join DCLineStaging L on L.sessionid = B.sessionid and B.objectid = L.lineID
	where B.ObjectName <> 'DC_Coverage'
	)
	
	---- Below query is used to pull the limit values from DCLimit tables if limits are tied to child coverage rather than parent, this is to cover odd scenarios in data
	
	select C.CoverageGuid, C.CoverageId, DLT.Type as LimitType, DLT.Value as LimitValue, C.InsuranceLine as Insuranceline,
	null as UnderlyingCompanyName, null as UnderlyingPolicyNumber, null as UnderlyingInsuranceLine, null as BlanketGroupNumber
	from DCLimitStaging DLT
	inner join PCoverage C on DLT.SessionId=C.SessionId and DLT.ObjectId=C.PCoverageId and  DLT.ObjectName='DC_Coverage'
	where DLT.Type is not null and DLT.Value is not null
	
	UNION    ---- By using UNION we are elimination duplicates coming from above and below queries
	
	---- Below query is used to pull the limit values from DCLimit tables if limits are tied to child coverage rather than parent, this is to cover odd scenarios in data
	
	select C.CoverageGuid, C.CoverageId, DLT.Type as LimitType, DLT.Value as LimitValue, C.InsuranceLine as Insuranceline,
	null as UnderlyingCompanyName, null as UnderlyingPolicyNumber, null as UnderlyingInsuranceLine, null as BlanketGroupNumber
	from DCLimitStaging DLT
	inner join PCoverage C on DLT.SessionId=C.SessionId and DLT.ObjectId=C.CoverageId and  DLT.ObjectName='DC_Coverage'
	where DLT.Type is not null and DLT.Value is not null
	
	union all
	
	select C.CoverageGUID, C.CoverageId, DLT.Type as LimitType, DLT.Value as LimitValue,  L.Type as InsuranceLine,
	EL.Description as UnderlyingCompanyName, EL.PolicyNumber as UnderlyingPolicyNumber, 'EmployersLiability' as UnderlyingInsuranceLine, null as BlanketGroupNumber
	from DCLineStaging L
	join WBCUPremiumDetailStage PD
	on PD.LineId = L.LineId
	join PCoverage C
	on L.SessionId=C.SessionId and C.ObjectId=PD.WBCUPremiumDetailId and C.ObjectName='WB_CU_PremiumDetail'
	join DCCUUmbrellaEmployersLiabilityStaging EL
	on c.SessionId=EL.SessionId and L.LineId=EL.LineId
	join DCLimitStaging DLT
	on EL.SessionId=DLT.SessionId
	and DLT.ObjectId=EL.CU_UmbrellaEmployersLiabilityId
	and DLT.ObjectName='DC_CU_UmbrellaEmployersLiability'
	where L.Type='CommercialUmbrella' and DLT.Type is not null and DLT.Value is not null
	
	union all
	
	select C.CoverageGUID, C.CoverageId, DLT.Type as LimitType, DLT.Value as LimitValue,  L.Type as InsuranceLine,
	GL.Description as UnderlyingCompanyName, GL.PolicyNumber as UnderlyingPolicyNumber, 
	case when DLT.Type in ('OccurrenceLimit','PersonalAdvertisingInjury','AggregateLimit','ProductsAggregateLimit') 
	then 'GeneralLiability'
	when DLT.Type in ('ProfessionalOccurrenceLimit','ProfessionalAggregateLimit')
	then 'CPPProfessionalLiability'
	when DLT.Type in ('AggregateNotApplicableForIowa','EachCommonCauseNotApplicableForIowa')
	then 'LiquorLiability'
	when DLT.Type in ('EBLAggregateLimit','EBLEachEmployeeLimit') 
	then 'CPPEmployeeBenefitsLiability'  
	end as UnderlyingInsuranceLine, null as BlanketGroupNumber
	from DCLineStaging L
	join WBCUPremiumDetailStage PD
	on PD.LineId = L.LineId
	join PCoverage C
	on PD.SessionId=C.SessionId and C.ObjectId=PD.WBCUPremiumDetailId and C.ObjectName='WB_CU_PremiumDetail'
	join DCCUUmbrellaGeneralLiabilityStaging GL
	on C.SessionId=GL.SessionId and L.LineId=GL.LineId
	join DCLimitStaging DLT
	on GL.SessionId=DLT.SessionId
	and DLT.ObjectId=GL.CU_UmbrellaGeneralLiabilityId
	and DLT.ObjectName='DC_CU_UmbrellaGeneralLiability'
	where L.Type='CommercialUmbrella' and DLT.Type is not null and DLT.Value is not null
	
	union all
	
	select C.CoverageGUID, C.CoverageId, DLT.Type as LimitType, DLT.Value as LimitValue,  L.Type as InsuranceLine,
	GL.Description as UnderlyingCompanyName, GL.PolicyNumber as UnderlyingPolicyNumber, 'GLOhioStopGapEmployersLiability' as UnderlyingInsuranceLine, null as BlanketGroupNumber
	from DCLineStaging L
	join WBCUPremiumDetailStage PD
	on PD.LineId = L.LineId
	join PCoverage C
	on PD.SessionId=C.SessionId and C.ObjectId=PD.WBCUPremiumDetailId and C.ObjectName='WB_CU_PremiumDetail'
	join DCCUUmbrellaGeneralLiabilityStaging GL
	on C.SessionId=GL.SessionId and L.LineId=GL.LineId
	join WBCUUmbrellaGeneralLiabilityStaging WGL
	on GL.SessionId=WGL.SessionId
	and GL.CU_UmbrellaGeneralLiabilityId = WGL.CU_UmbrellaGeneralLiabilityId
	join DCLimitStaging DLT
	on GL.SessionId=DLT.SessionId
	and DLT.ObjectId=WGL.WB_CU_UmbrellaGeneralLiabilityId
	and DLT.ObjectName='WB_CU_UmbrellaGeneralLiability'
	where L.Type='CommercialUmbrella' and DLT.Type is not null and DLT.Value is not null
	and DLT.Type in ( 'OHStopGapBodilyInjuryByAccidentEachAccident', 'OHStopGapBodilyInjuryByDiseaseEachEmployee', 'OHStopGapBodilyInjuryByDiseaseAggregate')
	
	union all
	
	select C.CoverageGUID, C.CoverageId, LTRIM(RTRIM(DLT.Type))+'- EACH OCCURRENCE' as LimitType, left(DLT.Value,charindex('/',DLT.Value)-1) +'000000' as LimitValue,  L.Type as InsuranceLine,
	BO.Description as UnderlyingCompanyName, BO.PolicyNumber as UnderlyingPolicyNumber, 'BusinessownersLiability' as UnderlyingInsuranceLine, null as BlanketGroupNumber
	from DCLineStaging L
	join WBCUPremiumDetailStage PD
	on PD.LineId = L.LineId
	join PCoverage C
	on PD.SessionId=C.SessionId and C.ObjectId=PD.WBCUPremiumDetailId and C.ObjectName='WB_CU_PremiumDetail'
	join DCCUUmbrellaBusinessOwnersStaging BO
	on BO.SessionId=L.SessionId and BO.LineId=L.LineId
	join DCLimitStaging DLT
	on BO.SessionId=DLT.SessionId
	and DLT.ObjectId=BO.CU_UmbrellaBusinessOwnersId
	and DLT.ObjectName='DC_CU_UmbrellaBusinessOwners'
	where L.Type='CommercialUmbrella' and DLT.Type is not null and DLT.Value is not null and DLT.Type = 'BusinessOwnersLiabilityLimit'
	
	
	
	union all
	
	select C.CoverageGUID, C.CoverageId, LTRIM(RTRIM(DLT.Type))+'- AGGREGATE' as LimitType, right(DLT.Value,len(DLT.Value)-charindex('/',DLT.Value)) +'000000' as LimitValue,  L.Type as InsuranceLine,
	BO.Description as UnderlyingCompanyName, BO.PolicyNumber as UnderlyingPolicyNumber, 'BusinessownersLiability' as UnderlyingInsuranceLine, null as BlanketGroupNumber
	from DCLineStaging L
	join WBCUPremiumDetailStage PD
	on PD.LineId = L.LineId
	join PCoverage C
	on PD.SessionId=C.SessionId and C.ObjectId=PD.WBCUPremiumDetailId and C.ObjectName='WB_CU_PremiumDetail'
	join DCCUUmbrellaBusinessOwnersStaging BO
	on BO.SessionId=L.SessionId and BO.LineId=L.LineId
	join DCLimitStaging DLT
	on BO.SessionId=DLT.SessionId
	and DLT.ObjectId=BO.CU_UmbrellaBusinessOwnersId
	and DLT.ObjectName='DC_CU_UmbrellaBusinessOwners'
	where L.Type='CommercialUmbrella' and DLT.Type is not null and DLT.Value is not null and DLT.Type = 'BusinessOwnersLiabilityLimit'
	
	
	union all
	
	select C.CoverageGUID, C.CoverageId, DLT.Type as LimitType, DLT.Value as LimitValue,  L.Type as InsuranceLine,
	BO.Description as UnderlyingCompanyName, BO.PolicyNumber as UnderlyingPolicyNumber, 'BOPEmployeeBenefitsLiability' as UnderlyingInsuranceLine, null as BlanketGroupNumber
	from DCLineStaging L
	join WBCUPremiumDetailStage PD
	on PD.LineId = L.LineId
	join PCoverage C
	on PD.SessionId=C.SessionId and C.ObjectId=PD.WBCUPremiumDetailId and C.ObjectName='WB_CU_PremiumDetail'
	join DCCUUmbrellaBusinessOwnersStaging BO
	on BO.SessionId=L.SessionId and BO.LineId=L.LineId
	join DCLimitStaging DLT
	on BO.SessionId=DLT.SessionId
	and DLT.ObjectId=BO.CU_UmbrellaBusinessOwnersId
	and DLT.ObjectName='DC_CU_UmbrellaBusinessOwners'
	where L.Type='CommercialUmbrella' and DLT.Type is not null and DLT.Value is not null and DLT.Type in ('EBLAggregateLimit','EBLEachEmployeeLimit')
	
	
	union all
	
	select C.CoverageGUID, C.CoverageId, DLT.Type as LimitType, DLT.Value as LimitValue,  L.Type as InsuranceLine,
	CA.Description as UnderlyingCompanyName, CA.PolicyNumber as UnderlyingPolicyNumber, 
	CASE WHEN DLT.Type in ('EBLAggregateLimit','EBLEachEmployeeLimit') 
	then 'CommercialAutoEmployeeBenefitsLiability' 
	when  DLT.Type in ( 
	'OHStopGapBodilyInjuryByAccidentEachAccident',
	'OHStopGapBodilyInjuryByDiseaseEachEmployee',
	'OHStopGapBodilyInjuryByDiseaseAggregate') then  'AutoOhioStopGapEmployersLiability'
	ELSE 'CommercialAutoLiability' END as UnderlyingInsuranceLine, 
	null as BlanketGroupNumber
	from DCLineStaging L
	join WBCUPremiumDetailStage PD
	on PD.LineId = L.LineId
	join PCoverage C
	on PD.SessionId=C.SessionId and C.ObjectId=PD.WBCUPremiumDetailId and C.ObjectName='WB_CU_PremiumDetail'
	join DCCUUmbrellaCommercialAutoStaging CA
	on CA.SessionId=L.SessionId and CA.LineId=L.LineId
	join DCLimitStaging DLT
	on CA.SessionId=DLT.SessionId
	and DLT.ObjectId=CA.CU_UmbrellaCommercialAutoId
	and DLT.ObjectName in ('DC_CU_UmbrellaCommercialAuto','WB_CU_UmbrellaCommercialAuto')
	where L.Type='CommercialUmbrella' and DLT.Type is not null and DLT.Value is not null
	
	union all
	
	select C.CoverageGUID, C.CoverageId, DLT.Type as LimitType, DLT.Value as LimitValue, L.Type as InsuranceLine,
	SMT.Description as UnderlyingCompanyName, SMT.PolicyNumber as UnderlyingPolicyNumber, 
	case when DLT.Type in ('Liability','GeneralAggregate','ProductsCompletedAggregate','PersonalAndAdvertisingInjury')
	then 'SMARTbusinessLiability' 
	when DLT.Type in ('ProfessionalLiability','ProfessionalLiabilityAggregateLimit')
	then 'SMARTProfessionalLiability'
	when DLT.Type in ('EBLAggregateLimit','EBLEachEmployeeLimit') 
	then 'SMARTEmployeeBenefitsLiability' 
	end as UnderlyingInsuranceLine, null as BlanketGroupNumber
	from DCLineStaging L
	join WBCUPremiumDetailStage PD
	on PD.LineId = L.LineId
	join PCoverage C
	on PD.SessionId=C.SessionId and C.ObjectId=PD.WBCUPremiumDetailId and C.ObjectName='WB_CU_PremiumDetail'
	join WBCUUmbrellaSMARTbusinessStage SMT
	on L.SessionId=SMT.SessionId and L.LineId=SMT.LineId
	join DCLimitStaging DLT
	on DLT.SessionId=SMT.SessionId
	and DLT.ObjectId=SMT.WBCUUmbrellaSMARTbusinessId
	and DLT.ObjectName='WB_CU_UmbrellaSMARTbusiness'
	where L.Type='CommercialUmbrella' and DLT.Type is not null and DLT.Value is not null
	
	union all
	
	select C.CoverageGUID, C.CoverageId, DLT.Type as LimitType, DLT.Value as LimitValue, L.Type as InsuranceLine,
	SBOP.Description as UnderlyingCompanyName, SBOP.PolicyNumber as UnderlyingPolicyNumber, 
	case when DLT.Type in ('BodilyInjuryByAccidentEachAccident', 'BodilyInjuryByDiseaseEachEmployee', 'BodilyInjuryByDiseaseAggregate') 
	then 'SBOPOhioStopGapEmployersLiability'
	when DLT.Type in ('ProfessionalLiabilityEachOccurrenceClaimLimit', 'ProfessionalLiabilityAggregate')
	then 'SBOPProfessionalLiability'
	when DLT.Type in ('PolicyAggregateLimit', 'PolicyPerOccurenceLimit', 'ProductsAggregateLimit', 'AggregateLimit')
	then  'SBOPGeneralLiability' 
	when DLT.Type in ('EBLAggregateLimit','EBLEachEmployeeLimit') 
	then 'SBOPEmployeeBenefitsLiability' end as UnderlyingInsuranceLine, 
	null as BlanketGroupNumber
	from DCLineStaging L
	join WBCUPremiumDetailStage PD
	on PD.LineId = L.LineId
	join PCoverage C
	on PD.SessionId=C.SessionId and C.ObjectId=PD.WBCUPremiumDetailId and C.ObjectName='WB_CU_PremiumDetail'
	join WBCUUmbrellaSBOPStage SBOP
	on L.SessionId=SBOP.SessionId and L.LineId=SBOP.LineId
	join DCLimitStaging DLT
	on DLT.SessionId=SBOP.SessionId
	and DLT.ObjectId=SBOP.WBCUUmbrellaSBOPId
	and DLT.ObjectName='WB_CU_UmbrellaSBOP'
	where L.Type='CommercialUmbrella' and DLT.Type is not null and DLT.Value is not null
	
	--CPP/SBOP Blanket
	Union all
	
	select c.CoverageGUID,c.CoverageId,cast(frm.value AS varchar(100))+dlt.Type as LimitType, dlt.Value,isnull(L.Type, C.InsuranceLine) as Insuranceline,
	null as UnderlyingCompanyName, null as UnderlyingPolicyNumber, null as UnderlyingInsuranceLine, convert(varchar(200), rg.Number)  as BlanketGroupNumber
	from DCLimitStaging DLT
	inner join DCCFRatingGroupStaging rg
	on DLT.ObjectId = rg.CF_RatingGroupId
	and DLT.ObjectName = 'DC_CF_RatingGroup'
	inner join DCFormStaging frm
	on frm.ObjectId = rg.CF_RatingGroupId
	and frm.ObjectName = 'DC_CF_RatingGroup'
	inner join DCCFRiskStaging r
	on rg.CF_RiskId = r.CF_RiskId
	inner join DCCFBuildingStage b
	on b.CFBuildingId = r.CF_BuildingId
	inner join DCLineStaging l
	on l.LineId = b.LineId
	inner join PCoverage c
	on c.ObjectId = l.LineId
	and c.ObjectName = 'DC_Line'
	where c.CoverageType = 'RatingGroup'
	and dlt.Type = 'StandardCalculated'
	
	--BusinessOwners Blanket
	Union all
	
	select c.CoverageGUID,c.CoverageId,bg.Type+dlt.Type as LimitType, dlt.Value,isnull(L.Type, C.InsuranceLine) as Insuranceline,
	null as UnderlyingCompanyName, null as UnderlyingPolicyNumber, null as UnderlyingInsuranceLine, convert(varchar(200), frm.Value) as BlanketGroupNumber
	from DCLimitStaging dlt
	inner join DCBPBlanketGroupStage bg
	on dlt.objectid = bg.BP_BlanketGroupId
	and dlt.ObjectName = 'DC_BP_BlanketGroup'
	inner join DCBPRiskStage r
	on bg.BP_RiskId = r.BPRiskId
	inner join DCLineStaging l
	on l.LineId = r.LineId
	inner join DCFormStaging frm
	on frm.ObjectName='DC_BP_BlanketGroup'
	and frm.ObjectId=bg.BP_BlanketGroupId
	inner join PCoverage c
	on c.ObjectId = l.LineId
	and c.ObjectName = 'DC_Line'
	where c.coverageType = 'Blanket'
	and dlt.Type = 'Standard'
	
	
	--Smart Ohio StopGap
	
	Union All
	
	 select  
	  C.CoverageGUID, C.CoverageId, DLT.Type as LimitType, DLT.Value as LimitValue,  L.Type as InsuranceLine,
	smt.Description as UnderlyingCompanyName, smt.PolicyNumber as UnderlyingPolicyNumber, 'SMARTOhioStopGapEmployersLiability' as UnderlyingInsuranceLine, null as BlanketGroupNumber
	from DCLineStaging L
	join WBCUPremiumDetailStage PD
	on PD.LineId = L.LineId
	join PCoverage C
	on PD.SessionId=C.SessionId and C.ObjectId=PD.WBCUPremiumDetailId and C.ObjectName='WB_CU_PremiumDetail'
	join WBCUUmbrellaSMARTbusinessStage SMT
	on L.SessionId=SMT.SessionId and L.LineId=SMT.LineId
	join DCLimitStaging DLT
	on DLT.SessionId=SMT.SessionId
	and DLT.ObjectId=SMT.WBCUUmbrellaSMARTbusinessId
	and DLT.ObjectName='WB_CU_UmbrellaSMARTbusiness'
	where L.Type='CommercialUmbrella' and DLT.Type is not null and DLT.Value is not null
	and DLT.Type in ( 
	'OHStopGapBodilyInjuryByAccidentEachAccident',
	'OHStopGapBodilyInjuryByDiseaseEachEmployee',
	'OHStopGapBodilyInjuryByDiseaseAggregate')
),
EXP_DataCollectSRC AS (
	SELECT
	CoverageGuid AS CoverageGuId,
	CoverageId,
	LimitType,
	LimitValue,
	-- *INF*: IIF(IN(LimitValue,'Actual Loss Sustained')=1, 'ActualLossSustained',LimitValue)
	IFF(LimitValue IN ('Actual Loss Sustained') = 1,
		'ActualLossSustained',
		LimitValue
	) AS o_LimitValue,
	InsuranceLine,
	UnderlyingCompanyName,
	UnderlyingPolicyKey,
	UnderlyingInsuranceLine,
	BlanketGroupNumber,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCLimitStaging
),
SQ_EDW AS (
	SELECT rc.CoverageType, rc.CoverageGUID, wpt.PremiumTransactionAKId, wpt.PremiumTransactionStageId, pt.PremiumTransactionID 
	FROM
	WorkPremiumTransaction wpt
	INNER JOIN PremiumTransaction pt ON   wpt.PremiumTransactionAKId=pt.PremiumTransactionAKID 
	AND pt.PremiumTransactionID NOT IN
	 (select WPTOL.PremiumTransactionID from dbo.WorkPremiumTransactionOffsetLineage WPTOL with (nolock) WHERE WPTOL.UpdateAttributeFlag = 1)
	INNER JOIN RatingCoverage rc ON  pt.RatingCoverageAKId=rc.RatingCoverageAKID AND pt.EffectiveDate=rc.EffectiveDate
),
JNR_Get_CvgType AS (SELECT
	EXP_DataCollectSRC.CoverageGuId, 
	EXP_DataCollectSRC.CoverageId, 
	EXP_DataCollectSRC.LimitType, 
	EXP_DataCollectSRC.o_LimitValue AS LimitValue, 
	EXP_DataCollectSRC.InsuranceLine, 
	EXP_DataCollectSRC.UnderlyingCompanyName, 
	EXP_DataCollectSRC.UnderlyingPolicyKey, 
	EXP_DataCollectSRC.UnderlyingInsuranceLine, 
	EXP_DataCollectSRC.BlanketGroupNumber, 
	EXP_DataCollectSRC.o_AuditId, 
	SQ_EDW.CoverageType, 
	SQ_EDW.CoverageGUID AS CoverageGUID_RatingCoverage, 
	SQ_EDW.PremiumTransactionAKId, 
	SQ_EDW.PremiumTransactionStageId, 
	SQ_EDW.PremiumTransactionID
	FROM EXP_DataCollectSRC
	INNER JOIN SQ_EDW
	ON SQ_EDW.CoverageGUID = EXP_DataCollectSRC.CoverageGuId AND SQ_EDW.PremiumTransactionStageId = EXP_DataCollectSRC.CoverageId
),
mplt_Load_Limits_IL_Layer_DCT AS (WITH
	SEQ_CoverageLimitID AS (
		CREATE SEQUENCE SEQ_CoverageLimitID
		START = 0
		INCREMENT = 1;
	),
	INPUT AS (
		
	),
	EXP_CoverageType_Extract AS (
		SELECT
		CoverageType AS i_CoverageType,
		LimitType,
		LimitValue,
		InsuranceLine,
		UnderlyingCompanyName,
		UnderlyingPolicyKey,
		UnderlyingInsuranceLine,
		BlanketGroupNumber,
		PremiumTransactionAKID AS PremiumTransactionAKId,
		PremiumTransactionID,
		-- *INF*: IIF( NOT ISNULL(UnderlyingInsuranceLine), 'UnderlyingUmbrella', i_CoverageType)
		IFF(UnderlyingInsuranceLine IS NOT NULL,
			'UnderlyingUmbrella',
			i_CoverageType
		) AS o_CoverageType_LKP,
		-- *INF*: IIF( NOT ISNULL(UnderlyingInsuranceLine), UnderlyingInsuranceLine, InsuranceLine)
		IFF(UnderlyingInsuranceLine IS NOT NULL,
			UnderlyingInsuranceLine,
			InsuranceLine
		) AS o_InsuranceLine_LKP,
		AuditId
		FROM INPUT
	),
	LKP_Valid_Limits AS (
		SELECT
		StandardLimitType,
		LimitLevel,
		InsuranceLine,
		CoverageType,
		LimitType
		FROM (
			SELECT 
				StandardLimitType,
				LimitLevel,
				InsuranceLine,
				CoverageType,
				LimitType
			FROM SupLimitType
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLine,CoverageType,LimitType ORDER BY StandardLimitType) = 1
	),
	FIL_UnnecessaryLimits AS (
		SELECT
		LKP_Valid_Limits.StandardLimitType, 
		LKP_Valid_Limits.LimitLevel, 
		EXP_CoverageType_Extract.PremiumTransactionAKId, 
		EXP_CoverageType_Extract.i_CoverageType AS CoverageType, 
		EXP_CoverageType_Extract.LimitType, 
		EXP_CoverageType_Extract.LimitValue, 
		EXP_CoverageType_Extract.InsuranceLine, 
		EXP_CoverageType_Extract.UnderlyingCompanyName, 
		EXP_CoverageType_Extract.UnderlyingPolicyKey, 
		EXP_CoverageType_Extract.UnderlyingInsuranceLine, 
		EXP_CoverageType_Extract.BlanketGroupNumber, 
		EXP_CoverageType_Extract.PremiumTransactionID, 
		EXP_CoverageType_Extract.AuditId
		FROM EXP_CoverageType_Extract
		LEFT JOIN LKP_Valid_Limits
		ON LKP_Valid_Limits.InsuranceLine = EXP_CoverageType_Extract.o_InsuranceLine_LKP AND LKP_Valid_Limits.CoverageType = EXP_CoverageType_Extract.o_CoverageType_LKP AND LKP_Valid_Limits.LimitType = EXP_CoverageType_Extract.LimitType
		WHERE Not ISNULL(LimitLevel)
	),
	EXP_Default_Values AS (
		SELECT
		StandardLimitType AS i_StandardLimitType,
		PremiumTransactionAKId AS i_PremiumTransactionAKId,
		CoverageType AS i_CoverageType,
		LimitType AS i_LimitType,
		LimitValue AS i_LimitValue,
		InsuranceLine AS i_InsuranceLine,
		UnderlyingCompanyName,
		UnderlyingPolicyKey,
		UnderlyingInsuranceLine,
		BlanketGroupNumber AS i_BlanketGroupNumber,
		PremiumTransactionID,
		AuditId,
		-- *INF*: IIF(ISNULL(i_BlanketGroupNumber),'',LTRIM(RTRIM(i_BlanketGroupNumber)) || ' ')
		IFF(i_BlanketGroupNumber IS NULL,
			'',
			LTRIM(RTRIM(i_BlanketGroupNumber
				)
			) || ' '
		) AS v_BlanketNumber,
		-- *INF*: LTRIM(RTRIM(i_CoverageType))
		LTRIM(RTRIM(i_CoverageType
			)
		) AS v_CoverageType,
		-- *INF*: LTRIM(RTRIM(i_LimitType))
		LTRIM(RTRIM(i_LimitType
			)
		) AS v_LimitType,
		-- *INF*: IIF(ISNULL(i_StandardLimitType), v_LimitType, i_StandardLimitType)
		IFF(i_StandardLimitType IS NULL,
			v_LimitType,
			i_StandardLimitType
		) AS v_CoverageLimitType,
		-- *INF*: REPLACESTR(0,LTRIM(RTRIM(i_LimitValue)), '$',CHR(44),'')
		REGEXP_REPLACE(LTRIM(RTRIM(i_LimitValue
			)
		),'$',CHR(44
		),'','i') AS v_CoverageLimitValue,
		-- *INF*: LTRIM(RTRIM(i_InsuranceLine))
		LTRIM(RTRIM(i_InsuranceLine
			)
		) AS v_InsuranceLine,
		v_InsuranceLine AS o_InsuranceLine,
		v_BlanketNumber || v_CoverageLimitType AS o_CoverageLimitType,
		-- *INF*: IIF(v_InsuranceLine='WorkersCompensation' AND IN(v_CoverageLimitType,'EachAccident','EachEmployeeDisease','Policy'),v_CoverageLimitValue || '000',v_CoverageLimitValue)
		IFF(v_InsuranceLine = 'WorkersCompensation' 
			AND v_CoverageLimitType IN ('EachAccident','EachEmployeeDisease','Policy'),
			v_CoverageLimitValue || '000',
			v_CoverageLimitValue
		) AS o_CoverageLimitValue,
		v_CoverageType AS o_CoverageType,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(UnderlyingInsuranceLine),
		-- 0,
		-- NOT ISNULL(v_CoverageLimitType) AND  NOT ISNULL(v_CoverageLimitValue),
		-- 1,
		-- 2)
		DECODE(TRUE,
			UnderlyingInsuranceLine IS NULL, 0,
			v_CoverageLimitType IS NULL 
			AND v_CoverageLimitValue IS NOT NOT NULL, 1,
			2
		) AS o_UnderlyingFlag
		FROM FIL_UnnecessaryLimits
	),
	RTR_EquipmentBreakdown AS (
		SELECT
		UnderlyingCompanyName,
		UnderlyingPolicyKey,
		UnderlyingInsuranceLine,
		PremiumTransactionID,
		o_InsuranceLine AS InsuranceLine,
		i_PremiumTransactionAKId AS PremiumTransactionAKId,
		o_CoverageLimitType AS CoverageLimitType,
		o_CoverageLimitValue AS CoverageLimitValue,
		o_CoverageType AS CoverageType,
		o_UnderlyingFlag AS UnderlyingFlag,
		AuditId
		FROM EXP_Default_Values
	),
	RTR_EquipmentBreakdown_EquipmentBreakdown AS (SELECT * FROM RTR_EquipmentBreakdown WHERE IN(CoverageType,'EquipmentBreakdown','EquipBreakdown') AND UnderlyingFlag=0),
	RTR_EquipmentBreakdown_NonEquipmentBreakdown AS (SELECT * FROM RTR_EquipmentBreakdown WHERE NOT IN(CoverageType,'EquipmentBreakdown','EquipBreakdown') AND UnderlyingFlag=0),
	RTR_EquipmentBreakdown_Underlying AS (SELECT * FROM RTR_EquipmentBreakdown WHERE UnderlyingFlag=1),
	AGG_Underlying AS (
		SELECT
		PremiumTransactionID,
		CoverageLimitType,
		CoverageLimitValue,
		UnderlyingCompanyName,
		UnderlyingPolicyKey AS UnderLyingPolicyKey,
		UnderlyingInsuranceLine,
		AuditId
		FROM RTR_EquipmentBreakdown_Underlying
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID, CoverageLimitType, UnderlyingCompanyName, UnderLyingPolicyKey, UnderlyingInsuranceLine ORDER BY NULL) = 1
	),
	AGG_EquipmentBreakdown AS (
		SELECT
		PremiumTransactionAKId,
		CoverageLimitType,
		CoverageLimitValue AS i_CoverageLimitValue,
		-- *INF*: TO_CHAR(SUM(TO_DECIMAL(i_CoverageLimitValue)))
		TO_CHAR(SUM(CAST(i_CoverageLimitValue AS FLOAT)
			)
		) AS o_CoverageLimitValue,
		AuditId
		FROM RTR_EquipmentBreakdown_EquipmentBreakdown
		GROUP BY PremiumTransactionAKId, CoverageLimitType
	),
	EXP_Underlying AS (
		SELECT
		PremiumTransactionID AS i_PremiumTransactionID,
		CoverageLimitType AS i_CoverageLimitType,
		CoverageLimitValue AS i_CoverageLimitValue,
		UnderlyingCompanyName AS i_UnderlyingCompanyName,
		UnderLyingPolicyKey AS i_UnderLyingPolicyKey,
		UnderlyingInsuranceLine AS i_UnderlyingInsuranceLine,
		'1' AS o_CurrentSnapshotFlag,
		AuditId AS AuditID,
		-- *INF*: TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
		TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'
		) AS o_EffectiveDate,
		-- *INF*: TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
		TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'
		) AS o_ExpirationDate,
		@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
		CURRENT_TIMESTAMP AS o_CreatedDate,
		CURRENT_TIMESTAMP AS o_ModifiedDate,
		i_PremiumTransactionID AS o_PremiumTransactionId,
		-- *INF*: IIF(ISNULL(i_UnderlyingCompanyName),'N/A',i_UnderlyingCompanyName)
		IFF(i_UnderlyingCompanyName IS NULL,
			'N/A',
			i_UnderlyingCompanyName
		) AS o_UnderlyingInsuranceCompanyName,
		-- *INF*: IIF(ISNULL(i_UnderLyingPolicyKey),'N/A',i_UnderLyingPolicyKey)
		IFF(i_UnderLyingPolicyKey IS NULL,
			'N/A',
			i_UnderLyingPolicyKey
		) AS o_UnderlyingPolicyKey,
		-- *INF*: IIF(ISNULL(i_UnderlyingInsuranceLine),'N/A',i_UnderlyingInsuranceLine)
		IFF(i_UnderlyingInsuranceLine IS NULL,
			'N/A',
			i_UnderlyingInsuranceLine
		) AS o_UnderlyingPolicyType,
		i_CoverageLimitValue AS o_UnderlyingPolicyLimit,
		i_CoverageLimitType AS o_UnderlyingPolicyLimitType
		FROM AGG_Underlying
	),
	Union_EquipmentBreakdown AS (
		SELECT CoverageLimitType, PremiumTransactionAKId, o_CoverageLimitValue AS CoverageLimitValue, AuditId
		FROM AGG_EquipmentBreakdown
		UNION
		SELECT CoverageLimitType, PremiumTransactionAKId, CoverageLimitValue, AuditId
		FROM RTR_EquipmentBreakdown_NonEquipmentBreakdown
	),
	LKP_CoverageDetailUnderlyingPolicy AS (
		SELECT
		CoverageDetailUnderlyingPolicyId,
		PremiumTransactionId,
		UnderlyingInsuranceCompanyName,
		UnderlyingPolicyKey,
		UnderlyingPolicyType,
		UnderlyingPolicyLimitType
		FROM (
			SELECT CoverageDetailUnderlyingPolicyId AS CoverageDetailUnderlyingPolicyId,
				CoverageDetailUnderlyingPolicy.PremiumTransactionId AS PremiumTransactionId,
				UnderlyingInsuranceCompanyName AS UnderlyingInsuranceCompanyName,
				UnderlyingPolicyKey AS UnderlyingPolicyKey,
				UnderlyingPolicyType AS UnderlyingPolicyType,
				UnderlyingPolicyLimitType AS UnderlyingPolicyLimitType
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailUnderlyingPolicy
			JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT ON PT.PremiumTransactionId = CoverageDetailUnderlyingPolicy.PremiumTransactionId
			@{pipeline().parameters.PTFILTERFORCDUP}
			WHERE CoverageDetailUnderlyingPolicy.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
			ORDER BY CoverageDetailUnderlyingPolicy.PremiumTransactionId,
				UnderlyingInsuranceCompanyName,
				UnderlyingPolicyKey,
				UnderlyingPolicyType,
				UnderlyingPolicyLimitType
				---
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionId,UnderlyingInsuranceCompanyName,UnderlyingPolicyKey,UnderlyingPolicyType,UnderlyingPolicyLimitType ORDER BY CoverageDetailUnderlyingPolicyId) = 1
	),
	AGG_Type_Value AS (
		SELECT
		CoverageLimitType,
		CoverageLimitValue,
		AuditId
		FROM Union_EquipmentBreakdown
		QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageLimitType, CoverageLimitValue ORDER BY NULL) = 1
	),
	SRT_by_Type_Value_CoverageLimitBridge AS (
		SELECT
		PremiumTransactionAKId, 
		CoverageLimitType, 
		CoverageLimitValue
		FROM Union_EquipmentBreakdown
		ORDER BY CoverageLimitType ASC, CoverageLimitValue ASC
	),
	RTR_Underlying_Insert AS (
		SELECT
		LKP_CoverageDetailUnderlyingPolicy.CoverageDetailUnderlyingPolicyId,
		EXP_Underlying.o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
		EXP_Underlying.AuditID,
		EXP_Underlying.o_EffectiveDate AS EffectiveDate,
		EXP_Underlying.o_ExpirationDate AS ExpirationDate,
		EXP_Underlying.o_SourceSystemId AS SourceSystemId,
		EXP_Underlying.o_CreatedDate AS CreatedDate,
		EXP_Underlying.o_ModifiedDate AS ModifiedDate,
		EXP_Underlying.o_PremiumTransactionId AS PremiumTransactionId,
		EXP_Underlying.o_UnderlyingInsuranceCompanyName AS UnderlyingInsuranceCompanyName,
		EXP_Underlying.o_UnderlyingPolicyKey AS UnderlyingPolicyKey,
		EXP_Underlying.o_UnderlyingPolicyType AS UnderlyingPolicyType,
		EXP_Underlying.o_UnderlyingPolicyLimit AS UnderlyingPolicyLimit,
		EXP_Underlying.o_UnderlyingPolicyLimitType AS UnderlyingPolicyLimitType
		FROM EXP_Underlying
		LEFT JOIN LKP_CoverageDetailUnderlyingPolicy
		ON LKP_CoverageDetailUnderlyingPolicy.PremiumTransactionId = EXP_Underlying.o_PremiumTransactionId AND LKP_CoverageDetailUnderlyingPolicy.UnderlyingInsuranceCompanyName = EXP_Underlying.o_UnderlyingInsuranceCompanyName AND LKP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyKey = EXP_Underlying.o_UnderlyingPolicyKey AND LKP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyType = EXP_Underlying.o_UnderlyingPolicyType AND LKP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyLimitType = EXP_Underlying.o_UnderlyingPolicyLimitType
	),
	RTR_Underlying_Insert_Insert AS (SELECT * FROM RTR_Underlying_Insert WHERE ISNULL(CoverageDetailUnderlyingPolicyId)),
	LKP_CoverageLimit_CoverageLimitID AS (
		SELECT
		CoverageLimitId,
		CoverageLimitType,
		CoverageLimitValue
		FROM (
			SELECT 
				CoverageLimitId,
				CoverageLimitType,
				CoverageLimitValue
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageLimit
			WHERE SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageLimitType,CoverageLimitValue ORDER BY CoverageLimitId DESC) = 1
	),
	OUTPUT_CoverageDetailUnderlyingPolicy_Insert AS (
		SELECT
		CurrentSnapshotFlag, 
		AuditID, 
		EffectiveDate, 
		ExpirationDate, 
		SourceSystemId, 
		CreatedDate, 
		ModifiedDate, 
		PremiumTransactionId, 
		UnderlyingInsuranceCompanyName, 
		UnderlyingPolicyKey, 
		UnderlyingPolicyType, 
		UnderlyingPolicyLimit, 
		UnderlyingPolicyLimitType
		FROM RTR_Underlying_Insert_Insert
	),
	EXP_Set_CoverageLimitID AS (
		SELECT
		LKP_CoverageLimit_CoverageLimitID.CoverageLimitId AS lkp_CoverageLimitId,
		SEQ_CoverageLimitID.NEXTVAL AS i_NEXTVAL,
		AGG_Type_Value.CoverageLimitType,
		AGG_Type_Value.CoverageLimitValue,
		-- *INF*: IIF(ISNULL(lkp_CoverageLimitId),i_NEXTVAL,lkp_CoverageLimitId)
		IFF(lkp_CoverageLimitId IS NULL,
			i_NEXTVAL,
			lkp_CoverageLimitId
		) AS CoverageLimitId,
		AGG_Type_Value.AuditId
		FROM AGG_Type_Value
		LEFT JOIN LKP_CoverageLimit_CoverageLimitID
		ON LKP_CoverageLimit_CoverageLimitID.CoverageLimitType = AGG_Type_Value.CoverageLimitType AND LKP_CoverageLimit_CoverageLimitID.CoverageLimitValue = AGG_Type_Value.CoverageLimitValue
	),
	FIL_Insert_CoverageLimit AS (
		SELECT
		lkp_CoverageLimitId, 
		CoverageLimitId, 
		CoverageLimitType, 
		CoverageLimitValue, 
		AuditId
		FROM EXP_Set_CoverageLimitID
		WHERE ISNULL(lkp_CoverageLimitId)
	),
	SRT_by_Type_Value_CoverageLimit AS (
		SELECT
		CoverageLimitType, 
		CoverageLimitValue, 
		CoverageLimitId, 
		AuditId
		FROM EXP_Set_CoverageLimitID
		ORDER BY CoverageLimitType ASC, CoverageLimitValue ASC
	),
	JNR_CoverageLimit_CoverageLimitBridge AS (SELECT
		SRT_by_Type_Value_CoverageLimit.CoverageLimitType AS CoverageLimitType_CoverageLimit, 
		SRT_by_Type_Value_CoverageLimit.CoverageLimitValue AS i_CoverageLimitValue_CoverageLimit, 
		SRT_by_Type_Value_CoverageLimit.CoverageLimitId, 
		SRT_by_Type_Value_CoverageLimit.AuditId, 
		SRT_by_Type_Value_CoverageLimitBridge.PremiumTransactionAKId, 
		SRT_by_Type_Value_CoverageLimitBridge.CoverageLimitType AS i_CoverageIdCoverageLimitType_Bridge, 
		SRT_by_Type_Value_CoverageLimitBridge.CoverageLimitValue AS i_CoverageIdCoverageLimitValue_Bridge
		FROM SRT_by_Type_Value_CoverageLimit
		INNER JOIN SRT_by_Type_Value_CoverageLimitBridge
		ON SRT_by_Type_Value_CoverageLimitBridge.CoverageLimitType = SRT_by_Type_Value_CoverageLimit.CoverageLimitType AND SRT_by_Type_Value_CoverageLimitBridge.CoverageLimitValue = SRT_by_Type_Value_CoverageLimit.CoverageLimitValue
	),
	EXP_Set_MetaData_CoverageLimit AS (
		SELECT
		CoverageLimitId,
		CoverageLimitType,
		CoverageLimitValue,
		AuditId AS AuditID,
		@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
		SYSDATE AS o_CreatedDate
		FROM FIL_Insert_CoverageLimit
	),
	LKP_CoverageLimitBridge AS (
		SELECT
		CoverageLimitBridgeId,
		PremiumTransactionAKId,
		CoverageLimitId
		FROM (
			SELECT CLB.CoverageLimitBridgeId as CoverageLimitBridgeId, 
			CLB.PremiumTransactionAKId as PremiumTransactionAKId, 
			CLB.CoverageLimitId as CoverageLimitId 
			FROM dbo.CoverageLimitBridge CLB
			INNER JOIN dbo.WorkPremiumTransaction WPT ON WPT.PremiumTransactionAKId = CLB.PremiumTransactionAKId
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId,CoverageLimitId ORDER BY CoverageLimitBridgeId DESC) = 1
	),
	OUTPUT_CoverageLimit_Insert AS (
		SELECT
		CoverageLimitId, 
		AuditID, 
		o_SourceSystemID AS SourceSystemID, 
		o_CreatedDate AS CreatedDate, 
		CoverageLimitType, 
		CoverageLimitValue
		FROM EXP_Set_MetaData_CoverageLimit
	),
	FIL_Insert_CoverageLimitBridge AS (
		SELECT
		LKP_CoverageLimitBridge.CoverageLimitBridgeId AS i_CoverageLimitBridgeId, 
		JNR_CoverageLimit_CoverageLimitBridge.PremiumTransactionAKId, 
		JNR_CoverageLimit_CoverageLimitBridge.CoverageLimitId, 
		JNR_CoverageLimit_CoverageLimitBridge.AuditId
		FROM JNR_CoverageLimit_CoverageLimitBridge
		LEFT JOIN LKP_CoverageLimitBridge
		ON LKP_CoverageLimitBridge.PremiumTransactionAKId = JNR_CoverageLimit_CoverageLimitBridge.PremiumTransactionAKId AND LKP_CoverageLimitBridge.CoverageLimitId = JNR_CoverageLimit_CoverageLimitBridge.CoverageLimitId
		WHERE ISNULL(i_CoverageLimitBridgeId) AND PremiumTransactionAKId<>-1
	),
	AGG_Group_Count AS (
		SELECT
		PremiumTransactionAKId,
		CoverageLimitId,
		-- *INF*: COUNT(1)
		COUNT(1
		) AS o_CoverageLimitCount,
		AuditId
		FROM FIL_Insert_CoverageLimitBridge
		GROUP BY PremiumTransactionAKId, CoverageLimitId
	),
	EXP_Set_MetaData_CoverageLimitBridge AS (
		SELECT
		PremiumTransactionAKId,
		CoverageLimitId,
		o_CoverageLimitCount AS CoverageLimitCount,
		AuditId AS AuditID,
		@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
		SYSDATE AS o_CreatedDate,
		'N/A' AS o_CoverageLimitControl
		FROM AGG_Group_Count
	),
	OUTPUT_CoverageLimitBridge_Insert AS (
		SELECT
		AuditID, 
		o_SourceSystemID AS SourceSystemID, 
		o_CreatedDate AS CreatedDate, 
		PremiumTransactionAKId, 
		CoverageLimitId, 
		CoverageLimitCount AS CoverageLimitIDCount, 
		o_CoverageLimitControl AS CoverageLimitControl
		FROM EXP_Set_MetaData_CoverageLimitBridge
	),
),
UPD_Insert_CoverageLimit AS (
	SELECT
	CoverageLimitId1 AS CoverageLimitId, 
	AuditID3 AS AuditID, 
	SourceSystemID2 AS SourceSystemID, 
	CreatedDate2 AS CreatedDate, 
	CoverageLimitType, 
	CoverageLimitValue
	FROM mplt_Load_Limits_IL_Layer_DCT
),
CoverageLimit AS (
	SET IDENTITY_INSERT @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageLimit  ON
	INSERT @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageLimit(CoverageLimitId ,AuditID,SourceSystemID,CreatedDate,CoverageLimitType,CoverageLimitValue) 
	SELECT S.CoverageLimitId,S.AuditID,S.SourceSystemID, S.CreatedDate,S.CoverageLimitType, S.CoverageLimitValue
	FROM UPD_Insert_CoverageLimit S
),
CoverageDetailUnderlyingPolicy AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailUnderlyingPolicy
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, PremiumTransactionId, UnderlyingInsuranceCompanyName, UnderlyingPolicyKey, UnderlyingPolicyType, UnderlyingPolicyLimit, UnderlyingPolicyLimitType)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AuditID1 AS AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	PremiumTransactionId1 AS PREMIUMTRANSACTIONID, 
	UNDERLYINGINSURANCECOMPANYNAME, 
	UnderlyingPolicyKey1 AS UNDERLYINGPOLICYKEY, 
	UNDERLYINGPOLICYTYPE, 
	UNDERLYINGPOLICYLIMIT, 
	UNDERLYINGPOLICYLIMITTYPE
	FROM mplt_Load_Limits_IL_Layer_DCT
),
CoverageLimitBridge AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageLimitBridge
	(AuditID, SourceSystemID, CreatedDate, PremiumTransactionAKId, CoverageLimitId, CoverageLimitIDCount, CoverageLimitControl)
	SELECT 
	AuditID2 AS AUDITID, 
	SourceSystemID1 AS SOURCESYSTEMID, 
	CreatedDate1 AS CREATEDDATE, 
	PremiumTransactionAKId1 AS PREMIUMTRANSACTIONAKID, 
	COVERAGELIMITID, 
	COVERAGELIMITIDCOUNT, 
	COVERAGELIMITCONTROL
	FROM mplt_Load_Limits_IL_Layer_DCT
),
SQ_CoverageDetailUnderlyingPolicy_Offsets AS (
	SELECT cdup.CurrentSnapshotFlag, 
	cdup.EffectiveDate, 
	cdup.ExpirationDate, 
	cdup.UnderlyingInsuranceCompanyName, 
	cdup.UnderlyingPolicyKey, 
	cdup.UnderlyingPolicyType, 
	cdup.UnderlyingPolicyLimit, 
	cdup.UnderlyingPolicyLimitType,
	 wptol.PremiumTransactionID 
	FROM
	 CoverageDetailUnderlyingPolicy cdup
	inner join  WorkPremiumTransactionOffsetLineage  wptol on
	 cdup.PremiumTransactionID=wptol.PreviousPremiumTransactionID
	 and wptol.updateattributeflag=1
	inner join PremiumTransaction PT on
	 wptol.PremiumTransactionID = PT.PremiumTransactionID 
	 and PT.OffsetOnsetCode = 'Offset'
),
EXP_CoverageDetailUnderlyingPolicy AS (
	SELECT
	CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	EffectiveDate,
	ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	UnderlyingInsuranceCompanyName,
	UnderlyingPolicyKey,
	UnderlyingPolicyType,
	UnderlyingPolicyLimit,
	UnderlyingPolicyLimitType,
	wrk_PremiumTransactionID
	FROM SQ_CoverageDetailUnderlyingPolicy_Offsets
),
LKP_CoverageDetailUnderLyingPolicy_Offset AS (
	SELECT
	CoverageDetailUnderlyingPolicyId,
	PremiumTransactionId,
	UnderlyingInsuranceCompanyName,
	UnderlyingPolicyKey,
	UnderlyingPolicyType,
	UnderlyingPolicyLimitType
	FROM (
		SELECT CDUP.CoverageDetailUnderlyingPolicyId as CoverageDetailUnderlyingPolicyId, 
		CDUP.PremiumTransactionId as PremiumTransactionId, 
		CDUP.UnderlyingInsuranceCompanyName as UnderlyingInsuranceCompanyName, 
		CDUP.UnderlyingPolicyKey as UnderlyingPolicyKey, 
		CDUP.UnderlyingPolicyType as UnderlyingPolicyType, 
		CDUP.UnderlyingPolicyLimitType as UnderlyingPolicyLimitType 
		FROM CoverageDetailUnderlyingPolicy CDUP
		INNER JOIN
		(SELECT DISTINCT PreviousPremiumTransactionID as PremiumTransactionId FROM  dbo. WorkPremiumTransactionOffsetLineage 
		UNION 
		SELECT DISTINCT PremiumTransactionId FROM  dbo. WorkPremiumTransactionOffsetLineage wpt) A
		ON CDUP.PremiumTransactionId = A.PremiumTransactionId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionId,UnderlyingInsuranceCompanyName,UnderlyingPolicyKey,UnderlyingPolicyType,UnderlyingPolicyLimitType ORDER BY CoverageDetailUnderlyingPolicyId) = 1
),
FIL_Offsets_Insert_CoverageDetailUnderLyingPolicy AS (
	SELECT
	LKP_CoverageDetailUnderLyingPolicy_Offset.CoverageDetailUnderlyingPolicyId, 
	EXP_CoverageDetailUnderlyingPolicy.CurrentSnapshotFlag, 
	EXP_CoverageDetailUnderlyingPolicy.AuditID, 
	EXP_CoverageDetailUnderlyingPolicy.EffectiveDate, 
	EXP_CoverageDetailUnderlyingPolicy.ExpirationDate, 
	EXP_CoverageDetailUnderlyingPolicy.SourceSystemId, 
	EXP_CoverageDetailUnderlyingPolicy.CreatedDate, 
	EXP_CoverageDetailUnderlyingPolicy.ModifiedDate, 
	EXP_CoverageDetailUnderlyingPolicy.UnderlyingInsuranceCompanyName, 
	EXP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyKey, 
	EXP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyType, 
	EXP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyLimit, 
	EXP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyLimitType, 
	EXP_CoverageDetailUnderlyingPolicy.wrk_PremiumTransactionID
	FROM EXP_CoverageDetailUnderlyingPolicy
	LEFT JOIN LKP_CoverageDetailUnderLyingPolicy_Offset
	ON LKP_CoverageDetailUnderLyingPolicy_Offset.PremiumTransactionId = EXP_CoverageDetailUnderlyingPolicy.wrk_PremiumTransactionID AND LKP_CoverageDetailUnderLyingPolicy_Offset.UnderlyingInsuranceCompanyName = EXP_CoverageDetailUnderlyingPolicy.UnderlyingInsuranceCompanyName AND LKP_CoverageDetailUnderLyingPolicy_Offset.UnderlyingPolicyKey = EXP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyKey AND LKP_CoverageDetailUnderLyingPolicy_Offset.UnderlyingPolicyType = EXP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyType AND LKP_CoverageDetailUnderLyingPolicy_Offset.UnderlyingPolicyLimitType = EXP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyLimitType
	WHERE ISNULL(CoverageDetailUnderlyingPolicyId)
),
TGT_CoverageDetailUnderlyingPolicy_Offsets_Insert AS (
	INSERT INTO CoverageDetailUnderlyingPolicy
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, PremiumTransactionId, UnderlyingInsuranceCompanyName, UnderlyingPolicyKey, UnderlyingPolicyType, UnderlyingPolicyLimit, UnderlyingPolicyLimitType)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	wrk_PremiumTransactionID AS PREMIUMTRANSACTIONID, 
	UNDERLYINGINSURANCECOMPANYNAME, 
	UNDERLYINGPOLICYKEY, 
	UNDERLYINGPOLICYTYPE, 
	UNDERLYINGPOLICYLIMIT, 
	UNDERLYINGPOLICYLIMITTYPE
	FROM FIL_Offsets_Insert_CoverageDetailUnderLyingPolicy
),
SQ_CoverageDetailUnderlyingPolicy_Deprecated AS (
	SELECT cdup.CurrentSnapshotFlag, 
	cdup.EffectiveDate, 
	cdup.ExpirationDate, 
	cdup.UnderlyingInsuranceCompanyName, 
	cdup.UnderlyingPolicyKey, 
	cdup.UnderlyingPolicyType, 
	cdup.UnderlyingPolicyLimit, 
	cdup.UnderlyingPolicyLimitType,
	 wptol.PremiumTransactionID 
	FROM
	 CoverageDetailUnderlyingPolicy cdup
	inner join  WorkPremiumTransactionOffsetLineage  wptol on
	 cdup.PremiumTransactionID=wptol.PreviousPremiumTransactionID
	 and wptol.updateattributeflag=1
	inner join PremiumTransaction PT on
	 wptol.PremiumTransactionID = PT.PremiumTransactionID 
	 and PT.OffsetOnsetCode = 'Deprecated'
),
EXP_CoverageDetailUnderlyingPolicy_Deprecated AS (
	SELECT
	CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	EffectiveDate,
	ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	UnderlyingInsuranceCompanyName,
	UnderlyingPolicyKey,
	UnderlyingPolicyType,
	UnderlyingPolicyLimit,
	UnderlyingPolicyLimitType,
	wrk_PremiumTransactionID
	FROM SQ_CoverageDetailUnderlyingPolicy_Deprecated
),
LKP_CoverageDetailUnderLyingPolicy_Deprecated AS (
	SELECT
	CoverageDetailUnderlyingPolicyId,
	PremiumTransactionId,
	UnderlyingInsuranceCompanyName,
	UnderlyingPolicyKey,
	UnderlyingPolicyType,
	UnderlyingPolicyLimitType
	FROM (
		SELECT CDUP.CoverageDetailUnderlyingPolicyId as CoverageDetailUnderlyingPolicyId, 
		CDUP.PremiumTransactionId as PremiumTransactionId, 
		CDUP.UnderlyingInsuranceCompanyName as UnderlyingInsuranceCompanyName, 
		CDUP.UnderlyingPolicyKey as UnderlyingPolicyKey, 
		CDUP.UnderlyingPolicyType as UnderlyingPolicyType, 
		CDUP.UnderlyingPolicyLimitType as UnderlyingPolicyLimitType 
		FROM CoverageDetailUnderlyingPolicy CDUP
		INNER JOIN
		(SELECT DISTINCT PreviousPremiumTransactionID as PremiumTransactionId FROM  dbo. WorkPremiumTransactionOffsetLineage 
		UNION 
		SELECT DISTINCT PremiumTransactionId FROM  dbo. WorkPremiumTransactionOffsetLineage wpt) A
		ON CDUP.PremiumTransactionId = A.PremiumTransactionId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionId,UnderlyingInsuranceCompanyName,UnderlyingPolicyKey,UnderlyingPolicyType,UnderlyingPolicyLimitType ORDER BY CoverageDetailUnderlyingPolicyId) = 1
),
FIL_Deprecated_Insert_CoverageDetailUnderLyingPolicy AS (
	SELECT
	LKP_CoverageDetailUnderLyingPolicy_Deprecated.CoverageDetailUnderlyingPolicyId, 
	EXP_CoverageDetailUnderlyingPolicy_Deprecated.CurrentSnapshotFlag, 
	EXP_CoverageDetailUnderlyingPolicy_Deprecated.AuditID, 
	EXP_CoverageDetailUnderlyingPolicy_Deprecated.EffectiveDate, 
	EXP_CoverageDetailUnderlyingPolicy_Deprecated.ExpirationDate, 
	EXP_CoverageDetailUnderlyingPolicy_Deprecated.SourceSystemId, 
	EXP_CoverageDetailUnderlyingPolicy_Deprecated.CreatedDate, 
	EXP_CoverageDetailUnderlyingPolicy_Deprecated.ModifiedDate, 
	EXP_CoverageDetailUnderlyingPolicy_Deprecated.UnderlyingInsuranceCompanyName, 
	EXP_CoverageDetailUnderlyingPolicy_Deprecated.UnderlyingPolicyKey, 
	EXP_CoverageDetailUnderlyingPolicy_Deprecated.UnderlyingPolicyType, 
	EXP_CoverageDetailUnderlyingPolicy_Deprecated.UnderlyingPolicyLimit, 
	EXP_CoverageDetailUnderlyingPolicy_Deprecated.UnderlyingPolicyLimitType, 
	EXP_CoverageDetailUnderlyingPolicy_Deprecated.wrk_PremiumTransactionID
	FROM EXP_CoverageDetailUnderlyingPolicy_Deprecated
	LEFT JOIN LKP_CoverageDetailUnderLyingPolicy_Deprecated
	ON LKP_CoverageDetailUnderLyingPolicy_Deprecated.PremiumTransactionId = EXP_CoverageDetailUnderlyingPolicy_Deprecated.wrk_PremiumTransactionID AND LKP_CoverageDetailUnderLyingPolicy_Deprecated.UnderlyingInsuranceCompanyName = EXP_CoverageDetailUnderlyingPolicy_Deprecated.UnderlyingInsuranceCompanyName AND LKP_CoverageDetailUnderLyingPolicy_Deprecated.UnderlyingPolicyKey = EXP_CoverageDetailUnderlyingPolicy_Deprecated.UnderlyingPolicyKey AND LKP_CoverageDetailUnderLyingPolicy_Deprecated.UnderlyingPolicyType = EXP_CoverageDetailUnderlyingPolicy_Deprecated.UnderlyingPolicyType AND LKP_CoverageDetailUnderLyingPolicy_Deprecated.UnderlyingPolicyLimitType = EXP_CoverageDetailUnderlyingPolicy_Deprecated.UnderlyingPolicyLimitType
	WHERE ISNULL(CoverageDetailUnderlyingPolicyId)
),
TGT_CoverageDetailUnderlyingPolicy_Deprecated_Insert AS (
	INSERT INTO CoverageDetailUnderlyingPolicy
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, PremiumTransactionId, UnderlyingInsuranceCompanyName, UnderlyingPolicyKey, UnderlyingPolicyType, UnderlyingPolicyLimit, UnderlyingPolicyLimitType)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	wrk_PremiumTransactionID AS PREMIUMTRANSACTIONID, 
	UNDERLYINGINSURANCECOMPANYNAME, 
	UNDERLYINGPOLICYKEY, 
	UNDERLYINGPOLICYTYPE, 
	UNDERLYINGPOLICYLIMIT, 
	UNDERLYINGPOLICYLIMITTYPE
	FROM FIL_Deprecated_Insert_CoverageDetailUnderLyingPolicy
),
SQ_CoverageLimitBridge_Insert_Offsets AS (
	SELECT CLB.CoverageLimitId, CLB.CoverageLimitIDCount, CLB.CoverageLimitControl, WPTOL.PremiumTransactionAKID 
	FROM
	 WorkPremiumTransactionOffsetLineage WPTOL
	 inner join CoverageLimitBridge CLB on
	 WPTOL.PreviousPremiumTransactionAKID = CLB.PremiumTransactionAKId
	 inner join PremiumTransaction PT on
	 WPTOL.PremiumTransactionID = PT.PremiumTransactionID and PT.OffsetOnsetCode = 'Offset'
	 where
	 WPTOL.UpdateAttributeFlag = 1
),
EXP_CoverageLimitBridge_PassThrough_Insert_Offsets AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	SYSDATE AS CreatedDate,
	CoverageLimitId,
	CoverageLimitIDCount,
	CoverageLimitControl,
	PremiumTransactionAKID AS Offset_PremiumTransactionAKID
	FROM SQ_CoverageLimitBridge_Insert_Offsets
),
LKP_CoverageLimitBridge_Offset AS (
	SELECT
	CoverageLimitBridgeId,
	PremiumTransactionAKId,
	CoverageLimitId
	FROM (
		SELECT CLB.CoverageLimitBridgeId as CoverageLimitBridgeId, 
		CLB.PremiumTransactionAKId as PremiumTransactionAKId, 
		CLB.CoverageLimitId as CoverageLimitId 
		FROM CoverageLimitBridge CLB
		INNER JOIN 
		(SELECT DISTINCT PreviousPremiumTransactionAKID as PremiumTransactionAKID FROM  dbo. WorkPremiumTransactionOffsetLineage  
		UNION 
		SELECT DISTINCT PremiumTransactionAKID FROM  dbo. WorkPremiumTransactionOffsetLineage  ) A
		ON CLB.PremiumTransactionAKId = A.PremiumTransactionAKID
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId,CoverageLimitId ORDER BY CoverageLimitBridgeId) = 1
),
FIL_Offset_Insert_CoverageLimitBridge AS (
	SELECT
	LKP_CoverageLimitBridge_Offset.CoverageLimitBridgeId, 
	EXP_CoverageLimitBridge_PassThrough_Insert_Offsets.AuditID, 
	EXP_CoverageLimitBridge_PassThrough_Insert_Offsets.SourceSystemID, 
	EXP_CoverageLimitBridge_PassThrough_Insert_Offsets.CreatedDate, 
	EXP_CoverageLimitBridge_PassThrough_Insert_Offsets.CoverageLimitId, 
	EXP_CoverageLimitBridge_PassThrough_Insert_Offsets.CoverageLimitIDCount, 
	EXP_CoverageLimitBridge_PassThrough_Insert_Offsets.CoverageLimitControl, 
	EXP_CoverageLimitBridge_PassThrough_Insert_Offsets.Offset_PremiumTransactionAKID
	FROM EXP_CoverageLimitBridge_PassThrough_Insert_Offsets
	LEFT JOIN LKP_CoverageLimitBridge_Offset
	ON LKP_CoverageLimitBridge_Offset.PremiumTransactionAKId = EXP_CoverageLimitBridge_PassThrough_Insert_Offsets.Offset_PremiumTransactionAKID AND LKP_CoverageLimitBridge_Offset.CoverageLimitId = EXP_CoverageLimitBridge_PassThrough_Insert_Offsets.CoverageLimitId
	WHERE ISNULL(CoverageLimitBridgeId)
),
TGT_CoverageLimitBridge_Insert_Offsets AS (
	INSERT INTO CoverageLimitBridge
	(AuditID, SourceSystemID, CreatedDate, PremiumTransactionAKId, CoverageLimitId, CoverageLimitIDCount, CoverageLimitControl)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	Offset_PremiumTransactionAKID AS PREMIUMTRANSACTIONAKID, 
	COVERAGELIMITID, 
	COVERAGELIMITIDCOUNT, 
	COVERAGELIMITCONTROL
	FROM FIL_Offset_Insert_CoverageLimitBridge
),
SQ_CoverageLimitBridge_Insert_Deprecated AS (
	SELECT CLB.CoverageLimitId, CLB.CoverageLimitIDCount, CLB.CoverageLimitControl, WPTOL.PremiumTransactionAKID 
	FROM
	 WorkPremiumTransactionOffsetLineage WPTOL
	 inner join CoverageLimitBridge CLB on
	 WPTOL.PreviousPremiumTransactionAKID = CLB.PremiumTransactionAKId
	 inner join PremiumTransaction PT on
	 WPTOL.PremiumTransactionID = PT.PremiumTransactionID and PT.OffsetOnsetCode = 'Deprecated'
	 where
	 WPTOL.UpdateAttributeFlag = 1
),
EXP_CoverageLimitBridge_PassThrough_Insert_Deprecated AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	SYSDATE AS CreatedDate,
	CoverageLimitId,
	CoverageLimitIDCount,
	CoverageLimitControl,
	PremiumTransactionAKID AS Offset_PremiumTransactionAKID
	FROM SQ_CoverageLimitBridge_Insert_Deprecated
),
LKP_CoverageLimitBridge_Deprecated AS (
	SELECT
	CoverageLimitBridgeId,
	PremiumTransactionAKId,
	CoverageLimitId
	FROM (
		SELECT CLB.CoverageLimitBridgeId as CoverageLimitBridgeId, 
		CLB.PremiumTransactionAKId as PremiumTransactionAKId, 
		CLB.CoverageLimitId as CoverageLimitId 
		FROM CoverageLimitBridge CLB
		INNER JOIN 
		(SELECT DISTINCT PreviousPremiumTransactionAKID as PremiumTransactionAKID FROM  dbo. WorkPremiumTransactionOffsetLineage  
		UNION 
		SELECT DISTINCT PremiumTransactionAKID FROM  dbo. WorkPremiumTransactionOffsetLineage  ) A
		on CLB.PremiumTransactionAKId = A.PremiumTransactionAKID
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId,CoverageLimitId ORDER BY CoverageLimitBridgeId) = 1
),
FIL_Deprecated_Insert_CoverageLimitBridge AS (
	SELECT
	LKP_CoverageLimitBridge_Deprecated.CoverageLimitBridgeId, 
	EXP_CoverageLimitBridge_PassThrough_Insert_Deprecated.AuditID, 
	EXP_CoverageLimitBridge_PassThrough_Insert_Deprecated.SourceSystemID, 
	EXP_CoverageLimitBridge_PassThrough_Insert_Deprecated.CreatedDate, 
	EXP_CoverageLimitBridge_PassThrough_Insert_Deprecated.CoverageLimitId, 
	EXP_CoverageLimitBridge_PassThrough_Insert_Deprecated.CoverageLimitIDCount, 
	EXP_CoverageLimitBridge_PassThrough_Insert_Deprecated.CoverageLimitControl, 
	EXP_CoverageLimitBridge_PassThrough_Insert_Deprecated.Offset_PremiumTransactionAKID
	FROM EXP_CoverageLimitBridge_PassThrough_Insert_Deprecated
	LEFT JOIN LKP_CoverageLimitBridge_Deprecated
	ON LKP_CoverageLimitBridge_Deprecated.PremiumTransactionAKId = EXP_CoverageLimitBridge_PassThrough_Insert_Deprecated.Offset_PremiumTransactionAKID AND LKP_CoverageLimitBridge_Deprecated.CoverageLimitId = EXP_CoverageLimitBridge_PassThrough_Insert_Deprecated.CoverageLimitId
	WHERE ISNULL(CoverageLimitBridgeId)
),
TGT_CoverageLimitBridge_Insert_Deprecated AS (
	INSERT INTO CoverageLimitBridge
	(AuditID, SourceSystemID, CreatedDate, PremiumTransactionAKId, CoverageLimitId, CoverageLimitIDCount, CoverageLimitControl)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	Offset_PremiumTransactionAKID AS PREMIUMTRANSACTIONAKID, 
	COVERAGELIMITID, 
	COVERAGELIMITIDCOUNT, 
	COVERAGELIMITCONTROL
	FROM FIL_Deprecated_Insert_CoverageLimitBridge
),