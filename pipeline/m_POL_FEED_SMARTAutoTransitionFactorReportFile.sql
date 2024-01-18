WITH
SQ_CA_Stage_Data AS (
	Declare @Date as date
	set @Date = convert(date,EOMONTH(getdate(),@{pipeline().parameters.NO_OF_MONTHS}));
	
	WITH CTE_SessionID AS	
	(select distinct T.SessionId as SessionId, L.LineId,T.TransactionDate as TransactionDate, T.Charge as PolicyPremium,T.IssuedUserName as CreatedUser from @{pipeline().parameters.SOURCE_TABLE_OWNER}.archDCTransactionStaging T with (NOLOCK)  
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.archDCLineStaging L with (NOLOCK) on L.SessionId=T.SessionId and L.AuditId=T.AuditId 
								and T.State='Committed' and T.Type in ('Renew','Reissue','Rewrite') and L.Type='CommercialAuto' and T.ExtractDate > @Date ) 
	
	Select Distinct
	P.PolicyNumber
	,W.PolicyVersionFormatted
	,T.TransactionDate
	,T.CreatedUser as TransactionCommittedUserId
	,isnull(CL.CompositeRating,0) as CompositeRating
	,M.Type as ModifierType
	,M.Scope as ModifierScope
	,M.Value as ModifierValue
	,CAL.Number as LocationNumber
	,T.PolicyPremium as PolicyPremium
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.archDCPolicyStaging P with (nolock)
	inner join CTE_SessionID T with (nolock) on T.SessionId=P.SessionId 
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchWBPolicyStaging W with (nolock) on T.SessionId=W.SessionId and P.PolicyId=W.PolicyId and P.AuditId=W.AuditId 
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchDCModifierStaging M with (nolock) on M.SessionId=T.SessionId and M.ExtractDate > @Date
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchDCCALocationStaging CAL with (nolock) on T.SessionId=CAL.SessionId and CAL.CA_LocationId=M.ObjectId and M.ObjectName='DC_CA_Location' and M.AuditId=CAL.AuditId
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchDCCALineStaging CL with (nolock) on CL.LineId=T.LineId and CL.SessionId=T.SessionId 
	where 
	M.ObjectName='DC_CA_Location' and 
	M.Type='ScheduledMod'
	and M.Scope in ('AutoTransitionLiability','AutoTransitionPhysicalDamage','Liability','PhysicalDamage')
	and (M.Value<>'0' and M.Value<>'1' and M.Value is not null)
	and P.ExtractDate > @Date 
	@{pipeline().parameters.WHERE_CLAUSE_CA}
	ORDER BY P.PolicyNumber, W.PolicyVersionFormatted, CAL.Number
),
EXP_CA_CleanModifiersData AS (
	SELECT
	PolicyNumber AS in_PolicyNumber,
	PolicyVersionFormatted AS in_PolicyVersionFormatted,
	TransactionDate AS in_TransactionDate,
	TransactionCommittedUserId AS in_TransactionCommittedUserId,
	CompositeRating AS in_CompositeRating,
	ModifierType AS in_ModifierType,
	ModifierScope AS in_ModifierScope,
	ModifierValue AS in_ModifierValue,
	LocationNumber AS in_LocationNumber,
	PolicyPremium AS in_PolicyPremium,
	-- *INF*: LTRIM(RTRIM(in_PolicyNumber)) ||  LPAD(LTRIM(RTRIM(in_PolicyVersionFormatted)),2,'0')
	LTRIM(RTRIM(in_PolicyNumber)) || LPAD(LTRIM(RTRIM(in_PolicyVersionFormatted)), 2, '0') AS o_PolicyKey,
	-- *INF*: IIF(ISNULL(in_PolicyPremium),0,in_PolicyPremium)
	IFF(in_PolicyPremium IS NULL, 0, in_PolicyPremium) AS o_PolicyPremium,
	-- *INF*: IIF(ISNULL(in_LocationNumber)=1,'0001',LPAD(LTRIM(RTRIM(TO_CHAR(in_LocationNumber))),4,'0'))
	IFF(
	    in_LocationNumber IS NULL = 1, '0001',
	    LPAD(LTRIM(RTRIM(TO_CHAR(in_LocationNumber))), 4, '0')
	) AS o_LocationNumber,
	-- *INF*: TO_CHAR(in_TransactionDate,'MM-DD-YYYY')
	TO_CHAR(in_TransactionDate, 'MM-DD-YYYY') AS o_TransactionDate,
	-- *INF*: IIF( LTRIM(RTRIM(in_TransactionCommittedUserId))='' OR ISNULL(LTRIM(RTRIM(in_TransactionCommittedUserId))),'N/A',
	-- IIF(INSTR(in_TransactionCommittedUserId,'\')<>0,
	-- SUBSTR(in_TransactionCommittedUserId, INSTR(in_TransactionCommittedUserId,'\')+1, LENGTH(in_TransactionCommittedUserId)),
	-- in_TransactionCommittedUserId
	-- )
	-- )
	IFF(
	    LTRIM(RTRIM(in_TransactionCommittedUserId)) = ''
	    or LTRIM(RTRIM(in_TransactionCommittedUserId)) IS NULL,
	    'N/A',
	    IFF(
	        REGEXP_INSTR(in_TransactionCommittedUserId, '\') <> 0,
	        SUBSTR(in_TransactionCommittedUserId, REGEXP_INSTR(in_TransactionCommittedUserId, '\') + 1, LENGTH(in_TransactionCommittedUserId)),
	        in_TransactionCommittedUserId
	    )
	) AS o_TransactionCommittedUserId,
	-- *INF*: IIF(in_CompositeRating='T','1','0')
	IFF(in_CompositeRating = 'T', '1', '0') AS o_CompositeRating,
	-- *INF*: LOWER(in_ModifierType)
	LOWER(in_ModifierType) AS o_ModifierType,
	-- *INF*: LOWER(in_ModifierScope)
	LOWER(in_ModifierScope) AS o_ModifierScope,
	-- *INF*: IIF(IN(in_ModifierScope ,'AutoTransitionLiability','AutoTransitionPhysicalDamage')=1, (1+((TO_DECIMAL(in_ModifierValue))/100)), TO_DECIMAL(in_ModifierValue)
	-- )
	IFF(
	    in_ModifierScope IN ('AutoTransitionLiability','AutoTransitionPhysicalDamage') = 1,
	    (1 + ((CAST(in_ModifierValue AS FLOAT)) / 100)),
	    CAST(in_ModifierValue AS FLOAT)
	) AS o_ModifierValue
	FROM SQ_CA_Stage_Data
),
AGG_Get_Mod_ForPolicy_Location AS (
	SELECT
	o_PolicyKey AS PolicyKey,
	o_LocationNumber AS LocationNumber,
	o_PolicyPremium AS in_PolicyPremium,
	o_TransactionDate AS in_TransactionDate,
	o_TransactionCommittedUserId AS in_TransactionCommittedUserId,
	o_CompositeRating AS in_CompositeRating,
	o_ModifierType AS in_ModifierType,
	o_ModifierScope AS in_ModifierScope,
	o_ModifierValue AS in_ModifierValue,
	-- *INF*: MAX(in_TransactionDate)
	MAX(in_TransactionDate) AS o_TransactionDate,
	-- *INF*: MAX(in_TransactionCommittedUserId)
	MAX(in_TransactionCommittedUserId) AS o_TransactionCommittedUserId,
	-- *INF*: MAX(in_CompositeRating)
	MAX(in_CompositeRating) AS o_CompositeRating,
	-- *INF*: MAX(in_PolicyPremium)
	MAX(in_PolicyPremium) AS o_PolicyPremium,
	-- *INF*: MAX(
	-- IIF(in_ModifierType='scheduledmod' and in_ModifierScope='physicaldamage',in_ModifierValue
	-- )
	-- )
	MAX(
	    IFF(
	        in_ModifierType = 'scheduledmod' and in_ModifierScope = 'physicaldamage',
	        in_ModifierValue
	    )) AS o_PhysicalDamageModifier,
	-- *INF*: MAX(
	-- IIF(in_ModifierType='scheduledmod' and in_ModifierScope='autotransitionphysicaldamage',in_ModifierValue
	-- )
	-- )
	MAX(
	    IFF(
	        in_ModifierType = 'scheduledmod' and in_ModifierScope = 'autotransitionphysicaldamage',
	        in_ModifierValue
	    )) AS o_TransitionPhysicalDamageModifier,
	-- *INF*: MAX(
	-- IIF(in_ModifierType='scheduledmod' and in_ModifierScope='liability',in_ModifierValue
	-- )
	-- )
	MAX(
	    IFF(
	        in_ModifierType = 'scheduledmod' and in_ModifierScope = 'liability',
	        in_ModifierValue
	    )) AS o_LiabilityModifier,
	-- *INF*: MAX(
	-- IIF(in_ModifierType='scheduledmod' and in_ModifierScope='autotransitionliability',in_ModifierValue
	-- )
	-- )
	MAX(
	    IFF(
	        in_ModifierType = 'scheduledmod' and in_ModifierScope = 'autotransitionliability',
	        in_ModifierValue
	    )) AS o_TransitionLiabilityModifier
	FROM EXP_CA_CleanModifiersData
	GROUP BY PolicyKey, LocationNumber
),
FIL_RemoveCARowsWithoutTransitionModifier AS (
	SELECT
	PolicyKey, 
	LocationNumber, 
	o_TransactionDate AS TransactionDate, 
	o_TransactionCommittedUserId AS TransactionCommittedUserId, 
	o_CompositeRating AS CompositeRating, 
	o_PolicyPremium AS PolicyPremium, 
	o_PhysicalDamageModifier AS PhysicalDamageModifier, 
	o_TransitionPhysicalDamageModifier AS TransitionPhysicalDamageModifier, 
	o_LiabilityModifier AS LiabilityModifier, 
	o_TransitionLiabilityModifier AS TransitionLiabilityModifier
	FROM AGG_Get_Mod_ForPolicy_Location
	WHERE IIF(
(NOT ISNULL(TransitionPhysicalDamageModifier) AND TransitionPhysicalDamageModifier<>1 AND TransitionPhysicalDamageModifier<>0) 
OR
(NOT ISNULL(TransitionLiabilityModifier) AND TransitionLiabilityModifier<>1 AND TransitionLiabilityModifier<>0), TRUE, FALSE
)
),
SQ_CA_RPT_EDM_Data AS (
	Declare @Date as date
	set @Date = convert(date,EOMONTH(getdate(),@{pipeline().parameters.NO_OF_MONTHS})) 
	
	Select  distinct
	 P.pol_key as PolicyKey
	,PO.PolicyOfferingDescription as PolicyOffering
	,P.pol_exp_date as PolicyExpirationDate
	,RL.LocationUnitNumber
	,PTRM.ScheduleModifiedFactor
	,PTRM.ExperienceModifiedFactor
	,SUM(PT.PremiumTransactionAmount) OVER (PARTITION BY P.pol_ak_id, PC.InsuranceLine) as DirectWrittenPremiumbyInsuranceLine
	,P.sup_bus_class_code_id
	,P.AgencyAKId
	,P.state_of_domicile_code
	,P.ProgramAKId
	,P.UnderwritingAssociateAKId
	,P.pol_ak_id as PolicyAkId
	,PC.InsuranceLine
	,CC.cust_num as CustomerNumber
	,CC.name as FirstNamedInsured
	,SPC.StrategicProfitCenterAbbreviation as StrategicProfitCenterAbbreviation
	from @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P with (nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL with (nolock) on RL.PolicyAKID=P.pol_ak_id and RL.CurrentSnapshotFlag=1 and P.crrnt_snpsht_flag=1 and P.source_sys_id='DCT'
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC with (nolock) on RL.RiskLocationAKID=PC.RiskLocationAKID and PC.CurrentSnapshotFlag=1
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC with (nolock) on RC.PolicyCoverageAKID=PC.PolicyCoverageAKID 
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT with (nolock) on PT.RatingCoverageAKId=RC.RatingCoverageAKID and PT.EffectiveDate=RC.EffectiveDate
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransactionRatingModifier PTRM with (nolock) on PTRM.PremiumTransactionID=PT.PremiumTransactionID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering PO with (nolock) on PO.PolicyOfferingAKId=P.PolicyOfferingAKId and PO.CurrentSnapshotFlag=1
	inner join VWContractCustomer CC with (nolock) on CC.contract_cust_ak_id=P.contract_cust_ak_id and CC.crrnt_snpsht_flag=1 
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter SPC with (nolock) on SPC.StrategicProfitCenterAKId=P.StrategicProfitCenterAKId 
	where PT.PremiumTransactionCode in ('Renew','Reissue','Rewrite','Restart')
	and PT.PremiumTransactionAmount <> 0
	and  (PO.PolicyOfferingAbbreviation='CPP' OR (PO.PolicyOfferingAbbreviation='SMART' and PC.InsuranceLine='CommercialAuto') )
	and PT.CreatedDate > @Date
	@{pipeline().parameters.WHERE_CLAUSE_RPT_EDM_CA}
	order by  P.pol_key, RL.LocationUnitNumber
),
JNR_CPP_SMART_Auto_TransMod AS (SELECT
	SQ_CA_RPT_EDM_Data.PolicyKey, 
	SQ_CA_RPT_EDM_Data.PolicyOffering, 
	SQ_CA_RPT_EDM_Data.PolicyExpirationDate, 
	SQ_CA_RPT_EDM_Data.LocationUnitNumber, 
	SQ_CA_RPT_EDM_Data.ScheduleModifiedFactor, 
	SQ_CA_RPT_EDM_Data.ExperienceModifiedFactor, 
	SQ_CA_RPT_EDM_Data.DirectWrittenPremiumbyInsuranceLine, 
	SQ_CA_RPT_EDM_Data.CustomerNumber, 
	SQ_CA_RPT_EDM_Data.FirstNamedInsured, 
	SQ_CA_RPT_EDM_Data.sup_bus_class_code_id, 
	SQ_CA_RPT_EDM_Data.AgencyAKId, 
	SQ_CA_RPT_EDM_Data.state_of_domicile_code, 
	SQ_CA_RPT_EDM_Data.ProgramAKId, 
	SQ_CA_RPT_EDM_Data.UnderwritingAssociateAKId, 
	SQ_CA_RPT_EDM_Data.PolicyAkId, 
	SQ_CA_RPT_EDM_Data.InsuranceLine, 
	SQ_CA_RPT_EDM_Data.StrategicProfitCenterAbbreviation, 
	FIL_RemoveCARowsWithoutTransitionModifier.PolicyKey AS PolicyKey_Stage, 
	FIL_RemoveCARowsWithoutTransitionModifier.LocationNumber AS LocationNumber_Stage, 
	FIL_RemoveCARowsWithoutTransitionModifier.TransactionDate, 
	FIL_RemoveCARowsWithoutTransitionModifier.TransactionCommittedUserId, 
	FIL_RemoveCARowsWithoutTransitionModifier.CompositeRating, 
	FIL_RemoveCARowsWithoutTransitionModifier.PhysicalDamageModifier, 
	FIL_RemoveCARowsWithoutTransitionModifier.TransitionPhysicalDamageModifier, 
	FIL_RemoveCARowsWithoutTransitionModifier.LiabilityModifier, 
	FIL_RemoveCARowsWithoutTransitionModifier.TransitionLiabilityModifier, 
	FIL_RemoveCARowsWithoutTransitionModifier.PolicyPremium
	FROM SQ_CA_RPT_EDM_Data
	INNER JOIN FIL_RemoveCARowsWithoutTransitionModifier
	ON FIL_RemoveCARowsWithoutTransitionModifier.PolicyKey = SQ_CA_RPT_EDM_Data.PolicyKey AND FIL_RemoveCARowsWithoutTransitionModifier.LocationNumber = SQ_CA_RPT_EDM_Data.LocationUnitNumber
),
EXP_Derive_AutoModifiers AS (
	SELECT
	FirstNamedInsured,
	PolicyKey,
	PolicyOffering,
	LocationUnitNumber,
	ExperienceModifiedFactor,
	DirectWrittenPremiumbyInsuranceLine,
	sup_bus_class_code_id,
	AgencyAKId,
	state_of_domicile_code,
	ProgramAKId,
	UnderwritingAssociateAKId,
	PolicyAkId,
	InsuranceLine,
	TransactionDate,
	TransactionCommittedUserId,
	CompositeRating,
	PolicyPremium AS PolicyDirectWrittenPremium,
	StrategicProfitCenterAbbreviation,
	CustomerNumber AS in_CustomerNumber,
	PolicyExpirationDate AS in_PolicyExpirationDate,
	ScheduleModifiedFactor AS in_ScheduleModifiedFactor,
	PhysicalDamageModifier AS in_PhysicalDamageModifier,
	TransitionPhysicalDamageModifier AS in_TransitionPhysicalDamageModifier,
	LiabilityModifier AS in_LiabilityModifier,
	TransitionLiabilityModifier AS in_TransitionLiabilityModifier,
	-- *INF*: IIF(ISNULL(in_CustomerNumber),'N/A', CHR(39)||in_CustomerNumber) ||CHR(39)
	IFF(in_CustomerNumber IS NULL, 'N/A', CHR(39) || in_CustomerNumber) || CHR(39) AS o_CustomerNumber,
	-- *INF*: TO_CHAR(in_PolicyExpirationDate,'MM-DD-YYYY')
	TO_CHAR(in_PolicyExpirationDate, 'MM-DD-YYYY') AS o_PolicyExpirationDate,
	-- *INF*: IIF(InsuranceLine='CommercialAuto', 0,in_ScheduleModifiedFactor)
	IFF(InsuranceLine = 'CommercialAuto', 0, in_ScheduleModifiedFactor) AS o_ScheduleModifiedFactor,
	0 AS o_SMARTTransition,
	-- *INF*: IIF(InsuranceLine='CommercialAuto' 
	-- AND NOT ISNULL(in_TransitionPhysicalDamageModifier) AND NOT ISNULL(in_PhysicalDamageModifier)
	-- AND in_TransitionPhysicalDamageModifier<>0
	-- ,(1+(in_PhysicalDamageModifier-in_TransitionPhysicalDamageModifier)), 0)
	IFF(
	    InsuranceLine = 'CommercialAuto'
	    and in_TransitionPhysicalDamageModifier IS NULL
	    and in_PhysicalDamageModifier IS NULL
	    and in_TransitionPhysicalDamageModifier NOT NOT <> 0,
	    (1 + (in_PhysicalDamageModifier - in_TransitionPhysicalDamageModifier)),
	    0
	) AS o_AutoPhysicalDamage,
	-- *INF*: IIF(InsuranceLine='CommercialAuto', in_TransitionPhysicalDamageModifier, 0)
	-- 
	IFF(InsuranceLine = 'CommercialAuto', in_TransitionPhysicalDamageModifier, 0) AS o_AutoPhysicalDamageTransition,
	-- *INF*: IIF(InsuranceLine='CommercialAuto' 
	-- AND NOT ISNULL(in_TransitionLiabilityModifier) AND NOT ISNULL(in_LiabilityModifier) AND in_TransitionLiabilityModifier<>0
	-- ,(1+(in_LiabilityModifier-in_TransitionLiabilityModifier)), 0)
	IFF(
	    InsuranceLine = 'CommercialAuto'
	    and in_TransitionLiabilityModifier IS NULL
	    and in_LiabilityModifier IS NULL
	    and in_TransitionLiabilityModifier NOT NOT <> 0,
	    (1 + (in_LiabilityModifier - in_TransitionLiabilityModifier)),
	    0
	) AS o_AutoLiability,
	-- *INF*: IIF(InsuranceLine='CommercialAuto', in_TransitionLiabilityModifier, 0)
	IFF(InsuranceLine = 'CommercialAuto', in_TransitionLiabilityModifier, 0) AS o_AutoLiabilityTransition
	FROM JNR_CPP_SMART_Auto_TransMod
),
SQ_SMART_Stage_Data AS (
	Declare @Date as date
	set @Date = convert(date,EOMONTH(getdate(),@{pipeline().parameters.NO_OF_MONTHS})); 
	
	WITH CTE_SessionID AS (select distinct T.SessionId as SessionId, T.TransactionDate as TransactionDate, T.Charge as PolicyPremium,T.IssuedUserName as CreatedUser 
							 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.archDCTransactionStaging T with (NOLOCK) 
						     inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.archDCLineStaging L with (NOLOCK) on L.SessionId=T.SessionId and L.AuditId=T.AuditId 
							 and T.State='Committed' and T.Type in ('Renew','Reissue','Rewrite') and L.Type='BusinessOwners' and T.ExtractDate > @Date) 
	
	Select distinct P.PolicyNumber as PolicyNumber
	,W.PolicyVersionFormatted as PolicyVersionFormatted
	,T.PolicyPremium as PolicyPremium
	,BPL.Number as LocationNumber
	,T.TransactionDate as TransactionDate
	,T.CreatedUser as TransactionCommittedUserId
	,(1+(CAST(M.Value as NUMERIC)/100) ) as SMARTTransitionModifier
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.archDCPolicyStaging P with (nolock)
	inner join CTE_SessionID T with (Nolock) on T.SessionId=P.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchWBPolicyStaging W with (nolock) on P.PolicyId=W.PolicyId and P.SessionId=W.SessionId and P.AuditId=W.AuditId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchDCModifierStaging M with (nolock) on M.SessionId=T.SessionId 
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchDCBPLocationStage BPL WITH (NOLOCK) on T.SessionId=BPL.SessionId and BPL.BPLocationId=M.ObjectId and M.ObjectName='DC_BP_Location' and M.AuditId=BPL.AuditId
	where 
	M.ObjectName='DC_BP_Location' 
	and M.Type='LocationIRPMTransitionValue'
	and M.Scope is null
	--and (M.Value<>'0' and M.Value<>'1' and M.Value is not null)
	and P.ExtractDate > @Date and M.ExtractDate > @Date 
	@{pipeline().parameters.WHERE_CLAUSE_SMART}
	ORDER BY P.PolicyNumber, W.PolicyVersionFormatted, BPL.Number
),
EXP_SMART_CleanTransitionModifierdata AS (
	SELECT
	PolicyNumber AS in_PolicyNumber,
	PolicyVersionFormatted AS in_PolicyVersionFormatted,
	PolicyPremium AS in_PolicyPremium,
	LocationNumber AS in_LocationNumber,
	TransactionDate AS in_TransactionDate,
	TransactionCommittedUserId AS in_TransactionCommittedUserId,
	SMARTTransitionModifier AS in_SMARTTransitionModifier,
	-- *INF*: LTRIM(RTRIM(in_PolicyNumber)) ||  LPAD(LTRIM(RTRIM(in_PolicyVersionFormatted)),2,'0')
	LTRIM(RTRIM(in_PolicyNumber)) || LPAD(LTRIM(RTRIM(in_PolicyVersionFormatted)), 2, '0') AS o_PolicyKey,
	-- *INF*: IIF(ISNULL(in_PolicyPremium),0,in_PolicyPremium)
	IFF(in_PolicyPremium IS NULL, 0, in_PolicyPremium) AS o_PolicyPremium,
	-- *INF*: TO_CHAR(in_TransactionDate,'MM-DD-YYYY')
	TO_CHAR(in_TransactionDate, 'MM-DD-YYYY') AS o_TransactionDate,
	-- *INF*: IIF( LTRIM(RTRIM(in_TransactionCommittedUserId))='' OR ISNULL(LTRIM(RTRIM(in_TransactionCommittedUserId))),'N/A',
	-- IIF(INSTR(in_TransactionCommittedUserId,'\')<>0,
	-- SUBSTR(in_TransactionCommittedUserId, INSTR(in_TransactionCommittedUserId,'\')+1, LENGTH(in_TransactionCommittedUserId)),
	-- in_TransactionCommittedUserId
	-- )
	-- )
	IFF(
	    LTRIM(RTRIM(in_TransactionCommittedUserId)) = ''
	    or LTRIM(RTRIM(in_TransactionCommittedUserId)) IS NULL,
	    'N/A',
	    IFF(
	        REGEXP_INSTR(in_TransactionCommittedUserId, '\') <> 0,
	        SUBSTR(in_TransactionCommittedUserId, REGEXP_INSTR(in_TransactionCommittedUserId, '\') + 1, LENGTH(in_TransactionCommittedUserId)),
	        in_TransactionCommittedUserId
	    )
	) AS o_TransactionCommittedUserId,
	-- *INF*: IIF(ISNULL(in_LocationNumber)=1,'0001',LPAD(LTRIM(RTRIM(TO_CHAR(in_LocationNumber))),4,'0'))
	IFF(
	    in_LocationNumber IS NULL = 1, '0001',
	    LPAD(LTRIM(RTRIM(TO_CHAR(in_LocationNumber))), 4, '0')
	) AS o_LocationNumber,
	-- *INF*: IIF(ISNULL(in_SMARTTransitionModifier) OR LTRIM(RTRIM(in_SMARTTransitionModifier))='',1,TO_DECIMAL(in_SMARTTransitionModifier) )
	IFF(
	    in_SMARTTransitionModifier IS NULL OR LTRIM(RTRIM(in_SMARTTransitionModifier)) = '', 1,
	    CAST(in_SMARTTransitionModifier AS FLOAT)
	) AS o_SMARTTransitionModifier
	FROM SQ_SMART_Stage_Data
),
JNR_ToGetSMARTAutoPolicesWithTransition AS (SELECT
	FIL_RemoveCARowsWithoutTransitionModifier.PolicyKey AS PolicyKey_CA, 
	EXP_SMART_CleanTransitionModifierdata.o_PolicyKey AS PolicyKey_SMART, 
	EXP_SMART_CleanTransitionModifierdata.o_PolicyPremium AS PolicyPremium, 
	EXP_SMART_CleanTransitionModifierdata.o_TransactionDate AS TransactionDate, 
	EXP_SMART_CleanTransitionModifierdata.o_TransactionCommittedUserId AS TransactionCommittedUserId, 
	EXP_SMART_CleanTransitionModifierdata.o_LocationNumber AS LocationNumber, 
	EXP_SMART_CleanTransitionModifierdata.o_SMARTTransitionModifier AS SMARTTransitionModifier
	FROM EXP_SMART_CleanTransitionModifierdata
	LEFT OUTER JOIN FIL_RemoveCARowsWithoutTransitionModifier
	ON FIL_RemoveCARowsWithoutTransitionModifier.PolicyKey = EXP_SMART_CleanTransitionModifierdata.o_PolicyKey
),
FIL_SMARTEligiblePolicesWithTransition AS (
	SELECT
	PolicyKey_CA, 
	PolicyKey_SMART, 
	PolicyPremium, 
	TransactionDate, 
	TransactionCommittedUserId, 
	LocationNumber, 
	SMARTTransitionModifier
	FROM JNR_ToGetSMARTAutoPolicesWithTransition
	WHERE IIF(
ISNULL(PolicyKey_CA) AND (SMARTTransitionModifier=1 OR SMARTTransitionModifier=0 ),FALSE,TRUE
)
),
SQ_SMART_RPT_EDM_Data AS (
	Declare @Date as date
	set @Date = convert(date,EOMONTH(getdate(),@{pipeline().parameters.NO_OF_MONTHS})) 
	
	Select  distinct
	 P.pol_key as PolicyKey
	,PO.PolicyOfferingDescription as PolicyOffering
	,P.pol_exp_date as PolicyExpirationDate
	,RL.LocationUnitNumber
	,PTRM.ScheduleModifiedFactor
	,PTRM.ExperienceModifiedFactor
	,SUM(PT.PremiumTransactionAmount) OVER (PARTITION BY P.pol_ak_id, PC.InsuranceLine) as DirectWrittenPremiumbyInsuranceLine
	,P.sup_bus_class_code_id
	,P.AgencyAKId
	,P.state_of_domicile_code
	,P.ProgramAKId
	,P.UnderwritingAssociateAKId
	,P.pol_ak_id as PolicyAkId
	,PC.InsuranceLine
	,CC.cust_num as CustomerNumber
	,CC.name as FirstNamedInsured
	,SPC.StrategicProfitCenterAbbreviation as StrategicProfitCenterAbbreviation 
	from @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P with (nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL with (nolock) on RL.PolicyAKID=P.pol_ak_id and RL.CurrentSnapshotFlag=1 and P.crrnt_snpsht_flag=1 and P.source_sys_id='DCT'
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC with (nolock) on RL.RiskLocationAKID=PC.RiskLocationAKID and PC.CurrentSnapshotFlag=1
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC with (nolock) on RC.PolicyCoverageAKID=PC.PolicyCoverageAKID 
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction PT with (nolock) on PT.RatingCoverageAKId=RC.RatingCoverageAKID and PT.EffectiveDate=RC.EffectiveDate
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransactionRatingModifier PTRM with (nolock) on PTRM.PremiumTransactionID=PT.PremiumTransactionID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering PO with (nolock) on PO.PolicyOfferingAKId=P.PolicyOfferingAKId and PO.CurrentSnapshotFlag=1
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.VWContractCustomer CC with (nolock) on CC.contract_cust_ak_id=P.contract_cust_ak_id and CC.crrnt_snpsht_flag=1
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter SPC with (nolock) on SPC.StrategicProfitCenterAKId=P.StrategicProfitCenterAKId 
	where PT.PremiumTransactionCode in ('Renew','Reissue','Rewrite')
	and PT.PremiumTransactionAmount <> 0
	and  PO.PolicyOfferingAbbreviation='SMART' and PC.InsuranceLine <> 'CommercialAuto'
	and PT.CreatedDate > @Date
	@{pipeline().parameters.WHERE_CLAUSE_RPT_EDM_SMART}
	order by  P.pol_key, RL.LocationUnitNumber
),
JNR_SMART_TransMod AS (SELECT
	SQ_SMART_RPT_EDM_Data.PolicyKey, 
	SQ_SMART_RPT_EDM_Data.PolicyOffering, 
	SQ_SMART_RPT_EDM_Data.PolicyExpirationDate, 
	SQ_SMART_RPT_EDM_Data.LocationUnitNumber, 
	SQ_SMART_RPT_EDM_Data.ScheduleModifiedFactor, 
	SQ_SMART_RPT_EDM_Data.ExperienceModifiedFactor, 
	SQ_SMART_RPT_EDM_Data.DirectWrittenPremiumbyInsuranceLine, 
	SQ_SMART_RPT_EDM_Data.CustomerNumber, 
	SQ_SMART_RPT_EDM_Data.FirstNamedInsured, 
	SQ_SMART_RPT_EDM_Data.sup_bus_class_code_id, 
	SQ_SMART_RPT_EDM_Data.AgencyAKId, 
	SQ_SMART_RPT_EDM_Data.state_of_domicile_code, 
	SQ_SMART_RPT_EDM_Data.ProgramAKId, 
	SQ_SMART_RPT_EDM_Data.UnderwritingAssociateAKId, 
	SQ_SMART_RPT_EDM_Data.PolicyAkId, 
	SQ_SMART_RPT_EDM_Data.InsuranceLine, 
	SQ_SMART_RPT_EDM_Data.StrategicProfitCenterAbbreviation, 
	FIL_SMARTEligiblePolicesWithTransition.PolicyKey_SMART AS PolicyKey_Stage, 
	FIL_SMARTEligiblePolicesWithTransition.PolicyPremium, 
	FIL_SMARTEligiblePolicesWithTransition.TransactionDate, 
	FIL_SMARTEligiblePolicesWithTransition.TransactionCommittedUserId, 
	FIL_SMARTEligiblePolicesWithTransition.LocationNumber AS LocationNumber_Stage, 
	FIL_SMARTEligiblePolicesWithTransition.SMARTTransitionModifier
	FROM SQ_SMART_RPT_EDM_Data
	INNER JOIN FIL_SMARTEligiblePolicesWithTransition
	ON FIL_SMARTEligiblePolicesWithTransition.PolicyKey_SMART = SQ_SMART_RPT_EDM_Data.PolicyKey AND FIL_SMARTEligiblePolicesWithTransition.LocationNumber = SQ_SMART_RPT_EDM_Data.LocationUnitNumber
),
EXP_Tns_SMARTTransitionModifiers AS (
	SELECT
	FirstNamedInsured,
	PolicyKey,
	PolicyOffering,
	PolicyExpirationDate AS in_PolicyExpirationDate,
	LocationUnitNumber,
	ScheduleModifiedFactor,
	ExperienceModifiedFactor,
	DirectWrittenPremiumbyInsuranceLine,
	sup_bus_class_code_id,
	AgencyAKId,
	state_of_domicile_code,
	ProgramAKId,
	UnderwritingAssociateAKId,
	PolicyAkId,
	InsuranceLine,
	TransactionDate,
	TransactionCommittedUserId,
	'0' AS CompositeRating,
	PolicyPremium AS PolicyDirectWrittenPremium,
	StrategicProfitCenterAbbreviation,
	CustomerNumber AS in_CustomerNumber,
	SMARTTransitionModifier AS in_SMARTTransitionModifier,
	-- *INF*: IIF(ISNULL(in_CustomerNumber),'N/A',CHR(39) || in_CustomerNumber)||CHR(39)
	IFF(in_CustomerNumber IS NULL, 'N/A', CHR(39) || in_CustomerNumber) || CHR(39) AS o_CustomerNumber,
	-- *INF*: TO_CHAR(in_PolicyExpirationDate,'MM-DD-YYYY')
	TO_CHAR(in_PolicyExpirationDate, 'MM-DD-YYYY') AS o_PolicyExpirationDate,
	in_SMARTTransitionModifier AS o_SMARTTransition,
	0 AS o_AutoPhysicalDamage,
	0 AS o_AutoPhysicalDamageTransition,
	0 AS o_AutoLiability,
	0 AS o_AutoLiabilityTransition
	FROM JNR_SMART_TransMod
),
UN_SMART_Auto_Data AS (
	SELECT o_CustomerNumber AS CustomerNumber, FirstNamedInsured, PolicyKey, PolicyOffering, o_PolicyExpirationDate AS PolicyExpirationDate, LocationUnitNumber, o_ScheduleModifiedFactor AS ScheduleModifiedFactor, ExperienceModifiedFactor, DirectWrittenPremiumbyInsuranceLine, sup_bus_class_code_id, AgencyAKId, state_of_domicile_code, ProgramAKId, UnderwritingAssociateAKId, InsuranceLine, TransactionDate, TransactionCommittedUserId, CompositeRating, PolicyDirectWrittenPremium, StrategicProfitCenterAbbreviation, o_SMARTTransition AS SMARTTransition, o_AutoPhysicalDamage AS AutoPhysicalDamage, o_AutoPhysicalDamageTransition AS AutoPhysicalDamageTransition, o_AutoLiability AS AutoLiability, o_AutoLiabilityTransition AS AutoLiabilityTransition
	FROM EXP_Derive_AutoModifiers
	UNION
	SELECT o_CustomerNumber AS CustomerNumber, FirstNamedInsured, PolicyKey, PolicyOffering, o_PolicyExpirationDate AS PolicyExpirationDate, LocationUnitNumber, ScheduleModifiedFactor, ExperienceModifiedFactor, DirectWrittenPremiumbyInsuranceLine, sup_bus_class_code_id, AgencyAKId, state_of_domicile_code, ProgramAKId, UnderwritingAssociateAKId, InsuranceLine, TransactionDate, TransactionCommittedUserId, CompositeRating, PolicyDirectWrittenPremium, StrategicProfitCenterAbbreviation, o_SMARTTransition AS SMARTTransition, o_AutoPhysicalDamage AS AutoPhysicalDamage, o_AutoPhysicalDamageTransition AS AutoPhysicalDamageTransition, o_AutoLiability AS AutoLiability, o_AutoLiabilityTransition AS AutoLiabilityTransition
	FROM EXP_Tns_SMARTTransitionModifiers
),
LKP_Agency AS (
	SELECT
	AgencyCode,
	DoingBusinessAsName,
	in_AgencyAKID,
	AgencyAKID
	FROM (
		select A.AgencyAKID as AgencyAKID , A.AgencyCode as AgencyCode,A.DoingBusinessAsName as DoingBusinessAsName from @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.Agency A where A.CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyAKID ORDER BY AgencyCode) = 1
),
LKP_BCCCode AS (
	SELECT
	sup_bus_class_code_id,
	StandardBusinessClassCode
	FROM (
		select sup_bus_class_code_id as sup_bus_class_code_id, StandardBusinessClassCode as StandardBusinessClassCode from @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_business_classification_code
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_bus_class_code_id ORDER BY sup_bus_class_code_id DESC) = 1
),
LKP_Program AS (
	SELECT
	ProgramDescription,
	ProgramAKId
	FROM (
		select PR.ProgramAKId as ProgramAKId, PR.ProgramDescription as ProgramDescription from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Program PR  where PR.CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProgramAKId ORDER BY ProgramDescription DESC) = 1
),
LKP_StateCode AS (
	SELECT
	state_abbrev,
	PrimaryRatingState
	FROM (
		select S.state_abbrev as state_abbrev, S.state_code as PrimaryRatingState
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_state S
		where S.crrnt_snpsht_flag=1 and S.source_sys_id='EXCEED'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_abbrev ORDER BY state_abbrev DESC) = 1
),
LKP_UnderwritingAssociate AS (
	SELECT
	UnderwritingAssociateAKID,
	UnderwritingRegionCodeDescription
	FROM (
		select UWA.UnderwritingAssociateAKID as UnderwritingAssociateAKID, UR.UnderwritingRegionCodeDescription as UnderwritingRegionCodeDescription from @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwritingAssociate UWA with (nolock) 
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.UnderwritingRegion UR with (nolock) on UR.UnderwritingManagerAKID=UWA.UnderwritingManagerAKID and UR.CurrentSnapshotFlag=1 and UWA.CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY UnderwritingAssociateAKID ORDER BY UnderwritingAssociateAKID) = 1
),
EXP_GetLookupData AS (
	SELECT
	UN_SMART_Auto_Data.CustomerNumber,
	UN_SMART_Auto_Data.FirstNamedInsured,
	UN_SMART_Auto_Data.PolicyKey,
	UN_SMART_Auto_Data.PolicyOffering,
	UN_SMART_Auto_Data.PolicyExpirationDate,
	UN_SMART_Auto_Data.LocationUnitNumber,
	UN_SMART_Auto_Data.ScheduleModifiedFactor,
	UN_SMART_Auto_Data.ExperienceModifiedFactor,
	UN_SMART_Auto_Data.DirectWrittenPremiumbyInsuranceLine,
	UN_SMART_Auto_Data.InsuranceLine,
	UN_SMART_Auto_Data.TransactionDate,
	UN_SMART_Auto_Data.TransactionCommittedUserId,
	UN_SMART_Auto_Data.CompositeRating,
	UN_SMART_Auto_Data.PolicyDirectWrittenPremium,
	UN_SMART_Auto_Data.StrategicProfitCenterAbbreviation,
	UN_SMART_Auto_Data.SMARTTransition,
	UN_SMART_Auto_Data.AutoPhysicalDamage,
	UN_SMART_Auto_Data.AutoPhysicalDamageTransition,
	UN_SMART_Auto_Data.AutoLiability,
	UN_SMART_Auto_Data.AutoLiabilityTransition,
	LKP_Program.ProgramDescription AS lkp_Program,
	LKP_Agency.AgencyCode AS lkp_AgencyCode,
	LKP_Agency.DoingBusinessAsName AS lkp_DoingBusinessAsName,
	LKP_UnderwritingAssociate.UnderwritingRegionCodeDescription AS lkp_UnderwritingRegionCode,
	LKP_StateCode.PrimaryRatingState AS lkp_PrimaryRatingState,
	LKP_BCCCode.StandardBusinessClassCode AS lkp_StandardBusinessClassCode,
	-- *INF*: IIF(ISNULL(lkp_Program),'N/A', lkp_Program)
	IFF(lkp_Program IS NULL, 'N/A', lkp_Program) AS o_Program,
	-- *INF*: SUBSTR(PolicyKey,1,7) || '-' || SUBSTR(PolicyKey,8,2)
	SUBSTR(PolicyKey, 1, 7) || '-' || SUBSTR(PolicyKey, 8, 2) AS o_PolicyKey,
	-- *INF*: IIF(ISNULL(lkp_AgencyCode),'N/A',lkp_AgencyCode)
	IFF(lkp_AgencyCode IS NULL, 'N/A', lkp_AgencyCode) AS o_AgencyCode,
	-- *INF*: IIF(ISNULL(lkp_DoingBusinessAsName),'N/A',lkp_DoingBusinessAsName)
	IFF(lkp_DoingBusinessAsName IS NULL, 'N/A', lkp_DoingBusinessAsName) AS o_AgencyName,
	-- *INF*: IIF(ISNULL(lkp_UnderwritingRegionCode),'N/A',lkp_UnderwritingRegionCode)
	IFF(lkp_UnderwritingRegionCode IS NULL, 'N/A', lkp_UnderwritingRegionCode) AS o_UnderwritingRegion,
	-- *INF*: IIF(ISNULL(lkp_PrimaryRatingState),'N/A',lkp_PrimaryRatingState)
	IFF(lkp_PrimaryRatingState IS NULL, 'N/A', lkp_PrimaryRatingState) AS o_PrimaryRatingState,
	-- *INF*: IIF(ISNULL(lkp_StandardBusinessClassCode) or lkp_StandardBusinessClassCode='N/A' ,'N/A',  lkp_StandardBusinessClassCode)
	IFF(
	    lkp_StandardBusinessClassCode IS NULL or lkp_StandardBusinessClassCode = 'N/A', 'N/A',
	    lkp_StandardBusinessClassCode
	) AS o_BusinessClassificationCode
	FROM UN_SMART_Auto_Data
	LEFT JOIN LKP_Agency
	ON LKP_Agency.AgencyAKID = UN_SMART_Auto_Data.AgencyAKId
	LEFT JOIN LKP_BCCCode
	ON LKP_BCCCode.sup_bus_class_code_id = UN_SMART_Auto_Data.sup_bus_class_code_id
	LEFT JOIN LKP_Program
	ON LKP_Program.ProgramAKId = UN_SMART_Auto_Data.ProgramAKId
	LEFT JOIN LKP_StateCode
	ON LKP_StateCode.state_abbrev = UN_SMART_Auto_Data.state_of_domicile_code
	LEFT JOIN LKP_UnderwritingAssociate
	ON LKP_UnderwritingAssociate.UnderwritingAssociateAKID = UN_SMART_Auto_Data.UnderwritingAssociateAKId
),
SRT_SMARTAutoTransitionFactor AS (
	SELECT
	StrategicProfitCenterAbbreviation, 
	o_UnderwritingRegion AS UnderwritingRegion, 
	PolicyOffering, 
	CustomerNumber, 
	o_PolicyKey AS PolicyKey, 
	TransactionCommittedUserId, 
	FirstNamedInsured, 
	InsuranceLine, 
	o_Program AS Program, 
	LocationUnitNumber AS LocationNumber, 
	ExperienceModifiedFactor AS ExperienceModificationFactor, 
	ScheduleModifiedFactor AS ScheduledModificationFactor, 
	SMARTTransition, 
	AutoPhysicalDamage, 
	AutoPhysicalDamageTransition, 
	AutoLiability, 
	AutoLiabilityTransition, 
	o_PrimaryRatingState AS PrimaryRatingState, 
	o_BusinessClassificationCode AS BusinessClassificationCode, 
	PolicyExpirationDate, 
	TransactionDate AS TransactionCommittedDate, 
	PolicyDirectWrittenPremium, 
	DirectWrittenPremiumbyInsuranceLine, 
	o_AgencyCode AS AgencyCode, 
	o_AgencyName AS AgencyName, 
	CompositeRating AS CompositeRatingIndicator
	FROM EXP_GetLookupData
	ORDER BY PolicyOffering ASC, PolicyKey ASC, InsuranceLine ASC, LocationNumber ASC, ExperienceModificationFactor ASC, ScheduledModificationFactor ASC, SMARTTransition ASC, AutoPhysicalDamage ASC, AutoPhysicalDamageTransition ASC, AutoLiability ASC, AutoLiabilityTransition ASC
),
AGG_RemoveDuplicates AS (
	SELECT
	StrategicProfitCenterAbbreviation,
	UnderwritingRegion,
	PolicyOffering,
	CustomerNumber,
	PolicyKey,
	TransactionCommittedUserId,
	FirstNamedInsured,
	InsuranceLine,
	Program,
	LocationNumber,
	ExperienceModificationFactor,
	ScheduledModificationFactor,
	SMARTTransition,
	AutoPhysicalDamage,
	AutoPhysicalDamageTransition,
	AutoLiability,
	AutoLiabilityTransition,
	PrimaryRatingState,
	BusinessClassificationCode,
	PolicyExpirationDate,
	TransactionCommittedDate,
	PolicyDirectWrittenPremium,
	DirectWrittenPremiumbyInsuranceLine,
	AgencyCode,
	AgencyName,
	CompositeRatingIndicator
	FROM SRT_SMARTAutoTransitionFactor
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyOffering, PolicyKey, InsuranceLine, LocationNumber, ExperienceModificationFactor, ScheduledModificationFactor, SMARTTransition, AutoPhysicalDamage, AutoPhysicalDamageTransition, AutoLiability, AutoLiabilityTransition ORDER BY NULL) = 1
),
EXP_TargetPassthrough AS (
	SELECT
	StrategicProfitCenterAbbreviation,
	UnderwritingRegion,
	TransactionCommittedUserId,
	CustomerNumber,
	FirstNamedInsured,
	PolicyOffering,
	Program,
	InsuranceLine,
	PolicyKey,
	LocationNumber,
	ExperienceModificationFactor,
	ScheduledModificationFactor,
	SMARTTransition,
	AutoLiability,
	AutoLiabilityTransition,
	AutoPhysicalDamage,
	AutoPhysicalDamageTransition,
	PrimaryRatingState,
	BusinessClassificationCode,
	PolicyExpirationDate,
	TransactionCommittedDate,
	PolicyDirectWrittenPremium,
	DirectWrittenPremiumbyInsuranceLine,
	AgencyCode,
	AgencyName,
	CompositeRatingIndicator,
	-- *INF*: SETMAXVARIABLE(@{pipeline().parameters.FILENAME},'TransitionFactorReport_'||TO_CHAR(ADD_TO_DATE(SYSDATE,'MONTH',-1),'MON YYYY')||'.csv')
	SETMAXVARIABLE(@{pipeline().parameters.FILENAME}, 'TransitionFactorReport_' || TO_CHAR(DATEADD(MONTH,- 1,CURRENT_TIMESTAMP), 'MON YYYY') || '.csv') AS FileName_MappingVariable,
	-- *INF*: 'TransitionFactorReport_'||TO_CHAR(ADD_TO_DATE(SYSDATE,'MONTH',-1),'MON YYYY')||'.csv'
	'TransitionFactorReport_' || TO_CHAR(DATEADD(MONTH,- 1,CURRENT_TIMESTAMP), 'MON YYYY') || '.csv' AS FileName
	FROM AGG_RemoveDuplicates
),
SMARTAutoTransitionFactorReportFile AS (
	INSERT INTO SMARTAutoTransitionFactorReportFile
	(StrategicProfitCenter, UnderwritingRegion, TransactionCommittedUserId, CustomerNumber, FirstNamedInsured, PolicyOffering, Program, InsuranceLine, PolicyKey, LocationNumber, ExperienceModificationFactor, ScheduledModificationFactor, SMARTTransition, AutoLiabilityScheduleModExclTransition, AutoLiabilityTransition, AutoPhysicalDamageScheduleModExclTransition, AutoPhysicalDamageTransition, PrimaryRatingState, BusinessClassificationCode, PolicyExpirationDate, TransactionCommittedDate, PolicyDirectWrittenPremium, DirectWrittenPremiumbyInsuranceLine, AgencyCode, AgencyName, CompositeRatingIndicator, FileName)
	SELECT 
	StrategicProfitCenterAbbreviation AS STRATEGICPROFITCENTER, 
	UNDERWRITINGREGION, 
	TRANSACTIONCOMMITTEDUSERID, 
	CUSTOMERNUMBER, 
	FIRSTNAMEDINSURED, 
	POLICYOFFERING, 
	PROGRAM, 
	INSURANCELINE, 
	POLICYKEY, 
	LOCATIONNUMBER, 
	EXPERIENCEMODIFICATIONFACTOR, 
	SCHEDULEDMODIFICATIONFACTOR, 
	SMARTTRANSITION, 
	AutoLiability AS AUTOLIABILITYSCHEDULEMODEXCLTRANSITION, 
	AUTOLIABILITYTRANSITION, 
	AutoPhysicalDamage AS AUTOPHYSICALDAMAGESCHEDULEMODEXCLTRANSITION, 
	AUTOPHYSICALDAMAGETRANSITION, 
	PRIMARYRATINGSTATE, 
	BUSINESSCLASSIFICATIONCODE, 
	POLICYEXPIRATIONDATE, 
	TRANSACTIONCOMMITTEDDATE, 
	POLICYDIRECTWRITTENPREMIUM, 
	DIRECTWRITTENPREMIUMBYINSURANCELINE, 
	AGENCYCODE, 
	AGENCYNAME, 
	COMPOSITERATINGINDICATOR, 
	FILENAME
	FROM EXP_TargetPassthrough
),