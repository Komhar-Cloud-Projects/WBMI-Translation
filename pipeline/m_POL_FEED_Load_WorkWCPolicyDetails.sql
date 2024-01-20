WITH
SQ_Policy_Tables AS (
	WITH CTE_PolicyDetail as 
	(
	SELECT distinct DT.HistoryID,DT.SessionId,DS.Purpose,WP.PolicyNumber, WP.PolicyVersionFormatted,WP.PolicyID,WCP.WB_WC_PolicyId
	FROM dbo.wb_wc_policy WCP with (nolock)
	inner join dbo.wb_cl_Policy CLP with (nolock)
	on CLP.WB_CL_PolicyId = WCP.WB_CL_PolicyId
	inner join dbo.wb_policy WP with(nolock)
	on WP.WB_PolicyId = CLP.WB_PolicyId
	inner join dbo.DC_Transaction DT with (NOLOCK)
	on DT.SessionId=WP.SessionId
	inner join dbo.DC_Session DS with (NOLOCK)
	on DS.SessionId=DT.SessionId
	inner join dbo.DC_Line DL with(nolock)
	on DT.SessionId=DL.SessionId
	where DL.Type='WorkersCompensation'
	and DS.Purpose='Onset'
	and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	and DT.State='Committed'
	
	)
	
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,WD.WB_WC_WorkDescriptionId ProcessID,'ExcludedWorkplace' Attribute , WD.ExcludedWorkplace Value 
	from dbo.WB_WC_WorkDescription WD with(nolock) 
	inner join CTE_PolicyDetail CTE
	on CTE.WB_WC_PolicyId = WD.WB_WC_PolicyId
	and WD.ExcludedWorkplace is NOT NULL
	
	UNION ALL
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,WD.WB_WC_WorkDescriptionId ProcessID,'VesselName' Attribute , WD.VesselName Value 
	from dbo.WB_WC_WorkDescription WD with(nolock) 
	inner join CTE_PolicyDetail CTE
	on CTE.WB_WC_PolicyId = WD.WB_WC_PolicyId
	
	UNION ALL
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,WD.WB_WC_WorkDescriptionId ProcessID,'WorkersCompensationLaw' Attribute , WD.WorkersCompensationLaw Value 
	from dbo.WB_WC_WorkDescription WD with(nolock) 
	inner join CTE_PolicyDetail CTE
	on CTE.WB_WC_PolicyId = WD.WB_WC_PolicyId
	
	UNION ALL
	
	select DT.HistoryID,DT.SessionId,DS.Purpose,WP.PolicyNumber, WP.PolicyVersionFormatted,WP.PolicyID,WD.WB_WC_WorkDescriptionId ProcessID,'MaritimeWorkDescriptionWC0002030484' Attribute , WCP.MaritimeWorkDescriptionWC0002030484 Value 
	from dbo.WB_WC_WorkDescription WD with(nolock) 
	inner join dbo.wb_wc_policy WCP with (nolock)
	on WCP.WB_WC_PolicyId = WD.WB_WC_PolicyId
	inner join dbo.wb_cl_Policy CLP with (nolock)
	on CLP.WB_CL_PolicyId = WCP.WB_CL_PolicyId
	inner join dbo.wb_policy WP with(nolock)
	on WP.WB_PolicyId = CLP.WB_PolicyId
	inner join dbo.DC_Transaction DT with (NOLOCK)
	on DT.SessionId=WP.SessionId
	inner join dbo.DC_Session DS with (NOLOCK)
	on DS.SessionId=DT.SessionId
	inner join dbo.DC_Line DL with(nolock)
	on DT.SessionId=DL.SessionId
	where DL.Type='WorkersCompensation'
	and DS.Purpose='Onset'
	and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	and DT.State='Committed'
	
	
	UNION ALL
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,WE.WB_WC_EmployeeId ProcessID,'NameOfEmployeeGroup' Attribute , WE.NameOfEmployeeGroup Value 
	from dbo.WB_WC_Employee WE with(nolock)
	inner join CTE_PolicyDetail CTE
	on CTE.WB_WC_PolicyId = WE.WB_WC_PolicyId
	
	
	UNION ALL
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,WE.WB_WC_EmployeeId ProcessID,'EmployeeGroupStateList' Attribute , WE.EmployeeGroupStateList Value 
	from dbo.WB_WC_Employee WE with(nolock)
	inner join CTE_PolicyDetail CTE
	on CTE.WB_WC_PolicyId = WE.WB_WC_PolicyId
	
	
	UNION ALL
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,WE.WB_WC_EmployeeId ProcessID,'EmployeeDesgWCLaw' Attribute , WE.EmployeeDesgWCLaw Value 
	from dbo.WB_WC_Employee WE with(nolock)
	inner join CTE_PolicyDetail CTE
	on CTE.WB_WC_PolicyId = WE.WB_WC_PolicyId
	
	
	UNION ALL
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,AE.WB_WC_AlternateEmployerId ProcessID,'AlternateEmployerName' Attribute , AE.AlternateEmployerName Value 
	from dbo.WB_WC_AlternateEmployer AE with(nolock)
	inner join CTE_PolicyDetail CTE
	on CTE.WB_WC_PolicyId = AE.WB_WC_PolicyId
	
	
	UNION ALL
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,AE.WB_WC_AlternateEmployerId ProcessID,'AlternateEmployerStateOfEmployment' Attribute , AE.AlternateEmployerStateOfEmployment Value 
	from dbo.WB_WC_AlternateEmployer AE with(nolock)
	inner join CTE_PolicyDetail CTE
	on CTE.WB_WC_PolicyId = AE.WB_WC_PolicyId
	
	
	UNION ALL
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,AE.WB_WC_AlternateEmployerId ProcessID,'AlternateEmployerContractOrProject' Attribute , AE.AlternateEmployerContractOrProject Value 
	from dbo.WB_WC_AlternateEmployer AE with(nolock)
	inner join CTE_PolicyDetail CTE
	on CTE.WB_WC_PolicyId = AE.WB_WC_PolicyId
	
	
	UNION ALL
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,AE.WB_WC_AlternateEmployerId ProcessID,'AddressOfAlternateEmployer' Attribute , 
	CONCAT(AE.AlternateEmployerAddress,',',AE.AlternateEmployerCity,',',AE.AlternateEmployerState,',',AE.AlternateEmployerZip) Value 
	from dbo.WB_WC_AlternateEmployer AE with(nolock)
	inner join CTE_PolicyDetail CTE
	on CTE.WB_WC_PolicyId = AE.WB_WC_PolicyId
	
	UNION ALL
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,CTE.WB_WC_PolicyId ProcessID,'EmployeeLeasingCompanyNameWC480316' Attribute , P.EmployeeLeasingCompanyNameWC480316 Value 
	from dbo.WB_WC_Policy P with(nolock) 
	inner join CTE_PolicyDetail CTE
	on CTE.WB_WC_PolicyId = P.WB_WC_PolicyId
	
	union all 
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,CTE.WB_WC_PolicyId ProcessID,'ClientNameWC480316' Attribute , P.ClientNameWC480316 Value 
	from dbo.WB_WC_Policy P with(nolock) 
	inner join CTE_PolicyDetail CTE
	on CTE.WB_WC_PolicyId = P.WB_WC_PolicyId
	
	union all 
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,CTE.WB_WC_PolicyId ProcessID,'TerminatedEffectiveDateWC480316' Attribute , convert(varchar(20),P.TerminatedEffectiveDateWC480316,112) Value 
	from dbo.WB_WC_Policy P with(nolock) 
	inner join CTE_PolicyDetail CTE
	on CTE.WB_WC_PolicyId = P.WB_WC_PolicyId
	
	union all 
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,CTE.WB_WC_PolicyId ProcessID,'EntitiesWC480316' Attribute , P.EntitiesWC480316 Value 
	from dbo.WB_WC_Policy P with(nolock) 
	inner join CTE_PolicyDetail CTE
	on CTE.WB_WC_PolicyId = P.WB_WC_PolicyId
	
	union all 
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,CTE.WB_WC_PolicyId ProcessID,'DateSentWC480316' Attribute, convert(varchar(20),P.DateSentWC480316,112) Value 
	from dbo.WB_WC_Policy P with(nolock) 
	inner join CTE_PolicyDetail CTE
	on CTE.WB_WC_PolicyId = P.WB_WC_PolicyId
	
	union all 
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,CTE.WB_WC_PolicyId ProcessID,'MaritimeWorkDescription201A' Attribute , P.MaritimeWorkDescription201A Value 
	from dbo.WB_WC_Policy P with(nolock) 
	inner join CTE_PolicyDetail CTE
	on CTE.WB_WC_PolicyId = P.WB_WC_PolicyId
	
	union all
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,AEW.WB_WC_AlternateEmployerWaiverId ProcessID,'AlternateEmployerWaiverDescription' Attribute , AEW.Description Value 
	from dbo.WB_WC_AlternateEmployerWaiver AEW with(nolock) 
	inner join CTE_PolicyDetail CTE
	on CTE.WB_WC_PolicyId = AEW.WB_WC_PolicyId
	
	union all
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,WBSP.WB_WC_SoleProprietorsId ProcessID,WBSP.Attribute , WBSP.Value 
	from CTE_PolicyDetail CTE
	inner join (
	Select A.WB_WC_PolicyId,'SoleProprietorExcluded' Attribute , A.SoleProprietorExcluded Value,A.WB_WC_SoleProprietorsId
	from dbo.WB_WC_SoleProprietors A with(nolock) 
	
	union ALL
	
	Select B.WB_WC_PolicyId,'IncludedSolePropState' Attribute , B.IncludedSolePropState Value,B.WB_WC_SoleProprietorsId
	from dbo.WB_WC_SoleProprietors B with(nolock) 
	
	union ALL
	
	Select C.WB_WC_PolicyId,'NameOfSoleProprietorIncluded' Attribute , C.NameOfSoleProprietorIncluded Value,C.WB_WC_SoleProprietorsId
	from dbo.WB_WC_SoleProprietors C with(nolock) 
	) WBSP
	on CTE.WB_WC_PolicyId = WBSP.WB_WC_PolicyId
	
	union all
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,WBP.WB_WC_PartnersId ProcessID,WBP.Attribute , WBP.Value 
	from CTE_PolicyDetail CTE
	inner join (
	Select A.WB_WC_PolicyId,'NameOfPartnerExcluded' Attribute , A.NameOfPartnerExcluded Value,A.WB_WC_PartnersId
	from dbo.WB_WC_Partners A with(nolock) 
	
	union ALL
	
	Select B.WB_WC_PolicyId,'IncludedPartnersState' Attribute , B.IncludedPartnersState Value,B.WB_WC_PartnersId
	from dbo.WB_WC_Partners B with(nolock) 
	
	union ALL
	
	Select C.WB_WC_PolicyId,'NameOfPartnersIncluded' Attribute , C.NameOfPartnersIncluded Value,C.WB_WC_PartnersId
	from dbo.WB_WC_Partners C with(nolock) 
	) WBP
	on CTE.WB_WC_PolicyId = WBP.WB_WC_PolicyId
	
	union all
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,WBOFF.WB_WC_OfficersId ProcessID,WBOFF.Attribute , WBOFF.Value 
	from CTE_PolicyDetail CTE
	inner join (
	Select A.WB_WC_PolicyId,'NameOfOfficersExcluded' Attribute , A.NameOfOfficersExcluded Value,A.WB_WC_OfficersId
	from dbo.WB_WC_Officers A with(nolock) 
	
	union ALL
	
	Select B.WB_WC_PolicyId,'IncludedOfficersState' Attribute , B.IncludedOfficersState Value,B.WB_WC_OfficersId
	from dbo.WB_WC_Officers B with(nolock) 
	
	union ALL
	
	Select C.WB_WC_PolicyId,'NameOfOfficersIncluded' Attribute , C.NameOfOfficersIncluded Value,C.WB_WC_OfficersId
	from dbo.WB_WC_Officers C with(nolock) 
	) WBOFF
	on CTE.WB_WC_PolicyId = WBOFF.WB_WC_PolicyId
	
	union all
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,WBOTH.WB_WC_OthersId ProcessID,WBOTH.Attribute , WBOTH.Value 
	from CTE_PolicyDetail CTE
	inner join (
	Select A.WB_WC_PolicyId,'NameOfOthersExcluded' Attribute , A.NameOfOthersExcluded Value,A.WB_WC_OthersId 
	from dbo.WB_WC_Others A with(nolock) 
	
	union ALL
	
	Select B.WB_WC_PolicyId,'IncludedOthersState' Attribute , B.IncludedOthersState Value,B.WB_WC_OthersId 
	from dbo.WB_WC_Others B with(nolock) 
	
	union ALL
	
	Select C.WB_WC_PolicyId,'NameOfOthersIncluded' Attribute , C.NameOfOthersIncluded Value,C.WB_WC_OthersId 
	from dbo.WB_WC_Others C with(nolock) 
	) WBOTH
	on CTE.WB_WC_PolicyId = WBOTH.WB_WC_PolicyId
	
	union ALL
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,Client.WB_WC_ClientsId ProcessID,Client.Attribute , client.Value 
	from CTE_PolicyDetail CTE
	inner join (
	Select WB_WC_PolicyId, WB_WC_ClientsId, SessionId,'IncludedClientName' Attribute, wc.includedclientname Value from WB_WC_Clients wc with (nolock)
	union all
	Select WB_WC_PolicyId, WB_WC_ClientsId, SessionId,'IncludedClientAddress' Attribute, wc.IncludedClientAddress Value from WB_WC_Clients wc with (nolock)
	union all
	Select WB_WC_PolicyId, WB_WC_ClientsId, SessionId,'ClientFEINNumber' Attribute, wc.ClientFEINNumber Value from WB_WC_Clients wc with (nolock)
	union all
	Select WB_WC_PolicyId, WB_WC_ClientsId, SessionId,'EstimatedPremium' Attribute, CAST(wc.EstimatedPremium as varchar(120)) as Value from WB_WC_Clients wc with (nolock)
	union all
	Select WB_WC_PolicyId, WB_WC_ClientsId, SessionId,'ClientNameEmployeeLeasing' Attribute, wc.ClientNameEmployeeLeasing Value from WB_WC_Clients wc with (nolock)
	union all
	Select WB_WC_PolicyId, WB_WC_ClientsId, SessionId,'ClientFEINNumberEmployeeLeasing' Attribute, wc.ClientFEINNumberEmployeeLeasing Value from WB_WC_Clients wc with (nolock)
	union all
	Select WB_WC_PolicyId, WB_WC_ClientsId, SessionId,'ClientAddressEmployeeLeasing' Attribute, wc.ClientAddressEmployeeLeasing Value from WB_WC_Clients wc with (nolock)
	union all
	Select WB_WC_PolicyId, WB_WC_ClientsId, SessionId,'LaborContractorAddress' Attribute, wc.LaborContractorAddress Value from WB_WC_Clients wc with (nolock)
	union all
	Select WB_WC_PolicyId, WB_WC_ClientsId, SessionId,'LaborContractor' Attribute, wc.LaborContractor Value from WB_WC_Clients wc with (nolock)
	union all
	Select WB_WC_PolicyId, WB_WC_ClientsId, SessionId,'LaborContractorPolicyNumber' Attribute, wc.LaborContractorPolicyNumber Value from WB_WC_Clients wc with (nolock)
	union all
	Select WB_WC_PolicyId, WB_WC_ClientsId, SessionId,'LaborContractorFEIN' Attribute, wc.LaborContractorFEIN Value from WB_WC_Clients wc with (nolock)
	)  Client on CTE.WB_WC_PolicyId=Client.WB_WC_PolicyId and CTE.SessionId=client.SessionId
	
	union ALL
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,WBCON.WB_WC_ContractorsId ProcessID,WBCON.Attribute , WBCON.Value 
	from CTE_PolicyDetail CTE
	inner join(
	Select C.WB_WC_PolicyId,'DesignatedContractor' Attribute , C.DesignatedContractor Value,C.WB_WC_ContractorsId 
	from dbo.WB_WC_Contractors C with(nolock) 
	
	union ALL
	
	Select C.WB_WC_PolicyId,'ClassCode' Attribute , C.ClassCode Value,C.WB_WC_ContractorsId 
	from dbo.WB_WC_Contractors C with(nolock) 
	
	union ALL
	
	Select C.WB_WC_PolicyId,'ClassCodeDescription' Attribute , C.ClassCodeDescription Value,C.WB_WC_ContractorsId 
	from dbo.WB_WC_Contractors C with(nolock) 
	
	union ALL
	
	Select C.WB_WC_PolicyId,'PremiumBasis' Attribute , C.PremiumBasis Value,C.WB_WC_ContractorsId 
	from dbo.WB_WC_Contractors C with(nolock) 
	
	union ALL
	
	Select C.WB_WC_PolicyId,'Rate' Attribute , C.Rate Value,C.WB_WC_ContractorsId 
	from dbo.WB_WC_Contractors C with(nolock) 
	
	union ALL
	
	Select C.WB_WC_PolicyId,'MinimumPremium' Attribute , C.MinimumPremium Value,C.WB_WC_ContractorsId 
	from dbo.WB_WC_Contractors C with(nolock) 
	
	union ALL
	
	Select C.WB_WC_PolicyId,'EstimatedAnnualPremium' Attribute , C.EstimatedAnnualPremium Value,C.WB_WC_ContractorsId 
	from dbo.WB_WC_Contractors C with(nolock) 
	) WBCON
	on CTE.WB_WC_PolicyId = WBCON.WB_WC_PolicyId
	
	union ALL
	
	select CTE.HistoryID,CTE.SessionId,CTE.Purpose,CTE.PolicyNumber, CTE.PolicyVersionFormatted,CTE.PolicyID,WBClient.ProcessID,WBClient.Attribute,WBClient.Value 
	from CTE_PolicyDetail CTE
	inner join(
	select P.WB_WC_PolicyId,'ClientNameWC220304' Attribute , P.ClientNameWC220304 Value,P.WB_WC_PolicyId ProcessID
	from dbo.WB_WC_Policy P with(nolock) 
	
	union ALL
	
	select P.WB_WC_PolicyId,'ClientAddressWC220304' Attribute , P.ClientAddressWC220304 Value,P.WB_WC_PolicyId ProcessID
	from dbo.WB_WC_Policy P with(nolock) 
	
	union ALL
	
	select P.WB_WC_PolicyId,'ClientCityWC220304' Attribute , P.ClientCityWC220304 Value,P.WB_WC_PolicyId ProcessID
	from dbo.WB_WC_Policy P with(nolock) 
	
	union ALL
	
	Select C.WB_WC_PolicyId,'IncludedClientCity' Attribute , C.IncludedClientCity Value,C.WB_WC_ClientsId ProcessID
	from dbo.WB_WC_Clients C with(nolock) 
	
	union ALL
	
	select P.WB_WC_PolicyId,'ClientStateWC220304' Attribute , P.ClientStateWC220304 Value,P.WB_WC_PolicyId ProcessID
	from dbo.WB_WC_Policy P with(nolock) 
	
	union ALL
	
	Select C.WB_WC_PolicyId,'IncludedClientState' Attribute , C.IncludedClientState Value,C.WB_WC_ClientsId ProcessID
	from dbo.WB_WC_Clients C with(nolock) 
	
	union ALL
	
	select P.WB_WC_PolicyId,'ClientZipWC220304' Attribute , P.ClientZipWC220304 Value,P.WB_WC_PolicyId ProcessID
	from dbo.WB_WC_Policy P with(nolock) 
	
	union ALL
	
	Select C.WB_WC_PolicyId,'IncludedClientZipcode' Attribute , C.IncludedClientZipcode Value,C.WB_WC_ClientsId ProcessID
	from dbo.WB_WC_Clients C with(nolock) 
	
	union ALL
	
	select P.WB_WC_PolicyId,'ClientFEINNumberWC220304' Attribute , P.ClientFEINNumberWC220304 Value,P.WB_WC_PolicyId ProcessID
	from dbo.WB_WC_Policy P with(nolock) 
	
	union ALL
	
	select P.WB_WC_PolicyId,'ClientUINumberwC220304' Attribute , P.ClientUINumberwC220304 Value,P.WB_WC_PolicyId ProcessID
	from dbo.WB_WC_Policy P with(nolock) 
	
	) WBClient
	on CTE.WB_WC_PolicyId = WBClient.WB_WC_PolicyId
),
EXP_SRC_DataCollect AS (
	SELECT
	HistoryID,
	SessionId,
	Purpose,
	PolicyNumber,
	PolicyVersionFormatted,
	PolicyId,
	ProcessID,
	Attribute,
	Value AS i_Value,
	-- *INF*: REG_REPLACE(i_Value, '[^\x21-\x7E]', ' ')
	-- 
	-- --IIF(NOT ISNULL(i_Value),REPLACECHR(0, REPLACECHR(0,i_Value,CHR(10),''),CHR(13),''),i_Value)
	-- -- filtering out anything outside of the most basic ascii set
	REGEXP_REPLACE(i_Value, '[^\x21-\x7E]', ' ') AS o_Value
	FROM SQ_Policy_Tables
),
LKP_LatestSessionID AS (
	SELECT
	SessionId,
	IN_HistoryID,
	IN_SessionId,
	IN_Purpose,
	Purpose,
	HistoryID
	FROM (
		Select distinct DT.HistoryID AS HistoryID,
		DS.Purpose AS Purpose,
		Max(DS.Sessionid) AS Sessionid
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
		on DT.Sessionid=DS.Sessionid
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
		on DT.Sessionid=DL.Sessionid
		where DL.Type='WorkersCompensation'
		and DS.Purpose='Onset'
		and DT.State='Committed'
		and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
		group by DT.HistoryID,DS.Purpose
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionId,Purpose,HistoryID ORDER BY SessionId) = 1
),
LKP_WorkWCTrackHistory AS (
	SELECT
	WCTrackHistoryID,
	Auditid,
	IN_HistoryID,
	IN_Purpose,
	HistoryID,
	Purpose
	FROM (
		SELECT 
		WorkWCTrackHistory.WCTrackHistoryID as WCTrackHistoryID, 
		WorkWCTrackHistory.Auditid as Auditid, 
		WorkWCTrackHistory.HistoryID as HistoryID, 
		WorkWCTrackHistory.Purpose as Purpose 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory
		order by WorkWCTrackHistory.HistoryID,WorkWCTrackHistory.Purpose,WorkWCTrackHistory.Auditid ASC
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY HistoryID,Purpose ORDER BY WCTrackHistoryID) = 1
),
EXP_RecordFlagging AS (
	SELECT
	LKP_WorkWCTrackHistory.WCTrackHistoryID AS lkp_WCTrackHistoryID,
	LKP_WorkWCTrackHistory.Auditid AS lkp_Auditid,
	CURRENT_TIMESTAMP AS ExtractDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: IIF(lkp_Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AND NOT ISNULL(lkp_SessionId),'1','0')
	IFF(lkp_Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AND lkp_SessionId IS NOT NULL, '1', '0') AS FilterFlag,
	EXP_SRC_DataCollect.PolicyId,
	EXP_SRC_DataCollect.ProcessID,
	EXP_SRC_DataCollect.Attribute,
	EXP_SRC_DataCollect.o_Value AS Value,
	LKP_LatestSessionID.SessionId AS lkp_SessionId
	FROM EXP_SRC_DataCollect
	LEFT JOIN LKP_LatestSessionID
	ON LKP_LatestSessionID.SessionId = EXP_SRC_DataCollect.SessionId AND LKP_LatestSessionID.Purpose = EXP_SRC_DataCollect.Purpose AND LKP_LatestSessionID.HistoryID = EXP_SRC_DataCollect.HistoryID
	LEFT JOIN LKP_WorkWCTrackHistory
	ON LKP_WorkWCTrackHistory.HistoryID = EXP_SRC_DataCollect.HistoryID AND LKP_WorkWCTrackHistory.Purpose = EXP_SRC_DataCollect.Purpose
),
FIL_ExcludeSubmittedRecords AS (
	SELECT
	lkp_WCTrackHistoryID AS WCTrackHistoryID, 
	ExtractDate, 
	AuditID, 
	FilterFlag, 
	PolicyId, 
	ProcessID, 
	Attribute, 
	Value
	FROM EXP_RecordFlagging
	WHERE FilterFlag='1'
),
WorkWCPolicyDetails AS (
	TRUNCATE TABLE WorkWCPolicyDetails;
	INSERT INTO WorkWCPolicyDetails
	(Auditid, ExtractDate, WCTrackHistoryID, PolicyID, ProcessID, Attribute, Value)
	SELECT 
	AuditID AS AUDITID, 
	EXTRACTDATE, 
	WCTRACKHISTORYID, 
	PolicyId AS POLICYID, 
	PROCESSID, 
	ATTRIBUTE, 
	VALUE
	FROM FIL_ExcludeSubmittedRecords
),