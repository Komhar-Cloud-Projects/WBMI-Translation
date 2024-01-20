WITH
SQ_DC_Policy_BP AS (
	with locbuild 
	as (
		select distinct
		dcp.PolicyNumber,
		wbp.PolicyVersionFormatted,
		LocBuildNumber = 
			stuff((select distinct ', ' + concat('Loc ',  bploc2.Number, ', Build ',  wrsk2.BuildingNumber)
				from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy dcp2 with(nolock)
				inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy wbp2 with(nolock) 
					on dcp2.PolicyId = wbp2.PolicyId
					and wbp2.SessionId = dcp2.SessionId
				inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Party wbpartyCust2 with(nolock) 
					on wbpartyCust2.SessionId = wbp2.SessionId 
					and wbpartyCust2.CustomerNum is not null
				inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage dc2 with(nolock)
					on dc2.SessionId = dcp2.SessionId
				inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_BP_Risk rsk2 with (nolock)
					on rsk2.BP_RiskId = dc2.ObjectId
					and dc2.ObjectName = 'DC_BP_Risk'
					and dc2.SessionId = dcp2.SessionId
				inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_BP_Risk wrsk2 with (nolock)
					on wrsk2.BP_RiskId = rsk2.BP_RiskId
					and wrsk2.SessionId = dcp2.SessionId
				inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_BP_Building bpbuild2 with (nolock)
					on bpbuild2.BP_BuildingId = rsk2.BP_BuildingId
				inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_BP_Location bploc2 with (nolock)
					on bploc2.BP_LocationId = bpbuild2.BP_LocationId
				where dcp2.PolicyNumber = dcp.PolicyNumber
				and wbp2.PolicyVersionFormatted = wbp.PolicyVersionFormatted
				and dc2.Type = 'FloodRisk'
				for xml path('')), 1, 2, '')
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy dcp with(nolock)
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy wbp with(nolock) 
			on dcp.PolicyId = wbp.PolicyId
			and wbp.SessionId = dcp.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction t with(nolock)
			on t.SessionId = dcp.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Party wbpartyCust with(nolock) 
			on wbpartyCust.SessionId = wbp.SessionId 
			and wbpartyCust.CustomerNum is not null
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage dc with(nolock)
			on dc.SessionId = dcp.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_BP_Risk rsk with (nolock)
			on rsk.BP_RiskId = dc.ObjectId
			and dc.ObjectName = 'DC_BP_Risk'
			and dc.SessionId = dcp.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_BP_Risk wrsk with (nolock)
			on wrsk.BP_RiskId = rsk.BP_RiskId
			and wrsk.SessionId = dcp.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_BP_Building bpbuild with (nolock)
			on bpbuild.BP_BuildingId = rsk.BP_BuildingId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_BP_Location bploc with (nolock)
			on bploc.BP_LocationId = bpbuild.BP_LocationId
		where dc.Type = 'FloodRisk'
		and t.Type not in ('Information', 'CancelPending', 'RescindCancelPending', 'NonRenew') --KAY: ADDED THIS LINE
		@{pipeline().parameters.PREV_MONTH}
		@{pipeline().parameters.PREV_MONTH_YEAR}
		@{pipeline().parameters.WHERE_CLAUSE_BP_LOCATION}
		group by dcp.PolicyNumber, wbp.PolicyVersionFormatted
	),
	
	
	FloodValues 
	as (
		select 
		dcp.PolicyNumber,
		wbp.PolicyVersionFormatted,
		dcp.sessionID,
		sum(dc.Change) as FloodChange,
		sum(dc.written) as FloodWritten			
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy dcp with(nolock)
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy wbp with(nolock) 
			on dcp.PolicyId = wbp.PolicyId
			and wbp.SessionId = dcp.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction t with(nolock)
			on t.SessionId = dcp.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage dc with(nolock)
			on dc.SessionId = dcp.SessionId
		where wbp.WBProduct = 'SMARTbusiness'
		and dc.Type = 'FloodRisk'
		and t.Type not in ('Information', 'CancelPending', 'RescindCancelPending', 'NonRenew') 
		and (dc.Change <> 0 or dc.Written <> 0 or dc.Premium <> 0) 
		@{pipeline().parameters.PREV_MONTH}
		@{pipeline().parameters.PREV_MONTH_YEAR}
		@{pipeline().parameters.WHERE_CLAUSE_BP_FLOODVALUES}
		group by dcp.PolicyNumber, wbp.PolicyVersionFormatted, dcp.sessionID
	)
	
	
	(select distinct t.Type as TransactionType
	,t.EffectiveDate as TransactionEffectiveDate
	,dcp.PolicyNumber as PolicyNumber
	,wbp.PolicyVersionFormatted as PolicyMod
	,wbpartyCust.CustomerNum as CustomerNumber
	,dcparty.Name as NamedInsured
	,wbag.Reference as AgencyCode
	,dcpartyag.Name as AgencyName
	,wbp.Division as Division
	,wbp.WBProduct as Product
	,wbp.PolicyIssueCodeDesc as PolicyIssueCodeDescription
	,dcp.PrimaryRatingState as RatingState
	,wbp.BCCCode as BCCCode
	,wbp.BCCCodeDesc as BCCCodeDescription
	,wbp.PolicyProgram as PolicyProgram
	,locbuild.LocBuildNumber as LocationsAndBuildings
	,bpla.FloodZone as FloodZone
	,wbp.TotalFloodLimit as TotalFloodLimit
	,wbp.TotalFloodDeductible as TotalFloodDeductible
	,'Yes' as 'BlanketFlood' --Identifies if flood premium is from Blanket coverage.
	,bpline.TotalFloodChangePremium as TotalFloodChangePremium
	,bpline.TotalFloodWrittenPremium as TotalFloodWrittenPremium 
	,ps.Written as PolicyPremium
	,bpfr.CertificateReceivedDate as CertificateReceived
	,bpfr.ReinsurerName as ReinsurerName
	,bpfr.Type as Type
	,bpfr.AmountCeded as AmountCeded
	,bpfr.Premium as ReinsurerPremium
	,t.TransactionDate as TransactionDate
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy dcp with(nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy wbp with(nolock) 
		on dcp.PolicyId = wbp.PolicyId
		and wbp.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line dcl with(nolock) 
		on dcl.PolicyId = dcp.PolicyId
		and dcl.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Line wbl with(nolock) 
		on dcl.LineId = wbl.LineId
		and wbl.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Party dcparty with(nolock) 
		on dcparty.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Party wbpartyCust with(nolock) 
		on wbpartyCust.SessionId = dcp.SessionId
		and dcparty.PartyId = wbpartyCust.PartyId
		and wbpartyCust.CustomerNum is not null
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Agency wbag with(nolock)
		on wbag.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_PartyAssociation dpa with(nolock) 
		on dpa.SessionId = dcparty.SessionId
		and dcparty.PartyId = dpa.PartyId
		and dpa.PartyAssociationType = 'Account'
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Party dcpartyag with(nolock) 
		on dcpartyag.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_PartyAssociation dpag with(nolock) 
		on dpag.SessionId = dcparty.SessionId
		and dcpartyag.PartyId = dpag.PartyId
		and dpag.PartyAssociationType = 'Agency'
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_BP_FacultativeReinsurer bpfr with(nolock)
		on bpfr.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_PremiumSubtotal ps with(nolock) 
		on ps.SessionId = dcp.SessionId
		and ps.ObjectName = 'DC_Policy'
		and ps.ObjectId = dcp.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_BP_LocationAccount bpla with(nolock)
		on bpla.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_BP_Line bpline with(nolock)
		on bpline.SessionId = dcp.SessionId
	inner join locbuild locbuild with (nolock)
		on locbuild.PolicyNumber = dcp.PolicyNumber
		and locbuild.PolicyVersionFormatted = wbp.PolicyVersionFormatted
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction t with(nolock)
		on t.SessionId = dcp.SessionId
	where wbp.WBProduct = 'SMARTbusiness'
	and ps.Type = 'Policy'
	and t.Type not in ('Information', 'CancelPending', 'RescindCancelPending', 'NonRenew')
	and ((bpline.TotalFloodPremium is not NULL and bpline.TotalFloodChangePremium is not NULL and bpline.TotalFloodWrittenPremium is not NULL) --KAY: ADDED THIS LINE so only blanket records are created from this logic
		AND (bpline.TotalFloodPremium <> 0 or bpline.TotalFloodChangePremium <> 0 or bpline.TotalFloodWrittenPremium <> 0)) --KAY: ADDED THIS LINE so only blanket records are created from this logic
	@{pipeline().parameters.PREV_MONTH}
	@{pipeline().parameters.PREV_MONTH_YEAR}
	@{pipeline().parameters.WHERE_CLAUSE_BP}
	
	UNION 
	
	select distinct t.Type as TransactionType
	,t.EffectiveDate as TransactionEffectiveDate
	,dcp.PolicyNumber as PolicyNumber
	,wbp.PolicyVersionFormatted as PolicyMod
	,wbpartyCust.CustomerNum as CustomerNumber
	,dcparty.Name as NamedInsured
	,wbag.Reference as AgencyCode
	,dcpartyag.Name as AgencyName
	,wbp.Division as Division
	,wbp.WBProduct as Product
	,wbp.PolicyIssueCodeDesc as PolicyIssueCodeDescription
	,dcp.PrimaryRatingState as RatingState
	,wbp.BCCCode as BCCCode
	,wbp.BCCCodeDesc as BCCCodeDescription
	,wbp.PolicyProgram as PolicyProgram
	,locbuild.LocBuildNumber as LocationsAndBuildings
	,bpla.FloodZone as FloodZone
	,wbp.TotalFloodLimit as TotalFloodLimit
	,wbp.TotalFloodDeductible as TotalFloodDeductible
	,'No' as 'BlanketFlood'
	,fv.FloodChange as TotalFloodChangePremium
	,fv.FloodWritten as TotalFloodWrittenPremium
	,ps.Written as PolicyPremium
	,bpfr.CertificateReceivedDate as CertificateReceived
	,bpfr.ReinsurerName as ReinsurerName
	,bpfr.Type as Type
	,bpfr.AmountCeded as AmountCeded
	,bpfr.Premium as ReinsurerPremium
	,t.TransactionDate as TransactionDate
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy dcp with(nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy wbp with(nolock) 
		on dcp.PolicyId = wbp.PolicyId
		and wbp.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line dcl with(nolock) 
		on dcl.PolicyId = dcp.PolicyId
		and dcl.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Line wbl with(nolock) 
		on dcl.LineId = wbl.LineId
		and wbl.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Party dcparty with(nolock) 
		on dcparty.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Party wbpartyCust with(nolock) 
		on wbpartyCust.SessionId = dcp.SessionId
		and dcparty.PartyId = wbpartyCust.PartyId
		and wbpartyCust.CustomerNum is not null
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Agency wbag with(nolock)
		on wbag.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_PartyAssociation dpa with(nolock) 
		on dpa.SessionId = dcparty.SessionId
		and dcparty.PartyId = dpa.PartyId
		and dpa.PartyAssociationType = 'Account'
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Party dcpartyag with(nolock) 
		on dcpartyag.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_PartyAssociation dpag with(nolock) 
		on dpag.SessionId = dcparty.SessionId
		and dcpartyag.PartyId = dpag.PartyId
		and dpag.PartyAssociationType = 'Agency'
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_BP_FacultativeReinsurer bpfr with(nolock)
		on bpfr.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_PremiumSubtotal ps with(nolock) 
		on ps.SessionId = dcp.SessionId
		and ps.ObjectName = 'DC_Policy'
		and ps.ObjectId = dcp.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_BP_LocationAccount bpla with(nolock)
		on bpla.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_BP_Line bpline with(nolock)
		on bpline.SessionId = dcp.SessionId
	inner join locbuild locbuild with (nolock)
		on locbuild.PolicyNumber = dcp.PolicyNumber
		and locbuild.PolicyVersionFormatted = wbp.PolicyVersionFormatted
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction t with(nolock)
		on t.SessionId = dcp.SessionId
	inner join FloodValues fv with (nolock)
		on fv.PolicyNumber = dcp.PolicyNumber
		and fv.PolicyVersionFormatted = wbp.PolicyVersionFormatted
		and fv.SessionId = t.SessionId
	where wbp.WBProduct = 'SMARTbusiness'
	and ps.Type = 'Policy'
	and t.Type not in ('Information', 'CancelPending', 'RescindCancelPending', 'NonRenew')
	@{pipeline().parameters.PREV_MONTH}
	@{pipeline().parameters.PREV_MONTH_YEAR}
	@{pipeline().parameters.WHERE_CLAUSE_BP2})
),
SQ_DC_Policy_CF AS (
	with locbuild 
	as (
		select distinct
		dcp.PolicyNumber,
		wbp.PolicyVersionFormatted,
		LocBuildNumber = 
			stuff((select distinct ', ' + concat('Loc ',  cfloc2.Number, ', Build ',  cfbuild2.LocationBuildingNumber)
				from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy dcp2 with(nolock)
				inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy wbp2 with(nolock) 
					on dcp2.PolicyId = wbp2.PolicyId
					and wbp2.SessionId = dcp2.SessionId
				inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Party wbpartyCust2 with(nolock) 
					on wbpartyCust2.SessionId = wbp2.SessionId 
					and wbpartyCust2.CustomerNum is not null
				inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage dc2 with(nolock)
					on dc2.SessionId = dcp2.SessionId
				inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_CF_Risk rsk2 with (nolock)
					on rsk2.CF_RiskId = dc2.ObjectId
					and dc2.ObjectName = 'DC_CF_Risk'
					and rsk2.SessionId = dcp2.SessionId
				inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_CF_Building cfbuild2 with (nolock)
					on cfbuild2.CF_BuildingId = rsk2.CF_BuildingId
				inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_CF_Location cfloc2 with (nolock)
					on cfloc2.CF_LocationId = cfbuild2.CF_LocationId
				where dcp2.PolicyNumber = dcp.PolicyNumber
				and wbp2.PolicyVersionFormatted = wbp.PolicyVersionFormatted
				and dc2.Type = 'FloodRisk'
				for xml path('')), 1, 2, '')
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy dcp with(nolock)
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy wbp with(nolock) 
			on dcp.PolicyId = wbp.PolicyId
			and wbp.SessionId = dcp.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction t with(nolock)
			on t.SessionId = dcp.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Party wbpartyCust with(nolock) 
			on wbpartyCust.SessionId = wbp.SessionId 
			and wbpartyCust.CustomerNum is not null
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage dc with(nolock)
			on dc.SessionId = dcp.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_CF_Risk rsk with (nolock)
			on rsk.CF_RiskId = dc.ObjectId
			and dc.ObjectName = 'DC_CF_Risk'
			and rsk.SessionId = dcp.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_CF_Building cfbuild with (nolock)
			on cfbuild.CF_BuildingId = rsk.CF_BuildingId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_CF_Location cfloc with (nolock)
			on cfloc.CF_LocationId = cfbuild.CF_LocationId
		where dc.Type = 'FloodRisk'
		and t.Type not in ('Information', 'CancelPending', 'RescindCancelPending', 'NonRenew')
		@{pipeline().parameters.PREV_MONTH}
		@{pipeline().parameters.PREV_MONTH_YEAR}
		@{pipeline().parameters.WHERE_CLAUSE_CF_LOCATION}
		group by dcp.PolicyNumber, wbp.PolicyVersionFormatted
	
	)
	
	
	select distinct t.Type as TransactionType
	,t.EffectiveDate as TransactionEffectiveDate
	,dcp.PolicyNumber as PolicyNumber
	,wbp.PolicyVersionFormatted as PolicyMod
	,wbpartyCust.CustomerNum as CustomerNumber
	,dcparty.Name as NamedInsured
	,wbag.Reference as AgencyCode
	,dcpartyag.Name as AgencyName
	,wbp.Division as Division
	,wbp.WBProduct as Product
	,wbp.PolicyIssueCodeDesc as PolicyIssueCodeDescription
	,dcp.PrimaryRatingState as RatingState
	,wbp.BCCCode as BCCCode
	,wbp.BCCCodeDesc as BCCCodeDescription
	,wbp.PolicyProgram as PolicyProgram
	,locbuild.LocBuildNumber as LocationsAndBuildings
	,cflp.FloodZone as FloodZone
	,wbp.TotalFloodLimit as TotalFloodLimit
	,wbp.TotalFloodDeductible as TotalFloodDeductible
	,'Yes' as 'BlanketFlood' -- Identifies if flood premium is from Blanket coverage. All flood premium on CPP is from Blanket. 
	,cfline.TotalFloodChangePremium as TotalFloodChangePremium 
	,cfline.TotalFloodWrittenPremium as TotalFloodWrittenPremium
	,ps.Written as PolicyPremium
	,cffr.CertificateReceived as CertificateReceived
	,cffr.ReinsurerName as ReinsurerName
	,cffr.Type as Type
	,cffr.AmountCeded as AmountCeded
	,cffr.ReinsurerPremium as ReinsurerPremium
	,t.TransactionDate as TransactionDate 
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy dcp with(nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy wbp with(nolock) 
		on dcp.PolicyId = wbp.PolicyId
		and wbp.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line dcl with(nolock) 
		on dcl.PolicyId = dcp.PolicyId
		and dcl.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Line wbl with(nolock) 
		on dcl.LineId = wbl.LineId
		and wbl.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Party dcparty with(nolock) 
		on dcparty.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Party wbpartyCust with(nolock) 
		on wbpartyCust.SessionId = dcp.SessionId
		and dcparty.PartyId = wbpartyCust.PartyId
		and wbpartyCust.CustomerNum is not null
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Agency wbag with(nolock)
		on wbag.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_PartyAssociation dpa with(nolock) 
		on dpa.SessionId = dcparty.SessionId
		and dcparty.PartyId = dpa.PartyId
		and dpa.PartyAssociationType = 'Account'
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Party dcpartyag with(nolock) 
		on dcpartyag.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_PartyAssociation dpag with(nolock) 
		on dpag.SessionId = dcparty.SessionId
		and dcpartyag.PartyId = dpag.PartyId
		and dpag.PartyAssociationType = 'Agency'
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CF_FacultativeReinsurer cffr with(nolock) 
		on cffr.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_PremiumSubtotal ps with(nolock) 
		on ps.SessionId = dcp.SessionId
		and ps.ObjectName = 'DC_Policy'
		and ps.ObjectId = dcp.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CF_LocationProperty cflp with(nolock)
		on cflp.SessionId = dcp.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CF_Line cfline with(nolock)
		on cfline.SessionId = dcp.SessionId
	inner join locbuild locbuild with (nolock)
		on locbuild.PolicyNumber = dcp.PolicyNumber
		and locbuild.PolicyVersionFormatted = wbp.PolicyVersionFormatted
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction t with(nolock)
		on t.SessionId = dcp.SessionId
	where wbp.WBProduct = 'Commercial Package'
	and ps.Type = 'Policy'
	and t.Type not in ('Information', 'CancelPending', 'RescindCancelPending', 'NonRenew')
	@{pipeline().parameters.PREV_MONTH}
	@{pipeline().parameters.PREV_MONTH_YEAR}
	@{pipeline().parameters.WHERE_CLAUSE_CF}
),
Union AS (
	SELECT TransactionType, TransactionEffectiveDate, PolicyNumber, PolicyMod, CustomerNumber, NamedInsured, AgencyCode, AgencyName, Division, Product, PolicyIssueCodeDescription, RatingState, BCCCode, BCCCodeDescription, PolicyProgram, LocationsAndBuildings, FloodZone, TotalFloodLimit, TotalFloodDeductible, BlanketFlood, TotalFloodChangePremium, TotalFloodWrittenPremium, PolicyPremium, CertificateReceived, ReinsurerName, Type, AmountCeded, ReinsurerPremium, TransactionDate
	FROM SQ_DC_Policy_CF
	UNION
	SELECT TransactionType, TransactionEffectiveDate, PolicyNumber, PolicyMod, CustomerNumber, NamedInsured, AgencyCode, AgencyName, Division, Product, PolicyIssueCodeDescription, RatingState, BCCCode, BCCCodeDescription, PolicyProgram, LocationsAndBuildings, FloodZone, TotalFloodLimit, TotalFloodDeductible, BlanketFlood, TotalFloodChangePremium, TotalFloodWrittenPremium, PolicyPremium, CertificateReceived, ReinsurerName, Type, AmountCeded, ReinsurerPremium, TransactionDate
	FROM SQ_DC_Policy_BP
),
SRT_BlanketFlood AS (
	SELECT
	PolicyNumber, 
	PolicyMod, 
	TransactionEffectiveDate, 
	TransactionDate, 
	TransactionType, 
	CustomerNumber, 
	NamedInsured, 
	AgencyCode, 
	AgencyName, 
	Division, 
	Product, 
	PolicyIssueCodeDescription, 
	RatingState, 
	BCCCode, 
	BCCCodeDescription, 
	PolicyProgram, 
	LocationsAndBuildings, 
	FloodZone, 
	TotalFloodLimit, 
	TotalFloodDeductible, 
	BlanketFlood, 
	TotalFloodChangePremium, 
	TotalFloodWrittenPremium, 
	PolicyPremium, 
	CertificateReceived, 
	ReinsurerName, 
	Type, 
	AmountCeded, 
	ReinsurerPremium
	FROM Union
	ORDER BY PolicyNumber ASC, PolicyMod ASC, TransactionEffectiveDate ASC, TransactionDate ASC
),
LKP_Program AS (
	SELECT
	ProgramDescription,
	lkp_PolicyProgram
	FROM (
		SELECT ProgramDescription as ProgramDescription,
		ProgramCode as lkp_PolicyProgram 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER_RPT_EDM}.Program
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY lkp_PolicyProgram ORDER BY ProgramDescription) = 1
),
EXP_BlanketFlood AS (
	SELECT
	SRT_BlanketFlood.TransactionType,
	SRT_BlanketFlood.TransactionEffectiveDate,
	SRT_BlanketFlood.PolicyNumber AS i_PolicyNumber,
	-- *INF*: CHR(39) || i_PolicyNumber || CHR(39)
	CHR(39) || i_PolicyNumber || CHR(39) AS v_PolicyNumber,
	v_PolicyNumber AS o_PolicyNumber,
	SRT_BlanketFlood.PolicyMod AS i_PolicyMod,
	-- *INF*: CHR(39) || i_PolicyMod || CHR(39)
	CHR(39) || i_PolicyMod || CHR(39) AS v_PolicyMod,
	v_PolicyMod AS o_PolicyMod,
	SRT_BlanketFlood.CustomerNumber AS i_CustomerNumber,
	-- *INF*: CHR(39) || i_CustomerNumber || CHR(39)
	CHR(39) || i_CustomerNumber || CHR(39) AS v_CustomerNumber,
	v_CustomerNumber AS o_CustomerNumber,
	SRT_BlanketFlood.NamedInsured AS i_NamedInsured,
	-- *INF*: CHR(34) || i_NamedInsured || CHR(34)
	CHR(34) || i_NamedInsured || CHR(34) AS v_NamedInsured,
	v_NamedInsured AS o_NamedInsured,
	SRT_BlanketFlood.AgencyCode,
	SRT_BlanketFlood.AgencyName AS i_AgencyName,
	-- *INF*: CHR(34) || i_AgencyName || CHR(34)
	CHR(34) || i_AgencyName || CHR(34) AS v_AgencyName,
	v_AgencyName AS o_AgencyName,
	SRT_BlanketFlood.Division,
	SRT_BlanketFlood.Product,
	SRT_BlanketFlood.PolicyIssueCodeDescription,
	SRT_BlanketFlood.RatingState,
	SRT_BlanketFlood.BCCCode AS i_BCCCode,
	-- *INF*: IIF(ISNULL(i_BCCCode) = 1, 'N/A', CHR(39) || i_BCCCode || CHR(39))
	IFF(i_BCCCode IS NULL = 1, 'N/A', CHR(39) || i_BCCCode || CHR(39)) AS v_BCCCode,
	v_BCCCode AS o_BCCCode,
	SRT_BlanketFlood.BCCCodeDescription AS i_BCCCodeDescription,
	-- *INF*: IIF(ISNULL(i_BCCCodeDescription) = 1, 'N/A', CHR(34) || i_BCCCodeDescription || CHR(34))
	IFF(i_BCCCodeDescription IS NULL = 1, 'N/A', CHR(34) || i_BCCCodeDescription || CHR(34)) AS v_BCCCodeDescription,
	v_BCCCodeDescription AS o_BCCCodeDescription,
	LKP_Program.ProgramDescription,
	SRT_BlanketFlood.LocationsAndBuildings AS i_LocationsAndBuildings,
	-- *INF*: CHR(34) || i_LocationsAndBuildings || CHR(34)
	CHR(34) || i_LocationsAndBuildings || CHR(34) AS v_LocationsAndBuildings,
	v_LocationsAndBuildings AS o_LocationsAndBuildings,
	SRT_BlanketFlood.FloodZone,
	SRT_BlanketFlood.TotalFloodLimit,
	SRT_BlanketFlood.TotalFloodDeductible,
	SRT_BlanketFlood.BlanketFlood,
	SRT_BlanketFlood.TotalFloodChangePremium,
	SRT_BlanketFlood.TotalFloodWrittenPremium,
	SRT_BlanketFlood.PolicyPremium,
	SRT_BlanketFlood.CertificateReceived AS i_CertificateReceived,
	-- *INF*: IIF(ISNULL(i_CertificateReceived) = 1, 'N/A', TO_CHAR(i_CertificateReceived, 'MM/DD/YYYY'))
	IFF(i_CertificateReceived IS NULL = 1, 'N/A', TO_CHAR(i_CertificateReceived, 'MM/DD/YYYY')) AS o_CertificateReceived,
	SRT_BlanketFlood.ReinsurerName AS i_ReinsurerName,
	-- *INF*: IIF(ISNULL(i_ReinsurerName ) = 1, 'N/A', CHR(34) || i_ReinsurerName || CHR(34))
	IFF(i_ReinsurerName IS NULL = 1, 'N/A', CHR(34) || i_ReinsurerName || CHR(34)) AS v_ReinsurerName,
	v_ReinsurerName AS o_ReinsurerName,
	SRT_BlanketFlood.Type AS i_Type,
	-- *INF*: IIF(ISNULL(i_Type) = 1, 'N/A', i_Type)
	IFF(i_Type IS NULL = 1, 'N/A', i_Type) AS o_Type,
	SRT_BlanketFlood.AmountCeded AS i_AmountCeded,
	-- *INF*: IIF(ISNULL(i_AmountCeded) = 1, 'N/A', TO_CHAR(i_AmountCeded))
	IFF(i_AmountCeded IS NULL = 1, 'N/A', TO_CHAR(i_AmountCeded)) AS o_AmountCeded,
	SRT_BlanketFlood.ReinsurerPremium AS i_ReinsurerPremium,
	-- *INF*: IIF(ISNULL(i_ReinsurerPremium) = 1, 'N/A', TO_CHAR(i_ReinsurerPremium))
	IFF(i_ReinsurerPremium IS NULL = 1, 'N/A', TO_CHAR(i_ReinsurerPremium)) AS o_ReinsurerPremium,
	SRT_BlanketFlood.TransactionDate
	FROM SRT_BlanketFlood
	LEFT JOIN LKP_Program
	ON LKP_Program.lkp_PolicyProgram = SRT_BlanketFlood.PolicyProgram
),
FloodReport AS (
	INSERT INTO BlanketFloodReport_ProductManagement
	(TransactionType, TransactionEffectiveDate, PolicyNumber, PolicyMod, CustomerNumber, NamedInsured, AgencyCode, AgencyName, Division, Product, PolicyIssueCodeDescription, RatingState, BCCCode, BCCCodeDescription, ProgramDescription, LocationsAndBuildings, FloodZone, TotalFloodLimit, TotalFloodDeductible, BlanketFlood, TotalFloodChangePremium, TotalFloodWrittenPremium, PolicyPremium, CertificateReceived, ReinsurerName, Type, AmountCeded, ReinsurerPremium)
	SELECT 
	TRANSACTIONTYPE, 
	TRANSACTIONEFFECTIVEDATE, 
	o_PolicyNumber AS POLICYNUMBER, 
	o_PolicyMod AS POLICYMOD, 
	o_CustomerNumber AS CUSTOMERNUMBER, 
	o_NamedInsured AS NAMEDINSURED, 
	AGENCYCODE, 
	o_AgencyName AS AGENCYNAME, 
	DIVISION, 
	PRODUCT, 
	POLICYISSUECODEDESCRIPTION, 
	RATINGSTATE, 
	o_BCCCode AS BCCCODE, 
	o_BCCCodeDescription AS BCCCODEDESCRIPTION, 
	PROGRAMDESCRIPTION, 
	o_LocationsAndBuildings AS LOCATIONSANDBUILDINGS, 
	FLOODZONE, 
	TOTALFLOODLIMIT, 
	TOTALFLOODDEDUCTIBLE, 
	BLANKETFLOOD, 
	TOTALFLOODCHANGEPREMIUM, 
	TOTALFLOODWRITTENPREMIUM, 
	POLICYPREMIUM, 
	o_CertificateReceived AS CERTIFICATERECEIVED, 
	o_ReinsurerName AS REINSURERNAME, 
	o_Type AS TYPE, 
	o_AmountCeded AS AMOUNTCEDED, 
	o_ReinsurerPremium AS REINSURERPREMIUM
	FROM EXP_BlanketFlood
),