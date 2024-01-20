WITH
SQ_Coverage_Miscellaneous AS (
	select distinct DT.HistoryID,
	DC.ID,
	WCLM.FormNumber,
	WCLM.FormCaption
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy DP
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy WP
	on DP.Policyid=WP.Policyid
	and DP.Sessionid=WP.Sessionid
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT
	on DP.SessionId=DT.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS
	on DT.Sessionid=DS.Sessionid
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage DC
	on DT.SessionId=DC.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CoverageMiscellaneous WCM
	on DC.CoverageId=WCM.CoverageId
	and DC.SessionId=WCM.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CL_CoverageMiscellaneous WCLM
	on WCLM.WB_CoverageMiscellaneousId=WCM.WB_CoverageMiscellaneousId
	and WCLM.SessionId=WCM.SessionId
	where WCM.FormRequired=1
	and DT.HistoryID in (select max(C.HistoryID) HistoryID 
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy A
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy B
		on A.SessionId=B.SessionId
		and A.PolicyId=B.PolicyId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction C
		on A.SessionId=C.SessionId
		where C.State='Committed'
		group by B.PolicyNumber, B.PolicyVersionFormatted)
	and DT.State='Committed'
	and DT.TransactionDate between @{pipeline().parameters.START_DATE} and @{pipeline().parameters.END_DATE}
	@{pipeline().parameters.WHERE_CLAUSE}
),
SQ_WB100 AS (
	select distinct WP.PolicyNumber,
	WP.PolicyVersionFormatted,
	DP.EffectiveDate PolicyEffectiveDate,
	DP.ExpirationDate PolicyExpirationDate,
	case when DP.Status='Cancelled' and DT.EffectiveDate>=getdate() then 'InForce'
	when DP.Status='InForce' and DP.EffectiveDate>Getdate() then 'FutureInForce' 
	when DP.Status='InForce' and DP.ExpirationDate<getdate() then 'NotInForce'
	else DP.Status end PolicyStatus,
	WPT.CustomerNum,
	WA.Reference AgencyCode,
	DPTAG.Name AgencyLegalName,
	DPTA.Name PartyName,
	WP.Division,
	DL.Type InsuranceLine,
	WP.WBProduct,
	WP.PolicyProgram,
	DP.PrimaryRatingState,
	DC.Written,
	WCCM.CoverageForm,
	WCCM.PageCaption,
	cast(WCCM.Text as varchar(max)) Text,
	DC.ID CoverageGUID,
	DT.HistoryID,
	WCCM.Signature
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy DP
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy WP
	on DP.Policyid=WP.Policyid
	and DP.Sessionid=WP.Sessionid
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT
	on DP.Sessionid=DT.Sessionid
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS
	on DP.Sessionid=DS.Sessionid
	inner Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL
	on DP.SessionId=DL.Sessionid
	and DP.PolicyId=DL.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage DC
	on DP.SessionId=DC.SessionId
	and DL.LineId=DC.ObjectID
	and DC.ObjectName='DC_Line'
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CoverageMiscellaneous WCM
	on DC.CoverageId=WCM.CoverageId
	and DC.SessionId=WCM.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Party DPTA
	on DP.SessionId=DPTA.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_PartyAssociation DPAA
	on DPTA.SessionId=DPAA.SessionId
	and DPTA.PartyId=DPAA.PartyId
	and DPAA.PartyAssociationType='Account'
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Party DPTAG
	on DP.SessionId=DPTAG.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_PartyAssociation DPAAG
	on DPTAG.SessionId=DPAAG.SessionId
	and DPTAG.PartyId=DPAAG.PartyId
	and DPAAG.PartyAssociationType='Agency'
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Party WPT
	on DPTA.SessionId=WPT.SessionId
	and DPTA.PartyId=WPT.PartyId
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Agency WA
	on DPTAG.SessionId=WA.SessionId
	and DPTAG.PartyId=WA.PartyId
	left Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CL_CoverageMiscellaneous WCCM
	on DC.SessionId=WCCM.SessionId
	and WCM.WB_CoverageMiscellaneousId=WCCM.WB_CoverageMiscellaneousId
	where WCM.FormRequired=1
	and DT.State='Committed'
	and DT.HistoryID in (select max(C.HistoryID) HistoryID 
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy A
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy B
		on A.SessionId=B.SessionId
		and A.PolicyId=B.PolicyId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction C
		on A.SessionId=C.SessionId
		where C.State='Committed'
		group by B.PolicyNumber,B.PolicyVersionFormatted)
	and DT.TransactionDate between @{pipeline().parameters.START_DATE} and @{pipeline().parameters.END_DATE}
	@{pipeline().parameters.WHERE_CLAUSE_WB}
),
JNR_WB100 AS (SELECT
	SQ_WB100.PolicyNumber, 
	SQ_WB100.PolicyVersionFormatted, 
	SQ_WB100.PolicyEffectiveDate, 
	SQ_WB100.PolicyExpirationDate, 
	SQ_WB100.PolicyStatus, 
	SQ_WB100.CustomerNum, 
	SQ_WB100.AgencyCode, 
	SQ_WB100.AgencyLegalName, 
	SQ_WB100.PartyName, 
	SQ_WB100.Division, 
	SQ_WB100.InsuranceLine, 
	SQ_WB100.WBProduct, 
	SQ_WB100.PolicyProgram, 
	SQ_WB100.PrimaryRatingState, 
	SQ_WB100.Written, 
	SQ_WB100.CoverageForm, 
	SQ_WB100.PageCaption, 
	SQ_WB100.Text, 
	SQ_WB100.CoverageGUID, 
	SQ_WB100.HistoryID, 
	SQ_Coverage_Miscellaneous.HistoryID AS HistoryID1, 
	SQ_Coverage_Miscellaneous.Id, 
	SQ_WB100.Signature, 
	SQ_Coverage_Miscellaneous.FormNumber, 
	SQ_Coverage_Miscellaneous.FormCaption
	FROM SQ_WB100
	LEFT OUTER JOIN SQ_Coverage_Miscellaneous
	ON SQ_Coverage_Miscellaneous.HistoryID = SQ_WB100.HistoryID AND SQ_Coverage_Miscellaneous.Id = SQ_WB100.CoverageGUID
),
SRT_WB100 AS (
	SELECT
	PolicyNumber, 
	PolicyVersionFormatted, 
	PolicyEffectiveDate, 
	PolicyExpirationDate, 
	PolicyStatus, 
	CustomerNum, 
	AgencyCode, 
	AgencyLegalName, 
	PartyName, 
	Division, 
	InsuranceLine, 
	WBProduct, 
	PolicyProgram, 
	PrimaryRatingState, 
	Written, 
	CoverageForm, 
	PageCaption, 
	Text, 
	CoverageGUID, 
	HistoryID, 
	Signature, 
	FormNumber, 
	FormCaption
	FROM JNR_WB100
	ORDER BY PolicyNumber ASC, PolicyVersionFormatted ASC
),
EXP_WB100 AS (
	SELECT
	PolicyNumber,
	PolicyVersionFormatted,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	PolicyStatus,
	CustomerNum,
	AgencyCode,
	AgencyLegalName AS i_AgencyLegalName,
	-- *INF*: chr(34) || i_AgencyLegalName || chr(34)
	chr(34) || i_AgencyLegalName || chr(34) AS o_AgencyLegalName,
	PartyName AS i_PartyName,
	-- *INF*: chr(34) || i_PartyName || chr(34)
	chr(34) || i_PartyName || chr(34) AS o_PartyName,
	Division AS i_Division,
	-- *INF*: chr(34) || i_Division || chr(34)
	chr(34) || i_Division || chr(34) AS o_Division,
	InsuranceLine,
	WBProduct,
	PolicyProgram,
	PrimaryRatingState,
	Written,
	CoverageForm AS i_CoverageForm,
	-- *INF*: chr(34) || i_CoverageForm || chr(34)
	chr(34) || i_CoverageForm || chr(34) AS o_CoverageForm,
	PageCaption AS i_PageCaption,
	-- *INF*: chr(34) || i_PageCaption || chr(34)
	chr(34) || i_PageCaption || chr(34) AS o_PageCaption,
	Text AS i_Text,
	-- *INF*: chr(34) || REPLACECHR(0,REPLACECHR(0, REPLACECHR(0, REPLACECHR(0, i_Text, CHR(10), ' '), CHR(13),' '), CHR(9),' '),CHR(34),'') || chr(34)
	chr(34) || REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(i_Text,CHR(10),' ','i'),CHR(13),' ','i'),CHR(9),' ','i'),CHR(34),'','i') || chr(34) AS o_Text,
	CoverageGUID,
	HistoryID,
	FormNumber,
	Signature AS i_Signature,
	-- *INF*: IIF(i_Signature = '1', 'Y', 'N')
	IFF(i_Signature = '1', 'Y', 'N') AS v_Signature,
	-- *INF*: chr(34) || v_Signature || chr(34)
	chr(34) || v_Signature || chr(34) AS o_Signature,
	FormCaption AS i_FormCaption,
	-- *INF*: chr(34) || i_FormCaption || chr(34)
	chr(34) || i_FormCaption || chr(34) AS o_FormCaption
	FROM SRT_WB100
),
WB100 AS (
	INSERT INTO WB100
	(PolicyNumber, PolicyVersionFormatted, PolicyEffectiveDate, PolicyExpirationDate, PolicyStatus, CustomerNum, AgencyCode, AgencyLegalName, PartyName, Division, InsuranceLine, WBProduct, PolicyProgram, PrimaryRatingState, Written, CoverageForm, PageCaption, Text, CoverageGUID, HistoryID, Signature, FormNumber, FormCaption)
	SELECT 
	POLICYNUMBER, 
	POLICYVERSIONFORMATTED, 
	POLICYEFFECTIVEDATE, 
	POLICYEXPIRATIONDATE, 
	POLICYSTATUS, 
	CUSTOMERNUM, 
	AGENCYCODE, 
	o_AgencyLegalName AS AGENCYLEGALNAME, 
	o_PartyName AS PARTYNAME, 
	o_Division AS DIVISION, 
	INSURANCELINE, 
	WBPRODUCT, 
	POLICYPROGRAM, 
	PRIMARYRATINGSTATE, 
	WRITTEN, 
	o_CoverageForm AS COVERAGEFORM, 
	o_PageCaption AS PAGECAPTION, 
	o_Text AS TEXT, 
	COVERAGEGUID, 
	HISTORYID, 
	o_Signature AS SIGNATURE, 
	FORMNUMBER, 
	o_FormCaption AS FORMCAPTION
	FROM EXP_WB100
),