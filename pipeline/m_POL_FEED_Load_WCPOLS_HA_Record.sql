WITH
SQ_WCPols00Record AS (
	SELECT
		WCTrackHistoryID,
		LinkData,
	     AuditId
		FROM dbo.WCPols00Record
	WHERE 1=1
	AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	ORDER BY WCTrackHistoryID
),
SQ_WorkWCForms_RecordHA AS (
	SELECT distinct
		ST.WCTrackHistoryID
		,F.FormName
		,PT.Name
		,Pol.TransactionEffectiveDate
		,CON_DC.Value DesignatedContractor
		,CON_CC.Value ClassCode
		,CON_CCD.Value ClassCodeDescription
		,CON_PB.Value PremiumBasis
		,CON_RATE.Value Rate
		,CON_MP.Value MinimumPremium
		,CON_EAP.Value EstimatedAnnualPremium
	
	FROM dbo.WorkWCStateTerm ST
	
	INNER JOIN dbo.WorkWCForms F
		ON ST.WCTrackHistoryID = F.WCTrackHistoryID
	AND F.FormName like 'WC220302%' AND (F.OnPolicy=1 or F.[Add]=1) AND (F.Remove is NULL or F.Remove=0)
	
	INNER JOIN dbo.WorkWCParty PT
		ON PT.WCTrackHistoryID = ST.WCTrackHistoryID
			AND PT.PartyAssociationType = 'Account'
	
	INNER JOIN dbo.WorkWCLine L
		ON L.WCTrackHistoryID = ST.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCPolicy Pol
		ON Pol.WCTrackHistoryID = ST.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCPolicyDetails CON_DC
		ON CON_DC.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND CON_DC.Attribute='DesignatedContractor'
	
	INNER JOIN dbo.WorkWCPolicyDetails CON_CC
		ON CON_CC.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND CON_DC.ProcessID=CON_CC.ProcessID
		AND CON_CC.Attribute='ClassCode'
	
	INNER JOIN dbo.WorkWCPolicyDetails CON_CCD
		ON CON_CCD.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND CON_CCD.Attribute='ClassCodeDescription'
		AND CON_DC.ProcessID=CON_CCD.ProcessID
	
	INNER JOIN dbo.WorkWCPolicyDetails CON_PB
		ON CON_PB.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND CON_PB.Attribute='PremiumBasis'
		AND CON_DC.ProcessID=CON_PB.ProcessID
	
	INNER JOIN dbo.WorkWCPolicyDetails CON_RATE
		ON CON_RATE.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND CON_RATE.Attribute='Rate'
		AND CON_DC.ProcessID=CON_RATE.ProcessID
	
	INNER JOIN dbo.WorkWCPolicyDetails CON_MP
		ON CON_MP.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND CON_MP.Attribute='MinimumPremium'
		AND CON_DC.ProcessID=CON_MP.ProcessID
	
	INNER JOIN dbo.WorkWCPolicyDetails CON_EAP
		ON CON_EAP.WCTrackHistoryID = ST.WCTrackHistoryID 
		AND CON_EAP.Attribute='EstimatedAnnualPremium'
		AND CON_DC.ProcessID=CON_EAP.ProcessID
	
	WHERE 1 = 1
	AND ST.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_HA}
	ORDER BY ST.WCTrackHistoryID
),
JNR_RecordHA AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	SQ_WorkWCForms_RecordHA.WCTrackHistoryID AS WCTrackHistoryID1, 
	SQ_WorkWCForms_RecordHA.FormName, 
	SQ_WorkWCForms_RecordHA.Name, 
	SQ_WorkWCForms_RecordHA.TransactionEffectiveDate, 
	SQ_WorkWCForms_RecordHA.DesignatedContractor, 
	SQ_WorkWCForms_RecordHA.ClassCode, 
	SQ_WorkWCForms_RecordHA.ClassCodeDescription, 
	SQ_WorkWCForms_RecordHA.PremiumBasis, 
	SQ_WorkWCForms_RecordHA.Rate, 
	SQ_WorkWCForms_RecordHA.MinimumPremium, 
	SQ_WorkWCForms_RecordHA.EstimatedAnnualPremium
	FROM SQ_WCPols00Record
	INNER JOIN SQ_WorkWCForms_RecordHA
	ON SQ_WorkWCForms_RecordHA.WCTrackHistoryID = SQ_WCPols00Record.WCTrackHistoryID
),
mplt_Parse_FormNameField AS (WITH
	INPUT_FormName AS (
		
	),
	EXPTRANS AS (
		SELECT
		ParsedNameOfForm,
		FormNameFromSource,
		-- *INF*: REVERSE(FormNameFromSource)
		REVERSE(FormNameFromSource) AS vReversedFromNameFromSource,
		-- *INF*: REVERSE(substr(vReversedFromNameFromSource,1,4))
		REVERSE(substr(vReversedFromNameFromSource, 1, 4)) AS vFormEdition,
		-- *INF*: DECODE(TRUE,
		-- substr(vReversedFromNameFromSource,5,1) >='A' and substr(vReversedFromNameFromSource,5,1) <='Z', substr(vReversedFromNameFromSource,5,1),
		-- ' '
		-- )
		-- 
		-- -- check if within A and Z, if not then space
		DECODE(
		    TRUE,
		    substr(vReversedFromNameFromSource, 5, 1) >= 'A' and substr(vReversedFromNameFromSource, 5, 1) <= 'Z', substr(vReversedFromNameFromSource, 5, 1),
		    ' '
		) AS vBureauCode,
		vFormEdition AS oFormEdition,
		vBureauCode AS oBureauCode
		FROM INPUT_FormName
	),
	OUTPUT_FormName AS (
		SELECT
		ParsedNameOfForm, 
		FormNameFromSource, 
		oFormEdition AS FormEdition, 
		oBureauCode AS BureauCode
		FROM EXPTRANS
	),
),
EXP_RecordOutput AS (
	SELECT
	SYSDATE AS ExtractDate,
	JNR_RecordHA.AuditId,
	JNR_RecordHA.WCTrackHistoryID,
	JNR_RecordHA.LinkData,
	'22' AS StateCode,
	'HA' AS RecordTypeCode,
	'WC220302' AS o_EndorsementNumber,
	mplt_Parse_FormNameField.ParsedNameOfForm1,
	mplt_Parse_FormNameField.FormNameFromSource1,
	mplt_Parse_FormNameField.BureauCode,
	mplt_Parse_FormNameField.FormEdition,
	JNR_RecordHA.DesignatedContractor,
	JNR_RecordHA.ClassCode,
	JNR_RecordHA.ClassCodeDescription,
	JNR_RecordHA.PremiumBasis,
	JNR_RecordHA.Rate,
	-- *INF*: TO_CHAR(TO_DECIMAL(Rate,3)*1000)
	TO_CHAR(CAST(Rate AS FLOAT) * 1000) AS v_Rate,
	v_Rate AS o_Rate,
	JNR_RecordHA.MinimumPremium,
	JNR_RecordHA.EstimatedAnnualPremium,
	JNR_RecordHA.Name,
	JNR_RecordHA.TransactionEffectiveDate,
	-- *INF*: TO_CHAR(TransactionEffectiveDate,'YYMMDD')
	TO_CHAR(TransactionEffectiveDate, 'YYMMDD') AS o_TransactionEffectiveDate
	FROM JNR_RecordHA
	 -- Manually join with mplt_Parse_FormNameField
),
WCPolsHARecord AS (
	INSERT INTO WCPolsHARecord
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, StateCode, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, NameOfIndependentContractor, ClassificationCode, ClassificationWording, RateChargedRate, MinimumPremiumAmount, EstimatedAnnualPremiumAmount, NameOfInsured, EndorsementEffectiveDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	STATECODE, 
	RECORDTYPECODE, 
	o_EndorsementNumber AS ENDORSEMENTNUMBER, 
	BureauCode AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER, 
	FormEdition AS CARRIERVERSIONIDENTIFIER, 
	DesignatedContractor AS NAMEOFINDEPENDENTCONTRACTOR, 
	ClassCode AS CLASSIFICATIONCODE, 
	ClassCodeDescription AS CLASSIFICATIONWORDING, 
	o_Rate AS RATECHARGEDRATE, 
	MinimumPremium AS MINIMUMPREMIUMAMOUNT, 
	EstimatedAnnualPremium AS ESTIMATEDANNUALPREMIUMAMOUNT, 
	Name AS NAMEOFINSURED, 
	o_TransactionEffectiveDate AS ENDORSEMENTEFFECTIVEDATE
	FROM EXP_RecordOutput
),