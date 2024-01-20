WITH
LKP_SupWCPOLS AS (
	SELECT
	WCPOLSCode,
	SourcesystemID,
	SourceCode,
	TableName,
	ProcessName,
	i_SourcesystemID,
	i_SourceCode,
	i_TableName,
	i_ProcessName
	FROM (
		SELECT
		     WCPOLSCode as WCPOLSCode
			,SourcesystemID as SourcesystemID
			,SourceCode as SourceCode
			,TableName as TableName
			,ProcessName as ProcessName
		FROM SupWCPOLS
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SourcesystemID,SourceCode,TableName,ProcessName ORDER BY WCPOLSCode) = 1
),
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
SQ_WorkWCForms AS (
	SELECT distinct S.WCTrackHistoryID
		,S.State
		,F.FormName
		,D_DT.StatCodeValue LossesSubjectToDeductibleCode
		,D_DB.StatCodeValue BasisOfDeductibleCalculationCode
		,D_MD.DeductibleValue MedicalDeductible
		,D_SDC.DeductibleValue SmallDeductibleCredit
		,P.TransactionDate
		,P.TransactionEffectiveDate
	FROM dbo.WorkWCStateTerm S
	
	INNER JOIN dbo.WorkWCPolicy P
		ON P.WCTrackHistoryID = S.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCCoverage C
		ON C.WCTrackHistoryID = S.WCTrackHistoryID
			AND C.ParentObjectId = S.WC_StateTermId
			AND C.ParentObjectName = 'DC_WC_StateTerm'
			AND C.ParentCoverageType = 'SmallDeductibleCredit' -- Per Amy C.
	
	LEFT JOIN dbo.WorkWCSTATDeductible D_MD
		ON D_MD.WCTrackHistoryID = C.WCTrackHistoryID
			AND D_MD.CoverageId = C.ParentCoverageId
			AND D_MD.CoverageType = C.ParentCoverageType
			AND D_MD.DeductibleType = 'MedicalDeductible'
	
	LEFT JOIN dbo.WorkWCSTATDeductible D_SDC
		ON D_SDC.WCTrackHistoryID = C.WCTrackHistoryID
			AND D_SDC.CoverageId = C.ParentCoverageId
			AND D_SDC.CoverageType = C.ParentCoverageType
			AND D_SDC.DeductibleType = 'SmallDeductibleCredit'
	
	LEFT JOIN dbo.WorkWCSTATDeductible D_DB
		ON D_DB.WCTrackHistoryID = C.WCTrackHistoryID
			AND D_DB.CoverageId = C.ParentCoverageId
			AND D_DB.CoverageType = C.ParentCoverageType
			AND D_DB.DeductibleType = 'SmallDeductibleCredit'
			AND D_DB.StatCodeType='DeductibleBasis'
	
	LEFT JOIN dbo.WorkWCSTATDeductible D_DT
		ON D_DT.WCTrackHistoryID = C.WCTrackHistoryID
			AND D_DT.CoverageId = C.ParentCoverageId
			AND D_DT.CoverageType = C.ParentCoverageType
			AND D_DT.DeductibleType = 'SmallDeductibleCredit'
			AND D_DT.StatCodeType='DeductibleType'
	
	INNER JOIN dbo.WorkWCForms F
		ON F.WCTrackHistoryID = S.WCTrackHistoryID
			AND (F.OnPolicy = '1' OR F.[Add] = '1')
			AND (F.[Remove] IS NULL OR F.[Remove] = '0')
			AND F.FormName IS NOT NULL
	
	WHERE 1 = 1
	AND S.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_FORMNAMES}
	@{pipeline().parameters.WHERE_CLAUSE_43}
	
	UNION
	
	--- Logic specific to IA State only
	SELECT distinct S.WCTrackHistoryID
		,S.State
		,F.FormName
		,'01' LossesSubjectToDeductibleCode
		,'01' BasisOfDeductibleCalculationCode
		,D_MD.DeductibleValue MedicalDeductible
		,NULL SmallDeductibleCredit
		,P.TransactionDate
		,P.TransactionEffectiveDate
		
	FROM dbo.WorkWCStateTerm S
	
	INNER JOIN dbo.WorkWCPolicy P
		ON P.WCTrackHistoryID = S.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCDeductible D_MD
		ON D_MD.WCTrackHistoryID = S.WCTrackHistoryID
			AND D_MD.DeductibleType = 'MedicalDeductible'
			AND CoverageType='ManualPremium'
	
	INNER JOIN dbo.WorkWCForms F
		ON F.WCTrackHistoryID = S.WCTrackHistoryID
			AND (F.OnPolicy = '1' OR F.[Add] = '1')
			AND (F.[Remove] IS NULL OR F.[Remove] = '0')
			AND F.FormName IS NOT NULL
	
	WHERE 1 = 1
	AND LEFT(F.FormName, 8) IN ('WC140603')
	AND S.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	AND DeductibleValue IS NOT NULL
	AND S.State='IA'
	@{pipeline().parameters.WHERE_CLAUSE_43}
	
	UNION
	
	--- Logic specific to TX State only
	SELECT distinct S.WCTrackHistoryID
		,S.State
		,F.FormName
		,D_DT.StatCodeValue LossesSubjectToDeductibleCode
		,D_DB.StatCodeValue BasisOfDeductibleCalculationCode
		,0 MedicalDeductible
		,D_SDC.DeductibleValue SmallDeductibleCredit
		,P.TransactionDate
		,P.TransactionEffectiveDate
		
	FROM dbo.WorkWCStateTerm S
	
	INNER JOIN dbo.WorkWCPolicy P
		ON P.WCTrackHistoryID = S.WCTrackHistoryID
	
	LEFT JOIN dbo.WorkWCCoverage C
		ON C.WCTrackHistoryID = S.WCTrackHistoryID
			AND C.ParentObjectId = S.WC_StateTermId
			AND C.ParentObjectName = 'DC_WC_StateTerm'
			AND C.ParentCoverageType = 'SmallDeductibleCredit' -- Per Amy C.
	
	LEFT JOIN dbo.WorkWCSTATDeductible D_SDC
		ON D_SDC.WCTrackHistoryID = C.WCTrackHistoryID
			AND D_SDC.CoverageId = C.ParentCoverageId
			AND D_SDC.CoverageType = C.ParentCoverageType
			AND D_SDC.DeductibleType in ('PerAccidentDeductibles','PerClaimDeductibles','MedicalOnlyDeductibles')
	
	LEFT JOIN dbo.WorkWCSTATDeductible D_DB
		ON D_DB.WCTrackHistoryID = C.WCTrackHistoryID
			AND D_DB.CoverageId = C.ParentCoverageId
			AND D_DB.CoverageType = C.ParentCoverageType
			AND D_DB.DeductibleType = 'SmallDeductibleCredit'
			AND D_DB.StatCodeType='DeductibleBasis'
	
	LEFT JOIN dbo.WorkWCSTATDeductible D_DT
		ON D_DT.WCTrackHistoryID = C.WCTrackHistoryID
			AND D_DT.CoverageId = C.ParentCoverageId
			AND D_DT.CoverageType = C.ParentCoverageType
			AND D_DT.DeductibleType = 'SmallDeductibleCredit'
			AND D_DT.StatCodeType='DeductibleType'
	
	INNER JOIN dbo.WorkWCForms F
		ON F.WCTrackHistoryID = S.WCTrackHistoryID
			AND (F.OnPolicy = '1' OR F.[Add] = '1')
			AND (F.[Remove] IS NULL OR F.[Remove] = '0')
			AND F.FormName IS NOT NULL
	
	WHERE 1 = 1
	AND LEFT(F.FormName, 8) IN ('WC420602','WC420605','WC420606')
	AND S.State='TX'
	AND D_SDC.DeductibleValue IS NOT NULL AND D_SDC.DeductibleValue<>'0'
	AND S.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_43}
	
	ORDER BY S.WCTrackHistoryID
),
JNR_43_Record AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	SQ_WorkWCForms.WCTrackHistoryID AS WCTrackHistoryID1, 
	SQ_WorkWCForms.State, 
	SQ_WorkWCForms.FormName, 
	SQ_WorkWCForms.LossesSubjectToDeductibleCode, 
	SQ_WorkWCForms.BasisOfDeductibleCalculationCode, 
	SQ_WorkWCForms.MedicalDeductible, 
	SQ_WorkWCForms.SmallDeductibleCredit, 
	SQ_WorkWCForms.TransactionDate, 
	SQ_WorkWCForms.TransactionEffectiveDate
	FROM SQ_WorkWCForms
	INNER JOIN SQ_WCPols00Record
	ON SQ_WCPols00Record.WCTrackHistoryID = SQ_WorkWCForms.WCTrackHistoryID
),
EXP_Output_Format AS (
	SELECT
	CURRENT_TIMESTAMP AS o_ExtractDate,
	AuditId,
	WCTrackHistoryID,
	LinkData,
	State,
	-- *INF*: :LKP.LKP_SupWCPOLS('DCT',State,'WCPOLS43Record','StateCodeRecord43')
	LKP_SUPWCPOLS__DCT_State_WCPOLS43Record_StateCodeRecord43.WCPOLSCode AS o_StateCode,
	FormName,
	LossesSubjectToDeductibleCode,
	BasisOfDeductibleCalculationCode,
	MedicalDeductible,
	SmallDeductibleCredit,
	'43' AS o_RecordTypeCode,
	-- *INF*: SUBSTR(FormName,1,8)
	SUBSTR(FormName, 1, 8) AS o_EndorsementNumber,
	-- *INF*: SUBSTR(FormName, Length(FormName)-4, 1)
	SUBSTR(FormName, Length(FormName) - 4, 1) AS v_BureauID,
	-- *INF*: IIF(v_BureauID >= 'A' and v_BureauID <= 'Z', v_BureauID,' ')
	IFF(v_BureauID >= 'A' and v_BureauID <= 'Z', v_BureauID, ' ') AS o_BureauVersionIdentifierEditionIdentifier,
	-- *INF*: SUBSTR(FormName, Length(FormName)-3, 4)
	SUBSTR(FormName, Length(FormName) - 3, 4) AS o_CarrierVersionIdentifier,
	-- *INF*: SUBSTR(FormName,1,8) || State
	SUBSTR(FormName, 1, 8) || State AS v_Lookup_String,
	-- *INF*: IIF(SUBSTR(FormName,1,8) = 'WC140603',MedicalDeductible,SmallDeductibleCredit)
	IFF(SUBSTR(FormName, 1, 8) = 'WC140603', MedicalDeductible, SmallDeductibleCredit) AS o_DeductibleAmountPerClaimAccident,
	TransactionDate,
	-- *INF*: To_Char(TransactionEffectiveDate, 'YYMMDD')
	To_Char(TransactionEffectiveDate, 'YYMMDD') AS o_EndorsementEffectiveDate,
	TransactionEffectiveDate
	FROM JNR_43_Record
	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_State_WCPOLS43Record_StateCodeRecord43
	ON LKP_SUPWCPOLS__DCT_State_WCPOLS43Record_StateCodeRecord43.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_State_WCPOLS43Record_StateCodeRecord43.SourceCode = State
	AND LKP_SUPWCPOLS__DCT_State_WCPOLS43Record_StateCodeRecord43.TableName = 'WCPOLS43Record'
	AND LKP_SUPWCPOLS__DCT_State_WCPOLS43Record_StateCodeRecord43.ProcessName = 'StateCodeRecord43'

),
LKP_SUPWCPOLSSmallDeductible AS (
	SELECT
	SUPWCPOLSSmallDeductibleID,
	In_StateCode,
	In_EndorsementNumber,
	StateCode,
	FormName
	FROM (
		Select SupWCPOLSSmallDeductibleID as SupWCPOLSSmallDeductibleID,
		StateCode as StateCode, FormName as FormName
		from SupWCPOLSSmallDeductible
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StateCode,FormName ORDER BY SUPWCPOLSSmallDeductibleID) = 1
),
EXP_Target AS (
	SELECT
	EXP_Output_Format.o_ExtractDate AS ExtractDate,
	EXP_Output_Format.AuditId,
	EXP_Output_Format.WCTrackHistoryID,
	EXP_Output_Format.LinkData,
	EXP_Output_Format.o_StateCode AS StateCode,
	EXP_Output_Format.FormName,
	EXP_Output_Format.o_RecordTypeCode AS RecordTypeCode,
	EXP_Output_Format.o_EndorsementNumber AS EndorsementNumber,
	EXP_Output_Format.o_BureauVersionIdentifierEditionIdentifier AS BureauVersionIdentifierEditionIdentifier,
	EXP_Output_Format.o_CarrierVersionIdentifier AS CarrierVersionIdentifier,
	EXP_Output_Format.LossesSubjectToDeductibleCode,
	EXP_Output_Format.BasisOfDeductibleCalculationCode,
	EXP_Output_Format.o_DeductibleAmountPerClaimAccident AS DeductibleAmountPerClaimAccident,
	EXP_Output_Format.o_EndorsementEffectiveDate AS EndorsementEffectiveDate,
	LKP_SUPWCPOLSSmallDeductible.SUPWCPOLSSmallDeductibleID
	FROM EXP_Output_Format
	LEFT JOIN LKP_SUPWCPOLSSmallDeductible
	ON LKP_SUPWCPOLSSmallDeductible.StateCode = EXP_Output_Format.State AND LKP_SUPWCPOLSSmallDeductible.FormName = EXP_Output_Format.o_EndorsementNumber
),
FIL_InvalidForms AS (
	SELECT
	ExtractDate, 
	AuditId, 
	WCTrackHistoryID, 
	LinkData, 
	StateCode, 
	FormName, 
	RecordTypeCode, 
	EndorsementNumber, 
	BureauVersionIdentifierEditionIdentifier, 
	CarrierVersionIdentifier, 
	LossesSubjectToDeductibleCode, 
	BasisOfDeductibleCalculationCode, 
	DeductibleAmountPerClaimAccident, 
	EndorsementEffectiveDate, 
	SUPWCPOLSSmallDeductibleID
	FROM EXP_Target
	WHERE NOT ISNULL(SUPWCPOLSSmallDeductibleID)
),
WCPols43Record AS (

	------------ PRE SQL ----------
	DELETE
	  FROM dbo.WCPols43Record
	  WHERE 1=1
	  AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	-------------------------------


	INSERT INTO WCPols43Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, StateCode, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, LossesSubjectToDeductibleCode, BasisOfDeductibleCalculationCode, DeductibleAmountPerClaimAccident, EndorsementEffectiveDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	STATECODE, 
	RECORDTYPECODE, 
	ENDORSEMENTNUMBER, 
	BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER, 
	CARRIERVERSIONIDENTIFIER, 
	LOSSESSUBJECTTODEDUCTIBLECODE, 
	BASISOFDEDUCTIBLECALCULATIONCODE, 
	DEDUCTIBLEAMOUNTPERCLAIMACCIDENT, 
	ENDORSEMENTEFFECTIVEDATE
	FROM FIL_InvalidForms
),