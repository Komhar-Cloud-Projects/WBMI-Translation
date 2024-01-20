WITH
SQ_WBCLPrintDocStage AS (
	SELECT WBCLPrintDocStage.ExtractDate, 
	WBCLPrintDocStage.SourceSystemId, 
	WBCLPrintDocStage.WB_CL_PrintJobId, 
	WBCLPrintDocStage.WB_CL_PrintDocId, 
	WBCLPrintDocStage.SessionId, 
	WBCLPrintDocStage.Caption, 
	WBCLPrintDocStage.FormName, 
	WBCLPrintDocStage.Manuscript, 
	WBCLPrintDocStage.[Order], 
	WBCLPrintDocStage.PrintDefault, 
	WBCLPrintDocStage.Selected,
	WBCLPrintDocStage.OnPolicy, 
	WBCLPrintDocStage.[Add], 
	WBCLPrintDocStage.[Remove]  
	FROM WBCLPrintDocStage
),
EXP_Metadata AS (
	SELECT
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WB_CL_PrintJobId,
	WB_CL_PrintDocId,
	SessionId,
	Caption,
	FormName,
	Manuscript,
	Order,
	PrintDefault,
	Selected,
	OnPolicy,
	Add,
	Remove
	FROM SQ_WBCLPrintDocStage
),
archWBCLPrintDocStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archWBCLPrintDocStage
	(ExtractDate, SourceSystemId, AuditId, WB_CL_PrintJobId, WB_CL_PrintDocId, SessionId, Caption, FormName, Manuscript, Order, PrintDefault, Selected, OnPolicy, Add, Remove)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WB_CL_PRINTJOBID, 
	WB_CL_PRINTDOCID, 
	SESSIONID, 
	CAPTION, 
	FORMNAME, 
	MANUSCRIPT, 
	ORDER, 
	PRINTDEFAULT, 
	SELECTED, 
	ONPOLICY, 
	ADD, 
	REMOVE
	FROM EXP_Metadata
),