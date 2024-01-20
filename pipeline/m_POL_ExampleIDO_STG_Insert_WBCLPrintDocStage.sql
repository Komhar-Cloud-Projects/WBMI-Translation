WITH
SQ_WB_CL_PrintDoc AS (
	SELECT	X.WB_CL_PrintJobId, 
			X.WB_CL_PrintDocId, 
			X.SessionId, 
			X.Caption, 
			X.FormName, 
			X.Manuscript, 
			X.[Order], 
			X.PrintDefault, 
			X.Selected,
			X.OnPolicy,
			X.[Add],
			X.[Remove] 
	FROM
			WB_CL_PrintDoc X WITH(nolock)
			INNER JOIN wbexampledata.dbo.wb_edwdataloadincrementalsessions Y WITH(
	                  nolock)
	               ON X.SessionId = Y.SessionId 
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	WB_CL_PrintJobId,
	WB_CL_PrintDocId,
	SessionId,
	Caption AS i_Caption,
	-- *INF*: REPLACESTR(0,i_Caption,'?','')
	REGEXP_REPLACE(i_Caption,'?','','i') AS o_Caption,
	FormName,
	Manuscript,
	Order,
	PrintDefault,
	Selected,
	OnPolicy,
	Add,
	Remove
	FROM SQ_WB_CL_PrintDoc
),
WBCLPrintDocStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCLPrintDocStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCLPrintDocStage
	(ExtractDate, SourceSystemId, WB_CL_PrintJobId, WB_CL_PrintDocId, SessionId, Caption, FormName, Manuscript, Order, PrintDefault, Selected, OnPolicy, Add, Remove)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	WB_CL_PRINTJOBID, 
	WB_CL_PRINTDOCID, 
	SESSIONID, 
	o_Caption AS CAPTION, 
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