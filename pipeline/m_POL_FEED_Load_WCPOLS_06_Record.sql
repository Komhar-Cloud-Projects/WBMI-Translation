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
SQ_WorkWCStateTerm AS (
	SELECT DISTINCT 
	WorkWCStateTerm.WCTrackHistoryID as WCTrackHistoryID, 
	WorkWCStateTerm.Auditid as Auditid, 
	WorkWCStateTerm.State as State, 
	WorkWCStateTerm.State as StateCode
	FROM
	WorkWCStateTerm
	WHERE 
	WorkWCStateTerm.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	order by 1,3
),
EXP_State_Input AS (
	SELECT
	WCTrackHistoryID,
	Auditid,
	State,
	StateCode
	FROM SQ_WorkWCStateTerm
),
AGG_combine_States AS (
	SELECT
	WCTrackHistoryID,
	Auditid,
	StateCode AS i_State,
	-- *INF*: IIF(WCTrackHistoryID=v_PrevWCTrackHistoryID,0,1)
	IFF(WCTrackHistoryID = v_PrevWCTrackHistoryID, 0, 1) AS v_NewTrackId,
	-- *INF*: DECODE(TRUE,
	-- v_NewTrackId=1, i_State,
	-- v_NewTrackId=0 AND i_State != v_PrevState, v_State ||','||i_State,
	-- v_NewTrackId=0, v_State,
	-- ''
	-- )
	-- 
	-- -- if new record overwrite State
	-- -- if not new record and State != previous State then concatenate comma and State
	-- -- if not new record retain State value (assumed State =  prevState)
	-- -- else blank out the field
	DECODE(
	    TRUE,
	    v_NewTrackId = 1, i_State,
	    v_NewTrackId = 0 AND i_State != v_PrevState, v_State || ',' || i_State,
	    v_NewTrackId = 0, v_State,
	    ''
	) AS v_State,
	i_State AS v_PrevState,
	WCTrackHistoryID AS v_PrevWCTrackHistoryID,
	v_State AS o_State
	FROM EXP_State_Input
	GROUP BY WCTrackHistoryID, Auditid
),
EXP_AggOutput AS (
	SELECT
	WCTrackHistoryID,
	o_State AS StateList
	FROM AGG_combine_States
),
SQ_WorkWCTrackHistory AS (
	SELECT 
	Track.WCTrackHistoryID as WCTrackHistoryID, 
	Line.RatingPlan as RatingPlan,
	Line.OtherStatesInsuranceConditional as OtherStatesInsuranceConditional,
	Policy.TransactionEffectiveDate as TransactionEffectiveDate,
	Policy.TransactionExpirationDate as TransactionExpirationDate
	FROM
	 WorkWCTrackHistory Track 	
	inner join WorkWCPolicy Policy on 
		Policy.WCTrackHistoryID=Track.WCTrackHistoryID
	inner join WorkWCLine Line on
		Line.WCTrackHistoryID=track.WCTrackHistoryID
	WHERE
	Track.Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_06}
	order by 1
),
EXP_Data_Input AS (
	SELECT
	WCTrackHistoryID,
	RatingPlan,
	OtherStatesInsuranceConditional,
	TransactionEffectiveDate,
	TransactionExpirationDate
	FROM SQ_WorkWCTrackHistory
),
JNR_CombineData AS (SELECT
	EXP_AggOutput.WCTrackHistoryID, 
	EXP_AggOutput.StateList, 
	EXP_Data_Input.WCTrackHistoryID AS WCTrackHistoryID1, 
	EXP_Data_Input.RatingPlan, 
	EXP_Data_Input.OtherStatesInsuranceConditional, 
	EXP_Data_Input.TransactionEffectiveDate, 
	EXP_Data_Input.TransactionExpirationDate
	FROM EXP_AggOutput
	RIGHT OUTER JOIN EXP_Data_Input
	ON EXP_Data_Input.WCTrackHistoryID = EXP_AggOutput.WCTrackHistoryID
),
EXP_CombinedData_postJoin AS (
	SELECT
	WCTrackHistoryID1 AS WCTrackHistoryID,
	StateList,
	RatingPlan,
	OtherStatesInsuranceConditional,
	TransactionEffectiveDate,
	TransactionExpirationDate
	FROM JNR_CombineData
),
SQ_WCPols00Record AS (
	SELECT
		WCTrackHistoryID,
		LinkData,
	      AuditId,
		TransactionCode
	FROM dbo.WCPols00Record
	WHERE 
	 AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	ORDER BY WCTrackHistoryID
),
JNR_DataCollect AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	SQ_WCPols00Record.TransactionCode, 
	EXP_CombinedData_postJoin.WCTrackHistoryID AS WCTrackHistoryID1, 
	EXP_CombinedData_postJoin.RatingPlan, 
	EXP_CombinedData_postJoin.OtherStatesInsuranceConditional, 
	EXP_CombinedData_postJoin.TransactionEffectiveDate, 
	EXP_CombinedData_postJoin.TransactionExpirationDate, 
	EXP_CombinedData_postJoin.StateList
	FROM SQ_WCPols00Record
	INNER JOIN EXP_CombinedData_postJoin
	ON EXP_CombinedData_postJoin.WCTrackHistoryID = SQ_WCPols00Record.WCTrackHistoryID
),
EXP_apply_rules AS (
	SELECT
	WCTrackHistoryID,
	LinkData,
	AuditId,
	TransactionCode,
	'06' AS RecordTypeCode,
	CURRENT_TIMESTAMP AS ExtractDate,
	RatingPlan,
	StateList,
	OtherStatesInsuranceConditional AS i_OtherStatesInsuranceConditional,
	-- *INF*: REPLACECHR(0,REPLACESTR(0,i_OtherStatesInsuranceConditional,'and',','),' ','')
	-- 
	-- -- Replace the word and with a comma and all spaces with and empty string
	REGEXP_REPLACE(REGEXP_REPLACE(i_OtherStatesInsuranceConditional,'and',',','i'),' ','','i') AS v_OtherStatesInsuranceConditional,
	StateList  || ','|| v_OtherStatesInsuranceConditional AS v_CombinedStateList,
	-- *INF*: REPLACECHR(0,v_CombinedStateList,',','')
	-- 
	-- -- remove commas, because the data entry is just that bad   ie... NDWI,IL,IA,WVMNALOR,CA
	REGEXP_REPLACE(v_CombinedStateList,',','','i') AS v_CombinedStateList_remove_commas,
	-- *INF*: REPLACECHR(0,v_OtherStatesInsuranceConditional,',','')
	-- 
	-- -- remove commas, because the data entry is just that bad   ie... NDWI,IL,IA,WVMNALOR,CA
	REGEXP_REPLACE(v_OtherStatesInsuranceConditional,',','','i') AS v_MonopolisticState_remove_commas,
	-- *INF*: DECODE(TRUE,
	-- SUBSTR(LTRIM(RTRIM(v_MonopolisticState_remove_commas)),1,8)='NDOHWAWY',1,
	-- 0
	-- )
	-- 
	-- -- check if any of the monopolistic States exist
	DECODE(
	    TRUE,
	    SUBSTR(LTRIM(RTRIM(v_MonopolisticState_remove_commas)), 1, 8) = 'NDOHWAWY', 1,
	    0
	) AS v_OtherStatesInsuranceConditional_monopolistic_flag,
	-- *INF*: DECODE(TRUE,
	-- RatingPlan='WCPool' OR length(rtrim(ltrim(v_OtherStatesInsuranceConditional)))=0,'3',
	-- v_OtherStatesInsuranceConditional_monopolistic_flag = 1,'2',
	-- '1'
	-- )
	-- 
	-- 
	-- 
	-- --If RatingPlan = 'WCPool' OR no states are selected for 3c then set to 3
	-- --ELSE
	-- --If monopolistic states exist then set to 2 (excluded)
	-- --ELSE
	-- --If monopolistic states do NOT exist AND other states are selected (for 3C) then set to 1 (included)
	DECODE(
	    TRUE,
	    RatingPlan = 'WCPool' OR length(rtrim(ltrim(v_OtherStatesInsuranceConditional))) = 0, '3',
	    v_OtherStatesInsuranceConditional_monopolistic_flag = 1, '2',
	    '1'
	) AS v_IncludeExclude,
	-- *INF*: DECODE(TRUE,
	-- v_IncludeExclude='1',v_MonopolisticState_remove_commas,
	-- v_IncludeExclude='2',v_CombinedStateList_remove_commas,
	-- '')
	DECODE(
	    TRUE,
	    v_IncludeExclude = '1', v_MonopolisticState_remove_commas,
	    v_IncludeExclude = '2', v_CombinedStateList_remove_commas,
	    ''
	) AS o_CombinedStateList,
	',' AS Delimiter,
	TransactionEffectiveDate AS i_TransactionEffectiveDate,
	TransactionExpirationDate AS i_TransactionExpirationDate,
	v_IncludeExclude AS o_IncludeExclude,
	-- *INF*: TO_CHAR(i_TransactionEffectiveDate,'YYMMDD')
	TO_CHAR(i_TransactionEffectiveDate, 'YYMMDD') AS o_TransactionEffectiveDate,
	-- *INF*: DECODE(TRUE,
	-- in(TransactionCode,'08','10','14','15'),TO_CHAR(i_TransactionExpirationDate,'YYMMDD'),
	-- '')
	DECODE(
	    TRUE,
	    TransactionCode IN ('08','10','14','15'), TO_CHAR(i_TransactionExpirationDate, 'YYMMDD'),
	    ''
	) AS o_TransactionExpirationDate
	FROM JNR_DataCollect
),
jtx_split_string AS (-- jtx_split_string

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
EXP_splitterOutput AS (
	SELECT
	OUTPUT_Field1,
	OUTPUT_Field2,
	OUTPUT_Field3,
	OUTPUT_Field4,
	OUTPUT_Field5,
	OUTPUT_Field6,
	OUTPUT_Field7,
	OUTPUT_Field8,
	OUTPUT_Field9,
	OUTPUT_Field10,
	OUTPUT_Field11,
	OUTPUT_Field12,
	OUTPUT_Field13,
	OUTPUT_Field14,
	OUTPUT_Field15,
	OUTPUT_Field16,
	OUTPUT_Field17,
	OUTPUT_Field18,
	OUTPUT_Field19,
	OUTPUT_Field20,
	OUTPUT_Field21,
	OUTPUT_Field22,
	OUTPUT_Field23,
	OUTPUT_Field24,
	OUTPUT_Field25,
	OUTPUT_Field26,
	OUTPUT_Field27,
	OUTPUT_Field28,
	OUTPUT_Field29,
	OUTPUT_Field30,
	OUTPUT_Field31,
	OUTPUT_Field32,
	OUTPUT_Field33,
	OUTPUT_Field34,
	OUTPUT_Field35,
	OUTPUT_Field36,
	OUTPUT_Field37,
	OUTPUT_Field38,
	OUTPUT_Field39,
	OUTPUT_Field40,
	OUTPUT_Field41,
	OUTPUT_Field42,
	OUTPUT_Field43,
	OUTPUT_Field44,
	OUTPUT_Field45,
	OUTPUT_Field46,
	OUTPUT_Field47,
	OUTPUT_Field48,
	OUTPUT_Field49,
	OUTPUT_Field50,
	OUTPUT_Field51,
	OUTPUT_Field52,
	OUTPUT_Field53,
	OUTPUT_Field54,
	OUTPUT_Field55,
	OUTPUT_Field56,
	OUTPUT_Field57,
	OUTPUT_Field58,
	OUTPUT_Field59,
	OUTPUT_Field60,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field1,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field1_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode01,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field2,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field2_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode02,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field3,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field3_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode03,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field4,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field4_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode04,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field5,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field5_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode05,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field6,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field6_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode06,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field7,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field7_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode07,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field8,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field8_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode08,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field9,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field9_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode09,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field10,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field10_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode10,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field11,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field11_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode11,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field12,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field12_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode12,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field13,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field13_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode13,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field14,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field14_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode14,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field15,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field15_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode15,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field16,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field16_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode16,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field17,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field17_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode17,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field18,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field18_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode18,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field19,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field19_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode19,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field20,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field20_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode20,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field21,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field21_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode21,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field22,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field22_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode22,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field23,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field23_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode23,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field24,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field24_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode24,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field25,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field25_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode25,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field26,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field26_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode26,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field27,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field27_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode27,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field28,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field28_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode28,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field29,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field29_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode29,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field30,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field30_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode30,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field31,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field31_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode31,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field32,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field32_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode32,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field33,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field33_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode33,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field34,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field34_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode34,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field35,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field35_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode35,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field36,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field36_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode36,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field37,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field37_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode37,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field38,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field38_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode38,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field39,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field39_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode39,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field40,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field40_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode40,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field41,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field41_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode41,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field42,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field42_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode42,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field43,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field43_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode43,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field44,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field44_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode44,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field45,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field45_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode45,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field46,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field46_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode46,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field47,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field47_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode47,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field48,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field48_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode48,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field49,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field49_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode49,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field50,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field50_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode50,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field51,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field51_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode51,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field52,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field52_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode52,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field53,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field53_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode53,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field54,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field54_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode54,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field55,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field55_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode55,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field56,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field56_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode56,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field57,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field57_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode57,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field58,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field58_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode58,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field59,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field59_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode59,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',OUTPUT_Field60,'WCPOLS06Record','StateCodeRecord06')
	LKP_SUPWCPOLS__DCT_OUTPUT_Field60_WCPOLS06Record_StateCodeRecord06.WCPOLSCode AS StateCode60
	FROM jtx_split_string
	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field1_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field1_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field1_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field1
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field1_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field1_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field2_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field2_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field2_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field2
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field2_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field2_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field3_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field3_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field3_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field3
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field3_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field3_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field4_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field4_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field4_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field4
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field4_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field4_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field5_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field5_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field5_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field5
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field5_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field5_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field6_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field6_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field6_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field6
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field6_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field6_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field7_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field7_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field7_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field7
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field7_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field7_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field8_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field8_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field8_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field8
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field8_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field8_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field9_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field9_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field9_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field9
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field9_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field9_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field10_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field10_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field10_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field10
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field10_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field10_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field11_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field11_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field11_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field11
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field11_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field11_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field12_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field12_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field12_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field12
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field12_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field12_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field13_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field13_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field13_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field13
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field13_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field13_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field14_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field14_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field14_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field14
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field14_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field14_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field15_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field15_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field15_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field15
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field15_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field15_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field16_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field16_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field16_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field16
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field16_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field16_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field17_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field17_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field17_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field17
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field17_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field17_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field18_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field18_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field18_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field18
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field18_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field18_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field19_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field19_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field19_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field19
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field19_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field19_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field20_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field20_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field20_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field20
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field20_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field20_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field21_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field21_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field21_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field21
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field21_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field21_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field22_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field22_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field22_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field22
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field22_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field22_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field23_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field23_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field23_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field23
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field23_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field23_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field24_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field24_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field24_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field24
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field24_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field24_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field25_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field25_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field25_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field25
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field25_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field25_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field26_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field26_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field26_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field26
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field26_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field26_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field27_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field27_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field27_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field27
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field27_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field27_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field28_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field28_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field28_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field28
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field28_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field28_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field29_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field29_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field29_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field29
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field29_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field29_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field30_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field30_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field30_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field30
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field30_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field30_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field31_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field31_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field31_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field31
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field31_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field31_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field32_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field32_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field32_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field32
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field32_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field32_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field33_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field33_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field33_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field33
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field33_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field33_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field34_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field34_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field34_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field34
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field34_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field34_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field35_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field35_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field35_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field35
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field35_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field35_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field36_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field36_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field36_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field36
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field36_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field36_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field37_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field37_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field37_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field37
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field37_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field37_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field38_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field38_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field38_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field38
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field38_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field38_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field39_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field39_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field39_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field39
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field39_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field39_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field40_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field40_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field40_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field40
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field40_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field40_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field41_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field41_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field41_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field41
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field41_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field41_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field42_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field42_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field42_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field42
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field42_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field42_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field43_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field43_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field43_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field43
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field43_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field43_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field44_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field44_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field44_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field44
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field44_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field44_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field45_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field45_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field45_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field45
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field45_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field45_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field46_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field46_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field46_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field46
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field46_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field46_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field47_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field47_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field47_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field47
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field47_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field47_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field48_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field48_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field48_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field48
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field48_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field48_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field49_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field49_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field49_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field49
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field49_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field49_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field50_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field50_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field50_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field50
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field50_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field50_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field51_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field51_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field51_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field51
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field51_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field51_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field52_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field52_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field52_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field52
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field52_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field52_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field53_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field53_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field53_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field53
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field53_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field53_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field54_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field54_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field54_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field54
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field54_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field54_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field55_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field55_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field55_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field55
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field55_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field55_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field56_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field56_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field56_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field56
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field56_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field56_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field57_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field57_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field57_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field57
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field57_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field57_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field58_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field58_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field58_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field58
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field58_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field58_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field59_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field59_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field59_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field59
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field59_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field59_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_OUTPUT_Field60_WCPOLS06Record_StateCodeRecord06
	ON LKP_SUPWCPOLS__DCT_OUTPUT_Field60_WCPOLS06Record_StateCodeRecord06.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field60_WCPOLS06Record_StateCodeRecord06.SourceCode = OUTPUT_Field60
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field60_WCPOLS06Record_StateCodeRecord06.TableName = 'WCPOLS06Record'
	AND LKP_SUPWCPOLS__DCT_OUTPUT_Field60_WCPOLS06Record_StateCodeRecord06.ProcessName = 'StateCodeRecord06'

),
WCPols06Record AS (

	------------ PRE SQL ----------
	DELETE
	  FROM dbo.WCPols06Record
	  WHERE 1=1
	  AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	-------------------------------


	INSERT INTO WCPols06Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, RecordTypeCode, InclusionExclusionCode, StateCode01, StateCode02, StateCode03, StateCode04, StateCode05, StateCode06, StateCode07, StateCode08, StateCode09, StateCode10, StateCode11, StateCode12, StateCode13, StateCode14, StateCode15, StateCode16, StateCode17, StateCode18, StateCode19, StateCode20, StateCode21, StateCode22, StateCode23, StateCode24, StateCode25, StateCode26, StateCode27, StateCode28, StateCode29, StateCode30, StateCode31, StateCode32, StateCode33, StateCode34, StateCode35, StateCode36, StateCode37, StateCode38, StateCode39, StateCode40, StateCode41, StateCode42, StateCode43, StateCode44, StateCode45, StateCode46, StateCode47, StateCode48, StateCode49, StateCode50, StateCode51, StateCode52, StateCode53, StateCode54, StateCode55, StateCode56, StateCode57, StateCode58, StateCode59, StateCode60, PolicyChangeEffectiveDate, PolicyChangeExpirationDate)
	SELECT 
	EXP_apply_rules.EXTRACTDATE, 
	EXP_apply_rules.AUDITID, 
	EXP_apply_rules.WCTRACKHISTORYID, 
	EXP_apply_rules.LINKDATA, 
	EXP_apply_rules.RECORDTYPECODE, 
	EXP_apply_rules.o_IncludeExclude AS INCLUSIONEXCLUSIONCODE, 
	EXP_splitterOutput.STATECODE01, 
	EXP_splitterOutput.STATECODE02, 
	EXP_splitterOutput.STATECODE03, 
	EXP_splitterOutput.STATECODE04, 
	EXP_splitterOutput.STATECODE05, 
	EXP_splitterOutput.STATECODE06, 
	EXP_splitterOutput.STATECODE07, 
	EXP_splitterOutput.STATECODE08, 
	EXP_splitterOutput.STATECODE09, 
	EXP_splitterOutput.STATECODE10, 
	EXP_splitterOutput.STATECODE11, 
	EXP_splitterOutput.STATECODE12, 
	EXP_splitterOutput.STATECODE13, 
	EXP_splitterOutput.STATECODE14, 
	EXP_splitterOutput.STATECODE15, 
	EXP_splitterOutput.STATECODE16, 
	EXP_splitterOutput.STATECODE17, 
	EXP_splitterOutput.STATECODE18, 
	EXP_splitterOutput.STATECODE19, 
	EXP_splitterOutput.STATECODE20, 
	EXP_splitterOutput.STATECODE21, 
	EXP_splitterOutput.STATECODE22, 
	EXP_splitterOutput.STATECODE23, 
	EXP_splitterOutput.STATECODE24, 
	EXP_splitterOutput.STATECODE25, 
	EXP_splitterOutput.STATECODE26, 
	EXP_splitterOutput.STATECODE27, 
	EXP_splitterOutput.STATECODE28, 
	EXP_splitterOutput.STATECODE29, 
	EXP_splitterOutput.STATECODE30, 
	EXP_splitterOutput.STATECODE31, 
	EXP_splitterOutput.STATECODE32, 
	EXP_splitterOutput.STATECODE33, 
	EXP_splitterOutput.STATECODE34, 
	EXP_splitterOutput.STATECODE35, 
	EXP_splitterOutput.STATECODE36, 
	EXP_splitterOutput.STATECODE37, 
	EXP_splitterOutput.STATECODE38, 
	EXP_splitterOutput.STATECODE39, 
	EXP_splitterOutput.STATECODE40, 
	EXP_splitterOutput.STATECODE41, 
	EXP_splitterOutput.STATECODE42, 
	EXP_splitterOutput.STATECODE43, 
	EXP_splitterOutput.STATECODE44, 
	EXP_splitterOutput.STATECODE45, 
	EXP_splitterOutput.STATECODE46, 
	EXP_splitterOutput.STATECODE47, 
	EXP_splitterOutput.STATECODE48, 
	EXP_splitterOutput.STATECODE49, 
	EXP_splitterOutput.STATECODE50, 
	EXP_splitterOutput.STATECODE51, 
	EXP_splitterOutput.STATECODE52, 
	EXP_splitterOutput.STATECODE53, 
	EXP_splitterOutput.STATECODE54, 
	EXP_splitterOutput.STATECODE55, 
	EXP_splitterOutput.STATECODE56, 
	EXP_splitterOutput.STATECODE57, 
	EXP_splitterOutput.STATECODE58, 
	EXP_splitterOutput.STATECODE59, 
	EXP_splitterOutput.STATECODE60, 
	EXP_apply_rules.o_TransactionEffectiveDate AS POLICYCHANGEEFFECTIVEDATE, 
	EXP_apply_rules.o_TransactionExpirationDate AS POLICYCHANGEEXPIRATIONDATE
	FROM EXP_splitterOutput
),