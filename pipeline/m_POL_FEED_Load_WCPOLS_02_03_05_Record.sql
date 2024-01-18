WITH
lkp_WorkWCParty AS (
	SELECT
	AOIEntityType,
	Name,
	WCTrackHistoryID,
	PartyAssociationType
	FROM (
		SELECT AOIEntityType as AOIEntityType,
		 Replace( REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		       REPLACE(REPLACE(REPLACE(REPLACE( REPLACE(Name, ',', ' '),
		        '!',''),'@',''),'#',''),'$',''),'%',''),
		        '^',''),'&',''),'*',''),' ',''),'.',' ')  as Name,
		 WCTrackHistoryID as WCTrackHistoryID, PartyAssociationType as PartyAssociationType 
		FROM dbo.WorkWCParty
		where PartyAssociationType in ('PrimaryEntity','SecondaryEntity','AdditionalInsured')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Name,WCTrackHistoryID,PartyAssociationType ORDER BY AOIEntityType) = 1
),
LKP_WCPOLS02Record AS (
	SELECT
	NameLinkIdentifier,
	In_PolicyKey,
	In_Name,
	PolicyKey,
	NameOfInsured
	FROM (
		select B.PolicyKey AS PolicyKey,
		A.NameOfInsured AS NameOfInsured,
		A.NameLinkIdentifier AS NameLinkIdentifier
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols02Record A
		inner join( select A.PolicyKey,max(WCTrackHistoryID) WCTrackHistoryID from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory A
		where exists (select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory B where A.WCTrackHistoryID=B.WCTrackHistoryID and B.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID})
		group by PolicyKey) B
		on A.WCTrackHistoryID=B.WCTrackHistoryID
		
		UNION
		
		select B.PolicyKey AS PolicyKey,
		'' AS NameOfInsured,
		max(A.NameLinkIdentifier) AS NameLinkIdentifier
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WCPols02Record A
		inner join( select A.PolicyKey,max(WCTrackHistoryID) WCTrackHistoryID from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory A
		where exists (select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory B where A.WCTrackHistoryID=B.WCTrackHistoryID and B.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID})
		group by PolicyKey) B
		on A.WCTrackHistoryID=B.WCTrackHistoryID
		group by B.PolicyKey
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,NameOfInsured ORDER BY NameLinkIdentifier) = 1
),
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
SQ_WorkWCParty AS (
	Select * from (
	Select B.*,
	first_value(B.FirstName) OVER	(PARTITION BY WCTrackHistoryID,PartyOrder ORDER BY WCTrackHistoryID,PartyOrder,PartyAssociationType,Order_Seq) FFN,
	first_value(B.LastName) OVER	(PARTITION BY WCTrackHistoryID,PartyOrder ORDER BY WCTrackHistoryID,PartyOrder,PartyAssociationType,Order_Seq) FLN,
	first_value(B.MiddleName) OVER	(PARTITION BY WCTrackHistoryID,PartyOrder ORDER BY WCTrackHistoryID,PartyOrder,PartyAssociationType,Order_Seq) FMN,
	first_value(B.PhoneNumber) OVER	(PARTITION BY WCTrackHistoryID,PartyOrder ORDER BY WCTrackHistoryID,PartyOrder,PartyAssociationType,Order_Seq) ph,
	Rank () OVER	(PARTITION BY WCTrackHistoryID,Name,PartyOrder ORDER BY WCTrackHistoryID,PartyOrder,PartyAssociationType,Order_Seq) SEQ
	from 
	(
	Select  A.WCTrackHistoryID			
	,ltrim(rtrim(B.DoingBusinessAs))	Name				
	,B.FEIN		
	,B.PartyAssociationType					
	, CASE WHEN B.PartyAssociationType= 'Account' THEN ISNULL(B.EntityType,B.AOIEntityType) ELSE B.AOIEntityType END as EntityType
	,ISNULL(B.EntityOtherType,B.AOIEntityOtherType) AOIEntityOtherType			
	,A.TransactionEffectiveDate	
	,A.TransactionExpirationDate	
	,ISNULL(B.BusinessOrIndividual,0) BusinessOrIndividual
	,case when B.PartyAssociationType in ('Account','PrimaryEntity') then 1
	when B.PartyAssociationType='SecondaryEntity' then 2
	else 3 end PartyOrder
	,A.PolicyKey
	,B.PhoneNumber
	,B.Email
	,ISNULL(rtrim(ltrim(B.LastName)),'') LastName
	,ISNULL(rtrim(ltrim(B.FirstName)),'') FirstName
	,ISNULL(rtrim(ltrim(B.MiddleName)),'') MiddleName
	,Deleted
	,2 Order_Seq
	, Replace( REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	       REPLACE(REPLACE(REPLACE(REPLACE( REPLACE(Name, ',', ' '),
	        '!',''),'@',''),'#',''),'$',''),'%',''),
	        '^',''),'&',''),'*',''),' ',''),'.',' ') as Name_Cleansed
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCPolicy A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCParty B
	on A.WCTrackHistoryID=B.WCTrackHistoryID 
	where B.PartyAssociationType in ('Account','PrimaryEntity','SecondaryEntity')
	and B.DoingBusinessAs is not null and rtrim(ltrim(B.DoingBusinessAs))<>''
	and B.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and ltrim(rtrim(B.DoingBusinessAs)) is not null
	@{pipeline().parameters.WHERE_CLAUSE_02}
	
	UNION
	
	select * from (Select A.WCTrackHistoryID			
	,case when BusinessOrIndividual=1 and B.EntityType='Partnership' then 
	case when rtrim(ltrim(LastName)) is null and rtrim(ltrim(FirstName)) is null and rtrim(ltrim(MiddleName)) is null 
	then  rtrim(ltrim(Name)) else ISNULL(rtrim(ltrim(LastName)),'')+' '+ISNULL(rtrim(ltrim(FirstName)),'')+' '+ISNULL(rtrim(ltrim(MiddleName)),'') end else rtrim(ltrim(Name)) end  Name
	 ,B.FEIN
	,B.PartyAssociationType						
	, CASE WHEN B.PartyAssociationType= 'Account' THEN ISNULL(B.EntityType,B.AOIEntityType) ELSE B.AOIEntityType END as EntityType
	,ISNULL(B.EntityOtherType,B.AOIEntityOtherType) AOIEntityOtherType				
	,A.TransactionEffectiveDate	
	,A.TransactionExpirationDate	
	,ISNULL(B.BusinessOrIndividual,0) BusinessOrIndividual
	,case when B.PartyAssociationType in ('Account','PrimaryEntity') then 1
	when B.PartyAssociationType='SecondaryEntity' then 2
	else 3 end PartyOrder
	,A.PolicyKey
	,B.PhoneNumber
	,B.Email
	,ISNULL(rtrim(ltrim(B.LastName)),'') LastName
	,ISNULL(rtrim(ltrim(B.FirstName)),'') FirstName
	,ISNULL(rtrim(ltrim(B.MiddleName)),'') MiddleName
	,Deleted
	, 1 Order_Seq
	, Replace( REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	       REPLACE(REPLACE(REPLACE(REPLACE( REPLACE(Name, ',', ' '),
	        '!',''),'@',''),'#',''),'$',''),'%',''),
	        '^',''),'&',''),'*',''),' ',''),'.',' ')  as Name_Cleansed
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCPolicy A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCParty B
	on A.WCTrackHistoryID=B.WCTrackHistoryID 
	where B.PartyAssociationType in ('Account','PrimaryEntity','SecondaryEntity')
	and B.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and case when B.BusinessOrIndividual=1 then 
	case when rtrim(ltrim(B.LastName)) is null and rtrim(ltrim(B.FirstName)) is null and rtrim(ltrim(B.MiddleName)) is null 
	then  rtrim(ltrim(B.Name)) else ISNULL(rtrim(ltrim(B.LastName)),'')+','+ISNULL(rtrim(ltrim(B.FirstName)),'')+','+ISNULL(rtrim(ltrim(B.MiddleName)),'') end else rtrim(ltrim(B.Name)) end is not null
	@{pipeline().parameters.WHERE_CLAUSE_02} ) A
	 )B ) C
	 where SEQ=1
	and case when C.PartyAssociationType<>'Account' and (ISNULL(rtrim(ltrim(FLN)),'')+''+ISNULL(rtrim(ltrim(FFN)),'')=REPLACE(Name,' ','') or 
	ISNULL(rtrim(ltrim(FFN)),'')+''+ISNULL(rtrim(ltrim(FLN)),'')=REPLACE(Name,' ','')) then 1 else 0 end =0
	and case when C.PartyAssociationType<>'Account' and 
	(ISNULL(rtrim(ltrim(FLN)),'')+''+ISNULL(rtrim(ltrim(FFN)),'')+''+ISNULL(rtrim(ltrim(FMN)),'')=REPLACE(Name,' ','') or 
	ISNULL(rtrim(ltrim(FFN)),'')+''+ISNULL(rtrim(ltrim(FLN)),'')+''+ISNULL(rtrim(ltrim(FMN)),'')=REPLACE(Name,' ','') or 
	ISNULL(rtrim(ltrim(FFN)),'')+''+ISNULL(rtrim(ltrim(FMN)),'')+''+ISNULL(rtrim(ltrim(FLN)),'')=REPLACE(Name,' ','') or 
	ISNULL(rtrim(ltrim(FLN)),'')+''+ISNULL(rtrim(ltrim(FMN)),'')+''+ISNULL(rtrim(ltrim(FFN)),'')=REPLACE(Name,' ','') or
	ISNULL(rtrim(ltrim(FMN)),'')+''+ISNULL(rtrim(ltrim(FFN)),'')+''+ISNULL(rtrim(ltrim(FLN)),'')=REPLACE(Name,' ','') or 
	ISNULL(rtrim(ltrim(FMN)),'')+''+ISNULL(rtrim(ltrim(FLN)),'')+''+ISNULL(rtrim(ltrim(FFN)),'')=REPLACE(Name,' ','')) then 1 else 0 end =0
	and case when C.PartyAssociationType<>'Account' and 
	(ISNULL(rtrim(ltrim(FMN)),'')+''+ISNULL(rtrim(ltrim(FFN)),'')=REPLACE(Name,' ','') or 
	ISNULL(rtrim(ltrim(FFN)),'')+''+ISNULL(rtrim(ltrim(FMN)),'')=REPLACE(Name,' ','')) then 1 else 0 end =0
	and case when C.PartyAssociationType<>'Account' and 
	(ISNULL(rtrim(ltrim(FMN)),'')+''+ISNULL(rtrim(ltrim(FLN)),'')=REPLACE(Name,' ','') or 
	ISNULL(rtrim(ltrim(FLN)),'')+''+ISNULL(rtrim(ltrim(FMN)),'')=REPLACE(Name,' ','')) then 1 else 0 end =0
	order by WCTrackHistoryID,PartyOrder,PartyAssociationType,Order_Seq--,Name
),
EXP_SrcDataCollect_Part AS (
	SELECT
	WCTrackHistoryID,
	Name,
	-- *INF*: DECODE(TRUE,
	-- BusinessOrIndividual='T' AND Order_Seq='1' AND ISNULL(LTRIM(RTRIM(LastName))) AND ISNULL(LTRIM(RTRIM(FirstName))) AND ISNULL(LTRIM(RTRIM(MiddleName))), LTRIM(RTRIM(Name)),
	-- BusinessOrIndividual='T' AND Order_Seq='1', LastName || ' ' || FirstName || ' ' || MiddleName,
	-- LTRIM(RTRIM(Name))
	-- )
	DECODE(
	    TRUE,
	    BusinessOrIndividual = 'T' AND Order_Seq = '1' AND LTRIM(RTRIM(LastName)) IS NULL AND LTRIM(RTRIM(FirstName)) IS NULL AND LTRIM(RTRIM(MiddleName)) IS NULL, LTRIM(RTRIM(Name)),
	    BusinessOrIndividual = 'T' AND Order_Seq = '1', LastName || ' ' || FirstName || ' ' || MiddleName,
	    LTRIM(RTRIM(Name))
	) AS v_NameIndicator,
	-- *INF*: LTRIM(RTRIM(Upper(REPLACECHR(1,Name,chr(13)||chr(10),''))))
	LTRIM(RTRIM(Upper(REGEXP_REPLACE(Name,chr(13) || chr(10),'')))) AS o_Name,
	-- *INF*: IIF(WCTrackHistoryID=v_WCTrackHistoryID,
	-- IIF(Upper(REPLACECHR(1,Name,chr(13)||chr(10),''))=Upper(v_Name),'Y','N'),'N')
	IFF(
	    WCTrackHistoryID = v_WCTrackHistoryID,
	    IFF(
	        Upper(REGEXP_REPLACE(Name,chr(13) || chr(10),'')) = Upper(v_Name), 'Y', 'N'
	    ),
	    'N'
	) AS v_Party_Delete_Flag,
	WCTrackHistoryID AS v_WCTrackHistoryID,
	-- *INF*: REPLACECHR(1,Name,chr(13)||chr(10),'')
	REGEXP_REPLACE(Name,chr(13) || chr(10),'') AS v_Name,
	FEIN,
	PartyAssociationType,
	EntityType,
	EntityOtherType,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	BusinessOrIndividual,
	PhoneNumber,
	PartyOrder,
	PolicyKey,
	Order_Seq,
	LastName,
	FirstName,
	MiddleName,
	Deleted AS DeletedName_Flag,
	v_Party_Delete_Flag AS o_Party_Delete_Flag,
	First_Value_FirstName,
	First_Value_LastName,
	First_Value_MiddleName,
	Seq,
	Name_cleansed,
	-- *INF*: iif( (LENGTH(First_Value_FirstName) > 0 and LENGTH(First_Value_LastName) > 0) , 1,0)
	IFF((LENGTH(First_Value_FirstName) > 0 and LENGTH(First_Value_LastName) > 0), 1, 0) AS v_name_flag,
	-- *INF*: REPLACECHR(1,First_Value_LastName,chr(44),' ')
	REGEXP_REPLACE(First_Value_LastName,chr(44),' ') AS v_lastname_cleanup,
	-- *INF*:  iif ( (not isnull(v_lastname_cleanup) and not isnull(First_Value_FirstName)), CONCAT( CONCAT( v_lastname_cleanup, ', ' ), First_Value_FirstName ) , ' ')
	IFF(
	    (v_lastname_cleanup IS NULL and First_Value_FirstName IS NOT NOT NULL),
	    CONCAT(CONCAT(v_lastname_cleanup, ', '), First_Value_FirstName),
	    ' '
	) AS v_lastname_firstname,
	-- *INF*:  iif ( (LENGTH(First_Value_MiddleName) > 0),CONCAT( CONCAT( v_lastname_firstname, ', ' ), First_Value_MiddleName ),v_lastname_firstname)
	IFF(
	    (LENGTH(First_Value_MiddleName) > 0),
	    CONCAT(CONCAT(v_lastname_firstname, ', '), First_Value_MiddleName),
	    v_lastname_firstname
	) AS v_Name_Individual,
	-- *INF*: LTRIM(RTRIM(Upper(REPLACECHR(1,Name,chr(13)||chr(10),''))))
	LTRIM(RTRIM(Upper(REGEXP_REPLACE(Name,chr(13) || chr(10),'')))) AS v_name_entity,
	-- *INF*: IIF(  (v_name_flag=1 and EntityType='Individual')  , upper(v_Name_Individual),v_name_entity)
	IFF(
	    (v_name_flag = 1 and EntityType = 'Individual'), upper(v_Name_Individual), v_name_entity
	) AS NameInsured,
	First_Value_PhoneNumber,
	Email
	FROM SQ_WorkWCParty
),
EXP_Set_NameLinkIdentifier AS (
	SELECT
	WCTrackHistoryID,
	o_Name AS Name,
	-- *INF*: TO_Integer(IIF(ISNULL(:LKP.LKP_WCPOLS02RECORD(PolicyKey,Name)),'0',:LKP.LKP_WCPOLS02RECORD(PolicyKey,Name)))
	CAST(
	    IFF(
	        LKP_WCPOLS02RECORD_PolicyKey_Name.NameLinkIdentifier IS NULL, '0',
	        LKP_WCPOLS02RECORD_PolicyKey_Name.NameLinkIdentifier
	    ) AS INTEGER) AS lkp_NameLinkIdentifier,
	-- *INF*: TO_INTEGER(
	-- IIF(ISNULL(:LKP.LKP_WCPOLS02RECORD(PolicyKey,'')),'0',:LKP.LKP_WCPOLS02RECORD(PolicyKey,''))
	-- )
	-- --TO_INTEGER(IIF(ISNULL(:LKP.LKP_WCPOLS02RECORD(PolicyKey,'')),'001',:LKP.LKP_WCPOLS02RECORD(PolicyKey,'')))
	CAST(
	    IFF(
	        LKP_WCPOLS02RECORD_PolicyKey.NameLinkIdentifier IS NULL, '0',
	        LKP_WCPOLS02RECORD_PolicyKey.NameLinkIdentifier
	    ) AS INTEGER) AS lkp_NameLinkIdentifier_Seed,
	-- *INF*: IIF(WCTrackHistoryID=v_prev_WCTrackHistoryID,v_count+1,1)
	IFF(WCTrackHistoryID = v_prev_WCTrackHistoryID, v_count + 1, 1) AS v_count,
	-- *INF*: Decode(TRUE,
	-- lkp_NameLinkIdentifier<>0,lkp_NameLinkIdentifier,
	-- lkp_NameLinkIdentifier_Seed+v_count)
	Decode(
	    TRUE,
	    lkp_NameLinkIdentifier <> 0, lkp_NameLinkIdentifier,
	    lkp_NameLinkIdentifier_Seed + v_count
	) AS v_NameLinkNumber,
	-- *INF*: lpad(to_char(v_NameLinkNumber),3,'0')
	lpad(to_char(v_NameLinkNumber), 3, '0') AS o_NameLinkIdentifier,
	WCTrackHistoryID AS v_prev_WCTrackHistoryID,
	FEIN,
	EntityType,
	EntityOtherType,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	BusinessOrIndividual,
	PolicyKey,
	First_Value_PhoneNumber AS PhoneNumber,
	Order_Seq,
	DeletedName_Flag,
	Name_cleansed,
	NameInsured,
	Email
	FROM EXP_SrcDataCollect_Part
	LEFT JOIN LKP_WCPOLS02RECORD LKP_WCPOLS02RECORD_PolicyKey_Name
	ON LKP_WCPOLS02RECORD_PolicyKey_Name.PolicyKey = PolicyKey
	AND LKP_WCPOLS02RECORD_PolicyKey_Name.NameOfInsured = Name

	LEFT JOIN LKP_WCPOLS02RECORD LKP_WCPOLS02RECORD_PolicyKey
	ON LKP_WCPOLS02RECORD_PolicyKey.PolicyKey = PolicyKey
	AND LKP_WCPOLS02RECORD_PolicyKey.NameOfInsured = ''

),
SQ_WCPols00Record AS (
	SELECT
		WCTrackHistoryID,
		LinkData,
	     AuditId
	FROM dbo.WCPols00Record
	WHERE 1=1
	AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE}
	ORDER BY WCTrackHistoryID
),
JNR_Party_TO_LinkData AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	EXP_Set_NameLinkIdentifier.WCTrackHistoryID AS WCTrackHistoryID1, 
	EXP_Set_NameLinkIdentifier.Name, 
	EXP_Set_NameLinkIdentifier.FEIN, 
	EXP_Set_NameLinkIdentifier.PartyAssociationType, 
	EXP_Set_NameLinkIdentifier.EntityType, 
	EXP_Set_NameLinkIdentifier.EntityOtherType, 
	EXP_Set_NameLinkIdentifier.TransactionEffectiveDate, 
	EXP_Set_NameLinkIdentifier.TransactionExpirationDate, 
	EXP_Set_NameLinkIdentifier.BusinessOrIndividual, 
	EXP_Set_NameLinkIdentifier.o_NameLinkIdentifier AS NameLinkIdentifier, 
	EXP_Set_NameLinkIdentifier.DeletedName_Flag, 
	EXP_Set_NameLinkIdentifier.Order_Seq, 
	EXP_Set_NameLinkIdentifier.Name_cleansed, 
	EXP_Set_NameLinkIdentifier.NameInsured
	FROM EXP_Set_NameLinkIdentifier
	INNER JOIN SQ_WCPols00Record
	ON SQ_WCPols00Record.WCTrackHistoryID = EXP_Set_NameLinkIdentifier.WCTrackHistoryID
),
EXP_Record02_DataPrep AS (
	SELECT
	WCTrackHistoryID,
	LinkData,
	Name,
	FEIN,
	PartyAssociationType,
	EntityType,
	EntityOtherType,
	-- *INF*: Ltrim(rtrim(EntityType))
	Ltrim(rtrim(EntityType)) AS o_EntityType,
	TransactionEffectiveDate,
	-- *INF*: TO_Char(TransactionEffectiveDate,'YYMMDD')
	TO_Char(TransactionEffectiveDate, 'YYMMDD') AS o_TransactionEffectiveDate,
	TransactionExpirationDate,
	-- *INF*: DECODE(TRUE,
	-- DeletedName_Flag='1',TO_CHAR(TransactionEffectiveDate,'YYMMDD'),
	-- TO_CHAR(TransactionExpirationDate,'YYMMDD')
	-- )
	DECODE(
	    TRUE,
	    DeletedName_Flag = '1', TO_CHAR(TransactionEffectiveDate, 'YYMMDD'),
	    TO_CHAR(TransactionExpirationDate, 'YYMMDD')
	) AS o_TransactionExpirationDate,
	BusinessOrIndividual,
	NameLinkIdentifier,
	DeletedName_Flag,
	Order_Seq,
	Name_cleansed,
	'PrimaryEntity' AS PartyAssociationType1,
	'SecondaryEntity' AS PartyAssociationType2,
	NameInsured
	FROM JNR_Party_TO_LinkData
),
LKP_Line AS (
	SELECT
	WCTrackHistoryID,
	AuditPeriod,
	RatingPlan,
	PrimaryLocationState,
	InterstateRiskID,
	MinimumPremiumMaximum,
	MinimumPremiumMaximumState,
	in_WCTrackHistoryID
	FROM (
		SELECT WCTrackHistoryID as WCTrackHistoryID
		      ,AuditPeriod as AuditPeriod
		      ,RatingPlan as RatingPlan
		      ,PrimaryLocationState as PrimaryLocationState
		      ,InterstateRiskID as InterstateRiskID
		     ,MinimumPremiumMaximum as MinimumPremiumMaximum
		     ,MinimumPremiumMaximumState as MinimumPremiumMaximumState
		  FROM dbo.WorkWCLine
		  WHERE Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WCTrackHistoryID ORDER BY WCTrackHistoryID) = 1
),
LKP_Party AS (
	SELECT
	BureauUnemploymentNumberState,
	StateUnemploymentNumber,
	IN_WCTrackHistoryID,
	WCTrackHistoryID
	FROM (
		Select DISTINCT WCTrackHistoryID AS WCTrackHistoryID,
		BureauUnemploymentNumberState AS BureauUnemploymentNumberState,
		StateUnemploymentNumber AS StateUnemploymentNumber from WorkWCParty
		where BureauUnemploymentNumberState IS NOT NULL AND StateUnemploymentNumber IS NOT NULL
		AND Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WCTrackHistoryID ORDER BY BureauUnemploymentNumberState) = 1
),
LKP_StateTerm AS (
	SELECT
	WCTrackHistoryID,
	State,
	EmployeeLeasing,
	EmployeeLeasingRatingOption,
	IntrastateRiskid,
	UnemploymentIDNumber,
	TotalStandardPremium,
	in_WCTrackHistoryID,
	in_PrimaryLocationState
	FROM (
		SELECT WCTrackHistoryID as WCTrackHistoryID
		      ,State as State
		      ,EmployeeLeasing as EmployeeLeasing
		      ,EmployeeLeasingRatingOption as EmployeeLeasingRatingOption
		      ,IntrastateRiskid as IntrastateRiskid
		     ,UnemploymentIDNumber as UnemploymentIDNumber
		  FROM dbo.WorkWCStateTerm
		WHERE Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WCTrackHistoryID,State ORDER BY WCTrackHistoryID) = 1
),
EXP_Record02_TGT_DataCollect AS (
	SELECT
	CURRENT_TIMESTAMP AS ExtractDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	EXP_Record02_DataPrep.WCTrackHistoryID,
	EXP_Record02_DataPrep.LinkData,
	'02' AS RecordTypeCode,
	EXP_Record02_DataPrep.BusinessOrIndividual,
	-- *INF*: DECODE (TRUE,
	-- BusinessOrIndividual='F','Business',
	-- BusinessOrIndividual='T','Individual',
	-- NULL)
	DECODE(
	    TRUE,
	    BusinessOrIndividual = 'F', 'Business',
	    BusinessOrIndividual = 'T', 'Individual',
	    NULL
	) AS v_TEMP_NAMETYPECODE,
	-- *INF*: IIF(BusinessOrIndividual='F',:LKP.LKP_SupWCPOLS('DCT','Business','WCPOLS02Record','NameTypeCode'),
	-- IIF(BusinessOrIndividual='T',:LKP.LKP_SupWCPOLS('DCT','Individual','WCPOLS02Record','NameTypeCode'),''))
	IFF(
	    BusinessOrIndividual = 'F',
	    LKP_SUPWCPOLS__DCT_Business_WCPOLS02Record_NameTypeCode.WCPOLSCode,
	    IFF(
	        BusinessOrIndividual = 'T',
	        LKP_SUPWCPOLS__DCT_Individual_WCPOLS02Record_NameTypeCode.WCPOLSCode,
	        ''
	    )
	) AS o_NameTypeCode,
	EXP_Record02_DataPrep.NameLinkIdentifier,
	LKP_StateTerm.EmployeeLeasing,
	LKP_StateTerm.EmployeeLeasingRatingOption,
	-- *INF*: IIF(EmployeeLeasing='T', :LKP.LKP_SupWCPOLS('DCT',EmployeeLeasingRatingOption,'WCPOLS02Record','ProfessionalEmployerOrganizationOrClientCompanyCode'),NULL)
	IFF(
	    EmployeeLeasing = 'T',
	    LKP_SUPWCPOLS__DCT_EmployeeLeasingRatingOption_WCPOLS02Record_ProfessionalEmployerOrganizationOrClientCompanyCode.WCPOLSCode,
	    NULL
	) AS o_ProfessionalEmployerOrganizationOrClientCompanyCode,
	EXP_Record02_DataPrep.Name,
	-- *INF*: ltrim(rtrim(Name))
	ltrim(rtrim(Name)) AS NameInsured,
	EXP_Record02_DataPrep.NameInsured AS NameInsured_02record,
	-- *INF*: iif(Order_Seq ='2', ltrim(rtrim(Name)),NameInsured_02record)
	IFF(Order_Seq = '2', ltrim(rtrim(Name)), NameInsured_02record) AS v_NameInsured_02record,
	v_NameInsured_02record AS o_NameInsured_02record,
	EXP_Record02_DataPrep.FEIN,
	-- *INF*: LTRIM(RTRIM(REPLACECHR(1,FEIN,'-','')))
	-- 
	-- 
	LTRIM(RTRIM(REGEXP_REPLACE(FEIN,'-',''))) AS FederalEmployerIdentificationNumber,
	'001' AS ContinuationSequenceNumber,
	EXP_Record02_DataPrep.o_EntityType AS EntityType,
	LKP_Line.PrimaryLocationState,
	LKP_Party.BureauUnemploymentNumberState AS IN_BureauUnemploymentNumberState,
	-- *INF*: :LKP.LKP_SupWCPOLS('DCT',IN_BureauUnemploymentNumberState,'WCPOLS02Record','StateCodeRecord02')
	LKP_SUPWCPOLS__DCT_IN_BureauUnemploymentNumberState_WCPOLS02Record_StateCodeRecord02.WCPOLSCode AS o_StateCode01,
	LKP_Party.StateUnemploymentNumber AS IN_StateUnemploymentNumber,
	LKP_StateTerm.UnemploymentIDNumber AS StateUnemploymentNumber01,
	'00' AS NameLinkCounterIdentifier,
	EXP_Record02_DataPrep.o_TransactionEffectiveDate AS PolicyChangeEffectiveDate,
	EXP_Record02_DataPrep.o_TransactionExpirationDate AS PolicyChangeExpirationDate,
	EXP_Record02_DataPrep.Order_Seq,
	'DBA' AS o_ProcessName,
	EXP_Record02_DataPrep.Name_cleansed,
	-- *INF*: :LKP.LKP_WORKWCPARTY(Name_cleansed,WCTrackHistoryID,'PrimaryEntity')
	-- 
	-- 
	LKP_WORKWCPARTY_Name_cleansed_WCTrackHistoryID_PrimaryEntity.AOIEntityType AS v_Primary_AOIEntityType,
	-- *INF*: :LKP.LKP_WORKWCPARTY(Name_cleansed,WCTrackHistoryID,'SecondaryEntity')
	-- 
	LKP_WORKWCPARTY_Name_cleansed_WCTrackHistoryID_SecondaryEntity.AOIEntityType AS v_Secondary_AOIEntityType,
	-- *INF*: :LKP.LKP_WORKWCPARTY(Name_cleansed,WCTrackHistoryID,'AdditionalInsured')
	-- 
	LKP_WORKWCPARTY_Name_cleansed_WCTrackHistoryID_AdditionalInsured.AOIEntityType AS v_AdditionalInsured,
	-- *INF*: DECODE(TRUE,
	-- not isnull(v_Primary_AOIEntityType), v_Primary_AOIEntityType,
	-- (isnull(v_Primary_AOIEntityType) and not isnull(v_Secondary_AOIEntityType)) ,v_Secondary_AOIEntityType,
	-- (isnull(v_Primary_AOIEntityType) and isnull(v_Secondary_AOIEntityType)  and not isnull(v_AdditionalInsured)) ,v_AdditionalInsured,
	-- v_Primary_AOIEntityType)
	-- 
	-- 
	DECODE(
	    TRUE,
	    v_Primary_AOIEntityType IS NOT NULL, v_Primary_AOIEntityType,
	    (v_Primary_AOIEntityType IS NULL and v_Secondary_AOIEntityType IS NOT NULL), v_Secondary_AOIEntityType,
	    (v_Primary_AOIEntityType IS NULL and v_Secondary_AOIEntityType IS NULL and v_AdditionalInsured IS NOT NULL), v_AdditionalInsured,
	    v_Primary_AOIEntityType
	) AS v_AOIEntityType,
	-- *INF*: IIF(ISNULL(:LKP.LKP_SupWCPOLS('DCT',v_AOIEntityType,'WCPols02Record','LegalNatureOfEntityCode')),'99',:LKP.LKP_SupWCPOLS('DCT',v_AOIEntityType,'WCPols02Record','LegalNatureOfEntityCode'))
	IFF(
	    LKP_SUPWCPOLS__DCT_v_AOIEntityType_WCPols02Record_LegalNatureOfEntityCode.WCPOLSCode IS NULL,
	    '99',
	    LKP_SUPWCPOLS__DCT_v_AOIEntityType_WCPols02Record_LegalNatureOfEntityCode.WCPOLSCode
	) AS LegalNatureOfEntityCode
	FROM EXP_Record02_DataPrep
	LEFT JOIN LKP_Line
	ON LKP_Line.WCTrackHistoryID = JNR_Party_TO_LinkData.WCTrackHistoryID
	LEFT JOIN LKP_Party
	ON LKP_Party.WCTrackHistoryID = JNR_Party_TO_LinkData.WCTrackHistoryID
	LEFT JOIN LKP_StateTerm
	ON LKP_StateTerm.WCTrackHistoryID = LKP_Line.WCTrackHistoryID AND LKP_StateTerm.State = LKP_Line.PrimaryLocationState
	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_Business_WCPOLS02Record_NameTypeCode
	ON LKP_SUPWCPOLS__DCT_Business_WCPOLS02Record_NameTypeCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_Business_WCPOLS02Record_NameTypeCode.SourceCode = 'Business'
	AND LKP_SUPWCPOLS__DCT_Business_WCPOLS02Record_NameTypeCode.TableName = 'WCPOLS02Record'
	AND LKP_SUPWCPOLS__DCT_Business_WCPOLS02Record_NameTypeCode.ProcessName = 'NameTypeCode'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_Individual_WCPOLS02Record_NameTypeCode
	ON LKP_SUPWCPOLS__DCT_Individual_WCPOLS02Record_NameTypeCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_Individual_WCPOLS02Record_NameTypeCode.SourceCode = 'Individual'
	AND LKP_SUPWCPOLS__DCT_Individual_WCPOLS02Record_NameTypeCode.TableName = 'WCPOLS02Record'
	AND LKP_SUPWCPOLS__DCT_Individual_WCPOLS02Record_NameTypeCode.ProcessName = 'NameTypeCode'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_EmployeeLeasingRatingOption_WCPOLS02Record_ProfessionalEmployerOrganizationOrClientCompanyCode
	ON LKP_SUPWCPOLS__DCT_EmployeeLeasingRatingOption_WCPOLS02Record_ProfessionalEmployerOrganizationOrClientCompanyCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_EmployeeLeasingRatingOption_WCPOLS02Record_ProfessionalEmployerOrganizationOrClientCompanyCode.SourceCode = EmployeeLeasingRatingOption
	AND LKP_SUPWCPOLS__DCT_EmployeeLeasingRatingOption_WCPOLS02Record_ProfessionalEmployerOrganizationOrClientCompanyCode.TableName = 'WCPOLS02Record'
	AND LKP_SUPWCPOLS__DCT_EmployeeLeasingRatingOption_WCPOLS02Record_ProfessionalEmployerOrganizationOrClientCompanyCode.ProcessName = 'ProfessionalEmployerOrganizationOrClientCompanyCode'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_IN_BureauUnemploymentNumberState_WCPOLS02Record_StateCodeRecord02
	ON LKP_SUPWCPOLS__DCT_IN_BureauUnemploymentNumberState_WCPOLS02Record_StateCodeRecord02.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_IN_BureauUnemploymentNumberState_WCPOLS02Record_StateCodeRecord02.SourceCode = IN_BureauUnemploymentNumberState
	AND LKP_SUPWCPOLS__DCT_IN_BureauUnemploymentNumberState_WCPOLS02Record_StateCodeRecord02.TableName = 'WCPOLS02Record'
	AND LKP_SUPWCPOLS__DCT_IN_BureauUnemploymentNumberState_WCPOLS02Record_StateCodeRecord02.ProcessName = 'StateCodeRecord02'

	LEFT JOIN LKP_WORKWCPARTY LKP_WORKWCPARTY_Name_cleansed_WCTrackHistoryID_PrimaryEntity
	ON LKP_WORKWCPARTY_Name_cleansed_WCTrackHistoryID_PrimaryEntity.Name = Name_cleansed
	AND LKP_WORKWCPARTY_Name_cleansed_WCTrackHistoryID_PrimaryEntity.WCTrackHistoryID = WCTrackHistoryID
	AND LKP_WORKWCPARTY_Name_cleansed_WCTrackHistoryID_PrimaryEntity.PartyAssociationType = 'PrimaryEntity'

	LEFT JOIN LKP_WORKWCPARTY LKP_WORKWCPARTY_Name_cleansed_WCTrackHistoryID_SecondaryEntity
	ON LKP_WORKWCPARTY_Name_cleansed_WCTrackHistoryID_SecondaryEntity.Name = Name_cleansed
	AND LKP_WORKWCPARTY_Name_cleansed_WCTrackHistoryID_SecondaryEntity.WCTrackHistoryID = WCTrackHistoryID
	AND LKP_WORKWCPARTY_Name_cleansed_WCTrackHistoryID_SecondaryEntity.PartyAssociationType = 'SecondaryEntity'

	LEFT JOIN LKP_WORKWCPARTY LKP_WORKWCPARTY_Name_cleansed_WCTrackHistoryID_AdditionalInsured
	ON LKP_WORKWCPARTY_Name_cleansed_WCTrackHistoryID_AdditionalInsured.Name = Name_cleansed
	AND LKP_WORKWCPARTY_Name_cleansed_WCTrackHistoryID_AdditionalInsured.WCTrackHistoryID = WCTrackHistoryID
	AND LKP_WORKWCPARTY_Name_cleansed_WCTrackHistoryID_AdditionalInsured.PartyAssociationType = 'AdditionalInsured'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_v_AOIEntityType_WCPols02Record_LegalNatureOfEntityCode
	ON LKP_SUPWCPOLS__DCT_v_AOIEntityType_WCPols02Record_LegalNatureOfEntityCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_v_AOIEntityType_WCPols02Record_LegalNatureOfEntityCode.SourceCode = v_AOIEntityType
	AND LKP_SUPWCPOLS__DCT_v_AOIEntityType_WCPols02Record_LegalNatureOfEntityCode.TableName = 'WCPols02Record'
	AND LKP_SUPWCPOLS__DCT_v_AOIEntityType_WCPols02Record_LegalNatureOfEntityCode.ProcessName = 'LegalNatureOfEntityCode'

),
FIL_DBANames AS (
	SELECT
	AuditId, 
	ExtractDate, 
	WCTrackHistoryID, 
	o_ProcessName AS ProcessName, 
	NameInsured, 
	Order_Seq
	FROM EXP_Record02_TGT_DataCollect
	WHERE Order_Seq='2'
),
WorkWCProcessUpdateTable AS (
	INSERT INTO WorkWCProcessUpdateTable
	(Auditid, ExtractDate, WCTrackHistoryID, ProcessName, AttributeValue)
	SELECT 
	AuditId AS AUDITID, 
	EXTRACTDATE, 
	WCTRACKHISTORYID, 
	PROCESSNAME, 
	NameInsured AS ATTRIBUTEVALUE
	FROM FIL_DBANames
),
LKP_WorkWCPolicy AS (
	SELECT
	WCTrackHistoryID,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	in_WCTrackHistoryID
	FROM (
		SELECT WCTrackHistoryID as WCTrackHistoryID
		      ,TransactionEffectiveDate as TransactionEffectiveDate
			,TransactionExpirationDate as TransactionExpirationDate
		  FROM dbo.WorkWCPolicy
		WHERE Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WCTrackHistoryID ORDER BY WCTrackHistoryID) = 1
),
EXP_WBMI_Format AS (
	SELECT
	SQ_WCPols00Record.WCTrackHistoryID,
	SQ_WCPols00Record.LinkData,
	SQ_WCPols00Record.AuditId,
	LKP_WorkWCPolicy.TransactionEffectiveDate,
	LKP_WorkWCPolicy.TransactionExpirationDate,
	'1900 South 18th Avenue' AS Address1,
	' ' AS Address2,
	'West Bend' AS City,
	'WI' AS StateProv,
	'53095' AS PostalCode,
	'USA' AS Country,
	'WBMI' AS LocationType,
	'999' AS NameLinkIdentifier
	FROM SQ_WCPols00Record
	LEFT JOIN LKP_WorkWCPolicy
	ON LKP_WorkWCPolicy.WCTrackHistoryID = SQ_WCPols00Record.WCTrackHistoryID
),
SQ_WorkWCLocation AS (
	with CTE_Party as (
	Select ltrim(rtrim(DoingBusinessAs)) Name,PartyAssociationType,WC_LocationId,WCTrackHistoryID,CASE when BusinessOrIndividual=1 then '1' else '0' end BusinessOrIndividual from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCParty
	union
	select 
	case when BusinessOrIndividual=1 and EntityType='Partnership' then 
	case when rtrim(ltrim(LastName)) is null and rtrim(ltrim(FirstName)) is null and rtrim(ltrim(MiddleName)) is null 
	then  rtrim(ltrim(Name)) else ISNULL(rtrim(ltrim(LastName)),'')+' '+ISNULL(rtrim(ltrim(FirstName)),'')+' '+ISNULL(rtrim(ltrim(MiddleName)),'') end else rtrim(ltrim(Name)) end  Name,
	
	PartyAssociationType,WC_LocationId,WCTrackHistoryID,CASE when BusinessOrIndividual=1 then '1' else '0' end BusinessOrIndividual
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCParty)
	
	
	Select WCTrackHistoryID,
	Name,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	NAICSCode,
	Address1,
	Address2,
	City,
	StateProv,
	PostalCode,
	Country,
	LocationType,
	LocationNumber,
	LocationOrder,
	PolicyKey,
	isnull(min(WC_Locationid) over(partition by WCTrackHistoryid,Name,rtrim(ltrim(Address1+' '+Address2)),City,StateProv,PostalCode),A.LocationOrder) WC_Locationid
	,Max(BusinessOrIndividual) over(partition by WCTrackHistoryid) BusinessOrIndividual
	,PartyAssociationType
	,case when LocationDeletedIndicator='1' then '1' else '0' END LocationDeletedIndicator
	from (
	Select A.WCTrackHistoryID,upper(REPLACE(B.Name,char(13)+char(10),'')) Name,A.TransactionEffectiveDate,A.TransactionExpirationDate,A.NAICSCode
	,ltrim(rtrim(upper(ISNULL(Address1,''))+' '+upper(ISNULL(Address2,'')))) Address1
	,'' Address2
	,upper(ISNULL(C.City,'')) City
	,upper(ISNULL(C.StateProv,'')) StateProv
	,ISNULL(C.PostalCode,'') PostalCode
	,upper(ISNULL(C.Country,'')) Country
	,C.LocationType,C.LocationNumber
	,Case when C.LocationType='Account' then 1
	when C.LocationType='Location' then 2
	else 3 end LocationOrder
	,A.PolicyKey,C.WC_LocationId,BusinessOrIndividual,PartyAssociationType,LocationDeletedIndicator
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCPolicy A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCLocation C
	on A.WCTrackHistoryID=C.WCTrackHistoryID
	right join CTE_Party B
	on B.WCTrackHistoryID=C.WCTrackHistoryID
	and (B.PartyAssociationType=C.LocationType or B.WC_LocationId=C.WC_LocationId)
	and C.LocationType in ('Account','Agency','Location')
	where A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and B.Name is not null
	--and (C.LocationDeletedIndicator=0 or C.LocationDeletedIndicator is null)
	@{pipeline().parameters.WHERE_CLAUSE_03}
	) A
	
	order by WCTrackHistoryID,Name,LocationOrder,LocationType,Address1,Address2,City,StateProv,PostalCode,Country,WC_Locationid
),
EXP_SRC AS (
	SELECT
	WCTrackHistoryID,
	Name,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	NAICSCode,
	Address1,
	Address2,
	City,
	StateProv,
	PostalCode,
	Country,
	LocationType,
	LocationNumber,
	LocationOrder,
	PolicyKey,
	WC_LocationId,
	BusinessOrIndividual,
	PartyAssociationType,
	LocationDeletedIndicator,
	-- *INF*: IIF(NOT ISNULL(Address1) AND LTRIM(RTRIM(Address1))<>'',LTRIM(RTRIM(Address1)),'')
	IFF(Address1 IS NULL AND LTRIM(RTRIM(Address1)) <NOT > '', LTRIM(RTRIM(Address1)), '') AS v_Address1,
	-- *INF*: IIF(NOT ISNULL(Address2) AND LTRIM(RTRIM(Address2))<>'',LTRIM(RTRIM(Address2)),'')
	IFF(Address2 IS NULL AND LTRIM(RTRIM(Address2)) <NOT > '', LTRIM(RTRIM(Address2)), '') AS v_Address2,
	-- *INF*: IIF(NOT ISNULL(City) AND LTRIM(RTRIM(City))<>'',LTRIM(RTRIM(City)),'')
	IFF(City IS NULL AND LTRIM(RTRIM(City)) <NOT > '', LTRIM(RTRIM(City)), '') AS v_City,
	-- *INF*: IIF(NOT ISNULL(StateProv) AND LTRIM(RTRIM(StateProv))<>'',LTRIM(RTRIM(StateProv)),'')
	IFF(StateProv IS NULL AND LTRIM(RTRIM(StateProv)) <NOT > '', LTRIM(RTRIM(StateProv)), '') AS v_StateProv,
	-- *INF*: IIF(NOT ISNULL(PostalCode) AND LTRIM(RTRIM(PostalCode))<>'',LTRIM(RTRIM(SUBSTR(PostalCode,1,5))),'')
	IFF(
	    PostalCode IS NULL AND LTRIM(RTRIM(PostalCode)) <NOT > '',
	    LTRIM(RTRIM(SUBSTR(PostalCode, 1, 5))),
	    ''
	) AS v_PostalCode_Mini,
	-- *INF*: UPPER(LTRIM(RTRIM(WCTrackHistoryID||LTRIM(RTRIM(v_Address1))||LTRIM(RTRIM(v_Address2))||LTRIM(RTRIM(v_City))||LTRIM(RTRIM(v_StateProv))||LTRIM(RTRIM(v_PostalCode_Mini)))))
	UPPER(LTRIM(RTRIM(WCTrackHistoryID || LTRIM(RTRIM(v_Address1)) || LTRIM(RTRIM(v_Address2)) || LTRIM(RTRIM(v_City)) || LTRIM(RTRIM(v_StateProv)) || LTRIM(RTRIM(v_PostalCode_Mini))))) AS o_AddressCompare,
	-- *INF*: LTRIM(RTRIM(v_PostalCode_Mini))
	LTRIM(RTRIM(v_PostalCode_Mini)) AS o_PostalCode_Mini
	FROM SQ_WorkWCLocation
),
LKP_Party_Location AS (
	SELECT
	Name,
	FirstName,
	LastName,
	MiddleName,
	WCTrackHistoryID
	FROM (
		Select WCTrackHistoryID AS WCTrackHistoryID,
		upper(REPLACE(ltrim(rtrim(Name)),char(13)+char(10),'')) AS Name,
		upper(REPLACE(ltrim(rtrim(FirstName)),char(13)+char(10),'')) AS FirstName,
		upper(REPLACE(ltrim(rtrim(LastName)),char(13)+char(10),'')) AS LastName,
		upper(REPLACE(ltrim(rtrim(MiddleName)),char(13)+char(10),'')) AS MiddleName from
		(
		select WCTrackHistoryID AS WCTrackHistoryID,
		case when BusinessOrIndividual=1 then 
		case when rtrim(ltrim(LastName)) is null and rtrim(ltrim(FirstName)) is null and rtrim(ltrim(MiddleName)) is null 
		then  rtrim(ltrim(Name)) else ISNULL(rtrim(ltrim(LastName)),'')+' '+ISNULL(rtrim(ltrim(FirstName)),'')+' '+ISNULL(rtrim(ltrim(MiddleName)),'') 
		end else rtrim(ltrim(Name)) end Name ,FirstName,LastName,MiddleName
		from DBO.WorkWCParty
		where PartyAssociationType='Account'
		and BusinessOrIndividual=1
		) A
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WCTrackHistoryID ORDER BY Name) = 1
),
EXP_SrcDataCollect_Location AS (
	SELECT
	EXP_SRC.WCTrackHistoryID,
	EXP_SRC.Name,
	-- *INF*: LTRIM(RTRIM(Upper(REPLACECHR(1,Name,chr(13)||chr(10),''))))
	LTRIM(RTRIM(Upper(REGEXP_REPLACE(Name,chr(13) || chr(10),'')))) AS o_Name,
	-- *INF*: REPLACECHR(1,Name,' ','')
	REGEXP_REPLACE(Name,' ','') AS v_CleansedName,
	LKP_Party_Location.Name AS Name_lkp,
	LKP_Party_Location.FirstName,
	LKP_Party_Location.LastName,
	LKP_Party_Location.MiddleName,
	-- *INF*: REPLACECHR(1,IIF(ISNULL(LTRIM(RTRIM(FirstName))),'',LTRIM(RTRIM(FirstName)))||IIF(ISNULL(LTRIM(RTRIM(LastName))),'',LTRIM(RTRIM(LastName))),' ','')
	-- 
	REGEXP_REPLACE(
	    IFF(
	        LTRIM(RTRIM(FirstName)) IS NULL, '', LTRIM(RTRIM(FirstName))
	    ) || 
	    IFF(
	        LTRIM(RTRIM(LastName)) IS NULL, '', LTRIM(RTRIM(LastName))
	    ),' ','') AS v_FirstLast,
	-- *INF*: REPLACECHR(1,IIF(ISNULL(LTRIM(RTRIM(LastName))),'',LTRIM(RTRIM(LastName)))||IIF(ISNULL(LTRIM(RTRIM(FirstName))),'',LTRIM(RTRIM(FirstName))),' ','')
	REGEXP_REPLACE(
	    IFF(
	        LTRIM(RTRIM(LastName)) IS NULL, '', LTRIM(RTRIM(LastName))
	    ) || 
	    IFF(
	        LTRIM(RTRIM(FirstName)) IS NULL, '', LTRIM(RTRIM(FirstName))
	    ),' ','') AS v_LastFirst,
	-- *INF*: REPLACECHR(1,IIF(ISNULL(LTRIM(RTRIM(LastName))),'',LTRIM(RTRIM(LastName)))||IIF(ISNULL(LTRIM(RTRIM(FirstName))),'',LTRIM(RTRIM(FirstName)))||IIF(ISNULL(LTRIM(RTRIM(MiddleName))),'',LTRIM(RTRIM(MiddleName))),' ','')
	REGEXP_REPLACE(
	    IFF(
	        LTRIM(RTRIM(LastName)) IS NULL, '', LTRIM(RTRIM(LastName))
	    ) || 
	    IFF(
	        LTRIM(RTRIM(FirstName)) IS NULL, '', LTRIM(RTRIM(FirstName))
	    ) || 
	    IFF(
	        LTRIM(RTRIM(MiddleName)) IS NULL, '', LTRIM(RTRIM(MiddleName))
	    ),' ','') AS v_LastFirstMiddleName,
	-- *INF*: REPLACECHR(1,IIF(ISNULL(LTRIM(RTRIM(FirstName))),'',LTRIM(RTRIM(FirstName)))||IIF(ISNULL(LTRIM(RTRIM(LastName))),'',LTRIM(RTRIM(LastName)))||IIF(ISNULL(LTRIM(RTRIM(MiddleName))),'',LTRIM(RTRIM(MiddleName))),' ','')
	REGEXP_REPLACE(
	    IFF(
	        LTRIM(RTRIM(FirstName)) IS NULL, '', LTRIM(RTRIM(FirstName))
	    ) || 
	    IFF(
	        LTRIM(RTRIM(LastName)) IS NULL, '', LTRIM(RTRIM(LastName))
	    ) || 
	    IFF(
	        LTRIM(RTRIM(MiddleName)) IS NULL, '', LTRIM(RTRIM(MiddleName))
	    ),' ','') AS v_FirstLastMiddleName,
	-- *INF*: REPLACECHR(1,IIF(ISNULL(LTRIM(RTRIM(FirstName))),'',LTRIM(RTRIM(FirstName)))||IIF(ISNULL(LTRIM(RTRIM(MiddleName))),'',LTRIM(RTRIM(MiddleName)))||IIF(ISNULL(LTRIM(RTRIM(LastName))),'',LTRIM(RTRIM(LastName))),' ','')
	REGEXP_REPLACE(
	    IFF(
	        LTRIM(RTRIM(FirstName)) IS NULL, '', LTRIM(RTRIM(FirstName))
	    ) || 
	    IFF(
	        LTRIM(RTRIM(MiddleName)) IS NULL, '', LTRIM(RTRIM(MiddleName))
	    ) || 
	    IFF(
	        LTRIM(RTRIM(LastName)) IS NULL, '', LTRIM(RTRIM(LastName))
	    ),' ','') AS v_FirstMiddleLastName,
	-- *INF*: REPLACECHR(1,IIF(ISNULL(LTRIM(RTRIM(LastName))),'',LTRIM(RTRIM(LastName)))||IIF(ISNULL(LTRIM(RTRIM(MiddleName))),'',LTRIM(RTRIM(MiddleName)))||IIF(ISNULL(LTRIM(RTRIM(FirstName))),'',LTRIM(RTRIM(FirstName))),' ','')
	REGEXP_REPLACE(
	    IFF(
	        LTRIM(RTRIM(LastName)) IS NULL, '', LTRIM(RTRIM(LastName))
	    ) || 
	    IFF(
	        LTRIM(RTRIM(MiddleName)) IS NULL, '', LTRIM(RTRIM(MiddleName))
	    ) || 
	    IFF(
	        LTRIM(RTRIM(FirstName)) IS NULL, '', LTRIM(RTRIM(FirstName))
	    ),' ','') AS v_LastMiddleFirstName,
	-- *INF*: REPLACECHR(1,IIF(ISNULL(LTRIM(RTRIM(MiddleName))),'',LTRIM(RTRIM(MiddleName)))||IIF(ISNULL(LTRIM(RTRIM(FirstName))),'',LTRIM(RTRIM(FirstName)))||IIF(ISNULL(LTRIM(RTRIM(LastName))),'',LTRIM(RTRIM(LastName))),' ','')
	REGEXP_REPLACE(
	    IFF(
	        LTRIM(RTRIM(MiddleName)) IS NULL, '', LTRIM(RTRIM(MiddleName))
	    ) || 
	    IFF(
	        LTRIM(RTRIM(FirstName)) IS NULL, '', LTRIM(RTRIM(FirstName))
	    ) || 
	    IFF(
	        LTRIM(RTRIM(LastName)) IS NULL, '', LTRIM(RTRIM(LastName))
	    ),' ','') AS v_MiddleFirstLastName,
	-- *INF*: REPLACECHR(1,IIF(ISNULL(LTRIM(RTRIM(MiddleName))),'',LTRIM(RTRIM(MiddleName)))||IIF(ISNULL(LTRIM(RTRIM(LastName))),'',LTRIM(RTRIM(LastName)))||IIF(ISNULL(LTRIM(RTRIM(FirstName))),'',LTRIM(RTRIM(FirstName))),' ','')
	REGEXP_REPLACE(
	    IFF(
	        LTRIM(RTRIM(MiddleName)) IS NULL, '', LTRIM(RTRIM(MiddleName))
	    ) || 
	    IFF(
	        LTRIM(RTRIM(LastName)) IS NULL, '', LTRIM(RTRIM(LastName))
	    ) || 
	    IFF(
	        LTRIM(RTRIM(FirstName)) IS NULL, '', LTRIM(RTRIM(FirstName))
	    ),' ','') AS v_MiddleLastFirstName,
	-- *INF*: REPLACECHR(1,IIF(ISNULL(LTRIM(RTRIM(MiddleName))),'',LTRIM(RTRIM(MiddleName)))||IIF(ISNULL(LTRIM(RTRIM(FirstName))),'',LTRIM(RTRIM(FirstName))),' ','')
	REGEXP_REPLACE(
	    IFF(
	        LTRIM(RTRIM(MiddleName)) IS NULL, '', LTRIM(RTRIM(MiddleName))
	    ) || 
	    IFF(
	        LTRIM(RTRIM(FirstName)) IS NULL, '', LTRIM(RTRIM(FirstName))
	    ),' ','') AS v_MiddleFirstName,
	-- *INF*: REPLACECHR(1,IIF(ISNULL(LTRIM(RTRIM(FirstName))),'',LTRIM(RTRIM(FirstName)))||IIF(ISNULL(LTRIM(RTRIM(MiddleName))),'',LTRIM(RTRIM(MiddleName))),' ','')
	REGEXP_REPLACE(
	    IFF(
	        LTRIM(RTRIM(FirstName)) IS NULL, '', LTRIM(RTRIM(FirstName))
	    ) || 
	    IFF(
	        LTRIM(RTRIM(MiddleName)) IS NULL, '', LTRIM(RTRIM(MiddleName))
	    ),' ','') AS v_FirstMiddleName,
	-- *INF*: REPLACECHR(1,IIF(ISNULL(LTRIM(RTRIM(LastName))),'',LTRIM(RTRIM(LastName)))||IIF(ISNULL(LTRIM(RTRIM(MiddleName))),'',LTRIM(RTRIM(MiddleName))),' ','')
	REGEXP_REPLACE(
	    IFF(
	        LTRIM(RTRIM(LastName)) IS NULL, '', LTRIM(RTRIM(LastName))
	    ) || 
	    IFF(
	        LTRIM(RTRIM(MiddleName)) IS NULL, '', LTRIM(RTRIM(MiddleName))
	    ),' ','') AS v_LastMiddleName,
	-- *INF*: REPLACECHR(1,IIF(ISNULL(LTRIM(RTRIM(MiddleName))),'',LTRIM(RTRIM(MiddleName)))||IIF(ISNULL(LTRIM(RTRIM(LastName))),'',LTRIM(RTRIM(LastName))),' ','')
	REGEXP_REPLACE(
	    IFF(
	        LTRIM(RTRIM(MiddleName)) IS NULL, '', LTRIM(RTRIM(MiddleName))
	    ) || 
	    IFF(
	        LTRIM(RTRIM(LastName)) IS NULL, '', LTRIM(RTRIM(LastName))
	    ),' ','') AS v_MiddleLastName,
	-- *INF*: DECODE(TRUE,
	-- PartyAssociationType='PrimaryEntity' AND BusinessOrIndividual='1' AND v_CleansedName=v_FirstLast,LTRIM(RTRIM(Name_lkp)),
	-- PartyAssociationType='PrimaryEntity' AND BusinessOrIndividual='1' AND v_CleansedName=v_LastFirst,LTRIM(RTRIM(Name_lkp)),
	-- PartyAssociationType='PrimaryEntity' AND BusinessOrIndividual='1' AND v_CleansedName=v_LastFirstMiddleName,LTRIM(RTRIM(Name_lkp)),
	-- PartyAssociationType='PrimaryEntity' AND BusinessOrIndividual='1' AND v_CleansedName=v_FirstLastMiddleName,LTRIM(RTRIM(Name_lkp)),
	-- PartyAssociationType='PrimaryEntity' AND BusinessOrIndividual='1' AND v_CleansedName=v_FirstMiddleLastName,LTRIM(RTRIM(Name_lkp)),
	-- PartyAssociationType='PrimaryEntity' AND BusinessOrIndividual='1' AND  v_CleansedName=v_LastMiddleFirstName,LTRIM(RTRIM(Name_lkp)),
	-- PartyAssociationType='PrimaryEntity' AND BusinessOrIndividual='1' AND v_CleansedName=v_MiddleFirstLastName,LTRIM(RTRIM(Name_lkp)),
	-- PartyAssociationType='PrimaryEntity' AND BusinessOrIndividual='1' AND v_CleansedName=v_MiddleLastFirstName,LTRIM(RTRIM(Name_lkp)),
	-- PartyAssociationType='PrimaryEntity' AND BusinessOrIndividual='1' AND v_CleansedName=v_MiddleFirstName,LTRIM(RTRIM(Name_lkp)),
	-- PartyAssociationType='PrimaryEntity' AND BusinessOrIndividual='1' AND v_CleansedName=v_FirstMiddleName,LTRIM(RTRIM(Name_lkp)),
	-- PartyAssociationType='PrimaryEntity' AND BusinessOrIndividual='1' AND v_CleansedName=v_LastMiddleName,LTRIM(RTRIM(Name_lkp)),
	-- PartyAssociationType='PrimaryEntity' AND BusinessOrIndividual='1' AND v_CleansedName=v_MiddleLastName,LTRIM(RTRIM(Name_lkp)),
	-- LTRIM(RTRIM(Name)))
	DECODE(
	    TRUE,
	    PartyAssociationType = 'PrimaryEntity' AND BusinessOrIndividual = '1' AND v_CleansedName = v_FirstLast, LTRIM(RTRIM(Name_lkp)),
	    PartyAssociationType = 'PrimaryEntity' AND BusinessOrIndividual = '1' AND v_CleansedName = v_LastFirst, LTRIM(RTRIM(Name_lkp)),
	    PartyAssociationType = 'PrimaryEntity' AND BusinessOrIndividual = '1' AND v_CleansedName = v_LastFirstMiddleName, LTRIM(RTRIM(Name_lkp)),
	    PartyAssociationType = 'PrimaryEntity' AND BusinessOrIndividual = '1' AND v_CleansedName = v_FirstLastMiddleName, LTRIM(RTRIM(Name_lkp)),
	    PartyAssociationType = 'PrimaryEntity' AND BusinessOrIndividual = '1' AND v_CleansedName = v_FirstMiddleLastName, LTRIM(RTRIM(Name_lkp)),
	    PartyAssociationType = 'PrimaryEntity' AND BusinessOrIndividual = '1' AND v_CleansedName = v_LastMiddleFirstName, LTRIM(RTRIM(Name_lkp)),
	    PartyAssociationType = 'PrimaryEntity' AND BusinessOrIndividual = '1' AND v_CleansedName = v_MiddleFirstLastName, LTRIM(RTRIM(Name_lkp)),
	    PartyAssociationType = 'PrimaryEntity' AND BusinessOrIndividual = '1' AND v_CleansedName = v_MiddleLastFirstName, LTRIM(RTRIM(Name_lkp)),
	    PartyAssociationType = 'PrimaryEntity' AND BusinessOrIndividual = '1' AND v_CleansedName = v_MiddleFirstName, LTRIM(RTRIM(Name_lkp)),
	    PartyAssociationType = 'PrimaryEntity' AND BusinessOrIndividual = '1' AND v_CleansedName = v_FirstMiddleName, LTRIM(RTRIM(Name_lkp)),
	    PartyAssociationType = 'PrimaryEntity' AND BusinessOrIndividual = '1' AND v_CleansedName = v_LastMiddleName, LTRIM(RTRIM(Name_lkp)),
	    PartyAssociationType = 'PrimaryEntity' AND BusinessOrIndividual = '1' AND v_CleansedName = v_MiddleLastName, LTRIM(RTRIM(Name_lkp)),
	    LTRIM(RTRIM(Name))
	) AS v_ComparedName,
	v_ComparedName AS o_ComparedName,
	-- *INF*: IIF(PartyAssociationType='PrimaryEntity' AND BusinessOrIndividual='1' AND NOT ISNULL(Name_lkp), LTRIM(RTRIM(Name_lkp)),LTRIM(RTRIM(Name)))
	IFF(
	    PartyAssociationType = 'PrimaryEntity' AND BusinessOrIndividual = '1' AND Name_lkp IS NOT NULL,
	    LTRIM(RTRIM(Name_lkp)),
	    LTRIM(RTRIM(Name))
	) AS v_Name_Indicator,
	v_Name_Indicator AS o_Name_lkp,
	EXP_SRC.TransactionEffectiveDate,
	EXP_SRC.TransactionExpirationDate,
	EXP_SRC.NAICSCode,
	-- *INF*: IIF(Name=v_Name and WC_LocationId=v_WC_LocationId ,'Y','N')
	IFF(Name = v_Name and WC_LocationId = v_WC_LocationId, 'Y', 'N') AS v_Location_Delete_Flag,
	EXP_SRC.Address1,
	EXP_SRC.Address2,
	EXP_SRC.City,
	EXP_SRC.StateProv,
	EXP_SRC.PostalCode,
	EXP_SRC.Country,
	EXP_SRC.LocationType,
	EXP_SRC.LocationNumber,
	EXP_SRC.LocationOrder,
	EXP_SRC.PolicyKey,
	EXP_SRC.WC_LocationId,
	EXP_SRC.BusinessOrIndividual,
	EXP_SRC.PartyAssociationType,
	-- *INF*: IIF(ISNULL(WC_LocationId),0,
	-- IIF(WCTrackHistoryID=v_WCTrackHistoryID and WC_LocationId=v_WC_LocationId,v_ExposureRecordLinkForLocationCode,v_ExposureRecordLinkForLocationCode+1))
	IFF(
	    WC_LocationId IS NULL, 0,
	    IFF(
	        WCTrackHistoryID = v_WCTrackHistoryID
	    and WC_LocationId = v_WC_LocationId,
	        v_ExposureRecordLinkForLocationCode,
	        v_ExposureRecordLinkForLocationCode + 1
	    )
	) AS v_ExposureRecordLinkForLocationCode,
	WCTrackHistoryID AS v_WCTrackHistoryID,
	Name AS v_Name,
	WC_LocationId AS v_WC_LocationId,
	EXP_SRC.o_AddressCompare AS AddressCompare,
	EXP_SRC.o_PostalCode_Mini AS PostalCode_Mini,
	EXP_SRC.LocationDeletedIndicator
	FROM EXP_SRC
	LEFT JOIN LKP_Party_Location
	ON LKP_Party_Location.WCTrackHistoryID = EXP_SRC.WCTrackHistoryID
),
SRT_ReOrder AS (
	SELECT
	WCTrackHistoryID, 
	o_Name AS Name, 
	o_ComparedName AS ComparedName, 
	o_Name_lkp, 
	TransactionEffectiveDate, 
	TransactionExpirationDate, 
	NAICSCode, 
	LocationOrder, 
	LocationType, 
	Address1, 
	Address2, 
	City, 
	StateProv, 
	PostalCode_Mini, 
	PostalCode, 
	Country, 
	LocationNumber, 
	PolicyKey, 
	WC_LocationId, 
	PartyAssociationType, 
	AddressCompare, 
	LocationDeletedIndicator
	FROM EXP_SrcDataCollect_Location
	ORDER BY WCTrackHistoryID ASC, ComparedName ASC, LocationOrder ASC, LocationType ASC, Address1 ASC, Address2 ASC, City ASC, StateProv ASC, PostalCode_Mini ASC, Country ASC, WC_LocationId ASC
),
EXP_Compare AS (
	SELECT
	WCTrackHistoryID,
	Name,
	ComparedName,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	NAICSCode,
	-- *INF*: IIF((ComparedName=v_Name and WC_LocationId=v_WC_LocationId AND WCTrackHistoryID=v_WCTrackHistoryID)
	-- --OR (ComparedName=v_Name and LTRIM(RTRIM(AddressCompare))=LTRIM(RTRIM(v_AddressCompare)))
	--  ,'Y','N')
	IFF(
	    (ComparedName = v_Name
	    and WC_LocationId = v_WC_LocationId
	    and WCTrackHistoryID = v_WCTrackHistoryID),
	    'Y',
	    'N'
	) AS v_Location_Delete_Flag,
	Address1,
	Address2,
	City,
	StateProv,
	PostalCode,
	Country,
	LocationType,
	LocationNumber,
	LocationOrder,
	PolicyKey,
	v_Name AS v_Name1,
	v_WCTrackHistoryID AS v_WCTrackHistoryID1,
	v_WC_LocationId AS v_WC_LocationId1,
	v_AddressCompare AS v_AddressCompare1,
	WC_LocationId,
	v_Location_Delete_Flag AS o_Location_Delete_Flag,
	-- *INF*: IIF(ISNULL(WC_LocationId),0,
	-- IIF(WCTrackHistoryID=v_WCTrackHistoryID and WC_LocationId=v_WC_LocationId,v_ExposureRecordLinkForLocationCode,v_ExposureRecordLinkForLocationCode+1))
	IFF(
	    WC_LocationId IS NULL, 0,
	    IFF(
	        WCTrackHistoryID = v_WCTrackHistoryID
	    and WC_LocationId = v_WC_LocationId,
	        v_ExposureRecordLinkForLocationCode,
	        v_ExposureRecordLinkForLocationCode + 1
	    )
	) AS v_ExposureRecordLinkForLocationCode,
	WCTrackHistoryID AS v_WCTrackHistoryID,
	ComparedName AS v_Name,
	WC_LocationId AS v_WC_LocationId,
	-- *INF*: to_char(v_ExposureRecordLinkForLocationCode)
	to_char(v_ExposureRecordLinkForLocationCode) AS o_ExposureRecordLinkForLocationCode,
	PartyAssociationType,
	o_Name_lkp AS Name_lkp,
	AddressCompare,
	AddressCompare AS v_AddressCompare,
	PostalCode_Mini,
	LocationDeletedIndicator
	FROM SRT_ReOrder
),
FIL_Eliminate_Duplicate_Location AS (
	SELECT
	WCTrackHistoryID, 
	Name, 
	TransactionEffectiveDate, 
	TransactionExpirationDate, 
	NAICSCode, 
	Address1, 
	Address2, 
	City, 
	StateProv, 
	PostalCode, 
	Country, 
	LocationType, 
	LocationNumber, 
	LocationOrder, 
	PolicyKey, 
	WC_LocationId, 
	o_Location_Delete_Flag AS Location_Delete_Flag, 
	o_ExposureRecordLinkForLocationCode AS ExposureRecordLinkForLocationCode, 
	PartyAssociationType, 
	Name_lkp, 
	AddressCompare, 
	LocationDeletedIndicator
	FROM EXP_Compare
	WHERE Location_Delete_Flag='N'
),
JNR_Name_Location AS (SELECT
	EXP_Set_NameLinkIdentifier.WCTrackHistoryID, 
	EXP_Set_NameLinkIdentifier.Name, 
	EXP_Set_NameLinkIdentifier.o_NameLinkIdentifier AS NameLinkIdentifier, 
	EXP_Set_NameLinkIdentifier.PhoneNumber, 
	EXP_Set_NameLinkIdentifier.DeletedName_Flag AS DeletedName_Indicator, 
	FIL_Eliminate_Duplicate_Location.WCTrackHistoryID AS WCTrackHistoryID1, 
	FIL_Eliminate_Duplicate_Location.Name AS Name1, 
	FIL_Eliminate_Duplicate_Location.TransactionEffectiveDate, 
	FIL_Eliminate_Duplicate_Location.TransactionExpirationDate, 
	FIL_Eliminate_Duplicate_Location.NAICSCode, 
	FIL_Eliminate_Duplicate_Location.Address1, 
	FIL_Eliminate_Duplicate_Location.Address2, 
	FIL_Eliminate_Duplicate_Location.City, 
	FIL_Eliminate_Duplicate_Location.StateProv, 
	FIL_Eliminate_Duplicate_Location.PostalCode, 
	FIL_Eliminate_Duplicate_Location.Country, 
	FIL_Eliminate_Duplicate_Location.LocationType, 
	FIL_Eliminate_Duplicate_Location.LocationNumber, 
	FIL_Eliminate_Duplicate_Location.LocationOrder, 
	FIL_Eliminate_Duplicate_Location.PolicyKey, 
	FIL_Eliminate_Duplicate_Location.WC_LocationId, 
	FIL_Eliminate_Duplicate_Location.ExposureRecordLinkForLocationCode, 
	FIL_Eliminate_Duplicate_Location.PartyAssociationType, 
	FIL_Eliminate_Duplicate_Location.Name_lkp AS o_Name_lkp, 
	FIL_Eliminate_Duplicate_Location.AddressCompare, 
	FIL_Eliminate_Duplicate_Location.LocationDeletedIndicator, 
	EXP_Set_NameLinkIdentifier.Email
	FROM EXP_Set_NameLinkIdentifier
	RIGHT OUTER JOIN FIL_Eliminate_Duplicate_Location
	ON FIL_Eliminate_Duplicate_Location.WCTrackHistoryID = EXP_Set_NameLinkIdentifier.WCTrackHistoryID AND FIL_Eliminate_Duplicate_Location.Name = EXP_Set_NameLinkIdentifier.Name
),
RTR_Location AS (
	SELECT
	WCTrackHistoryID,
	Name,
	NameLinkIdentifier,
	PhoneNumber,
	DeletedName_Indicator,
	WCTrackHistoryID1 AS WCTrackHistoryID_Location,
	Name1 AS Name_Location,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	NAICSCode,
	Address1,
	Address2,
	City,
	StateProv,
	PostalCode,
	Country,
	LocationType,
	LocationOrder,
	WC_LocationId,
	ExposureRecordLinkForLocationCode,
	PartyAssociationType,
	o_Name_lkp,
	AddressCompare,
	LocationDeletedIndicator,
	Email
	FROM JNR_Name_Location
),
RTR_Location_NULLNameLinkIdentifier AS (SELECT * FROM RTR_Location WHERE ISNULL(NameLinkIdentifier)),
RTR_Location_NOTNULLNameLinkIdentifier AS (SELECT * FROM RTR_Location WHERE NOT ISNULL(NameLinkIdentifier)),
EXPTRANS AS (
	SELECT
	NameLinkIdentifier,
	PhoneNumber,
	WCTrackHistoryID_Location AS WCTrackHistoryID1,
	Name_Location AS Name1,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	NAICSCode,
	Address1,
	Address2,
	City,
	StateProv,
	PostalCode,
	Country,
	LocationType,
	WC_LocationId,
	ExposureRecordLinkForLocationCode,
	PartyAssociationType,
	o_Name_lkp AS Name_lkp,
	AddressCompare,
	LocationOrder,
	LocationDeletedIndicator,
	DeletedName_Indicator,
	Email
	FROM RTR_Location_NOTNULLNameLinkIdentifier
),
SRT_Account AS (
	SELECT
	WCTrackHistoryID, 
	Name, 
	o_NameLinkIdentifier AS NameLinkIdentifier, 
	PhoneNumber, 
	Email
	FROM EXP_Set_NameLinkIdentifier
	ORDER BY WCTrackHistoryID ASC, Name ASC
),
SRT_Location AS (
	SELECT
	WCTrackHistoryID_Location AS WCTrackHistoryID_Location1, 
	Name_Location AS Name_Location1, 
	TransactionEffectiveDate AS TransactionEffectiveDate1, 
	TransactionExpirationDate AS TransactionExpirationDate1, 
	NAICSCode AS NAICSCode1, 
	Address AS Address11, 
	Address2 AS Address21, 
	City AS City1, 
	StateProv AS StateProv1, 
	PostalCode AS PostalCode1, 
	Country AS Country1, 
	LocationType AS LocationType1, 
	WC_LocationId AS WC_LocationId1, 
	ExposureRecordLinkForLocationCode AS ExposureRecordLinkForLocationCode1, 
	PartyAssociationType AS PartyAssociationType1, 
	o_Name_lkp AS o_Name_lkp1, 
	AddressCompare AS AddressCompare1, 
	LocationOrder AS LocationOrder1, 
	LocationDeletedIndicator AS LocationDeletedIndicator1, 
	DeletedName_Indicator AS DeletedName_Indicator1
	FROM RTR_Location_NULLNameLinkIdentifier
	ORDER BY WCTrackHistoryID_Location1 ASC, o_Name_lkp1 ASC
),
JNRTRANS AS (SELECT
	SRT_Account.WCTrackHistoryID, 
	SRT_Account.Name, 
	SRT_Account.NameLinkIdentifier, 
	SRT_Account.PhoneNumber, 
	SRT_Location.WCTrackHistoryID_Location1 AS WCTrackHistoryID_Location, 
	SRT_Location.Name_Location1 AS Name_Location, 
	SRT_Location.TransactionEffectiveDate1 AS TransactionEffectiveDate, 
	SRT_Location.TransactionExpirationDate1 AS TransactionExpirationDate, 
	SRT_Location.NAICSCode1 AS NAICSCode, 
	SRT_Location.Address11 AS Address1, 
	SRT_Location.Address21 AS Address2, 
	SRT_Location.City1 AS City, 
	SRT_Location.StateProv1 AS StateProv, 
	SRT_Location.PostalCode1 AS PostalCode, 
	SRT_Location.Country1 AS Country, 
	SRT_Location.LocationType1 AS LocationType, 
	SRT_Location.WC_LocationId1 AS WC_LocationId, 
	SRT_Location.ExposureRecordLinkForLocationCode1 AS ExposureRecordLinkForLocationCode, 
	SRT_Location.PartyAssociationType1 AS PartyAssociationType, 
	SRT_Location.o_Name_lkp1 AS Name_lkp, 
	SRT_Location.AddressCompare1 AS AddressCompare, 
	SRT_Location.LocationOrder1, 
	SRT_Location.LocationDeletedIndicator1 AS LocationDeletedIndicator, 
	SRT_Location.DeletedName_Indicator1, 
	SRT_Account.Email
	FROM SRT_Account
	RIGHT OUTER JOIN SRT_Location
	ON SRT_Location.WCTrackHistoryID_Location1 = SRT_Account.WCTrackHistoryID AND SRT_Location.o_Name_lkp1 = SRT_Account.Name
),
Union AS (
	SELECT WCTrackHistoryID_Location, PhoneNumber, TransactionEffectiveDate, TransactionExpirationDate, NAICSCode, Address1, Address2, City, StateProv, PostalCode, Country, LocationType, WC_LocationId, ExposureRecordLinkForLocationCode, NameLinkIdentifier, Name_lkp AS Name, AddressCompare, LocationOrder1 AS LocationOrder, LocationDeletedIndicator, DeletedName_Indicator1 AS DeletedName_Indicator, Email
	FROM JNRTRANS
	UNION
	SELECT WCTrackHistoryID1 AS WCTrackHistoryID_Location, PhoneNumber, TransactionEffectiveDate, TransactionExpirationDate, NAICSCode, Address1, Address2, City, StateProv, PostalCode, Country, LocationType, WC_LocationId, ExposureRecordLinkForLocationCode, NameLinkIdentifier, Name1 AS Name, AddressCompare, LocationOrder, LocationDeletedIndicator, DeletedName_Indicator, Email
	FROM EXPTRANS
),
EXP_Location AS (
	SELECT
	WCTrackHistoryID_Location AS WCTrackHistoryID1,
	PhoneNumber,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	NAICSCode,
	Address1,
	Address2,
	City,
	StateProv,
	PostalCode,
	Country,
	LocationType,
	NameLinkIdentifier,
	-- *INF*: IIF(LocationType='Agency','999',NameLinkIdentifier)
	IFF(LocationType = 'Agency', '999', NameLinkIdentifier) AS v_NameLinkIdentifier,
	v_NameLinkIdentifier AS o_NameLinkIdentifier,
	WC_LocationId,
	ExposureRecordLinkForLocationCode,
	Name,
	AddressCompare,
	-- *INF*: Decode(TRUE,
	-- LocationType = 'Account' and v_NameLinkIdentifier='001','1',
	-- LocationType = 'Agency','5',
	-- LocationType = 'WBMI','3',
	-- INSTR(Address1,'VARIOUS')>0 
	-- OR INSTR(Address1,'ADDRESS')>0 
	-- OR INSTR(Address1,'LOCATION')>0 
	-- OR INSTR(Address1,'USED')>0 
	-- OR INSTR(Address1,'SPECIFIED')>0,'6',
	-- '2')
	Decode(
	    TRUE,
	    LocationType = 'Account' and v_NameLinkIdentifier = '001', '1',
	    LocationType = 'Agency', '5',
	    LocationType = 'WBMI', '3',
	    REGEXP_INSTR(Address1, 'VARIOUS') > 0 OR REGEXP_INSTR(Address1, 'ADDRESS') > 0 OR REGEXP_INSTR(Address1, 'LOCATION') > 0 OR REGEXP_INSTR(Address1, 'USED') > 0 OR REGEXP_INSTR(Address1, 'SPECIFIED') > 0, '6',
	    '2'
	) AS o_AddressTypeCode,
	LocationOrder,
	LocationDeletedIndicator,
	DeletedName_Indicator,
	-- *INF*: DECODE(TRUE,
	-- DeletedName_Indicator='1','1',
	-- LocationDeletedIndicator='1','1',
	-- '0')
	DECODE(
	    TRUE,
	    DeletedName_Indicator = '1', '1',
	    LocationDeletedIndicator = '1', '1',
	    '0'
	) AS Name_Location_Deleted_Indicator,
	Email
	FROM Union
),
LKP_PrimaryLocation AS (
	SELECT
	Address1,
	Address2,
	City,
	StateProv,
	PostalCode,
	Country,
	WCTrackHistoryID
	FROM (
		select WCTrackHistoryID As WCTrackHistoryID,
		Address1 As Address1,
		Address2 As Address2,
		City As City,
		StateProv As StateProv,
		PostalCode As PostalCode,
		Country As Country 
		from 
		(select WCTrackHistoryID,Address1,Address2,City,StateProv,PostalCode,Country,LocationNumber,min(LocationNumber) over(Partition by WCTrackHistoryID) Min_LocationNumber
		 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCLocation
		where LocationDeletedIndicator=0
		and LocationType='Location') A
		where A.LocationNumber=A.Min_LocationNumber
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WCTrackHistoryID ORDER BY Address1) = 1
),
EXP_LocationPrep AS (
	SELECT
	EXP_Location.WCTrackHistoryID1,
	EXP_Location.PhoneNumber,
	EXP_Location.TransactionEffectiveDate,
	EXP_Location.TransactionExpirationDate,
	EXP_Location.NAICSCode,
	EXP_Location.Address1,
	EXP_Location.Address2,
	EXP_Location.City,
	EXP_Location.StateProv,
	EXP_Location.PostalCode AS IN_PostalCode,
	-- *INF*: REPLACECHR(1,IN_PostalCode,'-','')
	REGEXP_REPLACE(IN_PostalCode,'-','') AS v_PostalCode,
	-- *INF*: SUBSTR(LTRIM(RTRIM(IN_PostalCode)),1,5)
	SUBSTR(LTRIM(RTRIM(IN_PostalCode)), 1, 5) AS v_PostalCode_Mini,
	v_PostalCode AS PostalCode,
	v_PostalCode_Mini AS PostalCode_Mini,
	EXP_Location.Country,
	EXP_Location.LocationType,
	EXP_Location.o_NameLinkIdentifier AS NameLinkIdentifier,
	LKP_PrimaryLocation.Address1 AS lkp_Address1,
	LKP_PrimaryLocation.Address2 AS lkp_Address2,
	LKP_PrimaryLocation.City AS lkp_City,
	LKP_PrimaryLocation.StateProv AS lkp_StateProv,
	LKP_PrimaryLocation.PostalCode AS lkp_PostalCode,
	-- *INF*: REPLACECHR(1,lkp_PostalCode,'-','')
	REGEXP_REPLACE(lkp_PostalCode,'-','') AS v_lkp_PostalCode,
	-- *INF*: SUBSTR(LTRIM(RTRIM(v_lkp_PostalCode)),1,5)
	SUBSTR(LTRIM(RTRIM(v_lkp_PostalCode)), 1, 5) AS v_lkp_PostalCode_Mini,
	LKP_PrimaryLocation.Country AS lkp_Country,
	-- *INF*: Decode(TRUE,
	-- INSTR(Address1,'VARIOUS')>0 
	-- OR INSTR(Address1,'ADDRESS')>0 
	-- OR INSTR(Address1,'LOCATION')>0 
	-- OR INSTR(Address1,'USED')>0 
	-- OR INSTR(Address1,'SPECIFIED')>0,
	-- lkp_Address1,Address1)
	Decode(
	    TRUE,
	    REGEXP_INSTR(Address1, 'VARIOUS') > 0 OR REGEXP_INSTR(Address1, 'ADDRESS') > 0 OR REGEXP_INSTR(Address1, 'LOCATION') > 0 OR REGEXP_INSTR(Address1, 'USED') > 0 OR REGEXP_INSTR(Address1, 'SPECIFIED') > 0, lkp_Address1,
	    Address1
	) AS v_Address1,
	-- *INF*: Decode(TRUE,
	-- INSTR(Address1,'VARIOUS')>0 
	-- OR INSTR(Address1,'ADDRESS')>0 
	-- OR INSTR(Address1,'LOCATION')>0 
	-- OR INSTR(Address1,'USED')>0 
	-- OR INSTR(Address1,'SPECIFIED')>0,
	-- lkp_Address2,Address2)
	Decode(
	    TRUE,
	    REGEXP_INSTR(Address1, 'VARIOUS') > 0 OR REGEXP_INSTR(Address1, 'ADDRESS') > 0 OR REGEXP_INSTR(Address1, 'LOCATION') > 0 OR REGEXP_INSTR(Address1, 'USED') > 0 OR REGEXP_INSTR(Address1, 'SPECIFIED') > 0, lkp_Address2,
	    Address2
	) AS v_Address2,
	-- *INF*: Decode(TRUE,
	-- INSTR(Address1,'VARIOUS')>0 
	-- OR INSTR(Address1,'ADDRESS')>0 
	-- OR INSTR(Address1,'LOCATION')>0 
	-- OR INSTR(Address1,'USED')>0 
	-- OR INSTR(Address1,'SPECIFIED')>0,
	-- lkp_City,City)
	Decode(
	    TRUE,
	    REGEXP_INSTR(Address1, 'VARIOUS') > 0 OR REGEXP_INSTR(Address1, 'ADDRESS') > 0 OR REGEXP_INSTR(Address1, 'LOCATION') > 0 OR REGEXP_INSTR(Address1, 'USED') > 0 OR REGEXP_INSTR(Address1, 'SPECIFIED') > 0, lkp_City,
	    City
	) AS v_City,
	-- *INF*: Decode(TRUE,
	-- INSTR(Address1,'VARIOUS')>0 
	-- OR INSTR(Address1,'ADDRESS')>0 
	-- OR INSTR(Address1,'LOCATION')>0 
	-- OR INSTR(Address1,'USED')>0 
	-- OR INSTR(Address1,'SPECIFIED')>0,
	-- lkp_StateProv,StateProv)
	Decode(
	    TRUE,
	    REGEXP_INSTR(Address1, 'VARIOUS') > 0 OR REGEXP_INSTR(Address1, 'ADDRESS') > 0 OR REGEXP_INSTR(Address1, 'LOCATION') > 0 OR REGEXP_INSTR(Address1, 'USED') > 0 OR REGEXP_INSTR(Address1, 'SPECIFIED') > 0, lkp_StateProv,
	    StateProv
	) AS v_StateProv,
	-- *INF*: Decode(TRUE,
	-- INSTR(Address1,'VARIOUS')>0 
	-- OR INSTR(Address1,'ADDRESS')>0 
	-- OR INSTR(Address1,'LOCATION')>0 
	-- OR INSTR(Address1,'USED')>0 
	-- OR INSTR(Address1,'SPECIFIED')>0,
	-- v_lkp_PostalCode_Mini,v_PostalCode_Mini)
	Decode(
	    TRUE,
	    REGEXP_INSTR(Address1, 'VARIOUS') > 0 OR REGEXP_INSTR(Address1, 'ADDRESS') > 0 OR REGEXP_INSTR(Address1, 'LOCATION') > 0 OR REGEXP_INSTR(Address1, 'USED') > 0 OR REGEXP_INSTR(Address1, 'SPECIFIED') > 0, v_lkp_PostalCode_Mini,
	    v_PostalCode_Mini
	) AS v_PostalCode_Mini_for_AddressComp,
	v_Address1 AS o_Address1,
	v_Address2 AS o_Address2,
	v_City AS o_City,
	v_StateProv AS o_StateProv,
	-- *INF*: Decode(TRUE,
	-- INSTR(Address1,'VARIOUS')>0 
	-- OR INSTR(Address1,'ADDRESS')>0 
	-- OR INSTR(Address1,'LOCATION')>0 
	-- OR INSTR(Address1,'USED')>0 
	-- OR INSTR(Address1,'SPECIFIED')>0,
	-- v_lkp_PostalCode,v_PostalCode)
	Decode(
	    TRUE,
	    REGEXP_INSTR(Address1, 'VARIOUS') > 0 OR REGEXP_INSTR(Address1, 'ADDRESS') > 0 OR REGEXP_INSTR(Address1, 'LOCATION') > 0 OR REGEXP_INSTR(Address1, 'USED') > 0 OR REGEXP_INSTR(Address1, 'SPECIFIED') > 0, v_lkp_PostalCode,
	    v_PostalCode
	) AS o_PostalCode,
	-- *INF*: Decode(TRUE,
	-- INSTR(Address1,'VARIOUS')>0 
	-- OR INSTR(Address1,'ADDRESS')>0 
	-- OR INSTR(Address1,'LOCATION')>0 
	-- OR INSTR(Address1,'USED')>0 
	-- OR INSTR(Address1,'SPECIFIED')>0,
	-- lkp_Country,Country)
	Decode(
	    TRUE,
	    REGEXP_INSTR(Address1, 'VARIOUS') > 0 OR REGEXP_INSTR(Address1, 'ADDRESS') > 0 OR REGEXP_INSTR(Address1, 'LOCATION') > 0 OR REGEXP_INSTR(Address1, 'USED') > 0 OR REGEXP_INSTR(Address1, 'SPECIFIED') > 0, lkp_Country,
	    Country
	) AS o_Country,
	-- *INF*: Decode(TRUE,
	-- NameLinkIdentifier='002' and LocationType='Account',lkp_StateProv,
	-- StateProv)
	Decode(
	    TRUE,
	    NameLinkIdentifier = '002' and LocationType = 'Account', lkp_StateProv,
	    StateProv
	) AS o_stateLinkCodeState,
	EXP_Location.WC_LocationId,
	EXP_Location.ExposureRecordLinkForLocationCode,
	EXP_Location.AddressCompare AS IN_AddressCompare,
	EXP_Location.o_AddressTypeCode AS AddressTypeCode,
	-- *INF*: 
	-- 
	-- LTRIM(RTRIM(NameLinkIdentifier)) || IN_AddressCompare 
	-- 
	-- --LTRIM(RTRIM(NameLinkIdentifier)) || IN_AddressCompare  || AddressTypeCode
	-- 
	-- --LTRIM(RTRIM(NameLinkIdentifier))||LTRIM(RTRIM(WCTrackHistoryID1||LTRIM(RTRIM(v_Address1))||LTRIM(RTRIM(v_Address2))||LTRIM(RTRIM(v_City))||LTRIM(RTRIM(v_StateProv))||LTRIM(RTRIM(v_PostalCode_Mini_for_AddressComp))))
	LTRIM(RTRIM(NameLinkIdentifier)) || IN_AddressCompare AS o_AddressCompare,
	EXP_Location.LocationOrder,
	EXP_Location.LocationDeletedIndicator,
	EXP_Location.Name_Location_Deleted_Indicator,
	EXP_Location.Email
	FROM EXP_Location
	LEFT JOIN LKP_PrimaryLocation
	ON LKP_PrimaryLocation.WCTrackHistoryID = EXP_Location.WCTrackHistoryID1
),
SRTTRANS AS (
	SELECT
	WCTrackHistoryID1, 
	NameLinkIdentifier, 
	PhoneNumber, 
	TransactionEffectiveDate, 
	TransactionExpirationDate, 
	NAICSCode, 
	Address1, 
	Address2, 
	City, 
	StateProv, 
	PostalCode_Mini, 
	PostalCode, 
	Country, 
	LocationOrder, 
	AddressTypeCode, 
	LocationType, 
	o_Address1, 
	o_Address2, 
	o_City, 
	o_StateProv, 
	o_PostalCode, 
	o_Country, 
	o_stateLinkCodeState, 
	WC_LocationId, 
	ExposureRecordLinkForLocationCode, 
	o_AddressCompare AS AddressCompare, 
	LocationDeletedIndicator, 
	Name_Location_Deleted_Indicator, 
	Email
	FROM EXP_LocationPrep
	ORDER BY WCTrackHistoryID1 ASC, NameLinkIdentifier ASC, Address1 ASC, Address2 ASC, City ASC, StateProv ASC, PostalCode_Mini ASC, Country ASC, LocationOrder ASC, LocationType ASC, WC_LocationId ASC
),
EXP_Duplicates AS (
	SELECT
	WCTrackHistoryID1,
	NameLinkIdentifier,
	PhoneNumber,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	-- *INF*: IIF(LTRIM(RTRIM(AddressCompare))=LTRIM(RTRIM(v_AddressCompare))
	--  ,'Y','N')
	IFF(LTRIM(RTRIM(AddressCompare)) = LTRIM(RTRIM(v_AddressCompare)), 'Y', 'N') AS v_LocationDeleteFlag,
	NAICSCode,
	Address1,
	v_AddressCompare AS AddressCompare1,
	Address2,
	City,
	StateProv,
	PostalCode,
	Country,
	AddressTypeCode,
	LocationType,
	o_Address1,
	o_Address2,
	o_City,
	o_StateProv,
	o_PostalCode,
	o_Country,
	o_stateLinkCodeState,
	WC_LocationId,
	ExposureRecordLinkForLocationCode,
	'RemoveDupesFilter2Log' AS LogName,
	'LocationDeleteFlag, ' || v_LocationDeleteFlag||' , AddressTypeCode, '||AddressTypeCode || ', WC_LocationId, ' ||WC_LocationId || ', RuleStateProv,'||StateProv||', OutputStateProv ,' || o_StateProv ||', LocationType,'||LocationType||',LinkStateCode ,'|| o_stateLinkCodeState|| ', AddressLine1, '  || Address1||', AddressCompare, ' ||AddressCompare AS LogRecord,
	AddressCompare,
	AddressCompare AS v_AddressCompare,
	v_LocationDeleteFlag AS o_LocationDeleteFlag,
	LocationOrder,
	LocationDeletedIndicator,
	Name_Location_Deleted_Indicator,
	Email
	FROM SRTTRANS
),
FIL_RemoveDuplicates AS (
	SELECT
	WCTrackHistoryID1, 
	NameLinkIdentifier, 
	PhoneNumber, 
	TransactionEffectiveDate, 
	TransactionExpirationDate, 
	NAICSCode, 
	Address1, 
	Address2, 
	City, 
	StateProv, 
	PostalCode, 
	Country, 
	AddressTypeCode, 
	LocationType, 
	o_Address1, 
	o_Address2, 
	o_City, 
	o_StateProv, 
	o_PostalCode, 
	o_Country, 
	o_stateLinkCodeState, 
	WC_LocationId, 
	ExposureRecordLinkForLocationCode, 
	AddressCompare, 
	o_LocationDeleteFlag AS LocationDeleteFlag, 
	LocationDeletedIndicator, 
	Name_Location_Deleted_Indicator, 
	Email
	FROM EXP_Duplicates
	WHERE LocationDeleteFlag='N'
),
JNR_03_Record AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	FIL_RemoveDuplicates.WCTrackHistoryID1, 
	FIL_RemoveDuplicates.PhoneNumber, 
	FIL_RemoveDuplicates.TransactionEffectiveDate, 
	FIL_RemoveDuplicates.TransactionExpirationDate, 
	FIL_RemoveDuplicates.NAICSCode, 
	FIL_RemoveDuplicates.o_Address1 AS Address1, 
	FIL_RemoveDuplicates.o_Address2 AS Address2, 
	FIL_RemoveDuplicates.o_City AS City, 
	FIL_RemoveDuplicates.o_StateProv AS StateProv, 
	FIL_RemoveDuplicates.o_PostalCode AS PostalCode, 
	FIL_RemoveDuplicates.o_Country AS Country, 
	FIL_RemoveDuplicates.LocationType, 
	FIL_RemoveDuplicates.NameLinkIdentifier, 
	FIL_RemoveDuplicates.o_stateLinkCodeState AS stateLinkCodeState, 
	FIL_RemoveDuplicates.Address1 AS OriginalAddress1, 
	FIL_RemoveDuplicates.Address2 AS OriginalAddress2, 
	FIL_RemoveDuplicates.City AS OriginalCity, 
	FIL_RemoveDuplicates.StateProv AS OriginalStateProv, 
	FIL_RemoveDuplicates.PostalCode AS OriginalPostalCode, 
	FIL_RemoveDuplicates.Country AS OriginalCountry, 
	FIL_RemoveDuplicates.WC_LocationId, 
	FIL_RemoveDuplicates.ExposureRecordLinkForLocationCode, 
	FIL_RemoveDuplicates.AddressCompare, 
	FIL_RemoveDuplicates.LocationDeleteFlag, 
	FIL_RemoveDuplicates.LocationDeletedIndicator, 
	FIL_RemoveDuplicates.Name_Location_Deleted_Indicator, 
	FIL_RemoveDuplicates.Email
	FROM FIL_RemoveDuplicates
	INNER JOIN SQ_WCPols00Record
	ON SQ_WCPols00Record.WCTrackHistoryID = FIL_RemoveDuplicates.WCTrackHistoryID1
),
UN_PolicyLocation_CarrierLocation AS (
	SELECT WCTrackHistoryID, LinkData, AuditId, WCTrackHistoryID1 AS WCTrackHistoryID11, PhoneNumber, TransactionEffectiveDate, TransactionExpirationDate, NAICSCode, Address1, Address2, City, StateProv, PostalCode, Country, LocationType, NameLinkIdentifier, stateLinkCodeState, OriginalAddress1, OriginalAddress2, OriginalCity, OriginalStateProv, OriginalPostalCode, OriginalCountry, WC_LocationId, ExposureRecordLinkForLocationCode, AddressCompare, LocationDeletedIndicator, Name_Location_Deleted_Indicator, Email
	FROM JNR_03_Record
	UNION
	SELECT WCTrackHistoryID, LinkData, AuditId, WCTrackHistoryID11, PhoneNumber, TransactionEffectiveDate, TransactionExpirationDate, NAICSCode, LocationType, NameLinkIdentifier, Address1 AS OriginalAddress1, Address2 AS OriginalAddress2, City AS OriginalCity, StateProv AS OriginalStateProv, PostalCode AS OriginalPostalCode, Country AS OriginalCountry, Email
	FROM EXP_WBMI_Format
),
RTR_StateSplit AS (
	SELECT
	WCTrackHistoryID,
	LinkData,
	PhoneNumber,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	NAICSCode,
	Address1,
	Address2,
	City,
	StateProv,
	PostalCode,
	Country,
	LocationType,
	NameLinkIdentifier,
	stateLinkCodeState,
	OriginalAddress1,
	OriginalAddress2,
	OriginalCity,
	OriginalStateProv,
	OriginalPostalCode,
	OriginalCountry,
	WC_LocationId,
	ExposureRecordLinkForLocationCode,
	LocationDeletedIndicator,
	Name_Location_Deleted_Indicator,
	Email
	FROM UN_PolicyLocation_CarrierLocation
),
RTR_StateSplit_IN_IA_MO AS (SELECT * FROM RTR_StateSplit WHERE IN(OriginalStateProv,'AZ','IA','IN','KY','MO','MT','TX') and LocationType='Location'),
RTR_StateSplit_DEFAULT1 AS (SELECT * FROM RTR_StateSplit WHERE NOT ( (IN(OriginalStateProv,'AZ','IA','IN','KY','MO','MT','TX') and LocationType='Location') )),
EXP_Format_Output_IN_IA_MO AS (
	SELECT
	CURRENT_TIMESTAMP AS o_ExtractDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WCTrackHistoryID,
	LinkData,
	'03' AS o_RecordTypeCode,
	LocationType,
	-- *INF*: Decode(TRUE,
	-- LocationType = 'Account' and NameLinkIdentifier='001','1',
	-- LocationType = 'Agency','5',
	-- LocationType = 'WBMI','3',
	-- '2')
	Decode(
	    TRUE,
	    LocationType = 'Account' and NameLinkIdentifier = '001', '1',
	    LocationType = 'Agency', '5',
	    LocationType = 'WBMI', '3',
	    '2'
	) AS v_AddressTypeCode,
	v_AddressTypeCode AS o_AddressTypeCode,
	Country,
	-- *INF*: IIF (Country = 'US' or Country = 'USA' or Country = 'UnitedStatesOfAmerica' or Country = 'UNITEDSTATESOFAMERICA' or IsNull(Country) or rtrim(ltrim(Country))='','N','Y')
	IFF(
	    Country = 'US'
	    or Country = 'USA'
	    or Country = 'UnitedStatesOfAmerica'
	    or Country = 'UNITEDSTATESOFAMERICA'
	    or Country IS NULL
	    or rtrim(ltrim(Country)) = '',
	    'N',
	    'Y'
	) AS v_ForeignAddressIndicator,
	v_ForeignAddressIndicator AS o_ForeignAddressIndicator,
	-- *INF*: DECODE(TRUE,
	-- v_AddressTypeCode='6','0',
	-- '1')
	DECODE(
	    TRUE,
	    v_AddressTypeCode = '6', '0',
	    '1'
	) AS o_AddressStructureCode,
	Address AS Address1,
	Address2,
	-- *INF*: SUBSTR(Address1 || ' ' || Address2, 1, 60)
	SUBSTR(Address1 || ' ' || Address2, 1, 60) AS o_AddressStreet,
	City AS AddressCity,
	StateProv AS AddressState,
	PostalCode,
	-- *INF*: IIF (LENGTH(PostalCode) = 5 or LENGTH(PostalCode) = 9, PostalCode,
	-- IIF (LENGTH(PostalCode) = 10 and SUBSTR(PostalCode,6,1) = '-',CONCAT(SUBSTR(PostalCode,1,5),SUBSTR(PostalCode,7,4)),''))
	IFF(
	    LENGTH(PostalCode) = 5 or LENGTH(PostalCode) = 9, PostalCode,
	    IFF(
	        LENGTH(PostalCode) = 10
	    and SUBSTR(PostalCode, 6, 1) = '-',
	        CONCAT(SUBSTR(PostalCode, 1, 5), SUBSTR(PostalCode, 7, 4)),
	        ''
	    )
	) AS v_PostalCode,
	-- *INF*: IIF (IS_NUMBER(v_PostalCode),v_PostalCode,'')
	IFF(REGEXP_LIKE(v_PostalCode, '^[0-9]+$'), v_PostalCode, '') AS o_PostalCode,
	NameLinkIdentifier,
	-- *INF*: DECODE(TRUE,
	-- IN(LocationType,'WBMI','Agency'),'99',
	--  :LKP.LKP_SupWCPOLS('DCT',stateLinkCodeState,'WCPOLS03Record','StateCodeRecord03')
	-- )
	-- --IIF (LocationType = 'WBMI','99', :LKP.LKP_SupWCPOLS('DCT',stateLinkCodeState,'WCPOLS03Record','StateCodeRecord03'))
	-- 
	-- 
	DECODE(
	    TRUE,
	    LocationType IN ('WBMI','Agency'), '99',
	    LKP_SUPWCPOLS__DCT_stateLinkCodeState_WCPOLS03Record_StateCodeRecord03.WCPOLSCode
	) AS o_StateCodeLink,
	ExposureRecordLinkForLocationCode,
	-- *INF*: IIF(IN(v_AddressTypeCode,'3','4','5'),'99999',ExposureRecordLinkForLocationCode)
	IFF(v_AddressTypeCode IN ('3','4','5'), '99999', ExposureRecordLinkForLocationCode) AS o_ExposureRecordLinkForLocationCode,
	PhoneNumber AS PhoneNumberOfInsured,
	NAICSCode AS IndustryCode,
	-- *INF*: IIF(IN(v_AddressTypeCode,'1','2'),IndustryCode,'')
	IFF(v_AddressTypeCode IN ('1','2'), IndustryCode, '') AS o_IndustryCode,
	-- *INF*: IIF(v_ForeignAddressIndicator='Y',AddressState,'')
	-- --IIF (Country <> 'USA' and Country <> 'UnitedStatesOfAmerica' and Country <> 'UNITEDSTATESOFAMERICA',AddressState,'')
	IFF(v_ForeignAddressIndicator = 'Y', AddressState, '') AS o_GeographicArea,
	Email AS i_EmailAddress,
	-- *INF*: IIF(IN(v_AddressTypeCode, '1', '2', '6'), IIF(ISNULL(i_EmailAddress), '', i_EmailAddress), '')
	IFF(
	    v_AddressTypeCode IN ('1','2','6'),
	    IFF(
	        i_EmailAddress IS NULL, '', i_EmailAddress
	    ),
	    ''
	) AS o_EmailAddress,
	-- *INF*: DECODE(TRUE,
	-- v_ForeignAddressIndicator='Y' AND ISNULL(:LKP.LKP_SupWCPOLS('DCT',Country,'WCPOLS03Record','CountryCode')),'  ',
	-- v_ForeignAddressIndicator='Y',:LKP.LKP_SupWCPOLS('DCT',Country,'WCPOLS03Record','CountryCode'),
	-- ' ')
	DECODE(
	    TRUE,
	    v_ForeignAddressIndicator = 'Y' AND LKP_SUPWCPOLS__DCT_Country_WCPOLS03Record_CountryCode.WCPOLSCode IS NULL, '  ',
	    v_ForeignAddressIndicator = 'Y', LKP_SUPWCPOLS__DCT_Country_WCPOLS03Record_CountryCode.WCPOLSCode,
	    ' '
	) AS o_CountryCode,
	'00' AS NameLinkCounterIdentifier,
	TransactionEffectiveDate AS PolicyChangeEffectiveDate,
	-- *INF*: TO_CHAR(PolicyChangeEffectiveDate,'YYMMDD')
	TO_CHAR(PolicyChangeEffectiveDate, 'YYMMDD') AS o_PolicyChangeEffectiveDate,
	TransactionExpirationDate AS PolicyChangeExpirationDate,
	-- *INF*: DECODE(TRUE,
	-- Name_Location_Deleted_Indicator='1',TO_CHAR(PolicyChangeEffectiveDate,'YYMMDD'),
	-- TO_CHAR(PolicyChangeExpirationDate,'YYMMDD')
	-- )
	-- 
	DECODE(
	    TRUE,
	    Name_Location_Deleted_Indicator = '1', TO_CHAR(PolicyChangeEffectiveDate, 'YYMMDD'),
	    TO_CHAR(PolicyChangeExpirationDate, 'YYMMDD')
	) AS o_PolicyChangeExpirationDate,
	stateLinkCodeState,
	WC_LocationId AS WC_LocationId1,
	LocationDeletedIndicator,
	Name_Location_Deleted_Indicator
	FROM RTR_StateSplit_IN_IA_MO
	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_stateLinkCodeState_WCPOLS03Record_StateCodeRecord03
	ON LKP_SUPWCPOLS__DCT_stateLinkCodeState_WCPOLS03Record_StateCodeRecord03.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_stateLinkCodeState_WCPOLS03Record_StateCodeRecord03.SourceCode = stateLinkCodeState
	AND LKP_SUPWCPOLS__DCT_stateLinkCodeState_WCPOLS03Record_StateCodeRecord03.TableName = 'WCPOLS03Record'
	AND LKP_SUPWCPOLS__DCT_stateLinkCodeState_WCPOLS03Record_StateCodeRecord03.ProcessName = 'StateCodeRecord03'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_Country_WCPOLS03Record_CountryCode
	ON LKP_SUPWCPOLS__DCT_Country_WCPOLS03Record_CountryCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_Country_WCPOLS03Record_CountryCode.SourceCode = Country
	AND LKP_SUPWCPOLS__DCT_Country_WCPOLS03Record_CountryCode.TableName = 'WCPOLS03Record'
	AND LKP_SUPWCPOLS__DCT_Country_WCPOLS03Record_CountryCode.ProcessName = 'CountryCode'

),
WCPols03RecordINIAMO AS (
	INSERT INTO WCPols03RecordINIAMO
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, RecordTypeCode, AddressTypeCode, ForeignAddressIndicator, AddressStructureCode, AddressStreet, AddressCity, AddressState, AddressZipCode, NameLinkIdentifier, StateCodeLink, ExposureRecordLinkForLocationCode, PhoneNumberOfInsured, NumberOfEmployees, IndustryCode, GeographicArea, EmailAddress, CountryCode, NameLinkCounterIdentifier, PolicyChangeEffectiveDate, PolicyChangeExpirationDate)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_AuditId AS AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	o_RecordTypeCode AS RECORDTYPECODE, 
	o_AddressTypeCode AS ADDRESSTYPECODE, 
	o_ForeignAddressIndicator AS FOREIGNADDRESSINDICATOR, 
	o_AddressStructureCode AS ADDRESSSTRUCTURECODE, 
	o_AddressStreet AS ADDRESSSTREET, 
	ADDRESSCITY, 
	ADDRESSSTATE, 
	o_PostalCode AS ADDRESSZIPCODE, 
	NAMELINKIDENTIFIER, 
	o_StateCodeLink AS STATECODELINK, 
	o_ExposureRecordLinkForLocationCode AS EXPOSURERECORDLINKFORLOCATIONCODE, 
	PHONENUMBEROFINSURED, 
	NUMBEROFEMPLOYEES, 
	o_IndustryCode AS INDUSTRYCODE, 
	o_GeographicArea AS GEOGRAPHICAREA, 
	o_EmailAddress AS EMAILADDRESS, 
	o_CountryCode AS COUNTRYCODE, 
	NAMELINKCOUNTERIDENTIFIER, 
	o_PolicyChangeEffectiveDate AS POLICYCHANGEEFFECTIVEDATE, 
	o_PolicyChangeExpirationDate AS POLICYCHANGEEXPIRATIONDATE
	FROM EXP_Format_Output_IN_IA_MO
),
SQ_WorkWCLine AS (
	WITH Limit_CTE
	AS
	(
	SELECT
		MAX(CONVERT(FLOAT, LimitValue)) AS LimitValue
		,WCTrackHistoryID
		,CoverageId
	FROM dbo.WorkWCLimit
	WHERE 1 = 1
		AND LimitType = 'UnitsOfExposureEstimated'
		AND CoverageType = 'ManualPremium'
		AND ISNUMERIC(LimitValue) = 1
		AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	GROUP BY WCTrackHistoryID, CoverageId
	)
	
	
	SELECT
		cov.WCTrackHistoryID
		,loc.StateProv AS RiskState
		,trm.State AS StateTermState
		,det_C.Value AS ClassCode
		,det_S.Value AS StatCode
		,lim.LimitValue
		,cov.BaseRate
		,cov.ParentCoverageType
		,cov.ChildCoverageType
		,ISNULL(cov.Premium,0) Premium
		,cov.PeriodStartDate AS COV_PeriodStartDate
		,cov.PeriodEndDate AS COV_PeriodEndDate
		,trm.PeriodStartDate AS TRM_PeriodStartDate
		,trm.PeriodEndDate AS TRM_PeriodEndDate
		,pol.TransactionEffectiveDate
		,pol.TransactionExpirationDate
	      ,loc.WC_LocationId
	      ,cov.ParentCoverageId
		,WT.MNRequiredFlag
		,MIN(trm.PeriodStartDate) OVER(PARTITION BY cov.WCTrackHistoryID, State) FIRST_TRMPeriodStartDate
		,MAX(trm.PeriodStartDate) OVER(PARTITION BY cov.WCTrackHistoryID, State) LAST_TRMPeriodStartDate
		,MIN(cov.PeriodStartDate) OVER(PARTITION BY cov.WCTrackHistoryID, State) FIRST_COVPeriodStartDate
		,MAX(cov.PeriodStartDate) OVER(PARTITION BY cov.WCTrackHistoryID, State) LAST_CovPeriodStartDate
		,case when ParentCoverageDeleteFlag=1 then '1' else '0' END ParentCoverageDeleteFlag
	
	FROM dbo.WorkWCCoverage cov
	
	INNER JOIN dbo.WorkWCPolicy pol
		ON cov.WCTrackHistoryID = pol.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCTrackHistory WT
		ON WT.WCTrackHistoryID=cov.WCTrackHistoryID
		AND WT.Auditid=Cov.Auditid
	
	LEFT JOIN Limit_CTE lim
		ON cov.WCTrackHistoryID = lim.WCTrackHistoryID
			AND lim.CoverageId = cov.ParentCoverageId
	
	LEFT JOIN dbo.WorkWCCoverageDetails det_S
		ON cov.WCTrackHistoryID = det_S.WCTrackHistoryID
			AND det_S.CoverageId = cov.ParentCoverageId
			AND det_S.CoverageType <> 'ManualPremium'
			AND det_S.Attribute = 'StatCode'
	
	LEFT JOIN dbo.WorkWCCoverageDetails det_C
		ON cov.WCTrackHistoryID = det_C.WCTrackHistoryID
			AND det_C.CoverageId = cov.ParentCoverageId
			AND det_C.CoverageType = 'ManualPremium'
			AND det_C.Attribute = 'ClassCode'
	
	LEFT JOIN dbo.WorkWCStateTerm trm
		ON cov.WCTrackHistoryID = trm.WCTrackHistoryID
			AND trm.WC_StateTermId = cov.ParentObjectId
			AND cov.ParentObjectName = 'DC_WC_StateTerm'
	
	LEFT JOIN dbo.WorkWCRisk rsk
		ON cov.WCTrackHistoryID = rsk.WCTrackHistoryID
			AND rsk.WC_RiskID = cov.ParentObjectId
			AND cov.ParentObjectName = 'DC_WC_Risk'
	
	LEFT JOIN dbo.WorkWCLocation loc
		ON cov.WCTrackHistoryID = loc.WCTrackHistoryID
			AND rsk.WC_LocationId = loc.WC_LocationId
			--AND loc.LocationDeletedIndicator != 1
	
	WHERE 1 = 1
	AND cov.ParentCoverageType NOT IN ('ExperienceModification', 'ExpenseConstant', 'PremiumDiscount', 'EmployersLiability')
	--and cov.ParentCoverageDeleteFlag=0
	AND cov.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_05}
	
	ORDER BY cov.WCTrackHistoryID
),
JNR_Exposure_TO_LinkData AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	SQ_WorkWCLine.WCTrackHistoryID AS WCTrackHistoryID1, 
	SQ_WorkWCLine.RiskState, 
	SQ_WorkWCLine.StateTermState, 
	SQ_WorkWCLine.ClassCode, 
	SQ_WorkWCLine.StatCode, 
	SQ_WorkWCLine.LimitValue, 
	SQ_WorkWCLine.BaseRate, 
	SQ_WorkWCLine.ParentCoverageType, 
	SQ_WorkWCLine.ChildCoverageType, 
	SQ_WorkWCLine.Premium, 
	SQ_WorkWCLine.COV_PeriodStartDate, 
	SQ_WorkWCLine.COV_PeriodEndDate, 
	SQ_WorkWCLine.TRM_PeriodStartDate, 
	SQ_WorkWCLine.TRM_PeriodEndDate, 
	SQ_WorkWCLine.TransactionEffectiveDate, 
	SQ_WorkWCLine.TransactionExpirationDate, 
	SQ_WorkWCLine.WC_LocationId, 
	SQ_WorkWCLine.ParentCoverageId, 
	SQ_WorkWCLine.MNRequiredFlag, 
	SQ_WorkWCLine.FIRST_TRMPeriodStartDate, 
	SQ_WorkWCLine.LAST_TRMPeriodStartDate, 
	SQ_WorkWCLine.FIRST_COVPeriodStartDate, 
	SQ_WorkWCLine.LAST_CovPeriodStartDate, 
	SQ_WorkWCLine.ParentCoverageDeleteFlag
	FROM SQ_WorkWCLine
	INNER JOIN SQ_WCPols00Record
	ON SQ_WCPols00Record.WCTrackHistoryID = SQ_WorkWCLine.WCTrackHistoryID
),
EXP_05_Output AS (
	SELECT
	CURRENT_TIMESTAMP AS ExtractDate,
	AuditId,
	WCTrackHistoryID,
	LinkData,
	RiskState,
	StateTermState,
	-- *INF*: IIF(IsNull(StateTermState),RiskState,StateTermState)
	IFF(StateTermState IS NULL, RiskState, StateTermState) AS v_Lkp_State,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',v_Lkp_State,'WCPOLS05Record','StateCodeRecord05')
	LKP_SUPWCPOLS__DCT_v_Lkp_State_WCPOLS05Record_StateCodeRecord05.WCPOLSCode AS o_StateCode,
	'05' AS RecordTypeCode,
	ClassCode,
	StatCode,
	ParentCoverageType,
	ChildCoverageType,
	-- *INF*: DECODE(TRUE,
	-- --ParentCoverageType='SecondInjuryFund' AND LTRIM(RTRIM(v_Lkp_State))='MN','0174',
	-- ParentCoverageType = 'ManualPremium', substr(ClassCode,1,4),
	-- NOT IN(ParentCoverageType,'ExpenseConstant','ExperienceModification','PremiumDiscount' ),substr(StatCode,1,4),
	-- '0000'
	-- )
	-- 
	DECODE(
	    TRUE,
	    ParentCoverageType = 'ManualPremium', substr(ClassCode, 1, 4),
	    NOT ParentCoverageType IN ('ExpenseConstant','ExperienceModification','PremiumDiscount'), substr(StatCode, 1, 4),
	    '0000'
	) AS v_ClassificationCode,
	v_ClassificationCode AS o_ClassificationCode,
	BaseRate,
	-- *INF*: IIF(LTRIM(RTRIM(ParentCoverageType))='EmpIoyersLiabilityIncreasedLimits',BaseRate/100,BaseRate)
	-- 
	IFF(
	    LTRIM(RTRIM(ParentCoverageType)) = 'EmpIoyersLiabilityIncreasedLimits', BaseRate / 100,
	    BaseRate
	) AS v_BaseRate_PriorCheck,
	-- *INF*: IIF(v_BaseRate_PriorCheck<1,v_BaseRate_PriorCheck,v_BaseRate_PriorCheck*10000)
	-- 
	IFF(v_BaseRate_PriorCheck < 1, v_BaseRate_PriorCheck, v_BaseRate_PriorCheck * 10000) AS v_BaseRate_Check,
	-- *INF*: DECODE(TRUE,
	-- v_BaseRate_Check<1,
	-- TO_CHAR(SUBSTR(v_BaseRate_PriorCheck,1,INSTR(v_BaseRate_PriorCheck,'.'))) || TO_CHAR(RPAD(SUBSTR(v_BaseRate_PriorCheck,INSTR(v_BaseRate_PriorCheck,'.')+1,4),4,0)),
	-- TO_CHAR(v_BaseRate_Check)
	-- )
	DECODE(
	    TRUE,
	    v_BaseRate_Check < 1, TO_CHAR(SUBSTR(v_BaseRate_PriorCheck, 1, REGEXP_INSTR(v_BaseRate_PriorCheck, '.'))) || TO_CHAR(RPAD(SUBSTR(v_BaseRate_PriorCheck, REGEXP_INSTR(v_BaseRate_PriorCheck, '.') + 1, 4), 4, 0)),
	    TO_CHAR(v_BaseRate_Check)
	) AS v_BaseRate,
	-- *INF*: DECODE(TRUE,
	-- --ParentCoverageType='SecondInjuryFund',LPAD('0',10,'0'),
	-- NOT ISNULL (v_BaseRate),LPAD((Replacechr(1,v_BaseRate,'.','')),10,'0'),
	-- LPAD('0',10,'0')
	-- )
	DECODE(
	    TRUE,
	    v_BaseRate IS NOT NULL, LPAD((REGEXP_REPLACE(v_BaseRate,'.','')), 10, '0'),
	    LPAD('0', 10, '0')
	) AS o_ManualChargedRate,
	COV_PeriodStartDate,
	COV_PeriodEndDate,
	TRM_PeriodStartDate,
	TRM_PeriodEndDate,
	-- *INF*: IIF(ParentCoverageType = 'ManualPremium',COV_PeriodStartDate,TRM_PeriodStartDate)
	IFF(ParentCoverageType = 'ManualPremium', COV_PeriodStartDate, TRM_PeriodStartDate) AS v_PeriodStartDate,
	-- *INF*: IIF(ParentCoverageType = 'ManualPremium',COV_PeriodEndDate,TRM_PeriodEndDate)
	IFF(ParentCoverageType = 'ManualPremium', COV_PeriodEndDate, TRM_PeriodEndDate) AS v_PeriodEndDate,
	-- *INF*: TO_CHAR(v_PeriodStartDate,'YYMMDD')
	TO_CHAR(v_PeriodStartDate, 'YYMMDD') AS ExposurePeriodEffectiveDate,
	-- *INF*: DECODE(TRUE,
	-- v_ClassificationCode= '1212'  and IN(v_Lkp_State,'DE','PA' ), 'ClassCode',
	-- v_ClassificationCode= '0012'  and NOT IN(v_Lkp_State,'DE','PA'), 'ClassCode',
	-- ParentCoverageType <> 'ManualPremium' and not IsNull(TRM_PeriodStartDate),'StatCode',
	-- ParentCoverageType = 'ManualPremium' and ((NOT IN(ChildCoverageType,'USL&H','USLandH','USLANDH')) or IsNull(ChildCoverageType)),'StateActOrFederalActExcludingUSLHW',
	-- ParentCoverageType = 'ManualPremium' and IN(ChildCoverageType,'USL&H','USLandH','USLANDH'),'USLHW',NULL
	-- )
	DECODE(
	    TRUE,
	    v_ClassificationCode = '1212' and v_Lkp_State IN ('DE','PA'), 'ClassCode',
	    v_ClassificationCode = '0012' and NOT v_Lkp_State IN ('DE','PA'), 'ClassCode',
	    ParentCoverageType <> 'ManualPremium' and TRM_PeriodStartDate IS NOT NULL, 'StatCode',
	    ParentCoverageType = 'ManualPremium' and ((NOT ChildCoverageType IN ('USL&H','USLandH','USLANDH')) or ChildCoverageType IS NULL), 'StateActOrFederalActExcludingUSLHW',
	    ParentCoverageType = 'ManualPremium' and ChildCoverageType IN ('USL&H','USLandH','USLANDH'), 'USLHW',
	    NULL
	) AS v_ExpCodeLkp,
	-- *INF*: :LKP.LKP_SupWCPOLS('DCT',v_ExpCodeLkp,'WCPOLS05Record','ExposureActExposureCoverageCode')
	LKP_SUPWCPOLS__DCT_v_ExpCodeLkp_WCPOLS05Record_ExposureActExposureCoverageCode.WCPOLSCode AS o_ExposureActExposureCoverageCode,
	LimitValue AS EstimatedExposureAmount,
	-- *INF*: DECODE(TRUE,
	-- ParentCoverageDeleteFlag='1' AND Premium=0,'0',
	-- IN(v_ClassificationCode,'7709','0908','0909','0912','0913','0916','0923','8989'),TO_CHAR(EstimatedExposureAmount*100),
	-- TO_CHAR(EstimatedExposureAmount))
	DECODE(
	    TRUE,
	    ParentCoverageDeleteFlag = '1' AND Premium = 0, '0',
	    v_ClassificationCode IN ('7709','0908','0909','0912','0913','0916','0923','8989'), TO_CHAR(EstimatedExposureAmount * 100),
	    TO_CHAR(EstimatedExposureAmount)
	) AS o_EstimatedExposureAmount,
	Premium,
	-- *INF*: TO_CHAR(Abs(To_Integer(Premium)))
	TO_CHAR(Abs(CAST(Premium AS INTEGER))) AS o_EstimatedPremiumAmount,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(UPPER(ParentCoverageType))),'UNINSUREDEMPLOYERSFUND','SECONDINJURYFUND') AND LTRIM(RTRIM(UPPER(v_Lkp_State)))='NJ',TO_CHAR(BaseRate*10000),
	-- LPAD('0',10,'0'))
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(UPPER(ParentCoverageType))) IN ('UNINSUREDEMPLOYERSFUND','SECONDINJURYFUND') AND LTRIM(RTRIM(UPPER(v_Lkp_State))) = 'NJ', TO_CHAR(BaseRate * 10000),
	    LPAD('0', 10, '0')
	) AS v_PolicySurchargeFactor,
	-- *INF*: IIF(NOT ISNULL (v_PolicySurchargeFactor),LPAD((Replacechr(1,v_PolicySurchargeFactor,'.','')),10,'0'),
	-- LPAD('0',10,'0'))
	-- 
	-- 
	IFF(
	    v_PolicySurchargeFactor IS NOT NULL,
	    LPAD((REGEXP_REPLACE(v_PolicySurchargeFactor,'.','')), 10, '0'),
	    LPAD('0', 10, '0')
	) AS PolicySurchargeFactor,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	-- *INF*: DECODE(TRUE,
	-- ParentCoverageType = 'ManualPremium' AND FIRST_COVPeriodStartDate<> LAST_CovPeriodStartDate ,
	-- TO_CHAR(v_PeriodStartDate,'YYMMDD'),
	-- ParentCoverageType <> 'ManualPremium' AND FIRST_TRMPeriodStartDate<>LAST_TRMPeriodStartDate,TO_CHAR(v_PeriodStartDate,'YYMMDD'),TO_CHAR(TransactionEffectiveDate,'YYMMDD')
	-- )
	DECODE(
	    TRUE,
	    ParentCoverageType = 'ManualPremium' AND FIRST_COVPeriodStartDate <> LAST_CovPeriodStartDate, TO_CHAR(v_PeriodStartDate, 'YYMMDD'),
	    ParentCoverageType <> 'ManualPremium' AND FIRST_TRMPeriodStartDate <> LAST_TRMPeriodStartDate, TO_CHAR(v_PeriodStartDate, 'YYMMDD'),
	    TO_CHAR(TransactionEffectiveDate, 'YYMMDD')
	) AS PolicyChangeEffectiveDate,
	-- *INF*: DECODE(TRUE,
	-- ParentCoverageType = 'ManualPremium' AND FIRST_COVPeriodStartDate<> LAST_CovPeriodStartDate ,
	-- TO_CHAR(v_PeriodEndDate,'YYMMDD'),
	-- ParentCoverageType <> 'ManualPremium' AND FIRST_TRMPeriodStartDate<>LAST_TRMPeriodStartDate,TO_CHAR(v_PeriodEndDate,'YYMMDD'),TO_CHAR(TransactionExpirationDate,'YYMMDD')
	-- )
	-- 
	-- 
	-- 
	-- 
	-- 
	DECODE(
	    TRUE,
	    ParentCoverageType = 'ManualPremium' AND FIRST_COVPeriodStartDate <> LAST_CovPeriodStartDate, TO_CHAR(v_PeriodEndDate, 'YYMMDD'),
	    ParentCoverageType <> 'ManualPremium' AND FIRST_TRMPeriodStartDate <> LAST_TRMPeriodStartDate, TO_CHAR(v_PeriodEndDate, 'YYMMDD'),
	    TO_CHAR(TransactionExpirationDate, 'YYMMDD')
	) AS PolicyChangeExpirationDate,
	WC_LocationId,
	ParentCoverageId,
	MNRequiredFlag,
	-- *INF*: DECODE(TRUE,
	-- IN(UPPER(ParentCoverageType),'DTEC','TRIA') AND LTRIM(RTRIM(v_Lkp_State))<>'WI' AND (ISNULL(Premium) OR Premium=0),'0',
	-- (LTRIM(RTRIM(v_ClassificationCode))='9999' AND IN(UPPER(ParentCoverageType),'ADMINISTRATIONFUND','DIASURCHARGE','EMPLOYERASSESSMENT','INSURANCEGUARANTYASSOCIATION','OTHERTAXESANDASSESSMENTS1','OTHERTAXESANDASSESSMENTS2','OTHERTAXESANDASSESSMENTS3','SAFETYEDUCATIONANDTRAININGFUND','SECONDINJURYFUND','SECURITYFUNDCHARGE','STATEASSESSMENT','UNINSUREDEMPLOYERSFUND')),'0',
	-- '1'
	-- )
	DECODE(
	    TRUE,
	    UPPER(ParentCoverageType) IN ('DTEC','TRIA') AND LTRIM(RTRIM(v_Lkp_State)) <> 'WI' AND (Premium IS NULL OR Premium = 0), '0',
	    (LTRIM(RTRIM(v_ClassificationCode)) = '9999' AND UPPER(ParentCoverageType) IN ('ADMINISTRATIONFUND','DIASURCHARGE','EMPLOYERASSESSMENT','INSURANCEGUARANTYASSOCIATION','OTHERTAXESANDASSESSMENTS1','OTHERTAXESANDASSESSMENTS2','OTHERTAXESANDASSESSMENTS3','SAFETYEDUCATIONANDTRAININGFUND','SECONDINJURYFUND','SECURITYFUNDCHARGE','STATEASSESSMENT','UNINSUREDEMPLOYERSFUND')), '0',
	    '1'
	) AS Record05FilterFlag,
	FIRST_TRMPeriodStartDate,
	LAST_TRMPeriodStartDate,
	FIRST_COVPeriodStartDate,
	LAST_CovPeriodStartDate,
	ParentCoverageDeleteFlag
	FROM JNR_Exposure_TO_LinkData
	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_v_Lkp_State_WCPOLS05Record_StateCodeRecord05
	ON LKP_SUPWCPOLS__DCT_v_Lkp_State_WCPOLS05Record_StateCodeRecord05.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_v_Lkp_State_WCPOLS05Record_StateCodeRecord05.SourceCode = v_Lkp_State
	AND LKP_SUPWCPOLS__DCT_v_Lkp_State_WCPOLS05Record_StateCodeRecord05.TableName = 'WCPOLS05Record'
	AND LKP_SUPWCPOLS__DCT_v_Lkp_State_WCPOLS05Record_StateCodeRecord05.ProcessName = 'StateCodeRecord05'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_v_ExpCodeLkp_WCPOLS05Record_ExposureActExposureCoverageCode
	ON LKP_SUPWCPOLS__DCT_v_ExpCodeLkp_WCPOLS05Record_ExposureActExposureCoverageCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_v_ExpCodeLkp_WCPOLS05Record_ExposureActExposureCoverageCode.SourceCode = v_ExpCodeLkp
	AND LKP_SUPWCPOLS__DCT_v_ExpCodeLkp_WCPOLS05Record_ExposureActExposureCoverageCode.TableName = 'WCPOLS05Record'
	AND LKP_SUPWCPOLS__DCT_v_ExpCodeLkp_WCPOLS05Record_ExposureActExposureCoverageCode.ProcessName = 'ExposureActExposureCoverageCode'

),
FIL_ClassCode AS (
	SELECT
	ExtractDate, 
	AuditId, 
	WCTrackHistoryID, 
	LinkData, 
	o_StateCode AS StateCode, 
	RecordTypeCode, 
	o_ClassificationCode AS ClassificationCode, 
	o_ExposureActExposureCoverageCode AS ExposureActExposureCoverageCode, 
	o_ManualChargedRate AS ManualChargedRate, 
	ExposurePeriodEffectiveDate, 
	o_EstimatedExposureAmount AS EstimatedExposureAmount, 
	o_EstimatedPremiumAmount AS EstimatedPremiumAmount, 
	PolicyChangeEffectiveDate, 
	PolicyChangeExpirationDate, 
	WC_LocationId, 
	ParentCoverageId, 
	Record05FilterFlag, 
	ParentCoverageDeleteFlag, 
	PolicySurchargeFactor
	FROM EXP_05_Output
	WHERE Not IsNull(ClassificationCode) AND Record05FilterFlag='1'
),
EXP_Format_Output AS (
	SELECT
	CURRENT_TIMESTAMP AS o_ExtractDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WCTrackHistoryID,
	LinkData,
	'03' AS o_RecordTypeCode,
	LocationType,
	-- *INF*: Decode(TRUE,
	-- LocationType = 'Account' and NameLinkIdentifier='001','1',
	-- LocationType = 'Agency','5',
	-- LocationType = 'WBMI','3',
	-- INSTR(Address1,'VARIOUS')>0 
	-- OR INSTR(Address1,'ADDRESS')>0 
	-- OR INSTR(Address1,'LOCATION')>0 
	-- OR INSTR(Address1,'USED')>0 
	-- OR INSTR(Address1,'SPECIFIED')>0,'6',
	-- '2')
	Decode(
	    TRUE,
	    LocationType = 'Account' and NameLinkIdentifier = '001', '1',
	    LocationType = 'Agency', '5',
	    LocationType = 'WBMI', '3',
	    REGEXP_INSTR(Address1, 'VARIOUS') > 0 OR REGEXP_INSTR(Address1, 'ADDRESS') > 0 OR REGEXP_INSTR(Address1, 'LOCATION') > 0 OR REGEXP_INSTR(Address1, 'USED') > 0 OR REGEXP_INSTR(Address1, 'SPECIFIED') > 0, '6',
	    '2'
	) AS v_AddressTypeCode,
	v_AddressTypeCode AS o_AddressTypeCode,
	OriginalCountry AS Country,
	-- *INF*: IIF (Country = 'US' or Country = 'USA' or Country = 'UnitedStatesOfAmerica' or Country = 'UNITEDSTATESOFAMERICA' or IsNull(Country) or rtrim(ltrim(Country))='','N','Y')
	IFF(
	    Country = 'US'
	    or Country = 'USA'
	    or Country = 'UnitedStatesOfAmerica'
	    or Country = 'UNITEDSTATESOFAMERICA'
	    or Country IS NULL
	    or rtrim(ltrim(Country)) = '',
	    'N',
	    'Y'
	) AS v_ForeignAddressIndicator,
	v_ForeignAddressIndicator AS o_ForeignAddressIndicator,
	-- *INF*: DECODE(TRUE,
	-- v_AddressTypeCode='6','0',
	-- '1')
	DECODE(
	    TRUE,
	    v_AddressTypeCode = '6', '0',
	    '1'
	) AS o_AddressStructureCode,
	OriginalAddress1 AS Address1,
	OriginalAddress AS Address2,
	-- *INF*: DECODE(TRUE,
	-- v_AddressTypeCode='6','',
	-- SUBSTR(Address1 || ' ' || Address2, 1, 60)
	-- )
	DECODE(
	    TRUE,
	    v_AddressTypeCode = '6', '',
	    SUBSTR(Address1 || ' ' || Address2, 1, 60)
	) AS o_AddressStreet,
	OriginalCity AS AddressCity,
	-- *INF*: DECODE(TRUE,
	-- v_AddressTypeCode='6','',
	-- AddressCity)
	DECODE(
	    TRUE,
	    v_AddressTypeCode = '6', '',
	    AddressCity
	) AS o_AddressCity,
	OriginalStateProv AS AddressState,
	-- *INF*: DECODE(TRUE,
	-- v_AddressTypeCode='6','',
	-- AddressState)
	DECODE(
	    TRUE,
	    v_AddressTypeCode = '6', '',
	    AddressState
	) AS o_AddressState,
	OriginalPostalCode AS PostalCode,
	-- *INF*: IIF (LENGTH(PostalCode) = 5 or LENGTH(PostalCode) = 9, PostalCode,
	-- IIF (LENGTH(PostalCode) = 10 and SUBSTR(PostalCode,6,1) = '-',CONCAT(SUBSTR(PostalCode,1,5),SUBSTR(PostalCode,7,4)),''))
	IFF(
	    LENGTH(PostalCode) = 5 or LENGTH(PostalCode) = 9, PostalCode,
	    IFF(
	        LENGTH(PostalCode) = 10
	    and SUBSTR(PostalCode, 6, 1) = '-',
	        CONCAT(SUBSTR(PostalCode, 1, 5), SUBSTR(PostalCode, 7, 4)),
	        ''
	    )
	) AS v_PostalCode,
	-- *INF*: DECODE(TRUE,
	-- v_AddressTypeCode='6','',
	-- IS_NUMBER(v_PostalCode),v_PostalCode,'')
	DECODE(
	    TRUE,
	    v_AddressTypeCode = '6', '',
	    REGEXP_LIKE(v_PostalCode, '^[0-9]+$'), v_PostalCode,
	    ''
	) AS o_PostalCode,
	NameLinkIdentifier,
	-- *INF*: DECODE(TRUE,
	-- IN(LocationType,'WBMI','Agency'),'99',
	--  :LKP.LKP_SupWCPOLS('DCT',stateLinkCodeState,'WCPOLS03Record','StateCodeRecord03')
	-- )
	-- --IIF (LocationType = 'WBMI','99', :LKP.LKP_SupWCPOLS('DCT',stateLinkCodeState,'WCPOLS03Record','StateCodeRecord03'))
	DECODE(
	    TRUE,
	    LocationType IN ('WBMI','Agency'), '99',
	    LKP_SUPWCPOLS__DCT_stateLinkCodeState_WCPOLS03Record_StateCodeRecord03.WCPOLSCode
	) AS o_StateCodeLink,
	ExposureRecordLinkForLocationCode,
	-- *INF*: IIF(IN(v_AddressTypeCode,'3','4','5'),'99999',ExposureRecordLinkForLocationCode)
	IFF(v_AddressTypeCode IN ('3','4','5'), '99999', ExposureRecordLinkForLocationCode) AS o_ExposureRecordLinkForLocationCode,
	PhoneNumber AS PhoneNumberOfInsured,
	NAICSCode AS IndustryCode,
	-- *INF*: IIF(IN(v_AddressTypeCode,'1','2','6'),IndustryCode,'')
	IFF(v_AddressTypeCode IN ('1','2','6'), IndustryCode, '') AS o_IndustryCode,
	-- *INF*: IIF(v_ForeignAddressIndicator='Y',AddressState,'')
	-- --IIF (Country <> 'USA' and Country <> 'UnitedStatesOfAmerica' and Country <> 'UNITEDSTATESOFAMERICA',AddressState,'')
	IFF(v_ForeignAddressIndicator = 'Y', AddressState, '') AS o_GeographicArea,
	Email AS i_EmailAddress,
	-- *INF*: IIF(IN(v_AddressTypeCode, '1', '2', '6'), IIF(ISNULL(i_EmailAddress), '', i_EmailAddress), '')
	IFF(
	    v_AddressTypeCode IN ('1','2','6'),
	    IFF(
	        i_EmailAddress IS NULL, '', i_EmailAddress
	    ),
	    ''
	) AS o_EmailAddress,
	-- *INF*: DECODE(TRUE,
	-- v_ForeignAddressIndicator='Y' AND ISNULL(:LKP.LKP_SupWCPOLS('DCT',Country,'WCPOLS03Record','CountryCode')),'  ',
	-- v_ForeignAddressIndicator='Y',:LKP.LKP_SupWCPOLS('DCT',Country,'WCPOLS03Record','CountryCode'),
	-- ' ')
	-- 
	DECODE(
	    TRUE,
	    v_ForeignAddressIndicator = 'Y' AND LKP_SUPWCPOLS__DCT_Country_WCPOLS03Record_CountryCode.WCPOLSCode IS NULL, '  ',
	    v_ForeignAddressIndicator = 'Y', LKP_SUPWCPOLS__DCT_Country_WCPOLS03Record_CountryCode.WCPOLSCode,
	    ' '
	) AS o_CountryCode,
	'00' AS NameLinkCounterIdentifier,
	TransactionEffectiveDate AS PolicyChangeEffectiveDate,
	-- *INF*: TO_CHAR(PolicyChangeEffectiveDate,'YYMMDD')
	TO_CHAR(PolicyChangeEffectiveDate, 'YYMMDD') AS o_PolicyChangeEffectiveDate,
	TransactionExpirationDate AS PolicyChangeExpirationDate,
	-- *INF*: DECODE(TRUE,
	-- Name_Location_Deleted_Indicator='1',TO_CHAR(PolicyChangeEffectiveDate,'YYMMDD'),
	-- TO_CHAR(PolicyChangeExpirationDate,'YYMMDD')
	-- )
	-- 
	DECODE(
	    TRUE,
	    Name_Location_Deleted_Indicator = '1', TO_CHAR(PolicyChangeEffectiveDate, 'YYMMDD'),
	    TO_CHAR(PolicyChangeExpirationDate, 'YYMMDD')
	) AS o_PolicyChangeExpirationDate,
	stateLinkCodeState,
	WC_LocationId AS WC_LocationId2,
	LocationDeletedIndicator,
	Name_Location_Deleted_Indicator,
	-- *INF*: iif ( (v_AddressTypeCode = '2' and (SUBSTR(Address1,1,6) = 'PO BOX'  or SUBSTR(Address1,1,7) ='P O BOX' or SUBSTR(Address1,1,8) = 'P.O. BOX'))
	-- ,0, 1)
	-- 
	-- -- 0 filter out
	-- --1 loads into 03 record table
	-- 
	-- 
	IFF(
	    (v_AddressTypeCode = '2'
	    and (SUBSTR(Address1, 1, 6) = 'PO BOX'
	    or SUBSTR(Address1, 1, 7) = 'P O BOX'
	    or SUBSTR(Address1, 1, 8) = 'P.O. BOX')),
	    0,
	    1
	) AS o_process_flag
	FROM RTR_StateSplit_DEFAULT1
	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_stateLinkCodeState_WCPOLS03Record_StateCodeRecord03
	ON LKP_SUPWCPOLS__DCT_stateLinkCodeState_WCPOLS03Record_StateCodeRecord03.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_stateLinkCodeState_WCPOLS03Record_StateCodeRecord03.SourceCode = stateLinkCodeState
	AND LKP_SUPWCPOLS__DCT_stateLinkCodeState_WCPOLS03Record_StateCodeRecord03.TableName = 'WCPOLS03Record'
	AND LKP_SUPWCPOLS__DCT_stateLinkCodeState_WCPOLS03Record_StateCodeRecord03.ProcessName = 'StateCodeRecord03'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_Country_WCPOLS03Record_CountryCode
	ON LKP_SUPWCPOLS__DCT_Country_WCPOLS03Record_CountryCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_Country_WCPOLS03Record_CountryCode.SourceCode = Country
	AND LKP_SUPWCPOLS__DCT_Country_WCPOLS03Record_CountryCode.TableName = 'WCPOLS03Record'
	AND LKP_SUPWCPOLS__DCT_Country_WCPOLS03Record_CountryCode.ProcessName = 'CountryCode'

),
UN_Merge_LocationInfo AS (
	SELECT o_AddressTypeCode AS AddressTypeCode, NameLinkIdentifier, o_StateCodeLink AS StateCodeLink, o_ExposureRecordLinkForLocationCode AS ExposureRecordLinkForLocationCode, WC_LocationId1 AS WC_LocationId, LocationDeletedIndicator
	FROM EXP_Format_Output_IN_IA_MO
	UNION
	SELECT o_AddressTypeCode AS AddressTypeCode, NameLinkIdentifier, o_StateCodeLink AS StateCodeLink, o_ExposureRecordLinkForLocationCode AS ExposureRecordLinkForLocationCode, WC_LocationId2 AS WC_LocationId, LocationDeletedIndicator
	FROM EXP_Format_Output
),
JNR_Exposure_Location_LinkUp AS (SELECT
	FIL_ClassCode.ExtractDate, 
	FIL_ClassCode.AuditId, 
	FIL_ClassCode.WCTrackHistoryID, 
	FIL_ClassCode.LinkData, 
	FIL_ClassCode.StateCode, 
	FIL_ClassCode.RecordTypeCode, 
	FIL_ClassCode.ClassificationCode, 
	FIL_ClassCode.ExposureActExposureCoverageCode, 
	FIL_ClassCode.ManualChargedRate, 
	FIL_ClassCode.ExposurePeriodEffectiveDate, 
	FIL_ClassCode.EstimatedExposureAmount, 
	FIL_ClassCode.EstimatedPremiumAmount, 
	FIL_ClassCode.PolicyChangeEffectiveDate, 
	FIL_ClassCode.PolicyChangeExpirationDate, 
	FIL_ClassCode.WC_LocationId, 
	FIL_ClassCode.ParentCoverageId, 
	FIL_ClassCode.Record05FilterFlag, 
	FIL_ClassCode.ParentCoverageDeleteFlag, 
	FIL_ClassCode.PolicySurchargeFactor, 
	UN_Merge_LocationInfo.AddressTypeCode, 
	UN_Merge_LocationInfo.NameLinkIdentifier, 
	UN_Merge_LocationInfo.StateCodeLink, 
	UN_Merge_LocationInfo.ExposureRecordLinkForLocationCode, 
	UN_Merge_LocationInfo.WC_LocationId AS WC_LocationId1, 
	UN_Merge_LocationInfo.LocationDeletedIndicator
	FROM FIL_ClassCode
	LEFT OUTER JOIN UN_Merge_LocationInfo
	ON UN_Merge_LocationInfo.WC_LocationId = FIL_ClassCode.WC_LocationId
),
AGG_DuplicateElimination AS (
	SELECT
	ExtractDate,
	AuditId,
	WCTrackHistoryID,
	LinkData,
	StateCode,
	RecordTypeCode,
	ClassificationCode,
	ExposureActExposureCoverageCode,
	ManualChargedRate,
	ExposurePeriodEffectiveDate,
	EstimatedExposureAmount,
	EstimatedPremiumAmount,
	PolicyChangeEffectiveDate,
	PolicyChangeExpirationDate,
	ParentCoverageId,
	Record05FilterFlag,
	ParentCoverageDeleteFlag,
	AddressTypeCode,
	NameLinkIdentifier,
	-- *INF*: max(NameLinkIdentifier)
	max(NameLinkIdentifier) AS o_NameLinkIdentifier,
	StateCodeLink,
	ExposureRecordLinkForLocationCode,
	LocationDeletedIndicator,
	PolicySurchargeFactor
	FROM JNR_Exposure_Location_LinkUp
	GROUP BY WCTrackHistoryID, LinkData, StateCode, ClassificationCode, ManualChargedRate, ParentCoverageId
),
EXP_Record05_Tgt_DataCollect AS (
	SELECT
	ExtractDate,
	AuditId,
	WCTrackHistoryID,
	LinkData,
	StateCode,
	RecordTypeCode,
	ClassificationCode,
	ExposureActExposureCoverageCode,
	ManualChargedRate,
	ExposurePeriodEffectiveDate,
	EstimatedExposureAmount,
	EstimatedPremiumAmount,
	PolicyChangeEffectiveDate,
	PolicyChangeExpirationDate,
	-- *INF*: DECODE(TRUE,
	-- (LocationDeletedIndicator='1' OR ParentCoverageDeleteFlag='1'),PolicyChangeEffectiveDate,
	-- PolicyChangeExpirationDate
	-- )
	DECODE(
	    TRUE,
	    (LocationDeletedIndicator = '1' OR ParentCoverageDeleteFlag = '1'), PolicyChangeEffectiveDate,
	    PolicyChangeExpirationDate
	) AS o_PolicyChangeExpirationDate,
	Record05FilterFlag,
	AddressTypeCode,
	o_NameLinkIdentifier AS NameLinkIdentifier,
	StateCodeLink,
	ExposureRecordLinkForLocationCode,
	'00' AS NameLinkCounterIdentifier,
	LocationDeletedIndicator,
	ParentCoverageDeleteFlag,
	PolicySurchargeFactor
	FROM AGG_DuplicateElimination
),
WCPols05Record AS (

	------------ PRE SQL ----------
	DELETE
	  FROM dbo.WCPols05Record
	  WHERE 1=1
	  AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	-------------------------------


	INSERT INTO WCPols05Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, StateCode, RecordTypeCode, ClassificationCode, ExposureActExposureCoverageCode, ManualChargedRate, ExposurePeriodEffectiveDate, EstimatedExposureAmount, EstimatedPremiumAmount, NameLinkIdentifier, StateCodeLink, ExposureRecordLinkForExposureCode, NameLinkCounterIdentifier, PolicySurchargeFactor, PolicyChangeEffectiveDate, PolicyChangeExpirationDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	STATECODE, 
	RECORDTYPECODE, 
	CLASSIFICATIONCODE, 
	EXPOSUREACTEXPOSURECOVERAGECODE, 
	MANUALCHARGEDRATE, 
	EXPOSUREPERIODEFFECTIVEDATE, 
	ESTIMATEDEXPOSUREAMOUNT, 
	ESTIMATEDPREMIUMAMOUNT, 
	NAMELINKIDENTIFIER, 
	STATECODELINK, 
	ExposureRecordLinkForLocationCode AS EXPOSURERECORDLINKFOREXPOSURECODE, 
	NAMELINKCOUNTERIDENTIFIER, 
	POLICYSURCHARGEFACTOR, 
	POLICYCHANGEEFFECTIVEDATE, 
	o_PolicyChangeExpirationDate AS POLICYCHANGEEXPIRATIONDATE
	FROM EXP_Record05_Tgt_DataCollect
),
WCPols02Record AS (

	------------ PRE SQL ----------
	DELETE
	  FROM dbo.WCPols02Record
	  WHERE 1=1
	  AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	-------------------------------


	INSERT INTO WCPols02Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, RecordTypeCode, NameTypeCode, NameLinkIdentifier, ProfessionalEmployerOrganizationOrClientCompanyCode, NameOfInsured, FederalEmployerIdentificationNumber, ContinuationSequenceNumber, LegalNatureOfEntityCode, StateCode01, StateUnemploymentNumber01, StateCode02, StateUnemploymentNumber02, StateCode03, StateUnemploymentNumber03, StateUnemploymentNumberRecordSequenceNumber, NameLinkCounterIdentifier, PolicyChangeEffectiveDate, PolicyChangeExpirationDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	RECORDTYPECODE, 
	o_NameTypeCode AS NAMETYPECODE, 
	NAMELINKIDENTIFIER, 
	o_ProfessionalEmployerOrganizationOrClientCompanyCode AS PROFESSIONALEMPLOYERORGANIZATIONORCLIENTCOMPANYCODE, 
	o_NameInsured_02record AS NAMEOFINSURED, 
	FEDERALEMPLOYERIDENTIFICATIONNUMBER, 
	CONTINUATIONSEQUENCENUMBER, 
	LEGALNATUREOFENTITYCODE, 
	o_StateCode01 AS STATECODE01, 
	IN_StateUnemploymentNumber AS STATEUNEMPLOYMENTNUMBER01, 
	STATECODE02, 
	STATEUNEMPLOYMENTNUMBER02, 
	STATECODE03, 
	STATEUNEMPLOYMENTNUMBER03, 
	STATEUNEMPLOYMENTNUMBERRECORDSEQUENCENUMBER, 
	NAMELINKCOUNTERIDENTIFIER, 
	POLICYCHANGEEFFECTIVEDATE, 
	POLICYCHANGEEXPIRATIONDATE
	FROM EXP_Record02_TGT_DataCollect
),
fltr_DBA_POBOX AS (
	SELECT
	o_ExtractDate AS ExtractDate, 
	o_AuditId AS AuditId, 
	WCTrackHistoryID, 
	LinkData, 
	o_RecordTypeCode AS RecordTypeCode, 
	o_AddressTypeCode AS AddressTypeCode, 
	o_ForeignAddressIndicator AS ForeignAddressIndicator, 
	o_AddressStructureCode AS AddressStructureCode, 
	o_AddressStreet AS AddressStreet, 
	o_AddressCity AS AddressCity, 
	o_AddressState AS AddressState, 
	o_PostalCode AS AddressZipCode, 
	NameLinkIdentifier, 
	o_StateCodeLink AS StateCodeLink, 
	o_ExposureRecordLinkForLocationCode AS ExposureRecordLinkForLocationCode, 
	PhoneNumberOfInsured, 
	NumberOfEmployees, 
	o_IndustryCode AS IndustryCode, 
	o_GeographicArea AS GeographicArea, 
	o_EmailAddress AS EmailAddress, 
	o_CountryCode AS CountryCode, 
	NameLinkCounterIdentifier, 
	o_PolicyChangeEffectiveDate AS PolicyChangeEffectiveDate, 
	o_PolicyChangeExpirationDate AS PolicyChangeExpirationDate, 
	o_process_flag AS process_flag
	FROM EXP_Format_Output
	WHERE iif( process_flag = 1, TRUE,FALSE)
),
WCPols03Record AS (

	------------ PRE SQL ----------
	DELETE
	  FROM dbo.WCPols03Record
	  WHERE 1=1
	  AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	-------------------------------


	INSERT INTO WCPols03Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, RecordTypeCode, AddressTypeCode, ForeignAddressIndicator, AddressStructureCode, AddressStreet, AddressCity, AddressState, AddressZipCode, NameLinkIdentifier, StateCodeLink, ExposureRecordLinkForLocationCode, PhoneNumberOfInsured, NumberOfEmployees, IndustryCode, GeographicArea, EmailAddress, CountryCode, NameLinkCounterIdentifier, PolicyChangeEffectiveDate, PolicyChangeExpirationDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	RECORDTYPECODE, 
	ADDRESSTYPECODE, 
	FOREIGNADDRESSINDICATOR, 
	ADDRESSSTRUCTURECODE, 
	ADDRESSSTREET, 
	ADDRESSCITY, 
	ADDRESSSTATE, 
	ADDRESSZIPCODE, 
	NAMELINKIDENTIFIER, 
	STATECODELINK, 
	EXPOSURERECORDLINKFORLOCATIONCODE, 
	PHONENUMBEROFINSURED, 
	NUMBEROFEMPLOYEES, 
	INDUSTRYCODE, 
	GEOGRAPHICAREA, 
	EMAILADDRESS, 
	COUNTRYCODE, 
	NAMELINKCOUNTERIDENTIFIER, 
	POLICYCHANGEEFFECTIVEDATE, 
	POLICYCHANGEEXPIRATIONDATE
	FROM fltr_DBA_POBOX
),
GenericLoggingFlatFile1 AS (
	INSERT INTO GenericLoggingFlatFile
	(FileName, DataLogRow)
	SELECT 
	LogName AS FILENAME, 
	LogRecord AS DATALOGROW
	FROM EXP_Duplicates
),