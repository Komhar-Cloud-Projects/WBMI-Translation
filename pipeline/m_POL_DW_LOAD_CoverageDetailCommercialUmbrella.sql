WITH
SQ_PifDept1553Stage AS (
	select dept.PifSymbol,
	dept.PifPolicyNumber,
	dept.PifModule,
	dept.DECLPTText1701+dept.DECLPTText71791 as Text01,
	dept.DECLPTText1702+dept.DECLPTText71792 as Text02
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PifDept1553Stage dept
	where left(dept.PifSymbol,2) in ('CU','NU')
	and right(dept.DECLPTFormNumber,2)<>'01'
	-----------DEBUG----------------------------------
	--AND PifSymbol + PifPolicyNumber + PifModule = 'NUT179931501' and 1=2
	-----------DEBUG----------------------------------
	order by dept.PifDept1553StageId
),
EXP_GetValues AS (
	SELECT
	PifSymbol AS i_PifSymbol,
	PifPolicyNumber AS i_PifPolicyNumber,
	PifModule AS i_PifModule,
	i_PifSymbol || i_PifPolicyNumber || i_PifModule AS o_PolicyKey,
	Text1 AS i_Text1,
	Text2 AS i_Text2,
	v_Text1_Cur AS v_Text1_Prev,
	v_Text2_Cur AS v_Text2_Prev,
	-- *INF*: LTRIM(RTRIM(i_Text1))
	LTRIM(RTRIM(i_Text1)) AS v_Text1_Cur,
	-- *INF*: LTRIM(RTRIM(i_Text2))
	LTRIM(RTRIM(i_Text2)) AS v_Text2_Cur,
	-- *INF*: LTRIM(RTRIM(SUBSTR(v_Text1_Prev,INSTR(v_Text1_Prev,'@')+1,INSTR(v_Text1_Prev,'')-INSTR(v_Text1_Prev,'@')-1)))
	LTRIM(RTRIM(SUBSTR(v_Text1_Prev, INSTR(v_Text1_Prev, '@') + 1, INSTR(v_Text1_Prev, '') - INSTR(v_Text1_Prev, '@') - 1))) AS v_UnderlyingInsurer_1_Prev,
	-- *INF*: LTRIM(RTRIM(SUBSTR(v_Text1_Cur,INSTR(v_Text1_Cur,'@')+1,INSTR(v_Text1_Cur,'')-INSTR(v_Text1_Cur,'@')-1)))
	LTRIM(RTRIM(SUBSTR(v_Text1_Cur, INSTR(v_Text1_Cur, '@') + 1, INSTR(v_Text1_Cur, '') - INSTR(v_Text1_Cur, '@') - 1))) AS v_UnderlyingInsurer_1_Cur,
	-- *INF*: LTRIM(RTRIM(SUBSTR(v_Text2_Prev,INSTR(v_Text2_Prev,'@')+1,INSTR(v_Text2_Prev,'')-INSTR(v_Text2_Prev,'@')-1)))
	LTRIM(RTRIM(SUBSTR(v_Text2_Prev, INSTR(v_Text2_Prev, '@') + 1, INSTR(v_Text2_Prev, '') - INSTR(v_Text2_Prev, '@') - 1))) AS v_UnderlyingInsurer_2_Prev,
	-- *INF*: LTRIM(RTRIM(SUBSTR(v_Text2_Cur,INSTR(v_Text2_Cur,'@')+1,INSTR(v_Text2_Cur,'')-INSTR(v_Text2_Cur,'@')-1)))
	LTRIM(RTRIM(SUBSTR(v_Text2_Cur, INSTR(v_Text2_Cur, '@') + 1, INSTR(v_Text2_Cur, '') - INSTR(v_Text2_Cur, '@') - 1))) AS v_UnderlyingInsurer_2_Cur,
	-- *INF*: LTRIM(RTRIM(SUBSTR(v_Text1_Prev,INSTR(v_Text1_Prev,'@')+1,INSTR(v_Text1_Prev,'')-INSTR(v_Text1_Prev,'@')-1)))
	LTRIM(RTRIM(SUBSTR(v_Text1_Prev, INSTR(v_Text1_Prev, '@') + 1, INSTR(v_Text1_Prev, '') - INSTR(v_Text1_Prev, '@') - 1))) AS v_UnderlyingPolicyKey_1_Prev,
	-- *INF*: LTRIM(RTRIM(SUBSTR(v_Text1_Cur,INSTR(v_Text1_Cur,'@')+1,INSTR(v_Text1_Cur,'')-INSTR(v_Text1_Cur,'@')-1)))
	LTRIM(RTRIM(SUBSTR(v_Text1_Cur, INSTR(v_Text1_Cur, '@') + 1, INSTR(v_Text1_Cur, '') - INSTR(v_Text1_Cur, '@') - 1))) AS v_UnderlyingPolicyKey_1_Cur,
	-- *INF*: LTRIM(RTRIM(SUBSTR(v_Text2_Prev,INSTR(v_Text2_Prev,'@')+1,INSTR(v_Text2_Prev,'')-INSTR(v_Text2_Prev,'@')-1)))
	LTRIM(RTRIM(SUBSTR(v_Text2_Prev, INSTR(v_Text2_Prev, '@') + 1, INSTR(v_Text2_Prev, '') - INSTR(v_Text2_Prev, '@') - 1))) AS v_UnderlyingPolicyKey_2_Prev,
	-- *INF*: LTRIM(RTRIM(SUBSTR(v_Text2_Cur,INSTR(v_Text2_Cur,'@')+1,INSTR(v_Text2_Cur,'')-INSTR(v_Text2_Cur,'@')-1)))
	LTRIM(RTRIM(SUBSTR(v_Text2_Cur, INSTR(v_Text2_Cur, '@') + 1, INSTR(v_Text2_Cur, '') - INSTR(v_Text2_Cur, '@') - 1))) AS v_UnderlyingPolicyKey_2_Cur,
	-- *INF*: DECODE(TRUE,
	-- INSTR(v_Text1_Prev,'EMPLOYERS' || CHR(39) || ' LIABILITY INSURANCE')>0,1,
	-- INSTR(v_Text2_Prev,'EMPLOYERS' || CHR(39) || ' LIABILITY INSURANCE')>0,2,
	-- 0)
	DECODE(TRUE,
	INSTR(v_Text1_Prev, 'EMPLOYERS' || CHR(39) || ' LIABILITY INSURANCE') > 0, 1,
	INSTR(v_Text2_Prev, 'EMPLOYERS' || CHR(39) || ' LIABILITY INSURANCE') > 0, 2,
	0) AS v_EPLI_Indicator,
	-- *INF*: DECODE(TRUE,
	-- v_EPLI_Indicator=1,v_UnderlyingInsurer_2_Prev,
	-- v_EPLI_Indicator=2,v_UnderlyingInsurer_1_Cur,
	-- v_EPLI_Indicator=0,''
	-- )
	DECODE(TRUE,
	v_EPLI_Indicator = 1, v_UnderlyingInsurer_2_Prev,
	v_EPLI_Indicator = 2, v_UnderlyingInsurer_1_Cur,
	v_EPLI_Indicator = 0, '') AS o_EPLI_Company_Name,
	-- *INF*: DECODE(TRUE,
	-- v_EPLI_Indicator=1,v_UnderlyingPolicyKey_1_Cur,
	-- v_EPLI_Indicator=2,v_UnderlyingPolicyKey_2_Cur,
	-- v_EPLI_Indicator=0,''
	-- )
	DECODE(TRUE,
	v_EPLI_Indicator = 1, v_UnderlyingPolicyKey_1_Cur,
	v_EPLI_Indicator = 2, v_UnderlyingPolicyKey_2_Cur,
	v_EPLI_Indicator = 0, '') AS o_EPLI_PolicyKey,
	-- *INF*: DECODE(TRUE,
	-- INSTR(v_Text1_Prev,'BUSINESSOWNERS LIABILITY')>0,1,
	-- INSTR(v_Text2_Prev,'BUSINESSOWNERS LIABILITY')>0,2,
	-- 0)
	DECODE(TRUE,
	INSTR(v_Text1_Prev, 'BUSINESSOWNERS LIABILITY') > 0, 1,
	INSTR(v_Text2_Prev, 'BUSINESSOWNERS LIABILITY') > 0, 2,
	0) AS v_Business_Indicator,
	-- *INF*: DECODE(TRUE,
	-- v_Business_Indicator=1,v_UnderlyingInsurer_2_Prev,
	-- v_Business_Indicator=2,v_UnderlyingInsurer_1_Cur,
	-- v_Business_Indicator=0,''
	-- )
	DECODE(TRUE,
	v_Business_Indicator = 1, v_UnderlyingInsurer_2_Prev,
	v_Business_Indicator = 2, v_UnderlyingInsurer_1_Cur,
	v_Business_Indicator = 0, '') AS v_Business_Company_Name,
	-- *INF*: DECODE(TRUE,
	-- v_Business_Indicator=1,v_UnderlyingPolicyKey_1_Cur,
	-- v_Business_Indicator=2,v_UnderlyingPolicyKey_2_Cur,
	-- v_Business_Indicator=0,''
	-- )
	DECODE(TRUE,
	v_Business_Indicator = 1, v_UnderlyingPolicyKey_1_Cur,
	v_Business_Indicator = 2, v_UnderlyingPolicyKey_2_Cur,
	v_Business_Indicator = 0, '') AS v_Business_PolicyKey,
	-- *INF*: IIF(IN(SUBSTR(v_Business_PolicyKey,1,2),'BO','NA','NB'),'',v_Business_Company_Name)
	IFF(IN(SUBSTR(v_Business_PolicyKey, 1, 2), 'BO', 'NA', 'NB'), '', v_Business_Company_Name) AS o_BusinessOwners_Company_Name,
	-- *INF*: IIF(IN(SUBSTR(v_Business_PolicyKey,1,2),'BO','NA','NB'),'',v_Business_PolicyKey)
	IFF(IN(SUBSTR(v_Business_PolicyKey, 1, 2), 'BO', 'NA', 'NB'), '', v_Business_PolicyKey) AS o_BusinessOwners_PolicyKey,
	-- *INF*: DECODE(TRUE,
	-- INSTR(v_Text1_Prev,'GENERAL LIABILITY INSURANCE')>0,1,
	-- INSTR(v_Text2_Prev,'GENERAL LIABILITY INSURANCE')>0,2,
	-- 0)
	DECODE(TRUE,
	INSTR(v_Text1_Prev, 'GENERAL LIABILITY INSURANCE') > 0, 1,
	INSTR(v_Text2_Prev, 'GENERAL LIABILITY INSURANCE') > 0, 2,
	0) AS v_GL_Indicator,
	-- *INF*: DECODE(TRUE,
	-- v_GL_Indicator=1,v_UnderlyingInsurer_2_Prev,
	-- v_GL_Indicator=2,v_UnderlyingInsurer_1_Cur,
	-- v_GL_Indicator=0,''
	-- )
	DECODE(TRUE,
	v_GL_Indicator = 1, v_UnderlyingInsurer_2_Prev,
	v_GL_Indicator = 2, v_UnderlyingInsurer_1_Cur,
	v_GL_Indicator = 0, '') AS o_GL_Company_Name,
	-- *INF*: DECODE(TRUE,
	-- v_GL_Indicator=1,v_UnderlyingPolicyKey_1_Cur,
	-- v_GL_Indicator=2,v_UnderlyingPolicyKey_2_Cur,
	-- v_GL_Indicator=0,''
	-- )
	DECODE(TRUE,
	v_GL_Indicator = 1, v_UnderlyingPolicyKey_1_Cur,
	v_GL_Indicator = 2, v_UnderlyingPolicyKey_2_Cur,
	v_GL_Indicator = 0, '') AS o_GL_PolicyKey,
	-- *INF*: DECODE(TRUE,
	-- INSTR(v_Text1_Prev,'AUTOMOBILE LIABILITY INSURANCE')>0,1,
	-- INSTR(v_Text2_Prev,'AUTOMOBILE LIABILITY INSURANCE')>0,2,
	-- 0)
	DECODE(TRUE,
	INSTR(v_Text1_Prev, 'AUTOMOBILE LIABILITY INSURANCE') > 0, 1,
	INSTR(v_Text2_Prev, 'AUTOMOBILE LIABILITY INSURANCE') > 0, 2,
	0) AS v_CA_Indicator,
	-- *INF*: DECODE(TRUE,
	-- v_CA_Indicator=1,v_UnderlyingInsurer_2_Prev,
	-- v_CA_Indicator=2,v_UnderlyingInsurer_1_Cur,
	-- v_CA_Indicator=0,''
	-- )
	DECODE(TRUE,
	v_CA_Indicator = 1, v_UnderlyingInsurer_2_Prev,
	v_CA_Indicator = 2, v_UnderlyingInsurer_1_Cur,
	v_CA_Indicator = 0, '') AS o_CA_Company_Name,
	-- *INF*: DECODE(TRUE,
	-- v_CA_Indicator=1,v_UnderlyingPolicyKey_1_Cur,
	-- v_CA_Indicator=2,v_UnderlyingPolicyKey_2_Cur,
	-- v_CA_Indicator=0,''
	-- )
	DECODE(TRUE,
	v_CA_Indicator = 1, v_UnderlyingPolicyKey_1_Cur,
	v_CA_Indicator = 2, v_UnderlyingPolicyKey_2_Cur,
	v_CA_Indicator = 0, '') AS o_CA_PolicyKey,
	-- *INF*: IIF(SUBSTR(v_Business_PolicyKey,1,2)='BO',v_Business_Company_Name,'')
	IFF(SUBSTR(v_Business_PolicyKey, 1, 2) = 'BO', v_Business_Company_Name, '') AS o_SMARTBusiness_Company_Name,
	-- *INF*: IIF(SUBSTR(v_Business_PolicyKey,1,2)='BO',v_Business_PolicyKey,'')
	IFF(SUBSTR(v_Business_PolicyKey, 1, 2) = 'BO', v_Business_PolicyKey, '') AS o_SMARTBusiness_PolicyKey,
	-- *INF*: IIF(IN(SUBSTR(v_Business_PolicyKey,1,2),'NA','NB'),v_Business_Company_Name,'')
	IFF(IN(SUBSTR(v_Business_PolicyKey, 1, 2), 'NA', 'NB'), v_Business_Company_Name, '') AS o_SBOP_Company_Name,
	-- *INF*: IIF(IN(SUBSTR(v_Business_PolicyKey,1,2),'NA','NB'),v_Business_PolicyKey,'')
	IFF(IN(SUBSTR(v_Business_PolicyKey, 1, 2), 'NA', 'NB'), v_Business_PolicyKey, '') AS o_SBOP_PolicyKey,
	'N/A' AS o_UmbrellaCoverageScope,
	-- *INF*: TO_DATE('20140101','yyyymmdd')
	TO_DATE('20140101', 'yyyymmdd') AS o_RetroactiveDate
	FROM SQ_PifDept1553Stage
),
SQ_EDW AS (
	select distinct pt.PremiumTransactionID,
	sc.StatisticalCoverageHashKey,
	pol.pol_key,
	pt.PremiumTransactionHashKey,
	sc.MajorPerilSequenceNumber
	from @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction pt
	join @{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage sc
	on pt.StatisticalCoverageAKID=sc.StatisticalCoverageAKID
	and sc.SourceSystemID='PMS'
	and pt.SourceSystemID='PMS'
	join @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage pc
	on sc.PolicyCoverageAKID=pc.PolicyCoverageAKID
	and pc.SourceSystemID='PMS'
	join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol
	on pc.PolicyAKID=pol.pol_ak_id
	and pol.crrnt_snpsht_flag=1
	and left(pol.pol_sym,2) in ('CU','NU')
	and pol.source_sys_id='PMS'
	--------DEBUG-----------------------------
	--and pol.pol_key='CUA102598804'
	--------DEBUG-----------------------------
),
JNR_Umbrella AS (SELECT
	SQ_EDW.PremiumTransactionID, 
	SQ_EDW.StatisticalCoverageHashKey, 
	SQ_EDW.pol_key, 
	SQ_EDW.PremiumTransactionHashKey, 
	SQ_EDW.MajorPerilSequenceNumber, 
	EXP_GetValues.o_PolicyKey AS PolicyKey, 
	EXP_GetValues.o_EPLI_Company_Name AS EPLI_Company_Name, 
	EXP_GetValues.o_EPLI_PolicyKey AS EPLI_PolicyKey, 
	EXP_GetValues.o_BusinessOwners_Company_Name AS BusinessOwner_Company_Name, 
	EXP_GetValues.o_BusinessOwners_PolicyKey AS BusinessOwner_PolicyKey, 
	EXP_GetValues.o_GL_Company_Name AS GL_Company_Name, 
	EXP_GetValues.o_GL_PolicyKey AS GL_PolicyKey, 
	EXP_GetValues.o_CA_Company_Name AS CA_Company_Name, 
	EXP_GetValues.o_CA_PolicyKey AS CA_PolicyKey, 
	EXP_GetValues.o_SMARTBusiness_Company_Name AS SMARTBusiness_Company_Name, 
	EXP_GetValues.o_SMARTBusiness_PolicyKey AS SMARTBusiness_PolicyKey, 
	EXP_GetValues.o_SBOP_Company_Name AS SBOP_Company_Name, 
	EXP_GetValues.o_SBOP_PolicyKey AS SBOP_PolicyKey, 
	EXP_GetValues.o_UmbrellaCoverageScope AS UmbrellaCoverageScope, 
	EXP_GetValues.o_RetroactiveDate AS RetroactiveDate
	FROM SQ_EDW
	INNER JOIN EXP_GetValues
	ON EXP_GetValues.o_PolicyKey = SQ_EDW.pol_key
),
AGG_Umbrella AS (
	SELECT
	PremiumTransactionID, 
	StatisticalCoverageHashKey, 
	PremiumTransactionHashKey, 
	MajorPerilSequenceNumber, 
	EPLI_Company_Name AS i_EPLI_Company_Name, 
	MAX(i_EPLI_Company_Name) AS o_EPLI_Company_Name, 
	EPLI_PolicyKey AS i_EPLI_PolicyKey, 
	MAX(i_EPLI_PolicyKey) AS o_EPLI_PolicyKey, 
	BusinessOwner_Company_Name AS i_BusinessOwners_Company_Name, 
	MAX(i_BusinessOwners_Company_Name) AS o_BusinessOwners_Company_Name, 
	BusinessOwner_PolicyKey AS i_BusinessOwners_PolicyKey, 
	MAX(i_BusinessOwners_PolicyKey) AS o_BusinessOwners_PolicyKey, 
	GL_Company_Name AS i_GL_Company_Name, 
	MAX(i_GL_Company_Name) AS o_GL_Company_Name, 
	GL_PolicyKey AS i_GL_PolicyKey, 
	MAX(i_GL_PolicyKey) AS o_GL_PolicyKey, 
	CA_Company_Name AS i_CA_Company_Name, 
	MAX(i_CA_Company_Name) AS o_CA_Company_Name, 
	CA_PolicyKey AS i_CA_PolicyKey, 
	MAX(i_CA_PolicyKey) AS o_CA_PolicyKey, 
	SMARTBusiness_Company_Name AS i_SMARTBusiness_Company_Name, 
	MAX(i_SMARTBusiness_Company_Name) AS o_SMARTBusiness_Company_Name, 
	SMARTBusiness_PolicyKey AS i_SMARTBusiness_PolicyKey, 
	MAX(i_SMARTBusiness_PolicyKey) AS o_SMARTBusiness_PolicyKey, 
	SBOP_Company_Name AS i_SBOP_Company_Name, 
	MAX(i_SBOP_Company_Name) AS o_SBOP_Company_Name, 
	SBOP_PolicyKey AS i_SBOP_PolicyKey, 
	MAX(i_SBOP_PolicyKey) AS o_SBOP_PolicyKey, 
	UmbrellaCoverageScope, 
	RetroactiveDate
	FROM JNR_Umbrella
	GROUP BY PremiumTransactionID
),
EXP_DefaultValue AS (
	SELECT
	PremiumTransactionID AS i_PremiumTransactionID,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800/01/01 00:00:00','YYYY/MM/DD HH24:MI:SS')
	TO_DATE('1800/01/01 00:00:00', 'YYYY/MM/DD HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100/12/31 23:59:59','YYYY/MM/DD HH24:MI:SS')
	TO_DATE('2100/12/31 23:59:59', 'YYYY/MM/DD HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	StatisticalCoverageHashKey,
	MajorPerilSequenceNumber,
	o_EPLI_Company_Name AS i_EPLI_Company_Name,
	-- *INF*: IIF(i_EPLI_Company_Name='' OR i_EPLI_PolicyKey='','N/A',i_EPLI_Company_Name)
	IFF(i_EPLI_Company_Name = '' OR i_EPLI_PolicyKey = '', 'N/A', i_EPLI_Company_Name) AS o_EPLI_Company_Name,
	o_EPLI_PolicyKey AS i_EPLI_PolicyKey,
	-- *INF*: IIF(i_EPLI_PolicyKey='' OR i_EPLI_Company_Name='','N/A',REPLACESTR(0,i_EPLI_PolicyKey,' ',''))
	IFF(i_EPLI_PolicyKey = '' OR i_EPLI_Company_Name = '', 'N/A', REPLACESTR(0, i_EPLI_PolicyKey, ' ', '')) AS o_EPLI_PolicyKey,
	o_BusinessOwners_Company_Name AS i_BO_Company_Name,
	-- *INF*: IIF(i_BO_Company_Name='' OR i_BO_PolicyKey='','N/A',i_BO_Company_Name)
	IFF(i_BO_Company_Name = '' OR i_BO_PolicyKey = '', 'N/A', i_BO_Company_Name) AS o_BO_Company_Name,
	o_BusinessOwners_PolicyKey AS i_BO_PolicyKey,
	-- *INF*: IIF(i_BO_PolicyKey='' OR i_BO_Company_Name='','N/A',REPLACESTR(0,i_BO_PolicyKey,' ',''))
	IFF(i_BO_PolicyKey = '' OR i_BO_Company_Name = '', 'N/A', REPLACESTR(0, i_BO_PolicyKey, ' ', '')) AS o_BO_PolicyKey,
	o_GL_Company_Name AS i_GL_Company_Name,
	-- *INF*: IIF(i_GL_Company_Name='' OR i_GL_PolicyKey='','N/A',i_GL_Company_Name)
	IFF(i_GL_Company_Name = '' OR i_GL_PolicyKey = '', 'N/A', i_GL_Company_Name) AS o_GL_Company_Name,
	o_GL_PolicyKey AS i_GL_PolicyKey,
	-- *INF*: IIF(i_GL_PolicyKey='' OR i_GL_Company_Name='','N/A',REPLACESTR(0,i_GL_PolicyKey,' ',''))
	IFF(i_GL_PolicyKey = '' OR i_GL_Company_Name = '', 'N/A', REPLACESTR(0, i_GL_PolicyKey, ' ', '')) AS o_GL_PolicyKey,
	o_CA_Company_Name AS i_CA_Company_Name,
	-- *INF*: IIF(i_CA_Company_Name='' OR i_CA_PolicyKey='','N/A',i_CA_Company_Name)
	IFF(i_CA_Company_Name = '' OR i_CA_PolicyKey = '', 'N/A', i_CA_Company_Name) AS o_CA_Company_Name,
	o_CA_PolicyKey AS i_CA_PolicyKey,
	-- *INF*: IIF(i_CA_PolicyKey='' OR i_CA_Company_Name='','N/A',REPLACESTR(0,i_CA_PolicyKey,' ',''))
	IFF(i_CA_PolicyKey = '' OR i_CA_Company_Name = '', 'N/A', REPLACESTR(0, i_CA_PolicyKey, ' ', '')) AS o_CA_PolicyKey,
	o_SMARTBusiness_Company_Name AS i_SMARTBusiness_Company_Name,
	-- *INF*: IIF(i_SMARTBusiness_Company_Name='' OR i_SMARTBusiness_PolicyKey='','N/A',i_SMARTBusiness_Company_Name)
	IFF(i_SMARTBusiness_Company_Name = '' OR i_SMARTBusiness_PolicyKey = '', 'N/A', i_SMARTBusiness_Company_Name) AS o_SMARTBusiness_Company_Name,
	o_SMARTBusiness_PolicyKey AS i_SMARTBusiness_PolicyKey,
	-- *INF*: IIF(i_SMARTBusiness_PolicyKey='' OR i_SMARTBusiness_Company_Name='','N/A',REPLACESTR(0,i_SMARTBusiness_PolicyKey,' ',''))
	IFF(i_SMARTBusiness_PolicyKey = '' OR i_SMARTBusiness_Company_Name = '', 'N/A', REPLACESTR(0, i_SMARTBusiness_PolicyKey, ' ', '')) AS o_SMARTBusiness_PolicyKey,
	o_SBOP_Company_Name AS i_SBOP_Company_Name,
	-- *INF*: IIF(i_SBOP_Company_Name='' OR i_SBOP_PolicyKey='','N/A',i_SBOP_Company_Name)
	IFF(i_SBOP_Company_Name = '' OR i_SBOP_PolicyKey = '', 'N/A', i_SBOP_Company_Name) AS o_SBOP_Company_Name,
	o_SBOP_PolicyKey AS i_SBOP_PolicyKey,
	-- *INF*: IIF(i_SBOP_PolicyKey='' OR i_SBOP_Company_Name='','N/A',REPLACESTR(0,i_SBOP_PolicyKey,' ',''))
	IFF(i_SBOP_PolicyKey = '' OR i_SBOP_Company_Name = '', 'N/A', REPLACESTR(0, i_SBOP_PolicyKey, ' ', '')) AS o_SBOP_PolicyKey,
	UmbrellaCoverageScope,
	RetroactiveDate,
	-- *INF*: IIF(ISNULL(MajorPerilSequenceNumber) or MajorPerilSequenceNumber='N/A', -1, TO_INTEGER(MajorPerilSequenceNumber))
	IFF(MajorPerilSequenceNumber IS NULL OR MajorPerilSequenceNumber = 'N/A', - 1, TO_INTEGER(MajorPerilSequenceNumber)) AS v_UmbrellaLayer,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	v_UmbrellaLayer AS o_UmbrellaLayer
	FROM AGG_Umbrella
),
LKP_CoverageDetailCommercialUmbrella AS (
	SELECT
	PremiumTransactionID,
	CoverageGuid,
	UmbrellaCoverageScope,
	RetroactiveDate,
	UmbrellaLayer
	FROM (
		SELECT 
			PremiumTransactionID,
			CoverageGuid,
			UmbrellaCoverageScope,
			RetroactiveDate,
			UmbrellaLayer
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialUmbrella
		WHERE SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID) = 1
),
EXP_DetectChange AS (
	SELECT
	LKP_CoverageDetailCommercialUmbrella.PremiumTransactionID AS lkp_PremiumTransactionID,
	LKP_CoverageDetailCommercialUmbrella.CoverageGuid AS lkp_CoverageGuid,
	LKP_CoverageDetailCommercialUmbrella.UmbrellaCoverageScope AS lkp_UmbrellaCoverageScope,
	LKP_CoverageDetailCommercialUmbrella.RetroactiveDate AS lkp_RetroactiveDate,
	LKP_CoverageDetailCommercialUmbrella.UmbrellaLayer AS lkp_UmbrellaLayer,
	EXP_DefaultValue.o_PremiumTransactionID AS PremiumTransactionID,
	EXP_DefaultValue.o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	EXP_DefaultValue.o_AuditID AS AuditID,
	EXP_DefaultValue.o_EffectiveDate AS EffectiveDate,
	EXP_DefaultValue.o_ExpirationDate AS ExpirationDate,
	EXP_DefaultValue.o_SourceSystemID AS SourceSystemID,
	EXP_DefaultValue.o_CreatedDate AS CreatedDate,
	EXP_DefaultValue.o_ModifiedDate AS ModifiedDate,
	EXP_DefaultValue.StatisticalCoverageHashKey AS CoverageGuid,
	EXP_DefaultValue.o_EPLI_Company_Name AS UmbrellaEmployersLiabilityUnderlyingInsuranceCompanyName,
	EXP_DefaultValue.o_EPLI_PolicyKey AS UmbrellaEmployersLiabilityUnderlyingInsurancePolicyKey,
	EXP_DefaultValue.o_BO_Company_Name AS UmbrellaBusinessOwnersUnderlyingInsuranceCompanyName,
	EXP_DefaultValue.o_BO_PolicyKey AS UmbrellaBusinessOwnersUnderlyingInsurancePolicyKey,
	EXP_DefaultValue.o_GL_Company_Name AS UmbrellaGeneralLiabilityUnderlyingInsuranceCompanyName,
	EXP_DefaultValue.o_GL_PolicyKey AS UmbrellaGeneralLiabilityUnderlyingInsurancePolicyKey,
	EXP_DefaultValue.o_CA_Company_Name AS UmbrellaCommercialAutoUnderlyingInsuranceCompanyName,
	EXP_DefaultValue.o_CA_PolicyKey AS UmbrellaCommercialAutoUnderlyingInsurancePolicyKey,
	EXP_DefaultValue.o_SMARTBusiness_Company_Name AS UmbrellaSMARTbusinessUnderlyingInsuranceCompanyName,
	EXP_DefaultValue.o_SMARTBusiness_PolicyKey AS UmbrellaSMARTbusinessUnderlyingInsurancePolicyKey,
	EXP_DefaultValue.o_SBOP_Company_Name AS UmbrellaSBOPUnderlyingInsuranceCompanyName,
	EXP_DefaultValue.o_SBOP_PolicyKey AS UmbrellaSBOPUnderlyingInsurancePolicyKey,
	EXP_DefaultValue.UmbrellaCoverageScope,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_PremiumTransactionID), 'New', 
	-- lkp_CoverageGuid<>CoverageGuid
	-- or lkp_UmbrellaCoverageScope<>UmbrellaCoverageScope OR lkp_RetroactiveDate != RetroactiveDate
	-- or lkp_UmbrellaLayer<>UmbrellaLayer, 'Update',
	-- 'No Change'
	-- ) 
	-- 
	DECODE(TRUE,
	lkp_PremiumTransactionID IS NULL, 'New',
	lkp_CoverageGuid <> CoverageGuid OR lkp_UmbrellaCoverageScope <> UmbrellaCoverageScope OR lkp_RetroactiveDate != RetroactiveDate OR lkp_UmbrellaLayer <> UmbrellaLayer, 'Update',
	'No Change') AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag,
	EXP_DefaultValue.RetroactiveDate,
	EXP_DefaultValue.o_UmbrellaLayer AS UmbrellaLayer
	FROM EXP_DefaultValue
	LEFT JOIN LKP_CoverageDetailCommercialUmbrella
	ON LKP_CoverageDetailCommercialUmbrella.PremiumTransactionID = EXP_DefaultValue.o_PremiumTransactionID
),
RTR_InsertElseUpdate AS (
	SELECT
	lkp_PremiumTransactionID,
	PremiumTransactionID,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	CoverageGuid,
	UmbrellaEmployersLiabilityUnderlyingInsuranceCompanyName,
	UmbrellaEmployersLiabilityUnderlyingInsurancePolicyKey,
	UmbrellaBusinessOwnersUnderlyingInsuranceCompanyName,
	UmbrellaBusinessOwnersUnderlyingInsurancePolicyKey,
	UmbrellaGeneralLiabilityUnderlyingInsuranceCompanyName,
	UmbrellaGeneralLiabilityUnderlyingInsurancePolicyKey,
	UmbrellaCommercialAutoUnderlyingInsuranceCompanyName,
	UmbrellaCommercialAutoUnderlyingInsurancePolicyKey,
	UmbrellaSMARTbusinessUnderlyingInsuranceCompanyName,
	UmbrellaSMARTbusinessUnderlyingInsurancePolicyKey,
	UmbrellaSBOPUnderlyingInsuranceCompanyName,
	UmbrellaSBOPUnderlyingInsurancePolicyKey,
	UmbrellaCoverageScope,
	o_ChangeFlag AS ChangeFlag,
	RetroactiveDate,
	UmbrellaLayer
	FROM EXP_DetectChange
),
RTR_InsertElseUpdate_INSERT AS (SELECT * FROM RTR_InsertElseUpdate WHERE ChangeFlag='New'),
RTR_InsertElseUpdate_UPDATE AS (SELECT * FROM RTR_InsertElseUpdate WHERE ChangeFlag='Update'),
TGT_CoverageDetailCommercialUmbrella_Insert AS (
	INSERT INTO CoverageDetailCommercialUmbrella
	(PremiumTransactionID, CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CoverageGuid, UmbrellaCoverageScope, RetroactiveDate, UmbrellaLayer)
	SELECT 
	PREMIUMTRANSACTIONID, 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	COVERAGEGUID, 
	UMBRELLACOVERAGESCOPE, 
	RETROACTIVEDATE, 
	UMBRELLALAYER
	FROM RTR_InsertElseUpdate_INSERT
),
UPD_UpdateData AS (
	SELECT
	lkp_PremiumTransactionID AS PremiumTransactionID, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	CoverageGuid, 
	UmbrellaEmployersLiabilityUnderlyingInsuranceCompanyName, 
	UmbrellaEmployersLiabilityUnderlyingInsurancePolicyKey, 
	UmbrellaBusinessOwnersUnderlyingInsuranceCompanyName, 
	UmbrellaBusinessOwnersUnderlyingInsurancePolicyKey, 
	UmbrellaGeneralLiabilityUnderlyingInsuranceCompanyName, 
	UmbrellaGeneralLiabilityUnderlyingInsurancePolicyKey, 
	UmbrellaCommercialAutoUnderlyingInsuranceCompanyName, 
	UmbrellaCommercialAutoUnderlyingInsurancePolicyKey, 
	UmbrellaSMARTbusinessUnderlyingInsuranceCompanyName, 
	UmbrellaSMARTbusinessUnderlyingInsurancePolicyKey, 
	UmbrellaSBOPUnderlyingInsuranceCompanyName, 
	UmbrellaSBOPUnderlyingInsurancePolicyKey, 
	UmbrellaCoverageScope, 
	RetroactiveDate, 
	UmbrellaLayer
	FROM RTR_InsertElseUpdate_UPDATE
),
TGT_CoverageDetailCommercialUmbrella_Update AS (
	MERGE INTO CoverageDetailCommercialUmbrella AS T
	USING UPD_UpdateData AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.CoverageGuid = S.CoverageGuid, T.UmbrellaCoverageScope = S.UmbrellaCoverageScope, T.RetroactiveDate = S.RetroactiveDate, T.UmbrellaLayer = S.UmbrellaLayer
),