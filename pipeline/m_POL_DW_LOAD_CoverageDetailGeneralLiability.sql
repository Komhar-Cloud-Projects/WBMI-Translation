WITH
LKP_SupClassificationGeneralLiability AS (
	SELECT
	lkp_result,
	ClassCode,
	SublineCode,
	RatingStateCode
	FROM (
		SELECT ClassCode as ClassCode,
		SublineCode as SublineCode,
		RatingStateCode as RatingStateCode,
		ISOGeneralLiabilityClassSummary+'@1'
		       +ISOGeneralLiabilityClassGroupCode+'@2'
			     as lkp_result
		  FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationGeneralLiability
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode,SublineCode,RatingStateCode ORDER BY lkp_result) = 1
),
SQ_PMS AS (
	select PT.PremiumTransactionID AS PremiumTransactionID
	,SC.StatisticalCoverageHashKey AS StatisticalCoverageHashKey
	,case when SC.Riskunitgroup='286' then SC.RiskUnit else SC.ClassCode end ClassCode
	,SC.SublineCode AS SublineCode
	,rl.StateProvinceCode as StateCode
	,SC.ClassCodeOrganizationCode AS ClassCodeOrganizationCode
	,P.pol_sym AS pol_sym
	,P.pol_num AS pol_num
	,P.pol_mod AS pol_mod
	,SC.RiskUnitGroup AS RiskUnitGroup
	,PT.PremiumTransactionAKID AS PremiumTransactionAKID
	from @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT
	INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage SC
	ON
	PT.StatisticalCoverageAKID = SC.StatisticalCoverageAKID AND PT.SourceSystemID = 'PMS' AND SC.SourceSystemID = 'PMS' 
	--AND SC.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC
	ON
	SC.PolicyCoverageAKID = PC.PolicyCoverageAKID AND PC.SourceSystemID = 'PMS' --AND PC.CurrentSnapshotFlag=1
	INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy P
	ON
	PC.PolicyAKID = P.pol_ak_id AND P.source_sys_id = 'PMS'
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL 
	on RL.RiskLocationAKID = PC.RiskLocationAKID
	and RL.CurrentSnapshotFlag=1
	WHERE PT.CreatedDate >= '@{pipeline().parameters.SELECTION_START_TS}' 
	AND PC.InsuranceLine ='GL' AND P.crrnt_snpsht_flag = 1
	AND exists (select 1 from @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction WPT where WPT.PremiumTransactionAKID=PT.PremiumTransactionAKID)
),
EXP_Metadata AS (
	SELECT
	PremiumTransactionID AS i_PremiumTransactionID,
	StatisticalCoverageHashKey AS i_StatisticalCoverageHashKey,
	ClassCode AS i_ClassCode,
	SublineCode AS i_SublineCode,
	StateCode AS i_RatingStateCode,
	OriginatingOrganizationCode AS i_OriginatingOrganizationCode,
	pol_sym AS i_pol_sym,
	pol_num AS i_pol_num,
	pol_mod AS i_pol_mod,
	RiskUnitGroup AS i_RiskUnitGroup,
	PremiumTransactionAKID AS i_PremiumTransactionAKID,
	-- *INF*: RTRIM(LTRIM(i_ClassCode))
	RTRIM(LTRIM(i_ClassCode)) AS v_ClassCode,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	i_PremiumTransactionAKID AS o_PremiumTransactionAKID,
	-- *INF*: RTRIM(LTRIM(i_StatisticalCoverageHashKey))
	RTRIM(LTRIM(i_StatisticalCoverageHashKey)) AS o_StatisticalCoverageHashKey,
	-- *INF*: IIF(IN(v_ClassCode,'22222','22250'),1,0)
	IFF(IN(v_ClassCode, '22222', '22250'), 1, 0) AS o_EmploymentPracticesLiabilityInsuranceRollOnIndicator,
	-- *INF*: RTRIM(LTRIM(i_pol_sym))
	RTRIM(LTRIM(i_pol_sym)) AS o_pol_sym,
	-- *INF*: RTRIM(LTRIM(i_pol_num))
	RTRIM(LTRIM(i_pol_num)) AS o_pol_num,
	-- *INF*: RTRIM(LTRIM(i_pol_mod))
	RTRIM(LTRIM(i_pol_mod)) AS o_pol_mod,
	-- *INF*: RTRIM(LTRIM(i_RiskUnitGroup))
	RTRIM(LTRIM(i_RiskUnitGroup)) AS o_RiskUnitGroup,
	v_ClassCode AS o_ClassCode,
	-- *INF*: RTRIM(LTRIM(i_SublineCode))
	RTRIM(LTRIM(i_SublineCode)) AS o_SublineCode,
	-- *INF*: RTRIM(LTRIM(i_RatingStateCode))
	RTRIM(LTRIM(i_RatingStateCode)) AS o_RatingStateCode,
	-- *INF*: LTRIM(RTRIM(i_OriginatingOrganizationCode))
	LTRIM(RTRIM(i_OriginatingOrganizationCode)) AS o_OriginatingOrganizationCode
	FROM SQ_PMS
),
LKP_Pif43LXGLStage AS (
	SELECT
	Pmdlxg1YearRetro,
	Pmdlxg1MonthRetro,
	Pmdlxg1DayRetro,
	PifSymbol,
	PifPolicyNumber,
	PifModule
	FROM (
		SELECT 
			Pmdlxg1YearRetro,
			Pmdlxg1MonthRetro,
			Pmdlxg1DayRetro,
			PifSymbol,
			PifPolicyNumber,
			PifModule
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43LXGLStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule ORDER BY Pmdlxg1YearRetro) = 1
),
LKP_Pif_4514 AS (
	SELECT
	PremiumTransactionAKId,
	sar_code_7,
	o_PremiumTransactionAKID
	FROM (
		select PT.PremiumTransactionAKId as PremiumTransactionAKId , STG.sar_code_7  as sar_code_7
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction PT
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_4514_stage STG on STG.pif_4514_stage_id=PT.PremiumTransactionStageId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId ORDER BY PremiumTransactionAKId) = 1
),
LKP_pifDEPT1553Stage_EPLI AS (
	SELECT
	RetroDate,
	i_pol_sym,
	i_pol_num,
	i_pol_mod,
	PifSymbol,
	PifPolicyNumber,
	PifModule
	FROM (
		select PifSymbol as PifSymbol,
		PifPolicyNumber as PifPolicyNumber,
		PifModule as PifModule,
		ltrim(rtrim(
		case 
			 when charindex('ITEM  5.  PENDING OR PRIOR LITIGATION DATE:',DECLPTText1701)>0  then SUBSTRING(DECLPTText1701,charindex('ITEM  5.  PENDING OR PRIOR LITIGATION DATE:',DECLPTText1701)+43,len(DECLPTText1701)-43)
			 when CHARINDEX('',DECLPTText1701) > 0 AND charindex('RETRO DATE:',DECLPTText1701)>0 then SUBSTRING(SourceValue,CHARINDEX('@',DECLPTText1701)+1, CHARINDEX('',DECLPTText1701)-CHARINDEX('@',DECLPTText1701)-1)
		     when CHARINDEX('',SourceValue) > 0 AND charindex('SHOWN HERE',SourceValue )>0 then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('SHOWN HERE',SourceValue))+1, CHARINDEX('',SourceValue,CHARINDEX('@',SourceValue,charindex('SHOWN HERE',SourceValue)))-CHARINDEX('@',SourceValue,charindex('SHOWN HERE',SourceValue))-1)
			 when CHARINDEX('^',DECLPTText1701) > 0 AND charindex('RETRO DATE:',DECLPTText1701)>0 then SUBSTRING(SourceValue,CHARINDEX('@',DECLPTText1701)+1, CHARINDEX('^',DECLPTText1701)-CHARINDEX('@',DECLPTText1701)-1)
		     when CHARINDEX('^',SourceValue)>0 AND  charindex('SHOWN HERE',SourceValue )>0 then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('SHOWN HERE',SourceValue))+1, CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('SHOWN HERE',SourceValue)))-CHARINDEX('@',SourceValue,charindex('SHOWN HERE',SourceValue))-1)
			 else 'N/A' end))	 as RetroDate
		from (
		select PifSymbol,
		PifPolicyNumber,
		PifModule,
		ltrim(rtrim(DECLPTText1701))+' '+ltrim(rtrim(DECLPTText71791))+ltrim(rtrim(DECLPTText1702))+' '+ltrim(rtrim(DECLPTText71792)) as SourceValue,
		ltrim(rtrim(DECLPTText1701)) as DECLPTText1701
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PifDept1553Stage dept1553
		where DECLPTFormNumber in ('CPEPL01','CPEPL02','CPEPL03','CPEPL04','CPNFP02','NSDOA01','CPAPO01','CPAPO02','CPAPY01','CPFEA01','CPMYC01','CPNFE01','CPNFP01','ENSEF01','GLEXT01','GLEXT02')
		and not exists (
		select 1
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PifDept1553Stage
		where dept1553.PifSymbol=PifDept1553Stage.PifSymbol
		and dept1553.PifPolicyNumber=PifDept1553Stage.PifPolicyNumber
		and dept1553.PifModule=PifDept1553Stage.PifModule
		and dept1553.DECLPTSeq0098=PifDept1553Stage.DECLPTSeq0098
		and dept1553.DECLPTSeqSameForm=PifDept1553Stage.DECLPTSeqSameForm
		and dept1553.PifDept1553StageId<PifDept1553Stage.PifDept1553StageId)
		
		union all
		select 'N/A','N/A','N/A','N/A','N/A'
		)comment
		WHERE (charindex('SHOWN HERE',SourceValue )>0
		OR charindex('ITEM  5.  PENDING OR PRIOR LITIGATION DATE:',DECLPTText1701)>0
		OR charindex('RETRO DATE:',DECLPTText1701)>0)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule ORDER BY RetroDate) = 1
),
EXP_CoverageDetailGeneralLiabiliity AS (
	SELECT
	EXP_Metadata.o_PremiumTransactionID AS i_PremiumTransactionID,
	EXP_Metadata.o_StatisticalCoverageHashKey AS i_StatisticalCoverageHashKey,
	LKP_Pif43LXGLStage.Pmdlxg1YearRetro AS i_Pmdlxg1YearRetro,
	LKP_Pif43LXGLStage.Pmdlxg1MonthRetro AS i_Pmdlxg1MonthRetro,
	LKP_Pif43LXGLStage.Pmdlxg1DayRetro AS i_Pmdlxg1DayRetro,
	EXP_Metadata.o_EmploymentPracticesLiabilityInsuranceRollOnIndicator AS i_EmploymentPracticesLiabilityInsuranceRollOnIndicator,
	LKP_pifDEPT1553Stage_EPLI.RetroDate AS i_RetroDate_EPLI,
	EXP_Metadata.o_RiskUnitGroup AS i_RiskUnitGroup,
	LKP_Pif_4514.sar_code_7 AS i_sar_code_7,
	EXP_Metadata.o_ClassCode AS i_ClassCode,
	EXP_Metadata.o_SublineCode AS i_SublineCode,
	EXP_Metadata.o_RatingStateCode AS i_RatingStateCode,
	EXP_Metadata.o_OriginatingOrganizationCode AS i_OriginatingOrganizationCode,
	-- *INF*: DECODE(true,
	-- NOT ISNULL(:LKP.LKP_SupClassificationGeneralLiability(i_ClassCode,i_SublineCode,i_RatingStateCode)),:LKP.LKP_SupClassificationGeneralLiability(i_ClassCode,i_SublineCode,i_RatingStateCode),
	-- NOT ISNULL(:LKP.LKP_SupClassificationGeneralLiability(i_ClassCode,i_SublineCode,'99')),:LKP.LKP_SupClassificationGeneralLiability(i_ClassCode,i_SublineCode,'99'),
	-- 'N/A')
	DECODE(true,
		NOT LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_i_RatingStateCode.lkp_result IS NULL, LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_i_RatingStateCode.lkp_result,
		NOT LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_99.lkp_result IS NULL, LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_99.lkp_result,
		'N/A') AS v_lkp_result,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_Pmdlxg1YearRetro),1800,
	-- i_Pmdlxg1YearRetro=0,1800,
	-- TO_INTEGER(i_Pmdlxg1YearRetro)
	-- )
	-- --IIF(ISNULL(i_Pmdlxg1YearRetro),1800,TO_INTEGER(i_Pmdlxg1YearRetro))
	DECODE(TRUE,
		i_Pmdlxg1YearRetro IS NULL, 1800,
		i_Pmdlxg1YearRetro = 0, 1800,
		TO_INTEGER(i_Pmdlxg1YearRetro)) AS v_Pmdlxg1YearRetro,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_Pmdlxg1MonthRetro),1,
	-- i_Pmdlxg1MonthRetro=0,1,
	-- TO_INTEGER(i_Pmdlxg1MonthRetro)
	-- )
	-- 
	-- 
	-- --IIF(ISNULL(i_Pmdlxg1MonthRetro),1,TO_INTEGER(i_Pmdlxg1MonthRetro))
	DECODE(TRUE,
		i_Pmdlxg1MonthRetro IS NULL, 1,
		i_Pmdlxg1MonthRetro = 0, 1,
		TO_INTEGER(i_Pmdlxg1MonthRetro)) AS v_Pmdlxg1MonthRetro,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_Pmdlxg1DayRetro),1,
	-- i_Pmdlxg1DayRetro=0,1,
	-- TO_INTEGER(i_Pmdlxg1DayRetro)
	-- )
	-- 
	-- --IIF(ISNULL(i_Pmdlxg1DayRetro),1,TO_INTEGER(i_Pmdlxg1DayRetro))
	DECODE(TRUE,
		i_Pmdlxg1DayRetro IS NULL, 1,
		i_Pmdlxg1DayRetro = 0, 1,
		TO_INTEGER(i_Pmdlxg1DayRetro)) AS v_Pmdlxg1DayRetro,
	-- *INF*: REPLACESTR(0,LTRIM(RTRIM(i_RetroDate_EPLI)),' ','')
	REPLACESTR(0, LTRIM(RTRIM(i_RetroDate_EPLI)), ' ', '') AS v_RetroDate_EPLI_Trim,
	-- *INF*: DECODE(TRUE,
	-- INSTR(v_RetroDate_EPLI_Trim,'/')>0 AND INSTR(v_RetroDate_EPLI_Trim,'-')=0,'/',
	-- INSTR(v_RetroDate_EPLI_Trim,'/')=0 AND INSTR(v_RetroDate_EPLI_Trim,'-')>0,'-',
	-- NULL)
	DECODE(TRUE,
		INSTR(v_RetroDate_EPLI_Trim, '/') > 0 AND INSTR(v_RetroDate_EPLI_Trim, '-') = 0, '/',
		INSTR(v_RetroDate_EPLI_Trim, '/') = 0 AND INSTR(v_RetroDate_EPLI_Trim, '-') > 0, '-',
		NULL) AS v_DateDelimiter,
	-- *INF*: IIF(
	-- ISNULL(v_DateDelimiter),v_RetroDate_EPLI_Trim,
	-- LPAD(SUBSTR(v_RetroDate_EPLI_Trim,1,INSTR(v_RetroDate_EPLI_Trim,v_DateDelimiter)-1),2,'0')
	--  || LPAD(SUBSTR(v_RetroDate_EPLI_Trim,INSTR(v_RetroDate_EPLI_Trim,v_DateDelimiter)+1,INSTR(v_RetroDate_EPLI_Trim,v_DateDelimiter,1,2)-INSTR(v_RetroDate_EPLI_Trim,v_DateDelimiter)-1),2,'0')
	--  || SUBSTR(v_RetroDate_EPLI_Trim,INSTR(v_RetroDate_EPLI_Trim,v_DateDelimiter,1,2)+1)
	-- )
	IFF(v_DateDelimiter IS NULL, v_RetroDate_EPLI_Trim, LPAD(SUBSTR(v_RetroDate_EPLI_Trim, 1, INSTR(v_RetroDate_EPLI_Trim, v_DateDelimiter) - 1), 2, '0') || LPAD(SUBSTR(v_RetroDate_EPLI_Trim, INSTR(v_RetroDate_EPLI_Trim, v_DateDelimiter) + 1, INSTR(v_RetroDate_EPLI_Trim, v_DateDelimiter, 1, 2) - INSTR(v_RetroDate_EPLI_Trim, v_DateDelimiter) - 1), 2, '0') || SUBSTR(v_RetroDate_EPLI_Trim, INSTR(v_RetroDate_EPLI_Trim, v_DateDelimiter, 1, 2) + 1)) AS v_RetroDate_EPLI_Cleasing,
	-- *INF*: DECODE(TRUE,
	-- IS_DATE(v_RetroDate_EPLI_Cleasing,'MMDDYYYY') AND LENGTH(v_RetroDate_EPLI_Cleasing)=8 and SUBSTR(v_RetroDate_EPLI_Cleasing,5,4)>'1753',TO_DATE(v_RetroDate_EPLI_Cleasing,'MMDDYYYY'),
	-- IS_DATE(v_RetroDate_EPLI_Cleasing,'MMDDRR') AND LENGTH(v_RetroDate_EPLI_Cleasing)=6,TO_DATE(v_RetroDate_EPLI_Cleasing,'MMDDRR'),
	-- IS_DATE(v_RetroDate_EPLI_Cleasing,'MONTHDD,YYYY') AND INSTR(v_RetroDate_EPLI_Cleasing,',')>0,TO_DATE(v_RetroDate_EPLI_Cleasing,'MONTHDD,YYYY'),
	-- IS_DATE(v_RetroDate_EPLI_Cleasing,'MONTHDDYYYY'),TO_DATE(v_RetroDate_EPLI_Cleasing,'MONTHDDYYYY'),
	-- TO_DATE('01011800','MMDDYYYY')
	-- )
	DECODE(TRUE,
		IS_DATE(v_RetroDate_EPLI_Cleasing, 'MMDDYYYY') AND LENGTH(v_RetroDate_EPLI_Cleasing) = 8 AND SUBSTR(v_RetroDate_EPLI_Cleasing, 5, 4) > '1753', TO_DATE(v_RetroDate_EPLI_Cleasing, 'MMDDYYYY'),
		IS_DATE(v_RetroDate_EPLI_Cleasing, 'MMDDRR') AND LENGTH(v_RetroDate_EPLI_Cleasing) = 6, TO_DATE(v_RetroDate_EPLI_Cleasing, 'MMDDRR'),
		IS_DATE(v_RetroDate_EPLI_Cleasing, 'MONTHDD,YYYY') AND INSTR(v_RetroDate_EPLI_Cleasing, ',') > 0, TO_DATE(v_RetroDate_EPLI_Cleasing, 'MONTHDD,YYYY'),
		IS_DATE(v_RetroDate_EPLI_Cleasing, 'MONTHDDYYYY'), TO_DATE(v_RetroDate_EPLI_Cleasing, 'MONTHDDYYYY'),
		TO_DATE('01011800', 'MMDDYYYY')) AS v_RetroDate_EPLI,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_sar_code_7)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_sar_code_7) AS v_sar_code_7,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	i_StatisticalCoverageHashKey AS o_StatisticalCoverageHashKey,
	-- *INF*: IIF(IN(i_RiskUnitGroup,'366','901','286','287','903','902'),v_RetroDate_EPLI,
	-- MAKE_DATE_TIME(v_Pmdlxg1YearRetro,v_Pmdlxg1MonthRetro,v_Pmdlxg1DayRetro))
	IFF(IN(i_RiskUnitGroup, '366', '901', '286', '287', '903', '902'), v_RetroDate_EPLI, MAKE_DATE_TIME(v_Pmdlxg1YearRetro, v_Pmdlxg1MonthRetro, v_Pmdlxg1DayRetro)) AS o_RetroactiveDate,
	i_EmploymentPracticesLiabilityInsuranceRollOnIndicator AS o_EmploymentPracticesLiabilityInsuranceRollOnIndicator,
	-- *INF*: IIF(v_sar_code_7 = 'N/A','N/A',SUBSTR(v_sar_code_7,1,1))
	IFF(v_sar_code_7 = 'N/A', 'N/A', SUBSTR(v_sar_code_7, 1, 1)) AS o_LiabilityFormCode,
	-- *INF*: IIF(ISNULL(SUBSTR(v_lkp_result,1,instr(v_lkp_result,'@1')-1)) OR LENGTH(SUBSTR(v_lkp_result,1,instr(v_lkp_result,'@1')-1))=0 ,'N/A' , SUBSTR(v_lkp_result,1,instr(v_lkp_result,'@1')-1))
	IFF(SUBSTR(v_lkp_result, 1, instr(v_lkp_result, '@1') - 1) IS NULL OR LENGTH(SUBSTR(v_lkp_result, 1, instr(v_lkp_result, '@1') - 1)) = 0, 'N/A', SUBSTR(v_lkp_result, 1, instr(v_lkp_result, '@1') - 1)) AS o_ISOGeneralLiabilityClassSummary,
	-- *INF*: IIF(ISNULL(SUBSTR(v_lkp_result,instr(v_lkp_result,'@1')+2,instr(v_lkp_result,'@2')-instr(v_lkp_result,'@1')-2))  
	-- OR LENGTH(SUBSTR(v_lkp_result,instr(v_lkp_result,'@1')+2,instr(v_lkp_result,'@2')-instr(v_lkp_result,'@1')-2))=0
	-- ,'N/A'
	-- ,SUBSTR(v_lkp_result,instr(v_lkp_result,'@1')+2,instr(v_lkp_result,'@2')-instr(v_lkp_result,'@1')-2))
	IFF(SUBSTR(v_lkp_result, instr(v_lkp_result, '@1') + 2, instr(v_lkp_result, '@2') - instr(v_lkp_result, '@1') - 2) IS NULL OR LENGTH(SUBSTR(v_lkp_result, instr(v_lkp_result, '@1') + 2, instr(v_lkp_result, '@2') - instr(v_lkp_result, '@1') - 2)) = 0, 'N/A', SUBSTR(v_lkp_result, instr(v_lkp_result, '@1') + 2, instr(v_lkp_result, '@2') - instr(v_lkp_result, '@1') - 2)) AS o_ISOGeneralLiabilityClassGroupCode
	FROM EXP_Metadata
	LEFT JOIN LKP_Pif43LXGLStage
	ON LKP_Pif43LXGLStage.PifSymbol = EXP_Metadata.o_pol_sym AND LKP_Pif43LXGLStage.PifPolicyNumber = EXP_Metadata.o_pol_num AND LKP_Pif43LXGLStage.PifModule = EXP_Metadata.o_pol_mod
	LEFT JOIN LKP_Pif_4514
	ON LKP_Pif_4514.PremiumTransactionAKId = EXP_Metadata.o_PremiumTransactionAKID
	LEFT JOIN LKP_pifDEPT1553Stage_EPLI
	ON LKP_pifDEPT1553Stage_EPLI.PifSymbol = EXP_Metadata.o_pol_sym AND LKP_pifDEPT1553Stage_EPLI.PifPolicyNumber = EXP_Metadata.o_pol_num AND LKP_pifDEPT1553Stage_EPLI.PifModule = EXP_Metadata.o_pol_mod
	LEFT JOIN LKP_SUPCLASSIFICATIONGENERALLIABILITY LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_i_RatingStateCode
	ON LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_i_RatingStateCode.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_i_RatingStateCode.SublineCode = i_SublineCode
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_i_RatingStateCode.RatingStateCode = i_RatingStateCode

	LEFT JOIN LKP_SUPCLASSIFICATIONGENERALLIABILITY LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_99
	ON LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_99.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_99.SublineCode = i_SublineCode
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_99.RatingStateCode = '99'

),
LKP_CoverageDetailGeneralLiability AS (
	SELECT
	PremiumTransactionID,
	RetroactiveDate,
	LiabilityFormCode,
	ISOGeneralLiabilityClassSummary,
	ISOGeneralLiabilityClassGroupCode
	FROM (
		SELECT 
			PremiumTransactionID,
			RetroactiveDate,
			LiabilityFormCode,
			ISOGeneralLiabilityClassSummary,
			ISOGeneralLiabilityClassGroupCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailGeneralLiability
		WHERE SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and PremiumTransactionID  in (select pt.PremiumTransactionID from
		PremiumTransaction pt
		inner join WorkPremiumTransaction wpt
		on pt.PremiumTransactionAKID=wpt.PremiumTransactionAKId)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_CoverageDetailGeneralLiability.PremiumTransactionID AS lkp_PremiumTransactionID,
	LKP_CoverageDetailGeneralLiability.RetroactiveDate AS lkp_RetroactiveDate,
	LKP_CoverageDetailGeneralLiability.LiabilityFormCode AS lkp_LiabilityFormCode,
	LKP_CoverageDetailGeneralLiability.ISOGeneralLiabilityClassSummary AS lkp_ClassSummary,
	LKP_CoverageDetailGeneralLiability.ISOGeneralLiabilityClassGroupCode AS lkp_ClassGroupCode,
	EXP_CoverageDetailGeneralLiabiliity.o_PremiumTransactionID AS i_PremiumTransactionID,
	EXP_CoverageDetailGeneralLiabiliity.o_StatisticalCoverageHashKey AS i_StatisticalCoverageHashKey,
	EXP_CoverageDetailGeneralLiabiliity.o_RetroactiveDate AS i_RetroactiveDate,
	EXP_CoverageDetailGeneralLiabiliity.o_LiabilityFormCode AS i_LiabilityFormCode,
	EXP_CoverageDetailGeneralLiabiliity.o_ISOGeneralLiabilityClassSummary AS i_ISOGeneralLiabilityClassSummary,
	EXP_CoverageDetailGeneralLiabiliity.o_ISOGeneralLiabilityClassGroupCode AS i_ISOGeneralLiabilityClassGroupCode,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.US')
	TO_DATE('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.US') AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.US')
	TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.US') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_StatisticalCoverageHashKey AS o_StatisticalCoverageHashKey,
	i_RetroactiveDate AS o_RetroactiveDate,
	i_LiabilityFormCode AS o_LiabilityFormCode,
	-- *INF*: RTRIM(LTRIM(i_ISOGeneralLiabilityClassSummary))
	RTRIM(LTRIM(i_ISOGeneralLiabilityClassSummary)) AS o_ISOGeneralLiabilityClassSummary,
	-- *INF*: RTRIM(LTRIM(i_ISOGeneralLiabilityClassGroupCode))
	RTRIM(LTRIM(i_ISOGeneralLiabilityClassGroupCode)) AS o_ISOGeneralLiabilityClassGroupCode,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_PremiumTransactionID),'NEW',
	--  lkp_RetroactiveDate != i_RetroactiveDate 
	-- OR lkp_LiabilityFormCode != i_LiabilityFormCode 
	-- OR lkp_ClassSummary != i_ISOGeneralLiabilityClassSummary 
	-- OR lkp_ClassGroupCode != i_ISOGeneralLiabilityClassGroupCode 
	-- ,'UPDATE',
	-- 'NOCHANGE'
	-- )
	DECODE(TRUE,
		lkp_PremiumTransactionID IS NULL, 'NEW',
		lkp_RetroactiveDate != i_RetroactiveDate OR lkp_LiabilityFormCode != i_LiabilityFormCode OR lkp_ClassSummary != i_ISOGeneralLiabilityClassSummary OR lkp_ClassGroupCode != i_ISOGeneralLiabilityClassGroupCode, 'UPDATE',
		'NOCHANGE') AS o_ChangeFlag
	FROM EXP_CoverageDetailGeneralLiabiliity
	LEFT JOIN LKP_CoverageDetailGeneralLiability
	ON LKP_CoverageDetailGeneralLiability.PremiumTransactionID = EXP_CoverageDetailGeneralLiabiliity.o_PremiumTransactionID
),
RTR_INSERT_UPDATE AS (
	SELECT
	o_PremiumTransactionID AS PremiumTransactionID,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditID AS AuditID,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_StatisticalCoverageHashKey AS StatisticalCoverageHashKey,
	o_RetroactiveDate AS RetroactiveDate,
	o_LiabilityFormCode AS LiabilityFormCode,
	o_ISOGeneralLiabilityClassSummary AS ISOGeneralLiabilityClassSummary,
	o_ISOGeneralLiabilityClassGroupCode AS ISOGeneralLiabilityClassGroupCode,
	o_ChangeFlag AS ChangeFlag,
	lkp_PremiumTransactionID,
	lkp_RetroactiveDate,
	lkp_LiabilityFormCode,
	lkp_ClassSummary,
	lkp_ClassGroupCode
	FROM EXP_DetectChanges
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE ChangeFlag='NEW'),
RTR_INSERT_UPDATE_UPDATE AS (SELECT * FROM RTR_INSERT_UPDATE WHERE ChangeFlag='UPDATE'),
UPD_Exists AS (
	SELECT
	PremiumTransactionID, 
	ModifiedDate, 
	StatisticalCoverageHashKey, 
	RetroactiveDate, 
	LiabilityFormCode, 
	ISOGeneralLiabilityClassSummary, 
	ISOGeneralLiabilityClassGroupCode
	FROM RTR_INSERT_UPDATE_UPDATE
),
CoverageDetailGeneralLiability_UPDATE AS (
	MERGE INTO CoverageDetailGeneralLiability AS T
	USING UPD_Exists AS S
	ON T.PremiumTransactionID = S.PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.CoverageGuid = S.StatisticalCoverageHashKey, T.RetroactiveDate = S.RetroactiveDate, T.LiabilityFormCode = S.LiabilityFormCode, T.ISOGeneralLiabilityClassSummary = S.ISOGeneralLiabilityClassSummary, T.ISOGeneralLiabilityClassGroupCode = S.ISOGeneralLiabilityClassGroupCode
),
CoverageDetailGeneralLiability_INSERT AS (
	INSERT INTO CoverageDetailGeneralLiability
	(PremiumTransactionID, CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CoverageGuid, RetroactiveDate, LiabilityFormCode, ISOGeneralLiabilityClassSummary, ISOGeneralLiabilityClassGroupCode)
	SELECT 
	PREMIUMTRANSACTIONID, 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	StatisticalCoverageHashKey AS COVERAGEGUID, 
	RETROACTIVEDATE, 
	LIABILITYFORMCODE, 
	ISOGENERALLIABILITYCLASSSUMMARY, 
	ISOGENERALLIABILITYCLASSGROUPCODE
	FROM RTR_INSERT_UPDATE_INSERT
),