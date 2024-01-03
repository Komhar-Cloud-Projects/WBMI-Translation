WITH
LKP_PifDept1553Stage_UnderlyingInsuranceLine AS (
	SELECT
	UnderlyingInsuranceLine,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	DECLPTFormNumber,
	DECLPTSeqSameForm,
	DECLPTSeq0098
	FROM (
		select case when ltrim(rtrim(replace(SourceValue,'=','')))='LIMITS OF INSURANCE'
		then 'Umbrella'
		when charindex('GENERAL LIABILITY INSURANCE',SourceValue )>0
		and charindex('POLICY NUMBER:@',SourceValue )>0
		and substring(
		REPLACE(SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('POLICY NUMBER:',SourceValue))+1,
		CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('POLICY NUMBER:',SourceValue)))-CHARINDEX('@',SourceValue,charindex('POLICY NUMBER:',SourceValue))-1) , ' ', ''),
		1,2) in ('NA', 'NB')
		then 'SBOPGeneralLiability'
		when charindex('GENERAL LIABILITY INSURANCE',SourceValue )>0
		and charindex('POLICY NUMBER:@',SourceValue )>0
		and substring(
		REPLACE(SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('POLICY NUMBER:',SourceValue))+1,
		CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('POLICY NUMBER:',SourceValue)))-CHARINDEX('@',SourceValue,charindex('POLICY NUMBER:',SourceValue))-1) , ' ', ''),
		1,2) in ('BC', 'BD')
		then 'CBOPGeneralLiability'
		when charindex('GENERAL LIABILITY INSURANCE',SourceValue )>0
		and charindex('POLICY NUMBER:@',SourceValue )>0
		and substring(
		REPLACE(SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('POLICY NUMBER:',SourceValue))+1,
		CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('POLICY NUMBER:',SourceValue)))-CHARINDEX('@',SourceValue,charindex('POLICY NUMBER:',SourceValue))-1) , ' ', ''),
		1,2) in ('BO')
		then 'SMARTbusinessLiability'
		when charindex('GENERAL LIABILITY INSURANCE',SourceValue )>0
		then 'GeneralLiability'
		when charindex('EMPLOYERS'' LIABILITY INSURANCE',SourceValue )>0
		then 'EmployersLiability'
		when charindex('@BUSINESSOWNERS LIABILITY',SourceValue )>0
		then 'BusinessownersLiability'
		when charindex('@BUSSINESSOWNERS LIABILITY',SourceValue )>0
		then 'BusinessownersLiability'
		when charindex('GARAGE LIABILITY INSURANCE',SourceValue )>0
		then 'GarageLiability'
		when charindex('AUTOMOBILE LIABILITY INSURANCE',SourceValue )>0
		then 'CommercialAutoLiability'
		when charindex('@LIQUOR LIABLITY',SourceValue )>0
		then 'LiquorLiability'
		else 'N/A' end as UnderlyingInsuranceLine,
		PifSymbol as PifSymbol ,
		PifPolicyNumber as PifPolicyNumber,
		PifModule as PifModule,
		DECLPTFormNumber as DECLPTFormNumber,
		DECLPTSeqSameForm as DECLPTSeqSameForm,
		DECLPTSeq0098 as DECLPTSeq0098
		from (
		select PifSymbol,
		PifPolicyNumber,
		PifModule,
		DECLPTFormNumber,
		DECLPTSeqSameForm,
		DECLPTSeq0098,
		ltrim(rtrim(DECLPTText1701))+' '+ltrim(rtrim(DECLPTText71791))+' '+ltrim(rtrim(DECLPTText1702))+' '+ltrim(rtrim(DECLPTText71792)) as SourceValue
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PifDept1553Stage dept1553
		where LEFT(PifSymbol,2) in ('CU','NU','CP') and LEFT(DECLPTFormNumber,1)='U'
		) Src
		where  ltrim(rtrim(replace(SourceValue,'=','')))='LIMITS OF INSURANCE' or charindex('GENERAL LIABILITY INSURANCE',SourceValue )>0 or charindex('EMPLOYERS'' LIABILITY INSURANCE',SourceValue )>0 or charindex('@BUSINESSOWNERS LIABILITY',SourceValue )>0  or charindex('@BUSSINESSOWNERS LIABILITY',SourceValue )>0 or charindex('GARAGE LIABILITY INSURANCE',SourceValue )>0 or charindex('AUTOMOBILE LIABILITY INSURANCE',SourceValue )>0 or charindex('@LIQUOR LIABLITY',SourceValue )>0
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,DECLPTFormNumber,DECLPTSeqSameForm,DECLPTSeq0098 ORDER BY UnderlyingInsuranceLine DESC) = 1
),
LKP_PifDept1553Stage_UnderlyingCompanyName AS (
	SELECT
	UnderlyingCompanyName,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	DECLPTFormNumber,
	DECLPTSeqSameForm,
	DECLPTSeq0098
	FROM (
		select 
		case when charindex('INSURER:',SourceValue )>0
		then LTRIM(RTRIM(
		SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('INSURER:',SourceValue))+1,
		CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('INSURER:',SourceValue)))-CHARINDEX('@',SourceValue,charindex('INSURER:',SourceValue))-1) 
		))
		else
		'N/A'
		end as UnderlyingCompanyName,
		PifSymbol as PifSymbol ,
		PifPolicyNumber as PifPolicyNumber,
		PifModule as PifModule,
		DECLPTFormNumber as DECLPTFormNumber,
		DECLPTSeqSameForm as DECLPTSeqSameForm,
		DECLPTSeq0098 as DECLPTSeq0098
		from (
		select PifSymbol,
		PifPolicyNumber,
		PifModule,
		DECLPTFormNumber,
		DECLPTSeqSameForm,
		DECLPTSeq0098,
		ltrim(rtrim(DECLPTText1701))+' '+ltrim(rtrim(DECLPTText71791))+' '+ltrim(rtrim(DECLPTText1702))+' '+ltrim(rtrim(DECLPTText71792)) as SourceValue
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PifDept1553Stage dept1553
		where LEFT(PifSymbol,2) in ('CU','NU','CP') and LEFT(DECLPTFormNumber,1)='U'
		) Src
		where  charindex('INSURER:',SourceValue )>0 and LTRIM(RTRIM(
		SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('INSURER:',SourceValue))+1,
		CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('INSURER:',SourceValue)))-CHARINDEX('@',SourceValue,charindex('INSURER:',SourceValue))-1) 
		))<>''
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,DECLPTFormNumber,DECLPTSeqSameForm,DECLPTSeq0098 ORDER BY UnderlyingCompanyName DESC) = 1
),
LKP_PifDept1553Stage_UnderlyingPolicyKey AS (
	SELECT
	UnderlyingPolicyKey,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	DECLPTFormNumber,
	DECLPTSeqSameForm,
	DECLPTSeq0098
	FROM (
		select 
		case when charindex('POLICY NUMBER:',SourceValue )>0
		then REPLACE(SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('POLICY NUMBER:',SourceValue))+1,
		CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('POLICY NUMBER:',SourceValue)))-CHARINDEX('@',SourceValue,charindex('POLICY NUMBER:',SourceValue))-1) , ' ', '')
		else
		'N/A'
		end as UnderlyingPolicyKey,
		PifSymbol as PifSymbol ,
		PifPolicyNumber as PifPolicyNumber,
		PifModule as PifModule,
		DECLPTFormNumber as DECLPTFormNumber,
		DECLPTSeqSameForm as DECLPTSeqSameForm,
		DECLPTSeq0098 as DECLPTSeq0098
		from (
		select PifSymbol,
		PifPolicyNumber,
		PifModule,
		DECLPTFormNumber,
		DECLPTSeqSameForm,
		DECLPTSeq0098,
		ltrim(rtrim(DECLPTText1701))+' '+ltrim(rtrim(DECLPTText71791))+' '+ltrim(rtrim(DECLPTText1702))+' '+ltrim(rtrim(DECLPTText71792)) as SourceValue
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PifDept1553Stage dept1553
		where LEFT(PifSymbol,2) in ('CU','NU','CP') and LEFT(DECLPTFormNumber,1)='U'
		) Src
		where  charindex('POLICY NUMBER:',SourceValue )>0 and REPLACE(SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('POLICY NUMBER:',SourceValue))+1, CHARINDEX('^', SourceValue, CHARINDEX('@',SourceValue,charindex('POLICY NUMBER:',SourceValue)))-CHARINDEX('@',SourceValue,charindex('POLICY NUMBER:',SourceValue))-1) , ' ', '')<>''
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,DECLPTFormNumber,DECLPTSeqSameForm,DECLPTSeq0098 ORDER BY UnderlyingPolicyKey DESC) = 1
),
SQ_pif_4514_stage AS (
	SELECT 
	pif_4514_stage.pif_4514_stage_id as pif_4514_stage_id, 
	pif_4514_stage.pif_symbol as pif_symbol,
	pif_4514_stage.pif_policy_number as pif_policy_number,
	pif_4514_stage.pif_module as pif_module,
	LTRIM(RTRIM(pif_4514_stage.sar_insurance_line)) as sar_insurance_line, 
	(CASE WHEN Pif43RXCPStage.Pif43RXCPStageId IS NOT NULL THEN CONVERT(VARCHAR(3),Pif43RXCPStage.Pmdrxp1PmsDefSubjOfIns)
	ELSE LTRIM(RTRIM(pif_4514_stage.sar_risk_unit_group)) END) as sar_risk_unit_group, 
	LTRIM(RTRIM(pif_4514_stage.sar_unit))+LTRIM(RTRIM(pif_4514_stage.sar_risk_unit_continued)) as sar_unit,
	(CASE WHEN Pif43RXCPStage.Pif43RXCPStageId IS NOT NULL THEN Pif43RXCPStage.Pmdrxp1AmountOfInsurance
	ELSE pif_4514_stage.sar_exposure*1000 END) as sar_exposure, 
	LTRIM(RTRIM(pif_4514_stage.sar_major_peril)) as sar_major_peril, 
	LTRIM(RTRIM(pif_4514_stage.sar_seq_no)) as sar_seq_no, 
	LTRIM(RTRIM(pif_4514_stage.sar_type_bureau)) as sar_type_bureau, 
	LTRIM(RTRIM(pif_4514_stage.sar_class_1_4)) as sar_class_1_4, 
	LTRIM(RTRIM(pif_4514_stage.sar_code_1)) as sar_code_1, 
	LTRIM(RTRIM(pif_4514_stage.sar_code_2)) as sar_code_2, 
	LTRIM(RTRIM(pif_4514_stage.sar_code_3)) as sar_code_3, 
	LTRIM(RTRIM(pif_4514_stage.sar_code_5)) as sar_code_5, 
	LTRIM(RTRIM(pif_4514_stage.sar_code_6)) as sar_code_6,
	pif_4514_stage.sar_state as sar_state,
	Pif43RXCPStage.Pif43RXCPStageId as Pif43RXCPStageId
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_4514_stage pif_4514_stage
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43RXCPStage Pif43RXCPStage
	ON Pif43RXCPStage.PifSymbol=pif_4514_stage.pif_symbol
	AND Pif43RXCPStage.PifPolicyNumber=pif_4514_stage.pif_policy_number
	AND Pif43RXCPStage.PifModule=pif_4514_stage.pif_module
	AND Pif43RXCPStage.Pmdrxp1InsuranceLine=pif_4514_stage.sar_insurance_line
	AND Pif43RXCPStage.Pmdrxp1LocationNumber=CONVERT(int,pif_4514_stage.sar_location_x)
	AND Pif43RXCPStage.Pmdrxp1SubLocationNumber=CONVERT(int,pif_4514_stage.sar_sub_location_x)
	and right('000'+CONVERT(VARCHAR(3),Pif43RXCPStage.Pmdrxp1PmsDefSubjOfIns), 3)=LTRIM(RTRIM(pif_4514_stage.sar_risk_unit_group))
	WHERE pif_4514_stage.logical_flag IN ('0','1','2','3')
	@{pipeline().parameters.WHERE_CLAUSE_4514}
),
EXP_Set_Lookup_Conditions AS (
	SELECT
	sar_insurance_line AS i_sar_insurance_line,
	sar_major_peril AS i_sar_major_peril,
	sar_risk_unit_group AS i_sar_risk_unit_group,
	sar_unit AS i_sar_unit,
	sar_type_bureau AS i_sar_type_bureau,
	sar_class_1_4 AS i_sar_class_1_4,
	sar_seq_no AS i_sar_seq_no,
	sar_code_1 AS i_sar_code_1,
	sar_code_2 AS i_sar_code_2,
	sar_code_3 AS i_sar_code_3,
	sar_code_5 AS i_sar_code_5,
	sar_code_6 AS i_sar_code_6,
	Pif43RXCPStageId AS i_Pif43RXCPStageId,
	pif_4514_stage_id,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(i_Pif43RXCPStageId),'&&'||i_sar_risk_unit_group||'&&&&&&&&&',
	-- i_sar_insurance_line='CR' and in(i_sar_major_peril,'565','566') and in(i_sar_type_bureau,'CR','FT','BT'),i_sar_insurance_line||'&'||i_sar_major_peril||'&&'||i_sar_type_bureau||'&'||i_sar_unit||'&&&&&&&',
	-- i_sar_insurance_line = 'IM' and i_sar_major_peril = '551',i_sar_insurance_line||'&'||i_sar_major_peril||'&'||i_sar_risk_unit_group||'&&&'||i_sar_class_1_4||'&&&&&&',
	-- i_sar_insurance_line = 'GA' and i_sar_major_peril = '100' and in(i_sar_seq_no, '00','01') and i_sar_type_bureau = 'AL',i_sar_insurance_line||'&'||i_sar_major_peril||'&&'||i_sar_type_bureau||'&&&'||i_sar_seq_no||'&&'||i_sar_code_2||'&'||i_sar_code_3||'&&',
	-- i_sar_insurance_line = 'GA' and i_sar_major_peril='114' and i_sar_type_bureau='AL',i_sar_insurance_line||'&'||i_sar_major_peril||'&&'||i_sar_type_bureau||'&&&&&&&'||SUBSTR(i_sar_code_5,1,1)||'&',
	-- i_sar_insurance_line = 'GA' and in(i_sar_major_peril,'115','120') and i_sar_type_bureau='AL',i_sar_insurance_line||'&'||i_sar_major_peril||'&&'||i_sar_type_bureau||'&&&&&&&'||IIF(LENGTH(i_sar_code_5)>1,SUBSTR(i_sar_code_5,2,1),i_sar_code_5) ||'&',
	-- i_sar_insurance_line = 'GA' and i_sar_major_peril = '118' and i_sar_type_bureau='AL',i_sar_insurance_line||'&'||i_sar_major_peril||'&&'||i_sar_type_bureau||'&&&&&&&&'||i_sar_code_6,
	-- i_sar_insurance_line = 'GA' and i_sar_major_peril = '130' and i_sar_type_bureau='AN',i_sar_insurance_line||'&'||i_sar_major_peril||'&&'||i_sar_type_bureau||'&&&&'||i_sar_code_1||'&'||i_sar_code_2||'&&&',
	-- i_sar_insurance_line='GA' and i_sar_type_bureau='AP',i_sar_insurance_line||'&&&'||i_sar_type_bureau||'&'||i_sar_unit||'&&&&&&&',
	-- i_sar_major_peril='100' and i_sar_type_bureau='AL','&'||i_sar_major_peril||'&&'||i_sar_type_bureau||'&&&&&'||i_sar_code_2||'&'||i_sar_code_3||'&&',
	-- i_sar_major_peril='114' and i_sar_type_bureau='AL','&'||i_sar_major_peril||'&&'||i_sar_type_bureau||'&&&&&&&'||SUBSTR(i_sar_code_5,1,1)||'&',
	-- in(i_sar_major_peril,'115','116') and i_sar_type_bureau='AL','&'||i_sar_major_peril||'&&'||i_sar_type_bureau||'&&&&&&&'||IIF(LENGTH(i_sar_code_5)>1,SUBSTR(i_sar_code_5,2,1),i_sar_code_5)||'&',
	-- in(i_sar_major_peril,'118','119') and i_sar_type_bureau='AL','&'||i_sar_major_peril||'&&'||i_sar_type_bureau||'&&&&&&&&'||i_sar_code_6,
	-- i_sar_major_peril='130' and i_sar_type_bureau='AN','&'||i_sar_major_peril||'&&'||i_sar_type_bureau||'&&&&'||i_sar_code_1||'&'||i_sar_code_2||'&&&',
	-- NULL
	-- )
	DECODE(TRUE,
	NOT i_Pif43RXCPStageId IS NULL, '&&' || i_sar_risk_unit_group || '&&&&&&&&&',
	i_sar_insurance_line = 'CR' AND in(i_sar_major_peril, '565', '566') AND in(i_sar_type_bureau, 'CR', 'FT', 'BT'), i_sar_insurance_line || '&' || i_sar_major_peril || '&&' || i_sar_type_bureau || '&' || i_sar_unit || '&&&&&&&',
	i_sar_insurance_line = 'IM' AND i_sar_major_peril = '551', i_sar_insurance_line || '&' || i_sar_major_peril || '&' || i_sar_risk_unit_group || '&&&' || i_sar_class_1_4 || '&&&&&&',
	i_sar_insurance_line = 'GA' AND i_sar_major_peril = '100' AND in(i_sar_seq_no, '00', '01') AND i_sar_type_bureau = 'AL', i_sar_insurance_line || '&' || i_sar_major_peril || '&&' || i_sar_type_bureau || '&&&' || i_sar_seq_no || '&&' || i_sar_code_2 || '&' || i_sar_code_3 || '&&',
	i_sar_insurance_line = 'GA' AND i_sar_major_peril = '114' AND i_sar_type_bureau = 'AL', i_sar_insurance_line || '&' || i_sar_major_peril || '&&' || i_sar_type_bureau || '&&&&&&&' || SUBSTR(i_sar_code_5, 1, 1) || '&',
	i_sar_insurance_line = 'GA' AND in(i_sar_major_peril, '115', '120') AND i_sar_type_bureau = 'AL', i_sar_insurance_line || '&' || i_sar_major_peril || '&&' || i_sar_type_bureau || '&&&&&&&' || IFF(LENGTH(i_sar_code_5) > 1, SUBSTR(i_sar_code_5, 2, 1), i_sar_code_5) || '&',
	i_sar_insurance_line = 'GA' AND i_sar_major_peril = '118' AND i_sar_type_bureau = 'AL', i_sar_insurance_line || '&' || i_sar_major_peril || '&&' || i_sar_type_bureau || '&&&&&&&&' || i_sar_code_6,
	i_sar_insurance_line = 'GA' AND i_sar_major_peril = '130' AND i_sar_type_bureau = 'AN', i_sar_insurance_line || '&' || i_sar_major_peril || '&&' || i_sar_type_bureau || '&&&&' || i_sar_code_1 || '&' || i_sar_code_2 || '&&&',
	i_sar_insurance_line = 'GA' AND i_sar_type_bureau = 'AP', i_sar_insurance_line || '&&&' || i_sar_type_bureau || '&' || i_sar_unit || '&&&&&&&',
	i_sar_major_peril = '100' AND i_sar_type_bureau = 'AL', '&' || i_sar_major_peril || '&&' || i_sar_type_bureau || '&&&&&' || i_sar_code_2 || '&' || i_sar_code_3 || '&&',
	i_sar_major_peril = '114' AND i_sar_type_bureau = 'AL', '&' || i_sar_major_peril || '&&' || i_sar_type_bureau || '&&&&&&&' || SUBSTR(i_sar_code_5, 1, 1) || '&',
	in(i_sar_major_peril, '115', '116') AND i_sar_type_bureau = 'AL', '&' || i_sar_major_peril || '&&' || i_sar_type_bureau || '&&&&&&&' || IFF(LENGTH(i_sar_code_5) > 1, SUBSTR(i_sar_code_5, 2, 1), i_sar_code_5) || '&',
	in(i_sar_major_peril, '118', '119') AND i_sar_type_bureau = 'AL', '&' || i_sar_major_peril || '&&' || i_sar_type_bureau || '&&&&&&&&' || i_sar_code_6,
	i_sar_major_peril = '130' AND i_sar_type_bureau = 'AN', '&' || i_sar_major_peril || '&&' || i_sar_type_bureau || '&&&&' || i_sar_code_1 || '&' || i_sar_code_2 || '&&&',
	NULL) AS o_lkp_condition
	FROM SQ_pif_4514_stage
),
LKP_SupCoverageLimtRule AS (
	SELECT
	CoverageLimitType,
	CoverageLimitvalue,
	i_lkp_con,
	lkp_con
	FROM (
		select 
		Coveragelimittype as  CoverageLimitType,coveragelimitvalue as CoverageLimitvalue,
		(isnull(InsuranceLine,'')+'&'
		+isnull(MajorPerilCode,'')+'&'
		+isnull(RiskUnitGroup,'')+'&'
		+isnull(TypeBureauCode,'')+'&'
		+ISNULL(RiskUnit,'')+'&'
		+isnull(ClassCode,'')+'&'
		+isnull(MajorPerilSequenceNumber,'')+'&'
		+isnull(PMSBureauCode1,'')+'&'
		+isnull(PMSBureauCode2,'')+'&'
		+isnull(PMSBureauCode3,'')+'&'
		+isnull(PMSBureauCode5,'')+'&'
		+isnull(PMSBureauCode6,'')) as lkp_con 
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.SupCoverageLimtRule
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY lkp_con ORDER BY CoverageLimitType) = 1
),
EXP_Assign_Value_4514 AS (
	SELECT
	LKP_SupCoverageLimtRule.CoverageLimitType AS i_type,
	LKP_SupCoverageLimtRule.CoverageLimitvalue AS i_CoverageLimitvalue,
	SQ_pif_4514_stage.sar_exposure AS i_sar_exposure,
	-- *INF*: IIF(ISNULL(i_CoverageLimitvalue) OR IS_SPACES(i_CoverageLimitvalue) OR LENGTH(i_CoverageLimitvalue)=0,'0',LTRIM(RTRIM(i_CoverageLimitvalue)))
	IFF(i_CoverageLimitvalue IS NULL OR IS_SPACES(i_CoverageLimitvalue) OR LENGTH(i_CoverageLimitvalue) = 0, '0', LTRIM(RTRIM(i_CoverageLimitvalue))) AS v_CoverageLimitvalue,
	-- *INF*: TO_CHAR(i_sar_exposure)
	TO_CHAR(i_sar_exposure) AS v_sar_exposure,
	EXP_Set_Lookup_Conditions.pif_4514_stage_id,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_type)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_type) AS o_type,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(i_CoverageLimitvalue) AND NOT ISNULL(i_type),v_CoverageLimitvalue,
	-- NOT ISNULL(i_type),v_sar_exposure,'0')
	DECODE(TRUE,
	NOT i_CoverageLimitvalue IS NULL AND NOT i_type IS NULL, v_CoverageLimitvalue,
	NOT i_type IS NULL, v_sar_exposure,
	'0') AS o_value
	FROM EXP_Set_Lookup_Conditions
	 -- Manually join with SQ_pif_4514_stage
	LEFT JOIN LKP_SupCoverageLimtRule
	ON LKP_SupCoverageLimtRule.lkp_con = EXP_Set_Lookup_Conditions.o_lkp_condition
),
RTR_EPLI AS (
	SELECT
	pif_4514_stage_id,
	pif_symbol AS pol_sym,
	pif_policy_number AS pol_num,
	pif_module AS pol_mod,
	sar_insurance_line,
	sar_risk_unit_group,
	sar_unit
	FROM SQ_pif_4514_stage
),
RTR_EPLI_EachRelatedWrongfulEmploymentPractice AS (SELECT * FROM RTR_EPLI WHERE ((sar_insurance_line='GA' and sar_risk_unit_group='417') OR
(sar_insurance_line='GA' and sar_risk_unit_group='418') OR
(sar_insurance_line='GL' and sar_unit = '22222') OR
(sar_insurance_line='GL' and sar_unit = '22250') OR
(sar_insurance_line='BP' and sar_risk_unit_group='366') OR
(sar_insurance_line='BP' and sar_risk_unit_group='367') ) 
AND 
1=1),
RTR_EPLI_AggregateLimit AS (SELECT * FROM RTR_EPLI WHERE ((sar_insurance_line='GA' and sar_risk_unit_group='417') OR
(sar_insurance_line='GA' and sar_risk_unit_group='418') OR
(sar_insurance_line='GL' and sar_unit = '22222') OR
(sar_insurance_line='GL' and sar_unit = '22250') OR
(sar_insurance_line='BP' and sar_risk_unit_group='366') OR
(sar_insurance_line='BP' and sar_risk_unit_group='367') ) 
AND 
2=2),
EXP_EPLI_AggregateLimit AS (
	SELECT
	pif_4514_stage_id,
	pol_sym,
	pol_num,
	pol_mod,
	sar_insurance_line,
	sar_risk_unit_group,
	sar_unit,
	'AGGREGATE LIMIT' AS o_LimitType,
	-- *INF*: DECODE(TRUE,
	-- (sar_insurance_line='GA' and sar_risk_unit_group='417') , '100000',
	-- (sar_insurance_line='GA' and sar_risk_unit_group='418') ,'250000',
	-- (sar_insurance_line='GL' and sar_unit = '22222') , '100000',
	-- (sar_insurance_line='GL' and sar_unit = '22250'),'250000',
	-- (sar_insurance_line='BP' and sar_risk_unit_group='366') , '100000',
	-- (sar_insurance_line='BP' and sar_risk_unit_group='367') ,'250000',
	-- 'N/A')
	-- 
	DECODE(TRUE,
	( sar_insurance_line = 'GA' AND sar_risk_unit_group = '417' ), '100000',
	( sar_insurance_line = 'GA' AND sar_risk_unit_group = '418' ), '250000',
	( sar_insurance_line = 'GL' AND sar_unit = '22222' ), '100000',
	( sar_insurance_line = 'GL' AND sar_unit = '22250' ), '250000',
	( sar_insurance_line = 'BP' AND sar_risk_unit_group = '366' ), '100000',
	( sar_insurance_line = 'BP' AND sar_risk_unit_group = '367' ), '250000',
	'N/A') AS o_LimitValue
	FROM RTR_EPLI_AggregateLimit
),
EXP_EPLI_EachRelatedWrongfulEmploymentPractice AS (
	SELECT
	pif_4514_stage_id AS pif_4514_stage_id1,
	pol_sym,
	pol_num,
	pol_mod,
	sar_insurance_line,
	sar_risk_unit_group,
	sar_unit,
	'EachRelatedWrongfulEmploymentPractice' AS o_LimitType,
	-- *INF*: DECODE(TRUE,
	-- (sar_insurance_line='GA' and sar_risk_unit_group='417') , '100000',
	-- (sar_insurance_line='GA' and sar_risk_unit_group='418') ,'250000',
	-- (sar_insurance_line='GL' and sar_unit = '22222') , '100000',
	-- (sar_insurance_line='GL' and sar_unit = '22250'),'250000',
	-- (sar_insurance_line='BP' and sar_risk_unit_group='366') , '100000',
	-- (sar_insurance_line='BP' and sar_risk_unit_group='367') ,'250000',
	-- 'N/A')
	-- 
	DECODE(TRUE,
	( sar_insurance_line = 'GA' AND sar_risk_unit_group = '417' ), '100000',
	( sar_insurance_line = 'GA' AND sar_risk_unit_group = '418' ), '250000',
	( sar_insurance_line = 'GL' AND sar_unit = '22222' ), '100000',
	( sar_insurance_line = 'GL' AND sar_unit = '22250' ), '250000',
	( sar_insurance_line = 'BP' AND sar_risk_unit_group = '366' ), '100000',
	( sar_insurance_line = 'BP' AND sar_risk_unit_group = '367' ), '250000',
	'N/A') AS o_LimitValue
	FROM RTR_EPLI_EachRelatedWrongfulEmploymentPractice
),
SQ_Pif351Stage AS (
	SELECT
		PifSymbol,
		PifPolicyNumber,
		PifModule,
		UnitNum,
		VehicleCoverageCode1,
		VehicleCoverageCode2,
		VehicleCoverageCode3,
		VehicleCoverageCode4,
		VehicleCoverageCode5,
		VehicleCoverageCode6,
		VehicleCoverageCode7,
		VehicleCoverageCode8,
		VehicleCoverageCode9,
		VehicleCoverageCode10,
		VehicleCoverageCode11,
		VehicleCoverageCode12,
		VehicleCoverageCode13,
		VehicleCoverageCode14,
		VehicleCoverageLimit1,
		VehicleCoverageLimit2,
		VehicleCoverageLimit3,
		VehicleCoverageLimit4,
		VehicleCoverageLimit5,
		VehicleCoverageLimit6,
		VehicleCoverageLimit7,
		VehicleCoverageLimit8,
		VehicleCoverageLimit9,
		VehicleCoverageLimit10,
		VehicleCoverageLimit11,
		VehicleCoverageLimit12,
		VehicleCoverageLimit13,
		VehicleCoverageLimit14
	FROM Pif351Stage
	WHERE Pif351Stage.UnitNum>=52
),
NRM_CA_Limit AS (
),
EXP_CA_DefaultValue AS (
	SELECT
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	UnitNum AS i_UnitNum,
	-- *INF*: LPAD(TO_CHAR(i_UnitNum),3,'0')
	LPAD(TO_CHAR(i_UnitNum), 3, '0') AS o_UnitNum,
	VehicleCoverageCode,
	VehicleCoverageLimit AS i_VehicleCoverageLimit,
	-- *INF*: LTRIM(RTRIM(i_VehicleCoverageLimit))
	LTRIM(RTRIM(i_VehicleCoverageLimit)) AS o_VehicleCoverageLimit
	FROM NRM_CA_Limit
),
JNR_CA AS (SELECT
	EXP_CA_DefaultValue.PifSymbol, 
	EXP_CA_DefaultValue.PifPolicyNumber, 
	EXP_CA_DefaultValue.PifModule, 
	EXP_CA_DefaultValue.o_UnitNum AS UnitNum, 
	EXP_CA_DefaultValue.VehicleCoverageCode, 
	EXP_CA_DefaultValue.o_VehicleCoverageLimit AS VehicleCoverageLimit, 
	SQ_pif_4514_stage.pif_4514_stage_id, 
	SQ_pif_4514_stage.pif_symbol AS pol_sym, 
	SQ_pif_4514_stage.pif_policy_number AS pol_num, 
	SQ_pif_4514_stage.pif_module AS pol_mod, 
	SQ_pif_4514_stage.sar_unit, 
	SQ_pif_4514_stage.sar_seq_no, 
	SQ_pif_4514_stage.sar_state
	FROM SQ_pif_4514_stage
	INNER JOIN EXP_CA_DefaultValue
	ON EXP_CA_DefaultValue.PifSymbol = SQ_pif_4514_stage.pif_symbol AND EXP_CA_DefaultValue.PifPolicyNumber = SQ_pif_4514_stage.pif_policy_number AND EXP_CA_DefaultValue.PifModule = SQ_pif_4514_stage.pif_module AND EXP_CA_DefaultValue.o_UnitNum = SQ_pif_4514_stage.sar_unit AND EXP_CA_DefaultValue.VehicleCoverageCode = SQ_pif_4514_stage.sar_seq_no
),
EXP_TRANS_CA AS (
	SELECT
	VehicleCoverageCode AS i_VehicleCoverageCode,
	VehicleCoverageLimit AS i_VehicleCoverageLimit,
	pif_4514_stage_id AS i_pif_4514_stage_id,
	sar_state AS i_sar_state,
	-- *INF*: LTRIM(RTRIM(i_VehicleCoverageLimit))
	LTRIM(RTRIM(i_VehicleCoverageLimit)) AS v_VehicleCoverageLimit,
	-- *INF*: DECODE(TRUE,
	-- i_VehicleCoverageCode='01','CombinedSingleLimit',
	-- i_VehicleCoverageCode='07','MedicalPaymentLimit',
	-- i_VehicleCoverageCode='08','UninsuredMotoristSingleLimit',
	-- i_VehicleCoverageCode='09','UninsuredMotoristSplitLimit',
	-- i_VehicleCoverageCode='12','UnderinsuredMotoristSingleLimit',
	-- i_VehicleCoverageCode='13','UnderinsuredMotoristSplitLimit',
	-- i_VehicleCoverageCode='11','UninsuredMotoristPropertyDamage',
	-- i_VehicleCoverageCode='14' AND i_sar_state='22' AND IN(i_VehicleCoverageLimit,'Y','AY'),'PersonalInjuryProtectionBasicLimit',
	-- i_VehicleCoverageCode='14' AND i_sar_state='22' AND IN(i_VehicleCoverageLimit,'50','60','70','75','A50','A60','A70','A75'),'PersonalInjuryProtectionExcessLimit',
	-- i_VehicleCoverageCode='14' AND i_sar_state='15' AND i_VehicleCoverageLimit='Y','PersonalInjuryProtectionBasicLimit',
	-- i_VehicleCoverageCode='14' AND i_sar_state='15' AND IN(i_VehicleCoverageLimit,'12','27'),'PersonalInjuryProtectionExcessLimit',
	-- i_VehicleCoverageCode='14' AND i_sar_state='16' AND i_VehicleCoverageLimit='Y','PersonalInjuryProtectionBasicLimit',
	-- i_VehicleCoverageCode='14' AND i_sar_state='16' AND IN(i_VehicleCoverageLimit,'10','20','30','40','65','90'),'PersonalInjuryProtectionExcessLimit',
	-- 'N/A'
	-- )
	-- 
	-- 
	-- 
	-- 
	-- --DECODE(TRUE,
	-- --i_VehicleCoverageCode='01','CombinedSingleLimit',
	-- --i_VehicleCoverageCode='07','MedicalPaymentLimit',
	-- --i_VehicleCoverageCode='08','UninsuredMotoristSingleLimit',
	-- --i_VehicleCoverageCode='09','UninsuredMotoristSplitLimit',
	-- --i_VehicleCoverageCode='12','UnderinsuredMotoristSingleLimit',
	-- --i_VehicleCoverageCode='13','UnderinsuredMotoristSplitLimit',
	-- --i_VehicleCoverageCode='11','UninsuredMotoristPropertyDamage',
	-- --i_VehicleCoverageCode='14' AND i_sar_state='22' AND SUBSTR(i_VehicleCoverageLimit,1,1)='A','PersonalInjuryProtectionLimitWithStacking',
	-- --i_VehicleCoverageCode='14' AND i_sar_state='22','PersonalInjuryProtectionLimitWithoutStacking',
	-- --i_VehicleCoverageCode='14' AND i_sar_state='15','PersonalInjuryProtectionLimitWithStacking',
	-- --i_VehicleCoverageCode='14' AND i_sar_state='16','PersonalInjuryProtectionLimit',
	-- --i_VehicleCoverageCode='14' AND i_sar_state='21','PersonalInjuryProtectionLimit',
	-- --'N/A')
	DECODE(TRUE,
	i_VehicleCoverageCode = '01', 'CombinedSingleLimit',
	i_VehicleCoverageCode = '07', 'MedicalPaymentLimit',
	i_VehicleCoverageCode = '08', 'UninsuredMotoristSingleLimit',
	i_VehicleCoverageCode = '09', 'UninsuredMotoristSplitLimit',
	i_VehicleCoverageCode = '12', 'UnderinsuredMotoristSingleLimit',
	i_VehicleCoverageCode = '13', 'UnderinsuredMotoristSplitLimit',
	i_VehicleCoverageCode = '11', 'UninsuredMotoristPropertyDamage',
	i_VehicleCoverageCode = '14' AND i_sar_state = '22' AND IN(i_VehicleCoverageLimit, 'Y', 'AY'), 'PersonalInjuryProtectionBasicLimit',
	i_VehicleCoverageCode = '14' AND i_sar_state = '22' AND IN(i_VehicleCoverageLimit, '50', '60', '70', '75', 'A50', 'A60', 'A70', 'A75'), 'PersonalInjuryProtectionExcessLimit',
	i_VehicleCoverageCode = '14' AND i_sar_state = '15' AND i_VehicleCoverageLimit = 'Y', 'PersonalInjuryProtectionBasicLimit',
	i_VehicleCoverageCode = '14' AND i_sar_state = '15' AND IN(i_VehicleCoverageLimit, '12', '27'), 'PersonalInjuryProtectionExcessLimit',
	i_VehicleCoverageCode = '14' AND i_sar_state = '16' AND i_VehicleCoverageLimit = 'Y', 'PersonalInjuryProtectionBasicLimit',
	i_VehicleCoverageCode = '14' AND i_sar_state = '16' AND IN(i_VehicleCoverageLimit, '10', '20', '30', '40', '65', '90'), 'PersonalInjuryProtectionExcessLimit',
	'N/A') AS o_LimitType,
	-- *INF*: DECODE(TRUE,
	-- i_VehicleCoverageCode='01',
	-- DECODE(TRUE,
	-- v_VehicleCoverageLimit='25','25000',
	-- v_VehicleCoverageLimit='30','30000',
	-- v_VehicleCoverageLimit='40','40000',
	-- v_VehicleCoverageLimit='50','50000',
	-- v_VehicleCoverageLimit='75','75000',
	-- v_VehicleCoverageLimit='100','100000',
	-- v_VehicleCoverageLimit='150','150000',
	-- v_VehicleCoverageLimit='200','200000',
	-- v_VehicleCoverageLimit='250','250000',
	-- v_VehicleCoverageLimit='300','300000',
	-- v_VehicleCoverageLimit='400','400000',
	-- v_VehicleCoverageLimit='500','500000',
	-- v_VehicleCoverageLimit='750','750000',
	-- v_VehicleCoverageLimit='1M','1000000',
	-- v_VehicleCoverageLimit='100A','100000 Option A',
	-- v_VehicleCoverageLimit='100B','100000 Option B',
	-- v_VehicleCoverageLimit='100C','100000 Option C',
	-- v_VehicleCoverageLimit='300A','300000 Option A',
	-- v_VehicleCoverageLimit='300B','300000 Option B',
	-- v_VehicleCoverageLimit='300C','300000 Option C',
	-- v_VehicleCoverageLimit='500A','500000 Option A',
	-- v_VehicleCoverageLimit='500B','500000 Option B',
	-- v_VehicleCoverageLimit='500C','500000 Option C',
	-- v_VehicleCoverageLimit='750A','750000 Option A',
	-- v_VehicleCoverageLimit='750B','750000 Option B',
	-- v_VehicleCoverageLimit='750C','750000 Option C',
	-- v_VehicleCoverageLimit='1MA','1000000 Option A',
	-- v_VehicleCoverageLimit='1MB','1000000 Option B',
	-- v_VehicleCoverageLimit='1MC','1000000 Option C',
	-- 'N/A'
	-- ),
	-- 
	-- i_VehicleCoverageCode='07',
	-- DECODE(TRUE,
	-- v_VehicleCoverageLimit='500','500',
	-- v_VehicleCoverageLimit='1','1000',
	-- v_VehicleCoverageLimit='2','2000',
	-- v_VehicleCoverageLimit='5','5000',
	-- v_VehicleCoverageLimit='10','10000',
	-- 'N/A'),
	-- 
	-- IN(i_VehicleCoverageCode,'08','12'),
	-- DECODE(TRUE,
	-- v_VehicleCoverageLimit='30','30000',
	-- v_VehicleCoverageLimit='40','40000',
	-- v_VehicleCoverageLimit='50','50000',
	-- v_VehicleCoverageLimit='55','55000',
	-- v_VehicleCoverageLimit='60','60000',
	-- v_VehicleCoverageLimit='75','75000',
	-- v_VehicleCoverageLimit='100','100000',
	-- v_VehicleCoverageLimit='200','200000',
	-- v_VehicleCoverageLimit='250','250000',
	-- v_VehicleCoverageLimit='300','300000',
	-- v_VehicleCoverageLimit='350','350000',
	-- v_VehicleCoverageLimit='500','500000',
	-- v_VehicleCoverageLimit='600','600000',
	-- v_VehicleCoverageLimit='750','750000',
	-- v_VehicleCoverageLimit='1M','1000000',
	-- 'N/A'
	-- ),
	-- 
	-- IN(i_VehicleCoverageCode,'09','13'),
	-- DECODE(TRUE,
	-- v_VehicleCoverageLimit='15-30','15000/30000',
	-- v_VehicleCoverageLimit='20-40','20000/40000',
	-- v_VehicleCoverageLimit='25-50','25000/50000',
	-- v_VehicleCoverageLimit='30-60','30000/60000',
	-- v_VehicleCoverageLimit='50-100','50000/100000',
	-- v_VehicleCoverageLimit='100-300','100000/300000',
	-- v_VehicleCoverageLimit='250-500','250000/500000',
	-- v_VehicleCoverageLimit='300-300','300000/300000',
	-- v_VehicleCoverageLimit='300-500','300000/500000',
	-- v_VehicleCoverageLimit='500-500','500000/500000',
	-- v_VehicleCoverageLimit='500-1M','500000/1000000',
	-- v_VehicleCoverageLimit='1M-1M','1000000/1000000',
	-- 'N/A'),
	-- 
	-- 
	-- i_VehicleCoverageCode='11',
	-- DECODE(TRUE,
	-- v_VehicleCoverageLimit='7.5','7500',
	-- v_VehicleCoverageLimit='15','15000',
	-- v_VehicleCoverageLimit='60P','60000',
	-- v_VehicleCoverageLimit='100P','100000',
	-- v_VehicleCoverageLimit='250P','250000',
	-- IN(v_VehicleCoverageLimit,'300','300P'),'300000',
	-- v_VehicleCoverageLimit='350P','350000',
	-- v_VehicleCoverageLimit='500P','500000',
	-- v_VehicleCoverageLimit='750P','750000',
	-- v_VehicleCoverageLimit='1MP','1000000',
	-- 'N/A'),
	-- 
	-- i_VehicleCoverageCode='14' AND i_sar_state='22',
	-- DECODE(TRUE,
	-- IN(v_VehicleCoverageLimit,'Y','AY'),'40000',
	-- IN(v_VehicleCoverageLimit,'50','A50'),'50000',
	-- IN(v_VehicleCoverageLimit,'60','A60'),'60000',
	-- IN(v_VehicleCoverageLimit,'70','A70'),'70000',
	-- IN(v_VehicleCoverageLimit,'75','A75'),'75000',
	-- 'N/A'),
	-- 
	-- i_VehicleCoverageCode='14' AND i_sar_state='15',
	-- DECODE(TRUE,
	-- v_VehicleCoverageLimit='Y','0',
	-- v_VehicleCoverageLimit='12','12500',
	-- v_VehicleCoverageLimit='27','27500',
	-- 'N/A'),
	-- 
	-- i_VehicleCoverageCode='14' AND i_sar_state='16',
	-- DECODE(TRUE,
	-- v_VehicleCoverageLimit='Y','10000',  
	-- v_VehicleCoverageLimit='10','10000',
	-- v_VehicleCoverageLimit='20','20000',
	-- v_VehicleCoverageLimit='30','30000',
	-- v_VehicleCoverageLimit='40','40000',
	-- v_VehicleCoverageLimit='65','65000',
	-- v_VehicleCoverageLimit='90','90000',
	-- 'N/A'),
	-- 'N/A'
	-- )
	DECODE(TRUE,
	i_VehicleCoverageCode = '01', DECODE(TRUE,
	v_VehicleCoverageLimit = '25', '25000',
	v_VehicleCoverageLimit = '30', '30000',
	v_VehicleCoverageLimit = '40', '40000',
	v_VehicleCoverageLimit = '50', '50000',
	v_VehicleCoverageLimit = '75', '75000',
	v_VehicleCoverageLimit = '100', '100000',
	v_VehicleCoverageLimit = '150', '150000',
	v_VehicleCoverageLimit = '200', '200000',
	v_VehicleCoverageLimit = '250', '250000',
	v_VehicleCoverageLimit = '300', '300000',
	v_VehicleCoverageLimit = '400', '400000',
	v_VehicleCoverageLimit = '500', '500000',
	v_VehicleCoverageLimit = '750', '750000',
	v_VehicleCoverageLimit = '1M', '1000000',
	v_VehicleCoverageLimit = '100A', '100000 Option A',
	v_VehicleCoverageLimit = '100B', '100000 Option B',
	v_VehicleCoverageLimit = '100C', '100000 Option C',
	v_VehicleCoverageLimit = '300A', '300000 Option A',
	v_VehicleCoverageLimit = '300B', '300000 Option B',
	v_VehicleCoverageLimit = '300C', '300000 Option C',
	v_VehicleCoverageLimit = '500A', '500000 Option A',
	v_VehicleCoverageLimit = '500B', '500000 Option B',
	v_VehicleCoverageLimit = '500C', '500000 Option C',
	v_VehicleCoverageLimit = '750A', '750000 Option A',
	v_VehicleCoverageLimit = '750B', '750000 Option B',
	v_VehicleCoverageLimit = '750C', '750000 Option C',
	v_VehicleCoverageLimit = '1MA', '1000000 Option A',
	v_VehicleCoverageLimit = '1MB', '1000000 Option B',
	v_VehicleCoverageLimit = '1MC', '1000000 Option C',
	'N/A'),
	i_VehicleCoverageCode = '07', DECODE(TRUE,
	v_VehicleCoverageLimit = '500', '500',
	v_VehicleCoverageLimit = '1', '1000',
	v_VehicleCoverageLimit = '2', '2000',
	v_VehicleCoverageLimit = '5', '5000',
	v_VehicleCoverageLimit = '10', '10000',
	'N/A'),
	IN(i_VehicleCoverageCode, '08', '12'), DECODE(TRUE,
	v_VehicleCoverageLimit = '30', '30000',
	v_VehicleCoverageLimit = '40', '40000',
	v_VehicleCoverageLimit = '50', '50000',
	v_VehicleCoverageLimit = '55', '55000',
	v_VehicleCoverageLimit = '60', '60000',
	v_VehicleCoverageLimit = '75', '75000',
	v_VehicleCoverageLimit = '100', '100000',
	v_VehicleCoverageLimit = '200', '200000',
	v_VehicleCoverageLimit = '250', '250000',
	v_VehicleCoverageLimit = '300', '300000',
	v_VehicleCoverageLimit = '350', '350000',
	v_VehicleCoverageLimit = '500', '500000',
	v_VehicleCoverageLimit = '600', '600000',
	v_VehicleCoverageLimit = '750', '750000',
	v_VehicleCoverageLimit = '1M', '1000000',
	'N/A'),
	IN(i_VehicleCoverageCode, '09', '13'), DECODE(TRUE,
	v_VehicleCoverageLimit = '15-30', '15000/30000',
	v_VehicleCoverageLimit = '20-40', '20000/40000',
	v_VehicleCoverageLimit = '25-50', '25000/50000',
	v_VehicleCoverageLimit = '30-60', '30000/60000',
	v_VehicleCoverageLimit = '50-100', '50000/100000',
	v_VehicleCoverageLimit = '100-300', '100000/300000',
	v_VehicleCoverageLimit = '250-500', '250000/500000',
	v_VehicleCoverageLimit = '300-300', '300000/300000',
	v_VehicleCoverageLimit = '300-500', '300000/500000',
	v_VehicleCoverageLimit = '500-500', '500000/500000',
	v_VehicleCoverageLimit = '500-1M', '500000/1000000',
	v_VehicleCoverageLimit = '1M-1M', '1000000/1000000',
	'N/A'),
	i_VehicleCoverageCode = '11', DECODE(TRUE,
	v_VehicleCoverageLimit = '7.5', '7500',
	v_VehicleCoverageLimit = '15', '15000',
	v_VehicleCoverageLimit = '60P', '60000',
	v_VehicleCoverageLimit = '100P', '100000',
	v_VehicleCoverageLimit = '250P', '250000',
	IN(v_VehicleCoverageLimit, '300', '300P'), '300000',
	v_VehicleCoverageLimit = '350P', '350000',
	v_VehicleCoverageLimit = '500P', '500000',
	v_VehicleCoverageLimit = '750P', '750000',
	v_VehicleCoverageLimit = '1MP', '1000000',
	'N/A'),
	i_VehicleCoverageCode = '14' AND i_sar_state = '22', DECODE(TRUE,
	IN(v_VehicleCoverageLimit, 'Y', 'AY'), '40000',
	IN(v_VehicleCoverageLimit, '50', 'A50'), '50000',
	IN(v_VehicleCoverageLimit, '60', 'A60'), '60000',
	IN(v_VehicleCoverageLimit, '70', 'A70'), '70000',
	IN(v_VehicleCoverageLimit, '75', 'A75'), '75000',
	'N/A'),
	i_VehicleCoverageCode = '14' AND i_sar_state = '15', DECODE(TRUE,
	v_VehicleCoverageLimit = 'Y', '0',
	v_VehicleCoverageLimit = '12', '12500',
	v_VehicleCoverageLimit = '27', '27500',
	'N/A'),
	i_VehicleCoverageCode = '14' AND i_sar_state = '16', DECODE(TRUE,
	v_VehicleCoverageLimit = 'Y', '10000',
	v_VehicleCoverageLimit = '10', '10000',
	v_VehicleCoverageLimit = '20', '20000',
	v_VehicleCoverageLimit = '30', '30000',
	v_VehicleCoverageLimit = '40', '40000',
	v_VehicleCoverageLimit = '65', '65000',
	v_VehicleCoverageLimit = '90', '90000',
	'N/A'),
	'N/A') AS o_LimitValue,
	i_pif_4514_stage_id AS o_pif_4514_state_id
	FROM JNR_CA
),
SQ_Pif43RXGLStage AS (
	SELECT
	Pif43RXGLStage.Pmdrxg1FringeLimit1,
	Pif43RXGLStage.Pmdrxg1FringeLimit2,
	Pif43RXGLStage.Pmdrxg1FringeLimit3,
	Pif43RXGLStage.Pmdrxg1VolPdOcc,
	Pif43RXGLStage.Pmdrxg1VolPdAgg,
	pif_4514_stage.pif_4514_stage_id
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_4514_stage pif_4514_stage
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43RXGLStage Pif43RXGLStage
	ON pif_4514_stage.pif_symbol=Pif43RXGLStage.PifSymbol
	and pif_4514_stage.pif_module=Pif43RXGLStage.PifModule
	and pif_4514_stage.pif_policy_number=Pif43RXGLStage.pifpolicynumber
	and  Pif43RXGLStage.Pmdrxg1PmsDefGlSubline=pif_4514_stage.sar_risk_unit_group
	and Pif43RXGLStage.Pmdrxg1RiskTypeInd=substring(pif_4514_stage.sar_seq_rsk_unt_a,2,1)
	and pif_4514_stage.sar_insurance_line='GL'
	WHERE pif_4514_stage.logical_flag IN ('0','1','2','3')
	@{pipeline().parameters.WHERE_CLAUSE_RXGL}
),
EXP_Values_RXGL AS (
	SELECT
	Pmdrxg1FringeLimit1 AS i_Pmdrxg1FringeLimit1,
	Pmdrxg1FringeLimit2 AS i_Pmdrxg1FringeLimit2,
	Pmdrxg1FringeLimit3 AS i_Pmdrxg1FringeLimit3,
	Pmdrxg1VolPdOcc AS i_Pmdrxg1VolPdOcc,
	Pmdrxg1VolPdAgg AS i_Pmdrxg1VolPdAgg,
	pif_4514_stage_id,
	'MedicalPaymentLimit' AS o_type1,
	-- *INF*: IIF(ISNULL(i_Pmdrxg1FringeLimit1) OR LENGTH(i_Pmdrxg1FringeLimit1)=0 OR IS_SPACES(i_Pmdrxg1FringeLimit1) OR NOT IS_NUMBER(i_Pmdrxg1FringeLimit1),'0',TO_CHAR(TO_DECIMAL(i_Pmdrxg1FringeLimit1)))
	IFF(i_Pmdrxg1FringeLimit1 IS NULL OR LENGTH(i_Pmdrxg1FringeLimit1) = 0 OR IS_SPACES(i_Pmdrxg1FringeLimit1) OR NOT IS_NUMBER(i_Pmdrxg1FringeLimit1), '0', TO_CHAR(TO_DECIMAL(i_Pmdrxg1FringeLimit1))) AS o_value1,
	'DamageToPremisesRentedToYouLimit' AS o_type2,
	-- *INF*: IIF(ISNULL(i_Pmdrxg1FringeLimit2) OR LENGTH(i_Pmdrxg1FringeLimit2)=0 OR IS_SPACES(i_Pmdrxg1FringeLimit2) OR NOT IS_NUMBER(i_Pmdrxg1FringeLimit2),'0',TO_CHAR(TO_DECIMAL(i_Pmdrxg1FringeLimit2)))
	IFF(i_Pmdrxg1FringeLimit2 IS NULL OR LENGTH(i_Pmdrxg1FringeLimit2) = 0 OR IS_SPACES(i_Pmdrxg1FringeLimit2) OR NOT IS_NUMBER(i_Pmdrxg1FringeLimit2), '0', TO_CHAR(TO_DECIMAL(i_Pmdrxg1FringeLimit2))) AS o_value2,
	-- *INF*: iif(i_Pmdrxg1FringeLimit3>'0000000000','PersonalInjuryAndAdvertisingLiabilityLimit','PersonalInjuryLiabilityLimit')
	IFF(i_Pmdrxg1FringeLimit3 > '0000000000', 'PersonalInjuryAndAdvertisingLiabilityLimit', 'PersonalInjuryLiabilityLimit') AS o_type3,
	-- *INF*: IIF(ISNULL(i_Pmdrxg1FringeLimit3) OR LENGTH(i_Pmdrxg1FringeLimit3)=0 OR IS_SPACES(i_Pmdrxg1FringeLimit3) OR NOT IS_NUMBER(i_Pmdrxg1FringeLimit3),'0',TO_CHAR(TO_DECIMAL(i_Pmdrxg1FringeLimit3)))
	IFF(i_Pmdrxg1FringeLimit3 IS NULL OR LENGTH(i_Pmdrxg1FringeLimit3) = 0 OR IS_SPACES(i_Pmdrxg1FringeLimit3) OR NOT IS_NUMBER(i_Pmdrxg1FringeLimit3), '0', TO_CHAR(TO_DECIMAL(i_Pmdrxg1FringeLimit3))) AS o_value3,
	'VoluntaryPropertyDamageOccurrenceLimit' AS o_type4,
	-- *INF*: IIF(ISNULL(i_Pmdrxg1VolPdOcc),'0',TO_CHAR(i_Pmdrxg1VolPdOcc))
	IFF(i_Pmdrxg1VolPdOcc IS NULL, '0', TO_CHAR(i_Pmdrxg1VolPdOcc)) AS o_value4,
	'VoluntaryPropertyDamageAggregateLimit' AS o_type5,
	-- *INF*: IIF(ISNULL(i_Pmdrxg1VolPdAgg),'0',TO_CHAR(i_Pmdrxg1VolPdAgg))
	IFF(i_Pmdrxg1VolPdAgg IS NULL, '0', TO_CHAR(i_Pmdrxg1VolPdAgg)) AS o_value5
	FROM SQ_Pif43RXGLStage
),
NRM_Unpivot_Type_Value AS (
),
SQ_PifDept1553Stage AS (
	with PifDept1553Stage as (select PifSymbol,
	PifPolicyNumber,
	PifModule,
	DECLPTFormNumber,
	DECLPTSeqSameForm,
	DECLPTSeq0098,
	ltrim(rtrim(DECLPTText1701))+' '+ltrim(rtrim(DECLPTText71791))+' '+ltrim(rtrim(DECLPTText1702))+' '+ltrim(rtrim(DECLPTText71792)) as SourceValue
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PifDept1553Stage
	WHERE  LEFT(PifSymbol,2) in ('CU','NU')
	AND RIGHT(DECLPTFormNumber,2)='01' and DECLPTSeq0098 in (3,4,5,6) 
	union all
	select PifSymbol,
	PifPolicyNumber,
	PifModule,
	DECLPTFormNumber,
	DECLPTSeqSameForm,
	DECLPTSeq0098,
	ltrim(rtrim(DECLPTText1701))+' '+ltrim(rtrim(DECLPTText71791)) as SourceValue
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PifDept1553Stage
	WHERE  LEFT(PifSymbol,2) in ('CU','NU') AND RIGHT(DECLPTFormNumber,2) IN ('02','03')
	union all
	select PifSymbol,
	PifPolicyNumber,
	PifModule,
	DECLPTFormNumber,
	DECLPTSeqSameForm,
	DECLPTSeq0098,
	ltrim(rtrim(DECLPTText1702))+' '+ltrim(rtrim(DECLPTText71792))  as SourceValue
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.PifDept1553Stage
	WHERE  LEFT(PifSymbol,2) in ('CU','NU') AND RIGHT(DECLPTFormNumber,2) IN ('02','03')
	)
	
	select b.pif_4514_stage_id as pif_4514_stage_id,
	a.PifSymbol as PifSymbol,
	a.PifPolicyNumber as PifPolicyNumber,
	a.PifModule as PifModule,
	ltrim(rtrim(replace(replace(replace(replace(replace(replace(
	case when charindex('EACH OCCURRENCE LIMIT',SourceValue )>0
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('EACH OCCURRENCE LIMIT',SourceValue))+1,
	          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('EACH OCCURRENCE LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('EACH OCCURRENCE LIMIT',SourceValue))-1)
		 when charindex('PER OCCURRENCE LIMIT',SourceValue )>0
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('PER OCCURRENCE LIMIT',SourceValue))+1,
	          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('PER OCCURRENCE LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('PER OCCURRENCE LIMIT',SourceValue))-1)          
	     
		 when charindex('GENERAL AGGREGATE LIMIT',SourceValue )>0
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('GENERAL AGGREGATE LIMIT',SourceValue))+1,
	          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('GENERAL AGGREGATE LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('GENERAL AGGREGATE LIMIT',SourceValue))-1)
	                
	     when charindex('GENERAL AGGREGATE',SourceValue )>0
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('GENERAL AGGREGATE',SourceValue))+1,
	          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('GENERAL AGGREGATE',SourceValue)))-CHARINDEX('@',SourceValue,charindex('GENERAL AGGREGATE',SourceValue))-1)
	
		 when charindex('UNDERINSURED MOTORISTS COVERAGE EACH ACCIDENT LIMIT',SourceValue )>0
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('UNDERINSURED MOTORISTS COVERAGE EACH ACCIDENT LIMIT',SourceValue))+1,
	          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('UNDERINSURED MOTORISTS COVERAGE EACH ACCIDENT LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('UNDERINSURED MOTORISTS COVERAGE EACH ACCIDENT LIMIT',SourceValue))-1)
	
		 when charindex('UNINSURED MOTORISTS COVERAGE EACH ACCIDENT LIMIT',SourceValue )>0
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('UNINSURED MOTORISTS COVERAGE EACH ACCIDENT LIMIT',SourceValue))+1,
	          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('UNINSURED MOTORISTS COVERAGE EACH ACCIDENT LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('UNINSURED MOTORISTS COVERAGE EACH ACCIDENT LIMIT',SourceValue))-1)
			
		 when charindex('PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT',SourceValue )>0
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT',SourceValue))+1,
	          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT',SourceValue))-1)
	     
		 when charindex('PERSONAL INJURY AND ADVERTISING INJURY LIMIT',SourceValue )>0
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('PERSONAL INJURY AND ADVERTISING INJURY LIMIT',SourceValue))+1,
	          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('PERSONAL INJURY AND ADVERTISING INJURY LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('PERSONAL INJURY AND ADVERTISING INJURY LIMIT',SourceValue))-1)
	
		 when charindex('PERSONAL AND ADVERTISING INJURY LIMIT',SourceValue )>0
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('PERSONAL AND ADVERTISING INJURY LIMIT',SourceValue))+1,
	          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('PERSONAL AND ADVERTISING INJURY LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('PERSONAL AND ADVERTISING INJURY LIMIT',SourceValue))-1)
	
		 when charindex('PERSONAL AND ADVERTISING INJURY',SourceValue )>0
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('PERSONAL AND ADVERTISING INJURY',SourceValue))+1,
	          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('PERSONAL AND ADVERTISING INJURY',SourceValue)))-CHARINDEX('@',SourceValue,charindex('PERSONAL AND ADVERTISING INJURY',SourceValue))-1)
	
		 when charindex('PERSONAL AND ADVERTISING LIMIT',SourceValue )>0
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('PERSONAL AND ADVERTISING LIMIT',SourceValue))+1,
	          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('PERSONAL AND ADVERTISING LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('PERSONAL AND ADVERTISING LIMIT',SourceValue))-1)
	
	     when charindex('ILLINOIS AND OHIO',SourceValue )>0
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('ILLINOIS AND OHIO',SourceValue))+1,
	          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('ILLINOIS AND OHIO',SourceValue)))-CHARINDEX('@',SourceValue,charindex('ILLINOIS AND OHIO',SourceValue))-1)
	
	     when charindex('EACH CLAIM LIMIT',SourceValue )>0
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('EACH CLAIM LIMIT',SourceValue))+1,
	          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('EACH CLAIM LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('EACH CLAIM LIMIT',SourceValue))-1)
	
	     when charindex('AGGREGATE - GARAGE OPERATIONS OTHER THAN AUTO ONLY',SourceValue )>0
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('AGGREGATE - GARAGE OPERATIONS OTHER THAN AUTO ONLY',SourceValue))+1,
	          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('AGGREGATE - GARAGE OPERATIONS OTHER THAN AUTO ONLY',SourceValue)))-CHARINDEX('@',SourceValue,charindex('AGGREGATE - GARAGE OPERATIONS OTHER THAN AUTO ONLY',SourceValue))-1)
	
	     when charindex('EACH ACCIDENT - GARAGE OPERATIONS AUTO ONLY',SourceValue )>0
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('EACH ACCIDENT - GARAGE OPERATIONS AUTO ONLY',SourceValue))+1,
	          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('EACH ACCIDENT - GARAGE OPERATIONS AUTO ONLY',SourceValue)))-CHARINDEX('@',SourceValue,charindex('EACH ACCIDENT - GARAGE OPERATIONS AUTO ONLY',SourceValue))-1)
	
	     when charindex('EACH ACCIDENT - GARAGE OPERATIONS OTHER THAN AUTO ONLY',SourceValue )>0
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('EACH ACCIDENT - GARAGE OPERATIONS OTHER THAN AUTO ONLY',SourceValue))+1,
	          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('EACH ACCIDENT - GARAGE OPERATIONS OTHER THAN AUTO ONLY',SourceValue)))-CHARINDEX('@',SourceValue,charindex('EACH ACCIDENT - GARAGE OPERATIONS OTHER THAN AUTO ONLY',SourceValue))-1)
	
	     when charindex('AGGREGATE LIMIT',SourceValue )>0 			
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('AGGREGATE LIMIT',SourceValue))+1, 			
			  CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('AGGREGATE LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('AGGREGATE LIMIT',SourceValue))-1)
	     
	     when charindex('BODILY INJURY BY ACCIDENT:  EACH ACCIDENT',SourceValue )>0 			
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('BODILY INJURY BY ACCIDENT:  EACH ACCIDENT',SourceValue))+1, 			
			  CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('BODILY INJURY BY ACCIDENT:  EACH ACCIDENT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('BODILY INJURY BY ACCIDENT:  EACH ACCIDENT',SourceValue))-1)
		 
		 when charindex('BODILY INJURY BY DISEASE:   POLICY LIMIT',SourceValue )>0 			
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('BODILY INJURY BY DISEASE:   POLICY LIMIT',SourceValue))+1, 			
			  CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('BODILY INJURY BY DISEASE:   POLICY LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('BODILY INJURY BY DISEASE:   POLICY LIMIT',SourceValue))-1)
		
		 when charindex('BODILY INJURY BY DISEASE:   EACH EMPLOYEE',SourceValue )>0 			
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('BODILY INJURY BY DISEASE:   EACH EMPLOYEE',SourceValue))+1, 			
			  CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('BODILY INJURY BY DISEASE:   EACH EMPLOYEE',SourceValue)))-CHARINDEX('@',SourceValue,charindex('BODILY INJURY BY DISEASE:   EACH EMPLOYEE',SourceValue))-1)     
	
	     when charindex('PROPERTY DAMAGE-EACH ACCIDENT',SourceValue )>0 			
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('PROPERTY DAMAGE-EACH ACCIDENT',SourceValue))+1, 			
			  CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('PROPERTY DAMAGE-EACH ACCIDENT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('PROPERTY DAMAGE-EACH ACCIDENT',SourceValue))-1)     
	   
	     when charindex('PROPERTY DAMAGE -EACH ACCIDENT',SourceValue )>0 			
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('PROPERTY DAMAGE -EACH ACCIDENT',SourceValue))+1, 			
			  CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('PROPERTY DAMAGE -EACH ACCIDENT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('PROPERTY DAMAGE -EACH ACCIDENT',SourceValue))-1)     
	     
	     when charindex('PROPERTY DAMAGE- EACH ACCIDENT',SourceValue )>0 			
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('PROPERTY DAMAGE- EACH ACCIDENT',SourceValue))+1, 			
			  CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('PROPERTY DAMAGE- EACH ACCIDENT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('PROPERTY DAMAGE- EACH ACCIDENT',SourceValue))-1)     
	     
	     when charindex('PROPERTY DAMAGE EACH ACCIDENT',SourceValue )>0 			
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('PROPERTY DAMAGE EACH ACCIDENT',SourceValue))+1, 			
			  CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('PROPERTY DAMAGE EACH ACCIDENT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('PROPERTY DAMAGE EACH ACCIDENT',SourceValue))-1)     
	     
	     when charindex('PROPERTY DAMAGE: EACH ACCIDENT',SourceValue )>0 			
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('PROPERTY DAMAGE: EACH ACCIDENT',SourceValue))+1, 			
			  CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('PROPERTY DAMAGE: EACH ACCIDENT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('PROPERTY DAMAGE: EACH ACCIDENT',SourceValue))-1)     
	     
	     when charindex('PROPERTY DAMAGE/EACH ACCIDENT',SourceValue )>0 			
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('PROPERTY DAMAGE/EACH ACCIDENT',SourceValue))+1, 			
			  CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('PROPERTY DAMAGE/EACH ACCIDENT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('PROPERTY DAMAGE/EACH ACCIDENT',SourceValue))-1)     
	     
	     when charindex('PD - EACH ACCIDENT',SourceValue )>0 			
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('PD - EACH ACCIDENT',SourceValue))+1, 			
			  CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('PD - EACH ACCIDENT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('PD - EACH ACCIDENT',SourceValue))-1)     
	  
	     when charindex('EACH ACCIDENT-PROPERTY DAMAGE',SourceValue )>0 			
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('EACH ACCIDENT-PROPERTY DAMAGE',SourceValue))+1, 			
			  CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('EACH ACCIDENT-PROPERTY DAMAGE',SourceValue)))-CHARINDEX('@',SourceValue,charindex('EACH ACCIDENT-PROPERTY DAMAGE',SourceValue))-1)     
		
		 when charindex('PROP DAMAGE LIAB-EACH ACCIDENT',SourceValue )>0 			
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('PROP DAMAGE LIAB-EACH ACCIDENT',SourceValue))+1, 			
			  CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('PROP DAMAGE LIAB-EACH ACCIDENT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('PROP DAMAGE LIAB-EACH ACCIDENT',SourceValue))-1)     
		
		 when charindex('BODILY INJURY - EACH PERSON',SourceValue )>0 			
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('BODILY INJURY - EACH PERSON',SourceValue))+1, 			
			  CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('BODILY INJURY - EACH PERSON',SourceValue)))-CHARINDEX('@',SourceValue,charindex('BODILY INJURY - EACH PERSON',SourceValue))-1)     
	     
		 when charindex('BODILY INJURY-EACH PERSON',SourceValue )>0 			
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('BODILY INJURY-EACH PERSON',SourceValue))+1, 			
			  CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('BODILY INJURY-EACH PERSON',SourceValue)))-CHARINDEX('@',SourceValue,charindex('BODILY INJURY-EACH PERSON',SourceValue))-1)     
	     	 
		 when charindex('BODILY INJURY: EACH PERSON',SourceValue )>0 			
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('BODILY INJURY: EACH PERSON',SourceValue))+1, 			
			  CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('BODILY INJURY: EACH PERSON',SourceValue)))-CHARINDEX('@',SourceValue,charindex('BODILY INJURY: EACH PERSON',SourceValue))-1)     
	    
	     when charindex('EACH PERSON',SourceValue )>0 			
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('EACH PERSON',SourceValue))+1, 			
			  CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('EACH PERSON',SourceValue)))-CHARINDEX('@',SourceValue,charindex('EACH PERSON',SourceValue))-1)
	 	  
		 when charindex('EACH EMPLOYEE',SourceValue )>0
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('EACH EMPLOYEE',SourceValue))+1,
	          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('EACH EMPLOYEE',SourceValue)))-CHARINDEX('@',SourceValue,charindex('EACH EMPLOYEE',SourceValue))-1)
	
		 when charindex('EACH COMMON CAUSE',SourceValue )>0
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('EACH COMMON CAUSE',SourceValue))+1,
	          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('EACH COMMON CAUSE',SourceValue)))-CHARINDEX('@',SourceValue,charindex('EACH COMMON CAUSE',SourceValue))-1)
	
		 when charindex('EACH ACCIDENT',SourceValue )>0
	     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('EACH ACCIDENT',SourceValue))+1,
	          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('EACH ACCIDENT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('EACH ACCIDENT',SourceValue))-1)
	
		 else 'N/A'
	
	end,',',''),' ',''),'.',''),'O','0'),'\',''),'$','')))	  as VALUE,
	
	case when charindex('EACH OCCURRENCE LIMIT',SourceValue )>0
	     OR charindex('PER OCCURRENCE LIMIT',SourceValue )>0
	     then 'EACH OCCURRENCE LIMIT'
	     when charindex('UNDERINSURED MOTORISTS COVERAGE EACH ACCIDENT LIMIT',SourceValue )>0
	     then 'UNDERINSURED MOTORISTS COVERAGE EACH ACCIDENT LIMIT'
	     when charindex('UNINSURED MOTORISTS COVERAGE EACH ACCIDENT LIMIT',SourceValue )>0
	     then 'UNINSURED MOTORISTS COVERAGE EACH ACCIDENT LIMIT'
	     when charindex('PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT',SourceValue )>0
	     then 'PRODUCTS-COMPLETED OPERATIONS AGGREGATE LIMIT'
	     when charindex('PERSONAL INJURY AND ADVERTISING INJURY LIMIT',SourceValue )>0
	     OR charindex('PERSONAL AND ADVERTISING INJURY LIMIT',SourceValue )>0
	     OR charindex('PERSONAL AND ADVERTISING INJURY',SourceValue )>0
	     OR charindex('PERSONAL AND ADVERTISING LIMIT',SourceValue )>0
	     then 'PERSONAL INJURY AND ADVERTISING INJURY LIMIT'
	     when charindex('ILLINOIS AND OHIO',SourceValue )>0
	     then 'ILLINOIS AND OHIO'
	     when charindex('EACH CLAIM LIMIT',SourceValue )>0
	     then 'EACH CLAIM LIMIT'
	     when charindex('AGGREGATE - GARAGE OPERATIONS OTHER THAN AUTO ONLY',SourceValue )>0
	     then 'AGGREGATE - GARAGE OPERATIONS OTHER THAN AUTO ONLY'
	     when charindex('EACH ACCIDENT - GARAGE OPERATIONS AUTO ONLY',SourceValue )>0
	     then 'EACH ACCIDENT - GARAGE OPERATIONS AUTO ONLY'
	     when charindex('EACH ACCIDENT - GARAGE OPERATIONS OTHER THAN AUTO ONLY',SourceValue )>0
	     then 'EACH ACCIDENT - GARAGE OPERATIONS OTHER THAN AUTO ONLY' 
	     when charindex('AGGREGATE LIMIT',SourceValue )>0 	
	     OR charindex('GENERAL AGGREGATE LIMIT',SourceValue )>0
	     OR charindex('GENERAL AGGREGATE',SourceValue )>0	
		 then 'AGGREGATE LIMIT' 
		 when CHARINDEX('BODILY INJURY BY ACCIDENT:  EACH ACCIDENT',SourceValue)>0
		 then 'BODILY INJURY BY ACCIDENT:  EACH ACCIDENT'
		 when CHARINDEX('BODILY INJURY BY DISEASE:   POLICY LIMIT',SourceValue)>0
		 then 'BODILY INJURY BY DISEASE:   POLICY LIMIT'
		 when CHARINDEX('BODILY INJURY BY DISEASE:   EACH EMPLOYEE',SourceValue)>0
		 then 'BODILY INJURY BY DISEASE:   EACH EMPLOYEE'
	 	 when CHARINDEX('PROPERTY DAMAGE-EACH ACCIDENT',SourceValue)>0
	 	 OR CHARINDEX('PROPERTY DAMAGE-EACH ACCIDENT',SourceValue)>0
	 	 OR CHARINDEX('PROPERTY DAMAGE -EACH ACCIDENT',SourceValue)>0
	 	 OR CHARINDEX('PROPERTY DAMAGE- EACH ACCIDENT',SourceValue)>0
	 	 OR CHARINDEX('PROPERTY DAMAGE EACH ACCIDENT',SourceValue)>0
	 	 OR CHARINDEX('PROPERTY DAMAGE: EACH ACCIDENT',SourceValue)>0
	 	 OR CHARINDEX('PROPERTY DAMAGE/EACH ACCIDENT',SourceValue)>0
	 	 OR CHARINDEX('EACH ACCIDENT-PROPERTY DAMAGE',SourceValue)>0
	 	 OR CHARINDEX('PD - EACH ACCIDENT',SourceValue)>0 
	 	 OR CHARINDEX('PROP DAMAGE LIAB-EACH ACCIDENT',SourceValue)>0 
	       OR CHARINDEX('EACH ACCIDENT',SourceValue)>0
		 then 'PROPERTY DAMAGE - EACH ACCIDENT'   
		 when charindex('BODILY INJURY - EACH PERSON',SourceValue )>0 			
		 OR charindex('BODILY INJURY-EACH PERSON',SourceValue )>0 			  	 
		 OR charindex('BODILY INJURY: EACH PERSON',SourceValue )>0 	
		 OR charindex('EACH PERSON',SourceValue )>0 	
		 THEN 'BODILY INJURY - EACH PERSON'	
		 when charindex('EACH EMPLOYEE',SourceValue )>0
	       then 'EACH EMPLOYEE'
	  when charindex('EACH COMMON CAUSE',SourceValue )>0
	       then 'EACH COMMON CAUSE LIMIT'     	
	      ELSE 'N/A'end TYPE,
	
	LTRIM(RTRIM(DECLPTFormNumber)) as DECLPTFormNumber,
	a.DECLPTSeqSameForm as DECLPTSeqSameForm,
	a.DECLPTSeq0098 as DECLPTSeq0098
	
	from PifDept1553Stage a
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_4514_stage b
	ON  a.PifModule=b.pif_module
	and a.PifPolicyNumber=b.pif_policy_number
	and a.PifSymbol=b.pif_symbol
	WHERE b.logical_flag IN ('0','1','2','3') and left(b.pif_symbol,2) in ('CU','NU')
	@{pipeline().parameters.WHERE_CLAUSE_DEPT}
),
EXP_Default_Value_Dept1553 AS (
	SELECT
	PifSymbol AS i_PifSymbol,
	PifPolicyNumber AS i_PifPolicyNumber,
	PifModule AS i_PifModule,
	value AS i_value,
	type AS i_type,
	DECLPTFormNumber AS i_DECLPTFormNumber,
	DECLPTSeqSameForm AS i_DECLPTSeqSameForm,
	DECLPTSeq0098 AS i_DECLPTSeq0098,
	pif_4514_stage_id,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_type)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_type) AS v_type,
	-- *INF*: :LKP.LKP_PIFDEPT1553STAGE_UNDERLYINGINSURANCELINE(i_PifSymbol, i_PifPolicyNumber, i_PifModule, i_DECLPTFormNumber, i_DECLPTSeqSameForm, i_DECLPTSeq0098)
	LKP_PIFDEPT1553STAGE_UNDERLYINGINSURANCELINE_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.UnderlyingInsuranceLine AS v_UnderlyingInsuranceLine,
	-- *INF*: :LKP.LKP_PIFDEPT1553STAGE_UNDERLYINGCOMPANYNAME(i_PifSymbol, i_PifPolicyNumber, i_PifModule, i_DECLPTFormNumber, i_DECLPTSeqSameForm, i_DECLPTSeq0098)
	LKP_PIFDEPT1553STAGE_UNDERLYINGCOMPANYNAME_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.UnderlyingCompanyName AS v_UnderlyingCompanyName,
	-- *INF*: :LKP.LKP_PIFDEPT1553STAGE_UNDERLYINGPOLICYKEY(i_PifSymbol, i_PifPolicyNumber, i_PifModule, i_DECLPTFormNumber, i_DECLPTSeqSameForm, i_DECLPTSeq0098)
	LKP_PIFDEPT1553STAGE_UNDERLYINGPOLICYKEY_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.UnderlyingPolicyKey AS v_UnderlyingPolicyKey,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(v_UnderlyingInsuranceLine) OR v_UnderlyingInsuranceLine='N/A',
	-- v_type,
	-- v_UnderlyingInsuranceLine='Umbrella'  AND v_type<>'N/A',
	-- v_UnderlyingInsuranceLine || ' ' || v_type,
	-- IN(SUBSTR(i_DECLPTFormNumber,LENGTH(i_DECLPTFormNumber)-1,2),'02','03') AND v_type<>'N/A',
	-- 'Underlying-' || v_UnderlyingInsuranceLine || ' ' || v_type,
	-- v_type
	-- )
	DECODE(TRUE,
	v_UnderlyingInsuranceLine IS NULL OR v_UnderlyingInsuranceLine = 'N/A', v_type,
	v_UnderlyingInsuranceLine = 'Umbrella' AND v_type <> 'N/A', v_UnderlyingInsuranceLine || ' ' || v_type,
	IN(SUBSTR(i_DECLPTFormNumber, LENGTH(i_DECLPTFormNumber) - 1, 2), '02', '03') AND v_type <> 'N/A', 'Underlying-' || v_UnderlyingInsuranceLine || ' ' || v_type,
	v_type) AS v_CoverageLimitType,
	-- *INF*: IIF(ISNULL(i_value) OR NOT IS_NUMBER(i_value) OR LENGTH(i_value)=0 OR IS_SPACES(i_value),'0',TO_CHAR(TO_DECIMAL(i_value)))
	IFF(i_value IS NULL OR NOT IS_NUMBER(i_value) OR LENGTH(i_value) = 0 OR IS_SPACES(i_value), '0', TO_CHAR(TO_DECIMAL(i_value))) AS v_CoverageLimitValue,
	-- *INF*: v_CoverageLimitType
	-- 
	-- 
	-- 
	-- --IIF(IN(SUBSTR(i_DECLPTFormNumber,LENGTH(i_DECLPTFormNumber)-1,2),'02','03') AND v_type<>'N/A','Underlying - '||v_type,v_type)
	v_CoverageLimitType AS o_type,
	-- *INF*: v_CoverageLimitValue
	-- 
	-- 
	-- 
	-- --IIF(ISNULL(i_value) OR LENGTH(i_value)=0 OR IS_SPACES(i_value) OR NOT IS_NUMBER(i_value),'0',TO_CHAR(TO_DECIMAL(i_value)))
	v_CoverageLimitValue AS o_value,
	-- *INF*: IIF(ISNULL(v_UnderlyingCompanyName), 'N/A', v_UnderlyingCompanyName)
	IFF(v_UnderlyingCompanyName IS NULL, 'N/A', v_UnderlyingCompanyName) AS o_UnderlyingCompanyName,
	-- *INF*: IIF(ISNULL(v_UnderlyingPolicyKey), 'N/A', v_UnderlyingPolicyKey)
	IFF(v_UnderlyingPolicyKey IS NULL, 'N/A', v_UnderlyingPolicyKey) AS o_UnderlyingPolicyKey,
	v_UnderlyingInsuranceLine AS o_UnderlyingPolicyType,
	-- *INF*: IIF( NOT ISNULL(v_UnderlyingInsuranceLine) AND v_UnderlyingInsuranceLine<>'Umbrella' AND v_UnderlyingInsuranceLine<>'N/A' AND 
	-- (
	-- (v_CoverageLimitType<>'N/A' AND v_CoverageLimitValue<>'N/A' AND IS_NUMBER(v_CoverageLimitValue)=0) 
	-- OR 
	-- (IS_NUMBER(v_CoverageLimitValue)=1 AND TO_DECIMAL(v_CoverageLimitValue)>0)
	-- ), 1, 0)
	IFF(NOT v_UnderlyingInsuranceLine IS NULL AND v_UnderlyingInsuranceLine <> 'Umbrella' AND v_UnderlyingInsuranceLine <> 'N/A' AND ( ( v_CoverageLimitType <> 'N/A' AND v_CoverageLimitValue <> 'N/A' AND IS_NUMBER(v_CoverageLimitValue) = 0 ) OR ( IS_NUMBER(v_CoverageLimitValue) = 1 AND TO_DECIMAL(v_CoverageLimitValue) > 0 ) ), 1, 0) AS o_UnderlyingFlag
	FROM SQ_PifDept1553Stage
	LEFT JOIN LKP_PIFDEPT1553STAGE_UNDERLYINGINSURANCELINE LKP_PIFDEPT1553STAGE_UNDERLYINGINSURANCELINE_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098
	ON LKP_PIFDEPT1553STAGE_UNDERLYINGINSURANCELINE_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.PifSymbol = i_PifSymbol
	AND LKP_PIFDEPT1553STAGE_UNDERLYINGINSURANCELINE_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.PifPolicyNumber = i_PifPolicyNumber
	AND LKP_PIFDEPT1553STAGE_UNDERLYINGINSURANCELINE_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.PifModule = i_PifModule
	AND LKP_PIFDEPT1553STAGE_UNDERLYINGINSURANCELINE_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.DECLPTFormNumber = i_DECLPTFormNumber
	AND LKP_PIFDEPT1553STAGE_UNDERLYINGINSURANCELINE_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.DECLPTSeqSameForm = i_DECLPTSeqSameForm
	AND LKP_PIFDEPT1553STAGE_UNDERLYINGINSURANCELINE_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.DECLPTSeq0098 = i_DECLPTSeq0098

	LEFT JOIN LKP_PIFDEPT1553STAGE_UNDERLYINGCOMPANYNAME LKP_PIFDEPT1553STAGE_UNDERLYINGCOMPANYNAME_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098
	ON LKP_PIFDEPT1553STAGE_UNDERLYINGCOMPANYNAME_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.PifSymbol = i_PifSymbol
	AND LKP_PIFDEPT1553STAGE_UNDERLYINGCOMPANYNAME_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.PifPolicyNumber = i_PifPolicyNumber
	AND LKP_PIFDEPT1553STAGE_UNDERLYINGCOMPANYNAME_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.PifModule = i_PifModule
	AND LKP_PIFDEPT1553STAGE_UNDERLYINGCOMPANYNAME_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.DECLPTFormNumber = i_DECLPTFormNumber
	AND LKP_PIFDEPT1553STAGE_UNDERLYINGCOMPANYNAME_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.DECLPTSeqSameForm = i_DECLPTSeqSameForm
	AND LKP_PIFDEPT1553STAGE_UNDERLYINGCOMPANYNAME_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.DECLPTSeq0098 = i_DECLPTSeq0098

	LEFT JOIN LKP_PIFDEPT1553STAGE_UNDERLYINGPOLICYKEY LKP_PIFDEPT1553STAGE_UNDERLYINGPOLICYKEY_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098
	ON LKP_PIFDEPT1553STAGE_UNDERLYINGPOLICYKEY_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.PifSymbol = i_PifSymbol
	AND LKP_PIFDEPT1553STAGE_UNDERLYINGPOLICYKEY_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.PifPolicyNumber = i_PifPolicyNumber
	AND LKP_PIFDEPT1553STAGE_UNDERLYINGPOLICYKEY_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.PifModule = i_PifModule
	AND LKP_PIFDEPT1553STAGE_UNDERLYINGPOLICYKEY_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.DECLPTFormNumber = i_DECLPTFormNumber
	AND LKP_PIFDEPT1553STAGE_UNDERLYINGPOLICYKEY_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.DECLPTSeqSameForm = i_DECLPTSeqSameForm
	AND LKP_PIFDEPT1553STAGE_UNDERLYINGPOLICYKEY_i_PifSymbol_i_PifPolicyNumber_i_PifModule_i_DECLPTFormNumber_i_DECLPTSeqSameForm_i_DECLPTSeq0098.DECLPTSeq0098 = i_DECLPTSeq0098

),
RTR_Underlying AS (
	SELECT
	pif_4514_stage_id,
	o_type AS type,
	o_value AS value,
	o_UnderlyingCompanyName AS UnderlyingCompanyName,
	o_UnderlyingPolicyKey AS UnderLyingPolicyKey,
	o_UnderlyingPolicyType AS UnderlyingPolicyType,
	o_UnderlyingFlag AS UnderlyingFlag
	FROM EXP_Default_Value_Dept1553
),
RTR_Underlying_Underlying AS (SELECT * FROM RTR_Underlying WHERE UnderlyingFlag=1),
RTR_Underlying_DEFAULT1 AS (SELECT * FROM RTR_Underlying WHERE NOT ( (UnderlyingFlag=1) )),
UN_Dept_RXGL_4514 AS (
	SELECT o_type AS type, o_value AS value, pif_4514_stage_id
	FROM EXP_Assign_Value_4514
	UNION
	SELECT type, value, pif_4514_stage_id
	FROM RTR_Underlying_DEFAULT1
	UNION
	SELECT type, value, pif_4514_stage_id
	FROM NRM_Unpivot_Type_Value
	UNION
	SELECT o_LimitType AS type, o_LimitValue AS value, o_pif_4514_state_id AS pif_4514_stage_id
	FROM EXP_TRANS_CA
	UNION
	SELECT o_LimitType AS type, o_LimitValue AS value, pif_4514_stage_id
	FROM EXP_EPLI_AggregateLimit
	UNION
	SELECT o_LimitType AS type, o_LimitValue AS value, pif_4514_stage_id1 AS pif_4514_stage_id
	FROM EXP_EPLI_EachRelatedWrongfulEmploymentPractice
),
FIL_Invalid_Type AS (
	SELECT
	type, 
	value, 
	pif_4514_stage_id
	FROM UN_Dept_RXGL_4514
	WHERE (type<>'N/A' and value<>'N/A' and IS_NUMBER(value)=0) or (IS_NUMBER(value) and TO_DECIMAL(value)>0)
),
EXP_Calculate AS (
	SELECT
	type AS i_type,
	value AS i_value,
	-- *INF*: DECODE(TRUE,
	-- INSTR(i_type,'PersonalInjuryProtectionLimit',1,1)!=0 and INSTR(i_value,'Option',1,1)!=0, REVERSE(SUBSTR(REVERSE(i_value), 1, INSTR(REVERSE(i_value),' ', 1,1)-1)),
	-- INSTR(i_type,'PersonalInjuryProtectionLimit',1,1)!=0 and INSTR(i_value,'Basic',1,1)!=0, 'BasicPIP'
	-- , i_value)
	DECODE(TRUE,
	INSTR(i_type, 'PersonalInjuryProtectionLimit', 1, 1) != 0 AND INSTR(i_value, 'Option', 1, 1) != 0, REVERSE(SUBSTR(REVERSE(i_value), 1, INSTR(REVERSE(i_value), ' ', 1, 1) - 1)),
	INSTR(i_type, 'PersonalInjuryProtectionLimit', 1, 1) != 0 AND INSTR(i_value, 'Basic', 1, 1) != 0, 'BasicPIP',
	i_value) AS v_value,
	-- *INF*: DECODE(TRUE,
	-- INSTR(i_type,'PersonalInjuryProtectionLimit',1,1)!=0 and v_value='BasicPIP', 'PersonalInjuryProtectionBasicLimit',
	-- INSTR(i_type,'PersonalInjuryProtectionLimit',1,1)!=0 and v_value!='BasicPIP', 'PersonalInjuryProtectionExcessLimit'
	-- , i_type)
	DECODE(TRUE,
	INSTR(i_type, 'PersonalInjuryProtectionLimit', 1, 1) != 0 AND v_value = 'BasicPIP', 'PersonalInjuryProtectionBasicLimit',
	INSTR(i_type, 'PersonalInjuryProtectionLimit', 1, 1) != 0 AND v_value != 'BasicPIP', 'PersonalInjuryProtectionExcessLimit',
	i_type) AS v_type,
	v_type AS o_type,
	v_value AS o_value,
	pif_4514_stage_id
	FROM FIL_Invalid_Type
),
AGG_Type_Value AS (
	SELECT
	o_type AS type, 
	o_value AS value
	FROM EXP_Calculate
	QUALIFY ROW_NUMBER() OVER (PARTITION BY type, value ORDER BY NULL) = 1
),
LKP_CoverageLimit_CoverageLimitID AS (
	SELECT
	CoverageLimitId,
	CoverageLimitType,
	CoverageLimitValue
	FROM (
		SELECT 
			CoverageLimitId,
			CoverageLimitType,
			CoverageLimitValue
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageLimit
		WHERE SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageLimitType,CoverageLimitValue ORDER BY CoverageLimitId) = 1
),
SEQ_CoverageLimitID AS (
	CREATE SEQUENCE SEQ_CoverageLimitID
	START = 0
	INCREMENT = 1;
),
EXP_Set_CoverageLimitID AS (
	SELECT
	LKP_CoverageLimit_CoverageLimitID.CoverageLimitId AS lkp_CoverageLimitId,
	SEQ_CoverageLimitID.NEXTVAL AS i_NEXTVAL,
	AGG_Type_Value.type AS CoverageLimitType,
	AGG_Type_Value.value AS CoverageLimitValue,
	-- *INF*: IIF(ISNULL(lkp_CoverageLimitId),i_NEXTVAL,lkp_CoverageLimitId)
	IFF(lkp_CoverageLimitId IS NULL, i_NEXTVAL, lkp_CoverageLimitId) AS CoverageLimitId
	FROM AGG_Type_Value
	LEFT JOIN LKP_CoverageLimit_CoverageLimitID
	ON LKP_CoverageLimit_CoverageLimitID.CoverageLimitType = AGG_Type_Value.type AND LKP_CoverageLimit_CoverageLimitID.CoverageLimitValue = AGG_Type_Value.value
),
SRT_by_Type_Value_CoverageLimit AS (
	SELECT
	CoverageLimitType, 
	CoverageLimitValue, 
	CoverageLimitId
	FROM EXP_Set_CoverageLimitID
	ORDER BY CoverageLimitType ASC, CoverageLimitValue ASC
),
SRT_by_Type_Value_CoverageLimitBridge AS (
	SELECT
	o_type AS type, 
	o_value AS value, 
	pif_4514_stage_id
	FROM EXP_Calculate
	ORDER BY type ASC, value ASC
),
JNR_CoverageLimit_CoverageLimitBridge AS (SELECT
	SRT_by_Type_Value_CoverageLimitBridge.type AS i_type, 
	SRT_by_Type_Value_CoverageLimitBridge.value AS i_value, 
	SRT_by_Type_Value_CoverageLimitBridge.pif_4514_stage_id, 
	SRT_by_Type_Value_CoverageLimit.CoverageLimitType AS i_CoverageLimitType, 
	SRT_by_Type_Value_CoverageLimit.CoverageLimitValue AS i_CoverageLimitValue, 
	SRT_by_Type_Value_CoverageLimit.CoverageLimitId
	FROM SRT_by_Type_Value_CoverageLimitBridge
	INNER JOIN SRT_by_Type_Value_CoverageLimit
	ON SRT_by_Type_Value_CoverageLimit.CoverageLimitType = SRT_by_Type_Value_CoverageLimitBridge.type AND SRT_by_Type_Value_CoverageLimit.CoverageLimitValue = SRT_by_Type_Value_CoverageLimitBridge.value
),
LKP_WorkPremiumTransaction AS (
	SELECT
	PremiumTransactionAKId,
	PremiumTransactionStageId
	FROM (
		SELECT 
			PremiumTransactionAKId,
			PremiumTransactionStageId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction
		WHERE SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionStageId ORDER BY PremiumTransactionAKId) = 1
),
LKP_CoverageLimitBridge AS (
	SELECT
	CoverageLimitBridgeId,
	PremiumTransactionAKId,
	CoverageLimitId
	FROM (
		SELECT CLB.CoverageLimitBridgeId as CoverageLimitBridgeId,
		CLB.PremiumTransactionAKId as PremiumTransactionAKId,
		CLB.CoverageLimitId as CoverageLimitId 
		FROM dbo.CoverageLimitBridge CLB
		INNER JOIN WorkPremiumTransaction WPT ON CLB.PremiumTransactionAKId = WPT.PremiumTransactionAKId
		WHERE WPT.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId,CoverageLimitId ORDER BY CoverageLimitBridgeId) = 1
),
FIL_Insert_CoverageLimitBridge AS (
	SELECT
	LKP_CoverageLimitBridge.CoverageLimitBridgeId AS i_CoverageLimitBridgeId, 
	LKP_WorkPremiumTransaction.PremiumTransactionAKId, 
	JNR_CoverageLimit_CoverageLimitBridge.CoverageLimitId
	FROM JNR_CoverageLimit_CoverageLimitBridge
	LEFT JOIN LKP_CoverageLimitBridge
	ON LKP_CoverageLimitBridge.PremiumTransactionAKId = LKP_WorkPremiumTransaction.PremiumTransactionAKId AND LKP_CoverageLimitBridge.CoverageLimitId = JNR_CoverageLimit_CoverageLimitBridge.CoverageLimitId
	LEFT JOIN LKP_WorkPremiumTransaction
	ON LKP_WorkPremiumTransaction.PremiumTransactionStageId = JNR_CoverageLimit_CoverageLimitBridge.pif_4514_stage_id
	WHERE ISNULL(i_CoverageLimitBridgeId) AND PremiumTransactionAKId<>-1
),
AGG_Group_Count AS (
	SELECT
	PremiumTransactionAKId, 
	CoverageLimitId, 
	COUNT(1) AS o_CoverageLimitCount
	FROM FIL_Insert_CoverageLimitBridge
	GROUP BY PremiumTransactionAKId, CoverageLimitId
),
EXP_Set_MetaData_CoverageLimitBridge AS (
	SELECT
	PremiumTransactionAKId,
	CoverageLimitId,
	o_CoverageLimitCount AS CoverageLimitCount,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	'N/A' AS o_CoverageLimitControl
	FROM AGG_Group_Count
),
CoverageLimitBridge AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageLimitBridge
	(AuditID, SourceSystemID, CreatedDate, PremiumTransactionAKId, CoverageLimitId, CoverageLimitIDCount, CoverageLimitControl)
	SELECT 
	o_AuditID AS AUDITID, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	PREMIUMTRANSACTIONAKID, 
	COVERAGELIMITID, 
	CoverageLimitCount AS COVERAGELIMITIDCOUNT, 
	o_CoverageLimitControl AS COVERAGELIMITCONTROL
	FROM EXP_Set_MetaData_CoverageLimitBridge
),
LKP_PremiumTransaction AS (
	SELECT
	PremiumTransactionId,
	PremiumTransactionStageId
	FROM (
		SELECT PT.PremiumTransactionId as PremiumTransactionId,
		WPT.PremiumTransactionStageId as PremiumTransactionStageId
		FROM
		@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction WPT
		INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT
		ON WPT.PremiumTransactionAKID=PT.PremiumTransactionAKID
		AND PT.CurrentSnapshotFlag=1
		WHERE WPT.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionStageId ORDER BY PremiumTransactionId) = 1
),
AGG_Underlying AS (
	SELECT
	LKP_PremiumTransaction.PremiumTransactionId AS PremiumTransactionID, 
	RTR_Underlying_Underlying.type, 
	RTR_Underlying_Underlying.value, 
	RTR_Underlying_Underlying.UnderlyingCompanyName, 
	RTR_Underlying_Underlying.UnderLyingPolicyKey, 
	RTR_Underlying_Underlying.UnderlyingPolicyType
	FROM LKP_PremiumTransaction
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID, type, UnderlyingCompanyName, UnderLyingPolicyKey, UnderlyingPolicyType ORDER BY NULL) = 1
),
EXP_Underlying AS (
	SELECT
	PremiumTransactionID AS i_PremiumTransactionID,
	type AS i_type,
	value AS i_value,
	UnderlyingCompanyName AS i_UnderlyingCompanyName,
	UnderLyingPolicyKey AS i_UnderLyingPolicyKey,
	UnderlyingPolicyType AS i_UnderlyingPolicyType,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
	TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
	TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CURRENT_TIMESTAMP AS o_CreatedDate,
	CURRENT_TIMESTAMP AS o_ModifiedDate,
	i_PremiumTransactionID AS o_PremiumTransactionId,
	i_UnderlyingCompanyName AS o_UnderlyingInsuranceCompanyName,
	i_UnderLyingPolicyKey AS o_UnderlyingPolicyKey,
	i_UnderlyingPolicyType AS o_UnderlyingPolicyType,
	i_value AS o_UnderlyingPolicyLimit,
	i_type AS o_UnderlyingPolicyLimitType
	FROM AGG_Underlying
),
LKP_CoverageDetailUnderlyingPolicy AS (
	SELECT
	CoverageDetailUnderlyingPolicyId,
	PremiumTransactionId,
	UnderlyingInsuranceCompanyName,
	UnderlyingPolicyKey,
	UnderlyingPolicyType,
	UnderlyingPolicyLimitType
	FROM (
		SELECT CDUP.CoverageDetailUnderlyingPolicyId as CoverageDetailUnderlyingPolicyId,
		CDUP.PremiumTransactionId as PremiumTransactionId, 
		CDUP.UnderlyingInsuranceCompanyName as UnderlyingInsuranceCompanyName, 
		CDUP.UnderlyingPolicyKey as UnderlyingPolicyKey, 
		CDUP.UnderlyingPolicyType as UnderlyingPolicyType, 
		CDUP.UnderlyingPolicyLimitType as UnderlyingPolicyLimitType 
		 FROM dbo.CoverageDetailUnderlyingPolicy CDUP 
		 INNER JOIN dbo.PremiumTransaction PT ON CDUP.PremiumTransactionId = PT.Premiumtransactionid
		 INNER JOIN dbo.WorkPremiumTransaction WPT ON WPT.PremiumtransactionAKID = PT.PremiumtransactionAKID 
		AND PT.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionId,UnderlyingInsuranceCompanyName,UnderlyingPolicyKey,UnderlyingPolicyType,UnderlyingPolicyLimitType ORDER BY CoverageDetailUnderlyingPolicyId) = 1
),
RTR_Underlying_Insert AS (
	SELECT
	LKP_CoverageDetailUnderlyingPolicy.CoverageDetailUnderlyingPolicyId,
	EXP_Underlying.o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	EXP_Underlying.o_AuditID AS AuditID,
	EXP_Underlying.o_EffectiveDate AS EffectiveDate,
	EXP_Underlying.o_ExpirationDate AS ExpirationDate,
	EXP_Underlying.o_SourceSystemId AS SourceSystemId,
	EXP_Underlying.o_CreatedDate AS CreatedDate,
	EXP_Underlying.o_ModifiedDate AS ModifiedDate,
	EXP_Underlying.o_PremiumTransactionId AS PremiumTransactionId,
	EXP_Underlying.o_UnderlyingInsuranceCompanyName AS UnderlyingInsuranceCompanyName,
	EXP_Underlying.o_UnderlyingPolicyKey AS UnderlyingPolicyKey,
	EXP_Underlying.o_UnderlyingPolicyType AS UnderlyingPolicyType,
	EXP_Underlying.o_UnderlyingPolicyLimit AS UnderlyingPolicyLimit,
	EXP_Underlying.o_UnderlyingPolicyLimitType AS UnderlyingPolicyLimitType
	FROM EXP_Underlying
	LEFT JOIN LKP_CoverageDetailUnderlyingPolicy
	ON LKP_CoverageDetailUnderlyingPolicy.PremiumTransactionId = EXP_Underlying.o_PremiumTransactionId AND LKP_CoverageDetailUnderlyingPolicy.UnderlyingInsuranceCompanyName = EXP_Underlying.o_UnderlyingInsuranceCompanyName AND LKP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyKey = EXP_Underlying.o_UnderlyingPolicyKey AND LKP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyType = EXP_Underlying.o_UnderlyingPolicyType AND LKP_CoverageDetailUnderlyingPolicy.UnderlyingPolicyLimitType = EXP_Underlying.o_UnderlyingPolicyLimitType
),
RTR_Underlying_Insert_Insert AS (SELECT * FROM RTR_Underlying_Insert WHERE ISNULL(CoverageDetailUnderlyingPolicyId)),
TGT_CoverageDetailUnderlyingPolicy_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailUnderlyingPolicy
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, PremiumTransactionId, UnderlyingInsuranceCompanyName, UnderlyingPolicyKey, UnderlyingPolicyType, UnderlyingPolicyLimit, UnderlyingPolicyLimitType)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	PREMIUMTRANSACTIONID, 
	UNDERLYINGINSURANCECOMPANYNAME, 
	UNDERLYINGPOLICYKEY, 
	UNDERLYINGPOLICYTYPE, 
	UNDERLYINGPOLICYLIMIT, 
	UNDERLYINGPOLICYLIMITTYPE
	FROM RTR_Underlying_Insert_Insert
),
FIL_Insert_CoverageLimit AS (
	SELECT
	lkp_CoverageLimitId, 
	CoverageLimitType, 
	CoverageLimitValue, 
	CoverageLimitId
	FROM EXP_Set_CoverageLimitID
	WHERE ISNULL(lkp_CoverageLimitId)
),
EXP_Set_Metadata_CoverageLimit AS (
	SELECT
	CoverageLimitType,
	CoverageLimitValue,
	CoverageLimitId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	CoverageLimitType AS o_CoverageLimitType,
	CoverageLimitValue AS o_CoverageLimitValue
	FROM FIL_Insert_CoverageLimit
),
UPD_Insert_CoverageLimit AS (
	SELECT
	CoverageLimitId, 
	o_AuditID AS AuditID, 
	o_SourceSystemID AS SourceSystemID, 
	o_CreatedDate AS CreatedDate, 
	o_CoverageLimitType AS CoverageLimitType, 
	o_CoverageLimitValue AS CoverageLimitValue
	FROM EXP_Set_Metadata_CoverageLimit
),
CoverageLimit AS (
	SET IDENTITY_INSERT @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageLimit  ON
	INSERT @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageLimit(CoverageLimitId ,AuditID,SourceSystemID,CreatedDate,CoverageLimitType,CoverageLimitValue) 
	SELECT S.CoverageLimitId,S.AuditID,S.SourceSystemID, S.CreatedDate,S.CoverageLimitType, S.CoverageLimitValue
	FROM UPD_Insert_CoverageLimit S
),