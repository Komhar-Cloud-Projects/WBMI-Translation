WITH
SQ_pif_4514_stage AS (
	SELECT DISTINCT  
	pif_4514_stage_ID as pif_4514_stage_id,
	CASE
	WHEN sar_major_peril = '145' and sar_type_bureau = 'AP' and sar_insurance_line in ('CA','') THEN 
	(CASE WHEN sar_code_1+sar_code_2+sar_code_3 IN('703','710','715','755','734','735','756','757') THEN  'ComprehensiveFullGlassCoverageDeductible' 
	ELSE 'ComprehensiveDeductible' END)
	WHEN sar_major_peril = '170' and sar_type_bureau = 'AP' and sar_insurance_line in ('CA','') THEN  'CollisionDeductible'
	WHEN sar_major_peril = '130' and sar_insurance_line IN('CA','GA','') and sar_type_bureau = 'AN' THEN 'PersonalInjuryProtectionDeductible'
	WHEN sar_major_peril = '145' and sar_insurance_line = 'GA' and sar_type_bureau = 'AP' and sar_risk_unit_group = '116' and sar_unit = '971'THEN 'ComprehensiveDeductible'
	WHEN sar_major_peril = '170' and sar_insurance_line = 'GA' and sar_type_bureau = 'AP' THEN 'ComprehensiveDeductible'
	WHEN sar_major_peril = '171' and sar_type_bureau = 'AP' and sar_insurance_line in ('CA','') THEN 'BroadenedCollisionDeductible'
	WHEN sar_major_peril = '172' and sar_type_bureau = 'AP' and sar_insurance_line in ('CA','') THEN 'LimitedCollisionDeductible'
	WHEN D.Pif351StageId IS NOT NULL and sar_insurance_line in ('CA','') and sar_type_bureau in ('AL','AN','AP') THEN 'SingleLimitDeductible'
	WHEN sar_insurance_line='WC' THEN
	(CASE 
	WHEN Pif43IXUnmodWCRatingState = '13' and B.Pif43IXUnmodReportingClassCode = '9915' then  'CoinsuranceDeductible'
	WHEN Pif43IXUnmodWCRatingState = '13' and B.Pif43IXUnmodReportingClassCode = '9916' then  'CoinsuranceDeductible'
	WHEN Pif43IXUnmodWCRatingState = '13' and B.Pif43IXUnmodReportingClassCode = '9917' then  'CoinsuranceDeductible'
	WHEN Pif43IXUnmodWCRatingState = '13' and B.Pif43IXUnmodReportingClassCode = '9918' then  'CoinsuranceDeductible'
	WHEN Pif43IXUnmodWCRatingState = '13' and B.Pif43IXUnmodReportingClassCode = '9919' then  'CoinsuranceDeductible'
	ELSE 'MedicalDeductible' END)
	------------------------------------------------------
	WHEN A.sar_insurance_line IN('IM','CF','CR') THEN ISNULL(SDT.StandardDeductibleType,'N/A')
	WHEN A.sar_insurance_line='GL' THEN 
	(CASE
	WHEN not (LTRIM(RTRIM(A.sar_code_4)) is null OR LEN(LTRIM(RTRIM(A.sar_code_4)))=0)  THEN 'BodilyInjury'
	WHEN not (LTRIM(RTRIM(A.sar_code_6)) is null OR LEN(LTRIM(RTRIM(A.sar_code_6)))=0) THEN 'PropertyDamage'
	WHEN LTRIM(RTRIM(A.sar_class_1_4+A.sar_class_5_6)) = '22222' THEN 'EmploymentPracticesLiability'
	WHEN LTRIM(RTRIM(A.sar_major_peril)) = '550' THEN 'ProductWithdrawalLiability' 
	ELSE 'N/A' END)
	------------------------------------------------------
	ELSE 'N/A' END Type,
	
	CASE 
	WHEN sar_insurance_line = 'CF' THEN
	(CASE 
	WHEN sar_code_5 = '01' then 'FullCoverage'
	WHEN sar_code_5 = '05' then '500' 
	WHEN sar_code_5 = '07' then '1000'
	WHEN sar_code_5 = '08' then '2500' 
	WHEN sar_code_5 = '09' then '5000'
	WHEN sar_code_5 = '10' then '10000'
	WHEN sar_code_5 = '11' then '25000'
	WHEN sar_code_5 = '12' then '50000'
	WHEN sar_code_5 = '13' then '75000'
	ELSE 'N/A' END)
	WHEN sar_major_peril = '565' and sar_insurance_line = 'CR' THEN 
	(CASE 
	WHEN sar_code_7 = '0' then '0'
	WHEN sar_code_7 = '04' then '250'
	WHEN sar_code_7 = '05' then '500'
	WHEN sar_code_7 = '07' then '1000'
	WHEN sar_code_7 = '08' then '2500'
	WHEN sar_code_7 = '09' then '5000'
	WHEN sar_code_7 = '10' then '10000' 
	WHEN sar_code_7 = '11' then '25000'
	ELSE 'N/A' END)
	WHEN sar_major_peril = '565' and sar_insurance_line='CR' and sar_type_bureau = 'CR' THEN
	(CASE 
	WHEN sar_code_5 = '01' then '0'
	WHEN sar_code_5 = '02' then '50'
	WHEN sar_code_5 = '03' then '100'
	WHEN sar_code_5 = '04' then '250'
	WHEN sar_code_5 = '05' then '500'
	WHEN sar_code_5 = '06' then '750'
	WHEN sar_code_5 = '07' then '1000'
	WHEN sar_code_5 = '08' then '2500'
	WHEN sar_code_5 = '09' then '5000'
	WHEN sar_code_5 = '19' then '10000'
	WHEN sar_code_5 = '11' then '25000'
	ELSE 'N/A' END)
	WHEN sar_major_peril = '566' and sar_insurance_line='CR' and sar_type_bureau = 'FT' THEN
	(CASE 
	WHEN sar_code_5 = '01' THEN 'Blank'
	WHEN sar_code_5 = '02' THEN '50'
	WHEN sar_code_5 = '03' THEN '100'
	WHEN sar_code_5 = '04' THEN '250'
	WHEN sar_code_5 = '05' THEN '500'
	WHEN sar_code_5 = '06' THEN '750'
	WHEN sar_code_5 = '07' THEN '1000'
	WHEN sar_code_5 = '08' THEN '2500'
	WHEN sar_code_5 = '09' THEN '5000'
	WHEN sar_code_5 = '19' THEN '10000'
	WHEN sar_code_5 = '11' THEN '25000'
	ELSE 'N/A' END)
	WHEN sar_major_peril = '145' and sar_type_bureau = 'AP' and sar_insurance_line in ('CA','') THEN
	(CASE 
	WHEN sar_code_1 = '0' AND sar_code_2 = '0' AND sar_code_3 = '1' THEN '0'
	WHEN sar_code_1 = '0' AND sar_code_2 = '0' AND sar_code_3 = '3' THEN '50' 
	WHEN sar_code_1 = '0' AND sar_code_2 = '1' AND sar_code_3 = '0' THEN '100' 
	WHEN sar_code_1 = '0' AND sar_code_2 = '1' AND sar_code_3 = '5' THEN '200' 
	WHEN sar_code_1 = '0' AND sar_code_2 = '5' AND sar_code_3 = '5' THEN '250' 
	WHEN sar_code_1 = '7' AND sar_code_2 = '2' AND sar_code_3 = '6' THEN '500' 
	WHEN sar_code_1 = '7' AND sar_code_2 = '2' AND sar_code_3 = '7' THEN '1000' 
	WHEN sar_code_1 = '7' AND sar_code_2 = '3' AND sar_code_3 = '1' THEN '2000' 
	WHEN sar_code_1 = '7' AND sar_code_2 = '3' AND sar_code_3 = '7' THEN '5000' 
	WHEN sar_code_1 = '0' AND sar_code_2 = '9' AND sar_code_3 = '9' THEN '10000'
	WHEN sar_code_1+sar_code_2+sar_code_3='703' THEN '50'
	WHEN sar_code_1+sar_code_2+sar_code_3='710' THEN '100'
	WHEN sar_code_1+sar_code_2+sar_code_3='715' THEN '200'
	WHEN sar_code_1+sar_code_2+sar_code_3='755' THEN '250'
	WHEN sar_code_1+sar_code_2+sar_code_3='734' THEN '300'
	WHEN sar_code_1+sar_code_2+sar_code_3='735' THEN '400'
	WHEN sar_code_1+sar_code_2+sar_code_3='756' THEN '500'
	WHEN sar_code_1+sar_code_2+sar_code_3='757' THEN '1000'
	ELSE 'N/A' END)
	WHEN sar_major_peril = '170' and sar_type_bureau = 'AP' and sar_insurance_line in ('CA','') THEN
	(CASE 
	WHEN sar_code_1 = '0' AND sar_code_2 = '7' AND sar_code_3 = '4' THEN '100' 
	WHEN sar_code_1 = '0' AND sar_code_2 = '7' AND sar_code_3 = '5' THEN '150' 
	WHEN sar_code_1 = '0' AND sar_code_2 = '7' AND sar_code_3 = '6' THEN '250' 
	WHEN sar_code_1 = '0' AND sar_code_2 = '7' AND sar_code_3 = '7' THEN '500' 
	WHEN sar_code_1 = '0' AND sar_code_2 = '7' AND sar_code_3 = '8' THEN '1000'
	WHEN sar_code_1 = '0' AND sar_code_2 = '8' AND sar_code_3 = '4' THEN '2000' 
	WHEN sar_code_1 = '1' AND sar_code_2 = '0' AND sar_code_3 = '1' THEN '3000' 
	WHEN sar_code_1 = '1' AND sar_code_2 = '0' AND sar_code_3 = '2' THEN '5000' 
	WHEN sar_code_1 = '0' AND sar_code_2 = '9' AND sar_code_3 = '9' THEN '10000'
	WHEN sar_class_1_4 = '9020'and sar_code_1 = '0' AND sar_code_2 = '8' AND sar_code_3 = '8' THEN '0'
	ELSE 'N/A' END)
	
	WHEN sar_insurance_line IN('CA','GA','') and sar_major_peril = '130' and sar_type_bureau = 'AN' THEN
	(CASE WHEN sar_state = '16' THEN
	(CASE 
	WHEN sar_code_3 = '1' THEN 'Basic Full Coverage' 
	WHEN sar_code_3 = '2' THEN '250' 
	WHEN sar_code_3 = '3' THEN '500'
	ELSE 'N/A'
	END)
	ELSE (CASE 
	WHEN sar_code_3 = '1' THEN '0' 
	WHEN sar_code_3 = '2' THEN '100' 
	WHEN sar_code_3 = '3' THEN '200' 
	WHEN sar_code_3 = '4' THEN '300'
	ELSE 'N/A' END)
	END)
	
	WHEN sar_major_peril = '145' and sar_insurance_line = 'GA' and sar_type_bureau = 'AP' and sar_risk_unit_group = '116' and sar_unit = '971' THEN
	( CASE 
	WHEN sar_code_1 = '2' AND sar_code_2 = '0' AND sar_code_3 = '1' THEN '100/500'
	WHEN sar_code_1 = '2' AND sar_code_2 = '0' AND sar_code_3 = '2' THEN '250/1000'
	WHEN sar_code_1 = '2' AND sar_code_2 = '0' AND sar_code_3 = '3' THEN '500/2500'
	ELSE 'N/A' END)
	WHEN sar_major_peril = '170' and sar_insurance_line = 'GA' and sar_type_bureau = 'AP' THEN
	(CASE 
	WHEN sar_code_1 = '0' AND sar_code_2 = '7' AND sar_code_3 = '4' THEN '100'
	WHEN sar_code_1 = '0' AND sar_code_2 = '7' AND sar_code_3 = '6' THEN '250'
	WHEN sar_code_1 = '0' AND sar_code_2 = '7' AND sar_code_3 = '7' THEN '500'
	WHEN sar_code_1 = '0' AND sar_code_2 = '7' AND sar_code_3 = '8' THEN '1000'
	ELSE 'N/A'END)
	WHEN sar_major_peril = '171' and sar_type_bureau = 'AP' and sar_insurance_line in ('CA','') THEN
	(CASE WHEN sar_code_1+sar_code_2+sar_code_3='023' THEN '100'
	WHEN sar_code_1+sar_code_2+sar_code_3='024' THEN '250'
	WHEN sar_code_1+sar_code_2+sar_code_3='025' THEN '500'
	WHEN sar_code_1+sar_code_2+sar_code_3='026' THEN '1000'
	WHEN sar_code_1+sar_code_2+sar_code_3='301' THEN '2000'
	WHEN sar_code_1+sar_code_2+sar_code_3='302' THEN '3000'
	WHEN sar_code_1+sar_code_2+sar_code_3='303' THEN '5000'
	ELSE 'N/A' END)
	WHEN sar_major_peril = '172' and sar_type_bureau = 'AP' and sar_insurance_line in ('CA','') THEN
	(CASE WHEN sar_code_1+sar_code_2+sar_code_3='056' THEN 'No Deductible'
	WHEN sar_code_1+sar_code_2+sar_code_3='057' THEN '100'
	WHEN sar_code_1+sar_code_2+sar_code_3='058' THEN '250'
	ELSE 'N/A' END)
	WHEN D.Pif351StageId IS NOT NULL and sar_insurance_line in ('CA','') and sar_type_bureau in ('AL','AN','AP') THEN
	(CASE WHEN VehicleCoverageCode1='63' THEN LTRIM(RTRIM(VehicleCoverageLimit1))
	WHEN VehicleCoverageCode2='63' THEN LTRIM(RTRIM(VehicleCoverageLimit2))
	WHEN VehicleCoverageCode3='63' THEN LTRIM(RTRIM(VehicleCoverageLimit3))
	WHEN VehicleCoverageCode4='63' THEN LTRIM(RTRIM(VehicleCoverageLimit4))
	WHEN VehicleCoverageCode5='63' THEN LTRIM(RTRIM(VehicleCoverageLimit5))
	WHEN VehicleCoverageCode6='63' THEN LTRIM(RTRIM(VehicleCoverageLimit6))
	WHEN VehicleCoverageCode7='63' THEN LTRIM(RTRIM(VehicleCoverageLimit7))
	WHEN VehicleCoverageCode8='63' THEN LTRIM(RTRIM(VehicleCoverageLimit8))
	WHEN VehicleCoverageCode9='63' THEN LTRIM(RTRIM(VehicleCoverageLimit9))
	WHEN VehicleCoverageCode10='63' THEN LTRIM(RTRIM(VehicleCoverageLimit10))
	WHEN VehicleCoverageCode11='63' THEN LTRIM(RTRIM(VehicleCoverageLimit11))
	WHEN VehicleCoverageCode12='63' THEN LTRIM(RTRIM(VehicleCoverageLimit12))
	WHEN VehicleCoverageCode13='63' THEN LTRIM(RTRIM(VehicleCoverageLimit14))
	WHEN VehicleCoverageCode14='63' THEN LTRIM(RTRIM(VehicleCoverageLimit14))
	ELSE 'N/A' END)
	WHEN sar_insurance_line = 'WC' and Pif43IXUnmodWCRatingState IN('12','13','15','21','22','24','26') THEN
	(CASE
	WHEN B.Pif43IXUnmodReportingClassCode = '9931' then '1000'
	When B.Pif43IXUnmodReportingClassCode = '9940' then '500'
	When B.Pif43IXUnmodReportingClassCode = '9941' then '1000'
	When B.Pif43IXUnmodReportingClassCode = '9942' then '1500'
	When B.Pif43IXUnmodReportingClassCode = '9943' then '2000'
	When B.Pif43IXUnmodReportingClassCode = '9944' then '2500'
	When B.Pif43IXUnmodReportingClassCode = '9900' then '3000'
	When B.Pif43IXUnmodReportingClassCode = '9904' then '3500'
	When B.Pif43IXUnmodReportingClassCode = '9905' then '4000'
	When B.Pif43IXUnmodReportingClassCode = '9906' then '4500'
	When B.Pif43IXUnmodReportingClassCode = '9945' then '5000'
	When B.Pif43IXUnmodReportingClassCode = '9915' then '500'
	When B.Pif43IXUnmodReportingClassCode = '9916' then '1000'
	When B.Pif43IXUnmodReportingClassCode = '9917' then '1500'
	When B.Pif43IXUnmodReportingClassCode = '9918' then '2000'
	When B.Pif43IXUnmodReportingClassCode = '9919' then '2500'
	WHEN B.Pif43IXUnmodReportingClassCode = '9664' and Pmdi4w1ModifierPremBasis = '500' then '500'
	WHEN B.Pif43IXUnmodReportingClassCode = '9664' and Pmdi4w1ModifierPremBasis = '1000' then '1000'
	WHEN B.Pif43IXUnmodReportingClassCode = '9664' and Pmdi4w1ModifierPremBasis = '1500' then '1500'
	WHEN B.Pif43IXUnmodReportingClassCode = '9664' and Pmdi4w1ModifierPremBasis =  '2000' then '2000'
	WHEN B.Pif43IXUnmodReportingClassCode = '9664' and Pmdi4w1ModifierPremBasis =  '2500' then '2500'
	ELSE 'N/A' END)
	
	WHEN sar_insurance_line = 'WC' and sar_state='14' THEN 
	(CASE WHEN LTRIM(RTRIM(replace(replace(CASE WHEN LEN(DocumentText)>14 THEN substring(DocumentText,8,LEN(DocumentText)-14)
	ELSE SUBSTRING(DocumentText,8,LEN(DocumentText)) end,',',''),'.',''))) IN ('100','150','200','250','300','350','400','450','500','550','600','650','700','750','800','850','900','950','1000','1050','2000','2050','3000','3050','4000','4050','5000','5050','6000','6050','7000','7050','8000','8050','9000','9050','10000')
	THEN LTRIM(RTRIM(replace(replace(CASE WHEN LEN(DocumentText)>14 THEN substring(DocumentText,8,LEN(DocumentText)-14)
	ELSE SUBSTRING(DocumentText,8,LEN(DocumentText)) end,',',''),'.',''))) ELSE '0' end 
	)
	
	----------------------------------------------------------------
	WHEN A.sar_insurance_line='GL' and LTRIM(RTRIM(A.sar_major_peril)) = '550' THEN
	(CASE WHEN not (LTRIM(RTRIM(A.sar_code_4)) is null OR LEN(LTRIM(RTRIM(A.sar_code_4)))=0) THEN 
	(CASE WHEN A.sar_code_4 = '01' THEN '0'
	WHEN A.sar_code_4 ='02' THEN '50'
	WHEN A.sar_code_4 ='03' THEN '100'
	WHEN A.sar_code_4 ='04' THEN '250'
	WHEN A.sar_code_4 ='05' THEN '500' 
	WHEN A.sar_code_4 ='06' THEN '750'
	WHEN A.sar_code_4 ='07' THEN '1000'
	WHEN A.sar_code_4 ='09' THEN '2000'
	WHEN A.sar_code_4 ='08' THEN '2500' 
	WHEN A.sar_code_4 ='15' THEN '5000'
	WHEN A.sar_code_4 ='10' THEN '10000'
	WHEN A.sar_code_4 ='90' THEN '99999'
	ELSE 'N/A' END)
	WHEN not (LTRIM(RTRIM(A.sar_code_6)) is null OR LEN(LTRIM(RTRIM(A.sar_code_6)))=0) THEN
	(CASE WHEN A.sar_code_6 = '01' THEN '0'
	WHEN A.sar_code_6 ='02' THEN '50'
	WHEN A.sar_code_6 ='03' THEN '100'
	WHEN A.sar_code_6 ='04' THEN '250'
	WHEN A.sar_code_6 ='05' THEN '500' 
	WHEN A.sar_code_6 ='06' THEN '750'
	WHEN A.sar_code_6 ='07' THEN '1000'
	WHEN A.sar_code_6 ='09' THEN '2000'
	WHEN A.sar_code_6 ='08' THEN '2500' 
	WHEN A.sar_code_6 ='15' THEN '5000'
	WHEN A.sar_code_6 ='10' THEN '10000'
	WHEN A.sar_code_6 ='90' THEN '99999'
	ELSE 'N/A' END)
	ELSE 'N/A' END)
	WHEN A.sar_insurance_line='GL' and not (LTRIM(RTRIM(A.sar_code_4)) is null OR LEN(LTRIM(RTRIM(A.sar_code_4)))=0) THEN 
	(CASE WHEN A.sar_code_4 = '01' THEN '0'
	WHEN A.sar_code_4 ='02' THEN '50'
	WHEN A.sar_code_4 ='03' THEN '100'
	WHEN A.sar_code_4 ='04' THEN '250'
	WHEN A.sar_code_4 ='05' THEN '500' 
	WHEN A.sar_code_4 ='06' THEN '750'
	WHEN A.sar_code_4 ='07' THEN '1000'
	WHEN A.sar_code_4 ='09' THEN '2000'
	WHEN A.sar_code_4 ='08' THEN '2500' 
	WHEN A.sar_code_4 ='15' THEN '5000'
	WHEN A.sar_code_4 ='10' THEN '10000'
	WHEN A.sar_code_4 ='90' THEN '99999'
	ELSE 'N/A' END)
	WHEN A.sar_insurance_line='GL' and not (LTRIM(RTRIM(A.sar_code_6)) is null OR LEN(LTRIM(RTRIM(A.sar_code_6)))=0) THEN 
	(CASE WHEN A.sar_code_6 = '01' THEN '0'
	WHEN A.sar_code_6 ='02' THEN '50'
	WHEN A.sar_code_6 ='03' THEN '100'
	WHEN A.sar_code_6 ='04' THEN '250'
	WHEN A.sar_code_6 ='05' THEN '500' 
	WHEN A.sar_code_6 ='06' THEN '750'
	WHEN A.sar_code_6 ='07' THEN '1000'
	WHEN A.sar_code_6 ='09' THEN '2000'
	WHEN A.sar_code_6 ='08' THEN '2500' 
	WHEN A.sar_code_6 ='15' THEN '5000'
	WHEN A.sar_code_6 ='10' THEN '10000'
	WHEN A.sar_code_6 ='90' THEN '99999'
	ELSE 'N/A' END)
	WHEN A.sar_insurance_line='GL' and LTRIM(RTRIM(A.sar_class_1_4+A.sar_class_5_6))='22222' THEN '99999'
	----------------------------------------------------------------
	
	ELSE 'N/A' END AS Value
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_4514_stage A
	INNER JOIN  (SELECT DISTINCT Policykey  from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.work_policytransactionstatus 
	WHERE AuditID = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AND
	PolicyStatus <> 'NOCHANGE')  E
	ON  E.policykey = A.policykey
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43IXUnmodStage  B 
	ON  A.Pif_Symbol=B.PifSymbol  
	AND A.Pif_Policy_Number=B.PifPolicyNumber 
	AND A.Pif_Module=B.PifModule 
	AND B.Pif43IXUnmodInsuranceLine=A.sar_insurance_line
	AND A.sar_state=CONVERT(varchar,B.Pif43IXUnmodWCRatingState)
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43IXZWCModStage C
	ON  A.Pif_Symbol=C.PifSymbol 
	AND A.Pif_Policy_Number=C.PifPolicyNumber 
	AND A.Pif_Module=C.PifModule 
	AND C.Pmdi4w1InsuranceLine=A.sar_insurance_line
	AND A.Sar_state=CONVERT(varchar,Pmdi4w1WcRatingState)
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif351Stage D
	ON A.Pif_Symbol=D.PifSymbol
	AND A.Pif_Policy_Number=D.PifPolicyNumber
	AND A.Pif_Module=D.PifModule
	AND A.sar_unit=RIGHT('0'+CONVERT(VARCHAR,D.UnitNum),3)
	AND D.UnitNum>=52 
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif11Stage F
	ON A.Pif_Symbol=F.PifSymbol
	AND A.Pif_Policy_Number=F.PifPolicyNumber
	AND A.Pif_Module=F.PifModule
	AND F.DocumentName in ('140601','140603')
	AND F.DocumentText is not null
	LEFT JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SupDeductibleType SDT
	on SDT.RiskUnitGroupCode=A.sar_risk_unit_group
	AND SDT.InsuranceLine=A.sar_insurance_line
	AND SDT.SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	WHERE A.logical_flag IN ('0','1','2','3')
),
EXP_GetVal AS (
	SELECT
	Value AS i_Value,
	pif_4514_stage_id,
	Type,
	-- *INF*: rtrim(ltrim(i_Value))
	rtrim(ltrim(i_Value
		)
	) AS o_Value
	FROM SQ_pif_4514_stage
),
FIL_Value AS (
	SELECT
	pif_4514_stage_id, 
	Type, 
	o_Value AS Value
	FROM EXP_GetVal
	WHERE NOT ISNULL(Type) and Type<>'N/A' and Value<>'N/A'
),
LKP_WorkPremiumTransaction AS (
	SELECT
	PremiumTransactionAKId,
	PremiumTransactionStageId
	FROM (
		SELECT PremiumTransactionAKId AS PremiumTransactionAKId ,
		PremiumTransactionStageId AS PremiumTransactionStageId
		 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction A
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_4514_stage B
		ON A.PremiumTransactionStageId=B.pif_4514_stage_ID
		ORDER BY WorkPremiumTransactionId --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionStageId ORDER BY PremiumTransactionAKId DESC) = 1
),
FIL_InvalidAKID AS (
	SELECT
	FIL_Value.Type, 
	FIL_Value.Value, 
	LKP_WorkPremiumTransaction.PremiumTransactionAKId
	FROM FIL_Value
	LEFT JOIN LKP_WorkPremiumTransaction
	ON LKP_WorkPremiumTransaction.PremiumTransactionStageId = FIL_Value.pif_4514_stage_id
	WHERE NOT ISNULL(PremiumTransactionAKId)
),
SRT_BridgeRecords AS (
	SELECT
	Type, 
	Value, 
	PremiumTransactionAKId
	FROM FIL_InvalidAKID
	ORDER BY Type ASC, Value ASC
),
AGG_RemoveDup AS (
	SELECT
	Type,
	Value
	FROM FIL_Value
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Type, Value ORDER BY NULL) = 1
),
LKP_CoverageDeductible AS (
	SELECT
	CoverageDeductibleId,
	CoverageDeductibleType,
	CoverageDeductibleValue
	FROM (
		SELECT 
			CoverageDeductibleId,
			CoverageDeductibleType,
			CoverageDeductibleValue
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDeductible
		WHERE SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageDeductibleType,CoverageDeductibleValue ORDER BY CoverageDeductibleId) = 1
),
SEQ_CoverageDeductibleID AS (
	CREATE SEQUENCE SEQ_CoverageDeductibleID
	START = 0
	INCREMENT = 1;
),
EXP_Existing AS (
	SELECT
	LKP_CoverageDeductible.CoverageDeductibleId AS lkp_CoverageDeductibleId,
	SEQ_CoverageDeductibleID.NEXTVAL,
	AGG_RemoveDup.Type,
	AGG_RemoveDup.Value,
	-- *INF*: IIF(ISNULL(lkp_CoverageDeductibleId),NEXTVAL,lkp_CoverageDeductibleId)
	IFF(lkp_CoverageDeductibleId IS NULL,
		NEXTVAL,
		lkp_CoverageDeductibleId
	) AS o_CoverageDeductibleId
	FROM AGG_RemoveDup
	LEFT JOIN LKP_CoverageDeductible
	ON LKP_CoverageDeductible.CoverageDeductibleType = AGG_RemoveDup.Type AND LKP_CoverageDeductible.CoverageDeductibleValue = AGG_RemoveDup.Value
),
SRT_Data AS (
	SELECT
	Type, 
	Value, 
	o_CoverageDeductibleId AS CoverageDeductibleId
	FROM EXP_Existing
	ORDER BY Type ASC, Value ASC
),
JNR_AllData AS (SELECT
	SRT_Data.Type, 
	SRT_Data.Value, 
	SRT_BridgeRecords.Type AS Type1, 
	SRT_BridgeRecords.Value AS Value1, 
	SRT_BridgeRecords.PremiumTransactionAKId, 
	SRT_Data.CoverageDeductibleId
	FROM SRT_Data
	INNER JOIN SRT_BridgeRecords
	ON SRT_BridgeRecords.Type = SRT_Data.Type AND SRT_BridgeRecords.Value = SRT_Data.Value
),
LKP_CoverageDeductibleBridge AS (
	SELECT
	CoverageDeductibleBridgeId,
	PremiumTransactionAKId,
	CoverageDeductibleId
	FROM (
		SELECT 
			CoverageDeductibleBridgeId,
			PremiumTransactionAKId,
			CoverageDeductibleId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDeductibleBridge
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId,CoverageDeductibleId ORDER BY CoverageDeductibleBridgeId) = 1
),
FIL_Bridge AS (
	SELECT
	LKP_CoverageDeductibleBridge.CoverageDeductibleBridgeId AS lkp_CoverageDeductibleBridgeId, 
	JNR_AllData.PremiumTransactionAKId, 
	JNR_AllData.CoverageDeductibleId
	FROM JNR_AllData
	LEFT JOIN LKP_CoverageDeductibleBridge
	ON LKP_CoverageDeductibleBridge.PremiumTransactionAKId = JNR_AllData.PremiumTransactionAKId AND LKP_CoverageDeductibleBridge.CoverageDeductibleId = JNR_AllData.CoverageDeductibleId
	WHERE ISNULL(lkp_CoverageDeductibleBridgeId)
),
AGG_Cnt AS (
	SELECT
	PremiumTransactionAKId,
	CoverageDeductibleId,
	-- *INF*: COUNT(1)
	COUNT(1
	) AS o_CoverageDeductibleIdCount
	FROM FIL_Bridge
	GROUP BY PremiumTransactionAKId, CoverageDeductibleId
),
EXP_brigdege AS (
	SELECT
	PremiumTransactionAKId,
	CoverageDeductibleId,
	o_CoverageDeductibleIdCount AS CoverageDeductibleIdCount,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	'N/A' AS o_CoverageDeductibleControl
	FROM AGG_Cnt
),
CoverageDeductibleBridge AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDeductibleBridge
	(AuditID, SourceSystemID, CreatedDate, PremiumTransactionAKId, CoverageDeductibleId, CoverageDeductibleIdCount, CoverageDeductibleControl)
	SELECT 
	o_AuditID AS AUDITID, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	PREMIUMTRANSACTIONAKID, 
	COVERAGEDEDUCTIBLEID, 
	COVERAGEDEDUCTIBLEIDCOUNT, 
	o_CoverageDeductibleControl AS COVERAGEDEDUCTIBLECONTROL
	FROM EXP_brigdege
),
FIL_ExistingDeductible AS (
	SELECT
	lkp_CoverageDeductibleId, 
	Type, 
	Value, 
	o_CoverageDeductibleId AS CoverageDeductibleId
	FROM EXP_Existing
	WHERE ISNULL(lkp_CoverageDeductibleId)
),
EXP_Value AS (
	SELECT
	Type,
	Value,
	CoverageDeductibleId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	sysdate AS CreatedDate,
	Type AS CoverageDeductibleType,
	Value AS CoverageDeductibleValue
	FROM FIL_ExistingDeductible
),
UPD_Target AS (
	SELECT
	CoverageDeductibleId, 
	AuditID, 
	SourceSystemID, 
	CreatedDate, 
	CoverageDeductibleType, 
	CoverageDeductibleValue
	FROM EXP_Value
),
CoverageDeductible AS (
	SET IDENTITY_INSERT @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDeductible  ON
	INSERT @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDeductible (CoverageDeductibleId ,AuditID,SourceSystemID,CreatedDate,CoverageDeductibleType,CoverageDeductibleValue) 
	SELECT S.CoverageDeductibleId,S.AuditID,S.SourceSystemID, S.CreatedDate,S.CoverageDeductibleType, S.CoverageDeductibleValue
	FROM UPD_Target S
),