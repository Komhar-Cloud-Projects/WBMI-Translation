WITH
SQ_WorkDCTInsuranceLine AS (
	SELECT dp.SessionId,
	       dp.PolicyId,
	       dl.LineId,
	       dl.Type LineType,
	       wl.RiskGrade,
	       wcl.IsAuditable,
	       PriorInsurance.CarrierName PriorCarrierName,
	       PriorInsurance.PolicyNumber PriorPolicyNumber,
	       PriorInsurance.LineOfBusiness PriorLineOfBusiness,
	       dcm.value ExperienceModifier,
	       wl.FinalCommission,
	       wl.CommissionCustomerCareAmount
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPolicyStaging dp
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLineStaging dl
	     ON dp.PolicyId = dl.PolicyId
	        AND dp.SessionId = dl.SessionId
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLineStaging wl
	     ON dl.LineId = wl.LineId
	        AND dl.SessionId = wl.SessionId
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCLLineStage wcl
	     ON wl.WB_LineId = wcl.WBLineId
	        AND wl.SessionId = wcl.SessionId
	OUTER APPLY
	(
	    -- Get the policy key information from the Prior Insurance tables for one and only one policy 
	    SELECT TOP 1 dpi.CarrierName,
	                 wpi.CarrierNameOther,
	                 wcpi.PolicySymbol,
	                 dpi.PolicyNumber,
	                 wpi.LineOfBusiness,
	                 CASE
	                     WHEN ISNUMERIC(wcpi.PolicyMod) = 1
	                     THEN RIGHT('00'+CAST(wcpi.PolicyMod AS VARCHAR(2)), 2)
	                     ELSE NULL
	                 END AS PolicyMod
	    FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPriorInsuranceStaging dpi
	    INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBPriorInsuranceStage wpi
	         ON dpi.PriorInsuranceId = wpi.PriorInsuranceId
	            AND dpi.SessionId = wpi.SessionId
	    INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCLPriorInsuranceStage wcpi
	         ON wpi.WBPriorInsuranceId = wcpi.WBPriorInsuranceId
	            AND wpi.SessionId = wcpi.SessionId
	    WHERE dpi.PolicyId = dp.PolicyId
	          AND dpi.SessionId = dp.SessionId
	          AND wpi.PriorInsuranceId = dpi.PriorInsuranceId
	          --Attempt to match on Line of Business when we can for known lines, otherwise just pick one if a direct line to line match does not exist.
	          AND (CASE wpi.LineOfBusiness
	                   WHEN 'EPLI'
	                   THEN 'EmploymentPracticesLiab'
	                   WHEN 'Auto'
	                   THEN 'CommercialAuto'
	                   WHEN 'Crime'
	                   THEN 'Crime'
	                   WHEN 'WorkersComp'
	                   THEN 'WorkersCompensation'
	                   WHEN 'NFPDO'
	                   THEN 'DirectorsAndOfficersNFP'
	                   WHEN 'Umbrella'
	                   THEN 'CommercialUmbrella'
	               END = dl.Type
	               OR wpi.LineOfBusiness NOT IN('EPLI', 'Auto', 'Crime', 'WorkersComp', 'NFPDO', 'Umbrella'))
	    ORDER BY wcpi.PolicySymbol,
	             dpi.PolicyNumber,
	             wcpi.PolicyMod
	) PriorInsurance LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCModifierStaging DCM
	ON DCM.ObjectId = dl.LineId
	   AND DCM.ObjectName = 'DC_Line'
	   AND DCM.Type = 'ExperienceMod'
	   AND DCM.ModifierId IN
	(
	    SELECT MAX(a.ModifierId)
	    FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCModifierStaging a
	    WHERE a.ObjectId = DCM.ObjectId
	          AND a.ObjectName = 'DC_Line'
	          AND a.Type = 'ExperienceMod'
	)
),
EXP_Default AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SessionId,
	PolicyId,
	LineId,
	LineType,
	RiskGrade,
	IsAuditable AS i_IsAuditable,
	-- *INF*: DECODE(i_IsAuditable,'T',1,'F',0,NULL)
	DECODE(
	    i_IsAuditable,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IsAuditable,
	PriorCarrierName,
	PriorPolicyNumber,
	PriorLineOfBusiness,
	ExperienceModifier,
	FinalCommission,
	CommissionCustomerCareAmount
	FROM SQ_WorkDCTInsuranceLine
),
WorkDCTInsuranceLine AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkDCTInsuranceLine;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkDCTInsuranceLine
	(ExtractDate, SourceSystemId, SessionId, PolicyId, LineId, LineType, RiskGrade, IsAuditable, PriorCarrierName, PriorPolicyNumber, PriorLineOfBusiness, ExperienceModifier, FinalCommission, CommissionCustomerCareAmount)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	SESSIONID, 
	POLICYID, 
	LINEID, 
	LINETYPE, 
	RISKGRADE, 
	o_IsAuditable AS ISAUDITABLE, 
	PRIORCARRIERNAME, 
	PRIORPOLICYNUMBER, 
	PRIORLINEOFBUSINESS, 
	EXPERIENCEMODIFIER, 
	FINALCOMMISSION, 
	COMMISSIONCUSTOMERCAREAMOUNT
	FROM EXP_Default
),