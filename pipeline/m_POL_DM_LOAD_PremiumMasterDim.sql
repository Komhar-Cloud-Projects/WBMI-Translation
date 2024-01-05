WITH
SQ_PremiumMasterCalculation AS (
	SELECT DISTINCT 
	        LTRIM(RTRIM(A.PremiumMasterPolicyExpirationYear)) AS PremiumMasterPolicyExpirationYear,  
	        LTRIM(RTRIM(A.PremiumMasterPolicyTerm)) AS PremiumMasterPolicyTerm,  
	        LTRIM(RTRIM(A.PremiumMasterBureauPolicyType)) AS PremiumMasterBureauPolicyType,  
	        LTRIM(RTRIM(A.PremiumMasterAuditCode)) AS PremiumMasterAuditCode,  
	        LTRIM(RTRIM(A.PremiumMasterTypeBureauCode)) AS PremiumMasterTypeBureauCode,  
	        LTRIM(RTRIM(A.PremiumMasterBureauStatisticalLine)) AS PremiumMasterBureauStatisticalLine,  
	        LTRIM(RTRIM(A.PremiumMasterProductLine)) AS PremiumMasterProductLine,  
	        LTRIM(RTRIM(A.PremiumMasterClassCode)) AS PremiumMasterClassCode,  
	        LTRIM(RTRIM(A.PremiumMasterSubLine)) AS PremiumMasterSubLine,  
	        A.PremiumMasterStatisticalCode1 AS PremiumMasterStatisticalCode1,  
	        A.PremiumMasterStatisticalCode2 AS PremiumMasterStatisticalCode2,  
	        A.PremiumMasterStatisticalCode3 AS PremiumMasterStatisticalCode3,  
	        LTRIM(RTRIM(A.PremiumMasterRateModifier)) AS PremiumMasterRateModifier,  
	        LTRIM(RTRIM(A.PremiumMasterRateDeparture)) AS PremiumMasterRateDeparture,  
	        A.PremiumMasterBureauInceptionDate AS PremiumMasterBureauInceptionDate,
	        LTRIM(RTRIM(A.PremiumMasterCountersignAgencyType)) AS PremiumMasterCountersignAgencyType,  
	        LTRIM(RTRIM(A.PremiumMasterCountersignAgencyCode)) AS PremiumMasterCountersignAgencyCode,  
	        LTRIM(RTRIM(A.PremiumMasterCountersignAgencyState)) AS PremiumMasterCountersignAgencyState,  
	        LTRIM(RTRIM(A.PremiumMasterCountersignAgencyRate)) AS PremiumMasterCountersignAgencyRate,  
	        LTRIM(RTRIM(A.PremiumMasterRenewalIndicator)) AS PremiumMasterRenewalIndicator
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation A
	WHERE 
	A.CreatedDate > '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_Source AS (
	SELECT
	PremiumMasterPolicyExpirationYear,
	PremiumMasterPolicyTerm,
	PremiumMasterBureauPolicyType,
	PremiumMasterAuditCode,
	PremiumMasterTypeBureauCode,
	PremiumMasterBureauStatisticalLine,
	PremiumMasterProductLine,
	PremiumMasterClassCode,
	PremiumMasterSubLine,
	PremiumMasterStatisticalCode1,
	PremiumMasterStatisticalCode2,
	PremiumMasterStatisticalCode3,
	PremiumMasterRateModifier,
	PremiumMasterRateDeparture,
	PremiumMasterBureauInceptionDate,
	-- *INF*: TO_CHAR(PremiumMasterBureauInceptionDate,'YYYY')
	TO_CHAR(PremiumMasterBureauInceptionDate, 'YYYY') AS PremiumMasterBureauInceptionDate_out,
	PremiumMasterCountersignAgencyType,
	PremiumMasterCountersignAgencyCode,
	PremiumMasterCountersignAgencyState,
	PremiumMasterCountersignAgencyRate,
	PremiumMasterRenewalIndicator
	FROM SQ_PremiumMasterCalculation
),
mplt_PremiumMasterDimID AS (WITH
	INPUT AS (
		
	),
	EXP_Source AS (
		SELECT
		PremiumMasterPolicyExpirationYear,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterPolicyExpirationYear),'N/A',
		-- IS_SPACES(PremiumMasterPolicyExpirationYear),'N/A',
		-- LENGTH(PremiumMasterPolicyExpirationYear)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterPolicyExpirationYear)))
		DECODE(TRUE,
		PremiumMasterPolicyExpirationYear IS NULL, 'N/A',
		IS_SPACES(PremiumMasterPolicyExpirationYear), 'N/A',
		LENGTH(PremiumMasterPolicyExpirationYear) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterPolicyExpirationYear))) AS v_PremiumMasterPolicyExpirationYear,
		v_PremiumMasterPolicyExpirationYear AS PremiumMasterPolicyExpirationYear_out,
		PremiumMasterPolicyTerm,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterPolicyTerm),'N/A',
		-- IS_SPACES(PremiumMasterPolicyTerm),'N/A',
		-- LENGTH(PremiumMasterPolicyTerm)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterPolicyTerm)))
		DECODE(TRUE,
		PremiumMasterPolicyTerm IS NULL, 'N/A',
		IS_SPACES(PremiumMasterPolicyTerm), 'N/A',
		LENGTH(PremiumMasterPolicyTerm) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterPolicyTerm))) AS v_PremiumMasterPolicyTerm,
		v_PremiumMasterPolicyTerm AS PremiumMasterPolicyTerm_out,
		PremiumMasterBureauPolicyType,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterBureauPolicyType),'N/A',
		-- IS_SPACES(PremiumMasterBureauPolicyType),'N/A',
		-- LENGTH(PremiumMasterBureauPolicyType)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterBureauPolicyType)))
		DECODE(TRUE,
		PremiumMasterBureauPolicyType IS NULL, 'N/A',
		IS_SPACES(PremiumMasterBureauPolicyType), 'N/A',
		LENGTH(PremiumMasterBureauPolicyType) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterBureauPolicyType))) AS v_PremiumMasterBureauPolicyType,
		v_PremiumMasterBureauPolicyType AS PremiumMasterBureauPolicyType_out,
		PremiumMasterAuditCode,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterAuditCode),'N/A',
		-- IS_SPACES(PremiumMasterAuditCode),'N/A',
		-- LENGTH(PremiumMasterAuditCode)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterAuditCode)))
		DECODE(TRUE,
		PremiumMasterAuditCode IS NULL, 'N/A',
		IS_SPACES(PremiumMasterAuditCode), 'N/A',
		LENGTH(PremiumMasterAuditCode) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterAuditCode))) AS v_PremiumMasterAuditCode,
		v_PremiumMasterAuditCode AS PremiumMasterAuditCode_out,
		PremiumMasterTypeBureauCode,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterTypeBureauCode),'N/A',
		-- IS_SPACES(PremiumMasterTypeBureauCode),'N/A',
		-- LENGTH(PremiumMasterTypeBureauCode)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterTypeBureauCode)))
		DECODE(TRUE,
		PremiumMasterTypeBureauCode IS NULL, 'N/A',
		IS_SPACES(PremiumMasterTypeBureauCode), 'N/A',
		LENGTH(PremiumMasterTypeBureauCode) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterTypeBureauCode))) AS v_PremiumMasterTypeBureauCode,
		v_PremiumMasterTypeBureauCode AS PremiumMasterTypeBureauCode_out,
		PremiumMasterBureauStatisticalLine,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterBureauStatisticalLine),'N/A',
		-- IS_SPACES(PremiumMasterBureauStatisticalLine),'N/A',
		-- LENGTH(PremiumMasterBureauStatisticalLine)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterBureauStatisticalLine)))
		DECODE(TRUE,
		PremiumMasterBureauStatisticalLine IS NULL, 'N/A',
		IS_SPACES(PremiumMasterBureauStatisticalLine), 'N/A',
		LENGTH(PremiumMasterBureauStatisticalLine) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterBureauStatisticalLine))) AS v_PremiumMasterBureauStatisticalLine,
		v_PremiumMasterBureauStatisticalLine AS PremiumMasterBureauStatisticalLine_out,
		PremiumMasterProductLine,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterProductLine),'N/A',
		-- IS_SPACES(PremiumMasterProductLine),'N/A',
		-- LENGTH(PremiumMasterProductLine)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterProductLine)))
		DECODE(TRUE,
		PremiumMasterProductLine IS NULL, 'N/A',
		IS_SPACES(PremiumMasterProductLine), 'N/A',
		LENGTH(PremiumMasterProductLine) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterProductLine))) AS v_PremiumMasterProductLine,
		v_PremiumMasterProductLine AS PremiumMasterProductLine_out,
		PremiumMasterClassCode,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterClassCode),'N/A',
		-- IS_SPACES(PremiumMasterClassCode),'N/A',
		-- LENGTH(PremiumMasterClassCode)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterClassCode)))
		DECODE(TRUE,
		PremiumMasterClassCode IS NULL, 'N/A',
		IS_SPACES(PremiumMasterClassCode), 'N/A',
		LENGTH(PremiumMasterClassCode) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterClassCode))) AS v_PremiumMasterClassCode,
		v_PremiumMasterClassCode AS PremiumMasterClassCode_out,
		PremiumMasterExposure,
		PremiumMasterSubLine,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterSubLine),'N/A',
		-- IS_SPACES(PremiumMasterSubLine),'N/A',
		-- LENGTH(PremiumMasterSubLine)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterSubLine)))
		DECODE(TRUE,
		PremiumMasterSubLine IS NULL, 'N/A',
		IS_SPACES(PremiumMasterSubLine), 'N/A',
		LENGTH(PremiumMasterSubLine) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterSubLine))) AS v_PremiumMasterSubLine,
		v_PremiumMasterSubLine AS PremiumMasterSubLine_out,
		PremiumMasterStatisticalCode1,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterStatisticalCode1),'N/A',
		-- IS_SPACES(PremiumMasterStatisticalCode1),'N/A',
		-- LENGTH(PremiumMasterStatisticalCode1)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterStatisticalCode1)))
		DECODE(TRUE,
		PremiumMasterStatisticalCode1 IS NULL, 'N/A',
		IS_SPACES(PremiumMasterStatisticalCode1), 'N/A',
		LENGTH(PremiumMasterStatisticalCode1) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterStatisticalCode1))) AS v_PremiumMasterStatisticalCode1,
		-- *INF*: IIF(ISNULL(PremiumMasterStatisticalCode1) OR IS_SPACES(PremiumMasterStatisticalCode1) OR LENGTH(PremiumMasterStatisticalCode1)=0,'N/A',
		-- PremiumMasterStatisticalCode1)
		IFF(PremiumMasterStatisticalCode1 IS NULL OR IS_SPACES(PremiumMasterStatisticalCode1) OR LENGTH(PremiumMasterStatisticalCode1) = 0, 'N/A', PremiumMasterStatisticalCode1) AS PremiumMasterStatisticalCode1_out,
		PremiumMasterStatisticalCode2,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterStatisticalCode2),'N/A',
		-- IS_SPACES(PremiumMasterStatisticalCode2),'N/A',
		-- LENGTH(PremiumMasterStatisticalCode2)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterStatisticalCode2)))
		DECODE(TRUE,
		PremiumMasterStatisticalCode2 IS NULL, 'N/A',
		IS_SPACES(PremiumMasterStatisticalCode2), 'N/A',
		LENGTH(PremiumMasterStatisticalCode2) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterStatisticalCode2))) AS v_PremiumMasterStatisticalCode2,
		-- *INF*: IIF(ISNULL(PremiumMasterStatisticalCode2) OR IS_SPACES(PremiumMasterStatisticalCode2) OR LENGTH(PremiumMasterStatisticalCode2)=0,'N/A',
		-- PremiumMasterStatisticalCode2)
		IFF(PremiumMasterStatisticalCode2 IS NULL OR IS_SPACES(PremiumMasterStatisticalCode2) OR LENGTH(PremiumMasterStatisticalCode2) = 0, 'N/A', PremiumMasterStatisticalCode2) AS PremiumMasterStatisticalCode2_out,
		PremiumMasterStatisticalCode3,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterStatisticalCode3),'N/A',
		-- IS_SPACES(PremiumMasterStatisticalCode3),'N/A',
		-- LENGTH(PremiumMasterStatisticalCode3)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterStatisticalCode3)))
		DECODE(TRUE,
		PremiumMasterStatisticalCode3 IS NULL, 'N/A',
		IS_SPACES(PremiumMasterStatisticalCode3), 'N/A',
		LENGTH(PremiumMasterStatisticalCode3) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterStatisticalCode3))) AS v_PremiumMasterStatisticalCode3,
		-- *INF*: IIF(ISNULL(PremiumMasterStatisticalCode3) OR IS_SPACES(PremiumMasterStatisticalCode3) OR LENGTH(PremiumMasterStatisticalCode3)=0,'N/A',
		-- PremiumMasterStatisticalCode3)
		IFF(PremiumMasterStatisticalCode3 IS NULL OR IS_SPACES(PremiumMasterStatisticalCode3) OR LENGTH(PremiumMasterStatisticalCode3) = 0, 'N/A', PremiumMasterStatisticalCode3) AS PremiumMasterStatisticalCode3_out,
		PremiumMasterRateModifier,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterRateModifier),'N/A',
		-- IS_SPACES(PremiumMasterRateModifier),'N/A',
		-- LENGTH(PremiumMasterRateModifier)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterRateModifier)))
		DECODE(TRUE,
		PremiumMasterRateModifier IS NULL, 'N/A',
		IS_SPACES(PremiumMasterRateModifier), 'N/A',
		LENGTH(PremiumMasterRateModifier) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterRateModifier))) AS v_PremiumMasterRateModifier,
		v_PremiumMasterRateModifier AS PremiumMasterRateModifier_out,
		PremiumMasterRateDeparture,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterRateDeparture),'N/A',
		-- IS_SPACES(PremiumMasterRateDeparture),'N/A',
		-- LENGTH(PremiumMasterRateDeparture)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterRateDeparture)))
		DECODE(TRUE,
		PremiumMasterRateDeparture IS NULL, 'N/A',
		IS_SPACES(PremiumMasterRateDeparture), 'N/A',
		LENGTH(PremiumMasterRateDeparture) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterRateDeparture))) AS v_PremiumMasterRateDeparture,
		v_PremiumMasterRateDeparture AS PremiumMasterRateDeparture_out,
		PremiumMasterBureauInceptionDate,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterBureauInceptionDate),'N/A',
		-- IS_SPACES(PremiumMasterBureauInceptionDate),'N/A',
		-- LENGTH(PremiumMasterBureauInceptionDate)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterBureauInceptionDate)))
		DECODE(TRUE,
		PremiumMasterBureauInceptionDate IS NULL, 'N/A',
		IS_SPACES(PremiumMasterBureauInceptionDate), 'N/A',
		LENGTH(PremiumMasterBureauInceptionDate) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterBureauInceptionDate))) AS v_PremiumMasterBureauInceptionDate,
		v_PremiumMasterBureauInceptionDate AS PremiumMasterBureauInceptionDate_out,
		PremiumMasterCountersignAgencyType,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterCountersignAgencyType),'N/A',
		-- IS_SPACES(PremiumMasterCountersignAgencyType),'N/A',
		-- LENGTH(PremiumMasterCountersignAgencyType)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterCountersignAgencyType)))
		DECODE(TRUE,
		PremiumMasterCountersignAgencyType IS NULL, 'N/A',
		IS_SPACES(PremiumMasterCountersignAgencyType), 'N/A',
		LENGTH(PremiumMasterCountersignAgencyType) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterCountersignAgencyType))) AS v_PremiumMasterCountersignAgencyType,
		v_PremiumMasterCountersignAgencyType AS PremiumMasterCountersignAgencyType_out,
		PremiumMasterCountersignAgencyCode,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterCountersignAgencyCode),'N/A',
		-- IS_SPACES(PremiumMasterCountersignAgencyCode),'N/A',
		-- LENGTH(PremiumMasterCountersignAgencyCode)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterCountersignAgencyCode)))
		DECODE(TRUE,
		PremiumMasterCountersignAgencyCode IS NULL, 'N/A',
		IS_SPACES(PremiumMasterCountersignAgencyCode), 'N/A',
		LENGTH(PremiumMasterCountersignAgencyCode) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterCountersignAgencyCode))) AS v_PremiumMasterCountersignAgencyCode,
		v_PremiumMasterCountersignAgencyCode AS PremiumMasterCountersignAgencyCode_out,
		PremiumMasterCountersignAgencyState,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterCountersignAgencyState),'N/A',
		-- IS_SPACES(PremiumMasterCountersignAgencyState),'N/A',
		-- LENGTH(PremiumMasterCountersignAgencyState)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterCountersignAgencyState)))
		DECODE(TRUE,
		PremiumMasterCountersignAgencyState IS NULL, 'N/A',
		IS_SPACES(PremiumMasterCountersignAgencyState), 'N/A',
		LENGTH(PremiumMasterCountersignAgencyState) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterCountersignAgencyState))) AS v_PremiumMasterCountersignAgencyState,
		v_PremiumMasterCountersignAgencyState AS PremiumMasterCountersignAgencyState_out,
		PremiumMasterCountersignAgencyRate,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterCountersignAgencyRate),'N/A',
		-- IS_SPACES(PremiumMasterCountersignAgencyRate),'N/A',
		-- LENGTH(PremiumMasterCountersignAgencyRate)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterCountersignAgencyRate)))
		DECODE(TRUE,
		PremiumMasterCountersignAgencyRate IS NULL, 'N/A',
		IS_SPACES(PremiumMasterCountersignAgencyRate), 'N/A',
		LENGTH(PremiumMasterCountersignAgencyRate) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterCountersignAgencyRate))) AS v_PremiumMasterCountersignAgencyRate,
		v_PremiumMasterCountersignAgencyRate AS PremiumMasterCountersignAgencyRate_out,
		PremiumMasterRenewalIndicator,
		-- *INF*: DECODE(TRUE,
		-- ISNULL(PremiumMasterRenewalIndicator),'N/A',
		-- IS_SPACES(PremiumMasterRenewalIndicator),'N/A',
		-- LENGTH(PremiumMasterRenewalIndicator)=0,'N/A',
		-- LTRIM(RTRIM(PremiumMasterRenewalIndicator)))
		DECODE(TRUE,
		PremiumMasterRenewalIndicator IS NULL, 'N/A',
		IS_SPACES(PremiumMasterRenewalIndicator), 'N/A',
		LENGTH(PremiumMasterRenewalIndicator) = 0, 'N/A',
		LTRIM(RTRIM(PremiumMasterRenewalIndicator))) AS v_PremiumMasterRenewalIndicator,
		v_PremiumMasterRenewalIndicator AS PremiumMasterRenewalIndicator_out,
		-- *INF*: MD5(
		-- TO_CHAR(v_PremiumMasterPolicyExpirationYear)  ||  
		-- TO_CHAR(v_PremiumMasterPolicyTerm)  ||  
		-- TO_CHAR(v_PremiumMasterBureauPolicyType)  ||  
		-- TO_CHAR(v_PremiumMasterAuditCode)  ||  
		-- TO_CHAR(v_PremiumMasterTypeBureauCode)  ||  
		-- TO_CHAR(v_PremiumMasterBureauStatisticalLine)  ||  
		-- TO_CHAR(v_PremiumMasterProductLine)  ||  
		-- TO_CHAR(v_PremiumMasterClassCode)  ||  
		-- ----TO_CHAR(PremiumMasterExposure)  ||  
		-- TO_CHAR(v_PremiumMasterSubLine)  ||  
		-- TO_CHAR(v_PremiumMasterStatisticalCode1)  ||  
		-- TO_CHAR(v_PremiumMasterStatisticalCode2) ||  
		-- TO_CHAR(v_PremiumMasterStatisticalCode3) ||  
		-- TO_CHAR(v_PremiumMasterRateModifier) ||  
		-- TO_CHAR(v_PremiumMasterRateDeparture) ||  
		-- TO_CHAR(v_PremiumMasterBureauInceptionDate) ||  
		-- TO_CHAR(v_PremiumMasterCountersignAgencyType) ||  
		-- TO_CHAR(v_PremiumMasterCountersignAgencyCode) ||  
		-- TO_CHAR(v_PremiumMasterCountersignAgencyState) ||  
		-- TO_CHAR(v_PremiumMasterCountersignAgencyRate) ||  
		-- TO_CHAR(v_PremiumMasterRenewalIndicator) 
		-- 
		-- )
		MD5(TO_CHAR(v_PremiumMasterPolicyExpirationYear) || TO_CHAR(v_PremiumMasterPolicyTerm) || TO_CHAR(v_PremiumMasterBureauPolicyType) || TO_CHAR(v_PremiumMasterAuditCode) || TO_CHAR(v_PremiumMasterTypeBureauCode) || TO_CHAR(v_PremiumMasterBureauStatisticalLine) || TO_CHAR(v_PremiumMasterProductLine) || TO_CHAR(v_PremiumMasterClassCode) || TO_CHAR(v_PremiumMasterSubLine) || TO_CHAR(v_PremiumMasterStatisticalCode1) || TO_CHAR(v_PremiumMasterStatisticalCode2) || TO_CHAR(v_PremiumMasterStatisticalCode3) || TO_CHAR(v_PremiumMasterRateModifier) || TO_CHAR(v_PremiumMasterRateDeparture) || TO_CHAR(v_PremiumMasterBureauInceptionDate) || TO_CHAR(v_PremiumMasterCountersignAgencyType) || TO_CHAR(v_PremiumMasterCountersignAgencyCode) || TO_CHAR(v_PremiumMasterCountersignAgencyState) || TO_CHAR(v_PremiumMasterCountersignAgencyRate) || TO_CHAR(v_PremiumMasterRenewalIndicator)) AS v_PremiumMasterDimHashkey,
		v_PremiumMasterDimHashkey AS PremiumMasterDimHashkey_out
		FROM INPUT
	),
	LKP_PremiumMasterDim AS (
		SELECT
		PremiumMasterDimID,
		PremiumMasterClassCode_IN,
		PremiumMasterDimHashkey,
		PremiumMasterClassCode
		FROM (
			SELECT A.PremiumMasterDimID as PremiumMasterDimID, 
			A.PremiumMasterDimHashkey as PremiumMasterDimHashkey,
			A.PremiumMasterClassCode as PremiumMasterClassCode  
			FROM
			@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumMasterDim A
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumMasterDimHashkey,PremiumMasterClassCode ORDER BY PremiumMasterDimID DESC) = 1
	),
	OUTPUT AS (
		SELECT
		LKP_PremiumMasterDim.PremiumMasterDimID, 
		EXP_Source.PremiumMasterDimHashkey_out AS PremiumMasterDimHashkey, 
		EXP_Source.PremiumMasterPolicyExpirationYear_out AS PremiumMasterPolicyExpirationYear, 
		EXP_Source.PremiumMasterPolicyTerm_out AS PremiumMasterPolicyTerm, 
		EXP_Source.PremiumMasterBureauPolicyType_out AS PremiumMasterBureauPolicyType, 
		EXP_Source.PremiumMasterAuditCode_out AS PremiumMasterAuditCode, 
		EXP_Source.PremiumMasterTypeBureauCode_out AS PremiumMasterTypeBureauCode, 
		EXP_Source.PremiumMasterBureauStatisticalLine_out AS PremiumMasterBureauStatisticalLine, 
		EXP_Source.PremiumMasterProductLine_out AS PremiumMasterProductLine, 
		EXP_Source.PremiumMasterClassCode_out AS PremiumMasterClassCode, 
		EXP_Source.PremiumMasterExposure, 
		EXP_Source.PremiumMasterSubLine_out AS PremiumMasterSubLine, 
		EXP_Source.PremiumMasterStatisticalCode1_out AS PremiumMasterStatisticalCode1, 
		EXP_Source.PremiumMasterStatisticalCode2_out AS PremiumMasterStatisticalCode2, 
		EXP_Source.PremiumMasterStatisticalCode3_out AS PremiumMasterStatisticalCode3, 
		EXP_Source.PremiumMasterRateModifier_out AS PremiumMasterRateModifier, 
		EXP_Source.PremiumMasterRateDeparture_out AS PremiumMasterRateDeparture, 
		EXP_Source.PremiumMasterBureauInceptionDate_out AS PremiumMasterBureauInceptionDate, 
		EXP_Source.PremiumMasterCountersignAgencyType_out AS PremiumMasterCountersignAgencyType, 
		EXP_Source.PremiumMasterCountersignAgencyCode_out AS PremiumMasterCountersignAgencyCode, 
		EXP_Source.PremiumMasterCountersignAgencyState_out AS PremiumMasterCountersignAgencyState, 
		EXP_Source.PremiumMasterCountersignAgencyRate_out AS PremiumMasterCountersignAgencyRate, 
		EXP_Source.PremiumMasterRenewalIndicator_out AS PremiumMasterRenewalIndicator
		FROM EXP_Source
		LEFT JOIN LKP_PremiumMasterDim
		ON LKP_PremiumMasterDim.PremiumMasterDimHashkey = EXP_Source.PremiumMasterDimHashkey_out AND LKP_PremiumMasterDim.PremiumMasterClassCode = EXP_Source.PremiumMasterClassCode_out
	),
),
AGG_RemoveDuplicates AS (
	SELECT
	PremiumMasterDimID, 
	PremiumMasterPolicyExpirationYear1 AS PremiumMasterPolicyExpirationYear, 
	PremiumMasterPolicyTerm1 AS PremiumMasterPolicyTerm, 
	PremiumMasterBureauPolicyType1 AS PremiumMasterBureauPolicyType, 
	PremiumMasterAuditCode1 AS PremiumMasterAuditCode, 
	PremiumMasterTypeBureauCode1 AS PremiumMasterTypeBureauCode, 
	PremiumMasterBureauStatisticalLine1 AS PremiumMasterBureauStatisticalLine, 
	PremiumMasterProductLine1 AS PremiumMasterProductLine, 
	PremiumMasterClassCode1 AS PremiumMasterClassCode, 
	PremiumMasterSubLine1 AS PremiumMasterSubLine, 
	PremiumMasterStatisticalCode11 AS PremiumMasterStatisticalCode1, 
	PremiumMasterStatisticalCode21 AS PremiumMasterStatisticalCode2, 
	PremiumMasterStatisticalCode31 AS PremiumMasterStatisticalCode3, 
	PremiumMasterRateModifier1 AS PremiumMasterRateModifier, 
	PremiumMasterRateDeparture1 AS PremiumMasterRateDeparture, 
	PremiumMasterBureauInceptionDate1 AS PremiumMasterBureauInceptionDate, 
	PremiumMasterCountersignAgencyType1 AS PremiumMasterCountersignAgencyType, 
	PremiumMasterCountersignAgencyCode1 AS PremiumMasterCountersignAgencyCode, 
	PremiumMasterCountersignAgencyState1 AS PremiumMasterCountersignAgencyState, 
	PremiumMasterCountersignAgencyRate1 AS PremiumMasterCountersignAgencyRate, 
	PremiumMasterRenewalIndicator1 AS PremiumMasterRenewalIndicator, 
	PremiumMasterDimHashkey
	FROM mplt_PremiumMasterDimID
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumMasterDimHashkey ORDER BY NULL) = 1
),
EXP_Values AS (
	SELECT
	PremiumMasterDimID,
	PremiumMasterPolicyExpirationYear,
	PremiumMasterPolicyTerm,
	PremiumMasterBureauPolicyType,
	PremiumMasterAuditCode,
	PremiumMasterTypeBureauCode,
	PremiumMasterBureauStatisticalLine,
	PremiumMasterProductLine,
	PremiumMasterClassCode,
	PremiumMasterSubLine,
	PremiumMasterStatisticalCode1,
	PremiumMasterStatisticalCode2,
	PremiumMasterStatisticalCode3,
	PremiumMasterRateModifier,
	PremiumMasterRateDeparture,
	PremiumMasterBureauInceptionDate,
	PremiumMasterCountersignAgencyType,
	PremiumMasterCountersignAgencyCode,
	PremiumMasterCountersignAgencyState,
	PremiumMasterCountersignAgencyRate,
	PremiumMasterRenewalIndicator,
	PremiumMasterDimHashkey
	FROM AGG_RemoveDuplicates
),
RTR_Insert_Update AS (
	SELECT
	PremiumMasterDimID,
	PremiumMasterDimHashkey,
	PremiumMasterPolicyExpirationYear,
	PremiumMasterPolicyTerm,
	PremiumMasterBureauPolicyType,
	PremiumMasterAuditCode,
	PremiumMasterTypeBureauCode,
	PremiumMasterBureauStatisticalLine,
	PremiumMasterProductLine,
	PremiumMasterClassCode,
	PremiumMasterSubLine,
	PremiumMasterStatisticalCode1,
	PremiumMasterStatisticalCode2,
	PremiumMasterStatisticalCode3,
	PremiumMasterRateModifier,
	PremiumMasterRateDeparture,
	PremiumMasterBureauInceptionDate,
	PremiumMasterCountersignAgencyType,
	PremiumMasterCountersignAgencyCode,
	PremiumMasterCountersignAgencyState,
	PremiumMasterCountersignAgencyRate,
	PremiumMasterRenewalIndicator
	FROM EXP_Values
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE IIF(ISNULL(PremiumMasterDimID),TRUE,FALSE)),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE IIF(NOT ISNULL(PremiumMasterDimID),TRUE,FALSE)),
UPD_Update AS (
	SELECT
	PremiumMasterDimID, 
	PremiumMasterDimHashkey, 
	PremiumMasterPolicyExpirationYear, 
	PremiumMasterPolicyTerm, 
	PremiumMasterBureauPolicyType, 
	PremiumMasterAuditCode, 
	PremiumMasterTypeBureauCode, 
	PremiumMasterBureauStatisticalLine, 
	PremiumMasterProductLine, 
	PremiumMasterClassCode, 
	PremiumMasterSubLine, 
	PremiumMasterStatisticalCode1, 
	PremiumMasterStatisticalCode2, 
	PremiumMasterStatisticalCode AS PremiumMasterStatisticalCode3, 
	PremiumMasterRateModifier, 
	PremiumMasterRateDeparture, 
	PremiumMasterBureauInceptionDate, 
	PremiumMasterCountersignAgencyType, 
	PremiumMasterCountersignAgencyCode, 
	PremiumMasterCountersignAgencyState, 
	PremiumMasterCountersignAgencyRate, 
	PremiumMasterRenewalIndicator
	FROM RTR_Insert_Update_UPDATE
),
PremiumMasterDim_Update AS (
	MERGE INTO PremiumMasterDim AS T
	USING UPD_Update AS S
	ON T.PremiumMasterDimID = S.PremiumMasterDimID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.PremiumMasterDimHashkey = S.PremiumMasterDimHashkey, T.PremiumMasterPolicyExpirationYear = S.PremiumMasterPolicyExpirationYear, T.PremiumMasterPolicyTerm = S.PremiumMasterPolicyTerm, T.PremiumMasterBureauPolicyType = S.PremiumMasterBureauPolicyType, T.PremiumMasterAuditCode = S.PremiumMasterAuditCode, T.PremiumMasterTypeBureauCode = S.PremiumMasterTypeBureauCode, T.PremiumMasterBureauStatisticalLine = S.PremiumMasterBureauStatisticalLine, T.PremiumMasterProductLine = S.PremiumMasterProductLine, T.PremiumMasterClassCode = S.PremiumMasterClassCode, T.PremiumMasterSubLine = S.PremiumMasterSubLine, T.PremiumMasterStatisticalCode1 = S.PremiumMasterStatisticalCode1, T.PremiumMasterStatisticalCode2 = S.PremiumMasterStatisticalCode2, T.PremiumMasterStatisticalCode3 = S.PremiumMasterStatisticalCode3, T.PremiumMasterRateModifier = S.PremiumMasterRateModifier, T.PremiumMasterRateDeparture = S.PremiumMasterRateDeparture, T.PremiumMasterBureauInceptionDate = S.PremiumMasterBureauInceptionDate, T.PremiumMasterCountersignAgencyType = S.PremiumMasterCountersignAgencyType, T.PremiumMasterCountersignAgencyCode = S.PremiumMasterCountersignAgencyCode, T.PremiumMasterCountersignAgencyState = S.PremiumMasterCountersignAgencyState, T.PremiumMasterCountersignAgencyRate = S.PremiumMasterCountersignAgencyRate, T.PremiumMasterRenewalIndicator = S.PremiumMasterRenewalIndicator
),
EXP_Target AS (
	SELECT
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	PremiumMasterDimHashkey,
	PremiumMasterPolicyExpirationYear,
	PremiumMasterPolicyTerm,
	PremiumMasterBureauPolicyType,
	PremiumMasterAuditCode,
	PremiumMasterTypeBureauCode,
	PremiumMasterBureauStatisticalLine,
	PremiumMasterProductLine,
	PremiumMasterClassCode,
	PremiumMasterSubLine,
	PremiumMasterStatisticalCode AS PremiumMasterStatisticalCode1,
	PremiumMasterStatisticalCode2,
	PremiumMasterStatisticalCode3,
	PremiumMasterRateModifier,
	PremiumMasterRateDeparture,
	PremiumMasterBureauInceptionDate,
	PremiumMasterCountersignAgencyType,
	PremiumMasterCountersignAgencyCode,
	PremiumMasterCountersignAgencyState,
	PremiumMasterCountersignAgencyRate,
	PremiumMasterRenewalIndicator
	FROM RTR_Insert_Update_INSERT
),
PremiumMasterDim_Insert AS (
	INSERT INTO PremiumMasterDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, PremiumMasterDimHashkey, PremiumMasterPolicyExpirationYear, PremiumMasterPolicyTerm, PremiumMasterBureauPolicyType, PremiumMasterAuditCode, PremiumMasterTypeBureauCode, PremiumMasterBureauStatisticalLine, PremiumMasterProductLine, PremiumMasterClassCode, PremiumMasterSubLine, PremiumMasterStatisticalCode1, PremiumMasterStatisticalCode2, PremiumMasterStatisticalCode3, PremiumMasterRateModifier, PremiumMasterRateDeparture, PremiumMasterBureauInceptionDate, PremiumMasterCountersignAgencyType, PremiumMasterCountersignAgencyCode, PremiumMasterCountersignAgencyState, PremiumMasterCountersignAgencyRate, PremiumMasterRenewalIndicator)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	PREMIUMMASTERDIMHASHKEY, 
	PREMIUMMASTERPOLICYEXPIRATIONYEAR, 
	PREMIUMMASTERPOLICYTERM, 
	PREMIUMMASTERBUREAUPOLICYTYPE, 
	PREMIUMMASTERAUDITCODE, 
	PREMIUMMASTERTYPEBUREAUCODE, 
	PREMIUMMASTERBUREAUSTATISTICALLINE, 
	PREMIUMMASTERPRODUCTLINE, 
	PREMIUMMASTERCLASSCODE, 
	PREMIUMMASTERSUBLINE, 
	PREMIUMMASTERSTATISTICALCODE1, 
	PREMIUMMASTERSTATISTICALCODE2, 
	PREMIUMMASTERSTATISTICALCODE3, 
	PREMIUMMASTERRATEMODIFIER, 
	PREMIUMMASTERRATEDEPARTURE, 
	PREMIUMMASTERBUREAUINCEPTIONDATE, 
	PREMIUMMASTERCOUNTERSIGNAGENCYTYPE, 
	PREMIUMMASTERCOUNTERSIGNAGENCYCODE, 
	PREMIUMMASTERCOUNTERSIGNAGENCYSTATE, 
	PREMIUMMASTERCOUNTERSIGNAGENCYRATE, 
	PREMIUMMASTERRENEWALINDICATOR
	FROM EXP_Target
),