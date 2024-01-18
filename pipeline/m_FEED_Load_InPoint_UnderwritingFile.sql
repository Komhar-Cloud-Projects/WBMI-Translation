WITH
SQ_CommercialProductManagementExtract AS (
	SELECT
		CommercialProductManagementExtractId AS WorkCommericalProductManagementExtractId,
		AuditId,
		ModifiedDate,
		CustomerNumber,
		PolicyNumber,
		PolicyOfferingDescription,
		FirstNamedInsured,
		PolicyEffectiveDate,
		DirectWrittenPremium,
		DirectLossIncurred,
		DirectLossIncurredRatio,
		DirectLossIncurred3Yrs,
		DirectLossIncurredRatio3Yrs,
		PrimaryBusinessClassCode AS PrimaryBusinessClassificationCode,
		PrimaryBusinessClassDescription AS PrimaryBusinessClassificationDescript,
		StrategicProfitCenterDescription,
		ProgramCode,
		ProgramDescription,
		IndustryRiskGradeCode,
		AgencyCode,
		AbbreviatedAgencyName,
		UnderwritingRegionCodeDescription,
		PolicyIssueCode,
		RunDate AS PreviousMonthDate,
		AccountOver150KFlag AS AccountOver150K,
		InsuranceReferenceLineOfBusinessDescription,
		UnderWriterName,
		UnderWriterManagerName,
		StrategicBusinessGroupDescription,
		SalesTerritoryDescription,
		UnderwriterManagerEmailAddress,
		UnderwriterManagerCode,
		UnderwriterEmailAddress,
		UnderwriterCode,
		AccountSize
	FROM CommercialProductManagementExtract
),
EXP_WorkCommericalProductManagementExtract AS (
	SELECT
	-- *INF*: GET_DATE_PART ( SYSDATE, 'YYYY' )
	DATE_PART(CURRENT_TIMESTAMP, 'YYYY') AS v_Curr_Year,
	-- *INF*: GET_DATE_PART ( SYSDATE, 'YYYY' ) - 1
	DATE_PART(CURRENT_TIMESTAMP, 'YYYY') - 1 AS v_Prev_Year,
	-- *INF*: IIF(GET_DATE_PART ( SYSDATE, 'MM' )=1,
	-- v_Prev_Year||12,v_Curr_Year)
	IFF(DATE_PART(CURRENT_TIMESTAMP, 'MM') = 1, v_Prev_Year || 12, v_Curr_Year) AS v_Year,
	-- *INF*: IIF(GET_DATE_PART ( SYSDATE, 'MM' )-1 =1,'01',
	-- IIF(GET_DATE_PART ( SYSDATE, 'MM' )-1 =2,'02',
	-- IIF(GET_DATE_PART ( SYSDATE, 'MM' )-1 =3,'03',
	-- IIF(GET_DATE_PART ( SYSDATE, 'MM' )-1 =4,'04',
	--  IIF(GET_DATE_PART ( SYSDATE, 'MM' )-1 =5,'05',
	--  IIF(GET_DATE_PART ( SYSDATE, 'MM' )-1 =6,'06',
	--  IIF(GET_DATE_PART ( SYSDATE, 'MM' )-1 =7,'07',
	--  IIF(GET_DATE_PART ( SYSDATE, 'MM' )-1 =8,'08',
	--  IIF(GET_DATE_PART ( SYSDATE, 'MM' )-1 =9,'09',
	--  IIF(GET_DATE_PART ( SYSDATE, 'MM' )-1 =10,'10',
	--  IIF(GET_DATE_PART ( SYSDATE, 'MM' )-1 =11,'11',
	--  IIF(GET_DATE_PART ( SYSDATE, 'MM' )-1 =12,'12'))))))))))))
	IFF(
	    DATE_PART(CURRENT_TIMESTAMP, 'MM') - 1 = 1, '01',
	    IFF(
	        DATE_PART(CURRENT_TIMESTAMP, 'MM') - 1 = 2, '02',
	        IFF(
	            DATE_PART(CURRENT_TIMESTAMP, 'MM') - 1 = 3, '03',
	            IFF(
	                DATE_PART(CURRENT_TIMESTAMP, 'MM') - 1 = 4, '04',
	                IFF(
	                    DATE_PART(CURRENT_TIMESTAMP, 'MM') - 1 = 5, '05',
	                    IFF(
	                        DATE_PART(CURRENT_TIMESTAMP, 'MM') - 1 = 6, '06',
	                        IFF(
	                            DATE_PART(CURRENT_TIMESTAMP, 'MM') - 1 = 7,
	                            '07',
	                            IFF(
	                                DATE_PART(CURRENT_TIMESTAMP, 'MM') - 1 = 8,
	                                '08',
	                                IFF(
	                                    DATE_PART(CURRENT_TIMESTAMP, 'MM') - 1 = 9,
	                                    '09',
	                                    IFF(
	                                        DATE_PART(CURRENT_TIMESTAMP, 'MM') - 1 = 10,
	                                        '10',
	                                        IFF(
	                                            DATE_PART(CURRENT_TIMESTAMP, 'MM') - 1 = 11,
	                                            '11',
	                                            IFF(
	                                                DATE_PART(CURRENT_TIMESTAMP, 'MM') - 1 = 12,
	                                                '12'
	                                            )
	                                        )
	                                    )
	                                )
	                            )
	                        )
	                    )
	                )
	            )
	        )
	    )
	) AS v_Month,
	-- *INF*: --IIF(GET_DATE_PART ( SYSDATE, 'MM' )-1 =0,31,
	-- GET_DATE_PART(LAST_DAY(ADD_TO_DATE(SYSDATE,'MM',-1)),'DD')
	-- --)
	DATE_PART(LAST_DAY(DATEADD(MONTH,- 1,CURRENT_TIMESTAMP)), 'DD') AS v_Day,
	'us_ftp_westbend-uw_'||v_Year||v_Month||v_Day||'.csv' AS FileName,
	WorkCommericalProductManagementExtractId,
	AuditId,
	ModifiedDate,
	CustomerNumber,
	PolicyNumber,
	PolicyOfferingDescription AS PolicyOffering,
	FirstNamedInsured,
	-- *INF*:  REPLACESTR(1,'"'||FirstNamedInsured||'"','"','')
	-- 
	REGEXP_REPLACE('"' || FirstNamedInsured || '"','"','') AS o_FirstNamedInsured,
	PolicyEffectiveDate,
	-- *INF*: TO_Date (TO_CHAR (PolicyEffectiveDate, 'MON DD YYYY' ), 'MON DD YYYY' ) 
	TO_TIMESTAMP(TO_CHAR(PolicyEffectiveDate, 'MON DD YYYY'), 'MON DD YYYY') AS o_PolicyEffectiveDate,
	DirectWrittenPremium,
	-- *INF*: IIF(ISNULL(DirectWrittenPremium),'0.00',
	-- iif(DirectWrittenPremium<0.00 ,'('||DirectWrittenPremium||')',
	-- to_char(DirectWrittenPremium)))
	IFF(
	    DirectWrittenPremium IS NULL, '0.00',
	    IFF(
	        DirectWrittenPremium < 0.00, '(' || DirectWrittenPremium || ')',
	        to_char(DirectWrittenPremium)
	    )
	) AS o_DirectWrittenPremium,
	DirectLossIncurred,
	-- *INF*: IIF(ISNULL(DirectLossIncurred),'0.00',
	-- iif(DirectLossIncurred<0.00 ,'('||DirectLossIncurred||')',
	-- to_char(DirectLossIncurred)))
	IFF(
	    DirectLossIncurred IS NULL, '0.00',
	    IFF(
	        DirectLossIncurred < 0.00, '(' || DirectLossIncurred || ')',
	        to_char(DirectLossIncurred)
	    )
	) AS o_DirectLossIncurred,
	DirectLossIncurredRatio,
	-- *INF*: IIF(ISNULL(DirectLossIncurredRatio),0,DirectLossIncurredRatio)
	IFF(DirectLossIncurredRatio IS NULL, 0, DirectLossIncurredRatio) AS o_DirectLossIncurredRatio,
	DirectLossIncurred3Yrs,
	-- *INF*: IIF(ISNULL(DirectLossIncurred3Yrs),'0.00',
	-- iif(DirectLossIncurred3Yrs<0.00 ,'('||DirectLossIncurred3Yrs||')',
	-- to_char(DirectLossIncurred3Yrs)))
	IFF(
	    DirectLossIncurred3Yrs IS NULL, '0.00',
	    IFF(
	        DirectLossIncurred3Yrs < 0.00, '(' || DirectLossIncurred3Yrs || ')',
	        to_char(DirectLossIncurred3Yrs)
	    )
	) AS o_DirectLossIncurred3Yr,
	DirectLossIncurredRatio3Yrs,
	-- *INF*: IIF(ISNULL(DirectLossIncurredRatio3Yrs),'0.0',TO_CHAR(DirectLossIncurredRatio3Yrs))
	-- 
	-- 
	IFF(DirectLossIncurredRatio3Yrs IS NULL, '0.0', TO_CHAR(DirectLossIncurredRatio3Yrs)) AS o_DirectLossIncurredRatio3Yrs,
	PrimaryBusinessClassificationCode,
	PrimaryBusinessClassificationDescript,
	StrategicProfitCenterDescription,
	ProgramCode,
	ProgramDescription,
	IndustryRiskGradeCode,
	AgencyCode,
	AbbreviatedAgencyName,
	UnderwritingRegionCodeDescription,
	PolicyIssueCode,
	PreviousMonthDate,
	-- *INF*: TO_CHAR(to_date(PreviousMonthDate,'MM/DD/YYYY'),'YYYY MON')
	TO_CHAR(TO_TIMESTAMP(PreviousMonthDate, 'MM/DD/YYYY'), 'YYYY MON') AS o_PreviousMonthDate,
	AccountOver150K,
	-- *INF*: iif(AccountOver150K='True','Yes','No')
	IFF(AccountOver150K = 'True', 'Yes', 'No') AS o_AccountOver150K,
	InsuranceReferenceLineOfBusinessDescription AS i_InsuranceReferenceLineOfBusinessDescription,
	i_InsuranceReferenceLineOfBusinessDescription AS o_InsuranceReferenceLineOfBusinessDescription,
	UnderWriterName AS i_UnderWriterName,
	'' AS o_UnderWriterName,
	UnderWriterManagerName AS i_UnderWriterManagerName,
	'' AS o_UnderWriterManagerName,
	StrategicBusinessGroupDescription,
	SalesTerritoryDescription,
	UnderwriterManagerEmailAddress AS i_UnderwriterManagerEmailAddress,
	'' AS o_UnderwriterManagerEmailAddress,
	UnderwriterManagerCode AS i_UnderwriterManagerCode,
	'' AS o_UnderwriterManagerCode,
	UnderwriterEmailAddress AS i_UnderwriterEmailAddress,
	'' AS o_UnderwriterEmailAddress,
	UnderwriterCode AS i_UnderwriterCode,
	'' AS o_UnderwriterCode,
	AccountSize
	FROM SQ_CommercialProductManagementExtract
),
CommericalProductManagementExtractFile AS (
	INSERT INTO CommericalProductManagementExtractFile
	(FileName, CustomerNumber, PolicyNumber, PolicyOffering, FirstNamedInsured, PolicyEffectiveDate, DirectWrittenPremium, DirectLossIncurred, DirectLossIncurredRatio, DirectLossIncurred3Yr, DirectLossIncurredRatio3Yr, PrimaryBusinessClassificationCode, PrimaryBusinessClassificationDescript, StrategicBusinessGroupDescription, ProgramCode, ProgramDescription, IndustryRiskGradeCode, AgencyCode, AgencyName, UWRegion, PolicyIssueCode, AsofDate, AccountOver150K, InsuranceReferenceLineOfBusinessDescription, StrategicProfitCenterDescription, SalesTerritoryDescription, UnderWriterName, UnderWriterManagerName, UnderwriterManagerEmailAddress, UnderwriterEmailAddress, UnderwriterManagerWestBendAssociateID, UnderwriterWestBendAssociateID, AccountSize)
	SELECT 
	FILENAME, 
	CUSTOMERNUMBER, 
	POLICYNUMBER, 
	POLICYOFFERING, 
	o_FirstNamedInsured AS FIRSTNAMEDINSURED, 
	o_PolicyEffectiveDate AS POLICYEFFECTIVEDATE, 
	o_DirectWrittenPremium AS DIRECTWRITTENPREMIUM, 
	o_DirectLossIncurred AS DIRECTLOSSINCURRED, 
	o_DirectLossIncurredRatio AS DIRECTLOSSINCURREDRATIO, 
	o_DirectLossIncurred3Yr AS DIRECTLOSSINCURRED3YR, 
	o_DirectLossIncurredRatio3Yrs AS DIRECTLOSSINCURREDRATIO3YR, 
	PRIMARYBUSINESSCLASSIFICATIONCODE, 
	PRIMARYBUSINESSCLASSIFICATIONDESCRIPT, 
	STRATEGICBUSINESSGROUPDESCRIPTION, 
	PROGRAMCODE, 
	PROGRAMDESCRIPTION, 
	INDUSTRYRISKGRADECODE, 
	AGENCYCODE, 
	AbbreviatedAgencyName AS AGENCYNAME, 
	UnderwritingRegionCodeDescription AS UWREGION, 
	POLICYISSUECODE, 
	o_PreviousMonthDate AS ASOFDATE, 
	o_AccountOver150K AS ACCOUNTOVER150K, 
	o_InsuranceReferenceLineOfBusinessDescription AS INSURANCEREFERENCELINEOFBUSINESSDESCRIPTION, 
	STRATEGICPROFITCENTERDESCRIPTION, 
	SALESTERRITORYDESCRIPTION, 
	o_UnderWriterName AS UNDERWRITERNAME, 
	o_UnderWriterManagerName AS UNDERWRITERMANAGERNAME, 
	o_UnderwriterManagerEmailAddress AS UNDERWRITERMANAGEREMAILADDRESS, 
	o_UnderwriterEmailAddress AS UNDERWRITEREMAILADDRESS, 
	o_UnderwriterManagerCode AS UNDERWRITERMANAGERWESTBENDASSOCIATEID, 
	o_UnderwriterCode AS UNDERWRITERWESTBENDASSOCIATEID, 
	ACCOUNTSIZE
	FROM EXP_WorkCommericalProductManagementExtract
),