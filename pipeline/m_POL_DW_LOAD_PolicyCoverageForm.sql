WITH
SQ_Pif43GTLTITStage AS (
	SELECT pc.PolicyCoverageID,
	pif43.Pmd4tYearTransaction,
	pif43.Pmd4tMonthTransaction,
	pif43.Pmd4tDayTransaction,
	pif43.Pmd4tFormNo1,
	pif43.Pmd4tFormMonth1,
	pif43.Pmd4tFormYear1,
	pif43.Pmd4tFormNo2,
	pif43.Pmd4tFormMonth2,
	pif43.Pmd4tFormYear2,
	pif43.Pmd4tFormNo3,
	pif43.Pmd4tFormMonth3,
	pif43.Pmd4tFormYear3,
	pif43.Pmd4tFormNo4,
	pif43.Pmd4tFormMonth4,
	pif43.Pmd4tFormYear4,
	pif43.Pmd4tFormNo5,
	pif43.Pmd4tFormMonth5,
	pif43.Pmd4tFormYear5,
	pif43.Pmd4tFormNo6,
	pif43.Pmd4tFormMonth6,
	pif43.Pmd4tFormYear6,
	pif43.Pmd4tFormNo7,
	pif43.Pmd4tFormMonth7,
	pif43.Pmd4tFormYear7,
	pif43.Pmd4tFormNo8,
	pif43.Pmd4tFormMonth8,
	pif43.Pmd4tFormYear8,
	pif43.Pmd4tFormNo9,
	pif43.Pmd4tFormMonth9,
	pif43.Pmd4tFormYear9,
	pif43.Pmd4tFormNo10,
	pif43.Pmd4tFormMonth10,
	pif43.Pmd4tFormYear10,
	pif43.Pmd4tFormNo11,
	pif43.Pmd4tFormMonth11,
	pif43.Pmd4tFormYear11,
	pif43.Pmd4tFormNo12,
	pif43.Pmd4tFormMonth12,
	pif43.Pmd4tFormYear12,
	pif43.Pmd4tFormNo13,
	pif43.Pmd4tFormMonth13,
	pif43.Pmd4tFormYear13,
	pif43.Pmd4tFormNo14,
	pif43.Pmd4tFormMonth14,
	pif43.Pmd4tFormYear14,
	pif43.Pmd4tFormNo15,
	pif43.Pmd4tFormMonth15,
	pif43.Pmd4tFormYear15,
	pif43.Pmd4tFormNo16,
	pif43.Pmd4tFormMonth16,
	pif43.Pmd4tFormYear16
	
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43GTLTITStage pif43
	JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol
	ON pif43.PifSymbol=pol.pol_sym
	and pif43.PifPolicyNumber=pol.pol_num
	and pif43.PifModule=pol.pol_mod
	JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation rl
	on pol.pol_ak_id=rl.PolicyAKID
	and pif43.Pmd4tLocationNumber=rl.LocationUnitNumber
	JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage pc
	on rl.RiskLocationAKID=pc.RiskLocationAKID
	and pif43.Pmd4tInsuranceLine=pc.InsuranceLine
),
NRM_PolicyCoverageForm AS (
),
FIL_PolicyCoverageForm AS (
	SELECT
	PolicyCoverageID, 
	Pmd4tYearTransaction, 
	Pmd4tMonthTransaction, 
	Pmd4tDayTransaction, 
	Pmd4tFormNo, 
	Pmd4tFormMonth, 
	Pmd4tFormYear
	FROM NRM_PolicyCoverageForm
	WHERE NOT ISNULL(Pmd4tFormNo) OR LENGTH(LTRIM(RTRIM(Pmd4tFormNo)))>0
),
AGG_PolicyCoverageForm AS (
	SELECT
	PolicyCoverageID,
	Pmd4tYearTransaction,
	Pmd4tMonthTransaction,
	Pmd4tDayTransaction,
	Pmd4tFormNo,
	Pmd4tFormMonth,
	Pmd4tFormYear
	FROM FIL_PolicyCoverageForm
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyCoverageID, Pmd4tYearTransaction, Pmd4tMonthTransaction, Pmd4tDayTransaction, Pmd4tFormNo, Pmd4tFormMonth, Pmd4tFormYear ORDER BY NULL) = 1
),
EXP_GetValues AS (
	SELECT
	PolicyCoverageID AS i_PolicyCoverageID,
	Pmd4tYearTransaction AS i_Pmd4tYearTransaction,
	Pmd4tMonthTransaction AS i_Pmd4tMonthTransaction,
	Pmd4tDayTransaction AS i_Pmd4tDayTransaction,
	Pmd4tFormNo AS i_Pmd4tFormNo,
	Pmd4tFormMonth AS i_Pmd4tFormMonth,
	Pmd4tFormYear AS i_Pmd4tFormYear,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	SYSDATE AS o_CreateDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: LTRIM(RTRIM(i_Pmd4tFormNo))
	LTRIM(RTRIM(i_Pmd4tFormNo)) AS o_FormNum,
	-- *INF*: TO_DATE(TO_CHAR(i_Pmd4tYearTransaction) || LPAD(TO_CHAR(i_Pmd4tMonthTransaction),2,'0') || LPAD(TO_CHAR(i_Pmd4tDayTransaction),2,'0'),'YYYYMMDD')
	TO_DATE(TO_CHAR(i_Pmd4tYearTransaction) || LPAD(TO_CHAR(i_Pmd4tMonthTransaction), 2, '0') || LPAD(TO_CHAR(i_Pmd4tDayTransaction), 2, '0'), 'YYYYMMDD') AS v_FormTransactionDate,
	-- *INF*: IIF(ISNULL(i_Pmd4tFormMonth) OR ISNULL(i_Pmd4tFormYear) OR i_Pmd4tFormMonth=0, NULL,TO_DATE(LPAD(TO_CHAR(i_Pmd4tFormMonth),2,'0') || '01' || LPAD(TO_CHAR(i_Pmd4tFormYear),2,'0'),'MMDDRR'))
	IFF(i_Pmd4tFormMonth IS NULL OR i_Pmd4tFormYear IS NULL OR i_Pmd4tFormMonth = 0, NULL, TO_DATE(LPAD(TO_CHAR(i_Pmd4tFormMonth), 2, '0') || '01' || LPAD(TO_CHAR(i_Pmd4tFormYear), 2, '0'), 'MMDDRR')) AS v_FormDate,
	-- *INF*: IIF(ISNULL(v_FormDate),TO_DATE('18000101','YYYYMMDD'),v_FormDate)
	IFF(v_FormDate IS NULL, TO_DATE('18000101', 'YYYYMMDD'), v_FormDate) AS o_FormEditionDate,
	i_PolicyCoverageID AS o_PolicyCoverageID
	FROM AGG_PolicyCoverageForm
),
LKP_Form AS (
	SELECT
	FormId,
	FormNumber,
	FormEditionDate
	FROM (
		select FormId AS FormId,
		LTRIM(RTRIM(FormNumber)) AS FormNumber,
		FormEditionDate AS FormEditionDate
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.Form
		where SourceSystemID='PMS'
		order by FormNumber,FormEditionDate,FormExpirationDate,FormName desc
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY FormNumber,FormEditionDate ORDER BY FormId) = 1
),
FIL_Result AS (
	SELECT
	EXP_GetValues.o_SourceSystemID AS SourceSystemID, 
	EXP_GetValues.o_AuditID AS AuditID, 
	EXP_GetValues.o_CreateDate AS CreateDate, 
	EXP_GetValues.o_ModifiedDate AS ModifiedDate, 
	LKP_Form.FormId, 
	EXP_GetValues.o_PolicyCoverageID AS PolicyCoverageID
	FROM EXP_GetValues
	LEFT JOIN LKP_Form
	ON LKP_Form.FormNumber = EXP_GetValues.o_FormNum AND LKP_Form.FormEditionDate = EXP_GetValues.o_FormEditionDate
	WHERE NOT ISNULL(FormId)
),
LKP_PolicyCoverageForm AS (
	SELECT
	FormID,
	PolicyCoverageID
	FROM (
		SELECT 
			FormID,
			PolicyCoverageID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverageForm
		WHERE SourceSystemID='PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY FormID,PolicyCoverageID ORDER BY FormID) = 1
),
FIL_Existing AS (
	SELECT
	LKP_PolicyCoverageForm.FormID AS lkp_FormID, 
	FIL_Result.SourceSystemID, 
	FIL_Result.AuditID, 
	FIL_Result.CreateDate, 
	FIL_Result.ModifiedDate, 
	FIL_Result.FormId AS FormID, 
	FIL_Result.PolicyCoverageID
	FROM FIL_Result
	LEFT JOIN LKP_PolicyCoverageForm
	ON LKP_PolicyCoverageForm.FormID = FIL_Result.FormId AND LKP_PolicyCoverageForm.PolicyCoverageID = FIL_Result.PolicyCoverageID
	WHERE ISNULL(lkp_FormID)
),
PolicyCoverageForm AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverageForm
	(SourceSystemID, AuditID, CreatedDate, ModifiedDate, FormID, PolicyCoverageID)
	SELECT 
	SOURCESYSTEMID, 
	AUDITID, 
	CreateDate AS CREATEDDATE, 
	MODIFIEDDATE, 
	FORMID, 
	POLICYCOVERAGEID
	FROM FIL_Existing
),