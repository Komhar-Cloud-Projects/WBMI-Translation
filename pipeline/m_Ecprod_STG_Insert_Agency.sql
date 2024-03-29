WITH
SQ_Agency AS (
	SELECT
		AgencyCode,
		AgencyName,
		AgencyCity,
		AgencyState,
		AgencyPhone1,
		AgencyPhone2,
		AgencyPhone3,
		AgencyActiveCC,
		AgencyActiveYY,
		AgencyActiveMM,
		AgencyActiveDD,
		AgencyOwnerLName,
		AgencyOwnerFName,
		AgencyOwnerEmail,
		AgencyContactLName,
		AgencyContactFName,
		AgencyContactEmail,
		AgencyContactsubcode,
		AgencyUpdated,
		TimeStamp,
		AgencyPayCode,
		PAgencyCode,
		AgencyRSMTerr,
		AgencySR22,
		AgencyAppsSwitch,
		AgencyCommercialSwitch,
		AgencyBillClaimSwitch,
		HHSwitch,
		EFTAgreementFlag,
		InterfaceAgreementFlag,
		AppSubmissionSignupFlag,
		ChoicePointFlag,
		ChoicePoint_account,
		NSIBondFlag,
		iibond_bill_code,
		loss_history_code,
		modified_date,
		modified_user_id,
		service_center_code
	FROM Agency
),
EXP_Values AS (
	SELECT
	SYSDATE AS EXTRACT_DATE,
	SYSDATE AS AS_OF_DATE,
	'' AS RECORD_COUNT,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	AgencyCode,
	AgencyName,
	AgencyCity,
	AgencyState,
	AgencyPhone1,
	AgencyPhone2,
	AgencyPhone3,
	AgencyActiveCC,
	AgencyActiveYY,
	AgencyActiveMM,
	AgencyActiveDD,
	AgencyOwnerLName,
	AgencyOwnerFName,
	AgencyOwnerEmail,
	AgencyContactLName,
	AgencyContactFName,
	AgencyContactEmail,
	AgencyContactsubcode,
	AgencyUpdated,
	TimeStamp,
	AgencyPayCode,
	PAgencyCode,
	AgencyRSMTerr,
	AgencySR22,
	AgencyAppsSwitch,
	AgencyCommercialSwitch,
	AgencyBillClaimSwitch,
	HHSwitch,
	EFTAgreementFlag,
	InterfaceAgreementFlag,
	AppSubmissionSignupFlag,
	ChoicePointFlag,
	ChoicePoint_account,
	NSIBondFlag,
	iibond_bill_code,
	loss_history_code,
	modified_date,
	modified_user_id,
	service_center_code
	FROM SQ_Agency
),
agency_ecprod_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.agency_ecprod_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.agency_ecprod_stage
	(AgencyCode, AgencyName, AgencyCity, AgencyState, AgencyPhone1, AgencyPhone2, AgencyPhone3, AgencyActiveCC, AgencyActiveYY, AgencyActiveMM, AgencyActiveDD, AgencyOwnerLName, AgencyOwnerFName, AgencyOwnerEmail, AgencyContactLName, AgencyContactFName, AgencyContactEmail, AgencyContactsubcode, AgencyUpdated, TimeStamp, AgencyPayCode, PAgencyCode, AgencyRSMTerr, AgencySR22, AgencyAppsSwitch, AgencyCommercialSwitch, AgencyBillClaimSwitch, HHSwitch, EFTAgreementFlag, InterfaceAgreementFlag, AppSubmissionSignupFlag, ChoicePointFlag, ChoicePoint_account, NSIBondFlag, iibond_bill_code, loss_history_code, modified_date, modified_user_id, service_center_code, extract_date, as_of_date, record_count, source_system_id)
	SELECT 
	AGENCYCODE, 
	AGENCYNAME, 
	AGENCYCITY, 
	AGENCYSTATE, 
	AGENCYPHONE1, 
	AGENCYPHONE2, 
	AGENCYPHONE3, 
	AGENCYACTIVECC, 
	AGENCYACTIVEYY, 
	AGENCYACTIVEMM, 
	AGENCYACTIVEDD, 
	AGENCYOWNERLNAME, 
	AGENCYOWNERFNAME, 
	AGENCYOWNEREMAIL, 
	AGENCYCONTACTLNAME, 
	AGENCYCONTACTFNAME, 
	AGENCYCONTACTEMAIL, 
	AGENCYCONTACTSUBCODE, 
	AGENCYUPDATED, 
	TIMESTAMP, 
	AGENCYPAYCODE, 
	PAGENCYCODE, 
	AGENCYRSMTERR, 
	AGENCYSR22, 
	AGENCYAPPSSWITCH, 
	AGENCYCOMMERCIALSWITCH, 
	AGENCYBILLCLAIMSWITCH, 
	HHSWITCH, 
	EFTAGREEMENTFLAG, 
	INTERFACEAGREEMENTFLAG, 
	APPSUBMISSIONSIGNUPFLAG, 
	CHOICEPOINTFLAG, 
	CHOICEPOINT_ACCOUNT, 
	NSIBONDFLAG, 
	IIBOND_BILL_CODE, 
	LOSS_HISTORY_CODE, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	SERVICE_CENTER_CODE, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID
	FROM EXP_Values
),