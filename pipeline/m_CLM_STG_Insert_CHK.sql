WITH
SQ_Chk AS (
	SELECT
		RecordId,
		CTpId,
		Id,
		OrigId,
		AltSrt,
		AltSrt1,
		IdPre,
		ModVer,
		ModCd,
		CmpId,
		PayToNam1,
		PayToNam2,
		PayToNam3,
		IssDt,
		PayAmt,
		OrigPayAmt,
		ResrvAmt,
		BnkId,
		BnkNum,
		LosDt,
		Dt1,
		Dt2,
		Dt3,
		Dt4,
		Dt5,
		Time1,
		Time2,
		TranCd,
		TaxId,
		TaxTyp,
		Tax1099,
		RptAmt1099,
		SpltPay1099,
		VndTyp,
		VndId,
		AgentTyp,
		AgentId,
		MailToNam,
		MailToAdr1,
		MailToAdr2,
		MailToAdr3,
		MailToAdr4,
		MailToAdr5,
		City,
		State,
		CntyCd,
		CountryId,
		ZipCd,
		BillState,
		BillDt,
		PhNum1,
		PhNum2,
		FaxNum,
		FaxNumTyp,
		FaxToNam,
		EmailAdr,
		MrgId,
		MrgId2,
		PayCd,
		PayToCd,
		ReqId,
		ExamId,
		ExamNam,
		AdjId,
		CurId,
		Office,
		DeptCd,
		MailStop,
		ReissCd,
		AtchCd,
		ReqNum,
		ImpBch,
		ImpBnkBch,
		PrtBch,
		RcnBch,
		SavRcnBch,
		ExpBch,
		PdBch,
		VoidExpCd,
		PrevVoidExpCd,
		WriteOffExpCd,
		SrchLtrCd,
		PrtCnt,
		RcnCd,
		VoidCd,
		VoidId,
		VoidDt,
		UnVoidCd,
		UnVoidId,
		UnVoidDt,
		SigCd,
		SigCd1,
		SigCd2,
		DrftCd,
		DscCd,
		RestCd,
		XCd1,
		XCd2,
		XCd3,
		XCd4,
		XCd5,
		XCd6,
		XCd7,
		XCd8,
		XCd9,
		XCd10,
		PayRate,
		XRate1,
		XRate2,
		XRate3,
		XAmt1,
		XAmt2,
		XAmt3,
		XAmt4,
		XAmt5,
		XAmt6,
		XAmt7,
		XAmt8,
		XAmt9,
		XAmt10,
		SalaryAmt,
		MaritalStat,
		FedExempt,
		StateExempt,
		Day30Cd,
		PstCd,
		RsnCd,
		PdCd,
		PdDt,
		ApprovCd,
		ApprovDt,
		ApprovId,
		ApprovCd2,
		ApprovDt2,
		ApprovId2,
		ApprovCd3,
		ApprovDt3,
		ApprovId3,
		ApprovCd4,
		ApprovDt4,
		ApprovId4,
		ApprovCd5,
		ApprovDt5,
		ApprovId5,
		ApprovCd6,
		ApprovDt6,
		ApprovId6,
		ApprovCd7,
		ApprovDt7,
		ApprovId7,
		ApprovCd8,
		ApprovDt8,
		ApprovId8,
		ApprovCd9,
		ApprovDt9,
		ApprovId9,
		AddDt,
		AddTime,
		AddId,
		ChgDt,
		ChgTime,
		ChgId,
		SrceCd,
		FrmCd,
		RefNum,
		NamTyp,
		LstNam,
		FstNam,
		MidInit,
		Salutation,
		AcctNum,
		ExpAcct,
		DebitAcct,
		BnkAcct,
		BnkRout,
		AcctNam,
		EftTypCd,
		BnkAcct2,
		BnkRout2,
		AcctNam2,
		EftTypCd2,
		BnkAcct3,
		BnkRout3,
		AcctNam3,
		EftTypCd3,
		AllocPct1,
		AllocPct2,
		AllocPct3,
		OptCd,
		EftTranCd,
		AdviceTyp,
		RepRsn,
		EmployerTyp,
		EmployerId,
		EmployerNam,
		EmployerAdr1,
		EmployerAdr2,
		EmployerAdr3,
		ProviderTyp,
		ProviderId,
		ProviderNam,
		CarrierTyp,
		CarrierId,
		PolId,
		InsNam,
		InsAdr1,
		InsAdr2,
		InsAdr3,
		ClaimNum,
		ClmntNum,
		ClmntNam,
		ClmntAdr1,
		ClmntAdr2,
		ClmntAdr3,
		LosCause,
		DiagCd1,
		DiagCd2,
		DiagCd3,
		DiagCd4,
		ForRsn1,
		ForRsn2,
		ForRsn3,
		CommentTxt,
		XNum1,
		XNum2,
		XNum3,
		XNum4,
		TransferOutBch,
		TransferInBch,
		VchCnt,
		PrtDt,
		PrtId,
		TranDt,
		TranTime,
		TranTyp,
		TranId,
		BTpId,
		ExamTyp,
		Priority,
		DeliveryDt,
		CardNum,
		CardTyp,
		ExportStat,
		PrevExportStat
	FROM Chk
),
EXP_AUDIT_FIELDS AS (
	SELECT
	SYSDATE AS EXTRACT_DATE,
	SYSDATE AS AS_OF_DATE,
	'' AS RECORD_COUNT_OP,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	RecordId,
	CTpId,
	Id,
	TranCd,
	PdDt
	FROM SQ_Chk
),
Chk_stage AS (
	TRUNCATE TABLE Chk_stage;
	INSERT INTO Chk_stage
	(RecordId, CTpId, Id, TranCd, PdDt, extract_date, as_of_Date, record_count, source_system_id)
	SELECT 
	RECORDID, 
	CTPID, 
	ID, 
	TRANCD, 
	PDDT, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT_OP AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID
	FROM EXP_AUDIT_FIELDS
),