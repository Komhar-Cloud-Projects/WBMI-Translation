WITH
SQ_claim_payment_dim AS (
	SELECT
		claim_pay_dim_id,
		crrnt_snpsht_flag,
		audit_id,
		eff_from_date,
		eff_to_date,
		created_date,
		modified_date,
		prim_payee_name
	FROM claim_payment_dim
	WHERE claim_payment_dim.created_date >= '@{pipeline().parameters.SELECTION_START_TS}' or claim_payment_dim.modified_date  > = '@{pipeline().parameters.SELECTION_START_TS}'
),
EXPTRANS AS (
	SELECT
	claim_pay_dim_id,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	created_date,
	modified_date,
	prim_payee_name
	FROM SQ_claim_payment_dim
),
LKP_PRIM_PAYEE_NAME_MP_DIM AS (
	SELECT
	claim_pay_dim_id,
	claim_pay_dim_id_IN,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	created_date,
	modified_date
	FROM (
		SELECT 
			claim_pay_dim_id,
			claim_pay_dim_id_IN,
			crrnt_snpsht_flag,
			audit_id,
			eff_from_date,
			eff_to_date,
			created_date,
			modified_date
		FROM primary_payee_name_mp_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_dim_id ORDER BY claim_pay_dim_id) = 1
),
mplt_namebreak_double_metaphone AS (WITH
	INPUT AS (
		
	),
	DoubleMetaPhone AS (
	),
	EXPTRANS AS (
		SELECT
		input
		FROM INPUT
	),
	NameBreakerSixParts AS (
	),
	EXPTRANS1 AS (
		SELECT
		One_out AS One_in,
		-- *INF*: IIF(ISNULL(One_in),One_in,:SP.DOUBLEMETAPHONE(One_in))
		IFF(One_in IS NULL,
			One_in,
			:SP.DOUBLEMETAPHONE(One_in
			)
		) AS One_var,
		-- *INF*: IIF(isnull(One_var),One_var,substr(One_var,1,5))
		IFF(One_var IS NULL,
			One_var,
			substr(One_var, 1, 5
			)
		) AS One_primary,
		-- *INF*: IIF(isnull(One_var),One_var,substr(One_var,6,5))
		IFF(One_var IS NULL,
			One_var,
			substr(One_var, 6, 5
			)
		) AS One_secondary,
		Two_out AS Two_in,
		-- *INF*: IIF(ISNULL(Two_in),Two_in,:SP.DOUBLEMETAPHONE(Two_in))
		IFF(Two_in IS NULL,
			Two_in,
			:SP.DOUBLEMETAPHONE(Two_in
			)
		) AS Two_var,
		-- *INF*: IIF(isnull(Two_var),Two_var,substr(Two_var,1,5))
		IFF(Two_var IS NULL,
			Two_var,
			substr(Two_var, 1, 5
			)
		) AS Two_primary,
		-- *INF*: IIF(isnull(Two_var),Two_var,substr(Two_var,6,5))
		IFF(Two_var IS NULL,
			Two_var,
			substr(Two_var, 6, 5
			)
		) AS Two_secondary,
		Three_out AS Three_in,
		-- *INF*: IIF(ISNULL(Three_in),Three_in,:SP.DOUBLEMETAPHONE(Three_in))
		IFF(Three_in IS NULL,
			Three_in,
			:SP.DOUBLEMETAPHONE(Three_in
			)
		) AS Three_var,
		-- *INF*: IIF(isnull(Three_var),Three_var,substr(Three_var,1,5))
		IFF(Three_var IS NULL,
			Three_var,
			substr(Three_var, 1, 5
			)
		) AS Three_primary,
		-- *INF*: IIF(isnull(Three_var),Three_var,substr(Three_var,6,5))
		IFF(Three_var IS NULL,
			Three_var,
			substr(Three_var, 6, 5
			)
		) AS Three_secondary,
		Four_out AS Four_in,
		-- *INF*: IIF(ISNULL(Four_in),Four_in,:SP.DOUBLEMETAPHONE(Four_in))
		IFF(Four_in IS NULL,
			Four_in,
			:SP.DOUBLEMETAPHONE(Four_in
			)
		) AS Four_var,
		-- *INF*: IIF(isnull(Four_var),Four_var,substr(Four_var,1,5))
		IFF(Four_var IS NULL,
			Four_var,
			substr(Four_var, 1, 5
			)
		) AS Four_primary,
		-- *INF*: IIF(isnull(Four_var),Four_var,substr(Four_var,6,5))
		IFF(Four_var IS NULL,
			Four_var,
			substr(Four_var, 6, 5
			)
		) AS Four_secondary,
		Five_out AS Five_in,
		-- *INF*: IIF(ISNULL(Five_in),Five_in,:SP.DOUBLEMETAPHONE(Five_in))
		IFF(Five_in IS NULL,
			Five_in,
			:SP.DOUBLEMETAPHONE(Five_in
			)
		) AS Five_var,
		-- *INF*: IIF(isnull(Five_var),Five_var,substr(Five_var,1,5))
		IFF(Five_var IS NULL,
			Five_var,
			substr(Five_var, 1, 5
			)
		) AS Five_primary,
		-- *INF*: IIF(isnull(Five_var),Five_var,substr(Five_var,6,5))
		IFF(Five_var IS NULL,
			Five_var,
			substr(Five_var, 6, 5
			)
		) AS Five_secondary,
		Six_out AS Six_in,
		-- *INF*: IIF(ISNULL(Six_in),Six_in,:SP.DOUBLEMETAPHONE(Six_in))
		IFF(Six_in IS NULL,
			Six_in,
			:SP.DOUBLEMETAPHONE(Six_in
			)
		) AS Six_var,
		-- *INF*: IIF(isnull(Six_var),Six_var,substr(Six_var,1,5))
		IFF(Six_var IS NULL,
			Six_var,
			substr(Six_var, 1, 5
			)
		) AS Six_primary,
		-- *INF*: IIF(isnull(Six_var),Six_var,substr(Six_var,6,5))
		IFF(Six_var IS NULL,
			Six_var,
			substr(Six_var, 6, 5
			)
		) AS Six_secondary
		FROM NameBreakerSixParts
	),
	OUTPUT AS (
		SELECT
		One_in, 
		One_primary, 
		One_secondary, 
		Two_in, 
		Two_primary, 
		Two_secondary, 
		Three_in, 
		Three_primary, 
		Three_secondary, 
		Four_in, 
		Four_primary, 
		Four_secondary, 
		Five_in, 
		Five_primary, 
		Five_secondary, 
		Six_in, 
		Six_primary, 
		Six_secondary
		FROM EXPTRANS1
	),
),
EXP_consolidate AS (
	SELECT
	LKP_PRIM_PAYEE_NAME_MP_DIM.claim_pay_dim_id AS claim_pay_dim_id_lkp,
	LKP_PRIM_PAYEE_NAME_MP_DIM.claim_pay_dim_id_IN,
	LKP_PRIM_PAYEE_NAME_MP_DIM.crrnt_snpsht_flag,
	LKP_PRIM_PAYEE_NAME_MP_DIM.audit_id,
	LKP_PRIM_PAYEE_NAME_MP_DIM.eff_from_date,
	LKP_PRIM_PAYEE_NAME_MP_DIM.eff_to_date,
	LKP_PRIM_PAYEE_NAME_MP_DIM.created_date,
	LKP_PRIM_PAYEE_NAME_MP_DIM.modified_date,
	mplt_namebreak_double_metaphone.One_in,
	mplt_namebreak_double_metaphone.One_primary,
	mplt_namebreak_double_metaphone.One_secondary,
	mplt_namebreak_double_metaphone.Two_in,
	mplt_namebreak_double_metaphone.Two_primary,
	mplt_namebreak_double_metaphone.Two_secondary,
	mplt_namebreak_double_metaphone.Three_in,
	mplt_namebreak_double_metaphone.Three_primary,
	mplt_namebreak_double_metaphone.Three_secondary,
	mplt_namebreak_double_metaphone.Four_in,
	mplt_namebreak_double_metaphone.Four_primary,
	mplt_namebreak_double_metaphone.Four_secondary,
	mplt_namebreak_double_metaphone.Five_in,
	mplt_namebreak_double_metaphone.Five_primary,
	mplt_namebreak_double_metaphone.Five_secondary,
	mplt_namebreak_double_metaphone.Six_in,
	mplt_namebreak_double_metaphone.Six_primary,
	mplt_namebreak_double_metaphone.Six_secondary
	FROM mplt_namebreak_double_metaphone
	LEFT JOIN LKP_PRIM_PAYEE_NAME_MP_DIM
	ON LKP_PRIM_PAYEE_NAME_MP_DIM.claim_pay_dim_id = EXPTRANS.claim_pay_dim_id
),
RTRTRANS AS (
	SELECT
	claim_pay_dim_id_lkp AS lkp_claim_pay_dim_id,
	claim_pay_dim_id_IN,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	created_date,
	modified_date,
	One_in,
	One_primary,
	One_secondary,
	Two_in,
	Two_primary,
	Two_secondary,
	Three_in,
	Three_primary,
	Three_secondary,
	Four_in,
	Four_primary,
	Four_secondary,
	Five_in,
	Five_primary,
	Five_secondary,
	Six_in,
	Six_primary,
	Six_secondary
	FROM EXP_consolidate
),
RTRTRANS_UPDATE AS (SELECT * FROM RTRTRANS WHERE NOT ISNULL(lkp_claim_pay_dim_id)),
RTRTRANS_INSERT AS (SELECT * FROM RTRTRANS WHERE ISNULL(lkp_claim_pay_dim_id)),
UPD_INSERT AS (
	SELECT
	claim_pay_dim_id_IN AS claim_pay_dim_id3, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag3, 
	audit_id AS audit_id3, 
	eff_from_date AS eff_from_date3, 
	eff_to_date AS eff_to_date3, 
	created_date AS created_date3, 
	modified_date AS modified_date3, 
	One_in AS One_in3, 
	One_primary AS One_primary3, 
	One_secondary AS One_secondary3, 
	Two_in AS Two_in3, 
	Two_primary AS Two_primary3, 
	Two_secondary AS Two_secondary3, 
	Three_in AS Three_in3, 
	Three_primary AS Three_primary3, 
	Three_secondary AS Three_secondary3, 
	Four_in AS Four_in3, 
	Four_primary AS Four_primary3, 
	Four_secondary AS Four_secondary3, 
	Five_in AS Five_in3, 
	Five_primary AS Five_primary3, 
	Five_secondary AS Five_secondary3, 
	Six_in AS Six_in3, 
	Six_primary AS Six_primary3, 
	Six_secondary AS Six_secondary3
	FROM RTRTRANS_INSERT
),
primary_payee_name_mp_dim_INSERT AS (
	INSERT INTO primary_payee_name_mp_dim
	(claim_pay_dim_id, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, one, two, three, four, five, six, mpOnePrim, mpOneSec, mpTwoPrim, mpTwoSec, mpThreePrim, mpThreeSec, mpFourPrim, mpFourSec, mpFivePrim, mpFiveSec, mpSixPrim, mpSixSec)
	SELECT 
	claim_pay_dim_id3 AS CLAIM_PAY_DIM_ID, 
	crrnt_snpsht_flag3 AS CRRNT_SNPSHT_FLAG, 
	audit_id3 AS AUDIT_ID, 
	eff_from_date3 AS EFF_FROM_DATE, 
	eff_to_date3 AS EFF_TO_DATE, 
	created_date3 AS CREATED_DATE, 
	modified_date3 AS MODIFIED_DATE, 
	One_in3 AS ONE, 
	Two_in3 AS TWO, 
	Three_in3 AS THREE, 
	Four_in3 AS FOUR, 
	Five_in3 AS FIVE, 
	Six_in3 AS SIX, 
	One_primary3 AS MPONEPRIM, 
	One_secondary3 AS MPONESEC, 
	Two_primary3 AS MPTWOPRIM, 
	Two_secondary3 AS MPTWOSEC, 
	Three_primary3 AS MPTHREEPRIM, 
	Three_secondary3 AS MPTHREESEC, 
	Four_primary3 AS MPFOURPRIM, 
	Four_secondary3 AS MPFOURSEC, 
	Five_primary3 AS MPFIVEPRIM, 
	Five_secondary3 AS MPFIVESEC, 
	Six_primary3 AS MPSIXPRIM, 
	Six_secondary3 AS MPSIXSEC
	FROM UPD_INSERT
),
UPD_UPDATE AS (
	SELECT
	lkp_claim_pay_dim_id AS lkp_claim_pay_dim_id1, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag1, 
	audit_id AS audit_id1, 
	eff_from_date AS eff_from_date1, 
	eff_to_date AS eff_to_date1, 
	created_date AS created_date1, 
	modified_date AS modified_date1, 
	One_in AS One_in1, 
	One_primary AS One_primary1, 
	One_secondary AS One_secondary1, 
	Two_in AS Two_in1, 
	Two_primary AS Two_primary1, 
	Two_secondary AS Two_secondary1, 
	Three_in AS Three_in1, 
	Three_primary AS Three_primary1, 
	Three_secondary AS Three_secondary1, 
	Four_in AS Four_in1, 
	Four_primary AS Four_primary1, 
	Four_secondary AS Four_secondary1, 
	Five_in AS Five_in1, 
	Five_primary AS Five_primary1, 
	Five_secondary AS Five_secondary1, 
	Six_in AS Six_in1, 
	Six_primary AS Six_primary1, 
	Six_secondary AS Six_secondary1
	FROM RTRTRANS_UPDATE
),
primary_payee_name_mp_dim_UPDATE AS (
	MERGE INTO primary_payee_name_mp_dim AS T
	USING UPD_UPDATE AS S
	ON T.claim_pay_dim_id = S.lkp_claim_pay_dim_id1
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag1, T.audit_id = S.audit_id1, T.eff_from_date = S.eff_from_date1, T.eff_to_date = S.eff_to_date1, T.created_date = S.created_date1, T.modified_date = S.modified_date1, T.one = S.One_in1, T.two = S.Two_in1, T.three = S.Three_in1, T.four = S.Four_in1, T.five = S.Five_in1, T.six = S.Six_in1, T.mpOnePrim = S.One_primary1, T.mpOneSec = S.One_secondary1, T.mpTwoPrim = S.Two_primary1, T.mpTwoSec = S.Two_secondary1, T.mpThreePrim = S.Three_primary1, T.mpThreeSec = S.Three_secondary1, T.mpFourPrim = S.Four_primary1, T.mpFourSec = S.Four_secondary1, T.mpFivePrim = S.Five_primary1, T.mpFiveSec = S.Five_secondary1, T.mpSixPrim = S.Six_primary1, T.mpSixSec = S.Six_secondary1
),