SELECT mo, mo modoc, 7::numeric doctype, ACCNO, ACCDT, 0::numeric STEPEXP, NULL::numeric schetnrec, 2::numeric TYPECONT, c.smo CONT, sumprep sumcheck, 0::numeric SUMKSS, 0::numeric SUMSZP, 0::numeric SUMAPP, 0::numeric SUMSMP, 0::numeric COUNTCHECK, c.YEAR, c.MONTH, c.idrw, c.LASTUPDATE, c.createdate, null::date SVODDT, null::numeric MEDCHECK_IDRW, null::date buhdt, NULL::numeric SCHETTYPE
    FROM REQPREPMO c
    INNER JOIN REQPREPSMO s ON s.IDrw = c.REQPREPSMO_idrw 
    WHERE ACCDT  >= TO_DATE('2023-01-01', 'YYYY-MM-DD') AND s.DIRDT IS NOT NULL