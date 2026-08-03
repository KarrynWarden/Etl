SELECT distinct typecont, cont, code nbank, "name" detail, namebank, "account", bic, corracc, nameext, ls, kbk, innbank, kppbank
FROM public.spacc
where typecont<>1