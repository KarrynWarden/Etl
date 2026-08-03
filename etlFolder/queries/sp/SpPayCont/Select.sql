select  c.typecont,c.cont,coalesce(c.nameext,t.longname,t.name) name,c.inn,c.kpp,c.okato 
from sptf t
inner join spacc c on t.code  = c.cont and c.typecont  = 6
where 1=1
and t.dbegin  = (select max(dbegin) from sptf where code = t.code)
and c.code = (select max(code) from spacc where typecont = c.typecont and cont = c.cont)
---and c.CONT IN (104,1168,192,1140)
union all
select  c.typecont,c.cont,coalesce(c.nameext,t.name) name,c.inn,c.kpp,c.okato 
from spsmo t
inner join spacc c on t.code  = c.cont and c.typecont  = 2
where 1=1
and t.dbegin  = (select max(dbegin) from spsmo where code = t.code)
---and c.code = (select max(code) from spacc where typecont = c.typecont and cont = c.cont and lower(name) like '%омс%')
and c.dend = (select max(dend) from spacc where typecont = c.typecont and cont = c.cont and lower(name) like '%омс%')
and lower(c.name) like '%омс%'