select IDROW, IDEVENT, date_trunc('second',DSTART) dstart, null DCONF, date_trunc('second',DWORK) DWORK, REVENT, IDMESS, IDLOGSTART
, null IDLOGWORK, IDIEVENT, INITEVENT, null FLOW
, CREATEDATE ---новое поле
from public.ievent
