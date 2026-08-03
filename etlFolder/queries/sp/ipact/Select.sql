select dact, max(dcreate) dcreate, SMO
, sum(MPOPUL1) mpopul1,sum(WPOPUL1) wpopul1
, sum(MPOPUL2) mpopul2,sum(WPOPUL2) wpopul2
, sum(MPOPUL3) mpopul3,sum(WPOPUL3) wpopul3
, sum(MPOPUL4) mpopul4,sum(WPOPUL4) wpopul4
, max(iduser) iduser
from IPACT 
GROUP BY dact, SMO
