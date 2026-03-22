\# OLD INGESTION SCRIPTS



===

\## Context



These are old scripts that don't need to be used anymore



===

\## Reasonings



* v1\_TDAT\_ingestion.py

  * stopped using pandas to make the script more lightweight; tradeoff: more transformations in staging
* v1\_FHUB\_basic\_financials\_ingestion.py

  * kept crashing my EC2 because of how massive the pull is; stopped using pandas again

