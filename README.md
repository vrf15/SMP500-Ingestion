# S&P500 Ingestion Repo

This repo will show my entire pipelining process. Feel free to inspect the dags, dbt, and other components.

-----
### Future-Plans

LONG TERM

- Considering OLAP integration for platform familiarity
- Learn and deploy automated scrapers
- Creating a dashboard for each SP500 sector and learn ML principles from this project

SHORT TERM

- Finalize data ingestion from FMP, TNGO, TDAT, and FHUB.
- Bury myself in making lots of dbt work
- Analyze my current ingestion dlt_scripts and make alternate changes for specific situations

------
### List of APIs

- Tiingo (Current main SP500 prices api)
- TwelveData (What I use for Mid Cap 400 prices, but it only returns unadjusted prices so altnernatives req.)
- Finnhub (Very generous API pull limits for free-tier, currently exploring)
- Financial Modeling Prep (too restrictive on free tier)
- Alpha Vantage


