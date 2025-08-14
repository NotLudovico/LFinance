# Database Structure

The database.db file is an sqlite3 file that gets created to store the result of running the scraping scripts ishares.py, amundi.py or spdr.py, is structured in the following manner: 
- "etfs" table where general information about the etf is begin stores (name, issuer, ter,...)
- "etf_holdings" table where there's the many to many relation between etf and holdings
