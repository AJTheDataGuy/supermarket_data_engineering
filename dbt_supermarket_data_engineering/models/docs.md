{% docs raw_staging %}
Raw staging table for data from the supermarket (Coles) website.

Contains data for supermarket items:
1. Descriptions
2. Pricing including standardised pricing
3. Weekly specials

This table is not normalised and needs further transformation.

Data in the staging table is meant to be temporary
and constantly overwritten as data is appended to the data mart tables
and new data flows in each week.

For now, raw records from the coles website are not retained
except for what is moved intto the data mart. However, in the future, these
raw records can be stored as JSON documents in MongoDB if desired.

{% enddocs %}