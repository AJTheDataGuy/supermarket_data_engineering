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

{% docs dim_product_descriptions %}
Normalised dimension table containing unique description (text) data for each supermarket item

This table is incremental but only adds a new item if a new description
is found.

This table assumes that item ids for each product are constant and will not
change over time. For example if item id 101 is an orange, item 101 the next
week should not be a grapefruit.

{% enddocs %}

{% docs fct_product_prices %}
Normalised fact table containing pricing data each week for each supermarket item

This table is incremental but only adds a new item if a new last_updated date
is found.

This table assumes that item ids for each product are constant and will not
change over time. For example if item id 101 is an orange, item 101 the next
week should not be a grapefruit.

{% enddocs %}

{% docs fct_product_specials %}
Normalised fact table containing supermarket specials data each week for each item

This table is incremental but only adds a new item if a new last_updated date
is found.

This table assumes that item ids for each product are constant and will not
change over time. For example if item id 101 is an orange, item 101 the next
week should not be a grapefruit.

{% enddocs %}

{% docs mart_pricing_over_time %}
Non-normalised data mart table containing supermarket pricing data each week for each item

This table is designed for ease of use for analysing data in Power BI, Tableau, etc.

This table combines pricing data from the fct_product_prices table with descriptive
information from the dim_product_descriptions table. 

Additional in this table is a price per unit column which divides the price by the unit
i.e. $5 / 200 g = $0.025 / g. This is included in case the unit quanity changes over time.

For example, a product could be $5 in two different weeks but for 200g one week, and 150 g the next. It is important to capture this on a more standard basis.

{% enddocs %}

{% docs mart_specials_over_time %}
Non-normalised data mart table containing supermarket specials data each week

Only contains data for items that actually have specials (where the special description
is not null)

This table is designed for ease of use for analysing data in Power BI, Tableau, etc.

This table combines specials data from the fct_product_specials table with descriptive
information from the dim_product_descriptions table. 

{% enddocs %}