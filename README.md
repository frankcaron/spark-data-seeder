# spark-data-seeder
A simple script which can seed basic data into your Spark cluster's datastores. Includes a basic example of some sales data.

# Usage
Run this python script in your Spark shell (e.g., Databricks).

# Output
Generates tabular data in the following format:

  ```
  Row(date=datetime.datetime(2014, 5, 25, 0, 35, 14), channel=u'billboard', device=u'tablet', amount=66.49, lat=86.02231237806937, lon=35.94446992860989)
  ```

| date | channel | device | amount | lat | lon |
| ----- | ----- | ----- | ----- | ----- | ----- |
| 2014-05-23 00:30:14 | billboard | tablet | 66.49 | 86.02231237806937 | 35.94446992860989| 

# Thanks
Thanks to [@Ghnuberath](https://github.com/Ghnuberath) for his help on this one.
