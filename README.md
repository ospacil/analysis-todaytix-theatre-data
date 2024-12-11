## Analysis of theatre data from TodayTix Group APIs

The aim of this project is to explore data from [TodayTix Group APIs](https://developers.todaytixgroup.com/) on theatre shows, with bias towards the ones I might be interested in :-) I am using this project to learn some new technologies, and play around with Google Cloud Console in particular. So there is probably going to be a mixture of Python and R scripts, some dbt/sql and who knows what.

This is purely a personal project, with no commercial purpose in mind.

### The workflow
- [DONE] A Python script is deployed as a Google Cloud Run Function and configured via Cloud Scheduler to execute daily.
- [DONE] The Python code queries the TodayTix API for some information, parses the JSON response into a Polars DataFrame, and save the dataframe in parquet format to Google Cloud Storage bucket.
- [DONE] The bucket is imported to BigQuery as an external table.
- [DONE] Use `dbt` to process the above external table into a native BigQuery one, and augment with some useful computed columns. See [https://github.com/ospacil/dbt-todaytix-theatre-data](https://github.com/ospacil/dbt-todaytix-theatre-data).
- [IN_PROGRESS] Analyze the data using Python/R by directly querying BigQuery. The analysis document can be viewed at [https://ospacil.quarto.pub/oedipus-show-availability-analysis/](https://ospacil.quarto.pub/oedipus-show-availability-analysis/) - it is updated by a GitHub Action on every push to the `main` branch.
