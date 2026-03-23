# Databricks notebook source
# MAGIC %md
# MAGIC # Seed dim_locations — Reference Data
# MAGIC Run once to populate the location dimension with continent/country hierarchy.
# MAGIC Update as new locations are added.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

C = PipelineConfig

# COMMAND ----------

# Seed data — update this with your actual Genesys site locations
locations = [
    # APAC
    {"location_key": "Chennai",        "site_name": "Chennai DC",         "city": "Chennai",       "country": "India",          "country_code": "IN", "continent": "Asia",          "region": "APAC"},
    {"location_key": "Bangalore",      "site_name": "Bangalore Office",   "city": "Bangalore",     "country": "India",          "country_code": "IN", "continent": "Asia",          "region": "APAC"},
    {"location_key": "Hyderabad",      "site_name": "Hyderabad Office",   "city": "Hyderabad",     "country": "India",          "country_code": "IN", "continent": "Asia",          "region": "APAC"},
    {"location_key": "Mumbai",         "site_name": "Mumbai Office",      "city": "Mumbai",        "country": "India",          "country_code": "IN", "continent": "Asia",          "region": "APAC"},
    {"location_key": "Singapore",      "site_name": "Singapore Office",   "city": "Singapore",     "country": "Singapore",      "country_code": "SG", "continent": "Asia",          "region": "APAC"},
    {"location_key": "Sydney",         "site_name": "Sydney Office",      "city": "Sydney",        "country": "Australia",      "country_code": "AU", "continent": "Oceania",       "region": "APAC"},
    {"location_key": "Tokyo",          "site_name": "Tokyo Office",       "city": "Tokyo",         "country": "Japan",          "country_code": "JP", "continent": "Asia",          "region": "APAC"},

    # EMEA
    {"location_key": "London",         "site_name": "London Office",      "city": "London",        "country": "United Kingdom", "country_code": "GB", "continent": "Europe",        "region": "EMEA"},
    {"location_key": "Dublin",         "site_name": "Dublin Office",      "city": "Dublin",        "country": "Ireland",        "country_code": "IE", "continent": "Europe",        "region": "EMEA"},
    {"location_key": "Frankfurt",      "site_name": "Frankfurt DC",       "city": "Frankfurt",     "country": "Germany",        "country_code": "DE", "continent": "Europe",        "region": "EMEA"},
    {"location_key": "Paris",          "site_name": "Paris Office",       "city": "Paris",         "country": "France",         "country_code": "FR", "continent": "Europe",        "region": "EMEA"},
    {"location_key": "Dubai",          "site_name": "Dubai Office",       "city": "Dubai",         "country": "UAE",            "country_code": "AE", "continent": "Asia",          "region": "EMEA"},
    {"location_key": "Johannesburg",   "site_name": "Johannesburg Office","city": "Johannesburg",  "country": "South Africa",   "country_code": "ZA", "continent": "Africa",        "region": "EMEA"},

    # Americas
    {"location_key": "New York",       "site_name": "New York Office",    "city": "New York",      "country": "United States",  "country_code": "US", "continent": "North America", "region": "Americas"},
    {"location_key": "Dallas",         "site_name": "Dallas DC",          "city": "Dallas",        "country": "United States",  "country_code": "US", "continent": "North America", "region": "Americas"},
    {"location_key": "San Francisco",  "site_name": "San Francisco Office","city": "San Francisco","country": "United States",  "country_code": "US", "continent": "North America", "region": "Americas"},
    {"location_key": "Toronto",        "site_name": "Toronto Office",     "city": "Toronto",       "country": "Canada",         "country_code": "CA", "continent": "North America", "region": "Americas"},
    {"location_key": "Sao Paulo",      "site_name": "Sao Paulo Office",   "city": "Sao Paulo",     "country": "Brazil",         "country_code": "BR", "continent": "South America", "region": "Americas"},
]

# COMMAND ----------

df = spark.createDataFrame(locations)

(df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(C.SILVER_DIM_LOCATIONS)
)

print(f"Seeded {len(locations)} locations into {C.SILVER_DIM_LOCATIONS}")

# COMMAND ----------

display(spark.table(C.SILVER_DIM_LOCATIONS).orderBy("continent", "country", "city"))
