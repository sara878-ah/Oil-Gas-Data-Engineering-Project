#!/usr/bin/env python
# coding: utf-8

# ## Notebook 1
# 
# New notebook

# In[1]:


import os
import pandas as pd
import sqlite3
from pyspark.sql import functions as F

bronze_path = "/lakehouse/default/Files/bronze"

print(os.listdir(bronze_path))

db_path = "/lakehouse/default/Files/bronze/brazil_oil_production.sqlite"

conn = sqlite3.connect(db_path)

cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

tables = cursor.fetchall()

print(tables)

df = pd.read_sql_query(
    "SELECT * FROM production LIMIT 5",
    conn
)

display(df)

df = pd.read_sql_query(
    "SELECT * FROM production",
    conn
)

print("Rows:", len(df))
print("Columns:", len(df.columns))

print("\nColumn names:")
print(df.columns.tolist())

print("\nMissing values:")
print(df.isnull().sum())

print("\nDuplicate rows:", df.duplicated().sum())

print(df.dtypes)

print(
    df[['Year', 'Month']]
    .drop_duplicates()
    .sort_values(['Year', 'Month'])
    .head(20)
)

print("\nYears:")
print(sorted(df['Year'].unique()))

print("\nMonths:")
print(sorted(df['Month'].unique()))

problem_columns = [
    'Oil (m³)',
    'Condensate oil (m³)',
    'Others fluids injection (m³)'
]

for col in problem_columns:

    numeric = pd.to_numeric(
        df[col],
        errors='coerce'
    )

    invalid = df[
        df[col].notna() & numeric.isna()
    ][col].unique()

    print(f"\n{col}")
    print("Invalid values:", invalid[:20])
    print("Count:", len(invalid))

bad_others = (
    pd.to_numeric(
        df['Others fluids injection (m³)'],
        errors='coerce'
    ).isna()
    & df['Others fluids injection (m³)'].notna()
)

print("Affected rows:", bad_others.sum())

print("\nYears affected:")

print(
    df.loc[bad_others, 'Year']
    .value_counts()
    .sort_index()
)

print("\nMonths affected:")

print(
    df.loc[
        bad_others,
        ['Year', 'Month']
    ]
    .value_counts()
    .sort_index()
)

numeric_columns = [
    'Associated petroleum gas (Mm³)',
    'Non-associated petroleum gas (Mm³)',
    'Water (m³)',
    'Gas injection (Mm³)',
    'Secondary recovery water injection (m³)',
    'Wastewater injection (m³)',
    'CO2 injection (Mm³)',
    'Nitrogen injection (Mm³)',
    'Steam injection (t)',
    'Polymer injection (m³)'
]

for col in numeric_columns:

    print(f"\n{col}")
    print("Min:", df[col].min())
    print("Max:", df[col].max())
    print(
        "Negative values:",
        (df[col] < 0).sum()
    )

negative_water = df[
    df['Water (m³)'] < 0
]

display(
    negative_water[
        [
            'Year',
            'Month',
            'State',
            'Basin',
            'Field',
            'Well',
            'Water (m³)'
        ]
    ]
)

silver_df = df.copy()

print(
    "Row before cleaning:",
    len(silver_df)
)

numeric_fix_columns = [
    'Oil (m³)',
    'Condensate oil (m³)',
    'Others fluids injection (m³)'
]

for col in numeric_fix_columns:

    silver_df[col] = pd.to_numeric(
        silver_df[col],
        errors='coerce'
    )

print(
    silver_df[
        numeric_fix_columns
    ].dtypes
)

silver_df.loc[
    silver_df['Water (m³)'] < 0,
    'Water (m³)'
] = pd.NA

print(
    "Negative water values after cleaning:",
    (silver_df['Water (m³)'] < 0).sum()
)

print(
    "Missing water values after cleaning:",
    silver_df['Water (m³)'].isna().sum()
)

missing_summary = pd.DataFrame({
    'Missing_Count':
        silver_df.isna().sum(),

    'Missing_Percent':
        (
            silver_df.isna().mean()
            * 100
        ).round(2)
})

missing_summary = missing_summary[
    missing_summary['Missing_Count'] > 0
].sort_values(
    'Missing_Percent',
    ascending=False
)

display(missing_summary)

missing_summary_display = missing_summary.reset_index()

missing_summary_display.columns = [
    'Column',
    'Missing_Count',
    'Missing_Percent'
]

display(missing_summary_display)

silver_df['Installation'] = (
    silver_df['Installation']
    .fillna('Not Reported')
)

print(
    "Missing Installation:",
    silver_df['Installation']
    .isna()
    .sum()
)

silver_df['Date'] = pd.to_datetime(
    dict(
        year=silver_df['Year'],
        month=silver_df['Month'],
        day=1
    )
)

print(
    silver_df[
        ['Year', 'Month', 'Date']
    ].head()
)

silver_df = silver_df.rename(
    columns={
        'Year': 'year',
        'Month': 'month',
        'State': 'state',
        'Basin': 'basin',
        'Field': 'field',
        'Well': 'well',
        'Environment': 'environment',
        'Installation': 'installation',
        'Oil (m³)': 'oil_m3',
        'Condensate oil (m³)': 'condensate_oil_m3',
        'Associated petroleum gas (Mm³)': 'associated_gas_mm3',
        'Non-associated petroleum gas (Mm³)': 'non_associated_gas_mm3',
        'Water (m³)': 'water_m3',
        'Gas injection (Mm³)': 'gas_injection_mm3',
        'Secondary recovery water injection (m³)': 'secondary_water_injection_m3',
        'Wastewater injection (m³)': 'wastewater_injection_m3',
        'CO2 injection (Mm³)': 'co2_injection_mm3',
        'Nitrogen injection (Mm³)': 'nitrogen_injection_mm3',
        'Steam injection (t)': 'steam_injection_t',
        'Polymer injection (m³)': 'polymer_injection_m3',
        'Others fluids injection (m³)': 'other_fluids_injection_m3',
        'Date': 'date'
    }
)

print(silver_df.columns.tolist())

print("Rows:", len(silver_df))
print("Columns:", len(silver_df.columns))
print("Duplicates:", silver_df.duplicated().sum())

print("\nData Types:")
print(silver_df.dtypes)

print("\nNegative Water Values:")
print((silver_df['water_m3'] < 0).sum())

print("\nMissing Installation:")
print(silver_df['installation'].isna().sum())

display(silver_df.head())

spark_silver_df = spark.createDataFrame(
    silver_df
)

print(
    "Rows:",
    spark_silver_df.count()
)

spark_silver_df.printSchema()

(
    spark_silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_production")
)

check_df = spark.table(
    "silver_production"
)

print(
    "Saved rows:",
    check_df.count()
)

display(
    check_df.limit(5)
)

dim_date = (
    silver_df[
        ['date', 'year', 'month']
    ]
    .drop_duplicates()
    .copy()
)

dim_date = (
    dim_date
    .sort_values('date')
    .reset_index(drop=True)
)

dim_date['date_key'] = (
    dim_date['year'] * 100
    + dim_date['month']
)

dim_date = dim_date[
    [
        'date_key',
        'date',
        'year',
        'month'
    ]
]

display(
    dim_date.head(15)
)

print(
    "Rows:",
    len(dim_date)
)

well_check = (
    silver_df
    .groupby('well')
    .agg(
        environment_count=(
            'environment',
            'nunique'
        ),
        installation_count=(
            'installation',
            'nunique'
        )
    )
)

print(
    "Unique wells:",
    silver_df['well'].nunique()
)

print(
    "Wells with multiple environments:",
    (
        well_check['environment_count'] > 1
    ).sum()
)

print(
    "Wells with multiple installations:",
    (
        well_check['installation_count'] > 1
    ).sum()
)

dim_well = (
    silver_df[
        ['well', 'environment']
    ]
    .drop_duplicates()
    .copy()
)

dim_well = (
    dim_well
    .sort_values('well')
    .reset_index(drop=True)
)

dim_well['well_key'] = (
    dim_well.index + 1
)

dim_well = dim_well[
    [
        'well_key',
        'well',
        'environment'
    ]
]

display(dim_well.head())

print(
    "Rows:",
    len(dim_well)
)

field_check = (
    silver_df
    .groupby('field')
    .agg(
        basin_count=(
            'basin',
            'nunique'
        ),
        state_count=(
            'state',
            'nunique'
        )
    )
)

print(
    "Unique fields:",
    silver_df['field'].nunique()
)

print(
    "Fields with multiple basins:",
    (
        field_check['basin_count'] > 1
    ).sum()
)

print(
    "Fields with multiple states:",
    (
        field_check['state_count'] > 1
    ).sum()
)

problem_fields = field_check[
    (field_check['basin_count'] > 1)
    |
    (field_check['state_count'] > 1)
]

display(problem_fields)

problem_field_names = (
    problem_fields.index.tolist()
)

display(
    silver_df[
        silver_df['field']
        .isin(problem_field_names)
    ][
        ['field', 'basin', 'state']
    ]
    .drop_duplicates()
    .sort_values(
        ['field', 'basin', 'state']
    )
)

dim_field = (
    silver_df[
        ['field', 'basin', 'state']
    ]
    .drop_duplicates()
    .copy()
)

dim_field = (
    dim_field
    .sort_values(
        ['state', 'basin', 'field']
    )
    .reset_index(drop=True)
)

dim_field['field_key'] = (
    dim_field.index + 1
)

dim_field = dim_field[
    [
        'field_key',
        'field',
        'basin',
        'state'
    ]
]

display(dim_field.head())

print(
    "Rows:",
    len(dim_field)
)

fact_production = silver_df.merge(
    dim_date[
        ['date', 'date_key']
    ],
    on='date',
    how='left'
)

print(
    fact_production[
        ['date', 'date_key']
    ].head()
)

print(
    "Missing date_key:",
    fact_production[
        'date_key'
    ].isna().sum()
)

print(
    "Rows:",
    len(fact_production)
)

fact_production = fact_production.merge(
    dim_well[
        [
            'well',
            'environment',
            'well_key'
        ]
    ],
    on=[
        'well',
        'environment'
    ],
    how='left'
)

print(
    fact_production[
        [
            'well',
            'environment',
            'well_key'
        ]
    ].head()
)

print(
    "Missing well_key:",
    fact_production[
        'well_key'
    ].isna().sum()
)

print(
    "Rows:",
    len(fact_production)
)

fact_production = fact_production.merge(
    dim_field[
        [
            'field',
            'basin',
            'state',
            'field_key'
        ]
    ],
    on=[
        'field',
        'basin',
        'state'
    ],
    how='left'
)

print(
    fact_production[
        [
            'field',
            'basin',
            'state',
            'field_key'
        ]
    ].head()
)

print(
    "Missing field_key:",
    fact_production[
        'field_key'
    ].isna().sum()
)

print(
    "Rows:",
    len(fact_production)
)

print(
    fact_production[
        ['field_key']
    ].head()
)

print(
    "Missing field_key:",
    fact_production[
        'field_key'
    ].isna().sum()
)

print(
    "Rows:",
    len(fact_production)
)

print(
    fact_production.columns.tolist()
)

fact_production = fact_production[
    [
        'date_key',
        'well_key',
        'field_key',
        'installation',
        'oil_m3',
        'condensate_oil_m3',
        'associated_gas_mm3',
        'non_associated_gas_mm3',
        'water_m3',
        'gas_injection_mm3',
        'secondary_water_injection_m3',
        'wastewater_injection_m3',
        'co2_injection_mm3',
        'steam_injection_t',
        'polymer_injection_m3'
    ]
].copy()

print(
    "Rows:",
    len(fact_production)
)

print(
    "Columns:",
    len(fact_production.columns)
)

display(
    fact_production.head()
)

print(
    "Missing date_key:",
    fact_production[
        'date_key'
    ].isna().sum()
)

print(
    "Missing well_key:",
    fact_production[
        'well_key'
    ].isna().sum()
)

print(
    "Missing field_key:",
    fact_production[
        'field_key'
    ].isna().sum()
)

print(
    "Fact rows:",
    len(fact_production)
)

print(
    "Dim date rows:",
    len(dim_date)
)

print(
    "Dim well rows:",
    len(dim_well)
)

print(
    "Dim field rows:",
    len(dim_field)
)

spark_dim_date = spark.createDataFrame(
    dim_date
)

spark_dim_well = spark.createDataFrame(
    dim_well
)

spark_dim_field = spark.createDataFrame(
    dim_field
)

spark_fact_production = spark.createDataFrame(
    fact_production
)

(
    spark_dim_date.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_dim_date")
)

(
    spark_dim_well.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_dim_well")
)

(
    spark_dim_field.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_dim_field")
)

(
    spark_fact_production.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_fact_production")
)

print(
    "gold_dim_date:",
    spark.table(
        "gold_dim_date"
    ).count()
)

print(
    "gold_dim_well:",
    spark.table(
        "gold_dim_well"
    ).count()
)

print(
    "gold_dim_field:",
    spark.table(
        "gold_dim_field"
    ).count()
)

print(
    "gold_fact_production:",
    spark.table(
        "gold_fact_production"
    ).count()
)

check_2020 = (
    spark.table("gold_fact_production")
    .join(
        spark.table("gold_dim_date"),
        on="date_key",
        how="left"
    )
    .filter(
        F.col("year").isin(
            2019,
            2020
        )
    )
    .groupBy(
        "year",
        "month"
    )
    .agg(
        F.count("*").alias("rows"),
        F.sum("oil_m3").alias("total_oil_m3"),
        F.sum("water_m3").alias("total_water_m3"),
        F.sum("gas_injection_mm3").alias("gas_injection_mm3"),
        F.sum("secondary_water_injection_m3").alias("water_injection_m3")
    )
    .orderBy(
        "year",
        "month"
    )
)

display(check_2020)

(
    spark.table("gold_fact_production")
    .join(
        spark.table("gold_dim_date"),
        "date_key"
    )
    .filter(
        F.col("year") == 2020
    )
    .groupBy("month")
    .agg(
        F.count("*").alias("rows"),
        F.sum("oil_m3").alias("oil_m3")
    )
    .orderBy("month")
    .show()
)

check_oil = (
    spark.table("gold_fact_production")
    .join(
        spark.table("gold_dim_date"),
        "date_key"
    )
    .filter(
        F.col("year") == 2020
    )
    .groupBy("month")
    .agg(
        F.count("*").alias("total_rows"),
        F.count("oil_m3").alias("oil_reported_rows"),
        F.sum("oil_m3").alias("total_oil_m3"),
        F.avg("oil_m3").alias("avg_oil_m3"),
        F.max("oil_m3").alias("max_oil_m3")
    )
    .orderBy("month")
)

display(check_oil)

check_2020_metrics = (
    spark.table("gold_fact_production")
    .join(
        spark.table("gold_dim_date"),
        "date_key"
    )
    .filter(
        F.col("year") == 2020
    )
    .groupBy("month")
    .agg(
        F.sum("oil_m3").alias("oil_m3"),
        F.sum("water_m3").alias("water_m3"),
        F.sum("associated_gas_mm3").alias("associated_gas_mm3"),
        F.sum("gas_injection_mm3").alias("gas_injection_mm3"),
        F.sum("secondary_water_injection_m3").alias("water_injection_m3")
    )
    .orderBy("month")
)

display(check_2020_metrics)

