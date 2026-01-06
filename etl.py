import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
import warnings

warnings.filterwarnings("ignore")

# =====================================================
# DATABASE CONFIG
# =====================================================
DB_USER = "root"
DB_PASS = urllib.parse.quote_plus("220519772")
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "adventureworks_dw"

engine_root = create_engine(
    f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/",
    future=True
)

with engine_root.begin() as conn:
    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))

engine = create_engine(
    f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    future=True
)

# =====================================================
# HELPERS
# =====================================================
def safe_date(series):
    series = series.astype(str).str.strip()
    series = series.replace(['nan','NaT','None',''], pd.NA)
    return pd.to_datetime(series, errors='coerce', dayfirst=True)

def create_table(name, schema, pk):
    cols = ", ".join([f"{c} {t}" for c, t in schema.items()])
    pk_sql = ", ".join(pk)
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {name} (
                {cols},
                PRIMARY KEY ({pk_sql})
            )
        """))

def insert_skip_duplicates(df, table, pk_cols):
    if df.empty:
        print(f"{table} skipped (empty)")
        return

    df = df.dropna(subset=pk_cols).drop_duplicates(subset=pk_cols).copy()

    existing = pd.read_sql(
        f"SELECT {','.join(pk_cols)} FROM {table}", engine
    )

    for c in pk_cols:
        df[c] = df[c].astype(str)
        existing[c] = existing[c].astype(str)

    if not existing.empty:
        df = df.merge(existing, on=pk_cols, how="left", indicator=True)
        df = df[df["_merge"] == "left_only"].drop(columns="_merge")

    if not df.empty:
        df.to_sql(table, engine, if_exists="append", index=False, method="multi")
        print(f"Loaded {table}: {len(df)} rows")

# =====================================================
# TABLES
# =====================================================
create_table("dim_customer", {
    "customer_key":"INT",
    "prefix":"VARCHAR(10)",
    "first_name":"VARCHAR(50)",
    "last_name":"VARCHAR(50)",
    "birth_date":"DATE",
    "marital_status":"CHAR(1)",
    "gender":"CHAR(1)",
    "email":"VARCHAR(100)",
    "annual_income":"INT",
    "total_children":"INT",
    "education_level":"VARCHAR(50)",
    "occupation":"VARCHAR(50)",
    "home_owner":"CHAR(1)"
}, ["customer_key"])

create_table("dim_product_category", {
    "category_key":"INT",
    "category_name":"VARCHAR(50)"
}, ["category_key"])

create_table("dim_product_subcategory", {
    "subcategory_key":"INT",
    "subcategory_name":"VARCHAR(50)",
    "category_key":"INT"
}, ["subcategory_key"])

create_table("dim_product", {
    "product_key":"INT",
    "product_sku":"VARCHAR(30)",
    "product_name":"VARCHAR(100)",
    "model_name":"VARCHAR(50)",
    "product_description":"VARCHAR(255)",
    "color":"VARCHAR(20)",
    "size":"VARCHAR(10)",
    "style":"VARCHAR(10)",
    "cost":"DECIMAL(10,2)",
    "price":"DECIMAL(10,2)",
    "subcategory_key":"INT"
}, ["product_key"])

create_table("dim_sales_territory", {
    "territory_key":"INT",
    "region":"VARCHAR(50)",
    "country":"VARCHAR(50)",
    "continent":"VARCHAR(50)"
}, ["territory_key"])

create_table("dim_date", {
    "date_key":"DATE",
    "year":"INT",
    "month":"INT",
    "quarter":"INT"
}, ["date_key"])

create_table("fact_sales", {
    "order_number":"VARCHAR(20)",
    "order_line":"INT",
    "order_date":"DATE",
    "product_key":"INT",
    "customer_key":"INT",
    "territory_key":"INT",
    "quantity":"INT"
}, ["order_number","order_line"])

create_table("fact_returns", {
    "return_date":"DATE",
    "territory_key":"INT",
    "product_key":"INT",
    "return_quantity":"INT"
}, ["return_date","territory_key","product_key"])

# =====================================================
# EXTRACT
# =====================================================
customers     = pd.read_csv("data/customers.csv", encoding="latin1")
products      = pd.read_csv("data/products.csv", encoding="latin1")
categories    = pd.read_csv("data/categories.csv", encoding="latin1")
subcategories = pd.read_csv("data/subcategories.csv", encoding="latin1")
sales         = pd.read_csv("data/sales.csv", encoding="latin1")
returns       = pd.read_csv("data/returns.csv", encoding="latin1")
territories   = pd.read_csv("data/territories.csv", encoding="latin1")
calendar      = pd.read_csv("data/calendar.csv")

# =====================================================
# TRANSFORM
# =====================================================
customers.columns = customers.columns.str.lower()
customers["birth_date"] = safe_date(customers["birthdate"])
customers = customers.rename(columns={
    "customerkey":"customer_key",
    "firstname":"first_name",
    "lastname":"last_name",
    "emailaddress":"email",
    "maritalstatus":"marital_status",
    "educationlevel":"education_level",
    "homeowner":"home_owner",
    "annualincome":"annual_income",
    "totalchildren":"total_children"
})
customers = customers[[
    "customer_key","prefix","first_name","last_name","birth_date",
    "marital_status","gender","email","annual_income",
    "total_children","education_level","occupation","home_owner"
]]

categories.columns = categories.columns.str.lower()
categories = categories.rename(columns={
    "productcategorykey":"category_key",
    "categoryname":"category_name"
})[["category_key","category_name"]]

subcategories.columns = subcategories.columns.str.lower()
subcategories = subcategories.rename(columns={
    "productsubcategorykey":"subcategory_key",
    "subcategoryname":"subcategory_name",
    "productcategorykey":"category_key"
})[["subcategory_key","subcategory_name","category_key"]]

products.columns = products.columns.str.lower()
products = products.rename(columns={
    "productkey":"product_key",
    "productsubcategorykey":"subcategory_key",
    "productsku":"product_sku",
    "productname":"product_name",
    "modelname":"model_name",
    "productdescription":"product_description",
    "productcolor":"color",
    "productsize":"size",
    "productstyle":"style",
    "productcost":"cost",
    "productprice":"price"
})[[
    "product_key","product_sku","product_name","model_name",
    "product_description","color","size","style","cost","price","subcategory_key"
]]

sales.columns = sales.columns.str.lower()
sales["order_date"] = safe_date(sales["orderdate"])
sales = sales.rename(columns={
    "ordernumber":"order_number",
    "orderlineitem":"order_line",
    "productkey":"product_key",
    "customerkey":"customer_key",
    "territorykey":"territory_key",
    "orderquantity":"quantity"
})[[
    "order_number","order_line","order_date",
    "product_key","customer_key","territory_key","quantity"
]]

returns.columns = returns.columns.str.lower()
returns["return_date"] = safe_date(returns["returndate"])
returns = returns.rename(columns={
    "territorykey":"territory_key",
    "productkey":"product_key",
    "returnquantity":"return_quantity"
})[["return_date","territory_key","product_key","return_quantity"]]

territories.columns = territories.columns.str.lower()
territories = territories.rename(columns={
    "salesterritorykey":"territory_key"
})[["territory_key","region","country","continent"]]

calendar.columns = calendar.columns.str.lower()
calendar["date"] = safe_date(calendar["date"])
date_df = calendar.dropna(subset=["date"]).copy()
date_df.loc[:, "date_key"] = date_df["date"].dt.date
date_df.loc[:, "year"] = date_df["date"].dt.year
date_df.loc[:, "month"] = date_df["date"].dt.month
date_df.loc[:, "quarter"] = date_df["date"].dt.quarter
date_df = date_df[["date_key","year","month","quarter"]]

# =====================================================
# LOAD
# =====================================================
insert_skip_duplicates(customers, "dim_customer", ["customer_key"])
insert_skip_duplicates(categories, "dim_product_category", ["category_key"])
insert_skip_duplicates(subcategories, "dim_product_subcategory", ["subcategory_key"])
insert_skip_duplicates(products, "dim_product", ["product_key"])
insert_skip_duplicates(territories, "dim_sales_territory", ["territory_key"])
insert_skip_duplicates(date_df, "dim_date", ["date_key"])
insert_skip_duplicates(sales, "fact_sales", ["order_number","order_line"])
insert_skip_duplicates(returns, "fact_returns", ["return_date","territory_key","product_key"])

print("ADVENTUREWORKS ETL PIPELINE COMPLETED SUCCESSFULLY")