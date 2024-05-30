# Databricks notebook source
# MAGIC %md
# MAGIC ###Iowa_all - Fact

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType

iowaSchema = StructType([
    #StructField("ID", IntegerType(), True),
    StructField("InvoiceItemNumber", StringType(), True),
    StructField("Date", DateType(), True),
    StructField("StoreID", IntegerType(), True),
    StructField("StoreName", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("ZipCode", StringType(), True),
    StructField("StoreLocation", StringType(), True),
    StructField("CountyNumber", IntegerType(), True),
    StructField("County", StringType(), True),
    StructField("CategoryID", IntegerType(), True),
    StructField("CategoryName", StringType(), True),
    StructField("VendorID", IntegerType(), True),
    StructField("VendorName", StringType(), True),
    StructField("ItemID", IntegerType(), True),
    StructField("ItemName", StringType(), True),
    StructField("Pack", IntegerType(), True),
    StructField("BottleVolumeMl", IntegerType(), True),
    StructField("StateBottleCost", DoubleType(), True),
    StructField("StateBottleRetail", DoubleType(), True),
    StructField("BottlesSold", IntegerType(), True),
    StructField("SaleDollars", DoubleType(), True),
    StructField("VolumeSoldLiters", DoubleType(), True),
   StructField("VolumeSoldGallons", DoubleType(), True)
])

# COMMAND ----------

iowa_all = spark.read.csv("/mnt/mol/abc/Iowa_Liquor_Sales.csv", header = True, dateFormat="MM/dd/yyyy", schema = iowaSchema)
#Iowa_Liquor_Sales_20240406.csv

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, col, initcap, regexp_replace, cast, date_format, year

iowa_all = iowa_all \
    .withColumn("ID", monotonically_increasing_id()) \
    .withColumn("StoreName", initcap(col("StoreName"))) \
    .withColumn("Address", initcap(col("Address"))) \
    .withColumn("City", initcap(col("City"))) \
    .withColumn("County", initcap(col("County"))) \
    .withColumn("CategoryName", initcap(col("CategoryName"))) \
    .withColumn("ItemName", initcap(col("ItemName"))) \
    .drop(col("CountyNumber")) \
    .withColumn('ZipCode', regexp_replace('ZipCode', '-', '')) \
    .withColumn("PartitionKey", year(col("Date"))) \
    .withColumn("Date", date_format("Date", "yyyyMMdd"))
    

iowa_all = iowa_all \
    .withColumn("ZipCode",iowa_all.ZipCode.cast(IntegerType())) \
    .withColumn("Date", iowa_all.Date.cast(IntegerType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Iowa_sales - Fact (iowa_all downstream)

# COMMAND ----------

iowa_sales = iowa_all \
    .drop("ItemName", "CategoryName", "VendorName", "StoreName", "Address", "City", "ZipCode", "County", "StoreLocation") \
    .select("ID", "InvoiceItemNumber", "Date", "StoreID", "CategoryID", "VendorID", "ItemID", "Pack", "BottleVolumeMl", "StateBottleCost", "StateBottleRetail", "BottlesSold", "SaleDollars", "VolumeSoldLiters", "VolumeSoldGallons", "PartitionKey")

# COMMAND ----------

#option 1 - filter out null rows
# iowa_sales = iowa_sales \
#     .filter(
#         ((col("StoreID").isNotNull()) & (col("CategoryID").isNotNull()) & (col("VendorID").isNotNull()) & (col("ItemID").isNotNull()))
#         )

# COMMAND ----------

# option 2 - fill null rows

iowa_sales = iowa_sales.fillna(-1, subset=["StoreID", "CategoryID", "VendorID", "ItemID"])

# COMMAND ----------

# MAGIC %md
# MAGIC ###Iowa_population - Fact

# COMMAND ----------

from pyspark.sql.functions import rand, round

iowa_population = spark.createDataFrame(
    [
        (1, 'Polk', 493378), (2, 'Linn', 229308), (3, 'Scott', 174315), (4, 'Johnson', 153360), (5, 'Black Hawk', 131041),
        (6, 'Woodbury', 105526), (7, 'Dallas', 100367), (8, 'Dubuque', 98812), (9, 'Story', 98573), (10, 'Pottawattamie', 93543),
        (11, 'Warren', 52709), (12, 'Clinton', 46488), (13, 'Cerro Gordo', 42979), (14, 'Muscatine', 42968), (15, 'Marshall', 40049),
        (16, 'Des Moines', 38824), (17, 'Jasper', 37808), (18, 'Webster', 36986), (19, 'Sioux', 35815), (20, 'Wapello', 35292),
        (21, 'Marion', 33487), (22, 'Lee', 33442), (23, 'Boone', 26700), (24, 'Benton', 25652), (25, 'Plymouth', 25621),
        (26, 'Bremer', 25012), (27, 'Washington', 22534), (28, 'Mahaska', 22059), (29, 'Jones', 20733), (30, 'Carroll', 20728),
        (31, 'Buena Vista', 20687), (32, 'Buchanan', 20631), (33, 'Henry', 20436), (34, 'Winneshiek', 20050), (35, 'Fayette', 19519),
        (36, 'Jackson', 19470), (37, 'Poweshiek', 18608), (38, 'Cedar', 18479), (39, 'Dickinson', 17692), (40, 'Delaware', 17523),
        (41, 'Clayton', 17123), (42, 'Tama', 17017), (43, 'Hardin', 16861), (44, 'Madison', 16609), (45, 'Iowa', 16575),
        (46, 'Crawford', 16450), (47, 'Clay', 16423), (48, 'Jefferson', 15785), (49, 'Floyd', 15601), (50, 'Page', 15225),
        (51, 'Hamilton', 14979), (52, 'Kossuth', 14718), (53, 'Harrison', 14623), (54, 'Mills', 14605), (55, 'Butler', 14376),
        (56, 'OBrien', 14138), (57, 'Allamakee', 14046), (58, 'Cass', 13116), (59, 'Wright', 12897), (60, 'Grundy', 12351),
        (61, 'Appanoose', 12279), (62, 'Union', 12107), (63, 'Lyon', 12023), (64, 'Chickasaw', 11957), (65, 'Shelby', 11737),
        (66, 'Cherokee', 11601), (67, 'Louisa', 10823), (68, 'Hancock', 10790), (69, 'Winnebago', 10704), (70, 'Guthrie', 10619),
        (71, 'Mitchell', 10537), (72, 'Montgomery', 10285), (73, 'Keokuk', 10026), (74, 'Franklin', 10015), (75, 'Calhoun', 9893),
        (76, 'Sac', 9778), (77, 'Clarke', 9737), (78, 'Humboldt', 9606), (79, 'Howard', 9479), (80, 'Emmet', 9349),
        (81, 'Davis', 9093), (82, 'Palo Alto', 8938), (83, 'Greene', 8801), (84, 'Monona', 8671), (85, 'Lucas', 8666),
        (86, 'Decatur', 7684), (87, 'Monroe', 7574), (88, 'Adair', 7479), (89, 'Worth', 7417), (90, 'Van Buren', 7207),
        (91, 'Pocahontas', 7061), (92, 'Ida', 6969), (93, 'Fremont', 6639), (94, 'Wayne', 6484), (95, 'Osceola', 6149),
        (96, 'Taylor', 5916), (97, 'Audubon', 5651), (98, 'Ringgold', 4690),(99, 'Adams', 3680)
    ],
    ["ID", "County", "Population"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Iowa - dims

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import min, max

min_date_a = iowa_all.agg(min('Date')).collect()[0][0]
max_date_a = iowa_all.agg(max('Date')).collect()[0][0]

min_date = datetime.strptime(str(min_date_a), '%Y%m%d').strftime('%Y-%m-%d')
min_date = datetime.strptime(min_date, '%Y-%m-%d') 

max_date = datetime.strptime(str(max_date_a), '%Y%m%d').strftime('%Y-%m-%d')
max_date = datetime.strptime(max_date, '%Y-%m-%d') 

min_date = min_date.replace(month=1, day=1)
max_date = max_date.replace(month=12, day=31)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, sequence, to_date, min, max
from datetime import datetime

# Define the date range (modify as needed)
beginDate = str(min_date)
endDate = str(max_date)

# Generate an array of dates and explode it into a data frame
spark.sql(f"SELECT explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) AS calendarDate") \
    .createOrReplaceTempView('dates')

# Select desired columns for the calendar dimension
calendar_df = spark.sql(f"""
    SELECT
        year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) AS DateInt,
        CalendarDate,
        year(calendarDate) AS CalendarYear,
        date_format(calendarDate, 'MMMM') AS CalendarMonth,
        month(calendarDate) AS MonthOfYear,
        date_format(calendarDate, 'EEEE') AS CalendarDay,
        dayofweek(calendarDate) AS DayOfWeek,
        weekday(calendarDate) + 1 AS DayOfWeekStartMonday,
        CASE WHEN weekday(calendarDate) < 5 THEN 'Y' ELSE 'N' END AS IsWeekDay,
        dayofmonth(calendarDate) AS DayOfMonth,
        CASE WHEN calendarDate = last_day(calendarDate) THEN 'Y' ELSE 'N' END AS IsLastDayOfMonth,
        dayofyear(calendarDate) AS DayOfYear,
        weekofyear(calendarDate) AS WeekOfYearIso,
        quarter(calendarDate) AS QuarterOfYear,
        -- Customize fiscal periods as needed
        CASE WHEN month(calendarDate) >= 10 THEN year(calendarDate) + 1 ELSE year(calendarDate) END AS FiscalYearOctToSep,
        (month(calendarDate) + 2) % 12 + 1 AS FiscalMonthOctToSep,
        CASE WHEN month(calendarDate) >= 7 THEN year(calendarDate) + 1 ELSE year(calendarDate) END AS FiscalYearJulToJun,
        (month(calendarDate) + 5) % 12 + 1 AS FiscalMonthJulToJun
    FROM dates
    ORDER BY calendarDate
""")

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import row_number

window_spec_item = Window.partitionBy("ItemID").orderBy(col("ItemName").desc())
window_spec_category = Window.partitionBy("CategoryID").orderBy(col("CategoryName").desc())
window_spec_vendor = Window.partitionBy("VendorID").orderBy(col("VendorName").desc())
window_spec_store = Window.partitionBy("StoreID").orderBy(col("StoreName").desc())

# COMMAND ----------

iowa_item = iowa_all \
    .select("ItemID", "ItemName").distinct() \
    .filter(col("ItemID").isNotNull()) \
    .withColumn("row_number", row_number().over(window_spec_item)) \
    .filter(col("row_number") == 1) \
    .drop("row_number")

# option 2
iowa_item_unknown = spark.createDataFrame([(-1, "Unknown")], ["ItemID", "ItemName"])
iowa_item = iowa_item.union(iowa_item_unknown)

iowa_category = iowa_all \
    .select("CategoryID", "CategoryName").distinct() \
    .filter(col("CategoryID").isNotNull()) \
    .withColumn("row_number", row_number().over(window_spec_category)) \
    .filter(col("row_number") == 1) \
    .drop("row_number")

# option2
iowa_category_unknown = spark.createDataFrame([(-1, "Unknown")], ["CategoryID", "CategoryName"])
iowa_category = iowa_category.union(iowa_category_unknown)

iowa_vendor = iowa_all \
    .select("VendorID", "VendorName").distinct() \
    .filter(col("VendorID").isNotNull()) \
    .withColumn("row_number", row_number().over(window_spec_vendor)) \
    .filter(col("row_number") == 1) \
    .drop("row_number")

# option 2
iowa_vendor_unknown = spark.createDataFrame([(-1, "Unknown")], ["VendorID", "VendorName"])
iowa_vendor = iowa_vendor.union(iowa_vendor_unknown)


iowa_store = iowa_all \
    .select("StoreID", "StoreName", "Address", "City", "ZipCode", "County", "StoreLocation").distinct() \
    .filter(col("StoreID").isNotNull()) \
    .withColumn("row_number", row_number().over(window_spec_store)) \
    .filter(col("row_number") == 1) \
    .drop("row_number")

# option 2
iowa_store_unknown = spark.createDataFrame([(-1, "Unknown", "Unknown", "Unknown", -1, "Unknown", "Unknown")], ["StoreID", "StoreName", "Address", "City", "ZipCode", "County", "StoreLocation"])
iowa_store = iowa_store.union(iowa_store_unknown)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write to DELTA

# COMMAND ----------

iowa_item.write.mode(saveMode="overwrite").option("overwriteSchema", "true").format("delta").save("/mnt/mol/Iowa_Model/iowa_delta/iowa_item")
iowa_category.write.mode(saveMode="overwrite").option("overwriteSchema", "true").format("delta").save("/mnt/mol/Iowa_Model/iowa_delta/iowa_category")
iowa_vendor.write.mode(saveMode="overwrite").option("overwriteSchema", "true").format("delta").save("/mnt/mol/Iowa_Model/iowa_delta/iowa_vendor")
iowa_store.write.mode(saveMode="overwrite").option("overwriteSchema", "true").format("delta").save("/mnt/mol/Iowa_Model/iowa_delta/iowa_store")
iowa_sales.write.mode(saveMode="overwrite").option("overwriteSchema", "true").format("delta").partitionBy("PartitionKey").save("/mnt/mol/Iowa_Model/iowa_delta/iowa_sales")
calendar_df.write.mode(saveMode="overwrite").option("overwriteSchema", "true").format("delta").save("/mnt/mol/Iowa_Model/iowa_delta/iowa_date")
iowa_population.write.mode(saveMode="overwrite").option("overwriteSchema", "true").format("delta").save("/mnt/mol/Iowa_Model/iowa_delta/iowa_population")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Write to JSON

# COMMAND ----------

iowa_population.write.option("maxRecordsPerFile", 1).json("/mnt/mol/Iowa_Model/iowa_json/iowa_population/")

# COMMAND ----------

iowa_all_not_null = iowa_all.filter(
        (col("StoreID").isNotNull()) & 
         (col("CategoryID").isNotNull()) & 
         (col("VendorID").isNotNull()) & 
         (col("ItemID").isNotNull()) &
         (col("StoreName").isNotNull()) &
         (col("CategoryName").isNotNull()) &
         (col("VendorName").isNotNull()) &
         (col("ItemName").isNotNull())
         )

iowa_all_filter_json = iowa_all_not_null.filter(
    (iowa_all.Date >= 20240101) & 
    (iowa_all.Date <= 20240331)
    ).sample(False, 0.001)

# COMMAND ----------

iowa_all_filter_json.write.option("maxRecordsPerFile", 1).json("/mnt/mol/Iowa_Model/iowa_json/iowa_sales/")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write to CSV

# COMMAND ----------

iowa_all_filter_csv = iowa_all_not_null.filter(
    (iowa_all.Date >= 20240101) & 
    (iowa_all.Date <= 20240331)
    ).sample(False, 0.1)

# COMMAND ----------

iowa_all_filter_csv.coalesce(1).write.options(header='True', delimiter=',').csv('/mnt/mol/Iowa_Model/iowa_csv/Iowa_FactSales.csv')
iowa_population.coalesce(1).write.options(header='True', delimiter=',').csv('/mnt/mol/Iowa_Model/iowa_csv/Iowa_FactPopulation.csv')
iowa_store.coalesce(1).write.options(header='True', delimiter=',').csv('/mnt/mol/Iowa_Model/iowa_csv/Iowa_DimStore.csv')
iowa_item.coalesce(1).write.options(header='True', delimiter=',').csv('/mnt/mol/Iowa_Model/iowa_csv/Iowa_DimItem.csv')
iowa_category.coalesce(1).write.options(header='True', delimiter=',').csv('/mnt/mol/Iowa_Model/iowa_csv/Iowa_DimCategory.csv')
iowa_vendor.coalesce(1).write.options(header='True', delimiter=',').csv('/mnt/mol/Iowa_Model/iowa_csv/Iowa_DimVendor.csv')
calendar_df.coalesce(1).write.options(header='True', delimiter=',').csv('/mnt/mol/Iowa_Model/iowa_csv/Iowa_DimDate.csv')
