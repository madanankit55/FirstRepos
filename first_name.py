ALWAYS DO INSTALLING
pip install spark
pip install pyspark
1) QUESTION - COUNT WITH GROUP BY IN PYSPARK 
A) GET COUNT BASED ON COLUMN WISE WITH GROUP BY
B) HOW TO DUPLICATE RECORD COLUMN WISE
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df_spark = spark.createDataFrame(
    [
        (1,'Ankit Madan','Ghaziabad','2025-01-20',263767),
        (2,'Ramehs','Noida','2025-01-20',44443),
        (3,'Ankit Madan','Ghaziabad','2025-01-20',78783),
        (4,'XYB','Ghaziabad','2025-01-20',368767)
    ],    
    "id int, employee_name string, employee_city string, employee_date string, employee_salary bigint"
)

df_spark = df_spark.withColumn("employee_date", F.to_date("employee_date"))
df_spark.show()
duplicate=(df_spark
           .groupBy('employee_city')
           .count()
           .filter(F.col('count') > 1)
        #   .withColumnRenamed('count','Dup_count')
)


duplicate.show()
2) POSEXPLODEOUTER EXAMPLE IN PYSPARK
CASE I

df = spark.createDataFrame([
    (1, ["Java", "Python", "SQL"], [5, 10, 7]),
    (2, None, None),
], ["emp_id", "skills", "experience"])

df.show()
CASE II
from pyspark.sql import functions as F,Window
from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()

df_spark=spark.createDataFrame(
    [
      (1,["A","B","C"]),
      (2,None),
      (3,[])      
    ],
    "Id int, tags array<string>"
)
# df_spark.show()
df_spark.select("Id", "tags", F.posexplode_outer("tags").alias("pos", "tag")).show()
3) QUESTION - ADVANCE EXAMPLES FOR CUMMULATIVE SUM WITH WINDOWS FUNCTIONS + PARTITION BY AND ORDER BY + AVG

A) TOTAL SALARY : WINDOW + PARTITION BY
B) CUMMULATIVE SALARY :  WINDOW + ORDER BY 
C) AVERAGE SALARY :
CASE I
from pyspark.sql import functions as F,Window
from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()

df_spark=spark.createDataFrame(
    [('Ankit Madan', 35,50000),
    ('Mahesh',  25, 76002),
    ('Sumit ',  38,  20000),
    ('Rajesh ', 27,  450000),
    ('AJAX ', 76, 47873),
    ('Saurabh ', 40, 450000)
     ],
     "name string, age int, salary bigint"
)

df_spark.show()
total_salary = Window.partitionBy()
cummulative_salary = Window.orderBy(F.col("salary"))
avg_age = Window.orderBy(F.col("age"))

df_value = df_spark.select("name","age","salary",F.sum(F.col("salary")).over(total_salary).alias("total_salary"),
                           F.sum(F.col("salary")).over(cummulative_salary).alias("cummulative_salary"),
                           F.sum(F.col("age")).over(total_salary).alias("total_age"),
                            F.round(F.avg("age").over(total_salary), 2).alias("avg_age")
                           )

df_value.show()



# row_count_col = count("*").over(Window.partitionBy(lit(1)))
# # out = df_emp.select(
# #    col("name").alias("Employee Name"),col("age"),col("salary"),row_count_col.alias("Total Employee")
# # )z
# df_emp.select(col("name").alias("Employee Name"), col("age"),col("salary"), row_count_col.alias("Total Employee")).display()



CASE II
from pyspark.sql import SparkSession
from pyspark.sql import functions as F,Window

spark = SparkSession.builder.getOrCreate()
df_volume = spark.createDataFrame(
    [
    ("a", 100), 
    ("b", 200),
    ("c", 300),
    ("d", 400)
    ], "product string , amount bigint"
)
w_total_amount = Window.partitionBy()

w_cummulative_amount = Window.orderBy(F.col("amount"))


df_total = df_volume.select("product", "amount", sum(F.col("amount")).over(w_total_amount).alias("total_amount"),
                   sum(F.col("amount")).over(w_cummulative_amount).alias("cummulative_amount"),
                    # round((col("salary")/sum("salary").over(w_all)*lit(100)),2).alias("percentage"),
                    F.concat(round((F.col("amount"))/sum(F.col("amount")).over(w_total_amount)*F.lit(100),2).cast("string"),F.lit("%")).alias("percentage"))
                            
df_total.show()
CASE III : RUNNING TOTAL  NORMAL PROCESS
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [
    ("A", "2024-01-01", 100),
    ("A", "2024-01-02", 200),
    ("A", "2024-01-03", 300),
    ("B", "2024-01-01", 50),
    ("B", "2024-01-02", 150)
]

df = spark.createDataFrame(data, ["cust_id", "date", "amount"])

window_spec = Window.partitionBy("cust_id").orderBy("date")

df = df.withColumn("running_total",
                   F.sum("amount").over(window_spec))

df.show()

CASE IV : RUNNING TOTAL WITH UNBOUNDED PRECEDING → CURRENT ROW
from pyspark.sql import functions as F,Window
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df_spark=spark.createDataFrame(
    [('A', '2024-01-01', 100),
    ('A', '2024-01-02', 200),
    ('A', '2024-01-03', 300),
    ('B', '2024-01-01', 50),
    ('B', '2024-01-02', 150)],
    "cust_id string ,cust_date string, amount bigint"
)

df_spark=df_spark.withColumn("cust_date",F.to_date("cust_date"))
df_spark.show()

window_spec = Window.partitionBy("cust_id").orderBy("cust_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_spark = df_spark.withColumn("running_total", F.sum("amount").over(window_spec))
df_spark.show()
CASE V Sum Based on Condition (Sessionization) Reset the sum based on flag when its value is 1
from pyspark.sql.functions import sum as _sum

data = [
    ("A", 1, 100),
    ("A", 0, 200),
    ("A", 1, 300),
    ("A", 0, 400),
    ("B", 1, 200),
    ("B", 0, 400),
    ("B", 1, 600),
    ("B", 0, 800)
]

df = spark.createDataFrame(data, ["cust_id", "reset_flag", "amount"])

# Create group id based on reset flag
window_spec = Window.partitionBy("cust_id").orderBy("amount")

df = df.withColumn("grp",
                   _sum("reset_flag").over(window_spec))

df.show()

# Running sum per group
window_grp = Window.partitionBy("cust_id", "grp").orderBy("amount")

df = df.withColumn("running_total",
                   _sum("amount").over(window_grp))

df.show()
CASE VI CUMMULATIVE SUM WITH TOTAL AMOUNT , RUNNING TOTAL , CUMMULATIVE PERCENTAGE and CASE CONDITION
from pyspark.sql import functions as F,Window
from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()

df_spark =spark.createDataFrame(
    [
        ('A','2024-01-01',100),
        ('A','2024-01-02',200),
        ('A','2024-01-03',300),
        ('B','2024-01-01',50),
        ('B','2024-01-02',150)

    ],"cust_id string, cust_date string, amount bigint"
)
df_spark.show()

df_spark= df_spark.withColumn("cust_date",F.to_date("cust_date"))

window_total = Window.partitionBy("cust_id")

df_total =df_spark.withColumn("total_amount",F.sum("amount").over(window_total))

window_running = Window.partitionBy("cust_id").orderBy("cust_date")

df_total =df_total.withColumn("running_total",F.sum("amount").over(window_running))

df_total=df_total.withColumn("cummulative_percentage",F.round((F.col("running_total")/F.col("total_amount")) ,2))                                                     
           
df_total.show()

df_total=df_total.withColumn("pareto_flag",F.when(F.col("cummulative_percentage") <= 0.8, "Top 80%").otherwise("Rest"))

df_total.show()

PERFORMANCE MANAGING THRU repartition and sort partition 
from pyspark.sql import functions as F,Window
from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()

df_spark =spark.createDataFrame(
    [
        ('A','2024-01-01',100),
        ('A','2024-01-02',200),
        ('A','2024-01-03',300),
        ('B','2024-01-01',50),
        ('B','2024-01-02',150)

    ],"cust_id string, cust_date string, amount bigint"
)
df_spark.show()

df_spark= df_spark.withColumn("cust_date",F.to_date("cust_date"))


# df_spark = df_spark.repartition("cust_id")  # Reduce shuffle thru repartition
df_spark = df_spark.sortWithinPartitions("cust_id", "cust_date") # thru partition 

window_spec = Window.partitionBy("cust_id") \
    .orderBy("cust_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_spark = df_spark.withColumn("running_total", F.sum("amount").over(window_spec))
df_spark.show()


CASE VIII CUMMULATIVE REVENUE WITH GAPS WHEN DAYS GAPS MORE THAN 7 DAYS THEN WE WILL RESET THE FLAG DO CUMMULATIVE SUMMATION
from pyspark.sql import functions as F,Window
from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()

df =spark.createDataFrame(
    [
        ('A','2024-01-01',100),
        ('A','2024-01-02',200),
        ('A','2024-01-10',300),
        ('B','2024-01-01',50),
        ('B','2024-01-09',150)

    ],"cust_id string, date string, amount bigint"
)
df.show()

df=df.withColumn("date",F.to_date("date"))

window_spec = Window.partitionBy("cust_id").orderBy("date")

df = df.withColumn("prev_date", F.lag("date").over(window_spec))

df = df.withColumn("gap", F.datediff(F.col("date"),F.col("prev_date")))

df = df.withColumn("reset_flag",F.when(F.col("gap") > 7, 1).otherwise(0))

df = df.withColumn("grp",F.sum("reset_flag").over(window_spec))

window_grp = Window.partitionBy("cust_id", "grp").orderBy("date")

df = df.withColumn("running_total",F.sum("amount").over(window_grp))

df.show()
JOINS IN PYSPARK
CASE I Normal Joins
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df1 = spark.createDataFrame(
    [
        (1,'A',100),
        (2,'B',200),
        (3,'C',300)
    ],"id int , name string , amount integer"
)

df2 = spark.createDataFrame(
    [
        (1,'A','hell'),
        (2,'B','good'),
        (4,'bn','xyz')
    ], "id int , name string , type string"
)


df_joining =(
    df1.alias("d1")
            .join(
                    df2.alias("d2"),
                    (
                        (F.col("d1.id")== F.col("d2.id")) & 
                        (F.col("d1.name")== F.col("d2.name"))
                    ),
                    "inner"
                    )
            .select(
                        F.col("d1.id").alias("Id"),
                        F.col("d2.type").alias("Type")
                    )
    )


df_joining.show()
CASE II JOIN WITH FILTER WITH FULL JOIN
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession

spark= SparkSession.builder.getOrCreate()

df_source = spark.createDataFrame(
    [
        (1,'A'),
        (2,'B'),
        (3,'C'),
        (4,'D'),
    ],"id int , name string"
)

df_target = spark.createDataFrame(
    [
        (1,'A'),
        (2,'B'),
        (5,'X'),
        (6,'Y'),
        (7,'z'),
    ]," id int , name string"
)

missing_value = (
        df_source.alias("s")
        .join(
               df_target.alias("t"),                
                    (F.col("s.id") == F.col("t.id"))  & 
                    (F.col("s.name") == F.col("t.name")),                
                "full"            
        )
        .filter(
                    F.col("s.id").isNull()  | F.col("t.id").isNull()
               )
        .select(  
                    F.coalesce(F.col("s.id"),F.col("t.id")).alias("Id"),
                    F.coalesce(F.col("s.name"),F.col("t.name")).alias("Name"),
               )       

)

missing_value.show()

CASE ||| LEFT ANTI JOIN WITH UNION
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession

spark= SparkSession.builder.getOrCreate()

df_source = spark.createDataFrame(
    [
        (1,'A'),
        (2,'B'),
        (3,'C'),
        (4,'D'),
    ],"id int , name string"
)

df_target = spark.createDataFrame(
    [
        (1,'A'),
        (2,'B'),
        (5,'X'),
        (6,'Y'),
        (7,'z'),
    ]," id int , name string"
)
df_left = (
    df_source.alias("s")
            .join(
                    df_target.alias("t"),
                        (F.col("s.id") == F.col("t.id")) &
                        (F.col("s.name") == F.col("t.name"))
            ,"left_anti"
    )
    .select(
        F.col("s.id").alias("Id"), 
        F.col("s.name").alias("Name"))

)
df_right = (
    df_target.alias("t")
            .join(
                    df_source.alias("s"),
                        (F.col("s.id") == F.col("t.id")) &
                        (F.col("s.name") == F.col("t.name"))
            ,"left_anti"
    )
    .select(
        F.col("t.id").alias("Id"), 
        F.col("t.name").alias("Name"))

)

missing = df_left.union(df_right)
missing.show()






CASE IV EXCEPT ALL WITH JOIN
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession

spark= SparkSession.builder.getOrCreate()

df_source = spark.createDataFrame(
    [
        (1,'A'),
        (2,'B'),
        (3,'C'),
        (4,'D'),
    ],"id int , name string"
)

df_target = spark.createDataFrame(
    [
        (1,'A'),
        (2,'B'),
        (5,'X'),
        (6,'Y'),
        (7,'z'),
    ]," id int , name string"
)

df_source.exceptAll(df_target).union(df_target.exceptAll(df_source)).show()

df_source.explain("formatted")
CASE V Advance Chained Joins in Pyspark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.getOrCreate()

df_orders = spark.createDataFrame([
    (1, 101, 1000, "2024-01-01"),
    (2, 102, 2000, "2024-01-02"),
    (3, 103, 1500, "2024-01-03")
], ["order_id", "cust_id", "amount", "order_date"])

df_customers = spark.createDataFrame([
    (101, "Ankit", 1),
    (102, "Rahul", 2),
    (104, "John", 3)
], ["cust_id", "name", "region_id"])

df_products = spark.createDataFrame([
    (1, "Laptop"),
    (2, "Mobile"),
    (3, "Tablet")
], ["order_id", "product_name"])

df_payments = spark.createDataFrame([
    (1, "SUCCESS"),
    (2, "FAILED"),
    (3, "SUCCESS")
], ["order_id", "payment_status"])

df_regions = spark.createDataFrame([
    (1, "India"),
    (2, "USA"),
    (3, "UK")
], ["region_id", "country"])

df_final = df_orders.alias("o") \
    .join(df_customers.alias("c"),
          F.col("o.cust_id") == F.col("c.cust_id"),
          "left") \
    .join(df_products.alias("p"),
          "order_id",  # natural join
          "inner") \
    .join(df_payments.alias("pay"),
          (F.col("o.order_id") == F.col("pay.order_id")) &
          (F.col("pay.payment_status") == "SUCCESS"),  # conditional join
          "left") \
    .join(broadcast(df_regions).alias("r"),   # broadcast join
          F.col("c.region_id") == F.col("r.region_id"),
          "left") \
    .select(
        "o.order_id",
        "c.name",
        "p.product_name",
        "o.amount",
        "pay.payment_status",
        "r.country"
    )
# df_final.explain("formatted")
df_final.show()

df_clean = df_final.fillna({
    "payment_status": "PENDING",
    "country": "UNKNOWN"
})

df_clean.show()
df_agg = df_clean.groupBy("country") \
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("amount").alias("total_revenue")
    )

df_agg.show()

from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Existing Dimension (SCD Type 2)
df_dim = spark.createDataFrame([
    (101, "Ankit", "India", "2024-01-01", None, True),
    (102, "Rahul", "USA", "2024-01-01", None, True)
], ["cust_id", "name", "country", "effective_date", "end_date", "is_current"])

# Incoming Updates
df_updates = spark.createDataFrame([
    (101, "Ankit", "UK", "2024-02-01"),   # changed country
    (103, "John", "India", "2024-02-01")  # new customer
], ["cust_id", "name", "country", "update_date"])

df_joined = df_updates.alias("u") \
    .join(df_dim.alias("d"),
          (F.col("u.cust_id") == F.col("d.cust_id")) &
          (F.col("d.is_current") == True),
          "left")

df_changes = df_joined.withColumn(
    "is_changed",
    F.when(
        (F.col("u.country") != F.col("d.country")) |
        (F.col("d.cust_id").isNull()),
        1
    ).otherwise(0)
)

df_expire = df_changes.filter("is_changed = 1 AND d.cust_id IS NOT NULL") \
    .select(
        F.col("d.cust_id"),
        F.col("d.name"),
        F.col("d.country"),
        F.col("d.effective_date"),
        F.col("u.update_date").alias("end_date"),
        F.lit(False).alias("is_current")
    )

df_unchanged = df_dim.filter("is_current = true") \
    .join(df_updates, "cust_id", "left_anti")

df_new = df_changes.filter("is_changed = 1") \
    .select(
        F.col("u.cust_id"),
        F.col("u.name"),
        F.col("u.country"),
        F.col("u.update_date").alias("effective_date"),
        F.lit(None).cast("string").alias("end_date"),
        F.lit(True).alias("is_current")
    )

df_final = df_unchanged \
    .unionByName(df_expire) \
    .unionByName(df_new)

window_spec = Window.partitionBy("cust_id").orderBy(F.col("effective_date").desc())

df_final = df_final.withColumn(
    "rank",
    F.row_number().over(window_spec)
).withColumn(
    "is_current",
    F.when(F.col("rank") == 1, True).otherwise(False)
).drop("rank")
HOW TO READ PDF FILES IN PYSPARK
pip install pypdf2
# import os
# from PyPDF2 import PdfMerger

# merger = PdfMerger()

# for file in os.listdir():
#     if file.endswith(".pdf"):
#         merger.append(file)

# merger.write("merged.pdf")
# merger.close()
import os
from PyPDF2 import PdfMerger

merger = PdfMerger()

files = ["CG_FEB_2026_46368909_Payslip.pdf", "CG_JAN_2026_46368909_Payslip.pdf", "CG_DEC_2025_46368909_Payslip.pdf"]

for pdf in files:
    merger.append(pdf)

merger.write("merged.pdf")
merger.close()
SCHEMA EVOLUTIONS
UNION BY NAME in PYSPARK
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame(
[
    ('A','B',100),
    ('B','A',40),
    ('A','C',30),
    ('','A',25),
    ('A','',30),
    ('C','A',15)
],
["Col_A","Col_B","Amount"]
)

# 1. Normalize and sum A–B and B–A pairs
df_norm = df.filter((F.col("Col_A") != "") & (F.col("Col_B") != "")) \
    .withColumn("Col_low",  F.least("Col_A", "Col_B")) \
    .withColumn("Col_high", F.greatest("Col_A", "Col_B")) \
    .select(F.col("Col_low").alias("Col_A"),
            F.col("Col_high").alias("Col_B"),
            "Amount")

summed = df_norm.groupBy("Col_A", "Col_B") \
    .agg(F.sum("Amount").alias("Amount"))

# 2. Capture rows where Col_A is empty
df_colA_null = df.filter(F.col("Col_A") == "") \
    .select("Col_A","Col_B","Amount")

# 3. Capture rows where Col_B is empty
df_colB_null = df.filter(F.col("Col_B") == "") \
    .select("Col_A","Col_B","Amount")

# 4. Union all three datasets
df_all = summed.unionByName(df_colA_null, allowMissingColumns=True) \
               .unionByName(df_colB_null, allowMissingColumns=True)

df_all.show()
DATE FORMAT PROCESS in PYSPARK
from pyspark.sql import functions as F,Window
from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()

df_dateframe =spark.createDataFrame(
    [
   ('20.1.2026',50),
 ('1/18/2026',20),
 ('05-1-2026',70),
 ('01/22/2026',20),
 ('02.02.2026',20),
 ('2/12/2026',20),
 ('15-2-2026',40)
    ],
    ["date","amount"]
)
df_out = (
    df_dateframe
    .withColumn("has_slash", F.instr("date", "/") > 0)
    .withColumn("has_dot",   F.instr("date", ".") > 0)
    .withColumn("parts",     F.when(F.col("has_slash"), F.split("date", "/")))
    .withColumn(
        "new_value",
        F.when(
            F.col("has_slash") & (F.size("parts") == 3),
            F.concat(
                F.col("parts")[1], F.lit("/"),
                F.col("parts")[0], F.lit("/"),
                F.col("parts")[2]
            )
        ).when(
            F.col("has_slash"),  # slash present but not 3 tokens
            F.lit(None)          # or use F.col("date") if you want to keep original
        ).when(
            F.col("has_dot"),
            F.regexp_replace("date", r"\.", "-")   # dot → dash
        ).otherwise(F.col("date"))                 # e.g., already using dashes
    )
    # FINAL: replace slashes in new_value with dashes
    .withColumn("final_value", F.translate("new_value", "/", "-"))
)


# 1) Convert your final_value (like '18-1-2026') into DateType
df2 = df_out.withColumn("parsed_date", F.to_date("final_value", "d-M-yyyy"))

# 2) Normalize to month start, then aggregate sales by month
df_month = (
    df2.withColumn("month_start", F.date_trunc("month", F.col("parsed_date")))
       .groupBy("month_start")
       .agg(F.sum("amount").alias("current_sales"))
       .orderBy("month_start")
)

# 3) Use window + lag to get previous month's sales (previous row)
w = Window.orderBy("month_start")
df_mom = (
    df_month
    .withColumn("prev_sales", F.lag("current_sales", 1).over(w))  
    )


df_mom.show(truncate=False)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

spark=SparkSession.builder.getOrCreate()
# date dd:mm:yyyy
datafram =spark.createDataFrame([('09-1-2026',20),
            ('18.1.2026',40),
            ('01-01-2026',25),
            ('1/20/2026',20),
            ('1/19/2026',25),
            ('16.2.2026',30),
            ('22-2-2026',50),
            ],
            ["date","amount"])
df_clean = datafram.withColumn(
    "date_clean",
    F.regexp_replace("date", r"[./]", "-")
    # F.regexp_replace("date_clean", r"-(\d{3})$", r"-2\1")
)

# Convert to proper date format
df_final = df_clean.withColumn(
    "date_converted",
    F.try_to_date("date_clean", "dd-M-yyyy")
)

df_final.show(truncate=False)
df_final.printSchema()
How to get the record with consecutive days with no. of days and consecutive 3 days **with** more than 100 records
from pyspark.sql.functions import col,lag,lead,sum,datediff,when,count,min,max,lit
from pyspark.sql import Window
from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()

df=spark.createDataFrame([("1","2025-01-01",100),
("2","2025-01-02",29),
			 ("3","2025-01-03",150),
			 ("4","2025-01-04",99),
             ("5","2025-01-05",145),
             ("6","2025-01-06",1499),
			 ("7","2025-01-07",199),
			 ("8","2025-01-08",188)],
["id","date","people"]
)

# cast method

df_cast = df.withColumn("date",col("date").cast("date"))
df_filtered = df_cast.filter(col("people")>=100)

w = Window.orderBy("date")
df_group = (
            df_filtered.withColumn("previous_date",lag("date").over(w))
            .withColumn("new_group",(datediff(col("date"),col("previous_date"))!=1).cast("integer"))
            
            )

df_group.show()


df_grouped = df_group.withColumn("group_id", sum("new_group").over(w))

df_grouped.show(truncate=False)
w_lit = Window.partitionBy("group_id")
df_vals = (
  df_grouped.select(
    "id",
    "date",
    "people",    
    count(lit(1)).over(w_lit).alias("no_of_vals").cast("integer"))
)
df_consecutive_3_days= df_vals.filter(col("no_of_vals") >= 4)
df_consecutive_3_days.show(truncate=False)

# df_grouped = df_grouped.withColumn("group_id", sum("new_group").over(w))	")

# -- how to do aggregate with start and end date with no. of days
# agg = (
#     df_grouped.groupBy("group_id")
#     .agg(
#         min("date").alias("start_date"),
#         max("date").alias("end_date"),
#         count("*").alias("days_in_streak")
#         # sum("people").alias("total_people")
#     )
#     .orderBy("group_id")
# )

# agg.show(truncate=False)



How to do min and max amount with region and prodname wise 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number,filter,lit,sum,when,min,max
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

df_product = spark.createDataFrame([
    (1, "samsung","bnbc","PRO01",500),
    (2, "apple","dndj","PRO02",800),
    (3, "vivo","snjb","PRO3",350),
    (4, "oppo","snjb","PRO4",450),
    (5, "xiaomi","snjb","PRO5",290),
], ["prodid", "prodname","proddesc","prodcode","cost"])

df_order = spark.createDataFrame([
    (1, "nmc",1,"Noida",250),
    (1, "nmc",2,"Delhi",280),
    (1, "nmc",3,"Mumbai",140),
    (1, "nmc",4,"Bangalore",450),
    (1, "nmc",5,"Chennai",700),
    (2, "nmc",5,"Hyderabad",600),
    (2, "nmc",2,"Pune",700),
], ["orderId", "ordername","productid","region","qty"])

df_expr = df_product.join(
    df_order,
    df_product.prodid == df_order.productid,
    "inner"
)

df_amounts_full = df_expr.select(
    "prodname", "prodid", "prodcode", "orderId", "region", "qty",
    (col("cost") * col("qty")).alias("amount_val")
)

w_min = Window.partitionBy("prodname").orderBy(col("amount_val").asc())
w_max = Window.partitionBy("prodname").orderBy(col("amount_val").desc())

# df_amounts_full.display()

df_min = (df_amounts_full.withColumn("min_value", row_number().over(w_min))
        .where(col("min_value") == 1)
        .drop("min_value")
      .withColumnRenamed("amount_val", "min_amount")
      .withColumnRenamed("region", "min_region")
      .withColumnRenamed("orderId", "min_orderId")
      .withColumnRenamed("qty", "min_qty")
      .select("prodname","prodcode","min_region","min_amount")
)
df_min.show()

df_max =(df_amounts_full.withColumn("max_value",row_number().over(w_max))
         .where(col("max_value")==1)
         .drop("max_value")
         .withColumnRenamed("amount_val","max_amount")
         .withColumnRenamed("region","max_region")
         .withColumnRenamed("orderId","max_orderId")
         .withColumnRenamed("qty","max_qty")
         .select("prodname","prodcode","max_region","max_amount")
         )
df_max.show()

df_min_max = (
    df_min
    .join(df_max,df_min.prodname==df_max.prodname,                     
                                                    "inner")
    .select(df_min.prodname,df_min.prodcode,df_min.min_region,df_min.min_amount,df_max.max_region,df_max.max_amount))
df_min_max.show()

              

How to use struct feature in pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,max,concat_ws,struct,lit,sum,when,min

spark = SparkSession.builder.getOrCreate()

df_product = spark.createDataFrame([
    (1, "samsung","bnbc","PRO01",500),
    (2, "apple","dndj","PRO02",800	),
    (3, "vivo","snjb","PRO3",350),
    (4, "oppo","snjb","PRO4",450),
    (5, "xiaomi","snjb","PRO5",290),
], ["prodid", "prodname","proddesc","prodcode","cost"])

df_order = spark.createDataFrame([
    (1, "nmc",1,"Noida",250),
    (1, "nmc",2,"Delhi",280),
    (1, "nmc",3,"Mumbai",140),
    (1, "nmc",4,"Bangalore",450),
    (1, "nmc",5,"Chennai",700),
    (2, "nmc",5,"Hyderabad",600),
    (2, "nmc",2,"Pune",700),
], ["orderId", "ordername","productid","region","qty"])

# df_product.display()
# df_order.display()


df_expr = df_product.join(
    df_order,
    df_product.prodid == df_order.productid,
    "inner"
)

df_expr.select("prodid","prodname","prodcode","ordername","region","cost","qty",
		when(col("qty")>500, "High Volume")
		.when(col("qty")>300, "Medium Volume")
		.otherwise("Low Volume")
		.alias("Volume_Category"),
		concat_ws(" - ", col("prodname"),col("prodcode")).alias("Full_Description"),
		struct(col("region"),col("prodname")).alias("region_struct")
).show()



## how to show column with group by and aggregate in pyspark 
df_region_min = (
    df_expr.select("prodname",(col("cost")*col("qty")).alias("amount_val"))
    .groupBy("prodname")
    .agg(min("amount_val").alias("amount"))  
    
)

df_region_min.show()

# how to get min and max amount based on product name
df_aggs= (df_expr.withColumn("amount_val",col("cost")*col("qty"))
          .groupBy("prodname","region")
          .agg(min("amount_val").alias("min_amount"),
          max("amount_val").alias("max_amount")
               )
          )

df_aggs.show()







from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

df_product = spark.createDataFrame([
    (1, "samsung","bnbc","PRO01",500),
    (2, "apple","dndj","PRO02",800),
    (3, "vivo","snjb","PRO3",350),
    (4, "oppo","snjb","PRO4",450),
    (5, "xiaomi","snjb","PRO5",290),
], ["prodid", "prodname","proddesc","prodcode","cost"])

df_order = spark.createDataFrame([
    (1, "nmc",1,"Noida",250),
    (1, "nmc",2,"Delhi",280),
    (1, "nmc",3,"Mumbai",140),
    (1, "nmc",4,"Bangalore",450),
    (1, "nmc",5,"Chennai",700),
    (2, "nmc",5,"Hyderabad",600),
    (2, "nmc",2,"Pune",700),
], ["orderId", "ordername","productid","region","qty"])

df_expr = df_product.join(
    df_order,
    df_product.prodid == df_order.productid,
    "inner"
)

df_amounts_full = df_expr.select(
    "prodname", "prodid", "prodcode", "orderId", "region", "qty",
    (col("cost") * col("qty")).alias("amount_val")
)

w_min = Window.partitionBy("prodname").orderBy(col("amount_val"))
w_max = Window.partitionBy("prodname").orderBy(col("amount_val").desc())


df_min_row = (
    df_amounts_full
      .withColumn("rn_min", row_number().over(w_min))
      .where(col("rn_min") == 1)
      .drop("rn_min")
      .withColumnRenamed("amount_val", "min_amount")
      .withColumnRenamed("region", "min_region")
      .withColumnRenamed("orderId", "min_orderId")
      .withColumnRenamed("qty", "min_qty")
      .select("prodname", "prodid", "prodcode", "min_orderId", "min_region", "min_qty", "min_amount")
)

# Row with MAX amount per product
df_max_row = (
    df_amounts_full
      .withColumn("rn_max", row_number().over(w_max))
      .where(col("rn_max") == 1)
      .drop("rn_max")
      .withColumnRenamed("amount_val", "max_amount")
      .withColumnRenamed("region", "max_region")
      .withColumnRenamed("orderId", "max_orderId")
      .withColumnRenamed("qty", "max_qty")
      .select("prodname","prodid","prodcode", "max_orderId", "max_region", "max_qty", "max_amount")
)

# Join min and max rows per product
df_min_max_rows = (
    df_min_row
      .join(df_max_row, ["prodname", "prodid", "prodcode"], "inner")
)

# Use .display() on Databricks
# df_min_row.display()
# df_max_row.display()
df_min_max_rows.show()
How to get 100 people atleast 3 days or more than 3 days in an stadium
from pyspark.sql.functions import col, lag, datediff, sum as sf_sum
from pyspark.sql import Window 
from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()

df_consecutive= spark.createDataFrame([("1","2025-01-01",100),
			 ("2","2025-01-02",29),
			 ("3","2025-01-03",150),
			 ("4","2025-01-04",99),
             ("5","2025-01-05",145),
             ("6","2025-01-06",1499),
			 ("7","2025-01-07",199),
			 ("8","2025-01-08",188)],
			["id","date","people"])

df_consecutive.dtypes
# Convert date column to date type
df = df_consecutive.withColumn("date", col("date").cast("date"))

# df.show()
df_filtered = df.filter(col("people") >= 100)
# df_filtered.show()

w = Window.orderBy("date")


df_groups = (
    df_filtered
      .withColumn("prev_date", lag("date").over(w))
      .withColumn(
          "new_group",
          (datediff(col("date"), col("prev_date")) != 1).cast("int")
      )
)
# df_groups.show()
df_grouped = (
    df_groups
      .withColumn("group_id", sf_sum("new_group").over(w))
)
# df_grouped.show()

df_result = (
    df_grouped
      .groupBy("group_id")
      .count()
      .filter(col("count") >= 3)
      .join(df_grouped, "group_id")
      .select("id", "date", "people")
      .orderBy("date")
)

df_result.show()

