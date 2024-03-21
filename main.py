import pyspark.sql as ps

from pyspark.sql.functions import desc


name_dataframe = "<PATH>"
column_product = "<COLUMN>"
column_category = "<COLUMN>"

spark = ps.SparkSession.builder.getOrCreate()
spark.conf.set(
    "spark.sql.repl.eagerEval.enabled",
    True,
)

df: ps.DataFrame = spark.read.csv(
    name_dataframe,
    header=True,
    inferSchema=True,
    encoding="utf8",
)


new_df = (
    df.select(column_product, column_category).distinct().sort(desc(column_category))
)
new_df = new_df.na.drop(subset=[column_product])
print(new_df.show(30))
# Другого способа это сделать не нашёл.
