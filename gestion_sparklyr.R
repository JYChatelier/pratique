library(sparklyr)
library(dplyr)


spark_install(version = "3.3")

sc <- spark_connect(master = "local", version = '3.3')

mtcars <- copy_to(sc, mtcars, "mtcars")
flights <- copy_to(sc, nycflights13::flights, "flights")


df <- aws.s3::s3read_using(FUN = arrow::read_parquet,
                            object = "analyse_2009_2021.csv.parquet", 
                            bucket = "projet-iquale",
                            opts = list("region" = ""))

arrow::write_parquet(df,"analyse_2009_2021.csv.parquet")

aws