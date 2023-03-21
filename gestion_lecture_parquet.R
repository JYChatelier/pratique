library(dplyr)

minio <- arrow::S3FileSystem$create(
  access_key = Sys.getenv("AWS_ACCESS_KEY_ID"),
  secret_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
  session_token = Sys.getenv("AWS_SESSION_TOKEN"),
  scheme = "https",
  endpoint_override = Sys.getenv("AWS_S3_ENDPOINT")
)

df <- arrow::open_dataset(sources = minio$path("jchatelier/naiades"),
                    #partitioning = "year",
                    #format = "parquet", par défaut
                    )

n <- df %>% collect() %>% summarise(n = n())
annees <- df %>% collect() %>% distinct(year)

selection <- 2015:2018
df_selection <- arrow::open_dataset(
  sources = minio$path("jchatelier/naiades/year=2018"))
n <- df_selection %>% collect() %>% summarise(n = n())

