library(sfarrow)
path <- system.file("extdata", "world.parquet", package = "sfarrow")
world <- st_read_parquet(path)

library(DBI)
library(duckdb)
library(arrow)

dbGetQuery(con, "SELECT count(*) FROM read_parquet('Export stations - 50626866.parquet')")

minio <- arrow::S3FileSystem$create(
  access_key = Sys.getenv("AWS_ACCESS_KEY_ID"),
  secret_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
  session_token = Sys.getenv("AWS_SESSION_TOKEN"),
  scheme = "https",
  endpoint_override = Sys.getenv("AWS_S3_ENDPOINT"),
  region=""
)

Sys.setenv("AWS_ACCESS_KEY_ID" = "JN832LEECAJ39XY1H13T",
           "AWS_SECRET_ACCESS_KEY" = "yV2t41L8X1oy+x4dPZdwfwiqcmSwB9eBRlkZ3zdc",
           "AWS_DEFAULT_REGION" = "us-east-1",
           "AWS_SESSION_TOKEN" = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJKTjgzMkxFRUNBSjM5WFkxSDEzVCIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sImF1ZCI6WyJtaW5pby1kYXRhbm9kZSIsIm9ueXhpYSIsImFjY291bnQiXSwiYXV0aF90aW1lIjoxNjgxMTk4NTMwLCJhenAiOiJvbnl4aWEiLCJlbWFpbCI6ImplYW4teXZlcy5jaGF0ZWxpZXJAaW5lcmlzLmZyIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImV4cCI6MTY4MTI4NzM4MSwiZmFtaWx5X25hbWUiOiJDaGF0ZWxpZXIiLCJnaXZlbl9uYW1lIjoiSmVhbi1ZdmVzIiwiZ3JvdXBzIjpbImlxdWFsZSJdLCJpYXQiOjE2ODEyMDA5NjAsImlzcyI6Imh0dHBzOi8vYXV0aC5sYWIuc3NwY2xvdWQuZnIvYXV0aC9yZWFsbXMvc3NwY2xvdWQiLCJqdGkiOiI3ZjgxZGViYy03MmEwLTRiOTQtYTk0ZS02MTI0ZDcxNGY4YTEiLCJsb2NhbGUiOiJmciIsIm5hbWUiOiJKZWFuLVl2ZXMgQ2hhdGVsaWVyIiwibm9uY2UiOiI5MjAxYmI0My03OWNhLTQ5NDktYjYxNS0zMzJmY2RlYjBhMGYiLCJwb2xpY3kiOiJzdHNvbmx5IiwicHJlZmVycmVkX3VzZXJuYW1lIjoiamNoYXRlbGllciIsInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIiwiZGVmYXVsdC1yb2xlcy1zc3BjbG91ZCJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgZ3JvdXBzIGVtYWlsIiwic2Vzc2lvbl9zdGF0ZSI6ImE0ZjY4MjcyLWRlNzQtNDU5YS04YzJlLWQwY2U1MTA3NzEwZSIsInNpZCI6ImE0ZjY4MjcyLWRlNzQtNDU5YS04YzJlLWQwY2U1MTA3NzEwZSIsInN1YiI6IjZkOGUzODc5LWZiNjItNDQ4Ny05ZWJjLTc3YWVjZWUyYWQ1YiIsInR5cCI6IkJlYXJlciJ9.p7Ga3gM9qsYz7supZZYWKjOZFhMF47EdnO4eX411tX9-X2lyyIBvTHVT3ddn3kaNTQvn9DNFfEU8GaWb97AAPg",
           "AWS_S3_ENDPOINT"= "minio.lab.sspcloud.fr")


file_mtcars <- 
file_geodair <- "Export stations - 50626866.parquet"

read_parquet(file = file_mtcars)

con <- dbConnect(duckdb::duckdb())
dbSendQuery(con,"INSTALL httpfs")
dbSendQuery(con,"LOAD httpfs")
dbSendQuery(con,"SET s3_access_key_id='5946UH3WJIJ9S2F2UX2I'")
dbSendQuery(con,"SET s3_secret_access_key='agQu85kqztZRFAzUs0x8G1Qpq4J8jXnfZNYAHE3E'")
dbSendQuery(con,"SET s3_session_token='eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiI1OTQ2VUgzV0pJSjlTMkYyVVgySSIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sImF1ZCI6WyJtaW5pby1kYXRhbm9kZSIsIm9ueXhpYSIsImFjY291bnQiXSwiYXV0aF90aW1lIjoxNjgxMjQ3MzYxLCJhenAiOiJvbnl4aWEiLCJlbWFpbCI6ImplYW4teXZlcy5jaGF0ZWxpZXJAaW5lcmlzLmZyIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImV4cCI6MTY4MTMzMzc2MywiZmFtaWx5X25hbWUiOiJDaGF0ZWxpZXIiLCJnaXZlbl9uYW1lIjoiSmVhbi1ZdmVzIiwiZ3JvdXBzIjpbImlxdWFsZSJdLCJpYXQiOjE2ODEyNDczNjIsImlzcyI6Imh0dHBzOi8vYXV0aC5sYWIuc3NwY2xvdWQuZnIvYXV0aC9yZWFsbXMvc3NwY2xvdWQiLCJqdGkiOiIyNGJkYTM3Yy0zOTBiLTQ1NDQtYjUyMS01Mjk1ZWNiNjgyMzEiLCJsb2NhbGUiOiJmciIsIm5hbWUiOiJKZWFuLVl2ZXMgQ2hhdGVsaWVyIiwibm9uY2UiOiJmNmE5MjAzYi00MjEwLTQ5ZDQtODE5MC1lNTJlMTI2MmQ2NDAiLCJwb2xpY3kiOiJzdHNvbmx5IiwicHJlZmVycmVkX3VzZXJuYW1lIjoiamNoYXRlbGllciIsInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIiwiZGVmYXVsdC1yb2xlcy1zc3BjbG91ZCJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgZ3JvdXBzIGVtYWlsIiwic2Vzc2lvbl9zdGF0ZSI6IjhhMDQwOGYwLTY4Y2UtNDQxZS1hMTYxLTI4MGE5NWI0MjExMyIsInNpZCI6IjhhMDQwOGYwLTY4Y2UtNDQxZS1hMTYxLTI4MGE5NWI0MjExMyIsInN1YiI6IjZkOGUzODc5LWZiNjItNDQ4Ny05ZWJjLTc3YWVjZWUyYWQ1YiIsInR5cCI6IkJlYXJlciJ9.PR9woZOAknO2bEZjWUDhhWb_wy7-g1_8mNiX2E2bdY9tu1mBHtxs56DWBDaCxaTW9KwBS800vkgO4RiJOf_skw'")
dbSendQuery(con,"SET s3_endpoint='minio.lab.sspcloud.fr'")
#dbSendQuery(con,"SET s3_region='auto'")
dbSendQuery(con,"SET s3_url_style = 'path'") # sinon le path n'est pas bien construit
#dbSendQuery(con,"SET s3_use_ssl=false")

dbGetQuery(con, "SELECT * FROM read_parquet('s3://jchatelier/mtcars2/part-00000-803fc514-19d2-48cf-9d52-ffc606a53bff-c000.snappy.parquet')")
dbGetQuery(con, "SELECT count(*),CdStationMesureEauxSurface,CdParametre FROM read_parquet('s3://projet-iquale/analyse_2009_2021.csv.parquet') GROUP BY (CdStationMesureEauxSurface,CdParametre) LIMIT 10")
dbGetQuery(con, "SELECT count(*),CdStationMesureEauxSurface,CdParametre FROM read_parquet('s3://projet-iquale/naiades/year=2010/*.parquet') GROUP BY (CdStationMesureEauxSurface,CdParametre) LIMIT 10")

read_parquet(minio$path("jchatelier/mtcars2/part-00000-803fc514-19d2-48cf-9d52-ffc606a53bff-c000.snappy.parquet"))
dbDisconnect(con)
arrow::read_parquet(file = minio$path("projet-iquale/analyse_2009_2021.csv.parquet"))
