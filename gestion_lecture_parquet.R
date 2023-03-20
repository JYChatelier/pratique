library(dplyr)
Sys.setenv("AWS_ACCESS_KEY_ID" = "S2586T68D9JAKMWO6721",
           "AWS_SECRET_ACCESS_KEY" = "ctX299GbfknQarzYjxt6FHWlM6mfgGwl+Mzw02DL",
           "AWS_DEFAULT_REGION" = "us-east-1",
           "AWS_SESSION_TOKEN" = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJTMjU4NlQ2OEQ5SkFLTVdPNjcyMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sImF1ZCI6WyJtaW5pby1kYXRhbm9kZSIsIm9ueXhpYSIsImFjY291bnQiXSwiYXV0aF90aW1lIjoxNjc5MzQwMjQwLCJhenAiOiJvbnl4aWEiLCJlbWFpbCI6ImplYW4teXZlcy5jaGF0ZWxpZXJAaW5lcmlzLmZyIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImV4cCI6MTY3OTQyNjY1MSwiZmFtaWx5X25hbWUiOiJDaGF0ZWxpZXIiLCJnaXZlbl9uYW1lIjoiSmVhbi1ZdmVzIiwiZ3JvdXBzIjpbImlxdWFsZSJdLCJpYXQiOjE2NzkzNDAyNDAsImlzcyI6Imh0dHBzOi8vYXV0aC5sYWIuc3NwY2xvdWQuZnIvYXV0aC9yZWFsbXMvc3NwY2xvdWQiLCJqdGkiOiIxZjAwNjNkYy1lYjI0LTQ4NGItOTkxNy05M2U4ZDhjZjE0YTYiLCJsb2NhbGUiOiJmciIsIm5hbWUiOiJKZWFuLVl2ZXMgQ2hhdGVsaWVyIiwibm9uY2UiOiJkM2U3ZjljYy1iM2Y2LTQ3NDEtODNjNC1lMDdmOTg4NmIzYjIiLCJwb2xpY3kiOiJzdHNvbmx5IiwicHJlZmVycmVkX3VzZXJuYW1lIjoiamNoYXRlbGllciIsInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIiwiZGVmYXVsdC1yb2xlcy1zc3BjbG91ZCJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgZ3JvdXBzIGVtYWlsIiwic2Vzc2lvbl9zdGF0ZSI6ImQ4ZTRjYWRkLWM2ZmItNGRmZS05OTJkLTA2YWZlODNhMzE4NyIsInNpZCI6ImQ4ZTRjYWRkLWM2ZmItNGRmZS05OTJkLTA2YWZlODNhMzE4NyIsInN1YiI6IjZkOGUzODc5LWZiNjItNDQ4Ny05ZWJjLTc3YWVjZWUyYWQ1YiIsInR5cCI6IkJlYXJlciJ9.GTXEHVS8nNHcMlRwh2lwieshyD8V0eFNBsip91DI-c6hOq153P-OlglEnGKHqE-DbToPX9YWwyjzYUIFKYt86w",
           "AWS_S3_ENDPOINT"= "minio.lab.sspcloud.fr")

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

