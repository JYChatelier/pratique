library(sparklyr)
library(dplyr)
library(lubridate)

Sys.setenv("AWS_ACCESS_KEY_ID" = "2S41TUQ55I6D7XNIDAIP",
           "AWS_SECRET_ACCESS_KEY" = "5832aWwuoM6Vq4AEsSwsmSoqmKQ6oF6iHwrOd0w+",
           "AWS_DEFAULT_REGION" = "us-east-1",
           "AWS_SESSION_TOKEN" = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiIyUzQxVFVRNTVJNkQ3WE5JREFJUCIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sImF1ZCI6WyJtaW5pby1kYXRhbm9kZSIsIm9ueXhpYSIsImFjY291bnQiXSwiYXV0aF90aW1lIjoxNjc5MzAzNjg1LCJhenAiOiJvbnl4aWEiLCJlbWFpbCI6ImplYW4teXZlcy5jaGF0ZWxpZXJAaW5lcmlzLmZyIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImV4cCI6MTY3OTM5MDA4NiwiZmFtaWx5X25hbWUiOiJDaGF0ZWxpZXIiLCJnaXZlbl9uYW1lIjoiSmVhbi1ZdmVzIiwiZ3JvdXBzIjpbImlxdWFsZSJdLCJpYXQiOjE2NzkzMDM2ODUsImlzcyI6Imh0dHBzOi8vYXV0aC5sYWIuc3NwY2xvdWQuZnIvYXV0aC9yZWFsbXMvc3NwY2xvdWQiLCJqdGkiOiI1NGRkMTgzYy01ZWVmLTQ4N2QtOGMwNC04YTZlZWVmYmE1NjMiLCJsb2NhbGUiOiJmciIsIm5hbWUiOiJKZWFuLVl2ZXMgQ2hhdGVsaWVyIiwibm9uY2UiOiIwN2QwNWE2NS1hNzMwLTQ3YTAtODUzMy02OGQxMDk0MGU2YTciLCJwb2xpY3kiOiJzdHNvbmx5IiwicHJlZmVycmVkX3VzZXJuYW1lIjoiamNoYXRlbGllciIsInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIiwiZGVmYXVsdC1yb2xlcy1zc3BjbG91ZCJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgZ3JvdXBzIGVtYWlsIiwic2Vzc2lvbl9zdGF0ZSI6IjA4YTRmYTZkLTJjZGMtNDQxMS05MGM0LWI3MzViMjk5YzAzMyIsInNpZCI6IjA4YTRmYTZkLTJjZGMtNDQxMS05MGM0LWI3MzViMjk5YzAzMyIsInN1YiI6IjZkOGUzODc5LWZiNjItNDQ4Ny05ZWJjLTc3YWVjZWUyYWQ1YiIsInR5cCI6IkJlYXJlciJ9.r0FuKeJ6Bfoy8YfYNFobYfBP1o_gGwZBjC3H9MNldGu4h3fkS9ZfLUzN21VgaluARQx2taxVucHh9xFkDNltiw",
           "AWS_S3_ENDPOINT"= "minio.lab.sspcloud.fr")

#spark_install(version = "3.3")
#sc <- spark_connect(master = "local", version = '3.3')

#Ouverture de la session Spark déjà configurée
scc <- spark_config()
scc["spark.executor.memory"] <- "16G"
sc <- spark_connect(master = "k8s://https://kubernetes.default.svc:443",config = scc)
#Lecture du fichier Sirene au format CSV
spark_sirene_csv <- spark_read_csv(sc,
                                   name = "sirene_csv",
                                   path = "s3a://projet-spark-lab/diffusion/formation/data/sirene/sirene.csv")
#Lecture du fichier Sirene au format parquet
spark_sirene_parquet <- spark_read_parquet(sc,
                                           name = "sirene_parquet",
                                           path = "s3a://projet-spark-lab/diffusion/formation/data/sirene.parquet")

spark_write_parquet(spark_sirene_parquet,
                    path = "s3a://jchatelier/pipo",
                    mode = "overwrite",
                    partition_by = "etatAdministratifEtablissement")


spark_naiades_csv <- spark_read_csv(sc,
                                    name = "naiades_csv",
                                    path = "s3a://projet-iquale/analyse_2009_2021.csv")
spark_naiades_parquet <- spark_read_parquet(sc,
                                            name = "naiades_parquet",
                                            path = "s3a://projet-iquale/analyse_2009_2021.csv.parquet")

spark_naiades_parquet_2 <- dplyr::mutate(spark_naiades_parquet, year = year(as.Date(DatePrel)))

spark_write_parquet(spark_naiades_parquet_2,
                    path = "s3a://projet-iquale/naiades",
                    mode = "overwrite",
                    partition_by = "year")

spark_naiades_parquet_3 <- spark_read_parquet(sc,
                                              name = "naiades_parquet_reconstruit",
                                              path = "s3a://jchatelier/naiades")
annees <- 2015:2018
spark_naiades_parquet_multi_annees <- spark_read_parquet(sc,
                                              name = "naiades_parquet_multi_annees",
                                              path = paste0("s3a://jchatelier/naiades/year=",annees))

spark_naiades_parquet_w <- dplyr::mutate(spark_naiades_parquet_multi_annees, year = year(as.Date(DatePrel)))
spark_naiades_parquet_w %>% distinct(year)


aws.s3::bucketlist(region="")

Sys.setenv("AWS_ACCESS_KEY_ID" = "2S41TUQ55I6D7XNIDAIP",
           "AWS_SECRET_ACCESS_KEY" = "5832aWwuoM6Vq4AEsSwsmSoqmKQ6oF6iHwrOd0w+",
           "AWS_DEFAULT_REGION" = "us-east-1",
           "AWS_SESSION_TOKEN" = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiIyUzQxVFVRNTVJNkQ3WE5JREFJUCIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sImF1ZCI6WyJtaW5pby1kYXRhbm9kZSIsIm9ueXhpYSIsImFjY291bnQiXSwiYXV0aF90aW1lIjoxNjc5MzAzNjg1LCJhenAiOiJvbnl4aWEiLCJlbWFpbCI6ImplYW4teXZlcy5jaGF0ZWxpZXJAaW5lcmlzLmZyIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImV4cCI6MTY3OTM5MDA4NiwiZmFtaWx5X25hbWUiOiJDaGF0ZWxpZXIiLCJnaXZlbl9uYW1lIjoiSmVhbi1ZdmVzIiwiZ3JvdXBzIjpbImlxdWFsZSJdLCJpYXQiOjE2NzkzMDM2ODUsImlzcyI6Imh0dHBzOi8vYXV0aC5sYWIuc3NwY2xvdWQuZnIvYXV0aC9yZWFsbXMvc3NwY2xvdWQiLCJqdGkiOiI1NGRkMTgzYy01ZWVmLTQ4N2QtOGMwNC04YTZlZWVmYmE1NjMiLCJsb2NhbGUiOiJmciIsIm5hbWUiOiJKZWFuLVl2ZXMgQ2hhdGVsaWVyIiwibm9uY2UiOiIwN2QwNWE2NS1hNzMwLTQ3YTAtODUzMy02OGQxMDk0MGU2YTciLCJwb2xpY3kiOiJzdHNvbmx5IiwicHJlZmVycmVkX3VzZXJuYW1lIjoiamNoYXRlbGllciIsInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIiwiZGVmYXVsdC1yb2xlcy1zc3BjbG91ZCJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgZ3JvdXBzIGVtYWlsIiwic2Vzc2lvbl9zdGF0ZSI6IjA4YTRmYTZkLTJjZGMtNDQxMS05MGM0LWI3MzViMjk5YzAzMyIsInNpZCI6IjA4YTRmYTZkLTJjZGMtNDQxMS05MGM0LWI3MzViMjk5YzAzMyIsInN1YiI6IjZkOGUzODc5LWZiNjItNDQ4Ny05ZWJjLTc3YWVjZWUyYWQ1YiIsInR5cCI6IkJlYXJlciJ9.r0FuKeJ6Bfoy8YfYNFobYfBP1o_gGwZBjC3H9MNldGu4h3fkS9ZfLUzN21VgaluARQx2taxVucHh9xFkDNltiw",
           "AWS_S3_ENDPOINT"= "minio.lab.sspcloud.fr")

df1 <- aws.s3::s3read_using(FUN = arrow::read_parquet,
                     object = "naiades/2019",bucket = "jchatelier",region = "")

aws.s3::s3read_using(FUN = arrow::read_parquet,
                     object = "naiades/year=2009/part-00000-35f4f879-a1fe-4caf-95fb-70c0931f3093.c000.snappy.parquet", 
                     bucket = "jchatelier",
                     opts = list("region" = ""))
aws.s3::s3read_using(FUN = arrow::open_dataset,
                     sources = "naiades",
                     #schema = "year",
                     format = "parquet",
                     hive_style = NA,
                     #object = "naiades/year=2019/part-00001-35f4f879-a1fe-4caf-95fb-70c0931f3093.c000.snappy.parquet", 
                     object = "naiades", 
                     bucket = "jchatelier",
                     opts = list("region" = ""))

arrow::open_dataset(sources = "s3://jchatelier/naiades/",
                    partitioning = "year",
                    format = "parquet",
                    hive_style = TRUE)

arrow::read_parquet("s3a://jchatelier/naiades")
spark_disconnect(sc)
