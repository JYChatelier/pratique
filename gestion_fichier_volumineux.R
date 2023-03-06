library(microbenchmark)
library(ggplot2)

Sys.setenv("AWS_ACCESS_KEY_ID" = "IUWM65O3JYVBFHQ6SH4R",
           "AWS_SECRET_ACCESS_KEY" = "+OIKF+BDOKmOzie9WOo24h3u17+zeC4XPm3HNEkF",
           "AWS_DEFAULT_REGION" = "us-east-1",
           "AWS_SESSION_TOKEN" = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJJVVdNNjVPM0pZVkJGSFE2U0g0UiIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sImF1ZCI6WyJtaW5pby1kYXRhbm9kZSIsIm9ueXhpYSIsImFjY291bnQiXSwiYXV0aF90aW1lIjoxNjc4MDg3MTAxLCJhenAiOiJvbnl4aWEiLCJlbWFpbCI6ImplYW4teXZlcy5jaGF0ZWxpZXJAaW5lcmlzLmZyIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImV4cCI6MTY3ODE3NTAyMywiZmFtaWx5X25hbWUiOiJDaGF0ZWxpZXIiLCJnaXZlbl9uYW1lIjoiSmVhbi1ZdmVzIiwiZ3JvdXBzIjpbImlxdWFsZSJdLCJpYXQiOjE2NzgwODcxMDIsImlzcyI6Imh0dHBzOi8vYXV0aC5sYWIuc3NwY2xvdWQuZnIvYXV0aC9yZWFsbXMvc3NwY2xvdWQiLCJqdGkiOiIwZjhhMzg1Yy1hZDU0LTRhODItODZkYy05MDY2ODI2OTQ0ODYiLCJsb2NhbGUiOiJmciIsIm5hbWUiOiJKZWFuLVl2ZXMgQ2hhdGVsaWVyIiwibm9uY2UiOiJlZGEyZmI1Yi02ZmM1LTQyOTUtOTRhMy1kMWUwMjljNDZlZGUiLCJwb2xpY3kiOiJzdHNvbmx5IiwicHJlZmVycmVkX3VzZXJuYW1lIjoiamNoYXRlbGllciIsInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIiwiZGVmYXVsdC1yb2xlcy1zc3BjbG91ZCJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgZ3JvdXBzIGVtYWlsIiwic2Vzc2lvbl9zdGF0ZSI6ImM1ODNlNzg4LWQyMzgtNDM0OS05YzIxLWZkOWQ4ZjhmMGQ0NCIsInNpZCI6ImM1ODNlNzg4LWQyMzgtNDM0OS05YzIxLWZkOWQ4ZjhmMGQ0NCIsInN1YiI6IjZkOGUzODc5LWZiNjItNDQ4Ny05ZWJjLTc3YWVjZWUyYWQ1YiIsInR5cCI6IkJlYXJlciJ9.1j5xyFxXo_lw5zpnN6qEr4hmcCdrQsJTvKJv89K7b2pgL7KxrogBGWKB3tJZopgGiQ-UEJGpLgSRdxoFp45I9g",
           "AWS_S3_ENDPOINT"= "minio.lab.sspcloud.fr")

# Décompression du fichier zip sur l'espace de stockage S3
df <- aws.s3::s3read_using(FUN = utils::unzip,
                           object = "/analyse_2009_2021.zip",
                           bucket = "projet-iquale",    
                           opts = list("region" = ""))
# à ce stade un fichier csv est créé sur l'espace local de la session RStudio
# dans le working directory
df <- read.csv("analyse_2009_2021.csv")

# Lecture du fichier csv nouvellement créé dans un dataframe


mbm <- microbenchmark(
  "util::read.csv" = {aws.s3::s3read_using(FUN = read.csv,
                             sep=";",
                             header=TRUE,
                             object = "analyse_2009_2021.csv", 
                             bucket = "projet-iquale",
                             opts = list("region" = ""))},
  "data.table::fread" = {aws.s3::s3read_using(FUN = data.table::fread,
                             sep=";",
                             header=TRUE,
                             object = "analyse_2009_2021.csv", 
                             bucket = "projet-iquale",
                             opts = list("region" = ""))},
  
  "arrow::read_parquet" = {aws.s3::s3read_using(FUN = arrow::read_parquet,
                              object = "analyse_2009_2021.csv.parquet", 
                              bucket = "projet-iquale",
                              opts = list("region" = ""))},
  times = 3
  
  
)
mbm
autoplot(mbm)

system.time(
  df <- aws.s3::s3read_using(FUN = read.csv,
                           sep=";",
                           header=TRUE,
                           object = "analyse_2009_2021.csv", 
                           bucket = "projet-iquale",
                           opts = list("region" = ""))
)
system.time(
  df <- aws.s3::s3read_using(FUN = data.table::fread,
                             sep=";",
                             header=TRUE,
                             object = "analyse_2009_2021.csv", 
                             bucket = "projet-iquale",
                             opts = list("region" = ""))
)
# Copie du fichier csv local à la session sur l'espace de stockage S3
aws.s3::put_object("analyse_2009_2021.csv", 
                   object = "analyse_2009_2021.csv",
                   bucket = "projet-iquale",
                   multipart=TRUE,
                   show_progress = TRUE,
                   region = "")

# Transformation du format csv en format parquet
arrow::write_parquet(df,"analyse_2009_2021.csv.parquet")

# Lecture du fichier au format parquet dans un dataframe
system.time(
  df2 <- aws.s3::s3read_using(FUN = arrow::read_parquet,
                              object = "analyse_2009_2021.csv.parquet", 
                              bucket = "projet-iquale",
                              opts = list("region" = ""))
)

df2 <- arrow::read_parquet("analyse_2009_2021.csv.parquet")

# Copie du fichier au format parquet sur l'espace de stockage S3
aws.s3::put_object("analyse_2009_2021.csv.parquet", 
                   object = "analyse_2009_2021.csv.parquet",
                   bucket = "projet-iquale",
                   multipart=TRUE,
                   show_progress = TRUE,
                   region = "")

