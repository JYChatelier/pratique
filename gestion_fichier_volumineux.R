Sys.setenv("AWS_ACCESS_KEY_ID" = "GIL50BWFM3395OVRMOTM",
           "AWS_SECRET_ACCESS_KEY" = "qbwPJ176qWLZzH5Gxg1AgrXiHG+0p1nqqJxVMczN",
           "AWS_DEFAULT_REGION" = "us-east-1",
           "AWS_SESSION_TOKEN" = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJHSUw1MEJXRk0zMzk1T1ZSTU9UTSIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sImF1ZCI6WyJtaW5pby1kYXRhbm9kZSIsIm9ueXhpYSIsImFjY291bnQiXSwiYXV0aF90aW1lIjoxNjc3NzU3Mzk2LCJhenAiOiJvbnl4aWEiLCJlbWFpbCI6ImplYW4teXZlcy5jaGF0ZWxpZXJAaW5lcmlzLmZyIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImV4cCI6MTY3Nzg0MzkzMSwiZmFtaWx5X25hbWUiOiJDaGF0ZWxpZXIiLCJnaXZlbl9uYW1lIjoiSmVhbi1ZdmVzIiwiZ3JvdXBzIjpbImlxdWFsZSJdLCJpYXQiOjE2Nzc3NTczOTYsImlzcyI6Imh0dHBzOi8vYXV0aC5sYWIuc3NwY2xvdWQuZnIvYXV0aC9yZWFsbXMvc3NwY2xvdWQiLCJqdGkiOiIyY2ZiOTI3Ny05ZmM1LTQ0MDQtOTdjZS04MzcxNGIwMTg5NmUiLCJsb2NhbGUiOiJmciIsIm5hbWUiOiJKZWFuLVl2ZXMgQ2hhdGVsaWVyIiwibm9uY2UiOiI4YTM3OTM3NC0yNmIzLTQwMjQtOGY5NC1kOGFlMDI2YjMxNGMiLCJwb2xpY3kiOiJzdHNvbmx5IiwicHJlZmVycmVkX3VzZXJuYW1lIjoiamNoYXRlbGllciIsInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIiwiZGVmYXVsdC1yb2xlcy1zc3BjbG91ZCJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgZ3JvdXBzIGVtYWlsIiwic2Vzc2lvblBvbGljeSI6ImV5SldaWEp6YVc5dUlqb2lNakF4TWkweE1DMHhOeUlzSWxOMFlYUmxiV1Z1ZENJNlczc2lSV1ptWldOMElqb2lRV3hzYjNjaUxDSkJZM1JwYjI0aU9sc2ljek02S2lKZExDSlNaWE52ZFhKalpTSTZXeUpoY200NllYZHpPbk16T2pvNmNISnZhbVYwTFdseGRXRnNaU0lzSW1GeWJqcGhkM002Y3pNNk9qcHdjbTlxWlhRdGFYRjFZV3hsTHlvaVhYMHNleUpGWm1abFkzUWlPaUpCYkd4dmR5SXNJa0ZqZEdsdmJpSTZXeUp6TXpwTWFYTjBRblZqYTJWMElsMHNJbEpsYzI5MWNtTmxJanBiSW1GeWJqcGhkM002Y3pNNk9qb3FJbDBzSWtOdmJtUnBkR2x2YmlJNmV5SlRkSEpwYm1kTWFXdGxJanA3SW5Nek9uQnlaV1pwZUNJNkltUnBabVoxYzJsdmJpOHFJbjE5ZlN4N0lrVm1abVZqZENJNklrRnNiRzkzSWl3aVFXTjBhVzl1SWpwYkluTXpPa2RsZEU5aWFtVmpkQ0pkTENKU1pYTnZkWEpqWlNJNld5SmhjbTQ2WVhkek9uTXpPam82S2k5a2FXWm1kWE5wYjI0dktpSmRmVjE5Iiwic2Vzc2lvbl9zdGF0ZSI6IjE4NDYwNmQzLTAwODgtNDM2MS04MTBmLTM0YTUxNjUxMzcxZSIsInNpZCI6IjE4NDYwNmQzLTAwODgtNDM2MS04MTBmLTM0YTUxNjUxMzcxZSIsInN1YiI6IjZkOGUzODc5LWZiNjItNDQ4Ny05ZWJjLTc3YWVjZWUyYWQ1YiIsInR5cCI6IkJlYXJlciJ9.MXiEsFXwxBLLkxOIQyE_fKoRv2d5oxPmmKB9Y31i7si6rCEFCfkbhF2gnJYtS2c4nTY2L2cgCcVXfSvwnAT-tQ",
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

