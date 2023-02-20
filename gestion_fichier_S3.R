library("dplyr")

minio <- paws::s3(config = list(
  credentials = list(
    creds = list(
      access_key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
      secret_access_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
      session_token = Sys.getenv("AWS_SESSION_TOKEN")
    )),
  endpoint = paste0("https://", Sys.getenv("AWS_S3_ENDPOINT")),
  region = Sys.getenv("AWS_DEFAULT_REGION")))

minio$list_buckets()

connection <- aws.s3::s3HTTP(object = "Backup/10391.dat.gz",
                             bucket="projet-iquale", 
                             region = "", 
                             key=Sys.getenv("AWS_ACCESS_KEY_ID"),
                             secret = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
                             session_token = Sys.getenv("AWS_SESSION_TOKEN"))



obj <- aws.s3::get_object(object = "Backup/10391.dat.gz",
                      bucket = "projet-iquale",
                      region = "",
                      key=Sys.getenv("AWS_ACCESS_KEY_ID"),
                      secret = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
                      session_token = Sys.getenv("AWS_SESSION_TOKEN"))

df <- data.frame(data.table::rbindlist(aws.s3::get_bucket(bucket = "projet-iquale",
                                               region = "",
                                               key=Sys.getenv("AWS_ACCESS_KEY_ID"),
                                               secret = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
                                               session_token = Sys.getenv("AWS_SESSION_TOKEN"))))


df <- df %>% select(Key,LastModified,ETag,Size)
write.csv2(df,"projet-iquale.csv",row.names = FALSE, fileEncoding = "UTF-8")
