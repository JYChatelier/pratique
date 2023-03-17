library(SparkR)


#Ouverture de la session Spark déjà configurée
sparkR.session(master = "k8s://https://kubernetes.default.svc:443")

#Lecture du fichier Sirene au format CSV
spark_sirene_csv <- read.df("s3a://projet-spark-lab/diffusion/formation/data/sirene/sirene.csv", "csv", header = "true", inferSchema = "true")
createOrReplaceTempView(spark_sirene_csv, "sparkr_sirene_csv")
sirene_csv_count_sql <- sql("SELECT count(*) FROM sparkr_sirene_csv")
head(sirene_csv_count_sql)

#Lecture du fichier Sirene au format parquet
spark_sirene_parquet <- read.df("s3a://projet-spark-lab/diffusion/formation/data/sirene.parquet", "parquet")
createOrReplaceTempView(spark_sirene_parquet, "sparkr_sirene_parquet")
sirene_parquet_count_sql <- sql("SELECT count(*) FROM sparkr_sirene_parquet")
head(sirene_parquet_count_sql)

#Lecture du fichier Naiades au format csv
spark_naiades_csv <- read.df("s3a://projet-iquale/analyse_2009_2021.csv", "csv", header = "true", inferSchema = "true")
createOrReplaceTempView(spark_naiades_csv, "sparkr_naiades_csv")
naiades_csv_count_sql <- sql("SELECT count(*) FROM sparkr_naiades_csv")
head(naiades_csv_count_sql)

#Lecture du fichier Naiades au format parquet
spark_naiades_parquet <- read.df("s3a://projet-iquale/analyse_2009_2021.csv.parquet", "parquet")
createOrReplaceTempView(spark_naiades_parquet, "sparkr_naiades_parquet")
naiades_parquet_count_sql <- sql("SELECT count(*) FROM sparkr_naiades_parquet")
head(naiades_parquet_count_sql)


write.df(spark_sirene_parquet,
         path="s3a://jchatelier/sirene.parquet",
         #"parquet",
         mode = "overwrite",
         partitionBy = "etatAdministratifEtablissement")
df <- as.DataFrame(spark_sirene_parquet)
head(summarize(groupBy(
  spark_sirene_parquet, spark_sirene_parquet$etatAdministratifEtablissement), 
               count = n(spark_sirene_parquet$etatAdministratifEtablissement)))


spark_sirene_parquet.group_by(activitePrincipaleEtablissement)
