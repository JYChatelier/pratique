library(sparklyr)
library(dplyr)

#spark_install(version = "3.3")
#sc <- spark_connect(master = "local", version = '3.3')

#Ouverture de la session Spark déjà configurée
sc <- spark_connect(master = "k8s://https://kubernetes.default.svc:443")

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
                    partition_by = c("etatAdministratifEtablissement"))


spark_naiades_csv <- spark_read_csv(sc,
                                    name = "naiades_csv",
                                    path = "s3a://projet-iquale/analyse_2009_2021.csv")
spark_naiades_parquet <- spark_read_parquet(sc,
                                            name = "naiades_parquet",
                                            path = "s3a://projet-iquale/analyse_2009_2021.csv.parquet")

spark_disconnect_all()
