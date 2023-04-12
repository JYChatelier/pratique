library(DBI)
library(duckdb)
library(arrow)
library(dplyr)

#Ouverture d'une connexion à une base de données de type DuckDB
con <- dbConnect(duckdb::duckdb())

#Requête sur une fichier au format Parquet stocké en local
#Penser à faire un setwd("<répertoire du fichier en local>")
dbGetQuery(con, "SELECT count(*) FROM read_parquet('Export stations - 50626866.parquet')")
#On peut utiliser la syntaxe dplyr directement sur le fichier
tbl(con, 'Export stations - 50626866.parquet') |>
  collect()
  
#Installation d'un protocole pour gérer le stockage S3
dbSendQuery(con,"INSTALL httpfs")
dbSendQuery(con,"LOAD httpfs")
#Configuration des droits d'accès au stockage S3 depuis la connexion ouverte
dbSendQuery(con,"SET s3_access_key_id='5946UH3WJIJ9S2F2UX2I'")
dbSendQuery(con,"SET s3_secret_access_key='agQu85kqztZRFAzUs0x8G1Qpq4J8jXnfZNYAHE3E'")
dbSendQuery(con,"SET s3_session_token='eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiI1OTQ2VUgzV0pJSjlTMkYyVVgySSIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sImF1ZCI6WyJtaW5pby1kYXRhbm9kZSIsIm9ueXhpYSIsImFjY291bnQiXSwiYXV0aF90aW1lIjoxNjgxMjQ3MzYxLCJhenAiOiJvbnl4aWEiLCJlbWFpbCI6ImplYW4teXZlcy5jaGF0ZWxpZXJAaW5lcmlzLmZyIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImV4cCI6MTY4MTMzMzc2MywiZmFtaWx5X25hbWUiOiJDaGF0ZWxpZXIiLCJnaXZlbl9uYW1lIjoiSmVhbi1ZdmVzIiwiZ3JvdXBzIjpbImlxdWFsZSJdLCJpYXQiOjE2ODEyNDczNjIsImlzcyI6Imh0dHBzOi8vYXV0aC5sYWIuc3NwY2xvdWQuZnIvYXV0aC9yZWFsbXMvc3NwY2xvdWQiLCJqdGkiOiIyNGJkYTM3Yy0zOTBiLTQ1NDQtYjUyMS01Mjk1ZWNiNjgyMzEiLCJsb2NhbGUiOiJmciIsIm5hbWUiOiJKZWFuLVl2ZXMgQ2hhdGVsaWVyIiwibm9uY2UiOiJmNmE5MjAzYi00MjEwLTQ5ZDQtODE5MC1lNTJlMTI2MmQ2NDAiLCJwb2xpY3kiOiJzdHNvbmx5IiwicHJlZmVycmVkX3VzZXJuYW1lIjoiamNoYXRlbGllciIsInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIiwiZGVmYXVsdC1yb2xlcy1zc3BjbG91ZCJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgZ3JvdXBzIGVtYWlsIiwic2Vzc2lvbl9zdGF0ZSI6IjhhMDQwOGYwLTY4Y2UtNDQxZS1hMTYxLTI4MGE5NWI0MjExMyIsInNpZCI6IjhhMDQwOGYwLTY4Y2UtNDQxZS1hMTYxLTI4MGE5NWI0MjExMyIsInN1YiI6IjZkOGUzODc5LWZiNjItNDQ4Ny05ZWJjLTc3YWVjZWUyYWQ1YiIsInR5cCI6IkJlYXJlciJ9.PR9woZOAknO2bEZjWUDhhWb_wy7-g1_8mNiX2E2bdY9tu1mBHtxs56DWBDaCxaTW9KwBS800vkgO4RiJOf_skw'")
dbSendQuery(con,"SET s3_endpoint='minio.lab.sspcloud.fr'")
dbSendQuery(con,"SET s3_url_style = 'path'") # sinon le path n'est pas bien construit

#Requête du nombre de lignes du fichier par année et code paramètre
res_all1 <- dbGetQuery(con, "SELECT year(DatePrel::DATE),CdStationMesureEauxSurface,CdParametre, count(*) FROM read_parquet('s3://projet-iquale/analyse_2009_2021.csv.parquet') GROUP BY (year(DatePrel::DATE),CdStationMesureEauxSurface,CdParametre)")
res_all2 <- dbGetQuery(con, "SELECT year(DatePrel::DATE),CdStationMesureEauxSurface,CdParametre, count(*) FROM read_parquet('s3://projet-iquale/naiades/*/*.parquet')           GROUP BY (year(DatePrel::DATE),CdStationMesureEauxSurface,CdParametre)")
res_201x <- dbGetQuery(con, "SELECT year(DatePrel::DATE),CdStationMesureEauxSurface,CdParametre, count(*) FROM read_parquet('s3://projet-iquale/naiades/year=201*/*.parquet')   GROUP BY (year(DatePrel::DATE),CdStationMesureEauxSurface,CdParametre)")

#Listing des tables pour montrer qu'il n'y a pas eu formellement d'intégration de données dans des tables
dbListTables(con)

#Fermeture de la connexion à la base de données
dbDisconnect(con)
