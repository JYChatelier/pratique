library(DBI)
library(dplyr)

# Récupération des variables d'environnement
env_host     <- Sys.getenv('ENV_PG_HOST')
env_dbname   <- Sys.getenv('ENV_PG_DBNAME')
env_port     <- Sys.getenv('ENV_PG_PORT')
env_user     <- Sys.getenv('ENV_PG_USER')
env_password <- Sys.getenv('ENV_PG_PASSWD')

# Ouverture de la connexion à la base de données
con <- dbConnect(  RPostgres::Postgres(),
                   dbname = env_dbname,
                   host = env_host,
                   port = env_port,
                   user = env_user,
                   password = env_password)
# Listing des tables du schéma public
dbListTables(con)

# On se place dans le schéma radon et on liste les tables correspondantes
dbExecute(con, "set search_path to radon;")
dbListTables(con)

# Comptage de ligne sur une des tables du schéma radon
result <- dbGetQuery(con, "SELECT count(*) FROM radon.radon_metropole")
str(result)

# Utilisation des tables de dplyr pour explorer les données 
radon_db <- tbl(con, "radon_metropole")
radon_db %>%
  head()

# Fermeture de la connexion à la base
dbDisconnect(con)







