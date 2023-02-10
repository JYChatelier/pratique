library(DBI)
library(dplyr)
# Ouverture de la connexion à la base de données
con <- dbConnect(  RPostgres::Postgres(),
                   dbname = "dbiquale",
                   host = "postgresql-994591",
                   port = 5432,
                   user = "postgres",
                   password = "pfapevsplxmabtf68erd")
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







