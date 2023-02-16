
# Décompression du fichier zip sur l'espace de stockage S3
df <- aws.s3::s3read_using(FUN = utils::unzip,
                           object = "/analyse_2009_2021.zip",
                           bucket = "projet-iquale",    
                           opts = list("region" = ""))
# à ce stade un fichier csv est créé sur l'espace local de la session RStudio
# dans le working directory
df <- read.csv("analyse_2009_2021.csv")

# Lecture du fichier csv nouvellement créé dans un dataframe
df <- aws.s3::s3read_using(FUN = read.csv,
                           sep=";",
                           header=TRUE,
                           object = "analyse_2009_2021.csv", 
                           bucket = "projet-iquale",
                           opts = list("region" = ""))

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
df2 <- arrow::read_parquet("analyse_2009_2021.csv.parquet")

# Copie du fichier au format parquet sur l'espace de stockage S3
aws.s3::put_object("analyse_2009_2021.csv.parquet", 
                   object = "analyse_2009_2021.csv.parquet",
                   bucket = "projet-iquale",
                   multipart=TRUE,
                   show_progress = TRUE,
                   region = "")

