install.packages("dummies")
library(dummies)

setwd("D:/dataset")

datos.csv <- read.csv("SUMMIT_5k.csv",header=F)
# Ver si ha cargado correctamente el fichero
head(datos.csv)

#Paso 0.Analizar variables---------------------
#Resumen estad�stico de las variables.
s <- summary(datos.csv)
# Guardamos el resultado de las variables.
capture.output(s, file = "resumen.txt") 


#Paso 1. Normalizacion de variables.
# Examinar variabilidad de las variables numericas
sapply(datos.csv[1:28], var)
datos2 <- datos.csv[-c(1,3,5,6,11,12,15)]

summary(datos2)
# Quitamos la 2,4,8,9,10,14,16,17,21,23,24,25,26 Obtenemos solo las variables numericas
datos_numericos <- datos.csv[-c(1,2,3,4,5,6,8,9,10,11,12,14,15,16,17,18,21,23,24,25,26)]
summary(datos_numericos)

datos_no_numericos <- datos.csv[c(2,4,5,8,10,14,16,17,21,23,24,25,26)]
summary(datos_no_numericos)

#Se categorizan las variables
dfdummi <- dummy.data.frame(datos_no_numericos)

#Separamos dummy de las normalizadas.


# Se realiza el conjunto normalizado, pero dentro de aquellas variables que no sean dummies columnas 1 al 6
df_normalizadas = as.data.frame(scale(datos_numericos),center=TRUE,scale=TRUE)
#Cambiamos los NA por cero
normalizadas <- df_normalizadas
normalizadas[is.na(normalizadas)] <- 0



sapply(normalizadas, sd)  # obviamente la desviación es 1 para todas
sapply(normalizadas, mean) # y la media es 0 para todas
# Unimos los valores dummies y la variable de decision
# df_total tienen las variables normalizadas y las dummies por separado 
df_total <- cbind(normalizadas,dfdummi)

#PCA con variables normalizadas
df_total_stand <- prcomp(df_total, center=FALSE, scale=FALSE)
capture.output(df_total_stand, file = "df_total_stand.txt") 

# Conocer la varianza que recoge cada componente
plot(df_total_stand,
     main = "Varianza de cada componente")

# Gráfico útil para ver el codo
screeplot(df_total_stand,
          main = "Varianza de cada componente",
          type="lines",
          col=3)

# Proporción de varianza del total y proporción acumulada
summary(df_total_stand)
# Con las 5 primeras variables obtenemos el 91% de la varianza acumulada. Es decir esas 5 primeras variables dan
# casi toda la informacion destacable.
# Calculo de K por silhouette
library(fpc)
d1 <- df_total_stand$x[,1:5]


pamk.best <- pamk(d1)
cat("n�mero de grupos estimados por la media de silhouette", pamk.best$nc, "\n")
# Comprobamos que le equivoca de lleno la regla del pulgar: k = (n/2)^0.5 = este caso 50 


install.packages("cluster")
install.packages("clue")
install.packages("fpc")
library(cluster)
library(clue)
library(fpc)
#Cambiamos los NA por cero
df_fin <- d1
df_fin[is.na(df_fin)] <- 0


# Kmeans
clus <- kmeans(df_fin, centers=4)
clusplot(df_fin, clus$cluster, color=TRUE, shade=TRUE,labels=4,lines=0)

df_limpias <- normalizadas
df_limpias[is.na(df_limpias)] <- 0
plotcluster(df_limpias, clus$cluster)

