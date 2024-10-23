# Importación de librerías necesarias
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3_BeneficiariosMásFamilias').getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/xfif-myr2.csv'

# Lee el archivo .csv
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(file_path)

# Imprime el esquema
df.printSchema()

# Muestra las primeras filas del DataFrame
df.show()

# Eliminar duplicados
df = df.dropDuplicates()

# Manejar valores nulos, puedes decidir eliminarlos o imputarlos
df = df.na.fill({
    'Genero': 'No Especificado',  # Ejemplo de imputación
    'RangoEdad': 'No Especificado',
    'EstadoBeneficiario': 'Desconocido',
    'NombreDepartamentoAtencion': 'Desconocido'
})

# Contar los beneficiarios por género
genero_counts = df.groupBy('Genero').count().orderBy('count', ascending=False)

# Mostrar resultados
genero_counts.show()

# Guardar resultados en HDFS
genero_counts.write.csv('hdfs://localhost:9000/Tarea3/genero_counts.csv', header=True, mode='overwrite')

# Contar beneficiarios por rango de edad
edad_counts = df.groupBy('RangoEdad').count().orderBy('count', ascending=False)

# Mostrar resultados
edad_counts.show()

# Guardar resultados en HDFS
edad_counts.write.csv('hdfs://localhost:9000/Tarea3/edad_counts.csv', header=True, mode='overwrite')

# Contar beneficiarios activos e inactivos
estado_counts = df.groupBy('EstadoBeneficiario').count().orderBy('count', ascending=False)

# Mostrar resultados
estado_counts.show()

# Guardar resultados en HDFS
estado_counts.write.csv('hdfs://localhost:9000/Tarea3/estado_counts.csv', header=True, mode='overwrite')

# Contar beneficiarios por departamento
dept_counts = df.groupBy('NombreDepartamentoAtencion').count().orderBy('count', ascending=False)

# Mostrar resultados
dept_counts.show()

# Guardar resultados en HDFS
dept_counts.write.csv('hdfs://localhost:9000/Tarea3/dept_counts.csv', header=True, mode='overwrite')

