# Spexs.ai Data engineering challenge

## 1. Definicion de la arquitectura
Para la arquitectura, definiremos 2 diagramas: el primero seria el diseño optimo para implementar en un entorno productivo considerando los detalles propuestos en el challenge. Mientras que el segundo diagrama ilustra la solucion desarrollada a modo de ejemplo para simular el comportamiento de la plataforma de datos pero en un entorno local. Esta decision se debe principalmente al limitante de tiempo para desarrollar una solucion completa en un entorno cloud, como tambien los costos de desarrollo que implicaria desplegar los servicios. 
En la siguiente documentacion se haran las aclaraciones y diferenciaciones pertinentes para diferenciar el desarrollo local del potencial productivo representado en el primer diagrama.

### 1.1 Arquitectura Ideal - Cloud based
Debido a que el ejercicio propuesto en el challenge posee limitada descripcion del caso de uso, las siguientes suposiciones fueron tomadas en cuenta para el desarrollo:

- Las fuentes de datos no estan definidas (tipo, plataforma, cadencia, etc.), por lo que se asume que las fuentes de datos van a ingestar los archivos directamente en un bucket de S3.
- El lenguaje de programacion sera Python.
- El end-point de la plataforma sera una API expuesta para ser consumida por los distintos casos de uso (engineers, front-end, reporting tools, etc.)

![Alt text](./data/images/spexs-data-challenge(4).jpg)

- Ingestion: 
    - Los archivos se depositan en un bucket de S3, donde se pueden utilizar nomenclaturas y reglas especificas para almacenar los datasets dependiendo el origen de la fuente de datos
    - Al arrivar un nuevo archivo, S3 generara un evento que puede ser almacenado en la cola de SQS para posteriormente notificar a Glue de que nuevos archivos estan listos para ser procesados.
    - Glue se encargara de tomar los archivos CSV o en el formato de origen y moverlos hacia la bronze layer del warehouse (RDS - PostgreSql). Durante este proceso inicial, se agregaran campos de metadata necesarios para la auditoria y correcto trackeo de los archivos. Tambien tareas de limpieza y control de calidad leves pueden ejecutarse en esta instancia, asegurando por ejemplo la eliminacion de records vacios.
- Archivado
    - Luego de ser procesados, los archivos origen seran movidos de la landing zone a un archive, donde una lifecycle policy puede ser aplicada para evitar el guardado innecesario de datos en el tiempo, pero a su vez manteniendolos cierto tiempo en caso de que un reprocesamiento sea necesario.
- Procesamiento
    - Glue va a ser la herramienta principal para el movimiento y procesamiento (ETL) de los datos. Garantizando flexbilidad para la orquestracion, correcta auditoria, seguridad y manejo de los datos en las distintas fases del sistema.
    - Glue permite ser disparado de diferentes formas, mediante un evento de S3, un servicio. Esto da flexilidad en el resto de la implementacion, permitiendo varios caminos dependiendo de la necesidad y arquitectura del sistema.
- Almacenado
    - Para el almacenamiento, RDS PostgreSQL es la eleccion, ofreciendo capacidades de replicacion, escalabilidad, seguridad y flexibilidad de desarrollo para con los demas servicios utilizados en la plataforma.
    - La arquitectura Medallion fue la eleccion para el warehouse de datos, con una bronze layer que almacenara todos los datos recibidos desde el bucket de S3. Una silver layer que contara con objetos curados y procesados, por ejemplo combinando multiples fuentes de datos en objetos unicos para agrupar atributos similares. Finalmente la capa gold contiene los objetos desarrollados en base a las direcciones y requerimientos de las unidades de negocio o stakeholders. Aqui reglas especificas, metricas, KPIs y cualquier calculo complejo debe ser ejecutado. Siguiendo el modelo de datos explicado en detalle mas adelante en la documentacion.
- Auditoria
    - En la base de datos, una tabla de auditoria (AUDIT TABLE) sera la encargada de guardar los logs de cada uno de los pasos de la plataforma de datos: transformaciones, origen, destino, timestamps, usuarios, tiempo de computo, cantidad de records modificados, etc. son un ejemplo de los campos que pueden utilizarse en esta tabla que luego puede ser consultada por un reporte especifico para analizar el comportamiento del sistema, como tambien la generacion de alertas en caso de procesos que demoren mas de lo esperado, o fallen constantemente.
    - Una tabla de metadata tambien puede ser utilizada, en caso de cargas incrementales, slow changing dimentions, o schema drift. Almacenando tambien banderas para indicar al sistema comportamientos condicionales.
- Consumo
    - La API puede ser construida dentro de un contenedor de Docker, que luego sera deployado en ECS, y que mediante API Gateway conecta al resto de los servicios de la nube garantizando asi la correcta seguridad y latencia.
- Non functional requirements
    - Autoscaling groups, y spark clusters pueden ser implementados para asegurar correcto funcionamiento sin importar el volumen y velocidad de los datos
    - IAM y Secrets manager se implementan para garantizar correcta seguridad para usuarios de la nube como tambien accesos malintensionados.
    - Cloudwatch y Datadog se implementan para correcto logging, performance measurement, alerting and SLA measurement.


### 1.2 Arquitectura implementada - local based

La arquitectura de alto nivel que representa el sistema desarrollado se puede evaluar en el siguiente diagrama:

![Alt text](./data/images/spexs-data-challenge(3).jpg)

Para el desarrollo local y a la vez garantizar que los evaluadores de este sistema puedan replicar las funcionalidades en cualquier otra PC, se utilizo contenedores de Docker, con un total de 3 aplicaciones: Uno para la base de datos PostgreSQL, uno para el pipeline de ingestion y procesador de datos y un ultimo contenedor para la aplicacion y exposicion de la API.

### 1.3 Modelo de datos
Para el modelo de datos, se abordo una arquitectura del tipo Medallion, compuesta de 3 capas: Bronze, Silver y Gold. Con el siguiente proposito:
- Bronze layer: Datos crudos, con transformaciones minimas como eliminacion de filas nulas, agregado de campos de metadata como INSERTED_ON or EXTRACTED_ON necesarios para la correcta auditoria del sistema
- Silver layer: Datos curados, con limpieza mas profunda de los tipos de datos, nombres de atributos, transformaciones sin agregaciones o transformaciones de alto impacto, agregacion de multiples fuentes de datos en un solo objeto.
- Gold layer: Datos listos para el consumo, agregaciones alineadas a las reglas de negocios, metricas y KPIs, calculos complejos.

![Alt text](./data/images/spexs-data-challenge(1).jpg)

## 2. Estructura del repositorio

    spexs-data-challenge/
    ├── data/               # Almacenar archivos
    │   └── archive/        # Almacenamiento de datos luego del procesamiento
    │   └── images/         # Referido a la documentacion
    │   └── landing/        # Donde los archivos a procesar se almacenan
    ├── docker/             # Dockerfiles específicos
    │   └── postgres/       # Scripts de inicialización de DB
    ├── src/                # Código fuente principal
    │   ├── api/            # Rutas y controladores de FastAPI
            ├── app.py      # Punto de entrada de la aplicacion (API)
    │   ├── core/           # Configuraciones globales y variables de entorno
    │   ├── db/             # Conexión a la DB y modelos
    │   ├── ingestion/      # Lógica de procesamiento de CSV y Bulk Inserts
    │   ├── services/       # Lógica de negocio
            ├── gold_transformers/   # Definicion de los objetos de goldlayer
            ├── silver_transformers/ # Definicion de los objetos de silverlayer
    │   └── utils/          # Helpers
    ├── tests/              # Tests unitarios e integración
    ├── .env                # Variables de entorno
    ├── .gitignore          # Archivos a ignorar por GIT
    ├── docker-compose.yml  # Orquestación local
    ├── Dockerfile          # Especifica caracteristicas del container de Docker
    ├── orchestrator.py     # Punto de entrada del pipeline de procesamiento de datos
    ├── requirements.txt    # Dependencias del proyecto
    └── README.md           # Documentación técnica

## 3. Instrucciones de uso
### 3.1 Requerimientos
- Docker: Versión 20.10+
- Docker Compose: Versión 2.0+

### 3.2 Configuracion inicial
Crea un archivo .env en la raíz del proyecto basado en el siguiente ejemplo:

​`DB_USER=admin
DB_PASSWORD=spexs_secret
DB_NAME=challenge_db
DB_HOST=spexs_db
DB_PORT=5432​​`

### 3.3 Despliegue de la infraestructura
Iniciar PostgreSQL en el puerto 5432 y FastAPI en el puerto 8000.

 ​`docker compose up -d --build​`

### 3.4 Data ingestion y processing test
1. Cargar datos CSV en la carpeta local data/landing/. (El repositorio ya contiene archivos cargados en el formato y estructura del dataset)
2. Ejecutar el pipeline llamando al orquestrador de forma manual para procesar los archivos y aplicar transformaciones de silver/gold y actualizar dimensiones.

    ​`docker compose run --rm pipeline​`
    
    Ver progreso del sistema en la consola luego de ejecutar el comando.

3. Verificar que los archivos han sido movidos automaticamente a data/archive/

### 3.5 Consumo de la API 
Una vez los datos han sido procesados, es posible interactuar con el sistema atraves de los siguientes endpoints:
- Documentación Interactiva (Swagger): ​`http://localhost:8000/docs​`

- Métricas de Región: ​`* GET /stats?region=Turin&week=21&year=2018​`

- Estado de Ingesta (Websocket):

    Conecta a ​`ws://localhost:8000/ws/status​` para recibir la última actualización del pipeline sin necesidad de realizar polling.

En el repositorio dentro de tests/ se encuentra una coleccion de Postman con el metodo principal de la API y precargadas las configuraciones para realizar una consulta.

### 3.6 Monitoreo y auditoria
Para validar el éxito de las operaciones, el sistema cuenta con una tabla de logs dedicada. Puedes consultar el estado de cualquier proceso ejecutando:

`sudo docker exec -it spexs_db psql -U admin -d challenge_db -c "SELECT * FROM audit.ingestion_logs LIMIT 5;"`

## 4. Escalabilidad y decisiones tecnicas
### 4.1. Procesamiento ELT orientado a performance
- Transformaciones en base de datos: las agregaciones pesadas se ejecutan directamente en SQL aprovechando el motor de la DB y minimizando movimiento de datos y latencia entre la app y el server.
- Batch processing: La ingesta de datos utiliza el principio de uso de pedazos o Chunks de datos de 100000 records a la vez, esto garantiza minimo uso de memoria durante la ingestion de archivos masivos como se propone en el desafio.
### 4.2. Arquitectura desacoplada
- La API puede replicarse en muliples contenedores para atender picos de trafico y demanda manteniendo costo bajo en momentos de bajo uso.
- La separacion de los servicios API y PIPELINE en el diseño del docker compose permite aislar fallos en caso de que un error critico de ingesta ocurra, dejando la API de consulta disponible.

### 4.3 Resilencia y idempotencia
El pipeline fue diseñado para poder ejecutarse multiples veces sin generar efectos secundarios como valores duplicados o reprocesamiento masivo de datos existentes.

La logica de las queries de SQL utiliza principios de resolucion para asegurar que si la PK de un registro existe, el sistema puede ignorarlo o actualizarlo.

La ejecucion de las queries se encuentran dentro de bloques try/catch, donde si un error ocurre, un rollback es ejecutado para mantener integridad de los datos.

### 4.4 Auditoria y logs
La implementacion de una estrategia de monitoreo sin polling y logs, garantiza que cada paso del pipeline sea registrado de inicio a fin, indicando estado, cantidad de filas afectadas y posibles mensajes de error. 

El websocket implementado en la API expone el canal de comunicacion necesario para notificar el estado de las tareas criticas de ingesta.

### 4.5 Mejoras futuras para un desarrollo productivo
- Orquestracion: Utilizando Apache Airflow para migrar la funcionalidad del orquestrador de este codigo hacia una herramienta profesional diseñada para dependencias completas, autoretry, y visualizacion.
- Calculo de metricas de negocio de forma mas especifica. Ya que muchos de los calculos (como el de los bounding boxes) fue realizada de forma simplificada para acelerar el desarrollo y disminuir la complejidad del sistema
- Data quality checks: Validaciones automaticas y tests son necesarias en la capa de bronze para rechazar o reparar esquemas incorrectos antes de que los errores se propaguen por el warehouse
- Particionado: Estrategia depende de las necesidades reales de uso y reglas de negocio, pero se necesita en caso de contar con grandes volumenes de datos el uso de fechas o region para que las tablas y motor de SQL funcionen de forma optima.
