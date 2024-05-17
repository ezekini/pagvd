TP para la materia Programación Avanzada para Grandes Volúmenes de datos.
---
Info y archivos relevantes:
```
├── airflow_dags: DAG de airflow con los pasos de procesamiento/lectura/escritura
├── data: archivos *.csv para iniciar la db
├── src_api: Dockerfile, requeriments, funciones y definición de API
└── src_pipeline: código .py para los steps del DAG
```
- Python version `3.10.14`.
- Se recomienda crear un soft link de la carpeta `~/airflow/dags` apuntando a la carpeta del repo para que los cambios se actualicen automáticamente.



