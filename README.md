# Sistema de Optimización de Oferta y Horarios (TOO Score)

Una plataforma de **Big Data y Machine Learning** diseñada para servir de propuesta para la transfromación de la planeación académica manual en un proceso predictivo y automatizado basado en datos.

### 🔗 Enlaces Rápidos
> **🚀 [Ver Dashboard Deployado (Render)](https://dashboardproject-2ovi.onrender.com/)**
> **Link Datos: https://drive.google.com/drive/folders/19mLHLvi2CqWAkp8h2ZgMn7cDTc_hjI4J?usp=sharing **



---

## 📖 Descripción del Proyecto

Este proyecto busca platear una mejora en la generación de horarios escolares mediante una arquitectura **Serverless en AWS**. El sistema ingiere datos históricos y de streaming para calcular el **TOO Score (Tasa de Optimización Operacional)**, una métrica que garantiza:
* Minimización de conflictos de traslape.
* Optimización de recursos (aulas y profesores).
* Predicción de demanda estudiantil.

## ☁️ Arquitectura y Tecnologías (AWS)

El proyecto utiliza una arquitectura **Cloud-Native** desacoplada:

* 🧠 **Procesamiento (ETL & ML):** AWS Glue (Apache Spark) y AWS Step Functions para la orquestación.
* ⚡ **Ingesta (Streaming):** Amazon Kinesis Data Firehose.
* 🗄️ **Almacenamiento:**
    * **Data Lake:** Amazon S3 (Capas Raw/Processed).
    * **NoSQL:** Amazon DynamoDB (Resultados de baja latencia).
* 🚀 **Backend / API:** AWS Lambda y API Gateway.
* 📊 **Frontend:** Dashboard interactivo para visualización de KPIs y manejo de restricciones.

## ⚙️ Funcionamiento General

1.  **Simulación:** Se generan 10 años de historia académica y flujo de asistencia.
2.  **Pipeline:** Step Functions dispara los Jobs de Glue para limpieza y Feature Engineering.
3.  **Modelo:** Un modelo de ML (Gradient Boosted Trees) predice el éxito del horario y asigna el **TOO Score**.
4.  **Visualización:** Los resultados óptimos se exponen vía API Gateway al Dashboard administrativo.

---
*Proyecto Final - Big Data - Instituto Tecnológico de Celaya*
