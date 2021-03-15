# Pasos Especiales de Bamboo

Para ilustrar algunas funcionalidades avanzadas de Bamboo tomaremos un segundo caso de estudio con el dataset de Financial Crimes Enforcement Network (FINCEN) y nos enfocaremos en algunos aspectos prácticos:

* Escribir Pipelines autónomas capaces de descargar desde fuentes externas, transformar e ingestar sin crear archivos intermedios a menos que sean absolutamente necesarios.
* Escribir un Pipeline en el entorno de OEC.
* Utilizar pasos especiales de Bamboo para descargar archivos y descomprimirlos: `DownloadStep` y `UnzipStep`.
* Controlar la información que se envía de un paso a otro en Bamboo, utilizando `prev` correctamente.
* Manipular fechas con pandas.


## 1. Análisis Exploratorio de Datos

Los datos para la ETL se encuentran en: https://media.icij.org/uploads/2020/09/download_data_fincen_files.zip, dentro del archivo `.zip` hay dos archivos, sólo nos interesa `download_transactions_map.csv`. Las primeras líneas del archivo nos muestran su estructura:

|id    |icij_sar_id|filer_org_name_id               |filer_org_name                   |begin_date  |end_date    |originator_bank_id     |originator_bank        |originator_bank_country|originator_iso|beneficiary_bank_id                               |beneficiary_bank             |beneficiary_bank_country|beneficiary_iso|number_transactions|amount_transactions|
|------|-----------|--------------------------------|---------------------------------|------------|------------|-----------------------|-----------------------|-----------------------|--------------|--------------------------------------------------|-----------------------------|------------------------|---------------|-------------------|-------------------|
|223254|3297       |the-bank-of-new-york-mellon-corp|The Bank of New York Mellon Corp.|Mar 25, 2015|Sep 25, 2015|cimb-bank-berhad       |CIMB Bank Berhad       |Singapore              |SGP           |barclays-bank-plc-london-england-gbr              |Barclays Bank Plc            |United Kingdom          |GBR            |68                 |5.689852347E7      |
|223255|3297       |the-bank-of-new-york-mellon-corp|The Bank of New York Mellon Corp.|Mar 30, 2015|Sep 25, 2015|cimb-bank-berhad       |CIMB Bank Berhad       |Singapore              |SGP           |barclays-bank-plc-london-england-gbr              |Barclays Bank Plc            |United Kingdom          |GBR            |118                |1.1623836125E8     |
|223258|2924       |the-bank-of-new-york-mellon-corp|The Bank of New York Mellon Corp.|Jul 5, 2012 |Jul 5, 2012 |barclays-bank-plc-ho-uk|Barclays Bank Plc Ho UK|United Kingdom         |GBR           |skandinaviska-enskilda-banken-stockholm-sweden-swe|Skandinaviska Enskilda Banken|Sweden                  |SWE            |                   |5000               |
|223259|2924       |the-bank-of-new-york-mellon-corp|The Bank of New York Mellon Corp.|Jun 20, 2012|Jun 20, 2012|barclays-bank-plc-ho-uk|Barclays Bank Plc Ho UK|United Kingdom         |GBR           |skandinaviska-enskilda-banken-stockholm-sweden-swe|Skandinaviska Enskilda Banken|Sweden                  |SWE            |                   |9990               |
|223260|2924       |the-bank-of-new-york-mellon-corp|The Bank of New York Mellon Corp.|May 31, 2012|May 31, 2012|barclays-bank-plc-ho-uk|Barclays Bank Plc Ho UK|United Kingdom         |GBR           |skandinaviska-enskilda-banken-stockholm-sweden-swe|Skandinaviska Enskilda Banken|Sweden                  |SWE            |                   |12000              |

A continuación describimos las columnas:

* **id**: ID de la transacción.
* **icij_sar_id**: ID de la conexión registrada entre los bancos por ICIJ.
* **filer_org_name_id** y **filer_org_name**: ID de la organización que reportó la actividad como sospechosa y su nombre. 👨🏻‍🚀🔪
* **begin_date** y **end_date**: Fechas de inicio y final de la transacción.
* **originator_bank_id**, **originator_bank**, **originator_bank_country** y **originator_iso**: ID del banco de origen de la transacción, su nombre, país y código ISO del país. 
* **beneficiary_bank_id**, **beneficiary_bank**, **beneficiary_bank_country** y **beneficiary_iso**: ID, nombre, país y código ISO del país para el banco beneficiario de la transacción. 
* **number_transactions**: Número de transacciones realizadas en el período, cuando este valor no aparezca, llenaremos con `1`.
* **amount_transactions**: Monto total de las transacciones realizadas en el período.

Las transformaciones se realizarán directamente en el Pipeline de Bamboo, por lo tanto omitiremos ese paso, el modelo relacional no será escrito, sólo necesitamos tener en cuenta el resultado final que esperamos al ingestar.


## 2. Resultado Esperado

Primero tomamos en cuenta la estructura de los archivos para determinar las dimensiones y medidas:

* Podemos observar en el conjunto de datos elementos únicos de cada transacción que corresponden a dimensiones: ID de transacción, ID de conexión, Banco que reporta, fecha de inicio y final, Banco de origen y Banco beneficiario.
* Existen dos medidas en cada transacción: Número y Monto Total. Sería interesante tener otra medida que entregue el número de días que duró la transacción, podemos crearla con pandas.
* Dado que muchas columnas dependen de la información de las organizaciones y bancos, crearemos una dimensión para cada uno de ellos. Además de una dimensión para trabajar con las fechas.

Aquí están los aspectos generales de lo que deberíamos lograr con el Pipeline de Bamboo, tomando en cuenta los puntos anteriores:

* Descargar el archivo comprimido (`.zip`) que contiene el archivo de fuente utilizando `DownloadStep`.
* Descomprimir el archivo con `UnzipStep` y sólo utilizar `download_transaction_map.csv`.
* Crear un paso `DateStep`que retorne un DataFrame con columnas: `date_id, year, quarter, month, month_name, day, day_name`. 
* Crear un paso `OrganizationsStep` que retorne un DataFrame con las siguientes columnas: `filer_id, filer_name_id, filer_name`.
* Crear un paso `BanksStep` que retorne un DataFrame con las siguientes columnas: `bank_id, bank_name_id, bank_name, bank_country, bank_iso`.
* Crear un paso `FactStep` para retornar un DataFrame con columnas: `transaction_id, connection_id, filer_id, begin_date, end_date, originator_id, beneficiary_id, transactions, total_amount, time_range`.
* Crear un paso `LoadStep` para cada una de las dimensiones y la Fact Table.
