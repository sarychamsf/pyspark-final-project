# Caso práctico Apache Spark

## Enunciado

Una compañía de telecomunicaciones quiere obtener insights de la muestra de 30 clientes obtenida al final de este mes y con información también del mes anterior. Los analistas de datos tendrán que agregar esta información, que se proporciona para responder a 10 cuestiones que ha planteado la directiva.

Las tablas de input son las mismas que se utilizaron en el ejercicio guiado de Spark:

* **df_clientes**: Muestra información de cada cliente (id, nombre, edad, ciudad, país).
* **df_facturas_mes_ant**: Muestra la factura del mes anterior junto con el plan que tenían contratado para cada cliente. Un mismo cliente puede tener contratadas dos ofertas con la compañía, en cuyo caso habrá dos filas (de ahora en adelante, registros) con el mismo “id_cliente” y distinto “id_oferta”.
* **df_facturas_mes_actual**: Es una tabla equivalente a la anterior, pero muestra la información actualizada a último día del mes actual (agosto de 2020).
* **df_consumos_diarios**: Muestra para cada cliente, y diariamente, los datos móviles consumidos, el tráfico, SMS enviados y minutos de llamadas a fijos y móviles durante el mes en curso.
* **df_ofertas**: Muestra un catálogo de los distintos paquetes ofrecidos por la empresa, junto con una descripción y sus precios.

## Se pide

* Documento con capturas de la PySpark shell con los comandos y dataframes resultantes que resuelven cada una de las 10 cuestiones. Como anexo, añadir captura del lanzamiento por terminal de Linux del spark2-submit con el script “.py” que se genere.
* Script “.py”, sobre el que se ejecuta el comando “spark2-submit”.
* HTML exportado de Databricks con las cuestiones resueltas.
