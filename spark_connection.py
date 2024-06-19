from py4j.protocol import Py4JJavaError
from config import CASSANDRA_INFO, SPARK_INFO
from pyspark.sql import SparkSession
from pyspark.errors import PySparkException, PythonException, PySparkAttributeError
import logging

"""
spark 를 연결 하는 함수.
:return: SparkSession and result_code
"""


def connect_spark() -> SparkSession and dict:
    spark = ""
    error_code = {"ERROR": ""}

    try:
        # host = str(CASSANDRA_INFO["HOST"][0])
        host = str(CASSANDRA_INFO["SERVER_HOST"][0])
        port = str(CASSANDRA_INFO["PORT"])
        app_name = "kc_irt_3pl"
        log_level = SPARK_INFO["LOG_LEVEL"]

        spark = SparkSession \
            .builder \
            .appName(app_name) \
            .master("local") \
            .config("spark.jars.packages",
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
            .config("spark.cassandra.connection.host", host) \
            .config("spark.cassandra.connection.port", port) \
            .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
            .getOrCreate()

        spark.sparkContext.setLogLevel(log_level)

    except IndexError:
        error_code["ERROR"] = "[IndexError] check config.py host"
        logging.error(error_code["ERROR"])
    except KeyError:
        error_code["ERROR"] = "[KeyError] check config.py keys or def connect_spark()"
        logging.error(error_code["ERROR"])
    except NameError:
        error_code["ERROR"] = "[NameError] check def connect_spark()"
        logging.error(error_code["ERROR"])
    except PySparkAttributeError:
        error_code["ERROR"] = "[PySparkAttributeError] Attribute error"
        logging.error(error_code["ERROR"])
    except AttributeError:
        error_code["ERROR"] = "[AttributeError] check def connect_spark()"
        logging.error(error_code["ERROR"])
    except Py4JJavaError:
        error_code["ERROR"] = "[Py4JJavaError] PySpark와 Java 간의 연결 문제"
        logging.error(error_code["ERROR"])
    except PythonException:
        error_code["ERROR"] = "[PythonException] Python error"
        logging.error(error_code["ERROR"])
    except PySparkException as spark_E:
        error_code["ERROR"] = f"[PySparkException] error : {spark_E}"
        logging.error(error_code["ERROR"])
    except Exception as err:
        error_code["ERROR"] = f"Unexpected {err=}, {type(err)=}"
        logging.error(error_code["ERROR"])
    finally:
        return spark, error_code

