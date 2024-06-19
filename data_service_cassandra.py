import logging
import pyspark
from pyspark.sql import Window
from py4j.protocol import Py4JJavaError
from pyspark.errors import PySparkException, PythonException, PySparkAttributeError, PySparkValueError, \
    PySparkTypeError, AnalysisException
from pyspark.sql.functions import col, current_timestamp, regexp_replace, when, max, to_timestamp
from pyspark.sql.types import IntegerType
from validation import check_raw_data_count, check_irt_3pl_count, check_irt_ability_count
from config import CASSANDRA_INFO, SPARK_INFO
from pyspark.sql import functions as F

"""
spark 를 연동 하여 cassandra 에 있는 data 를 가져 오는 함수.
:param spark: SparkSession
:return: pyspark.sql.DataFrame -> DataFrame[stu_id: string,
                                            test_id: int,
                                            ques_id: string,
                                            ques_no: int,
                                            crt_yn: int,
                                            attempt: int]

                                   [Row(stu_id='2d283a3e85', test_id=1, ques_id='2972', ques_no=4, crt_yn=1, attempt=1),
                                    Row(stu_id='2d283a3e85', test_id=1, ques_id='2972', ques_no=4, crt_yn=0, attempt=2)]
"""


def get_raw_data(spark, error_code) -> pyspark.sql.DataFrame and dict:
    raw_df = ""

    try:
        # cassandra config
        raw_data_table = CASSANDRA_INFO["RAW_DATA_TABLE"]  # 문항 데이터 테이블
        keyspace = CASSANDRA_INFO["KEYSPACE"]
        read_format = SPARK_INFO["READ_FORMAT"]  # format 형식

        # select data
        raw_df = spark.createDataFrame(spark.read.format(read_format)
                                       .option("keyspace", keyspace)
                                       .option("table", raw_data_table).load()
                                       .select("stu_id",  # 학생 ID
                                               "test_id",  # 시험 ID
                                               "ques_id",  # 문항 ID
                                               "ques_no",  # 문항 번호
                                               "crt_yn",  # 정오
                                               "attempt").collect())  # 시험 응시 횟수

        raw_df = raw_df.withColumn("crt_yn",
                                   when(raw_df.crt_yn == "Y",
                                        regexp_replace(raw_df.crt_yn, "Y", "1").cast(IntegerType()))
                                   .when(raw_df.crt_yn == "N",
                                         regexp_replace(raw_df.crt_yn, "N", "0").cast(IntegerType()))) \
            .withColumn("ques_no", col("ques_no").cast(IntegerType())) \
            .withColumn("test_id", col("test_id").cast(IntegerType())) \
            .withColumn("attempt", col("attempt").cast(IntegerType()))

        # check_raw_data_count
        check_raw_data_count(raw_df)
    except IndexError:
        error_code["ERROR"] = "[IndexError] check config.py host"
        logging.error(error_code["ERROR"])
    except KeyError:
        error_code["ERROR"] = "[KeyError] check config.py keys or def connect_spark()"
        logging.error(error_code["ERROR"])
    except NameError:
        error_code["ERROR"] = "[NameError] check def get_raw_data()"
        logging.error(error_code["ERROR"])
    except PySparkAttributeError:
        error_code["ERROR"] = "[PySparkAttributeError] Attribute error"
        logging.error(error_code["ERROR"])
    except AttributeError:
        error_code["ERROR"] = "[AttributeError] check def get_raw_data()"
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
        return raw_df, error_code


"""
spark 를 연동 하여 cassandra 에 있는 data 를 가져 오는 함수.
:param spark: SparkSession
:return: pyspark.sql.DataFrame -> raw_shape: DataFrame[test_id: int,                        # 시험 ID
                                                       ques_id: string,                     # 문항 ID
                                                       difficulty: double,                  # 난이도
                                                       discrimination: double,              # 변별도
                                                       guessing: double]                    # 추측도

                                                  [Row(test_id=40,                          
                                                       ques_id='6142',                      
                                                       difficulty=-0.8744813722702853,     
                                                       discrimination=1.1762422661132808,  
                                                       guessing=4.640049290474223e-08),     
                                                   Row(test_id=17,
                                                       ques_id='1589',
                                                       difficulty=0.7387156428576748,
                                                       discrimination=0.25,
                                                       guessing=0.33)]
"""


def get_3pl_data(spark, error_code) -> pyspark.sql.DataFrame and dict:
    irt_3pl_df = ""

    try:
        # cassandra config
        irt_3pl_data_table = CASSANDRA_INFO["IRT_3PL"]
        keyspace = CASSANDRA_INFO["KEYSPACE"]
        read_format = SPARK_INFO["READ_FORMAT"]  # format 형식

        # select data
        irt_3pl_df = spark.read.format(read_format) \
            .option("keyspace", keyspace).option("table", irt_3pl_data_table).load() \
            .select("test_id",  # 시험 ID
                    "ques_id",  # 문항 ID
                    "difficulty",  # 난이도
                    "discrimination",  # 변별도
                    "guessing",  # 추측도
                    max(to_timestamp("reg_dttm"))
                    .over(Window.partitionBy("test_id", "ques_id"))) \
            .withColumnRenamed("max(to_timestamp(reg_dttm)) OVER (PARTITION BY test_id,"
                               " ques_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)", "reg_dttm")

        # check_raw_data_count
        check_irt_3pl_count(irt_3pl_df)
    except IndexError:
        error_code["ERROR"] = "[IndexError] check config.py host"
        logging.error(error_code["ERROR"])
    except KeyError:
        error_code["ERROR"] = "[KeyError] check config.py keys or def connect_spark()"
        logging.error(error_code["ERROR"])
    except NameError:
        error_code["ERROR"] = "[NameError] check def get_raw_data()"
        logging.error(error_code["ERROR"])
    except PySparkAttributeError:
        error_code["ERROR"] = "[PySparkAttributeError] Attribute error"
        logging.error(error_code["ERROR"])
    except AttributeError:
        error_code["ERROR"] = "[AttributeError] check def get_raw_data()"
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
        return irt_3pl_df, error_code


"""
spark 를 연동 하여 cassandra 에 있는 data 를 가져 오는 함수.
:param spark: SparkSession
:return: pyspark.sql.DataFrame -> raw_shape: DataFrame[[stu_id: string,                     # 학생 ID
                                                        test_id: bigint,                    # 시험 ID
                                                        ability: double]                    # 능력

                                                   [Row(stu_id='1025c769a',
                                                        test_id=76,
                                                        ability=0.513884897312539),
                                                    Row(stu_id='5ba3152ff4',
                                                        test_id=41,
                                                        ability=-0.43475359735281044)]
"""


def get_ability_data(spark, error_code) -> pyspark.sql.DataFrame and dict:
    irt_ability_df = ""

    try:
        # cassandra config
        irt_ability_data_table = CASSANDRA_INFO["IRT_ABILITY"]
        keyspace = CASSANDRA_INFO["KEYSPACE"]
        read_format = SPARK_INFO["READ_FORMAT"]  # format 형식

        # select data
        irt_ability_df = spark.createDataFrame(spark.read.format(read_format)
                                               .option("keyspace", keyspace)
                                               .option("table", irt_ability_data_table).load()
                                               .select("stu_id",  # 학생 ID
                                                       "test_id",  # 시험 ID
                                                       "ability").collect())  # 학생 능력

        # check_ability_data_count
        check_irt_ability_count(irt_ability_df)
    except IndexError:
        error_code["ERROR"] = "[IndexError] check config.py host"
        logging.error(error_code["ERROR"])
    except KeyError:
        error_code["ERROR"] = "[KeyError] check config.py keys or def connect_spark()"
        logging.error(error_code["ERROR"])
    except NameError:
        error_code["ERROR"] = "[NameError] check def get_raw_data()"
        logging.error(error_code["ERROR"])
    except PySparkAttributeError:
        error_code["ERROR"] = "[PySparkAttributeError] Attribute error"
        logging.error(error_code["ERROR"])
    except AttributeError:
        error_code["ERROR"] = "[AttributeError] check def get_raw_data()"
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
        return irt_ability_df, error_code


"""
cassandra 에 3pl_result_data 를 삽입 하는 함수. 
"""


def put_3pl_result_data(dataframe, error_code) -> dict:
    try:
        keyspace = CASSANDRA_INFO["KEYSPACE"]
        irt_3pl_table = CASSANDRA_INFO["IRT_3PL"]
        dataframe.withColumn("uuid", F.expr("uuid()")) \
            .withColumn("reg_dttm", current_timestamp()) \
            .write.format("org.apache.spark.sql.cassandra") \
            .option("table", irt_3pl_table).option("keyspace", keyspace).mode("append") \
            .options(consistencyLevel="EACH_QUORUM").save()

    except AnalysisException as analysis:
        error_code["ERROR"] = f"[AnalysisException] : {analysis.message}"
    except PySparkAttributeError:
        error_code["ERROR"] = "[PySparkAttributeError] Attribute error"
    except PySparkValueError:
        error_code["ERROR"] = "[PySparkValueError] Attribute error"
    except PySparkTypeError:
        error_code["ERROR"] = "[PySparkTypeError] Attribute error"
    except PythonException:
        error_code["ERROR"] = "[PythonException] Python error"
    except PySparkException as spark_E:
        error_code["ERROR"] = f"[PySparkException] error : {spark_E}"
    except ValueError:
        error_code["ERROR"] = "[ValueError] check def put_result_data()"
    except IndexError:
        error_code["ERROR"] = "[IndexError] check config.py host"
    except KeyError:
        error_code["ERROR"] = "[KeyError] check config.py keys or def connect_spark()"
    except AttributeError:
        error_code["ERROR"] = "[AttributeError] check def put_result_data()"
    except NameError:
        error_code["ERROR"] = "[NameError] check def put_result_data()"
    except Py4JJavaError as py:
        error_code["ERROR"] = "[Py4JJavaError] PySpark와 Java 간의 연결 문제 -> ValueError 가능성 있음 + " + py.errmsg
    except Exception as err:
        error_code["ERROR"] = f"Unexpected {err=}, {type(err)=}"
    finally:
        return error_code


"""
cassandra 에 ability_result_data 를 삽입 하는 함수. 
"""


def put_ability_result_data(dataframe, error_code) -> dict:
    try:
        keyspace = CASSANDRA_INFO["KEYSPACE"]
        irt_ability_table = CASSANDRA_INFO["IRT_ABILITY"]
        dataframe.withColumn("uuid", F.expr("uuid()")) \
            .withColumn("reg_dttm", current_timestamp()) \
            .write.format("org.apache.spark.sql.cassandra") \
            .option("table", irt_ability_table).option("keyspace", keyspace).mode("append") \
            .options(consistencyLevel="EACH_QUORUM").save()

    except AnalysisException as analysis:
        error_code["ERROR"] = f"[AnalysisException] : {analysis.message}"
    except PySparkAttributeError:
        error_code["ERROR"] = "[PySparkAttributeError] Attribute error"
    except PySparkValueError:
        error_code["ERROR"] = "[PySparkValueError] Attribute error"
    except PySparkTypeError:
        error_code["ERROR"] = "[PySparkTypeError] Attribute error"
    except PythonException:
        error_code["ERROR"] = "[PythonException] Python error"
    except PySparkException as spark_E:
        error_code["ERROR"] = f"[PySparkException] error : {spark_E}"
    except ValueError:
        error_code["ERROR"] = "[ValueError] check def put_result_data()"
    except IndexError:
        error_code["ERROR"] = "[IndexError] check config.py host"
    except KeyError:
        error_code["ERROR"] = "[KeyError] check config.py keys or def connect_spark()"
    except AttributeError:
        error_code["ERROR"] = "[AttributeError] check def put_result_data()"
    except NameError:
        error_code["ERROR"] = "[NameError] check def put_result_data()"
    except Py4JJavaError as py:
        error_code["ERROR"] = "[Py4JJavaError] PySpark와 Java 간의 연결 문제 -> ValueError 가능성 있음 + " + py.errmsg
    except Exception as err:
        error_code["ERROR"] = f"Unexpected {err=}, {type(err)=}"
    finally:
        return error_code


"""
cassandra 에 probability_result_data 를 삽입 하는 함수. 
"""


def put_probability_result_data(dataframe, error_code) -> dict:
    try:
        keyspace = CASSANDRA_INFO["KEYSPACE"]
        irt_probability_table = CASSANDRA_INFO["IRT_PROBABILITY"]
        dataframe.withColumn("uuid", F.expr("uuid()")) \
            .withColumn("reg_dttm", current_timestamp()) \
            .write.format("org.apache.spark.sql.cassandra") \
            .option("table", irt_probability_table).option("keyspace", keyspace).mode("append") \
            .options(consistencyLevel="EACH_QUORUM").save()

    except AnalysisException as analysis:
        error_code["ERROR"] = f"[AnalysisException] : {analysis.message}"
    except PySparkAttributeError:
        error_code["ERROR"] = "[PySparkAttributeError] Attribute error"
    except PySparkValueError:
        error_code["ERROR"] = "[PySparkValueError] Attribute error"
    except PySparkTypeError:
        error_code["ERROR"] = "[PySparkTypeError] Attribute error"
    except PythonException:
        error_code["ERROR"] = "[PythonException] Python error"
    except PySparkException as spark_E:
        error_code["ERROR"] = f"[PySparkException] error : {spark_E}"
    except ValueError:
        error_code["ERROR"] = "[ValueError] check def put_result_data()"
    except IndexError:
        error_code["ERROR"] = "[IndexError] check config.py host"
    except KeyError:
        error_code["ERROR"] = "[KeyError] check config.py keys or def connect_spark()"
    except AttributeError:
        error_code["ERROR"] = "[AttributeError] check def put_result_data()"
    except NameError:
        error_code["ERROR"] = "[NameError] check def put_result_data()"
    except Py4JJavaError as py:
        error_code["ERROR"] = "[Py4JJavaError] PySpark와 Java 간의 연결 문제 -> ValueError 가능성 있음 + " + py.errmsg
    except Exception as err:
        error_code["ERROR"] = f"Unexpected {err=}, {type(err)=}"
    finally:
        return error_code
