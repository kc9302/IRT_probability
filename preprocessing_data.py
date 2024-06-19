import pyspark
from py4j.protocol import Py4JJavaError
from pyspark.errors import PySparkException, PythonException, PySparkAttributeError, PySparkValueError, PySparkTypeError
from pyspark.sql import functions as F
from pyspark.sql.functions import exp

"""
spark 환경에서 3pl model 을 분산 처리와 result data 를 전처리 하기 위한 함수.
:param dataframe: pyspark.sql.DataFrame
:return: pyspark.rdd.RDD -> raw_shape: DataFrame[stu_id: string,
                                                 test_id: bigint,
                                                 ability: double]
                                                 
                                            [Row(stu_id='1025c769a',
                                                 test_id=1,
                                                 ability=0.35587038121285636),
                                             Row(stu_id='134a7bbd62',
                                                 test_id=1,
                                                 ability=0.5296043904939152)]
"""


def distributed_processing(irt_3pl_df, irt_ability_df, error_code) -> pyspark.rdd.RDD and dict:
    preprocessed_result_data = ""
    try:
        preprocessed_result_data = irt_3pl_df.join(irt_ability_df, on=["test_id"], how='inner')
        guessing = F.col("guessing")
        discrimination = F.col("discrimination")
        ability = F.col("ability")
        difficulty = F.col("difficulty")
        preprocessed_result_data = preprocessed_result_data.withColumn("probability", guessing + (1 - guessing) /
                                                                       (1 + exp(-1.702 * discrimination * (
                                                                                   ability - difficulty))))\
            .select("stu_id", "test_id", "ques_id", "probability")

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
        error_code["ERROR"] = "[ValueError] check def distributed_processing()"
    except IndexError:
        error_code["ERROR"] = "[IndexError] check config.py host"
    except KeyError:
        error_code["ERROR"] = "[KeyError] check config.py keys or def connect_spark()"
    except AttributeError:
        error_code["ERROR"] = "[AttributeError] check def distributed_processing()"
    except NameError:
        error_code["ERROR"] = "[NameError] check def distributed_processing()"
    except Py4JJavaError as py:
        error_code["ERROR"] = "[Py4JJavaError] PySpark와 Java 간의 연결 문제 -> ValueError 가능성 있음 " + py.errmsg
    except Exception as err:
        error_code["ERROR"] = f"Unexpected {err=}, {type(err)=}"
    finally:
        return preprocessed_result_data, error_code
