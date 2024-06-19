from data_service_cassandra import get_ability_data, get_3pl_data, put_probability_result_data
from spark_connection import connect_spark
from preprocessing_data import distributed_processing
from validation import check_spark_session, check_distributed_processing, check_error_code
from util import set_logging
import logging


def run_model():
    result_code = ""

    # connect to spark
    spark, error_code = connect_spark()

    # check_spark_session
    check_spark_session(error_code)

    # select data
    irt_3pl_df, error_code = get_3pl_data(spark, error_code)
    irt_ability_df, error_code = get_ability_data(spark, error_code)

    # distributed_processing
    preprocessed_result_data, error_code = distributed_processing(irt_3pl_df, irt_ability_df, error_code)

    # check_distributed_processing
    check_distributed_processing(error_code)

    # insert preprocessed_result_data
    error_code = put_probability_result_data(preprocessed_result_data, error_code)

    # spark 종료
    spark.stop()

    # check_error_code
    result_code = check_error_code(error_code)

    return result_code


if __name__ == "__main__":
    # config logging
    set_logging()

    # run model
    run_model()
