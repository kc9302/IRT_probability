from pyspark.sql import functions as F
import logging


def check_data_count(raw_data_df, preprocessed_result_data):
    raw_group_by = raw_data_df.groupBy('test_id', 'ques_id').agg(F.collect_list("crt_yn")).count()
    preprocessed_result_data_count = preprocessed_result_data.count()
    if preprocessed_result_data_count != raw_group_by:
        logging.error(f"raw_groupBy : {str(raw_group_by)}")
        logging.error(f"preprocessed_result_data : {str(preprocessed_result_data_count)}")
        raise Exception("전처리 오류")
    else:
        logging.debug("check_data_count validation success")


def check_raw_data_count(raw_df):
    if raw_df.count() == 1:
        logging.error("raw_data_count : 1")
        raise Exception("raw_data_count : 1")
    else:
        logging.debug("check_raw_data_count validation success")


def check_irt_3pl_count(irt_3pl_df):
    if irt_3pl_df.count() == 1:
        logging.error("irt_3pl_df_count : 1")
        raise Exception("irt_3pl_df_count : 1")
    else:
        logging.debug("check_irt_3pl_count validation success")


def check_irt_ability_count(irt_ability_df):
    if irt_ability_df.count() == 1:
        logging.error("irt_ability_df_count : 1")
        raise Exception("irt_ability_df_count : 1")
    else:
        logging.debug("check_irt_ability_count validation success")


def check_spark_session(error_code):
    if len(error_code["ERROR"]) != 0:
        logging.error(error_code["ERROR"])
        raise Exception(error_code["ERROR"])
    else:
        logging.debug("check_spark_session validation success")


def check_distributed_processing(error_code):
    if len(error_code["ERROR"]) != 0:
        logging.error(error_code["ERROR"])
        raise Exception(error_code["ERROR"])
    else:
        logging.debug("check_distributed_processing validation success")


def check_error_code(error_code):
    if len(error_code["ERROR"]) == 0:
        success_result_code = {"SUCCESS": "success"}
        result_code = success_result_code
        logging.debug("check_error_code validation success")
        return result_code
    elif len(error_code["ERROR"]) != 0:
        logging.error(error_code["ERROR"])
        result_code = error_code
        return result_code
