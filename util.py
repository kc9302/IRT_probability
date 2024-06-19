import datetime
import logging
import os


def set_logging():

    if not os.path.isdir("./logs"):
        # logs 폴더 생성
        os.mkdir('logs')

    # 현재 시간을 저장
    date_info = datetime.datetime.now()
    # 파일 이름
    log_file_name = "./logs/IRT_3PL_log_{}.log".format(date_info.today().strftime("%y%m%d_%H_%M_%S"))
    logging.basicConfig(filename=log_file_name,
                        datefmt="%Y-%m-%d %H-%M-%S",
                        level=logging.DEBUG,
                        format="%(asctime)s %(levelname)s:%(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s:%(message)s"))
    logging.getLogger().addHandler(console_handler)
    return logging

