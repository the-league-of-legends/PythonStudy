import csv

import logging


def save_csv(file_name, info_list):
    try:
        with open(file_name, "a", encoding="utf-8-sig", newline="")as f:
            writer = csv.writer(f)
            writer.writerow(info_list)
    except Exception as ex:
        print("---------------------------------save_csv 异常")
        logging.exception(ex)
        print(ex)
