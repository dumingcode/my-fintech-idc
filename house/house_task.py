import datetime
import random
import time

import pandas as pd
from loguru import logger

from mongo import dal
from stock import basic
from stock import price
from stock import dividend_share
from stock import quant
from config import cons as ct

import numpy as np
import talib
from db import redisDal
import json
import os

# 自动任务：从本地文件中获取北京二手房历史网签数据


def run_house_extract_localhis_data_task():
    """
        从本地文件中获取北京二手房历史网签数据
    Parameters
    ------
    Return
    -------
        result 是否正常结束
    """
    try:
        with open(os.path.abspath('./house/data/house.json')) as hisData:
            content = hisData.read()
            contentObj = json.loads(content)
            arr = contentObj['data']
            for deal in arr:
                date = deal['Date']
                date = str(date[0:10:1]).replace('-', '', 2)
                obj = {
                    'houseNum': deal['House_num'],
                    'houseSquare': deal['House_square'],
                    'onlineNum': deal['Online_num'],
                    'onlineSquare': deal['Online_square'],
                    'date': int(date)
                }
                res = dal.updateOne({'_id': int(date)}, 'house', obj, True)
    except Exception as exp:
        logger.critical(exp)
        return False
    logger.info(
        '*********run_house_extract_localhis_data_task end********')
    return True
