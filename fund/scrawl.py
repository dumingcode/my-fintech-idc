"""
抓取晨星基金网对基金的分析数据
"""
import json
from loguru import logger
from config import cons as ct
import requests

"""
    获取晨星基金对应基金的benchmark比较基准
"""


def scrawlBenchmark(fcid: str) -> object:
    try:
        benchmarkUrl = ct.mstarBanchmark(fcid)
        html = requests.get(benchmarkUrl, headers=ct.mstarHeaders())
        if html.status_code != 200:
            logger.critical(
                f'{benchmarkUrl} is error code :{html.status_code}')
            return None
        ret_jsons = json.loads(html.text)
        logger.info(ret_jsons)
        return ret_jsons
    except Exception as err:
        logger.critical(err)
        return None
