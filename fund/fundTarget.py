"""
创建出需要抓取的基金名单-选取晨星各类基金3星以上评级
"""
import json
from loguru import logger
from config import cons as ct
from mongo import dal
import datetime
import os
import time
from bs4 import BeautifulSoup


def createFundTarget():
    try:
        for idx in range(27, 32):
            fileStr = f'./fund/data/targetFund/基金筛选_晨星网{idx}.html'
            createSingleFundTarget(fileStr)
    except Exception as err:
        logger.critical(err)


def createSingleFundTarget(filePath: str):
    try:
        soup = BeautifulSoup(open(
            os.path.abspath(filePath),
            newline='', encoding='utf-8'))
        table = soup.find("table", id="ctl00_cphMain_gridResult")
        tbody = table.contents[1]
        idx = 0
        for tr in list(tbody.children):
            idx += 1
            if idx == 1:
                continue
            cxcodes = tr.contents[1].find("span")['tag']
            cxcode = cxcodes.split('|')[0]
            _id = tr.contents[2].get_text()
            name = tr.contents[3].get_text()
            data = {
                '_id': _id,
                'cxcode': cxcode,
                'name': name
            }
            logger.info(data)
            dal.updateOne(
                {'_id': _id}, 'fund_target', data, True)
    except Exception as err:
        logger.critical(err)
