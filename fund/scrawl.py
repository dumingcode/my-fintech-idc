"""
抓取晨星基金网对基金的分析数据
"""
import json
from loguru import logger
from config import cons as ct
import requests
from mongo import dal
import datetime
import csv
import os
import time
import random
from util import utils

"""
    获取晨星基金对应基金的benchmark比较基准
"""


def scrawlBenchmark(fcid: str) -> object:
    try:
        benchmarkUrl = ct.mstarBanchmark(fcid)
        html = requests.get(benchmarkUrl, headers=ct.mstarHeaders())
        if html.status_code != 200:
            logger.critical(
                f'scrawlBenchmark {fcid} is error code :{html.status_code}')
            return None
        ret_jsons = json.loads(html.text)
        return ret_jsons
    except Exception as err:
        logger.critical(err)
        return None


def scrawlFee(fcid: str) -> object:
    try:
        feeUrl = ct.mstarFee(fcid)
        html = requests.get(feeUrl, headers=ct.mstarHeaders())
        if html.status_code != 200:
            logger.critical(
                f'scrawlFee {fcid} is error code :{html.status_code}')
            return None
        ret_jsons = json.loads(html.text)
        return ret_jsons
    except Exception as err:
        logger.critical(err)
        return None


def scrawlManage(fcid: str) -> object:
    try:
        url = ct.mstarManage(fcid)
        html = requests.get(url, headers=ct.mstarHeaders())
        if html.status_code != 200:
            logger.critical(
                f'scrawlManage {fcid} is error code :{html.status_code}')
            return None
        ret_jsons = json.loads(html.text)
        return ret_jsons
    except Exception as err:
        logger.critical(err)
        return None


def scrawlReturn(fcid: str) -> object:
    try:
        url = ct.mstarReturn(fcid)
        html = requests.get(url, headers=ct.mstarHeaders())
        if html.status_code != 200:
            logger.critical(
                f'scrawlReturn {fcid} is error code :{html.status_code}')
            return None
        ret_jsons = json.loads(html.text)
        return ret_jsons
    except Exception as err:
        logger.critical(err)
        return None


def scrawlPerformance(fcid: str) -> object:
    try:
        url = ct.mstarPerformance(fcid)
        html = requests.get(url, headers=ct.mstarHeaders())
        if html.status_code != 200:
            logger.critical(
                f'scrawlPerformance {fcid} is error code :{html.status_code}')
            return None
        ret_jsons = json.loads(html.text)
        return ret_jsons
    except Exception as err:
        logger.critical(err)
        return None


def scrawlRating(fcid: str) -> object:
    try:
        url = ct.mstarRating(fcid)
        html = requests.get(url, headers=ct.mstarHeaders())
        if html.status_code != 200:
            logger.critical(
                f'scrawlRating {fcid} is error code :{html.status_code}')
            return None
        ret_jsons = json.loads(html.text)
        return ret_jsons
    except Exception as err:
        logger.critical(err)
        return None


def scrawlPortfolio(fcid: str) -> object:
    try:
        url = ct.mstarPortfolio(fcid)
        html = requests.get(url, headers=ct.mstarHeaders())
        if html.status_code != 200:
            logger.critical(
                f'scrawlPortfolio {fcid} is error code :{html.status_code}')
            return None
        ret_jsons = json.loads(html.text)
        return ret_jsons
    except Exception as err:
        logger.critical(err)
        return None

# 初始化共同基金抓取目标


def initFundTarget():
    try:
        with open(os.path.abspath('./fund/data/fund.csv'), newline='',
                  encoding='gbk') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                logger.info(row)
                data = {
                    '_id': row['code'][0:6],
                    'cxcode': row['cxcode'],
                    'name': row['name']
                }
                dal.updateOne(
                    {'_id': row['code'][0:6]}, 'fundTarget', data, True)
    except Exception as exp:
        logger.critical(exp)
        return False
    return True

# 抽取晨星基金数据中的概要信息供前端展示


def extractOutline(fund: object) -> object:
    '''
        从晨星返回报文中构建出概要信息供前端展示
        :param fund:晨星返回数据解析对象
    '''
    outline = {}
    try:
        outline['FundName'] = fund['benchmark']['FundName']
        outline['CategoryName'] = fund['benchmark']['CategoryName']
        outline['BanchmarkName'] = fund['benchmark']['BanchmarkName']
        outline['Management'] = fund['fee']['Management']
        outline['Custodial'] = fund['fee']['Custodial']
        outline['Redemption'] = ''
        outline['LastRedemption'] = ''  # 记录持有期最久的赎回费率
        redemptions = fund['fee']['Redemption']
        for redemp in redemptions:
            key = utils.clearHtmlRe(redemp['Key'])
            value = utils.clearHtmlRe(redemp['Value'])
            outline['Redemption'] += f'{key}:{value};'
            outline['LastRedemption'] = f'{key}:{value}'

        outline['Profile'] = fund['manage']['Profile']  # 投资目标
        outline['InceptionDate'] = fund['manage']['InceptionDate']  # 成立日期
        outline['Managers'] = ''
        managers = fund['manage']['Managers']  # 投资经理
        for manager in managers:
            if not manager['Leave']:
                mName = manager['ManagerName']
                mRange = manager['ManagementRange']
                mTime = manager['ManagementTime']
                outline['Managers'] += f'{mName},{mRange},{mTime};'
        # 回报
        currentReturn = fund['return']['CurrentReturn']
        # 年化回报计算截止日期
        outline['EffectiveDate'] = currentReturn['EffectiveDate']
        # 年化回报收益率
        outline['Return'] = []
        curReturns = currentReturn['Return']
        for curReturn in curReturns:
            outline['Return'].append({'Name': curReturn['Name'],
                                      'Return': curReturn['Return'],
                                      'ReturnToInd': curReturn['ReturnToInd'],
                                      'ReturnToCat': curReturn['ReturnToCat']
                                      })
        # 业绩表现
        performances = fund['performance']
        outline['Worst3MonReturn'] = performances.get('Worst3MonReturn', '')
        outline['Worst6MonReturn'] = performances.get('Worst6MonReturn', '')
        outline['performance'] = []
        for key in performances.keys():
            per = performances[key]
            if not isinstance(per, dict):
                continue
            outline['performance'].append({
                'Year': per['Year'],
                'ReturnYear': per['ReturnYear'],
                'ReturnQ1': per['ReturnQ1'],
                'ReturnQ2': per['ReturnQ2'],
                'ReturnQ3': per['ReturnQ3'],
                'ReturnQ4': per['ReturnQ4']
            })
        # 评级rating
        rating = fund['rating']
        outline['RatingDate'] = rating['RatingDate']
        outline['Rating1Year'] = rating['Rating1Year']
        outline['Rating2Year'] = rating['Rating2Year']
        outline['Rating3Year'] = rating['Rating3Year']
        outline['Rating5Year'] = rating['Rating5Year']
        outline['Rating10Year'] = rating['Rating10Year']
        outline['RiskAssessment'] = rating['RiskAssessment']
        # portfolio组合
        portfolio = fund['portfolio']
        outline['Cash'] = portfolio['Cash']
        outline['Stock'] = portfolio['Stock']
        outline['Bond'] = portfolio['Bond']
        outline['PortfolioEffectiveDate'] = portfolio['EffectiveDate'][6:17]
        outline['Bond'] = portfolio['Bond']
        outline['TopStockWeight'] = portfolio['TopStockWeight']
        outline['TopBondsWeight'] = portfolio['TopBondsWeight']
        return outline
    except Exception as err:
        logger.critical(err)
        return None


class MstarScrawl:
    def scrawlSingleFundInfo(self, fcid: str, code: str):
        try:
            benchmark = scrawlBenchmark(fcid)
            fee = scrawlFee(fcid)
            portfolio = scrawlPortfolio(fcid)
            rating = scrawlRating(fcid)
            performance = scrawlPerformance(fcid)
            returns = scrawlReturn(fcid)
            manage = scrawlManage(fcid)
            fund = {
                'code': code,
                'benchmark': benchmark,
                'fee': fee,
                'portfolio': portfolio,
                'rating': rating,
                'performance': performance,
                'return': returns,
                'manage': manage,
                'date': datetime.date.today().strftime('%Y%m%d')
            }
            extractOutline(fund)
            res = dal.updateOne({'_id': fcid}, 'fund', fund, True)
            return res
        except Exception as err:
            logger.critical(err)
            return -1

    def scrawlFundTask(self):
        try:
            fundArray = dal.queryMany(None, {'_id': 1, 'cxcode': 1},
                                      0, None, 'fundTarget')
            fundArr = list(fundArray)
            if len(fundArr) == 0:
                return None
            for fund in fundArr:
                code = fund['_id']
                cxcode = fund['cxcode']
                if (code is None or cxcode is None):
                    continue
                time.sleep(random.randint(20, 30))
                logger.info(fund)
                self.scrawlSingleFundInfo(cxcode, code)
        except Exception as err:
            logger.critical(err)
            return -1
