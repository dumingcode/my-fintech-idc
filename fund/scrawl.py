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
from elasticsearch import es
from db import redisDal

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


"""
此函数已取消
"""


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
                    {'_id': row['code'][0:6]}, 'fund_target', data, True)
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
    code = fund['code']
    try:
        outline['Code'] = code
        outline['FundName'] = fund['benchmark']['FundName']
        outline['CategoryName'] = fund['benchmark']['CategoryName']
        outline['CategoryId'] = fund['benchmark']['CategoryId']
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
        outline['ManagerName'] = ''
        managers = fund['manage']['Managers']  # 投资经理
        outline['ManagerTime'] = datetime.date.today().strftime('%Y-%m-%d')
        for manager in managers:
            if not manager['Leave']:
                mName = manager['ManagerName']
                mRange = manager['ManagementRange']
                mTime = manager['ManagementTime']
                outline['ManagerName'] += f'{mName};'
                outline['Managers'] += f'{mName},{mRange},{mTime};'
                if outline['ManagerTime'] > mRange.split()[0]:
                    outline['ManagerTime'] = mRange.split()[0]

        # 回报
        currentReturn = fund['return']['CurrentReturn']
        # 年化回报计算截止日期
        outline['returnEffectiveDate'] = currentReturn['EffectiveDate']
        # 年化回报收益率
        outline['Return'] = []
        curReturns = currentReturn['Return']
        for curReturn in curReturns:
            if curReturn['Name'] == '今年以来回报':
                outline['thisYear'] = curReturn['Return']
            if curReturn['Name'] == '三年回报（年化）':
                outline['Return3Year'] = curReturn['Return']
            if curReturn['Name'] == '五年回报（年化）':
                outline['Return5Year'] = curReturn['Return']
            if curReturn['Name'] == '十年回报（年化）':
                outline['Return10Year'] = curReturn['Return']
        # 业绩表现
        performances = fund['performance']
        outline['Worst3MonReturn'] = performances.get('Worst3MonReturn', '')
        outline['Worst6MonReturn'] = performances.get('Worst6MonReturn', '')
        outline['performance'] = []
        thisYear = datetime.date.today().strftime('%Y')
        for key in performances.keys():
            per = performances[key]
            if not isinstance(per, dict):
                continue
            outline['performance'].append({
                'Year': per['Year'],
                'ReturnYear': per['ReturnYear'] if per['Year'] != thisYear
                else outline['thisYear'],
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
        outline['RiskStats'] = rating['RiskStats']
        # portfolio组合
        portfolio = fund['portfolio']
        outline['Cash'] = portfolio['Cash']
        outline['Stock'] = portfolio['Stock']
        outline['Bond'] = portfolio['Bond']
        outline['Top10StockHoldings'] = portfolio['Top10StockHoldings']
        outline['Top5BondHoldings'] = portfolio['Top5BondHoldings']
        ptime = time.localtime(
            int(portfolio['EffectiveDate'][6:16]))
        pdate = time.strftime('%Y-%m-%d', ptime)
        portfolio['date'] = pdate
        outline['PortfolioEffectiveDate'] = pdate
        outline['Bond'] = portfolio['Bond']
        outline['TopStockWeight'] = portfolio['TopStockWeight']
        outline['TopBondsWeight'] = portfolio['TopBondsWeight']
        res = dal.updateOne({'_id': f'{code}-{pdate}'},
                            'fund_portfolio', portfolio, True)
        return outline
    except Exception as err:
        logger.critical(err)
        return None


def constructSearchEs(outline: object):
    searchObj = {}
    try:
        searchObj['Code'] = outline['Code']
        searchObj['FundName'] = outline['FundName']
        searchObj['InceptionDate'] = outline['InceptionDate']
        searchObj['ManagerTime'] = outline['ManagerTime']
        searchObj['Rating3Year'] = int(outline['Rating3Year'])
        searchObj['Rating5Year'] = int(outline['Rating5Year'])
        searchObj['Rating10Year'] = int(outline['Rating10Year'])
        if outline['Return3Year'] != '':
            searchObj['Return3Year'] = float(outline['Return3Year'])
        if outline['Return5Year'] != '':
            searchObj['Return5Year'] = float(outline['Return5Year'])
        if outline['Return10Year'] != '':
            searchObj['Return10Year'] = float(outline['Return10Year'])
        searchObj['Cash'] = outline['Cash']
        searchObj['Stock'] = outline['Stock']
        searchObj['Bond'] = outline['Bond']
        searchObj['TopStockWeight'] = outline['TopStockWeight']
        searchObj['TopBondsWeight'] = outline['TopBondsWeight']
        if outline['Worst3MonReturn'] != '':
            searchObj['Worst3MonReturn'] = float(outline['Worst3MonReturn'])
        if outline['Worst6MonReturn'] != '':
            searchObj['Worst6MonReturn'] = float(outline['Worst6MonReturn'])
        searchObj['CategoryId'] = outline['CategoryId']
        searchObj['ManagerName'] = outline['ManagerName']
        for per in outline['performance']:
            year = per['Year']
            if per['ReturnYear'] != '':
                searchObj[year] = float(per['ReturnYear'])
        res = es.insert_document('fund', json.dumps(
            searchObj), searchObj['Code'])
        logger.info(searchObj)
        logger.info(res)
    except Exception as err:
        logger.critical(err)
        return -1


def initEsIndexMapping():
    settings = {
        "settings": {
            "number_of_shards": "3",
            "number_of_replicas": "1",
            "index": {
                "analysis": {
                    "analyzer": {
                        "pinyin_analyzer": {
                            "tokenizer": "pinyin_tokenizer"
                        }
                    },
                    "tokenizer": {
                        "pinyin_tokenizer": {
                            "type": "pinyin",
                            "keep_separate_first_letter": True,
                            "keep_first_letter": True,
                            "keep_joined_full_pinyin ": True
                        }
                    }
                }
            }
        }
    }
    res = es.create_index_setting('fund', json.dumps(settings))
    logger.info(res)
    mappings = {
        "_doc": {
            "properties": {
                "FundName": {
                    "type": "text",
                    "analyzer": "pinyin_analyzer",
                    "search_analyzer": "pinyin_analyzer"
                },
                "ManagerName": {
                    "type": "text",
                    "analyzer": "pinyin_analyzer",
                    "search_analyzer": "pinyin_analyzer"
                },
                "Code": {
                    "type": "keyword"
                },
                "InceptionDate": {
                    "type": "date",
                    "format": "yyyy-MM-dd"
                },
                "ManagerTime": {
                    "type": "date",
                    "format": "yyyy-MM-dd"
                },
                "Rating3Year": {
                    "type": "integer"
                },
                "Rating5Year": {
                    "type": "integer"
                },
                "Rating10Year": {
                    "type": "integer"
                },
                "Cash": {
                    "type": "float"
                },
                "Stock": {
                    "type": "float"
                },
                "Bond": {
                    "type": "float"
                },
                "StoTopStockWeight": {
                    "type": "float"
                },
                "TopBondsWeight": {
                    "type": "float"
                },
                "2020": {
                    "type": "float"
                },
                "2019": {
                    "type": "float"
                },
                "2018": {
                    "type": "float"
                },
                "2017": {
                    "type": "float"
                },
                "2016": {
                    "type": "float"
                },
                "2015": {
                    "type": "float"
                },
                "2014": {
                    "type": "float"
                },
                "2013": {
                    "type": "float"
                },
                "Worst3MonReturn": {
                    "type": "float"
                },
                "Worst6MonReturn": {
                    "type": "float"
                },
                "CategoryId": {
                    "type": "keyword"
                },
                "Return3Year": {
                    "type": "float"
                },
                "Return5Year": {
                    "type": "float"
                },
                "Return10Year": {
                    "type": "float"
                }
            }
        }
    }
    res = es.create_mapping('test', json.dumps(mappings))
    logger.info(res)


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
                'fcid': fcid,
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
            # 存储fund 原始数据
            res = dal.updateOne({'_id': fcid}, 'fund', fund, True)
            # 构建fund 详情数据
            outline = extractOutline(fund)
            redisRes = redisDal.redisSet(f'fund:{code}', json.dumps(outline))
            res = dal.updateOne({'_id': fcid}, 'fund_outline', outline, True)
            logger.info(f'{code} redis result:' +
                        str(redisRes) + ' mongodb result:' + str(res))
            # 构建ES
            constructSearchEs(outline)
            return res
        except Exception as err:
            logger.critical(err)
            return -1

    def scrawlFundTask(self):
        try:
            fundArray = dal.queryMany(None, {'_id': 1, 'cxcode': 1},
                                      0, None, 'fund_target')
            fundArr = list(fundArray)
            if len(fundArr) == 0:
                return None
            for fund in fundArr:
                code = fund['_id']
                cxcode = fund['cxcode']
                if (code is None or cxcode is None):
                    continue
                time.sleep(random.randint(15, 20))
                logger.info(fund)
                self.scrawlSingleFundInfo(cxcode, code)
        except Exception as err:
            logger.critical(err)
            return -1
