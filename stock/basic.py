import tushare as ts
import pandas as pd
from config import cons as ct
import requests
from loguru import logger
import json
import re


def get_hs_stock_list(param):
    """
        沪深股市股票列表
    Parameters
    ------
        Dict
        is_hs: 是否沪深港通标的，N否 H沪股通 S深股通
        list_status: 上市状态： L上市 D退市 P暂停上市
        exchange: 交易所 SSE上交所 SZSE深交所 HKEX港交所(未上线)
        fields: ts_code,symbol,name,area,industry,list_date
    Return
    -------
        DataFrame
            股票列表(DataFrame):
                ts_code       ts股票代码
                symbol        市场代码
                name          名称
                area          上市地区
                industry      行业
                list_date     上市日期
    """
    ts.set_token(ct.conf('TOKEN'))
    pro = ts.pro_api()
    df = pro.stock_basic(
        list_status=param['list_status'], exchange=param['exchange'],
        filelds=param['fields'])
    new = pd.DataFrame({'ts_code': ['399300.SZ'],
                        'symbol': ['399300']})
    df = df.append(new)
    return df


def get_hs_cb_list(type: str = 'cb', status: str = 'L') -> []:
    """
        沪深股市转债列表
    Parameters
    ------
        Dict
        type: cb（转债） 、 eb（可交换债）、 ''表示两者都有、默认eb
        status: 上市状态： L上市 W未上市或暂停上市、默认L
    Return
    -------
        DataFrame
            转债列表(DataFrame):
                BONDCODE          转债代码
                CORRESNAME        转债名称
                SWAPSCODE         正股代码
                SECURITYSHORTNAME 正股名称
                SNAME             转债名称
                STARTDATE     申购开始时间
    """
    cb_url = ct.cbListUrl()
    try:
        html = requests.get(cb_url)
        if html.status_code != 200:
            raise RuntimeError(f'{cb_url} is error code :{html.status_code}')
        content = html.text
        pattern = re.compile(r'{(.*?)}', re.M)
        match = pattern.findall(content)
        i = 0
        ret = []
        while i < len(match)-1:
            i += 1
            try:
                sjson = json.loads('{'+match[i]+'}')
                if sjson.get('BONDCODE') is None:
                    continue
                ret.append({'BONDCODE': sjson['BONDCODE'], 'CORRESNAME':
                            sjson['CORRESNAME'], 'SWAPSCODE':
                            sjson['SWAPSCODE'], 'SECURITYSHORTNAME':
                            sjson['SECURITYSHORTNAME'], 'SNAME':
                            sjson['SNAME'], 'STARTDATE': sjson['STARTDATE'],
                            'LISTDATE':  '-' if sjson['LISTDATE'] == '-'
                                else int(sjson['LISTDATE'][0:10].replace('-', ''))
                            })
            except Exception as err:
                logger.error(
                    f'{match[i]} json parse error')
                continue
    except Exception as err:
        logger.error(err)
        return ret
    return ret
