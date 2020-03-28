import requests
import json
from loguru import logger
from config import cons as ct


def create_index(index_name: str):
    ret_jsons = None
    try:
        html = requests.put(
            ct.conf('ES')['url'] + index_name)
        ret_jsons = json.loads(html.text)
    except Exception as err:
        logger.error(err)
        return None
    return ret_jsons


def create_index_setting(index_name: str, settings: str):
    ret_jsons = None
    try:
        headers = {"Content-Type": "application/json"}
        html = requests.put(
            ct.conf('ES')['url'] + index_name,
            data=settings, headers=headers)
        ret_jsons = json.loads(html.text)
    except Exception as err:
        logger.error(err)
        return None
    return ret_jsons


def delete_index(index_name: str):
    ret_jsons = None
    try:
        html = requests.delete(ct.conf('ES')['url'] + index_name)
        ret_jsons = json.loads(html.text)
    except Exception as err:
        logger.error(err)
        return None
    return ret_jsons


def create_mapping(index_name: str, mapping_obj: str):
    ret_jsons = None
    headers = {"Content-Type": "application/json"}
    try:
        html = requests.post(
            ct.conf('ES')['url'] + index_name + '/_doc/_mapping',
            data=mapping_obj, headers=headers)
        ret_jsons = json.loads(html.text)
    except Exception as err:
        logger.error(err)
        return None
    return ret_jsons


def insert_document(index_name: str, document: str, key: str):
    ret_jsons = None
    headers = {"Content-Type": "application/json"}
    try:
        html = requests.put(
            ct.conf('ES')['url'] + index_name + '/_doc/' + key,
            data=document, headers=headers)
        ret_jsons = json.loads(html.text)
    except Exception as err:
        logger.error(err)
        return None
    return ret_jsons
