import json
from loguru import logger
from config import cons as ct
from elasticsearch import es


def init_cbond_es():
    ret_jsons = None
    try:
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
        res = es.create_index_setting('cbond', json.dumps(settings))
        if(res['acknowledged'] is not True):
            raise RuntimeError('创建index失败')
        mappings = {
            "_doc": {
                "properties": {
                    "bondname": {
                        "type": "text",
                        "analyzer": "pinyin_analyzer",
                        "search_analyzer": "pinyin_analyzer"
                    },
                    "stockname": {
                        "type": "text",
                        "analyzer": "pinyin_analyzer",
                        "search_analyzer": "pinyin_analyzer"
                    },
                    "bondcode": {
                        "type": "keyword"
                    },
                    "stockcode": {
                        "type": "keyword"
                    }
                }
            }
        }
        res = es.create_mapping('cbond', json.dumps(mappings))
    except Exception as err:
        logger.error(err)
        return None
    return ret_jsons


def init_stock_es():
    ret_jsons = None
    try:
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
        res = es.create_index_setting('stock', json.dumps(settings))
        if(res['acknowledged'] is not True):
            raise RuntimeError('创建index失败')
        mappings = {
            "_doc": {
                "properties": {
                    "stockname": {
                        "type": "text",
                        "analyzer": "pinyin_analyzer",
                        "search_analyzer": "pinyin_analyzer"
                    },
                    "stockcode": {
                        "type": "keyword"
                    }
                }
            }
        }
        res = es.create_mapping('stock', json.dumps(mappings))
    except Exception as err:
        logger.error(err)
        return None
    return ret_jsons
