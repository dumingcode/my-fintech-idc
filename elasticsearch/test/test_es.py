from elasticsearch import es
import json
from loguru import logger


def test_es_create_index_setting():
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
    res = es.create_index_setting('test', json.dumps(settings))
    print(res)
    assert res['acknowledged'] is True


def test_es_create_mapping():
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
    res = es.create_mapping('test', json.dumps(mappings))
    print(res)
    assert res['acknowledged'] is True


def test_insert_document():
    document = {
        'bondcode': '110064',
        'bondname': '建工转债',
        'stockcode': '600939',
        'stockname': '重庆建工'
    }
    res = es.insert_document('test', json.dumps(
        document), document['bondcode'])
    document = {
        'bondcode': '113574',
        'bondname': '华体转债',
        'stockcode': '603679',
        'stockname': '华体科技'
    }
    res = es.insert_document('test', json.dumps(
        document), document['bondcode'])
    document = {
        'bondcode': '128103',
        'bondname': '同德转债',
        'stockcode': '002360',
        'stockname': '同德化工'
    }
    res = es.insert_document('test', json.dumps(
        document), document['bondcode'])
    assert res is not None


def test_delete_document():
    res = es.delete_document('test', '128103')
    assert res is not None


def test_es_delete_index():
    res = es.delete_index('test')
    print(res)
    assert res['acknowledged'] is True


'''
{
  "query": {
    "bool": {
      "must": {
        "multi_match": {
          "query": "huati",
          "type": "best_fields",
          "analyzer": "pinyin_analyzer"
        }
      }
    }
  }
}
'''
