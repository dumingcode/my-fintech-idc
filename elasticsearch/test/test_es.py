from elasticsearch import es
import json


def te1st_es_delete_index():
    res = es.delete_index('test')
    print(res)
    assert res['acknowledged'] is True


def tes1t_es_create_index_setting():
    settings = {
        "settings": {
            "number_of_shards": "3",
            "number_of_replicas": "1",
            "index": {
                "analysis": {
                    "analyzer": {
                        "ik_pinyin_analyzer": {
                            "type": "custom",
                            "tokenizer": "ik_max_word",
                            "filter": "pinyin_filter"
                        }
                    },
                    "filter": {
                        "pinyin_filter": {
                            "type": "pinyin",
                            "keep_separate_first_letter": False,
                            "keep_first_letter": True
                        }
                    }
                }
            }
        }
    }
    res = es.create_index_setting('test', json.dumps(settings))
    print(res)
    assert res['acknowledged'] is True


def tes1t_es_create_mapping():
    mappings = {
        "_doc": {
            "properties": {
                "name": {
                    "type": "text",
                    "analyzer": "ik_pinyin_analyzer",
                    "search_analyzer": "ik_pinyin_analyzer"
                },
                "code": {
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
        'code': '110064',
        'name': '建工转债'
    }
    res = es.insert_document('test', json.dumps(document), document['code'])
    document = {
        'code': '127015',
        'name': '希望转债'
    }
    res = es.insert_document('test', json.dumps(document), document['code'])
    document = {
        'code': '128092',
        'name': '唐人转债'
    }
    res = es.insert_document('test', json.dumps(document), document['code'])
    assert res is not None


# {"query":{"bool":{"must":{"multi_match":{"query":"trzz","type":"best_fields","analyzer":"ik_pinyin_analyzer"}}}}}
