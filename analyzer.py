"""
The aim of this file is to create settings for ElasticSearch.
We create analyzer for each database : CPF and Cnet.
Both analyzer use same filters: "french_elision","lowercase","french_stop","french_keywords".
The only difference comes from the mapping where we select the text to analyze : node1 for Cnet
and object ("o") for CPF.
After compilation, 2 indexes are created with these settings
"""

from elasticsearch import Elasticsearch

def function_analyzer(tokenizer):

    es = Elasticsearch()
    
    analyzercpf={
      "settings": {
        "analysis": {
          "filter": {
            "french_elision": {
              "type":         "elision",
              "articles_case": True,
              "articles": [
                  "l", "m", "t", "qu", "n", "s",
                  "j", "d", "c", "jusqu", "quoiqu",
                  "lorsqu", "puisqu", "autres", "Autres"
                ]
            },
            "french_stop": {
              "type":       "stop",
              "stopwords":  "_french_" 
            },
            "french_keywords": {
              "type":       "keyword_marker",
              "keywords":   [] 
            }
          },
          "analyzer": {
            "rebuilt_french": {
              "tokenizer":  "standard",
              "filter": [
                "french_elision",
                "lowercase",
                "french_stop",
                "french_keywords"
              ]
            }
          }
        }
      },
      "mappings": {
        "properties": {
          "o": {
            "type": "text",
            "analyzer": "rebuilt_french"
            }
          }
        }
      }
    
    analyzer={
      "settings": {
        "analysis": {
          "filter": {
            "french_elision": {
              "type":         "elision",
              "articles_case": True,
              "articles": [
                  "l", "m", "t", "qu", "n", "s",
                  "j", "d", "c", "jusqu", "quoiqu",
                  "lorsqu", "puisqu", "autres", "Autres"
                ]
            },
            "custom_words": {
                "type": "stop",
                "ignore_case": True,
                "stopwords": [ "autres", "Autres", "à l'exclusion", "exclusion", "tant", "tant que"]
            },
            "french_stop": {
              "type":       "stop",
              "stopwords":  "_french_" 
            },
            "french_keywords": {
              "type":       "keyword_marker",
              "keywords":   [] 
            }
          },
        "tokenizer": {
        "my_tokenizer": {
          "type": "ngram",
          "min_gram": 3,
          "max_gram": 3,
          "token_chars": [
            "letter",
            "digit"
          ]
        }
      },
          "analyzer": {
            "rebuilt_french": {
              "tokenizer":  tokenizer,
              "filter": [
                "french_elision",
                "custom_words",
                "lowercase",
                "french_stop",
                "french_keywords"
              ]
            }
          }
        }
      },
      "mappings": {
        "properties": {
          "_node1": {
            "type": "text",
            "analyzer": "rebuilt_french"
               }
            }
          }
        }

    
    """ 
    Here, because we want to compare standard & ngram tokenizer and build a Database with these two tokenizers,
    we have to reset our indexes to change the analyzer
    """
    
    if tokenizer == "my_tokenizer":    
        es.indices.delete(index="cpf", ignore=[400, 404])
        es.indices.create(index="cpf", body=analyzercpf)
        
        es.indices.delete(index="conceptnet", ignore=[400, 404])
        es.indices.create(index="conceptnet", body=analyzer)
        
    return(es)


    