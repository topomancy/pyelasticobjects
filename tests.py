"""
Unit tests for pyelasticobjects.  These require an elasticsearch server running
on the default port (localhost:9200).
"""
from pyelasticobjects import *
import json
import unittest

# copied with impunity from pyelasticsearch's test.py
#
class ObjectSearchTestCase(unittest.TestCase):
    def setUp(self):
        self.conn = ObjectSearch('http://localhost:9200/')

    def tearDown(self):
        try:
            self.conn.delete_index("test-index")
        except:
            pass

    def assertResultContains(self, result, expected):
        for (key, value) in expected.items():
            self.assertEquals(value, result[key])

class ResultTestCase(ObjectSearchTestCase):
    def testIndex(self):
        result = self.conn.index("test-index", "user", {"user": "Joe Tester"})
        self.assertTrue(isinstance(result, Result))
        self.assertTrue(result.ok)
        self.assertEqual(result.index, "test-index")
        self.assertEqual(result.type, "user")
        self.assertTrue(result.id)

    def testCount(self):
        self.conn.index("test-index", "user", {"user": "Joe Tester"})
        self.conn.refresh(["test-index"])
        result = self.conn.count("Joe")
        self.assertEqual(result.count, 1)
        self.assertTrue(isinstance(result.shards, dict))

    def testCreateIndex(self):
        result = self.conn.create_index("test-index")
        self.assertTrue(isinstance(result, Result))
        self.assertEqual(result.acknowledged, True)
        self.assertEqual(result.ok, True)

class DocumentTestCase(ObjectSearchTestCase):
    def testGet(self):
        self.conn.index("test-index", "user", {"user": "Joe Tester"}, 1)
        doc = self.conn.get("test-index", "user", 1)
        self.assertTrue(isinstance(doc, Document))
        self.assertEqual(doc.index, "test-index")
        self.assertEqual(doc.type, "user")
        self.assertEqual(doc.id, "1")
        self.assertTrue(isinstance(doc.source, dict))
        self.assertEqual(doc["user"], "Joe Tester")
        self.assertEqual(doc.source["user"], doc["user"])

    def testSetItem(self):
        doc = Document({"_source": {}})
        doc["user"] = "Joe Q. Tester"
        self.assertEqual(doc.source["user"], "Joe Q. Tester")

    def testDelItem(self):
        doc = Document({"_source": {"user": "Joe Q. Tester"}})
        del doc["user"]
        self.assertEqual(len(doc.source), 0)

    def testFromPython(self):
        fixture = {"_source": {"user": "Joe Q. Tester"}}
        doc = Document(fixture)
        json_str = json.dumps(self.conn.from_python(doc))
        self.assertTrue(isinstance(json_str, str))
        thawed = json.loads(json_str)
        self.assertTrue(isinstance(thawed, dict))
        self.assertEqual(thawed["user"], fixture["_source"]["user"])

class SearchResultTestCase(ObjectSearchTestCase):
    def indexSomeDocs(self):
        self.conn.index("test-index", "user", {"user": "Joe Tester"}, 1)
        self.conn.index("test-index", "user", {"user": "Jane Tester"}, 2)
        self.conn.index("test-index", "user", {"user": "Doc Indexer"}, 3)
        self.conn.refresh(["test-index"])
        
    def testSearch(self):
        self.indexSomeDocs()
        docs = self.conn.search("user:Tester")
        self.assertTrue(isinstance(docs, SearchResult))
        self.assertTrue(isinstance(docs.hits, dict))
        self.assertEqual(docs.hits["total"], 2)
        self.assertTrue(len(docs), 2)
        self.assertTrue(isinstance(docs[0], Document))
        self.assertTrue("user" in docs[0])
        self.assertTrue(isinstance(docs[1], Document))
        self.assertTrue("user" in docs[1])

    def testMoreLikeThis(self):
        self.indexSomeDocs()
        docs = self.conn.more_like_this("test-index", "user", 1, ['user'], min_term_freq=1, min_doc_freq=1)
        self.assertTrue(isinstance(docs, SearchResult))
        self.assertTrue(isinstance(docs.hits, dict))
        self.assertEqual(docs.hits["total"], len(docs))
        self.assertTrue(isinstance(docs[0], Document))
        self.assertTrue("user" in docs[0])

    def tFromPython(self):
        fixture = {"_source": {"user": "Joe Q. Tester"}}
        doc = Document(fixture)
        json_str = json.dumps(self.conn.from_python(doc))
        self.assertTrue(isinstance(json_str, str))
        thawed = json.loads(json_str)
        self.assertTrue(isinstance(thawed, dict))
        self.assertEqual(thawed["user"], fixture["_source"]["user"])

    def indexSomeGeoDocs(self):
        self.conn.create_index('test-index')
        self.conn.put_mapping('test-index', 'user', {'user': {'properties': {'name': {'type': 'string'},"location" : { "type" : "geo_point" }}}})
        self.conn.index("test-index", "user", {"name": "Joe Tester", "age" : 25, "place":"leeds", "location":"53.8, -1.5"}, 1)
        self.conn.index("test-index", "user", {"name": "Jane Tester",  "age" : 30, "place":"san francisco", "location":"37.7, -122.4"}, 2)
        self.conn.index("test-index", "user", {"name": "Doc Testing",  "age" : 32, "place":"bombay", "location" : "19.0, 72.8"}, 3)
        self.conn.refresh(["test-index"])


    def testSearchFromDSL(self):
        self.indexSomeGeoDocs()
        query = {'query': {
                            'filtered': {
                                'query': {
                                    'query_string': {'query': 'name:Test*'}
                                },
                                'filter': {
                                    'range': {
                                        'age': {
                                            'from': 27,
                                            'to': 37,
                                        },
                                    },
                                },
                            },
                        },
                    }
        results = self.conn.search(query, index='test-index', doc_type='user')
        self.assertEqual(2, len(results.hits["hits"]))
        self.assertTrue("name" in results[0])

    def testBBOXSearchFromDSL(self):
        self.indexSomeGeoDocs()
        query = {
            'query': {
                "filtered": {
                    "query" : {
                        "match_all" : {}
                    },
                    "filter": {
                        "geo_bounding_box": {
                            "user.location": {
                                "top_left": { 
                                    "lat": 61.27,
                                    "lon": -20.39
                                },
                                "bottom_right": {
                                    "lat": 1.05,
                                    "lon": 88.94
                                }
                            }
                        }
                    }
                }
            }
        }
        results = self.conn.search(query, index='test-index', doc_type='user')
        self.assertEqual("leeds", results[0]["place"])
        self.assertEqual("bombay", results[1]["place"])
        self.assertEqual(2, len(results.hits["hits"]))



if __name__ == "__main__":
    unittest.main()

