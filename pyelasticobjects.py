from pyelasticsearch import ElasticSearch
from collections import MutableMapping, Sequence

__author__ = 'Schuyler Erle'
# __all__ = ['ObjectSearch']
__version__ = '0.1.0'
__version_info__ = tuple(__version__.split('.'))

get_version = lambda: __version_info__

class Result(object):
    def __init__(self, result):
        for attr, val in result.items():
            if attr[0] == "_": attr = attr[1:]
            setattr(self, attr, val)
    
    # pyelasticsearch._send_request expects to
    # be able to call DocumentSet.get("error", ...)
    def get(self, key, default=None):
        return getattr(self, key, default)

class Document(Result, MutableMapping):
    @property
    def _map(self):
        return self.source

    def __setitem__(self, key, val):
        self._map[key] = val

    def __getitem__(self, key):
        return self._map[key]

    def __delitem__(self, key):
        del self._map[key]

    def __iter__(self):
        return self._map.__iter__()

    def __len__(self):
        return len(self._map)

class DocumentSet(Result, Sequence):
    @property
    def _seq(self):
        return self.docs

    def __init__(self, result):
        super(DocumentSet, self).__init__(result)
        for i in range(len(self._seq)):
            self._seq[i] = Document(self._seq[i])

    def __getitem__(self, idx):
        return self._seq[idx]

    def __len__(self):
        return len(self._seq)

class SearchResult(DocumentSet):
    @property
    def _seq(self):
        return self.hits["hits"]

class ObjectSearch(ElasticSearch):
    __response_types = {
        "_source": Document,
        "hits": SearchResult,
    }

    def _prep_response(self, response):
        response = super(ObjectSearch, self)._prep_response(response)
        for item, wrapper in self.__response_types.items():
            if item in response:
                return wrapper(response)
        return Result(response)

    def from_python(self, value):
        if isinstance(value, Document):
            return value._map
        elif isinstance(value, DocumentSet):
            return value._seq
        else:
            return super(ObjectSearch, self).from_python(value)
