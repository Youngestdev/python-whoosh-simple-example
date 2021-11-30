import json
from typing import Dict, List, Sequence

from whoosh.analysis import StemmingAnalyzer
from whoosh.fields import *
from whoosh.filedb.filestore import RamStorage
from whoosh.qparser import MultifieldParser


class Indexer:

    def __init__(self, schema):
        self.schema = schema
        schema.add('crawled_sls_data', TEXT(stored=True))
        self.ix = RamStorage().create_index(self.schema)

    def index_documents(self, docs: Sequence):
        writer = self.ix.writer()

        for doc in docs:
            d = {k: v for k, v in doc.items() if k in self.schema.stored_names()}
            d['crawled_sls_data'] = json.dumps(doc)  # raw version of all of doc
            writer.add_document(**d)
        writer.commit(optimize=True)

    def query(self, q: str, fields: Sequence, highlight: bool = True) -> List[Dict]:
        search_results = []
        with self.ix.searcher() as searcher:
            results = searcher.search(MultifieldParser(fields, schema=self.schema).parse(q))
            for r in results:
                d = json.loads(r['crawled_sls_data'])
                if highlight:
                    for f in fields:
                        if r[f] and isinstance(r[f], str):
                            d[f] = r.highlights(f) or r[f]

                search_results.append(d)

        return search_results


if __name__ == '__main__':

    # docs = [
    #     {
    #         "publication_title": "First document banana",
    #         "publication_url": "This is the first document we've added in San Francisco!",
    #         "tags": ['foo', 'bar'],
    #         "authors": ["Abdul"],
    #         "pub_date": "2020-10-20"
    #     },
    #     {
    #         "publication_title": "Second document",
    #         "publication_url": "This is the second document we've added in San Francisco!",
    #         "tags": ['foo', 'bob'],
    #         "authors_name": [
    #             'Remi'
    #         ],
    #         "pub_date": "2020-10-20"
    #     },
    #
    # ]

    json_file = open("experimental.json")
    docs = json.load(json_file)

    # Expensive play.

    result = []

    # for key in range(len(docs)):
    #     docs[key]["authors_name"] = docs[key]["authors_name".replace('"', '')]

    schema = Schema(
        publication_title=TEXT(stored=True, analyzer=StemmingAnalyzer()),
        publication_url=TEXT(stored=True),
        authors_name=KEYWORD(stored=True, commas=True, scorable=True),
        profile_links=KEYWORD(stored=True, commas=True, scorable=True),
        pub_date=TEXT(stored=True)
    )

    engine = Indexer(schema)
    engine.index_documents(docs)

    fields_to_search = ["publication_title", "publication_url", "authors_name"]

    for q in ["Horton, E.", "Blood cancer", "Acute", "cancer"]:
        print(f"Query:: {q}")
        print("\t", engine.query(q, fields_to_search, highlight=True))
        print("-" * 70)
