from base_fetcher import BaseFetcher

class PolicyRatesFetcher(BaseFetcher):
    def fetch(self):
        # for now, just return some dummy data
        return {
            "effr": 5.0,
            "cpi_yoy": 3.0
        }

    def normalize(self, raw):
        # no changes for now
        return raw
