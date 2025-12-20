class BaseFetcher:
    def fetch(self):
        raise NotImplementedError

    def normalize(self, raw):
        raise NotImplementedError

    def health(self):
        return "OK"
