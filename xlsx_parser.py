import pandas as pd
from abc import ABC, abstractmethod


class XlsxParser(ABC):

    def __init__(self, path_to_xlsx):
        self.path_to_xlsx = path_to_xlsx

    def _load_xlsx(self, *args, **kwargs):
        return pd.read_excel(self.path_to_xlsx)

    @abstractmethod
    def parse(self, *args, **kwargs):
        pass


class SimpleReportParser(XlsxParser):

    def _load_xlsx(self, skip_from_top):
        return pd.read_excel(self.path_to_xlsx, skiprows=skip_from_top)

    def parse(self, skip_from_top=0, only_raws=False):
        df = pd.DataFrame(self._load_xlsx(skip_from_top))
        return df.values
