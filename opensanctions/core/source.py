import os
from importlib import import_module

from opensanctions.core.dataset import Dataset


class SourceData(object):
    """Data source specification."""

    def __init__(self, config):
        self.url = config.get("url")
        self.mode = config.get("mode")
        self.format = config.get("format")
        self.api_key = config.get("api_key")
        if self.api_key is not None:
            self.api_key = os.path.expandvars(self.api_key)

    def to_dict(self):
        return {"url": self.url, "fomat": self.format, "mode": self.mode}


class SourcePublisher(object):
    """Publisher information, eg. the government authority."""

    def __init__(self, config):
        self.url = config.get("url")
        self.title = config.get("title")

    def to_dict(self):
        return {"url": self.url, "title": self.title}


class Source(Dataset):
    """A source to be included in OpenSanctions, backed by a crawler that can
    acquire and transform the data.
    """

    TYPE = "source"

    def __init__(self, file_path, config):
        super().__init__(self.TYPE, file_path, config)
        self.url = config.get("url", "")
        self.country = config.get("country", "zz")
        self.category = config.get("category", "other")
        self.entry_point = config.get("entry_point")
        self.data = SourceData(config.get("data", {}))
        self.publisher = SourcePublisher(config.get("publisher", {}))

    @property
    def sources(self):
        return set([self])

    @property
    def method(self):
        """Load the actual crawler code behind the dataset."""
        method = "crawl"
        package = self.entry_point 
        # the entry point is the relevant crawler .py file in the crawlers directory for each source, config found in each source .yaml config fields
        # e.g. entry_point: opensanctions.crawlers.un_sc_sanctions in un_sc_sanctions.yml
        # by default the method is "crawl", so it would be calling the "crawl" method from the relevant crawler package (.py file) through getattr.
        # since it is a @property function, it is actually a getter to return the crawl() function and call it from Context class line self.dataset.method(self)
        # as self.dataset.crawl(). Since crawl(context) function in crawler .py takes context argument, it is actually then passing the (self) in .method(self)
        # actually as the context argument in crawl(context).
        if package is None:
            raise RuntimeError("The dataset has no entry point!")
        if ":" in package:
            package, method = package.rsplit(":", 1)
        module = import_module(package)
        return getattr(module, method)

    def to_dict(self):
        data = super().to_dict()
        data.update(
            {
                "url": self.url,
                "country": self.country,
                "entry_point": self.entry_point,
                "data": self.data.to_dict(),
                "publisher": self.publisher.to_dict(),
            }
        )
        return data

    
    #---------------MOD--------------
    
    
    @property
    def get_date_method(self):
        """Load the actual crawler code behind the dataset."""
        method = "get_date" #MOD 
        package = self.entry_point 

        if package is None:
            raise RuntimeError("The dataset has no entry point!")
        if ":" in package:
            #package, method = package.rsplit(":", 1)
            package, _ = package.rsplit(":", 1) #MOD
                
        module = import_module(package)
        return getattr(module, method)