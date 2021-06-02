import yaml
from banal import ensure_list
from ftmstore import get_dataset as get_store

from opensanctions import settings
from opensanctions.core.lookup import Lookup
from opensanctions.util import joinslug


class Dataset(object):
    """A dataset is a unit of execution of crawlers, and a grouping of entities.
    There are two types: sources (which relate to a specific crawlers), and
    collections (which group sources into more useful units)."""

    ALL = "all"

    def __init__(self, type_, file_path, config):
        self.type = type_
        self.file_path = file_path
        self.name = config.get("name", file_path.stem)
        self.prefix = config.get("prefix", self.name)
        self.title = config.get("title", self.name)
        self.description = config.get("description", "")

        # Collections can be part of other collections.
        collections = ensure_list(config.get("collections"))
        if self.name != self.ALL:
            collections.append(self.ALL)
        self.collections = set(collections)

        self.lookups = {}
        for name, lconfig in config.get("lookups", {}).items():
            self.lookups[name] = Lookup(name, lconfig)

    def make_slug(self, *parts, strict=True):
        return joinslug(*parts, prefix=self.prefix, strict=strict)

    @property
    def datasets(self):
        return set([self])

    @property
    def store(self):
        name = f"{self.type}_{self.name}"
        return get_store(name, database_uri=settings.DATABASE_URI)

    @classmethod
    def _from_metadata(cls, file_path):
        from opensanctions.core.source import Source
        from opensanctions.core.collection import Collection

        with open(file_path, "r") as fh:
            config = yaml.load(fh, Loader=yaml.SafeLoader)

        type_ = config.get("type", Source.TYPE)
        type_ = type_.lower().strip()
        if type_ == Collection.TYPE:
            return Collection(file_path, config)
        if type_ == Source.TYPE:
            return Source(file_path, config)

    @classmethod
    def _load_cache(cls):
        if not hasattr(cls, "_cache"):
            cls._cache = {}
            for glob in ("**/*.yml", "**/*.yaml"):
                for file_path in settings.METADATA_PATH.glob(glob):
                    dataset = cls._from_metadata(file_path)
                    cls._cache[dataset.name] = dataset
        return cls._cache

    @classmethod
    def all(cls):
        return cls._load_cache().values()

    @classmethod
    def get(cls, name):
        '''INFO load_cache, if cls._cache not already exist, calls cls._from_metadata(file_path) for each file_path. This opens the relevant yaml.
        if it is a collection yaml, it will have TYPE "collection", otherwise it won't have a "type" field, and the config.get("type", Source.TYPE)
        will give Source.TYPE constant from Source class (which = 'source') to the type_ var. thus, base of TYPE, it will return either a Collection
        or a Source object. These objects are children of the Dataset class, in fact "file_path" & "config" are passed as they are needed in the 
        call of super().__init__ Dataset constructor.
        Collection type has .datasets and .sources, and does some things to group and use and all of that. (maybe check more layer) 
        The Source class has the methods .method(self), sources, and to_dict.
        the .method(self) is called in Context(source).Crawl()'''
        return cls._load_cache().get(name) # get here is dictionary method, cos _load_cache() creates dict if not exists & returns

    @classmethod
    def names(cls):
        """An array of all dataset names found in the metadata path."""
        return list(sorted((dataset.name for dataset in cls.all())))

    def to_dict(self):
        return {
            "name": self.name,
            "type": self.type,
            "title": self.title,
            "description": self.description,
        }

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.type + self.name)
