from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataScrapperConfig:
    root_dir: Path
    source_URL: str
    data_dir: Path
    meta_data_dir: Path
    search_string: str