import string

from dataclasses import field

from hydra.core.config_store import ConfigStore
from omegaconf import MISSING
from pydantic.dataclasses import dataclass

@dataclass
class SpellCorrectionModelConfig:
    _target_: str = "cybulde.utils.utils.SpellCorrectionModel"
    max_dictionary_edit_distance: int = 2
    prefix_length: int = 7
    count_threshold: int = 1


@dataclass
class DatasetCleanerConfig:
    _target_: str = MISSING


@dataclass
class StopWordsDatasetCleanerConfig(DatasetCleanerConfig):
    _target_: str = "cybulde.data_processing.dataset_cleaners.StopWordsDatasetCleaner"


@dataclass
class ToLowerCaseDatasetCleanerConfig(DatasetCleanerConfig):
    _target_: str = "cybulde.data_processing.dataset_cleaners.ToLowerCaseDatasetCleaner"


@dataclass
class URLDatasetCleanerConfig(DatasetCleanerConfig):
    _target_: str = "cybulde.data_processing.dataset_cleaners.URLDatasetCleaner"


@dataclass
class PunctuationDatasetCleanerConfig(DatasetCleanerConfig):
    _target_: str = "cybulde.data_processing.dataset_cleaners.PunctuationDatasetCleaner"
    punctuation: str = string.punctuation


@dataclass
class NonLettersDatasetCleanerConfig(DatasetCleanerConfig):
    _target_: str = "cybulde.data_processing.dataset_cleaners.NonLettersDatasetCleaner"


@dataclass
class NewLineCharacterDatasetCleanerConfig(DatasetCleanerConfig):
    _target_: str = "cybulde.data_processing.dataset_cleaners.NewLineCharacterDatasetCleaner"


@dataclass
class NonAsciiDatasetCleanerConfig(DatasetCleanerConfig):
    _target_: str = "cybulde.data_processing.dataset_cleaners.NonAsciiDatasetCleaner"


@dataclass
class ReferenceToAccountDatasetCleanerConfig(DatasetCleanerConfig):
    _target_: str = "cybulde.data_processing.dataset_cleaners.ReferenceToAccountDatasetCleaner"


@dataclass
class RetweetDatasetCleanerConfig(DatasetCleanerConfig):
    _target_: str = "cybulde.data_processing.dataset_cleaners.RetweetDatasetCleaner"


@dataclass
class SpellCorrectionDatasetCleanerConfig(DatasetCleanerConfig):
    _target_: str = "cybulde.data_processing.dataset_cleaners.SpellCorrectionDatasetCleaner"
    spell_correction_model: SpellCorrectionModelConfig = SpellCorrectionModelConfig()


@dataclass
class DatasetCleanerManagerConfig:
    _target_: str = "cybulde.data_processing.dataset_cleaners.DatasetCleanerManager"
    dataset_cleaners: dict[str, DatasetCleanerConfig] = field(default_factory=lambda: {})


def setup_config() -> None:
    cs = ConfigStore.instance()

    cs.store(
        group="dataset_cleaner_manager/dataset_cleaner",
        name="stop_words_dataset_cleaner_schema",
        node=StopWordsDatasetCleanerConfig,
    )  # 1

    cs.store(
        group="dataset_cleaner_manager/dataset_cleaner",
        name="to_lower_case_dataset_cleaner_schema",
        node=ToLowerCaseDatasetCleanerConfig,
    )  # 2
    cs.store(
        group="dataset_cleaner_manager/dataset_cleaner", name="url_dataset_cleaner_schema", node=URLDatasetCleanerConfig
    )  # 3
    cs.store(
        group="dataset_cleaner_manager/dataset_cleaner",
        name="punctuation_dataset_cleaner_schema",
        node=PunctuationDatasetCleanerConfig,
    )  # 4
    cs.store(
        group="dataset_cleaner_manager/dataset_cleaner",
        name="non_letters_dataset_cleaner_schema",
        node=NonLettersDatasetCleanerConfig,
    )  # 5

    cs.store(
        group="dataset_cleaner_manager/dataset_cleaner",
        name="new_line_character_dataset_cleaner_schema",
        node=NewLineCharacterDatasetCleanerConfig,
    )  # 6
    cs.store(
        group="dataset_cleaner_manager/dataset_cleaner",
        name="non_ascii_dataset_cleaner_schema",
        node=NonAsciiDatasetCleanerConfig,
    )  # 7
    cs.store(
        group="dataset_cleaner_manager/dataset_cleaner",
        name="reference_to_account_dataset_cleaner_schema",
        node=ReferenceToAccountDatasetCleanerConfig,
    )  # 8
    cs.store(
        group="dataset_cleaner_manager/dataset_cleaner",
        name="re_tweet_dataset_cleaner_schema",
        node=RetweetDatasetCleanerConfig,
    )  # 9
    cs.store(
        group="dataset_cleaner_manager/dataset_cleaner",
        name="spell_correction_dataset_cleaner_schema",
        node=SpellCorrectionDatasetCleanerConfig,
    )  # 10

    cs.store(
        group="dataset_cleaner_manager", name="dataset_cleaner_manager_schema", node=DatasetCleanerManagerConfig
    )  # 11
