import warnings

warnings.filterwarnings(action="ignore", category=RuntimeWarning, module=r".schema.*")
warnings.filterwarnings("ignore", category=RuntimeWarning, message="fields may not start with an underscore")
