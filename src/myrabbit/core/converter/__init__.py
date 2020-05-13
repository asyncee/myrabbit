from .converter import Converter
from .dataclass import DataclassConverter
from .pydantic import PydanticConverter

DEFAULT_CONVERTERS = [DataclassConverter(), PydanticConverter()]
