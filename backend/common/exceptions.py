class EmptyValueForCachingError(Exception):
    pass


class MissingKeyError(Exception):
    pass


class IncompleteParsingError(Exception):
    pass


class PDFConversionFailedException(Exception):
    pass


class TextExtractionFailedException(Exception):
    pass


class CorruptFileException(Exception):
    pass


class EmptyFileException(Exception):
    pass


class PdfToImageException(Exception):
    pass


class ImageToTextException(Exception):
    pass


class FileSystemException(Exception):
    pass


class BlankFuncParamException(Exception):
    pass


class InvalidAPIKeyException(Exception):
    pass
