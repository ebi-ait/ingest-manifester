class BundleFileUploadError(Exception):
    """There was a failure in bundle file upload."""


class InvalidBundleError(Exception):
    """There was a failure in bundle validation."""


class NoUploadAreaFoundError(Exception):
    """Export couldn't be as no upload area found"""


class FileDuplication(Exception):

    def __init__(self, staging_area_uuid, file_name):
        message = f'A file with name "{file_name}" already exists in staging area ' \
                  f'{staging_area_uuid}.'
        super(FileDuplication, self).__init__(message)
