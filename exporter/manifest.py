_BUNDLE_FILE_TYPE_DATA = 'data'
_BUNDLE_FILE_TYPE_LINKS = 'links'

_metadata_type_attr_map = {
    'biomaterial': 'fileBiomaterialMap',
    'file': 'fileFilesMap',
    'process': 'fileProcessMap',
    'project': 'fileProjectMap',
    'protocol': 'fileProtocolMap',
}


class FileManifest:

    def __init__(self, envelopeUuid=None):
        self.envelopeUuid = envelopeUuid if envelopeUuid is not None else {}
        self.dataFiles = []
        self.fileBiomaterialMap = {}
        self.fileProcessMap = {}
        self.fileFilesMap = {}
        self.fileProjectMap = {}
        self.fileProtocolMap = {}

    def add_bundle_file(self, metadata_type, entry: dict):
        if metadata_type == _BUNDLE_FILE_TYPE_LINKS:
            pass  # we don't want to track links.json in BundleManifests
        elif metadata_type == _BUNDLE_FILE_TYPE_DATA:
            self.dataFiles.extend(entry.keys())
        else:
            attr_mapping = _metadata_type_attr_map.get(metadata_type)
            if attr_mapping:
                file_map = getattr(self, attr_mapping)
                file_map.update(entry)
            else:
                raise KeyError(f'Cannot map unknown metadata type [{metadata_type}].')
