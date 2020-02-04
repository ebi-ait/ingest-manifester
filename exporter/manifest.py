from ingest.api.ingestapi import IngestApi
from ingest.exporter.exceptions import MultipleProjectsError

_BUNDLE_FILE_TYPE_DATA = 'data'
_BUNDLE_FILE_TYPE_LINKS = 'links'

_metadata_type_attr_map = {
    'biomaterial': 'fileBiomaterialMap',
    'file': 'fileFilesMap',
    'process': 'fileProcessMap',
    'project': 'fileProjectMap',
    'protocol': 'fileProtocolMap',
}


class ProcessInfo:
    def __init__(self):
        self.project = {}

        # uuid => object mapping
        self.input_biomaterials = {}
        self.derived_by_processes = {}
        self.input_files = {}
        self.derived_files = {}
        self.protocols = {}
        self.supplementary_files = {}
        self.input_bundle = None


# TODO Reconsider to not do this for the archiver
# If needed design the assay manifest structure
# Use Bundle Manifest for now, bundle uuid and version will be null
class AssayManifest:
    def __init__(self, envelopeUuid=None):
        self.envelopeUuid = envelopeUuid if envelopeUuid is not None else ''
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


class AssayManifestGenerator:
    def __init__(self, ingest_api: IngestApi):
        self.ingest_api = ingest_api
        self.related_entities_cache = {}

    def generate_manifest(self, process_uuid: str, submission_uuid: str):
        process = self.ingest_api.get_entity_by_uuid('processes', process_uuid)
        process_info = self.get_all_process_info(process)
        assay_manifest = self.build_assay_manifest(process_info, submission_uuid)
        assay_manifest_resource = self.ingest_api.create_bundle_manifest(assay_manifest)
        return assay_manifest_resource

    def get_all_process_info(self, process_resource):
        process_info = ProcessInfo()
        process_info.input_bundle = self._get_input_bundle(process_resource)

        process_info.project = self._get_project_info(process_resource)

        if not process_info.project:  # get from input bundle
            project_uuid_lists = list(process_info.input_bundle['fileProjectMap'].values())

            if not project_uuid_lists and not project_uuid_lists[0]:
                raise Exception('Input bundle manifest has no list of project uuid.')  # very unlikely to happen

            project_uuid = project_uuid_lists[0][0]
            process_info.project = self.ingest_api.get_project_by_uuid(project_uuid)

        self._recurse_process(process_resource, process_info)

        if process_info.project:
            supplementary_files = self.ingest_api.get_related_entities('supplementaryFiles', process_info.project,
                                                                       'files')
            for supplementary_file in supplementary_files:
                uuid = supplementary_file['uuid']['uuid']
                process_info.supplementary_files[uuid] = supplementary_file

        return process_info

    def build_assay_manifest(self, process_info: ProcessInfo, submission_uuid: str):
        metadata_by_type = self._get_metadata_by_type(process_info)
        assay_manifest = AssayManifest()
        assay_manifest.envelopeUuid = submission_uuid

        assay_manifest.fileProjectMap = dict()
        for (metadata_uuid, doc) in metadata_by_type['project'].items():
            assay_manifest.fileProjectMap[metadata_uuid] = [metadata_uuid]

        assay_manifest.fileBiomaterialMap = dict()
        for (metadata_uuid, doc) in metadata_by_type['biomaterial'].items():
            assay_manifest.fileBiomaterialMap[metadata_uuid] = [metadata_uuid]

        assay_manifest.fileProcessMap = dict()
        for (metadata_uuid, doc) in metadata_by_type['process'].items():
            assay_manifest.fileProcessMap[metadata_uuid] = [metadata_uuid]

        assay_manifest.fileProtocolMap = dict()
        for (metadata_uuid, doc) in metadata_by_type['protocol'].items():
            assay_manifest.fileProtocolMap[metadata_uuid] = [metadata_uuid]

        assay_manifest.fileFilesMap = dict()
        for (metadata_uuid, doc) in metadata_by_type['file'].items():
            assay_manifest.fileFilesMap[metadata_uuid] = [metadata_uuid]

        files = metadata_by_type['file']

        assay_manifest.dataFiles = list()
        assay_manifest.dataFiles = assay_manifest.dataFiles = list()
        assay_manifest.dataFiles = [f['uuid']['uuid'] for f in files.values()]

        return assay_manifest

    def _get_metadata_by_type(self, process_info: ProcessInfo) -> dict:
        #  given a ProcessInfo, pull out all the metadata and return as a map of UUID->metadata documents
        simplified = dict()
        simplified['process'] = dict(process_info.derived_by_processes)
        simplified['biomaterial'] = dict(process_info.input_biomaterials)
        simplified['protocol'] = dict(process_info.protocols)
        simplified['file'] = dict(process_info.derived_files)
        simplified['file'].update(process_info.input_files)
        simplified['file'].update(process_info.supplementary_files)

        simplified['project'] = dict()
        simplified['project'][process_info.project['uuid']['uuid']] = process_info.project

        return simplified

    def _get_project_info(self, process):
        projects = list(self.ingest_api.get_related_entities('projects', process, 'projects'))

        if len(projects) > 1:
            raise MultipleProjectsError('Can only be one project in bundle')

        # TODO add checking for project only on an assay process
        # TODO an analysis process may have no link to a project

        return projects[0] if projects else None

    # get all related info of a process
    def _recurse_process(self, process, process_info: ProcessInfo):
        uuid = process['uuid']['uuid']
        process_info.derived_by_processes[uuid] = process

        # get all derived by processes using input biomaterial and input files
        derived_by_processes = []

        # wrapper process has the links to input biomaterials and derived files to check if a process is an assay
        input_biomaterials = self._get_related_entities('inputBiomaterials', process, 'biomaterials')
        for input_biomaterial in input_biomaterials:
            uuid = input_biomaterial['uuid']['uuid']
            process_info.input_biomaterials[uuid] = input_biomaterial
            derived_by_processes.extend(
                self._get_related_entities('derivedByProcesses', input_biomaterial, 'processes'))

        input_files = self._get_related_entities('inputFiles', process, 'files')
        for input_file in input_files:
            uuid = input_file['uuid']['uuid']
            process_info.input_files[uuid] = input_file
            derived_by_processes.extend(
                self._get_related_entities('derivedByProcesses', input_file, 'processes'))

        derived_files = self._get_related_entities('derivedFiles', process, 'files')

        protocols = self._get_related_entities('protocols', process, 'protocols')
        for protocol in protocols:
            uuid = protocol['uuid']['uuid']
            process_info.protocols[uuid] = protocol

        for derived_file in derived_files:
            uuid = derived_file['uuid']['uuid']
            process_info.derived_files[uuid] = derived_file

        for derived_by_process in derived_by_processes:
            self._recurse_process(derived_by_process, process_info)

    def _get_related_entities(self, relationship, entity, entity_type):
        entity_uuid = entity['uuid']['uuid']

        cache = self.related_entities_cache.get(entity_uuid)
        if cache and cache.get(relationship):
            return self.related_entities_cache.get(entity_uuid).get(relationship)

        related_entities = list(self.ingest_api.get_related_entities(relationship, entity, entity_type))

        if not self.related_entities_cache.get(entity_uuid):
            self.related_entities_cache[entity_uuid] = {}

        if not self.related_entities_cache.get(entity_uuid).get(relationship):
            self.related_entities_cache[entity_uuid][relationship] = []

        self.related_entities_cache[entity_uuid][relationship] = related_entities

        return related_entities

    def _get_input_bundle(self, process):
        bundle_manifests = list(
            self.ingest_api.get_related_entities('inputBundleManifests', process, 'bundleManifests'))

        return bundle_manifests[0] if bundle_manifests else None
