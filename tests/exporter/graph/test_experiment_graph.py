from unittest import TestCase

from exporter.graph.experiment_graph import LinkSet, ProcessLink, SupplementaryFileLink, SupplementedEntity


class TestUtils:
    @staticmethod
    def gen_process_link(uuid: str) -> ProcessLink:
        return ProcessLink(uuid, "process", [], [], [])

    @staticmethod
    def gen_supplementary_file_link(uuid: str) -> SupplementaryFileLink:
        return SupplementaryFileLink(SupplementedEntity("some_concrete_type", uuid), [])


class ExperimentGraphTest(TestCase):

    def test_link_set_no_duplicates(self):
        link_set = LinkSet()
        link_set.add_links([TestUtils.gen_process_link("mock-process-uuid-1"),
                            TestUtils.gen_process_link("mock-process-uuid-2"),
                            TestUtils.gen_process_link("mock-process-uuid-2"),
                            TestUtils.gen_supplementary_file_link("mock-entity-uuid-1"),
                            TestUtils.gen_supplementary_file_link("mock-entity-uuid-2"),
                            TestUtils.gen_supplementary_file_link("mock-entity-uuid-2")])

        self.assertEqual(len(link_set.get_links()), 4)

    def test_link_set_to_dict(self):
        link_set = LinkSet()
        link_set.add_links([TestUtils.gen_process_link("mock-process-uuid-1"),
                            TestUtils.gen_process_link("mock-process-uuid-2"),
                            TestUtils.gen_supplementary_file_link("mock-entity-uuid-1")])

        links_dict = link_set.to_dict()
        suppl_link_dicts = [link_dict for link_dict in links_dict
                            if link_dict["link_type"] == "supplementary_file_link"]

        process_link_dicts = [link_dict for link_dict in links_dict
                              if link_dict["link_type"] == "process_link"]

        self.assertEqual(len(suppl_link_dicts), 1)
        self.assertEqual(len(process_link_dicts), 2)
