import importlib.util
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch


def load_incubator_module():
    module_path = Path(
        "/home/tao/workspace/reef-imaging/reef_imaging/control/cytomat-control/start_hypha_service_incubator.py"
    )
    spec = importlib.util.spec_from_file_location("reef_test_incubator_service", module_path)
    module = importlib.util.module_from_spec(spec)
    fake_cytomat = types.SimpleNamespace(Cytomat=object)
    with patch.dict(sys.modules, {"cytomat": fake_cytomat}):
        spec.loader.exec_module(module)
    return module


class IncubatorServiceTests(unittest.TestCase):
    def test_get_well_plate_type_reads_samples_json(self):
        module = load_incubator_module()
        service = module.IncubatorService(local=True, simulation=True)

        with tempfile.TemporaryDirectory() as temp_dir:
            samples_path = Path(temp_dir) / "samples.json"
            samples_path.write_text(
                json.dumps(
                    [
                        {"incubator_slot": 1, "well_plate_type": "384"},
                        {"incubator_slot": 2, "well_plate_type": "96"},
                    ]
                ),
                encoding="utf-8",
            )
            service.samples_file = str(samples_path)

            self.assertEqual(service.get_well_plate_type(1), "384")
            self.assertEqual(service.get_well_plate_type(), {1: "384", 2: "96"})
