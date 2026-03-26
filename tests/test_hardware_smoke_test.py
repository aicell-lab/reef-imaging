import unittest

from reef_imaging.hardware_smoke_test import (
    build_cycle_plan,
    build_hamilton_cycle_plan,
    parse_slot_selection,
    HamiltonCycle,
)


class HardwareSmokeTestHelpers(unittest.TestCase):
    def test_parse_slot_selection_accepts_unique_slots_up_to_limit(self):
        self.assertEqual(parse_slot_selection("1, 3,5", [1, 2, 3, 4, 5]), [1, 3, 5])

    def test_parse_slot_selection_rejects_invalid_requests(self):
        with self.assertRaises(ValueError):
            parse_slot_selection("", [1, 2])
        with self.assertRaises(ValueError):
            parse_slot_selection("1,1", [1, 2])
        with self.assertRaises(ValueError):
            parse_slot_selection("1,2,3,4,5,6", [1, 2, 3, 4, 5, 6])
        with self.assertRaises(ValueError):
            parse_slot_selection("9", [1, 2, 3])

    def test_build_cycle_plan_expands_samples_across_all_microscopes(self):
        cycles = build_cycle_plan(
            [
                {"incubator_slot": 1, "name": "alpha", "well_plate_type": "96"},
                {"incubator_slot": 2, "name": "beta", "well_plate_type": "384"},
            ],
            ["microscope-squid-1", "microscope-squid-2"],
        )

        self.assertEqual(
            [(cycle.incubator_slot, cycle.microscope_id) for cycle in cycles],
            [
                (1, "microscope-squid-1"),
                (1, "microscope-squid-2"),
                (2, "microscope-squid-1"),
                (2, "microscope-squid-2"),
            ],
        )

    def test_build_hamilton_cycle_plan_incubator_only(self):
        samples = [{"incubator_slot": 1, "name": "alpha"}]
        cycles = build_hamilton_cycle_plan(samples, "incubator_only")
        
        self.assertEqual(len(cycles), 2)
        self.assertEqual(cycles[0].cycle_type, "incubator_to_hamilton")
        self.assertEqual(cycles[1].cycle_type, "hamilton_to_incubator")
        self.assertIsNone(cycles[0].microscope_id)

    def test_build_hamilton_cycle_plan_microscope_only(self):
        samples = [{"incubator_slot": 1, "name": "alpha"}]
        cycles = build_hamilton_cycle_plan(samples, "microscope_only", ["microscope-squid-1"])
        
        self.assertEqual(len(cycles), 2)
        self.assertEqual(cycles[0].cycle_type, "hamilton_to_microscope")
        self.assertEqual(cycles[1].cycle_type, "microscope_to_hamilton")
        self.assertEqual(cycles[0].microscope_id, "microscope-squid-1")

    def test_build_hamilton_cycle_plan_full(self):
        samples = [{"incubator_slot": 1, "name": "alpha"}]
        cycles = build_hamilton_cycle_plan(samples, "full", ["microscope-squid-1"])
        
        self.assertEqual(len(cycles), 4)
        self.assertEqual(cycles[0].cycle_type, "incubator_to_hamilton")
        self.assertEqual(cycles[1].cycle_type, "hamilton_to_microscope")
        self.assertEqual(cycles[2].cycle_type, "microscope_to_hamilton")
        self.assertEqual(cycles[3].cycle_type, "hamilton_to_incubator")


if __name__ == "__main__":
    unittest.main()
