import argparse
import subprocess
import sys
import xml.etree.ElementTree as ET

import yaml


def read_yaml_file(filename):
    """
    Read YAML from a filename
    """
    with open(filename, "r") as file:
        data = yaml.safe_load(file)
    return data


def parse_indices(index_str):
    """
    This can parse a number or range into actual indices.
    E.g., 0, 0-3, etc.
    """
    indices = set()
    for part in index_str.split(","):
        if "-" in part:
            start, end = map(int, part.split("-"))
            indices.update(range(start, end + 1))
        else:
            indices.add(int(part))
    return sorted(list(indices))


class TopologyParser:
    """
    A class to fetch, parse, and analyze hwloc topology data.
    """

    def __init__(self):
        self.xml_content = None
        self.core_map = {}
        self.numa_map = {}

    def load_from_file(self, file_path):
        """
        Load from XML file.
        """
        print(f"Reading topology from provided file: {file_path}")
        self.xml_content = read_yaml_file(file_path)
        self.parse()

    def load_from_lstopo(self):
        """
        Use lstopo to derive topology of running system.
        """
        print("No topology file provided. Generating live topology using 'lstopo'...")
        command = ["lstopo", "-p", "--no-io", "--output-format", "xml"]
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        self.xml_content = result.stdout
        self.parse()

    def parse(self):
        """
        Parse the xml topology into cores, pus, etc.
        """
        if not self.xml_content:
            raise RuntimeError("Cannot parse, no XML content loaded.")

        root = ET.fromstring(self.xml_content)
        self.core_map = {}
        self.numa_map = {}

        # Each core has some number of PU (processing units) under it.
        # I think for affinity we need to care about both, although notably
        # flux only is looking at the level of a core.
        for element in root.findall('.//object[@type="Core"]'):
            try:
                core_id = int(element.get("os_index"))
                pu_ids = [
                    int(pu.get("os_index"))
                    for pu in element.findall('./object[@type="PU"]')
                ]
                if pu_ids:
                    self.core_map[core_id] = sorted(pu_ids)
            except Exception as e:
                print(f"Warning: issue with {element}: {e}")

        # The numa nodes contain the Core -> PU units
        for element in root.findall('.//object[@type="NUMANode"]'):
            try:
                numa_id = int(element.get("os_index"))
                cpuset = element.get("cpuset")
                if cpuset:
                    self.numa_map[numa_id] = cpuset
            except Exception as e:
                print(f"Warning: issue with {element}: {e}")

    def evaluate_expression(self, expression_list):
        """
        Calculates a list of cpusets by splitting the expression into groups
        using 'AND' as a separator. Can also handle one group, obviously.
        """
        if not expression_list:
            return []

        groups = []
        current_group = []
        for item in expression_list:
            # numa:0 x core:0-1 AND numa:0 + core:2-3
            if item.upper() == "AND":
                if not current_group:
                    raise ValueError(
                        "Found 'AND' separator without a preceding group expression."
                    )
                groups.append(current_group)
                # Reset for next group
                current_group = []
            else:
                current_group.append(item)

        # Append the final group after the loop finishes
        if not current_group:
            raise ValueError("Expression cannot end with a dangling 'AND' separator.")
        groups.append(current_group)

        # Evaluate each group expression to get its integer mask
        # A group can share the same mask (they have access to same cpuset)
        # so we need to include the order (we assume to be rank)
        final_masks = {}
        for i, group in enumerate(groups):
            mask_int = self.evaluate_group(group)
            expression = " ".join(group)
            final_masks[f"{i} {expression}"] = mask_int
        return final_masks

    def evaluate_group(self, expression_list):
        """
        Calculate a final cpuset by evaluating an expression of locations and operators.
        E.g., an expression list will be like ["numa:0", "x", "core:0-7"]. See the comments
        below for examples.
        """
        if not expression_list:
            return "0x00000000"

        # Start with the mask of the very first location.
        current_mask_int = self.calculate_cpuset_for_location(expression_list[0])

        # Create an iterator for the rest of the list, which should be operator-location pairs.
        it = iter(expression_list[1:])

        for operator in it:
            try:
                # After an operator, there MUST be another location.
                location = next(it)
            except StopIteration:
                raise ValueError(
                    f"Error: Operator '{operator}' must be followed by a location."
                )

            location_mask_int = self.calculate_cpuset_for_location(location)

            # Union: "I want all of NUMA node 0 for my main computation, but I also need Core 15 on
            # NUMA node 1 because it's physically closest to the infiniband device"
            if operator == "+":
                current_mask_int |= location_mask_int

            # Intersection:  "I want the first two physical cores that are located on NUMA node 0."
            elif operator == "x":
                current_mask_int &= location_mask_int

            # XOR: "My job was supposed to run on NUMA node 0, but I think it might have spilled onto other CPUs.
            # I have the actual mask from the running job. Show me which CPUs are mismatched."
            elif operator == "^":
                current_mask_int ^= location_mask_int

            # Difference:  "I want to run my simulation on the entire machine, BUT NOT on Core 0,
            # who is a jerk, or I'm reserving for the OS, SSH, and monitoring daemons."
            elif operator == "~":
                current_mask_int &= ~location_mask_int
            else:
                raise ValueError(
                    f"Error: Unknown operator '{operator}'. Use '+', 'x', '^', or '~'."
                )

        # Important: if you return hex here, it is in a different format than
        # what hwloc will return (e.g., like 0xf vs the same with more 0s)
        return f"0x{current_mask_int:08x}"

    def calculate_cpuset_for_location(self, location_str):
        """
        Private helper to calculate the cpuset for a single location string.
        Returns the mask as an INTEGER for easy combination.
        """
        if location_str.lower().startswith("0x"):
            return int(location_str, 16)

        # Give the user feedback the input provided sucks
        try:
            obj_type, obj_index_str = location_str.lower().split(":", 1)
        except Exception as e:
            raise e

        try:
            indices = parse_indices(obj_index_str)
        except ValueError:
            print(f"Error: Invalid index format in '{location_str}'.", file=sys.stderr)
            return None

        target_pus = set()

        # We expect to get a pu, numa, or core. We can eventually add gpus to this.
        # But I'm not sure how they show up in the topology / if the logic is the same.
        if obj_type == "numa":
            mask_int = 0
            for index in indices:
                if index not in self.numa_map:
                    raise ValueError(
                        f"Error: NUMA node index {index} not found.", file=sys.stderr
                    )
                mask_int |= int(self.numa_map[index], 16)
            return mask_int

        elif obj_type == "core":
            for index in indices:
                if index not in self.core_map:
                    raise ValueError(f"Error: Core index {index} not found.")
                target_pus.update(self.core_map[index])

        elif obj_type == "pu":
            max_pu = self.num_pus - 1
            for index in indices:
                if not (0 <= index <= max_pu):
                    raise ValueError(
                        f"Error: PU index {index} is out of range (0-{max_pu})."
                    )
            target_pus.update(indices)
        else:
            raise ValueError(
                f"Error: Unsupported type '{obj_type}'. Use 'numa', 'core', or 'pu'."
            )

        final_mask_int = 0
        for pu_id in target_pus:
            final_mask_int |= 1 << pu_id
        return final_mask_int

    @property
    def num_cores(self):
        """
        Number of physical cores found in the topology.
        """
        return len(self.core_map)

    @property
    def num_pus(self):
        """
        Total number of Processing Units (PUs) found.
        """
        return sum(len(pus) for pus in self.core_map.values())

    def predict_binding_outputs(self, cpuset_mask_str):
        """
        Predicts the logical PU list and the fully-contained core list for a
        given cpuset mask.
        """
        # Guard clause: if the topology hasn't been parsed, we can't make predictions.
        if not self.core_map:
            raise RuntimeError("Topology not parsed. Cannot make predictions.")

        # Convert the input hexadecimal string (e.g., '0xf') into an integer (e.g., 15).
        # The base '16' tells the int() function to interpret the string as hexadecimal.
        mask_int = int(cpuset_mask_str, 16)

        # Create a set of all PU indices that are "active" in the cpuset mask.
        # For each 'i', check if the i-th bit in the mask integer is set to 1.
        # (mask_int >> i): right-shift the mask so the bit for PU 'i' is in the last position.
        # & 1: Performs a bitwise AND with 1 to isolate that last bit.
        # If the result is 1, PU 'i' is part of the binding and is added to the set.
        bound_pus_set = {i for i in range(self.num_pus) if (mask_int >> i) & 1}

        # First prediction: the 'logical_cpu_list' (like 'taskset -c')
        # To create the human-readable string of PUs (like '0,1,2,3'):
        # 1. Convert the set of bound PUs into a sorted list
        logical_cpu_list = ",".join(map(str, sorted(list(bound_pus_set))))

        # Prediction 2: The 'cores' list (like 'hwloc-calc --intersect core')
        fully_contained_cores = []

        # Iterate through each physical core that was found in the topology.
        # We sort by core_id to ensure the final output list is also sorted.
        # I think this is OK to do.
        for core_id, pus_in_core in sorted(self.core_map.items()):
            # Check if a core is "fully contained" meaning all PUs are in the subset of
            # the known PUs for the core (allowed by the PU set mask)
            # E.g., if core 0 requires PUs {0, 1} and the mask allows {0, 1, 2, 3},
            # then {0, 1} is a subset of {0, 1, 2, 3}, and the condition is true.
            if set(pus_in_core).issubset(bound_pus_set):
                # If the core is fully contained, add its ID to our results list.
                fully_contained_cores.append(core_id)

        # Let's consider both.
        cores_list = ",".join(map(str, fully_contained_cores))
        return logical_cpu_list, cores_list

    def __str__(self):
        return f"Topology with {len(self.numa_map)} NUMA nodes, {self.num_cores} cores, and {self.num_pus} PUs."


def main():
    parser = argparse.ArgumentParser(
        description="Predict hwloc bindings from a high-level expression of locations and operators.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""Operators:
  +     UNION (OR) of two cpusets
  x     INTERSECTION (AND) of two cpusets
  ^     XOR (symmetric difference) of two cpusets
  ~     DIFFERENCE (cpus in first set but not second)

Example: python3 predict_topology.py numa:0 x core:0-7""",
    )
    parser.add_argument(
        "-t",
        "--topology-file",
        help="Path to a lstopo XML file.\nIf not provided, runs 'lstopo' live.",
    )
    parser.add_argument(
        "-m",
        "--mask-only",
        action="store_true",
        help="Only output the calculated hexadecimal cpuset mask.",
    )
    parser.add_argument(
        "expression", nargs="+", help="An expression of locations and operators."
    )

    args = parser.parse_args()
    topology = TopologyParser()

    if args.topology_file:
        topology.load_from_file(args.topology_file)
    else:
        topology.load_from_lstopo()

    print(f"Successfully parsed: {topology}.")

    # These are calculated masks, with index by the expression
    masks = topology.evaluate_expression(args.expression)
    if args.mask_only:
        print(masks)
        sys.exit(0)

    for group, mask in masks.items():
        logical_list, core_list = topology.predict_binding_outputs(mask)
        print("\n" + "=" * 50)
        print(f"Group {group} resolved to Combined Cpuset Mask: {mask}")
        print("=" * 50)
        print("Predicted 'logical_cpu_list' (PUs in mask):")
        print(f"  -> {logical_list or '(none)'}\n")
        print("Predicted 'cores' list (fully contained):")
        print(f"  -> {core_list or '(none)'}")
        print("=" * 50)


if __name__ == "__main__":
    main()
