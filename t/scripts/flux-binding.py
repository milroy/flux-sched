#!/usr/bin/env python3

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
import threading
from collections import defaultdict

here = os.path.abspath(os.path.dirname(__file__))
show_topology = os.path.join(here, "shell", "binding", "show_topology.sh")
report_mapping = os.path.join(here, "shell", "binding", "report_mapping.sh")
bash = shutil.which("bash")

for script in [show_topology, report_mapping]:
    if not os.path.exists(script):
        raise ValueError(f"{script} does not exist.")

if not bash:
    raise ValueError("Cannot find bash command... uhh.")


def discover_topology(faux_xml=None):
    """
    Discovery the hardware topology (the actual, or a provided xml that is faux)
    and return as a topology lookup of sorts.
    """
    print("🥸  Discovering Ground Truth Hardware Topology")
    try:
        # If the user provides an xml to simulate, use that instead
        cmd = [bash, show_topology]
        if faux_xml is not None:
            cmd.append(faux_xml)

        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    except Exception as e:
        sys.exit(f"Could not execute topology show script: {e}")

    # The cores are cpusets
    topology = defaultdict(lambda: {"cores": {}, "gpus": []})
    for line in result.stdout.strip().split("\n"):
        if not line.startswith("NUMA_NODE"):
            continue

        parts = shlex.split(line)
        numa_id = -1

        for part in parts:
            if part.startswith("NUMA_NODE="):
                numa_id = int(part.split("=")[1])
            # This now correctly looks for "_CPUSET="
            elif part.startswith("CORE") and "_CPUSET=" in part:
                core_part, cpuset = part.split("_CPUSET=")
                core_id = int(core_part.replace("CORE", ""))
                if numa_id != -1:
                    topology[numa_id]["cores"][core_id] = cpuset
            elif part.startswith("GPUS="):
                gpus_str = part.split("=")[1]
                gpus = [g.replace("GPU:", "") for g in gpus_str.split(",") if g]
                if numa_id != -1:
                    topology[numa_id]["gpus"] = sorted(gpus)

    print(f"{json.dumps(topology, indent=4)}")
    return topology


def stream(cmd):
    """
    Executes a command and streams its stdout and stderr to the console
    """
    captured_stdout = []
    captured_stderr = []
    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8"
    )

    def reader_thread(pipe, output_list, prefix):
        try:
            for line in pipe:
                line_stripped = line.strip()
                print(line_stripped, flush=True)
                output_list.append(line_stripped)
        finally:
            pipe.close()

    # Last argument is a prefix
    stdout_thread = threading.Thread(
        target=reader_thread, args=(process.stdout, captured_stdout, "")
    )
    stderr_thread = threading.Thread(
        target=reader_thread, args=(process.stderr, captured_stderr, "")
    )
    stdout_thread.start()
    stderr_thread.start()
    stdout_thread.join()
    stderr_thread.join()
    return_code = process.wait()
    return return_code, "\n".join(captured_stdout) + "\n".join(captured_stderr)


def predict_layout(topology, num_tasks, cores_per_task=None, match_policy="low"):
    """
    Predicts which physical core each rank will be bound to based on the exclusivity.
    This is more of an example of a test - we could have other flux commands like it.
    It assumes we are in a flux instance.
    """
    cores_per_task = cores_per_task or 1
    print(
        f"\nPredicting Layout for {num_tasks} Tasks with {cores_per_task} core(s) each ---"
    )

    # Create a flat, sorted list of all available Core objects.
    available_cores = []
    for numa_id in sorted(topology.keys()):
        for core_id, cpuset in sorted(topology[numa_id]["cores"].items()):
            available_cores.append({"id": core_id, "cpuset": cpuset})

    if num_tasks * cores_per_task > len(available_cores):
        print(
            f"Warning: Requesting more total cores ({num_tasks * cores_per_task}) than available ({len(available_cores)})."
        )

    predictions = {}

    # If match policy is high, reverse the cpuset list
    # E.g., this will take 0-7 and reverse to 7-0
    if match_policy == "high":
        available_cores.reverse()

        # Get the number of tasks, we will select from these cpuset
        # E.g,. this will select 6-7
        available_cores = available_cores[0:num_tasks]

        # Reverse back, because the first flux rank will select still from the lowest cpuset
        available_cores.reverse()

    core_idx = 0
    for i in range(num_tasks):  # For each task (rank)
        # This assumes cores_per_task=1 for this test.
        if core_idx < len(available_cores):
            cpuset = available_cores[core_idx]["cpuset"]
            core_id_str = subprocess.run(
                ["hwloc-calc", "--quiet", "--intersect", "core", cpuset],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            print(f"hwloc-calc --pulist {cpuset}")
            predictions[i] = cpuset + f" ({core_id_str})"
            core_idx += 1

    print(f"Predicted cpuset for each rank: {predictions}")
    return predictions


def run_and_capture(
    nodes, tasks, exclusive=False, cores_per_task=None, cpu_affinity=None
):
    """
    Runs the Flux job and captures the binding report from each rank.
    """
    print(f"\nRunning Experiment with {nodes} nodes and {tasks} tasks")
    cmd = ["flux", "run", "-N", str(nodes), "-n", str(tasks)]
    if cpu_affinity is not None:
        cmd += ["-o", "cpu-affinity=per-task"]
    if exclusive:
        cmd.append("--exclusive")
    if cores_per_task is not None:
        cmd += ["--cores-per-task", str(cores_per_task)]

    # We are basically running <flux> <report-mapping-script>
    # Flux starts the job, and the job inherits some mapping of cpusets.
    # We then use hwloc tools to inspect that mapping. The match policy
    # is important and expected to be consistent in the tests (low)
    cmd += [bash, report_mapping]
    print(f"Executing: {shlex.join(cmd)}")

    try:
        return_code, raw_output = stream(cmd)
    except Exception as e:
        sys.exit(e.stderr)

    if return_code != 0:
        print("Warning, application did not return with 0 exit code.")
    print("\nParsing actual bindings from job output...")
    actual_layout = {}

    for line in raw_output.strip().split("\n"):
        parts = line.split()
        if "PEWPEWPEW" not in line:
            continue

        # PEWPEWPEW
        parts.pop(0)
        print(" " + " ".join(parts))
        rank_id, node_name, cpuset, cores = parts
        rank_id = int(rank_id)
        actual_layout[rank_id] = cpuset + f" ({cores})"

    print(f"Actual core for each rank: {actual_layout}")
    return actual_layout


def validate_results(predictions, actual_layout):
    """
    Compares the predicted layout to the actual layout and reports PASS/FAIL.
    """
    print("\n4. Validating Results")

    if not predictions or not actual_layout:
        print("🔴 Could not generate predictions or get actual results.")
        return False

    mismatches = 0
    all_ranks = sorted(set(predictions.keys()) | set(actual_layout.keys()))

    for rank in all_ranks:
        predicted_core = predictions.get(rank, "N/A")
        actual_core = actual_layout.get(rank, "N/A")

        if predicted_core == actual_core:
            print(f"🟢 Rank {rank} - Bound to physical core {actual_core}")
        else:
            print(
                f"🔴 Rank {rank} - Predicted cpuset: {predicted_core}, Actual cpuset: {actual_core}"
            )
            mismatches += 1

    if mismatches == 0:
        print("\n🎉 Implicit exclusivity and dense packing are working as predicted.")
        return True
    else:
        print(f"\n❌ Found {mismatches} binding mismatches.")
        return False


def get_parser():
    parser = argparse.ArgumentParser(
        description="Validate Flux's binding behavior.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    # This isn't currently used because we need to actually bind
    parser.add_argument("--xml", help="Path to a faux XML file")
    parser.add_argument(
        "--exclusive",
        help="Request exclusive",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--cpu-affinity",
        default=None,
        help="Add cpu-affinity",
        choices=["none", "per-task"],
    )
    # Note that this is only relevant if prediction is done
    parser.add_argument(
        "--match-policy",
        default="low",
        help="The match policy.",
        choices=["low", "high"],
    )
    parser.add_argument(
        "-N", "--nodes", type=int, default=1, help="The number of nodes (default: 1)."
    )
    parser.add_argument(
        "-n",
        "--ntasks",
        dest="tasks",
        type=int,
        default=2,
        help="The number of tasks to use for the test run (default: 2).",
    )
    parser.add_argument(
        "-c",
        "--cores-per-task",
        type=int,
        default=None,
        help="The number of CORES (not PUs) to bind per task.",
    )
    return parser


def main():
    parser = get_parser()
    args, _ = parser.parse_known_args()
    # Since we need to look at binding, we need to look at actual topology
    topology = discover_topology(args.xml)
    # It's OK if it changes as long as we can predict it
    predictions = predict_layout(
        topology, args.tasks, args.cores_per_task, args.match_policy
    )
    actual_layout = run_and_capture(
        args.nodes,
        args.tasks,
        exclusive=args.exclusive,
        cores_per_task=args.cores_per_task,
        cpu_affinity=args.cpu_affinity,
    )
    success = validate_results(predictions, actual_layout)
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
