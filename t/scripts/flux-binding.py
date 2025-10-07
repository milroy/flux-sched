#!/usr/bin/env python3

import argparse
import yaml
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
import threading

here = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, here)

# Yes, I did that intentionally. This is porcelain!
import predict_topology as tp  # noqa

report_mapping = os.path.join(here, "shell", "binding", "report_mapping.sh")
bash = shutil.which("bash")

for script in [report_mapping]:
    if not os.path.exists(script):
        raise ValueError(f"{script} does not exist.")

if not bash:
    raise ValueError("Cannot find bash command... uhh.")


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
    return ",".join(str(x) for x in sorted(list(indices)))


def read_yaml_file(filename):
    """
    Read YAML from a filename
    """
    with open(filename, "r") as file:
        data = yaml.safe_load(file)
    return data


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
                # I'm printing in the output parser instead
                # because it's formatted better. You can print here for debugging.
                # print(line_stripped, flush=True)
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


class CommandLineRunner:
    def __init__(
        self,
        nodes,
        tasks,
        exclusive=False,
        cores_per_task=None,
        cpu_affinity=None,
        **kwargs,
    ):
        """
        Assemble a flux run command from command line flags, etc.
        """
        self.nodes = nodes
        self.tasks = tasks
        self.exclusive = exclusive
        self.cores_per_task = cores_per_task
        self.cpu_affinity = cpu_affinity

    def parse_binding_output(self, raw_output):
        """
        Given an output string, parse the binding information.
        """
        actual_layout = {}

        for line in raw_output.strip().split("\n"):
            parts = line.split()
            if "PEWPEWPEW" not in line:
                print(line)
            if "PEWPEWPEW" not in line:
                continue

            # PEWPEWPEW
            parts.pop(0)

            # The cpuset is a mask that picks out the cores
            # $rank $node $cpuset_mask $logical_cpu_list $taskset_list" >&2
            rank_id, node_name, cpuset, logical_pus, physical_cores, taskset_pus = parts
            rank_id = int(rank_id)
            actual_layout[rank_id] = {
                "cpuset": cpuset,
                "cores": physical_cores,
                "pmu": logical_pus,
            }
        return actual_layout

    def run(self):
        """
        Runs the Flux job and captures the binding report from each rank.

        This is a run from the command line (non jobspec)
        """
        print(f"\nRunning Experiment with {self.nodes} nodes and {self.tasks} tasks")
        cmd = ["flux", "run", "-N", str(self.nodes), "-n", str(self.tasks)]
        if self.cpu_affinity is not None:
            cmd += ["-o", "cpu-affinity=per-task"]
        if self.exclusive:
            cmd.append("--exclusive")
        if self.cores_per_task is not None:
            cmd += ["--cores-per-task", str(self.cores_per_task)]

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
        return self.parse_binding_output(raw_output)


class JobSpecRunner(CommandLineRunner):
    """
    The Jobspec running loads and runs a job from a yaml jobspec
    """

    def __init__(self, filename, **kwargs):
        self.load_jobspec(filename, **kwargs)

    def load_jobspec(self, filename, **kwargs):
        """
        Load jobspec from filename
        """
        # Assume it is yaml, throw up otherwise
        self.jobspec = read_yaml_file(filename)
        # We need to update the command
        self.jobspec["tasks"][0]["command"] = [bash, report_mapping]
        self.args = kwargs

    def run(self):
        """
        From from a Jobspec using Flux
        """
        import flux
        import flux.job

        handle = flux.Flux()
        js = flux.job.Jobspec(**self.jobspec)
        js.environment = dict(os.environ)

        # CPU affinity is added as a shell option.
        cpu_affinity = self.args.get("cpu_affinity")
        if cpu_affinity is not None:
            js.setattr_shell_option("cpu-affinity", cpu_affinity)

        job_id = flux.job.submit(handle, js)
        time.sleep(2)
        output = flux.job.output.job_output(handle, job_id)
        return self.parse_binding_output(output.stdout + output.stderr)


def validate_result(rank, prediction, actual):
    """
    Compares the predicted layout to the actual layout and reports PASS/FAIL.
    """
    if not prediction or not actual:
        print("🔴 Could not generate predictions or get actual results.")
        return False

    for field in ["cpuset", "cores", "pmu"]:
        predicted_value = prediction[field]
        actual_value = actual[field]
        if predicted_value == actual_value:
            print(f"🟢 Rank {rank} - {field} {actual_value}")
        else:
            print(
                f"🔴 Rank {rank} - Predicted {field}: {predicted_value}, Actual {field}: {actual_value}"
            )
            return False
    return True


def get_parser():
    parser = argparse.ArgumentParser(
        description="Validate Flux's binding behavior.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--xml",
        help="Provide topology.xml to the TopologyParser (that makes predictions)",
    )
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
    parser.add_argument(
        "-N", "--nodes", type=int, default=1, help="The number of nodes (default: 1)."
    )
    parser.add_argument(
        "--jobspec", "-j", help="Provide a jobspec instead.", default=None
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
    args, expression = parser.parse_known_args()

    # Did we get a jobspec, or assembling on the fly?
    # Note that if we get a jobspec, a lot of the affinity args are added
    # as attributes. We likely should add more.
    if args.jobspec is not None:
        runner = JobSpecRunner(args.jobspec, **vars(args))
    else:
        runner = CommandLineRunner(**vars(args))

    # This is organized by flux task rank. Here is one with cpu affinity per task -N1 -n2
    # {1: {'cpuset': '0x0000000c', 'cores': '2,3'},
    #  0: {'cpuset': '0x00000003', 'cores': '0,1'}}
    actual_layouts = runner.run()

    # If no expression, just print the layout
    if not expression:
        sys.exit(0)

    # Make predictions based on the topology we are expecting
    topology = tp.TopologyParser()
    if args.xml is not None:
        topology.load_from_file(args.xml)
    else:
        topology.load_from_lstopo()

    print(f"Successfully loaded topology: {topology}.")
    print(f"Evaluating expression: {expression}.")

    # These are calculated masks for the topology desired.
    # The function assumes you are providing in the order you want,
    # e.g., ranks 0..N
    masks = topology.evaluate_expression(expression)

    # We assume that the topology provided is in the order of ranks. E.g.,
    # core:0-1 x numa:0 AND numa:0 x core:1-2
    # Says: rank 0 has core:0-1 x numa:0
    #       rank 1 has numa:0 x core:1-2
    rank = 0
    predicted_layouts = {}
    for group, mask in masks.items():
        logical_list, core_list = topology.predict_binding_outputs(mask)
        predicted_layouts[rank] = {
            "cpuset": mask,
            "cores": core_list,
            "pmu": logical_list,
        }
        rank += 1

    print(f"Predicted layout: {json.dumps(predicted_layouts, indent=4)}")

    # Now evaluate each one.
    if len(predicted_layouts) != len(actual_layouts):
        print("Error: predicted length != actual, check your expressions?:")
        print(f" Predicted: {json.dumps(predicted_layouts, indent=4)}\n")
        print(f" Actual: {json.dumps(actual_layouts, indent=4)}")
        sys.exit(1)

    print("\nValidating Results")
    mismatches = 0
    for rank, actual_layout in actual_layouts.items():
        if rank not in predicted_layouts:
            raise ValueError(f"Rank {rank} was not predicted.")
        predicted_layout = predicted_layouts[rank]
        success = validate_result(rank, predicted_layout, actual_layout)
        if not success:
            mismatches += 1

    if mismatches == 0:
        print("\n🎉 Implicit exclusivity and dense packing are working as predicted.")
    else:
        print(f"\n💔 Found {mismatches} binding mismatches.")


if __name__ == "__main__":
    main()
