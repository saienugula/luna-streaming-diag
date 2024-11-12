#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys

def usage():
    print("""
    Usage: script.py -t <type> [options] [path]
    ----- Required --------
      -t type - valid choices are "docker", "kube", "standalone"
    ----- Options --------
      -n namespace of the pulsar cluster (default: pulsar)
      -o output_dir - where to put resulting file (default: current directory)
    """)

def check_type(type_arg):
    if type_arg not in ["docker", "kube", "standalone"]:
        usage()
        sys.exit(1)

def check_output_dir(output_dir):
    if not output_dir:
        output_dir = os.path.join(os.getcwd(), "pulsar_diag")
        os.makedirs(output_dir, exist_ok=True)
    elif not os.path.exists(output_dir):
        
        output_dir = os.path.join(output_dir, "pulsar_diag")
        os.makedirs(output_dir)
    print(f"Using {output_dir} as the output directory.")
    return output_dir

def collect_logs(args):
    if args.type == "docker":
        print("Collecting logs from docker")
        container_ids = subprocess.getoutput("docker ps -a | grep pulsar | awk '{print $1}'").split()
        for container_id in container_ids:
            subprocess.run(["docker", "cp", f"{container_id}:/var/log/pulsar", args.output_dir])

    elif args.type == "kube":
        print("Collecting broker logs...")
        os.makedirs(f"{args.output_dir}/logs/broker", exist_ok=True)
        pods = subprocess.getoutput(f"kubectl get pods -n {args.namespace} | grep -i broker | awk '{{print $1}}'").split()
        for pod in pods:
            with open(f"{args.output_dir}/logs/broker/{pod}.log", "w") as log_file:
                subprocess.run(["kubectl", "-n", args.namespace, "logs", pod], stdout=log_file)

        print("Collecting proxy logs...")
        os.makedirs(f"{args.output_dir}/logs/proxy", exist_ok=True)
        pods = subprocess.getoutput(f"kubectl get pods -n {args.namespace} | grep -i proxy | awk '{{print $1}}'").split()
        for pod in pods:
            with open(f"{args.output_dir}/logs/proxy/{pod}.log", "w") as log_file:
                subprocess.run(["kubectl", "-n", args.namespace, "logs", pod], stdout=log_file)

        print("Collecting bookie logs...")
        os.makedirs(f"{args.output_dir}/logs/bookie", exist_ok=True)
        pods = subprocess.getoutput(f"kubectl get pods -n {args.namespace} | grep -i bookkeeper | awk '{{print $1}}'").split()
        for pod in pods:
            with open(f"{args.output_dir}/logs/bookie/{pod}.log", "w") as log_file:
                subprocess.run(["kubectl", "-n", args.namespace, "logs", pod], stdout=log_file)

        print("Collecting zookeeper logs...")
        os.makedirs(f"{args.output_dir}/logs/zookeeper", exist_ok=True)
        pods = subprocess.getoutput(f"kubectl get pods -n {args.namespace} | grep -i zookeeper | awk '{{print $1}}'").split()
        for pod in pods:
            with open(f"{args.output_dir}/logs/zookeeper/{pod}.log", "w") as log_file:
                subprocess.run(["kubectl", "-n", args.namespace, "logs", pod], stdout=log_file)

    elif args.type == "standalone":
        print("Collecting logs from standalone")
        subprocess.run(["cp", "-r", "/var/log/pulsar", args.output_dir])

def fetch_tenants_info(args):
    if args.type == "docker":
        print("Fetching pulsar info from docker")
        container_ids = subprocess.getoutput("docker ps -a | grep pulsar | awk '{print $1}'").split()
        for container_id in container_ids:
            result = subprocess.run(["docker", "exec", "-it", container_id, "/pulsar/bin/pulsar-admin", "brokers", "list"],
                capture_output=True, text=True, check=True)
            print(result.stdout)
        



    if args.type == "kube":
    # Get the list of pods matching 'bastion' in their name
     pods = subprocess.getoutput(f"kubectl get pods -n {args.namespace} | grep -i bastion | awk '{{print $1}}'").split()

    # Prepare output file
     output_file_path = os.path.join(args.output_dir, "tenants_namespaces_topics_list.txt")

    # Clear or create the file before appending data
     with open(output_file_path, "w") as f:
         f.write("Pulsar Tenants, Namespaces, and Topics\n")

     for pod in pods:
        try:
            # Fetch tenants
            result = subprocess.run(["kubectl", "-n", args.namespace, "exec", "-it", pod, "--", "/pulsar/bin/pulsar-admin", "tenants", "list"],
                capture_output=True, text=True, check=True)
            tenants = result.stdout.splitlines()
        except subprocess.CalledProcessError as e:
            print(f"Error fetching tenants from pod {pod}: {e}")
            continue
        for tenant in tenants:
            tenant = tenant.strip()
            # Write tenant to file
            with open(output_file_path, "a") as f:
                f.write(f"\nTenant: {tenant}\n")

            try:
                # Fetch namespaces for the tenant
                result = subprocess.run(["kubectl", "-n", args.namespace, "exec", "-it", pod, "--", "/pulsar/bin/pulsar-admin", "namespaces", "list", tenant],
                    capture_output=True, text=True, check=True)
                pulsar_namespaces = result.stdout.splitlines()
            except subprocess.CalledProcessError as e:
                print(f"Error fetching namespaces for tenant {tenant}: {e}")
                continue
          
            for pns in pulsar_namespaces:
                #write namespace retention policies to file
                with open(output_file_path, "a") as f:
                    f.write(f"  Namespace: {pns}\n")
                try:
                    # Fetch namespace retention policies
                    retention_policies = subprocess.run(["kubectl", "-n", args.namespace, "exec", "-it", pod, "--", "/pulsar/bin/pulsar-admin", "namespaces", "get-retention", pns],
                        capture_output=True, text=True, check=True)
                    retention = retention_policies.stdout.splitlines()
                    # Write retention policies to file
                    with open(output_file_path, "a") as f:
                        f.write(f"          Retention Policies: {retention}\n")
                except subprocess.CalledProcessError as e:
                    print(f"Error fetching retention policies for namespace {pns}: {e}")
                    continue
                
            for pns in pulsar_namespaces:
                try:
                    # Fetch topics for the namespace
                    topics_result = subprocess.run(["kubectl", "-n", args.namespace, "exec", "-it", pod, "--", "/pulsar/bin/pulsar-admin", "topics", "list", pns],
                        capture_output=True, text=True, check=True)
                    topics = topics_result.stdout.splitlines()
                except subprocess.CalledProcessError as e:
                    print(f"Error fetching topics for namespace {pns}: {e}")
                    continue

                for topic in topics:
                    # Write topic to file
                    with open(output_file_path, "a") as f:
                        f.write(f"     Topic: {topic}\n")
                    try:
                        # Fetch topic stats
                        topic_stats = subprocess.run(["kubectl", "-n", args.namespace, "exec", "-it", pod, "--", "/pulsar/bin/pulsar-admin", "topics", "stats", topic],
                            capture_output=True, text=True, check=True)
                        stats = topic_stats.stdout.splitlines()
                        # Write stats to file
                        with open(output_file_path, "a") as f:
                            f.write(f"          Stats: {stats}\n")
                    except subprocess.CalledProcessError as e:
                        print(f"Error fetching stats for topic {topic}: {e}")
                        continue

def get_pulsar_config(args):
    if args.type == "docker":
        print("Fetching pulsar config from docker")
        container_ids = subprocess.getoutput("docker ps -a | grep pulsar | awk '{print $1}'").split()
        for container_id in container_ids:
            result = subprocess.run(["docker", "exec", "-it", container_id, "/pulsar/bin/pulsar-admin", "brokers", "list"],
                capture_output=True, text=True, check=True)
            print(result.stdout)

    elif args.type == "kube":
        output_file_path = f"{args.output_dir}/conf"
        os.makedirs(output_file_path, exist_ok=True)
        print("Fetching pulsar config from kube")
        pods = subprocess.getoutput(f"kubectl get pods -n {args.namespace} | grep -i bastion | awk '{{print $1}}'").split()
        if len(pods) == 0:
            print("No bastion pods found in the namespace... Trying to fetch config from the proxy pod")
            pods = subprocess.getoutput(f"kubectl get pods -n {args.namespace} | grep -i proxy | awk '{{print $1}}'").split()
            if len(pods) == 0:
                print("No proxy pods found in the namespace... Trying to fetch config from the broker pod")
                pods = subprocess.getoutput(f"kubectl get pods -n {args.namespace} | grep -i broker | awk '{{print $1}}'").split()
            else:
                print("No broker/proxy/bastion pods found in the namespace... {args.namespace} Exiting...")
                sys.exit(1)

        else:
            first_pod = pods[0]
            print(f"Fetching pulsar config from pod {first_pod}")
            #print("kubectl", "-n", args.namespace, "cp", f"{first_pod}:/pulsar/conf/broker.conf", f"{output_file_path}/broker.conf")
            subprocess.run(["kubectl", "-n", args.namespace, "cp", f"{first_pod}:/pulsar/conf/broker.conf", f"{output_file_path}/broker.conf"],
                capture_output=True, text=True, check=True)
            subprocess.run(["kubectl", "-n", args.namespace, "cp", f"{first_pod}:/pulsar/conf/proxy.conf", f"{output_file_path}/proxy.conf"],
                capture_output=True, text=True, check=True)
            subprocess.run(["kubectl", "-n", args.namespace, "cp", f"{first_pod}:/pulsar/conf/bookkeeper.conf", f"{output_file_path}/bookkeeper.conf"],
                capture_output=True, text=True, check=True)
            subprocess.run(["kubectl", "-n", args.namespace, "cp", f"{first_pod}:/pulsar/conf/zookeeper.conf", f"{output_file_path}/zookeeper.conf"],
                capture_output=True, text=True, check=True)


    elif args.type == "standalone":
        print("Fetching pulsar config from standalone")
        result = subprocess.run(["/pulsar/bin/pulsar-admin", "brokers", "list"],
            capture_output=True, text=True, check=True)
        print(result.stdout)

def describe_pods(args):
    #Describe all pods in the namespace
    output_file_path = f"{args.output_dir}/describe_pods"
    os.makedirs(output_file_path, exist_ok=True)
    if args.type == "kube":
        print("Describing pods...")
        pods = subprocess.getoutput(f"kubectl get pods -n {args.namespace} --no-headers -o custom-columns=NAME:.metadata.name").split()
        for pod in pods:
            with open(f"{output_file_path}/{pod}.yaml", "w") as file:
                subprocess.run(["kubectl", "-n", args.namespace, "describe", "pod", pod], stdout=file, text=True)

def main():
    if len(sys.argv) == 1:
        usage()
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Collect logs from Pulsar deployments.")
    parser.add_argument("-t", "--type", required=True, choices=["docker", "kube", "standalone"], help="Deployment type")
    parser.add_argument("-n", "--namespace", default="pulsar", help="Namespace of the Pulsar cluster (default: pulsar)")
    parser.add_argument("-o", "--output_dir", help="Output directory for logs (default: current directory)")
    args = parser.parse_args()
    if args.namespace == "pulsar":
        print("Using default namespace as 'pulsar'")
    else:
        print(f"Using namespace: {args.namespace}")

    check_type(args.type)
    args.output_dir = check_output_dir(args.output_dir)
    collect_logs(args)
    
    get_pulsar_config(args)
    describe_pods(args)
    fetch_tenants_info(args)


    ##kubectl explain deployment --recursive

if __name__ == "__main__":   
    main()
