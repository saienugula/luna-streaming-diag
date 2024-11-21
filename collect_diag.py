#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys
import logging



def usage():
    print("""
    Usage: diag.py -t <type> [options] [path]
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

def setup_logging(loglevel):
    # Set the logging level based on the loglevel argument
    level = logging.DEBUG if loglevel == "DEBUG" else logging.INFO
    
    # Configure logging with the appropriate level and formatter
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s' if loglevel == "DEBUG" else '%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.basicConfig(level=loglevel, format=format)

class kubernetesPods:
    def __init__(self, namespace):
        self.namespace = namespace
        self.pods = []
        self.pod_info = []
        self.pod_status = []
        self.pod_name = []
    
    def get_pods(self):
        try:
            pods_info = subprocess.getoutput(f"kubectl get pods -n {self.namespace} --no-headers -o custom-columns=NAME:.metadata.name,STATUS:.status.phase").splitlines()
            self.pods = [line.split() for line in pods_info]
            return self.pods
            logger.debug(f"Pods information: {self.pods}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error fetching pods information: {e}")
            return
        except Exception as e:
            logger.error(f"Unexpected error fetching pods information: {e}")
            return
    def get_pod_info(self):
        return[self.pod_info.append(pod[0]) for pod in self.pods]
        logger.debug(f"Pods information: {self.pod_info}")
    def get_pod_status(self):
        return[self.pod_status.append(pod[1]) for pod in self.pods]
        logger.debug(f"Pods information: {self.pod_status}")



def collect_logs(args, broker_pods=None, proxy_pods=None, bookie_pods=None, zookeeper_pods=None, bastion_pods=None,):
    logger = logging.getLogger("LogsCollector")
    if args.type == "docker":
        #print("Collecting logs from docker")
        logger.info("Collecting logs from docker")
        container_ids = subprocess.getoutput("docker ps -a | grep pulsar | awk '{print $1}'").split()
        for container_id in container_ids:
            subprocess.run(["docker", "cp", f"{container_id}:/var/log/pulsar", args.output_dir])

    elif args.type == "kube":
        #print("Collecting broker,bookkeeper,zookeeper and proxy logs")
        logger.info("Collecting logs from kube")
        #create logs directory
        os.makedirs(f"{args.output_dir}/logs", exist_ok=True)
        # Collect Broker logs
        #print("Collecting broker logs...")
        if not broker_pods:
            logger.info(f"No running Broker pods found in namespace {args.namespace}")
            return
        logger.debug(f"Broker Pods: {broker_pods}")
        for pod in broker_pods:
            try:
                output_file_path = f"{args.output_dir}/logs/broker/{pod}"
                os.makedirs(output_file_path, exist_ok=True)
                # Collect logs from running pods
                logger.debug(f"Collecting Broker logs from pod {pod}")
                log_file_path = f"{output_file_path}/{pod}.log"
                with open(log_file_path, "w") as log_file:
                    subprocess.run(["kubectl", "-n", args.namespace, "logs", pod, "--all-containers=true"],stdout=log_file,stderr=subprocess.PIPE,check=True)
                    logger.debug(f"Successfully collected Broker Logs from pod {pod}")
                # Copy logs from the pod
                process = subprocess.Popen(["kubectl", "-n", args.namespace, "cp", f"{pod}:/pulsar/logs/", output_file_path],stderr=subprocess.PIPE,stdout=subprocess.PIPE)
                _, err = process.communicate()
                logger.debug(f"Successfully copied Broker Logs from pod {pod}")
                # Only print errors that are not the tar warning
                if err and b"Removing leading '/'" not in err:
                    print(f"Error copying logs from pod {pod}: {err.decode()}")
            except subprocess.CalledProcessError as e:
                print(f"Error collecting logs for pod {pod}: {e}")
                continue
            except Exception as e:
                print(f"Unexpected error for pod {pod}: {e}")
                continue
       
        # Collect Proxy logs
        #print("Collecting proxy logs...")
        if not proxy_pods:
            print("No running Proxy pods found.")
            return
        logger.debug(f"Proxy Pods: {proxy_pods}")
        for pod in proxy_pods:
            try:
                output_file_path = f"{args.output_dir}/logs/proxy/{pod}"
                os.makedirs(output_file_path, exist_ok=True)
                logger.debug(f"Collecting Proxy logs from pod {pod}")
                # Collect logs from running pods
                log_file_path = f"{output_file_path}/{pod}.log"
                with open(log_file_path, "w") as log_file:
                    subprocess.run(["kubectl", "-n", args.namespace, "logs", pod, "--all-containers=true"],stdout=log_file,stderr=subprocess.PIPE,check=True)
                    logger.debug(f"Succesfully collected Proxy Logs from pod {pod}")
                # Copy logs from the pod
                process = subprocess.Popen(["kubectl", "-n", args.namespace, "cp", f"{pod}:/pulsar/logs/", output_file_path],stderr=subprocess.PIPE,stdout=subprocess.PIPE)
                _, err = process.communicate()
                # Only print errors that are not the tar warning
                logger.debug(f"succesfully copied Proxy Logs from pod {pod}")
                if err and b"Removing leading '/'" not in err:
                    logger.debug(f"Error copying logs from pod {pod}: {err.decode()}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Error collecting logs for pod {pod}: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error for pod {pod}: {e}")
                continue
       
       ## Collecting bookie logs
        #print("Collecting bookie logs...")
        if not bookie_pods:
            print("No running Bookie pods found.")
            return
        logger.debug(f"Bookie Pods: {bookie_pods}")
        for pod in bookie_pods:
            try:
                output_file_path = f"{args.output_dir}/logs/bookie/{pod}"
                os.makedirs(output_file_path, exist_ok=True)
                # Collect logs from running pods
                log_file_path = f"{output_file_path}/{pod}.log"
                with open(log_file_path, "w") as log_file:
                    subprocess.run(["kubectl", "-n", args.namespace, "logs", pod, "--all-containers=true"],stdout=log_file,stderr=subprocess.PIPE,check=True)
                    logger.debug(f"succesfully collected Bookie Logs from pod {pod}")
                # Copy logs from the pod
                process = subprocess.Popen(["kubectl", "-n", args.namespace, "cp", f"{pod}:/pulsar/logs/", output_file_path],stderr=subprocess.PIPE,stdout=subprocess.PIPE)
                _, err = process.communicate()
                logger.debug(f"succesfully copied Bookie Logs from pod {pod}")
                # Only print errors that are not the tar warning
                if err and b"Removing leading '/'" not in err:
                    logger.debug(f"Error copying logs from pod {pod}: {err.decode()}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Error collecting logs for pod {pod}: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error for pod {pod}: {e}")
                continue
        # Collect Zookeeper logs
        #print("Collecting Zookeeper logs...")
        if not zookeeper_pods:
            print("No running Zookeeper pods found.")
            return
        logger.debug(f"Zookeeper Pods: {zookeeper_pods}")
        for pod in zookeeper_pods:
            try:
                output_file_path = f"{args.output_dir}/logs/zookeeper/{pod}"
                os.makedirs(output_file_path, exist_ok=True)

            # Collect logs from running pods
                log_file_path = f"{output_file_path}/{pod}.log"
                with open(log_file_path, "w") as log_file:
                    subprocess.run(["kubectl", "-n", args.namespace, "logs", pod, "--all-containers=true"],stdout=log_file,stderr=subprocess.PIPE,check=True)
                    logger.debug(f"succesfully collected Zookeeper Logs from pod {pod}")
            # Copy logs from the pod
                process = subprocess.Popen(["kubectl", "-n", args.namespace, "cp", f"{pod}:/pulsar/logs/", output_file_path],stderr=subprocess.PIPE,stdout=subprocess.PIPE)
                logger.debug(f"succesfully copied Zookeeper Logs from pod {pod}")
                _, err = process.communicate()
            # Only print errors that are not the tar warning
                if err and b"Removing leading '/'" not in err:
                    logger.debug(f"Error copying logs from pod {pod}: {err.decode()}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Error collecting logs for pod {pod}: {e}")
                continue  # Continue to the next pod if an error occurs
            except Exception as e:
                logger.error(f"Unexpected error for pod {pod}: {e}")
                continue  # Continue to the next pod if any other exception occurs
        logger.info("Successfully collected all logs from all pods")

    elif args.type == "standalone":
        print("Collecting logs from standalone")
        subprocess.run(["cp", "-r", "/var/log/pulsar", args.output_dir])

def fetch_tenants_info(args, broker_pods=None, proxy_pods=None, bastion_pods=None):
    logger = logging.getLogger("TenantsInfoCollector")
    if args.type == "docker":
        print("Fetching pulsar info from docker")
        container_ids = subprocess.getoutput("docker ps -a | grep pulsar | awk '{print $1}'").split()
        for container_id in container_ids:
            result = subprocess.run(["docker", "exec", "-it", container_id, "/pulsar/bin/pulsar-admin", "brokers", "list"],
                capture_output=True, text=True, check=True)
            print(result.stdout)
        



    if args.type == "kube":
    # Get the list of pods matching 'bastion' in their name
     logger.info(f"Collecting pulsar topic stats from the kube and namespace {args.namespace}")
     try:
            # Fetch pods information
            admin_pod = bastion_pods
            logger.debug(f"Pods information: {admin_pod}")

            if not admin_pod:
                logger.info("No running bastion pods found. trying to fetch from proxy pods")
                admin_pod = proxy_pods
                logger.debug(f"Pods information: {admin_pod}")
            if not admin_pod:
                logger.info("No running proxy pods found. trying to fetch from broker pods")
                admin_pod = broker_pods
                logger.debug(f"Pods information: {admin_pod}")
            if not admin_pod:
                logger.info("No running broker,proxy,bastion pods found. Exiting...")
                return      

            logger.info(f"found {len(admin_pod)} pods and will use the first one")
            for pod in admin_pod:
                pod_name = pod

     except subprocess.CalledProcessError as e:
        logger.error(f"Error fetching pods information: {e}")
        return
     except Exception as e:
        logger.error(f"Unexpected error fetching pods information: {e}")
        return

    # Prepare output file
     output_file_path = os.path.join(args.output_dir, "tenants_namespaces_topics_list.txt")

    # Clear or create the file before appending data
     with open(output_file_path, "w") as f:
         f.write("Pulsar Tenants, Namespaces, and Topics\n")
     logger.info(f"Fetching pulsar info from pod {pod_name}")
     
     #for pod_name in pod_name:
     try:
            # Fetch tenants
            result = subprocess.run(["kubectl", "-n", args.namespace, "exec", "-it", pod_name, "--", "/pulsar/bin/pulsar-admin", "tenants", "list"],
                capture_output=True, text=True, check=True)
            tenants = result.stdout.splitlines()
            logger.debug(f"Tenants: {tenants}")
     except subprocess.CalledProcessError as e:
        logger.error(f"Error fetching tenants from pod {pod_name}: {e}")
        return
     for tenant in tenants:
            tenant = tenant.strip()
            logger.debug(f"Tenant: {tenant}")
            # Write tenant to file
            with open(output_file_path, "a") as f:
                f.write(f"\nTenant: {tenant}\n")

            try:
                logger.debug(f"Fetching namespaces for tenant {tenant}")
                # Fetch namespaces for the tenant
                result = subprocess.run(["kubectl", "-n", args.namespace, "exec", "-it", pod_name, "--", "/pulsar/bin/pulsar-admin", "namespaces", "list", tenant],
                    capture_output=True, text=True, check=True)
                pulsar_namespaces = result.stdout.splitlines()
                logger.debug(f"Namespaces: {pulsar_namespaces}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Error fetching namespaces for tenant {tenant}: {e}")
                continue
          
            for pns in pulsar_namespaces:
                #write namespace retention policies to file
                with open(output_file_path, "a") as f:
                    f.write(f"  Namespace: {pns}\n")
                try:
                    # Fetch namespace retention policies
                    logger.debug(f"Fetching retention policies for namespace {pns}")
                    retention_policies = subprocess.run(["kubectl", "-n", args.namespace, "exec", "-it", pod_name, "--", "/pulsar/bin/pulsar-admin", "namespaces", "get-retention", pns],
                        capture_output=True, text=True, check=True)
                    retention = retention_policies.stdout.splitlines()
                    # Write retention policies to file
                    with open(output_file_path, "a") as f:
                        f.write(f"          Retention Policies: {retention}\n")
                except subprocess.CalledProcessError as e:
                    logger.error(f"Error fetching retention policies for namespace {pns}: {e}")
                    continue
                
            for pns in pulsar_namespaces:
                try:
                    # Fetch topics for the namespace
                    logger.debug(f"Fetching topics for namespace {pns}")
                    topics_result = subprocess.run(["kubectl", "-n", args.namespace, "exec", "-it", pod_name, "--", "/pulsar/bin/pulsar-admin", "topics", "list", pns],
                        capture_output=True, text=True, check=True)
                    topics = topics_result.stdout.splitlines()
                except subprocess.CalledProcessError as e:
                    logger.error(f"Error fetching topics for namespace {pns}: {e}")
                    continue

                for topic in topics:
                    # Write topic to file
                    with open(output_file_path, "a") as f:
                        f.write(f"     Topic: {topic}\n")
                    try:
                        # Fetch topic stats
                        logger.debug(f"Fetching stats for topic {topic}")
                        topic_stats = subprocess.run(["kubectl", "-n", args.namespace, "exec", "-it", pod_name, "--", "/pulsar/bin/pulsar-admin", "topics", "stats", topic],
                            capture_output=True, text=True, check=True)
                        stats = topic_stats.stdout.splitlines()
                        # Write stats to file
                        with open(output_file_path, "a") as f:
                            f.write(f"          Stats: {stats}\n")
                    except subprocess.CalledProcessError as e:
                        logger.error(f"Error fetching stats for topic {topic}: {e}")
                        continue

     logger.info(f"Successfully Collected the Tenants, Namespaces, and Topics info and saved to {output_file_path}")

def get_pulsar_config(args, broker_pods=None, proxy_pods=None, bookie_pods=None, zookeeper_pods=None, bastion_pods=None):
    if args.type == "docker":
        print("Fetching pulsar config from docker")
        container_ids = subprocess.getoutput("docker ps -a | grep pulsar | awk '{print $1}'").split()
        for container_id in container_ids:
            result = subprocess.run(["docker", "exec", "-it", container_id, "/pulsar/bin/pulsar-admin", "brokers", "list"],
                capture_output=True, text=True, check=True)
            print(result.stdout)

    elif args.type == "kube":
        logger = logging.getLogger("PulsarConfigCollector")
        logger.info("Collecting pulsar config from kube")
        output_file_path = f"{args.output_dir}/conf"
        os.makedirs(output_file_path, exist_ok=True)
        logger.debug(f"bastion Pods: {bastion_pods}")

        admin_pod = bastion_pods

        if not bastion_pods:
            logger.debug(f"pods information: {bastion_pods}")
            logger.info(f"No running bastion pods found in the namespace {args.namespace} Trying to fetch config from the proxy pod")
            admin_pod = proxy_pods
        if not proxy_pods:
            logger.info(f"No running proxy pods found in the namespace {args.namespace} Trying to fetch config from the broker pod")
            admin_pod = broker_pods
        if not broker_pods:
            logger.info(f"No running broker pods found in the namespace {args.namespace} Trying to fetch config from the zookeeper pod")
            admin_pod = zookeeper_pods
        if not zookeeper_pods:
            logger.info(f"No running zookeeper pods found in the namespace {args.namespace} Exiting...")
            return
        for pod in admin_pod:
            first_pod = pod
        logger.debug(f"Fetching pulsar config from pod {first_pod}")
        try:
                # Fetch pulsar config from the pod
            subprocess.run(["kubectl", "-n", args.namespace, "cp", f"{first_pod}:/pulsar/conf/broker.conf", f"{output_file_path}/broker.conf"],
                    capture_output=True, text=True, check=True)
            subprocess.run(["kubectl", "-n", args.namespace, "cp", f"{first_pod}:/pulsar/conf/proxy.conf", f"{output_file_path}/proxy.conf"],
                    capture_output=True, text=True, check=True)
            subprocess.run(["kubectl", "-n", args.namespace, "cp", f"{first_pod}:/pulsar/conf/bookkeeper.conf", f"{output_file_path}/bookkeeper.conf"],
                    capture_output=True, text=True, check=True)
            subprocess.run(["kubectl", "-n", args.namespace, "cp", f"{first_pod}:/pulsar/conf/zookeeper.conf", f"{output_file_path}/zookeeper.conf"],
                    capture_output=True, text=True, check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Error fetching pulsar config from pod {first_pod}: {e}")
            return
        except Exception as e:
            logger.error(f"Unexpected error fetching pulsar config from pod {first_pod}: {e}")
            return
        logger.info(f"Successfully collected pulsar configs from pod {first_pod}")


    elif args.type == "standalone":
        print("Fetching pulsar config from standalone")
        result = subprocess.run(["/pulsar/bin/pulsar-admin", "brokers", "list"],
            capture_output=True, text=True, check=True)
        print(result.stdout)

def describe_pods(args, pods_info=None):
    #Describe all pods in the namespace
    output_file_path = f"{args.output_dir}/describe_pods"
    os.makedirs(output_file_path, exist_ok=True)
    if args.type == "kube":
        logger = logging.getLogger("PodsDescriptionCollector")
        logger.info(f"Describing pods in the namespace {args.namespace}")
        for pods in pods_info:
            pod_name = pods[0]
            pod_status = pods[1]
            logger.debug(f"Describing pod: {pod_name}")
            try:
                with open(f"{output_file_path}/{pod_name}.yaml", "w") as file:
                    subprocess.run(["kubectl", "-n", args.namespace, "describe", "pod", pod_name], stdout=file, text=True)
            except subprocess.CalledProcessError as e:
                logger.error(f"Error describing pod {pod_name}: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error describing pod {pod_name}: {e}")
                continue
  

def main():
    if len(sys.argv) == 1:
        usage()
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Collect logs from Pulsar deployments.")
    parser.add_argument("-t", "--type", required=True, choices=["docker", "kube", "standalone"], help="Deployment type")
    parser.add_argument("-n", "--namespace", default="pulsar", help="Namespace of the Pulsar cluster (default: pulsar)")
    parser.add_argument("-o", "--output_dir", help="Output directory for logs (default: current directory)")
    parser.add_argument("-l", "--loglevel", default="INFO", help="Log level (default: INFO)")
    args = parser.parse_args()
    if args.namespace == "pulsar":
        print("Using default namespace as 'pulsar'")
    else:
        print(f"Using namespace: {args.namespace}")
    setup_logging(args.loglevel)

    check_type(args.type)
    args.output_dir = check_output_dir(args.output_dir)

    
    if args.type == "kube":

        kube_pods = kubernetesPods(args.namespace)
        kube_pods.get_pods()
        #logging.debug(f"Pods information: {kube_pods.pods}")
        kube_pods.get_pod_info()
        logging.debug(f"Pods information: {kube_pods.pod_info}")
        kube_pods.get_pod_status()
        logging.debug(f"Pods information: {kube_pods.pod_status}")
        #Finding proxy,broker,bookie,zookeeper and bastion pods

        broker_pods = [pod[0] for pod in kube_pods.pods if "broker" in pod[0].lower() and "Running" in pod[1]]
        logging.debug(f"Broker Pods: {broker_pods}")
        proxy_pods = [pod[0] for pod in kube_pods.pods if "proxy" in pod[0].lower() and "Running" in pod[1]]
        logging.debug(f"Proxy Pods: {proxy_pods}")
        bookie_pods = [pod[0] for pod in kube_pods.pods if "bookkeeper" in pod[0].lower() and "Running" in pod[1]]
        logging.debug(f"Found Bookie Pods: {bookie_pods}")
        zookeeper_pods = [pod[0] for pod in kube_pods.pods if "zookeeper" in pod[0].lower() and "Running" in pod[1]]
        logging.debug(f"Zookeeper Pods: {zookeeper_pods}")
        bastion_pods = [pod[0] for pod in kube_pods.pods if "bastion" in pod[0].lower() and "Running" in pod[1]]
        logging.debug(f"Bastion Pods: {bastion_pods}")
        
        
        collect_logs(args, broker_pods, proxy_pods, bookie_pods, zookeeper_pods)
        get_pulsar_config(args, broker_pods, proxy_pods, bookie_pods, zookeeper_pods, bastion_pods)
        describe_pods(args, kube_pods.pods)
        fetch_tenants_info(args, broker_pods,proxy_pods,bastion_pods)
    #fetch_tenants_info(args, kube_pods.pods)
        logging.info("Successfully collected pulsar cluster diagnostics")



if __name__ == "__main__":   
    main()


