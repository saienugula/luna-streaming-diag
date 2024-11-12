# luna-streaming-diag

Collect the diagnostics of luna streaming(Apache Pulsar) deployed in kubernetes or in Docker or as a standalone cluster.

```
Usage: collect_diag.py -t <type> [options] [path]
    ----- Required --------
      -t type - valid choices are "docker", "kube", "standalone"
    ----- Options --------
      -n namespace of the pulsar cluster (default: pulsar)
      -o output_dir - where to put resulting file (default: current directory)
```
