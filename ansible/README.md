# ansible for ordo setup

> [!NOTE]
> We assume each VM already has ordo downloaded and built (baked into our custom AMI snapshot).

> [!CAUTION]
> HAProxy stats auth is set to the placeholder `admin:password` in [templates/haproxy.cfg.j2](templates/haproxy.cfg.j2). Change it before use.

## Update inventory.ini

Generate `inventory.ini` from AWS tags (default region: `ap-southeast-2`):
```bash
python3 py/update_inventory.py
```

> [!NOTE]
> Tag instances like this so `update_inventory.py` can find them:
> - `Name=ordo-lineairdb`
> - `Name=ordo-haproxy`
> - `Name=ordo-bench`
> - `Name=ordo-mysql`
> - `Project=Ordo`

## Run playbook

Connectivity check:
```bash
ansible -i inventory.ini all -m ping
```

Full run (LineairDB → MySQL → HAProxy):
```bash
ansible-playbook -i inventory.ini site.yml
```

## Run BenchBase and collect CPU/throughput results

Run the benchmark and collect CPU logs (set sample_interval=1) plus throughput output:
```bash
ansible-playbook -i inventory.ini measure_cpu.yml -e "run_id=$(date +%Y%m%d-%H%M%S) sample_interval=1"
```

CPU logs are stored under `result/<config>/cpu/<host>/cpu-<run_id>.log`.
Throughput results are written to `result/<config>/throughput/throughput_raw.csv`.

## Plot results

Plot throughput:
```bash
python3 py/plot_throughput.py
```

Plot CPU usage:
```bash
python3 py/plot_cpu.py
```
