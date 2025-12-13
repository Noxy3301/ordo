# ansible for ordo setup

> [!NOTE]
> We assume each VM already has ordo downloaded and built (baked into our custom AMI snapshot).

> [!CAUTION]
> HAProxy stats auth is set to the placeholder `admin:password` in [templates/haproxy.cfg.j2](templates/haproxy.cfg.j2). Change it before use.

## Run playbook

Connectivity check:
```bash
ansible -i inventory.ini all -m ping
```

Full run (LineairDB → MySQL → HAProxy):
```bash
ansible-playbook -i inventory.ini site.yml
```

Target a subset (e.g., HAProxy only):
```bash
ansible-playbook -i inventory.ini site.yml --limit haproxy
```

## Run benchbase (YCSB-B sample)

```bash
./scripts/run_dist.sh \
    --profile b \
    --time 30 \
    --rate 0 \
    --scalefactor 1 \
    --mysql-host {{ hostvars['haproxy-1'].ansible_host }} \
    --mysql-port 3307
```
