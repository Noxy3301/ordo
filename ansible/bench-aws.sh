set -euo pipefail

# Change to script directory
cd "$(dirname "$0")"

cleanup_done=false
cleanup() {
  if $cleanup_done; then
    return
  fi
  cleanup_done=true
  set +e

  # stop spot requests
  spot_ids=$(aws ec2 describe-spot-instance-requests \
    --region ap-southeast-2 \
    --query 'SpotInstanceRequests[].SpotInstanceRequestId' \
    --output text)
  if [ -n "$spot_ids" ]; then
    aws ec2 cancel-spot-instance-requests \
      --no-cli-pager \
      --region ap-southeast-2 \
      --spot-instance-request-ids $spot_ids \
      > /dev/null
  fi

  # terminate instances
  instance_ids=$(aws ec2 describe-instances \
    --region ap-southeast-2 \
    --query 'Reservations[].Instances[].InstanceId' \
    --output text)
  if [ -n "$instance_ids" ]; then
    aws ec2 terminate-instances \
      --no-cli-pager \
      --region ap-southeast-2 \
      --instance-ids $instance_ids \
      > /dev/null
  fi
}

on_error() {
  echo "ERROR: command failed; cleaning up." >&2
  cleanup
  exit 1
}
trap on_error ERR

# MySQL
aws ec2 run-instances \
  --no-cli-pager \
  --region ap-southeast-2 \
  --subnet-id subnet-0a15ff55a4cae198b\
  --launch-template LaunchTemplateId=lt-0f28a9f6b02e1c019,Version='$Default' \
  --count 16 \
  --instance-type c6i.4xlarge \
  --instance-market-options MarketType=spot \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=ordo-mysql},{Key=Project,Value=Ordo}]" \
  --query 'Instances[].InstanceId' \
  --output text

# BenchBase
aws ec2 run-instances \
  --no-cli-pager \
  --region ap-southeast-2 \
  --subnet-id subnet-0a15ff55a4cae198b\
  --launch-template LaunchTemplateId=lt-0f28a9f6b02e1c019,Version='$Default' \
  --count 4 \
  --instance-type c6i.8xlarge \
  --instance-market-options MarketType=spot \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=ordo-bench},{Key=Project,Value=Ordo}]" \
  --query 'Instances[].InstanceId' \
  --output text

# HAProxy
aws ec2 run-instances \
  --no-cli-pager \
  --region ap-southeast-2 \
  --subnet-id subnet-0a15ff55a4cae198b\
  --launch-template LaunchTemplateId=lt-0f28a9f6b02e1c019,Version='$Default' \
  --count 1 \
  --instance-type c6i.16xlarge \
  --instance-market-options MarketType=spot \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=ordo-haproxy},{Key=Project,Value=Ordo}]" \
  --query 'Instances[].InstanceId' \
  --output text

# LineairDB
aws ec2 run-instances \
  --no-cli-pager \
  --region ap-southeast-2 \
  --subnet-id subnet-0a15ff55a4cae198b\
  --launch-template LaunchTemplateId=lt-0f28a9f6b02e1c019,Version='$Default' \
  --count 1 \
  --instance-type c6i.16xlarge \
  --instance-market-options MarketType=spot \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=ordo-lineairdb},{Key=Project,Value=Ordo}]" \
  --query 'Instances[].InstanceId' \
  --output text

# wait for instances to initialize (for update inventory)
sleep 30

# configure ansible inventory
python3 py/update_inventory.py

# wait for instances to be running
ansible -i inventory.ini all -m wait_for_connection -a "timeout=300 delay=5 sleep=2" -o

# setup and run benchmark
# ansible-playbook -i inventory.ini site.yml

# ansible-playbook -i inventory.ini update.yml

ansible-playbook -i inventory.ini lineairdb.yml &
ansible-playbook -i inventory.ini mysql.yml &
ansible-playbook -i inventory.ini haproxy.yml &
wait
ansible-playbook -i inventory.ini benchbase.yml

ansible-playbook -i inventory.ini measure_usage.yml -e "run_id=$(date +%Y%m%d-%H%M%S) sample_interval=1"

# plot results
python3 py/plot_throughput.py
python3 py/plot_cpu.py

cleanup
