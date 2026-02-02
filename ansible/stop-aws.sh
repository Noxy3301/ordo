# stop spot requests
aws ec2 cancel-spot-instance-requests \
  --no-cli-pager \
  --region ap-southeast-2 \
  --spot-instance-request-ids $(
    aws ec2 describe-spot-instance-requests \
      --region ap-southeast-2 \
      --query 'SpotInstanceRequests[].SpotInstanceRequestId' \
      --output text
  ) \
  > /dev/null

# terminate instances
aws ec2 terminate-instances \
  --no-cli-pager \
  --region ap-southeast-2 \
  --instance-ids $(
    aws ec2 describe-instances \
      --region ap-southeast-2 \
      --query 'Reservations[].Instances[].InstanceId' \
      --output text
  ) \
  > /dev/null