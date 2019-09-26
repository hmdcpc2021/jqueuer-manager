#!/bin/sh

settings_file="./_settings"

. $settings_file

if [ -z "$PUBLIC_KEY" ]; then
  echo "Please, set PUBLIC_KEY in file named \"$settings_file\"!"
  exit
fi

if [ -z "$DRIVE_ID" ]; then
  echo "Please, set DRIVE_ID in file named \"$settings_file\"!"
  exit
fi

if [ -z "$FIREWALL_POLICY" ]; then
  echo "Please, set FIREWALL_POLICY in file named \"$settings_file\"!"
  exit
fi

echo "Replacing CloudSigma info in jqueuer.yaml..."
sed -i "s/libdrive_id: .*/libdrive_id: $DRIVE_ID/g" jqueuer.yaml
sed -i "s/public_key_id: .*/public_key_id: $PUBLIC_KEY/g" jqueuer.yaml
sed -i "s/firewall_policy: .*/firewall_policy: $FIREWALL_POLICY/g" jqueuer.yaml
echo "OK"