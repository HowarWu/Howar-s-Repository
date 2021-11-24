#! /bin/sh

target_terraform_version=0.13.7

current_terraform_version=$(terraform version)

echo "Current terraform version is: ${current_terraform_version}"

wget -q https://releases.hashicorp.com/terraform/${target_terraform_version}/terraform_${target_terraform_version}_linux_amd64.zip &&

unzip terraform_${target_terraform_version}_linux_amd64.zip &&

chmod +x terraform &&

sudo mv terraform /usr/local/bin/ -f &&

current_terraform_version=$(terraform version) &&

echo "Terraform has been upgraded to ${current_terraform_version} successfully!" &&

rm -rf terraform_${target_terraform_version}_linux_amd64.zip
