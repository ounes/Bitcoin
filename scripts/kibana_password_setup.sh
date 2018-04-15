echo "# AUTO GENERATED ENTRY!" >> kibana/config/kibana.yml
echo "elastic.password: $(./extract_password.sh elastic)" >> kibana/config/kibana.yml
