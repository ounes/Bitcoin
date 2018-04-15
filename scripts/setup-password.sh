. ./docker-compose.alias
docker-compose up -d db
# creating password temp file
docker-compose exec db bash -c 'yes | bin/x-pack/setup-passwords auto' > setup-passwords.txt

docker-compose restart db
