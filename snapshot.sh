. docker-compose.alias
curl localhost:9200/_snapshot/mybackup \
     -u elastic:$ELASTIC_PASSWORD \
     -d '''{
    "type":"fs",
    "settings": {
        "location": "my_backuplocation"
    }
}''' \
-H "Content-Type: application/json"
