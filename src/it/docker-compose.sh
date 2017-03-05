export HOST_NAME=`ifconfig en0 | awk '{ print $2}' | grep -E -o '([0-9]{1,3}[\\.]){3}[0-9]{1,3}'`
docker-compose -f src/it/docker-compose.yml up -d
