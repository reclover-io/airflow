#!/usr/bin/bash
# stop script

# environment setting
. ./env

# error_message - show error message template
function error_message() {
    clear
    echo "[Problem]"
    echo "$1"
    echo ""
    exit 1
}


# check required files
declare -a FILES=("./env" $COMPOSE_FILE)
for f in "${FILES[@]}"
do
    if [ ! -f $f ]; then
        error_message "File \"$f\" missing, Please check before run script"
    fi
done


# is_container_shutdown - check status container is shutdown
function is_container_shutdown() {
   docker ps | grep $SERVICE_NAME  >/dev/null 2>&1
   if [ $? -eq 0 ]; then
       return 1
   fi
   return 0
}

# probe_container_status - probe container status
function probe_container_status() {
    COUNTER=0
    while :
    do
        COUNTER=`expr $COUNTER + 1`
        if [ is_container_shutdown ]; then
            echo "Service \"$SERVICE_NAME\" status => shutdown completed."
            break
        fi

        if [ $COUNTER -gt $WAITING_TIME ]; then
            break
        fi

        echo "."
        sleep 1
    done
}

# stop container service
if is_container_shutdown; then
    echo "Service \"$SERVICE_NAME\" is not running."
else
    if [ -f $COMPOSE_FILE ]; then
        COMPOSE_PROJECT_NAME=$SERVICE_NAME docker compose -f $COMPOSE_FILE down
    fi
    probe_container_status
fi