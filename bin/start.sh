#!/usr/bin/bash
# start script

# environment files
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


# is_container_running - check container status is running
function is_container_running() {
    docker inspect -f '{{.State.Running}}' $SERVICE_NAME
    return 0
}

# probe_container_status - probe container status
function probe_container_status() {
    COUNTER=0
    while :
    do
        COUNTER=`expr $COUNTER + 1`
        if [ is_container_running ]; then
            echo "Service \"$SERVICE_NAME\" status => running."
            break
        fi

        if [ $COUNTER -gt $WAITING_TIME ]; then
            break
        fi

        echo "."
        sleep 1
    done
}


# start services
if [ -f $COMPOSE_FILE ]; then
    COMPOSE_PROJECT_NAME=$SERVICE_NAME docker compose -f $COMPOSE_FILE up -d
fi
probe_container_status