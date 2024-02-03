#!/bin/bash

waitForContainerHealth() {
    local service_name=$1
    local max_retries=30
    local current_retry=0

    echo "Waiting for $service_name to be healthy..."

    # Loop until the container is healthy or max retries reached
    while [ "$current_retry" -lt "$max_retries" ]; do
        if docker inspect --format '{{.State.Health.Status}}' "$service_name" | grep -q "healthy"; then
            echo "$service_name is healthy."
            break
        fi

        sleep 5
        current_retry=$((current_retry + 1))
    done

    # Check if max retries reached
    if [ "$current_retry" -eq "$max_retries" ]; then
        echo "Error: $service_name did not become healthy within the specified time."
        exit 1
    fi
}


putFilesInNameNode() {
    echo "-------------------------------------------------------------------"
    echo "---------------PUTTING CSV FILES IN THE NAME NODE------------------"
    echo "-------------------------------------------------------------------"
    docker exec -it  namenode bash -c 'hadoop fs -mkdir -p /user/root/movie/data && hadoop fs -put /hadoop/movieFinders/*.csv /user/root/movie/data && exit'    
}

buildProject() {
    echo "-------------------------------------------------------------------"
    echo "----------------------BUILDING THE PROJECT-------------------------"
    echo "-------------------------------------------------------------------"
    mvn clean package

    echo "-------------------------------------------------------------------"
    echo "----------------MOVING JAR FILE TO THE DATA FOLDER----------------"
    echo "-------------------------------------------------------------------"
    mv target/movieFinder-1.0-SNAPSHOT.jar data/
}

execMovieFinder() {
    echo "-------------------------------------------------------------------"
    echo "--------------------REMOVING PREVIOUS OUTPUT-----------------------"
    echo "-------------------------------------------------------------------"
    docker exec -it resourcemanager bash -c "hadoop fs -rm -r /out/ && exit"

    echo "-------------------------------------------------------------------"
    echo "-----------------------EXECUTING THE JOBS--------------------------"
    echo "-------------------------------------------------------------------"
    docker exec -it resourcemanager bash -c "hadoop jar hadoop/movieFinders/movieFinder-1.0-SNAPSHOT.jar /user/root/movie/data/ratings.csv /user/root/movie/data/movies.csv /out/ && exit"
}

printingResutls() {

    echo "-------------------------------------------------------------------"
    echo "---------------------PRINTING THE RESULTS--------------------------"
    echo "-------------------------------------------------------------------"
    docker exec -it resourcemanager bash -c "hadoop fs -cat /out/job3/* && exit"
}

gettingTheResults() (

    echo "-------------------------------------------------------------------"
    echo "-----------------------GETTING THE RESULTS-------------------------"
    echo "-------------------------------------------------------------------"
    rm data/_SUCCESS
    rm data/part-r-00000
    docker exec -it resourcemanager bash -c "hadoop fs -get /out/job3/* /hadoop/movieFinders/ && exit"
)

docker-compose up -d

wait_for_container_health "namenode"
wait_for_container_health "resourcemanager"
    
put_files_in_namenode
build_project
exec_movie_finder

if [ "$1" == "-p" ] || [ "$1" == "--print" ] || [ "$2" == "-p" ] || [ "$2" == "--print" ]; then
    printingResutls
fi

if [ "$1" == "-g" ] || [ "$1" == "--get" ] || [ "$2" == "-g" ] || [ "$2" == "--get" ]; then
    gettingTheResults
fi