
# MovieFinder
Hadoop Map Reduce movie finder project

Wrote an article about it : [Dev.to Article](https://dev.to/djulo/create-a-hadoop-map-reduce-project-with-multiple-mapper-multiple-jobs-and-multiple-inputs-48bp)

This project will take two files (ratings.csv) and (movies.csv) and count the numbers of users have for favourite film(s) a given list.

The output file look like this :

```
188 users have liked the following film(s) : Dangerous Minds (1995)

189 users have liked the following film(s) : Truman Show The (1998)

194 users have liked the following film(s) : 12 Angry Men (1957)
```

### Build the app

To build the app :

```bash
mvn clean package
```

Move the jar file to the data folder : 

```bash
mv target/movieFinder-1.0-SNAPSHOT.jar data/
```

###  How to run

To run the Hadoop stack use dopcker compose as : 

```bash
docker compose up -d
```

You will now connect to the namenode container :

```bash
docker exec -it namenode bash
```

In the folder *data/* put the movies.csv and ratings.csv

Use hadoop fs comment to put the files in hte namenode :

```bash
hadoop fs -mkdir -p /user/root/movie/data
hadoop fs -put hadoop/movieFinders/*.csv /user/root/movie/data
```

If you want to check if everyone is there :
```bash
hadoop fs -ls /user/root/movie/data/
```

Connect now to the resourcemanager container :

```bash
docker exec -it resourcemanager bash
```

Let's execute our hadoop jobs : 

```bash
hadoop jar hadoop/movieFinders/movieFinder-1.0-SNAPSHOT.jar /user/root/movie/data/ratings.csv /user/root/movie/data/movies.csv /out/
```

When it's done you can retrieve the output files :

```bash
hadoop fs -get /out/job3/* /hadoop/movieFinder/
```
