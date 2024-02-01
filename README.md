# movieFinder
Hadoop Map Reduce movie finder project


```
hadoop fs -rm -r /out/ 
hadoop jar hadoop/movieFinders/movieFinder-1.0-SNAPSHOT.jar /user/root/movie/data/ratings.csv /user/root/movie/data/movies.csv /out/
hadoop fs -cat /out/*
```