# hacker-news-data

I want to find historical frequencies of key terms using the HackerNews API. I plan to analyse:

- Frequency of programming language mentions, to determine relative popularity over time

- Categories of most popular posts

- which users have the highest cumulative score ( most site influence )


## Data Collection


I used the [Hacker News API](https://github.com/HackerNews/API) to collect all comments and posts(stories) from the [Hacker News Site](news.ycombinator.com)

I used 20 instances of the worker.py process in parallel to request each comment and post.

All data was stored in a Postgres database. 

The dispatcher.py script created the instances of the worker.py and assigned jobs to the workers (number of objects to download) to prevent duplicate downloads. A jobs table was used in postgres to organise the jobs for the workers.

Example API response:

```js

{
  "by" : "dhouston",
  "descendants" : 71,
  "id" : 8863,
  "kids" : [ 9224, 8917, 8884, 8887, 8952, 8869, 8873, 8958, 8940, 8908, 9005, 9671, 9067, 9055, 8865, 8881, 8872, 8955, 10403, 8903, 8928, 9125, 8998, 8901, 8902, 8907, 8894, 8870, 8878, 8980, 8934, 8943, 8876 ],
  "score" : 104,
  "time" : 1175714200,
  "title" : "My YC app: Dropbox - Throw away your USB drive",
  "type" : "story",
  "url" : "http://www.getdropbox.com/u/2/screencast.html"
}

```

### Design History

- I started with a simple program that fetched from the api and stored to an sqlite database

- I implemented multiprocessing to speed up the retrieval rate, taking download length from ~100 days to ~10 days. I made the change from sqlite to postgres to make use of jsonb and lean it for kids column.

- Made use of Asynchronous functions to process quer

## Data Processing





