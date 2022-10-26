# MLBaconNumber
Charlie Morton was teammates with Tom Glavine who was teammates with Phil Niekro who played in 1964. If we continue this pattern, what is the fewest number of players required to reach the [first baseball game in a professional league](https://www.retrosheet.org/1stGame.htm)?

This repository gives a definitive answer by searching the graph of professional teammates for the shortest path. Cutting to the chase, the fewest number of players required is 8:

| Player | Start | End |
| --- | --- | --- |
| Old | oldest team | newest team |

As recently as <>, the fewest number of players was 7:


This concept is the same as [Erdős numbers](https://en.wikipedia.org/wiki/Erd%C5%91s_number) and [Bacon numbers](https://en.wikipedia.org/wiki/Six_Degrees_of_Kevin_Bacon#Bacon_numbers), but since playing careers are much shorter than academic or acting careers, the "MLBacon number" of today's players are quickly increasing. For a table of all players and their MLBacon number, see [data/bacon_numbers.csv](data/bacon_numbers.csv).

## How it works (high-level)
1. Example of input roster
1. Building the graph (explain why teammate-to-teammate is too complex)
1. Running the shortest path algorithm. Example of output table.
1. Building the output table.
## Technologies used
## How to run locally
## Contributing
## Acknowledgements
