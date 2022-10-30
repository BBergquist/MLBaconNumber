# MLBaconNumber
Charlie Morton was teammates with Tom Glavine who was teammates with Phil Niekro who played in 1964. If we continue this pattern, what is the fewest number of players required to reach the [first baseball game in a professional league](https://www.retrosheet.org/1stGame.htm)?

This repository gives a definitive answer by searching the graph of professional teammates for the shortest path. Cutting to the chase, the fewest number of players required is 8:

| Player | Late Career | Early Career |
| --- | --- | --- |
| Charlie Morton | 2022 Atlanta Braves | 2008 Atlanta Braves |
| Tom Glavine | 2008 Atlanta Braves | 1987 Atlanta Braves |
| Phil Niekro | 1987 Atlanta Braves | 1964 Milwaukee Braves |
| Warren Spahn | 1964 Milwaukee Braves | 1942 Boston Braves |
| Paul Waner | 1942 Boston Braves | 1926 Pittsburgh Pirates |
| Babe Adams | 1926 Pittsburgh Pirates | 1906 St. Louis Cardinals |
| Jake Beckley | 1906 St. Louis Cardinals | 1889 Pittsburgh Alleghenys |
| Deacon White | 1889 Pittsburgh Alleghenys | *Played in the 1st Game* |

This concept is the same as [Erdős numbers](https://en.wikipedia.org/wiki/Erd%C5%91s_number) and [Bacon numbers](https://en.wikipedia.org/wiki/Six_Degrees_of_Kevin_Bacon#Bacon_numbers), but since playing careers are much shorter than academic or acting careers, the "MLBacon number" of today's players are quickly increasing. As recently as 2008, the fewest number of players was 7 (via Tom Glavine, who is list above). By 2024 or 2025, the fewest number of players will likely be 9. For a table of all players and their MLBacon number, see [data/bacon_numbers.csv](data/bacon_numbers.csv). The colums for bacon_numbers are:

| Column Name | Description |
| --- | --- |
| player_id | This id is defined by retrosheet.org. It is guaranteed to uniquely identify a player. |
| last_name | The player's last name |
| first_name | The player's first name |
| bacon_number | The player's bacon_number |
| max_year | The last year the player was in a professional league. |
| v0 | v0 stands for vertex 0. This is always the same as player_id. |
| e1 | e0 stands for edge 1. This is the team where the players in v0 and v1 played together. |
| v1 | v1 (vertex 1) is the next player in the chain (e.g., Tom Glavine). This is also their retrosheet id. |
| ... | *There are more vertex and edge columns that continue the chain to the first game.* |

Unfortunately, I haven't found a good way to search Retrosheets or Google for looking up a player, so the easiest method might be using excel to filter the MLBacon number CSV to find the player's first and last name.

The vertex and edge terms are because I built a [graph](https://en.wikipedia.org/wiki/Graph_(discrete_mathematics)) of all players and their teammates to find the shortest path to the first game. If you are more interested in the graphs, see the "How it works" section.

## How it works (high-level)
1. Example of input roster
1. Building the graph (explain why teammate-to-teammate is too complex)
1. Running the shortest path algorithm. Example of output table.
1. Building the output table.
## Technologies used
## How to run locally
## Contributing
## Acknowledgements
