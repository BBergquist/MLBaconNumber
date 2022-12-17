# Contributing
I welcome any contributions, whether it is bug reports, new ideas for analysis, or code changes. Please
use the Issues page for features and bug reports or tag me in a pull request for code changes.

## Running the code
To run the code, you will need:
1. If running windows: [WSL2 installed](https://learn.microsoft.com/en-us/windows/wsl/install)
1. Docker Desktop installed
2. VSCode installed
3. VSCode's [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
installed.

Once you have those pre-reqs installed, you can navigate to the [src/igraph](src/igraph) folder and then `Reopen folder in Dev Container`
to kick off the dev container process. It will take some time for the dev container to be setup, but once VSCode finishes,
you can open [src/igraph/mlbacon_numbers.igraph.ipynb](src/igraph/mlbacon_numbers.igraph.ipynb) and run it. If these steps
do not work for you, please open an issue. I haven't had the chance to validate these from a clean-slate, so there's a chance
some part of the setup hasn't been automated.
