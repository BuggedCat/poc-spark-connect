# Spark Connect
*Para a versão em português [README](../README.md)* :sunglasses:

This project aims to demonstrate the use of [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) included in version 3.4 of Spark, which enables Spark and its ecosystem to be used from anywhere by connecting applications, IDEs and Notebooks.

## Spark Connect Architecture

![Spark Connect Architecture](imgs/spark-connect-api.png "Arquitetura do Spark Connect")

### How it works

The Spark Connect client translates DataFrame operations into unresolved logical query plans which are encoded using protocol buffers and sent to the server using the gRPC framework.[*](https://spark.apache.org/docs/latest/spark-connect-overview.html)

![Spark Connect Operations](imgs/spark-connect-communication.png "Funcionamento do Spark Connect")

### Project Docker cluster architecture


```mermaid
graph LR
    Client([Application]) .-> Spark-Connect;
    Jupyter([JupyterHub]) .-> Spark-Connect;
    subgraph Spark-Cluster
        Spark-Connect --> Driver;
        Driver --> Worker1;
        Driver --> Worker2;
        Driver --> Worker3;
        Driver --> Worker4;
    end
    classDef nodes fill:#326ce5,color:#fff;
    class Spark-Connect,Driver,Worker1,Worker2,Worker3,Worker4 nodes;
```

## Requirements

- [Docker and Docker Compose](https://docs.docker.com/engine/install/)
- [Python 3.10+](https://www.python.org/downloads/release/python-31010/)
- [Poetry](https://python-poetry.org/docs/) 

## How to run

### Start Spark local cluster

```bash
docker compose up --build
```

### Running the sample code


- In the project folder install the dependencies with Poetry
```bash
poetry install
```

- Activating the virtual environment
```bash
poetry shell
```

- Running the script
```bash
python src/read_sample.py
```

- If everithing worked there will be a new folder `rendimentos` and a new file `rendimentos-schema.json` inside the `data` folder.