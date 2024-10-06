#!/bin/bash

docker build . -t acidview/py-docker
docker run -p 8501:8501 acidview/py-docker
