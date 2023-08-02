#!/bin/bash
docker inspect -f '{{.State.Status}}' airflow-webserver