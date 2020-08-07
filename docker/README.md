# Using the docker image

```
cd /path/to/hadoop-lzo/
```

## Create a jar having amd64 and aarch64 libraries for hadoop-lzo

```
docker-compose -f docker/docker-compose.yaml run cross_compile
```
The default version of aarch64 gcc is `4.9-2016.02` and lzo is `2.10`. 
Update the parameter in `docker-compose.yaml` to use a version you want.
