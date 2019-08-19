# Simple backtesting system

### Preparing dev environment

Make sure to have installed Python3.

```
pip3 install pipenv
pipenv sync
```

### Write tests

```
pipenv run test
```

### Install new package

```
pipenv install <package_name>
pipenv lock
```

### Build the container

`docker build . -t gianluca91/backtesting:latest`

### Run the container

`docker run --rm -p 8888:8888 gianluca91/backtesting`

### Run the container mounting the volume

`docker run --rm -p 8888:8888 --mount src="$(pwd)/src",target=/lib/src,type=bind gianluca91/backtesting`

### Push the container to Docker Hub

`docker login --username=yourhubusername --email=youremail@company.com`
