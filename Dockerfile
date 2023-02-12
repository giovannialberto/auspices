# Use the Python 3.8 Docker image as the base image
FROM python:3.8

# Set the environment variables to the values of the passed credentials
ENV BEARER_TOKEN=${BEARER_TOKEN}

# set the working directory in the container
WORKDIR /code

# copy the dependencies file to the working directory
COPY requirements.txt .

# install dependencies
RUN pip install -r requirements.txt

# copy the content of the local src directory to the working directory
COPY src/ .

# command to run on container start
CMD [ "python", "./main.py" ]