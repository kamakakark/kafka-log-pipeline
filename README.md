# kafka-log-pipeline

# log-producer

## Setup

install kafka using docker

1) https://towardsdatascience.com/kafka-docker-python-408baf0e1088
   git clone https://github.com/wurstmeister/kafka-docker.git 
   cd kafka-docker/
2) Inside kafka-docker, create a text file named docker-compose-expose.yml with the 
 following content (you can use your favourite text editor):

3) Now, you are ready to start the Kafka cluster with:
   docker-compose -f docker-compose-expose.yml up   


4)  run log-producer
    python log-producer-main.py localhost:9092 /Users/a569514/Downloads/others/foobar.txt

brew install pip
pip install --user pipenv

Note : Consider adding this directory to PATH or, if you prefer to suppress this warning, use --no-warn-script-location.
pipenv install requests

## Start log consumer
pipenv run python main.py