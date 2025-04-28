PROJECT_NAME=bigdata-env

build:
	docker build -t $(PROJECT_NAME) .

up:
	docker-compose up

down:
	docker-compose down

clean:
	docker-compose down --rmi all --volumes --remove-orphans
	docker system prune -af

shell:
	docker exec -it bigdata-container bash