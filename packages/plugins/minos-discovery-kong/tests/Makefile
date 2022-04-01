up:
	$(MAKE) build
	echo "Starting containers..."
	docker-compose up --quiet-pull --detach

build:
	echo "Building images..."
	docker-compose build --progress=plain --pull

down:
	echo "Stopping containers..."
	docker-compose down --remove-orphans

clean:
	echo "Cleaning..."
	docker system prune -a
	docker volume rm $(docker volume ls -q)
	docker rm -f $(docker ps -a -q)

logs:
	echo "Showing logs..."
	docker-compose logs --follow

export-logs:
	echo "Showing logs..."
	docker-compose logs --no-color > logs.txt

integration-tests:
	echo "Running Integration Tests..."
	docker-compose run tavern