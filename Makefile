.PHONY: help up down logs build clean

help:
	@echo "DistroSense commands:"
	@echo "  make up       - Start all services"
	@echo "  make down     - Stop all services"
	@echo "  make logs     - Tail logs"
	@echo "  make build    - Build all Docker images"
	@echo "  make clean    - Remove volumes + containers"

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

build:
	docker compose build

clean:
	docker compose down -v --remove-orphans