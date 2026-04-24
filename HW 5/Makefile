.PHONY: up down clean test aggregate export query-ch query-pg query-s3 send-test-event

up:
	docker compose up -d --build

down:
	docker compose down

clean:
	docker compose down -v

test:
	docker compose run --rm test_runner

aggregate:
	curl -X POST "http://localhost:8001/run?date=$(shell date -u +%F)" || echo "Aggregation service not ready"

export:
	curl -X POST "http://localhost:8002/run?date=$(shell date -u +%F)&format=parquet" || echo "Export service not ready"

query-ch:
	docker exec clickhouse_db clickhouse-client --query "SELECT event_date, event_type, count() FROM movie_analytics.raw_events GROUP BY event_date, event_type ORDER BY event_date DESC LIMIT 20"

query-pg:
	docker exec postgres_analytics psql -U metrics_user -d metrics_store -c "SELECT metric_date, metric_name, metric_value FROM metrics_daily ORDER BY metric_date DESC, metric_name LIMIT 30"

query-s3:
	@echo "Open http://localhost:9002 (minioadmin/minioadmin) and browse bucket movie-analytics"

send-test-event:
	curl -X POST http://localhost:8000/events -H "Content-Type: application/json" -d '{"user_id":"demo_user","movie_id":"demo_movie","event_type":"VIEW_STARTED","device_type":"DESKTOP","session_id":"demo-session","progress_seconds":0}'