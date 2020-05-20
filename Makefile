init:
	docker-compose run scripts ./scripts/init.sh

recover:
	docker-compose run scripts ./scripts/recover.sh

restart_failed_tasks:
	docker-compose run scripts ./scripts/restart_failed_tasks.sh

export_authors:
	docker-compose run scripts ./scripts/authors.sh export authors.backup.json

import_authors:
	docker-compose run scripts ./scripts/authors.sh import authors.backup.json
