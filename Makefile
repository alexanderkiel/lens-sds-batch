deps-prod:
	lein with-profile production deps :tree

build:
	lein test
	docker build .
