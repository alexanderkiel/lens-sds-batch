deps-prod:
	lein with-profile production deps :tree

build:
	docker build .
